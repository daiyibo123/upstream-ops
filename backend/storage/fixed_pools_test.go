package storage

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	appcrypto "github.com/bejix/upstream-ops/backend/crypto"
	"gorm.io/gorm"
)

func fixedPoolTestCipher(t *testing.T) *appcrypto.Cipher {
	t.Helper()
	cipher, err := appcrypto.NewCipher("fixed-pool-storage-test")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	return cipher
}

func TestAutoMigrateIncludesOAuthAccountsWithoutCreatingFixedPools(t *testing.T) {
	db := openTestDB(t)
	if !db.Migrator().HasTable(&OAuthAccount{}) {
		t.Fatal("shared AutoMigrate did not create oauth_accounts")
	}
	var channelCount int64
	if err := db.Model(&Channel{}).Where("type IN ?", []ChannelType{ChannelTypeChatGPTPool, ChannelTypeGrokPool}).Count(&channelCount).Error; err != nil {
		t.Fatalf("count fixed channels: %v", err)
	}
	if channelCount != 0 {
		t.Fatalf("AutoMigrate created %d fixed channels; fixed data must be ensured explicitly", channelCount)
	}
}

func TestEnsureFixedOAuthPoolScopesIdempotent(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	groups := NewUpstreamGroupKeys(db)
	cipher := fixedPoolTestCipher(t)

	if err := EnsureFixedOAuthPoolScopes(channels, groups, cipher); err != nil {
		t.Fatalf("first ensure: %v", err)
	}

	var firstChannels []Channel
	if err := db.Where("type IN ?", []ChannelType{ChannelTypeChatGPTPool, ChannelTypeGrokPool}).Order("id ASC").Find(&firstChannels).Error; err != nil {
		t.Fatalf("load first channels: %v", err)
	}
	var firstGroups []UpstreamGroupKey
	if err := db.Where("group_ref IN ?", []string{GPTPoolScopeRef, GrokPoolScopeRef}).Order("id ASC").Find(&firstGroups).Error; err != nil {
		t.Fatalf("load first groups: %v", err)
	}
	if len(firstChannels) != 2 || len(firstGroups) != 2 {
		t.Fatalf("first ensure created channels=%d groups=%d, want 2 and 2", len(firstChannels), len(firstGroups))
	}

	channelIDs := map[ChannelType]uint{}
	for _, channel := range firstChannels {
		channelIDs[channel.Type] = channel.ID
	}
	groupState := map[string]struct {
		id     uint
		cipher string
	}{}
	for _, group := range firstGroups {
		groupState[group.GroupRef] = struct {
			id     uint
			cipher string
		}{id: group.ID, cipher: group.KeyCipher}
		if !group.Enabled || group.Status != "alive" || group.KeyCipher == "" {
			t.Fatalf("fixed group is not schedulable: %+v", group)
		}
		def, ok := fixedPoolDefinitionByGroupRef(group.GroupRef)
		if !ok || group.ChannelType != def.channelType || group.ChannelID != channelIDs[def.channelType] {
			t.Fatalf("fixed group mapping is wrong: %+v", group)
		}
	}

	if err := EnsureFixedOAuthPoolScopes(channels, groups, cipher); err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	var secondChannels []Channel
	if err := db.Where("type IN ?", []ChannelType{ChannelTypeChatGPTPool, ChannelTypeGrokPool}).Order("id ASC").Find(&secondChannels).Error; err != nil {
		t.Fatalf("load second channels: %v", err)
	}
	var secondGroups []UpstreamGroupKey
	if err := db.Where("group_ref IN ?", []string{GPTPoolScopeRef, GrokPoolScopeRef}).Order("id ASC").Find(&secondGroups).Error; err != nil {
		t.Fatalf("load second groups: %v", err)
	}
	if len(secondChannels) != 2 || len(secondGroups) != 2 {
		t.Fatalf("second ensure created duplicates: channels=%d groups=%d", len(secondChannels), len(secondGroups))
	}
	for _, channel := range secondChannels {
		if channel.ID != channelIDs[channel.Type] {
			t.Fatalf("channel %s id changed from %d to %d", channel.Type, channelIDs[channel.Type], channel.ID)
		}
	}
	for _, group := range secondGroups {
		first := groupState[group.GroupRef]
		if group.ID != first.id || group.KeyCipher != first.cipher {
			t.Fatalf("group %s was not stable: before=%+v after id=%d cipher=%q", group.GroupRef, first, group.ID, group.KeyCipher)
		}
	}

	candidates, err := groups.ListCandidates(time.Now())
	if err != nil {
		t.Fatalf("list candidates: %v", err)
	}
	found := map[string]bool{}
	for _, candidate := range candidates {
		if IsFixedPoolScopeRef(candidate.GroupRef) {
			found[candidate.GroupRef] = true
		}
	}
	if !found[GPTPoolScopeRef] || !found[GrokPoolScopeRef] {
		t.Fatalf("fixed groups missing from candidates: %+v", found)
	}

	payload, err := json.Marshal(secondGroups)
	if err != nil {
		t.Fatalf("marshal groups: %v", err)
	}
	serialized := string(payload)
	if strings.Contains(serialized, "key_cipher") || strings.Contains(serialized, fixedPoolMarker) {
		t.Fatalf("serialized fixed groups exposed internal marker or cipher: %s", serialized)
	}
}

func TestEnsureFixedOAuthPoolScopesRepairsLegacyRecordsByStableIdentity(t *testing.T) {
	db := openTestDB(t)
	cipher := fixedPoolTestCipher(t)

	chatLegacy := Channel{Name: ChatGPTPoolName, Type: ChannelTypeNewAPI, SiteURL: "https://legacy-chat.test", Username: "legacy"}
	grokLegacy := Channel{Name: GrokPoolName, Type: ChannelTypeSub2API, SiteURL: "https://legacy-grok.test", Username: "legacy"}
	if err := db.Create(&chatLegacy).Error; err != nil {
		t.Fatalf("create chat legacy channel: %v", err)
	}
	if err := db.Create(&grokLegacy).Error; err != nil {
		t.Fatalf("create grok legacy channel: %v", err)
	}

	chatGroup := UpstreamGroupKey{
		ChannelID: chatLegacy.ID, ChannelType: ChannelTypeNewAPI,
		GroupRef: GPTPoolScopeRef, GroupName: "legacy-gpt", Ratio: 0.25,
		RatioScalePercent: 75, Priority: 7, Charity: true, KeyCipher: "legacy-chat-cipher",
		Enabled: false, Status: "disabled", FailureCount: 3,
	}
	grokGroup := UpstreamGroupKey{
		ChannelID: chatLegacy.ID, ChannelType: ChannelTypeNewAPI,
		GroupRef: GrokPoolScopeRef, GroupName: "legacy-grok", Ratio: 0.5,
		RatioScalePercent: 60, Priority: 4, KeyCipher: "legacy-grok-cipher",
		Enabled: false, Status: "dead", FailureCount: 3,
	}
	if err := db.Session(&gorm.Session{SkipHooks: true}).Create(&chatGroup).Error; err != nil {
		t.Fatalf("create chat legacy group: %v", err)
	}
	if err := db.Session(&gorm.Session{SkipHooks: true}).Create(&grokGroup).Error; err != nil {
		t.Fatalf("create grok legacy group: %v", err)
	}

	if err := EnsureFixedOAuthPoolScopes(NewChannels(db), NewUpstreamGroupKeys(db), cipher); err != nil {
		t.Fatalf("ensure legacy records: %v", err)
	}

	var chat Channel
	if err := db.Where("type = ?", ChannelTypeChatGPTPool).First(&chat).Error; err != nil {
		t.Fatalf("load repaired chat channel: %v", err)
	}
	var grok Channel
	if err := db.Where("type = ?", ChannelTypeGrokPool).First(&grok).Error; err != nil {
		t.Fatalf("load repaired grok channel: %v", err)
	}
	if chat.ID != chatLegacy.ID || grok.ID != grokLegacy.ID {
		t.Fatalf("legacy channel ids changed: chat %d/%d grok %d/%d", chatLegacy.ID, chat.ID, grokLegacy.ID, grok.ID)
	}

	var repairedChatGroup UpstreamGroupKey
	if err := db.Where("group_ref = ?", GPTPoolScopeRef).First(&repairedChatGroup).Error; err != nil {
		t.Fatalf("load repaired chat group: %v", err)
	}
	var repairedGrokGroup UpstreamGroupKey
	if err := db.Where("group_ref = ?", GrokPoolScopeRef).First(&repairedGrokGroup).Error; err != nil {
		t.Fatalf("load repaired grok group: %v", err)
	}
	if repairedChatGroup.ID != chatGroup.ID || repairedGrokGroup.ID != grokGroup.ID {
		t.Fatalf("legacy group ids changed")
	}
	if repairedChatGroup.ChannelID != chat.ID || repairedGrokGroup.ChannelID != grok.ID {
		t.Fatalf("legacy group mapping was not repaired: chat=%+v grok=%+v", repairedChatGroup, repairedGrokGroup)
	}
	if repairedChatGroup.Ratio != 0.25 || repairedChatGroup.RatioScalePercent != 75 || repairedChatGroup.Priority != 7 || !repairedChatGroup.Charity {
		t.Fatalf("chat scheduling preferences were not preserved: %+v", repairedChatGroup)
	}
	if !repairedChatGroup.Enabled || repairedChatGroup.Status != "alive" || repairedChatGroup.FailureCount != 0 || repairedChatGroup.DisabledUntil != nil {
		t.Fatalf("chat group was not reactivated: %+v", repairedChatGroup)
	}

	if err := db.Model(&Channel{}).Where("id = ?", chat.ID).Update("name", "renamed-chat-pool").Error; err != nil {
		t.Fatalf("corrupt chat name: %v", err)
	}
	if err := db.Model(&Channel{}).Where("id = ?", grok.ID).Update("name", "renamed-grok-pool").Error; err != nil {
		t.Fatalf("corrupt grok name: %v", err)
	}
	if err := EnsureFixedOAuthPoolScopes(NewChannels(db), NewUpstreamGroupKeys(db), cipher); err != nil {
		t.Fatalf("ensure by stable identity: %v", err)
	}
	if err := db.First(&chat, chat.ID).Error; err != nil || chat.Name != ChatGPTPoolName {
		t.Fatalf("chat name not repaired by type: channel=%+v err=%v", chat, err)
	}
	if err := db.First(&grok, grok.ID).Error; err != nil || grok.Name != GrokPoolName {
		t.Fatalf("grok name not repaired by type: channel=%+v err=%v", grok, err)
	}
}

func TestFixedPoolCannotBeOrdinarilyCreatedUpdatedOrDeleted(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	groups := NewUpstreamGroupKeys(db)
	if err := EnsureFixedOAuthPoolScopes(channels, groups, fixedPoolTestCipher(t)); err != nil {
		t.Fatalf("ensure fixed pools: %v", err)
	}

	if err := channels.Create(&Channel{Name: "forged-pool", Type: ChannelTypeChatGPTPool, SiteURL: "oauth-pool://forged"}); !errors.Is(err, ErrFixedPoolChannelImmutable) {
		t.Fatalf("create fixed type error = %v, want immutable", err)
	}
	chat, err := channels.List()
	if err != nil {
		t.Fatalf("list channels: %v", err)
	}
	var fixedChannel Channel
	for _, channel := range chat {
		if channel.Type == ChannelTypeChatGPTPool {
			fixedChannel = channel
			break
		}
	}
	if fixedChannel.ID == 0 || !fixedChannel.Fixed || fixedChannel.PoolType != "chatgpt" {
		t.Fatalf("fixed channel metadata missing: %+v", fixedChannel)
	}

	changed := fixedChannel
	changed.Name = "mutable"
	if err := channels.Update(&changed); !errors.Is(err, ErrFixedPoolChannelImmutable) {
		t.Fatalf("update fixed channel error = %v, want immutable", err)
	}
	if err := channels.Delete(fixedChannel.ID); !errors.Is(err, ErrFixedPoolChannelImmutable) {
		t.Fatalf("delete fixed channel error = %v, want immutable", err)
	}

	var fixedGroup UpstreamGroupKey
	if err := db.Where("group_ref = ?", GPTPoolScopeRef).First(&fixedGroup).Error; err != nil {
		t.Fatalf("load fixed group: %v", err)
	}
	if err := groups.Delete(fixedGroup.ID); !errors.Is(err, ErrFixedPoolGroupImmutable) {
		t.Fatalf("delete fixed group error = %v, want immutable", err)
	}
	altered := fixedGroup
	altered.ChannelID++
	if err := db.Save(&altered).Error; !errors.Is(err, ErrFixedPoolGroupImmutable) {
		t.Fatalf("alter fixed group mapping error = %v, want immutable", err)
	}
	if err := db.Model(&UpstreamGroupKey{}).Where("id = ?", fixedGroup.ID).Update("ratio", 0.42).Error; err != nil {
		t.Fatalf("update fixed group operational field: %v", err)
	}

	ordinary := Channel{Name: "ordinary", Type: ChannelTypeNewAPI, SiteURL: "https://ordinary.test"}
	if err := channels.Create(&ordinary); err != nil {
		t.Fatalf("create ordinary channel: %v", err)
	}
	forgedGroup := UpstreamGroupKey{
		ChannelID: ordinary.ID, ChannelType: ordinary.Type,
		GroupRef: GPTPoolScopeRef, GroupName: "forged", KeyCipher: "cipher", Enabled: true, Status: "alive",
	}
	if err := groups.Upsert(&forgedGroup); !errors.Is(err, ErrFixedPoolGroupImmutable) {
		t.Fatalf("create reserved fixed group error = %v, want immutable", err)
	}
	ordinaryScopedGroup := UpstreamGroupKey{
		ChannelID: ordinary.ID, ChannelType: ordinary.Type,
		GroupRef: "pool-scope:future", GroupName: "future", KeyCipher: "cipher", Enabled: true, Status: "alive",
	}
	if err := groups.Upsert(&ordinaryScopedGroup); err != nil {
		t.Fatalf("exact fixed-ref protection rejected an ordinary prefixed group: %v", err)
	}
	ordinary.Name = "ordinary-updated"
	if err := channels.Update(&ordinary); err != nil {
		t.Fatalf("update ordinary channel: %v", err)
	}
	if err := channels.Delete(ordinary.ID); err != nil {
		t.Fatalf("delete ordinary channel: %v", err)
	}
	if _, err := channels.FindByID(ordinary.ID); err == nil {
		t.Fatal("ordinary channel still exists after delete")
	}
}
