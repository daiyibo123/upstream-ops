package storage

import (
	"errors"
	"fmt"
	"strings"

	appcrypto "github.com/bejix/upstream-ops/backend/crypto"
	"gorm.io/gorm"
)

const (
	ChatGPTPoolName = "chatgpt号池"
	GrokPoolName    = "grok号池"

	GPTPoolGroupName  = "gpt号池"
	GrokPoolGroupName = "grok号池"

	GPTPoolScopeRef  = "pool-scope:gpt"
	GrokPoolScopeRef = "pool-scope:grok"

	fixedPoolMarker = "fixed-oauth-pool-scope"
)

var (
	ErrFixedPoolChannelImmutable = errors.New("fixed OAuth pool channel is immutable")
	ErrFixedPoolGroupImmutable   = errors.New("fixed OAuth pool group mapping is immutable")
)

type fixedOAuthPoolDefinition struct {
	poolType    OAuthPool
	channelType ChannelType
	channelName string
	siteURL     string
	groupRef    string
	groupName   string
	description string
}

var fixedOAuthPools = []fixedOAuthPoolDefinition{
	{
		poolType:    OAuthPoolChatGPT,
		channelType: ChannelTypeChatGPTPool,
		channelName: ChatGPTPoolName,
		siteURL:     "oauth-pool://chatgpt",
		groupRef:    GPTPoolScopeRef,
		groupName:   GPTPoolGroupName,
		description: "由 chatgpt号池中的可调度 OAuth 账号提供服务",
	},
	{
		poolType:    OAuthPoolGrok,
		channelType: ChannelTypeGrokPool,
		channelName: GrokPoolName,
		siteURL:     "oauth-pool://grok",
		groupRef:    GrokPoolScopeRef,
		groupName:   GrokPoolGroupName,
		description: "由 grok号池中的可调度 OAuth 账号提供服务",
	},
}

func IsFixedPoolChannelType(channelType ChannelType) bool {
	switch channelType {
	case ChannelTypeChatGPTPool, ChannelTypeGrokPool:
		return true
	default:
		return false
	}
}

// IsFixedPoolScopeRef intentionally accepts only the two reserved values.
// A prefix check would accidentally make future ordinary groups immutable.
func IsFixedPoolScopeRef(groupRef string) bool {
	switch strings.TrimSpace(groupRef) {
	case GPTPoolScopeRef, GrokPoolScopeRef:
		return true
	default:
		return false
	}
}

func IsPoolScopeGroup(key UpstreamGroupKey) bool {
	def, ok := fixedPoolDefinitionByGroupRef(key.GroupRef)
	return ok && key.ChannelType == def.channelType
}

func fixedPoolDefinitionByChannelType(channelType ChannelType) (fixedOAuthPoolDefinition, bool) {
	for _, def := range fixedOAuthPools {
		if def.channelType == channelType {
			return def, true
		}
	}
	return fixedOAuthPoolDefinition{}, false
}

func fixedPoolDefinitionByGroupRef(groupRef string) (fixedOAuthPoolDefinition, bool) {
	groupRef = strings.TrimSpace(groupRef)
	for _, def := range fixedOAuthPools {
		if def.groupRef == groupRef {
			return def, true
		}
	}
	return fixedOAuthPoolDefinition{}, false
}

func markFixedPoolChannel(channel *Channel) {
	if channel == nil {
		return
	}
	def, ok := fixedPoolDefinitionByChannelType(channel.Type)
	channel.Fixed = ok
	if ok {
		channel.PoolType = def.poolType
	} else {
		channel.PoolType = ""
	}
}

func markFixedPoolChannels(channels []Channel) {
	for i := range channels {
		markFixedPoolChannel(&channels[i])
	}
}

// EnsureFixedOAuthPools creates or repairs only the two virtual pool channels.
// EnsureFixedOAuthPoolScopes should normally be used so channel and group
// creation is atomic.
func (r *Channels) EnsureFixedOAuthPools() ([]Channel, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("channels repository is nil")
	}
	var channels []Channel
	err := r.db.Transaction(func(tx *gorm.DB) error {
		var err error
		channels, err = ensureFixedOAuthPoolChannels(tx)
		return err
	})
	return channels, err
}

// EnsureFixedOAuthPoolScopes is intentionally separate from AutoMigrate: the
// schema migration stays side-effect free, while application startup can call
// this after the cipher and repositories have been initialized.
func EnsureFixedOAuthPoolScopes(channels *Channels, groups *UpstreamGroupKeys, cipher *appcrypto.Cipher) error {
	if channels == nil || channels.db == nil {
		return errors.New("channels repository is nil")
	}
	if groups == nil || groups.db == nil {
		return errors.New("upstream group repository is nil")
	}
	if cipher == nil {
		return errors.New("cipher is nil")
	}
	if channels.db != groups.db {
		return errors.New("fixed OAuth pool repositories use different databases")
	}

	return channels.db.Transaction(func(tx *gorm.DB) error {
		fixedChannels, err := ensureFixedOAuthPoolChannels(tx)
		if err != nil {
			return err
		}
		for i, def := range fixedOAuthPools {
			if err := ensureFixedOAuthPoolGroup(tx, def, fixedChannels[i], cipher); err != nil {
				return err
			}
		}
		return nil
	})
}

func ensureFixedOAuthPoolChannels(tx *gorm.DB) ([]Channel, error) {
	channels := make([]Channel, 0, len(fixedOAuthPools))
	for _, def := range fixedOAuthPools {
		channel, err := ensureFixedOAuthPoolChannel(tx, def)
		if err != nil {
			return nil, err
		}
		channels = append(channels, channel)
	}
	return channels, nil
}

func ensureFixedOAuthPoolChannel(tx *gorm.DB, def fixedOAuthPoolDefinition) (Channel, error) {
	var channel Channel
	err := tx.Where("type = ?", def.channelType).Order("id ASC").First(&channel).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// Older experimental versions identified the records by their display
		// name. Adopt that row once, then use ChannelType as the stable identity.
		err = tx.Where("name = ?", def.channelName).First(&channel).Error
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return Channel{}, fmt.Errorf("find fixed %s channel: %w", def.poolType, err)
	}

	if errors.Is(err, gorm.ErrRecordNotFound) {
		channel = Channel{
			Name:           def.channelName,
			Type:           def.channelType,
			SiteURL:        def.siteURL,
			Username:       "system",
			CredentialMode: CredentialModeToken,
			MonitorEnabled: false,
			SortOrder:      1,
		}
		if err := tx.Session(&gorm.Session{SkipHooks: true}).Create(&channel).Error; err != nil {
			return Channel{}, fmt.Errorf("create fixed %s channel: %w", def.poolType, err)
		}
	} else {
		// Keep operator-facing ordering and accounting fields, while repairing
		// all identity and transport fields that define the virtual pool.
		if err := releaseReservedFixedPoolName(tx, def, channel.ID); err != nil {
			return Channel{}, err
		}
		updates := map[string]any{
			"name":            def.channelName,
			"type":            def.channelType,
			"site_url":        def.siteURL,
			"username":        "system",
			"credential_mode": CredentialModeToken,
			"monitor_enabled": false,
		}
		if err := tx.Session(&gorm.Session{SkipHooks: true}).Model(&Channel{}).Where("id = ?", channel.ID).Updates(updates).Error; err != nil {
			return Channel{}, fmt.Errorf("repair fixed %s channel: %w", def.poolType, err)
		}
		if err := tx.First(&channel, channel.ID).Error; err != nil {
			return Channel{}, fmt.Errorf("reload fixed %s channel: %w", def.poolType, err)
		}
	}
	markFixedPoolChannel(&channel)
	return channel, nil
}

func releaseReservedFixedPoolName(tx *gorm.DB, def fixedOAuthPoolDefinition, fixedID uint) error {
	var owner Channel
	err := tx.Where("name = ? AND id <> ?", def.channelName, fixedID).First(&owner).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("find conflicting fixed %s channel name: %w", def.poolType, err)
	}

	base := fmt.Sprintf("legacy-%s-channel-%d", def.poolType, owner.ID)
	name := base
	for suffix := 2; ; suffix++ {
		var count int64
		if err := tx.Model(&Channel{}).Where("name = ?", name).Count(&count).Error; err != nil {
			return fmt.Errorf("check legacy channel name: %w", err)
		}
		if count == 0 {
			break
		}
		name = fmt.Sprintf("%s-%d", base, suffix)
	}
	if err := tx.Session(&gorm.Session{SkipHooks: true}).Model(&Channel{}).Where("id = ?", owner.ID).Update("name", name).Error; err != nil {
		return fmt.Errorf("release reserved fixed %s channel name: %w", def.poolType, err)
	}
	return nil
}

func ensureFixedOAuthPoolGroup(tx *gorm.DB, def fixedOAuthPoolDefinition, channel Channel, cipher *appcrypto.Cipher) error {
	var matches []UpstreamGroupKey
	if err := tx.Where("group_ref = ?", def.groupRef).Order("id ASC").Find(&matches).Error; err != nil {
		return fmt.Errorf("find fixed %s group: %w", def.poolType, err)
	}

	var group UpstreamGroupKey
	if len(matches) > 0 {
		group = matches[0]
		for _, candidate := range matches {
			if candidate.ChannelID == channel.ID {
				group = candidate
				break
			}
		}
	}

	if group.ID == 0 {
		marker, err := cipher.Encrypt(fixedPoolMarker)
		if err != nil {
			return fmt.Errorf("encrypt fixed %s group marker: %w", def.poolType, err)
		}
		group = UpstreamGroupKey{
			ChannelID:             channel.ID,
			ChannelName:           channel.Name,
			ChannelURL:            channel.SiteURL,
			ChannelType:           def.channelType,
			ClientFormat:          "openai",
			RequestMode:           "responses",
			RequestModeSource:     "manual",
			AuthMode:              "bearer",
			GroupRef:              def.groupRef,
			GroupName:             def.groupName,
			GroupDesc:             def.description,
			Ratio:                 1,
			RatioScalePercent:     100,
			InputPricePerMillion:  DefaultInputPricePerMillion,
			OutputPricePerMillion: DefaultOutputPricePerMillion,
			KeyCipher:             marker,
			Enabled:               true,
			Status:                "alive",
		}
		if err := tx.Session(&gorm.Session{SkipHooks: true}).Create(&group).Error; err != nil {
			return fmt.Errorf("create fixed %s group: %w", def.poolType, err)
		}
	} else {
		keyCipher := group.KeyCipher
		if strings.TrimSpace(keyCipher) == "" {
			var err error
			keyCipher, err = cipher.Encrypt(fixedPoolMarker)
			if err != nil {
				return fmt.Errorf("encrypt fixed %s group marker: %w", def.poolType, err)
			}
		}
		updates := map[string]any{
			"channel_id":          channel.ID,
			"channel_name":        channel.Name,
			"channel_url":         channel.SiteURL,
			"channel_type":        def.channelType,
			"client_format":       "openai",
			"request_mode":        "responses",
			"request_mode_source": "manual",
			"auth_mode":           "bearer",
			"group_ref":           def.groupRef,
			"group_name":          def.groupName,
			"group_desc":          def.description,
			"key_cipher":          keyCipher,
			"enabled":             true,
			"status":              "alive",
			"failure_count":       0,
			"disabled_until":      nil,
			"last_error":          "",
		}
		if err := tx.Session(&gorm.Session{SkipHooks: true}).Model(&UpstreamGroupKey{}).Where("id = ?", group.ID).Updates(updates).Error; err != nil {
			return fmt.Errorf("repair fixed %s group: %w", def.poolType, err)
		}
	}

	// A short-lived experimental build could create the same reserved ref on
	// multiple channels. Keep the canonical row and remove only those invalid
	// duplicate synthetic groups.
	if len(matches) > 1 {
		if err := tx.Session(&gorm.Session{SkipHooks: true}).Where("group_ref = ? AND id <> ?", def.groupRef, group.ID).Delete(&UpstreamGroupKey{}).Error; err != nil {
			return fmt.Errorf("remove duplicate fixed %s groups: %w", def.poolType, err)
		}
	}
	return nil
}

func (key *UpstreamGroupKey) BeforeCreate(_ *gorm.DB) error {
	if key != nil && (IsFixedPoolScopeRef(key.GroupRef) || IsFixedPoolChannelType(key.ChannelType)) {
		return ErrFixedPoolGroupImmutable
	}
	return nil
}

func (key *UpstreamGroupKey) BeforeUpdate(tx *gorm.DB) error {
	if key == nil {
		return nil
	}
	if key.ID != 0 {
		var current UpstreamGroupKey
		err := tx.Session(&gorm.Session{NewDB: true, SkipHooks: true}).First(&current, key.ID).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		currentFixed := IsFixedPoolScopeRef(current.GroupRef)
		destinationFixed := IsFixedPoolScopeRef(key.GroupRef)
		if destinationFixed && !currentFixed {
			return ErrFixedPoolGroupImmutable
		}
		if currentFixed && (key.ChannelID != current.ChannelID || key.ChannelType != current.ChannelType || key.GroupRef != current.GroupRef) {
			return ErrFixedPoolGroupImmutable
		}
		return nil
	}

	if !tx.Statement.Changed("ChannelID", "ChannelType", "GroupRef") {
		return nil
	}
	matched, err := fixedGroupsMatchedByStatement(tx)
	if err != nil {
		return err
	}
	if matched {
		return ErrFixedPoolGroupImmutable
	}
	return nil
}

func (key *UpstreamGroupKey) BeforeDelete(tx *gorm.DB) error {
	if key != nil && IsFixedPoolScopeRef(key.GroupRef) {
		return ErrFixedPoolGroupImmutable
	}
	matched, err := fixedGroupsMatchedByStatement(tx)
	if err != nil {
		return err
	}
	if matched {
		return ErrFixedPoolGroupImmutable
	}
	return nil
}

func fixedGroupsMatchedByStatement(tx *gorm.DB) (bool, error) {
	query := tx.Session(&gorm.Session{NewDB: true, SkipHooks: true}).Model(&UpstreamGroupKey{})
	if tx.Statement != nil && tx.Statement.Schema != nil {
		if value, ok := tx.Statement.Clauses["WHERE"]; ok && value.Expression != nil {
			query = query.Clauses(value.Expression)
		} else if key, ok := tx.Statement.Dest.(*UpstreamGroupKey); ok && key.ID != 0 {
			query = query.Where("id = ?", key.ID)
		} else {
			return false, nil
		}
	}
	var count int64
	if err := query.Where("group_ref IN ?", []string{GPTPoolScopeRef, GrokPoolScopeRef}).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}
