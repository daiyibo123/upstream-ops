package storage

import (
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := Open(DBConfig{
		Driver:       DBDriverSQLite,
		Path:         filepath.Join(t.TempDir(), "test.db"),
		MaxOpenConns: 20,
		MaxIdleConns: 5,
	})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get sql.DB: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	return db
}

func TestUsageLogsClearPreservesGatewayKeyUsage(t *testing.T) {
	db := openTestDB(t)
	keys := NewGatewayKeys(db)
	logs := NewUsageLogs(db)
	key := &GatewayKey{
		Name:      "test-key",
		KeyPrefix: "sk-test",
		KeyHash:   "hash-test",
		KeyCipher: "cipher",
		Enabled:   true,
	}
	if err := keys.Create(key); err != nil {
		t.Fatalf("create gateway key: %v", err)
	}
	now := time.Now()
	if err := keys.AddUsage(key.ID, 10, 5, 15, 4, 0.03, now); err != nil {
		t.Fatalf("add usage: %v", err)
	}
	if err := logs.Add(&UsageLog{
		GatewayKeyID:       key.ID,
		GatewayKeyName:     key.Name,
		GatewayKeyIsPublic: true,
		PromptTokens:       10,
		CompletionTokens:   5,
		TotalTokens:        15,
		CachedTokens:       4,
		CreatedAt:          now,
	}); err != nil {
		t.Fatalf("add usage log: %v", err)
	}
	items, total, err := logs.List(50, 0)
	if err != nil {
		t.Fatalf("list usage logs: %v", err)
	}
	if total != 1 || len(items) != 1 || !items[0].GatewayKeyIsPublic {
		t.Fatalf("usage log did not preserve public-key snapshot: %#v", items)
	}

	deleted, err := logs.Clear()
	if err != nil {
		t.Fatalf("clear usage logs: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted logs = %d, want 1", deleted)
	}
	_, total, err = logs.List(50, 0)
	if err != nil {
		t.Fatalf("list usage logs: %v", err)
	}
	if total != 0 {
		t.Fatalf("usage logs total = %d, want 0", total)
	}
	updated, err := keys.FindByID(key.ID)
	if err != nil {
		t.Fatalf("load gateway key: %v", err)
	}
	if updated.TodayTokens != 15 || updated.TotalTokens != 15 ||
		updated.TodayPromptTokens != 10 || updated.TotalPromptTokens != 10 ||
		updated.TodayCachedTokens != 4 || updated.TotalCachedTokens != 4 {
		t.Fatalf("gateway key usage changed after clear: %#v", updated)
	}
}

func TestUsageLogsStats(t *testing.T) {
	db := openTestDB(t)
	logs := NewUsageLogs(db)
	entries := []UsageLog{
		{Status: "success", TotalTokens: 30, FirstTokenMS: 100, DurationMS: 400},
		{Status: "failed", TotalTokens: 0, FirstTokenMS: 0, DurationMS: 200},
		{Status: "estimated", TotalTokens: 20, FirstTokenMS: 300, DurationMS: 600},
	}
	for i := range entries {
		if err := logs.Add(&entries[i]); err != nil {
			t.Fatalf("add usage log %d: %v", i, err)
		}
	}

	stats, err := logs.Stats()
	if err != nil {
		t.Fatalf("usage log stats: %v", err)
	}
	if stats.TotalRequests != 3 || stats.SuccessRequests != 2 || stats.TotalTokens != 50 {
		t.Fatalf("unexpected usage log stats: %#v", stats)
	}
	if stats.AvgFirstTokenMS != 200 || stats.AvgDurationMS != 400 {
		t.Fatalf("unexpected usage log averages: %#v", stats)
	}
	usageItems, usageTotal, err := logs.ListView(50, 0, "usage")
	if err != nil || usageTotal != 2 || len(usageItems) != 2 {
		t.Fatalf("usage view = total %d items %#v err %v", usageTotal, usageItems, err)
	}
	eventItems, eventTotal, err := logs.ListView(50, 0, "events")
	if err != nil || eventTotal != 1 || len(eventItems) != 1 || eventItems[0].Status != "failed" {
		t.Fatalf("events view = total %d items %#v err %v", eventTotal, eventItems, err)
	}
	usageStats, err := logs.StatsView("usage")
	if err != nil || usageStats.TotalRequests != 2 || usageStats.SuccessRequests != 2 || usageStats.TotalTokens != 50 {
		t.Fatalf("usage-only stats = %#v err %v", usageStats, err)
	}
}

func TestIPPolicyUpsertUpdatesBlockedMessage(t *testing.T) {
	db := openTestDB(t)
	policies := NewIPPolicies(db)
	item := &IPPolicy{IP: "203.0.113.8", Blocked: true, BlockedMessage: "首次封禁提示"}
	if err := policies.Upsert(item); err != nil {
		t.Fatalf("create IP policy: %v", err)
	}
	item.BlockedMessage = "更新后的封禁提示"
	if err := policies.Upsert(item); err != nil {
		t.Fatalf("update IP policy: %v", err)
	}
	stored, err := policies.Find(item.IP)
	if err != nil {
		t.Fatalf("find IP policy: %v", err)
	}
	if stored == nil || stored.BlockedMessage != item.BlockedMessage {
		t.Fatalf("blocked message was not updated: %#v", stored)
	}
}

func TestAutoMigrateNormalizesLegacyUnprobedGroupStatuses(t *testing.T) {
	db := openTestDB(t)
	for index, status := range []string{"unknown", "unchecked", "untested", ""} {
		group := &UpstreamGroupKey{ChannelID: uint(index + 1), GroupRef: "legacy-status-" + strconv.Itoa(index), GroupName: "legacy", Status: status}
		if err := db.Create(group).Error; err != nil {
			t.Fatalf("create %q legacy group: %v", status, err)
		}
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("rerun auto migrate: %v", err)
	}
	var remaining int64
	if err := db.Model(&UpstreamGroupKey{}).Where("status IS NULL OR TRIM(status) = '' OR LOWER(TRIM(status)) IN ?", []string{"unknown", "unchecked", "untested"}).Count(&remaining).Error; err != nil {
		t.Fatalf("count legacy statuses: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("legacy unprobed statuses remaining = %d", remaining)
	}
}

func TestAutoMigrateNormalizesTransientGroupStatusesAndFailureCounts(t *testing.T) {
	db := openTestDB(t)
	future := time.Now().Add(time.Hour)
	for index, status := range []string{"rate_limited", "network_error", "timeout", "upstream_error", "server_error"} {
		group := &UpstreamGroupKey{
			ChannelID: uint(index + 100), GroupRef: "transient-status-" + strconv.Itoa(index), GroupName: "transient",
			Enabled: true, Status: status, FailureCount: 4, DisabledUntil: &future,
		}
		if err := db.Create(group).Error; err != nil {
			t.Fatalf("create %q transient group: %v", status, err)
		}
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("rerun auto migrate: %v", err)
	}
	var groups []UpstreamGroupKey
	if err := db.Where("group_ref LIKE ?", "transient-status-%").Find(&groups).Error; err != nil {
		t.Fatalf("load normalized transient groups: %v", err)
	}
	if len(groups) != 5 {
		t.Fatalf("normalized transient groups = %d, want 5", len(groups))
	}
	for _, group := range groups {
		if group.Status != "alive" || group.FailureCount != 0 || group.DisabledUntil != nil {
			t.Fatalf("transient group was not fully normalized: %#v", group)
		}
	}
}

func TestExpiredCooldownResetsTransientFailureCount(t *testing.T) {
	db := openTestDB(t)
	past := time.Now().Add(-time.Minute)
	channel := &Channel{Name: "cooldown-channel", Type: ChannelTypeNewAPI, SiteURL: "https://example.test"}
	if err := db.Create(channel).Error; err != nil {
		t.Fatalf("create cooldown channel: %v", err)
	}
	group := &UpstreamGroupKey{ChannelID: channel.ID, GroupRef: "cooldown-reset", GroupName: "cooldown-reset", KeyCipher: "cipher", Enabled: true, Status: "network_error", FailureCount: 3, DisabledUntil: &past}
	if err := db.Create(group).Error; err != nil {
		t.Fatalf("create cooldown group: %v", err)
	}
	repo := NewUpstreamGroupKeys(db)
	if _, err := repo.ListCandidates(time.Now()); err != nil {
		t.Fatalf("list candidates: %v", err)
	}
	stored, err := repo.FindByID(group.ID)
	if err != nil {
		t.Fatalf("reload cooldown group: %v", err)
	}
	if stored.Status != "alive" || stored.FailureCount != 0 || stored.DisabledUntil != nil {
		t.Fatalf("expired cooldown should fully recover the group: %#v", stored)
	}
}

func TestUpstreamGroupKeySearchMatchesChannelURLNameAndGroup(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "示例渠道", SiteURL: "https://relay.example.test/v1", Type: ChannelTypeNewAPI}
	if err := db.Create(channel).Error; err != nil {
		t.Fatalf("create channel: %v", err)
	}
	repo := NewUpstreamGroupKeys(db)
	if err := repo.Upsert(&UpstreamGroupKey{ChannelID: channel.ID, ChannelName: "旧名称", ChannelType: channel.Type, GroupRef: "group-a", GroupName: "高速分组", KeyCipher: "cipher", Enabled: true}); err != nil {
		t.Fatalf("create group: %v", err)
	}
	for _, query := range []string{"relay.example.test", "示例渠道", "高速分组", "group-a"} {
		items, total, err := repo.ListPage(10, 0, query)
		if err != nil || total != 1 || len(items) != 1 {
			t.Fatalf("search %q: total=%d items=%#v err=%v", query, total, items, err)
		}
		if items[0].ChannelName != channel.Name || items[0].ChannelURL != channel.SiteURL {
			t.Fatalf("search result source was not refreshed from channel: %#v", items[0])
		}
	}
}

func TestUpsertKeepsManualRatioScaleDuringAutomaticSync(t *testing.T) {
	db := openTestDB(t)
	repo := NewUpstreamGroupKeys(db)
	initial := &UpstreamGroupKey{
		ChannelID:         7,
		ChannelName:       "upstream",
		GroupRef:          "auto-group",
		GroupName:         "old group",
		Ratio:             0.2,
		RatioScalePercent: 25,
		KeyCipher:         "cipher",
		Enabled:           true,
		Status:            "alive",
	}
	if err := repo.Upsert(initial); err != nil {
		t.Fatalf("create group: %v", err)
	}
	// This mirrors an overwrite sync: upstream metadata and the advertised
	// ratio are refreshed, but the operator's real-cost correction is local
	// configuration and must survive the sync.
	if err := repo.Upsert(&UpstreamGroupKey{
		ChannelID:         initial.ChannelID,
		ChannelName:       initial.ChannelName,
		GroupRef:          initial.GroupRef,
		GroupName:         "renamed group",
		Ratio:             0.5,
		RatioScalePercent: 100,
		Enabled:           true,
	}); err != nil {
		t.Fatalf("upsert synced group: %v", err)
	}
	stored, err := repo.FindByChannelGroup(initial.ChannelID, initial.GroupRef)
	if err != nil || stored == nil {
		t.Fatalf("reload synced group: group=%#v err=%v", stored, err)
	}
	if stored.Ratio != 0.5 || stored.RatioScalePercent != 25 {
		t.Fatalf("sync overwrote manual ratio correction: %#v", stored)
	}
}

func TestAggregateBalanceTrend(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now().In(trendLocation)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)
	day2 := day0.AddDate(0, 0, -2)

	snapshots := []BalanceSnapshot{
		{ChannelID: 1, Balance: 10, SampledAt: day2.Add(9 * time.Hour)},
		{ChannelID: 1, Balance: 20, SampledAt: day2.Add(12 * time.Hour)},
		{ChannelID: 2, Balance: 5, SampledAt: day2.Add(10 * time.Hour)},
		{ChannelID: 1, Balance: 7, SampledAt: day1.Add(11 * time.Hour)},
		{ChannelID: 2, Balance: 3, SampledAt: day1.Add(18 * time.Hour)},
		{ChannelID: 2, Balance: 9, SampledAt: day0.Add(8 * time.Hour)},
		{ChannelID: 2, Balance: 11, SampledAt: day0.Add(22 * time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot := snapshot
		if err := rates.AppendBalance(&snapshot); err != nil {
			t.Fatalf("append balance: %v", err)
		}
	}

	got, err := rates.AggregateBalanceTrend(3)
	if err != nil {
		t.Fatalf("aggregate balance trend: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 days, got %d", len(got))
	}

	want := []DailyAggregate{
		{Day: day2, Balance: 25},
		{Day: day1, Balance: 10},
		{Day: day0, Balance: 11},
	}
	for i := range want {
		if !got[i].Day.Equal(want[i].Day) {
			t.Fatalf("day %d mismatch: got %s want %s", i, got[i].Day, want[i].Day)
		}
		if got[i].Balance != want[i].Balance {
			t.Fatalf("balance %d mismatch: got %v want %v", i, got[i].Balance, want[i].Balance)
		}
	}
}

func TestChannelProxyEnabledPersists(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	ch := &Channel{
		Name:           "proxy-channel",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		ProxyEnabled:   true,
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	got, err := channels.FindByID(ch.ID)
	if err != nil {
		t.Fatalf("find channel: %v", err)
	}
	if !got.ProxyEnabled {
		t.Fatal("proxy_enabled = false, want true")
	}
}

func TestProxyEnabledPersistsForCaptchaAndNotification(t *testing.T) {
	db := openTestDB(t)

	captchas := NewCaptchas(db)
	cfg := &CaptchaConfig{
		Name:         "solver-proxy",
		Type:         CaptchaCapSolver,
		APIKeyCipher: "x",
		Enabled:      true,
		ProxyEnabled: true,
	}
	if err := captchas.Create(cfg); err != nil {
		t.Fatalf("create captcha: %v", err)
	}
	gotCaptcha, err := captchas.FindByID(cfg.ID)
	if err != nil {
		t.Fatalf("find captcha: %v", err)
	}
	if !gotCaptcha.ProxyEnabled {
		t.Fatal("captcha proxy_enabled = false, want true")
	}

	notifies := NewNotifications(db)
	notify := &NotificationChannel{
		Name:         "notify-proxy",
		Type:         NotifyTelegram,
		ConfigCipher: "x",
		Enabled:      true,
		ProxyEnabled: true,
	}
	if err := notifies.CreateChannel(notify); err != nil {
		t.Fatalf("create notification: %v", err)
	}
	gotNotify, err := notifies.FindChannel(notify.ID)
	if err != nil {
		t.Fatalf("find notification: %v", err)
	}
	if !gotNotify.ProxyEnabled {
		t.Fatal("notification proxy_enabled = false, want true")
	}
}

func TestAggregateBalanceTrendFillsMissingDays(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now().In(trendLocation)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)
	day2 := day0.AddDate(0, 0, -2)

	snapshots := []BalanceSnapshot{
		{ChannelID: 1, Balance: 10, SampledAt: day2.Add(9 * time.Hour)},
		{ChannelID: 1, Balance: 20, SampledAt: day0.Add(12 * time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot := snapshot
		if err := rates.AppendBalance(&snapshot); err != nil {
			t.Fatalf("append balance: %v", err)
		}
	}

	got, err := rates.AggregateBalanceTrend(3)
	if err != nil {
		t.Fatalf("aggregate balance trend: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 days, got %d", len(got))
	}

	want := []DailyAggregate{
		{Day: day2, Balance: 10},
		{Day: day1, Balance: 0},
		{Day: day0, Balance: 20},
	}
	for i := range want {
		if !got[i].Day.Equal(want[i].Day) {
			t.Fatalf("day %d mismatch: got %s want %s", i, got[i].Day, want[i].Day)
		}
		if got[i].Balance != want[i].Balance {
			t.Fatalf("balance %d mismatch: got %v want %v", i, got[i].Balance, want[i].Balance)
		}
	}
}

func TestAggregateCostTrend(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now().In(trendLocation)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)
	day2 := day0.AddDate(0, 0, -2)

	snapshots := []CostSnapshot{
		{ChannelID: 1, TodayCost: 1.1, SampledAt: day2.Add(9 * time.Hour)},
		{ChannelID: 1, TodayCost: 2.2, SampledAt: day2.Add(18 * time.Hour)},
		{ChannelID: 2, TodayCost: 0.8, SampledAt: day2.Add(10 * time.Hour)},
		{ChannelID: 1, TodayCost: 3.5, SampledAt: day1.Add(11 * time.Hour)},
		{ChannelID: 2, TodayCost: 1.2, SampledAt: day1.Add(13 * time.Hour)},
		{ChannelID: 2, TodayCost: 1.8, SampledAt: day1.Add(22 * time.Hour)},
		{ChannelID: 1, TodayCost: 4.0, SampledAt: day0.Add(8 * time.Hour)},
		{ChannelID: 2, TodayCost: 2.5, SampledAt: day0.Add(21 * time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot := snapshot
		if err := rates.AppendCost(&snapshot); err != nil {
			t.Fatalf("append cost: %v", err)
		}
	}

	got, err := rates.AggregateCostTrend(3)
	if err != nil {
		t.Fatalf("aggregate cost trend: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 days, got %d", len(got))
	}

	want := []DailyCostAggregate{
		{Day: day2, Cost: 3.0},
		{Day: day1, Cost: 5.3},
		{Day: day0, Cost: 6.5},
	}
	for i := range want {
		if !got[i].Day.Equal(want[i].Day) {
			t.Fatalf("day %d mismatch: got %s want %s", i, got[i].Day, want[i].Day)
		}
		if got[i].Cost != want[i].Cost {
			t.Fatalf("cost %d mismatch: got %v want %v", i, got[i].Cost, want[i].Cost)
		}
	}
}

func TestAggregateCostTrendFillsMissingDays(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now().In(trendLocation)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)
	day2 := day0.AddDate(0, 0, -2)

	snapshots := []CostSnapshot{
		{ChannelID: 1, TodayCost: 1.5, SampledAt: day2.Add(9 * time.Hour)},
		{ChannelID: 1, TodayCost: 2.5, SampledAt: day0.Add(12 * time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot := snapshot
		if err := rates.AppendCost(&snapshot); err != nil {
			t.Fatalf("append cost: %v", err)
		}
	}

	got, err := rates.AggregateCostTrend(3)
	if err != nil {
		t.Fatalf("aggregate cost trend: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 days, got %d", len(got))
	}

	want := []DailyCostAggregate{
		{Day: day2, Cost: 1.5},
		{Day: day1, Cost: 0},
		{Day: day0, Cost: 2.5},
	}
	for i := range want {
		if !got[i].Day.Equal(want[i].Day) {
			t.Fatalf("day %d mismatch: got %s want %s", i, got[i].Day, want[i].Day)
		}
		if got[i].Cost != want[i].Cost {
			t.Fatalf("cost %d mismatch: got %v want %v", i, got[i].Cost, want[i].Cost)
		}
	}
}

func TestAggregateTrendUsesShanghaiDayBoundary(t *testing.T) {
	oldNow := trendNow
	trendNow = func() time.Time {
		return time.Date(2026, 6, 19, 16, 30, 0, 0, time.UTC)
	}
	t.Cleanup(func() { trendNow = oldNow })

	db := openTestDB(t)
	rates := NewRates(db)

	day0 := time.Date(2026, 6, 20, 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)

	balanceSnapshots := []BalanceSnapshot{
		{ChannelID: 1, Balance: 10, SampledAt: time.Date(2026, 6, 19, 15, 59, 0, 0, time.UTC)},
		{ChannelID: 1, Balance: 20, SampledAt: time.Date(2026, 6, 19, 16, 1, 0, 0, time.UTC)},
	}
	for _, snapshot := range balanceSnapshots {
		snapshot := snapshot
		if err := rates.AppendBalance(&snapshot); err != nil {
			t.Fatalf("append balance: %v", err)
		}
	}

	costSnapshots := []CostSnapshot{
		{ChannelID: 1, TodayCost: 1.5, SampledAt: time.Date(2026, 6, 19, 15, 59, 0, 0, time.UTC)},
		{ChannelID: 1, TodayCost: 2.5, SampledAt: time.Date(2026, 6, 19, 16, 1, 0, 0, time.UTC)},
	}
	for _, snapshot := range costSnapshots {
		snapshot := snapshot
		if err := rates.AppendCost(&snapshot); err != nil {
			t.Fatalf("append cost: %v", err)
		}
	}

	balances, err := rates.AggregateBalanceTrend(2)
	if err != nil {
		t.Fatalf("aggregate balance trend: %v", err)
	}
	if len(balances) != 2 {
		t.Fatalf("balance days = %d, want 2", len(balances))
	}
	if !balances[0].Day.Equal(day1) || balances[0].Balance != 10 {
		t.Fatalf("previous shanghai day = %#v, want day %s balance 10", balances[0], day1)
	}
	if !balances[1].Day.Equal(day0) || balances[1].Balance != 20 {
		t.Fatalf("current shanghai day = %#v, want day %s balance 20", balances[1], day0)
	}

	costs, err := rates.AggregateCostTrend(2)
	if err != nil {
		t.Fatalf("aggregate cost trend: %v", err)
	}
	if len(costs) != 2 {
		t.Fatalf("cost days = %d, want 2", len(costs))
	}
	if !costs[0].Day.Equal(day1) || costs[0].Cost != 1.5 {
		t.Fatalf("previous shanghai day cost = %#v, want day %s cost 1.5", costs[0], day1)
	}
	if !costs[1].Day.Equal(day0) || costs[1].Cost != 2.5 {
		t.Fatalf("current shanghai day cost = %#v, want day %s cost 2.5", costs[1], day0)
	}
}

func TestDeleteCostSnapshotsBefore(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now()
	oldSnapshot := CostSnapshot{ChannelID: 1, TodayCost: 1.2, SampledAt: now.AddDate(0, 0, -10)}
	newSnapshot := CostSnapshot{ChannelID: 1, TodayCost: 2.3, SampledAt: now.AddDate(0, 0, -2)}
	if err := rates.AppendCost(&oldSnapshot); err != nil {
		t.Fatalf("append old cost: %v", err)
	}
	if err := rates.AppendCost(&newSnapshot); err != nil {
		t.Fatalf("append new cost: %v", err)
	}

	deleted, err := rates.DeleteCostSnapshotsBefore(now.AddDate(0, 0, -5))
	if err != nil {
		t.Fatalf("delete cost snapshots: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}

	var count int64
	if err := db.Model(&CostSnapshot{}).Count(&count).Error; err != nil {
		t.Fatalf("count cost snapshots: %v", err)
	}
	if count != 1 {
		t.Fatalf("remaining count = %d, want 1", count)
	}
}

func TestTryClaimCooldown(t *testing.T) {
	db := openTestDB(t)
	notifications := NewNotifications(db)

	ok, err := notifications.TryClaimCooldown(1, EventBalanceLow, time.Minute)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !ok {
		t.Fatal("first claim should succeed")
	}

	ok, err = notifications.TryClaimCooldown(1, EventBalanceLow, time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if ok {
		t.Fatal("second claim should be blocked by cooldown")
	}

	oldTime := time.Now().Add(-2 * time.Minute)
	if err := db.Model(&NotificationCooldown{}).
		Where("channel_id = ? AND event = ?", 1, EventBalanceLow).
		Updates(map[string]any{
			"last_sent_at": oldTime,
			"updated_at":   oldTime,
		}).Error; err != nil {
		t.Fatalf("age cooldown: %v", err)
	}

	ok, err = notifications.TryClaimCooldown(1, EventBalanceLow, time.Minute)
	if err != nil {
		t.Fatalf("third claim: %v", err)
	}
	if !ok {
		t.Fatal("third claim should succeed after cooldown expires")
	}
}

func TestTryClaimCooldownConcurrent(t *testing.T) {
	db := openTestDB(t)
	notifications := NewNotifications(db)

	var claimed int32
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ok, err := notifications.TryClaimCooldown(2, EventBalanceLow, time.Minute)
			if err != nil {
				t.Errorf("concurrent claim: %v", err)
				return
			}
			if ok {
				atomic.AddInt32(&claimed, 1)
			}
		}()
	}
	wg.Wait()

	if claimed != 1 {
		t.Fatalf("expected exactly one successful claim, got %d", claimed)
	}
}

func TestUpstreamAnnouncementsSyncDedupes(t *testing.T) {
	db := openTestDB(t)
	announcements := NewUpstreamAnnouncements(db)

	now := time.Now()
	items := []UpstreamAnnouncement{
		{SourceKey: "a", Title: "A", Content: "one", FirstSeenAt: now},
		{SourceKey: "a", Title: "A2", Content: "dup", FirstSeenAt: now.Add(time.Second)},
	}
	newItems, err := announcements.Sync(1, items)
	if err != nil {
		t.Fatalf("sync announcements: %v", err)
	}
	if len(newItems) != 1 {
		t.Fatalf("new items = %d, want 1", len(newItems))
	}

	exists, err := announcements.Exists(1, "a")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Fatal("expected announcement to exist")
	}
}

func TestUpstreamAnnouncementsListLatest(t *testing.T) {
	db := openTestDB(t)
	announcements := NewUpstreamAnnouncements(db)

	now := time.Now()
	publishedOld := now.Add(-3 * time.Hour)
	publishedNew := now.Add(-1 * time.Hour)
	items := []UpstreamAnnouncement{
		{ChannelID: 1, SourceKey: "a", Content: "body", PublishedAt: &publishedOld, FirstSeenAt: now.Add(3 * time.Minute)},
		{ChannelID: 1, SourceKey: "b", Content: "body", PublishedAt: &publishedNew, FirstSeenAt: now.Add(1 * time.Minute)},
		{ChannelID: 1, SourceKey: "c", Content: "body", FirstSeenAt: now.Add(4 * time.Minute)},
	}
	for _, item := range items {
		item := item
		if err := db.Create(&item).Error; err != nil {
			t.Fatalf("create announcement: %v", err)
		}
	}

	list, err := announcements.ListLatest(2)
	if err != nil {
		t.Fatalf("list latest: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("list len = %d, want 2", len(list))
	}
	if list[0].SourceKey != "b" || list[1].SourceKey != "a" {
		t.Fatalf("unexpected order: %#v", list)
	}
}

func TestUpstreamAnnouncementsDeleteByChannel(t *testing.T) {
	db := openTestDB(t)
	announcements := NewUpstreamAnnouncements(db)

	now := time.Now()
	if _, err := announcements.Sync(1, []UpstreamAnnouncement{{
		SourceKey:   "a",
		Content:     "one",
		FirstSeenAt: now,
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}
	if _, err := announcements.Sync(2, []UpstreamAnnouncement{{
		SourceKey:   "b",
		Content:     "two",
		FirstSeenAt: now,
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}

	rows, err := announcements.DeleteByChannel(1)
	if err != nil {
		t.Fatalf("delete by channel: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	list, total, err := announcements.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list page: %v", err)
	}
	if total != 1 || len(list) != 1 || list[0].ChannelID != 2 {
		t.Fatalf("unexpected remaining announcements: total=%d list=%#v", total, list)
	}
}

func TestUpstreamAnnouncementsDeleteBefore(t *testing.T) {
	db := openTestDB(t)
	announcements := NewUpstreamAnnouncements(db)

	oldTime := time.Now().AddDate(0, 0, -10)
	newTime := time.Now()
	if _, err := announcements.Sync(1, []UpstreamAnnouncement{{
		SourceKey:   "old",
		Content:     "old",
		FirstSeenAt: oldTime,
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}
	if _, err := announcements.Sync(1, []UpstreamAnnouncement{{
		SourceKey:   "new",
		Content:     "new",
		FirstSeenAt: newTime,
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}

	rows, err := announcements.DeleteBefore(time.Now().AddDate(0, 0, -5))
	if err != nil {
		t.Fatalf("delete before: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	list, total, err := announcements.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list page: %v", err)
	}
	if total != 1 || len(list) != 1 || list[0].SourceKey != "new" {
		t.Fatalf("unexpected remaining announcements: total=%d list=%#v", total, list)
	}
}

func TestUpdateCosts(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)

	c := &Channel{
		Name:           "test",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := channels.Create(c); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	if err := channels.UpdateCosts(c.ID, 1.23, 9.87); err != nil {
		t.Fatalf("update costs: %v", err)
	}

	got, err := channels.FindByID(c.ID)
	if err != nil {
		t.Fatalf("find channel: %v", err)
	}
	if got.TodayCost == nil || *got.TodayCost != 1.23 {
		t.Fatalf("today cost mismatch: %#v", got.TodayCost)
	}
	if got.TotalCost == nil || *got.TotalCost != 9.87 {
		t.Fatalf("total cost mismatch: %#v", got.TotalCost)
	}
}

func TestHardDeleteAllowsReusingNames(t *testing.T) {
	db := openTestDB(t)

	channels := NewChannels(db)
	ch := &Channel{
		Name:           "demo",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := channels.Delete(ch.ID); err != nil {
		t.Fatalf("delete channel: %v", err)
	}
	ch = &Channel{
		Name:           "demo",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("recreate channel: %v", err)
	}

	captchas := NewCaptchas(db)
	cfg := &CaptchaConfig{
		Name:         "solver",
		Type:         CaptchaCapSolver,
		APIKeyCipher: "x",
		Enabled:      true,
	}
	if err := captchas.Create(cfg); err != nil {
		t.Fatalf("create captcha: %v", err)
	}
	if err := captchas.Delete(cfg.ID); err != nil {
		t.Fatalf("delete captcha: %v", err)
	}
	cfg = &CaptchaConfig{
		Name:         "solver",
		Type:         CaptchaCapSolver,
		APIKeyCipher: "x",
		Enabled:      true,
	}
	if err := captchas.Create(cfg); err != nil {
		t.Fatalf("recreate captcha: %v", err)
	}

	notifications := NewNotifications(db)
	notify := &NotificationChannel{
		Name:         "telegram",
		Type:         NotifyTelegram,
		ConfigCipher: "x",
		Enabled:      true,
	}
	if err := notifications.CreateChannel(notify); err != nil {
		t.Fatalf("create notification channel: %v", err)
	}
	if err := notifications.DeleteChannel(notify.ID); err != nil {
		t.Fatalf("delete notification channel: %v", err)
	}
	notify = &NotificationChannel{
		Name:         "telegram",
		Type:         NotifyTelegram,
		ConfigCipher: "x",
		Enabled:      true,
	}
	if err := notifications.CreateChannel(notify); err != nil {
		t.Fatalf("recreate notification channel: %v", err)
	}
}

func TestDeleteChannelCleansScopedState(t *testing.T) {
	db := openTestDB(t)

	channels := NewChannels(db)
	ch := &Channel{
		Name:           "demo",
		Type:           ChannelTypeSub2API,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	now := time.Now()
	if err := db.Create(&AuthSession{ChannelID: ch.ID}).Error; err != nil {
		t.Fatalf("create auth session: %v", err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: ch.ID, ModelName: "old", Ratio: 1, LastSeenAt: now}).Error; err != nil {
		t.Fatalf("create rate snapshot: %v", err)
	}
	if err := db.Create(&RateChangeLog{ChannelID: ch.ID, ModelName: "old", NewRatio: 1, ChangedAt: now}).Error; err != nil {
		t.Fatalf("create rate change: %v", err)
	}
	if err := db.Create(&BalanceSnapshot{ChannelID: ch.ID, Balance: 1, SampledAt: now}).Error; err != nil {
		t.Fatalf("create balance snapshot: %v", err)
	}
	if err := db.Create(&CostSnapshot{ChannelID: ch.ID, TodayCost: 1, SampledAt: now}).Error; err != nil {
		t.Fatalf("create cost snapshot: %v", err)
	}
	if err := db.Create(&MonitorLog{ChannelID: ch.ID, Job: MonitorJobBalance, Success: true, StartedAt: now, FinishedAt: now}).Error; err != nil {
		t.Fatalf("create monitor log: %v", err)
	}
	if err := db.Create(&NotificationCooldown{ChannelID: ch.ID, Event: EventBalanceLow, LastSentAt: now}).Error; err != nil {
		t.Fatalf("create cooldown: %v", err)
	}
	if err := db.Create(&NotificationLog{ChannelID: 99, UpstreamChannelID: ch.ID, Event: EventBalanceLow, Subject: "alert", Success: true, SentAt: now}).Error; err != nil {
		t.Fatalf("create notification log: %v", err)
	}
	if err := db.Create(&NotificationLog{ChannelID: 99, Event: EventBalanceLow, Subject: "demo 余额低于阈值", Success: true, SentAt: now}).Error; err != nil {
		t.Fatalf("create legacy notification log: %v", err)
	}
	if err := db.Create(&UpstreamAnnouncement{ChannelID: ch.ID, SourceKey: "a", Content: "deleted", FirstSeenAt: now}).Error; err != nil {
		t.Fatalf("create announcement: %v", err)
	}

	if err := channels.Delete(ch.ID); err != nil {
		t.Fatalf("delete channel: %v", err)
	}

	for _, tt := range []struct {
		name  string
		model any
	}{
		{"auth_sessions", &AuthSession{}},
		{"rate_snapshots", &RateSnapshot{}},
		{"rate_change_logs", &RateChangeLog{}},
		{"balance_snapshots", &BalanceSnapshot{}},
		{"cost_snapshots", &CostSnapshot{}},
		{"monitor_logs", &MonitorLog{}},
		{"notification_cooldowns", &NotificationCooldown{}},
		{"upstream_announcements", &UpstreamAnnouncement{}},
		{"notification_logs", &NotificationLog{}},
	} {
		var count int64
		q := db.Model(tt.model).Where("channel_id = ?", ch.ID)
		if tt.name == "notification_logs" {
			q = db.Model(tt.model).Where("upstream_channel_id = ? OR subject LIKE ?", ch.ID, "%"+ch.Name+"%")
		}
		if err := q.Count(&count).Error; err != nil {
			t.Fatalf("count %s: %v", tt.name, err)
		}
		if count != 0 {
			t.Fatalf("%s count = %d, want 0", tt.name, count)
		}
	}
}

func TestAutoMigrateDropsDeletedAtColumns(t *testing.T) {
	db := openTestDB(t)

	for _, ddl := range []string{
		"ALTER TABLE channels ADD COLUMN deleted_at datetime",
		"ALTER TABLE captcha_configs ADD COLUMN deleted_at datetime",
		"ALTER TABLE notification_channels ADD COLUMN deleted_at datetime",
		"CREATE INDEX idx_channels_deleted_at ON channels(deleted_at)",
		"CREATE INDEX idx_captcha_configs_deleted_at ON captcha_configs(deleted_at)",
		"CREATE INDEX idx_notification_channels_deleted_at ON notification_channels(deleted_at)",
	} {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("exec %q: %v", ddl, err)
		}
	}

	now := time.Now()
	activeChannel := &Channel{
		Name:           "active-channel",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	deletedChannel := &Channel{
		Name:           "deleted-channel",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := db.Create(activeChannel).Error; err != nil {
		t.Fatalf("create active channel: %v", err)
	}
	if err := db.Create(deletedChannel).Error; err != nil {
		t.Fatalf("create deleted channel: %v", err)
	}
	if err := db.Table("channels").Where("id = ?", deletedChannel.ID).Update("deleted_at", now).Error; err != nil {
		t.Fatalf("mark deleted channel: %v", err)
	}

	activeCaptcha := &CaptchaConfig{Name: "active-captcha", Type: CaptchaCapSolver, APIKeyCipher: "x", Enabled: true}
	deletedCaptcha := &CaptchaConfig{Name: "deleted-captcha", Type: CaptchaCapSolver, APIKeyCipher: "x", Enabled: true}
	if err := db.Create(activeCaptcha).Error; err != nil {
		t.Fatalf("create active captcha: %v", err)
	}
	if err := db.Create(deletedCaptcha).Error; err != nil {
		t.Fatalf("create deleted captcha: %v", err)
	}
	if err := db.Table("captcha_configs").Where("id = ?", deletedCaptcha.ID).Update("deleted_at", now).Error; err != nil {
		t.Fatalf("mark deleted captcha: %v", err)
	}

	activeNotify := &NotificationChannel{Name: "active-notify", Type: NotifyTelegram, ConfigCipher: "x", Enabled: true}
	deletedNotify := &NotificationChannel{Name: "deleted-notify", Type: NotifyTelegram, ConfigCipher: "x", Enabled: true}
	if err := db.Create(activeNotify).Error; err != nil {
		t.Fatalf("create active notification channel: %v", err)
	}
	if err := db.Create(deletedNotify).Error; err != nil {
		t.Fatalf("create deleted notification channel: %v", err)
	}
	if err := db.Table("notification_channels").Where("id = ?", deletedNotify.ID).Update("deleted_at", now).Error; err != nil {
		t.Fatalf("mark deleted notification channel: %v", err)
	}

	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}

	for _, table := range []string{"channels", "captcha_configs", "notification_channels"} {
		hasColumn, err := tableHasColumn(db, table, "deleted_at")
		if err != nil {
			t.Fatalf("inspect %s.deleted_at: %v", table, err)
		}
		if hasColumn {
			t.Fatalf("%s.deleted_at still exists", table)
		}
	}

	var count int64
	if err := db.Model(&Channel{}).Count(&count).Error; err != nil {
		t.Fatalf("count channels: %v", err)
	}
	if count != 1 {
		t.Fatalf("channel count = %d, want 1", count)
	}
	if err := db.Model(&CaptchaConfig{}).Count(&count).Error; err != nil {
		t.Fatalf("count captchas: %v", err)
	}
	if count != 1 {
		t.Fatalf("captcha count = %d, want 1", count)
	}
	if err := db.Model(&NotificationChannel{}).Count(&count).Error; err != nil {
		t.Fatalf("count notification channels: %v", err)
	}
	if count != 1 {
		t.Fatalf("notification channel count = %d, want 1", count)
	}
}
