package scheduler

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/monitor"
	"github.com/bejix/upstream-ops/backend/storage"
	"gorm.io/gorm"
)

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := storage.Open(storage.DBConfig{
		Driver: storage.DBDriverSQLite,
		Path:   filepath.Join(t.TempDir(), "scheduler-test.db"),
	})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := storage.AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func TestRunRetentionDeletesAnnouncements(t *testing.T) {
	db := openTestDB(t)
	announcements := storage.NewUpstreamAnnouncements(db)
	notifies := storage.NewNotifications(db)
	monLogs := storage.NewMonitorLogs(db)
	rates := storage.NewRates(db)

	oldTime := time.Now().AddDate(0, 0, -10)
	if _, err := announcements.Sync(1, []storage.UpstreamAnnouncement{{
		ChannelID:   1,
		SourceKey:   "old",
		Content:     "old",
		FirstSeenAt: oldTime,
	}}); err != nil {
		t.Fatalf("sync announcement: %v", err)
	}

	s := New(
		config.SchedulerConfig{
			Retention: config.RetentionConfig{
				AnnouncementsDays: 1,
			},
		},
		&monitor.Service{},
		monLogs,
		rates,
		notifies,
		announcements,
		nil,
		nil,
		nil,
		config.ProxyConfig{},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	s.runRetention()

	list, total, err := announcements.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list announcements: %v", err)
	}
	if total != 0 || len(list) != 0 {
		t.Fatalf("announcements not cleaned: total=%d list=%#v", total, list)
	}
}

func TestRunRetentionDeletesOnlyUsageLogsWhenConfigured(t *testing.T) {
	db := openTestDB(t)
	announcements := storage.NewUpstreamAnnouncements(db)
	notifies := storage.NewNotifications(db)
	monLogs := storage.NewMonitorLogs(db)
	rates := storage.NewRates(db)
	usageLogs := storage.NewUsageLogs(db)
	channels := storage.NewChannels(db)
	gatewayKeys := storage.NewGatewayKeys(db)
	groupKeys := storage.NewUpstreamGroupKeys(db)

	oldTime := time.Now().AddDate(0, 0, -10)
	freshTime := time.Now()
	if err := usageLogs.Add(&storage.UsageLog{GatewayKeyName: "old", ChannelName: "old", TotalTokens: 1, CreatedAt: oldTime}); err != nil {
		t.Fatalf("insert old usage log: %v", err)
	}
	if err := usageLogs.Add(&storage.UsageLog{GatewayKeyName: "fresh", ChannelName: "fresh", TotalTokens: 1, CreatedAt: freshTime}); err != nil {
		t.Fatalf("insert fresh usage log: %v", err)
	}
	if err := monLogs.Append(&storage.MonitorLog{ChannelID: 1, Job: storage.MonitorJobBalance, Success: true, StartedAt: oldTime, FinishedAt: oldTime}); err != nil {
		t.Fatalf("insert monitor log: %v", err)
	}
	if err := notifies.AppendLog(&storage.NotificationLog{ChannelID: 1, Event: storage.EventBalanceLow, Subject: "old notification", Success: true, SentAt: oldTime}); err != nil {
		t.Fatalf("insert notification log: %v", err)
	}
	if _, err := announcements.Sync(1, []storage.UpstreamAnnouncement{{
		ChannelID:   1,
		SourceKey:   "old",
		Content:     "old announcement",
		FirstSeenAt: oldTime,
	}}); err != nil {
		t.Fatalf("insert announcement: %v", err)
	}
	channel := &storage.Channel{Name: "keep-channel", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("insert channel: %v", err)
	}
	if err := gatewayKeys.Create(&storage.GatewayKey{Name: "keep-key", KeyPrefix: "sk-keep", KeyHash: "hash-keep", KeyCipher: "cipher", Enabled: true}); err != nil {
		t.Fatalf("insert gateway key: %v", err)
	}
	if err := groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: channel.Name, ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", KeyCipher: "cipher", Status: "alive", Enabled: true,
	}); err != nil {
		t.Fatalf("insert upstream group key: %v", err)
	}

	s := New(
		config.SchedulerConfig{
			Retention: config.RetentionConfig{
				UsageLogsDays: 1,
			},
		},
		&monitor.Service{},
		monLogs,
		rates,
		notifies,
		announcements,
		usageLogs,
		nil,
		nil,
		config.ProxyConfig{},
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	s.runRetention()

	var usageCount, monitorCount, notificationCount, announcementCount, channelCount, gatewayKeyCount, groupKeyCount int64
	if err := db.Model(&storage.UsageLog{}).Count(&usageCount).Error; err != nil {
		t.Fatalf("count usage logs: %v", err)
	}
	if err := db.Model(&storage.MonitorLog{}).Count(&monitorCount).Error; err != nil {
		t.Fatalf("count monitor logs: %v", err)
	}
	if err := db.Model(&storage.NotificationLog{}).Count(&notificationCount).Error; err != nil {
		t.Fatalf("count notification logs: %v", err)
	}
	if err := db.Model(&storage.UpstreamAnnouncement{}).Count(&announcementCount).Error; err != nil {
		t.Fatalf("count announcements: %v", err)
	}
	if err := db.Model(&storage.Channel{}).Count(&channelCount).Error; err != nil {
		t.Fatalf("count channels: %v", err)
	}
	if err := db.Model(&storage.GatewayKey{}).Count(&gatewayKeyCount).Error; err != nil {
		t.Fatalf("count gateway keys: %v", err)
	}
	if err := db.Model(&storage.UpstreamGroupKey{}).Count(&groupKeyCount).Error; err != nil {
		t.Fatalf("count upstream group keys: %v", err)
	}
	if usageCount != 1 {
		t.Fatalf("usage logs count = %d, want only the fresh row", usageCount)
	}
	if monitorCount != 1 || notificationCount != 1 || announcementCount != 1 || channelCount != 1 || gatewayKeyCount != 1 || groupKeyCount != 1 {
		t.Fatalf("non-usage data changed: monitor=%d notification=%d announcement=%d channel=%d gatewayKey=%d groupKey=%d",
			monitorCount, notificationCount, announcementCount, channelCount, gatewayKeyCount, groupKeyCount)
	}
}
