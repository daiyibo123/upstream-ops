// Package scheduler 用 robfig/cron 触发周期性扫描。
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/monitor"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	cfg           config.SchedulerConfig
	log           *slog.Logger
	cron          *cron.Cron
	monitor       *monitor.Service
	monLogs       *storage.MonitorLogs
	rates         *storage.Rates
	notifies      *storage.Notifications
	announcements *storage.UpstreamAnnouncements
	captchas      *storage.Captchas
	cipher        *crypto.Cipher
	proxy         config.ProxyConfig
	gateway       *gateway.Service
}

func New(
	cfg config.SchedulerConfig,
	m *monitor.Service,
	monLogs *storage.MonitorLogs,
	rates *storage.Rates,
	notifies *storage.Notifications,
	announcements *storage.UpstreamAnnouncements,
	captchas *storage.Captchas,
	cipher *crypto.Cipher,
	proxy config.ProxyConfig,
	gatewaySvc *gateway.Service,
	log *slog.Logger,
) *Scheduler {
	return &Scheduler{
		cfg:           cfg,
		log:           log,
		cron:          cron.New(cron.WithSeconds()),
		monitor:       m,
		monLogs:       monLogs,
		rates:         rates,
		notifies:      notifies,
		announcements: announcements,
		captchas:      captchas,
		cipher:        cipher,
		proxy:         proxy,
		gateway:       gatewaySvc,
	}
}

func (s *Scheduler) Start() error {
	if s.cfg.BalanceCron != "" {
		if _, err := s.cron.AddFunc(s.cfg.BalanceCron, s.runBalance); err != nil {
			return err
		}
	}
	if s.cfg.RateCron != "" {
		if _, err := s.cron.AddFunc(s.cfg.RateCron, s.runRates); err != nil {
			return err
		}
	}
	if s.cfg.GatewayHealthCron != "" && s.gateway != nil {
		if _, err := s.cron.AddFunc(s.cfg.GatewayHealthCron, s.runGatewayHealth); err != nil {
			return err
		}
	}
	if s.cfg.Retention.Cron != "" && s.hasRetention() {
		if _, err := s.cron.AddFunc(s.cfg.Retention.Cron, s.runRetention); err != nil {
			return err
		}
	}
	s.cron.Start()
	s.log.Info("scheduler started",
		"balanceCron", s.cfg.BalanceCron,
		"rateCron", s.cfg.RateCron,
		"gatewayHealthCron", s.cfg.GatewayHealthCron,
		"retentionCron", s.cfg.Retention.Cron,
		"concurrency", s.cfg.Concurrency,
	)
	return nil
}

func (s *Scheduler) Stop() {
	if s.cron != nil {
		<-s.cron.Stop().Done()
	}
}

func (s *Scheduler) runBalance() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s.monitor.ScanAllBalances(ctx)
}

func (s *Scheduler) runRates() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s.monitor.ScanAllRates(ctx)
}

func (s *Scheduler) runGatewayHealth() {
	if s.gateway == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	result, err := s.gateway.TestAllGroupKeys(ctx)
	if err != nil {
		s.log.Warn("gateway health check failed", "err", err)
		return
	}
	s.log.Info("gateway health checked", "checked", result.Checked, "alive", result.Alive, "dead", result.Dead)
}

func (s *Scheduler) hasRetention() bool {
	r := s.cfg.Retention
	return r.MonitorLogsDays > 0 ||
		r.BalanceSnapshotsDays > 0 ||
		r.NotificationLogsDays > 0 ||
		r.AnnouncementsDays > 0
}

// runRetention 按配置删除过期历史。任一表失败不影响其它，全部错误写日志。
func (s *Scheduler) runRetention() {
	r := s.cfg.Retention
	now := time.Now()

	if r.MonitorLogsDays > 0 {
		cutoff := now.AddDate(0, 0, -r.MonitorLogsDays)
		n, err := s.monLogs.DeleteBefore(cutoff)
		if err != nil {
			s.log.Warn("retention monitor_logs failed", "err", err)
		} else if n > 0 {
			s.log.Info("retention monitor_logs deleted", "rows", n, "before", cutoff)
		}
	}

	if r.BalanceSnapshotsDays > 0 {
		cutoff := now.AddDate(0, 0, -r.BalanceSnapshotsDays)
		n, err := s.rates.DeleteBalanceSnapshotsBefore(cutoff)
		if err != nil {
			s.log.Warn("retention balance_snapshots failed", "err", err)
		} else if n > 0 {
			s.log.Info("retention balance_snapshots deleted", "rows", n, "before", cutoff)
		}

		n, err = s.rates.DeleteCostSnapshotsBefore(cutoff)
		if err != nil {
			s.log.Warn("retention cost_snapshots failed", "err", err)
		} else if n > 0 {
			s.log.Info("retention cost_snapshots deleted", "rows", n, "before", cutoff)
		}
	}

	if r.NotificationLogsDays > 0 {
		cutoff := now.AddDate(0, 0, -r.NotificationLogsDays)
		n, err := s.notifies.DeleteLogsBefore(cutoff)
		if err != nil {
			s.log.Warn("retention notification_logs failed", "err", err)
		} else if n > 0 {
			s.log.Info("retention notification_logs deleted", "rows", n, "before", cutoff)
		}
	}

	if r.AnnouncementsDays > 0 && s.announcements != nil {
		cutoff := now.AddDate(0, 0, -r.AnnouncementsDays)
		n, err := s.announcements.DeleteBefore(cutoff)
		if err != nil {
			s.log.Warn("retention announcements failed", "err", err)
		} else if n > 0 {
			s.log.Info("retention announcements deleted", "rows", n, "before", cutoff)
		}
	}
}
