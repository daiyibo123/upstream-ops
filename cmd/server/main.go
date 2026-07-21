package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bejix/upstream-ops/backend/api"
	"github.com/bejix/upstream-ops/backend/auth"
	"github.com/bejix/upstream-ops/backend/channel"
	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/logger"
	"github.com/bejix/upstream-ops/backend/monitor"
	"github.com/bejix/upstream-ops/backend/notify"
	"github.com/bejix/upstream-ops/backend/oauthadmin"
	"github.com/bejix/upstream-ops/backend/oauthpool"
	"github.com/bejix/upstream-ops/backend/runtimeconfig"
	"github.com/bejix/upstream-ops/backend/scheduler"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/bejix/upstream-ops/web"
	"github.com/gin-gonic/gin"

	// 注册 connector 实现。
	_ "github.com/bejix/upstream-ops/backend/connector/newapi"
	_ "github.com/bejix/upstream-ops/backend/connector/sub2api"
)

func main() {
	configPath := flag.String("config", "", "path to config.yaml (optional; env vars also supported)")
	flag.Parse()

	cfg, usedConfigPath, err := config.LoadWithPath(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config failed: %v\n", err)
		os.Exit(1)
	}
	resolvedConfigPath := config.ResolvePath(*configPath, usedConfigPath)

	log := logger.New(cfg.Log.Level, cfg.Log.Format)
	log.Info("starting application", "title", cfg.App.Title, "port", cfg.Server.Port, "mode", cfg.Server.Mode)

	if _, err := os.Stat(resolvedConfigPath); errors.Is(err, os.ErrNotExist) {
		if err := config.Save(resolvedConfigPath, cfg); err != nil {
			log.Error("create config failed", "path", resolvedConfigPath, "err", err)
			os.Exit(1)
		}
		log.Info("config created", "path", resolvedConfigPath)
	}

	cipher, err := crypto.NewCipher(cfg.Security.AppSecret)
	if err != nil {
		log.Error("init cipher failed (set APP_SECRET)", "err", err)
		os.Exit(1)
	}

	// Auth：Docker 自用部署默认开启；显式关闭时所有 /api/* 免 token。
	// 开启时账号/密码必填，token secret 缺省回退到 AppSecret。
	var authSvc *auth.Service
	if cfg.Auth.Enabled {
		tokenSecret := cfg.Auth.TokenSecret
		if tokenSecret == "" {
			tokenSecret = cfg.Security.AppSecret
		}
		authSvc, err = auth.New(
			cfg.Auth.Username,
			cfg.Auth.Password,
			tokenSecret,
			time.Duration(cfg.Auth.SessionTTLHours)*time.Hour,
		)
		if err != nil {
			log.Error("init auth failed (set ADMIN_USERNAME / ADMIN_PASSWORD or AUTH_ENABLED=false)", "err", err)
			os.Exit(1)
		}
		log.Info("auth enabled", "username", cfg.Auth.Username)
	} else {
		log.Warn("auth disabled — all /api/* endpoints are open; set AUTH_ENABLED=true for production exposure")
	}

	db, err := storage.Open(cfg.Database.ToStorageConfig())
	if err != nil {
		log.Error("open database failed", "err", err)
		os.Exit(1)
	}
	if err := storage.AutoMigrate(db); err != nil {
		log.Error("auto migrate failed", "err", err)
		os.Exit(1)
	}

	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	notifies := storage.NewNotifications(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monLogs := storage.NewMonitorLogs(db)
	gatewayKeys := storage.NewGatewayKeys(db)
	gatewayAffinities := storage.NewGatewayAffinities(db)
	upstreamGroupKeys := storage.NewUpstreamGroupKeys(db)
	if err := storage.EnsureFixedOAuthPoolScopes(channels, upstreamGroupKeys, cipher); err != nil {
		log.Error("ensure fixed OAuth pools failed", "err", err)
		os.Exit(1)
	}
	oauthAccounts := storage.NewOAuthAccounts(db, cipher)
	oauthPoolSvc := oauthpool.NewService(oauthAccounts)
	if err := oauthPoolSvc.UpdateProxyConfig(cfg.Proxy); err != nil {
		log.Error("configure OAuth pool proxy failed", "err", err)
		os.Exit(1)
	}
	oauthAdminSvc := oauthadmin.New(oauthAccounts, oauthPoolSvc)

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monLogs, cipher)
	channelSvc.UpdateProxyConfig(cfg.Proxy)
	channelSvc.UpdateUpstreamConfig(cfg.Upstream)
	dispatcher := notify.NewDispatcher(notifies, cipher, log, notify.Policy{
		AppTitle:                                 cfg.App.Title,
		NotificationPrefix:                       cfg.App.NotificationPrefix,
		BatchRateChanges:                         cfg.Notifications.BatchRateChanges,
		MinChangePct:                             cfg.Notifications.MinChangePct,
		BalanceLowCooldown:                       time.Duration(cfg.Notifications.BalanceLowCooldownMinutes) * time.Minute,
		SubscriptionDailyRemainingThresholdPct:   cfg.Notifications.SubscriptionDailyRemainingThresholdPct,
		SubscriptionWeeklyRemainingThresholdPct:  cfg.Notifications.SubscriptionWeeklyRemainingThresholdPct,
		SubscriptionMonthlyRemainingThresholdPct: cfg.Notifications.SubscriptionMonthlyRemainingThresholdPct,
		SubscriptionExpiryThreshold:              time.Duration(cfg.Notifications.SubscriptionExpiryThresholdHours) * time.Hour,
		SubscriptionAlertCooldown:                time.Duration(cfg.Notifications.SubscriptionAlertCooldownMinutes) * time.Minute,
		SendMaxAttempts:                          cfg.Notifications.SendMaxAttempts,
	})
	dispatcher.UpdateProxyConfig(cfg.Proxy)
	monitorSvc := monitor.NewService(channels, announcements, rates, monLogs, channelSvc, dispatcher, log)
	gatewaySvc := gateway.NewService(channels, gatewayKeys, gatewayAffinities, upstreamGroupKeys, cipher, channelSvc, log)
	gatewaySvc.UpdateUpstreamConfig(cfg.Upstream)
	gatewaySvc.UpdateAppConfig(cfg.App)
	gatewaySvc.UpdateProxyConfig(cfg.Proxy)
	gatewaySvc.SetOAuthPool(oauthPoolSvc)
	gatewaySvc.SetOAuthAccounts(oauthAccounts)
	usageLogs := storage.NewUsageLogs(db)
	gatewaySvc.SetUsageLogs(usageLogs)
	gatewaySvc.SetIPPolicies(storage.NewIPPolicies(db))

	schedulerFactory := func(scfg config.SchedulerConfig, pcfg config.ProxyConfig) *scheduler.Scheduler {
		return scheduler.New(scfg, monitorSvc, monLogs, rates, notifies, announcements, usageLogs, captchas, cipher, pcfg, gatewaySvc, log)
	}
	sch := schedulerFactory(cfg.Scheduler, cfg.Proxy)
	if err := sch.Start(); err != nil {
		log.Error("start scheduler failed", "err", err)
		os.Exit(1)
	}
	defer sch.Stop()

	runtimeMgr := runtimeconfig.New(
		resolvedConfigPath,
		cfg.Security.AppSecret,
		log,
		dispatcher,
		channelSvc,
		gatewaySvc,
		authSvc,
		sch,
		cfg.Proxy,
		cfg.Upstream,
		schedulerFactory,
	)
	runtimeMgr.SetOAuthPoolService(oauthPoolSvc)

	gin.SetMode(cfg.Server.Mode)
	router := gin.New()
	router.Use(gin.Recovery())
	if len(cfg.Server.TrustedProxies) > 0 {
		_ = router.SetTrustedProxies(cfg.Server.TrustedProxies)
	}

	// 仅在嵌入了真实前端产物时挂载静态 handler。
	// 本地 `go run` 跑出来的二进制 dist 是空占位，此时由 vite dev server 接管 :3010。
	var frontendFS fs.FS
	if web.HasFrontend() {
		frontendFS = web.DistFS()
		log.Info("frontend embedded, serving SPA on /")
	} else {
		log.Info("no embedded frontend, run vite dev server separately for UI")
	}

	api.Register(router, &api.Deps{
		DB:            db,
		Cipher:        cipher,
		Runtime:       runtimeMgr,
		Channels:      channels,
		Sessions:      authSessions,
		Captchas:      captchas,
		Notifies:      notifies,
		Announcements: announcements,
		Rates:         rates,
		MonLogs:       monLogs,
		ChannelSvc:    channelSvc,
		Monitor:       monitorSvc,
		Dispatcher:    dispatcher,
		Gateway:       gatewaySvc,
		OAuthAdmin:    oauthAdminSvc,
		Log:           log,
		Frontend:      frontendFS,
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", "err", err)
			os.Exit(1)
		}
	}()
	log.Info("http server listening", "addr", srv.Addr)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Info("shutdown signal received")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Error("http shutdown error", "err", err)
	}
	log.Info("bye")
}
