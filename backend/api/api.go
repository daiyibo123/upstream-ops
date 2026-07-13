// Package api 注册所有 HTTP 路由，组装各业务 handler。
//
// 单用户场景下走 HMAC token 鉴权：账号密码写在 config 里，登录后下发 token。
package api

import (
	"context"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"

	"github.com/bejix/upstream-ops/backend/channel"
	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/notify"
	"github.com/bejix/upstream-ops/backend/runtimeconfig"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type monitorService interface {
	RefreshBalance(ctx context.Context, c *storage.Channel) error
	RefreshRates(ctx context.Context, c *storage.Channel) error
	CheckSubscriptionUsageAlerts(ctx context.Context, c *storage.Channel) error
}

type channelService interface {
	Create(in channel.CreateInput) (*storage.Channel, error)
	Update(id uint, in channel.UpdateInput) (*storage.Channel, error)
	Delete(id uint) error
	ClearLoginInfo(id uint) (*storage.Channel, error)
	TestLogin(ctx context.Context, channelID uint) error
	RedeemCode(ctx context.Context, channelID uint, code string) (*connector.RedeemResult, error)
	GetSubscriptionInfo(ctx context.Context, channelID uint) (*connector.SubscriptionInfo, error)
	CreateSubscription(ctx context.Context, channelID uint, req connector.SubscriptionRequest) (*connector.SubscriptionLaunch, error)
	GetSubscriptionUsage(ctx context.Context, channelID uint) (*connector.SubscriptionUsageInfo, error)
	ListAPIKeys(ctx context.Context, channelID uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error)
	ListAPIKeyGroups(ctx context.Context, channelID uint) ([]connector.APIKeyGroup, error)
	CreateAPIKey(ctx context.Context, channelID uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error)
	UpdateAPIKey(ctx context.Context, channelID uint, keyID int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error)
	DeleteAPIKey(ctx context.Context, channelID uint, keyID int64) error
	RevealAPIKey(ctx context.Context, channelID uint, keyID int64) (string, error)
}

// Deps 把所有 handler 需要的依赖打包传入。
type Deps struct {
	DB            *gorm.DB
	Cipher        *crypto.Cipher
	Runtime       *runtimeconfig.Manager
	Channels      *storage.Channels
	Sessions      *storage.AuthSessions
	Captchas      *storage.Captchas
	Notifies      *storage.Notifications
	Announcements *storage.UpstreamAnnouncements
	Rates         *storage.Rates
	MonLogs       *storage.MonitorLogs
	ChannelSvc    channelService
	Monitor       monitorService
	Dispatcher    *notify.Dispatcher
	Gateway       *gateway.Service
	Log           *slog.Logger

	// Frontend 可选：传入嵌入的前端 dist 文件系统。nil 表示不挂载（本地开发用 vite dev server）。
	Frontend fs.FS
}

// Register 把所有路由挂到给定 gin engine。
func Register(r *gin.Engine, d *Deps) {
	r.GET("/healthz", func(c *gin.Context) {
		sqlDB, err := d.DB.DB()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "down", "err": err.Error()})
			return
		}
		if err := sqlDB.Ping(); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "db_down", "err": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	api := r.Group("/api")
	registerPublicDashboard(api.Group("/public"), d)
	if d.Runtime != nil {
		api.Use(d.Runtime.AuthMiddleware())
	}
	{
		registerVersion(api, d)
		registerAuth(api, d)
		registerChannels(api, d)
		registerNotifications(api, d)
		registerAnnouncements(api, d)
		registerRates(api, d)
		registerMonitorLogs(api, d)
		registerDashboard(api, d)
		registerSettings(api, d)
		registerGatewayAPI(api, d)
	}

	registerGatewayProxy(r, d)

	if d.Frontend != nil {
		registerFrontend(r, d.Frontend, d)
	}
}

func gatewayResponsesAliasPath(method, path string) (string, bool) {
	if method != http.MethodPost {
		return "", false
	}
	clean := "/" + strings.Trim(strings.TrimSpace(path), "/")
	switch clean {
	case "/responses", "/v1/responses":
		return "/v1/responses", true
	}
	parts := strings.Split(strings.Trim(clean, "/"), "/")
	if len(parts) == 2 && parts[1] == "responses" {
		switch strings.ToLower(parts[0]) {
		case "", "api", "assets", "healthz":
			return "", false
		default:
			return "/v1/responses", true
		}
	}
	return "", false
}

// registerFrontend 把嵌入的前端 dist 挂在根路径，并处理 SPA fallback：
//
//   - GET /assets/*  → 直接返回文件
//   - GET /          → 返回 index.html
//   - GET /channels  → 返回 index.html（React Router 客户端路由）
//
// /api/*、/healthz 都已被前面的具体路由占了，不会走到这里。
// 安全起见仍然做一次前缀拦截，避免任何意外情况下"未鉴权读 index.html"压到 /api 上。
func registerFrontend(r *gin.Engine, dist fs.FS, d *Deps) {
	fileServer := http.FileServer(http.FS(dist))

	r.NoRoute(func(c *gin.Context) {
		path := c.Request.URL.Path
		if proxyPath, ok := gatewayResponsesAliasPath(c.Request.Method, path); ok {
			handleGatewayProxy(c, d, gatewayProxyPathWithQuery(c, proxyPath))
			return
		}

		// 永远不让 SPA fallback 覆盖 API / 健康检查路径。
		if strings.HasPrefix(path, "/api/") || path == "/healthz" {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}

		// 文件存在就直接 serve，否则回落到 index.html。
		clean := strings.TrimPrefix(path, "/")
		if clean == "" {
			clean = "index.html"
		}
		if _, err := fs.Stat(dist, clean); err != nil {
			c.Request.URL.Path = "/"
		}
		fileServer.ServeHTTP(c.Writer, c.Request)
	})
}

// fail 统一错误响应。
func fail(c *gin.Context, status int, err error) {
	c.JSON(status, gin.H{"error": err.Error()})
}
