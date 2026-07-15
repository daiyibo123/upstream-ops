package api

import (
	"net/http"
	"strings"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/gin-gonic/gin"
)

type settingsConfigView struct {
	App           config.AppConfig           `json:"app"`
	Auth          config.AuthConfig          `json:"auth"`
	Scheduler     config.SchedulerConfig     `json:"scheduler"`
	Notifications config.NotificationsConfig `json:"notifications"`
	Proxy         config.ProxyConfig         `json:"proxy"`
	Upstream      config.UpstreamConfig      `json:"upstream"`
}

type settingsConfigInput struct {
	App           config.AppConfig           `json:"app" binding:"required"`
	Auth          config.AuthConfig          `json:"auth" binding:"required"`
	Scheduler     config.SchedulerConfig     `json:"scheduler" binding:"required"`
	Notifications config.NotificationsConfig `json:"notifications" binding:"required"`
	Proxy         config.ProxyConfig         `json:"proxy"`
	Upstream      config.UpstreamConfig      `json:"upstream"`
}

func registerSettings(g *gin.RouterGroup, d *Deps) {
	gs := g.Group("/settings")
	gs.GET("/config", func(c *gin.Context) { getSettingsConfig(c, d) })
	gs.PUT("/config", func(c *gin.Context) { saveSettingsConfig(c, d) })
	gs.POST("/apply", func(c *gin.Context) { applySettingsConfig(c, d) })
	gs.POST("/proxy/test", func(c *gin.Context) { testProxy(c) })
	gs.GET("/response-interception", func(c *gin.Context) {
		cfg, err := config.LoadFile(d.Runtime.ConfigPath())
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": cfg.App.ResponseInterceptionRules})
	})
	gs.PUT("/response-interception", func(c *gin.Context) {
		var rules []config.ResponseInterceptionRule
		if err := c.ShouldBindJSON(&rules); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		cfg, err := config.LoadFile(d.Runtime.ConfigPath())
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		clean := make([]config.ResponseInterceptionRule, 0, len(rules))
		for _, rule := range rules {
			rule.Content = strings.TrimSpace(rule.Content)
			if rule.Content != "" {
				clean = append(clean, rule)
			}
		}
		cfg.App.ResponseInterceptionRules = clean
		if err := config.Save(d.Runtime.ConfigPath(), cfg); err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		if _, err := d.Runtime.ApplyFromFile(); err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": clean})
	})
}

func getSettingsConfig(c *gin.Context, d *Deps) {
	cfg, err := config.LoadFile(d.Runtime.ConfigPath())
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"config_path": d.Runtime.ConfigPath(),
			"config": settingsConfigView{
				App:           cfg.App,
				Auth:          cfg.Auth,
				Scheduler:     cfg.Scheduler,
				Notifications: cfg.Notifications,
				Proxy:         cfg.Proxy,
				Upstream:      cfg.Upstream,
			},
		},
	})
}

func saveSettingsConfig(c *gin.Context, d *Deps) {
	var in settingsConfigInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}

	path := d.Runtime.ConfigPath()
	cfg, err := config.LoadFile(path)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}

	cfg.App.Title = in.App.Title
	cfg.App.NotificationPrefix = in.App.NotificationPrefix
	// The public dashboard reads this flag directly from config.yaml. Keep it
	// here instead of silently restoring the previous value on every save.
	cfg.App.HomepageCheapestEnabled = in.App.HomepageCheapestEnabled
	cfg.App.ResponseInterceptionRules = in.App.ResponseInterceptionRules
	cfg.Auth = in.Auth
	cfg.Scheduler = in.Scheduler
	cfg.Notifications = in.Notifications
	cfg.Proxy = in.Proxy
	cfg.Upstream = in.Upstream.WithDefaults()

	if err := config.Save(path, cfg); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"config_path": path,
			"message":     "已写入配置文件",
		},
	})
}

func applySettingsConfig(c *gin.Context, d *Deps) {
	result, err := d.Runtime.ApplyFromFile()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}
