package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/gin-gonic/gin"
)

const maskedSettingSecret = "********"

type settingsConfigView struct {
	App           config.AppConfig           `json:"app"`
	Auth          config.AuthConfig          `json:"auth"`
	Scheduler     config.SchedulerConfig     `json:"scheduler"`
	Notifications config.NotificationsConfig `json:"notifications"`
	Proxy         config.ProxyConfig         `json:"proxy"`
	Upstream      config.UpstreamConfig      `json:"upstream"`
}

// settingsConfigInput 不再对子配置加 binding:"required"。gin 的 required 对结构体
// 值类型的语义是"整个结构体不能是零值"，一旦前端某个子配置（例如 notifications 的
// 数值全为 0、scheduler 的 cron 全为空）恰好落在零值，ShouldBindJSON 就会直接 400，
// 表现为"点保存没反应"。保存逻辑本身已经在已有配置文件基础上逐字段覆盖并用
// WithDefaults() 兜底，因此这里接受零值即可，缺失字段走各自默认。
type settingsConfigInput struct {
	App           config.AppConfig           `json:"app"`
	Auth          config.AuthConfig          `json:"auth"`
	Scheduler     config.SchedulerConfig     `json:"scheduler"`
	Notifications config.NotificationsConfig `json:"notifications"`
	Proxy         config.ProxyConfig         `json:"proxy"`
	Upstream      config.UpstreamConfig      `json:"upstream"`
}

func registerSettings(g *gin.RouterGroup, d *Deps) {
	gs := g.Group("/settings")
	gs.GET("/config", func(c *gin.Context) { getSettingsConfig(c, d) })
	gs.PUT("/config", func(c *gin.Context) { saveSettingsConfig(c, d) })
	gs.POST("/apply", func(c *gin.Context) { applySettingsConfig(c, d) })
	gs.POST("/proxy/test", func(c *gin.Context) { testProxy(c) })
	gs.GET("/proxy/targets", func(c *gin.Context) {
		items := []gin.H{
			{"id": config.ProxyTargetChatGPTPool, "name": "chatgpt号池", "kind": "oauth_pool", "fixed": true},
			{"id": config.ProxyTargetGrokPool, "name": "grok号池", "kind": "oauth_pool", "fixed": true},
			{"id": config.ProxyTargetGPTPoolChannel, "name": "gpt号池", "kind": "fixed_channel", "fixed": true},
			{"id": config.ProxyTargetGrokPoolChannel, "name": "grok号池", "kind": "fixed_channel", "fixed": true},
		}
		if d.Channels != nil {
			channels, err := d.Channels.List()
			if err != nil {
				fail(c, http.StatusInternalServerError, err)
				return
			}
			for _, channel := range channels {
				kind := strings.ToLower(strings.TrimSpace(string(channel.Type)))
				if kind == "chatgpt_pool" || kind == "grok_pool" {
					continue
				}
				items = append(items, gin.H{"id": config.ProxyChannelTarget(channel.ID), "name": channel.Name, "kind": "channel", "channel_id": channel.ID})
			}
		}
		c.JSON(http.StatusOK, gin.H{"data": items})
	})
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
	safeProxy := cfg.Proxy
	if strings.TrimSpace(safeProxy.Username) != "" {
		safeProxy.Username = maskSettingValue(safeProxy.Username)
	}
	if safeProxy.Password != "" {
		safeProxy.Password = maskedSettingSecret
	}
	safeProxy.SelectedTargets = config.CleanProxyTargets(safeProxy.SelectedTargets)
	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"config_path": d.Runtime.ConfigPath(),
			"config": settingsConfigView{
				App:           cfg.App,
				Auth:          cfg.Auth,
				Scheduler:     cfg.Scheduler,
				Notifications: cfg.Notifications,
				Proxy:         safeProxy,
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
	// 公益 Key 的展示业务数据（name/password 等）走独立的 /gateway/public-key 路径，
	// config.yaml 的 app.publicKey 已废弃。但单 IP 并发上限是运维配置，存 config.yaml
	// 并只同步这一个字段，避免误触公益 Key 的业务数据。
	cfg.App.PublicKey.IPConcurrencyLimit = in.App.PublicKey.IPConcurrencyLimit
	// 路由缓存粘性是运维配置，存 config.yaml。WithDefaults 兜底数值阈值，避免前端
	// 传入 0 / 不可达阈值让粘性逻辑退化。
	cfg.App.RouteAffinity = in.App.RouteAffinity.WithDefaults()
	cfg.Auth = in.Auth
	cfg.Scheduler = in.Scheduler
	cfg.Notifications = in.Notifications
	proxy := in.Proxy
	if proxy.Password == maskedSettingSecret {
		proxy.Password = cfg.Proxy.Password
	}
	if proxy.Username == maskSettingValue(cfg.Proxy.Username) {
		proxy.Username = cfg.Proxy.Username
	}
	proxy.SelectedTargets = config.CleanProxyTargets(proxy.SelectedTargets)
	if proxy.Enabled {
		if _, err := proxy.URL(); err != nil {
			fail(c, http.StatusBadRequest, fmt.Errorf("invalid proxy configuration: %w", err))
			return
		}
	}
	cfg.Proxy = proxy
	cfg.Upstream = in.Upstream.WithDefaults()

	if err := config.Save(path, cfg); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}

	// 保存后立即加载到运行时，做到"编辑保存即生效"，无需前端再点一次“应用”。
	// 独立的 /settings/apply 仍保留（供仅重载运行时的场景），但正常保存路径一步到位。
	// ApplyFromFile 失败不回滚已写入的文件：文件是权威配置源，下次重启或手动 apply
	// 仍会生效；这里只把错误透传给前端提示，避免"保存成功但运行时没变"却无任何反馈。
	result, applyErr := d.Runtime.ApplyFromFile()
	if applyErr != nil {
		fail(c, http.StatusInternalServerError, applyErr)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"config_path": path,
			"message":     "已保存并生效",
			"apply":       result,
		},
	})
}

func maskSettingValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= 2 {
		return maskedSettingSecret
	}
	return string(runes[:1]) + "***" + string(runes[len(runes)-1:])
}

func applySettingsConfig(c *gin.Context, d *Deps) {
	result, err := d.Runtime.ApplyFromFile()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}
