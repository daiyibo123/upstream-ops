package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/bejix/upstream-ops/backend/notify"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func registerNotifications(g *gin.RouterGroup, d *Deps) {
	gpc := g.Group("/notifications/channels")
	gpc.GET("", func(c *gin.Context) {
		list, err := d.Notifies.ListChannels()
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": list})
	})
	gpc.POST("", func(c *gin.Context) { createNotifyChannel(c, d) })
	gpc.PUT("/:id", func(c *gin.Context) { updateNotifyChannel(c, d) })
	gpc.DELETE("/:id", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		if err := d.Notifies.DeleteChannel(id); err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	gpc.POST("/:id/test", func(c *gin.Context) { testNotify(c, d) })

	g.GET("/notifications/logs", func(c *gin.Context) {
		page, pageSize, err := parsePageQuery(c)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		list, total, err := d.Notifies.ListLogsPage(page, pageSize)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		channels, err := d.Notifies.ListChannels()
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		channelMeta := make(map[uint]gin.H, len(channels))
		for _, ch := range channels {
			channelMeta[ch.ID] = gin.H{
				"channel_name": ch.Name,
				"channel_type": ch.Type,
			}
		}
		items := make([]gin.H, 0, len(list))
		for _, item := range list {
			meta := channelMeta[item.ChannelID]
			row := gin.H{
				"id":                  item.ID,
				"channel_id":          item.ChannelID,
				"upstream_channel_id": item.UpstreamChannelID,
				"event":               item.Event,
				"subject":             item.Subject,
				"body":                item.Body,
				"success":             item.Success,
				"error_message":       item.ErrorMessage,
				"sent_at":             item.SentAt,
			}
			for k, v := range meta {
				row[k] = v
			}
			items = append(items, row)
		}
		pages := 1
		if total > 0 {
			pages = int((total + int64(pageSize) - 1) / int64(pageSize))
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"items":     items,
			"total":     total,
			"page":      page,
			"page_size": pageSize,
			"pages":     pages,
		}})
	})
}

type notifyChannelInput struct {
	Name          string                          `json:"name" binding:"required"`
	Type          storage.NotificationChannelType `json:"type" binding:"required"`
	Config        string                          `json:"config"` // JSON string；编辑时可留空保留原值
	Subscriptions string                          `json:"subscriptions"`
	Enabled       bool                            `json:"enabled"`
	ProxyEnabled  bool                            `json:"proxy_enabled"`
}

// normalizeSubscriptions 把输入的订阅 JSON 字符串规整为 "[]" 或合法订阅规则数组。
// 解析失败返回错误以便 API 返回 400。
func normalizeSubscriptions(raw string) (string, error) {
	if raw == "" || raw == "null" {
		return "[]", nil
	}
	var list []notify.Subscription
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		return "", err
	}
	out, err := json.Marshal(list)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func createNotifyChannel(c *gin.Context, d *Deps) {
	var in notifyChannelInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	configJSON := strings.TrimSpace(in.Config)
	if configJSON == "" {
		fail(c, http.StatusBadRequest, errors.New("config is required"))
		return
	}
	if !allowedNotifyType(in.Type) {
		fail(c, http.StatusBadRequest, errors.New("notification type is disabled"))
		return
	}
	if err := validateNotifyConfig(in.Type, configJSON); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	subs, err := normalizeSubscriptions(in.Subscriptions)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	cipherCfg, err := d.Cipher.Encrypt(configJSON)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	ch := &storage.NotificationChannel{
		Name:          in.Name,
		Type:          in.Type,
		ConfigCipher:  cipherCfg,
		Subscriptions: subs,
		Enabled:       in.Enabled,
		ProxyEnabled:  in.ProxyEnabled,
	}
	if err := d.Notifies.CreateChannel(ch); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": ch})
}

func updateNotifyChannel(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	ch, err := d.Notifies.FindChannel(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	oldType := ch.Type
	var in notifyChannelInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if !allowedNotifyType(in.Type) {
		fail(c, http.StatusBadRequest, errors.New("notification type is disabled"))
		return
	}
	configPatch := strings.TrimSpace(in.Config)
	if in.Type != ch.Type && configPatch == "" {
		fail(c, http.StatusBadRequest, errors.New("config is required when changing notification type"))
		return
	}
	subs, err := normalizeSubscriptions(in.Subscriptions)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	ch.Name = in.Name
	ch.Type = in.Type
	ch.Enabled = in.Enabled
	ch.ProxyEnabled = in.ProxyEnabled
	ch.Subscriptions = subs
	if configPatch != "" {
		configJSON := configPatch
		var err error
		if in.Type == oldType {
			configJSON, err = mergeNotifyConfig(d, ch, configPatch)
		}
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		if err := validateNotifyConfig(in.Type, configJSON); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		cipherCfg, err := d.Cipher.Encrypt(configJSON)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		ch.ConfigCipher = cipherCfg
	}
	if err := d.Notifies.UpdateChannel(ch); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": ch})
}

func allowedNotifyType(t storage.NotificationChannelType) bool {
	switch t {
	case storage.NotifyEmail, storage.NotifyWecom, storage.NotifyFeishu:
		return true
	default:
		return false
	}
}

func validateNotifyConfig(t storage.NotificationChannelType, cfg string) error {
	if strings.TrimSpace(cfg) == "" {
		return errors.New("config is required")
	}
	_, err := notify.Build(&storage.NotificationChannel{Type: t}, cfg)
	if err != nil {
		return fmt.Errorf("invalid notification config: %w", err)
	}
	return nil
}

func mergeNotifyConfig(d *Deps, ch *storage.NotificationChannel, patchJSON string) (string, error) {
	if strings.TrimSpace(patchJSON) == "" {
		return "", errors.New("config is required")
	}
	var patch map[string]any
	if err := json.Unmarshal([]byte(patchJSON), &patch); err != nil {
		return "", fmt.Errorf("invalid notification config: %w", err)
	}
	if len(patch) == 0 {
		return "", errors.New("notification config is empty")
	}
	if d.Cipher == nil || ch == nil || ch.ConfigCipher == "" {
		return patchJSON, nil
	}
	existingJSON, err := d.Cipher.Decrypt(ch.ConfigCipher)
	if err != nil {
		return "", fmt.Errorf("decrypt existing notification config: %w", err)
	}
	var merged map[string]any
	if strings.TrimSpace(existingJSON) != "" {
		if err := json.Unmarshal([]byte(existingJSON), &merged); err != nil {
			return "", fmt.Errorf("invalid existing notification config: %w", err)
		}
	}
	if merged == nil {
		merged = map[string]any{}
	}
	for key, value := range patch {
		merged[key] = value
	}
	out, err := json.Marshal(merged)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func testNotify(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	ch, err := d.Notifies.FindChannel(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	appTitle := publicDashboardTitle(d)
	msg := notify.Message{
		Subject: "测试通知",
		Body:    fmt.Sprintf("这是一条来自 %s 的测试消息。", appTitle),
	}
	if err := d.Dispatcher.Send(c.Request.Context(), ch, msg); err != nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}
