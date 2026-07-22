package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/runtimeconfig"
	"github.com/gin-gonic/gin"
)

func TestProxyTargetsExposeExactlyFourStableScopes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerSettings(router.Group("/api"), &Deps{})
	request := httptest.NewRequest(http.MethodGet, "/api/settings/proxy/targets", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var body struct {
		Data []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	want := []struct{ id, name string }{
		{config.ProxyTargetChatGPTPool, "gpt号池"},
		{config.ProxyTargetGrokPool, "grok号池"},
		{config.ProxyTargetGPTPoolChannel, "gpt渠道"},
		{config.ProxyTargetGrokPoolChannel, "grok渠道"},
	}
	if len(body.Data) != len(want) {
		t.Fatalf("proxy targets=%#v", body.Data)
	}
	for index := range want {
		if body.Data[index].ID != want[index].id || body.Data[index].Name != want[index].name {
			t.Fatalf("proxy target %d=%#v, want %#v", index, body.Data[index], want[index])
		}
	}
}

func TestSaveSettingsKeepsAppVersion(t *testing.T) {
	gin.SetMode(gin.TestMode)

	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := &config.Config{
		App: config.AppConfig{
			Title:                   "Old",
			NotificationPrefix:      "[Old] ",
			HomepageCheapestEnabled: true,
		},
		Auth: config.AuthConfig{SessionTTLHours: 168},
	}
	if err := config.Save(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	registerSettings(api, &Deps{
		Runtime: runtimeconfig.New(path, "", nil, nil, nil, nil, nil, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil),
	})

	body := `{
		"app":{"title":"New","notificationPrefix":"[New] ","homepageCheapestEnabled":false,"publicKey":{"enabled":true,"name":"公益 Key","key":"sk-public","password":"secret","passwordHint":"hint","expiresAt":"2099-01-01"}},
		"auth":{"enabled":false,"username":"admin","password":"","tokenSecret":"","sessionTTLHours":24},
		"scheduler":{"balanceCron":"37 */15 * * * *","rateCron":"13 */30 * * * *","concurrency":4,"retention":{"cron":"0 17 3 * * *","monitorLogsDays":30,"balanceSnapshotsDays":90,"notificationLogsDays":90,"announcementsDays":90,"usageLogsDays":1}},
		"notifications":{"batchRateChanges":true,"minChangePct":0,"balanceLowCooldownMinutes":60,"subscriptionDailyRemainingThresholdPct":0,"subscriptionWeeklyRemainingThresholdPct":0,"subscriptionMonthlyRemainingThresholdPct":0,"subscriptionExpiryThresholdHours":0,"subscriptionAlertCooldownMinutes":1440,"sendMaxAttempts":3},
		"proxy":{"enabled":true,"versionCheckEnabled":true,"protocol":"socks5","host":"127.0.0.1","port":1080,"username":"u","password":"p"},
		"upstream":{"timeoutSeconds":45,"userAgent":"custom-agent"}
	}`
	req := httptest.NewRequest(http.MethodPut, "/api/settings/config", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	got, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if got.App.Title != "New" {
		t.Fatalf("title = %q", got.App.Title)
	}
	if got.App.NotificationPrefix != "[New] " {
		t.Fatalf("notification prefix = %q", got.App.NotificationPrefix)
	}
	if got.App.HomepageCheapestEnabled {
		t.Fatal("homepage cheapest flag was not persisted as false")
	}
	if got.Auth.SessionTTLHours != 24 {
		t.Fatalf("session TTL = %d, want 24", got.Auth.SessionTTLHours)
	}
	if !got.Proxy.Enabled || !got.Proxy.VersionCheckEnabled || got.Proxy.Protocol != "socks5" || got.Proxy.Host != "127.0.0.1" || got.Proxy.Port != 1080 || got.Proxy.Username != "u" || got.Proxy.Password != "p" {
		t.Fatalf("proxy = %#v", got.Proxy)
	}
	if got.Upstream.TimeoutSeconds != 45 || got.Upstream.UserAgent != "custom-agent" {
		t.Fatalf("upstream = %#v", got.Upstream)
	}
}

// TestSaveSettingsAcceptsZeroValueSubConfigs 锁定"点保存没反应"的回归：子配置
// 恰好全是零值（scheduler cron 全空、notifications 数值全 0、auth 未启用）时，
// 请求不能因为 binding:"required" 被判成 400，而应正常写入并各自走默认兜底。
func TestSaveSettingsAcceptsZeroValueSubConfigs(t *testing.T) {
	gin.SetMode(gin.TestMode)

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := config.Save(path, &config.Config{}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	registerSettings(api, &Deps{
		Runtime: runtimeconfig.New(path, "", nil, nil, nil, nil, nil, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil),
	})

	body := `{
		"app":{"title":"Zero","notificationPrefix":"","homepageCheapestEnabled":false},
		"auth":{"enabled":false,"username":"","password":"","tokenSecret":"","sessionTTLHours":0},
		"scheduler":{"balanceCron":"","rateCron":"","gatewayHealthCron":"","concurrency":0,"retention":{"cron":"","monitorLogsDays":0,"balanceSnapshotsDays":0,"notificationLogsDays":0,"announcementsDays":0,"usageLogsDays":0}},
		"notifications":{"batchRateChanges":false,"minChangePct":0,"balanceLowCooldownMinutes":0,"subscriptionDailyRemainingThresholdPct":0,"subscriptionWeeklyRemainingThresholdPct":0,"subscriptionMonthlyRemainingThresholdPct":0,"subscriptionExpiryThresholdHours":0,"subscriptionAlertCooldownMinutes":0,"sendMaxAttempts":0},
		"proxy":{},
		"upstream":{}
	}`
	req := httptest.NewRequest(http.MethodPut, "/api/settings/config", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("zero-value sub-configs must still save: status = %d, body = %s", rec.Code, rec.Body.String())
	}

	got, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if got.App.Title != "Zero" {
		t.Fatalf("title = %q", got.App.Title)
	}
	// 空的 upstream 子配置应被 WithDefaults() 兜底成放宽后的默认超时。
	if got.Upstream.TimeoutSeconds != config.DefaultUpstreamTimeoutSeconds {
		t.Fatalf("upstream timeout = %d, want default %d", got.Upstream.TimeoutSeconds, config.DefaultUpstreamTimeoutSeconds)
	}
	if got.Upstream.StreamFirstEventTimeoutSeconds != config.DefaultStreamFirstEventTimeoutSeconds {
		t.Fatalf("stream first event timeout = %d, want default %d", got.Upstream.StreamFirstEventTimeoutSeconds, config.DefaultStreamFirstEventTimeoutSeconds)
	}
}
