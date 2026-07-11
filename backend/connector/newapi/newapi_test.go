package newapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
)

func TestSetHTTPConfigAppliesUserAgentAndTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != "custom-agent" {
			t.Fatalf("user agent = %q", got)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{}}`))
	}))
	defer srv.Close()

	c := New()
	c.SetHTTPConfig(connector.HTTPConfig{
		Timeout:   45 * time.Second,
		UserAgent: "custom-agent",
	})
	if c.http.GetClient().Timeout != 45*time.Second {
		t.Fatalf("timeout = %s", c.http.GetClient().Timeout)
	}
	if _, err := c.getJSON(context.Background(), srv.URL, nil); err != nil {
		t.Fatalf("getJSON: %v", err)
	}
}

func TestLoginAddsExtraParams(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["username"] != "u" || body["password"] != "p" || body["device_id"] != "d1" {
			t.Fatalf("body = %#v", body)
		}
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "abc"})
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"id":7}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	session, err := c.Login(context.Background(), &connector.Channel{
		SiteURL:          srv.URL,
		Username:         "u",
		Password:         "p",
		LoginExtraParams: map[string]any{"device_id": "d1"},
	})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if session.UserID != "7" || session.Cookie == "" {
		t.Fatalf("session = %#v", session)
	}
}

func TestGetCosts(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self/stat", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("type"); got != "0" {
			t.Fatalf("type = %q, want 0", got)
		}
		if r.URL.Query().Get("start_timestamp") == "" || r.URL.Query().Get("end_timestamp") == "" {
			t.Fatalf("expected start/end timestamp")
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota":1000000,"rpm":0,"tpm":0}}`))
	})
	mux.HandleFunc("/api/user/self", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota":3416846,"used_quota":39091119}}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	res, err := c.GetCosts(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	})
	if err != nil {
		t.Fatalf("GetCosts: %v", err)
	}
	if res.TodayCost != 2 {
		t.Fatalf("today cost = %v, want 2", res.TodayCost)
	}
	if res.TotalCost != 78.1822 {
		t.Fatalf("total cost = %v, want 78.1822", res.TotalCost)
	}
}

func TestGetCostsAppliesManualRechargeMultiplier(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self/stat", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota":1000000}}`))
	})
	mux.HandleFunc("/api/user/self", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"used_quota":5000000}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	multiplier := 2.0
	res, err := c.GetCosts(context.Background(), &connector.Channel{
		SiteURL:                srv.URL,
		RechargeMultiplier:     &multiplier,
		RechargeMultiplierMode: connector.RechargeMultiplierModeDivide,
	}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	})
	if err != nil {
		t.Fatalf("GetCosts: %v", err)
	}
	if res.TodayCost != 1 {
		t.Fatalf("today cost = %v, want 1", res.TodayCost)
	}
	if res.TotalCost != 5 {
		t.Fatalf("total cost = %v, want 5", res.TotalCost)
	}
}

func TestGetCostsAppliesUpstreamRechargeMultiplier(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota_per_unit":500000,"price":7.2}}`))
	})
	mux.HandleFunc("/api/log/self/stat", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota":1000000}}`))
	})
	mux.HandleFunc("/api/user/self", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"used_quota":5000000}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	res, err := c.GetCosts(context.Background(), &connector.Channel{
		SiteURL:                srv.URL,
		RechargeMultiplierMode: connector.RechargeMultiplierModeDivide,
	}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	})
	if err != nil {
		t.Fatalf("GetCosts: %v", err)
	}
	if res.TodayCost != 14.4 {
		t.Fatalf("today cost = %v, want 14.4", res.TodayCost)
	}
	if res.TotalCost != 72 {
		t.Fatalf("total cost = %v, want 72", res.TotalCost)
	}
}

func TestGetRechargeInfo(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/topup/info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"pay_methods":"[{\"name\":\"支付宝\",\"type\":\"alipay\",\"min_topup\":\"10\"},{\"name\":\"微信\",\"type\":\"wxpay\",\"min_topup\":\"12\"},{\"name\":\"Stripe\",\"type\":\"stripe\",\"min_topup\":\"30\"}]","min_topup":8,"amount_options":"[10,20,50]"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetRechargeInfo(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	})
	if err != nil {
		t.Fatalf("GetRechargeInfo: %v", err)
	}
	if len(info.Methods) != 2 {
		t.Fatalf("methods len = %d, want 2", len(info.Methods))
	}
	if info.Methods[0].Type != "alipay" || info.Methods[0].MinAmount != 10 {
		t.Fatalf("alipay method = %#v", info.Methods[0])
	}
	if info.AmountStep != 1 {
		t.Fatalf("amount step = %v, want 1", info.AmountStep)
	}
	if len(info.PresetAmounts) != 3 || info.PresetAmounts[2] != 50 {
		t.Fatalf("preset amounts = %#v", info.PresetAmounts)
	}
}

func TestCreateRecharge(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/pay", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Cookie"); got != "session=1" {
			t.Fatalf("cookie = %q", got)
		}
		if got := r.Header.Get("New-Api-User"); got != "7" {
			t.Fatalf("user header = %q", got)
		}
		_, _ = w.Write([]byte(`{"message":"success","data":{"pid":"123","type":"alipay"},"url":"https://pay.example.com/submit"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateRecharge(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	}, connector.RechargeRequest{
		Amount:        20,
		PaymentMethod: "alipay",
	})
	if err != nil {
		t.Fatalf("CreateRecharge: %v", err)
	}
	if launch.Mode != "form" {
		t.Fatalf("mode = %q, want form", launch.Mode)
	}
	if launch.FormAction != "https://pay.example.com/submit" {
		t.Fatalf("action = %q", launch.FormAction)
	}
	if launch.FormFields["pid"] != "123" {
		t.Fatalf("pid = %q", launch.FormFields["pid"])
	}
}

func TestCreateRechargeReturnsDataError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/pay", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"message":"error","data":"支付方式不存在"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	_, err := c.CreateRecharge(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	}, connector.RechargeRequest{
		Amount:        20,
		PaymentMethod: "alipay",
	})
	if err == nil || !strings.Contains(err.Error(), "支付方式不存在") {
		t.Fatalf("err = %v, want data error", err)
	}
}

func TestCreateRechargeRejectsFloatAmount(t *testing.T) {
	c := New()
	_, err := c.CreateRecharge(context.Background(), &connector.Channel{}, &connector.AuthSession{}, connector.RechargeRequest{
		Amount:        1.5,
		PaymentMethod: "alipay",
	})
	if err == nil || !strings.Contains(err.Error(), "正整数") {
		t.Fatalf("err = %v, want integer error", err)
	}
}

func TestSubscriptionUnsupported(t *testing.T) {
	c := New()
	_, err := c.GetSubscriptionInfo(context.Background(), &connector.Channel{}, &connector.AuthSession{})
	if err == nil || !strings.Contains(err.Error(), "不支持订阅") {
		t.Fatalf("GetSubscriptionInfo err = %v, want unsupported error", err)
	}
	_, err = c.CreateSubscription(context.Background(), &connector.Channel{}, &connector.AuthSession{}, connector.SubscriptionRequest{})
	if err == nil || !strings.Contains(err.Error(), "不支持订阅") {
		t.Fatalf("CreateSubscription err = %v, want unsupported error", err)
	}
	_, err = c.GetSubscriptionUsage(context.Background(), &connector.Channel{}, &connector.AuthSession{})
	if err == nil || !strings.Contains(err.Error(), "不支持订阅") {
		t.Fatalf("GetSubscriptionUsage err = %v, want unsupported error", err)
	}
}

func TestListAPIKeys(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/self/groups", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"default":{"ratio":1.25,"desc":"默认分组"}}}`))
	})
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("p"); got != "2" {
			t.Fatalf("p = %q, want 2", got)
		}
		if got := r.URL.Query().Get("page_size"); got != "10" {
			t.Fatalf("page_size = %q, want 10", got)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[{"id":9,"key":"sk**********abcd","status":1,"name":"main","created_time":1700000000,"accessed_time":1700000100,"expired_time":-1,"remain_quota":100,"used_quota":5,"unlimited_quota":false,"model_limits_enabled":true,"model_limits":"gpt-4","allow_ips":"1.1.1.1","group":"default","cross_group_retry":true}],"total":1,"page":2,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	page, err := c.ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	}, connector.APIKeyQuery{Page: 2, PageSize: 10})
	if err != nil {
		t.Fatalf("ListAPIKeys: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].Status != "active" || page.Items[0].AllowIPs != "1.1.1.1" || page.Items[0].GroupName != "default" || page.Items[0].GroupRatio != 1.25 {
		t.Fatalf("page = %#v", page)
	}
}

func TestListAPIKeyGroups(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/self/groups", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"default":{"ratio":1.25,"desc":"默认分组"},"auto":{"ratio":"自动","desc":"跳过"}}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	groups, err := c.ListAPIKeyGroups(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	})
	if err != nil {
		t.Fatalf("ListAPIKeyGroups: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != "default" || groups[0].Description != "默认分组" || groups[0].Ratio != 1.25 {
		t.Fatalf("groups = %#v", groups)
	}
}

func TestGetAnnouncements(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"announcements":[{"id":12,"title":"平台公告","content":"维护通知","type":"warning","publishDate":"2026-01-02T03:04:05Z","updated_at":"2026-01-02T04:04:05Z"}]}}`))
	})
	mux.HandleFunc("/api/notice", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":"站点公告"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	items, err := c.GetAnnouncements(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{})
	if err != nil {
		t.Fatalf("GetAnnouncements: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("items len = %d, want 2", len(items))
	}
	if items[0].SourceKey != "id:12" || items[0].Title != "平台公告" || items[0].Type != "warning" {
		t.Fatalf("first item = %#v", items[0])
	}
	if !strings.HasPrefix(items[1].SourceKey, "hash:") || items[1].Content != "站点公告" {
		t.Fatalf("second item = %#v", items[1])
	}
}

func TestGetAnnouncementsFromNoticeOnly(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"announcements":[]}}`))
	})
	mux.HandleFunc("/api/notice", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":"文本公告"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	items, err := c.GetAnnouncements(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{})
	if err != nil {
		t.Fatalf("GetAnnouncements: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("items len = %d, want 1", len(items))
	}
	if !strings.HasPrefix(items[0].SourceKey, "hash:") || items[0].Content != "文本公告" {
		t.Fatalf("item = %#v", items[0])
	}
}

func TestGetAnnouncementsEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"announcements":[]}}`))
	})
	mux.HandleFunc("/api/notice", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":""}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	items, err := c.GetAnnouncements(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{})
	if err != nil {
		t.Fatalf("GetAnnouncements: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("items len = %d, want 0", len(items))
	}
}

func TestSearchAPIKeys(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/token/search", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("keyword"); got != "main" {
			t.Fatalf("keyword = %q, want main", got)
		}
		if got := r.URL.Query().Get("token"); got != "main" {
			t.Fatalf("token = %q, want main", got)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[],"total":0,"page":1,"page_size":20,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	_, err := c.ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	}, connector.APIKeyQuery{Search: "main"})
	if err != nil {
		t.Fatalf("ListAPIKeys search: %v", err)
	}
}

func TestCreateUpdateDeleteRevealAPIKey(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create: %v", err)
			}
			if body["name"] != "main" || body["custom_key"] != "sk-custom" {
				t.Fatalf("create body = %#v", body)
			}
			_, _ = w.Write([]byte(`{"success":true,"message":""}`))
		case http.MethodPut:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode update: %v", err)
			}
			if body["id"] != float64(9) || body["status"] != float64(2) {
				t.Fatalf("update body = %#v", body)
			}
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"id":9,"status":2,"name":"main disabled"}}`))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	mux.HandleFunc("/api/token/9", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want DELETE", r.Method)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":""}`))
	})
	mux.HandleFunc("/api/token/9/key", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"key":"sk-full"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	session := &connector.AuthSession{Cookie: "session=1", UserID: "7"}
	_, err := c.CreateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, connector.APIKeyCreateRequest{
		Name:      "main",
		CustomKey: "sk-custom",
	})
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	updated, err := c.UpdateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, 9, connector.APIKeyUpdateRequest{
		Status: strPtr("disabled"),
	})
	if err != nil {
		t.Fatalf("UpdateAPIKey: %v", err)
	}
	if updated.Status != "disabled" {
		t.Fatalf("updated status = %q", updated.Status)
	}
	if err := c.DeleteAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, 9); err != nil {
		t.Fatalf("DeleteAPIKey: %v", err)
	}
	key, err := c.RevealAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, 9)
	if err != nil {
		t.Fatalf("RevealAPIKey: %v", err)
	}
	if key != "sk-full" {
		t.Fatalf("key = %q", key)
	}
}

func strPtr(v string) *string {
	return &v
}
