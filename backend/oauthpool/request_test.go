package oauthpool

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestResolveChatGPTCodexRequestHeaders(t *testing.T) {
	service := NewService(nil, WithEndpoints(Endpoints{ChatGPTCodex: "https://chatgpt.test/backend-api/codex"}))
	account := aliveAccount(1, storage.OAuthPoolChatGPT)
	request, err := service.ResolveRequest(storage.OAuthPoolChatGPT, account, storage.OAuthCredentials{
		AccessToken: "secret-access", AccountID: "acct-123",
	}, "gpt-5.4", RequestInput{Path: "/v1/responses", Body: []byte(`{"model":"gpt-5.4","input":"hi"}`), Stream: true, Header: http.Header{"Authorization": {"Bearer caller"}, "Cookie": {"caller=secret"}}})
	if err != nil {
		t.Fatal(err)
	}
	if request.URL != "https://chatgpt.test/backend-api/codex/responses" || request.Header.Get("Authorization") != "Bearer secret-access" || request.Header.Get("chatgpt-account-id") != "acct-123" {
		t.Fatalf("request = %#v", request)
	}
	if request.Header.Get("Cookie") != "" || request.Header.Get("Originator") == "" || request.Header.Get("OpenAI-Beta") == "" {
		t.Fatalf("headers = %#v", request.Header)
	}
}

func TestResolveGrokBuildRequestHeaders(t *testing.T) {
	service := NewService(nil, WithEndpoints(Endpoints{GrokBuild: "https://build.test/v1"}))
	account := aliveAccount(1, storage.OAuthPoolGrok)
	request, err := service.ResolveRequest(storage.OAuthPoolGrok, account, storage.OAuthCredentials{AccessToken: "grok-access", AccountID: "user-1"}, "grok-4.5", RequestInput{Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	if request.URL != "https://build.test/v1/responses" || request.Header.Get("Authorization") != "Bearer grok-access" || request.Header.Get("X-XAI-Token-Auth") != "xai-grok-cli" {
		t.Fatalf("request = %#v", request)
	}
	for _, name := range []string{"x-grok-client-version", "x-grok-client-identifier", "x-grok-client-mode", "x-authenticateresponse", "x-grok-agent-id", "x-grok-req-id", "x-grok-user-id"} {
		if request.Header.Get(name) == "" {
			t.Fatalf("missing %s in %#v", name, request.Header)
		}
	}
}

func TestResolveGrokSSOAndConsoleCookies(t *testing.T) {
	service := NewService(nil, WithEndpoints(Endpoints{GrokWeb: "https://grok.test", GrokConsole: "https://console.test/v1"}))
	credentials := storage.OAuthCredentials{SSOToken: "sso=secret-sso; ignored=yes", Cookie: "cf_clearance=clear; sso=old; arbitrary=drop"}
	webAccount := aliveAccount(1, storage.OAuthPoolGrok)
	webAccount.SourceFormat = "grok_sso"
	webRequest, err := service.ResolveRequest(storage.OAuthPoolGrok, webAccount, credentials, "grok-chat-fast", RequestInput{Body: []byte(`{"model":"grok-chat-fast","input":"hi"}`), Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	if webRequest.Provider != ProviderGrokWeb || webRequest.URL != "https://grok.test/rest/app-chat/conversations/new" {
		t.Fatalf("web request = %#v", webRequest)
	}
	cookie := webRequest.Header.Get("Cookie")
	if cookie != "sso=secret-sso; sso-rw=secret-sso; cf_clearance=clear" || strings.Contains(cookie, "arbitrary") || strings.Contains(cookie, "old") {
		t.Fatalf("web cookie = %q", cookie)
	}
	if !strings.Contains(string(webRequest.Body), `"message":"hi"`) {
		t.Fatalf("web body = %s", webRequest.Body)
	}

	consoleAccount := webAccount
	consoleAccount.SourceFormat = "grok_console"
	consoleRequest, err := service.ResolveRequest(storage.OAuthPoolGrok, consoleAccount, credentials, "grok-4.3", RequestInput{Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	if consoleRequest.Provider != ProviderGrokConsole || consoleRequest.URL != "https://console.test/v1/responses" || consoleRequest.Header.Get("Authorization") != "Bearer anonymous" || consoleRequest.Header.Get("x-cluster") == "" {
		t.Fatalf("console request = %#v", consoleRequest)
	}
}

func TestResolveRequestRejectsCrossPoolModel(t *testing.T) {
	service := NewService(nil)
	account := aliveAccount(1, storage.OAuthPoolGrok)
	_, err := service.ResolveRequest(storage.OAuthPoolGrok, account, storage.OAuthCredentials{AccessToken: "token"}, "gpt-5.4", RequestInput{})
	if err == nil {
		t.Fatal("cross-pool model was accepted")
	}
}

func TestResolvedRequestBuildsIndependentHTTPRequest(t *testing.T) {
	resolved := ResolvedRequest{Method: http.MethodPost, URL: "https://example.test/v1/responses", Header: http.Header{"X-Test": {"one"}}, Body: []byte("body")}
	request, err := resolved.HTTPRequest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(request.Body)
	request.Header.Set("X-Test", "two")
	if string(data) != "body" || resolved.Header.Get("X-Test") != "one" {
		t.Fatalf("request=%s headers=%v resolved=%v", data, request.Header, resolved.Header)
	}
}

func TestQueryQuotaParsesGrokWebAndBuildResponses(t *testing.T) {
	web := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/rest/rate-limits" || request.Header.Get("Cookie") == "" {
			t.Fatalf("web quota request = %s headers=%v", request.URL, request.Header)
		}
		_, _ = writer.Write([]byte(`{"windowSizeSeconds":3600,"remainingQueries":7,"totalQueries":10}`))
	}))
	defer web.Close()
	service := NewService(nil, WithEndpoints(Endpoints{GrokWeb: web.URL}))
	webAccount := aliveAccount(1, storage.OAuthPoolGrok)
	webAccount.SourceFormat = "grok_sso"
	quota, err := service.QueryQuota(context.Background(), storage.OAuthPoolGrok, webAccount, storage.OAuthCredentials{SSOToken: "sso-token"})
	if err != nil || quota.Used == nil || quota.Limit == nil || *quota.Used != 3 || *quota.Limit != 10 || quota.ResetAt == nil {
		t.Fatalf("web quota = %#v, err = %v", quota, err)
	}

	build := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/billing" || request.URL.Query().Get("format") != "credits" || request.Header.Get("Authorization") != "Bearer build-token" {
			t.Fatalf("build quota request = %s headers=%v", request.URL, request.Header)
		}
		_, _ = writer.Write([]byte(`{"config":{"monthlyLimit":100,"used":25,"billingPeriodEnd":"2026-08-01T00:00:00Z"}}`))
	}))
	defer build.Close()
	service = NewService(nil, WithEndpoints(Endpoints{GrokBuild: build.URL + "/v1"}))
	buildAccount := aliveAccount(2, storage.OAuthPoolGrok)
	buildAccount.SourceFormat = "cliproxyapi"
	quota, err = service.QueryQuota(context.Background(), storage.OAuthPoolGrok, buildAccount, storage.OAuthCredentials{AccessToken: "build-token"})
	wantReset := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	if err != nil || quota.Used == nil || quota.Limit == nil || *quota.Used != 25 || *quota.Limit != 100 || quota.ResetAt == nil || !quota.ResetAt.Equal(wantReset) {
		t.Fatalf("build quota = %#v, err = %v", quota, err)
	}
}
