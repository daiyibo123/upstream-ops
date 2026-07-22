package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/oauthpool"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestManualGrokRelayConvertsNativeJSONStream(t *testing.T) {
	// 手动添加的 Grok 中转渠道以 application/json 直接回传 Grok 原生连续 JSON 帧，
	// 而非 OpenAI SSE。修复前网关会原样透传裸帧，客户端无法解析（表现为不返回内容）。
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w,
			`{"result":{"conversation":{"conversationId":"conversation-1"}}}`+
				`{"result":{"response":{"token":"Hel","messageTag":"final"}}}`+
				`{"result":{"response":{"token":"lo","messageTag":"final"}}}`+
				`{"result":{"response":{"modelResponse":{"message":"Hello","parentResponseId":"response-1"}}}}`,
		)
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("m", 32))
	channel := &storage.Channel{Name: "grok-relay", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-grok-relay")
	if err != nil {
		t.Fatalf("encrypt upstream key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "grok-relay", ChannelType: storage.ChannelTypeSub2API,
		ClientFormat: "any", GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}
	env.svc.InvalidateSchedulingCache()

	t.Run("responses stream", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"grok-chat-fast","input":"ping","stream":true}`))
		req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
			t.Fatalf("proxy responses stream: %v body=%s", err, rec.Body.String())
		}
		body := rec.Body.String()
		if rec.Code != http.StatusOK || !strings.Contains(body, "response.output_text.delta") || !strings.Contains(body, "Hello") || !strings.Contains(body, "response.completed") {
			t.Fatalf("unexpected Responses stream: status=%d body=%s", rec.Code, body)
		}
	})

	t.Run("responses non stream", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"grok-chat-fast","input":"ping","stream":false}`))
		req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
			t.Fatalf("proxy non-stream response: %v body=%s", err, rec.Body.String())
		}
		var response map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode response: %v body=%s", err, rec.Body.String())
		}
		if rec.Code != http.StatusOK || responseText(response) != "Hello" || stringValue(response["status"]) != "completed" {
			t.Fatalf("unexpected non-stream response: status=%d body=%s", rec.Code, rec.Body.String())
		}
	})
}

func TestManualGrokRelayConvertsNativeSSEStream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w,
			"data: {\"result\":{\"conversation\":{\"conversationId\":\"conversation-1\"}}}\n\n"+
				"data: {\"result\":{\"response\":{\"token\":\"Hel\",\"messageTag\":\"final\"}}}\n\n"+
				"data: {\"result\":{\"response\":{\"token\":\"lo\",\"messageTag\":\"final\"}}}\n\n"+
				"data: [DONE]\n\n",
		)
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("s", 32))
	channel := &storage.Channel{Name: "grok-sse-relay", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-grok-sse-relay")
	if err != nil {
		t.Fatalf("encrypt upstream key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: channel.Name, ChannelType: storage.ChannelTypeSub2API,
		ClientFormat: "any", GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}
	env.svc.InvalidateSchedulingCache()

	for _, stream := range []bool{true, false} {
		t.Run(fmt.Sprintf("responses stream=%v", stream), func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":"grok-chat-fast","input":"ping","stream":%v}`, stream)))
			req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
			req.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			if err := env.svc.Proxy(recorder, req, "/v1/responses"); err != nil {
				t.Fatalf("proxy: %v body=%s", err, recorder.Body.String())
			}
			body := recorder.Body.String()
			if recorder.Code != http.StatusOK || !strings.Contains(body, "Hello") {
				t.Fatalf("unexpected response: status=%d body=%s", recorder.Code, body)
			}
			if stream && (!strings.Contains(body, "response.output_text.delta") || !strings.Contains(body, "response.completed")) {
				t.Fatalf("native SSE was not converted to Responses SSE: %s", body)
			}
			if !stream {
				var response map[string]any
				if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || responseText(response) != "Hello" {
					t.Fatalf("native SSE was not converted to Responses JSON: err=%v body=%s", err, body)
				}
			}
		})
	}
}

func TestManualGrokRawJSONStartsBeforeUpstreamEOF(t *testing.T) {
	releaseUpstream := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		flusher, _ := w.(http.Flusher)
		_, _ = fmt.Fprint(w, `{"result":{"conversation":{"conversationId":"conversation-1"}}}`)
		flusher.Flush()
		_, _ = fmt.Fprint(w, `{"result":{"response":{"token":"Hello","messageTag":"final"}}}`)
		flusher.Flush()
		<-releaseUpstream
		_, _ = fmt.Fprint(w, `{"result":{"response":{"modelResponse":{"message":"Hello","parentResponseId":"response-1"}}}}`)
	}))

	env := newGatewayProxyTestEnv(t, strings.Repeat("r", 32))
	channel := &storage.Channel{Name: "grok-long-lived-relay", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-grok-long-lived")
	if err != nil {
		t.Fatalf("encrypt upstream key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: channel.Name, ChannelType: storage.ChannelTypeSub2API,
		ClientFormat: "any", GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}
	env.svc.InvalidateSchedulingCache()

	gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := env.svc.Proxy(w, r, r.URL.Path); err != nil {
			var gatewayErr *GatewayError
			if errors.As(err, &gatewayErr) {
				w.WriteHeader(gatewayErr.Status)
				_, _ = w.Write(gatewayErr.Body)
				return
			}
			http.Error(w, err.Error(), http.StatusBadGateway)
		}
	}))
	closed := false
	cleanup := func() {
		if !closed {
			close(releaseUpstream)
			closed = true
		}
		gatewayServer.Close()
		upstream.Close()
	}
	defer cleanup()

	req, err := http.NewRequest(http.MethodPost, gatewayServer.URL+"/v1/responses", strings.NewReader(`{"model":"grok-chat-fast","input":"ping","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 2 * time.Second}
	started := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("gateway did not publish native Grok output before upstream EOF: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		_ = resp.Body.Close()
		t.Fatalf("first output waited for upstream EOF: %s", elapsed)
	} else {
		t.Logf("first output arrived before upstream EOF in %s", elapsed)
	}
	close(releaseUpstream)
	closed = true
	body, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if readErr != nil || resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "Hello") || !strings.Contains(string(body), "response.completed") {
		t.Fatalf("unexpected streamed response: status=%d err=%v body=%s", resp.StatusCode, readErr, body)
	}
}

func TestLooksLikeGrokWebNativeStream(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"native result frame", `{"result":{"response":{"token":"hi"}}}`, true},
		{"native conversation frame", `{"result":{"conversation":{"conversationId":"c1"}}}`, true},
		{"leading whitespace", "  \n{\"result\":{\"response\":{\"token\":\"hi\"}}}", true},
		{"error then result", `{"error":{"message":"x"}}{"result":{"response":{"token":"hi"}}}`, true},
		{"openai responses json", `{"id":"resp_1","object":"response","output":[]}`, false},
		{"openai chat json", `{"id":"chatcmpl-1","object":"chat.completion","choices":[]}`, false},
		{"bare error only", `{"error":{"message":"nope"}}`, false},
		{"sse data line", `data: {"result":{"response":{"token":"hi"}}}`, false},
		{"empty", ``, false},
		{"result without grok fields", `{"result":{"foo":"bar"}}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := looksLikeGrokWebNativeStream([]byte(tc.body)); got != tc.want {
				t.Fatalf("looksLikeGrokWebNativeStream(%q) = %v, want %v", tc.body, got, tc.want)
			}
		})
	}
}

func TestGrokWebFixedPoolConvertsContinuousJSONForResponsesChatAndNonStream(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Cookie"), "sso=grok-test-token") {
			t.Fatalf("Grok Web credentials were not applied: %q", r.Header.Get("Cookie"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w,
			`{"result":{"conversation":{"conversationId":"conversation-1"}}}`+
				`{"result":{"response":{"token":"Hel","messageTag":"final"}}}`+
				`{"result":{"response":{"token":"lo","messageTag":"final"}}}`+
				`{"result":{"response":{"modelResponse":{"message":"Hello","parentResponseId":"response-1"}}}}`,
		)
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("g", 32))
	if err := storage.EnsureFixedOAuthPoolScopes(env.channels, env.groupKeys, env.cipher); err != nil {
		t.Fatalf("ensure fixed pools: %v", err)
	}
	accounts := storage.NewOAuthAccounts(env.db, env.cipher)
	imported, err := accounts.ImportJSON(storage.OAuthPoolGrok, []byte(`{"type":"grok","sso":"grok-test-token"}`))
	if err != nil || imported.Succeeded != 1 {
		t.Fatalf("import Grok account=%#v err=%v", imported, err)
	}
	if err := accounts.RecordRuntimeSuccess(storage.OAuthPoolGrok, imported.Items[0].AccountID, time.Now()); err != nil {
		t.Fatalf("activate Grok account: %v", err)
	}
	poolService := oauthpool.NewService(accounts, oauthpool.WithEndpoints(oauthpool.Endpoints{GrokWeb: upstream.URL}))
	env.svc.SetOAuthPool(poolService)
	env.svc.SetOAuthAccounts(accounts)
	env.svc.InvalidateSchedulingCache()

	t.Run("responses stream", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"grok-chat-fast","input":"ping","stream":true}`))
		req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
		req.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		if err := env.svc.Proxy(recorder, req, "/v1/responses"); err != nil {
			t.Fatalf("proxy responses stream: %v body=%s", err, recorder.Body.String())
		}
		body := recorder.Body.String()
		if recorder.Code != http.StatusOK || !strings.Contains(body, "response.output_text.delta") || !strings.Contains(body, "Hello") || !strings.Contains(body, "response.completed") {
			t.Fatalf("unexpected Responses stream: status=%d body=%s", recorder.Code, body)
		}
	})

	t.Run("chat stream", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"grok-chat-fast","messages":[{"role":"user","content":"ping"}],"stream":true}`))
		req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
		req.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		if err := env.svc.Proxy(recorder, req, "/v1/chat/completions"); err != nil {
			t.Fatalf("proxy chat stream: %v body=%s", err, recorder.Body.String())
		}
		body := recorder.Body.String()
		if recorder.Code != http.StatusOK || !strings.Contains(body, "chat.completion.chunk") || !strings.Contains(body, `"content":"Hel"`) || !strings.Contains(body, `"content":"lo"`) || !strings.Contains(body, "[DONE]") {
			t.Fatalf("unexpected Chat stream: status=%d body=%s", recorder.Code, body)
		}
	})

	t.Run("responses non stream", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"grok-chat-fast","input":"ping","stream":false}`))
		req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
		req.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		if err := env.svc.Proxy(recorder, req, "/v1/responses"); err != nil {
			t.Fatalf("proxy non-stream response: %v body=%s", err, recorder.Body.String())
		}
		var response map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode response: %v body=%s", err, recorder.Body.String())
		}
		if recorder.Code != http.StatusOK || responseText(response) != "Hello" || stringValue(response["status"]) != "completed" {
			t.Fatalf("unexpected non-stream response: status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	})
}

func TestGrokWebStreamOnlyAllowsFailoverBeforeOutput(t *testing.T) {
	service := &Service{}
	request := normalizedRequest{ResponseMode: "responses", Body: []byte(`{"model":"grok-chat-fast"}`), Stream: true}

	t.Run("error before output", func(t *testing.T) {
		ctx, guard := newFirstOutputGuard(context.Background(), time.Second)
		defer guard.Close()
		_ = ctx
		recorder := httptest.NewRecorder()
		retry, _, err := service.streamGrokWebOAuthResponse(strings.NewReader(`{"error":{"message":"temporary upstream failure"}}`), request, nil, recorder, guard)
		if err == nil || !retry || recorder.Body.Len() != 0 {
			t.Fatalf("retry=%v err=%v body=%q", retry, err, recorder.Body.String())
		}
	})

	t.Run("error after output", func(t *testing.T) {
		ctx, guard := newFirstOutputGuard(context.Background(), time.Second)
		defer guard.Close()
		_ = ctx
		recorder := httptest.NewRecorder()
		stream := `{"result":{"response":{"token":"visible","messageTag":"final"}}}{"error":{"message":"later failure"}}`
		retry, _, err := service.streamGrokWebOAuthResponse(strings.NewReader(stream), request, nil, recorder, guard)
		if err == nil || retry || !strings.Contains(recorder.Body.String(), "visible") {
			t.Fatalf("retry=%v err=%v body=%q", retry, err, recorder.Body.String())
		}
	})
}

func TestGrokWebNativeFramesWrappedInSSEAreConverted(t *testing.T) {
	sseBody := "data: {\"result\":{\"conversation\":{\"conversationId\":\"c1\"}}}\n\n" +
		"data: {\"result\":{\"response\":{\"token\":\"Hel\",\"messageTag\":\"final\"}}}\n\n" +
		"data: {\"result\":{\"response\":{\"token\":\"lo\",\"messageTag\":\"final\"}}}\n\n" +
		"data: [DONE]\n\n"
	body := io.NopCloser(strings.NewReader(sseBody))
	reader := newSSEStreamReader(body)
	buffered, err := preflightSSEStream(reader, body, time.Second, nil)
	if err != nil {
		t.Fatalf("preflight native Grok SSE: %v", err)
	}
	if !bufferedSSELooksLikeGrokWeb(buffered) {
		t.Fatalf("native Grok SSE was not recognized: %#v", buffered)
	}
	ctx, guard := newFirstOutputGuard(context.Background(), time.Second)
	defer guard.Close()
	_ = ctx
	recorder := httptest.NewRecorder()
	request := normalizedRequest{ResponseMode: "responses", Stream: true, Body: []byte(`{"model":"grok-4"}`)}
	retry, usage, err := (&Service{}).streamGrokWebSSEEvents(buffered, reader, request, nil, recorder, guard)
	if err != nil || retry || !strings.Contains(recorder.Body.String(), "response.output_text.delta") || !strings.Contains(recorder.Body.String(), "Hello") || usage.GeneratedText != "Hello" {
		t.Fatalf("retry=%v usage=%#v err=%v body=%q", retry, usage, err, recorder.Body.String())
	}
	if !looksLikeGrokWebNativeSSEBody([]byte(sseBody)) {
		t.Fatal("complete native Grok SSE body was not recognized")
	}
	normalized, err := normalizeGrokWebOAuthResponse(strings.NewReader(sseBody), normalizedRequest{ResponseMode: "responses", Body: []byte(`{"model":"grok-4"}`)})
	if err != nil || !strings.Contains(string(normalized), "Hello") || !strings.Contains(string(normalized), `"status":"completed"`) {
		t.Fatalf("normalize native Grok SSE: err=%v body=%s", err, normalized)
	}
}
