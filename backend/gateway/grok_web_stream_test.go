package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/oauthpool"
	"github.com/bejix/upstream-ops/backend/storage"
)

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
