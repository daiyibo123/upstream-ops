package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestChatToResponsesPreservesHistoricalTools(t *testing.T) {
	body, _, err := chatToResponsesBody([]byte(`{
		"model":"gpt-test",
		"messages":[
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"write_file","arguments":"{\"path\":\"a.txt\"}"}}]},
			{"role":"tool","tool_call_id":"call_1","content":"ok"}
		],
		"tools":[{"type":"function","function":{"name":"write_file","description":"write","parameters":{"type":"object","properties":{"path":{"type":"string"}}}}}],
		"tool_choice":{"type":"function","function":{"name":"write_file"}}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	var request map[string]any
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	input, _ := request["input"].([]any)
	if len(input) != 2 || stringValue(input[0].(map[string]any)["type"]) != "function_call" || stringValue(input[1].(map[string]any)["type"]) != "function_call_output" {
		t.Fatalf("historical tool linkage was lost: %s", body)
	}
	tools, _ := request["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("tools=%#v body=%s", tools, body)
	}
	tool := tools[0].(map[string]any)
	if stringValue(tool["name"]) != "write_file" || tool["function"] != nil || tool["parameters"] == nil {
		t.Fatalf("Chat tool was not flattened for Responses: %#v", tool)
	}
	choice, _ := request["tool_choice"].(map[string]any)
	if stringValue(choice["name"]) != "write_file" {
		t.Fatalf("tool choice=%#v", choice)
	}
}

func TestGatewayModelsUsesLocalStrictCatalogWithoutUpstreamRequest(t *testing.T) {
	var upstreamHits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		http.Error(w, "must not be called", http.StatusInternalServerError)
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("z", 32))
	channel := &storage.Channel{Name: "zcode-upstream", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	candidate := &storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: channel.Name, ChannelURL: channel.SiteURL, ChannelType: channel.Type,
		ClientFormat: "openai", RequestMode: "responses", GroupRef: "zcode", GroupName: "zcode", Ratio: 1,
		KeyCipher: "test", Enabled: true, Status: "alive", ModelRestrictionEnabled: true,
		AvailableModels: `["gpt-allowed","gpt-blocked"]`, SupportedModels: `["gpt-allowed"]`,
	}
	if err := env.db.Create(candidate).Error; err != nil {
		t.Fatal(err)
	}
	env.svc.InvalidateSchedulingCache()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	recorder := httptest.NewRecorder()
	if err := env.svc.Proxy(recorder, req, "/v1/models"); err != nil {
		t.Fatal(err)
	}
	body := recorder.Body.String()
	if recorder.Code != http.StatusOK || !strings.Contains(body, "gpt-allowed") || strings.Contains(body, "gpt-blocked") {
		t.Fatalf("unexpected local model catalog: status=%d body=%s", recorder.Code, body)
	}
	if upstreamHits.Load() != 0 {
		t.Fatalf("model listing contacted upstream %d times", upstreamHits.Load())
	}
}

func TestResponsesToChatPreservesFunctionCalls(t *testing.T) {
	body, err := responsesToChat([]byte(`{
		"id":"resp_1","object":"response","model":"gpt-test","status":"completed","output":[
			{"id":"fc_1","type":"function_call","call_id":"call_1","name":"write_file","arguments":"{\"path\":\"a.txt\"}","status":"completed"}
		],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	choices := response["choices"].([]any)
	choice := choices[0].(map[string]any)
	message := choice["message"].(map[string]any)
	toolCalls, _ := message["tool_calls"].([]any)
	if stringValue(choice["finish_reason"]) != "tool_calls" || len(toolCalls) != 1 || stringValue(toolCalls[0].(map[string]any)["id"]) != "call_1" {
		t.Fatalf("Responses tool call was lost: %s", body)
	}
}

func TestResponsesToChatStreamPreservesFunctionCallsAndUsage(t *testing.T) {
	stream := strings.Join([]string{
		`event: response.output_item.added`,
		`data: {"type":"response.output_item.added","response_id":"resp_1","output_index":0,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"write_file","arguments":"","status":"in_progress"}}`,
		"",
		`event: response.function_call_arguments.delta`,
		`data: {"type":"response.function_call_arguments.delta","response_id":"resp_1","output_index":0,"item_id":"fc_1","delta":"{\"path\":\"a.txt\"}"}`,
		"",
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"id":"resp_1","model":"gpt-test","status":"completed","output":[],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}}`,
		"",
	}, "\n")
	recorder := httptest.NewRecorder()
	if _, err := streamResponsesAsChatEvents(recorder, nil, newSSEStreamReader(strings.NewReader(stream))); err != nil {
		t.Fatalf("convert stream: %v body=%s", err, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, marker := range []string{`"tool_calls"`, `"id":"call_1"`, `"arguments":"{\"path\":\"a.txt\"}"`, `"finish_reason":"tool_calls"`, `"total_tokens":5`, "[DONE]"} {
		if !strings.Contains(body, marker) {
			t.Fatalf("converted stream missing %q: %s", marker, body)
		}
	}
}

func TestChatStreamWrapsNonSSECompletion(t *testing.T) {
	recorder := httptest.NewRecorder()
	body := []byte(`{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"gpt-test","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)
	usage, err := streamNonSSEAsChatEvents(recorder, http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, body, &storage.UpstreamGroupKey{})
	if err != nil {
		t.Fatal(err)
	}
	out := recorder.Body.String()
	if !strings.Contains(out, `"content":"hello"`) || !strings.Contains(out, `"total_tokens":3`) || !strings.Contains(out, "[DONE]") || usage.Total != 3 {
		t.Fatalf("non-SSE Chat response was not wrapped correctly: usage=%#v body=%s", usage, out)
	}
}

func TestZCodeSensitiveHeadersAreNotForwarded(t *testing.T) {
	for _, header := range []string{
		"Authorization", "X-Api-Key", "Cookie", "OpenAI-Organization", "OpenAI-Project",
		"ChatGPT-Account-ID", "X-Forwarded-For", "X-Real-IP", "Forwarded",
	} {
		if !skipRequestHeader(header) {
			t.Fatalf("sensitive client header %q would be forwarded", header)
		}
	}
	for _, header := range []string{"User-Agent", "X-Stainless-Lang", "Idempotency-Key", "OpenAI-Beta"} {
		if skipRequestHeader(header) {
			t.Fatalf("compatible metadata header %q was stripped", header)
		}
	}
}

func TestZCodeDoubleEncodedToolArgumentsAreNormalizedOnce(t *testing.T) {
	double := `"{\"path\":\"a.txt\",\"lines\":[1,2]}"`
	if got := normalizeDoubleEncodedToolArguments(double); got != `{"lines":[1,2],"path":"a.txt"}` && got != `{"path":"a.txt","lines":[1,2]}` {
		t.Fatalf("double encoded arguments=%q", got)
	}
	for _, value := range []string{`{"path":"a.txt"}`, `plain text`, `"plain text"`} {
		if got := normalizeDoubleEncodedToolArguments(value); got != value {
			t.Fatalf("ordinary arguments changed: input=%q output=%q", value, got)
		}
	}
}
