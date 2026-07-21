package gateway

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestChatToResponsesStreamPreservesGrokReasoning(t *testing.T) {
	stream := strings.Join([]string{
		`data: {"id":"chatcmpl-grok","object":"chat.completion.chunk","model":"grok-4.5","choices":[{"delta":{"reasoning_content":"Thinking..."},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl-grok","object":"chat.completion.chunk","model":"grok-4.5","choices":[{"delta":{},"finish_reason":"stop"}]}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n")
	recorder := httptest.NewRecorder()
	usage, err := streamChatAsResponsesEvents(recorder, nil, newSSEStreamReader(strings.NewReader(stream)))
	if err != nil {
		t.Fatalf("bridge reasoning stream: %v body=%s", err, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, marker := range []string{"response.reasoning_summary_part.added", "response.reasoning_summary_text.delta", "Thinking...", "response.reasoning_summary_text.done", "response.completed", "[DONE]"} {
		if !strings.Contains(body, marker) {
			t.Fatalf("reasoning stream missing %q: %s", marker, body)
		}
	}
	if strings.Contains(body, `"type":"message"`) {
		t.Fatalf("reasoning-only completion must not synthesize an empty message item: %s", body)
	}
	if usage.Status == "failed" || usage.Status == "interrupted" {
		t.Fatalf("unexpected usage status: %#v", usage)
	}
}

func TestChatToResponsesStreamPreservesReasoningAndContent(t *testing.T) {
	stream := "data: " + `{"id":"chatcmpl-grok","object":"chat.completion.chunk","model":"grok-4.5","choices":[{"delta":{"reasoning":"Plan"},"finish_reason":null}]}` + "\n\n" +
		"data: " + `{"id":"chatcmpl-grok","object":"chat.completion.chunk","model":"grok-4.5","choices":[{"delta":{"content":"Answer"},"finish_reason":"stop"}]}` + "\n\n"
	recorder := httptest.NewRecorder()
	if _, err := streamChatAsResponsesEvents(recorder, nil, newSSEStreamReader(strings.NewReader(stream))); err != nil {
		t.Fatalf("bridge mixed stream: %v body=%s", err, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "response.reasoning_summary_text.delta") || !strings.Contains(body, "response.output_text.delta") || !strings.Contains(body, "Plan") || !strings.Contains(body, "Answer") {
		t.Fatalf("mixed stream lost output: %s", body)
	}
}

func TestChatToResponsesNonStreamPreservesGrokReasoning(t *testing.T) {
	body, err := chatToResponsesResponse([]byte(`{
		"id":"chatcmpl-grok","object":"chat.completion","model":"grok-4.5",
		"choices":[{"message":{"role":"assistant","content":"","reasoning_content":"Thinking..."},"finish_reason":"stop"}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode converted response: %v body=%s", err, body)
	}
	output, _ := response["output"].([]any)
	if len(output) != 1 {
		t.Fatalf("output=%#v", output)
	}
	item, _ := output[0].(map[string]any)
	if stringValue(item["type"]) != "reasoning" || !strings.Contains(string(body), "Thinking...") {
		t.Fatalf("reasoning output missing: %s", body)
	}
}
