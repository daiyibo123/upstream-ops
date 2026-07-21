package gateway

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

const grokWebMaxFrameBytes = 8 << 20

type grokWebStreamState struct {
	text         strings.Builder
	reasoning    strings.Builder
	upstreamText strings.Builder
}

func consumeGrokWebJSONObjects(source io.Reader, consume func(map[string]any) error) error {
	reader := bufio.NewReaderSize(source, 64<<10)
	frame := make([]byte, 0, 64<<10)
	depth := 0
	inString := false
	escaped := false
	for {
		value, err := reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if depth != 0 {
					return io.ErrUnexpectedEOF
				}
				return nil
			}
			return err
		}
		if depth == 0 {
			if value != '{' {
				continue
			}
			frame = frame[:0]
			frame = append(frame, value)
			depth = 1
			inString = false
			escaped = false
			continue
		}
		frame = append(frame, value)
		if len(frame) > grokWebMaxFrameBytes {
			return fmt.Errorf("Grok Web response frame exceeds %d MiB", grokWebMaxFrameBytes>>20)
		}
		if inString {
			switch {
			case escaped:
				escaped = false
			case value == '\\':
				escaped = true
			case value == '"':
				inString = false
			}
			continue
		}
		switch value {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				var root map[string]any
				if err := json.Unmarshal(frame, &root); err != nil {
					return fmt.Errorf("decode Grok Web response frame: %w", err)
				}
				if err := consume(root); err != nil {
					return err
				}
			}
		}
	}
}

func (s *grokWebStreamState) consumeFrame(root map[string]any) (kind, delta string, err error) {
	if value := root["error"]; value != nil {
		return "", "", grokWebFrameError(value)
	}
	result, _ := root["result"].(map[string]any)
	response, _ := result["response"].(map[string]any)
	if response == nil {
		return "", "", nil
	}
	if value := response["error"]; value != nil {
		return "", "", grokWebFrameError(value)
	}
	if modelResponse, _ := response["modelResponse"].(map[string]any); modelResponse != nil {
		if err := grokWebModelResponseError(modelResponse); err != nil {
			return "", "", err
		}
		if message := strings.TrimSpace(stringValue(modelResponse["message"])); message != "" {
			current := s.upstreamText.String()
			switch {
			case current == message, strings.HasPrefix(current, message):
				return "", "", nil
			case current != "" && !strings.HasPrefix(message, current):
				return "", "", nil
			default:
				delta = message[len(current):]
				s.upstreamText.WriteString(delta)
				s.text.WriteString(delta)
				return "text", delta, nil
			}
		}
	}
	token := stringValue(response["token"])
	if token == "" || strings.EqualFold(strings.TrimSpace(stringValue(response["messageTag"])), "tool_usage_card") {
		return "", "", nil
	}
	if thinking, _ := response["isThinking"].(bool); thinking {
		s.reasoning.WriteString(token)
		return "reasoning", token, nil
	}
	tag := strings.ToLower(strings.TrimSpace(stringValue(response["messageTag"])))
	if tag != "" && tag != "final" {
		return "", "", nil
	}
	s.upstreamText.WriteString(token)
	s.text.WriteString(token)
	return "text", token, nil
}

func grokWebModelResponseError(modelResponse map[string]any) error {
	values, _ := modelResponse["streamErrors"].([]any)
	for _, value := range values {
		if message := grokWebErrorMessage(value); message != "" {
			return errors.New(message)
		}
	}
	return nil
}

func grokWebFrameError(value any) error {
	message := grokWebErrorMessage(value)
	if message == "" {
		message = "Grok Web stream reported an upstream error"
	}
	return errors.New(message)
}

func grokWebErrorMessage(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case map[string]any:
		for _, key := range []string{"message", "detail", "error", "code"} {
			if nested := grokWebErrorMessage(typed[key]); nested != "" {
				return nested
			}
		}
	case []any:
		for _, nested := range typed {
			if message := grokWebErrorMessage(nested); message != "" {
				return message
			}
		}
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	}
	return ""
}

func (s *Service) streamGrokWebOAuthResponse(body io.Reader, request normalizedRequest, key *storage.UpstreamGroupKey, w http.ResponseWriter, firstOutput *firstOutputGuard) (bool, usageTokens, error) {
	state := &grokWebStreamState{}
	responseID := "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	model := routingRequestModel(request)
	mode := request.ResponseMode
	started := false
	created := time.Now().Unix()

	start := func() error {
		if started {
			return nil
		}
		if !firstOutput.MarkReady() {
			return firstOutput.timeoutError()
		}
		setStreamResponseHeaders(w)
		if !responseWriterStarted(w) {
			w.WriteHeader(http.StatusOK)
		}
		started = true
		if mode == "chat" {
			return writeChatStreamChunk(w, strings.Replace(responseID, "resp_", "chatcmpl_", 1), model, created, map[string]any{"role": "assistant"}, nil)
		}
		return writeResponsesStreamStart(w, responseID, model)
	}

	err := consumeGrokWebJSONObjects(body, func(root map[string]any) error {
		kind, delta, err := state.consumeFrame(root)
		if err != nil {
			return err
		}
		if delta == "" {
			return nil
		}
		if kind == "text" && s.interceptedResponseContent(key, delta) != "" {
			return errors.New("response content intercepted")
		}
		if err := start(); err != nil {
			return err
		}
		if mode == "chat" {
			field := "content"
			if kind == "reasoning" {
				field = "reasoning_content"
			}
			return writeChatStreamChunk(w, strings.Replace(responseID, "resp_", "chatcmpl_", 1), model, created, map[string]any{field: delta}, nil)
		}
		event := "response.output_text.delta"
		if kind == "reasoning" {
			event = "response.reasoning_summary_text.delta"
		}
		return writeSSEEvent(w, sseEvent{Event: event, Data: mustJSON(map[string]any{"type": event, "response_id": responseID, "delta": delta})})
	})
	usage := usageTokens{Model: model, ResponseID: responseID, GeneratedText: state.text.String(), Estimated: true}
	if err != nil {
		if firstOutput.TimedOut() {
			err = firstOutput.timeoutError()
		}
		return !started, usage, err
	}
	if !started {
		return true, usage, errors.New("Grok Web stream ended without generated output")
	}
	if mode == "chat" {
		if err := writeChatStreamChunk(w, strings.Replace(responseID, "resp_", "chatcmpl_", 1), model, created, map[string]any{}, "stop"); err != nil {
			return false, usage, err
		}
		if err := writeSSEData(w, "[DONE]"); err != nil {
			return false, usage, err
		}
		return false, usage, nil
	}
	if err := writeResponsesStreamEnd(w, responseID, model, state.text.String(), usage); err != nil {
		return false, usage, err
	}
	return false, usage, nil
}

func normalizeGrokWebOAuthResponse(body io.Reader, request normalizedRequest) ([]byte, error) {
	state := &grokWebStreamState{}
	if err := consumeGrokWebJSONObjects(body, func(root map[string]any) error {
		_, _, err := state.consumeFrame(root)
		return err
	}); err != nil {
		return nil, err
	}
	text := state.text.String()
	if strings.TrimSpace(text) == "" {
		return nil, errors.New("Grok Web response ended without generated output")
	}
	model := routingRequestModel(request)
	id := "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	usage := usageTokens{Model: model, ResponseID: id, GeneratedText: text, Estimated: true}
	if request.ResponseMode == "chat" {
		return json.Marshal(map[string]any{
			"id": strings.Replace(id, "resp_", "chatcmpl_", 1), "object": "chat.completion", "created": time.Now().Unix(), "model": model,
			"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text}, "finish_reason": "stop"}},
		})
	}
	return json.Marshal(buildResponsesCompletedResponse(id, model, responseItemID(id), text, usage))
}
