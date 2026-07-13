package notify

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-resty/resty/v2"
)

const notifyHTTPTimeout = 20 * time.Second

func newNotifyHTTPClient() *resty.Client {
	return resty.New().SetTimeout(notifyHTTPTimeout)
}

func checkRobotResponse(platform string, resp *resty.Response, codeKeys []string, messageKeys []string) error {
	if resp == nil {
		return fmt.Errorf("%s returned empty response", platform)
	}
	rawBody := strings.TrimSpace(resp.String())
	body := compactHTTPBody(rawBody)
	if resp.IsError() {
		if body == "" {
			return fmt.Errorf("%s returned %s", platform, resp.Status())
		}
		return fmt.Errorf("%s returned %s: %s", platform, resp.Status(), body)
	}
	if rawBody == "" {
		return nil
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(rawBody), &payload); err != nil {
		// Some test webhooks and lightweight compatible gateways return plain text on
		// success. HTTP 2xx is enough for those; official robots return JSON with a
		// result code, which we validate below when present.
		return nil
	}
	code, ok := firstNumber(payload, codeKeys...)
	if !ok || code == 0 {
		return nil
	}
	msg := firstString(payload, messageKeys...)
	if msg == "" {
		msg = body
	}
	return fmt.Errorf("%s returned code %g: %s", platform, code, msg)
}

func firstNumber(payload map[string]any, keys ...string) (float64, bool) {
	for _, key := range keys {
		value, ok := payload[key]
		if !ok {
			continue
		}
		switch v := value.(type) {
		case float64:
			return v, true
		case int:
			return float64(v), true
		case json.Number:
			n, err := v.Float64()
			return n, err == nil
		case string:
			var n json.Number = json.Number(strings.TrimSpace(v))
			parsed, err := n.Float64()
			return parsed, err == nil
		}
	}
	return 0, false
}

func firstString(payload map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := payload[key]
		if !ok {
			continue
		}
		if s, ok := value.(string); ok {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func compactHTTPBody(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return ""
	}
	return truncateUTF8Bytes(body, 600)
}

func messageText(msg Message) string {
	subject := strings.TrimSpace(msg.Subject)
	body := strings.TrimSpace(msg.Body)
	switch {
	case subject == "":
		return body
	case body == "":
		return subject
	default:
		return subject + "\n" + body
	}
}

func truncateUTF8Bytes(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	const suffix = "\n\n…内容过长，已截断。"
	if maxBytes <= len(suffix) {
		return suffix
	}
	limit := maxBytes - len(suffix)
	cut := 0
	for idx := range s {
		if idx > limit {
			break
		}
		cut = idx
	}
	if cut <= 0 {
		cut = limit
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
	}
	return strings.TrimSpace(s[:cut]) + suffix
}
