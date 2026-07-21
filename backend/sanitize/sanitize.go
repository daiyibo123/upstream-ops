package sanitize

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
)

const redacted = "[redacted]"

var (
	bearerPattern = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+\-/=]+`)
	headerPattern = regexp.MustCompile(`(?i)(authorization|proxy-authorization|cookie|set-cookie)\s*[:=]\s*[^\r\n]+`)
	fieldPattern  = regexp.MustCompile(`(?i)(access_token|refresh_token|id_token|session_token|api[_-]?key|secret|password)\s*[:=]\s*["']?[^\s,"'}]+`)
	urlPattern    = regexp.MustCompile(`(?i)\b(https?|socks5h?)://[^\s/@:]+:[^\s/@]+@`)
)

func RedactText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	var decoded any
	if json.Unmarshal([]byte(value), &decoded) == nil {
		decoded = redactValue(decoded)
		if body, err := json.Marshal(decoded); err == nil {
			value = string(body)
		}
	}
	value = bearerPattern.ReplaceAllString(value, "Bearer "+redacted)
	value = headerPattern.ReplaceAllStringFunc(value, func(match string) string {
		if index := strings.IndexAny(match, ":="); index >= 0 {
			return match[:index+1] + redacted
		}
		return redacted
	})
	value = fieldPattern.ReplaceAllStringFunc(value, func(match string) string {
		if index := strings.IndexAny(match, ":="); index >= 0 {
			return match[:index+1] + redacted
		}
		return redacted
	})
	value = urlPattern.ReplaceAllString(value, `$1://`+redacted+`@`)
	return value
}

func redactValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			if SensitiveKey(key) {
				out[key] = redacted
			} else {
				out[key] = redactValue(item)
			}
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, item := range typed {
			out[index] = redactValue(item)
		}
		return out
	default:
		return value
	}
}

func SensitiveKey(key string) bool {
	key = strings.ToLower(strings.NewReplacer("-", "_", " ", "_").Replace(strings.TrimSpace(key)))
	for _, marker := range []string{"authorization", "cookie", "token", "secret", "password", "credential", "api_key", "apikey", "proxy_url"} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}

func Truncate(value string, maxRunes int) string {
	value = strings.TrimSpace(value)
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	if maxRunes <= 3 {
		return string(runes[:maxRunes])
	}
	return string(runes[:maxRunes-3]) + "..."
}

func MaskIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if at := strings.Index(value, "@"); at > 1 {
		return value[:1] + "***" + value[at:]
	}
	runes := []rune(value)
	if len(runes) <= 6 {
		return "***"
	}
	return string(runes[:3]) + "***" + string(runes[len(runes)-2:])
}

func SafeProxyURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Host == "" {
		return ""
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}
