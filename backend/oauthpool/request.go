package oauthpool

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

type ProviderKind string

const (
	ProviderChatGPTCodex ProviderKind = "chatgpt_codex"
	ProviderGrokBuild    ProviderKind = "grok_build"
	ProviderGrokWeb      ProviderKind = "grok_web"
	ProviderGrokConsole  ProviderKind = "grok_console"
)

type Endpoints struct {
	ChatGPTCodex string
	GrokBuild    string
	GrokWeb      string
	GrokConsole  string
}

func (e Endpoints) withDefaults() Endpoints {
	if strings.TrimSpace(e.ChatGPTCodex) == "" {
		e.ChatGPTCodex = "https://chatgpt.com/backend-api/codex"
	}
	if strings.TrimSpace(e.GrokBuild) == "" {
		e.GrokBuild = "https://cli-chat-proxy.grok.com/v1"
	}
	if strings.TrimSpace(e.GrokWeb) == "" {
		e.GrokWeb = "https://grok.com"
	}
	if strings.TrimSpace(e.GrokConsole) == "" {
		e.GrokConsole = "https://console.x.ai/v1"
	}
	e.ChatGPTCodex = strings.TrimRight(strings.TrimSpace(e.ChatGPTCodex), "/")
	e.GrokBuild = strings.TrimRight(strings.TrimSpace(e.GrokBuild), "/")
	e.GrokWeb = strings.TrimRight(strings.TrimSpace(e.GrokWeb), "/")
	e.GrokConsole = strings.TrimRight(strings.TrimSpace(e.GrokConsole), "/")
	return e
}

type RequestInput struct {
	Method string
	Path   string
	Header http.Header
	Body   []byte
	Stream bool
}

type ResolvedRequest struct {
	Provider ProviderKind
	Method   string
	URL      string
	Header   http.Header
	Body     []byte
	Stream   bool
}

func (r ResolvedRequest) HTTPRequest(ctx context.Context) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, r.Method, r.URL, bytes.NewReader(r.Body))
	if err != nil {
		return nil, err
	}
	request.Header = r.Header.Clone()
	return request, nil
}

func (l *DispatchLease) ResolveRequest(input RequestInput) (ResolvedRequest, error) {
	if l == nil || l.service == nil {
		return ResolvedRequest{}, errors.New("oauth pool lease is nil")
	}
	return l.service.ResolveRequest(l.Pool, l.Account, l.Credentials, l.Model, input)
}

func (s *Service) ResolveRequest(pool storage.OAuthPool, account storage.OAuthAccount, credentials storage.OAuthCredentials, model string, input RequestInput) (ResolvedRequest, error) {
	if !supportsPoolModel(pool, model) {
		return ResolvedRequest{}, ErrUnsupportedModel
	}
	provider := providerFor(pool, account, credentials)
	if provider == "" {
		return ResolvedRequest{}, errors.New("oauth credential type is not recognizable")
	}
	method := strings.ToUpper(strings.TrimSpace(input.Method))
	if method == "" {
		method = http.MethodPost
	}
	header := cloneForwardHeaders(input.Header)
	body := append([]byte(nil), input.Body...)
	if len(body) == 0 {
		body = defaultRequestBody(model, input.Stream)
	}
	endpoints := s.endpoints.Load()
	if endpoints == nil {
		defaults := (Endpoints{}).withDefaults()
		endpoints = &defaults
	}

	result := ResolvedRequest{Provider: provider, Method: method, Header: header, Body: body, Stream: input.Stream}
	switch provider {
	case ProviderChatGPTCodex:
		token := strings.TrimSpace(credentials.AccessToken)
		if token == "" {
			return ResolvedRequest{}, errors.New("ChatGPT access token is missing")
		}
		result.URL = joinEndpoint(endpoints.ChatGPTCodex, input.Path, "/responses")
		header.Set("Authorization", "Bearer "+token)
		accountID := firstNonEmpty(credentials.AccountID, credentials.Organization, account.ExternalID)
		if accountID != "" {
			header.Set("chatgpt-account-id", accountID)
		}
		header.Set("Originator", config.DefaultCodexOriginator)
		header.Set("Version", config.DefaultCodexVersion)
		header.Set("OpenAI-Beta", "responses=experimental")
		header.Set("User-Agent", config.DefaultUpstreamUserAgent)
	case ProviderGrokBuild:
		token := strings.TrimSpace(credentials.AccessToken)
		if token == "" {
			return ResolvedRequest{}, errors.New("Grok Build access token is missing")
		}
		result.URL = joinEndpoint(endpoints.GrokBuild, input.Path, "/responses")
		applyGrokBuildHeaders(header, token, firstNonEmpty(credentials.AccountID, account.ExternalID), model)
	case ProviderGrokConsole:
		token := normalizedSSOToken(credentials)
		if token == "" {
			return ResolvedRequest{}, errors.New("Grok Console SSO token is missing")
		}
		result.URL = joinEndpoint(endpoints.GrokConsole, input.Path, "/responses")
		applyGrokConsoleHeaders(header, token, credentials.Cookie)
	case ProviderGrokWeb:
		token := normalizedSSOToken(credentials)
		if token == "" {
			return ResolvedRequest{}, errors.New("Grok Web SSO token is missing")
		}
		prompt, parseErr := extractPrompt(body)
		if parseErr != nil {
			return ResolvedRequest{}, parseErr
		}
		webBody, marshalErr := json.Marshal(buildGrokWebPayload(prompt, grokWebMode(model)))
		if marshalErr != nil {
			return ResolvedRequest{}, marshalErr
		}
		result.URL = endpoints.GrokWeb + "/rest/app-chat/conversations/new"
		result.Body = webBody
		applyGrokWebHeaders(header, token, credentials.Cookie, endpoints.GrokWeb)
	}
	if len(result.Body) > 0 {
		header.Set("Content-Type", "application/json")
	}
	if input.Stream {
		header.Set("Accept", "text/event-stream")
	}
	return result, nil
}

func providerFor(pool storage.OAuthPool, account storage.OAuthAccount, credentials storage.OAuthCredentials) ProviderKind {
	if pool == storage.OAuthPoolChatGPT {
		return ProviderChatGPTCodex
	}
	if pool != storage.OAuthPoolGrok {
		return ""
	}
	source := strings.ToLower(strings.TrimSpace(account.SourceFormat))
	if strings.Contains(source, "console") {
		return ProviderGrokConsole
	}
	if strings.Contains(source, "sso") || normalizedSSOToken(credentials) != "" && strings.TrimSpace(credentials.AccessToken) == "" {
		return ProviderGrokWeb
	}
	if strings.TrimSpace(credentials.AccessToken) != "" || strings.TrimSpace(credentials.RefreshToken) != "" {
		return ProviderGrokBuild
	}
	return ""
}

func cloneForwardHeaders(source http.Header) http.Header {
	value := make(http.Header)
	for name, values := range source {
		lower := strings.ToLower(strings.TrimSpace(name))
		switch lower {
		case "authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key", "chatgpt-account-id", "content-length", "host":
			continue
		}
		value[name] = append([]string(nil), values...)
	}
	return value
}

func applyGrokBuildHeaders(header http.Header, token, userID, model string) {
	const version = "0.2.102"
	header.Set("Authorization", "Bearer "+token)
	header.Set("X-XAI-Token-Auth", "xai-grok-cli")
	header.Set("x-grok-client-version", version)
	header.Set("x-grok-client-identifier", "grok-shell")
	header.Set("x-grok-client-mode", "headless")
	header.Set("x-authenticateresponse", "authenticate-response")
	header.Set("x-grok-agent-id", processAgentID)
	header.Set("x-grok-req-id", randomID())
	if userID != "" {
		header.Set("x-grok-user-id", userID)
	}
	if strings.TrimSpace(model) != "" {
		header.Set("x-grok-model-override", strings.TrimSpace(model))
	}
	header.Set("User-Agent", "grok-shell/"+version+" (linux; x86_64)")
	header.Set("Accept-Encoding", "identity")
}

func applyGrokConsoleHeaders(header http.Header, token, cookies string) {
	header.Set("Authorization", "Bearer anonymous")
	header.Set("Cookie", buildSSOCookie(token, cookies))
	header.Set("Origin", "https://console.x.ai")
	header.Set("Referer", "https://console.x.ai/")
	header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	header.Set("User-Agent", browserUserAgent)
	header.Set("x-cluster", "https://us-east-1.api.x.ai")
	header.Set("Sec-Fetch-Dest", "empty")
	header.Set("Sec-Fetch-Mode", "cors")
	header.Set("Sec-Fetch-Site", "same-origin")
}

func applyGrokWebHeaders(header http.Header, token, cookies, baseURL string) {
	header.Set("Cookie", buildSSOCookie(token, cookies))
	header.Set("Origin", baseURL)
	header.Set("Referer", baseURL+"/")
	header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	header.Set("User-Agent", browserUserAgent)
	header.Set("x-xai-request-id", randomID())
	header.Set("Sec-Fetch-Dest", "empty")
	header.Set("Sec-Fetch-Mode", "cors")
	header.Set("Sec-Fetch-Site", "same-origin")
}

const browserUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

var processAgentID = randomID()

func randomID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value)
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

func normalizedSSOToken(credentials storage.OAuthCredentials) string {
	value := firstNonEmpty(credentials.SSOToken, cookieValue(credentials.Cookie, "sso"), cookieValue(credentials.Cookie, "sso-rw"))
	value = strings.TrimSpace(value)
	if strings.HasPrefix(strings.ToLower(value), "sso=") {
		value = strings.TrimSpace(value[len("sso="):])
	}
	if token, _, found := strings.Cut(value, ";"); found {
		value = token
	}
	return strings.TrimSpace(strings.NewReplacer("\r", "", "\n", "", "\x00", "").Replace(value))
}

func buildSSOCookie(token, raw string) string {
	parts := []string{"sso=" + token, "sso-rw=" + token}
	seen := map[string]bool{"sso": true, "sso-rw": true}
	for _, part := range strings.Split(raw, ";") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		name = strings.ToLower(strings.TrimSpace(name))
		value = strings.TrimSpace(value)
		if seen[name] || value == "" || len(value) > 16<<10 || strings.ContainsAny(value, "\r\n\x00") {
			continue
		}
		if name == "cf_clearance" || name == "__cf_bm" || name == "_cfuvid" || strings.HasPrefix(name, "cf_chl_") {
			seen[name] = true
			parts = append(parts, name+"="+value)
		}
	}
	return strings.Join(parts, "; ")
}

func cookieValue(raw, wanted string) string {
	for _, part := range strings.Split(raw, ";") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && strings.EqualFold(strings.TrimSpace(name), wanted) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func joinEndpoint(base, requestedPath, fallbackPath string) string {
	path := strings.TrimSpace(requestedPath)
	if path == "" || path == "/v1/responses" || path == "/responses" {
		path = fallbackPath
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	relative, relativeErr := url.Parse(path)
	if relativeErr == nil {
		path = relative.Path
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return base + path
	}
	if strings.HasSuffix(baseURL.Path, "/v1") && strings.HasPrefix(path, "/v1/") {
		path = strings.TrimPrefix(path, "/v1")
	}
	if strings.HasSuffix(baseURL.Path, "/codex") && strings.HasPrefix(path, "/v1/") {
		path = strings.TrimPrefix(path, "/v1")
	}
	baseURL.Path = strings.TrimRight(baseURL.Path, "/") + path
	if relativeErr == nil {
		baseURL.RawQuery = relative.RawQuery
	}
	return baseURL.String()
}

func defaultRequestBody(model string, stream bool) []byte {
	value, _ := json.Marshal(map[string]any{
		"model": model, "input": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_text", "text": "hi"}}}},
		"stream": stream, "max_output_tokens": 8,
	})
	return value
}

func extractPrompt(body []byte) (string, error) {
	var root map[string]any
	if json.Unmarshal(body, &root) != nil {
		return "", errors.New("Grok SSO request body is invalid JSON")
	}
	var parts []string
	appendTextValues(root["instructions"], &parts)
	appendTextValues(root["input"], &parts)
	appendTextValues(root["messages"], &parts)
	value := strings.TrimSpace(strings.Join(parts, "\n"))
	if value == "" {
		return "", errors.New("Grok SSO request has no text input")
	}
	return value, nil
}

func appendTextValues(value any, output *[]string) {
	switch typed := value.(type) {
	case string:
		if strings.TrimSpace(typed) != "" {
			*output = append(*output, strings.TrimSpace(typed))
		}
	case []any:
		for _, item := range typed {
			appendTextValues(item, output)
		}
	case map[string]any:
		for _, key := range []string{"text", "content", "input_text", "output", "message"} {
			if nested, ok := typed[key]; ok {
				appendTextValues(nested, output)
			}
		}
	}
}

func buildGrokWebPayload(message, mode string) map[string]any {
	return map[string]any{
		"collectionIds": []any{}, "disabledConnectorIds": []any{}, "disableMemory": true,
		"disableSearch": false, "enableImageGeneration": false, "enableImageStreaming": false,
		"fileAttachments": []any{}, "imageAttachments": []any{}, "isAsyncChat": false,
		"message": message, "modeId": mode, "responseMetadata": map[string]any{},
		"sendFinalMetadata": true, "temporary": true,
	}
}

func grokWebMode(model string) string {
	model = strings.ToLower(model)
	switch {
	case strings.Contains(model, "heavy"):
		return "heavy"
	case strings.Contains(model, "expert"):
		return "expert"
	case strings.Contains(model, "auto"):
		return "auto"
	default:
		return "fast"
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func readLimited(body io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("upstream response exceeds %d bytes", limit)
	}
	return data, nil
}
