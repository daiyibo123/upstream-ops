package oauthpool

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

const (
	checkTotalTimeout = 45 * time.Second
	checkFirstOutput  = 25 * time.Second
	checkBodyLimit    = 1 << 20
)

// Check implements api.OAuthHealthChecker without importing the API package.
// A 2xx header alone is insufficient: the stream must contain real generated
// text or reasoning before the account is declared alive.
func (s *Service) Check(ctx context.Context, pool storage.OAuthPool, account storage.OAuthAccount, credentials storage.OAuthCredentials) storage.OAuthHealthResult {
	if state, err := s.state(pool); err == nil {
		// Health checks may run before the first gateway request. Create the
		// process-local state now so a confirmed 401/429 becomes visible to the
		// scheduler immediately, before the API handler finishes persistence.
		s.runtimeFor(state, account, s.now().UTC())
	}
	model := healthModel(providerFor(pool, account, credentials))
	resolved, err := s.ResolveRequest(pool, account, credentials, model, RequestInput{Method: http.MethodPost, Stream: true})
	if err != nil {
		return storage.OAuthHealthResult{Status: storage.OAuthStatusDead, Error: storage.RedactSensitiveText(err.Error())}
	}
	checkCtx, cancel := context.WithTimeout(ctx, checkTotalTimeout)
	defer cancel()
	response, err := s.Do(checkCtx, pool, resolved)
	if err != nil {
		s.ReportFailure(pool, account.ID, Failure{Kind: FailureTemporary, Err: err})
		until := s.now().UTC().Add(time.Duration(s.cooldownNS.Load()))
		return storage.OAuthHealthResult{Status: storage.OAuthStatusCooling, Error: storage.RedactSensitiveText(err.Error()), DisabledUntil: &until, Transient: true}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := readLimited(response.Body, 64<<10)
		return s.healthHTTPFailure(pool, account, resolved.Provider, response.StatusCode, response.Header, body)
	}

	if err := waitForMeaningfulOutput(checkCtx, response.Body, resolved.Provider, checkFirstOutput); err != nil {
		s.ReportFailure(pool, account.ID, Failure{Kind: FailureTemporary, Err: err})
		until := s.now().UTC().Add(time.Duration(s.cooldownNS.Load()))
		return storage.OAuthHealthResult{Status: storage.OAuthStatusCooling, Error: storage.RedactSensitiveText(err.Error()), DisabledUntil: &until, Transient: true}
	}
	s.ReportSuccess(pool, account.ID)
	result := storage.OAuthHealthResult{Status: storage.OAuthStatusAlive, Schedulable: true}
	quotaCtx, quotaCancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
	defer quotaCancel()
	if quota, quotaErr := s.QueryQuota(quotaCtx, pool, account, credentials); quotaErr == nil {
		result.QuotaUsed = quota.Used
		result.QuotaLimit = quota.Limit
		result.QuotaUnit = quota.Unit
		result.QuotaResetAt = quota.ResetAt
	}
	return result
}

func healthModel(provider ProviderKind) string {
	switch provider {
	case ProviderChatGPTCodex:
		return "gpt-5.4"
	case ProviderGrokWeb:
		return "grok-chat-fast"
	case ProviderGrokConsole:
		return "grok-4.3"
	default:
		return "grok-4.5"
	}
}

func (s *Service) healthHTTPFailure(pool storage.OAuthPool, account storage.OAuthAccount, provider ProviderKind, status int, header http.Header, body []byte) storage.OAuthHealthResult {
	message := safeUpstreamError(status, body)
	switch status {
	case http.StatusUnauthorized:
		s.ReportFailure(pool, account.ID, Failure{Kind: FailureAuth, StatusCode: status})
		return storage.OAuthHealthResult{Status: storage.OAuthStatusDead, Error: message}
	case http.StatusTooManyRequests:
		retry := parseRetryAfter(header.Get("Retry-After"), s.now().UTC())
		if retry <= 0 {
			retry = time.Duration(s.rateLimitNS.Load())
		}
		until := s.now().UTC().Add(retry)
		s.ReportFailure(pool, account.ID, Failure{Kind: FailureRateLimit, StatusCode: status, RetryAfter: retry})
		return storage.OAuthHealthResult{Status: storage.OAuthStatusRateLimited, Error: message, DisabledUntil: &until}
	case http.StatusForbidden:
		lower := strings.ToLower(string(body))
		if provider == ProviderGrokBuild && containsAny(lower, "invalid token", "token expired", "unauthorized", "authentication") {
			s.ReportFailure(pool, account.ID, Failure{Kind: FailureAuth, StatusCode: status})
			return storage.OAuthHealthResult{Status: storage.OAuthStatusDead, Error: message}
		}
		// Web/Console 403 is normally a proxy, Cloudflare, or browser-session
		// rejection. Preserve the credential and cool it for a recovery probe.
		until := s.now().UTC().Add(time.Duration(s.cooldownNS.Load()))
		s.ReportFailure(pool, account.ID, Failure{Kind: FailureTemporary, StatusCode: status})
		return storage.OAuthHealthResult{Status: storage.OAuthStatusCooling, Error: message, DisabledUntil: &until, Transient: true}
	default:
		until := s.now().UTC().Add(time.Duration(s.cooldownNS.Load()))
		s.ReportFailure(pool, account.ID, Failure{Kind: FailureTemporary, StatusCode: status})
		return storage.OAuthHealthResult{Status: storage.OAuthStatusCooling, Error: message, DisabledUntil: &until, Transient: true}
	}
}

func waitForMeaningfulOutput(ctx context.Context, body io.Reader, provider ProviderKind, timeout time.Duration) error {
	type result struct{ err error }
	completed := make(chan result, 1)
	go func() {
		var err error
		if provider == ProviderGrokWeb {
			err = scanGrokWebStream(body)
		} else {
			err = scanSSEStream(body)
		}
		completed <- result{err: err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.New("upstream did not produce meaningful stream output before timeout")
	case value := <-completed:
		return value.err
	}
}

func scanSSEStream(source io.Reader) error {
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 64<<10), checkBodyLimit)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") || strings.HasPrefix(line, "event:") {
			continue
		}
		if strings.HasPrefix(line, "data:") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		if line == "[DONE]" {
			return errors.New("stream completed without generated output")
		}
		if meaningfulJSON([]byte(line)) {
			return nil
		}
		if failedJSON([]byte(line)) {
			return errors.New("upstream stream reported failure")
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("upstream stream ended without generated output")
}

func scanGrokWebStream(source io.Reader) error {
	decoder := json.NewDecoder(io.LimitReader(source, checkBodyLimit))
	for {
		var root map[string]any
		if err := decoder.Decode(&root); err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("Grok Web stream ended without generated output")
			}
			return err
		}
		if root["error"] != nil {
			return errors.New("Grok Web stream reported failure")
		}
		result, _ := root["result"].(map[string]any)
		response, _ := result["response"].(map[string]any)
		if response == nil {
			continue
		}
		if response["error"] != nil {
			return errors.New("Grok Web stream reported failure")
		}
		if token, _ := response["token"].(string); strings.TrimSpace(token) != "" {
			return nil
		}
		if modelResponse, _ := response["modelResponse"].(map[string]any); meaningfulValue(modelResponse) {
			return nil
		}
	}
}

func meaningfulJSON(data []byte) bool {
	var value any
	if json.Unmarshal(data, &value) != nil {
		return strings.TrimSpace(string(data)) != ""
	}
	return meaningfulValue(value)
}

func meaningfulValue(value any) bool {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed) != ""
	case []any:
		for _, item := range typed {
			if meaningfulValue(item) {
				return true
			}
		}
	case map[string]any:
		if event, _ := typed["type"].(string); strings.Contains(strings.ToLower(event), "failed") {
			return false
		}
		for _, key := range []string{"delta", "text", "output_text", "reasoning", "content"} {
			if nested, exists := typed[key]; exists && meaningfulValue(nested) {
				return true
			}
		}
		for _, key := range []string{"output", "response", "choices", "message"} {
			if nested, exists := typed[key]; exists && meaningfulValue(nested) {
				return true
			}
		}
	}
	return false
}

func failedJSON(data []byte) bool {
	var value map[string]any
	if json.Unmarshal(data, &value) != nil {
		return false
	}
	if value["error"] != nil {
		return true
	}
	event, _ := value["type"].(string)
	return strings.Contains(strings.ToLower(event), "failed")
}

type QuotaResult struct {
	Used    *float64
	Limit   *float64
	Unit    string
	ResetAt *time.Time
}

func (s *Service) QueryQuota(ctx context.Context, pool storage.OAuthPool, account storage.OAuthAccount, credentials storage.OAuthCredentials) (QuotaResult, error) {
	provider := providerFor(pool, account, credentials)
	if provider == ProviderGrokConsole {
		limit := float64(20)
		return QuotaResult{Limit: &limit, Unit: "requests/hour"}, nil
	}
	resolved, err := s.resolveQuotaRequest(pool, account, credentials, provider)
	if err != nil {
		return QuotaResult{}, err
	}
	response, err := s.Do(ctx, pool, resolved)
	if err != nil {
		return QuotaResult{}, err
	}
	defer response.Body.Close()
	body, err := readLimited(response.Body, 4<<20)
	if err != nil {
		return QuotaResult{}, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return QuotaResult{}, fmt.Errorf("quota upstream returned HTTP %d", response.StatusCode)
	}
	return parseQuotaBody(provider, body)
}

func (s *Service) resolveQuotaRequest(pool storage.OAuthPool, account storage.OAuthAccount, credentials storage.OAuthCredentials, provider ProviderKind) (ResolvedRequest, error) {
	endpoints := s.endpoints.Load()
	if endpoints == nil {
		defaults := (Endpoints{}).withDefaults()
		endpoints = &defaults
	}
	header := make(http.Header)
	result := ResolvedRequest{Provider: provider, Method: http.MethodGet, Header: header}
	switch provider {
	case ProviderChatGPTCodex:
		token := strings.TrimSpace(credentials.AccessToken)
		if token == "" {
			return ResolvedRequest{}, errors.New("ChatGPT access token is missing")
		}
		parsed, _ := url.Parse(endpoints.ChatGPTCodex)
		result.URL = parsed.Scheme + "://" + parsed.Host + "/backend-api/wham/usage"
		header.Set("Authorization", "Bearer "+token)
		if accountID := firstNonEmpty(credentials.AccountID, credentials.Organization, account.ExternalID); accountID != "" {
			header.Set("chatgpt-account-id", accountID)
		}
	case ProviderGrokBuild:
		token := strings.TrimSpace(credentials.AccessToken)
		if token == "" {
			return ResolvedRequest{}, errors.New("Grok Build access token is missing")
		}
		result.URL = joinEndpoint(endpoints.GrokBuild, "/billing?format=credits", "/billing?format=credits")
		applyGrokBuildHeaders(header, token, firstNonEmpty(credentials.AccountID, account.ExternalID), "")
	case ProviderGrokWeb:
		token := normalizedSSOToken(credentials)
		if token == "" {
			return ResolvedRequest{}, errors.New("Grok Web SSO token is missing")
		}
		result.Method = http.MethodPost
		result.URL = endpoints.GrokWeb + "/rest/rate-limits"
		result.Body = []byte(`{"modelName":"fast"}`)
		applyGrokWebHeaders(header, token, credentials.Cookie, endpoints.GrokWeb)
		header.Set("Content-Type", "application/json")
	default:
		return ResolvedRequest{}, errors.New("quota query is unsupported for credential type")
	}
	return result, nil
}

func parseQuotaBody(provider ProviderKind, body []byte) (QuotaResult, error) {
	var root any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if decoder.Decode(&root) != nil {
		return QuotaResult{}, errors.New("quota response is not valid JSON")
	}
	if provider == ProviderGrokWeb {
		values, _ := root.(map[string]any)
		remaining := numberAt(values, "remainingQueries", "remaining")
		total := numberAt(values, "totalQueries", "total", "limit")
		if total == nil {
			return QuotaResult{}, errors.New("Grok Web quota response lacks totalQueries")
		}
		used := float64(0)
		if remaining != nil {
			used = maxFloat(0, *total-*remaining)
		}
		window := numberAt(values, "windowSizeSeconds")
		var reset *time.Time
		if window != nil && *window > 0 {
			value := time.Now().UTC().Add(time.Duration(*window) * time.Second)
			reset = &value
		}
		return QuotaResult{Used: &used, Limit: total, Unit: "requests", ResetAt: reset}, nil
	}
	used := findNumber(root, "used", "includedUsed", "totalUsed", "onDemandUsed")
	limit := findNumber(root, "limit", "monthlyLimit", "total", "onDemandCap")
	if used == nil {
		if percent := findNumber(root, "used_percent", "usagePercent", "creditUsagePercent"); percent != nil {
			used = percent
			value := float64(100)
			limit = &value
			return QuotaResult{Used: used, Limit: limit, Unit: "percent"}, nil
		}
	}
	if used == nil && limit == nil {
		return QuotaResult{}, errors.New("quota response has no recognizable usage fields")
	}
	return QuotaResult{Used: used, Limit: limit, Unit: "credits", ResetAt: findTime(root, "reset_at", "resetAt", "billingPeriodEnd", "usagePeriodEnd")}, nil
}

func numberAt(values map[string]any, keys ...string) *float64 {
	for _, key := range keys {
		if value, exists := values[key]; exists {
			if number := numericValue(value); number != nil {
				return number
			}
		}
	}
	return nil
}

func findNumber(value any, keys ...string) *float64 {
	switch typed := value.(type) {
	case map[string]any:
		if number := numberAt(typed, keys...); number != nil {
			return number
		}
		for _, nested := range typed {
			if number := findNumber(nested, keys...); number != nil {
				return number
			}
		}
	case []any:
		for _, nested := range typed {
			if number := findNumber(nested, keys...); number != nil {
				return number
			}
		}
	}
	return nil
}

func numericValue(value any) *float64 {
	var number float64
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return nil
		}
		number = parsed
	case float64:
		number = typed
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return nil
		}
		number = parsed
	default:
		return nil
	}
	return &number
}

func findTime(value any, keys ...string) *time.Time {
	switch typed := value.(type) {
	case map[string]any:
		for _, key := range keys {
			if raw, ok := typed[key].(string); ok {
				if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
					parsed = parsed.UTC()
					return &parsed
				}
			}
		}
		for _, nested := range typed {
			if parsed := findTime(nested, keys...); parsed != nil {
				return parsed
			}
		}
	case []any:
		for _, nested := range typed {
			if parsed := findTime(nested, keys...); parsed != nil {
				return parsed
			}
		}
	}
	return nil
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if parsed, err := http.ParseTime(value); err == nil && parsed.After(now) {
		return parsed.Sub(now)
	}
	return 0
}

func safeUpstreamError(status int, body []byte) string {
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = http.StatusText(status)
	}
	if len(message) > 2048 {
		message = message[:2048]
	}
	return storage.RedactSensitiveText(fmt.Sprintf("upstream HTTP %d: %s", status, message))
}

func containsAny(value string, markers ...string) bool {
	for _, marker := range markers {
		if strings.Contains(value, marker) {
			return true
		}
	}
	return false
}

func maxFloat(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}
