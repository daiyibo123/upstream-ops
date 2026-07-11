package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/channel"
	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/connector"
	appcrypto "github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/storage"
)

const (
	gatewayKeyPrefix = "sk-"
	healthPath       = "/v1/models"
	responsesPath    = "/v1/responses"

	proxyAttemptTimeout      = 25 * time.Second
	healthProbeTimeout       = 8 * time.Second
	streamFirstEventTimeout  = 8 * time.Second
	streamPreflightMaxEvents = 16
	streamPreflightMaxBytes  = 64 << 10
)

type Service struct {
	channels   *storage.Channels
	gateway    *storage.GatewayKeys
	affinities *storage.GatewayAffinities
	groupKeys  *storage.UpstreamGroupKeys
	cipher     *appcrypto.Cipher
	channelSvc *channel.Service
	log        *slog.Logger
	clients    sync.Map
	runtime    sync.Map
	configMu   sync.RWMutex
	upstream   config.UpstreamConfig
}

type CreateGatewayKeyInput struct {
	Name          string `json:"name"`
	DailyLimit    int64  `json:"daily_limit"`
	TotalLimit    int64  `json:"total_limit"`
	ExpiresInDays int    `json:"expires_in_days"`
}

type GatewayKeyOutput struct {
	ID          uint       `json:"id"`
	Name        string     `json:"name"`
	KeyPrefix   string     `json:"key_prefix"`
	Key         string     `json:"key,omitempty"`
	Enabled     bool       `json:"enabled"`
	DailyLimit  int64      `json:"daily_limit"`
	TotalLimit  int64      `json:"total_limit"`
	TodayTokens int64      `json:"today_tokens"`
	TotalTokens int64      `json:"total_tokens"`
	UsageDate   string     `json:"usage_date,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	LastUsedAt  *time.Time `json:"last_used_at,omitempty"`
	LastUsedIP  string     `json:"last_used_ip,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

type BootstrapResult struct {
	Created int             `json:"created"`
	Updated int             `json:"updated"`
	Skipped int             `json:"skipped"`
	Failed  int             `json:"failed"`
	Items   []BootstrapItem `json:"items"`
}

type BootstrapItem struct {
	ChannelID   uint    `json:"channel_id"`
	ChannelName string  `json:"channel_name"`
	GroupRef    string  `json:"group_ref"`
	GroupName   string  `json:"group_name"`
	Ratio       float64 `json:"ratio"`
	Created     bool    `json:"created"`
	Error       string  `json:"error,omitempty"`
}

type HealthResult struct {
	Checked int                `json:"checked"`
	Alive   int                `json:"alive"`
	Dead    int                `json:"dead"`
	Items   []HealthResultItem `json:"items"`
}

type HealthResultItem struct {
	ID          uint       `json:"id"`
	ChannelID   uint       `json:"channel_id"`
	ChannelName string     `json:"channel_name"`
	GroupRef    string     `json:"group_ref"`
	GroupName   string     `json:"group_name"`
	Ratio       float64    `json:"ratio"`
	Status      string     `json:"status"`
	LatencyMS   int64      `json:"latency_ms"`
	Error       string     `json:"error,omitempty"`
	CheckedAt   *time.Time `json:"checked_at,omitempty"`
}

type UpdateGroupKeyInput struct {
	ConcurrencyLimit int `json:"concurrency_limit"`
}

type normalizedRequest struct {
	Method       string
	Path         string
	Header       http.Header
	Body         []byte
	ResponseMode string
	Stream       bool
	AffinityKey  string
}

type usageTokens struct {
	Prompt     int64
	Completion int64
	Total      int64
	ResponseID string
}

type groupRuntimeState struct {
	mu             sync.Mutex
	disabledUntil  time.Time
	avgLatencyMS   float64
	inFlight       int
	lastObservedAt time.Time
}

type sseEvent struct {
	Event string
	Data  string
}

type sseStreamReader struct {
	scanner *bufio.Scanner
	event   string
	data    strings.Builder
	closed  bool
}

type GatewayError struct {
	Status int
	Body   []byte
	Header http.Header
}

func (e *GatewayError) Error() string {
	if len(e.Body) > 0 {
		return string(e.Body)
	}
	return http.StatusText(e.Status)
}

func NewService(
	channels *storage.Channels,
	gatewayKeys *storage.GatewayKeys,
	affinities *storage.GatewayAffinities,
	groupKeys *storage.UpstreamGroupKeys,
	cipher *appcrypto.Cipher,
	channelSvc *channel.Service,
	log *slog.Logger,
) *Service {
	return &Service{
		channels:   channels,
		gateway:    gatewayKeys,
		affinities: affinities,
		groupKeys:  groupKeys,
		cipher:     cipher,
		channelSvc: channelSvc,
		log:        log,
		upstream:   config.UpstreamConfig{}.WithDefaults(),
	}
}

func (s *Service) UpdateUpstreamConfig(cfg config.UpstreamConfig) {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	s.upstream = cfg.WithDefaults()
}

func (s *Service) upstreamConfig() config.UpstreamConfig {
	s.configMu.RLock()
	cfg := s.upstream
	s.configMu.RUnlock()
	return cfg.WithDefaults()
}

func HashKey(key string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(key)))
	return hex.EncodeToString(sum[:])
}

func (s *Service) CreateGatewayKey(input CreateGatewayKeyInput) (*GatewayKeyOutput, error) {
	name := strings.TrimSpace(input.Name)
	if name == "" {
		name = "default"
	}
	key, err := randomGatewayKey()
	if err != nil {
		return nil, err
	}
	ciphertext, err := s.cipher.Encrypt(key)
	if err != nil {
		return nil, err
	}
	rec := &storage.GatewayKey{
		Name:       name,
		KeyPrefix:  visiblePrefix(key),
		KeyHash:    HashKey(key),
		KeyCipher:  ciphertext,
		Enabled:    true,
		DailyLimit: maxInt64(0, input.DailyLimit),
		TotalLimit: maxInt64(0, input.TotalLimit),
	}
	if input.ExpiresInDays > 0 {
		expiresAt := time.Now().AddDate(0, 0, input.ExpiresInDays)
		rec.ExpiresAt = &expiresAt
	}
	if err := s.gateway.Create(rec); err != nil {
		return nil, err
	}
	out := gatewayKeyOutput(*rec)
	out.Key = key
	return &out, nil
}

func (s *Service) ListGatewayKeys() ([]GatewayKeyOutput, error) {
	list, err := s.gateway.List()
	if err != nil {
		return nil, err
	}
	out := make([]GatewayKeyOutput, 0, len(list))
	for _, item := range list {
		out = append(out, gatewayKeyOutput(item))
	}
	return out, nil
}

func (s *Service) RevealGatewayKey(id uint) (string, error) {
	key, err := s.gateway.FindByID(id)
	if err != nil {
		return "", err
	}
	return s.cipher.Decrypt(key.KeyCipher)
}

func (s *Service) DeleteGatewayKey(id uint) error {
	return s.gateway.Delete(id)
}

func (s *Service) Authenticate(raw string, ip string) (*storage.GatewayKey, error) {
	key := strings.TrimSpace(raw)
	if key == "" {
		return nil, errors.New("missing gateway key")
	}
	rec, err := s.gateway.FindEnabledByHash(HashKey(key))
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, errors.New("invalid gateway key")
	}
	if rec.ExpiresAt != nil && time.Now().After(*rec.ExpiresAt) {
		return nil, errors.New("gateway key expired")
	}
	_ = s.gateway.Touch(rec.ID, ip)
	return rec, nil
}

func (s *Service) ListGroupKeys() ([]storage.UpstreamGroupKey, error) {
	return s.groupKeys.List()
}

func (s *Service) UpdateGroupKey(id uint, input UpdateGroupKeyInput) (*storage.UpstreamGroupKey, error) {
	limit := input.ConcurrencyLimit
	if limit < 0 {
		limit = 0
	}
	if err := s.groupKeys.UpdateConcurrencyLimit(id, limit); err != nil {
		return nil, err
	}
	return s.groupKeys.FindByID(id)
}

func (s *Service) BootstrapGroupKeys(ctx context.Context) (*BootstrapResult, error) {
	channels, err := s.channels.List()
	if err != nil {
		return nil, err
	}
	result := &BootstrapResult{Items: []BootstrapItem{}}
	for i := range channels {
		ch := channels[i]
		if ch.Type != storage.ChannelTypeSub2API && ch.Type != storage.ChannelTypeNewAPI {
			continue
		}
		groups, err := s.channelSvc.ListAPIKeyGroups(ctx, ch.ID)
		if err != nil {
			result.Failed++
			result.Items = append(result.Items, BootstrapItem{
				ChannelID:   ch.ID,
				ChannelName: ch.Name,
				Error:       err.Error(),
			})
			continue
		}
		for _, group := range groups {
			item := BootstrapItem{
				ChannelID:   ch.ID,
				ChannelName: ch.Name,
				GroupName:   strings.TrimSpace(group.Name),
				Ratio:       normalizedRatio(group.Ratio),
			}
			groupRef, groupID := groupRefFor(ch.Type, group)
			item.GroupRef = groupRef
			if groupRef == "" || item.GroupName == "" {
				result.Skipped++
				continue
			}
			existing, err := s.groupKeys.FindByChannelGroup(ch.ID, groupRef)
			if err != nil {
				result.Failed++
				item.Error = err.Error()
				result.Items = append(result.Items, item)
				continue
			}
			if existing != nil && existing.KeyCipher != "" {
				upsert := upstreamGroupKeyFrom(ch, group, groupRef, "")
				upsert.UpstreamKeyID = existing.UpstreamKeyID
				if err := s.groupKeys.Upsert(upsert); err != nil {
					result.Failed++
					item.Error = err.Error()
				} else {
					result.Updated++
				}
				result.Items = append(result.Items, item)
				continue
			}
			key, err := s.createUpstreamKey(ctx, ch, group, groupID)
			if err != nil {
				result.Failed++
				item.Error = err.Error()
				result.Items = append(result.Items, item)
				continue
			}
			key.GroupRef = groupRef
			if err := s.groupKeys.Upsert(key); err != nil {
				result.Failed++
				item.Error = err.Error()
				result.Items = append(result.Items, item)
				continue
			}
			result.Created++
			item.Created = true
			result.Items = append(result.Items, item)
		}
	}
	return result, nil
}

func (s *Service) TestAllGroupKeys(ctx context.Context) (*HealthResult, error) {
	list, err := s.groupKeys.List()
	if err != nil {
		return nil, err
	}
	result := &HealthResult{Items: []HealthResultItem{}}
	for i := range list {
		item := s.testGroupKey(ctx, &list[i])
		result.Checked++
		if item.Status == "alive" {
			result.Alive++
		} else {
			result.Dead++
		}
		result.Items = append(result.Items, item)
	}
	return result, nil
}

func (s *Service) Proxy(w http.ResponseWriter, r *http.Request, path string) error {
	rawKey := extractGatewayKey(r.Header)
	gatewayKey, err := s.Authenticate(rawKey, clientIP(r))
	if err != nil {
		return &GatewayError{Status: http.StatusUnauthorized, Body: jsonError(err.Error())}
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return &GatewayError{Status: http.StatusBadRequest, Body: jsonError("read request body: " + err.Error())}
	}
	_ = r.Body.Close()
	if err := enforceGatewayQuota(gatewayKey); err != nil {
		return &GatewayError{Status: http.StatusTooManyRequests, Body: jsonError(err.Error())}
	}

	normalized, err := normalizeProxyRequest(r, path, body)
	if err != nil {
		return &GatewayError{Status: http.StatusBadRequest, Body: jsonError(err.Error())}
	}
	normalized = s.rectifyBeforeSend(normalized)

	candidates, err := s.groupKeys.ListCandidates(time.Now())
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return &GatewayError{Status: http.StatusServiceUnavailable, Body: jsonError("no alive upstream group keys available")}
	}
	candidates = s.orderCandidatesForRequest(candidates, normalized)

	var errorsSeen []string
	var saturatedSeen []string
	var disabledSeen []string
	// finalErr 承载"客户端错、换 key 也没用"路径的返回值。
	// 在 stream 已写字节 / 明确 client-side 400 等场景下，我们把 err 记进来后不再继续 fail-over。
	var finalErr error
	for i := range candidates {
		candidate := candidates[i]
		if s.runtimeDisabled(candidate.ID) {
			disabledSeen = append(disabledSeen, fmt.Sprintf("%s/%s", candidate.ChannelName, candidate.GroupName))
			continue
		}

		// 把单个候选的尝试封装到闭包里，用 defer release() 保证 panic / 早退时不泄漏并发额度。
		outcome := s.attemptCandidate(r.Context(), gatewayKey, normalized, &candidate, w)

		switch outcome.kind {
		case candSuccess:
			return nil
		case candSaturated:
			saturatedSeen = append(saturatedSeen, fmt.Sprintf("%s/%s", candidate.ChannelName, candidate.GroupName))
			continue
		case candRetryable:
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.ChannelName, candidate.GroupName, outcome.errMsg))
			s.markProxyFailure(candidate.ID, outcome.errMsg)
			continue
		case candFatal:
			// 明确"客户端错 / 已写字节无法切换"路径：仍然记一次失败方便下次跳过，
			// 但不再继续 fail-over，把当前错误吐给调用方。
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.ChannelName, candidate.GroupName, outcome.errMsg))
			if outcome.markFailure {
				s.markProxyFailure(candidate.ID, outcome.errMsg)
			}
			finalErr = outcome.err
			break
		}
		if finalErr != nil {
			return finalErr
		}
	}
	message := "all upstream group keys failed: " + strings.Join(errorsSeen, " | ")
	if len(errorsSeen) == 0 && len(saturatedSeen) > 0 {
		message = "all upstream group keys are at concurrency limit: " + strings.Join(saturatedSeen, " | ")
	}
	if len(errorsSeen) == 0 && len(saturatedSeen) == 0 && len(disabledSeen) > 0 {
		message = "all upstream group keys are temporarily disabled by recent failures: " + strings.Join(disabledSeen, " | ")
	}
	return &GatewayError{
		Status: http.StatusServiceUnavailable,
		Body:   jsonError(message),
	}
}

// candOutcomeKind 描述一次候选尝试的走向：
//
//   - candSuccess    成功，Proxy 直接返回 nil；
//   - candSaturated  候选被并发上限拒收，Proxy 记录后跳到下一个；
//   - candRetryable  可 fail-over 的失败（含网络错、5xx、大部分 4xx、200 但 body 是错），
//     Proxy 会 markProxyFailure 后继续；
//   - candFatal      不能再切候选（stream 已经写字节、或明确的客户端请求错），Proxy 直接返回 err。
type candOutcomeKind int

const (
	candSuccess candOutcomeKind = iota
	candSaturated
	candRetryable
	candFatal
)

type candOutcome struct {
	kind        candOutcomeKind
	err         error
	errMsg      string
	markFailure bool
}

// attemptCandidate 在单个候选上跑一次完整的请求尝试（含 rectifier 二次），
// 通过 defer 保证并发额度一定释放（避免早退 / panic 泄漏计数）。
func (s *Service) attemptCandidate(
	ctx context.Context,
	gatewayKey *storage.GatewayKey,
	normalized normalizedRequest,
	candidate *storage.UpstreamGroupKey,
	w http.ResponseWriter,
) candOutcome {
	release, ok := s.tryAcquireCandidate(candidate.ID, candidate.ConcurrencyLimit)
	if !ok {
		return candOutcome{kind: candSaturated}
	}
	defer release()

	if normalized.Stream {
		return s.attemptStream(ctx, gatewayKey, normalized, candidate, w)
	}
	return s.attemptNonStream(ctx, gatewayKey, normalized, candidate, w)
}

func (s *Service) attemptStream(
	ctx context.Context,
	gatewayKey *storage.GatewayKey,
	normalized normalizedRequest,
	candidate *storage.UpstreamGroupKey,
	w http.ResponseWriter,
) candOutcome {
	start := time.Now()
	retry, usage, err := s.streamProxyCandidate(ctx, normalized, candidate, w)
	if err == nil {
		s.recordRuntimeSuccess(candidate.ID, time.Since(start))
		_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, time.Now())
		_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
		s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
		return candOutcome{kind: candSuccess}
	}
	errMsg := err.Error()
	if rectified, reason, ok := s.rectifyAfterFailure(normalized, errMsg); ok {
		start = time.Now()
		retry, usage, err = s.streamProxyCandidate(ctx, rectified, candidate, w)
		if err == nil {
			s.recordRuntimeSuccess(candidate.ID, time.Since(start))
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			return candOutcome{kind: candSuccess}
		}
		errMsg = reason + " retry failed: " + err.Error()
	}
	if retry {
		return candOutcome{kind: candRetryable, err: err, errMsg: errMsg}
	}
	// 流已经开始写 / 明确 fatal：仍然记一次失败（这样下次调度不会又选中这个坏候选），
	// 但不再切候选（否则会往同一个 ResponseWriter 上二次写头/写字节）。
	return candOutcome{kind: candFatal, err: err, errMsg: errMsg, markFailure: true}
}

func (s *Service) attemptNonStream(
	ctx context.Context,
	gatewayKey *storage.GatewayKey,
	normalized normalizedRequest,
	candidate *storage.UpstreamGroupKey,
	w http.ResponseWriter,
) candOutcome {
	start := time.Now()
	status, header, respBody, retry, err := s.tryProxyCandidate(ctx, normalized, candidate)
	if err == nil {
		s.recordRuntimeSuccess(candidate.ID, time.Since(start))
		usage := extractUsage(respBody)
		_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, time.Now())
		_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
		s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
		writeProxyResponse(w, status, header, respBody, candidate, normalized.ResponseMode)
		return candOutcome{kind: candSuccess}
	}
	errMsg := err.Error()
	if rectified, reason, ok := s.rectifyAfterFailure(normalized, errMsg); ok {
		start = time.Now()
		status, header, respBody, retry, err = s.tryProxyCandidate(ctx, rectified, candidate)
		if err == nil {
			s.recordRuntimeSuccess(candidate.ID, time.Since(start))
			usage := extractUsage(respBody)
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			writeProxyResponse(w, status, header, respBody, candidate, normalized.ResponseMode)
			return candOutcome{kind: candSuccess}
		}
		errMsg = reason + " retry failed: " + err.Error()
	}
	if retry {
		_ = status
		_ = header
		_ = respBody
		return candOutcome{kind: candRetryable, err: err, errMsg: errMsg}
	}
	// 非 stream 场景下的 fatal 走这里（客户端参数错等）；不 markFailure，因为不是候选的锅。
	return candOutcome{kind: candFatal, err: err, errMsg: errMsg, markFailure: false}
}

func (s *Service) streamProxyCandidate(ctx context.Context, request normalizedRequest, key *storage.UpstreamGroupKey, w http.ResponseWriter) (bool, usageTokens, error) {
	ch, err := s.channels.FindByID(key.ChannelID)
	if err != nil {
		return true, usageTokens{}, err
	}
	upstreamKey, err := s.cipher.Decrypt(key.KeyCipher)
	if err != nil {
		return true, usageTokens{}, err
	}
	upstreamURL, err := joinUpstreamURL(ch.SiteURL, request.Path)
	if err != nil {
		return true, usageTokens{}, err
	}
	req, err := http.NewRequestWithContext(ctx, request.Method, upstreamURL, bytes.NewReader(request.Body))
	if err != nil {
		return true, usageTokens{}, err
	}
	copyRequestHeaders(req.Header, request.Header)
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(upstreamKey))
	req.Header.Set("X-UpstreamOps-Group", key.GroupName)
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "codex-cli/0.1 upstream-ops")
	}

	client := s.httpClientFor(ctx, ch)
	resp, err := client.Do(req)
	if err != nil {
		return true, usageTokens{}, err
	}
	defer resp.Body.Close()

	header := cloneHeader(resp.Header)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if !isEventStream(header) {
			respBody, readErr := readLimitedBody(resp.Body, 64<<20)
			if readErr != nil {
				return true, usageTokens{}, readErr
			}
			if isUpstreamErrorBody(respBody) {
				return true, usageTokens{}, fmt.Errorf("upstream returned error payload: %s", truncateBody(respBody, 240))
			}
			writeProxyResponse(w, resp.StatusCode, header, respBody, key, request.ResponseMode)
			return false, extractUsage(respBody), nil
		}
		reader := newSSEStreamReader(resp.Body)
		buffered, err := preflightSSEStream(reader, resp.Body)
		if err != nil {
			return true, usageTokens{}, err
		}
		copyResponseHeaders(w, header, key)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(resp.StatusCode)
		switch request.ResponseMode {
		case "chat":
			usage, err := streamResponsesAsChatEvents(w, buffered, reader)
			return false, usage, err
		case "claude":
			usage, err := streamResponsesAsClaudeEvents(w, buffered, reader)
			return false, usage, err
		}
		usage, err := streamRawSSE(w, buffered, reader)
		return false, usage, err
	}
	respBody, readErr := readLimitedBody(resp.Body, 64<<20)
	if readErr != nil {
		return true, usageTokens{}, readErr
	}
	errText := truncateBody(respBody, 240)
	if shouldRetryUpstreamStatus(resp.StatusCode, errText) {
		return true, usageTokens{}, fmt.Errorf("upstream returned HTTP %d: %s", resp.StatusCode, truncateBody(respBody, 240))
	}
	return false, usageTokens{}, &GatewayError{Status: resp.StatusCode, Header: header, Body: respBody}
}

func (s *Service) createUpstreamKey(ctx context.Context, ch storage.Channel, group connector.APIKeyGroup, groupID *int64) (*storage.UpstreamGroupKey, error) {
	customKey, err := randomOpenAIKey()
	if err != nil {
		return nil, err
	}
	unlimited := true
	expiredTime := int64(-1)
	crossGroupRetry := false
	req := connector.APIKeyCreateRequest{
		Name:            fmt.Sprintf("UpstreamOps Gateway - %s - %s", ch.Name, group.Name),
		CustomKey:       customKey,
		UnlimitedQuota:  &unlimited,
		ExpiredTime:     &expiredTime,
		CrossGroupRetry: &crossGroupRetry,
	}
	switch ch.Type {
	case storage.ChannelTypeSub2API:
		req.GroupID = groupID
	case storage.ChannelTypeNewAPI:
		req.Group = group.Name
	default:
		return nil, fmt.Errorf("unsupported channel type %s", ch.Type)
	}
	created, err := s.channelSvc.CreateAPIKey(ctx, ch.ID, req)
	if err != nil {
		return nil, err
	}
	if created == nil {
		return nil, errors.New("upstream did not return key metadata")
	}
	fullKey := strings.TrimSpace(created.Key)
	if fullKey == "" && created.ID > 0 {
		fullKey, err = s.channelSvc.RevealAPIKey(ctx, ch.ID, created.ID)
		if err != nil {
			return nil, err
		}
	}
	if fullKey == "" {
		fullKey = customKey
	}
	if fullKey == "" {
		return nil, errors.New("upstream did not return a usable key")
	}
	ciphertext, err := s.cipher.Encrypt(fullKey)
	if err != nil {
		return nil, err
	}
	rec := upstreamGroupKeyFrom(ch, group, "", ciphertext)
	rec.UpstreamKeyID = created.ID
	return rec, nil
}

func upstreamGroupKeyFrom(ch storage.Channel, group connector.APIKeyGroup, groupRef, keyCipher string) *storage.UpstreamGroupKey {
	if groupRef == "" {
		groupRef, _ = groupRefFor(ch.Type, group)
	}
	return &storage.UpstreamGroupKey{
		ChannelID:        ch.ID,
		ChannelName:      ch.Name,
		ChannelType:      ch.Type,
		GroupRef:         groupRef,
		GroupName:        strings.TrimSpace(group.Name),
		GroupDesc:        strings.TrimSpace(group.Description),
		Ratio:            normalizedRatio(group.Ratio),
		ConcurrencyLimit: 0,
		KeyCipher:        keyCipher,
		Status:           "unknown",
		FailureCount:     0,
	}
}

func groupRefFor(channelType storage.ChannelType, group connector.APIKeyGroup) (string, *int64) {
	if channelType == storage.ChannelTypeSub2API && group.ID != nil {
		return strconv.FormatInt(*group.ID, 10), group.ID
	}
	name := strings.TrimSpace(group.Name)
	return name, nil
}

func (s *Service) testGroupKey(ctx context.Context, key *storage.UpstreamGroupKey) HealthResultItem {
	start := time.Now()
	item := HealthResultItem{
		ID:          key.ID,
		ChannelID:   key.ChannelID,
		ChannelName: key.ChannelName,
		GroupRef:    key.GroupRef,
		GroupName:   key.GroupName,
		Ratio:       key.Ratio,
	}
	req := normalizedRequest{
		Method: http.MethodGet,
		Path:   healthPath,
		Header: http.Header{},
	}
	status, _, body, err := s.requestCandidate(ctx, req, key, healthProbeTimeout)
	item.LatencyMS = time.Since(start).Milliseconds()
	now := time.Now()
	item.CheckedAt = &now
	if err == nil && status >= 200 && status < 300 {
		item.Status = "alive"
		_ = s.groupKeys.MarkSuccess(key.ID)
		return item
	}
	if err == nil {
		err = fmt.Errorf("HTTP %d: %s", status, truncateBody(body, 240))
	}
	item.Status = "dead"
	item.Error = err.Error()
	s.markProxyFailure(key.ID, item.Error)
	return item
}

func (s *Service) tryProxyCandidate(ctx context.Context, request normalizedRequest, key *storage.UpstreamGroupKey) (int, http.Header, []byte, bool, error) {
	status, header, respBody, err := s.requestCandidate(ctx, request, key, proxyAttemptTimeout)
	if err != nil {
		return 0, nil, nil, true, err
	}
	if status >= 200 && status < 300 {
		if isUpstreamErrorBody(respBody) {
			return status, header, respBody, true, fmt.Errorf("upstream returned error payload: %s", truncateBody(respBody, 240))
		}
		return status, header, respBody, false, nil
	}
	errText := truncateBody(respBody, 240)
	if shouldRetryUpstreamStatus(status, errText) {
		return status, header, respBody, true, fmt.Errorf("upstream returned HTTP %d: %s", status, truncateBody(respBody, 240))
	}
	return status, header, respBody, false, &GatewayError{Status: status, Header: header, Body: respBody}
}

func (s *Service) requestCandidate(ctx context.Context, request normalizedRequest, key *storage.UpstreamGroupKey, timeout time.Duration) (int, http.Header, []byte, error) {
	ch, err := s.channels.FindByID(key.ChannelID)
	if err != nil {
		return 0, nil, nil, err
	}
	upstreamKey, err := s.cipher.Decrypt(key.KeyCipher)
	if err != nil {
		return 0, nil, nil, err
	}
	upstreamURL, err := joinUpstreamURL(ch.SiteURL, request.Path)
	if err != nil {
		return 0, nil, nil, err
	}
	reqCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, request.Method, upstreamURL, bytes.NewReader(request.Body))
	if err != nil {
		return 0, nil, nil, err
	}
	copyRequestHeaders(req.Header, request.Header)
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(upstreamKey))
	req.Header.Set("X-UpstreamOps-Group", key.GroupName)
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "codex-cli/0.1 upstream-ops")
	}

	client := s.httpClientFor(ctx, ch)
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()

	header := cloneHeader(resp.Header)
	respBody, readErr := readLimitedBody(resp.Body, 64<<20)
	if readErr != nil {
		return resp.StatusCode, header, nil, readErr
	}
	return resp.StatusCode, header, respBody, nil
}

func (s *Service) httpClientFor(ctx context.Context, ch *storage.Channel) *http.Client {
	proxyURL := ""
	if ch != nil && ch.ProxyEnabled && s.channelSvc != nil {
		if resolved, err := s.channelSvc.Resolve(ctx, ch); err == nil && strings.TrimSpace(resolved.ProxyURL) != "" {
			proxyURL = strings.TrimSpace(resolved.ProxyURL)
		}
	}
	cacheKey := httpClientCacheKey(ch, proxyURL)
	if cached, ok := s.clients.Load(cacheKey); ok {
		return cached.(*http.Client)
	}
	client := &http.Client{Transport: buildProxyTransport(proxyURL)}
	actual, _ := s.clients.LoadOrStore(cacheKey, client)
	return actual.(*http.Client)
}

func httpClientCacheKey(ch *storage.Channel, proxyURL string) string {
	if ch == nil {
		return "default|" + strings.TrimSpace(proxyURL)
	}
	return fmt.Sprintf("%d|%t|%s", ch.ID, ch.ProxyEnabled, strings.TrimSpace(proxyURL))
}

func buildProxyTransport(proxyURL string) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 64
	transport.IdleConnTimeout = 90 * time.Second
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.ExpectContinueTimeout = time.Second
	transport.ResponseHeaderTimeout = proxyAttemptTimeout
	if proxyURL = strings.TrimSpace(proxyURL); proxyURL != "" {
		if parsed, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(parsed)
		}
	}
	return transport
}

func (s *Service) markProxyFailure(id uint, msg string) {
	delay := 30 * time.Second
	if current, err := s.groupKeys.FindByID(id); err == nil && current != nil {
		switch {
		case current.FailureCount <= 0:
			delay = 30 * time.Second
		case current.FailureCount == 1:
			delay = time.Minute
		case current.FailureCount == 2:
			delay = 3 * time.Minute
		default:
			delay = time.Duration(math.Min(float64(current.FailureCount), 10)) * time.Minute
		}
	}
	until := time.Now().Add(delay)
	s.recordRuntimeFailure(id, until)
	if err := s.groupKeys.MarkFailure(id, msg, until); err != nil && s.log != nil {
		s.log.Warn("mark upstream group failed", "id", id, "err", err)
	}
}

func normalizeProxyRequest(r *http.Request, path string, body []byte) (normalizedRequest, error) {
	cleanPath := path
	rawQuery := ""
	if idx := strings.Index(cleanPath, "?"); idx >= 0 {
		rawQuery = cleanPath[idx+1:]
		cleanPath = cleanPath[:idx]
	}
	req := normalizedRequest{
		Method:       r.Method,
		Path:         path,
		Header:       cloneHeader(r.Header),
		Body:         body,
		ResponseMode: "raw",
	}
	if r.Method == http.MethodGet || strings.TrimSpace(string(body)) == "" {
		return req, nil
	}
	req.AffinityKey = affinityLookupKey(body)
	switch cleanPath {
	case "/v1/chat/completions":
		converted, stream, err := chatToResponsesBody(body)
		if err != nil {
			return req, err
		}
		req.Path = responsesPath
		if rawQuery != "" {
			req.Path += "?" + rawQuery
		}
		req.Body = converted
		req.ResponseMode = "chat"
		req.Stream = stream
	case responsesPath:
		req.ResponseMode = "responses"
		req.Stream = requestStream(body)
	case "/v1/messages":
		converted, stream, err := claudeToResponsesBody(body)
		if err != nil {
			return req, err
		}
		req.Path = responsesPath
		req.Body = converted
		req.ResponseMode = "claude"
		req.Stream = stream
	default:
		req.Stream = requestStream(body)
	}
	return req, nil
}

func chatToResponsesBody(body []byte) ([]byte, bool, error) {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, false, fmt.Errorf("decode chat request: %w", err)
	}
	stream := boolField(raw, "stream")
	if messages, ok := raw["messages"]; ok {
		raw["input"] = normalizeChatMessages(messages)
		delete(raw, "messages")
	}
	moveField(raw, "max_tokens", "max_output_tokens")
	moveField(raw, "max_completion_tokens", "max_output_tokens")
	delete(raw, "n")
	delete(raw, "logprobs")
	delete(raw, "top_logprobs")
	out, err := json.Marshal(raw)
	return out, stream, err
}

func normalizeChatMessages(messages any) any {
	items, ok := messages.([]any)
	if !ok {
		return messages
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		msg, _ := item.(map[string]any)
		role, _ := msg["role"].(string)
		if role == "" {
			role = "user"
		}
		out = append(out, map[string]any{
			"role":    role,
			"content": normalizeChatContent(msg["content"], role),
		})
	}
	return out
}

func normalizeChatContent(content any, role string) any {
	switch v := content.(type) {
	case string:
		return v
	case []any:
		out := make([]map[string]any, 0, len(v))
		textType := "input_text"
		if role == "assistant" {
			textType = "output_text"
		}
		for _, item := range v {
			part, _ := item.(map[string]any)
			typ := strings.ToLower(strings.TrimSpace(fmt.Sprint(part["type"])))
			switch typ {
			case "text", "input_text", "output_text":
				text := strings.TrimSpace(fmt.Sprint(part["text"]))
				if text != "" {
					out = append(out, map[string]any{"type": textType, "text": text})
				}
			case "image_url", "input_image":
				if image := chatImageBlockToResponses(part); image != nil {
					out = append(out, image)
				}
			default:
				if text, ok := part["text"].(string); ok && strings.TrimSpace(text) != "" {
					out = append(out, map[string]any{"type": textType, "text": text})
				}
			}
		}
		return out
	default:
		return fmt.Sprint(v)
	}
}

func chatImageBlockToResponses(part map[string]any) map[string]any {
	if url, ok := part["image_url"].(string); ok && strings.TrimSpace(url) != "" {
		return map[string]any{"type": "input_image", "image_url": strings.TrimSpace(url)}
	}
	if obj, ok := part["image_url"].(map[string]any); ok {
		if url, ok := obj["url"].(string); ok && strings.TrimSpace(url) != "" {
			return map[string]any{"type": "input_image", "image_url": strings.TrimSpace(url)}
		}
	}
	if url, ok := part["image_url"].(map[string]string); ok {
		if strings.TrimSpace(url["url"]) != "" {
			return map[string]any{"type": "input_image", "image_url": strings.TrimSpace(url["url"])}
		}
	}
	if url, ok := part["image_url"].(fmt.Stringer); ok && strings.TrimSpace(url.String()) != "" {
		return map[string]any{"type": "input_image", "image_url": strings.TrimSpace(url.String())}
	}
	if url, ok := part["image_url"].(any); ok {
		if s := strings.TrimSpace(fmt.Sprint(url)); s != "" && s != "<nil>" && !strings.HasPrefix(s, "map[") {
			return map[string]any{"type": "input_image", "image_url": s}
		}
	}
	return nil
}

func claudeToResponsesBody(body []byte) ([]byte, bool, error) {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, false, fmt.Errorf("decode claude request: %w", err)
	}
	stream := boolField(raw, "stream")
	out := map[string]any{}
	for _, key := range []string{"model", "temperature", "top_p", "tools", "tool_choice", "metadata"} {
		if value, ok := raw[key]; ok {
			out[key] = value
		}
	}
	if system, ok := raw["system"]; ok {
		out["instructions"] = claudeSystemText(system)
	}
	if messages, ok := raw["messages"]; ok {
		out["input"] = normalizeClaudeMessages(messages)
	}
	if maxTokens, ok := raw["max_tokens"]; ok {
		out["max_output_tokens"] = maxTokens
	}
	if reasoning := claudeThinkingToResponsesReasoning(raw["thinking"]); reasoning != nil {
		out["reasoning"] = reasoning
	}
	if stream {
		out["stream"] = true
	}
	encoded, err := json.Marshal(out)
	return encoded, stream, err
}

func (s *Service) rectifierConfig() config.RequestRectifierConfig {
	return s.upstreamConfig().RequestRectifier
}

func (s *Service) rectifyBeforeSend(request normalizedRequest) normalizedRequest {
	cfg := s.rectifierConfig()
	if !cfg.Enabled || !cfg.UnsupportedImageFallback || !cfg.HeuristicTextOnlyModels {
		return request
	}
	body, changed := replaceImagesForTextOnlyModel(request.Body)
	if !changed {
		return request
	}
	request.Body = body
	return request
}

func (s *Service) rectifyAfterFailure(request normalizedRequest, errMsg string) (normalizedRequest, string, bool) {
	cfg := s.rectifierConfig()
	if !cfg.Enabled || strings.TrimSpace(string(request.Body)) == "" {
		return request, "", false
	}
	if cfg.ThinkingBudget && looksLikeThinkingBudgetError(errMsg) {
		if body, changed := normalizeThinkingBudget(request.Body, request.ResponseMode); changed {
			request.Body = body
			return request, "thinking budget rectifier", true
		}
	}
	if cfg.ThinkingSignature && looksLikeThinkingSignatureError(errMsg) {
		if body, changed := stripThinkingArtifacts(request.Body); changed {
			request.Body = body
			return request, "thinking signature rectifier", true
		}
	}
	if cfg.UnsupportedImageFallback && looksLikeUnsupportedImageError(errMsg) {
		if body, changed := replaceImagesWithUnsupportedMarker(request.Body); changed {
			request.Body = body
			return request, "unsupported image rectifier", true
		}
	}
	return request, "", false
}

func (s *Service) orderCandidatesForRequest(candidates []storage.UpstreamGroupKey, request normalizedRequest) []storage.UpstreamGroupKey {
	ordered := s.orderCandidatesWithRuntime(candidates)
	if s.affinities == nil || request.AffinityKey == "" {
		return ordered
	}
	affinity, err := s.affinities.Find(HashKey(request.AffinityKey), time.Now())
	if err != nil || affinity == nil || affinity.GroupKeyID == 0 {
		return ordered
	}
	for i, item := range ordered {
		if item.ID != affinity.GroupKeyID {
			continue
		}
		if statusRank(item.Status) > statusRank("unknown") {
			return ordered
		}
		out := append([]storage.UpstreamGroupKey{item}, ordered[:i]...)
		out = append(out, ordered[i+1:]...)
		return out
	}
	return ordered
}

func (s *Service) orderCandidatesWithRuntime(candidates []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	out := orderCandidates(candidates)
	sort.SliceStable(out, func(i, j int) bool {
		if rankI, rankJ := statusRank(out[i].Status), statusRank(out[j].Status); rankI != rankJ {
			return rankI < rankJ
		}
		if out[i].Ratio != out[j].Ratio {
			return out[i].Ratio < out[j].Ratio
		}
		if out[i].FailureCount != out[j].FailureCount {
			return out[i].FailureCount < out[j].FailureCount
		}
		latI, okI := s.runtimeLatency(out[i].ID)
		latJ, okJ := s.runtimeLatency(out[j].ID)
		switch {
		case okI && okJ && latI != latJ:
			return latI < latJ
		case okI != okJ:
			return okI
		default:
			return out[i].ID < out[j].ID
		}
	})
	return out
}

func (s *Service) runtimeState(id uint) *groupRuntimeState {
	actual, _ := s.runtime.LoadOrStore(id, &groupRuntimeState{})
	return actual.(*groupRuntimeState)
}

func (s *Service) runtimeDisabled(id uint) bool {
	state, ok := s.runtime.Load(id)
	if !ok {
		return false
	}
	current := state.(*groupRuntimeState)
	current.mu.Lock()
	defer current.mu.Unlock()
	return !current.disabledUntil.IsZero() && time.Now().Before(current.disabledUntil)
}

func (s *Service) runtimeLatency(id uint) (float64, bool) {
	state, ok := s.runtime.Load(id)
	if !ok {
		return 0, false
	}
	current := state.(*groupRuntimeState)
	current.mu.Lock()
	defer current.mu.Unlock()
	if current.avgLatencyMS <= 0 {
		return 0, false
	}
	return current.avgLatencyMS, true
}

func (s *Service) tryAcquireCandidate(id uint, limit int) (func(), bool) {
	if limit <= 0 {
		return func() {}, true
	}
	state := s.runtimeState(id)
	state.mu.Lock()
	if state.inFlight >= limit {
		state.mu.Unlock()
		return nil, false
	}
	state.inFlight++
	state.lastObservedAt = time.Now()
	state.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			state.mu.Lock()
			if state.inFlight > 0 {
				state.inFlight--
			}
			state.lastObservedAt = time.Now()
			state.mu.Unlock()
		})
	}, true
}

func (s *Service) recordRuntimeSuccess(id uint, duration time.Duration) {
	state := s.runtimeState(id)
	state.mu.Lock()
	defer state.mu.Unlock()
	ms := float64(duration.Milliseconds())
	if ms < 1 {
		ms = 1
	}
	if state.avgLatencyMS <= 0 {
		state.avgLatencyMS = ms
	} else {
		state.avgLatencyMS = state.avgLatencyMS*0.75 + ms*0.25
	}
	state.disabledUntil = time.Time{}
	state.lastObservedAt = time.Now()
}

func (s *Service) recordRuntimeFailure(id uint, disabledUntil time.Time) {
	state := s.runtimeState(id)
	state.mu.Lock()
	defer state.mu.Unlock()
	state.disabledUntil = disabledUntil
	state.lastObservedAt = time.Now()
}

func orderCandidates(in []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	out := append([]storage.UpstreamGroupKey(nil), in...)
	sort.SliceStable(out, func(i, j int) bool {
		if rankI, rankJ := statusRank(out[i].Status), statusRank(out[j].Status); rankI != rankJ {
			return rankI < rankJ
		}
		if out[i].Ratio != out[j].Ratio {
			return out[i].Ratio < out[j].Ratio
		}
		if out[i].FailureCount != out[j].FailureCount {
			return out[i].FailureCount < out[j].FailureCount
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func (s *Service) rememberAffinity(request normalizedRequest, responseID string, groupKeyID uint) {
	if s.affinities == nil || groupKeyID == 0 {
		return
	}
	keys := make([]string, 0, 2)
	if strings.TrimSpace(request.AffinityKey) != "" {
		keys = append(keys, request.AffinityKey)
	}
	if responseID = strings.TrimSpace(responseID); responseID != "" {
		keys = append(keys, "response:"+responseID)
	}
	if len(keys) == 0 {
		return
	}
	now := time.Now()
	expiresAt := now.Add(24 * time.Hour)
	seen := map[string]struct{}{}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if err := s.affinities.Upsert(HashKey(key), groupKeyID, expiresAt, now); err != nil && s.log != nil {
			s.log.Warn("remember gateway affinity failed", "err", err)
		}
	}
}

func writeProxyResponse(w http.ResponseWriter, status int, header http.Header, body []byte, key *storage.UpstreamGroupKey, mode string) {
	outBody := body
	outHeader := cloneHeader(header)
	if !isEventStream(outHeader) {
		switch mode {
		case "chat":
			if converted, err := responsesToChat(body); err == nil {
				outBody = converted
				outHeader.Set("Content-Type", "application/json")
			}
		case "claude":
			if converted, err := responsesToClaude(body); err == nil {
				outBody = converted
				outHeader.Set("Content-Type", "application/json")
			}
		}
	}
	copyResponseHeaders(w, outHeader, key)
	w.WriteHeader(status)
	_, _ = w.Write(outBody)
}

func responsesToChat(body []byte) ([]byte, error) {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	text := responseText(raw)
	model, _ := raw["model"].(string)
	id, _ := raw["id"].(string)
	if id == "" {
		id = "chatcmpl-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	resp := map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]any{{
			"index": 0,
			"message": map[string]any{
				"role":    "assistant",
				"content": text,
			},
			"finish_reason": "stop",
		}},
	}
	if usage, ok := raw["usage"]; ok {
		if usageMap, ok := usage.(map[string]any); ok {
			tokens := usageFromMap(usageMap)
			resp["usage"] = map[string]int64{
				"prompt_tokens":     tokens.Prompt,
				"completion_tokens": tokens.Completion,
				"total_tokens":      tokens.Total,
			}
		} else {
			resp["usage"] = usage
		}
	}
	return json.Marshal(resp)
}

func responsesToClaude(body []byte) ([]byte, error) {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	text := responseText(raw)
	model, _ := raw["model"].(string)
	id, _ := raw["id"].(string)
	if id == "" {
		id = "msg_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	usage := extractUsage(body)
	resp := map[string]any{
		"id":            id,
		"type":          "message",
		"role":          "assistant",
		"model":         model,
		"content":       []map[string]string{{"type": "text", "text": text}},
		"stop_reason":   "end_turn",
		"stop_sequence": nil,
		"usage": map[string]int64{
			"input_tokens":  usage.Prompt,
			"output_tokens": usage.Completion,
		},
	}
	return json.Marshal(resp)
}

func extractUsage(body []byte) usageTokens {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return usageTokens{}
	}
	responseID := responseIDFromMap(raw)
	usageRaw, ok := raw["usage"].(map[string]any)
	if !ok {
		return usageTokens{ResponseID: responseID}
	}
	usage := usageFromMap(usageRaw)
	usage.ResponseID = responseID
	return usage
}

func usageFromSSEData(data string) usageTokens {
	payload := strings.TrimSpace(data)
	if payload == "" || payload == "[DONE]" {
		return usageTokens{}
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(payload), &raw); err != nil {
		return usageTokens{}
	}
	responseID := responseIDFromMap(raw)
	if usageRaw, ok := raw["usage"].(map[string]any); ok {
		if usage := usageFromMap(usageRaw); usage.Total > 0 {
			usage.ResponseID = responseID
			return usage
		}
	}
	if responseRaw, ok := raw["response"].(map[string]any); ok {
		if responseID == "" {
			responseID = responseIDFromMap(responseRaw)
		}
		if usageRaw, ok := responseRaw["usage"].(map[string]any); ok {
			usage := usageFromMap(usageRaw)
			usage.ResponseID = responseID
			return usage
		}
	}
	return usageTokens{ResponseID: responseID}
}

func responseIDFromMap(raw map[string]any) string {
	for _, key := range []string{"id", "response_id"} {
		if v, ok := raw[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	if response, ok := raw["response"].(map[string]any); ok {
		return responseIDFromMap(response)
	}
	return ""
}

func isUpstreamErrorBody(body []byte) bool {
	msg := errorMessageFromJSON(body)
	return msg != ""
}

func errorMessageFromJSON(body []byte) string {
	if len(bytes.TrimSpace(body)) == 0 {
		return ""
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return ""
	}
	if msg := messageFromErrorValue(raw["error"]); msg != "" {
		return msg
	}
	if response, ok := raw["response"].(map[string]any); ok {
		status, _ := response["status"].(string)
		if strings.EqualFold(status, "failed") || strings.EqualFold(status, "incomplete") || strings.EqualFold(status, "cancelled") {
			if msg := messageFromErrorValue(response["error"]); msg != "" {
				return msg
			}
			return "upstream response " + status
		}
	}
	if success, ok := raw["success"].(bool); ok && !success {
		if msg := stringValue(raw["message"]); msg != "" {
			return msg
		}
		return "upstream returned success=false"
	}
	if code, ok := numericValue(raw["code"]); ok && code != 0 {
		if msg := stringValue(raw["message"]); msg != "" {
			return msg
		}
		return "upstream returned code " + strconv.FormatInt(code, 10)
	}
	typ, _ := raw["type"].(string)
	if strings.Contains(strings.ToLower(typ), "error") || strings.Contains(strings.ToLower(typ), "failed") {
		if msg := stringValue(raw["message"]); msg != "" {
			return msg
		}
		return "upstream stream event " + typ
	}
	return ""
}

func messageFromErrorValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	case map[string]any:
		for _, key := range []string{"message", "detail", "error", "type", "code"} {
			if msg := stringValue(v[key]); msg != "" {
				return msg
			}
		}
		return "upstream returned error"
	default:
		msg := strings.TrimSpace(fmt.Sprint(v))
		if msg == "" || msg == "<nil>" {
			return ""
		}
		return msg
	}
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case nil:
		return ""
	default:
		return strings.TrimSpace(fmt.Sprint(v))
	}
}

func numericValue(value any) (int64, bool) {
	switch v := value.(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case int:
		return int64(v), true
	case json.Number:
		n, err := v.Int64()
		return n, err == nil
	default:
		s := strings.TrimSpace(fmt.Sprint(v))
		if s == "" || s == "<nil>" {
			return 0, false
		}
		n, err := strconv.ParseInt(s, 10, 64)
		return n, err == nil
	}
}

func usageFromMap(usageRaw map[string]any) usageTokens {
	prompt := intField(usageRaw, "prompt_tokens")
	if prompt == 0 {
		prompt = intField(usageRaw, "input_tokens")
	}
	completion := intField(usageRaw, "completion_tokens")
	if completion == 0 {
		completion = intField(usageRaw, "output_tokens")
	}
	total := intField(usageRaw, "total_tokens")
	if total == 0 {
		total = prompt + completion
	}
	return usageTokens{Prompt: prompt, Completion: completion, Total: total}
}

type limitedCapture struct {
	buf bytes.Buffer
	max int
}

type flushResponseWriter struct {
	w http.ResponseWriter
}

func (f flushResponseWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	if flusher, ok := f.w.(http.Flusher); ok {
		flusher.Flush()
	}
	return n, err
}

func (c *limitedCapture) Write(p []byte) (int, error) {
	if c == nil || c.max <= 0 {
		return len(p), nil
	}
	remaining := c.max - c.buf.Len()
	if remaining > 0 {
		if len(p) > remaining {
			_, _ = c.buf.Write(p[:remaining])
		} else {
			_, _ = c.buf.Write(p)
		}
	}
	return len(p), nil
}

func (c *limitedCapture) Bytes() []byte {
	if c == nil {
		return nil
	}
	return c.buf.Bytes()
}

func streamResponsesAsChat(w http.ResponseWriter, r io.Reader) (usageTokens, error) {
	return streamResponsesAsChatEvents(w, nil, newSSEStreamReader(r))
}

func streamResponsesAsChatEvents(w http.ResponseWriter, buffered []sseEvent, reader *sseStreamReader) (usageTokens, error) {
	created := time.Now().Unix()
	id := "chatcmpl-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	model := ""
	roleSent := false
	doneSent := false
	var best usageTokens
	err := readSSEEvents(buffered, reader, func(event, data string) error {
		if data == "" || data == "[DONE]" {
			return nil
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(data), &raw); err != nil {
			return nil
		}
		if response, ok := raw["response"].(map[string]any); ok {
			if v, ok := response["id"].(string); ok && v != "" {
				id = v
				best.ResponseID = v
			}
			if v, ok := response["model"].(string); ok && v != "" {
				model = v
			}
			if usageRaw, ok := response["usage"].(map[string]any); ok {
				if usage := usageFromMap(usageRaw); usage.Total > 0 {
					best = usage
				}
			}
		}
		if v, ok := raw["response_id"].(string); ok && v != "" {
			id = v
			best.ResponseID = v
		}
		if v, ok := raw["model"].(string); ok && v != "" {
			model = v
		}
		if usageRaw, ok := raw["usage"].(map[string]any); ok {
			if usage := usageFromMap(usageRaw); usage.Total > 0 {
				best = usage
			}
		}
		typ, _ := raw["type"].(string)
		if typ == "" {
			typ = event
		}
		if typ == "response.output_text.delta" {
			if !roleSent {
				if err := writeChatStreamChunk(w, id, model, created, map[string]any{"role": "assistant"}, nil); err != nil {
					return err
				}
				roleSent = true
			}
			delta, _ := raw["delta"].(string)
			if delta != "" {
				return writeChatStreamChunk(w, id, model, created, map[string]any{"content": delta}, nil)
			}
		}
		if typ == "response.completed" {
			if !roleSent {
				if err := writeChatStreamChunk(w, id, model, created, map[string]any{"role": "assistant"}, nil); err != nil {
					return err
				}
				roleSent = true
			}
			if err := writeChatStreamChunk(w, id, model, created, map[string]any{}, "stop"); err != nil {
				return err
			}
			doneSent = true
			return writeSSEData(w, "[DONE]")
		}
		return nil
	})
	if err != nil {
		return best, err
	}
	if !doneSent {
		if !roleSent {
			if err := writeChatStreamChunk(w, id, model, created, map[string]any{"role": "assistant"}, nil); err != nil {
				return best, err
			}
		}
		if err := writeChatStreamChunk(w, id, model, created, map[string]any{}, "stop"); err != nil {
			return best, err
		}
		if err := writeSSEData(w, "[DONE]"); err != nil {
			return best, err
		}
	}
	return best, nil
}

func streamResponsesAsClaude(w http.ResponseWriter, r io.Reader) (usageTokens, error) {
	return streamResponsesAsClaudeEvents(w, nil, newSSEStreamReader(r))
}

func streamResponsesAsClaudeEvents(w http.ResponseWriter, buffered []sseEvent, reader *sseStreamReader) (usageTokens, error) {
	id := "msg_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	model := ""
	started := false
	stopped := false
	var best usageTokens
	err := readSSEEvents(buffered, reader, func(event, data string) error {
		if data == "" || data == "[DONE]" {
			return nil
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(data), &raw); err != nil {
			return nil
		}
		if response, ok := raw["response"].(map[string]any); ok {
			if v, ok := response["id"].(string); ok && v != "" {
				id = v
				best.ResponseID = v
			}
			if v, ok := response["model"].(string); ok && v != "" {
				model = v
			}
			if usageRaw, ok := response["usage"].(map[string]any); ok {
				if usage := usageFromMap(usageRaw); usage.Total > 0 {
					best = usage
				}
			}
		}
		if v, ok := raw["model"].(string); ok && v != "" {
			model = v
		}
		typ, _ := raw["type"].(string)
		if typ == "" {
			typ = event
		}
		if typ == "response.output_text.delta" {
			if !started {
				if err := writeClaudeStart(w, id, model); err != nil {
					return err
				}
				started = true
			}
			delta, _ := raw["delta"].(string)
			if delta != "" {
				return writeClaudeEvent(w, "content_block_delta", map[string]any{
					"type":  "content_block_delta",
					"index": 0,
					"delta": map[string]any{"type": "text_delta", "text": delta},
				})
			}
		}
		if typ == "response.completed" {
			if !started {
				if err := writeClaudeStart(w, id, model); err != nil {
					return err
				}
				started = true
			}
			if err := writeClaudeEvent(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": 0}); err != nil {
				return err
			}
			if err := writeClaudeEvent(w, "message_delta", map[string]any{
				"type":  "message_delta",
				"delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil},
				"usage": map[string]any{"output_tokens": best.Completion},
			}); err != nil {
				return err
			}
			stopped = true
			return writeClaudeEvent(w, "message_stop", map[string]any{"type": "message_stop"})
		}
		return nil
	})
	if err != nil {
		return best, err
	}
	if !stopped {
		if !started {
			if err := writeClaudeStart(w, id, model); err != nil {
				return best, err
			}
		}
		if err := writeClaudeEvent(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": 0}); err != nil {
			return best, err
		}
		if err := writeClaudeEvent(w, "message_stop", map[string]any{"type": "message_stop"}); err != nil {
			return best, err
		}
	}
	return best, nil
}

func readSSE(r io.Reader, emit func(event, data string) error) error {
	return readSSEEvents(nil, newSSEStreamReader(r), emit)
}

func newSSEStreamReader(r io.Reader) *sseStreamReader {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 2<<20)
	return &sseStreamReader{scanner: scanner}
}

func (r *sseStreamReader) Next() (sseEvent, error) {
	if r == nil || r.closed {
		return sseEvent{}, io.EOF
	}
	dispatch := func() (sseEvent, bool) {
		if r.data.Len() == 0 && r.event == "" {
			return sseEvent{}, false
		}
		ev := sseEvent{Event: r.event, Data: r.data.String()}
		r.event = ""
		r.data.Reset()
		return ev, true
	}
	for r.scanner.Scan() {
		line := strings.TrimRight(r.scanner.Text(), "\r")
		if line == "" {
			if ev, ok := dispatch(); ok {
				return ev, nil
			}
			continue
		}
		switch {
		case strings.HasPrefix(line, "event:"):
			r.event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			if r.data.Len() > 0 {
				r.data.WriteByte('\n')
			}
			r.data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	r.closed = true
	if err := r.scanner.Err(); err != nil {
		return sseEvent{}, err
	}
	if ev, ok := dispatch(); ok {
		return ev, nil
	}
	return sseEvent{}, io.EOF
}

func readSSEEvents(buffered []sseEvent, reader *sseStreamReader, emit func(event, data string) error) error {
	for _, ev := range buffered {
		if err := emit(ev.Event, ev.Data); err != nil {
			return err
		}
	}
	for {
		ev, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := emit(ev.Event, ev.Data); err != nil {
			return err
		}
	}
}

func preflightSSEStream(reader *sseStreamReader, closer io.Closer) ([]sseEvent, error) {
	buffered := make([]sseEvent, 0, 4)
	totalBytes := 0
	for len(buffered) < streamPreflightMaxEvents && totalBytes < streamPreflightMaxBytes {
		ev, err := readNextSSEWithTimeout(reader, closer, streamFirstEventTimeout)
		if errors.Is(err, io.EOF) {
			if len(buffered) == 0 {
				return nil, errors.New("upstream stream ended before sending data")
			}
			return buffered, nil
		}
		if err != nil {
			return nil, err
		}
		if ev.Data == "" && ev.Event == "" {
			continue
		}
		if failed, msg := streamEventFailure(ev); failed {
			return nil, errors.New(msg)
		}
		buffered = append(buffered, ev)
		totalBytes += len(ev.Event) + len(ev.Data)
		if streamEventReady(ev) {
			return buffered, nil
		}
	}
	if len(buffered) == 0 {
		return nil, errors.New("upstream stream did not send a usable event")
	}
	return buffered, nil
}

func readNextSSEWithTimeout(reader *sseStreamReader, closer io.Closer, timeout time.Duration) (sseEvent, error) {
	type result struct {
		ev  sseEvent
		err error
	}
	done := make(chan result, 1)
	go func() {
		ev, err := reader.Next()
		done <- result{ev: ev, err: err}
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case res := <-done:
		return res.ev, res.err
	case <-timer.C:
		_ = closer.Close()
		return sseEvent{}, fmt.Errorf("upstream stream did not send first event within %s", timeout)
	}
}

func streamEventReady(ev sseEvent) bool {
	typ := sseEventType(ev)
	if typ == "response.completed" || typ == "response.output_text.done" {
		return true
	}
	if strings.Contains(typ, ".delta") {
		return true
	}
	switch strings.TrimSpace(ev.Event) {
	case "response.completed", "response.output_text.delta":
		return true
	default:
		return false
	}
}

func streamEventFailure(ev sseEvent) (bool, string) {
	typ := sseEventType(ev)
	if strings.Contains(typ, "failed") || strings.Contains(typ, "error") || strings.EqualFold(strings.TrimSpace(ev.Event), "error") {
		if msg := errorMessageFromJSON([]byte(ev.Data)); msg != "" {
			return true, msg
		}
		return true, "upstream stream failed: " + firstNonEmpty(typ, ev.Event, truncateBody([]byte(ev.Data), 240))
	}
	if msg := errorMessageFromJSON([]byte(ev.Data)); msg != "" {
		return true, msg
	}
	return false, ""
}

func sseEventType(ev sseEvent) string {
	var raw map[string]any
	if err := json.Unmarshal([]byte(ev.Data), &raw); err == nil {
		if typ, _ := raw["type"].(string); strings.TrimSpace(typ) != "" {
			return strings.TrimSpace(typ)
		}
	}
	return strings.TrimSpace(ev.Event)
}

func streamRawSSE(w http.ResponseWriter, buffered []sseEvent, reader *sseStreamReader) (usageTokens, error) {
	var best usageTokens
	err := readSSEEvents(buffered, reader, func(event, data string) error {
		if usage := usageFromSSEData(data); usage.Total > 0 {
			best = usage
		}
		return writeSSEEvent(w, sseEvent{Event: event, Data: data})
	})
	return best, err
}

func writeSSEEvent(w http.ResponseWriter, ev sseEvent) error {
	if strings.TrimSpace(ev.Event) != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", ev.Event); err != nil {
			return err
		}
	}
	if ev.Data == "" {
		if _, err := fmt.Fprint(w, "\n"); err != nil {
			return err
		}
	} else {
		for _, line := range strings.Split(ev.Data, "\n") {
			if _, err := fmt.Fprintf(w, "data: %s\n", line); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprint(w, "\n"); err != nil {
			return err
		}
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}

func writeChatStreamChunk(w http.ResponseWriter, id, model string, created int64, delta map[string]any, finish any) error {
	chunk := map[string]any{
		"id":      id,
		"object":  "chat.completion.chunk",
		"created": created,
		"model":   model,
		"choices": []map[string]any{{
			"index":         0,
			"delta":         delta,
			"finish_reason": finish,
		}},
	}
	data, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	return writeSSEData(w, string(data))
}

func writeClaudeStart(w http.ResponseWriter, id, model string) error {
	if err := writeClaudeEvent(w, "message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":            id,
			"type":          "message",
			"role":          "assistant",
			"model":         model,
			"content":       []any{},
			"stop_reason":   nil,
			"stop_sequence": nil,
			"usage":         map[string]any{"input_tokens": 0, "output_tokens": 0},
		},
	}); err != nil {
		return err
	}
	return writeClaudeEvent(w, "content_block_start", map[string]any{
		"type":          "content_block_start",
		"index":         0,
		"content_block": map[string]any{"type": "text", "text": ""},
	})
}

func writeClaudeEvent(w http.ResponseWriter, event string, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
		return err
	}
	return writeSSEData(w, string(data))
}

func writeSSEData(w http.ResponseWriter, data string) error {
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}

func extractStreamUsage(body []byte) usageTokens {
	lines := strings.Split(string(body), "\n")
	var best usageTokens
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(payload), &raw); err != nil {
			continue
		}
		if usageRaw, ok := raw["usage"].(map[string]any); ok {
			if usage := usageFromMap(usageRaw); usage.Total > 0 {
				best = usage
			}
		}
		if responseRaw, ok := raw["response"].(map[string]any); ok {
			if usageRaw, ok := responseRaw["usage"].(map[string]any); ok {
				if usage := usageFromMap(usageRaw); usage.Total > 0 {
					best = usage
				}
			}
		}
	}
	return best
}

func responseText(raw map[string]any) string {
	if text, ok := raw["output_text"].(string); ok && strings.TrimSpace(text) != "" {
		return text
	}
	output, _ := raw["output"].([]any)
	var b strings.Builder
	for _, item := range output {
		obj, _ := item.(map[string]any)
		content, _ := obj["content"].([]any)
		for _, part := range content {
			p, _ := part.(map[string]any)
			if text, ok := p["text"].(string); ok {
				b.WriteString(text)
			}
		}
	}
	return b.String()
}

func enforceGatewayQuota(key *storage.GatewayKey) error {
	if key == nil {
		return errors.New("invalid gateway key")
	}
	todayTokens := key.TodayTokens
	if key.UsageDate != time.Now().Format("2006-01-02") {
		todayTokens = 0
	}
	if key.DailyLimit > 0 && todayTokens >= key.DailyLimit {
		return errors.New("gateway key daily token limit exceeded")
	}
	if key.TotalLimit > 0 && key.TotalTokens >= key.TotalLimit {
		return errors.New("gateway key total token limit exceeded")
	}
	if key.ExpiresAt != nil && time.Now().After(*key.ExpiresAt) {
		return errors.New("gateway key expired")
	}
	return nil
}

func gatewayKeyOutput(key storage.GatewayKey) GatewayKeyOutput {
	todayTokens := key.TodayTokens
	if key.UsageDate != "" && key.UsageDate != time.Now().Format("2006-01-02") {
		todayTokens = 0
	}
	return GatewayKeyOutput{
		ID:          key.ID,
		Name:        key.Name,
		KeyPrefix:   key.KeyPrefix,
		Enabled:     key.Enabled,
		DailyLimit:  key.DailyLimit,
		TotalLimit:  key.TotalLimit,
		TodayTokens: todayTokens,
		TotalTokens: key.TotalTokens,
		UsageDate:   key.UsageDate,
		ExpiresAt:   key.ExpiresAt,
		LastUsedAt:  key.LastUsedAt,
		LastUsedIP:  key.LastUsedIP,
		CreatedAt:   key.CreatedAt,
		UpdatedAt:   key.UpdatedAt,
	}
}

func randomGatewayKey() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return gatewayKeyPrefix + hex.EncodeToString(buf), nil
}

func randomOpenAIKey() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "sk-" + hex.EncodeToString(buf), nil
}

func visiblePrefix(key string) string {
	if len(key) <= 16 {
		return key
	}
	return key[:16]
}

func extractBearer(header string) string {
	header = strings.TrimSpace(header)
	if strings.HasPrefix(strings.ToLower(header), "bearer ") {
		return strings.TrimSpace(header[7:])
	}
	return header
}

func extractGatewayKey(header http.Header) string {
	if key := extractBearer(header.Get("Authorization")); key != "" {
		return key
	}
	return strings.TrimSpace(header.Get("X-Api-Key"))
}

func affinityLookupKey(body []byte) string {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return ""
	}
	if id := stringValue(raw["previous_response_id"]); id != "" {
		return "response:" + id
	}
	if id := stringValue(raw["response_id"]); id != "" {
		return "response:" + id
	}
	if id := conversationAffinityValue(raw["conversation"]); id != "" {
		return "conversation:" + id
	}
	if metadata, ok := raw["metadata"].(map[string]any); ok {
		for _, key := range []string{"conversation_id", "session_id", "thread_id", "codex_session_id"} {
			if id := stringValue(metadata[key]); id != "" {
				return "metadata:" + key + ":" + id
			}
		}
	}
	// 兜底：chat/messages 类请求（OpenAI Chat Completions、Anthropic Messages）没有
	// previous_response_id 也没有 session 元数据，此时用 (model + 前若干条 user/system 消息)
	// 的 hash 做亲和 key。同一段上文 24 小时内会落到同一个候选，保证上游 prompt 缓存命中。
	if seed := chatAffinitySeed(raw); seed != "" {
		return "chat:" + seed
	}
	return ""
}

// chatAffinitySeed 提取"稳定标识一段上文"的种子：model + 前若干条 user/system 消息的前缀。
// 只取每条 content 的前 4KB 文本、共 3 条，避免长上文里每次追加一条新消息导致 seed 变化。
func chatAffinitySeed(raw map[string]any) string {
	if raw == nil {
		return ""
	}
	model := strings.TrimSpace(fmt.Sprint(raw["model"]))
	messages, _ := raw["messages"].([]any)
	input, _ := raw["input"].([]any)
	var pool []any
	pool = append(pool, messages...)
	pool = append(pool, input...)
	if model == "" && len(pool) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(model)
	b.WriteByte('|')
	// system 消息 + 前 2 条其它消息就足以标识一段上文的起点。
	captured := 0
	systemCaptured := false
	for _, item := range pool {
		if captured >= 3 {
			break
		}
		msg, _ := item.(map[string]any)
		role := strings.ToLower(strings.TrimSpace(fmt.Sprint(msg["role"])))
		text := flattenAffinityContent(msg["content"])
		if text == "" {
			continue
		}
		if role == "system" || role == "developer" {
			if systemCaptured {
				continue
			}
			systemCaptured = true
		}
		b.WriteString(role)
		b.WriteByte(':')
		if len(text) > 4096 {
			text = text[:4096]
		}
		b.WriteString(text)
		b.WriteByte('\n')
		captured++
	}
	if instr := strings.TrimSpace(fmt.Sprint(raw["instructions"])); instr != "" {
		if len(instr) > 2048 {
			instr = instr[:2048]
		}
		b.WriteString("instructions:")
		b.WriteString(instr)
	}
	if b.Len() == 0 {
		return ""
	}
	return b.String()
}

func flattenAffinityContent(content any) string {
	switch v := content.(type) {
	case string:
		return strings.TrimSpace(v)
	case []any:
		var b strings.Builder
		for _, part := range v {
			obj, _ := part.(map[string]any)
			if obj == nil {
				continue
			}
			if text, ok := obj["text"].(string); ok {
				b.WriteString(text)
				b.WriteByte('\n')
			}
		}
		return strings.TrimSpace(b.String())
	default:
		return ""
	}
}

func conversationAffinityValue(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case map[string]any:
		for _, key := range []string{"id", "conversation_id", "thread_id"} {
			if id := stringValue(v[key]); id != "" {
				return id
			}
		}
	}
	return ""
}

func requestStream(body []byte) bool {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return false
	}
	return boolField(raw, "stream")
}

func boolField(raw map[string]any, key string) bool {
	value, ok := raw[key]
	if !ok {
		return false
	}
	b, _ := value.(bool)
	return b
}

func moveField(raw map[string]any, from, to string) {
	if value, ok := raw[from]; ok {
		if _, exists := raw[to]; !exists {
			raw[to] = value
		}
		delete(raw, from)
	}
}

func claudeSystemText(system any) string {
	switch v := system.(type) {
	case string:
		return v
	case []any:
		var b strings.Builder
		for _, item := range v {
			obj, _ := item.(map[string]any)
			if text, ok := obj["text"].(string); ok {
				if b.Len() > 0 {
					b.WriteByte('\n')
				}
				b.WriteString(text)
			}
		}
		return b.String()
	default:
		return fmt.Sprint(v)
	}
}

func normalizeClaudeMessages(messages any) any {
	items, ok := messages.([]any)
	if !ok {
		return messages
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		msg, _ := item.(map[string]any)
		role, _ := msg["role"].(string)
		if role == "" {
			role = "user"
		}
		out = append(out, map[string]any{
			"role":    role,
			"content": normalizeClaudeContent(msg["content"], role),
		})
	}
	return out
}

func normalizeClaudeContent(content any, role string) any {
	switch v := content.(type) {
	case string:
		return v
	case []any:
		out := make([]map[string]any, 0, len(v))
		textType := "input_text"
		if role == "assistant" {
			textType = "output_text"
		}
		for _, item := range v {
			part, _ := item.(map[string]any)
			if text, ok := part["text"].(string); ok {
				out = append(out, map[string]any{"type": textType, "text": text})
				continue
			}
			if image := claudeImageBlockToResponses(part); image != nil {
				out = append(out, image)
				continue
			}
		}
		return out
	default:
		return fmt.Sprint(v)
	}
}

func claudeThinkingToResponsesReasoning(value any) map[string]any {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	typ := strings.ToLower(strings.TrimSpace(fmt.Sprint(obj["type"])))
	if typ != "enabled" {
		return nil
	}
	budget, _ := numericValue(obj["budget_tokens"])
	effort := "medium"
	switch {
	case budget >= 32000:
		effort = "high"
	case budget > 0 && budget < 4096:
		effort = "low"
	}
	return map[string]any{"effort": effort}
}

func claudeImageBlockToResponses(part map[string]any) map[string]any {
	if strings.TrimSpace(fmt.Sprint(part["type"])) != "image" {
		return nil
	}
	source, _ := part["source"].(map[string]any)
	sourceType := strings.TrimSpace(fmt.Sprint(source["type"]))
	switch sourceType {
	case "base64":
		mediaType := strings.TrimSpace(fmt.Sprint(source["media_type"]))
		data := strings.TrimSpace(fmt.Sprint(source["data"]))
		if mediaType == "" || data == "" {
			return unsupportedImageTextBlock()
		}
		return map[string]any{"type": "input_image", "image_url": "data:" + mediaType + ";base64," + data}
	case "url":
		url := strings.TrimSpace(fmt.Sprint(source["url"]))
		if url == "" {
			return unsupportedImageTextBlock()
		}
		return map[string]any{"type": "input_image", "image_url": url}
	default:
		return unsupportedImageTextBlock()
	}
}

func looksLikeThinkingSignatureError(msg string) bool {
	s := strings.ToLower(msg)
	return strings.Contains(s, "thinking") &&
		(strings.Contains(s, "signature") ||
			strings.Contains(s, "redacted_thinking") ||
			strings.Contains(s, "invalid_request_error") ||
			strings.Contains(s, "incompatible"))
}

func looksLikeThinkingBudgetError(msg string) bool {
	s := strings.ToLower(msg)
	return strings.Contains(s, "thinking") &&
		(strings.Contains(s, "budget_tokens") ||
			strings.Contains(s, "at least 1024") ||
			strings.Contains(s, "max_tokens") ||
			strings.Contains(s, "token budget"))
}

func looksLikeUnsupportedImageError(msg string) bool {
	s := strings.ToLower(msg)
	return (strings.Contains(s, "image") || strings.Contains(s, "vision") || strings.Contains(s, "multimodal")) &&
		(strings.Contains(s, "unsupported") ||
			strings.Contains(s, "not support") ||
			strings.Contains(s, "does not support") ||
			strings.Contains(s, "text-only") ||
			strings.Contains(s, "text only"))
}

func replaceImagesForTextOnlyModel(body []byte) ([]byte, bool) {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return body, false
	}
	model := strings.TrimSpace(fmt.Sprint(raw["model"]))
	if !isHeuristicTextOnlyModel(model) {
		return body, false
	}
	return mutateJSONBody(body, func(v any) (any, bool) {
		return replaceImageBlocks(v)
	})
}

func isHeuristicTextOnlyModel(model string) bool {
	m := strings.ToLower(strings.TrimSpace(model))
	if m == "" {
		return false
	}
	for _, marker := range []string{
		"gpt-3.5",
		"text-davinci",
		"text-curie",
		"text-babbage",
		"text-ada",
		"claude-3-haiku-text",
	} {
		if strings.Contains(m, marker) {
			return true
		}
	}
	return false
}

func normalizeThinkingBudget(body []byte, responseMode string) ([]byte, bool) {
	return mutateJSONBody(body, func(v any) (any, bool) {
		raw, ok := v.(map[string]any)
		if !ok {
			return v, false
		}
		changed := false
		if responseMode == "responses" || responseMode == "chat" || responseMode == "claude" {
			reasoning, _ := raw["reasoning"].(map[string]any)
			if reasoning == nil {
				reasoning = map[string]any{}
			}
			if reasoning["effort"] != "high" {
				reasoning["effort"] = "high"
				raw["reasoning"] = reasoning
				changed = true
			}
			if current, ok := numericValue(raw["max_output_tokens"]); !ok || current < 64000 {
				raw["max_output_tokens"] = float64(64000)
				changed = true
			}
			delete(raw, "max_tokens")
			return raw, true
		}
		raw["thinking"] = map[string]any{
			"type":          "enabled",
			"budget_tokens": float64(32000),
		}
		changed = true
		if current, ok := numericValue(raw["max_tokens"]); !ok || current < 64000 {
			raw["max_tokens"] = float64(64000)
			changed = true
		}
		return raw, changed
	})
}

func stripThinkingArtifacts(body []byte) ([]byte, bool) {
	return mutateJSONBody(body, func(v any) (any, bool) {
		return stripThinkingValue(v)
	})
}

func replaceImagesWithUnsupportedMarker(body []byte) ([]byte, bool) {
	return mutateJSONBody(body, func(v any) (any, bool) {
		return replaceImageBlocks(v)
	})
}

func mutateJSONBody(body []byte, mutate func(any) (any, bool)) ([]byte, bool) {
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		return body, false
	}
	updated, changed := mutate(raw)
	if !changed {
		return body, false
	}
	encoded, err := json.Marshal(updated)
	if err != nil {
		return body, false
	}
	return encoded, true
}

func stripThinkingValue(v any) (any, bool) {
	switch value := v.(type) {
	case []any:
		out := make([]any, 0, len(value))
		changed := false
		for _, item := range value {
			if isThinkingBlock(item) {
				changed = true
				continue
			}
			next, itemChanged := stripThinkingValue(item)
			changed = changed || itemChanged
			out = append(out, next)
		}
		return out, changed
	case map[string]any:
		changed := false
		for key, item := range value {
			lower := strings.ToLower(key)
			if lower == "thinking" || lower == "signature" || lower == "thinking_signature" {
				delete(value, key)
				changed = true
				continue
			}
			next, itemChanged := stripThinkingValue(item)
			if itemChanged {
				value[key] = next
				changed = true
			}
		}
		return value, changed
	default:
		return v, false
	}
}

func isThinkingBlock(v any) bool {
	obj, ok := v.(map[string]any)
	if !ok {
		return false
	}
	typ := strings.ToLower(strings.TrimSpace(fmt.Sprint(obj["type"])))
	return typ == "thinking" || typ == "redacted_thinking"
}

func replaceImageBlocks(v any) (any, bool) {
	switch value := v.(type) {
	case []any:
		changed := false
		out := make([]any, 0, len(value))
		for _, item := range value {
			if isImageBlock(item) {
				out = append(out, unsupportedImageTextBlock())
				changed = true
				continue
			}
			next, itemChanged := replaceImageBlocks(item)
			changed = changed || itemChanged
			out = append(out, next)
		}
		return out, changed
	case map[string]any:
		if isImageBlock(value) {
			return unsupportedImageTextBlock(), true
		}
		changed := false
		for key, item := range value {
			next, itemChanged := replaceImageBlocks(item)
			if itemChanged {
				value[key] = next
				changed = true
			}
		}
		return value, changed
	default:
		return v, false
	}
}

func isImageBlock(v any) bool {
	obj, ok := v.(map[string]any)
	if !ok {
		return false
	}
	typ := strings.ToLower(strings.TrimSpace(fmt.Sprint(obj["type"])))
	return typ == "image" || typ == "input_image" || typ == "image_url"
}

func unsupportedImageTextBlock() map[string]any {
	return map[string]any{"type": "input_text", "text": "[Unsupported Image]"}
}

func clientIP(r *http.Request) string {
	for _, name := range []string{"X-Forwarded-For", "X-Real-IP"} {
		raw := strings.TrimSpace(r.Header.Get(name))
		if raw == "" {
			continue
		}
		if name == "X-Forwarded-For" {
			raw = strings.TrimSpace(strings.Split(raw, ",")[0])
		}
		if raw != "" {
			return raw
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func joinUpstreamURL(siteURL, path string) (string, error) {
	base, err := url.Parse(strings.TrimRight(siteURL, "/"))
	if err != nil {
		return "", err
	}
	rawQuery := ""
	if idx := strings.Index(path, "?"); idx >= 0 {
		rawQuery = path[idx+1:]
		path = path[:idx]
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	base.Path = strings.TrimRight(base.Path, "/") + path
	base.RawQuery = rawQuery
	return base.String(), nil
}

func copyRequestHeaders(dst http.Header, src http.Header) {
	for k, values := range src {
		if skipRequestHeader(k) {
			continue
		}
		for _, v := range values {
			dst.Add(k, v)
		}
	}
}

func copyResponseHeaders(out io.Writer, header http.Header, key *storage.UpstreamGroupKey) {
	rw, ok := out.(http.ResponseWriter)
	if !ok {
		return
	}
	dst := rw.Header()
	for k, values := range header {
		if skipResponseHeader(k) {
			continue
		}
		for _, v := range values {
			dst.Add(k, v)
		}
	}
	dst.Set("X-UpstreamOps-Channel", key.ChannelName)
	dst.Set("X-UpstreamOps-Group", key.GroupName)
	dst.Set("X-UpstreamOps-Ratio", strconv.FormatFloat(key.Ratio, 'f', -1, 64))
}

func cloneHeader(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, values := range h {
		out[k] = append([]string(nil), values...)
	}
	return out
}

func readLimitedBody(r io.Reader, max int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > max {
		return nil, fmt.Errorf("upstream response body exceeds %d bytes", max)
	}
	return body, nil
}

func skipRequestHeader(name string) bool {
	switch strings.ToLower(name) {
	case "authorization", "x-api-key", "host", "content-length", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func skipResponseHeader(name string) bool {
	switch strings.ToLower(name) {
	case "content-length", "content-encoding", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func isEventStream(header http.Header) bool {
	return strings.Contains(strings.ToLower(header.Get("Content-Type")), "text/event-stream")
}

func statusRank(status string) int {
	switch status {
	case "alive":
		return 0
	case "unknown":
		return 1
	case "dead":
		return 2
	default:
		return 1
	}
}

func intField(raw map[string]any, key string) int64 {
	value, ok := raw[key]
	if !ok {
		return 0
	}
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case json.Number:
		n, _ := v.Int64()
		return n
	default:
		n, _ := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(v)), 10, 64)
		return n
	}
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// shouldRetryStatus 决定收到某个 HTTP 状态码时是否切换到下一个上游候选。
//
// 采用"默认 retry"的白名单反向策略：只有极少数"客户端请求本身就有毛病"的
// 状态码（400 校验失败但不像模型问题、413 请求过大、415 媒体类型不支持、422
// 语义不通过、501 未实现）才停止 fail-over 直接把上游错误吐给调用方；其它
// 一律换下一个候选。上游账号"死掉"最常见的返回是 400/402/451/460/499 加上
// 各种自定义信息（quota_exhausted / billing / plan expired / model not available），
// 黑名单方案容易漏，白名单反着写更稳。
func shouldRetryStatus(status int) bool {
	if status >= 200 && status < 300 {
		return false
	}
	// 明确"客户端错、换 key 也没用"的少数状态码：不 fail-over。
	switch status {
	case http.StatusRequestEntityTooLarge, // 413
		http.StatusUnsupportedMediaType, // 415
		http.StatusExpectationFailed,    // 417
		http.StatusUnprocessableEntity,  // 422
		http.StatusNotImplemented:       // 501
		return false
	}
	// 其它 4xx / 5xx / 0（网络错）默认 retry。
	return true
}

func shouldRetryUpstreamStatus(status int, msg string) bool {
	if !shouldRetryStatus(status) {
		return false
	}
	// 400 特判：只有明显是"客户端参数错"（比如缺字段、格式错）才不 retry；
	// 其它 400（含模型不支持、quota 用尽、billing 拒绝等被上游写成 400 的情况）都 retry。
	if status == http.StatusBadRequest && looksLikeClientRequestError(msg) {
		return false
	}
	return true
}

// looksLikeClientRequestError 识别"客户端参数写错"类的 400，此时切换 key 也没意义。
// 保守匹配，宁可多 retry 一次，也不要把可能是上游账号死掉的 400 卡在同一个 key。
func looksLikeClientRequestError(msg string) bool {
	s := strings.ToLower(msg)
	if looksLikeUnsupportedModelError(msg) {
		return false
	}
	// 明确的"请求体本身有问题"标志：schema / json / required field 缺失。
	markers := []string{
		"invalid json",
		"invalid_request_error",
		"missing required",
		"required field",
		"required parameter",
		"schema validation",
		"decode error",
		"parse error",
		"unmarshal",
	}
	for _, m := range markers {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}

func looksLikeUnsupportedModelError(msg string) bool {
	s := strings.ToLower(msg)
	return (strings.Contains(s, "model") || strings.Contains(s, "模型") || strings.Contains(s, "channel") || strings.Contains(s, "渠道")) &&
		(strings.Contains(s, "not found") ||
			strings.Contains(s, "not support") ||
			strings.Contains(s, "does not support") ||
			strings.Contains(s, "unsupported") ||
			strings.Contains(s, "unavailable") ||
			strings.Contains(s, "not available") ||
			strings.Contains(s, "no available") ||
			strings.Contains(s, "不存在") ||
			strings.Contains(s, "不支持") ||
			strings.Contains(s, "不可用") ||
			strings.Contains(s, "无可用"))
}

func truncateBody(body []byte, max int) string {
	s := strings.TrimSpace(string(body))
	if s == "" {
		return http.StatusText(http.StatusBadGateway)
	}
	rs := []rune(s)
	if len(rs) <= max {
		return s
	}
	return string(rs[:max]) + "..."
}

func jsonError(message string) []byte {
	body, _ := json.Marshal(map[string]any{
		"error": map[string]string{
			"message": message,
			"type":    "upstream_ops_gateway_error",
		},
	})
	return body
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func normalizedRatio(v float64) float64 {
	if v <= 0 {
		return 1
	}
	return v
}
