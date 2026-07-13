package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
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
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/channel"
	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/connector"
	appcrypto "github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/progress"
	"github.com/bejix/upstream-ops/backend/storage"
)

const (
	gatewayKeyPrefix = "sk-"
	healthPath       = "/v1/models"
	responsesPath    = "/v1/responses"

	proxyAttemptTimeout       = 60 * time.Second
	healthProbeTimeout        = 15 * time.Second
	healthProbeRetryJitterMax = 15 * time.Second
	// streamFirstEventTimeout 是"等上游吐出第一个有效 SSE 事件"的最长等待。
	// Codex / o1 / o3 这类带 reasoning 的请求可能长时间没有可见文本；部分
	// 中转站还会缓冲 response.created。这里给到 5 分钟，避免在上游仍在推理时
	// 主动 Close，导致客户端报 "stream closed before response.completed"。
	streamFirstEventTimeout = 5 * time.Minute
	// streamIdleTimeout 是正式转发阶段"两个事件之间"的最长间隔。推理模型两次事件
	// 之间可能有较长停顿；超过后才认为上游卡死，并由 Responses 兜底逻辑补终态/[DONE]。
	streamIdleTimeout              = 5 * time.Minute
	streamHeartbeatInterval        = 15 * time.Second
	streamPreflightMaxEvents       = 16
	streamPreflightMaxBytes        = 64 << 10
	proxyTransientFailureThreshold = 3
	proxyTransientFailureCooldown  = 45 * time.Second
	proxyServerErrorCooldown       = 60 * time.Second
	proxyTimeoutCooldown           = 75 * time.Second
	proxyNetworkErrorCooldown      = 30 * time.Second
	proxyRateLimitCooldown         = 90 * time.Second
	proxyPermanentFailureCooldown  = 30 * time.Minute
	defaultHealthProbeBatchSize    = 30
)

var errResponsesStreamTerminal = errors.New("responses stream terminal event emitted")

type Service struct {
	channels   *storage.Channels
	gateway    *storage.GatewayKeys
	affinities *storage.GatewayAffinities
	groupKeys  *storage.UpstreamGroupKeys
	usageLogs  *storage.UsageLogs
	ipPolicies *storage.IPPolicies
	cipher     *appcrypto.Cipher
	channelSvc *channel.Service
	log        *slog.Logger
	clients    sync.Map
	runtime    sync.Map
	keyRuntime sync.Map
	ipRuntime  sync.Map
	configMu   sync.RWMutex
	upstream   config.UpstreamConfig
}

type CreateGatewayKeyInput struct {
	Name              string  `json:"name"`
	ClientFormat      string  `json:"client_format"`
	AllowedGroupScope string  `json:"allowed_group_scope"`
	AllowedGroupIDs   []uint  `json:"allowed_group_ids"`
	DailyLimit        int64   `json:"daily_limit"`
	TotalLimit        int64   `json:"total_limit"`
	CostPerMillion    float64 `json:"cost_per_million"`
	BalanceLimit      float64 `json:"balance_limit"`
	ConcurrencyLimit  int     `json:"concurrency_limit"`
	ExpiresInDays     int     `json:"expires_in_days"`
}

type UpdateGatewayKeyInput struct {
	Name              *string    `json:"name"`
	Enabled           *bool      `json:"enabled"`
	ClientFormat      *string    `json:"client_format"`
	AllowedGroupScope *string    `json:"allowed_group_scope"`
	AllowedGroupIDs   []uint     `json:"allowed_group_ids"`
	DailyLimit        *int64     `json:"daily_limit"`
	TotalLimit        *int64     `json:"total_limit"`
	CostPerMillion    *float64   `json:"cost_per_million"`
	BalanceLimit      *float64   `json:"balance_limit"`
	ConcurrencyLimit  *int       `json:"concurrency_limit"`
	ExpiresInDays     *int       `json:"expires_in_days"`
	ExpiresAt         *time.Time `json:"expires_at"`
}

type IPPolicyInput struct {
	IP                      string `json:"ip"`
	Blocked                 bool   `json:"blocked"`
	PublicConcurrencyExempt bool   `json:"public_concurrency_exempt"`
	Note                    string `json:"note"`
}

type GatewayKeyOutput struct {
	ID                 uint       `json:"id"`
	Name               string     `json:"name"`
	KeyPrefix          string     `json:"key_prefix"`
	Key                string     `json:"key,omitempty"`
	Enabled            bool       `json:"enabled"`
	ClientFormat       string     `json:"client_format"`
	AllowedGroupScope  string     `json:"allowed_group_scope"`
	AllowedGroupIDs    []uint     `json:"allowed_group_ids,omitempty"`
	DailyLimit         int64      `json:"daily_limit"`
	TotalLimit         int64      `json:"total_limit"`
	TodayTokens        int64      `json:"today_tokens"`
	TotalTokens        int64      `json:"total_tokens"`
	TodayPromptTokens  int64      `json:"today_prompt_tokens"`
	TotalPromptTokens  int64      `json:"total_prompt_tokens"`
	TodayCachedTokens  int64      `json:"today_cached_tokens"`
	TotalCachedTokens  int64      `json:"total_cached_tokens"`
	TodayCacheHitRate  float64    `json:"today_cache_hit_rate"`
	TotalCacheHitRate  float64    `json:"total_cache_hit_rate"`
	CostPerMillion     float64    `json:"cost_per_million"`
	BalanceLimit       float64    `json:"balance_limit"`
	ConcurrencyLimit   int        `json:"concurrency_limit"`
	BalanceRemaining   float64    `json:"balance_remaining"`
	TodayCost          float64    `json:"today_cost"`
	TotalCost          float64    `json:"total_cost"`
	UsageDate          string     `json:"usage_date,omitempty"`
	ExpiresAt          *time.Time `json:"expires_at,omitempty"`
	IsPublic           bool       `json:"is_public"`
	PublicName         string     `json:"public_name,omitempty"`
	PublicPasswordHint string     `json:"public_password_hint,omitempty"`
	LastUsedAt         *time.Time `json:"last_used_at,omitempty"`
	LastUsedIP         string     `json:"last_used_ip,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

type GatewayKeyUsageOutput struct {
	ID                uint    `json:"id"`
	Name              string  `json:"name"`
	KeyPrefix         string  `json:"key_prefix"`
	TodayTokens       int64   `json:"today_tokens"`
	TodayCost         float64 `json:"today_cost"`
	TotalTokens       int64   `json:"total_tokens"`
	TotalCost         float64 `json:"total_cost"`
	TodayPromptTokens int64   `json:"today_prompt_tokens"`
	TotalPromptTokens int64   `json:"total_prompt_tokens"`
	TodayCachedTokens int64   `json:"today_cached_tokens"`
	TotalCachedTokens int64   `json:"total_cached_tokens"`
	TodayCacheHitRate float64 `json:"today_cache_hit_rate"`
	TotalCacheHitRate float64 `json:"total_cache_hit_rate"`
	CostPerMillion    float64 `json:"cost_per_million"`
	BalanceLimit      float64 `json:"balance_limit"`
	BalanceRemaining  float64 `json:"balance_remaining"`
	UsageDate         string  `json:"usage_date,omitempty"`
}

type ConfigurePublicGatewayKeyInput struct {
	GatewayKeyID uint    `json:"gateway_key_id"`
	Enabled      bool    `json:"enabled"`
	Name         string  `json:"name"`
	Password     *string `json:"password"`
	PasswordHint string  `json:"password_hint"`
}

type PublicGatewayKeyOutput struct {
	ID                uint       `json:"id"`
	Enabled           bool       `json:"enabled"`
	Name              string     `json:"name"`
	KeyPrefix         string     `json:"key_prefix"`
	MaskedKey         string     `json:"masked_key,omitempty"`
	PasswordRequired  bool       `json:"password_required"`
	PasswordHint      string     `json:"password_hint,omitempty"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	TodayTokens       int64      `json:"today_tokens"`
	TotalTokens       int64      `json:"total_tokens"`
	TodayPromptTokens int64      `json:"today_prompt_tokens"`
	TotalPromptTokens int64      `json:"total_prompt_tokens"`
	TodayCachedTokens int64      `json:"today_cached_tokens"`
	TotalCachedTokens int64      `json:"total_cached_tokens"`
	TodayCacheHitRate float64    `json:"today_cache_hit_rate"`
	TotalCacheHitRate float64    `json:"total_cache_hit_rate"`
	LastUsedAt        *time.Time `json:"last_used_at,omitempty"`
}

type BootstrapResult struct {
	Created int             `json:"created"`
	Updated int             `json:"updated"`
	Skipped int             `json:"skipped"`
	Failed  int             `json:"failed"`
	Removed int             `json:"removed"`
	Items   []BootstrapItem `json:"items"`
}

type BootstrapItem struct {
	ChannelID   uint    `json:"channel_id"`
	ChannelName string  `json:"channel_name"`
	GroupRef    string  `json:"group_ref"`
	GroupName   string  `json:"group_name"`
	Ratio       float64 `json:"ratio"`
	Created     bool    `json:"created"`
	Removed     bool    `json:"removed,omitempty"`
	Error       string  `json:"error,omitempty"`
}

// These groups are for image/blocked routes and must never be pulled into the
// text gateway automatically. They can still be added manually if needed.
var bootstrapKeyBlockKeywords = []string{"图", "img", "im2", "ban"}

func blockedBootstrapKeyKeyword(ch storage.Channel, group connector.APIKeyGroup) (string, bool) {
	text := strings.ToLower(strings.Join([]string{
		ch.Name,
		group.Name,
		group.Description,
	}, " "))
	for _, keyword := range bootstrapKeyBlockKeywords {
		if strings.Contains(text, strings.ToLower(keyword)) {
			return keyword, true
		}
	}
	return "", false
}

type HealthResult struct {
	Total          int                `json:"total"`
	Checked        int                `json:"checked"`
	Alive          int                `json:"alive"`
	Dead           int                `json:"dead"`
	ZeroBalance    int                `json:"zero_balance"`
	RateLimited    int                `json:"rate_limited"`
	Forbidden      int                `json:"forbidden"`
	NonGeneration  int                `json:"non_generation"`
	AuthFailed     int                `json:"auth_failed"`
	Timeout        int                `json:"timeout"`
	NetworkError   int                `json:"network_error"`
	UpstreamError  int                `json:"upstream_error"`
	ModelError     int                `json:"model_error"`
	InvalidRequest int                `json:"invalid_request"`
	ServerError    int                `json:"server_error"`
	BatchSize      int                `json:"batch_size"`
	Batches        int                `json:"batches"`
	Items          []HealthResultItem `json:"items"`
}

type HealthResultItem struct {
	ID          uint       `json:"id"`
	ChannelID   uint       `json:"channel_id"`
	ChannelName string     `json:"channel_name"`
	GroupRef    string     `json:"group_ref"`
	GroupName   string     `json:"group_name"`
	Ratio       float64    `json:"ratio"`
	Status      string     `json:"status"`
	ErrorType   string     `json:"error_type,omitempty"`
	LatencyMS   int64      `json:"latency_ms"`
	Error       string     `json:"error,omitempty"`
	CheckedAt   *time.Time `json:"checked_at,omitempty"`
	Batch       int        `json:"batch,omitempty"`
}

type HealthTestOptions struct {
	BatchSize int
	GroupIDs  []uint
}

type UpdateGroupKeyInput struct {
	ConcurrencyLimit *int    `json:"concurrency_limit"`
	Enabled          *bool   `json:"enabled"`
	RequestMode      *string `json:"request_mode"`
	Priority         *int    `json:"priority"`
	ClientFormat     *string `json:"client_format"`
	Charity          *bool   `json:"charity"`
}

type normalizedRequest struct {
	Method       string
	Path         string
	Header       http.Header
	Body         []byte
	RequestModel string
	ResponseMode string
	Stream       bool
	AffinityKey  string
	ClientIP     string
	AltPath      string
	AltBody      []byte
	AltMode      string
	AltStream    bool
}

type usageTokens struct {
	Prompt       int64
	Completion   int64
	Total        int64
	Cached       int64
	Model        string
	ResponseID   string
	SoftFailure  string
	Status       string
	FirstTokenMS int64
	DurationMS   int64
}

type groupRuntimeState struct {
	mu             sync.Mutex
	disabledUntil  time.Time
	avgLatencyMS   float64
	inFlight       int
	lastObservedAt time.Time
}

type keyRuntimeState struct {
	mu             sync.Mutex
	inFlight       int
	queue          []chan struct{}
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
	// closer/idleTimeout 可选：设置后，正式转发阶段每次读事件都带这个 idle 超时，
	// 避免上游中途卡住导致 reader.Next() 无限阻塞、客户端超时断流。
	closer            io.Closer
	idleTimeout       time.Duration
	heartbeatInterval time.Duration
	heartbeat         func() error
}

type timingResponseWriter struct {
	http.ResponseWriter
	start        time.Time
	firstTokenMS atomic.Int64
	wrote        atomic.Bool
}

func (w *timingResponseWriter) MarkFirstToken() {
	if w == nil {
		return
	}
	ms := time.Since(w.start).Milliseconds()
	if ms < 1 {
		ms = 1
	}
	w.firstTokenMS.CompareAndSwap(0, ms)
}

func (w *timingResponseWriter) FirstTokenMS() int64 {
	if w == nil {
		return 0
	}
	return w.firstTokenMS.Load()
}

func (w *timingResponseWriter) Started() bool {
	if w == nil {
		return false
	}
	return w.wrote.Load()
}

func (w *timingResponseWriter) WriteHeader(status int) {
	if w == nil || w.ResponseWriter == nil {
		return
	}
	w.wrote.Store(true)
	w.ResponseWriter.WriteHeader(status)
}

func (w *timingResponseWriter) Write(p []byte) (int, error) {
	if w == nil || w.ResponseWriter == nil {
		return 0, http.ErrAbortHandler
	}
	w.wrote.Store(true)
	return w.ResponseWriter.Write(p)
}

func (w *timingResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

type responseStartChecker interface {
	Started() bool
}

type responseWrittenChecker interface {
	Written() bool
}

func responseWriterStarted(w http.ResponseWriter) bool {
	if checker, ok := w.(responseStartChecker); ok {
		return checker.Started()
	}
	if checker, ok := w.(responseWrittenChecker); ok {
		return checker.Written()
	}
	return false
}

type firstTokenMarker interface {
	MarkFirstToken()
}

func markFirstToken(w http.ResponseWriter) {
	if marker, ok := w.(firstTokenMarker); ok {
		marker.MarkFirstToken()
	}
}

type GatewayError struct {
	Status int
	Body   []byte
	Header http.Header
}

const (
	publicGatewayQuotaExhaustedMessage = "key的额度已经用光等待重置"
	publicGatewayExpiredMessage        = "key已经过期，等待新key发布，请关注网站首页"
)

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

// SetUsageLogs 注入使用记录仓库（可选）。为空时不记录，功能降级但不影响主流程。
func (s *Service) SetUsageLogs(logs *storage.UsageLogs) {
	s.usageLogs = logs
}

func (s *Service) SetIPPolicies(policies *storage.IPPolicies) {
	s.ipPolicies = policies
}

func (s *Service) ListIPPolicies() ([]storage.IPPolicy, error) {
	if s.ipPolicies == nil {
		return []storage.IPPolicy{}, nil
	}
	return s.ipPolicies.List()
}

func (s *Service) UpdateIPPolicy(ip string, blocked, publicConcurrencyExempt bool, note string) (*storage.IPPolicy, error) {
	ip = strings.TrimSpace(ip)
	if net.ParseIP(ip) == nil {
		return nil, errors.New("invalid IP address")
	}
	if s.ipPolicies == nil {
		return nil, errors.New("IP policy store is unavailable")
	}
	item := &storage.IPPolicy{IP: ip, Blocked: blocked, PublicConcurrencyExempt: publicConcurrencyExempt, Note: strings.TrimSpace(note)}
	if err := s.ipPolicies.Upsert(item); err != nil {
		return nil, err
	}
	return s.ipPolicies.Find(ip)
}

func (s *Service) DeleteIPPolicy(ip string) error {
	if s.ipPolicies == nil {
		return nil
	}
	return s.ipPolicies.Delete(ip)
}

// modelFromRequestBody 从请求体里取 model 字段，用于使用记录展示。
func modelFromRequestBody(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return ""
	}
	if m, ok := raw["model"].(string); ok {
		return strings.TrimSpace(m)
	}
	return ""
}

func usageLogModel(request normalizedRequest, usage usageTokens) string {
	for _, model := range []string{request.RequestModel, usage.Model, modelFromRequestBody(request.Body)} {
		if trimmed := strings.TrimSpace(model); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// ListUsageLogs 分页返回使用记录。
func (s *Service) ListUsageLogs(limit, offset int) ([]storage.UsageLog, int64, error) {
	if s.usageLogs == nil {
		return []storage.UsageLog{}, 0, nil
	}
	return s.usageLogs.List(limit, offset)
}

// ClearUsageLogs 删除请求明细日志，但保留 GatewayKey 上的当日/累计用量统计。
func (s *Service) ClearUsageLogs() (int64, error) {
	if s.usageLogs == nil {
		return 0, nil
	}
	return s.usageLogs.Clear()
}

// recordUsageLog 在请求成功后异步写一条使用记录。失败只记 warn，绝不影响主请求。
func (s *Service) recordUsageLog(gatewayKey *storage.GatewayKey, candidate *storage.UpstreamGroupKey, model, requestIP string, usage usageTokens) {
	if s.usageLogs == nil || candidate == nil {
		return
	}
	entry := &storage.UsageLog{
		ChannelID:        candidate.ChannelID,
		ChannelName:      candidate.ChannelName,
		GroupName:        candidate.GroupName,
		Model:            model,
		ClientFormat:     candidate.ClientFormat,
		PromptTokens:     usage.Prompt,
		CompletionTokens: usage.Completion,
		TotalTokens:      usage.Total,
		CachedTokens:     usage.Cached,
		Ratio:            candidate.Ratio,
		Status:           usageStatus(usage),
		FirstTokenMS:     maxInt64(0, usage.FirstTokenMS),
		DurationMS:       maxInt64(0, usage.DurationMS),
		RequestIP:        strings.TrimSpace(requestIP),
	}
	if gatewayKey != nil {
		entry.GatewayKeyID = gatewayKey.ID
		entry.GatewayKeyName = gatewayKey.Name
	}
	if err := s.usageLogs.Add(entry); err != nil && s.log != nil {
		s.log.Warn("record usage log failed", "err", err)
	}
}

func gatewayUsageCost(usage usageTokens, candidate *storage.UpstreamGroupKey) float64 {
	if candidate == nil {
		return 0
	}
	promptTokens := usage.Prompt
	if promptTokens < 0 {
		promptTokens = 0
	}
	completionTokens := usage.Completion
	if completionTokens < 0 {
		completionTokens = 0
	}
	totalTokens := usage.Total
	if totalTokens <= 0 {
		totalTokens = promptTokens + completionTokens
	}
	if promptTokens+completionTokens <= 0 && totalTokens > 0 {
		promptTokens = totalTokens
	}
	inputPrice := candidate.InputPricePerMillion
	if inputPrice <= 0 {
		inputPrice = storage.DefaultInputPricePerMillion
	}
	outputPrice := candidate.OutputPricePerMillion
	if outputPrice <= 0 {
		outputPrice = storage.DefaultOutputPricePerMillion
	}
	ratio := normalizedRatio(candidate.Ratio)
	return (float64(promptTokens)*inputPrice + float64(completionTokens)*outputPrice) * ratio / 1_000_000
}

func usageStatus(usage usageTokens) string {
	if status := strings.TrimSpace(usage.Status); status != "" {
		return status
	}
	if strings.TrimSpace(usage.SoftFailure) != "" {
		return "interrupted"
	}
	return "success"
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

const (
	gatewayGroupScopeAll      = "all"
	gatewayGroupScopeSelected = "selected"
	gatewayGroupScopeCharity  = "charity"
	gatewayGroupScopeNormal   = "normal"
)

func normalizeGatewayGroupScope(scope string, ids []uint) string {
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case gatewayGroupScopeSelected:
		return gatewayGroupScopeSelected
	case gatewayGroupScopeCharity:
		return gatewayGroupScopeCharity
	case gatewayGroupScopeNormal, "non_charity", "non-charity", "paid":
		return gatewayGroupScopeNormal
	case gatewayGroupScopeAll:
		return gatewayGroupScopeAll
	}
	if len(ids) > 0 {
		return gatewayGroupScopeSelected
	}
	return gatewayGroupScopeAll
}

func normalizeGatewayGroupSelection(scope string, ids []uint) (string, string) {
	normalized := normalizeGatewayGroupScope(scope, ids)
	if normalized != gatewayGroupScopeSelected {
		return normalized, ""
	}
	return normalized, encodeUintList(ids)
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
	balanceLimit := math.Max(0, input.BalanceLimit)
	concurrencyLimit := input.ConcurrencyLimit
	if concurrencyLimit < 0 {
		concurrencyLimit = 0
	}
	scope, allowedGroupIDs := normalizeGatewayGroupSelection(input.AllowedGroupScope, input.AllowedGroupIDs)
	rec := &storage.GatewayKey{
		Name:              name,
		KeyPrefix:         visiblePrefix(key),
		KeyHash:           HashKey(key),
		KeyCipher:         ciphertext,
		Enabled:           true,
		ClientFormat:      normalizeClientFormat(input.ClientFormat),
		AllowedGroupScope: scope,
		AllowedGroupIDs:   allowedGroupIDs,
		DailyLimit:        maxInt64(0, input.DailyLimit),
		TotalLimit:        maxInt64(0, input.TotalLimit),
		CostPerMillion:    math.Max(0, input.CostPerMillion),
		BalanceLimit:      balanceLimit,
		ConcurrencyLimit:  concurrencyLimit,
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

func (s *Service) GatewayKeyUsage(id uint) (*GatewayKeyUsageOutput, error) {
	key, err := s.gateway.FindByID(id)
	if err != nil {
		return nil, err
	}
	out := gatewayKeyUsageOutput(*key)
	return &out, nil
}

func (s *Service) FindGatewayKeyByRaw(raw string) (*GatewayKeyOutput, error) {
	key := strings.TrimSpace(raw)
	if key == "" {
		return nil, nil
	}
	rec, err := s.gateway.FindEnabledByHash(HashKey(key))
	if err != nil || rec == nil {
		return nil, err
	}
	out := gatewayKeyOutput(*rec)
	return &out, nil
}

func (s *Service) UpdateGatewayKey(id uint, input UpdateGatewayKeyInput) (*GatewayKeyOutput, error) {
	key, err := s.gateway.FindByID(id)
	if err != nil {
		return nil, err
	}
	if input.Name != nil {
		name := strings.TrimSpace(*input.Name)
		if name == "" {
			name = "default"
		}
		key.Name = name
	}
	if input.Enabled != nil {
		key.Enabled = *input.Enabled
	}
	if input.ClientFormat != nil {
		key.ClientFormat = normalizeClientFormat(*input.ClientFormat)
	}
	if input.AllowedGroupIDs != nil {
		key.AllowedGroupIDs = encodeUintList(input.AllowedGroupIDs)
		if input.AllowedGroupScope == nil {
			key.AllowedGroupScope = normalizeGatewayGroupScope("", input.AllowedGroupIDs)
			if key.AllowedGroupScope != gatewayGroupScopeSelected {
				key.AllowedGroupIDs = ""
			}
		}
	}
	if input.AllowedGroupScope != nil {
		ids := decodeUintList(key.AllowedGroupIDs)
		if input.AllowedGroupIDs != nil {
			ids = input.AllowedGroupIDs
		}
		key.AllowedGroupScope, key.AllowedGroupIDs = normalizeGatewayGroupSelection(*input.AllowedGroupScope, ids)
	}
	if input.DailyLimit != nil {
		key.DailyLimit = maxInt64(0, *input.DailyLimit)
	}
	if input.TotalLimit != nil {
		key.TotalLimit = maxInt64(0, *input.TotalLimit)
	}
	if input.CostPerMillion != nil {
		key.CostPerMillion = math.Max(0, *input.CostPerMillion)
	}
	if input.BalanceLimit != nil {
		key.BalanceLimit = math.Max(0, *input.BalanceLimit)
		if key.BalanceLimit > 0 && key.TotalCost >= key.BalanceLimit {
			key.Enabled = false
		}
	}
	if input.ConcurrencyLimit != nil {
		key.ConcurrencyLimit = *input.ConcurrencyLimit
		if key.ConcurrencyLimit < 0 {
			key.ConcurrencyLimit = 0
		}
	}
	if input.ExpiresInDays != nil {
		days := *input.ExpiresInDays
		if days > 0 {
			expiresAt := time.Now().AddDate(0, 0, days)
			key.ExpiresAt = &expiresAt
		} else {
			key.ExpiresAt = nil
		}
	} else if input.ExpiresAt != nil {
		if input.ExpiresAt.IsZero() {
			key.ExpiresAt = nil
		} else {
			key.ExpiresAt = input.ExpiresAt
		}
	}
	if err := s.gateway.Update(key); err != nil {
		return nil, err
	}
	out := gatewayKeyOutput(*key)
	return &out, nil
}

func (s *Service) RevealGatewayKey(id uint) (string, error) {
	key, err := s.gateway.FindByID(id)
	if err != nil {
		return "", err
	}
	return s.cipher.Decrypt(key.KeyCipher)
}

func (s *Service) GetPublicGatewayKey() (*PublicGatewayKeyOutput, error) {
	key, err := s.gateway.FindPublic()
	if err != nil || key == nil {
		return nil, err
	}
	return s.publicGatewayKeyOutput(key), nil
}

func (s *Service) ConfigurePublicGatewayKey(input ConfigurePublicGatewayKeyInput) (*PublicGatewayKeyOutput, error) {
	if input.GatewayKeyID == 0 {
		return nil, errors.New("请选择一个调用 key")
	}
	key, err := s.gateway.FindByID(input.GatewayKeyID)
	if err != nil {
		return nil, err
	}
	if input.Enabled && !key.Enabled {
		return nil, errors.New("不能将已停用的调用 key 设置为公益 key")
	}
	key.IsPublic = input.Enabled
	key.PublicName = strings.TrimSpace(input.Name)
	key.PublicPasswordHint = strings.TrimSpace(input.PasswordHint)
	if input.Password != nil {
		if *input.Password == "" {
			key.PublicPasswordCipher = ""
		} else {
			ciphertext, err := s.cipher.Encrypt(*input.Password)
			if err != nil {
				return nil, err
			}
			key.PublicPasswordCipher = ciphertext
		}
	}
	if !input.Enabled {
		key.PublicName = ""
		key.PublicPasswordHint = ""
		key.PublicPasswordCipher = ""
		if err := s.gateway.Update(key); err != nil {
			return nil, err
		}
		return s.publicGatewayKeyOutput(key), nil
	}
	if err := s.gateway.SetPublic(key); err != nil {
		return nil, err
	}
	return s.publicGatewayKeyOutput(key), nil
}

func (s *Service) ResetPublicGatewayKeyVerification() (*PublicGatewayKeyOutput, error) {
	key, err := s.gateway.FindPublic()
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, errors.New("暂无已配置的公益 key")
	}
	if err := s.gateway.ResetPublicVerification(key.ID); err != nil {
		return nil, err
	}
	key.PublicPasswordCipher = ""
	key.PublicPasswordHint = ""
	return s.publicGatewayKeyOutput(key), nil
}

func (s *Service) RevealPublicGatewayKey(password string) (string, *PublicGatewayKeyOutput, error) {
	key, err := s.gateway.FindPublic()
	if err != nil || key == nil || !key.Enabled {
		return "", nil, errors.New("public key is not available")
	}
	if key.ExpiresAt != nil && time.Now().After(*key.ExpiresAt) {
		return "", nil, errors.New("public key expired")
	}
	if key.PublicPasswordCipher != "" {
		expected, err := s.cipher.Decrypt(key.PublicPasswordCipher)
		if err != nil {
			return "", nil, err
		}
		if subtle.ConstantTimeCompare([]byte(password), []byte(expected)) != 1 {
			return "", nil, errors.New("public key password mismatch")
		}
	}
	raw, err := s.cipher.Decrypt(key.KeyCipher)
	if err != nil {
		return "", nil, err
	}
	return raw, s.publicGatewayKeyOutput(key), nil
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
	if rec.BalanceLimit > 0 && rec.TotalCost >= rec.BalanceLimit {
		_ = s.gateway.Disable(rec.ID)
		return nil, errors.New("gateway key balance exhausted")
	}
	_ = s.gateway.Touch(rec.ID, ip)
	return rec, nil
}

func (s *Service) ListGroupKeys() ([]storage.UpstreamGroupKey, error) {
	return s.groupKeys.List()
}

func (s *Service) ListGroupKeysPage(limit, offset int, search string) ([]storage.UpstreamGroupKey, int64, error) {
	return s.groupKeys.ListPage(limit, offset, search)
}

func (s *Service) GroupKeyCounts() (storage.UpstreamGroupKeyCounts, error) {
	return s.groupKeys.Counts()
}

func (s *Service) UpdateGroupKey(id uint, input UpdateGroupKeyInput) (*storage.UpstreamGroupKey, error) {
	if input.ConcurrencyLimit != nil {
		limit := *input.ConcurrencyLimit
		if limit < 0 {
			limit = 0
		}
		if err := s.groupKeys.UpdateConcurrencyLimit(id, limit); err != nil {
			return nil, err
		}
	}
	if input.Enabled != nil {
		if err := s.groupKeys.UpdateEnabled(id, *input.Enabled); err != nil {
			return nil, err
		}
		s.clearRuntimeDisable(id)
	}
	if input.RequestMode != nil {
		if err := s.groupKeys.UpdateRequestMode(id, normalizeUpstreamRequestMode(*input.RequestMode)); err != nil {
			return nil, err
		}
	}
	if input.Priority != nil {
		priority := *input.Priority
		if priority < 0 {
			priority = 0
		}
		if err := s.groupKeys.UpdatePriority(id, priority); err != nil {
			return nil, err
		}
	}
	if input.ClientFormat != nil {
		if err := s.groupKeys.UpdateClientFormat(id, normalizeClientFormat(*input.ClientFormat)); err != nil {
			return nil, err
		}
	}
	if input.Charity != nil {
		if err := s.groupKeys.UpdateCharity(id, *input.Charity); err != nil {
			return nil, err
		}
	}
	return s.groupKeys.FindByID(id)
}

// ManualGroupKeyInput 是"手动添加渠道分组"的入参：不登录上游，直接填分组名 + key。
// 用于那些无法登录、只能拿到 key 的上游。
type ManualGroupKeyInput struct {
	ChannelID    uint    `json:"channel_id"`   // 绑定到已有渠道（可选，二选一）
	SiteURL      string  `json:"site_url"`     // 或直接给上游地址，自动建一个 manual 渠道
	ChannelName  string  `json:"channel_name"` // manual 渠道名
	GroupName    string  `json:"group_name"`   // 分组名（必填）
	GroupDesc    string  `json:"group_description"`
	Key          string  `json:"key"`           // 上游 key 明文（必填，存库前加密）
	Ratio        float64 `json:"ratio"`         // 倍率
	ClientFormat string  `json:"client_format"` // openai / claude
	RequestMode  string  `json:"request_mode"`  // responses / chat
	Charity      bool    `json:"charity"`
	Priority     int     `json:"priority"`
}

// CreateManualGroupKey 手动创建一个上游分组密钥，不经过登录/自动同步。
func (s *Service) CreateManualGroupKey(ctx context.Context, input ManualGroupKeyInput) (*storage.UpstreamGroupKey, error) {
	groupName := strings.TrimSpace(input.GroupName)
	rawKey := sanitizeManualSecret(input.Key)
	if groupName == "" {
		return nil, errors.New("分组名称不能为空")
	}
	if rawKey == "" {
		return nil, errors.New("上游 key 不能为空")
	}

	// 解析/创建目标渠道。
	var ch *storage.Channel
	if input.ChannelID > 0 {
		found, err := s.channels.FindByID(input.ChannelID)
		if err != nil {
			return nil, fmt.Errorf("渠道不存在: %w", err)
		}
		ch = found
	} else {
		siteURL, err := normalizeManualAPIBaseURL(input.SiteURL)
		if err != nil {
			return nil, err
		}
		if siteURL == "" {
			return nil, errors.New("请选择已有渠道或填写上游地址")
		}
		name := strings.TrimSpace(input.ChannelName)
		if name == "" {
			name = siteURL
		}
		// 建一个"手动"渠道：token 凭据模式 + 关闭监控（不登录、不自动扫描），仅承载手动分组。
		newCh := &storage.Channel{
			Name:           name,
			Type:           storage.ChannelTypeSub2API,
			SiteURL:        siteURL,
			Username:       "manual",
			CredentialMode: storage.CredentialModeToken,
			MonitorEnabled: true,
		}
		if err := s.channels.Create(newCh); err != nil {
			return nil, fmt.Errorf("创建渠道失败: %w", err)
		}
		ch = newCh
	}

	cipher, err := s.cipher.Encrypt(rawKey)
	if err != nil {
		return nil, fmt.Errorf("加密 key 失败: %w", err)
	}

	format := normalizeClientFormat(input.ClientFormat)
	mode := normalizeUpstreamRequestMode(input.RequestMode)
	ratio := input.Ratio
	if ratio <= 0 {
		ratio = 1
	}
	// groupRef 用分组名归一化，保证同渠道下唯一。
	groupRef := "manual:" + strings.ToLower(groupName)
	rec := &storage.UpstreamGroupKey{
		ChannelID:             ch.ID,
		ChannelName:           ch.Name,
		ChannelType:           ch.Type,
		ClientFormat:          format,
		RequestMode:           mode,
		GroupRef:              groupRef,
		GroupName:             groupName,
		GroupDesc:             strings.TrimSpace(input.GroupDesc),
		Ratio:                 ratio,
		InputPricePerMillion:  storage.DefaultInputPricePerMillion,
		OutputPricePerMillion: storage.DefaultOutputPricePerMillion,
		Priority:              input.Priority,
		Charity:               input.Charity,
		Enabled:               true,
		KeyCipher:             cipher,
		Status:                "unknown",
	}
	if err := s.groupKeys.Upsert(rec); err != nil {
		return nil, fmt.Errorf("保存分组失败: %w", err)
	}
	return s.groupKeys.FindByChannelGroup(ch.ID, groupRef)
}

// DeleteGroupKey 删除一个上游分组密钥记录。
// 用于用户手动清理"上游已经删掉、本地却残留并一直显示 dead"的分组。
// 只删本地记录，不去动上游（上游那边可能已经没了，也可能是用户手动贴的 key）。
func (s *Service) DeleteGroupKey(id uint) error {
	if id == 0 {
		return errors.New("invalid group key id")
	}
	s.clearRuntimeDisable(id)
	return s.groupKeys.Delete(id)
}

// ClearGroupKeyCooldown 手动解除某个上游分组的冷却，立即恢复可调度。
func (s *Service) ClearGroupKeyCooldown(id uint) (*storage.UpstreamGroupKey, error) {
	if id == 0 {
		return nil, errors.New("invalid group key id")
	}
	s.clearRuntimeDisable(id)
	if err := s.groupKeys.ClearCooldown(id); err != nil {
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
		// upstreamRefs 记录本轮上游确实返回的分组 ref，循环后用它清理已消失的本地残留。
		// 只有在 ListAPIKeyGroups 成功（上面 err==nil）时才做清理，避免上游偶发失败/返回不全导致误删。
		upstreamRefs := make(map[string]struct{}, len(groups))
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
			upstreamRefs[groupRef] = struct{}{}
			if keyword, blocked := blockedBootstrapKeyKeyword(ch, group); blocked {
				result.Skipped++
				item.Error = fmt.Sprintf("命中关键词 %q，已跳过创建 Key", keyword)
				// A group that becomes an image/blocked group must not remain
				// schedulable just because it was synchronized in an older run.
				// Delete only the matching auto-synced record; a separately named
				// manual record is never addressed by this upstream group ref.
				if existing, findErr := s.groupKeys.FindByChannelGroup(ch.ID, groupRef); findErr != nil {
					result.Failed++
					item.Error = findErr.Error()
				} else if existing != nil {
					if deleteErr := s.DeleteGroupKey(existing.ID); deleteErr != nil {
						result.Failed++
						item.Error = deleteErr.Error()
					} else {
						result.Removed++
						item.Removed = true
					}
				}
				result.Items = append(result.Items, item)
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
		// 清理残留：上游已经删掉的分组，本地不该继续留着（否则会一直显示 dead）。
		// 只在上面 ListAPIKeyGroups 成功、且本轮确实拿到过至少一个有效分组时才清理，
		// 双保险避免"上游临时返回空/token 权限不足只返回部分"时误删用户手动贴的 key。
		if len(upstreamRefs) > 0 {
			if locals, err := s.groupKeys.ListByChannel(ch.ID); err == nil {
				for i := range locals {
					local := locals[i]
					// Manual entries are intentionally outside upstream discovery and
					// must survive every automatic synchronization.
					if strings.HasPrefix(strings.ToLower(local.GroupRef), "manual:") {
						continue
					}
					if _, stillThere := upstreamRefs[local.GroupRef]; stillThere {
						continue
					}
					if err := s.DeleteGroupKey(local.ID); err == nil {
						result.Removed++
						result.Items = append(result.Items, BootstrapItem{
							ChannelID:   ch.ID,
							ChannelName: ch.Name,
							GroupName:   local.GroupName,
							GroupRef:    local.GroupRef,
							Removed:     true,
						})
					}
				}
			}
		}
	}
	return result, nil
}

func (s *Service) TestAllGroupKeys(ctx context.Context, batchSizes ...int) (*HealthResult, error) {
	batchSize := 0
	if len(batchSizes) > 0 {
		batchSize = batchSizes[0]
	}
	return s.TestGroupKeys(ctx, HealthTestOptions{BatchSize: batchSize})
}

func (s *Service) TestGroupKeys(ctx context.Context, opts HealthTestOptions) (*HealthResult, error) {
	list, err := s.groupKeys.List()
	if err != nil {
		return nil, err
	}
	list = filterHealthTestGroupKeys(list, opts.GroupIDs)
	// Health checks are intentionally limited to OpenAI / Responses groups.
	// Claude, Grok and Chat-mode bridges use different upstream contracts and
	// must not be put through the OpenAI gpt-5.5 probe by either the UI or a
	// direct API request.
	list = filterOpenAIHealthGroups(list)

	batchSize := normalizeHealthProbeBatchSize(opts.BatchSize)
	observer := progress.FromContext(ctx)
	probeCtx := context.Background()

	result := &HealthResult{
		BatchSize: batchSize,
		Items:     make([]HealthResultItem, len(list)),
	}

	enabled := make([]int, 0, len(list))
	for i := range list {
		if !list[i].Enabled {
			result.Items[i] = healthResultItemFromGroup(&list[i], "disabled")
			continue
		}
		enabled = append(enabled, i)
	}
	result.Total = len(enabled)
	for pos, idx := range enabled {
		item := healthResultItemFromGroup(&list[idx], "queued")
		observer.Emit(progress.Event{
			Stage:   progress.StageGatewayHealth,
			Message: fmt.Sprintf("等待测活：%s / %s", list[idx].ChannelName, list[idx].GroupName),
			Data:    healthProgressPayload("queued", item, 0, batchSize, 0, result.Total),
			Time:    time.Now(),
			Index:   pos + 1,
			Total:   result.Total,
		})
	}
	if len(enabled) > 0 {
		result.Batches = (len(enabled) + batchSize - 1) / batchSize
	}

	var completed int64
	if len(enabled) > 0 {
		observer.Emit(progress.Event{
			Stage:   progress.StageGatewayHealth,
			Message: fmt.Sprintf("开始并发测活：并发 %d 个，共 %d 个分组", minInt(batchSize, len(enabled)), result.Total),
			Data: map[string]any{
				"status":     "batch_start",
				"batch":      1,
				"batches":    result.Batches,
				"batch_size": batchSize,
				"completed":  0,
				"total":      result.Total,
			},
			Time:  time.Now(),
			Index: 0,
			Total: result.Total,
		})
	}

	type healthJob struct {
		pos int
		idx int
	}
	workerCount := minInt(batchSize, len(enabled))
	jobs := make(chan healthJob)
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				batchNo := job.pos/batchSize + 1
				idx := job.idx
				checking := healthResultItemFromGroup(&list[idx], "checking")
				checking.Batch = batchNo
				observer.Emit(progress.Event{
					Stage:   progress.StageGatewayHealth,
					Message: fmt.Sprintf("正在测活：%s / %s", list[idx].ChannelName, list[idx].GroupName),
					Data:    healthProgressPayload("checking", checking, batchNo, batchSize, int(atomic.LoadInt64(&completed)), result.Total),
					Time:    time.Now(),
					Index:   int(atomic.LoadInt64(&completed)),
					Total:   result.Total,
				})

				// Timeout starts here, after this item leaves the queue.
				// Queued groups must never inherit earlier probes' elapsed time.
				itemCtx, cancel := context.WithTimeout(probeCtx, 70*time.Second)
				item := s.testGroupKey(itemCtx, &list[idx])
				cancel()
				item.Batch = batchNo
				result.Items[idx] = item
				done := int(atomic.AddInt64(&completed, 1))
				ok := item.Status == "alive"
				observer.Emit(progress.Event{
					Stage:   progress.StageGatewayHealth,
					Message: fmt.Sprintf("测活完成：%s / %s %s", item.ChannelName, item.GroupName, statusTextForHealth(item.Status)),
					OK:      &ok,
					Data:    healthProgressPayload(item.Status, item, batchNo, batchSize, done, result.Total),
					Time:    time.Now(),
					Index:   done,
					Total:   result.Total,
				})
			}
		}()
	}
	for pos, idx := range enabled {
		jobs <- healthJob{pos: pos, idx: idx}
	}
	close(jobs)
	wg.Wait()

	for i := range result.Items {
		if result.Items[i].ID == 0 && i < len(list) {
			result.Items[i] = healthResultItemFromGroup(&list[i], "unknown")
		}
		switch result.Items[i].Status {
		case "alive":
			result.Checked++
			result.Alive++
		case "dead":
			result.Checked++
			result.Dead++
		case "zero_balance":
			result.Checked++
			result.ZeroBalance++
		case "rate_limited":
			result.Checked++
			result.RateLimited++
		case "forbidden":
			result.Checked++
			result.Forbidden++
		case "non_generation":
			result.Checked++
			result.NonGeneration++
		case "auth_failed":
			result.Checked++
			result.AuthFailed++
		case "timeout":
			result.Checked++
			result.Timeout++
		case "network_error":
			result.Checked++
			result.NetworkError++
		case "upstream_error":
			result.Checked++
			result.UpstreamError++
		case "model_error":
			result.Checked++
			result.ModelError++
		case "invalid_request":
			result.Checked++
			result.InvalidRequest++
		case "server_error":
			result.Checked++
			result.ServerError++
		case "disabled":
		default:
			result.Checked++
		}
	}
	summary := fmt.Sprintf("测活完成：%d/%d 存活", result.Alive, result.Checked)
	summary = appendHealthResultSummary(summary, result)
	observer.Emit(progress.Event{
		Stage:   progress.StageDone,
		Message: summary,
		OK:      ptrBool(true),
		Data:    result,
		Time:    time.Now(),
		Index:   result.Checked,
		Total:   result.Total,
	})
	return result, nil
}

func filterHealthTestGroupKeys(list []storage.UpstreamGroupKey, ids []uint) []storage.UpstreamGroupKey {
	if len(ids) == 0 {
		return list
	}
	allowed := make(map[uint]bool, len(ids))
	for _, id := range ids {
		if id > 0 {
			allowed[id] = true
		}
	}
	if len(allowed) == 0 {
		return []storage.UpstreamGroupKey{}
	}
	out := make([]storage.UpstreamGroupKey, 0, minInt(len(list), len(allowed)))
	for _, key := range list {
		if allowed[key.ID] {
			out = append(out, key)
		}
	}
	return out
}

func normalizeHealthProbeBatchSize(size int) int {
	if size <= 0 {
		return defaultHealthProbeBatchSize
	}
	if size > 100 {
		return 100
	}
	return size
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func healthResultItemFromGroup(key *storage.UpstreamGroupKey, status string) HealthResultItem {
	if key == nil {
		return HealthResultItem{Status: status}
	}
	return HealthResultItem{
		ID:          key.ID,
		ChannelID:   key.ChannelID,
		ChannelName: key.ChannelName,
		GroupRef:    key.GroupRef,
		GroupName:   key.GroupName,
		Ratio:       key.Ratio,
		Status:      status,
	}
}

func healthProgressPayload(status string, item HealthResultItem, batch, batchSize, completed, total int) map[string]any {
	return map[string]any{
		"status":     status,
		"item":       item,
		"batch":      batch,
		"batch_size": batchSize,
		"completed":  completed,
		"total":      total,
	}
}

func appendHealthResultSummary(summary string, result *HealthResult) string {
	if result == nil {
		return summary
	}
	parts := make([]string, 0, 10)
	add := func(count int, label string) {
		if count > 0 {
			parts = append(parts, fmt.Sprintf("%d 个%s", count, label))
		}
	}
	add(result.ZeroBalance, "零余额/额度不足")
	add(result.RateLimited, "限流/额度限制")
	add(result.Forbidden, "403 拒绝访问")
	add(result.NonGeneration, "非生成返回")
	add(result.AuthFailed, "认证失败")
	add(result.Timeout, "超时")
	add(result.NetworkError, "网络错误")
	add(result.UpstreamError, "上游错误")
	add(result.ModelError, "模型错误")
	add(result.InvalidRequest, "请求格式错误")
	add(result.ServerError, "上游 5xx")
	if len(parts) == 0 {
		return summary
	}
	return summary + "，" + strings.Join(parts, "，")
}

func statusTextForHealth(status string) string {
	switch status {
	case "alive":
		return "存活"
	case "dead":
		return "死亡"
	case "zero_balance":
		return "零余额/额度不足"
	case "rate_limited":
		return "限流/额度限制"
	case "forbidden":
		return "403 拒绝访问"
	case "non_generation":
		return "非生成返回"
	case "auth_failed":
		return "认证失败"
	case "timeout":
		return "超时"
	case "network_error":
		return "网络错误"
	case "upstream_error":
		return "上游错误"
	case "model_error":
		return "模型错误"
	case "invalid_request":
		return "请求格式错误"
	case "server_error":
		return "上游 5xx"
	case "disabled":
		return "停用"
	default:
		return status
	}
}

func ptrBool(v bool) *bool {
	return &v
}

// TestGroupKey immediately tests one upstream group. It deliberately owns its
// context so the browser request ending cannot turn a real probe into a false
// "context canceled" death result.
func (s *Service) TestGroupKey(id uint) (*HealthResultItem, error) {
	key, err := s.groupKeys.FindByID(id)
	if err != nil {
		return nil, err
	}
	if normalizeClientFormat(key.ClientFormat) != "openai" || normalizeUpstreamRequestMode(key.RequestMode) != "responses" {
		return nil, errors.New("one-click health check only supports OpenAI Responses-format groups")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	result := s.testGroupKey(ctx, key)
	return &result, nil
}

func filterOpenAIHealthGroups(groups []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	result := make([]storage.UpstreamGroupKey, 0, len(groups))
	for _, group := range groups {
		if normalizeClientFormat(group.ClientFormat) == "openai" && normalizeUpstreamRequestMode(group.RequestMode) == "responses" {
			result = append(result, group)
		}
	}
	return result
}

func (s *Service) Proxy(w http.ResponseWriter, r *http.Request, path string) error {
	requestIP := clientIP(r)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return &GatewayError{Status: http.StatusBadRequest, Body: jsonError("read request body: " + err.Error())}
	}
	_ = r.Body.Close()

	normalized, err := normalizeProxyRequest(r, path, body)
	if err != nil {
		if isResponsesStreamRequestPath(path) && requestWantsStream(r, body, rawQueryFromPath(path)) {
			return writeResponsesGatewayFailureStream(w, "gateway_invalid_request", friendlyGatewayStreamFailureMessage(err.Error()))
		}
		return &GatewayError{Status: http.StatusBadRequest, Body: jsonError(err.Error())}
	}
	normalized.ClientIP = requestIP
	normalized = s.rectifyBeforeSend(normalized)

	failGatewayRequest := func(status int, code, message string) error {
		if shouldWriteResponsesTerminalForGatewayFailure(normalized) {
			return writeResponsesGatewayFailureStream(w, code, friendlyGatewayStreamFailureMessage(message))
		}
		return &GatewayError{Status: status, Body: jsonError(message)}
	}
	cancelGatewayRequest := func(message string) error {
		if shouldWriteResponsesTerminalForGatewayFailure(normalized) {
			return writeResponsesGatewayCancelledStream(w, "gateway_request_cancelled", friendlyGatewayStreamFailureMessage(message))
		}
		return &GatewayError{Status: http.StatusRequestTimeout, Body: jsonError(message)}
	}

	policy, err := s.lookupIPPolicy(requestIP)
	if err != nil {
		return failGatewayRequest(http.StatusInternalServerError, "gateway_error", err.Error())
	}
	if policy != nil && policy.Blocked {
		return failGatewayRequest(http.StatusForbidden, "gateway_forbidden", "IP has been banned by this gateway")
	}
	rawKey := extractGatewayKey(r.Header)
	gatewayKey, err := s.Authenticate(rawKey, requestIP)
	if err != nil {
		if message, ok := s.publicGatewayLimitOrExpiredMessage(rawKey, nil, err); ok && shouldWriteResponsesTerminalForGatewayFailure(normalized) {
			return writeResponsesGatewayTextStream(w, normalized.RequestModel, message)
		}
		return failGatewayRequest(http.StatusUnauthorized, "gateway_auth_failed", err.Error())
	}
	if err := validateClientFormat(gatewayKey.ClientFormat, normalized.ResponseMode); err != nil {
		return failGatewayRequest(http.StatusBadRequest, "gateway_invalid_request", err.Error())
	}
	releaseKeySlot, err := s.acquireGatewayKeySlot(r.Context(), gatewayKey)
	if err != nil {
		return cancelGatewayRequest("gateway key concurrency queue canceled: " + err.Error())
	}
	defer releaseKeySlot()
	releaseIPSlot, err := s.acquirePublicIPSlot(r.Context(), gatewayKey, requestIP, policy)
	if err != nil {
		return cancelGatewayRequest("public key IP concurrency queue canceled: " + err.Error())
	}
	defer releaseIPSlot()

	refreshedKey, err := s.gateway.FindByID(gatewayKey.ID)
	if err != nil {
		return failGatewayRequest(http.StatusInternalServerError, "gateway_error", err.Error())
	}
	gatewayKey = refreshedKey
	if !gatewayKey.Enabled {
		if message, ok := s.publicGatewayLimitOrExpiredMessage(rawKey, gatewayKey, errors.New("invalid gateway key")); ok && shouldWriteResponsesTerminalForGatewayFailure(normalized) {
			return writeResponsesGatewayTextStream(w, normalized.RequestModel, message)
		}
		return failGatewayRequest(http.StatusUnauthorized, "gateway_auth_failed", "invalid gateway key")
	}
	if err := enforceGatewayQuota(gatewayKey); err != nil {
		if message, ok := s.publicGatewayLimitOrExpiredMessage(rawKey, gatewayKey, err); ok && shouldWriteResponsesTerminalForGatewayFailure(normalized) {
			return writeResponsesGatewayTextStream(w, normalized.RequestModel, message)
		}
		return failGatewayRequest(http.StatusTooManyRequests, "gateway_quota_exceeded", err.Error())
	}

	now := time.Now()
	candidates, err := s.groupKeys.ListCandidates(now)
	if err != nil {
		return failGatewayRequest(http.StatusInternalServerError, "gateway_error", err.Error())
	}
	candidates = filterCandidatesForGatewayKey(gatewayKey, candidates)
	candidates = filterCandidatesForClientFormat(gatewayKey.ClientFormat, normalized.ResponseMode, candidates)
	if len(candidates) == 0 {
		return failGatewayRequest(http.StatusServiceUnavailable, "upstream_unavailable", gatewayKeyScopeEmptyMessage(gatewayKey))
	}
	candidates = s.orderCandidatesForRequest(candidates, normalized)

	var errorsSeen []string
	var saturatedSeen []string
	var disabledSeen []string
	var cooldownFallback []storage.UpstreamGroupKey
	attemptedActiveCandidate := false
	// finalErr 承载"客户端错、换 key 也没用"路径的返回值。
	// 在 stream 已写字节 / 明确 client-side 400 等场景下，我们把 err 记进来后不再继续 fail-over。
	var finalErr error
	for i := range candidates {
		candidate := candidates[i]
		if until, ok := candidateCooldownUntil(candidate, now); ok {
			disabledSeen = append(disabledSeen, cooldownMessage(candidate, until))
			cooldownFallback = append(cooldownFallback, candidate)
			continue
		}
		if until, ok := s.runtimeDisabledUntil(candidate.ID); ok {
			disabledSeen = append(disabledSeen, cooldownMessage(candidate, until))
			cooldownFallback = append(cooldownFallback, candidate)
			continue
		}
		attemptedActiveCandidate = true

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
	if !attemptedActiveCandidate && len(cooldownFallback) > 0 && candidateScopeFallbackAllowed(gatewayKey, candidates) {
		cooldownFallback = sortCooldownFallbackCandidates(filterCooldownFallbackCandidates(cooldownFallback), now)
		if msg := cooldownFallbackMessage(cooldownFallback); msg != "" && s.log != nil {
			s.log.Warn("gateway probing cooldown upstream groups", "scope", gatewayKeyScopeLabel(gatewayKey), "message", msg)
		}
		for i := range cooldownFallback {
			candidate := cooldownFallback[i]
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
				errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.ChannelName, candidate.GroupName, outcome.errMsg))
				if outcome.markFailure {
					s.markProxyFailure(candidate.ID, outcome.errMsg)
				}
				finalErr = outcome.err
			}
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
		message = "upstreams temporarily unavailable; retry after cooldown: " + strings.Join(disabledSeen, " | ")
	} else if len(disabledSeen) > 0 {
		message += " | temporarily unavailable: " + strings.Join(disabledSeen, " | ")
	}
	return failGatewayRequest(http.StatusServiceUnavailable, "upstream_unavailable", message)
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

	normalized = requestForCandidate(normalized, candidate)
	if normalized.Stream {
		return s.attemptStream(ctx, gatewayKey, normalized, candidate, w)
	}
	return s.attemptNonStream(ctx, gatewayKey, normalized, candidate, w)
}

func requestForCandidate(request normalizedRequest, candidate *storage.UpstreamGroupKey) normalizedRequest {
	if candidate == nil || !request.hasAlt() {
		return request
	}
	// A native Anthropic Messages request is normalized internally so the
	// gateway can keep one scheduling path.  It must, however, be restored
	// before it reaches a Claude upstream.  Sending /v1/responses to such an
	// upstream made health checks pass while real requests failed.
	if normalizeClientFormat(candidate.ClientFormat) == "claude" && request.ResponseMode == "claude" {
		return request.alt()
	}
	if shouldPreferChatBridgeForResponsesStream(request, candidate) {
		return request.altWithFallbackToSelf()
	}
	switch normalizeUpstreamRequestMode(candidate.RequestMode) {
	case "chat":
		return request.alt()
	default:
		return request
	}
}

func shouldPreferChatBridgeForResponsesStream(request normalizedRequest, candidate *storage.UpstreamGroupKey) bool {
	// Keep native Responses as the first path whenever the upstream is marked
	// as Responses-capable.  Falling back to chat before trying /v1/responses
	// can drop Codex reasoning/tool fields and hurts prompt-cache affinity.
	return false
}

// applyUpstreamAuthHeaders sets the common API headers and the headers xAI
// expects for Grok's OpenAI-compatible endpoints.
func applyUpstreamAuthHeaders(header http.Header, key *storage.UpstreamGroupKey, upstreamKey string) {
	if key != nil && normalizeClientFormat(key.ClientFormat) == "claude" {
		// Anthropic-compatible upstreams require x-api-key rather than the
		// OpenAI Bearer header.  Keep the protocol version explicit so relays
		// do not silently select an incompatible default.
		header.Del("Authorization")
		header.Set("X-Api-Key", strings.TrimSpace(upstreamKey))
		if header.Get("Anthropic-Version") == "" {
			header.Set("Anthropic-Version", "2023-06-01")
		}
		header.Set("Content-Type", "application/json")
		return
	}
	header.Set("Authorization", "Bearer "+strings.TrimSpace(upstreamKey))
	header.Set("Content-Type", "application/json")
	if key != nil {
		header.Set("X-UpstreamOps-Group", key.GroupName)
	}
	if key != nil && normalizeClientFormat(key.ClientFormat) == "grok" {
		header.Set("Accept", "application/json, text/event-stream")
		header.Set("User-Agent", "upstream-ops-grok/1.0")
		return
	}
	if header.Get("User-Agent") == "" {
		header.Set("User-Agent", "codex-cli/0.1 upstream-ops")
	}
}

func (r normalizedRequest) hasAlt() bool {
	return r.AltPath != "" && len(r.AltBody) > 0
}

func (r normalizedRequest) alt() normalizedRequest {
	if !r.hasAlt() {
		return r
	}
	r.Path = r.AltPath
	r.Body = append([]byte(nil), r.AltBody...)
	r.ResponseMode = firstNonEmpty(r.AltMode, "raw")
	r.Stream = r.AltStream
	return r
}

func (r normalizedRequest) altWithFallbackToSelf() normalizedRequest {
	if !r.hasAlt() {
		return r
	}
	origPath := r.Path
	origBody := append([]byte(nil), r.Body...)
	origMode := r.ResponseMode
	origStream := r.Stream
	out := r.alt()
	out.AltPath = origPath
	out.AltBody = origBody
	out.AltMode = origMode
	out.AltStream = origStream
	return out
}

func (s *Service) attemptStream(
	ctx context.Context,
	gatewayKey *storage.GatewayKey,
	normalized normalizedRequest,
	candidate *storage.UpstreamGroupKey,
	w http.ResponseWriter,
) candOutcome {
	start := time.Now()
	timedWriter := &timingResponseWriter{ResponseWriter: w, start: start}
	failBeforeStreamBody := func(err error, errMsg string, markFailure bool) candOutcome {
		if shouldWriteResponsesTerminalForGatewayFailure(normalized) && !timedWriter.Started() {
			message := friendlyGatewayStreamFailureMessage(streamFailureMessageFromError(err, errMsg))
			if writeErr := writeResponsesGatewayFailureStream(w, "upstream_error", message); writeErr == nil {
				return candOutcome{kind: candSuccess}
			} else {
				return candOutcome{kind: candFatal, err: writeErr, errMsg: writeErr.Error(), markFailure: false}
			}
		}
		return candOutcome{kind: candFatal, err: err, errMsg: errMsg, markFailure: markFailure}
	}
	retry, usage, err := s.streamProxyCandidate(ctx, normalized, candidate, timedWriter)
	if err == nil {
		duration := time.Since(start)
		usage.FirstTokenMS = timedWriter.FirstTokenMS()
		usage.DurationMS = duration.Milliseconds()
		s.recordRuntimeSuccess(candidate.ID, duration)
		_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, usage.Cached, gatewayUsageCost(usage, candidate), time.Now())
		_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
		s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
		s.recordUsageLog(gatewayKey, candidate, usageLogModel(normalized, usage), normalized.ClientIP, usage)
		if usage.SoftFailure != "" {
			s.markProxyFailure(candidate.ID, usage.SoftFailure)
		}
		return candOutcome{kind: candSuccess}
	}
	errMsg := err.Error()
	// The downstream client may close a long-running stream at any time.  That
	// cancels this request context and is not evidence that the upstream is
	// unhealthy, so it must not trigger the five-minute scheduler cooldown.
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		if shouldWriteResponsesTerminalForGatewayFailure(normalized) {
			if writeErr := writeResponsesGatewayCancelledStream(w, "gateway_request_cancelled", "请求已取消。"); writeErr == nil {
				return candOutcome{kind: candSuccess}
			} else {
				return candOutcome{kind: candFatal, err: writeErr, errMsg: writeErr.Error(), markFailure: false}
			}
		}
		return candOutcome{kind: candFatal, err: err, errMsg: errMsg, markFailure: false}
	}
	if !retry {
		// streamProxyCandidate returns retry=false after response headers/body may
		// already have been written to the downstream client. From that point on
		// we must not try fallbacks or another candidate on the same writer.
		return failBeforeStreamBody(err, errMsg, true)
	}
	if fallback, reason, ok := fallbackRequestAfterFailure(normalized, errMsg); ok {
		start = time.Now()
		timedWriter = &timingResponseWriter{ResponseWriter: w, start: start}
		retry, usage, err = s.streamProxyCandidate(ctx, fallback, candidate, timedWriter)
		if err == nil {
			duration := time.Since(start)
			usage.FirstTokenMS = timedWriter.FirstTokenMS()
			usage.DurationMS = duration.Milliseconds()
			s.recordRuntimeSuccess(candidate.ID, duration)
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, usage.Cached, gatewayUsageCost(usage, candidate), time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			s.recordUsageLog(gatewayKey, candidate, usageLogModel(normalized, usage), normalized.ClientIP, usage)
			if usage.SoftFailure != "" {
				s.markProxyFailure(candidate.ID, usage.SoftFailure)
			}
			return candOutcome{kind: candSuccess}
		}
		errMsg = reason + " retry failed: " + err.Error()
		if !retry {
			return failBeforeStreamBody(err, errMsg, true)
		}
	}
	if rectified, reason, ok := s.rectifyAfterFailure(normalized, errMsg); ok {
		start = time.Now()
		timedWriter = &timingResponseWriter{ResponseWriter: w, start: start}
		retry, usage, err = s.streamProxyCandidate(ctx, rectified, candidate, timedWriter)
		if err == nil {
			duration := time.Since(start)
			usage.FirstTokenMS = timedWriter.FirstTokenMS()
			usage.DurationMS = duration.Milliseconds()
			s.recordRuntimeSuccess(candidate.ID, duration)
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, usage.Cached, gatewayUsageCost(usage, candidate), time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			s.recordUsageLog(gatewayKey, candidate, usageLogModel(normalized, usage), normalized.ClientIP, usage)
			if usage.SoftFailure != "" {
				s.markProxyFailure(candidate.ID, usage.SoftFailure)
			}
			return candOutcome{kind: candSuccess}
		}
		errMsg = reason + " retry failed: " + err.Error()
	}
	if retry {
		return candOutcome{kind: candRetryable, err: err, errMsg: errMsg}
	}
	// 流已经开始写 / 明确 fatal：仍然记一次失败（这样下次调度不会又选中这个坏候选），
	// 但不再切候选（否则会往同一个 ResponseWriter 上二次写头/写字节）。
	return failBeforeStreamBody(err, errMsg, true)
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
		duration := time.Since(start)
		s.recordRuntimeSuccess(candidate.ID, duration)
		usage := extractUsage(respBody)
		usage.FirstTokenMS = duration.Milliseconds()
		usage.DurationMS = duration.Milliseconds()
		_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, usage.Cached, gatewayUsageCost(usage, candidate), time.Now())
		_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
		s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
		s.recordUsageLog(gatewayKey, candidate, usageLogModel(normalized, usage), normalized.ClientIP, usage)
		writeProxyResponse(w, status, header, respBody, candidate, normalized.ResponseMode)
		return candOutcome{kind: candSuccess}
	}
	errMsg := err.Error()
	if fallback, reason, ok := fallbackRequestAfterFailure(normalized, errMsg); ok {
		start = time.Now()
		status, header, respBody, retry, err = s.tryProxyCandidate(ctx, fallback, candidate)
		if err == nil {
			duration := time.Since(start)
			s.recordRuntimeSuccess(candidate.ID, duration)
			usage := extractUsage(respBody)
			usage.FirstTokenMS = duration.Milliseconds()
			usage.DurationMS = duration.Milliseconds()
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, usage.Cached, gatewayUsageCost(usage, candidate), time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			s.recordUsageLog(gatewayKey, candidate, usageLogModel(normalized, usage), normalized.ClientIP, usage)
			writeProxyResponse(w, status, header, respBody, candidate, fallback.ResponseMode)
			return candOutcome{kind: candSuccess}
		}
		errMsg = reason + " retry failed: " + err.Error()
		if !retry {
			return candOutcome{kind: candFatal, err: err, errMsg: errMsg, markFailure: false}
		}
	}
	if rectified, reason, ok := s.rectifyAfterFailure(normalized, errMsg); ok {
		start = time.Now()
		status, header, respBody, retry, err = s.tryProxyCandidate(ctx, rectified, candidate)
		if err == nil {
			duration := time.Since(start)
			s.recordRuntimeSuccess(candidate.ID, duration)
			usage := extractUsage(respBody)
			usage.FirstTokenMS = duration.Milliseconds()
			usage.DurationMS = duration.Milliseconds()
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, usage.Cached, gatewayUsageCost(usage, candidate), time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			s.recordUsageLog(gatewayKey, candidate, usageLogModel(normalized, usage), normalized.ClientIP, usage)
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
	applyUpstreamAuthHeaders(req.Header, key, upstreamKey)
	if request.Stream {
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Cache-Control", "no-cache")
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
			if request.Stream && (request.ResponseMode == "responses" || request.ResponseMode == "responses_from_chat") {
				usage, err := streamNonSSEAsResponsesEvents(w, resp.StatusCode, header, respBody, key, request.ResponseMode)
				return false, usage, err
			}
			writeProxyResponse(w, resp.StatusCode, header, respBody, key, request.ResponseMode)
			return false, extractUsage(respBody), nil
		}
		reader := newSSEStreamReader(resp.Body)
		// 正式转发阶段的 idle 读超时：上游连续 streamIdleTimeout 没有任何新事件就判定卡死，
		// 主动关连接返回错误，避免 reader.Next() 无限阻塞导致客户端 stream closed。
		reader.closer = resp.Body
		reader.idleTimeout = streamIdleTimeout
		setStreamResponseHeaders(w)
		reader.heartbeatInterval = streamHeartbeatInterval
		reader.heartbeat = func() error {
			return writeSSEHeartbeat(w)
		}
		buffered, err := preflightSSEStream(reader, resp.Body)
		if err != nil {
			return true, usageTokens{}, err
		}
		copyResponseHeaders(w, header, key)
		setStreamResponseHeaders(w)
		if !responseWriterStarted(w) {
			w.WriteHeader(resp.StatusCode)
		}
		switch request.ResponseMode {
		case "chat":
			usage, err := streamResponsesAsChatEvents(w, buffered, reader)
			return false, usage, err
		case "claude":
			usage, err := streamResponsesAsClaudeEvents(w, buffered, reader)
			return false, usage, err
		case "responses_from_chat":
			// 降级路径：客户端发的是 responses，但这个上游只支持 chat，我们已把请求转成
			// chat/completions 发出去，此处再把上游的 chat SSE 流转回 responses 事件给客户端。
			// 有些中转会把 chat 路径又路由到 responses 实现，返回体已经是 responses SSE；
			// 这时必须按原生 responses 流处理，否则会把 delta 当成未知 chat chunk 丢掉。
			if bufferedSSELooksLikeResponses(buffered) {
				usage, err := streamRawSSE(w, buffered, reader, "responses")
				return false, usage, err
			}
			usage, err := streamChatAsResponsesEvents(w, buffered, reader)
			return false, usage, err
		}
		if request.ResponseMode == "responses" && bufferedSSELooksLikeChatCompletion(buffered) {
			usage, err := streamChatAsResponsesEvents(w, buffered, reader)
			return false, usage, err
		}
		usage, err := streamRawSSE(w, buffered, reader, request.ResponseMode)
		return false, usage, err
	}
	respBody, readErr := readLimitedBody(resp.Body, 64<<20)
	if readErr != nil {
		return true, usageTokens{}, readErr
	}
	errText := truncateBody(respBody, 240)
	if shouldRetryUpstreamStatus(resp.StatusCode, errText) {
		return true, usageTokens{}, errors.New(upstreamHTTPErrorMessage(resp.StatusCode, header, respBody))
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
		ChannelID:             ch.ID,
		ChannelName:           ch.Name,
		ChannelType:           ch.Type,
		ClientFormat:          inferGroupClientFormat(group.Name, group.Description),
		RequestMode:           "responses",
		GroupRef:              groupRef,
		GroupName:             strings.TrimSpace(group.Name),
		GroupDesc:             strings.TrimSpace(group.Description),
		Ratio:                 normalizedRatio(group.Ratio),
		InputPricePerMillion:  storage.DefaultInputPricePerMillion,
		OutputPricePerMillion: storage.DefaultOutputPricePerMillion,
		Enabled:               true,
		ConcurrencyLimit:      0,
		KeyCipher:             keyCipher,
		Status:                "unknown",
		FailureCount:          0,
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
	item := HealthResultItem{
		ID:          key.ID,
		ChannelID:   key.ChannelID,
		ChannelName: key.ChannelName,
		GroupRef:    key.GroupRef,
		GroupName:   key.GroupName,
		Ratio:       key.Ratio,
	}
	if !key.Enabled {
		item.Status = "disabled"
		return item
	}
	status, body, latencyMS, err := s.healthProbeCandidate(ctx, key)
	item.LatencyMS = latencyMS
	now := time.Now()
	item.CheckedAt = &now
	if err == nil && status >= 200 && status < 300 {
		item.Status = "alive"
		_ = s.groupKeys.MarkHealthSuccess(key.ID, item.LatencyMS)
		return item
	}

	firstErr := healthProbeError(status, body, err)
	firstFailureStatus := healthFailureStatus(status, body, err)
	if shouldSkipHealthRetry(firstFailureStatus) {
		item.Status = firstFailureStatus
		item.ErrorType = firstFailureStatus
		item.Error = firstErr.Error()
		s.markHealthFailureWithStatus(key.ID, item.Status, item.Error, item.LatencyMS)
		return item
	}
	delay := healthRetryDelay(key.ID)
	if waitHealthRetry(ctx, delay) {
		status, body, retryLatencyMS, err := s.healthProbeCandidate(ctx, key)
		item.LatencyMS = retryLatencyMS
		now = time.Now()
		item.CheckedAt = &now
		if err == nil && status >= 200 && status < 300 {
			item.Status = "alive"
			_ = s.groupKeys.MarkHealthSuccess(key.ID, item.LatencyMS)
			return item
		}
		item.Status = healthFailureStatus(status, body, err)
		item.ErrorType = item.Status
		item.Error = fmt.Sprintf("first probe failed: %v; retry after %s failed: %v", firstErr, delay, healthProbeError(status, body, err))
	} else {
		// 首探已经明确失败，只是"抖动重试的等待"被 ctx 取消（批量扫描超时 / 关机）。
		// 关键取舍：绝不能停在 unknown 就 return —— 那样不落库，DB 会保留上一轮的 alive，
		// 造成"上游已经死了、面板还显示绿色"的僵尸绿（用户明确报过这个 bug）。
		// 首探失败本身就是可信的失败信号，直接落 dead；下一轮 cron 会用退避后重新复活探测，
		// 真的活着会在下轮转回 alive，代价只是一轮的延迟，远好过僵尸绿。
		item.Status = "dead"
		item.ErrorType = item.Status
		item.Error = fmt.Sprintf("first probe failed: %v; retry wait canceled: %v", firstErr, ctx.Err())
		s.markHealthFailureWithStatus(key.ID, item.Status, item.Error, item.LatencyMS)
		return item
	}
	if item.Status == "" {
		item.Status = "dead"
	}
	if item.ErrorType == "" && item.Status != "alive" {
		item.ErrorType = item.Status
	}
	s.markHealthFailureWithStatus(key.ID, item.Status, item.Error, item.LatencyMS)
	return item
}

func healthFailureStatus(status int, body []byte, err error) string {
	text := healthFailureText(body, err)
	switch {
	case looksLikeZeroBalanceFailure(status, text):
		return "zero_balance"
	case looksLikeRateLimitedFailure(status, text):
		return "rate_limited"
	case looksLikeForbiddenFailure(status, text):
		return "forbidden"
	case looksLikeAuthFailure(status, text):
		return "auth_failed"
	case looksLikeUnsupportedModelError(text):
		return "model_error"
	case looksLikeClientRequestError(text) || status == http.StatusUnprocessableEntity:
		return "invalid_request"
	case looksLikeNonGenerationFailure(text):
		return "non_generation"
	case looksLikeTimeoutFailure(err, text):
		return "timeout"
	case looksLikeNetworkFailure(status, err, text):
		return "network_error"
	case looksLikeUpstreamErrorFailure(text):
		return "upstream_error"
	case status >= 500 && status < 600:
		return "server_error"
	case status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout:
		return "server_error"
	}
	return "dead"
}

func shouldSkipHealthRetry(status string) bool {
	switch status {
	case "zero_balance", "rate_limited", "forbidden", "auth_failed", "model_error", "invalid_request", "non_generation":
		return true
	default:
		return false
	}
}

func (s *Service) healthProbeCandidate(ctx context.Context, key *storage.UpstreamGroupKey) (int, []byte, int64, error) {
	// Claude 类型渠道：走 Anthropic Messages 格式探测，绝不用 openai 的 /v1/models + /v1/responses，
	// 否则 claude 上游不认这些端点，测活必然失败（这正是"claude 渠道一测就死"的原因）。
	if normalizeClientFormat(key.ClientFormat) == "claude" {
		return s.healthProbeClaude(ctx, key)
	}
	start := time.Now()
	// The one-click check is intentionally OpenAI-only. Use one stable model
	// instead of /v1/models discovery: model lists are often filtered or stale,
	// which used to make a healthy channel look dead before the real probe ran.
	model := "gpt-5.5"
	req := healthGenerationProbeRequest(model)
	req = requestForCandidate(req, key)
	status, _, body, err := s.requestHealthProbeCandidate(ctx, req, key, healthProbeTimeout)
	if fallback, _, ok := healthProbeFallbackRequest(req, status, body, err); ok {
		status, _, body, err = s.requestHealthProbeCandidate(ctx, fallback, key, healthProbeTimeout)
	}
	latencyMS := time.Since(start).Milliseconds()
	if err != nil {
		return status, body, latencyMS, err
	}
	if status >= 200 && status < 300 && isUpstreamErrorBody(body) {
		return status, body, latencyMS, fmt.Errorf("upstream returned error payload: %s", truncateBody(body, 240))
	}
	if status >= 200 && status < 300 && !looksLikeHealthGenerationSuccess(body) {
		return status, body, latencyMS, fmt.Errorf("upstream returned non-generation payload: %s", truncateBody(body, 240))
	}
	return status, body, latencyMS, nil
}

func healthProbeFallbackRequest(request normalizedRequest, status int, body []byte, err error) (normalizedRequest, string, bool) {
	if !request.hasAlt() {
		return request, "", false
	}
	if err != nil || status < 200 || status >= 300 {
		return fallbackRequestAfterFailure(request, healthProbeError(status, body, err).Error())
	}
	if request.ResponseMode != "responses_from_chat" {
		return request, "", false
	}
	if isUpstreamErrorBody(body) {
		return fallbackRequestAfterFailure(request, fmt.Sprintf("upstream returned error payload: %s", truncateBody(body, 240)))
	}
	if !looksLikeHealthGenerationSuccess(body) {
		return fallbackRequestAfterFailure(request, fmt.Sprintf("upstream returned non-generation payload: %s", truncateBody(body, 240)))
	}
	return request, "", false
}

// healthProbeClaude 用 Anthropic Messages 格式测活 claude 类型渠道。
// 直接打 /v1/messages，不做 /v1/models 发现（claude 中转站常不提供或格式不同）。
func (s *Service) healthProbeClaude(ctx context.Context, key *storage.UpstreamGroupKey) (int, []byte, int64, error) {
	start := time.Now()
	model := defaultHealthProbeModel(key.ClientFormat)
	body, _ := json.Marshal(map[string]any{
		"model":      model,
		"max_tokens": 1,
		"messages": []map[string]any{
			{"role": "user", "content": "hi"},
		},
		"stream": true,
	})
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	req := normalizedRequest{
		Method:       http.MethodPost,
		Path:         "/v1/messages",
		Header:       header,
		Body:         body,
		ResponseMode: "raw",
		Stream:       true,
	}
	status, _, respBody, err := s.requestHealthProbeCandidate(ctx, req, key, healthProbeTimeout)
	latencyMS := time.Since(start).Milliseconds()
	if err != nil {
		return status, respBody, latencyMS, err
	}
	if status < 200 || status >= 300 {
		return status, respBody, latencyMS, healthProbeError(status, respBody, nil)
	}
	if isUpstreamErrorBody(respBody) {
		return status, respBody, latencyMS, fmt.Errorf("upstream returned error payload: %s", truncateBody(respBody, 240))
	}
	// claude 成功响应含 content / role / type=message 等字段。
	if !looksLikeClaudeSuccess(respBody) {
		return status, respBody, latencyMS, fmt.Errorf("upstream returned non-message payload: %s", truncateBody(respBody, 240))
	}
	return status, respBody, latencyMS, nil
}

// looksLikeClaudeSuccess 判断 Anthropic Messages 响应是否为正常生成结果。
func looksLikeClaudeSuccess(body []byte) bool {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return false
	}
	if t, _ := raw["type"].(string); t != "" {
		switch t {
		case "message", "message_start", "content_block_start", "content_block_delta", "content_block_stop", "message_stop":
			return true
		}
	}
	if _, ok := raw["message"]; ok {
		return true
	}
	if _, ok := raw["content"]; ok {
		return true
	}
	if _, ok := raw["id"]; ok {
		if _, ok := raw["role"]; ok {
			return true
		}
	}
	return false
}

func (s *Service) discoverHealthProbeModel(ctx context.Context, key *storage.UpstreamGroupKey) (string, int, []byte, error) {
	req := normalizedRequest{
		Method: http.MethodGet,
		Path:   healthPath,
		Header: http.Header{},
	}
	status, _, body, err := s.requestCandidate(ctx, req, key, healthProbeTimeout)
	if err != nil {
		return "", status, body, fmt.Errorf("model discovery failed: %w", err)
	}
	if status < 200 || status >= 300 {
		return "", status, body, fmt.Errorf("model discovery failed: %w", healthProbeError(status, body, nil))
	}
	if isUpstreamErrorBody(body) {
		return "", status, body, fmt.Errorf("model discovery returned error payload: %s", truncateBody(body, 240))
	}
	return selectHealthProbeModel(extractHealthProbeModels(body), key.ClientFormat), status, body, nil
}

func shouldFallbackHealthModelDiscovery(status int) bool {
	return status == http.StatusNotFound || status == http.StatusMethodNotAllowed || status == http.StatusNotImplemented
}

func healthGenerationProbeRequest(model string) normalizedRequest {
	// 测活探针：发一句 "hi"，并把 max tokens 限到 1，尽量少烧 token（测活是付费的）。
	responsesBody, _ := json.Marshal(map[string]any{
		"model":             model,
		"input":             "hi",
		"max_output_tokens": 1,
		"stream":            true,
	})
	chatBody, _ := json.Marshal(map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "user", "content": "hi"},
		},
		"max_tokens": 1,
		"stream":     true,
	})
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	return normalizedRequest{
		Method:       http.MethodPost,
		Path:         responsesPath,
		Header:       header,
		Body:         responsesBody,
		ResponseMode: "responses",
		Stream:       true,
		AltPath:      "/v1/chat/completions",
		AltBody:      chatBody,
		AltMode:      "responses_from_chat",
		AltStream:    true,
	}
}

func extractHealthProbeModels(body []byte) []string {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil
	}
	var out []string
	appendHealthModelValues(&out, raw["data"])
	appendHealthModelValues(&out, raw["models"])
	return uniqueStrings(out)
}

func appendHealthModelValues(out *[]string, value any) {
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			appendHealthModelValues(out, item)
		}
	case []string:
		for _, item := range v {
			if item = strings.TrimSpace(item); item != "" {
				*out = append(*out, item)
			}
		}
	case map[string]any:
		for _, key := range []string{"id", "model", "name"} {
			if item := stringValue(v[key]); item != "" {
				*out = append(*out, item)
				return
			}
		}
	case string:
		if v = strings.TrimSpace(v); v != "" {
			*out = append(*out, v)
		}
	}
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func selectHealthProbeModel(models []string, format string) string {
	format = normalizeClientFormat(format)
	if format == "claude" {
		for _, model := range models {
			if strings.Contains(strings.ToLower(model), "claude") {
				return model
			}
		}
	}
	generative := make([]string, 0, len(models))
	for _, model := range models {
		if !looksLikeNonGenerativeModel(model) {
			generative = append(generative, model)
		}
	}
	preferred := []string{
		"gpt-4o-mini",
		"gpt-4.1-mini",
		"gpt-4o",
		"gpt-4.1",
		"gpt-3.5",
		"chatgpt",
		"gpt",
	}
	for _, marker := range preferred {
		for _, model := range generative {
			if strings.Contains(strings.ToLower(model), marker) {
				return model
			}
		}
	}
	if len(generative) > 0 {
		return generative[0]
	}
	if len(models) > 0 {
		return models[0]
	}
	return defaultHealthProbeModel(format)
}

func defaultHealthProbeModel(format string) string {
	switch normalizeClientFormat(format) {
	case "claude":
		return "claude-3-haiku-20240307"
	case "grok":
		return "grok-3-mini"
	default:
		return "gpt-4o-mini"
	}
}

func looksLikeNonGenerativeModel(model string) bool {
	s := strings.ToLower(strings.TrimSpace(model))
	for _, marker := range []string{
		"embedding",
		"rerank",
		"moderation",
		"whisper",
		"tts",
		"audio",
		"image",
		"dall",
		"babbage",
		"davinci",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

func looksLikeHealthGenerationSuccess(body []byte) bool {
	if len(bytes.TrimSpace(body)) == 0 {
		return false
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return false
	}
	if typ := strings.ToLower(stringValue(raw["type"])); typ != "" {
		if strings.HasPrefix(typ, "response.") || strings.Contains(typ, ".delta") || strings.Contains(typ, "_delta") {
			return true
		}
	}
	if obj := strings.ToLower(stringValue(raw["object"])); obj == "chat.completion.chunk" {
		return true
	}
	for _, key := range []string{"id", "choices", "output", "output_text", "content", "usage"} {
		if _, ok := raw[key]; ok {
			return true
		}
	}
	if response, ok := raw["response"].(map[string]any); ok {
		if id := stringValue(response["id"]); id != "" {
			return true
		}
		status := strings.ToLower(stringValue(response["status"]))
		return status == "completed" || status == "complete" || status == "succeeded"
	}
	return false
}

func healthProbeError(status int, body []byte, err error) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("HTTP %d: %s", status, truncateBody(body, 240))
}

func healthFailureText(body []byte, err error) string {
	text := strings.ToLower(strings.TrimSpace(string(body)))
	if err != nil {
		text = strings.TrimSpace(text + " " + strings.ToLower(err.Error()))
	}
	return text
}

func looksLikeZeroBalanceFailure(status int, text string) bool {
	if status == http.StatusPaymentRequired {
		return true
	}
	if text == "" {
		return false
	}
	if strings.Contains(text, "rate_limit") || strings.Contains(text, "rate limit") || strings.Contains(text, "too many requests") {
		return false
	}
	markers := []string{
		"insufficient_quota",
		"insufficient quota",
		"quota_exhausted",
		"quota exhausted",
		"quota exceeded",
		"exceeded your current quota",
		"额度不足",
		"额度已用尽",
		"余额不足",
		"余额已用尽",
		"欠费",
		"insufficient_balance",
		"insufficient balance",
		"balance not enough",
		"balance is not enough",
		"not enough balance",
		"out of credit",
		"out of credits",
		"insufficient credit",
		"insufficient credits",
		"no credit",
		"no credits",
		"credit exhausted",
		"credits exhausted",
		"billing hard limit",
		"billing quota",
		"payment required",
		"plan expired",
		"subscription expired",
	}
	for _, marker := range markers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func looksLikeRateLimitedFailure(status int, text string) bool {
	if status == http.StatusTooManyRequests {
		return true
	}
	if text == "" {
		return false
	}
	markers := []string{
		"codex_rate_limits",
		"rate_limits",
		"rate_limit",
		"rate limit",
		"rate-limit",
		"too many requests",
		"limit_reached",
		"limit reached",
		"allowed\":false",
		"allowed':false",
		"reset_after_seconds",
		"window_minutes",
		"used_percent",
		"requests per",
		"tokens per",
		"temporarily rate limited",
		"concurrency limit",
		"限流",
		"速率限制",
		"频率限制",
		"请求过快",
		"达到限制",
		"额度限制",
		"用量限制",
	}
	for _, marker := range markers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func looksLikeForbiddenFailure(status int, text string) bool {
	return status == http.StatusForbidden || strings.Contains(text, "403 forbidden") || strings.Contains(text, "http 403")
}

func looksLikeAuthFailure(status int, text string) bool {
	if status == http.StatusUnauthorized {
		return true
	}
	markers := []string{
		"invalid api key",
		"incorrect api key",
		"invalid x-api-key",
		"unauthorized",
		"authentication failed",
		"authentication error",
		"auth failed",
		"api key is invalid",
		"api key invalid",
		"无效的 api key",
		"无效api key",
		"认证失败",
		"鉴权失败",
	}
	for _, marker := range markers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func looksLikeNonGenerationFailure(text string) bool {
	return strings.Contains(text, "upstream returned non-generation payload") ||
		strings.Contains(text, "upstream returned non-message payload")
}

func looksLikeTimeoutFailure(err error, text string) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	for _, marker := range []string{
		"context deadline exceeded",
		"client.timeout",
		"timeout awaiting response",
		"i/o timeout",
		"timed out",
		"超时",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func looksLikeNetworkFailure(status int, err error, text string) bool {
	if err != nil && status == 0 {
		return true
	}
	for _, marker := range []string{
		"no such host",
		"connection refused",
		"connection reset",
		"connection closed",
		"tls handshake",
		"certificate",
		"eof",
		"broken pipe",
		"network is unreachable",
		"proxyconnect",
		"server misbehaving",
		"连接被拒绝",
		"连接重置",
		"网络不可达",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func looksLikeUpstreamErrorFailure(text string) bool {
	return strings.Contains(text, "upstream returned error payload") ||
		strings.Contains(text, "model discovery returned error payload") ||
		strings.Contains(text, "upstream returned success=false") ||
		strings.Contains(text, "upstream returned code ") ||
		strings.Contains(text, "upstream stream event")
}

func healthRetryDelay(keyID uint) time.Duration {
	if healthProbeRetryJitterMax <= 0 {
		return 0
	}
	maxSeconds := int(healthProbeRetryJitterMax / time.Second)
	if maxSeconds <= 1 {
		return healthProbeRetryJitterMax
	}
	return time.Duration(int(keyID)%maxSeconds+1) * time.Second
}

func waitHealthRetry(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
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
		return status, header, respBody, true, errors.New(upstreamHTTPErrorMessage(status, header, respBody))
	}
	return status, header, respBody, false, &GatewayError{Status: status, Header: header, Body: respBody}
}

func upstreamHTTPErrorMessage(status int, header http.Header, body []byte) string {
	suffix := ""
	if retryAfter := strings.TrimSpace(header.Get("Retry-After")); retryAfter != "" {
		suffix = " (retry-after: " + retryAfter + ")"
	}
	return fmt.Sprintf("upstream returned HTTP %d%s: %s", status, suffix, truncateBody(body, 240))
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
	applyUpstreamAuthHeaders(req.Header, key, upstreamKey)

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

func (s *Service) requestHealthProbeCandidate(ctx context.Context, request normalizedRequest, key *storage.UpstreamGroupKey, timeout time.Duration) (int, http.Header, []byte, error) {
	if !request.Stream {
		return s.requestCandidate(ctx, request, key, timeout)
	}
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
	applyUpstreamAuthHeaders(req.Header, key, upstreamKey)

	client := s.httpClientFor(ctx, ch)
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()

	header := cloneHeader(resp.Header)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || !isEventStream(header) {
		respBody, readErr := readLimitedBody(resp.Body, 64<<20)
		if readErr != nil {
			return resp.StatusCode, header, nil, readErr
		}
		return resp.StatusCode, header, respBody, nil
	}
	reader := newSSEStreamReader(resp.Body)
	buffered, err := preflightSSEStream(reader, resp.Body)
	body := healthProbeSSEBody(buffered)
	if err != nil {
		return resp.StatusCode, header, body, err
	}
	return resp.StatusCode, header, body, nil
}

func healthProbeSSEBody(events []sseEvent) []byte {
	for _, ev := range events {
		data := strings.TrimSpace(ev.Data)
		if data != "" && data != "[DONE]" {
			return []byte(data)
		}
	}
	if len(events) == 0 {
		return nil
	}
	return []byte(strings.TrimSpace(events[0].Data))
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
	// 流式推理请求可能在上游完成较长 reasoning 后才返回响应头；如果这里仍用
	// 60s，会早于 streamFirstEventTimeout 断开，Codex 直连仍会看到
	// "stream closed before response.completed"。非流请求有 requestCandidate 的
	// per-request context 超时兜底，不会因为这个 transport 上限被无限拉长。
	transport.ResponseHeaderTimeout = streamFirstEventTimeout
	if proxyURL = strings.TrimSpace(proxyURL); proxyURL != "" {
		if parsed, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(parsed)
		}
	}
	return transport
}

func (s *Service) markProxyFailure(id uint, msg string) {
	status := proxyFailureStatus(msg)
	policy := s.proxyFailurePolicy(id, status, msg)
	var disabledUntil *time.Time
	if policy.cooldown > 0 {
		until := time.Now().Add(policy.cooldown)
		disabledUntil = &until
		s.recordRuntimeFailure(id, until)
	} else {
		s.clearRuntimeDisable(id)
	}
	if err := s.groupKeys.MarkProxyFailureStatus(id, status, msg, disabledUntil); err != nil && s.log != nil {
		s.log.Warn("mark upstream group failed", "id", id, "err", err)
	}
	if policy.disableKey {
		if err := s.groupKeys.UpdateEnabled(id, false); err != nil && s.log != nil {
			s.log.Warn("disable upstream group key failed", "id", id, "err", err)
		}
	}
}

type proxyFailurePolicy struct {
	cooldown   time.Duration
	disableKey bool
}

func (s *Service) proxyFailurePolicy(id uint, status string, msg string) proxyFailurePolicy {
	switch status {
	case "rate_limited":
		delay := proxyRateLimitCooldown
		if hinted, ok := retryAfterDurationFromText(msg, time.Now()); ok {
			delay = hinted
		}
		return proxyFailurePolicy{cooldown: clampProxyCooldown(delay, time.Second, proxyPermanentFailureCooldown)}
	case "zero_balance", "auth_failed":
		return proxyFailurePolicy{disableKey: true}
	case "forbidden":
		return proxyFailurePolicy{cooldown: proxyPermanentFailureCooldown}
	}
	current, err := s.groupKeys.FindByID(id)
	if err != nil || current == nil {
		return proxyFailurePolicy{}
	}
	nextFailures := current.FailureCount + 1
	if nextFailures < proxyTransientFailureThreshold {
		return proxyFailurePolicy{}
	}
	delay := proxyTransientCooldownBase(status) * time.Duration(nextFailures-proxyTransientFailureThreshold+1)
	if delay > 3*time.Minute {
		delay = 3 * time.Minute
	}
	return proxyFailurePolicy{cooldown: delay}
}

func clampProxyCooldown(delay, minDelay, maxDelay time.Duration) time.Duration {
	if delay <= 0 {
		return minDelay
	}
	if minDelay > 0 && delay < minDelay {
		return minDelay
	}
	if maxDelay > 0 && delay > maxDelay {
		return maxDelay
	}
	return delay
}

func proxyTransientCooldownBase(status string) time.Duration {
	switch status {
	case "server_error":
		return proxyServerErrorCooldown
	case "timeout":
		return proxyTimeoutCooldown
	case "network_error":
		return proxyNetworkErrorCooldown
	default:
		return proxyTransientFailureCooldown
	}
}

func proxyFailureStatus(msg string) string {
	status := extractHTTPStatus(msg)
	return healthFailureStatus(status, []byte(msg), errors.New(msg))
}

func extractHTTPStatus(msg string) int {
	msg = strings.ToLower(msg)
	for _, marker := range []string{"http ", "status ", "returned "} {
		idx := strings.Index(msg, marker)
		if idx < 0 {
			continue
		}
		rest := strings.TrimSpace(msg[idx+len(marker):])
		if len(rest) < 3 {
			continue
		}
		code, err := strconv.Atoi(rest[:3])
		if err == nil && code >= 100 && code <= 599 {
			return code
		}
	}
	return 0
}

func retryAfterDurationFromText(msg string, now time.Time) (time.Duration, bool) {
	if now.IsZero() {
		now = time.Now()
	}
	if value, ok := retryAfterHeaderValueFromText(msg); ok {
		if seconds, err := strconv.Atoi(value); err == nil {
			return time.Duration(seconds) * time.Second, true
		}
		if delay, err := time.ParseDuration(value); err == nil {
			return delay, true
		}
		if when, err := http.ParseTime(value); err == nil {
			return when.Sub(now), true
		}
	}
	if seconds, ok := numericJSONFieldFromText(msg, "reset_after_seconds"); ok {
		return time.Duration(seconds) * time.Second, true
	}
	if seconds, ok := numericJSONFieldFromText(msg, "retry_after_seconds"); ok {
		return time.Duration(seconds) * time.Second, true
	}
	return 0, false
}

func retryAfterHeaderValueFromText(msg string) (string, bool) {
	lower := strings.ToLower(msg)
	idx := strings.Index(lower, "retry-after:")
	if idx < 0 {
		return "", false
	}
	raw := strings.TrimSpace(msg[idx+len("retry-after:"):])
	raw = strings.Trim(raw, "\"' ")
	for _, sep := range []string{")", "\n", "\r", ";"} {
		if cut := strings.Index(raw, sep); cut >= 0 {
			raw = raw[:cut]
		}
	}
	raw = strings.Trim(raw, "\"' ")
	return raw, raw != ""
}

func numericJSONFieldFromText(text string, field string) (int, bool) {
	lower := strings.ToLower(text)
	field = strings.ToLower(strings.TrimSpace(field))
	for _, marker := range []string{`"` + field + `"`, `'` + field + `'`, field} {
		idx := strings.Index(lower, marker)
		if idx < 0 {
			continue
		}
		rest := lower[idx+len(marker):]
		colon := strings.Index(rest, ":")
		if colon < 0 {
			continue
		}
		rest = strings.TrimSpace(rest[colon+1:])
		rest = strings.TrimLeft(rest, "\"' ")
		end := 0
		for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
			end++
		}
		if end == 0 {
			continue
		}
		value, err := strconv.Atoi(rest[:end])
		if err == nil {
			return value, true
		}
	}
	return 0, false
}

func (s *Service) markHealthFailure(id uint, msg string, latencyMS int64) {
	s.markHealthFailureWithStatus(id, "dead", msg, latencyMS)
}

func (s *Service) markHealthFailureWithStatus(id uint, status string, msg string, latencyMS int64) {
	if strings.TrimSpace(status) == "" {
		status = "dead"
	}
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
	if err := s.groupKeys.MarkHealthFailureStatus(id, status, msg, until, latencyMS); err != nil && s.log != nil {
		s.log.Warn("mark upstream health failed", "id", id, "err", err)
	}
}

func normalizeProxyRequest(r *http.Request, path string, body []byte) (normalizedRequest, error) {
	cleanPath := path
	rawQuery := ""
	if idx := strings.Index(cleanPath, "?"); idx >= 0 {
		rawQuery = cleanPath[idx+1:]
		cleanPath = cleanPath[:idx]
	}
	wantsStream := requestWantsStream(r, body, rawQuery)
	cleanPath = strings.TrimRight(cleanPath, "/")
	if cleanPath == "" {
		cleanPath = "/"
	}
	req := normalizedRequest{
		Method:       r.Method,
		Path:         path,
		Header:       cloneHeader(r.Header),
		Body:         body,
		RequestModel: modelFromRequestBody(body),
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
		if wantsStream {
			converted = ensureRequestStreamFlag(converted, true)
			stream = true
		}
		req.Path = responsesPath
		if rawQuery != "" {
			req.Path += "?" + rawQuery
		}
		req.Body = converted
		req.ResponseMode = "chat"
		req.Stream = stream
		req.AltPath = path
		req.AltBody = append([]byte(nil), body...)
		if wantsStream {
			req.AltBody = ensureRequestStreamFlag(req.AltBody, true)
		}
		req.AltMode = "raw"
		req.AltStream = stream
	case responsesPath:
		req.Path = responsesPath
		if rawQuery != "" {
			req.Path += "?" + rawQuery
		}
		req.ResponseMode = "responses"
		req.Stream = wantsStream
		if wantsStream {
			req.Body = ensureRequestStreamFlag(req.Body, true)
		}
		// 给显式 RequestMode=chat 的候选准备一个 chat/completions 兼容体。
		// 原生 Responses 候选仍直接透传完整 Responses 请求，不再隐藏降级到 Chat。
		if converted, altStream, err := responsesToChatRequestBody(body); err == nil {
			if wantsStream {
				converted = ensureRequestStreamFlag(converted, true)
				altStream = true
			}
			req.AltPath = "/v1/chat/completions"
			req.AltBody = converted
			req.AltMode = "responses_from_chat"
			req.AltStream = altStream
		}
	case "/v1/messages":
		converted, stream, err := claudeToResponsesBody(body)
		if err != nil {
			return req, err
		}
		if wantsStream {
			converted = ensureRequestStreamFlag(converted, true)
			stream = true
		}
		req.Path = responsesPath
		req.Body = converted
		req.ResponseMode = "claude"
		req.Stream = stream
		// Preserve the native request.  It is selected for Claude upstreams by
		// requestForCandidate; the normalized Responses representation remains
		// useful for the rest of the gateway pipeline.
		req.AltPath = path
		req.AltBody = append([]byte(nil), body...)
		if wantsStream {
			req.AltBody = ensureRequestStreamFlag(req.AltBody, true)
		}
		req.AltMode = "raw"
		req.AltStream = stream
	default:
		req.Stream = wantsStream
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

// responsesToChatRequestBody 把一个 Responses API 请求体转换成等价的 Chat Completions 请求体。
// 用途：Codex 不经 ccswitch 直连网关时发的是原生 /v1/responses；但很多中转站上游只认
// /v1/chat/completions。给这类上游一个可回退的 chat 请求体，避免"上游不支持 responses"
// 导致的流中断（客户端表现为 stream closed before response.completed）。
func responsesToChatRequestBody(body []byte) ([]byte, bool, error) {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, false, fmt.Errorf("decode responses request: %w", err)
	}
	stream := boolField(raw, "stream")
	out := map[string]any{}
	for k, v := range raw {
		switch k {
		case "input", "instructions", "max_output_tokens", "previous_response_id",
			"store", "reasoning", "text", "include", "parallel_tool_calls", "truncation",
			"tools", "tool_choice":
			// 这些是 Responses 专有字段，下面单独处理或直接丢弃。
			continue
		default:
			out[k] = v
		}
	}
	messages := responsesInputToChatMessages(raw["input"])
	// instructions 在 Responses 里相当于 system 提示，转成一条 system 消息放最前。
	if instr := strings.TrimSpace(fmt.Sprint(raw["instructions"])); instr != "" && raw["instructions"] != nil {
		messages = append([]map[string]any{{"role": "system", "content": instr}}, messages...)
	}
	if len(messages) == 0 {
		messages = []map[string]any{{"role": "user", "content": "."}}
	}
	out["messages"] = messages
	if tools := responsesToolsToChatTools(raw["tools"]); len(tools) > 0 {
		out["tools"] = tools
		if choice := responsesToolChoiceToChat(raw["tool_choice"]); choice != nil {
			out["tool_choice"] = choice
		}
	}
	if mt, ok := raw["max_output_tokens"]; ok {
		out["max_tokens"] = mt
	}
	out["stream"] = stream
	encoded, err := json.Marshal(out)
	return encoded, stream, err
}

// responsesToolsToChatTools converts Responses API tool declarations into Chat Completions function tools.
func responsesToolsToChatTools(value any) []map[string]any {
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	seen := map[string]bool{}
	for _, item := range items {
		switch tool := item.(type) {
		case string:
			name := strings.TrimSpace(tool)
			if converted := chatFunctionTool(name, "", nil); converted != nil && !seen[name] {
				out = append(out, converted)
				seen[name] = true
			}
		case map[string]any:
			out = append(out, responsesToolToChatTools(tool, "", seen)...)
		}
	}
	return out
}

func responsesToolToChatTools(tool map[string]any, namespace string, seen map[string]bool) []map[string]any {
	if tool == nil {
		return nil
	}
	typ := strings.ToLower(strings.TrimSpace(stringValue(tool["type"])))
	name := strings.TrimSpace(stringValue(tool["name"]))
	if namespace != "" && name != "" {
		name = namespace + "__" + name
	}
	description := stringValue(tool["description"])
	params := firstNonNil(tool["parameters"], tool["input_schema"], tool["schema"])
	switch typ {
	case "function", "custom":
		if converted := chatFunctionTool(name, description, params); converted != nil && !seen[name] {
			seen[name] = true
			return []map[string]any{converted}
		}
	case "tool_search":
		name = "tool_search"
		if converted := chatFunctionTool(name, "Search for an available tool by query.", map[string]any{
			"type":       "object",
			"properties": map[string]any{"query": map[string]any{"type": "string"}},
			"required":   []string{"query"},
		}); converted != nil && !seen[name] {
			seen[name] = true
			return []map[string]any{converted}
		}
	case "namespace":
		ns := strings.Trim(strings.ReplaceAll(name, ".", "__"), "_")
		children, _ := tool["tools"].([]any)
		out := make([]map[string]any, 0, len(children))
		for _, child := range children {
			if childMap, ok := child.(map[string]any); ok {
				out = append(out, responsesToolToChatTools(childMap, ns, seen)...)
			}
		}
		return out
	}
	return nil
}

func chatFunctionTool(name, description string, params any) map[string]any {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	if params == nil {
		params = map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        name,
			"description": description,
			"parameters":  params,
		},
	}
}

func responsesToolChoiceToChat(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		choice := strings.TrimSpace(v)
		switch strings.ToLower(choice) {
		case "", "auto", "none", "required":
			if choice == "" {
				return nil
			}
			return choice
		default:
			return map[string]any{"type": "function", "function": map[string]any{"name": choice}}
		}
	case map[string]any:
		typ := strings.ToLower(strings.TrimSpace(stringValue(v["type"])))
		if typ == "function" || typ == "custom" {
			name := strings.TrimSpace(stringValue(v["name"]))
			if name == "" {
				if fn, ok := v["function"].(map[string]any); ok {
					name = strings.TrimSpace(stringValue(fn["name"]))
				}
			}
			if name != "" {
				return map[string]any{"type": "function", "function": map[string]any{"name": name}}
			}
		}
	}
	return nil
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

// responsesInputToChatMessages converts Responses input into Chat Completions messages.
func responsesInputToChatMessages(input any) []map[string]any {
	switch v := input.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return []map[string]any{{"role": "user", "content": v}}
	case []any:
		out := make([]map[string]any, 0, len(v))
		for _, item := range v {
			msg, _ := item.(map[string]any)
			if msg == nil {
				continue
			}
			role, _ := msg["role"].(string)
			if role == "" {
				role = "user"
			}
			out = append(out, map[string]any{
				"role":    role,
				"content": flattenResponsesContentToText(msg["content"]),
			})
		}
		return out
	default:
		return nil
	}
}

// flattenResponsesContentToText 把 Responses 的 content（可能是 [{type,text}] 数组）压平成纯文本，
// chat/completions 的 content 接受纯字符串，最兼容。
func flattenResponsesContentToText(content any) any {
	switch v := content.(type) {
	case string:
		return v
	case []any:
		var b strings.Builder
		for _, part := range v {
			obj, _ := part.(map[string]any)
			if obj == nil {
				continue
			}
			if text, ok := obj["text"].(string); ok {
				b.WriteString(text)
			}
		}
		return b.String()
	default:
		return ""
	}
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
	if shouldSkipSameKeyRetry(errMsg) {
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

func fallbackRequestAfterFailure(request normalizedRequest, errMsg string) (normalizedRequest, string, bool) {
	if !request.hasAlt() || request.ResponseMode == "raw" {
		return request, "", false
	}
	if shouldSkipSameKeyRetry(errMsg) {
		return request, "", false
	}
	// chat 桥接失败时，先尝试回到原生 Responses，再决定是否换候选：
	// 1) chat 端点不存在/路由不通；
	// 2) chat 端点对该模型/兼容体不支持；
	// 3) 上游直接返回了 responses 语义的错误。
	if request.ResponseMode == "responses_from_chat" &&
		(looksLikeEndpointMissingError(errMsg) || looksLikeUnsupportedModelError(errMsg) || looksLikeResponsesEndpointError(errMsg)) {
		return request.alt(), "upstream native responses fallback", true
	}
	if request.ResponseMode == "responses" {
		// Native Responses requests must stay native.  Automatically downgrading
		// to chat/completions requires translating the body and can drop
		// reasoning/tool/instructions/input semantics that Codex relies on.
		// Explicit RequestMode=chat candidates still use the Chat→Responses
		// bridge through requestForCandidate; this branch only disables hidden
		// fallback after a native Responses attempt has already failed.
		return request, "", false
	}
	if !looksLikeResponsesEndpointError(errMsg) && !looksLikeEndpointMissingError(errMsg) {
		return request, "", false
	}
	return request.alt(), "upstream chat-completions compatibility", true
}

func shouldSkipSameKeyRetry(errMsg string) bool {
	switch proxyFailureStatus(errMsg) {
	case "zero_balance", "rate_limited", "forbidden", "auth_failed":
		return true
	default:
		return false
	}
}

// looksLikeEndpointMissingError 判断错误是否像"这个 HTTP 端点在上游根本不存在/不被支持"。
// 用于显式兼容路径的二次判断；不强求错误信息里出现具体端点名。
// 注意：必须排除 model/image/content 语义——那些是"端点在、但模型或内容不支持"，
// 有各自的处理路径（换候选 / 图片降级），不能被 chat 降级抢先。
func looksLikeEndpointMissingError(msg string) bool {
	s := strings.ToLower(msg)
	// 明显是"模型/内容"层面的错误，交给别的路径处理，这里直接放行不拦。
	for _, exclude := range []string{"model", "模型", "image", "图片", "content", "vision", "multimodal"} {
		if strings.Contains(s, exclude) {
			return false
		}
	}
	// 端点/路由层面的"不存在"信号。
	for _, marker := range []string{
		"404",
		"not found",
		"page not found",
		"no route",
		"unknown endpoint",
		"invalid endpoint",
		"invalid url",
		"method not allowed",
		"405",
		"接口不存在",
		"未找到",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

func looksLikeResponsesEndpointError(msg string) bool {
	s := strings.ToLower(msg)
	if !strings.Contains(s, "responses") && !strings.Contains(s, "/v1/responses") {
		return false
	}
	for _, marker := range []string{
		"404",
		"not found",
		"no route",
		"unsupported",
		"not support",
		"does not support",
		"unknown endpoint",
		"invalid endpoint",
		"invalid url",
		"接口不存在",
		"不支持",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

func (s *Service) orderCandidatesForRequest(candidates []storage.UpstreamGroupKey, request normalizedRequest) []storage.UpstreamGroupKey {
	ordered := s.orderCandidatesWithRuntime(candidates)
	ordered = preferSameGroupSchedulableCandidates(ordered)
	if s.affinities == nil || request.AffinityKey == "" {
		return ordered
	}
	affinity, err := s.affinities.Find(HashKey(request.AffinityKey), time.Now())
	if err != nil || affinity == nil || affinity.GroupKeyID == 0 {
		return ordered
	}
	// 区分硬 / 软亲和：
	//   硬亲和 —— previous_response_id / conversation / session 这类"有状态"请求，
	//            响应 ID 只存在于当初那台上游，换任何别的上游都会直接失败，
	//            因此必须无条件把原上游顶到最前，绝不能为省钱 / 状态未知而放弃。
	//   软亲和 —— chat: 前缀，是我们为了上游 prompt 缓存命中率做的"尽量同一台"，
	//            纯优化、不影响正确性，所以当它明显更贵或已经不健康时可以让位给更便宜的。
	hard := affinityIsHard(request.AffinityKey)
	for i, item := range ordered {
		if item.ID != affinity.GroupKeyID {
			continue
		}
		if !hard {
			// 软亲和：目标已经不是健康态，或比当前最优候选更贵，就放弃粘性、回归成本排序。
			if statusRank(item.Status) > statusRank("unknown") {
				return ordered
			}
			if len(ordered) > 0 && affinityWouldPromoteCostlier(item, ordered[0]) {
				return ordered
			}
		}
		out := append([]storage.UpstreamGroupKey{item}, ordered[:i]...)
		out = append(out, ordered[i+1:]...)
		out = preferSameGroupSchedulableCandidates(out)
		return out
	}
	return ordered
}

func preferSameGroupSchedulableCandidates(candidates []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	if len(candidates) < 3 {
		return candidates
	}
	target := dispatchGroupIdentity(candidates[0])
	if target == "" {
		return candidates
	}
	out := make([]storage.UpstreamGroupKey, 0, len(candidates))
	rest := make([]storage.UpstreamGroupKey, 0, len(candidates))
	out = append(out, candidates[0])
	for _, item := range candidates[1:] {
		if candidateSchedulable(item) && dispatchGroupIdentity(item) == target {
			out = append(out, item)
			continue
		}
		rest = append(rest, item)
	}
	out = append(out, rest...)
	return out
}

func dispatchGroupIdentity(item storage.UpstreamGroupKey) string {
	name := strings.ToLower(strings.TrimSpace(item.GroupName))
	if name == "" {
		name = strings.ToLower(strings.TrimSpace(item.GroupRef))
	}
	if name == "" {
		return ""
	}
	return normalizeClientFormat(item.ClientFormat) + "|" + normalizeUpstreamRequestMode(item.RequestMode) + "|" + name
}

// affinityIsHard 判断一个亲和 key 是否是"必须回原上游"的有状态亲和。
// 只有我们自己合成的 chat: 缓存种子是软亲和，其余（response / conversation / metadata）都是硬亲和。
func affinityIsHard(rawKey string) bool {
	return rawKey != "" && !strings.HasPrefix(rawKey, "chat:")
}

func (s *Service) orderCandidatesWithRuntime(candidates []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	out := orderCandidates(candidates)
	sort.SliceStable(out, func(i, j int) bool {
		schedI := candidateSchedulable(out[i])
		schedJ := candidateSchedulable(out[j])
		if schedI != schedJ {
			return schedI
		}
		if schedI && out[i].Charity != out[j].Charity {
			return out[i].Charity
		}
		if rankI, rankJ := statusRank(out[i].Status), statusRank(out[j].Status); rankI != rankJ {
			return rankI < rankJ
		}
		if out[i].Charity != out[j].Charity {
			return out[i].Charity
		}
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
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
	_, ok := s.runtimeDisabledUntil(id)
	return ok
}

func (s *Service) runtimeDisabledUntil(id uint) (time.Time, bool) {
	state, ok := s.runtime.Load(id)
	if !ok {
		return time.Time{}, false
	}
	current := state.(*groupRuntimeState)
	current.mu.Lock()
	defer current.mu.Unlock()
	if current.disabledUntil.IsZero() || !time.Now().Before(current.disabledUntil) {
		return time.Time{}, false
	}
	return current.disabledUntil, true
}

func candidateCooldownUntil(item storage.UpstreamGroupKey, now time.Time) (time.Time, bool) {
	if item.DisabledUntil == nil || item.DisabledUntil.IsZero() {
		return time.Time{}, false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if !now.Before(*item.DisabledUntil) {
		return time.Time{}, false
	}
	return *item.DisabledUntil, true
}

func cooldownMessage(item storage.UpstreamGroupKey, until time.Time) string {
	name := strings.TrimSpace(item.ChannelName + "/" + item.GroupName)
	if name == "/" {
		name = fmt.Sprintf("group-key #%d", item.ID)
	}
	return fmt.Sprintf("%s retry_at=%s retry_after=%s", name, until.Format(time.RFC3339), retryAfterText(time.Until(until)))
}

func retryAfterText(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Second {
		return "0s"
	}
	d = d.Round(time.Second)
	if d < time.Minute {
		return d.String()
	}
	if d < time.Hour {
		mins := int(d / time.Minute)
		secs := int((d % time.Minute) / time.Second)
		if secs == 0 {
			return fmt.Sprintf("%dm", mins)
		}
		return fmt.Sprintf("%dm%ds", mins, secs)
	}
	return d.String()
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

func (s *Service) keyRuntimeState(id uint) *keyRuntimeState {
	actual, _ := s.keyRuntime.LoadOrStore(id, &keyRuntimeState{})
	return actual.(*keyRuntimeState)
}

func (s *Service) acquireGatewayKeySlot(ctx context.Context, key *storage.GatewayKey) (func(), error) {
	if key == nil || key.ConcurrencyLimit <= 0 {
		return func() {}, nil
	}
	state := s.keyRuntimeState(key.ID)
	state.mu.Lock()
	if state.inFlight < key.ConcurrencyLimit && len(state.queue) == 0 {
		state.inFlight++
		state.lastObservedAt = time.Now()
		state.mu.Unlock()
		return s.gatewayKeySlotRelease(state), nil
	}
	wait := make(chan struct{})
	state.queue = append(state.queue, wait)
	state.lastObservedAt = time.Now()
	state.mu.Unlock()

	select {
	case <-wait:
		return s.gatewayKeySlotRelease(state), nil
	case <-ctx.Done():
		state.mu.Lock()
		removed := false
		for i, item := range state.queue {
			if item == wait {
				copy(state.queue[i:], state.queue[i+1:])
				state.queue[len(state.queue)-1] = nil
				state.queue = state.queue[:len(state.queue)-1]
				removed = true
				break
			}
		}
		state.mu.Unlock()
		if removed {
			return nil, ctx.Err()
		}
		return s.gatewayKeySlotRelease(state), nil
	}
}

func (s *Service) lookupIPPolicy(ip string) (*storage.IPPolicy, error) {
	if s.ipPolicies == nil || strings.TrimSpace(ip) == "" {
		return nil, nil
	}
	return s.ipPolicies.Find(ip)
}

func (s *Service) acquirePublicIPSlot(ctx context.Context, key *storage.GatewayKey, ip string, policy *storage.IPPolicy) (func(), error) {
	if key == nil || !key.IsPublic || strings.TrimSpace(ip) == "" || (policy != nil && policy.PublicConcurrencyExempt) {
		return func() {}, nil
	}
	stateAny, _ := s.ipRuntime.LoadOrStore(ip, &keyRuntimeState{})
	state := stateAny.(*keyRuntimeState)
	state.mu.Lock()
	if state.inFlight < 5 && len(state.queue) == 0 {
		state.inFlight++
		state.lastObservedAt = time.Now()
		state.mu.Unlock()
		return s.gatewayKeySlotRelease(state), nil
	}
	wait := make(chan struct{})
	state.queue = append(state.queue, wait)
	state.lastObservedAt = time.Now()
	state.mu.Unlock()
	select {
	case <-wait:
		return s.gatewayKeySlotRelease(state), nil
	case <-ctx.Done():
		state.mu.Lock()
		for i, item := range state.queue {
			if item == wait {
				copy(state.queue[i:], state.queue[i+1:])
				state.queue[len(state.queue)-1] = nil
				state.queue = state.queue[:len(state.queue)-1]
				state.mu.Unlock()
				return nil, ctx.Err()
			}
		}
		state.mu.Unlock()
		return s.gatewayKeySlotRelease(state), nil
	}
}

func (s *Service) gatewayKeySlotRelease(state *keyRuntimeState) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			s.releaseGatewayKeySlot(state)
		})
	}
}

func (s *Service) releaseGatewayKeySlot(state *keyRuntimeState) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.lastObservedAt = time.Now()
	if len(state.queue) > 0 {
		wait := state.queue[0]
		copy(state.queue[0:], state.queue[1:])
		state.queue[len(state.queue)-1] = nil
		state.queue = state.queue[:len(state.queue)-1]
		close(wait)
		return
	}
	if state.inFlight > 0 {
		state.inFlight--
	}
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

func (s *Service) clearRuntimeDisable(id uint) {
	state := s.runtimeState(id)
	state.mu.Lock()
	defer state.mu.Unlock()
	state.disabledUntil = time.Time{}
	state.lastObservedAt = time.Now()
}

func orderCandidates(in []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	out := append([]storage.UpstreamGroupKey(nil), in...)
	sort.SliceStable(out, func(i, j int) bool {
		schedI := candidateSchedulable(out[i])
		schedJ := candidateSchedulable(out[j])
		if schedI != schedJ {
			return schedI
		}
		if schedI && out[i].Charity != out[j].Charity {
			return out[i].Charity
		}
		if rankI, rankJ := statusRank(out[i].Status), statusRank(out[j].Status); rankI != rankJ {
			return rankI < rankJ
		}
		if out[i].Charity != out[j].Charity {
			return out[i].Charity
		}
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
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

func candidateSchedulable(item storage.UpstreamGroupKey) bool {
	return statusRank(item.Status) <= statusRank("unknown")
}

func affinityWouldPromoteCostlier(item, best storage.UpstreamGroupKey) bool {
	if candidateSchedulable(item) && candidateSchedulable(best) && item.Charity != best.Charity {
		return best.Charity
	}
	if statusRank(item.Status) > statusRank(best.Status) {
		return true
	}
	if item.Priority < best.Priority {
		return true
	}
	if item.Priority == best.Priority && item.Ratio > best.Ratio {
		return true
	}
	return false
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
		case "responses_from_chat":
			// 降级路径的非流式版本：上游返回 chat.completion，转回 responses 对象给客户端。
			if converted, err := chatToResponsesResponse(body); err == nil {
				outBody = converted
				outHeader.Set("Content-Type", "application/json")
			}
		case "responses":
			if looksLikeChatCompletionResponse(body) {
				if converted, err := chatToResponsesResponse(body); err == nil {
					outBody = converted
					outHeader.Set("Content-Type", "application/json")
				}
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

// chatToResponsesResponse 把上游返回的 chat.completion（非流式）转换成 Responses 对象，
// 用于 responses→chat 降级后，把 chat 回复还原成客户端期待的 responses 格式。
func chatToResponsesResponse(body []byte) ([]byte, error) {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	text := chatCompletionText(raw)
	model, _ := raw["model"].(string)
	id, _ := raw["id"].(string)
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(id)), "resp") {
		id = "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	resp := map[string]any{
		"id":         id,
		"object":     "response",
		"created_at": time.Now().Unix(),
		"model":      model,
		"status":     "completed",
		"output": []map[string]any{{
			"type":   "message",
			"role":   "assistant",
			"status": "completed",
			"content": []map[string]any{{
				"type": "output_text",
				"text": text,
			}},
		}},
		"output_text": text,
	}
	if usage, ok := raw["usage"]; ok {
		if usageMap, ok := usage.(map[string]any); ok {
			tokens := usageFromMap(usageMap)
			resp["usage"] = map[string]int64{
				"input_tokens":  tokens.Prompt,
				"output_tokens": tokens.Completion,
				"total_tokens":  tokens.Total,
			}
		} else {
			resp["usage"] = usage
		}
	}
	return json.Marshal(resp)
}

func streamNonSSEAsResponsesEvents(w http.ResponseWriter, status int, header http.Header, body []byte, key *storage.UpstreamGroupKey, mode string) (usageTokens, error) {
	outBody := body
	if mode == "responses_from_chat" || looksLikeChatCompletionResponse(body) {
		if converted, err := chatToResponsesResponse(body); err == nil {
			outBody = converted
		}
	}
	id, model, text, usage := responsesCompletionPartsFromBody(outBody)
	copyResponseHeaders(w, header, key)
	setStreamResponseHeaders(w)
	w.WriteHeader(status)
	if err := writeResponsesStreamStart(w, id, model); err != nil {
		return usage, err
	}
	if text != "" {
		if err := writeResponsesTextDelta(w, id, text); err != nil {
			return usage, err
		}
	}
	if err := writeResponsesStreamEnd(w, id, model, text, usage); err != nil {
		return usage, err
	}
	return usage, nil
}

func responsesCompletionPartsFromBody(body []byte) (string, string, string, usageTokens) {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil || raw == nil {
		return "", "", strings.TrimSpace(string(body)), usageTokens{}
	}
	if responseRaw, ok := raw["response"].(map[string]any); ok {
		raw = responseRaw
	}
	id := responseIDFromMap(raw)
	if id == "" {
		id = "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	model := strings.TrimSpace(stringValue(raw["model"]))
	text := responseText(raw)
	if text == "" {
		text = strings.TrimSpace(string(body))
	}
	usage := usageTokens{ResponseID: id}
	if usageRaw, ok := raw["usage"].(map[string]any); ok {
		usage = usageFromMap(usageRaw)
		usage.ResponseID = id
	}
	usage.Model = model
	return id, model, text, usage
}

func buildResponsesCompletedResponse(id, model, itemID, text string, usage usageTokens) map[string]any {
	if id == "" {
		id = "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	if itemID == "" {
		itemID = "item_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	item := map[string]any{
		"id":      itemID,
		"type":    "message",
		"role":    "assistant",
		"status":  "completed",
		"content": []map[string]any{{"type": "output_text", "text": text}},
	}
	resp := map[string]any{
		"id":          id,
		"object":      "response",
		"created_at":  time.Now().Unix(),
		"status":      "completed",
		"model":       model,
		"output":      []map[string]any{item},
		"output_text": text,
	}
	if usage.Total > 0 {
		resp["usage"] = map[string]int64{
			"input_tokens":  usage.Prompt,
			"output_tokens": usage.Completion,
			"total_tokens":  usage.Total,
		}
	}
	return resp
}

func writeResponsesStreamStart(w http.ResponseWriter, id, model string) error {
	if err := writeResponsesCreated(w, id, model); err != nil {
		return err
	}
	return writeResponsesOutputStart(w, id)
}

func writeResponsesCreated(w http.ResponseWriter, id, model string) error {
	if id == "" {
		id = "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	created := map[string]any{
		"type": "response.created",
		"response": map[string]any{
			"id":         id,
			"object":     "response",
			"created_at": time.Now().Unix(),
			"status":     "in_progress",
			"model":      model,
			"output":     []any{},
		},
	}
	return writeSSEEvent(w, sseEvent{Event: "response.created", Data: mustJSON(created)})
}

func writeResponsesOutputStart(w http.ResponseWriter, id string) error {
	if id == "" {
		id = "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	itemID := responseItemID(id)
	added := map[string]any{
		"type":         "response.output_item.added",
		"response_id":  id,
		"output_index": 0,
		"item": map[string]any{
			"id":      itemID,
			"type":    "message",
			"role":    "assistant",
			"status":  "in_progress",
			"content": []any{},
		},
	}
	if err := writeSSEEvent(w, sseEvent{Event: "response.output_item.added", Data: mustJSON(added)}); err != nil {
		return err
	}
	part := map[string]any{
		"type":          "response.content_part.added",
		"response_id":   id,
		"item_id":       itemID,
		"output_index":  0,
		"content_index": 0,
		"part":          map[string]any{"type": "output_text", "text": ""},
	}
	return writeSSEEvent(w, sseEvent{Event: "response.content_part.added", Data: mustJSON(part)})
}

func writeResponsesTextDelta(w http.ResponseWriter, id, delta string) error {
	payload := map[string]any{
		"type":          "response.output_text.delta",
		"response_id":   id,
		"item_id":       responseItemID(id),
		"output_index":  0,
		"content_index": 0,
		"delta":         delta,
	}
	return writeSSEEvent(w, sseEvent{Event: "response.output_text.delta", Data: mustJSON(payload)})
}

func writeResponsesFunctionCallAdded(w http.ResponseWriter, responseID string, outputIndex int, callID, name string) error {
	itemID := responseFunctionItemID(responseID, outputIndex)
	payload := map[string]any{
		"type":         "response.output_item.added",
		"response_id":  responseID,
		"output_index": outputIndex,
		"item": map[string]any{
			"id":        itemID,
			"type":      "function_call",
			"call_id":   callID,
			"name":      name,
			"arguments": "",
			"status":    "in_progress",
		},
	}
	return writeSSEEvent(w, sseEvent{Event: "response.output_item.added", Data: mustJSON(payload)})
}

func writeResponsesFunctionCallArgumentsDelta(w http.ResponseWriter, responseID string, outputIndex int, callID, delta string) error {
	payload := map[string]any{
		"type":         "response.function_call_arguments.delta",
		"response_id":  responseID,
		"item_id":      responseFunctionItemID(responseID, outputIndex),
		"output_index": outputIndex,
		"call_id":      callID,
		"delta":        delta,
	}
	return writeSSEEvent(w, sseEvent{Event: "response.function_call_arguments.delta", Data: mustJSON(payload)})
}

func writeResponsesFunctionCallDone(w http.ResponseWriter, responseID string, outputIndex int, callID, name, arguments string) error {
	itemID := responseFunctionItemID(responseID, outputIndex)
	argsDone := map[string]any{
		"type":         "response.function_call_arguments.done",
		"response_id":  responseID,
		"item_id":      itemID,
		"output_index": outputIndex,
		"call_id":      callID,
		"arguments":    arguments,
	}
	if err := writeSSEEvent(w, sseEvent{Event: "response.function_call_arguments.done", Data: mustJSON(argsDone)}); err != nil {
		return err
	}
	itemDone := map[string]any{
		"type":         "response.output_item.done",
		"response_id":  responseID,
		"output_index": outputIndex,
		"item": map[string]any{
			"id":        itemID,
			"type":      "function_call",
			"call_id":   callID,
			"name":      name,
			"arguments": arguments,
			"status":    "completed",
		},
	}
	return writeSSEEvent(w, sseEvent{Event: "response.output_item.done", Data: mustJSON(itemDone)})
}

func writeResponsesStreamEnd(w http.ResponseWriter, id, model, text string, usage usageTokens) error {
	if id == "" {
		id = "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	itemID := responseItemID(id)
	textDone := map[string]any{
		"type":          "response.output_text.done",
		"response_id":   id,
		"item_id":       itemID,
		"output_index":  0,
		"content_index": 0,
		"text":          text,
	}
	if err := writeSSEEvent(w, sseEvent{Event: "response.output_text.done", Data: mustJSON(textDone)}); err != nil {
		return err
	}
	partDone := map[string]any{
		"type":          "response.content_part.done",
		"response_id":   id,
		"item_id":       itemID,
		"output_index":  0,
		"content_index": 0,
		"part":          map[string]any{"type": "output_text", "text": text},
	}
	if err := writeSSEEvent(w, sseEvent{Event: "response.content_part.done", Data: mustJSON(partDone)}); err != nil {
		return err
	}
	itemDone := map[string]any{
		"type":         "response.output_item.done",
		"response_id":  id,
		"output_index": 0,
		"item": map[string]any{
			"id":      itemID,
			"type":    "message",
			"role":    "assistant",
			"status":  "completed",
			"content": []map[string]any{{"type": "output_text", "text": text}},
		},
	}
	if err := writeSSEEvent(w, sseEvent{Event: "response.output_item.done", Data: mustJSON(itemDone)}); err != nil {
		return err
	}
	completed := buildResponsesCompletedResponse(id, model, itemID, text, usage)
	if err := writeSSEEvent(w, sseEvent{Event: "response.completed", Data: mustJSON(map[string]any{"type": "response.completed", "response": completed})}); err != nil {
		return err
	}
	return writeSSEData(w, "[DONE]")
}

func writeResponsesStreamFailure(w http.ResponseWriter, id, model, code, message string) error {
	if id == "" {
		id = "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	code = strings.TrimSpace(code)
	if code == "" {
		code = "upstream_error"
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = "upstream stream failed"
	}
	payload := map[string]any{
		"type": "response.failed",
		"response": map[string]any{
			"id":     id,
			"object": "response",
			"status": "failed",
			"model":  model,
			"output": []any{},
			"error":  map[string]any{"code": code, "message": message},
		},
	}
	if err := writeSSEEvent(w, sseEvent{Event: "response.failed", Data: mustJSON(payload)}); err != nil {
		return err
	}
	return writeSSEData(w, "[DONE]")
}

func writeResponsesStreamCancelled(w http.ResponseWriter, id, model, code, message string) error {
	if id == "" {
		id = "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	code = strings.TrimSpace(code)
	if code == "" {
		code = "cancelled"
	}
	message = strings.TrimSpace(message)
	if message == "" {
		message = "request cancelled"
	}
	payload := map[string]any{
		"type": "response.cancelled",
		"response": map[string]any{
			"id":     id,
			"object": "response",
			"status": "cancelled",
			"model":  model,
			"output": []any{},
			"error":  map[string]any{"code": code, "message": message},
		},
	}
	if err := writeSSEEvent(w, sseEvent{Event: "response.cancelled", Data: mustJSON(payload)}); err != nil {
		return err
	}
	return writeSSEData(w, "[DONE]")
}

func writeResponsesGatewayFailureStream(w http.ResponseWriter, code, message string) error {
	setStreamResponseHeaders(w)
	if !responseWriterStarted(w) {
		w.WriteHeader(http.StatusOK)
	}
	id := "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if err := writeResponsesCreated(w, id, ""); err != nil {
		return err
	}
	return writeResponsesStreamFailure(w, id, "", code, message)
}

func writeResponsesGatewayTextStream(w http.ResponseWriter, model, text string) error {
	setStreamResponseHeaders(w)
	if !responseWriterStarted(w) {
		w.WriteHeader(http.StatusOK)
	}
	id := "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	text = strings.TrimSpace(text)
	if text == "" {
		text = "请求暂时无法完成，请稍后重试。"
	}
	if err := writeResponsesStreamStart(w, id, strings.TrimSpace(model)); err != nil {
		return err
	}
	if err := writeResponsesTextDelta(w, id, text); err != nil {
		return err
	}
	return writeResponsesStreamEnd(w, id, strings.TrimSpace(model), text, usageTokens{})
}

func writeResponsesGatewayCancelledStream(w http.ResponseWriter, code, message string) error {
	setStreamResponseHeaders(w)
	if !responseWriterStarted(w) {
		w.WriteHeader(http.StatusOK)
	}
	id := "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if err := writeResponsesCreated(w, id, ""); err != nil {
		return err
	}
	return writeResponsesStreamCancelled(w, id, "", code, message)
}

func shouldWriteResponsesTerminalForGatewayFailure(request normalizedRequest) bool {
	switch request.ResponseMode {
	case "responses", "responses_from_chat":
		return request.Stream
	default:
		return false
	}
}

func isResponsesStreamRequestPath(path string) bool {
	clean := path
	if idx := strings.Index(clean, "?"); idx >= 0 {
		clean = clean[:idx]
	}
	clean = "/" + strings.Trim(strings.TrimSpace(clean), "/")
	return clean == responsesPath
}

func friendlyGatewayStreamFailureMessage(message string) string {
	trimmed := strings.TrimSpace(message)
	lower := strings.ToLower(trimmed)
	switch {
	case lower == "":
		return "请求暂时无法完成，请稍后重试。"
	case strings.Contains(lower, "no alive upstream group keys available") ||
		strings.Contains(lower, "all upstream group keys failed") ||
		strings.Contains(lower, "upstreams temporarily unavailable"):
		return "当前没有可用上游，请稍后重试；如果持续出现，请检查上游渠道状态。"
	case strings.Contains(lower, "concurrency") || strings.Contains(lower, "queue canceled"):
		return "当前请求过多或排队已取消，请稍后重试。"
	case strings.Contains(lower, "daily token limit") ||
		strings.Contains(lower, "total token limit") ||
		strings.Contains(lower, "balance exhausted") ||
		strings.Contains(lower, "quota"):
		return "网关密钥额度已用尽，请检查额度或更换密钥。"
	case strings.Contains(lower, "invalid gateway key") ||
		strings.Contains(lower, "missing gateway key") ||
		strings.Contains(lower, "gateway key expired") ||
		strings.Contains(lower, "invalid api key") ||
		strings.Contains(lower, "unauthorized") ||
		strings.Contains(lower, "authenticate"):
		return "网关密钥无效或已失效，请检查请求密钥。"
	case strings.Contains(lower, "ip has been banned") || strings.Contains(lower, "forbidden"):
		return "当前 IP 已被网关拒绝访问。"
	case strings.Contains(lower, "client format") ||
		strings.Contains(lower, "request format") ||
		strings.Contains(lower, "only accepts"):
		return "请求格式不支持当前网关密钥，请检查接口或密钥配置。"
	default:
		return "请求暂时无法完成：" + trimmed
	}
}

func streamFailureMessageFromError(err error, fallback string) string {
	var gerr *GatewayError
	if errors.As(err, &gerr) {
		if msg := errorMessageFromJSON(gerr.Body); msg != "" {
			return msg
		}
		if text := strings.TrimSpace(string(gerr.Body)); text != "" {
			return truncateBody([]byte(text), 240)
		}
	}
	if strings.TrimSpace(fallback) != "" {
		return fallback
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

func responseItemID(responseID string) string {
	if responseID == "" {
		responseID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return "item_" + strings.TrimPrefix(responseID, "resp_")
}

func responseFunctionItemID(responseID string, outputIndex int) string {
	if responseID == "" {
		responseID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return fmt.Sprintf("fc_%s_%d", strings.TrimPrefix(responseID, "resp_"), outputIndex)
}

func mustJSON(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}

func setStreamResponseHeaders(w http.ResponseWriter) {
	if w == nil {
		return
	}
	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache, no-transform")
	header.Set("Connection", "keep-alive")
	header.Set("X-Accel-Buffering", "no")
}

func looksLikeChatCompletionResponse(body []byte) bool {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return false
	}
	obj := strings.ToLower(strings.TrimSpace(stringValue(raw["object"])))
	if obj == "chat.completion" || obj == "chat.completion.chunk" {
		return true
	}
	if _, ok := raw["choices"].([]any); ok {
		if _, hasResponseObject := raw["output"].([]any); !hasResponseObject {
			return true
		}
	}
	return false
}

// chatCompletionText 从 chat.completion 响应里提取助手回复文本。
func chatCompletionText(raw map[string]any) string {
	choices, _ := raw["choices"].([]any)
	var b strings.Builder
	for _, c := range choices {
		choice, _ := c.(map[string]any)
		if choice == nil {
			continue
		}
		msg, _ := choice["message"].(map[string]any)
		if msg == nil {
			continue
		}
		switch content := msg["content"].(type) {
		case string:
			b.WriteString(content)
		case []any:
			for _, part := range content {
				obj, _ := part.(map[string]any)
				if obj == nil {
					continue
				}
				if text, ok := obj["text"].(string); ok {
					b.WriteString(text)
				}
			}
		}
	}
	return b.String()
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
	model := responseModelFromMap(raw)
	usageRaw, ok := raw["usage"].(map[string]any)
	if !ok {
		if responseRaw, ok := raw["response"].(map[string]any); ok {
			if responseID == "" {
				responseID = responseIDFromMap(responseRaw)
			}
			if model == "" {
				model = responseModelFromMap(responseRaw)
			}
			if nestedUsageRaw, ok := responseRaw["usage"].(map[string]any); ok {
				usage := usageFromMap(nestedUsageRaw)
				usage.ResponseID = responseID
				usage.Model = model
				return usage
			}
		}
		return usageTokens{ResponseID: responseID, Model: model}
	}
	usage := usageFromMap(usageRaw)
	usage.ResponseID = responseID
	usage.Model = model
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
	model := responseModelFromMap(raw)
	if usageRaw, ok := raw["usage"].(map[string]any); ok {
		if usage := usageFromMap(usageRaw); usage.Total > 0 {
			usage.ResponseID = responseID
			usage.Model = model
			return usage
		}
	}
	if responseRaw, ok := raw["response"].(map[string]any); ok {
		if responseID == "" {
			responseID = responseIDFromMap(responseRaw)
		}
		if model == "" {
			model = responseModelFromMap(responseRaw)
		}
		if usageRaw, ok := responseRaw["usage"].(map[string]any); ok {
			usage := usageFromMap(usageRaw)
			usage.ResponseID = responseID
			usage.Model = model
			return usage
		}
	}
	return usageTokens{ResponseID: responseID, Model: model}
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

func responseModelFromMap(raw map[string]any) string {
	if raw == nil {
		return ""
	}
	if model := stringValue(raw["model"]); strings.TrimSpace(model) != "" {
		return strings.TrimSpace(model)
	}
	if response, ok := raw["response"].(map[string]any); ok {
		return responseModelFromMap(response)
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
		// 只有 "failed" 才是真正的上游错误。
		// "incomplete" / "cancelled" 在通用 JSON 判断里不作为上游错误；
		// Responses SSE 终态由 streamRawSSE 单独规范化，保证只输出
		// response.completed / response.failed / response.cancelled。
		if strings.EqualFold(status, "failed") {
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
	if prompt < 0 {
		prompt = 0
	}
	if completion < 0 {
		completion = 0
	}
	if total < 0 {
		total = 0
	}
	cached := cachedTokensFromUsage(usageRaw)
	if cached < 0 {
		cached = 0
	}
	if prompt > 0 && cached > prompt {
		cached = prompt
	}
	return usageTokens{Prompt: prompt, Completion: completion, Total: total, Cached: cached}
}

func cachedTokensFromUsage(usageRaw map[string]any) int64 {
	for _, path := range [][]string{
		{"prompt_tokens_details", "cached_tokens"},
		{"input_tokens_details", "cached_tokens"},
	} {
		if n := nestedIntField(usageRaw, path...); n > 0 {
			return n
		}
	}
	for _, key := range []string{
		"cached_tokens",
		"cache_read_input_tokens",
		"prompt_cache_hit_tokens",
		"cache_hit_tokens",
		"cached_input_tokens",
	} {
		if n := intField(usageRaw, key); n > 0 {
			return n
		}
	}
	return 0
}

func nestedIntField(raw map[string]any, path ...string) int64 {
	if len(path) == 0 {
		return 0
	}
	var cur any = raw
	for i, key := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			return 0
		}
		value, ok := obj[key]
		if !ok {
			return 0
		}
		if i == len(path)-1 {
			n, ok := numericValue(value)
			if !ok {
				return 0
			}
			return n
		}
		cur = value
	}
	return 0
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

// streamChatAsResponsesEvents 把上游的 chat.completion.chunk SSE 流转换成 Responses SSE 事件流，
// 用于 responses→chat 降级：客户端要 responses 格式，上游只会 chat，这里做流式桥接。
// 发出的事件序列：response.created → response.output_text.delta* → response.completed → [DONE]
func streamChatAsResponsesEvents(w http.ResponseWriter, buffered []sseEvent, reader *sseStreamReader) (usageTokens, error) {
	id := "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	model := ""
	startSent := false
	var best usageTokens
	var textBuf strings.Builder
	type chatToolCallState struct {
		OutputIndex int
		CallID      string
		Name        string
		Added       bool
		Args        strings.Builder
	}
	toolCalls := map[int]*chatToolCallState{}
	toolOrder := make([]int, 0)
	sawDone := false

	emitStart := func() error {
		if startSent {
			return nil
		}
		startSent = true
		return writeResponsesStreamStart(w, id, model)
	}
	emitToolCalls := func(raw map[string]any) error {
		choices, _ := raw["choices"].([]any)
		for _, c := range choices {
			choice, _ := c.(map[string]any)
			if choice == nil {
				continue
			}
			delta, _ := choice["delta"].(map[string]any)
			if delta == nil {
				continue
			}
			calls, _ := delta["tool_calls"].([]any)
			for _, item := range calls {
				call, _ := item.(map[string]any)
				if call == nil {
					continue
				}
				idx := int(intField(call, "index"))
				state, ok := toolCalls[idx]
				if !ok {
					state = &chatToolCallState{OutputIndex: idx + 1}
					toolCalls[idx] = state
					toolOrder = append(toolOrder, idx)
				}
				if callID := stringValue(call["id"]); callID != "" {
					state.CallID = callID
				}
				fn, _ := call["function"].(map[string]any)
				if name := stringValue(fn["name"]); name != "" {
					state.Name = name
				}
				argsDelta := stringValue(fn["arguments"])
				if state.CallID == "" {
					state.CallID = "call_" + strconv.FormatInt(time.Now().UnixNano()+int64(idx), 36)
				}
				if state.Name == "" && argsDelta == "" {
					continue
				}
				if !state.Added {
					if err := emitStart(); err != nil {
						return err
					}
					if err := writeResponsesFunctionCallAdded(w, id, state.OutputIndex, state.CallID, state.Name); err != nil {
						return err
					}
					state.Added = true
				}
				if argsDelta != "" {
					state.Args.WriteString(argsDelta)
					if err := writeResponsesFunctionCallArgumentsDelta(w, id, state.OutputIndex, state.CallID, argsDelta); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	err := readSSEEvents(buffered, reader, func(event, data string) error {
		if data == "" {
			return nil
		}
		if data == "[DONE]" {
			sawDone = true
			return errResponsesStreamTerminal
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(data), &raw); err != nil {
			return fmt.Errorf("decode upstream chat stream event: %w", err)
		}
		if v, ok := raw["model"].(string); ok && v != "" {
			model = v
			best.Model = v
		}
		if v, ok := raw["id"].(string); ok && strings.HasPrefix(strings.ToLower(strings.TrimSpace(v)), "resp") {
			id = v
			best.ResponseID = v
		}
		if usageRaw, ok := raw["usage"].(map[string]any); ok {
			if usage := usageFromMap(usageRaw); usage.Total > 0 {
				usage.ResponseID = firstNonEmpty(usage.ResponseID, best.ResponseID, id)
				usage.Model = firstNonEmpty(usage.Model, best.Model, model)
				best = usage
			}
		}
		if _, ok := raw["choices"].([]any); ok {
			if err := emitStart(); err != nil {
				return err
			}
		}
		// 从 chat chunk 里取出增量文本。
		delta := chatChunkDeltaText(raw)
		if delta != "" {
			textBuf.WriteString(delta)
			if err := writeResponsesTextDelta(w, id, delta); err != nil {
				return err
			}
		}
		if err := emitToolCalls(raw); err != nil {
			return err
		}
		if chatChunkHasFinish(raw) {
			sawDone = true
			return errResponsesStreamTerminal
		}
		return nil
	})
	streamErr := err
	if errors.Is(streamErr, errResponsesStreamTerminal) {
		streamErr = nil
	}
	if err := emitStart(); err != nil {
		return best, err
	}
	if streamErr != nil || !sawDone {
		message := "upstream chat stream ended before normal completion"
		if streamErr != nil {
			message += ": " + streamErr.Error()
		}
		if err := writeResponsesStreamFailure(w, id, model, "upstream_stream_interrupted", message); err != nil {
			return best, err
		}
		best.SoftFailure = message
		best.Status = "interrupted"
		return best, nil
	}
	for _, idx := range toolOrder {
		state := toolCalls[idx]
		if state == nil || !state.Added {
			continue
		}
		if err := writeResponsesFunctionCallDone(w, id, state.OutputIndex, state.CallID, state.Name, state.Args.String()); err != nil {
			return best, err
		}
	}
	// 收尾：补齐 Responses 生命周期终态，保证 Codex 一定能看到 response.completed。
	if err := writeResponsesStreamEnd(w, id, model, textBuf.String(), best); err != nil {
		return best, err
	}
	return best, nil
}

func chatChunkHasFinish(raw map[string]any) bool {
	choices, _ := raw["choices"].([]any)
	for _, c := range choices {
		choice, _ := c.(map[string]any)
		if choice == nil {
			continue
		}
		if finish, ok := choice["finish_reason"]; ok && finish != nil && strings.TrimSpace(fmt.Sprint(finish)) != "" {
			return true
		}
	}
	return false
}

// chatChunkDeltaText 从一个 chat.completion.chunk 里提取本次增量文本。
func chatChunkDeltaText(raw map[string]any) string {
	choices, _ := raw["choices"].([]any)
	var b strings.Builder
	for _, c := range choices {
		choice, _ := c.(map[string]any)
		if choice == nil {
			continue
		}
		delta, _ := choice["delta"].(map[string]any)
		if delta == nil {
			continue
		}
		switch content := delta["content"].(type) {
		case string:
			b.WriteString(content)
		case []any:
			for _, part := range content {
				obj, _ := part.(map[string]any)
				if obj == nil {
					continue
				}
				if text, ok := obj["text"].(string); ok {
					b.WriteString(text)
				}
			}
		}
	}
	return b.String()
}

func streamResponsesAsChatEvents(w http.ResponseWriter, buffered []sseEvent, reader *sseStreamReader) (usageTokens, error) {
	created := time.Now().Unix()
	id := "chatcmpl-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	model := ""
	roleSent := false
	doneSent := false
	var best usageTokens
	err := readSSEEvents(buffered, reader, func(event, data string) error {
		if data == "" {
			return nil
		}
		if data == "[DONE]" {
			return errResponsesStreamTerminal
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
				best.Model = v
			}
			if usageRaw, ok := response["usage"].(map[string]any); ok {
				if usage := usageFromMap(usageRaw); usage.Total > 0 {
					usage.ResponseID = firstNonEmpty(usage.ResponseID, best.ResponseID, id)
					usage.Model = firstNonEmpty(usage.Model, best.Model, model)
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
			best.Model = v
		}
		if usageRaw, ok := raw["usage"].(map[string]any); ok {
			if usage := usageFromMap(usageRaw); usage.Total > 0 {
				usage.ResponseID = firstNonEmpty(usage.ResponseID, best.ResponseID, id)
				usage.Model = firstNonEmpty(usage.Model, best.Model, model)
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
			if err := writeSSEData(w, "[DONE]"); err != nil {
				return err
			}
			return errResponsesStreamTerminal
		}
		return nil
	})
	if errors.Is(err, errResponsesStreamTerminal) {
		err = nil
	}
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
		if data == "" {
			return nil
		}
		if data == "[DONE]" {
			return errResponsesStreamTerminal
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
				best.Model = v
			}
			if usageRaw, ok := response["usage"].(map[string]any); ok {
				if usage := usageFromMap(usageRaw); usage.Total > 0 {
					usage.ResponseID = firstNonEmpty(usage.ResponseID, best.ResponseID, id)
					usage.Model = firstNonEmpty(usage.Model, best.Model, model)
					best = usage
				}
			}
		}
		if v, ok := raw["model"].(string); ok && v != "" {
			model = v
			best.Model = v
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
			if err := writeClaudeEvent(w, "message_stop", map[string]any{"type": "message_stop"}); err != nil {
				return err
			}
			return errResponsesStreamTerminal
		}
		return nil
	})
	if errors.Is(err, errResponsesStreamTerminal) {
		err = nil
	}
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
			value := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if r.data.Len() > 0 {
				r.data.WriteByte('\n')
			}
			r.data.WriteString(value)
			if sseDataLineReady(value) {
				if ev, ok := dispatch(); ok {
					return ev, nil
				}
			}
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

func sseDataLineReady(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || value == "[DONE]" {
		return true
	}
	return strings.HasPrefix(value, "{") && strings.HasSuffix(value, "}")
}

func readSSEEvents(buffered []sseEvent, reader *sseStreamReader, emit func(event, data string) error) error {
	for _, ev := range buffered {
		if err := emit(ev.Event, ev.Data); err != nil {
			return err
		}
	}
	for {
		var ev sseEvent
		var err error
		// 有 closer + idleTimeout 的 reader（真实上游转发）走带超时的读，
		// 上游卡住超过 idle 就主动关连接返回错误，避免无限阻塞 → 客户端断流。
		if reader != nil && reader.closer != nil && reader.idleTimeout > 0 {
			ev, err = readNextSSEWithTimeout(reader, reader.closer, reader.idleTimeout, "next event")
		} else {
			ev, err = reader.Next()
		}
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
		ev, err := readNextSSEWithTimeout(reader, closer, streamFirstEventTimeout, "first event")
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

func readNextSSEWithTimeout(reader *sseStreamReader, closer io.Closer, timeout time.Duration, label string) (sseEvent, error) {
	type result struct {
		ev  sseEvent
		err error
	}
	done := make(chan result, 1)
	go func() {
		ev, err := reader.Next()
		done <- result{ev: ev, err: err}
	}()
	if strings.TrimSpace(label) == "" {
		label = "event"
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var heartbeat <-chan time.Time
	var ticker *time.Ticker
	if reader != nil && reader.heartbeat != nil {
		interval := reader.heartbeatInterval
		if interval <= 0 {
			interval = streamHeartbeatInterval
		}
		ticker = time.NewTicker(interval)
		heartbeat = ticker.C
		defer ticker.Stop()
	}
	for {
		select {
		case res := <-done:
			return res.ev, res.err
		case <-heartbeat:
			if err := reader.heartbeat(); err != nil {
				_ = closer.Close()
				return sseEvent{}, err
			}
		case <-timer.C:
			_ = closer.Close()
			return sseEvent{}, fmt.Errorf("upstream stream did not send %s within %s", label, timeout)
		}
	}
}

func streamEventReady(ev sseEvent) bool {
	data := strings.TrimSpace(ev.Data)
	if data == "[DONE]" {
		return true
	}
	if sseEventLooksLikeChatCompletion(ev) {
		return true
	}
	typ := sseEventType(ev)
	if strings.HasPrefix(typ, "response.") {
		return true
	}
	if typ == "response.completed" || typ == "response.output_text.done" {
		return true
	}
	if strings.Contains(typ, ".delta") || strings.Contains(typ, "_delta") {
		return true
	}
	switch typ {
	case "message_start", "content_block_start", "content_block_delta", "content_block_stop", "message_stop":
		return true
	}
	switch strings.TrimSpace(ev.Event) {
	case "response.completed", "response.output_text.delta":
		return true
	default:
		return strings.TrimSpace(ev.Event) != "" || data != ""
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

func bufferedSSELooksLikeChatCompletion(events []sseEvent) bool {
	for _, ev := range events {
		if sseEventLooksLikeChatCompletion(ev) {
			return true
		}
	}
	return false
}

func bufferedSSELooksLikeResponses(events []sseEvent) bool {
	for _, ev := range events {
		if sseEventLooksLikeResponses(ev) {
			return true
		}
	}
	return false
}

func sseEventLooksLikeResponses(ev sseEvent) bool {
	if strings.HasPrefix(sseEventType(ev), "response.") {
		return true
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(ev.Event)), "response.") {
		return true
	}
	data := strings.TrimSpace(ev.Data)
	if data == "" || data == "[DONE]" {
		return false
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		return false
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(stringValue(raw["type"]))), "response.") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(stringValue(raw["object"])), "response") {
		return true
	}
	if _, ok := raw["response"].(map[string]any); ok {
		return true
	}
	return false
}

func sseEventLooksLikeChatCompletion(ev sseEvent) bool {
	data := strings.TrimSpace(ev.Data)
	if data == "" || data == "[DONE]" {
		return false
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(stringValue(raw["object"])), "chat.completion.chunk") {
		return true
	}
	if _, ok := raw["choices"].([]any); ok {
		if _, hasResponseType := raw["type"].(string); !hasResponseType {
			return true
		}
	}
	return false
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

func streamRawSSE(w http.ResponseWriter, buffered []sseEvent, reader *sseStreamReader, responseMode string) (usageTokens, error) {
	var best usageTokens
	if responseMode != "responses" {
		err := readSSEEvents(buffered, reader, func(event, data string) error {
			usage := usageFromSSEData(data)
			if usage.ResponseID != "" {
				best.ResponseID = usage.ResponseID
			}
			if usage.Model != "" {
				best.Model = usage.Model
			}
			if usage.Total > 0 {
				usage.ResponseID = firstNonEmpty(usage.ResponseID, best.ResponseID)
				usage.Model = firstNonEmpty(usage.Model, best.Model)
				best = usage
			}
			return writeSSEEvent(w, sseEvent{Event: event, Data: data})
		})
		return best, err
	}

	completedSeen := false
	failedSeen := false
	doneSent := false
	createdSeen := false
	outputStarted := false
	model := ""
	respID := "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	var textBuf strings.Builder

	sendDone := func() error {
		if doneSent {
			return nil
		}
		doneSent = true
		return writeSSEData(w, "[DONE]")
	}

	emitSyntheticFailure := func(streamErr error) error {
		message := "upstream stream ended before response.completed"
		if streamErr != nil && !errors.Is(streamErr, errResponsesStreamTerminal) {
			message += ": " + streamErr.Error()
		}
		if !createdSeen {
			if err := writeResponsesCreated(w, respID, model); err != nil {
				return err
			}
			createdSeen = true
		}
		if err := writeResponsesStreamFailure(w, respID, model, "upstream_stream_interrupted", message); err != nil {
			return err
		}
		failedSeen = true
		doneSent = true
		best.SoftFailure = message
		best.Status = "interrupted"
		return nil
	}

	err := readSSEEvents(buffered, reader, func(event, data string) error {
		trimmedData := strings.TrimSpace(data)
		if trimmedData == "" {
			return nil
		}
		if trimmedData == "[DONE]" {
			if !completedSeen && !failedSeen {
				if err := emitSyntheticFailure(errors.New("upstream sent [DONE] before response.completed")); err != nil {
					return err
				}
				return errResponsesStreamTerminal
			}
			if !doneSent {
				doneSent = true
				if err := writeSSEData(w, "[DONE]"); err != nil {
					return err
				}
			}
			return errResponsesStreamTerminal
		}
		var strict map[string]any
		if err := json.Unmarshal([]byte(trimmedData), &strict); err != nil {
			if writeErr := emitSyntheticFailure(fmt.Errorf("decode upstream responses stream event: %w", err)); writeErr != nil {
				return writeErr
			}
			return errResponsesStreamTerminal
		}
		if usage := usageFromSSEData(data); usage.Total > 0 {
			usage.ResponseID = firstNonEmpty(usage.ResponseID, best.ResponseID, respID)
			usage.Model = firstNonEmpty(usage.Model, best.Model, model)
			best = usage
		}
		if id, m := sseResponseIDAndModel(data); id != "" || m != "" {
			if id != "" {
				respID = id
				best.ResponseID = id
			}
			if m != "" {
				model = m
				best.Model = m
			}
		}

		ev := sseEvent{Event: event, Data: data}
		if failed, msg := streamEventFailure(ev); failed {
			failedSeen = true
			if err := writeResponsesStreamFailure(w, respID, model, "upstream_error", msg); err != nil {
				return err
			}
			best.SoftFailure = msg
			best.Status = "failed"
			doneSent = true
			return errResponsesStreamTerminal
		}

		typ := sseEventType(ev)
		if strings.HasPrefix(typ, "response.") && strings.TrimSpace(event) == "" {
			event = typ
		}
		switch typ {
		case "response.created":
			createdSeen = true
		case "response.output_item.added":
			if !createdSeen {
				if err := writeResponsesCreated(w, respID, model); err != nil {
					return err
				}
				createdSeen = true
			}
			outputStarted = true
		case "response.content_part.added":
			if !createdSeen {
				if err := writeResponsesCreated(w, respID, model); err != nil {
					return err
				}
				createdSeen = true
			}
		case "response.output_text.delta":
			if !createdSeen {
				if err := writeResponsesCreated(w, respID, model); err != nil {
					return err
				}
				createdSeen = true
			}
			if !outputStarted {
				if err := writeResponsesOutputStart(w, respID); err != nil {
					return err
				}
				outputStarted = true
			}
			if delta := responseDeltaText(data); delta != "" {
				textBuf.WriteString(delta)
			}
		case "response.completed", "response.done":
			if !createdSeen {
				if err := writeResponsesCreated(w, respID, model); err != nil {
					return err
				}
				createdSeen = true
			}
			completedSeen = true
			if typ == "response.done" {
				event = "response.completed"
				data = normalizeResponseDoneEventData(data, respID, model, textBuf.String(), best)
			}
			if err := writeSSEEvent(w, sseEvent{Event: event, Data: data}); err != nil {
				return err
			}
			if err := sendDone(); err != nil {
				return err
			}
			return errResponsesStreamTerminal
		case "response.failed", "response.cancelled":
			failedSeen = true
			if err := writeSSEEvent(w, sseEvent{Event: event, Data: data}); err != nil {
				return err
			}
			if err := sendDone(); err != nil {
				return err
			}
			return errResponsesStreamTerminal
		case "response.incomplete":
			failedSeen = true
			message := "上游响应未完整完成。"
			if msg := errorMessageFromJSON([]byte(data)); msg != "" {
				message = msg
			}
			if err := writeResponsesStreamFailure(w, respID, model, "upstream_incomplete", message); err != nil {
				return err
			}
			doneSent = true
			best.SoftFailure = message
			best.Status = "interrupted"
			return errResponsesStreamTerminal
		}
		return writeSSEEvent(w, sseEvent{Event: event, Data: data})
	})
	if errors.Is(err, errResponsesStreamTerminal) {
		return best, nil
	}
	if failedSeen {
		if writeErr := sendDone(); writeErr != nil {
			return best, writeErr
		}
		return best, nil
	}
	if !completedSeen {
		if writeErr := emitSyntheticFailure(err); writeErr != nil {
			return best, writeErr
		}
		return best, nil
	}
	if writeErr := sendDone(); writeErr != nil {
		return best, writeErr
	}
	if err != nil {
		best.SoftFailure = "upstream stream ended after response.completed: " + err.Error()
		return best, nil
	}
	return best, nil
}

func responseDeltaText(data string) string {
	var raw map[string]any
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		return ""
	}
	return strings.TrimSpace(stringValue(raw["delta"]))
}

func normalizeResponseDoneEventData(data, respID, model, text string, usage usageTokens) string {
	var raw map[string]any
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		return data
	}
	raw["type"] = "response.completed"
	resp, _ := raw["response"].(map[string]any)
	if resp == nil {
		raw["response"] = buildResponsesCompletedResponse(respID, model, responseItemID(respID), text, usage)
		return mustJSON(raw)
	}
	if strings.TrimSpace(stringValue(resp["id"])) == "" && strings.TrimSpace(respID) != "" {
		resp["id"] = respID
	}
	if strings.TrimSpace(stringValue(resp["object"])) == "" {
		resp["object"] = "response"
	}
	resp["status"] = "completed"
	if strings.TrimSpace(stringValue(resp["model"])) == "" && strings.TrimSpace(model) != "" {
		resp["model"] = model
	}
	if _, ok := resp["usage"]; !ok && usage.Total > 0 {
		resp["usage"] = map[string]int64{
			"input_tokens":  usage.Prompt,
			"output_tokens": usage.Completion,
			"total_tokens":  usage.Total,
		}
	}
	if _, ok := resp["output_text"]; !ok && strings.TrimSpace(text) != "" {
		resp["output_text"] = text
	}
	return mustJSON(raw)
}

// sseResponseIDAndModel 从一个 responses SSE data 里尽量提取 response id 和 model。
func sseResponseIDAndModel(data string) (string, string) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		return "", ""
	}
	id := ""
	model := ""
	if resp, ok := raw["response"].(map[string]any); ok {
		if v, ok := resp["id"].(string); ok {
			id = v
		}
		if v, ok := resp["model"].(string); ok {
			model = v
		}
	}
	if id == "" {
		if v, ok := raw["response_id"].(string); ok {
			id = v
		}
	}
	if model == "" {
		if v, ok := raw["model"].(string); ok {
			model = v
		}
	}
	return id, model
}

func writeSSEEvent(w http.ResponseWriter, ev sseEvent) error {
	if sseEventMarksFirstToken(ev) {
		markFirstToken(w)
	}
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
	if chatStreamChunkHasToken(delta) {
		markFirstToken(w)
	}
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
	if strings.Contains(strings.ToLower(event), "delta") {
		markFirstToken(w)
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

func writeSSEHeartbeat(w http.ResponseWriter) error {
	if _, err := fmt.Fprint(w, ": upstream-ops ping\n\n"); err != nil {
		return err
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}

func sseEventMarksFirstToken(ev sseEvent) bool {
	typ := strings.ToLower(strings.TrimSpace(sseEventType(ev)))
	event := strings.ToLower(strings.TrimSpace(ev.Event))
	return strings.Contains(typ, ".delta") ||
		strings.Contains(typ, "_delta") ||
		strings.Contains(event, ".delta") ||
		strings.Contains(event, "_delta")
}

func chatStreamChunkHasToken(delta map[string]any) bool {
	if delta == nil {
		return false
	}
	if content := stringValue(delta["content"]); content != "" {
		return true
	}
	return len(delta) > 0 && delta["role"] == nil
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
		if responseID := responseIDFromMap(raw); responseID != "" {
			best.ResponseID = responseID
		}
		if model := responseModelFromMap(raw); model != "" {
			best.Model = model
		}
		if usageRaw, ok := raw["usage"].(map[string]any); ok {
			if usage := usageFromMap(usageRaw); usage.Total > 0 {
				usage.ResponseID = firstNonEmpty(usage.ResponseID, best.ResponseID)
				usage.Model = firstNonEmpty(usage.Model, best.Model)
				best = usage
			}
		}
		if responseRaw, ok := raw["response"].(map[string]any); ok {
			if usageRaw, ok := responseRaw["usage"].(map[string]any); ok {
				if usage := usageFromMap(usageRaw); usage.Total > 0 {
					usage.ResponseID = firstNonEmpty(usage.ResponseID, best.ResponseID)
					usage.Model = firstNonEmpty(usage.Model, best.Model)
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
	if key.BalanceLimit > 0 && key.TotalCost >= key.BalanceLimit {
		return errors.New("gateway key balance exhausted")
	}
	if key.ExpiresAt != nil && time.Now().After(*key.ExpiresAt) {
		return errors.New("gateway key expired")
	}
	return nil
}

func (s *Service) publicGatewayLimitOrExpiredMessage(rawKey string, key *storage.GatewayKey, cause error) (string, bool) {
	if key == nil && s != nil && s.gateway != nil {
		rawKey = strings.TrimSpace(rawKey)
		if rawKey != "" {
			found, err := s.gateway.FindByHash(HashKey(rawKey))
			if err == nil {
				key = found
			} else if s.log != nil {
				s.log.Warn("lookup public gateway key for friendly stream failed", "err", err)
			}
		}
	}
	return publicGatewayLimitOrExpiredMessage(key, cause, time.Now())
}

func publicGatewayLimitOrExpiredMessage(key *storage.GatewayKey, cause error, now time.Time) (string, bool) {
	if key == nil || !key.IsPublic {
		return "", false
	}
	lower := ""
	if cause != nil {
		lower = strings.ToLower(cause.Error())
	}
	if publicGatewayKeyExpired(key, now) || strings.Contains(lower, "expired") {
		return publicGatewayExpiredMessage, true
	}
	if publicGatewayKeyQuotaExhausted(key, now) || gatewayQuotaError(cause) {
		return publicGatewayQuotaExhaustedMessage, true
	}
	return "", false
}

func publicGatewayKeyExpired(key *storage.GatewayKey, now time.Time) bool {
	return key != nil && key.ExpiresAt != nil && now.After(*key.ExpiresAt)
}

func publicGatewayKeyQuotaExhausted(key *storage.GatewayKey, now time.Time) bool {
	if key == nil {
		return false
	}
	todayTokens := key.TodayTokens
	if key.UsageDate != now.Format("2006-01-02") {
		todayTokens = 0
	}
	return (key.DailyLimit > 0 && todayTokens >= key.DailyLimit) ||
		(key.TotalLimit > 0 && key.TotalTokens >= key.TotalLimit) ||
		(key.BalanceLimit > 0 && key.TotalCost >= key.BalanceLimit)
}

func gatewayQuotaError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "daily token limit") ||
		strings.Contains(lower, "total token limit") ||
		strings.Contains(lower, "balance exhausted") ||
		strings.Contains(lower, "quota")
}

func cacheHitRate(cachedTokens, promptTokens int64) float64 {
	if promptTokens <= 0 {
		return 0
	}
	if cachedTokens < 0 {
		cachedTokens = 0
	}
	if cachedTokens > promptTokens {
		cachedTokens = promptTokens
	}
	return float64(cachedTokens) / float64(promptTokens)
}

func gatewayKeyOutput(key storage.GatewayKey) GatewayKeyOutput {
	todayTokens := key.TodayTokens
	todayCost := key.TodayCost
	todayPromptTokens := key.TodayPromptTokens
	todayCachedTokens := key.TodayCachedTokens
	if key.UsageDate != "" && key.UsageDate != time.Now().Format("2006-01-02") {
		todayTokens = 0
		todayCost = 0
		todayPromptTokens = 0
		todayCachedTokens = 0
	}
	balanceRemaining := 0.0
	if key.BalanceLimit > 0 {
		balanceRemaining = math.Max(0, key.BalanceLimit-key.TotalCost)
	}
	return GatewayKeyOutput{
		ID:                 key.ID,
		Name:               key.Name,
		KeyPrefix:          key.KeyPrefix,
		Enabled:            key.Enabled,
		DailyLimit:         key.DailyLimit,
		TotalLimit:         key.TotalLimit,
		TodayTokens:        todayTokens,
		TotalTokens:        key.TotalTokens,
		TodayPromptTokens:  todayPromptTokens,
		TotalPromptTokens:  key.TotalPromptTokens,
		TodayCachedTokens:  todayCachedTokens,
		TotalCachedTokens:  key.TotalCachedTokens,
		TodayCacheHitRate:  cacheHitRate(todayCachedTokens, todayPromptTokens),
		TotalCacheHitRate:  cacheHitRate(key.TotalCachedTokens, key.TotalPromptTokens),
		CostPerMillion:     key.CostPerMillion,
		BalanceLimit:       key.BalanceLimit,
		ConcurrencyLimit:   key.ConcurrencyLimit,
		BalanceRemaining:   balanceRemaining,
		TodayCost:          todayCost,
		TotalCost:          key.TotalCost,
		UsageDate:          key.UsageDate,
		ExpiresAt:          key.ExpiresAt,
		IsPublic:           key.IsPublic,
		PublicName:         key.PublicName,
		PublicPasswordHint: key.PublicPasswordHint,
		LastUsedAt:         key.LastUsedAt,
		LastUsedIP:         key.LastUsedIP,
		ClientFormat:       normalizeClientFormat(key.ClientFormat),
		AllowedGroupScope:  normalizeGatewayGroupScope(key.AllowedGroupScope, decodeUintList(key.AllowedGroupIDs)),
		AllowedGroupIDs:    decodeUintList(key.AllowedGroupIDs),
		CreatedAt:          key.CreatedAt,
		UpdatedAt:          key.UpdatedAt,
	}
}

func gatewayKeyUsageOutput(key storage.GatewayKey) GatewayKeyUsageOutput {
	out := gatewayKeyOutput(key)
	return GatewayKeyUsageOutput{
		ID:                out.ID,
		Name:              out.Name,
		KeyPrefix:         out.KeyPrefix,
		TodayTokens:       out.TodayTokens,
		TodayCost:         out.TodayCost,
		TotalTokens:       out.TotalTokens,
		TotalCost:         out.TotalCost,
		TodayPromptTokens: out.TodayPromptTokens,
		TotalPromptTokens: out.TotalPromptTokens,
		TodayCachedTokens: out.TodayCachedTokens,
		TotalCachedTokens: out.TotalCachedTokens,
		TodayCacheHitRate: out.TodayCacheHitRate,
		TotalCacheHitRate: out.TotalCacheHitRate,
		CostPerMillion:    out.CostPerMillion,
		BalanceLimit:      out.BalanceLimit,
		BalanceRemaining:  out.BalanceRemaining,
		UsageDate:         out.UsageDate,
	}
}

func publicGatewayKeyOutput(key *storage.GatewayKey) *PublicGatewayKeyOutput {
	if key == nil {
		return nil
	}
	todayTokens := key.TodayTokens
	todayPromptTokens := key.TodayPromptTokens
	todayCachedTokens := key.TodayCachedTokens
	if key.UsageDate != "" && key.UsageDate != time.Now().Format("2006-01-02") {
		todayTokens = 0
		todayPromptTokens = 0
		todayCachedTokens = 0
	}
	name := strings.TrimSpace(key.PublicName)
	if name == "" {
		name = key.Name
	}
	return &PublicGatewayKeyOutput{
		ID:                key.ID,
		Enabled:           key.IsPublic && key.Enabled,
		Name:              name,
		KeyPrefix:         key.KeyPrefix,
		PasswordRequired:  key.PublicPasswordCipher != "",
		PasswordHint:      key.PublicPasswordHint,
		ExpiresAt:         key.ExpiresAt,
		TodayTokens:       todayTokens,
		TotalTokens:       key.TotalTokens,
		TodayPromptTokens: todayPromptTokens,
		TotalPromptTokens: key.TotalPromptTokens,
		TodayCachedTokens: todayCachedTokens,
		TotalCachedTokens: key.TotalCachedTokens,
		TodayCacheHitRate: cacheHitRate(todayCachedTokens, todayPromptTokens),
		TotalCacheHitRate: cacheHitRate(key.TotalCachedTokens, key.TotalPromptTokens),
		LastUsedAt:        key.LastUsedAt,
	}
}

func (s *Service) publicGatewayKeyOutput(key *storage.GatewayKey) *PublicGatewayKeyOutput {
	out := publicGatewayKeyOutput(key)
	if out == nil || s == nil || s.cipher == nil {
		return out
	}
	raw, err := s.cipher.Decrypt(key.KeyCipher)
	if err != nil {
		return out
	}
	out.MaskedKey = maskGatewayKey(raw)
	return out
}

func maskGatewayKey(key string) string {
	if key == "" {
		return ""
	}
	if len(key) <= 12 {
		return "********"
	}
	return key[:6] + "******" + key[len(key)-4:]
}

func filterCandidatesForGatewayKey(key *storage.GatewayKey, candidates []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	if key == nil {
		return candidates
	}
	ids := decodeUintList(key.AllowedGroupIDs)
	scope := normalizeGatewayGroupScope(key.AllowedGroupScope, ids)
	if scope == gatewayGroupScopeAll {
		return candidates
	}
	out := make([]storage.UpstreamGroupKey, 0, len(candidates))
	allowed := decodeUintSet(key.AllowedGroupIDs)
	for _, candidate := range candidates {
		switch scope {
		case gatewayGroupScopeSelected:
			if allowed[candidate.ID] {
				out = append(out, candidate)
			}
		case gatewayGroupScopeCharity:
			if candidate.Charity {
				out = append(out, candidate)
			}
		case gatewayGroupScopeNormal:
			if !candidate.Charity {
				out = append(out, candidate)
			}
		}
	}
	return out
}

func temporaryCooldownFallbackCandidate(candidate storage.UpstreamGroupKey) bool {
	switch strings.ToLower(strings.TrimSpace(candidate.Status)) {
	case "", "alive", "unknown", "rate_limited", "dead", "server_error", "timeout", "network_error", "upstream_error":
		return true
	default:
		return false
	}
}

func sortCooldownFallbackCandidates(candidates []storage.UpstreamGroupKey, now time.Time) []storage.UpstreamGroupKey {
	out := append([]storage.UpstreamGroupKey(nil), candidates...)
	sort.SliceStable(out, func(i, j int) bool {
		untilI, okI := candidateCooldownUntil(out[i], now)
		untilJ, okJ := candidateCooldownUntil(out[j], now)
		if okI && okJ && !untilI.Equal(untilJ) {
			return untilI.Before(untilJ)
		}
		if okI != okJ {
			return okI
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func cooldownFallbackMessage(candidates []storage.UpstreamGroupKey) string {
	if len(candidates) == 0 {
		return ""
	}
	names := make([]string, 0, minInt(3, len(candidates)))
	for i, candidate := range candidates {
		if i >= 3 {
			break
		}
		name := strings.TrimSpace(candidate.ChannelName + "/" + candidate.GroupName)
		if name == "/" {
			name = fmt.Sprintf("#%d", candidate.ID)
		}
		names = append(names, name)
	}
	return "all matching upstream groups are cooling down; probing fallback: " + strings.Join(names, " | ")
}

func anyCharityCandidate(candidates []storage.UpstreamGroupKey) bool {
	for _, candidate := range candidates {
		if candidate.Charity {
			return true
		}
	}
	return false
}

func anyNormalCandidate(candidates []storage.UpstreamGroupKey) bool {
	for _, candidate := range candidates {
		if !candidate.Charity {
			return true
		}
	}
	return false
}

func gatewayKeyScopeEmptyMessage(key *storage.GatewayKey) string {
	if key == nil {
		return "no alive upstream group keys available"
	}
	scope := normalizeGatewayGroupScope(key.AllowedGroupScope, decodeUintList(key.AllowedGroupIDs))
	switch scope {
	case gatewayGroupScopeSelected:
		return "this gateway key has no matching selected upstream group keys available"
	case gatewayGroupScopeCharity:
		return "no charity upstream group keys available for this gateway key"
	case gatewayGroupScopeNormal:
		return "no non-charity upstream group keys available for this gateway key"
	default:
		return "no alive upstream group keys available"
	}
}

func gatewayKeyScopeLabel(key *storage.GatewayKey) string {
	if key == nil {
		return "all"
	}
	switch normalizeGatewayGroupScope(key.AllowedGroupScope, decodeUintList(key.AllowedGroupIDs)) {
	case gatewayGroupScopeSelected:
		return "selected"
	case gatewayGroupScopeCharity:
		return "charity"
	case gatewayGroupScopeNormal:
		return "non-charity"
	default:
		return "all"
	}
}

func candidateScopeFallbackAllowed(key *storage.GatewayKey, candidates []storage.UpstreamGroupKey) bool {
	scope := normalizeGatewayGroupScope("", nil)
	if key != nil {
		scope = normalizeGatewayGroupScope(key.AllowedGroupScope, decodeUintList(key.AllowedGroupIDs))
	}
	switch scope {
	case gatewayGroupScopeCharity:
		return anyCharityCandidate(candidates)
	case gatewayGroupScopeNormal:
		return anyNormalCandidate(candidates)
	case gatewayGroupScopeSelected:
		return len(candidates) > 0
	default:
		return len(candidates) > 0
	}
}

func filterCooldownFallbackCandidates(candidates []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	out := make([]storage.UpstreamGroupKey, 0, len(candidates))
	for _, candidate := range candidates {
		if temporaryCooldownFallbackCandidate(candidate) {
			out = append(out, candidate)
		}
	}
	return out
}

func filterCandidatesForClientFormat(keyFormat, responseMode string, candidates []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	format := normalizeClientFormat(keyFormat)
	if format == "any" {
		format = normalizeClientFormat(responseMode)
	}
	out := make([]storage.UpstreamGroupKey, 0, len(candidates))
	for _, candidate := range candidates {
		candidateFormat := normalizeClientFormat(candidate.ClientFormat)
		if candidateFormat == "any" || candidateFormat == format {
			out = append(out, candidate)
		}
	}
	return out
}

func validateClientFormat(format string, responseMode string) error {
	format = normalizeClientFormat(format)
	switch format {
	case "any":
		return nil
	case "claude":
		if responseMode != "claude" {
			return errors.New("this gateway key only accepts Claude Messages requests")
		}
	case "grok":
		if responseMode == "claude" {
			return errors.New("this gateway key only accepts Grok OpenAI-compatible requests")
		}
	default:
		if responseMode == "claude" {
			return errors.New("this gateway key only accepts OpenAI-compatible requests")
		}
	}
	return nil
}

func inferGroupClientFormat(name, description string) string {
	text := strings.ToLower(strings.TrimSpace(name + " " + description))
	for _, marker := range []string{"claude", "anthropic", "opus", "sonnet", "haiku"} {
		if strings.Contains(text, marker) {
			return "claude"
		}
	}
	// Short aliases must be matched as tokens. A substring check would turn
	// unrelated names such as "classic" or "account" into Claude channels.
	for _, token := range strings.FieldsFunc(text, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'))
	}) {
		if token == "cc" || token == "cs" || token == "kiro" || token == "max" {
			return "claude"
		}
	}
	for _, marker := range []string{"grok", "xai", "x.ai"} {
		if strings.Contains(text, marker) {
			return "grok"
		}
	}
	return "openai"
}

func normalizeClientFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "claude":
		return "claude"
	case "grok", "xai", "x.ai":
		return "grok"
	case "any":
		return "any"
	default:
		return "openai"
	}
}

func normalizeUpstreamRequestMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "chat", "chat_completions", "chat-completions", "completions":
		return "chat"
	default:
		return "responses"
	}
}

func encodeUintList(values []uint) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(values))
	seen := map[uint]bool{}
	for _, value := range values {
		if value == 0 || seen[value] {
			continue
		}
		seen[value] = true
		parts = append(parts, strconv.FormatUint(uint64(value), 10))
	}
	return strings.Join(parts, ",")
}

func decodeUintList(raw string) []uint {
	set := decodeUintSet(raw)
	out := make([]uint, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func decodeUintSet(raw string) map[uint]bool {
	out := map[uint]bool{}
	for _, part := range strings.Split(raw, ",") {
		n, err := strconv.ParseUint(strings.TrimSpace(part), 10, 64)
		if err != nil || n == 0 {
			continue
		}
		out[uint(n)] = true
	}
	return out
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
	// OpenAI/Codex may provide a stable prompt-cache key even when each turn's
	// input is different. Pinning that cache family to the same upstream keeps
	// provider-side prompt caching eligible instead of bouncing between keys.
	if key := strings.TrimSpace(stringValue(raw["prompt_cache_key"])); key != "" {
		return "prompt-cache:" + key
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

func rawQueryFromPath(path string) string {
	if idx := strings.Index(path, "?"); idx >= 0 {
		return path[idx+1:]
	}
	return ""
}

func requestWantsStream(r *http.Request, body []byte, rawQuery string) bool {
	if requestStream(body) {
		return true
	}
	if strings.TrimSpace(rawQuery) == "" && r != nil && r.URL != nil {
		rawQuery = r.URL.RawQuery
	}
	if rawQuery != "" {
		values, err := url.ParseQuery(rawQuery)
		if err == nil {
			switch strings.ToLower(strings.TrimSpace(values.Get("stream"))) {
			case "1", "true", "yes", "on":
				return true
			}
		}
	}
	if r != nil && strings.Contains(strings.ToLower(r.Header.Get("Accept")), "text/event-stream") {
		return true
	}
	return false
}

func requestStream(body []byte) bool {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return false
	}
	return boolField(raw, "stream")
}

func ensureRequestStreamFlag(body []byte, stream bool) []byte {
	if !stream {
		return body
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return body
	}
	raw["stream"] = true
	out, err := json.Marshal(raw)
	if err != nil {
		return body
	}
	return out
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
	siteURL = strings.TrimSpace(siteURL)
	if siteURL == "" {
		return "", errors.New("upstream base URL is empty")
	}
	base, err := url.Parse(strings.TrimRight(siteURL, "/"))
	if err != nil {
		return "", err
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return "", fmt.Errorf("upstream base URL must start with http:// or https://: %s", siteURL)
	}
	if base.Host == "" {
		return "", fmt.Errorf("upstream base URL host is empty: %s", siteURL)
	}
	rawQuery := ""
	if idx := strings.Index(path, "?"); idx >= 0 {
		rawQuery = path[idx+1:]
		path = path[:idx]
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	basePath := strings.TrimRight(base.Path, "/")
	if strings.HasSuffix(strings.ToLower(basePath), "/v1") && (path == "/v1" || strings.HasPrefix(path, "/v1/")) {
		path = strings.TrimPrefix(path, "/v1")
		if path == "" {
			path = "/"
		}
	}
	base.Path = basePath + path
	base.RawQuery = rawQuery
	return base.String(), nil
}

func sanitizeManualSecret(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "\"'")
	return strings.TrimSpace(value)
}

func normalizeManualAPIBaseURL(siteURL string) (string, error) {
	siteURL = strings.TrimSpace(siteURL)
	siteURL = strings.Trim(siteURL, "\"'")
	siteURL = strings.TrimSpace(siteURL)
	if siteURL == "" {
		return "", nil
	}
	parsed, err := url.Parse(strings.TrimRight(siteURL, "/"))
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("上游地址必须以 http:// 或 https:// 开头: %s", siteURL)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("上游地址 host 为空: %s", siteURL)
	}
	pathLower := strings.ToLower(strings.TrimRight(parsed.Path, "/"))
	for _, marker := range []string{"/admin", "/dashboard", "/console", "/login"} {
		if pathLower == marker || strings.HasSuffix(pathLower, marker) || strings.Contains(pathLower, marker+"/") {
			return "", errors.New("上游地址看起来是管理后台页面，请填写 API Base URL，例如 https://example.com 或 https://example.com/v1")
		}
	}
	for _, suffix := range []string{"/v1/chat/completions", "/v1/responses", "/v1/models", "/chat/completions", "/responses", "/models"} {
		if strings.HasSuffix(pathLower, suffix) {
			parsed.Path = strings.TrimRight(parsed.Path[:len(parsed.Path)-len(suffix)], "/")
			break
		}
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/"), nil
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
	case "rate_limited":
		return 2
	case "dead", "server_error", "timeout", "network_error", "upstream_error":
		return 3
	case "zero_balance", "forbidden", "auth_failed", "model_error", "invalid_request", "non_generation":
		return 4
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
	// 模型不存在/无权限这类错误，哪怕上游塞进了 invalid_request_error 或 422，
	// 也应该继续换下一个候选，而不是把第一次撞到的渠道当成“客户端参数错”。
	if looksLikeUnsupportedModelError(msg) {
		return true
	}
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
			strings.Contains(s, "model_not_found") ||
			strings.Contains(s, "does not exist") ||
			strings.Contains(s, "doesn't exist") ||
			strings.Contains(s, "not exist") ||
			strings.Contains(s, "no such model") ||
			strings.Contains(s, "not support") ||
			strings.Contains(s, "does not support") ||
			strings.Contains(s, "unsupported") ||
			strings.Contains(s, "model_not_supported") ||
			strings.Contains(s, "model_not_available") ||
			strings.Contains(s, "unsupported_model") ||
			strings.Contains(s, "invalid model") ||
			strings.Contains(s, "invalid_model") ||
			strings.Contains(s, "unknown model") ||
			strings.Contains(s, "model is invalid") ||
			strings.Contains(s, "model invalid") ||
			strings.Contains(s, "unavailable") ||
			strings.Contains(s, "not available") ||
			strings.Contains(s, "no available") ||
			strings.Contains(s, "do not have access") ||
			strings.Contains(s, "don't have access") ||
			strings.Contains(s, "not have access") ||
			strings.Contains(s, "no access") ||
			strings.Contains(s, "without access") ||
			strings.Contains(s, "not enabled") ||
			strings.Contains(s, "not allowed") ||
			strings.Contains(s, "not permitted") ||
			strings.Contains(s, "permission") ||
			strings.Contains(s, "不存在") ||
			strings.Contains(s, "不支持") ||
			strings.Contains(s, "不可用") ||
			strings.Contains(s, "无可用") ||
			strings.Contains(s, "没有可用") ||
			strings.Contains(s, "没有权限") ||
			strings.Contains(s, "无权限") ||
			strings.Contains(s, "无访问权限") ||
			strings.Contains(s, "没有访问权限") ||
			strings.Contains(s, "无权访问") ||
			strings.Contains(s, "不能访问") ||
			strings.Contains(s, "无法访问") ||
			strings.Contains(s, "未开通") ||
			strings.Contains(s, "未开放"))
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
