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
	"unicode"

	"github.com/bejix/upstream-ops/backend/channel"
	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/connector"
	appcrypto "github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/oauthpool"
	"github.com/bejix/upstream-ops/backend/progress"
	"github.com/bejix/upstream-ops/backend/sanitize"
	"github.com/bejix/upstream-ops/backend/storage"
	"gorm.io/gorm"
)

const (
	gatewayKeyPrefix = "sk-"
	healthPath       = "/v1/models"
	responsesPath    = "/v1/responses"

	proxyAttemptTimeout       = 45 * time.Second
	healthProbeTimeout        = 6 * time.Second
	healthProbeRunTimeout     = 20 * time.Second
	healthProbeRetryJitterMax = 3 * time.Second
	healthTransientAttempts   = 1
	healthPerChannelParallel  = 1
	// Some upstream routers rate-limit short probe bursts even when ordinary
	// user traffic is healthy. Keys at the same API base are already serialized;
	// leave a small gap before the next key is probed as well.
	healthProbeUpstreamMinInterval = 500 * time.Millisecond
	// streamFirstEventTimeout covers response headers plus the first usable SSE
	// generation/tool event. Before that semantic boundary no upstream bytes are
	// exposed, so a stalled relay can still be replaced safely. Once output starts,
	// streamIdleTimeout below preserves long-running reasoning streams.
	streamFirstEventTimeout = 3 * time.Second
	// streamIdleTimeout 是正式转发阶段"两个事件之间"的最长间隔。推理模型两次事件
	// 之间可能有较长停顿；超过后才认为上游卡死，并由 Responses 兜底逻辑补终态/[DONE]。
	streamIdleTimeout       = 5 * time.Minute
	streamHeartbeatInterval = 15 * time.Second
	streamPreflightTimeout  = 3 * time.Second
	// streamPreflightMaxEvents / streamPreflightMaxBytes 是首字节落地前的缓冲上限。
	// 正常流一旦确认出现了可见输出且不命中拦截词就立即放行，不会等满上限，因此调大
	// 上限不增加正常请求的首字节延迟；它只对"疑似拦截、持续 hold"的流生效。适度放宽到
	// 24 事件 / 96KB，覆盖拦截文案出现在稍靠后位置（前面先吐了少量 reasoning/占位）
	// 的情况，让"公益token休息了 / 当前没有可用上游"等软失败仍能在写首字节前被拦下并
	// 丝滑切换到下一个候选，而不是把这句话直接透传给 Codex 造成断流。
	streamPreflightMaxEvents        = 24
	streamPreflightMaxBytes         = 96 << 10
	healthTransientFailureThreshold = 3
	healthTransientFailureCooldown  = 45 * time.Second
	healthServerErrorCooldown       = 60 * time.Second
	healthTimeoutCooldown           = 75 * time.Second
	healthNetworkErrorCooldown      = 30 * time.Second
	healthRateLimitCooldown         = 90 * time.Second
	proxyPermanentFailureCooldown   = 30 * time.Minute
	// proxyHintedCooldownMax 是"上游明确给出 Retry-After / reset_after_seconds 提示"时
	// 允许的最长冷却。无提示的临时故障仍走 proxyPermanentFailureCooldown(30m) 封顶；但上游
	// 明说的长窗口（多小时 / 多天的配额耗尽）应被尊重，否则会每 30 分钟反复探测一个明知
	// 还没恢复的渠道、白白消耗 fail-over 预算。8 天与常见 7 天配额窗口留一天余量对齐。
	proxyHintedCooldownMax       = 8 * 24 * time.Hour
	modelSupportPositiveTTL      = 2 * time.Hour
	modelSupportNegativeTTL      = 15 * time.Minute
	defaultHealthProbeBatchSize  = 10
	automaticHealthProbeMaxRatio = 0.1

	openAIHealthProbePrimaryModel  = "gpt-5.4"
	openAIHealthProbeFallbackModel = "gpt-5.5"
	healthProbePrompt              = "1+1="
	healthProbeMaxOutputTokens     = 4
)

var errResponsesStreamTerminal = errors.New("responses stream terminal event emitted")

var builtinModelPriceRules = []modelPriceRule{
	// Keep these values explicit and local so accounting never depends on
	// upstream-reported usage/cost fields.
	{Prefix: "gpt-5.4-mini", Price: modelPrice{InputPerMillion: 5, CachedInputPerMillion: 0.5, OutputPerMillion: 30}},
	{Prefix: "gpt-5.4", Price: modelPrice{InputPerMillion: 5, CachedInputPerMillion: 0.5, OutputPerMillion: 30}},
	{Prefix: "gpt-5.5", Price: modelPrice{InputPerMillion: 5, CachedInputPerMillion: 0.5, OutputPerMillion: 30}},
	{Prefix: "gpt-5.6", Price: modelPrice{InputPerMillion: 5, CachedInputPerMillion: 0.5, OutputPerMillion: 30}},
}

var defaultGrokPoolModelCatalog = []string{
	"grok-4.5",
	"grok-4.3",
	"grok-code-fast-1",
	"grok-chat-fast",
}

type Service struct {
	channels          *storage.Channels
	gateway           *storage.GatewayKeys
	affinities        *storage.GatewayAffinities
	groupKeys         *storage.UpstreamGroupKeys
	usageLogs         *storage.UsageLogs
	ipPolicies        *storage.IPPolicies
	cipher            *appcrypto.Cipher
	channelSvc        *channel.Service
	log               *slog.Logger
	clients           sync.Map
	runtime           sync.Map
	keyRuntime        sync.Map
	ipRuntime         sync.Map
	healthProbeSlots  sync.Map
	healthJobs        sync.Map
	dispatchCursors   sync.Map
	healthJobMu       sync.Mutex
	candidateCache    candidateSnapshotCache
	configMu          sync.RWMutex
	upstream          config.UpstreamConfig
	app               config.AppConfig
	proxy             config.ProxyConfig
	oauthPool         *oauthpool.Service
	oauthAccounts     *storage.OAuthAccounts
	oauthFailureDirty sync.Map
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
	MaxGroupRatio     float64 `json:"max_group_ratio"`
	RoutePreference   string  `json:"route_preference"`
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
	MaxGroupRatio     *float64   `json:"max_group_ratio"`
	RoutePreference   *string    `json:"route_preference"`
	ExpiresInDays     *int       `json:"expires_in_days"`
	ExpiresAt         *time.Time `json:"expires_at"`
	DisabledMessage   *string    `json:"disabled_message"`
}

type BatchDisableGatewayKeysInput struct {
	IDs     []uint `json:"ids"`
	Message string `json:"message"`
}

type IPPolicyInput struct {
	IP                      string `json:"ip"`
	Blocked                 bool   `json:"blocked"`
	PublicConcurrencyExempt bool   `json:"public_concurrency_exempt"`
	Note                    string `json:"note"`
	// BlockedMessage 是命中封禁时回给客户端的自定义文案。留空时用兜底
	// gatewayIPBannedMessage（"IP已被封禁"），参考 GatewayKey.DisabledMessage 的做法。
	BlockedMessage string `json:"blocked_message"`
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
	MaxGroupRatio      float64    `json:"max_group_ratio"`
	RoutePreference    string     `json:"route_preference"`
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
	DisabledMessage    string     `json:"disabled_message,omitempty"`
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
	Created         int             `json:"created"`
	Updated         int             `json:"updated"`
	Skipped         int             `json:"skipped"`
	Failed          int             `json:"failed"`
	Removed         int             `json:"removed"`
	ModelsSynced    int             `json:"models_synced"`
	ModelSyncFailed int             `json:"model_sync_failed"`
	Items           []BootstrapItem `json:"items"`
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
// The upstream names are not normalized and may use Chinese names or English
// aliases such as image / im2, so every listed keyword is matched broadly.
var bootstrapKeyBlockKeywords = []string{"图", "image", "img", "im2", "ban", "香蕉"}

func blockedBootstrapKeyKeyword(group connector.APIKeyGroup) (string, bool) {
	// The exclusion is intentionally scoped to the discovered group. A channel
	// title such as "BananAI" must not suppress all of its normal text groups.
	text := strings.ToLower(strings.Join([]string{group.Name, group.Description}, " "))
	for _, keyword := range bootstrapKeyBlockKeywords {
		if strings.Contains(text, strings.ToLower(keyword)) {
			return keyword, true
		}
	}
	return "", false
}

func isManualBootstrapChannel(ch storage.Channel) bool {
	return ch.CredentialMode == storage.CredentialModeToken &&
		strings.EqualFold(strings.TrimSpace(ch.Username), "manual")
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
	// MaxRatio limits a batch by effective ratio. A zero value leaves direct
	// service callers unrestricted; the dashboard's one-click policy sets 0.1.
	MaxRatio float64
	// Serial makes a batch strictly one-at-a-time. InterGroupDelay is applied
	// after a completed probe and before the next probe begins.
	Serial          bool
	InterGroupDelay time.Duration
}

// HealthJobOutput is a durable-in-process snapshot of a background one-click
// health check. The browser can poll it after navigation or reload; the check
// itself never depends on an SSE connection remaining open.
type HealthJobOutput struct {
	ID         string        `json:"id"`
	Status     string        `json:"status"`
	Message    string        `json:"message,omitempty"`
	Total      int           `json:"total"`
	Completed  int           `json:"completed"`
	StartedAt  time.Time     `json:"started_at"`
	FinishedAt *time.Time    `json:"finished_at,omitempty"`
	Result     *HealthResult `json:"result,omitempty"`
	Error      string        `json:"error,omitempty"`
}

type healthJob struct {
	mu  sync.RWMutex
	out HealthJobOutput
}

func (j *healthJob) snapshot() HealthJobOutput {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.out
}

type healthJobObserver struct{ job *healthJob }

func (o healthJobObserver) Emit(event progress.Event) {
	if o.job == nil {
		return
	}
	o.job.mu.Lock()
	defer o.job.mu.Unlock()
	if event.Message != "" {
		o.job.out.Message = event.Message
	}
	if event.Total > 0 {
		o.job.out.Total = event.Total
	}
	if event.Index > o.job.out.Completed {
		o.job.out.Completed = event.Index
	}
	if event.Stage == progress.StageError {
		o.job.out.Error = event.Message
	}
}

// OneClickHealthTestOptions returns the safe policy used by the dashboard's
// one-click check. Different API base URLs run in parallel, while the
// service-scoped upstream slots keep keys at the same API base serialized.
func OneClickHealthTestOptions(groupIDs []uint) HealthTestOptions {
	opts := HealthTestOptions{
		BatchSize: defaultHealthProbeBatchSize,
		GroupIDs:  groupIDs,
	}
	// MaxRatio 是"全量兜底扫描"时用来控制成本的上限（只测低倍率/公益渠道），
	// 定时任务与"不指定任何分组"的一键测活会走这里。但当调用方明确传入 groupIDs
	// 时，说明用户在前端主动勾选了要测的分组——哪怕是高倍率渠道也应当尊重其意图，
	// 否则前端把它标成"排队中"、后端却因倍率超限静默跳过，表现为"点了测活没反应"。
	if len(groupIDs) == 0 {
		opts.MaxRatio = automaticHealthProbeMaxRatio
	}
	return opts
}

// OneClickHealthTestOptions returns the dashboard policy with the service's
// currently applied full-scan ratio cap. Explicitly selected groups remain
// uncapped, matching the background-job and SSE execution paths.
func (s *Service) OneClickHealthTestOptions(groupIDs []uint) HealthTestOptions {
	opts := OneClickHealthTestOptions(groupIDs)
	if len(groupIDs) == 0 {
		opts.MaxRatio = s.healthProbeMaxRatio()
	}
	return opts
}

// StartOneClickHealthJob starts the low-cost OpenAI probe
// in the background. It is intentionally detached from the HTTP request so a
// slow large batch does not make the control page appear frozen or get
// cancelled when the browser navigates away.
func (s *Service) StartOneClickHealthJob(groupIDs []uint) (*HealthJobOutput, error) {
	if s == nil || s.groupKeys == nil {
		return nil, errors.New("gateway health service is unavailable")
	}
	// Keep at most one dashboard batch running per gateway process. Individual
	// API bases are still serialized inside that batch.
	s.healthJobMu.Lock()
	defer s.healthJobMu.Unlock()
	var active *HealthJobOutput
	s.healthJobs.Range(func(_, value any) bool {
		job, ok := value.(*healthJob)
		if !ok {
			return true
		}
		snapshot := job.snapshot()
		if snapshot.Status == "running" {
			active = &snapshot
			return false
		}
		return true
	})
	if active != nil {
		return active, nil
	}
	jobID, err := randomHealthJobID()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	job := &healthJob{out: HealthJobOutput{ID: jobID, Status: "running", Message: "后台测活任务已启动", StartedAt: now}}
	s.healthJobs.Store(jobID, job)
	s.pruneHealthJobs(now)
	opts := s.OneClickHealthTestOptions(groupIDs)
	go func() {
		ctx := progress.WithObserver(context.Background(), healthJobObserver{job: job})
		result, runErr := s.TestGroupKeys(ctx, opts)
		finished := time.Now()
		job.mu.Lock()
		defer job.mu.Unlock()
		job.out.FinishedAt = &finished
		job.out.Result = result
		if result != nil {
			job.out.Total = result.Total
			job.out.Completed = result.Checked
		}
		if runErr != nil {
			job.out.Status = "failed"
			job.out.Error = runErr.Error()
			job.out.Message = "后台测活失败"
			return
		}
		job.out.Status = "completed"
		job.out.Message = "后台测活完成"
	}()
	out := job.snapshot()
	return &out, nil
}

func (s *Service) HealthJob(id string) (*HealthJobOutput, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.New("health job id is required")
	}
	value, ok := s.healthJobs.Load(id)
	if !ok {
		return nil, errors.New("health job not found or expired")
	}
	out := value.(*healthJob).snapshot()
	return &out, nil
}

func (s *Service) pruneHealthJobs(now time.Time) {
	if s == nil {
		return
	}
	const retention = time.Hour
	s.healthJobs.Range(func(key, value any) bool {
		job, ok := value.(*healthJob)
		if !ok {
			s.healthJobs.Delete(key)
			return true
		}
		snapshot := job.snapshot()
		if snapshot.FinishedAt != nil && now.Sub(*snapshot.FinishedAt) > retention {
			s.healthJobs.Delete(key)
		}
		return true
	})
}

func randomHealthJobID() (string, error) {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate health job id: %w", err)
	}
	return "health_" + hex.EncodeToString(buf), nil
}

type UpdateGroupKeyInput struct {
	ConcurrencyLimit  *int     `json:"concurrency_limit"`
	Enabled           *bool    `json:"enabled"`
	RequestMode       *string  `json:"request_mode"`
	Priority          *int     `json:"priority"`
	RatioScalePercent *float64 `json:"ratio_scale_percent"`
	ClientFormat      *string  `json:"client_format"`
	Charity           *bool    `json:"charity"`
	Key               *string  `json:"key"`
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
	// ToolKinds preserves the Responses declaration while a Chat-only upstream
	// is used. In particular, Codex routes `custom_tool_call` (exec/apply_patch)
	// differently from a JSON-schema `function_call`.
	ToolKinds map[string]string
}

type usageTokens struct {
	Prompt        int64
	Completion    int64
	Total         int64
	Cached        int64
	Model         string
	ResponseID    string
	SoftFailure   string
	Status        string
	FirstTokenMS  int64
	DurationMS    int64
	Estimated     bool
	GeneratedText string
}

type modelPrice struct {
	InputPerMillion       float64
	CachedInputPerMillion float64
	OutputPerMillion      float64
}

type modelPriceRule struct {
	Prefix string
	Price  modelPrice
}

type groupRuntimeState struct {
	mu                sync.Mutex
	disabledUntil     time.Time
	needsRecovery     bool
	recoveryInFlight  bool
	avgFirstTokenMS   float64
	avgLatencyMS      float64
	inFlight          int
	lastObservedAt    time.Time
	modelCapabilities map[string]modelCapability
}

type modelCapability struct {
	supported bool
	expiresAt time.Time
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
	// sanitizeFailure is configured for proxied streams after preflight. It
	// removes provider-specific interception text from terminal SSE events.
	sanitizeFailure func(string) (string, bool)
	// interceptContent is configured for proxied streams after preflight. It
	// reports whether the visible text accumulated so far matches an
	// interception rule (for example a provider's "公益 token 休息了" or a relay
	// error phrase smuggled inside normal content deltas). This catches错误话术
	// that only appears *after* the first byte is written — preflight alone
	// cannot see it — so the stream is cut and the候选 route enters cooldown.
	interceptContent func(text string) bool
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

const (
	gatewayQuotaExhaustedMessage = "额度已经消耗光"
	gatewayIPBannedMessage       = "IP已被封禁"
	// defaultPublicIPConcurrencyLimit 仅作兜底默认值。实际生效值从 app.publicKey.ipConcurrencyLimit
	// 配置读取（见 publicIPConcurrencyBudget），可在系统设置的公益 Key 配置里调整。
	defaultPublicIPConcurrencyLimit = config.DefaultPublicIPConcurrencyLimit
)

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

func (s *Service) UpdateIPPolicy(ip string, blocked, publicConcurrencyExempt bool, note, blockedMessage string) (*storage.IPPolicy, error) {
	ip = strings.TrimSpace(ip)
	if net.ParseIP(ip) == nil {
		return nil, errors.New("invalid IP address")
	}
	if s.ipPolicies == nil {
		return nil, errors.New("IP policy store is unavailable")
	}
	item := &storage.IPPolicy{IP: ip, Blocked: blocked, PublicConcurrencyExempt: publicConcurrencyExempt, Note: strings.TrimSpace(note), BlockedMessage: strings.TrimSpace(blockedMessage)}
	if err := s.ipPolicies.Upsert(item); err != nil {
		return nil, err
	}
	return s.ipPolicies.Find(ip)
}

func ipBlockedMessage(policy *storage.IPPolicy) string {
	if policy != nil {
		if message := strings.TrimSpace(policy.BlockedMessage); message != "" {
			return message
		}
	}
	return gatewayIPBannedMessage
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
	if model := modelFromMap(raw); model != "" {
		return model
	}
	return ""
}

func modelFromMap(raw map[string]any) string {
	if raw == nil {
		return ""
	}
	for _, key := range []string{"model", "requested_model", "original_model", "target_model", "codex_model", "openai_model", "model_slug"} {
		if model := strings.TrimSpace(stringValue(raw[key])); model != "" {
			return model
		}
	}
	for _, key := range []string{"metadata", "extra", "options", "params", "request"} {
		if nested, ok := raw[key].(map[string]any); ok {
			if model := modelFromMap(nested); model != "" {
				return model
			}
		}
	}
	return ""
}

func requestModelFromHTTP(r *http.Request, body []byte, rawQuery string) string {
	if model := modelFromRequestBody(body); model != "" {
		return model
	}
	for _, name := range []string{
		"X-Model",
		"X-OpenAI-Model",
		"OpenAI-Model",
		"X-Requested-Model",
		"X-Codex-Model",
		"X-CCSwitch-Model",
		"X-Upstream-Model",
	} {
		if model := strings.TrimSpace(r.Header.Get(name)); model != "" {
			return model
		}
	}
	values, err := url.ParseQuery(rawQuery)
	if err == nil {
		for _, key := range []string{"model", "requested_model", "original_model"} {
			if model := strings.TrimSpace(values.Get(key)); model != "" {
				return model
			}
		}
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
func (s *Service) ListUsageLogs(limit, offset int, views ...string) ([]storage.UsageLog, int64, error) {
	if s.usageLogs == nil {
		return []storage.UsageLog{}, 0, nil
	}
	view := "all"
	if len(views) > 0 {
		view = views[0]
	}
	items, total, err := s.usageLogs.ListView(limit, offset, view)
	// 用量明细的倍率 / 公益标记沿用写入时的快照（recordUsageLog 记录当时的有效倍率），
	// 不按渠道当前配置实时刷新：明细是历史计费凭证，运维事后改倍率不应改动已产生的账单，
	// 显示值必须与当时实际计费口径一致，避免"历史账单金额随现在的倍率漂移"。
	return items, total, err
}

func (s *Service) UsageLogDetail(id uint) (*storage.UsageLog, error) {
	if s.usageLogs == nil {
		return nil, gorm.ErrRecordNotFound
	}
	entry, err := s.usageLogs.Find(id)
	if err != nil {
		return nil, err
	}
	entry.ErrorMessage = sanitize.Truncate(sanitize.RedactText(entry.ErrorMessage), 1000)
	entry.ErrorDetail = sanitize.Truncate(sanitize.RedactText(entry.ErrorDetail), 4096)
	if entry.ErrorDetail == "" && entry.ErrorMessage != "" {
		attempt := dispatchLogAttempt{
			startedAt: entry.CreatedAt, channel: entry.ChannelName, pool: entry.OAuthPool,
			model: entry.Model, attempt: entry.DispatchAttempt,
		}
		entry.ErrorDetail = dispatchErrorDetail(attempt, entry.Status, entry.ErrorCode, entry.ErrorStatus, entry.ErrorMessage)
	}
	return entry, nil
}

// UsageLogStats 返回当前保留使用明细的聚合指标。
func (s *Service) UsageLogStats(views ...string) (storage.UsageLogStats, error) {
	if s.usageLogs == nil {
		return storage.UsageLogStats{}, nil
	}
	view := "all"
	if len(views) > 0 {
		view = views[0]
	}
	return s.usageLogs.StatsView(view)
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
		ChannelID:            candidate.ChannelID,
		ChannelName:          candidate.ChannelName,
		UpstreamGroupKeyID:   candidate.ID,
		UpstreamGroupCharity: candidate.Charity,
		GroupName:            candidate.GroupName,
		Model:                model,
		ClientFormat:         candidate.ClientFormat,
		PromptTokens:         usage.Prompt,
		CompletionTokens:     usage.Completion,
		TotalTokens:          usage.Total,
		CachedTokens:         usage.Cached,
		Ratio:                effectiveGroupRatio(*candidate),
		Status:               usageStatus(usage),
		FirstTokenMS:         maxInt64(0, usage.FirstTokenMS),
		DurationMS:           maxInt64(0, usage.DurationMS),
		RequestIP:            strings.TrimSpace(requestIP),
	}
	if gatewayKey != nil {
		entry.GatewayKeyID = gatewayKey.ID
		entry.GatewayKeyName = gatewayKey.Name
		entry.GatewayKeyIsPublic = gatewayKey.IsPublic
	}
	if err := s.usageLogs.Add(entry); err != nil && s.log != nil {
		s.log.Warn("record usage log failed", "err", err)
	}
}

type dispatchLogAttempt struct {
	id        uint
	startedAt time.Time
	channel   string
	pool      string
	model     string
	attempt   int
}

type dispatchLogContextKey struct{}

func (s *Service) startDispatchLog(gatewayKey *storage.GatewayKey, candidate *storage.UpstreamGroupKey, model, requestIP string, attemptNumbers ...int) dispatchLogAttempt {
	startedAt := time.Now()
	attemptNumber := 0
	if len(attemptNumbers) > 0 && attemptNumbers[0] > 0 {
		attemptNumber = attemptNumbers[0]
	}
	attempt := dispatchLogAttempt{startedAt: startedAt, model: strings.TrimSpace(model), attempt: attemptNumber}
	if s.usageLogs == nil {
		return attempt
	}
	entry := &storage.UsageLog{
		Model:        strings.TrimSpace(model),
		Status:       "dispatching",
		RequestIP:    strings.TrimSpace(requestIP),
		CreatedAt:    startedAt,
		DurationMS:   0,
		FirstTokenMS: 0,
	}
	if gatewayKey != nil {
		entry.GatewayKeyID = gatewayKey.ID
		entry.GatewayKeyName = gatewayKey.Name
		entry.GatewayKeyIsPublic = gatewayKey.IsPublic
	}
	if candidate != nil {
		attempt.channel = candidate.ChannelName
		entry.ChannelID = candidate.ChannelID
		entry.ChannelName = candidate.ChannelName
		entry.UpstreamGroupKeyID = candidate.ID
		entry.UpstreamGroupCharity = candidate.Charity
		entry.GroupName = candidate.GroupName
		entry.ClientFormat = candidate.ClientFormat
		entry.Ratio = effectiveGroupRatio(*candidate)
		if pool, fixed := oauthPoolForCandidate(candidate); fixed {
			attempt.pool = string(pool)
			entry.OAuthPool = string(pool)
		}
	}
	entry.DispatchAttempt = attemptNumber
	if err := s.usageLogs.Add(entry); err != nil {
		if s.log != nil {
			s.log.Warn("record dispatch start failed", "err", err)
		}
		return attempt
	}
	attempt.id = entry.ID
	return attempt
}

func (s *Service) finishDispatchLog(attempt dispatchLogAttempt, status, errMsg string, usage usageTokens) {
	if s.usageLogs == nil || attempt.id == 0 {
		return
	}
	status = strings.TrimSpace(status)
	if status == "" {
		status = usageStatus(usage)
	}
	durationMS := usage.DurationMS
	if durationMS <= 0 && !attempt.startedAt.IsZero() {
		durationMS = time.Since(attempt.startedAt).Milliseconds()
	}
	safeError := sanitize.Truncate(sanitize.RedactText(errMsg), 2000)
	errorCode, errorStatus := dispatchErrorClassification(status, safeError)
	detail := dispatchErrorDetail(attempt, status, errorCode, errorStatus, safeError)
	updates := map[string]any{
		"status":            status,
		"prompt_tokens":     maxInt64(0, usage.Prompt),
		"completion_tokens": maxInt64(0, usage.Completion),
		"total_tokens":      maxInt64(0, usage.Total),
		"cached_tokens":     maxInt64(0, usage.Cached),
		"first_token_ms":    maxInt64(0, usage.FirstTokenMS),
		"duration_ms":       maxInt64(0, durationMS),
		"error_message":     sanitize.Truncate(safeError, 1000),
		"error_code":        errorCode,
		"error_status":      errorStatus,
		"error_detail":      detail,
	}
	if err := s.usageLogs.Update(attempt.id, updates); err != nil && s.log != nil {
		s.log.Warn("record dispatch finish failed", "id", attempt.id, "err", err)
	}
}

func dispatchErrorClassification(status, message string) (string, int) {
	lower := strings.ToLower(strings.TrimSpace(message))
	for _, code := range []int{400, 401, 403, 404, 408, 409, 422, 429, 500, 502, 503, 504} {
		marker := strconv.Itoa(code)
		if strings.Contains(lower, "http "+marker) || strings.Contains(lower, "status "+marker) {
			return "upstream_http_error", code
		}
	}
	switch {
	case strings.Contains(lower, "oauth_pool_unavailable"):
		return "oauth_pool_unavailable", http.StatusServiceUnavailable
	case strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline exceeded"):
		return "upstream_timeout", http.StatusGatewayTimeout
	case strings.Contains(lower, "proxy"):
		return "proxy_error", http.StatusBadGateway
	case strings.Contains(lower, "auth") || strings.Contains(lower, "unauthorized"):
		return "upstream_auth_error", http.StatusUnauthorized
	case status == "saturated":
		return "concurrency_saturated", http.StatusServiceUnavailable
	case status == "cooldown_skipped":
		return "temporary_unschedulable", http.StatusServiceUnavailable
	case strings.TrimSpace(message) != "":
		return "upstream_error", http.StatusBadGateway
	default:
		return "", 0
	}
}

func dispatchErrorDetail(attempt dispatchLogAttempt, status, code string, errorStatus int, message string) string {
	if message == "" && code == "" {
		return ""
	}
	payload := map[string]any{
		"request_time": attempt.startedAt.UTC(), "channel": attempt.channel, "pool": attempt.pool,
		"model": attempt.model, "attempt": attempt.attempt, "status": status,
		"error_code": code, "error_status": errorStatus, "error": message,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return sanitize.Truncate(message, 4096)
	}
	return sanitize.Truncate(sanitize.RedactText(string(raw)), 4096)
}

func (s *Service) attachOAuthDispatchAccount(ctx context.Context, lease *oauthpool.DispatchLease, accountAttempt int) {
	if s.usageLogs == nil || lease == nil {
		return
	}
	id, _ := ctx.Value(dispatchLogContextKey{}).(uint)
	if id == 0 {
		return
	}
	identifier := firstNonEmpty(lease.Account.Email, lease.Account.ExternalID, strconv.FormatUint(uint64(lease.Account.ID), 10))
	_ = s.usageLogs.Update(id, map[string]any{
		"oauth_pool": lease.Pool, "oauth_account": sanitize.MaskIdentifier(identifier), "dispatch_attempt": accountAttempt,
	})
}

func (s *Service) recordDispatchSkipLog(gatewayKey *storage.GatewayKey, candidate *storage.UpstreamGroupKey, model, requestIP, status, errMsg string) {
	attempt := s.startDispatchLog(gatewayKey, candidate, model, requestIP)
	s.finishDispatchLog(attempt, status, errMsg, usageTokens{})
}

func truncateLogMessage(value string, max int) string {
	value = strings.TrimSpace(value)
	if max <= 0 || len(value) <= max {
		return value
	}
	if max <= 3 {
		return value[:max]
	}
	return value[:max-3] + "..."
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
	price := priceForModelOrCandidate(usage.Model, *candidate)
	inputPrice := price.InputPerMillion
	cachedInputPrice := price.CachedInputPerMillion
	outputPrice := price.OutputPerMillion
	cachedTokens := usage.Cached
	if cachedTokens < 0 {
		cachedTokens = 0
	}
	if cachedTokens > promptTokens {
		cachedTokens = promptTokens
	}
	uncachedPromptTokens := promptTokens - cachedTokens
	ratio := effectiveGroupRatio(*candidate)
	return (float64(uncachedPromptTokens)*inputPrice + float64(cachedTokens)*cachedInputPrice + float64(completionTokens)*outputPrice) * ratio / 1_000_000
}

func priceForModelOrCandidate(model string, candidate storage.UpstreamGroupKey) modelPrice {
	if price, ok := builtinModelPrice(model); ok {
		return price
	}
	input := candidate.InputPricePerMillion
	if input <= 0 {
		input = storage.DefaultInputPricePerMillion
	}
	output := candidate.OutputPricePerMillion
	if output <= 0 {
		output = storage.DefaultOutputPricePerMillion
	}
	return modelPrice{InputPerMillion: input, CachedInputPerMillion: input, OutputPerMillion: output}
}

func builtinModelPrice(model string) (modelPrice, bool) {
	normalized := strings.ToLower(strings.TrimSpace(model))
	if normalized == "" {
		return modelPrice{}, false
	}
	for _, rule := range builtinModelPriceRules {
		prefix := strings.ToLower(strings.TrimSpace(rule.Prefix))
		if prefix == "" {
			continue
		}
		if normalized == prefix || strings.HasPrefix(normalized, prefix+"-") || strings.HasPrefix(normalized, prefix+".") {
			price := rule.Price
			if price.InputPerMillion <= 0 {
				price.InputPerMillion = storage.DefaultInputPricePerMillion
			}
			if price.OutputPerMillion <= 0 {
				price.OutputPerMillion = storage.DefaultOutputPricePerMillion
			}
			if price.CachedInputPerMillion <= 0 {
				price.CachedInputPerMillion = price.InputPerMillion
			}
			return price, true
		}
	}
	return modelPrice{}, false
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

func (s *Service) UpdateAppConfig(cfg config.AppConfig) {
	s.configMu.Lock()
	s.app = cfg
	s.configMu.Unlock()
}

func (s *Service) UpdateProxyConfig(cfg config.ProxyConfig) {
	cfg.SelectedTargets = config.CleanProxyTargets(cfg.SelectedTargets)
	s.configMu.Lock()
	s.proxy = cfg
	s.configMu.Unlock()
	s.clients.Range(func(key, value any) bool {
		if client, ok := value.(*http.Client); ok {
			client.CloseIdleConnections()
		}
		s.clients.Delete(key)
		return true
	})
}

// SetOAuthPool attaches the account scheduler used only after the normal
// ratio/group scheduler has selected one of the fixed pool channels.
func (s *Service) SetOAuthPool(service *oauthpool.Service) {
	s.configMu.Lock()
	s.oauthPool = service
	s.configMu.Unlock()
}

func (s *Service) SetOAuthAccounts(repository *storage.OAuthAccounts) {
	s.configMu.Lock()
	s.oauthAccounts = repository
	s.configMu.Unlock()
}

func (s *Service) oauthPoolService() *oauthpool.Service {
	s.configMu.RLock()
	service := s.oauthPool
	s.configMu.RUnlock()
	return service
}

func (s *Service) oauthAccountRepository() *storage.OAuthAccounts {
	s.configMu.RLock()
	repository := s.oauthAccounts
	s.configMu.RUnlock()
	return repository
}

func (s *Service) appConfig() config.AppConfig {
	s.configMu.RLock()
	cfg := s.app
	s.configMu.RUnlock()
	return cfg
}

func (s *Service) upstreamConfig() config.UpstreamConfig {
	s.configMu.RLock()
	cfg := s.upstream
	s.configMu.RUnlock()
	return cfg.WithDefaults()
}

func (s *Service) proxyConfig() config.ProxyConfig {
	s.configMu.RLock()
	cfg := s.proxy
	s.configMu.RUnlock()
	return cfg
}

// streamFirstEventBudget 是"等上游吐出第一个可见生成事件"的窗口，可配置。
// 推理模型出字前的 reasoning 阶段常需 5-30s，秒级窗口会把可用渠道误判成卡死。
// 常量 streamFirstEventTimeout 仅作兜底默认值。
func (s *Service) streamFirstEventBudget() time.Duration {
	if secs := s.upstreamConfig().StreamFirstEventTimeoutSeconds; secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return streamFirstEventTimeout
}

// healthProbeBudget 是单次测活"等一个可见生成事件"的窗口，可配置。
func (s *Service) healthProbeBudget() time.Duration {
	if secs := s.upstreamConfig().HealthProbeTimeoutSeconds; secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return healthProbeTimeout
}

// streamInterceptionScanEvents 是首字节落地前额外缓冲扫描拦截词的可见事件数。
// 0（默认）= 关闭，preflight 一见到首个可见输出就提交、走低延迟；>0 时首字节前
// 会多扫描这些可见事件，命中拦截词就在写首字节前整体切到下一个候选（连命中前那段
// 都不显示），代价是首字节延迟增加。
func (s *Service) streamInterceptionScanEvents() int {
	n := s.upstreamConfig().StreamInterceptionScanEvents
	if n < 0 {
		return 0
	}
	return n
}

// maxDispatchAttempts 是单次请求最多"真正尝试"的候选数上限（冷却/内存禁用被跳过的
// 候选不计）。<=0 视为不限，行为回退到历史"遍历全部候选"。配合收敛后的首字节窗口，
// 把最坏单请求等待压到"上限 × 首字节窗口"以内。
func (s *Service) maxDispatchAttempts() int {
	n := s.upstreamConfig().MaxDispatchAttemptsPerRequest
	if n <= 0 {
		return 0
	}
	return n
}

// healthRunBudget 是单个渠道整轮测活的外层信封。测活内部可能按配置清单先后探测
// 多个模型（各自最多 healthProbeBudget），因此外层要按清单长度留出足够余量，
// 否则放宽后的单次窗口会被外层提前掐断。清单越长，外层信封越大。
func (s *Service) healthRunBudget() time.Duration {
	// 至少按 2 个模型算，避免清单只配 1 个时外层过紧、连接抖动就被掐断。
	probes := len(s.healthProbeModels())
	if probes < 2 {
		probes = 2
	}
	budget := s.healthProbeBudget()*time.Duration(probes) + 10*time.Second
	if budget < healthProbeRunTimeout {
		return healthProbeRunTimeout
	}
	return budget
}

// healthProbeModels 返回一键/定时测活对 OpenAI 渠道按顺序尝试的模型清单，可在
// 系统设置里配置（app 通过 UpdateUpstreamConfig 热更新，无需重启）。WithDefaults
// 已保证清单非空（留空回退内置 gpt-5.4 → gpt-5.5），这里再兜底一次以防万一。
func (s *Service) healthProbeModels() []string {
	models := s.upstreamConfig().HealthProbeModels
	if len(models) == 0 {
		return append([]string(nil), config.DefaultOpenAIHealthProbeModels...)
	}
	return models
}

// healthProbeMaxRatio 返回"全量兜底扫描"的倍率成本上限，可在系统设置里配置。
// <=0 时回退默认值。明确勾选分组的一键测活不受此限制（见调用方对 GroupIDs 的判断）。
func (s *Service) healthProbeMaxRatio() float64 {
	if r := s.upstreamConfig().HealthProbeMaxRatio; r > 0 {
		return r
	}
	return config.DefaultHealthProbeMaxRatio
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

func normalizeGatewayMaxGroupRatio(value float64) float64 {
	if value <= 0 {
		return 0
	}
	if value <= 0.05 {
		return 0.05
	}
	if value <= 0.1 {
		return 0.1
	}
	// The UI exposes only 0 / 0.05 / 0.1.  Keeping unexpected larger API
	// values unlimited avoids surprising callers by silently allowing a
	// partially-documented fourth tier.
	return 0
}

const (
	gatewayRouteRatioFirst    = "ratio_first"
	gatewayRoutePoolFirst     = "pool_first"
	gatewayRouteUpstreamFirst = "upstream_first"
)

func normalizeGatewayRoutePreference(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case gatewayRoutePoolFirst:
		return gatewayRoutePoolFirst
	case gatewayRouteUpstreamFirst:
		return gatewayRouteUpstreamFirst
	default:
		return gatewayRouteRatioFirst
	}
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
		MaxGroupRatio:     normalizeGatewayMaxGroupRatio(input.MaxGroupRatio),
		RoutePreference:   normalizeGatewayRoutePreference(input.RoutePreference),
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
	if input.DisabledMessage != nil {
		key.DisabledMessage = strings.TrimSpace(*input.DisabledMessage)
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
	if input.MaxGroupRatio != nil {
		key.MaxGroupRatio = normalizeGatewayMaxGroupRatio(*input.MaxGroupRatio)
	}
	if input.RoutePreference != nil {
		key.RoutePreference = normalizeGatewayRoutePreference(*input.RoutePreference)
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

func (s *Service) BatchDisableGatewayKeys(input BatchDisableGatewayKeysInput) ([]GatewayKeyOutput, error) {
	message := strings.TrimSpace(input.Message)
	if message == "" {
		message = "此调用 Key 已停用，请联系管理员。"
	}
	seen := map[uint]struct{}{}
	result := make([]GatewayKeyOutput, 0, len(input.IDs))
	for _, id := range input.IDs {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		enabled := false
		out, err := s.UpdateGatewayKey(id, UpdateGatewayKeyInput{Enabled: &enabled, DisabledMessage: &message})
		if err != nil {
			return nil, err
		}
		result = append(result, *out)
	}
	if len(result) == 0 {
		return nil, errors.New("select at least one gateway key")
	}
	return result, nil
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
	// A quota-exhausted key is disabled after settlement.  Looking up all keys
	// first preserves the real quota/expiry reason instead of masking it as an
	// invalid key on later requests.
	rec, err := s.gateway.FindByHash(HashKey(key))
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
	if !rec.Enabled {
		return nil, errors.New("gateway key disabled")
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
	defer s.InvalidateSchedulingCache()
	key, err := s.groupKeys.FindByID(id)
	if err != nil {
		return nil, err
	}

	keyChanged := input.Key != nil
	if input.Key != nil {
		if !isManualGroupKey(key) {
			return nil, errors.New("only manually added group keys can replace the upstream key")
		}
		rawKey := normalizeUpstreamAPIKey(*input.Key)
		if rawKey == "" {
			return nil, errors.New("upstream key cannot be empty")
		}
		cipher, err := s.cipher.Encrypt(rawKey)
		if err != nil {
			return nil, fmt.Errorf("encrypt upstream key: %w", err)
		}
		if err := s.groupKeys.UpdateManualKey(id, cipher); err != nil {
			return nil, err
		}
		s.clearRuntimeDisable(id)
	}
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
	if input.Priority != nil {
		priority := *input.Priority
		if priority < 0 {
			priority = 0
		}
		if err := s.groupKeys.UpdatePriority(id, priority); err != nil {
			return nil, err
		}
	}
	formatChanged := false
	if input.ClientFormat != nil {
		format := normalizeClientFormat(*input.ClientFormat)
		formatChanged = format != normalizeClientFormat(key.ClientFormat)
		if err := s.groupKeys.UpdateClientFormat(id, format); err != nil {
			return nil, err
		}
		key.ClientFormat = format
	}

	// A request mode has two independent pieces of state: the actual protocol
	// used by the forwarder and whether that protocol was detected or explicitly
	// selected by an administrator. Keeping that source lets a broken automatic
	// probe be corrected and prevents later syncs from undoing the correction.
	shouldDetect := false
	if input.RequestMode != nil {
		mode, source, err := requestModeConfigForClientFormat(key.ClientFormat, *input.RequestMode)
		if err != nil {
			return nil, err
		}
		if err := s.groupKeys.UpdateRequestModeConfig(id, mode, source); err != nil {
			return nil, err
		}
		key.RequestMode = mode
		key.RequestModeSource = source
		shouldDetect = source == "auto"
	} else if formatChanged {
		// A format change invalidates the former protocol choice. Return to
		// automatic detection unless the caller explicitly supplied a compatible
		// request mode in this same update.
		mode := defaultRequestModeForClientFormat(key.ClientFormat)
		if err := s.groupKeys.UpdateRequestModeConfig(id, mode, "auto"); err != nil {
			return nil, err
		}
		key.RequestMode = mode
		key.RequestModeSource = "auto"
		shouldDetect = true
	}
	if input.Charity != nil {
		if err := s.groupKeys.UpdateCharity(id, *input.Charity); err != nil {
			return nil, err
		}
	}
	if input.RatioScalePercent != nil {
		if err := s.groupKeys.UpdateRatioScalePercent(id, *input.RatioScalePercent); err != nil {
			return nil, err
		}
	}
	// Replacing a secret in a healthy manual group normally means the operator
	// rotated a Key at the same provider. Its proven protocol and authentication
	// header are group configuration, not properties that should be discarded
	// for every new Key. Keep them until a real request receives 401/403; the
	// pre-first-byte alternate-header retry below then repairs only that Key
	// without making the edit flow send extra probes or flipping a working
	// Bearer/x-api-key contract.
	preserveHealthyManualContract := keyChanged && !shouldDetect && isManualGroupKey(key) &&
		strings.EqualFold(strings.TrimSpace(key.Status), "alive") &&
		strings.TrimSpace(key.AuthMode) != ""
	if shouldDetect || (keyChanged && !preserveHealthyManualContract) {
		ctx, cancel := context.WithTimeout(context.Background(), manualRequestModeDetectTimeout)
		defer cancel()
		// Automatic configurations are re-detected after a Key replacement. A
		// manual group that has not yet proved healthy receives a header-only
		// probe; healthy manual groups retain their established contract above.
		if shouldDetect {
			if detected, detectErr := s.DetectGroupRequestMode(ctx, id); detectErr == nil && detected != nil {
				return detected, nil
			}
		}
		if detected, detectErr := s.DetectGroupAuthMode(ctx, id); detectErr == nil && detected != nil {
			return detected, nil
		}
	}
	return s.groupKeys.FindByID(id)
}

// RevealManualGroupKey returns the plaintext key for a locally created manual group.
func (s *Service) RevealManualGroupKey(id uint) (string, error) {
	key, err := s.groupKeys.FindByID(id)
	if err != nil {
		return "", err
	}
	if !isManualGroupKey(key) {
		return "", errors.New("only manually added group keys can be revealed here")
	}
	return s.cipher.Decrypt(key.KeyCipher)
}

func isManualGroupKey(key *storage.UpstreamGroupKey) bool {
	return key != nil && strings.HasPrefix(strings.ToLower(strings.TrimSpace(key.GroupRef)), "manual:")
}

const (
	manualRequestModeDetectTimeout   = 45 * time.Second
	requestModeDetectionProbeTimeout = 8 * time.Second
)

// DetectGroupRequestMode probes the protocol that belongs to this channel
// format. OpenAI relays are detected as Responses or Chat; Claude and Grok are
// verified with their native Messages and Chat contracts respectively.
func (s *Service) DetectGroupRequestMode(ctx context.Context, id uint) (*storage.UpstreamGroupKey, error) {
	key, err := s.groupKeys.FindByID(id)
	if err != nil {
		return nil, err
	}
	// A manually selected protocol is an administrator repair, not a hint for
	// the detector. It remains in force until the administrator explicitly
	// chooses "auto" again through UpdateGroupKey.
	if strings.EqualFold(strings.TrimSpace(key.RequestModeSource), "manual") {
		return key, nil
	}
	switch normalizeClientFormat(key.ClientFormat) {
	case "openai":
		return s.detectOpenAIGroupRequestMode(ctx, key)
	case "claude":
		return s.detectFixedGroupRequestMode(ctx, key, "messages")
	case "grok":
		return s.detectFixedGroupRequestMode(ctx, key, "chat")
	default:
		return key, nil
	}
}

// DetectOpenAIGroupRequestMode is kept for callers that intentionally need
// only the OpenAI/GPT branch. The general detector is used by channel sync and
// manual-key editing.
func (s *Service) DetectOpenAIGroupRequestMode(ctx context.Context, id uint) (*storage.UpstreamGroupKey, error) {
	key, err := s.groupKeys.FindByID(id)
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(strings.TrimSpace(key.RequestModeSource), "manual") {
		return key, nil
	}
	if normalizeClientFormat(key.ClientFormat) != "openai" {
		return key, nil
	}
	return s.detectOpenAIGroupRequestMode(ctx, key)
}

// DetectGroupAuthMode rechecks authentication for the existing request
// protocol. It is used after replacing a key whose protocol was manually
// selected: protocol configuration stays intact, while Bearer/x-api-key is
// discovered independently for that concrete key.
func (s *Service) DetectGroupAuthMode(ctx context.Context, id uint) (*storage.UpstreamGroupKey, error) {
	key, err := s.groupKeys.FindByID(id)
	if err != nil {
		return nil, err
	}
	probeCtx, cancel := context.WithTimeout(ctx, manualRequestModeDetectTimeout)
	defer cancel()
	for _, authMode := range upstreamAuthModesForProbe(key) {
		candidate := *key
		candidate.AuthMode = authMode
		status, body, _, usedModel, probeErr := s.healthProbeCandidateWithModel(probeCtx, &candidate)
		succeeded := healthProbeSucceeded(status, body, probeErr)
		if !succeeded && !healthProbeProvesProtocolReachable(status, body, probeErr) {
			continue
		}
		if err := s.groupKeys.UpdateAuthMode(key.ID, authMode); err != nil {
			return nil, err
		}
		if succeeded {
			if err := s.groupKeys.MarkHealthSuccessWithModel(key.ID, 0, usedModel); err != nil {
				return nil, err
			}
		}
		return s.groupKeys.FindByID(key.ID)
	}
	return key, errors.New("could not detect a working authentication header for this upstream key")
}

// DetectManualGroupKeyRequestMode remains the narrow public helper used by
// manual-key editing. It delegates to the general detector after enforcing
// that the caller is operating on a manually added upstream key.
func (s *Service) DetectManualGroupKeyRequestMode(ctx context.Context, id uint) (*storage.UpstreamGroupKey, error) {
	key, err := s.groupKeys.FindByID(id)
	if err != nil {
		return nil, err
	}
	if !isManualGroupKey(key) {
		return nil, errors.New("only manually added group keys support request-mode detection")
	}
	return s.DetectGroupRequestMode(ctx, id)
}

func (s *Service) detectOpenAIGroupRequestMode(ctx context.Context, key *storage.UpstreamGroupKey) (*storage.UpstreamGroupKey, error) {
	probeCtx, cancel := context.WithTimeout(ctx, manualRequestModeDetectTimeout)
	defer cancel()
	models := []string{openAIHealthProbePrimaryModel, openAIHealthProbeFallbackModel}
	if detected, ok, err := s.detectOpenAIGroupRequestModeForModels(probeCtx, key, models, false); err != nil {
		return nil, err
	} else if ok {
		return detected, nil
	}

	// Some OpenAI-compatible relays expose only a newer model (for example
	// gpt-5.6).  A gpt-5.4 probe must not make those healthy relays look dead.
	// Only after the two tiny default probes fail do we ask /v1/models and try
	// one advertised text model. This preserves the low-cost fast path while
	// still discovering a compatible protocol for model-limited channels.
	for _, authMode := range upstreamAuthModesForProbe(key) {
		candidate := *key
		candidate.AuthMode = authMode
		if model, _, _, err := s.discoverHealthProbeModel(probeCtx, &candidate); err == nil {
			before := len(models)
			models = appendDistinctHealthProbeModel(models, model)
			if detected, ok, detectErr := s.detectOpenAIGroupRequestModeForModels(probeCtx, &candidate, models[before:], false); detectErr != nil {
				return nil, detectErr
			} else if ok {
				return detected, nil
			}
		}
	}
	if detected, ok, detectErr := s.detectOpenAIGroupRequestModeForModels(probeCtx, key, models, true); detectErr != nil {
		return nil, detectErr
	} else if ok {
		return detected, nil
	}
	return key, errors.New("could not detect a working responses or chat endpoint for this upstream key")
}

func (s *Service) detectOpenAIGroupRequestModeForModels(ctx context.Context, key *storage.UpstreamGroupKey, models []string, acceptTentative bool) (*storage.UpstreamGroupKey, bool, error) {
	// A relay can expose a valid authenticated endpoint while rejecting only
	// the small probe model. Remember that as protocol evidence and use it only
	// if none of the configured probe models can complete a generation. This
	// keeps a model-limited but otherwise usable Codex channel schedulable.
	tentativeMode := ""
	tentativeAuthMode := ""
	for _, model := range models {
		if strings.TrimSpace(model) == "" {
			continue
		}
		for _, mode := range []string{"responses", "chat"} {
			request := healthGenerationProbeRequest(model)
			if mode == "chat" {
				request = request.alt()
			}
			for _, authMode := range upstreamAuthModesForProbe(key) {
				candidate := *key
				candidate.AuthMode = authMode
				status, _, body, probeErr := s.requestHealthProbeCandidate(ctx, request, &candidate, requestModeDetectionProbeTimeout)
				if !healthProbeSucceeded(status, body, probeErr) {
					if tentativeMode == "" && healthProbeProvesProtocolReachable(status, body, probeErr) {
						tentativeMode = mode
						tentativeAuthMode = authMode
					}
					continue
				}
				if err := s.groupKeys.UpdateRequestMode(key.ID, mode); err != nil {
					return nil, false, err
				}
				if err := s.groupKeys.UpdateAuthMode(key.ID, authMode); err != nil {
					return nil, false, err
				}
				if err := s.groupKeys.MarkHealthSuccessWithModel(key.ID, 0, model); err != nil {
					return nil, false, err
				}
				detected, err := s.groupKeys.FindByID(key.ID)
				return detected, true, err
			}
		}
	}
	if tentativeMode != "" && acceptTentative {
		if err := s.groupKeys.UpdateRequestMode(key.ID, tentativeMode); err != nil {
			return nil, false, err
		}
		if err := s.groupKeys.UpdateAuthMode(key.ID, tentativeAuthMode); err != nil {
			return nil, false, err
		}
		detected, err := s.groupKeys.FindByID(key.ID)
		return detected, true, err
	}
	return key, false, nil
}

func healthProbeProvesProtocolReachable(status int, body []byte, err error) bool {
	if status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusNotFound || status == http.StatusMethodNotAllowed {
		return false
	}
	classification := healthFailureStatus(status, body, err)
	return classification == "model_error" || classification == "invalid_request"
}

func appendDistinctHealthProbeModel(models []string, model string) []string {
	model = strings.TrimSpace(model)
	if model == "" {
		return models
	}
	for _, existing := range models {
		if strings.EqualFold(strings.TrimSpace(existing), model) {
			return models
		}
	}
	return append(models, model)
}

func (s *Service) detectFixedGroupRequestMode(ctx context.Context, key *storage.UpstreamGroupKey, mode string) (*storage.UpstreamGroupKey, error) {
	probeCtx, cancel := context.WithTimeout(ctx, manualRequestModeDetectTimeout)
	defer cancel()
	for _, authMode := range upstreamAuthModesForProbe(key) {
		candidate := *key
		candidate.AuthMode = authMode
		usedModel := defaultHealthProbeModel(candidate.ClientFormat)
		var (
			status int
			body   []byte
			err    error
		)
		switch normalizeClientFormat(candidate.ClientFormat) {
		case "claude":
			status, body, _, err = s.healthProbeClaude(probeCtx, &candidate)
		case "grok":
			status, body, _, err = s.healthProbeGrok(probeCtx, &candidate)
		default:
			return key, errors.New("unsupported fixed-protocol channel format")
		}
		if !healthProbeSucceeded(status, body, err) {
			continue
		}
		if err := s.groupKeys.UpdateRequestMode(key.ID, mode); err != nil {
			return nil, err
		}
		if err := s.groupKeys.UpdateAuthMode(key.ID, authMode); err != nil {
			return nil, err
		}
		if err := s.groupKeys.MarkHealthSuccessWithModel(key.ID, 0, usedModel); err != nil {
			return nil, err
		}
		return s.groupKeys.FindByID(key.ID)
	}
	return key, errors.New("could not detect a working request protocol or authentication header for this upstream key")
}

func healthProbeSucceeded(status int, body []byte, err error) bool {
	return err == nil && status >= http.StatusOK && status < http.StatusMultipleChoices &&
		!isUpstreamErrorBody(body) && looksLikeHealthGenerationSuccess(body)
}

// detectGroupRequestModes refreshes endpoint capability in parallel so a
// large one-click group sync does not serially wait on every upstream.
// Detection failure deliberately keeps the prior mode; forwarding still has
// its protocol fallback and the next sync can retry the probe.
func (s *Service) detectGroupRequestModes(ids []uint) {
	const maxConcurrentDetections = 10
	seen := make(map[uint]struct{}, len(ids))
	sem := make(chan struct{}, maxConcurrentDetections)
	var wg sync.WaitGroup
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		wg.Add(1)
		sem <- struct{}{}
		go func(groupID uint) {
			defer wg.Done()
			defer func() { <-sem }()
			key, err := s.groupKeys.FindByID(groupID)
			if err != nil || key == nil || strings.EqualFold(strings.TrimSpace(key.RequestModeSource), "manual") {
				return
			}
			release := s.acquireHealthProbeUpstreamSlot(*key)
			defer release()
			ctx, cancel := context.WithTimeout(context.Background(), manualRequestModeDetectTimeout)
			defer cancel()
			_, _ = s.DetectGroupRequestMode(ctx, groupID)
		}(id)
	}
	wg.Wait()
}

// syncBootstrapGroupModels refreshes /v1/models after an automatic group-key
// reconciliation. Manual groups keep their explicitly managed model lists, and
// Claude/Grok groups are skipped because they do not use the OpenAI models API.
// Existing lists are preserved when an upstream is temporarily unavailable.
func (s *Service) syncBootstrapGroupModels(ctx context.Context, groups []storage.UpstreamGroupKey) (int, int) {
	const maxConcurrentSyncs = 10
	sem := make(chan struct{}, maxConcurrentSyncs)
	var wg sync.WaitGroup
	var synced atomic.Int64
	var failed atomic.Int64

	for i := range groups {
		key := groups[i]
		if !key.Enabled || key.KeyCipher == "" || isManualGroupKey(&key) || normalizeClientFormat(key.ClientFormat) != "openai" {
			continue
		}
		wg.Add(1)
		go func(candidate storage.UpstreamGroupKey) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				failed.Add(1)
				return
			}
			release := s.acquireHealthProbeUpstreamSlot(candidate)
			defer release()
			policy, err := s.SyncGroupKeyModels(ctx, candidate.ID)
			if err != nil || len(policy.AvailableModels) == 0 {
				failed.Add(1)
				if err != nil && s.log != nil {
					s.log.Warn("bootstrap model sync failed", "id", candidate.ID, "channel", candidate.ChannelName, "group", candidate.GroupName, "err", err)
				}
				return
			}
			synced.Add(1)
		}(key)
	}

	wg.Wait()
	return int(synced.Load()), int(failed.Load())
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
	RequestMode  string  `json:"request_mode"`  // auto, responses, chat, or messages (format-dependent)
	Charity      bool    `json:"charity"`
	Priority     int     `json:"priority"`
}

// CreateManualGroupKey 手动创建一个上游分组密钥，不经过登录/自动同步。
func (s *Service) CreateManualGroupKey(ctx context.Context, input ManualGroupKeyInput) (*storage.UpstreamGroupKey, error) {
	defer s.InvalidateSchedulingCache()
	groupName := strings.TrimSpace(input.GroupName)
	rawKey := normalizeUpstreamAPIKey(input.Key)
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
			MonitorEnabled: false,
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
	mode, modeSource, err := requestModeConfigForClientFormat(format, input.RequestMode)
	if err != nil {
		return nil, err
	}
	ratio := input.Ratio
	if ratio <= 0 {
		ratio = 1
	}
	// A manual channel can legitimately contain multiple keys for the same
	// visible group. Keep the first stable reference for editing, then attach a
	// non-reversible key fingerprint for additional keys so their independent
	// request protocol and authentication header are never overwritten.
	groupRef := "manual:" + strings.ToLower(groupName)
	if existing, findErr := s.groupKeys.FindByChannelGroup(ch.ID, groupRef); findErr != nil {
		return nil, findErr
	} else if existing != nil {
		existingSecret, decryptErr := s.cipher.Decrypt(existing.KeyCipher)
		if decryptErr != nil || subtle.ConstantTimeCompare([]byte(existingSecret), []byte(rawKey)) != 1 {
			groupRef += ":" + HashKey(rawKey)[:12]
		}
	}
	rec := &storage.UpstreamGroupKey{
		ChannelID:             ch.ID,
		ChannelName:           ch.Name,
		ChannelURL:            ch.SiteURL,
		ChannelType:           ch.Type,
		ClientFormat:          format,
		RequestMode:           mode,
		RequestModeSource:     modeSource,
		AuthMode:              defaultAuthModeForClientFormat(format),
		GroupRef:              groupRef,
		GroupName:             groupName,
		GroupDesc:             strings.TrimSpace(input.GroupDesc),
		Ratio:                 ratio,
		RatioScalePercent:     100,
		InputPricePerMillion:  storage.DefaultInputPricePerMillion,
		OutputPricePerMillion: storage.DefaultOutputPricePerMillion,
		Priority:              input.Priority,
		Charity:               input.Charity,
		Enabled:               true,
		KeyCipher:             cipher,
		// A protocol probe is a best-effort convenience, not an eligibility
		// gate. Keep a manually supplied key schedulable even if its provider
		// blocks probing or only permits the model used by real traffic.
		Status: "alive",
	}
	if err := s.groupKeys.Upsert(rec); err != nil {
		return nil, fmt.Errorf("保存分组失败: %w", err)
	}
	saved, err := s.groupKeys.FindByChannelGroup(ch.ID, groupRef)
	if err != nil || saved == nil {
		return saved, err
	}
	// Only automatic protocol configuration is probed. A manual protocol still
	// receives a header-only probe, because the replacement key may require a
	// different auth header from other keys at the same URL.
	if modeSource == "auto" {
		if detected, detectErr := s.DetectGroupRequestMode(ctx, saved.ID); detectErr == nil && detected != nil {
			return detected, nil
		}
	} else if detected, detectErr := s.DetectGroupAuthMode(ctx, saved.ID); detectErr == nil && detected != nil {
		return detected, nil
	}
	return saved, nil
}

// DeleteGroupKey 删除一个上游分组密钥记录。
// 用于用户手动清理"上游已经删掉、本地却残留并一直显示 dead"的分组。
// 只删本地记录，不去动上游（上游那边可能已经没了，也可能是用户手动贴的 key）。
func (s *Service) DeleteGroupKey(id uint) error {
	defer s.InvalidateSchedulingCache()
	if id == 0 {
		return errors.New("invalid group key id")
	}
	s.clearRuntimeDisable(id)
	return s.groupKeys.Delete(id)
}

// ClearGroupKeyCooldown 手动解除某个上游分组的冷却，立即恢复可调度。
func (s *Service) ClearGroupKeyCooldown(id uint) (*storage.UpstreamGroupKey, error) {
	defer s.InvalidateSchedulingCache()
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
	defer s.InvalidateSchedulingCache()
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
		if isManualBootstrapChannel(ch) {
			// Manual channels have no account-backed group list. Their manually
			// added keys must not be queried, overwritten, or removed here.
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
			if keyword, blocked := blockedBootstrapKeyKeyword(group); blocked {
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
		// A successful ListAPIKeyGroups call is the source of truth for automatic
		// groups. Delete every stale automatic record even if the returned list is
		// empty; otherwise groups removed upstream remain schedulable forever.
		// Manually added records remain intentionally outside this synchronization.
		if locals, err := s.groupKeys.ListByChannel(ch.ID); err == nil {
			for i := range locals {
				local := locals[i]
				if isManualGroupKey(&local) {
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
	// Group metadata defaults to Responses on creation, but the actual protocol
	// is discovered from the upstream instead of being operator-set.
	// Run this after reconciliation so every enabled channel key, including
	// previously created manual keys, has its upstream protocol refreshed from
	// a real probe. This changes only the local protocol capability flag; it
	// never syncs, edits, or removes manual channel data.
	if all, listErr := s.groupKeys.List(); listErr == nil {
		ids := make([]uint, 0, len(all))
		for i := range all {
			key := all[i]
			if key.Enabled && key.KeyCipher != "" {
				ids = append(ids, key.ID)
			}
		}
		s.detectGroupRequestModes(ids)
		result.ModelsSynced, result.ModelSyncFailed = s.syncBootstrapGroupModels(ctx, all)
	}
	return result, nil
}

func (s *Service) TestAllGroupKeys(ctx context.Context, batchSizes ...int) (*HealthResult, error) {
	batchSize := 0
	if len(batchSizes) > 0 {
		batchSize = batchSizes[0]
	}
	// 定时兜底扫描同样只测低倍率/公益渠道，倍率上限取系统设置里的可配值。
	return s.TestGroupKeys(ctx, HealthTestOptions{BatchSize: batchSize, MaxRatio: s.healthProbeMaxRatio()})
}

func (s *Service) TestGroupKeys(ctx context.Context, opts HealthTestOptions) (*HealthResult, error) {
	list, err := s.groupKeys.List()
	if err != nil {
		return nil, err
	}
	list = filterHealthTestGroupKeys(list, opts.GroupIDs)
	// One-click health checks cover only OpenAI-format groups.  Claude and Grok
	// use their own request contracts and remain available through per-group
	// testing, so they cannot be accidentally probed as an OpenAI model.
	list = filterOpenAIHealthGroups(list)
	if opts.MaxRatio > 0 {
		list = filterHealthGroupsByMaxRatio(list, opts.MaxRatio)
	}
	// The probe itself performs the protocol fallback and persists the winning
	// Responses/Chat mode. Keeping that work inside the one real health request
	// avoids sending duplicate probes before every batch while still correcting
	// legacy records automatically.

	batchSize := normalizeHealthProbeBatchSize(opts.BatchSize)
	if opts.Serial {
		batchSize = 1
	}
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
	if !opts.Serial {
		enabled = interleaveHealthTestGroupIndexes(list, enabled)
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

				// A queued group starts its timeout only after it owns the upstream
				// slot.  Otherwise a busy same-site queue would make a healthy group
				// expire before its first HTTP request.
				releaseChannelSlot := s.acquireHealthProbeUpstreamSlot(list[idx])
				// Use the same independent deadline as the single-group action.
				// The timeout begins after this job owns the upstream slot, so batch
				// scheduling cannot change the meaning of a health result.
				itemCtx, cancel := context.WithTimeout(probeCtx, s.healthRunBudget())
				item := s.testGroupKeyWithUpstreamSlot(itemCtx, &list[idx])
				releaseChannelSlot()
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
				if opts.Serial && opts.InterGroupDelay > 0 && job.pos+1 < len(enabled) {
					time.Sleep(opts.InterGroupDelay)
				}
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
			result.Items[i] = healthResultItemFromGroup(&list[i], "alive")
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

// interleaveHealthTestGroupIndexes ensures workers do not all block behind the
// first channel in the DB order.  With per-upstream serialization this keeps
// up to ten distinct upstreams probing in parallel, while every individual
// upstream receives only one request at a time.
func interleaveHealthTestGroupIndexes(list []storage.UpstreamGroupKey, enabled []int) []int {
	if len(enabled) < 2 {
		return enabled
	}
	queues := make(map[string][]int, len(enabled))
	order := make([]string, 0, len(enabled))
	for _, idx := range enabled {
		if idx < 0 || idx >= len(list) {
			continue
		}
		upstream := healthProbeUpstreamIdentity(list[idx])
		if upstream == "" {
			upstream = fmt.Sprintf("group:%d", list[idx].ID)
		}
		if _, exists := queues[upstream]; !exists {
			order = append(order, upstream)
		}
		queues[upstream] = append(queues[upstream], idx)
	}
	out := make([]int, 0, len(enabled))
	for len(out) < len(enabled) {
		for _, upstream := range order {
			queue := queues[upstream]
			if len(queue) == 0 {
				continue
			}
			out = append(out, queue[0])
			queues[upstream] = queue[1:]
		}
	}
	return out
}

func healthProbeUpstreamIdentity(key storage.UpstreamGroupKey) string {
	if url := strings.ToLower(strings.TrimRight(strings.TrimSpace(key.ChannelURL), "/")); url != "" {
		return "url:" + url
	}
	if key.ChannelID > 0 {
		return fmt.Sprintf("channel:%d", key.ChannelID)
	}
	return ""
}

// acquireHealthProbeUpstreamSlot is service-scoped so the one-click check and
// a per-group check cannot probe the same API Base URL concurrently. Different
// upstreams still run in parallel through the global batch workers.
func (s *Service) acquireHealthProbeUpstreamSlot(key storage.UpstreamGroupKey) func() {
	if s == nil || healthPerChannelParallel <= 0 {
		return func() {}
	}
	upstream := healthProbeUpstreamIdentity(key)
	if upstream == "" {
		return func() {}
	}
	created := make(chan struct{}, healthPerChannelParallel)
	actual, _ := s.healthProbeSlots.LoadOrStore(upstream, created)
	slot := actual.(chan struct{})
	slot <- struct{}{}
	return func() {
		if healthProbeUpstreamMinInterval > 0 {
			time.Sleep(healthProbeUpstreamMinInterval)
		}
		<-slot
	}
}

func normalizeHealthProbeBatchSize(size int) int {
	if size <= 0 {
		return defaultHealthProbeBatchSize
	}
	// One-click health checks deliberately execute in batches of at most ten.
	// Higher values made a small number of large upstreams return temporary 5XX
	// responses even though the same keys worked immediately afterwards.
	if size > defaultHealthProbeBatchSize {
		return defaultHealthProbeBatchSize
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
		Ratio:       effectiveGroupRatio(*key),
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
	ctx, cancel := context.WithTimeout(context.Background(), s.healthRunBudget())
	defer cancel()
	result := s.testGroupKey(ctx, key)
	return &result, nil
}

func filterOpenAIHealthGroups(groups []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	result := make([]storage.UpstreamGroupKey, 0, len(groups))
	for _, group := range groups {
		if normalizeClientFormat(group.ClientFormat) == "openai" {
			result = append(result, group)
		}
	}
	return result
}

func filterHealthGroupsByMaxRatio(groups []storage.UpstreamGroupKey, maxRatio float64) []storage.UpstreamGroupKey {
	if maxRatio <= 0 {
		return groups
	}
	result := make([]storage.UpstreamGroupKey, 0, len(groups))
	for _, group := range groups {
		if effectiveGroupRatio(group) <= maxRatio+1e-9 {
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
	normalized = ensureCodexResponsesLiteReasoningContext(normalized)
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

	policy, err := s.lookupRequestIPPolicy(r, requestIP)
	if err != nil {
		return failGatewayRequest(http.StatusInternalServerError, "gateway_error", err.Error())
	}
	if policy != nil && policy.Blocked {
		bannedMessage := ipBlockedMessage(policy)
		if shouldWriteResponsesTerminalForGatewayFailure(normalized) {
			return writeResponsesGatewayTextStream(w, normalized.RequestModel, bannedMessage)
		}
		return failGatewayRequest(http.StatusForbidden, "gateway_forbidden", bannedMessage)
	}
	rawKey := extractGatewayKey(r.Header)
	gatewayKey, err := s.Authenticate(rawKey, requestIP)
	if err != nil {
		if message, ok := s.gatewayLimitOrExpiredMessage(rawKey, nil, err); ok {
			if shouldWriteResponsesTerminalForGatewayFailure(normalized) {
				return writeResponsesGatewayTextStream(w, normalized.RequestModel, message)
			}
			return failGatewayRequest(http.StatusUnauthorized, "gateway_auth_failed", message)
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
		if message, ok := s.gatewayLimitOrExpiredMessage(rawKey, gatewayKey, errors.New("gateway key disabled")); ok {
			if shouldWriteResponsesTerminalForGatewayFailure(normalized) {
				return writeResponsesGatewayTextStream(w, normalized.RequestModel, message)
			}
			return failGatewayRequest(http.StatusUnauthorized, "gateway_auth_failed", message)
		}
		return failGatewayRequest(http.StatusUnauthorized, "gateway_auth_failed", "invalid gateway key")
	}
	if err := enforceGatewayQuota(gatewayKey); err != nil {
		if message, ok := s.gatewayLimitOrExpiredMessage(rawKey, gatewayKey, err); ok {
			if shouldWriteResponsesTerminalForGatewayFailure(normalized) {
				return writeResponsesGatewayTextStream(w, normalized.RequestModel, message)
			}
			return failGatewayRequest(http.StatusTooManyRequests, "gateway_quota_exceeded", message)
		}
		return failGatewayRequest(http.StatusTooManyRequests, "gateway_quota_exceeded", err.Error())
	}
	if isGatewayModelListRequest(r.Method, path) {
		return s.writeGatewayModelList(r.Context(), w, gatewayKey)
	}
	// Codex normally supplies previous_response_id or prompt_cache_key, but
	// independent requests do not always carry either.  Without a soft affinity
	// every such request is re-ranked from scratch and can bounce between two
	// healthy, similarly-priced channels.  Keep a short, per-key/per-IP/model
	// route affinity so a working channel is reused for cache warmth and lower
	// connection/first-token variance.  It is soft: unhealthy or cooling-down
	// channels still immediately fall back to another candidate.
	if normalized.AffinityKey == "" {
		normalized.AffinityKey = implicitRequestAffinityKey(gatewayKey, normalized)
	}

	now := time.Now()
	requestedModel := routingRequestModel(normalized)
	candidates, err := s.schedulingCandidates(now, requestedModel)
	if err != nil {
		return failGatewayRequest(http.StatusInternalServerError, "gateway_error", err.Error())
	}
	candidates = filterCandidatesForGatewayKey(gatewayKey, candidates)
	candidates = filterCandidatesForClientFormat(gatewayKey.ClientFormat, normalized.ResponseMode, candidates)
	candidates = filterSchedulableCandidates(candidates)
	if len(candidates) == 0 {
		message := gatewayKeyScopeEmptyMessage(gatewayKey)
		s.recordDispatchSkipLog(gatewayKey, nil, requestedModel, normalized.ClientIP, "failed", message)
		return failGatewayRequest(http.StatusServiceUnavailable, "upstream_unavailable", message)
	}
	candidates = s.filterKnownUnsupportedModelCandidates(candidates, requestedModel)
	if len(candidates) == 0 {
		message := fmt.Sprintf("no configured upstream supports requested model %q", requestedModel)
		s.recordDispatchSkipLog(gatewayKey, nil, requestedModel, normalized.ClientIP, "failed", message)
		return failGatewayRequest(http.StatusBadRequest, "model_not_supported", message)
	}
	candidates = s.orderCandidatesForRequest(candidates, normalized, gatewayKey)

	var errorsSeen []string
	var saturatedSeen []string
	var disabledSeen []string
	modelUnsupportedFailures := 0
	rememberUnsupportedModel := func(candidate storage.UpstreamGroupKey, errMsg string) {
		// Provider routers may return “model temporarily unavailable” with an
		// HTTP 503. That is a transient routing fault, not proof this upstream
		// can never serve the requested model.
		if requestedModel == "" || !isDefinitiveUnsupportedModelFailure(errMsg) {
			return
		}
		s.rememberCandidateModelCapability(candidate.ID, requestedModel, false)
		modelUnsupportedFailures++
	}
	delayedSameGroupFallback := make([]storage.UpstreamGroupKey, 0)
	delayedDispatchGroups := map[string]struct{}{}
	delayDispatchGroup := func(candidate storage.UpstreamGroupKey) {
		if group := dispatchGroupIdentity(candidate); group != "" {
			delayedDispatchGroups[group] = struct{}{}
		}
	}
	delaySameDispatchGroup := func(candidate storage.UpstreamGroupKey, errMsg string) {
		if !shouldDelaySameDispatchGroupAfterFailure(errMsg) {
			return
		}
		delayDispatchGroup(candidate)
	}
	// finalErr 承载"客户端错、换 key 也没用"路径的返回值。
	// 在 stream 已写字节 / 明确 client-side 400 等场景下，我们把 err 记进来后不再继续 fail-over。
	var finalErr error
	// attemptsUsed 统计"真正发起过上游尝试"的候选数（冷却/内存禁用跳过、并发饱和立即
	// 退回的都不计）。maxAttempts>0 时达到上限即停止继续 fail-over，避免候选池很大时
	// 逐个候选各等一个首字节窗口叠成分钟级等待。<=0 视为不限（逃生阀）。
	maxAttempts := s.maxDispatchAttempts()
	attemptsMade := 0
	slowFirstOutputAttempts := 0
	attemptCapReached := false
	for i := range candidates {
		candidate := candidates[i]
		if until, ok := candidateCooldownUntil(candidate, now); ok {
			message := cooldownMessage(candidate, until)
			disabledSeen = append(disabledSeen, message)
			delayDispatchGroup(candidate)
			continue
		}
		if until, ok := s.runtimeDisabledUntil(candidate.ID); ok {
			message := cooldownMessage(candidate, until)
			disabledSeen = append(disabledSeen, message)
			delayDispatchGroup(candidate)
			continue
		}
		if group := dispatchGroupIdentity(candidate); group != "" {
			if _, delayed := delayedDispatchGroups[group]; delayed {
				delayedSameGroupFallback = append(delayedSameGroupFallback, candidate)
				continue
			}
		}
		// 把单个候选的尝试封装到闭包里，用 defer release() 保证 panic / 早退时不泄漏并发额度。
		dispatchLog := s.startDispatchLog(gatewayKey, &candidate, requestedModel, normalized.ClientIP, attemptsMade+1)
		attemptCtx := context.WithValue(r.Context(), dispatchLogContextKey{}, dispatchLog.id)
		outcome := s.attemptCandidate(attemptCtx, gatewayKey, normalized, &candidate, w)
		// 只统计"真正发起过尝试"的候选（冷却/内存禁用被 continue 跳过的不计）。
		attemptsMade++

		switch outcome.kind {
		case candSuccess:
			s.finishDispatchLog(dispatchLog, outcome.logStatus, outcome.errMsg, outcome.usage)
			return nil
		case candSaturated:
			s.finishDispatchLog(dispatchLog, "saturated", "candidate is at concurrency limit", usageTokens{})
			saturatedSeen = append(saturatedSeen, fmt.Sprintf("%s/%s", candidate.ChannelName, candidate.GroupName))
			delayDispatchGroup(candidate)
			// 并发饱和不是真正把请求发给了上游、也没等首字节，不占用尝试预算。
			attemptsMade--
			continue
		case candRetryable:
			s.finishDispatchLog(dispatchLog, "switched", outcome.errMsg, usageTokens{})
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.ChannelName, candidate.GroupName, outcome.errMsg))
			rememberUnsupportedModel(candidate, outcome.errMsg)
			delaySameDispatchGroup(candidate, outcome.errMsg)
			if shouldMarkProxyFailure(outcome.errMsg) {
				s.markProxyFailure(candidate.ID, outcome.errMsg)
			}
			if isSlowFirstOutputFailure(outcome.errMsg) {
				slowFirstOutputAttempts++
				if slowFirstOutputAttempts >= 3 {
					attemptCapReached = true
					break
				}
			}
			// 达到单请求尝试上限即停止继续 fail-over，避免候选池很大时逐个各等一个
			// 首字节窗口叠成分钟级等待。maxAttempts<=0 表示不限（逃生阀）。
			if maxAttempts > 0 && attemptsMade >= maxAttempts {
				attemptCapReached = true
				break
			}
			continue
		case candFatal:
			// 明确"客户端错 / 已写字节无法切换"路径：仍然记一次失败方便下次跳过，
			// 但不再继续 fail-over，把当前错误吐给调用方。
			s.finishDispatchLog(dispatchLog, "failed", outcome.errMsg, usageTokens{})
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.ChannelName, candidate.GroupName, outcome.errMsg))
			rememberUnsupportedModel(candidate, outcome.errMsg)
			if outcome.markFailure && shouldMarkProxyFailure(outcome.errMsg) {
				s.markProxyFailure(candidate.ID, outcome.errMsg)
			}
			finalErr = outcome.err
			break
		}
		if finalErr != nil {
			return finalErr
		}
		// switch 里的 break 只跳出 switch，这里把"达到尝试上限"提升为跳出候选主循环。
		if attemptCapReached {
			break
		}
	}
	if !attemptCapReached && len(delayedSameGroupFallback) > 0 {
		delayedSameGroupFallback = s.orderCandidatesWithRuntime(delayedSameGroupFallback, requestedModel)
		for i := range delayedSameGroupFallback {
			candidate := delayedSameGroupFallback[i]
			if err := r.Context().Err(); err != nil {
				// 与主循环一致：客户端已断开就静默终止，不再用已取消的 context 尝试候选
				// （否则每个候选都会被误判成 network_error 冷却 300s，拖垮候选池）。
				s.recordDispatchSkipLog(gatewayKey, nil, requestedModel, normalized.ClientIP, "canceled", "client disconnected before dispatch completed")
				return cancelGatewayRequest("client disconnected during dispatch: " + err.Error())
			}
			if until, ok := candidateCooldownUntil(candidate, time.Now()); ok {
				message := cooldownMessage(candidate, until)
				disabledSeen = append(disabledSeen, message)
				continue
			}
			if until, ok := s.runtimeDisabledUntil(candidate.ID); ok {
				message := cooldownMessage(candidate, until)
				disabledSeen = append(disabledSeen, message)
				continue
			}
			dispatchLog := s.startDispatchLog(gatewayKey, &candidate, requestedModel, normalized.ClientIP, attemptsMade+1)
			attemptCtx := context.WithValue(r.Context(), dispatchLogContextKey{}, dispatchLog.id)
			outcome := s.attemptCandidate(attemptCtx, gatewayKey, normalized, &candidate, w)
			// 同组延迟兜底同样计入单请求尝试预算（并发饱和不计，见 candSaturated）。
			attemptsMade++
			switch outcome.kind {
			case candSuccess:
				s.finishDispatchLog(dispatchLog, outcome.logStatus, outcome.errMsg, outcome.usage)
				return nil
			case candSaturated:
				s.finishDispatchLog(dispatchLog, "saturated", "candidate is at concurrency limit", usageTokens{})
				saturatedSeen = append(saturatedSeen, fmt.Sprintf("%s/%s", candidate.ChannelName, candidate.GroupName))
				attemptsMade--
				continue
			case candRetryable:
				s.finishDispatchLog(dispatchLog, "switched", outcome.errMsg, usageTokens{})
				errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.ChannelName, candidate.GroupName, outcome.errMsg))
				rememberUnsupportedModel(candidate, outcome.errMsg)
				if shouldMarkProxyFailure(outcome.errMsg) {
					s.markProxyFailure(candidate.ID, outcome.errMsg)
				}
				if isSlowFirstOutputFailure(outcome.errMsg) {
					slowFirstOutputAttempts++
					if slowFirstOutputAttempts >= 3 {
						attemptCapReached = true
					}
				}
				if maxAttempts > 0 && attemptsMade >= maxAttempts {
					attemptCapReached = true
				}
				if attemptCapReached {
					break
				}
				continue
			case candFatal:
				s.finishDispatchLog(dispatchLog, "failed", outcome.errMsg, usageTokens{})
				errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.ChannelName, candidate.GroupName, outcome.errMsg))
				rememberUnsupportedModel(candidate, outcome.errMsg)
				if outcome.markFailure && shouldMarkProxyFailure(outcome.errMsg) {
					s.markProxyFailure(candidate.ID, outcome.errMsg)
				}
				finalErr = outcome.err
			}
			if finalErr != nil {
				return finalErr
			}
			if attemptCapReached {
				break
			}
		}
	}
	message := "all upstream group keys failed: " + strings.Join(errorsSeen, " | ")
	if requestedModel != "" && len(errorsSeen) > 0 && modelUnsupportedFailures == len(errorsSeen) &&
		len(saturatedSeen) == 0 && len(disabledSeen) == 0 {
		message = fmt.Sprintf("no configured upstream supports requested model %q", requestedModel)
	}
	if len(errorsSeen) == 0 && len(saturatedSeen) > 0 {
		message = "all upstream group keys are at concurrency limit: " + strings.Join(saturatedSeen, " | ")
	}
	if len(errorsSeen) == 0 && len(saturatedSeen) == 0 && len(disabledSeen) > 0 {
		message = "upstreams temporarily unavailable; retry after cooldown: " + strings.Join(disabledSeen, " | ")
	} else if len(disabledSeen) > 0 {
		message += " | temporarily unavailable: " + strings.Join(disabledSeen, " | ")
	}
	s.recordDispatchSkipLog(gatewayKey, nil, requestedModel, normalized.ClientIP, "failed", message)
	return failGatewayRequest(http.StatusServiceUnavailable, "upstream_unavailable", message)
}

func isGatewayModelListRequest(method, path string) bool {
	if method != http.MethodGet {
		return false
	}
	if index := strings.Index(path, "?"); index >= 0 {
		path = path[:index]
	}
	path = "/" + strings.Trim(strings.TrimSpace(path), "/")
	return path == healthPath
}

func (s *Service) writeGatewayModelList(ctx context.Context, w http.ResponseWriter, key *storage.GatewayKey) error {
	candidates, err := s.schedulingCandidates(time.Now(), "")
	if err != nil {
		return &GatewayError{Status: http.StatusInternalServerError, Body: jsonError(err.Error())}
	}
	candidates = filterCandidatesForGatewayKey(key, candidates)
	candidates = filterCandidatesForClientFormat(key.ClientFormat, "responses", candidates)
	models := make(map[string]map[string]any)
	for index := range candidates {
		candidate := candidates[index]
		if !candidateSchedulable(candidate) {
			continue
		}
		if _, cooling := candidateCooldownUntil(candidate, time.Now()); cooling {
			continue
		}
		if _, cooling := s.runtimeDisabledUntil(candidate.ID); cooling {
			continue
		}
		catalog := decodeModelNames(candidate.AvailableModels)
		if candidate.ModelRestrictionEnabled {
			catalog = decodeModelNames(candidate.SupportedModels)
		} else if len(catalog) == 0 {
			catalog = decodeModelNames(candidate.SupportedModels)
		}
		if len(catalog) == 0 && !candidate.ModelRestrictionEnabled {
			switch candidate.ChannelType {
			case storage.ChannelTypeGrokPool:
				catalog = append([]string(nil), defaultGrokPoolModelCatalog...)
			case storage.ChannelTypeChatGPTPool:
				catalog = append([]string(nil), config.DefaultOpenAIHealthProbeModels...)
			default:
				if candidate.HealthProbeModel != "" {
					catalog = []string{candidate.HealthProbeModel}
				}
			}
		}
		for _, model := range catalog {
			model = strings.TrimSpace(model)
			if model == "" {
				continue
			}
			if pool, fixed := oauthPoolForCandidate(&candidate); fixed {
				poolService := s.oauthPoolService()
				if poolService == nil {
					continue
				}
				available, availabilityErr := poolService.HasAvailable(ctx, pool, model)
				if availabilityErr != nil || !available {
					continue
				}
			}
			normalized := normalizeModelCapabilityKey(model)
			if _, exists := models[normalized]; exists {
				continue
			}
			models[normalized] = map[string]any{
				"id": model, "object": "model", "created": int64(0), "owned_by": firstNonEmpty(candidate.ChannelName, "gateway"),
			}
		}
	}
	keys := make([]string, 0, len(models))
	for model := range models {
		keys = append(keys, model)
	}
	sort.Strings(keys)
	data := make([]map[string]any, 0, len(keys))
	for _, model := range keys {
		data = append(data, models[model])
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	return json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
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
	usage       usageTokens
	logStatus   string
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
	release, ok := s.tryAcquireScheduledCandidate(candidate)
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

const oauthPoolAccountAttemptsPerCandidate = 3

type oauthPoolLeaseContextKey struct{}

type oauthPoolAttemptContext struct {
	service *oauthpool.Service
	lease   *oauthpool.DispatchLease
}

func oauthPoolForCandidate(candidate *storage.UpstreamGroupKey) (storage.OAuthPool, bool) {
	if candidate == nil {
		return "", false
	}
	switch candidate.ChannelType {
	case storage.ChannelTypeChatGPTPool:
		return storage.OAuthPoolChatGPT, true
	case storage.ChannelTypeGrokPool:
		return storage.OAuthPoolGrok, true
	default:
		return "", false
	}
}

func oauthPoolLeaseFromContext(ctx context.Context) *oauthPoolAttemptContext {
	attempt, _ := ctx.Value(oauthPoolLeaseContextKey{}).(*oauthPoolAttemptContext)
	return attempt
}

func (s *Service) streamProxyOAuthPoolCandidate(ctx context.Context, request normalizedRequest, key *storage.UpstreamGroupKey, w http.ResponseWriter, pool storage.OAuthPool) (bool, usageTokens, error) {
	service := s.oauthPoolService()
	if service == nil {
		return true, usageTokens{}, errors.New("oauth_pool_unavailable: OAuth pool service is not configured")
	}
	poolCtx := ctx
	cancel := func() {}
	if budget := s.streamFirstEventBudget(); budget > 0 {
		poolCtx, cancel = context.WithTimeout(ctx, budget)
	}
	defer cancel()
	excluded := make(map[uint]bool, oauthPoolAccountAttemptsPerCandidate)
	var lastErr error
	for attempt := 0; attempt < oauthPoolAccountAttemptsPerCandidate; attempt++ {
		lease, err := service.AcquireContext(poolCtx, pool, routingRequestModel(request), excluded)
		if err != nil {
			if lastErr != nil {
				return true, usageTokens{}, fmt.Errorf("oauth_pool_unavailable: %s pool exhausted after %d attempts: %w", pool, attempt, lastErr)
			}
			return true, usageTokens{}, fmt.Errorf("oauth_pool_unavailable: %s: %w", pool, err)
		}
		excluded[lease.Account.ID] = true
		s.attachOAuthDispatchAccount(poolCtx, lease, attempt+1)
		attemptCtx := context.WithValue(poolCtx, oauthPoolLeaseContextKey{}, &oauthPoolAttemptContext{service: service, lease: lease})
		retry, usage, err := s.streamProxyCandidate(attemptCtx, request, key, w)
		if err == nil {
			lease.ReportSuccess()
			s.recordOAuthPoolSuccess(lease)
			lease.Release()
			return retry, usage, nil
		}
		if !retry && !shouldCountOAuthAccountFailure(err, 0) {
			lease.Ignore()
			lease.Release()
			return retry, usage, err
		}
		failure := oauthPoolFailure(err, 0, nil)
		lease.ReportFailure(failure)
		s.recordOAuthPoolFailure(lease, failure)
		lease.Release()
		lastErr = err
		if !retry || responseWriterStarted(w) {
			return retry, usage, err
		}
	}
	return true, usageTokens{}, fmt.Errorf("oauth_pool_unavailable: %s pool account retry limit reached: %w", pool, lastErr)
}

func oauthPoolFailure(err error, status int, header http.Header) oauthpool.Failure {
	if gatewayErr := new(GatewayError); errors.As(err, &gatewayErr) {
		status = gatewayErr.Status
		header = gatewayErr.Header
	}
	retryAfter := time.Duration(0)
	if header != nil {
		raw := strings.TrimSpace(header.Get("Retry-After"))
		if seconds, parseErr := strconv.Atoi(raw); parseErr == nil && seconds > 0 {
			retryAfter = time.Duration(seconds) * time.Second
		} else if parsed, parseErr := http.ParseTime(raw); parseErr == nil {
			retryAfter = time.Until(parsed)
		}
	}
	failure := oauthpool.FailureFromHTTP(status, retryAfter, err)
	if status == http.StatusForbidden && looksLikeAuthFailure(status, errorText(err)) {
		failure.Kind = oauthpool.FailureAuth
	}
	return failure
}

func oauthAccountRuntimeKey(pool storage.OAuthPool, id uint) string {
	return string(pool) + ":" + strconv.FormatUint(uint64(id), 10)
}

func (s *Service) recordOAuthPoolFailure(lease *oauthpool.DispatchLease, failure oauthpool.Failure) {
	if lease == nil {
		return
	}
	repository := s.oauthAccountRepository()
	if repository == nil {
		return
	}
	status := storage.OAuthStatusCooling
	var disabledUntil *time.Time
	now := time.Now().UTC()
	switch failure.Kind {
	case oauthpool.FailureAuth, oauthpool.FailurePermanent:
		status = storage.OAuthStatusDead
	case oauthpool.FailureRateLimit:
		status = storage.OAuthStatusRateLimited
		cooldown := failure.RetryAfter
		if cooldown <= 0 {
			cooldown = time.Minute
		}
		until := now.Add(cooldown)
		disabledUntil = &until
	default:
		until := now.Add(time.Minute)
		disabledUntil = &until
	}
	message := "OAuth upstream request failed"
	if failure.Err != nil {
		message = failure.Err.Error()
	}
	if err := repository.RecordRuntimeFailure(lease.Pool, lease.Account.ID, status, message, disabledUntil, 3); err != nil {
		if s.log != nil {
			s.log.Warn("persist OAuth pool failure", "pool", lease.Pool, "account_id", lease.Account.ID, "err", err)
		}
		return
	}
	s.oauthFailureDirty.Store(oauthAccountRuntimeKey(lease.Pool, lease.Account.ID), struct{}{})
}

func (s *Service) recordOAuthPoolSuccess(lease *oauthpool.DispatchLease) {
	if lease == nil {
		return
	}
	key := oauthAccountRuntimeKey(lease.Pool, lease.Account.ID)
	_, dirty := s.oauthFailureDirty.LoadAndDelete(key)
	needsPersistence := dirty || lease.HalfOpen || lease.Account.ConsecutiveFails > 0 || lease.Account.Status != storage.OAuthStatusAlive
	if !needsPersistence {
		return
	}
	repository := s.oauthAccountRepository()
	if repository == nil {
		return
	}
	if err := repository.RecordRuntimeSuccess(lease.Pool, lease.Account.ID, time.Now().UTC()); err != nil {
		s.oauthFailureDirty.Store(key, struct{}{})
		if s.log != nil {
			s.log.Warn("persist OAuth pool recovery", "pool", lease.Pool, "account_id", lease.Account.ID, "err", err)
		}
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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
		return stripCodexResponsesLiteHeader(request.altWithFallbackToSelf())
	}
	switch normalizeUpstreamRequestMode(candidate.RequestMode) {
	case "chat":
		if requestRequiresNativeResponsesWire(request) {
			return request
		}
		return stripCodexResponsesLiteHeader(request.alt())
	default:
		return request
	}
}

func requestRequiresNativeResponsesWire(request normalizedRequest) bool {
	if request.ResponseMode != "responses" {
		return false
	}
	model := normalizeModelCapabilityKey(routingRequestModel(request))
	if strings.HasPrefix(model, "gpt-5.6") {
		return true
	}
	if strings.TrimSpace(headerValueCaseInsensitive(request.Header, codexResponsesLiteHeader)) != "" {
		return true
	}
	if len(request.ToolKinds) > 0 {
		return true
	}
	return responsesBodyRequiresNativeWire(request.Body)
}

func responsesBodyRequiresNativeWire(body []byte) bool {
	var raw map[string]any
	if json.Unmarshal(body, &raw) != nil || raw == nil {
		return false
	}
	for _, key := range []string{"tools", "tool_choice", "parallel_tool_calls", "previous_response_id", "response_id"} {
		if meaningfulJSONValue(raw[key]) {
			return true
		}
	}
	if includeRequiresNativeResponses(raw["include"]) {
		return true
	}
	input, _ := raw["input"].([]any)
	for _, value := range input {
		item, _ := value.(map[string]any)
		if item == nil {
			continue
		}
		typ := strings.ToLower(strings.TrimSpace(stringValue(item["type"])))
		if typ == "function_call" || typ == "custom_tool_call" ||
			typ == "function_call_output" || strings.HasSuffix(typ, "_call_output") {
			return true
		}
	}
	return false
}

func includeRequiresNativeResponses(value any) bool {
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			if strings.EqualFold(strings.TrimSpace(stringValue(item)), "reasoning.encrypted_content") {
				return true
			}
		}
	case []string:
		for _, item := range v {
			if strings.EqualFold(strings.TrimSpace(item), "reasoning.encrypted_content") {
				return true
			}
		}
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "reasoning.encrypted_content")
	}
	return false
}

func meaningfulJSONValue(value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(v) != ""
	case []any:
		return len(v) > 0
	case []string:
		return len(v) > 0
	case map[string]any:
		return len(v) > 0
	case bool:
		return v
	default:
		return true
	}
}

const codexResponsesLiteHeader = "X-OpenAI-Internal-Codex-Responses-Lite"

// ensureCodexResponsesLiteReasoningContext honors the contract advertised by
// Codex Responses-Lite. Some OpenAI-compatible upstreams reject the complete
// request unless reasoning.context is exactly all_turns; keeping the caller's
// other reasoning settings intact avoids a needless multi-key failover storm.
func ensureCodexResponsesLiteReasoningContext(request normalizedRequest) normalizedRequest {
	if request.ResponseMode != "responses" || strings.TrimSpace(headerValueCaseInsensitive(request.Header, codexResponsesLiteHeader)) == "" {
		return request
	}
	var raw map[string]any
	if json.Unmarshal(request.Body, &raw) != nil || raw == nil {
		return request
	}
	_, suppliedReasoning := raw["reasoning"]
	reasoning, _ := raw["reasoning"].(map[string]any)
	if reasoning == nil {
		reasoning = map[string]any{}
	}
	if !strings.EqualFold(strings.TrimSpace(stringValue(reasoning["context"])), "all_turns") {
		reasoning["context"] = "all_turns"
	}
	raw["reasoning"] = reasoning
	// Codex carries encrypted reasoning between response/tool turns. Native
	// Responses upstreams need it explicitly included; without it a model can
	// still answer text but lose the context required to continue a tool edit.
	if suppliedReasoning {
		raw["include"] = appendUniqueStringValue(raw["include"], "reasoning.encrypted_content")
	}
	if body, err := json.Marshal(raw); err == nil {
		request.Body = body
	}
	return request
}

func appendUniqueStringValue(value any, wanted string) []any {
	wanted = strings.TrimSpace(wanted)
	items := make([]any, 0, 2)
	switch current := value.(type) {
	case []any:
		items = append(items, current...)
	case []string:
		for _, item := range current {
			items = append(items, item)
		}
	case string:
		if strings.TrimSpace(current) != "" {
			items = append(items, current)
		}
	}
	for _, item := range items {
		if strings.EqualFold(strings.TrimSpace(stringValue(item)), wanted) {
			return items
		}
	}
	if wanted != "" {
		items = append(items, wanted)
	}
	return items
}

// The Lite header belongs to native Responses traffic. A fallback Chat request
// cannot honor it and forwarding it can make a compatible chat-only upstream
// reject an otherwise valid conversion.
func stripCodexResponsesLiteHeader(request normalizedRequest) normalizedRequest {
	for key := range request.Header {
		if strings.EqualFold(key, codexResponsesLiteHeader) {
			request.Header.Del(key)
		}
	}
	return request
}

func headerValueCaseInsensitive(header http.Header, name string) string {
	if value := header.Get(name); value != "" {
		return value
	}
	for key, values := range header {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return values[0]
		}
	}
	return ""
}

func shouldPreferChatBridgeForResponsesStream(request normalizedRequest, candidate *storage.UpstreamGroupKey) bool {
	// Keep native Responses as the first path whenever the upstream is marked
	// as Responses-capable.  Falling back to chat before trying /v1/responses
	// can drop Codex reasoning/tool fields and hurts prompt-cache affinity.
	return false
}

const openAIResponsesBeta = "responses=experimental"

func userAgentProductName(userAgent string) (string, bool) {
	userAgent = strings.TrimSpace(userAgent)
	if userAgent == "" {
		return "", false
	}
	end := len(userAgent)
	for i, r := range userAgent {
		if r == '/' || r == '(' {
			end = i
			break
		}
	}
	product := strings.TrimSpace(userAgent[:end])
	if product == "" || len(product) > 64 {
		return "", false
	}
	for i := 0; i < len(product); i++ {
		if product[i] < 0x20 || product[i] > 0x7e {
			return "", false
		}
	}
	return product, true
}

func codexOriginatorFromUserAgent(userAgent string) (string, bool) {
	product, ok := userAgentProductName(userAgent)
	if !ok {
		return "", false
	}
	normalized := strings.ToLower(product)
	switch normalized {
	case "codex_cli_rs", "codex-tui", "codex_vscode", "codex_vscode_copilot",
		"codex_app", "codex_chatgpt_desktop", "codex_atlas", "codex_exec", "codex_sdk_ts":
		return normalized, true
	default:
		if strings.HasPrefix(normalized, "codex ") {
			return product, true
		}
		return "", false
	}
}

func applyOpenAIRequestIdentity(header http.Header, synthesizedUserAgent bool) {
	if synthesizedUserAgent {
		originator, ok := userAgentProductName(header.Get("User-Agent"))
		if !ok {
			originator = config.DefaultCodexOriginator
		}
		if canonical, ok := codexOriginatorFromUserAgent(header.Get("User-Agent")); ok {
			originator = canonical
		}
		header.Set("Originator", originator)
		header.Set("Version", config.DefaultCodexVersion)
		header.Set("OpenAI-Beta", openAIResponsesBeta)
		return
	}
	if originator, ok := codexOriginatorFromUserAgent(header.Get("User-Agent")); ok {
		// The Codex upstream validates Originator against the first User-Agent
		// product. Keep the real inbound UA byte-for-byte and repair only the
		// paired identity field when a proxy or client supplied a stale value.
		header.Set("Originator", originator)
	}
}

// applyUpstreamAuthHeaders applies the credential contract detected for this
// exact upstream key. Do not derive it only from the channel: one relay can
// have both a Bearer key and an x-api-key key at the same time.
//
// Existing manual records can contain a pasted header value such as
// "Authorization: Bearer sk-...". Normalize it here as well as when saving,
// so those records recover without an administrator having to delete and add
// the channel again.
func (s *Service) applyUpstreamAuthHeaders(header http.Header, key *storage.UpstreamGroupKey, upstreamKey string) {
	upstreamKey = normalizeUpstreamAPIKey(upstreamKey)
	header.Del("Authorization")
	header.Del("X-Api-Key")
	if upstreamAuthModeForKey(key) == "x_api_key" {
		header.Set("X-Api-Key", upstreamKey)
	} else {
		header.Set("Authorization", "Bearer "+upstreamKey)
	}
	header.Set("Content-Type", "application/json")
	if key != nil {
		header.Set("X-Gateway-Group", key.GroupName)
	}
	if key != nil && normalizeClientFormat(key.ClientFormat) == "claude" && header.Get("Anthropic-Version") == "" {
		header.Set("Anthropic-Version", "2023-06-01")
	}
	if key != nil && normalizeClientFormat(key.ClientFormat) == "grok" {
		header.Set("Accept", "application/json, text/event-stream")
		header.Set("User-Agent", "upstream-ops-grok/1.0")
		return
	}
	synthesizedUserAgent := strings.TrimSpace(header.Get("User-Agent")) == ""
	if synthesizedUserAgent {
		configured := strings.TrimSpace(s.upstreamConfig().UserAgent)
		if configured != "" {
			header.Set("User-Agent", configured)
		} else {
			header.Set("User-Agent", config.DefaultUpstreamUserAgent)
		}
	}
	if key != nil && normalizeClientFormat(key.ClientFormat) == "openai" {
		applyOpenAIRequestIdentity(header, synthesizedUserAgent)
	}
}

// decryptUpstreamAPIKey loads a stored upstream credential and accepts legacy
// manual records that were saved with a pasted Authorization/X-Api-Key prefix.
// The normalized secret is kept in memory only and must never be logged.
func (s *Service) decryptUpstreamAPIKey(key *storage.UpstreamGroupKey) (string, error) {
	if key == nil {
		return "", errors.New("upstream key is required")
	}
	raw, err := s.cipher.Decrypt(key.KeyCipher)
	if err != nil {
		return "", err
	}
	raw = normalizeUpstreamAPIKey(raw)
	if raw == "" {
		return "", errors.New("upstream key cannot be empty")
	}
	return raw, nil
}

func alternateUpstreamAuthMode(key *storage.UpstreamGroupKey) string {
	if upstreamAuthModeForKey(key) == "x_api_key" {
		return "bearer"
	}
	return "x_api_key"
}

func shouldRetryWithAlternateAuthHeader(err error) bool {
	if err == nil {
		return false
	}
	var gatewayErr *GatewayError
	if errors.As(err, &gatewayErr) {
		// Many compatible relays return 403 (rather than 401) when the key is
		// sent in the wrong header. This is still safe to retry once because no
		// upstream generation bytes have been sent to the client yet.
		if gatewayErr.Status == http.StatusUnauthorized || gatewayErr.Status == http.StatusForbidden {
			return true
		}
		return looksLikeAuthFailure(gatewayErr.Status, string(gatewayErr.Body))
	}
	return looksLikeAuthFailure(0, err.Error())
}

func (s *Service) persistDetectedAuthMode(key *storage.UpstreamGroupKey, mode string) {
	if key == nil {
		return
	}
	mode = normalizeUpstreamAuthMode(mode)
	key.AuthMode = mode
	if err := s.groupKeys.UpdateAuthMode(key.ID, mode); err != nil && s.log != nil {
		s.log.Warn("persist detected upstream authentication header", "group_key_id", key.ID, "err", err)
	}
	s.InvalidateSchedulingCache()
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
				return candOutcome{kind: candSuccess, errMsg: errMsg, logStatus: "failed"}
			} else {
				return candOutcome{kind: candFatal, err: writeErr, errMsg: writeErr.Error(), markFailure: false}
			}
		}
		return candOutcome{kind: candFatal, err: err, errMsg: errMsg, markFailure: markFailure}
	}
	retry, usage, err := s.streamProxyCandidate(ctx, normalized, candidate, timedWriter)
	_, fixedPoolCandidate := oauthPoolForCandidate(candidate)
	if err != nil && !fixedPoolCandidate && !timedWriter.Started() && shouldRetryWithAlternateAuthHeader(err) {
		alternate := *candidate
		alternate.AuthMode = alternateUpstreamAuthMode(candidate)
		retry, usage, err = s.streamProxyCandidate(ctx, normalized, &alternate, timedWriter)
		if err == nil {
			s.persistDetectedAuthMode(candidate, alternate.AuthMode)
		}
	}
	if err == nil {
		s.calculateLocalUsage(&usage, normalized, candidate)
		duration := time.Since(start)
		s.rememberCandidateModelCapability(candidate.ID, routingRequestModel(normalized), true)
		usage.FirstTokenMS = timedWriter.FirstTokenMS()
		usage.DurationMS = duration.Milliseconds()
		s.recordRuntimeSuccess(candidate.ID, duration, time.Duration(usage.FirstTokenMS)*time.Millisecond)
		_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, usage.Cached, gatewayUsageCost(usage, candidate), time.Now())
		_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
		s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
		if usage.SoftFailure != "" {
			s.markProxyFailure(candidate.ID, usage.SoftFailure)
		}
		return candOutcome{kind: candSuccess, usage: usage}
	}
	errMsg := sanitize.Truncate(sanitize.RedactText(err.Error()), 2000)
	// The downstream client may close a long-running stream at any time.  That
	// cancels this request context and is not evidence that the upstream is
	// unhealthy, so it must not trigger the five-minute scheduler cooldown.
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		if shouldWriteResponsesTerminalForGatewayFailure(normalized) {
			if writeErr := writeResponsesGatewayCancelledStream(w, "gateway_request_cancelled", "请求已取消。"); writeErr == nil {
				return candOutcome{kind: candSuccess, errMsg: err.Error(), logStatus: "interrupted"}
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
		if !timedWriter.Started() && shouldFailoverBeforeStreamWrite(err) {
			return candOutcome{kind: candRetryable, err: err, errMsg: errMsg, markFailure: true}
		}
		return failBeforeStreamBody(err, errMsg, true)
	}
	if fallback, reason, ok := fallbackRequestAfterCandidateFailure(normalized, candidate, errMsg, timedWriter.Started()); ok {
		start = time.Now()
		timedWriter = &timingResponseWriter{ResponseWriter: w, start: start}
		retry, usage, err = s.streamProxyCandidate(ctx, fallback, candidate, timedWriter)
		if err == nil {
			s.calculateLocalUsage(&usage, normalized, candidate)
			s.rememberSuccessfulProtocolFallback(candidate, normalized, fallback)
			duration := time.Since(start)
			s.rememberCandidateModelCapability(candidate.ID, routingRequestModel(normalized), true)
			usage.FirstTokenMS = timedWriter.FirstTokenMS()
			usage.DurationMS = duration.Milliseconds()
			s.recordRuntimeSuccess(candidate.ID, duration, time.Duration(usage.FirstTokenMS)*time.Millisecond)
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, usage.Cached, gatewayUsageCost(usage, candidate), time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			if usage.SoftFailure != "" {
				s.markProxyFailure(candidate.ID, usage.SoftFailure)
			}
			return candOutcome{kind: candSuccess, usage: usage}
		}
		errMsg = reason + " retry failed: " + err.Error()
		if !retry {
			if !timedWriter.Started() && shouldFailoverBeforeStreamWrite(err) {
				return candOutcome{kind: candRetryable, err: err, errMsg: errMsg, markFailure: true}
			}
			return failBeforeStreamBody(err, errMsg, true)
		}
	}
	if rectified, reason, ok := s.rectifyAfterFailure(normalized, errMsg); ok {
		start = time.Now()
		timedWriter = &timingResponseWriter{ResponseWriter: w, start: start}
		retry, usage, err = s.streamProxyCandidate(ctx, rectified, candidate, timedWriter)
		if err == nil {
			s.calculateLocalUsage(&usage, normalized, candidate)
			duration := time.Since(start)
			s.rememberCandidateModelCapability(candidate.ID, routingRequestModel(normalized), true)
			usage.FirstTokenMS = timedWriter.FirstTokenMS()
			usage.DurationMS = duration.Milliseconds()
			s.recordRuntimeSuccess(candidate.ID, duration, time.Duration(usage.FirstTokenMS)*time.Millisecond)
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, usage.Cached, gatewayUsageCost(usage, candidate), time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			if usage.SoftFailure != "" {
				s.markProxyFailure(candidate.ID, usage.SoftFailure)
			}
			return candOutcome{kind: candSuccess, usage: usage}
		}
		errMsg = reason + " retry failed: " + err.Error()
	}
	if retry {
		return candOutcome{kind: candRetryable, err: err, errMsg: errMsg}
	}
	if !timedWriter.Started() && shouldFailoverBeforeStreamWrite(err) {
		return candOutcome{kind: candRetryable, err: err, errMsg: errMsg, markFailure: true}
	}
	// 流已经开始写 / 明确 fatal：仍然记一次失败（这样下次调度不会又选中这个坏候选），
	// 但不再切候选（否则会往同一个 ResponseWriter 上二次写头/写字节）。
	return failBeforeStreamBody(err, errMsg, true)
}

// shouldFailoverBeforeStreamWrite separates an upstream-specific refusal from
// a malformed client request. When no downstream byte has been written yet,
// 401/403/404/429/5xx from one upstream must not become a final Codex error:
// another healthy Key in the same charity group can still serve the request.
func shouldFailoverBeforeStreamWrite(err error) bool {
	if err == nil {
		return false
	}
	var gatewayErr *GatewayError
	if !errors.As(err, &gatewayErr) {
		return true
	}
	switch gatewayErr.Status {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return false
	default:
		return gatewayErr.Status >= http.StatusUnauthorized && gatewayErr.Status <= http.StatusNetworkAuthenticationRequired
	}
}

func isSlowFirstOutputFailure(message string) bool {
	message = strings.ToLower(strings.TrimSpace(message))
	if message == "" {
		return false
	}
	for _, marker := range []string{
		"timeout awaiting response headers",
		"did not send a usable generation event within",
		"did not produce first output within",
		"first_output_timeout",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

// streamCandidatePreflightBudget 返回 preflight（首字节落地前的不可见缓冲阶段）
// 还能等多久。它就是"首字节窗口减去已消耗的响应头等待时间"，不再做秒级二次封顶：
// 早期的 streamPreflightTimeout=3s 封顶会在首字节窗口放宽后仍把 preflight 砍到 3s，
// 从而继续误杀 reasoning 阶段较长的推理模型。firstOutputBudget 由调用方按配置传入。
func streamCandidatePreflightBudget(firstOutputBudget, elapsed time.Duration) time.Duration {
	remaining := firstOutputBudget - elapsed
	if remaining <= 0 {
		return 0
	}
	return remaining
}

type firstOutputGuard struct {
	mu       sync.Mutex
	cancel   context.CancelFunc
	timer    *time.Timer
	ready    bool
	timedOut bool
	timeout  time.Duration
}

func newFirstOutputGuard(parent context.Context, timeout time.Duration) (context.Context, *firstOutputGuard) {
	ctx, cancel := context.WithCancel(parent)
	guard := &firstOutputGuard{cancel: cancel, timeout: timeout}
	if timeout > 0 {
		guard.timer = time.AfterFunc(timeout, func() {
			guard.mu.Lock()
			defer guard.mu.Unlock()
			if guard.ready {
				return
			}
			guard.timedOut = true
			guard.cancel()
		})
	}
	return ctx, guard
}

func (g *firstOutputGuard) MarkReady() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.timedOut {
		return false
	}
	g.ready = true
	if g.timer != nil {
		g.timer.Stop()
	}
	return true
}

func (g *firstOutputGuard) TimedOut() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.timedOut
}

func (g *firstOutputGuard) Close() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.ready = true
	if g.timer != nil {
		g.timer.Stop()
	}
	g.mu.Unlock()
	g.cancel()
}

func (g *firstOutputGuard) timeoutError() error {
	timeout := streamFirstEventTimeout
	if g != nil && g.timeout > 0 {
		timeout = g.timeout
	}
	return fmt.Errorf("upstream did not produce first output within %s", timeout)
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
	_, fixedPoolCandidate := oauthPoolForCandidate(candidate)
	if err != nil && !fixedPoolCandidate && shouldRetryWithAlternateAuthHeader(err) {
		alternate := *candidate
		alternate.AuthMode = alternateUpstreamAuthMode(candidate)
		status, header, respBody, retry, err = s.tryProxyCandidate(ctx, normalized, &alternate)
		if err == nil {
			s.persistDetectedAuthMode(candidate, alternate.AuthMode)
		}
	}
	if err == nil {
		duration := time.Since(start)
		s.rememberCandidateModelCapability(candidate.ID, routingRequestModel(normalized), true)
		s.recordRuntimeSuccess(candidate.ID, duration, duration)
		usage := extractUsage(respBody)
		usage.GeneratedText = generatedTextFromResponse(respBody)
		s.calculateLocalUsage(&usage, normalized, candidate)
		usage.FirstTokenMS = duration.Milliseconds()
		usage.DurationMS = duration.Milliseconds()
		_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, usage.Cached, gatewayUsageCost(usage, candidate), time.Now())
		_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
		s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
		writeProxyResponse(w, status, header, respBody, candidate, normalized.ResponseMode)
		return candOutcome{kind: candSuccess, usage: usage}
	}
	// 下游客户端随时可能中断一个非流式请求（超时、导航离开、代理断连）。这会取消本请求
	// 的 context，client.Do 返回包着 context.Canceled 的错误（status=0），绝不代表上游不健康。
	// 必须在这里显式拦下：否则它会被 healthFailureStatus 归到 network_error → 冷却 300s，
	// 且调度循环会拿着已取消的 context 继续尝试后续每个候选、逐个瞬间失败并各自冷却，
	// 一次客户端断开就能把整个候选池打进冷却。对齐 attemptStream 的 context.Canceled 处理：
	// 返回 fatal 且不 markFailure，让调度循环停止 fail-over。
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return candOutcome{kind: candFatal, err: err, errMsg: err.Error(), markFailure: false}
	}
	errMsg := sanitize.Truncate(sanitize.RedactText(err.Error()), 2000)
	if fallback, reason, ok := fallbackRequestAfterCandidateFailure(normalized, candidate, errMsg, false); ok {
		start = time.Now()
		status, header, respBody, retry, err = s.tryProxyCandidate(ctx, fallback, candidate)
		if err == nil {
			s.rememberSuccessfulProtocolFallback(candidate, normalized, fallback)
			duration := time.Since(start)
			s.rememberCandidateModelCapability(candidate.ID, routingRequestModel(normalized), true)
			s.recordRuntimeSuccess(candidate.ID, duration, duration)
			usage := extractUsage(respBody)
			usage.GeneratedText = generatedTextFromResponse(respBody)
			s.calculateLocalUsage(&usage, normalized, candidate)
			usage.FirstTokenMS = duration.Milliseconds()
			usage.DurationMS = duration.Milliseconds()
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, usage.Cached, gatewayUsageCost(usage, candidate), time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			writeProxyResponse(w, status, header, respBody, candidate, fallback.ResponseMode)
			return candOutcome{kind: candSuccess, usage: usage}
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
			s.rememberCandidateModelCapability(candidate.ID, routingRequestModel(normalized), true)
			s.recordRuntimeSuccess(candidate.ID, duration, duration)
			usage := extractUsage(respBody)
			usage.GeneratedText = generatedTextFromResponse(respBody)
			s.calculateLocalUsage(&usage, normalized, candidate)
			usage.FirstTokenMS = duration.Milliseconds()
			usage.DurationMS = duration.Milliseconds()
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, usage.Cached, gatewayUsageCost(usage, candidate), time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			writeProxyResponse(w, status, header, respBody, candidate, normalized.ResponseMode)
			return candOutcome{kind: candSuccess, usage: usage}
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
	if pool, fixed := oauthPoolForCandidate(key); fixed && oauthPoolLeaseFromContext(ctx) == nil {
		return s.streamProxyOAuthPoolCandidate(ctx, request, key, w, pool)
	}
	var err error
	firstEventBudget := s.streamFirstEventBudget()
	requestCtx, firstOutput := newFirstOutputGuard(ctx, firstEventBudget)
	defer firstOutput.Close()
	firstOutputStartedAt := time.Now()
	var resp *http.Response
	var resolvedProvider oauthpool.ProviderKind
	if attempt := oauthPoolLeaseFromContext(ctx); attempt != nil && attempt.lease != nil && attempt.service != nil {
		resolved, resolveErr := attempt.lease.ResolveRequest(oauthpool.RequestInput{
			Method: request.Method, Path: request.Path, Header: request.Header, Body: request.Body, Stream: request.Stream,
		})
		if resolveErr != nil {
			return true, usageTokens{}, resolveErr
		}
		resolvedProvider = resolved.Provider
		resp, err = attempt.service.Do(requestCtx, attempt.lease.Pool, resolved)
	} else {
		ch, findErr := s.channels.FindByID(key.ChannelID)
		if findErr != nil {
			return true, usageTokens{}, findErr
		}
		upstreamKey, decryptErr := s.decryptUpstreamAPIKey(key)
		if decryptErr != nil {
			return true, usageTokens{}, decryptErr
		}
		upstreamURL, joinErr := joinUpstreamURL(ch.SiteURL, request.Path)
		if joinErr != nil {
			return true, usageTokens{}, joinErr
		}
		req, requestErr := http.NewRequestWithContext(requestCtx, request.Method, upstreamURL, bytes.NewReader(request.Body))
		if requestErr != nil {
			return true, usageTokens{}, requestErr
		}
		copyRequestHeaders(req.Header, request.Header)
		s.applyUpstreamAuthHeaders(req.Header, key, upstreamKey)
		if request.Stream {
			req.Header.Set("Accept", "text/event-stream")
			req.Header.Set("Cache-Control", "no-cache")
			req.Header.Set("Accept-Encoding", "identity")
		}
		client, clientErr := s.httpClientFor(ctx, ch)
		if clientErr != nil {
			return true, usageTokens{}, clientErr
		}
		resp, err = client.Do(req)
	}
	if err != nil {
		if firstOutput.TimedOut() {
			return true, usageTokens{}, firstOutput.timeoutError()
		}
		return true, usageTokens{}, err
	}
	defer resp.Body.Close()

	header := cloneHeader(resp.Header)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if resolvedProvider == oauthpool.ProviderGrokWeb {
			return s.streamGrokWebOAuthResponse(resp.Body, request, key, w, firstOutput)
		}
		if !isEventStream(header) {
			respBody, readErr := readLimitedBody(resp.Body, 64<<20)
			if readErr != nil {
				if firstOutput.TimedOut() {
					return true, usageTokens{}, firstOutput.timeoutError()
				}
				return true, usageTokens{}, readErr
			}
			if s.interceptedResponseContent(key, string(respBody)) != "" {
				return true, usageTokens{}, errors.New("response content intercepted")
			}
			if isUpstreamErrorBody(respBody) {
				return true, usageTokens{}, fmt.Errorf("upstream returned error payload: %s", truncateBody(respBody, 240))
			}
			if !firstOutput.MarkReady() {
				return true, usageTokens{}, firstOutput.timeoutError()
			}
			if request.Stream && (request.ResponseMode == "responses" || request.ResponseMode == "responses_from_chat") {
				usage, err := streamNonSSEAsResponsesEvents(w, resp.StatusCode, header, respBody, key, request.ResponseMode, request.ToolKinds)
				return false, usage, err
			}
			if request.Stream && (request.ResponseMode == "chat" || request.ResponseMode == "raw" && isChatCompletionPath(request.Path)) {
				usage, err := streamNonSSEAsChatEvents(w, resp.StatusCode, header, respBody, key)
				return false, usage, err
			}
			writeProxyResponse(w, resp.StatusCode, header, respBody, key, request.ResponseMode)
			return false, extractUsage(respBody), nil
		}
		reader := newSSEStreamReader(resp.Body)
		// Preflight must not write anything downstream.  If we emit even a
		// heartbeat before proving this upstream can produce a valid event, the
		// request is pinned to this candidate and we can no longer fail over to a
		// healthier charity/low-ratio key without corrupting the Codex stream.
		preflightBudget := streamCandidatePreflightBudget(firstEventBudget, time.Since(firstOutputStartedAt))
		if preflightBudget <= 0 {
			return true, usageTokens{}, firstOutput.timeoutError()
		}
		scanEvents := s.streamInterceptionScanEvents()
		buffered, err := preflightSSEStream(reader, resp.Body, preflightBudget, func(events []sseEvent) bool { return s.shouldHoldInterceptionPreflight(key, events, scanEvents) })
		if err != nil {
			if firstOutput.TimedOut() {
				return true, usageTokens{}, firstOutput.timeoutError()
			}
			if s.interceptedResponseContent(key, err.Error()) != "" {
				return true, usageTokens{}, errors.New("response content intercepted")
			}
			return true, usageTokens{}, err
		}
		if !firstOutput.MarkReady() {
			return true, usageTokens{}, firstOutput.timeoutError()
		}
		// 首字节前对已缓冲事件做整体拦截扫描：把 buffered 里所有可见文本拼起来一起判定，
		// 这样分散在多个 delta 的拦截词（"公益"+"token"+"休息了"）也能在提交前命中。命中即
		// 整体切到下一个候选（retry=true，连命中前那段都不透传）。扫描窗口
		// streamInterceptionScanEvents>0 时，preflight 已多缓冲了这些事件，命中率更高。
		var preflightText strings.Builder
		for _, event := range buffered {
			preflightText.WriteString(streamEventInterceptableText(event))
		}
		if s.interceptedResponseContent(key, preflightText.String()) != "" {
			return true, usageTokens{}, errors.New("response content intercepted")
		}
		// 正式转发阶段的 idle 读超时：上游连续 streamIdleTimeout 没有任何新事件就判定卡死，
		// 主动关连接返回错误，避免 reader.Next() 无限阻塞导致客户端 stream closed。
		reader.sanitizeFailure = func(message string) (string, bool) {
			if s.interceptedResponseContent(key, message) != "" {
				return "upstream stream interrupted", true
			}
			return message, false
		}
		// interceptContent 在正式转发阶段对逐段累积的可见文本做拦截判定。preflight 只
		// 能在首字节前检查 buffered 事件，一旦上游先吐正常文本、随后再插入"公益 token
		// 休息了"之类的错误话术（grok 尤其如此），preflight 已放行、无法再切候选。这里
		// 在转发循环里持续累积可见文本并复用同一套 needles，命中即中断本次流并让调用方
		// 把该渠道打入冷却，避免把错误话术继续透传给客户端。
		reader.interceptContent = func(text string) bool {
			return s.interceptedResponseContent(key, text) != ""
		}
		reader.closer = resp.Body
		reader.idleTimeout = streamIdleTimeout
		reader.heartbeatInterval = streamHeartbeatInterval
		reader.heartbeat = func() error {
			return writeSSEHeartbeat(w)
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
			usage, err := streamChatAsResponsesEvents(w, buffered, reader, request.ToolKinds)
			return false, usage, err
		}
		if request.ResponseMode == "responses" && bufferedSSELooksLikeChatCompletion(buffered) {
			usage, err := streamChatAsResponsesEvents(w, buffered, reader, request.ToolKinds)
			return false, usage, err
		}
		usage, err := streamRawSSE(w, buffered, reader, request.ResponseMode)
		return false, usage, err
	}
	respBody, readErr := readLimitedBody(resp.Body, 64<<20)
	if readErr != nil {
		if firstOutput.TimedOut() {
			return true, usageTokens{}, firstOutput.timeoutError()
		}
		return true, usageTokens{}, readErr
	}
	errText := truncateBody(respBody, 240)
	if s.interceptedResponseContent(key, string(respBody)) != "" {
		return true, usageTokens{}, errors.New("response content intercepted")
	}
	if shouldRetryUpstreamStatus(resp.StatusCode, errText) {
		return true, usageTokens{}, errors.New(upstreamHTTPErrorMessage(resp.StatusCode, header, respBody))
	}
	return false, usageTokens{}, &GatewayError{Status: resp.StatusCode, Header: header, Body: respBody}
}

func normalizeResponseInterceptionText(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, value)
}

func (s *Service) responseInterceptionNeedles(key *storage.UpstreamGroupKey) []string {
	// These provider-side messages mean the free/public allocation is resting,
	// not that the user's request is invalid. Handle their common spellings even
	// without an administrator rule so another healthy route can be tried before
	// any stream bytes are sent to Codex.
	needles := []string{
		"请求暂时无法完成",
		"公益token",
		"公益 token",
		"公益token休息了",
		"公益 token 休息了",
		"gpt休息了",
		"gpt 休息了",
		// 上游（尤其是另一个网关/中转）把"自己没有可用渠道"的软失败当作正文或
		// 错误 body 返回时的特征串。命中即在写首字节前切换到下一个候选，避免把
		// 一句"当前没有可用上游，请稍后重试"直接透传给 Codex 造成断流。
		// 这些串都足够有特征，不会误伤正常生成内容（不加宽泛的"请稍后重试"单词）。
		"stream disconnected before completion",
		"当前没有可用上游",
		"auth_unavailable",
		"no auth available",
	}
	for _, rule := range s.appConfig().ResponseInterceptionRules {
		needle := strings.TrimSpace(rule.Content)
		if !rule.Enabled || needle == "" {
			continue
		}
		if rule.ChannelID != 0 && (key == nil || rule.ChannelID != key.ChannelID) {
			continue
		}
		needles = append(needles, needle)
	}
	return needles
}

func (s *Service) interceptedResponseContent(key *storage.UpstreamGroupKey, content string) string {
	if strings.TrimSpace(content) == "" {
		return ""
	}
	lower := strings.ToLower(content)
	compact := normalizeResponseInterceptionText(content)
	for _, needle := range s.responseInterceptionNeedles(key) {
		needle = strings.TrimSpace(needle)
		if needle == "" {
			continue
		}
		if strings.Contains(lower, strings.ToLower(needle)) || strings.Contains(compact, normalizeResponseInterceptionText(needle)) {
			return needle
		}
	}
	return ""
}

// shouldHoldInterceptionPreflight keeps a stream uncommitted while a visible
// output prefix could still become an interception rule. This catches errors
// split across several SSE deltas (for example "公益" + "token" + "休息了")
// before the first byte is written, so the scheduler may safely choose another
// compatible upstream instead of leaving Codex with a disconnected stream.
//
// scanEvents>0 开启"固定扫描窗口"：即使当前文本已不是任何 needle 的前缀（上游先吐了
// 正常文本），只要缓冲的可见事件数还没到窗口上限，就继续持有流多缓冲几个事件再判定。
// 这样"正常文本 + 随后插入的拦截话术"也能在首字节前整体命中并无缝切换候选（连命中前
// 那段都不透传），代价是首字节延迟增加。scanEvents<=0 时退回历史"仅前缀持有"行为。
func (s *Service) shouldHoldInterceptionPreflight(key *storage.UpstreamGroupKey, events []sseEvent, scanEvents int) bool {
	var text strings.Builder
	for _, event := range events {
		text.WriteString(streamEventInterceptableText(event))
	}
	value := normalizeResponseInterceptionText(text.String())
	if value == "" || s.interceptedResponseContent(key, text.String()) != "" {
		return false
	}
	if streamBufferedHasTerminal(events) {
		return false
	}
	// 扫描窗口未用满前先持有：无论当前是否为 needle 前缀，都多缓冲一个可见事件继续扫描，
	// 直到攒够 scanEvents 个带可见文本的事件才落地首字节。
	if scanEvents > 0 && countVisibleInterceptableEvents(events) < scanEvents {
		return true
	}
	for _, needle := range s.responseInterceptionNeedles(key) {
		needle = normalizeResponseInterceptionText(needle)
		if needle == "" {
			continue
		}
		if len(value) >= 2 && strings.HasPrefix(needle, value) {
			return true
		}
		for offset := 1; offset < len(value); offset++ {
			suffix := value[offset:]
			// A one-byte suffix (for example the final "g" in "pong") is
			// too ambiguous to reserve a stream just because a rule starts with
			// that byte. Keep single Chinese-character prefixes intact while
			// waiting for at least two bytes of ASCII text such as "gp".
			if len(suffix) < 2 {
				continue
			}
			if strings.HasPrefix(needle, suffix) {
				return true
			}
		}
	}
	return false
}

// countVisibleInterceptableEvents 统计已缓冲事件里"带可见可拦截文本"的事件数，
// 供扫描窗口判定用。只数真正含可见文本的事件，纯生命周期噪声（response.created
// 之类）不占窗口额度，避免窗口被无内容事件提前耗尽。
func countVisibleInterceptableEvents(events []sseEvent) int {
	n := 0
	for _, ev := range events {
		if strings.TrimSpace(streamEventInterceptableText(ev)) != "" {
			n++
		}
	}
	return n
}

func streamEventInterceptableText(ev sseEvent) string {
	data := strings.TrimSpace(ev.Data)
	if data == "" || data == "[DONE]" {
		return ""
	}
	var text strings.Builder
	if message := errorMessageFromJSON([]byte(data)); message != "" {
		text.WriteString(message)
	}
	if delta := responseDeltaText(data); delta != "" {
		text.WriteString(delta)
	}
	var raw map[string]any
	if json.Unmarshal([]byte(data), &raw) == nil {
		text.WriteString(chatChunkDeltaText(raw))
		text.WriteString(responseVisibleText(raw))
		if response, _ := raw["response"].(map[string]any); response != nil {
			text.WriteString(responseVisibleText(response))
		}
	}
	return text.String()
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
		Name:            fmt.Sprintf("%s - %s - %s", firstNonEmpty(strings.TrimSpace(s.appConfig().Title), "AI Gateway"), ch.Name, group.Name),
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

// autoSyncEnabledDefaultMaxRatio 是"覆盖同步"新建自动分组时默认启用的倍率上限。
// 超过该倍率的分组默认置为 Enabled=false（不参与调度也不测活），避免同步一批渠道后
// 高倍率分组被自动拉进调度池烧钱；运维确认后可在渠道页手动开启。只影响新建：已存在
// 分组的 enabled 不在 upsertColumns 内，覆盖同步不会翻动用户手动开关。
const autoSyncEnabledDefaultMaxRatio = 0.1

func upstreamGroupKeyFrom(ch storage.Channel, group connector.APIKeyGroup, groupRef, keyCipher string) *storage.UpstreamGroupKey {
	if groupRef == "" {
		groupRef, _ = groupRefFor(ch.Type, group)
	}
	format := inferGroupClientFormat(group.Name, group.Description)
	ratio := normalizedRatio(group.Ratio)
	// 倍率高于阈值的分组默认关闭：ratio<=0 视为"未知倍率"，同样保守关闭，等待运维确认。
	enabled := ratio > 0 && ratio <= autoSyncEnabledDefaultMaxRatio
	return &storage.UpstreamGroupKey{
		ChannelID:             ch.ID,
		ChannelName:           ch.Name,
		ChannelURL:            ch.SiteURL,
		ChannelType:           ch.Type,
		ClientFormat:          format,
		RequestMode:           "responses",
		RequestModeSource:     "auto",
		AuthMode:              defaultAuthModeForClientFormat(format),
		GroupRef:              groupRef,
		GroupName:             strings.TrimSpace(group.Name),
		GroupDesc:             strings.TrimSpace(group.Description),
		Ratio:                 ratio,
		RatioScalePercent:     100,
		InputPricePerMillion:  storage.DefaultInputPricePerMillion,
		OutputPricePerMillion: storage.DefaultOutputPricePerMillion,
		Enabled:               enabled,
		ConcurrencyLimit:      0,
		KeyCipher:             keyCipher,
		Status:                "alive",
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

type healthProbeAttempt struct {
	status    int
	body      []byte
	latencyMS int64
	model     string
	err       error
}

func (attempt healthProbeAttempt) succeeded() bool {
	return attempt.err == nil && attempt.status >= http.StatusOK && attempt.status < http.StatusMultipleChoices
}

func (s *Service) testGroupKey(ctx context.Context, key *storage.UpstreamGroupKey) HealthResultItem {
	if key == nil {
		return HealthResultItem{Status: "disabled"}
	}
	release := s.acquireHealthProbeUpstreamSlot(*key)
	defer release()
	return s.testGroupKeyWithUpstreamSlot(ctx, key)
}

func (s *Service) testGroupKeyWithUpstreamSlot(ctx context.Context, key *storage.UpstreamGroupKey) HealthResultItem {
	item := HealthResultItem{
		ID:          key.ID,
		ChannelID:   key.ChannelID,
		ChannelName: key.ChannelName,
		GroupRef:    key.GroupRef,
		GroupName:   key.GroupName,
		Ratio:       effectiveGroupRatio(*key),
	}
	if !key.Enabled {
		item.Status = "disabled"
		return item
	}

	attempts, retriesCompleted := s.healthProbeAttempts(ctx, key)
	if len(attempts) == 0 {
		attempts = append(attempts, healthProbeAttempt{err: errors.New("health probe did not run")})
	}
	last := attempts[len(attempts)-1]
	// A manual key can be valid while a relay rejects the first probe because
	// that particular key uses the other common authentication header. Before a
	// 401/403 is persisted as an unusable channel, re-detect on the same key and
	// rerun the probe. This mirrors the live request fallback and prevents the
	// add-channel dialog from poisoning a working manual key.
	if !last.succeeded() && isManualGroupKey(key) {
		initialStatus := healthFailureStatus(last.status, last.body, last.err)
		if initialStatus == "auth_failed" || initialStatus == "forbidden" {
			var detected *storage.UpstreamGroupKey
			var detectErr error
			if strings.EqualFold(strings.TrimSpace(key.RequestModeSource), "manual") {
				detected, detectErr = s.DetectGroupAuthMode(ctx, key.ID)
			} else {
				detected, detectErr = s.DetectGroupRequestMode(ctx, key.ID)
			}
			if detectErr == nil && detected != nil {
				key = detected
				attempts, retriesCompleted = s.healthProbeAttempts(ctx, key)
				if len(attempts) > 0 {
					last = attempts[len(attempts)-1]
				}
			}
		}
	}
	item.LatencyMS = last.latencyMS
	now := time.Now()
	item.CheckedAt = &now
	if last.succeeded() {
		item.Status = "alive"
		// 测活成功必须同时清掉内存里的 runtime disable：MarkHealthSuccessWithModel 只清 DB 的
		// disabled_until，而调度时会同时检查 DB 冷却和 runtimeDisabledUntil（并发降级 / 代理失败
		// 冷却都记在内存）。漏清会造成"测活显示活、DB 冷却已解，但渠道仍被内存态冻住不可调度"，
		// 便宜渠道被压在 fallback 层、付费渠道反而被选中，全被冻时直接显示"无可用上游"。
		s.clearRuntimeDisable(key.ID)
		_ = s.groupKeys.MarkHealthSuccessWithModel(key.ID, item.LatencyMS, last.model)
		if last.model != "" {
			s.rememberCandidateModelCapability(key.ID, last.model, true)
			if healthProbeModelIsOneOf(last.model, openAIHealthProbeFallbackModel) {
				s.rememberCandidateModelCapability(key.ID, openAIHealthProbePrimaryModel, false)
			} else if !healthProbeModelIsOneOf(last.model, openAIHealthProbePrimaryModel, openAIHealthProbeFallbackModel) {
				s.rememberCandidateModelCapability(key.ID, openAIHealthProbePrimaryModel, false)
				s.rememberCandidateModelCapability(key.ID, openAIHealthProbeFallbackModel, false)
			}
		}
		return item
	}

	failureStatus := healthFailureStatus(last.status, last.body, last.err)
	lastErr := healthProbeError(last.status, last.body, last.err)
	item.ErrorType = failureStatus
	item.Error = healthProbeAttemptsError(attempts, retriesCompleted, lastErr)

	if failureStatus == "rate_limited" {
		// A one-click probe must not consume the last slot in an upstream rate
		// window and then label a generally usable channel as limited. Preserve
		// the diagnostic in last_error, but leave routing to real traffic, where
		// an actual 429 still applies its normal per-key cooldown.
		item.Status = "alive"
		s.markHealthInconclusive(key.ID, item.Error, item.LatencyMS)
		return item
	}
	if shouldSkipHealthRetry(failureStatus) {
		item.Status = failureStatus
		s.markHealthFailureWithStatus(key.ID, item.Status, item.Error, item.LatencyMS)
		return item
	}

	item.Status = failureStatus
	s.markHealthFailureWithStatus(key.ID, item.Status, item.Error, item.LatencyMS)
	return item
}

// healthProbeAttempts runs one full generation probe. The OpenAI probe already
// performs the required gpt-5.4 -> gpt-5.5 fallback inside that attempt; replaying
// the entire sequence for a transient fault doubled latency and quota use while
// adding little signal. Transient results remain inconclusive and real traffic
// owns the retry/cooldown decision.
func (s *Service) healthProbeAttempts(ctx context.Context, key *storage.UpstreamGroupKey) ([]healthProbeAttempt, bool) {
	attempts := make([]healthProbeAttempt, 0, healthTransientAttempts)
	for retry := 0; retry < healthTransientAttempts; retry++ {
		if retry > 0 {
			if !waitHealthRetry(ctx, healthRetryDelayForAttempt(key.ID, retry)) {
				return attempts, false
			}
		}
		status, body, latencyMS, model, err := s.healthProbeCandidateWithModel(ctx, key)
		attempt := healthProbeAttempt{status: status, body: body, latencyMS: latencyMS, model: model, err: err}
		attempts = append(attempts, attempt)
		if attempt.succeeded() || !shouldRetryTransientHealthStatus(healthFailureStatus(status, body, err)) {
			return attempts, true
		}
	}
	return attempts, true
}

func shouldRetryTransientHealthStatus(status string) bool {
	switch status {
	case "dead", "server_error", "timeout", "network_error", "upstream_error":
		return true
	default:
		return false
	}
}

func healthProbeAttemptsError(attempts []healthProbeAttempt, retriesCompleted bool, lastErr error) string {
	if lastErr == nil {
		lastErr = errors.New("health probe failed")
	}
	if len(attempts) <= 1 {
		if !retriesCompleted {
			return fmt.Sprintf("probe retry canceled: %v", lastErr)
		}
		return lastErr.Error()
	}
	if !retriesCompleted {
		return fmt.Sprintf("probe retry canceled after %d attempts: %v", len(attempts), lastErr)
	}
	return fmt.Sprintf("probe failed after %d attempts: %v", len(attempts), lastErr)
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
		// Persist 401 as a distinct authentication failure so its own upstream
		// key can be disabled. The frontend intentionally renders this in the
		// same 403-access-refused bucket, without poisoning sibling keys.
		return "auth_failed"
	case looksLikeUpstreamRoutingUnavailable(text):
		return "upstream_error"
	case looksLikeUnsupportedModelError(text):
		return "model_error"
	case looksLikeClientRequestError(text) || status == http.StatusUnprocessableEntity:
		return "invalid_request"
	case looksLikeNonGenerationFailure(text):
		return "non_generation"
	case looksLikeTimeoutFailure(err, text):
		return "timeout"
	case looksLikeUpstreamErrorFailure(text):
		return "upstream_error"
	case looksLikeNetworkFailure(status, err, text):
		return "network_error"
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
	status, body, latencyMS, _, err := s.healthProbeCandidateWithModel(ctx, key)
	return status, body, latencyMS, err
}

func (s *Service) healthProbeCandidateWithModel(ctx context.Context, key *storage.UpstreamGroupKey) (int, []byte, int64, string, error) {
	// Claude 类型渠道：走 Anthropic Messages 格式探测，绝不用 openai 的 /v1/models + /v1/responses，
	// 否则 claude 上游不认这些端点，测活必然失败（这正是"claude 渠道一测就死"的原因）。
	if normalizeClientFormat(key.ClientFormat) == "claude" {
		status, body, latencyMS, err := s.healthProbeClaude(ctx, key)
		return status, body, latencyMS, defaultHealthProbeModel(key.ClientFormat), err
	}
	if normalizeClientFormat(key.ClientFormat) == "grok" {
		status, body, latencyMS, err := s.healthProbeGrok(ctx, key)
		return status, body, latencyMS, defaultHealthProbeModel(key.ClientFormat), err
	}
	start := time.Now()
	// The one-click check is intentionally OpenAI-only. Use a configured, stable
	// model list instead of /v1/models discovery: model lists are often filtered
	// or stale, which used to make a healthy channel look dead before the real
	// probe ran. The list is configurable (system settings) so new upstream models
	// can be adopted without a code change; it is tried in order until one works.
	probeModels := s.healthProbeModels()
	var (
		status    int
		body      []byte
		usedModel string
		err       error
	)
	for i, model := range probeModels {
		// allowCompatibleModelRetry 仅对清单首个模型开启，与历史行为一致：首个模型
		// 命中"协议兼容重试"时记住能力后立即返回，避免逐个模型重复探测浪费额度。
		status, body, usedModel, err = s.healthProbeOpenAIModel(ctx, key, model, i == 0)
		if !shouldTryHealthFallbackModel(status, body, err) {
			break
		}
	}
	if shouldFallbackHealthModelDiscovery(status, body, err) {
		if model, _, _, discoverErr := s.discoverHealthProbeModel(ctx, key); discoverErr == nil &&
			!healthProbeModelIsOneOf(model, probeModels...) {
			status, body, usedModel, err = s.healthProbeOpenAIModel(ctx, key, model, false)
		}
	}
	latencyMS := time.Since(start).Milliseconds()
	if err != nil {
		return status, body, latencyMS, usedModel, err
	}
	if status >= 200 && status < 300 && isUpstreamErrorBody(body) {
		return status, body, latencyMS, usedModel, fmt.Errorf("upstream returned error payload: %s", truncateBody(body, 240))
	}
	if status >= 200 && status < 300 && !looksLikeHealthGenerationSuccess(body) {
		return status, body, latencyMS, usedModel, fmt.Errorf("upstream returned non-generation payload: %s", truncateBody(body, 240))
	}
	return status, body, latencyMS, usedModel, nil
}

func shouldTryHealthFallbackModel(status int, body []byte, err error) bool {
	if healthProbeSucceeded(status, body, err) {
		return false
	}
	return true
}

func (s *Service) healthProbeOpenAIModel(ctx context.Context, key *storage.UpstreamGroupKey, model string, allowCompatibleModelRetry bool) (int, []byte, string, error) {
	req := healthGenerationProbeRequest(model)
	req = requestForCandidate(req, key)
	status, _, body, err := s.requestHealthProbeCandidate(ctx, req, key, s.healthProbeBudget())
	if allowCompatibleModelRetry && shouldRetryHealthWithCompatibleModel(body, err) {
		return status, body, model, err
	}
	if !strings.EqualFold(strings.TrimSpace(key.RequestModeSource), "manual") {
		if fallback, _, ok := healthProbeFallbackRequest(req, status, body, err); ok {
			originalStatus, originalBody, originalErr := status, body, err
			fallbackStatus, _, fallbackBody, fallbackErr := s.requestHealthProbeCandidate(ctx, fallback, key, s.healthProbeBudget())
			if fallbackErr == nil && healthProbeSucceeded(fallbackStatus, fallbackBody, nil) {
				// The health request is a real streamed generation probe. If its
				// alternate protocol succeeds, remember that capability immediately
				// so the next Codex request does not need to rediscover it.
				mode := "responses"
				if fallback.ResponseMode == "responses_from_chat" {
					mode = "chat"
				}
				if normalizeUpstreamRequestMode(key.RequestMode) != mode &&
					!strings.EqualFold(strings.TrimSpace(key.RequestModeSource), "manual") {
					_ = s.groupKeys.UpdateRequestMode(key.ID, mode)
				}
				return fallbackStatus, fallbackBody, model, nil
			}
			// Keep the original result when the alternate protocol also fails. That
			// preserves meaningful statuses such as rate limit or non-generation
			// instead of replacing them with a misleading 404 from the fallback URL.
			return originalStatus, originalBody, model, originalErr
		}
	}
	return status, body, model, err
}

func shouldRetryHealthWithCompatibleModel(body []byte, err error) bool {
	text := healthFailureText(body, err)
	if looksLikeUnsupportedModelError(text) {
		return true
	}
	if looksLikeUpstreamRoutingUnavailable(text) {
		return true
	}
	return false
}

// healthProbeGrok uses xAI's OpenAI-compatible Chat Completions contract.
// Some Grok relays deliberately expose no Responses endpoint, so using the
// OpenAI /responses probe would turn a healthy Grok group into a false death.
func (s *Service) healthProbeGrok(ctx context.Context, key *storage.UpstreamGroupKey) (int, []byte, int64, error) {
	start := time.Now()
	body, _ := json.Marshal(map[string]any{
		"model":      "grok-4.5",
		"messages":   []map[string]string{{"role": "user", "content": healthProbePrompt}},
		"max_tokens": healthProbeMaxOutputTokens,
		"stream":     true,
	})
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	req := normalizedRequest{Method: http.MethodPost, Path: "/v1/chat/completions", Header: header, Body: body, ResponseMode: "raw", Stream: true}
	status, _, respBody, err := s.requestHealthProbeCandidate(ctx, req, key, s.healthProbeBudget())
	latencyMS := time.Since(start).Milliseconds()
	if err != nil {
		return status, respBody, latencyMS, err
	}
	if status < 200 || status >= 300 {
		return status, respBody, latencyMS, healthProbeError(status, respBody, nil)
	}
	if isUpstreamErrorBody(respBody) || !looksLikeHealthGenerationSuccess(respBody) {
		return status, respBody, latencyMS, fmt.Errorf("upstream returned non-generation payload: %s", truncateBody(respBody, 240))
	}
	return status, respBody, latencyMS, nil
}

func healthProbeFallbackRequest(request normalizedRequest, status int, body []byte, err error) (normalizedRequest, string, bool) {
	if !request.hasAlt() {
		return request, "", false
	}
	// For a health probe, a native Responses endpoint that is missing, accepts
	// the request but emits no generation, or rejects this representation can
	// safely try the Chat-compatible probe. This is intentionally narrower than
	// real request forwarding: a live Codex Responses request keeps its native
	// payload and never silently loses reasoning/tool fields.
	if request.ResponseMode == "responses" && shouldTryHealthChatProbe(status, body, err) {
		return request.alt(), "health chat-completions compatibility", true
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

func shouldTryHealthChatProbe(status int, body []byte, err error) bool {
	if err == nil && status >= http.StatusOK && status < http.StatusMultipleChoices && looksLikeHealthGenerationSuccess(body) {
		return false
	}
	text := healthFailureText(body, err)
	if isUpstreamErrorBody(body) &&
		!looksLikeEndpointMissingError(text) &&
		!looksLikeUnsupportedModelError(text) &&
		!looksLikeClientRequestError(text) {
		return false
	}
	classification := healthFailureStatus(status, body, err)
	switch classification {
	case "zero_balance", "rate_limited", "forbidden", "auth_failed", "timeout", "network_error", "upstream_error", "server_error":
		return false
	case "model_error", "invalid_request", "non_generation", "dead":
		return true
	}
	return err == nil && status >= http.StatusOK && status < http.StatusMultipleChoices && !looksLikeHealthGenerationSuccess(body)
}

// healthProbeClaude 用 Anthropic Messages 格式测活 claude 类型渠道。
// 直接打 /v1/messages，不做 /v1/models 发现（claude 中转站常不提供或格式不同）。
func (s *Service) healthProbeClaude(ctx context.Context, key *storage.UpstreamGroupKey) (int, []byte, int64, error) {
	start := time.Now()
	model := defaultHealthProbeModel(key.ClientFormat)
	body, _ := json.Marshal(map[string]any{
		"model":      model,
		"max_tokens": healthProbeMaxOutputTokens,
		"messages": []map[string]any{
			{"role": "user", "content": healthProbePrompt},
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
	status, _, respBody, err := s.requestHealthProbeCandidate(ctx, req, key, s.healthProbeBudget())
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
	status, _, body, err := s.requestCandidate(ctx, req, key, s.healthProbeBudget())
	if err != nil {
		return "", status, body, fmt.Errorf("model discovery failed: %w", err)
	}
	if status < 200 || status >= 300 {
		return "", status, body, fmt.Errorf("model discovery failed: %w", healthProbeError(status, body, nil))
	}
	if isUpstreamErrorBody(body) {
		return "", status, body, fmt.Errorf("model discovery returned error payload: %s", truncateBody(body, 240))
	}
	models := extractHealthProbeModels(body)
	s.rememberDiscoveredModelCapabilities(key, models)
	return selectHealthProbeModel(models, key.ClientFormat), status, body, nil
}

func (s *Service) rememberDiscoveredModelCapabilities(key *storage.UpstreamGroupKey, models []string) {
	if s == nil || key == nil || key.ID == 0 {
		return
	}
	remembered := 0
	for _, model := range models {
		model = normalizeModelCapabilityKey(model)
		if model == "" {
			continue
		}
		s.rememberCandidateModelCapability(key.ID, model, true)
		remembered++
		if remembered >= 128 {
			return
		}
	}
}

type GroupModelPolicy struct {
	AvailableModels    []string `json:"available_models"`
	SupportedModels    []string `json:"supported_models"`
	RestrictionEnabled bool     `json:"restriction_enabled"`
}

// GroupKeyModels returns the discovered catalog and the explicit allowlist.
// Legacy rows stay unrestricted until an operator synchronizes or saves them.
func (s *Service) GroupKeyModels(id uint) (GroupModelPolicy, error) {
	if s == nil || s.groupKeys == nil {
		return GroupModelPolicy{}, errors.New("gateway group key store is unavailable")
	}
	key, err := s.groupKeys.FindByID(id)
	if err != nil {
		return GroupModelPolicy{}, err
	}
	if key == nil {
		return GroupModelPolicy{}, errors.New("upstream group key not found")
	}
	return groupModelPolicyFromKey(key), nil
}

// SyncGroupKeyModels discovers the upstream catalog without silently enabling
// newly advertised models. The first sync starts with all discovered models
// selected; later syncs preserve only selections that still exist.
func (s *Service) SyncGroupKeyModels(ctx context.Context, id uint) (GroupModelPolicy, error) {
	if s == nil || s.groupKeys == nil {
		return GroupModelPolicy{}, errors.New("gateway group key store is unavailable")
	}
	key, err := s.groupKeys.FindByID(id)
	if err != nil {
		return GroupModelPolicy{}, err
	}
	if key == nil {
		return GroupModelPolicy{}, errors.New("upstream group key not found")
	}
	models := append([]string(nil), defaultGrokPoolModelCatalog...)
	if key.ChannelType != storage.ChannelTypeGrokPool {
		modelPath := healthPath
		if key.ChannelType == storage.ChannelTypeChatGPTPool {
			modelPath += "?client_version=" + url.QueryEscape(config.DefaultCodexVersion)
		}
		req := normalizedRequest{Method: http.MethodGet, Path: modelPath, Header: http.Header{}}
		status, _, body, requestErr := s.requestCandidate(ctx, req, key, s.healthProbeBudget())
		if requestErr != nil {
			return GroupModelPolicy{}, fmt.Errorf("fetch upstream models failed: %w", requestErr)
		}
		if status < 200 || status >= 300 {
			return GroupModelPolicy{}, fmt.Errorf("fetch upstream models failed: %w", healthProbeError(status, body, nil))
		}
		if isUpstreamErrorBody(body) {
			return GroupModelPolicy{}, fmt.Errorf("fetch upstream models returned error payload: %s", truncateBody(body, 240))
		}
		models = uniqueStrings(extractHealthProbeModels(body))
	}
	if len(models) == 0 {
		return GroupModelPolicy{}, errors.New("upstream returned no models")
	}
	sort.Strings(models)
	previous := groupModelPolicyFromKey(key)
	selected := append([]string(nil), models...)
	if previous.RestrictionEnabled {
		selected = intersectModelNames(previous.SupportedModels, models)
	}
	policy := GroupModelPolicy{AvailableModels: models, SupportedModels: selected, RestrictionEnabled: true}
	if err := s.persistGroupKeyModelPolicy(id, policy); err != nil {
		return GroupModelPolicy{}, err
	}
	return policy, nil
}

// SetGroupKeyModels saves a strict allowlist. An empty selection intentionally
// denies every model for this route; it no longer means "unrestricted".
func (s *Service) SetGroupKeyModels(id uint, models []string) (GroupModelPolicy, error) {
	if s == nil || s.groupKeys == nil {
		return GroupModelPolicy{}, errors.New("gateway group key store is unavailable")
	}
	key, err := s.groupKeys.FindByID(id)
	if err != nil {
		return GroupModelPolicy{}, err
	}
	if key == nil {
		return GroupModelPolicy{}, errors.New("upstream group key not found")
	}
	current := groupModelPolicyFromKey(key)
	cleaned := uniqueStrings(models)
	available := current.AvailableModels
	if len(available) == 0 && len(cleaned) > 0 {
		// Backward compatible manual bootstrap for deployments that cannot expose a
		// model endpoint. Subsequent syncs still use the discovered catalog.
		available = append([]string(nil), cleaned...)
	}
	if unknown := modelNamesOutsideCatalog(cleaned, available); len(unknown) > 0 {
		return GroupModelPolicy{}, fmt.Errorf("models are not present in the discovered catalog: %s", strings.Join(unknown, ", "))
	}
	policy := GroupModelPolicy{AvailableModels: available, SupportedModels: cleaned, RestrictionEnabled: true}
	if err := s.persistGroupKeyModelPolicy(id, policy); err != nil {
		return GroupModelPolicy{}, err
	}
	return policy, nil
}

func (s *Service) persistGroupKeyModelPolicy(id uint, policy GroupModelPolicy) error {
	availableJSON, err := json.Marshal(policy.AvailableModels)
	if err != nil {
		return fmt.Errorf("encode available models: %w", err)
	}
	supportedJSON, err := json.Marshal(policy.SupportedModels)
	if err != nil {
		return fmt.Errorf("encode supported models: %w", err)
	}
	if err := s.groupKeys.UpdateModelPolicy(id, string(availableJSON), string(supportedJSON), policy.RestrictionEnabled); err != nil {
		return err
	}
	s.InvalidateSchedulingCache()
	return nil
}

func groupModelPolicyFromKey(key *storage.UpstreamGroupKey) GroupModelPolicy {
	if key == nil {
		return GroupModelPolicy{}
	}
	return GroupModelPolicy{
		AvailableModels:    decodeModelNames(key.AvailableModels),
		SupportedModels:    decodeModelNames(key.SupportedModels),
		RestrictionEnabled: key.ModelRestrictionEnabled,
	}
}

func decodeModelNames(raw string) []string {
	var models []string
	if strings.TrimSpace(raw) == "" || json.Unmarshal([]byte(raw), &models) != nil {
		return nil
	}
	return uniqueStrings(models)
}

func intersectModelNames(selected, available []string) []string {
	allowed := make(map[string]struct{}, len(available))
	for _, model := range available {
		allowed[normalizeModelCapabilityKey(model)] = struct{}{}
	}
	out := make([]string, 0, len(selected))
	for _, model := range selected {
		if _, ok := allowed[normalizeModelCapabilityKey(model)]; ok {
			out = append(out, model)
		}
	}
	return uniqueStrings(out)
}

func modelNamesOutsideCatalog(selected, available []string) []string {
	if len(selected) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(available))
	for _, model := range available {
		allowed[normalizeModelCapabilityKey(model)] = struct{}{}
	}
	var out []string
	for _, model := range selected {
		if _, ok := allowed[normalizeModelCapabilityKey(model)]; !ok {
			out = append(out, model)
		}
	}
	return out
}

func shouldFallbackHealthModelDiscovery(status int, body []byte, err error) bool {
	// Model discovery is deliberately only a fallback for a model capability
	// miss. Do not turn real 401/403/429, balance, network, or router failures
	// into extra calls that hide the actual reason or consume more quota.
	if status == http.StatusUnauthorized || status == http.StatusForbidden ||
		status == http.StatusTooManyRequests || status == http.StatusPaymentRequired {
		return false
	}
	if status >= http.StatusInternalServerError || looksLikeTimeoutFailure(err, healthFailureText(body, err)) ||
		looksLikeNetworkFailure(status, err, healthFailureText(body, err)) {
		return false
	}
	text := healthFailureText(body, err)
	if looksLikeUnsupportedModelError(text) || looksLikeEndpointMissingError(text) || looksLikeResponsesEndpointError(text) {
		return true
	}
	// Several OpenAI-compatible relays hide an unsupported probe model behind a
	// generic 400/422 message.  One /models lookup followed by one real tiny
	// generation probe is safer than marking a working model-limited channel dead.
	return status == http.StatusBadRequest || status == http.StatusUnprocessableEntity
}

func healthProbeModelIsOneOf(model string, values ...string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(model), strings.TrimSpace(value)) {
			return true
		}
	}
	return false
}

func healthGenerationProbeRequest(model string) normalizedRequest {
	// Use the native Codex/Responses input-list shape. Several compatible
	// relays reject the shorthand string input even though real Codex requests
	// work, which previously caused a false protocol fallback to Chat.
	responsesBody, _ := json.Marshal(map[string]any{
		"model": model,
		"input": []map[string]any{{
			"role": "user",
			"content": []map[string]any{{
				"type": "input_text",
				"text": healthProbePrompt,
			}},
		}},
		"reasoning":         map[string]any{"effort": "low"},
		"max_output_tokens": healthProbeMaxOutputTokens,
		"stream":            true,
	})
	chatBody, _ := json.Marshal(map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "user", "content": healthProbePrompt},
		},
		"reasoning_effort": "low",
		"max_tokens":       healthProbeMaxOutputTokens,
		"stream":           true,
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
		for _, key := range []string{"id", "model", "name", "slug", "public_id", "model_name"} {
			if item := stringValue(v[key]); item != "" {
				*out = append(*out, item)
				return
			}
		}
		// Some providers expose models as an object keyed by model ID rather than
		// an array. Keep the key and still recurse into values for richer objects.
		for key, item := range v {
			key = strings.TrimSpace(key)
			if key != "" {
				*out = append(*out, key)
			}
			appendHealthModelValues(out, item)
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
		return "claude-opus-4-7"
	case "grok":
		return "grok-4.5"
	default:
		return openAIHealthProbePrimaryModel
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
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return false
	}
	if looksLikeHealthMathAnswer(trimmed) {
		return true
	}
	var raw map[string]any
	if err := json.Unmarshal(trimmed, &raw); err != nil {
		var text string
		if err := json.Unmarshal(trimmed, &text); err == nil && looksLikeHealthMathAnswer([]byte(text)) {
			return true
		}
		return false
	}
	if _, ok := raw["error"]; ok {
		return false
	}
	if typ := strings.ToLower(stringValue(raw["type"])); typ != "" {
		if strings.Contains(typ, "failed") || strings.Contains(typ, "error") {
			return false
		}
		if typ == "response.completed" {
			if response, ok := raw["response"].(map[string]any); ok {
				return strings.TrimSpace(responseText(response)) != ""
			}
			return strings.TrimSpace(responseText(raw)) != ""
		}
		if typ == "response.output_item.done" || typ == "response.content_part.done" {
			if strings.TrimSpace(responseText(raw)) != "" || strings.TrimSpace(stringValue(raw["text"])) != "" {
				return true
			}
			if item, ok := raw["item"].(map[string]any); ok && strings.TrimSpace(responseText(map[string]any{"output": []any{item}})) != "" {
				return true
			}
			if part, ok := raw["part"].(map[string]any); ok && strings.TrimSpace(stringValue(part["text"])) != "" {
				return true
			}
			return false
		}
		if typ == "response.output_text.delta" {
			return strings.TrimSpace(stringValue(raw["delta"])) != ""
		}
		if strings.Contains(typ, ".delta") || strings.Contains(typ, "_delta") {
			return strings.TrimSpace(stringValue(raw["delta"])) != "" || raw["content"] != nil || raw["text"] != nil
		}
		if typ == "response.incomplete" && (stringValue(raw["output_text"]) != "" || raw["output"] != nil) {
			return true
		}
	}
	if obj := strings.ToLower(stringValue(raw["object"])); obj == "chat.completion.chunk" {
		return chatCompletionChunkHasGeneration(raw)
	}
	if choices, ok := raw["choices"].([]any); ok && len(choices) > 0 {
		return chatChoicesHaveGeneration(choices)
	}
	if strings.TrimSpace(responseText(raw)) != "" || strings.TrimSpace(stringValue(raw["output_text"])) != "" {
		return true
	}
	if response, ok := raw["response"].(map[string]any); ok {
		if _, ok := response["error"]; ok {
			return false
		}
		status := strings.ToLower(stringValue(response["status"]))
		if (status == "completed" || status == "complete" || status == "succeeded") && strings.TrimSpace(responseText(response)) != "" {
			return true
		}
	}
	return false
}

func looksLikeHealthMathAnswer(body []byte) bool {
	text := strings.TrimSpace(string(bytes.Trim(body, `"'`)))
	text = strings.TrimSuffix(text, ".")
	text = strings.TrimSpace(text)
	switch text {
	case "2", "2.0":
		return true
	default:
		return false
	}
}

func chatCompletionChunkHasGeneration(raw map[string]any) bool {
	choices, ok := raw["choices"].([]any)
	if !ok {
		return false
	}
	return chatChoicesHaveGeneration(choices)
}

func chatChoicesHaveGeneration(choices []any) bool {
	for _, item := range choices {
		choice, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if delta, ok := choice["delta"].(map[string]any); ok {
			if strings.TrimSpace(stringValue(delta["content"])) != "" || strings.TrimSpace(stringValue(delta["reasoning_content"])) != "" {
				return true
			}
		}
		if message, ok := choice["message"].(map[string]any); ok {
			if strings.TrimSpace(stringValue(message["content"])) != "" {
				return true
			}
		}
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
	text = strings.ToLower(text)
	markers := []string{
		"invalid api key",
		"incorrect api key",
		"invalid x-api-key",
		"invalid token",
		"token invalid",
		"token is invalid",
		"invalid access token",
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

func looksLikeUpstreamRoutingUnavailable(text string) bool {
	if text == "" {
		return false
	}
	markers := []string{
		"all upstream group keys are temporarily disabled",
		"temporarily disabled by recent failures",
		"provider agent router",
		"cc switch local proxy failed",
		"ccswitch local proxy failed",
		"no available channel",
		"no available channels",
		"no available provider",
		"no available providers",
		"no available upstream",
		"no healthy upstream",
		"no healthy provider",
		"no usable upstream",
		"all upstreams unavailable",
		"all providers unavailable",
		"no route available",
		"route unavailable",
		"provider unavailable",
		"upstream temporarily unavailable",
		"无可用渠道",
		"没有可用渠道",
		"暂无可用渠道",
		"无可用上游",
		"没有可用上游",
		"暂无可用上游",
		"无可用供应商",
		"没有可用供应商",
		"上游暂不可用",
		"渠道暂不可用",
		"供应商暂不可用",
		"路由不可用",
		"无可用路由",
		"全部上游",
		"全部渠道",
		"临时禁用",
		"暂时禁用",
	}
	for _, marker := range markers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func healthRetryDelayForAttempt(keyID uint, retry int) time.Duration {
	if healthProbeRetryJitterMax <= 0 {
		return 0
	}
	maxSeconds := int(healthProbeRetryJitterMax / time.Second)
	if maxSeconds < 1 {
		maxSeconds = 1
	}
	// retry is one-based here.  The deterministic key jitter keeps a batch
	// spread over a few seconds, while later attempts back off a little longer.
	baseSeconds := retry
	if baseSeconds > maxSeconds {
		baseSeconds = maxSeconds
	}
	jitter := int(keyID) % maxSeconds
	delaySeconds := baseSeconds + jitter
	if delaySeconds > maxSeconds {
		delaySeconds = maxSeconds
	}
	return time.Duration(delaySeconds) * time.Second
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
		if s.interceptedResponseContent(key, string(respBody)) != "" {
			return status, header, respBody, true, errors.New("response content intercepted")
		}
		if isUpstreamErrorBody(respBody) {
			return status, header, respBody, true, fmt.Errorf("upstream returned error payload: %s", truncateBody(respBody, 240))
		}
		return status, header, respBody, false, nil
	}
	if s.interceptedResponseContent(key, string(respBody)) != "" {
		return status, header, respBody, true, errors.New("response content intercepted")
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
	if pool, fixed := oauthPoolForCandidate(key); fixed && oauthPoolLeaseFromContext(ctx) == nil {
		return s.requestOAuthPoolCandidate(ctx, request, key, timeout, pool)
	}
	var err error
	reqCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	var resp *http.Response
	var resolvedProvider oauthpool.ProviderKind
	if attempt := oauthPoolLeaseFromContext(ctx); attempt != nil && attempt.lease != nil && attempt.service != nil {
		resolved, resolveErr := attempt.lease.ResolveRequest(oauthpool.RequestInput{
			Method: request.Method, Path: request.Path, Header: request.Header, Body: request.Body, Stream: request.Stream,
		})
		if resolveErr != nil {
			return 0, nil, nil, resolveErr
		}
		resolvedProvider = resolved.Provider
		resp, err = attempt.service.Do(reqCtx, attempt.lease.Pool, resolved)
	} else {
		ch, findErr := s.channels.FindByID(key.ChannelID)
		if findErr != nil {
			return 0, nil, nil, findErr
		}
		upstreamKey, decryptErr := s.decryptUpstreamAPIKey(key)
		if decryptErr != nil {
			return 0, nil, nil, decryptErr
		}
		upstreamURL, joinErr := joinUpstreamURL(ch.SiteURL, request.Path)
		if joinErr != nil {
			return 0, nil, nil, joinErr
		}
		req, requestErr := http.NewRequestWithContext(reqCtx, request.Method, upstreamURL, bytes.NewReader(request.Body))
		if requestErr != nil {
			return 0, nil, nil, requestErr
		}
		copyRequestHeaders(req.Header, request.Header)
		s.applyUpstreamAuthHeaders(req.Header, key, upstreamKey)
		client, clientErr := s.httpClientFor(ctx, ch)
		if clientErr != nil {
			return 0, nil, nil, clientErr
		}
		resp, err = client.Do(req)
	}
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()

	header := cloneHeader(resp.Header)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && resolvedProvider == oauthpool.ProviderGrokWeb {
		respBody, parseErr := normalizeGrokWebOAuthResponse(resp.Body, request)
		if parseErr != nil {
			return resp.StatusCode, header, nil, parseErr
		}
		header.Set("Content-Type", "application/json; charset=utf-8")
		return resp.StatusCode, header, respBody, nil
	}
	respBody, readErr := readLimitedBody(resp.Body, 64<<20)
	if readErr != nil {
		return resp.StatusCode, header, nil, readErr
	}
	return resp.StatusCode, header, respBody, nil
}

func (s *Service) requestOAuthPoolCandidate(ctx context.Context, request normalizedRequest, key *storage.UpstreamGroupKey, timeout time.Duration, pool storage.OAuthPool) (int, http.Header, []byte, error) {
	service := s.oauthPoolService()
	if service == nil {
		return 0, nil, nil, errors.New("oauth_pool_unavailable: OAuth pool service is not configured")
	}
	poolCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		poolCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	excluded := make(map[uint]bool, oauthPoolAccountAttemptsPerCandidate)
	var lastErr error
	for attemptNumber := 0; attemptNumber < oauthPoolAccountAttemptsPerCandidate; attemptNumber++ {
		lease, err := service.AcquireContext(poolCtx, pool, routingRequestModel(request), excluded)
		if err != nil {
			if lastErr != nil {
				return 0, nil, nil, fmt.Errorf("oauth_pool_unavailable: %s pool exhausted after %d attempts: %w", pool, attemptNumber, lastErr)
			}
			return 0, nil, nil, fmt.Errorf("oauth_pool_unavailable: %s: %w", pool, err)
		}
		excluded[lease.Account.ID] = true
		s.attachOAuthDispatchAccount(poolCtx, lease, attemptNumber+1)
		attemptCtx := context.WithValue(poolCtx, oauthPoolLeaseContextKey{}, &oauthPoolAttemptContext{service: service, lease: lease})
		status, header, body, requestErr := s.requestCandidate(attemptCtx, request, key, 0)
		if requestErr == nil && status >= 200 && status < 300 {
			lease.ReportSuccess()
			s.recordOAuthPoolSuccess(lease)
			lease.Release()
			return status, header, body, nil
		}
		if requestErr != nil {
			lastErr = requestErr
		} else {
			lastErr = &GatewayError{Status: status, Header: header, Body: body}
		}
		retryAccount := requestErr != nil || status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusTooManyRequests || status >= 500
		if !retryAccount {
			lease.Ignore()
			lease.Release()
			return status, header, body, requestErr
		}
		failure := oauthPoolFailure(lastErr, status, header)
		lease.ReportFailure(failure)
		s.recordOAuthPoolFailure(lease, failure)
		lease.Release()
	}
	return 0, nil, nil, fmt.Errorf("oauth_pool_unavailable: %s pool account retry limit reached: %w", pool, lastErr)
}

func shouldCountOAuthAccountFailure(err error, status int) bool {
	var gatewayErr *GatewayError
	if errors.As(err, &gatewayErr) {
		status = gatewayErr.Status
	}
	switch status {
	case http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity:
		return false
	default:
		return true
	}
}

func (s *Service) requestHealthProbeCandidate(ctx context.Context, request normalizedRequest, key *storage.UpstreamGroupKey, timeout time.Duration) (int, http.Header, []byte, error) {
	if !request.Stream {
		return s.requestCandidate(ctx, request, key, timeout)
	}
	ch, err := s.channels.FindByID(key.ChannelID)
	if err != nil {
		return 0, nil, nil, err
	}
	upstreamKey, err := s.decryptUpstreamAPIKey(key)
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
	s.applyUpstreamAuthHeaders(req.Header, key, upstreamKey)
	// Health probes are streamed too. Avoid a compressed/buffered probe being
	// mistaken for an unhealthy upstream merely because its first token arrived
	// late at the gateway.
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Accept-Encoding", "identity")

	client, err := s.httpClientFor(ctx, ch)
	if err != nil {
		return 0, nil, nil, err
	}
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
	buffered, err := preflightHealthSSEStream(reader, resp.Body, timeout)
	body := healthProbeSSEBody(buffered)
	if err != nil {
		// 对齐调度：preflight 返回的错误文本也可能是软失败话术（"当前没有可用上游"
		// 之类被上游当正文返回），命中拦截词就归到上游错误而不是网络/超时错误。
		if s.interceptedResponseContent(key, err.Error()) != "" {
			return resp.StatusCode, header, body, errInterceptedHealthProbe
		}
		return resp.StatusCode, header, body, err
	}
	// 关键对齐点：测活曾只要 preflightHealthSSEStream 判到"有可见文本"就算活，
	// 完全不跑调度那套拦截词 / 终态失败检查，于是上游把"stream disconnected before
	// completion / 公益token休息了 / 当前没有可用上游"当正文吐回来时，调度命中拦截词
	// 切候选、测活却判活 → "测活活、调度死"。这里让测活复用调度的判定：
	//   1. 把 buffered 所有可见文本拼起来跑一遍拦截词；
	//   2. 对每个事件跑 preflight/终态失败检查（response.cancelled/incomplete、[DONE] 无输出等）。
	// 任一命中就把这次测活判成上游错误（非存活）。
	if failed, msg := healthBufferedDispatchFailure(buffered); failed {
		return resp.StatusCode, header, body, errors.New(msg)
	}
	var interceptText strings.Builder
	for _, ev := range buffered {
		interceptText.WriteString(streamEventInterceptableText(ev))
	}
	if s.interceptedResponseContent(key, interceptText.String()) != "" {
		return resp.StatusCode, header, body, errInterceptedHealthProbe
	}
	return resp.StatusCode, header, body, nil
}

// errInterceptedHealthProbe 是测活命中拦截词/软失败话术时的固定错误。文本包含
// "no available upstream" 特征串，使 looksLikeUpstreamRoutingUnavailable →
// healthFailureStatus 归到 upstream_error（前端显示"上游错误"），与调度对齐。
var errInterceptedHealthProbe = errors.New("health probe response content intercepted: no available upstream")

// healthBufferedDispatchFailure 复用调度 preflight 的终态失败判定：对测活缓冲到的
// 事件逐个检查 streamEventPreflightFailure / streamEventTerminalWithoutOutput，
// 命中即返回失败信息。让测活对"终态但无可见输出"的流与调度一致地判死。
func healthBufferedDispatchFailure(events []sseEvent) (bool, string) {
	sawGeneration := false
	for _, ev := range events {
		if failed, msg := streamEventFailure(ev); failed {
			return true, msg
		}
		if failed, msg := streamEventPreflightFailure(ev); failed {
			if sawGeneration {
				continue
			}
			return true, msg
		}
		if streamEventHasVisibleText(ev) {
			sawGeneration = true
		}
	}
	return false, ""
}

func healthProbeSSEBody(events []sseEvent) []byte {
	var fallback []byte
	for _, ev := range events {
		data := strings.TrimSpace(ev.Data)
		if data != "" && data != "[DONE]" {
			body := []byte(data)
			if looksLikeHealthGenerationSuccess(body) {
				return body
			}
			if fallback == nil {
				fallback = body
			}
		}
	}
	if fallback != nil {
		return fallback
	}
	if len(events) == 0 {
		return nil
	}
	return []byte(strings.TrimSpace(events[0].Data))
}

func (s *Service) httpClientFor(_ context.Context, ch *storage.Channel) (*http.Client, error) {
	proxyURL := ""
	if ch != nil {
		cfg := s.proxyConfig()
		targets := []string{config.ProxyChannelTarget(ch.ID)}
		switch strings.ToLower(strings.TrimSpace(string(ch.Type))) {
		case "chatgpt_pool":
			targets = append(targets, config.ProxyTargetChatGPTPool, config.ProxyTargetGPTPoolChannel)
		case "grok_pool":
			targets = append(targets, config.ProxyTargetGrokPool, config.ProxyTargetGrokPoolChannel)
		}
		if cfg.AppliesTo(targets...) {
			resolved, err := cfg.URL()
			if err != nil {
				return nil, fmt.Errorf("proxy configuration error: %w", err)
			}
			proxyURL = strings.TrimSpace(resolved)
		}
	}
	cacheKey := httpClientCacheKey(ch, proxyURL)
	if cached, ok := s.clients.Load(cacheKey); ok {
		return cached.(*http.Client), nil
	}
	client := &http.Client{Transport: buildProxyTransport(proxyURL)}
	actual, _ := s.clients.LoadOrStore(cacheKey, client)
	return actual.(*http.Client), nil
}

func httpClientCacheKey(ch *storage.Channel, proxyURL string) string {
	if ch == nil {
		return "default|" + HashKey(strings.TrimSpace(proxyURL))
	}
	return fmt.Sprintf("%d|%s", ch.ID, HashKey(strings.TrimSpace(proxyURL)))
}

func buildProxyTransport(proxyURL string) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 256
	transport.MaxIdleConnsPerHost = 64
	transport.IdleConnTimeout = 90 * time.Second
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.ExpectContinueTimeout = time.Second
	// Streaming requests enforce the shorter first-output budget with their
	// request context. Keep the shared transport header timeout at the full
	// per-attempt budget so non-stream requests are not cut off after six seconds.
	transport.ResponseHeaderTimeout = proxyAttemptTimeout
	if proxyURL = strings.TrimSpace(proxyURL); proxyURL != "" {
		if parsed, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(parsed)
		}
	}
	return transport
}

func (s *Service) markProxyFailure(id uint, msg string) {
	if !shouldMarkProxyFailure(msg) {
		return
	}
	status := proxyFailureStatus(msg)
	policy := s.proxyFailurePolicy(status, msg)
	immediate := status == "rate_limited" || status == "zero_balance" || status == "auth_failed" || status == "forbidden"
	persistedStatus := "alive"
	if immediate {
		persistedStatus = status
	}
	result, err := s.groupKeys.RecordDispatchFailure(
		id,
		persistedStatus,
		truncateLogMessage(msg, 1000),
		healthTransientFailureThreshold,
		5*time.Minute,
		policy.cooldown,
		immediate,
	)
	if err != nil && s.log != nil {
		s.log.Warn("mark upstream group failed", "id", id, "err", err)
	}
	if result.DisabledUntil != nil {
		s.recordRuntimeFailure(id, *result.DisabledUntil)
	}
	if policy.disableKey {
		if err := s.groupKeys.UpdateEnabled(id, false); err != nil && s.log != nil {
			s.log.Warn("disable upstream group key failed", "id", id, "err", err)
		}
	}
	s.InvalidateSchedulingCache()
}

func isTemporaryProxyFailureStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "rate_limited", "network_error", "timeout", "server_error", "upstream_error", "dead":
		return true
	default:
		return false
	}
}

// A requested model can be unavailable on one otherwise healthy upstream.
// It should trigger same-request failover, but must not turn the whole group
// red or put its key into cooldown: the next request may use a supported model.
//
// 慢首字节（reasoning 阶段较长的推理模型）同理：它只是"这一次等太久"，绝不代表
// 渠道坏了。首字节窗口放宽后仍偶发超时的，只做当次 fail-over，既不冷却也不累加
// 失败计数、状态保持 alive，避免可用渠道被逐个打进冷却导致候选池枯竭。
func shouldMarkProxyFailure(msg string) bool {
	return !looksLikeUnsupportedModelError(msg) &&
		!looksLikeClientRequestError(msg)
}

func shouldDelaySameDispatchGroupAfterFailure(msg string) bool {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return false
	}
	return !looksLikeClientRequestError(msg)
}

type proxyFailurePolicy struct {
	cooldown   time.Duration
	disableKey bool
}

func (s *Service) proxyFailurePolicy(status string, msg string) proxyFailurePolicy {
	switch status {
	case "rate_limited":
		// 上游明确给出的 Retry-After / reset 边界优先，且尊重其真实长度（长窗口用 8 天上限，
		// 不再被 30 分钟封顶反复探测）。无提示时退回用户可配的默认冷却（默认 300s）。
		if hinted, ok := retryAfterDurationFromText(msg, time.Now()); ok {
			return proxyFailurePolicy{cooldown: clampProxyCooldown(hinted, time.Second, proxyHintedCooldownMax)}
		}
		return proxyFailurePolicy{cooldown: clampProxyCooldown(s.temporaryFailureCooldown(), time.Second, proxyPermanentFailureCooldown)}
	case "zero_balance", "auth_failed":
		return proxyFailurePolicy{disableKey: true}
	case "forbidden":
		return proxyFailurePolicy{cooldown: proxyPermanentFailureCooldown}
	}
	// 其它临时故障（502/503/500 等 server_error、network_error、timeout、upstream_error）：
	// 上游若明确给了 Retry-After / reset_after_seconds，就按它的重试边界冷却（尊重真实长度），
	// 而不是一律套用默认 5 分钟。502 (retry-after: 60) 应只冷却 60s，避免把可用渠道长时间
	// 踢出候选池；无提示时退回默认冷却（默认 300s）。
	if hinted, ok := retryAfterDurationFromText(msg, time.Now()); ok {
		return proxyFailurePolicy{cooldown: clampProxyCooldown(hinted, time.Second, proxyHintedCooldownMax)}
	}
	return proxyFailurePolicy{cooldown: clampProxyCooldown(s.temporaryFailureCooldown(), time.Second, proxyPermanentFailureCooldown)}
}

func (s *Service) temporaryFailureCooldown() time.Duration {
	seconds := s.upstreamConfig().TemporaryFailureCooldownSeconds
	if seconds <= 0 {
		seconds = config.DefaultTemporaryFailureCooldownSeconds
	}
	return time.Duration(seconds) * time.Second
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
	currentFailures := 0
	if current, err := s.groupKeys.FindByID(id); err == nil && current != nil {
		currentFailures = current.FailureCount
	}
	delay := healthFailureCooldown(status, currentFailures+1)
	var disabledUntil *time.Time
	if delay > 0 {
		until := time.Now().Add(delay)
		disabledUntil = &until
		s.recordRuntimeFailure(id, until)
	} else {
		s.clearRuntimeDisable(id)
	}
	if err := s.groupKeys.MarkHealthFailureStatus(id, status, msg, disabledUntil, latencyMS); err != nil && s.log != nil {
		s.log.Warn("mark upstream health failed", "id", id, "err", err)
	}
}

func (s *Service) markHealthInconclusive(id uint, msg string, latencyMS int64) {
	s.clearRuntimeDisable(id)
	if err := s.groupKeys.MarkHealthInconclusive(id, msg, latencyMS); err != nil && s.log != nil {
		s.log.Warn("mark upstream health inconclusive", "id", id, "err", err)
	}
}

func healthFailureCooldown(status string, nextFailures int) time.Duration {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "rate_limited":
		return healthRateLimitCooldown
	case "zero_balance", "forbidden", "auth_failed":
		return proxyPermanentFailureCooldown
	case "invalid_request", "model_error", "non_generation":
		return 0
	}
	if nextFailures < healthTransientFailureThreshold {
		return 0
	}
	delay := healthTransientCooldownBase(status) * time.Duration(nextFailures-healthTransientFailureThreshold+1)
	if delay > 3*time.Minute {
		delay = 3 * time.Minute
	}
	return delay
}

func healthTransientCooldownBase(status string) time.Duration {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "server_error":
		return healthServerErrorCooldown
	case "timeout":
		return healthTimeoutCooldown
	case "network_error":
		return healthNetworkErrorCooldown
	default:
		return healthTransientFailureCooldown
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
		RequestModel: requestModelFromHTTP(r, body, rawQuery),
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
		req.ToolKinds = responsesToolKinds(body)
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
	if tools := chatToolsToResponsesTools(raw["tools"]); len(tools) > 0 {
		raw["tools"] = tools
	} else {
		delete(raw, "tools")
	}
	if choice := chatToolChoiceToResponses(raw["tool_choice"]); choice != nil {
		raw["tool_choice"] = choice
	} else {
		delete(raw, "tool_choice")
	}
	normalizeChatReasoningForResponses(raw)
	moveField(raw, "max_tokens", "max_output_tokens")
	moveField(raw, "max_completion_tokens", "max_output_tokens")
	delete(raw, "n")
	delete(raw, "logprobs")
	delete(raw, "top_logprobs")
	delete(raw, "stream_options")
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
	messages := responsesInputToChatMessages(raw["input"], raw["tools"])
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
	}
	if choice := responsesToolChoiceToChat(raw["tool_choice"]); choice != nil {
		out["tool_choice"] = choice
	}
	if parallel, ok := raw["parallel_tool_calls"]; ok {
		out["parallel_tool_calls"] = parallel
	}
	if mt, ok := raw["max_output_tokens"]; ok {
		out["max_tokens"] = mt
	}
	if effort := responsesReasoningEffort(raw["reasoning"]); effort != "" {
		out["reasoning_effort"] = effort
	}
	out["stream"] = stream
	encoded, err := json.Marshal(out)
	return encoded, stream, err
}

func normalizeChatReasoningForResponses(raw map[string]any) {
	if raw == nil {
		return
	}
	effort := strings.TrimSpace(stringValue(raw["reasoning_effort"]))
	if effort == "" {
		return
	}
	reasoning, _ := raw["reasoning"].(map[string]any)
	if reasoning == nil {
		reasoning = map[string]any{}
	}
	if strings.TrimSpace(stringValue(reasoning["effort"])) == "" {
		reasoning["effort"] = effort
	}
	raw["reasoning"] = reasoning
	delete(raw, "reasoning_effort")
}

func responsesReasoningEffort(value any) string {
	reasoning, _ := value.(map[string]any)
	if reasoning == nil {
		return ""
	}
	return strings.TrimSpace(stringValue(reasoning["effort"]))
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

// responsesToolKinds keeps the semantic type of tools that must be flattened
// for a Chat Completions upstream. A Codex custom tool (such as exec or
// apply_patch) may travel upstream as a Chat function, but it must be restored
// as custom_tool_call on the return path or the Codex client will not dispatch
// the local tool.
func responsesToolKinds(body []byte) map[string]string {
	var raw map[string]any
	if json.Unmarshal(body, &raw) != nil || raw == nil {
		return nil
	}
	tools, _ := raw["tools"].([]any)
	if len(tools) == 0 {
		return nil
	}
	kinds := make(map[string]string)
	for _, value := range tools {
		tool, _ := value.(map[string]any)
		if tool == nil || !strings.EqualFold(strings.TrimSpace(stringValue(tool["type"])), "custom") {
			continue
		}
		if name := strings.TrimSpace(stringValue(tool["name"])); name != "" {
			kinds[name] = "custom"
		}
	}
	if len(kinds) == 0 {
		return nil
	}
	return kinds
}

func responseToolKind(kindSets []map[string]string, name string) string {
	name = strings.TrimSpace(name)
	if name == "" || len(kindSets) == 0 || kindSets[0] == nil {
		return "function"
	}
	if kindSets[0][name] == "custom" {
		return "custom"
	}
	return "function"
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
func responsesInputToChatMessages(input any, tools ...any) []map[string]any {
	switch v := input.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return []map[string]any{{"role": "user", "content": v}}
	case []any:
		out := make([]map[string]any, 0, len(v))
		seenToolCalls := map[string]bool{}
		defaultToolName := firstResponsesToolName(firstNonNil(tools...))
		for _, item := range v {
			msg, _ := item.(map[string]any)
			if msg == nil {
				continue
			}
			typ := strings.ToLower(strings.TrimSpace(stringValue(msg["type"])))
			// A Responses follow-up commonly contains function_call_output (and
			// Codex can use other *_call_output item types) instead of repeating
			// the entire conversation. A Chat-compatible upstream needs a native
			// role=tool message; flattening this item into text loses the tool
			// result and makes the model answer conversationally rather than carry
			// on with the requested edit/tool workflow.
			if typ == "function_call_output" || strings.HasSuffix(typ, "_call_output") {
				callID := strings.TrimSpace(stringValue(firstNonNil(msg["call_id"], msg["id"])))
				if callID != "" {
					if !seenToolCalls[callID] {
						out = append(out, syntheticChatToolCallMessage(callID, responseCallOutputName(msg, defaultToolName)))
						seenToolCalls[callID] = true
					}
					out = append(out, map[string]any{
						"role":         "tool",
						"tool_call_id": callID,
						"content":      responsesToolOutputToChatContent(firstNonNil(msg["output"], msg["content"])),
					})
					continue
				}
			}
			// Preserve explicit prior function calls when a client sends the
			// complete Responses input. Chat APIs require the assistant tool-call
			// message immediately before its role=tool result.
			if typ == "function_call" || typ == "custom_tool_call" {
				name := strings.TrimSpace(stringValue(msg["name"]))
				callID := strings.TrimSpace(stringValue(firstNonNil(msg["call_id"], msg["id"])))
				if name != "" && callID != "" {
					out = append(out, map[string]any{
						"role":    "assistant",
						"content": nil,
						"tool_calls": []map[string]any{{
							"id":   callID,
							"type": "function",
							"function": map[string]any{
								"name":      name,
								"arguments": stringValue(firstNonNil(msg["arguments"], msg["input"])),
							},
						}},
					})
					seenToolCalls[callID] = true
					continue
				}
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

func syntheticChatToolCallMessage(callID, name string) map[string]any {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "previous_tool_call"
	}
	return map[string]any{
		"role":    "assistant",
		"content": nil,
		"tool_calls": []map[string]any{{
			"id":   callID,
			"type": "function",
			"function": map[string]any{
				"name":      name,
				"arguments": "{}",
			},
		}},
	}
}

func responseCallOutputName(msg map[string]any, fallback string) string {
	for _, key := range []string{"name", "tool_name", "function_name"} {
		if value := strings.TrimSpace(stringValue(msg[key])); value != "" {
			return value
		}
	}
	if fn, ok := msg["function"].(map[string]any); ok {
		if value := strings.TrimSpace(stringValue(fn["name"])); value != "" {
			return value
		}
	}
	return fallback
}

func firstResponsesToolName(value any) string {
	items, _ := value.([]any)
	for _, item := range items {
		switch tool := item.(type) {
		case string:
			if name := strings.TrimSpace(tool); name != "" {
				return name
			}
		case map[string]any:
			if name := firstResponsesToolNameFromMap(tool); name != "" {
				return name
			}
		}
	}
	return ""
}

func firstResponsesToolNameFromMap(tool map[string]any) string {
	if tool == nil {
		return ""
	}
	if name := strings.TrimSpace(stringValue(tool["name"])); name != "" {
		return name
	}
	if fn, ok := tool["function"].(map[string]any); ok {
		if name := strings.TrimSpace(stringValue(fn["name"])); name != "" {
			return name
		}
	}
	if children, ok := tool["tools"].([]any); ok {
		return firstResponsesToolName(children)
	}
	return ""
}

// responsesToolOutputToChatContent keeps structured tool output intact for a
// Chat-compatible upstream. Tool output is often JSON; converting it with
// fmt.Sprint loses valid JSON structure and can make a coding agent ignore an
// edit result or an error returned by its tool.
func responsesToolOutputToChatContent(output any) string {
	switch value := output.(type) {
	case nil:
		return ""
	case string:
		return value
	case []byte:
		return string(value)
	default:
		encoded, err := json.Marshal(value)
		if err == nil {
			return string(encoded)
		}
		return fmt.Sprint(value)
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
		if msg == nil {
			continue
		}
		role, _ := msg["role"].(string)
		if role == "" {
			role = "user"
		}
		if role == "tool" {
			callID := strings.TrimSpace(stringValue(msg["tool_call_id"]))
			if callID != "" {
				out = append(out, map[string]any{
					"type": "function_call_output", "call_id": callID,
					"output": responsesToolOutputToChatContent(msg["content"]),
				})
				continue
			}
		}
		content := normalizeChatContent(msg["content"], role)
		calls, _ := msg["tool_calls"].([]any)
		legacy, _ := msg["function_call"].(map[string]any)
		if meaningfulJSONValue(content) || len(calls) == 0 && legacy == nil {
			out = append(out, map[string]any{"role": role, "content": content})
		}
		for index, value := range calls {
			call, _ := value.(map[string]any)
			fn, _ := call["function"].(map[string]any)
			name := strings.TrimSpace(stringValue(fn["name"]))
			if name == "" {
				continue
			}
			callID := strings.TrimSpace(stringValue(call["id"]))
			if callID == "" {
				callID = fmt.Sprintf("call_%d", index)
			}
			out = append(out, map[string]any{
				"type": "function_call", "call_id": callID, "name": name,
				"arguments": normalizeDoubleEncodedToolArguments(stringValue(fn["arguments"])),
			})
		}
		if legacy != nil && strings.TrimSpace(stringValue(legacy["name"])) != "" {
			out = append(out, map[string]any{
				"type": "function_call", "call_id": "call_legacy", "name": stringValue(legacy["name"]),
				"arguments": normalizeDoubleEncodedToolArguments(stringValue(legacy["arguments"])),
			})
		}
	}
	return out
}

func normalizeDoubleEncodedToolArguments(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return value
	}
	var decoded any
	if json.Unmarshal([]byte(trimmed), &decoded) != nil {
		return value
	}
	nested, ok := decoded.(string)
	if !ok {
		return value
	}
	var structured any
	if json.Unmarshal([]byte(strings.TrimSpace(nested)), &structured) != nil {
		return value
	}
	switch structured.(type) {
	case map[string]any, []any:
		encoded, err := json.Marshal(structured)
		if err == nil {
			return string(encoded)
		}
	}
	return value
}

func chatToolsToResponsesTools(value any) []map[string]any {
	items, _ := value.([]any)
	out := make([]map[string]any, 0, len(items))
	for _, value := range items {
		tool, _ := value.(map[string]any)
		if tool == nil {
			continue
		}
		typ := strings.ToLower(strings.TrimSpace(stringValue(tool["type"])))
		if typ != "" && typ != "function" {
			continue
		}
		fn, _ := tool["function"].(map[string]any)
		if fn == nil {
			fn = tool
		}
		name := strings.TrimSpace(stringValue(fn["name"]))
		if name == "" {
			continue
		}
		converted := map[string]any{
			"type": "function", "name": name,
			"parameters": firstNonNil(fn["parameters"], fn["input_schema"], map[string]any{"type": "object", "properties": map[string]any{}}),
		}
		if description := stringValue(fn["description"]); description != "" {
			converted["description"] = description
		}
		if strict, ok := fn["strict"]; ok {
			converted["strict"] = strict
		}
		out = append(out, converted)
	}
	return out
}

func chatToolChoiceToResponses(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		choice := strings.TrimSpace(typed)
		if choice == "" {
			return nil
		}
		return choice
	case map[string]any:
		fn, _ := typed["function"].(map[string]any)
		name := strings.TrimSpace(stringValue(firstNonNil(fn["name"], typed["name"])))
		if name != "" {
			return map[string]any{"type": "function", "name": name}
		}
	}
	return nil
}

func normalizeChatContent(content any, role string) any {
	switch v := content.(type) {
	case nil:
		return ""
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
		// A saved Responses capability can become stale after an upstream route
		// change. When the upstream explicitly says the Responses route does not
		// exist, retry exactly once through the prepared Chat bridge *before any
		// downstream SSE byte has been written*. This is a protocol recovery path,
		// not a general model/content fallback: reasoning, tools and input remain
		// native whenever the Responses endpoint exists.
		if looksLikeResponsesToolCallStateError(errMsg) {
			return stripCodexResponsesLiteHeader(request.alt()), "stateless tool-call chat bridge", true
		}
		if looksLikeResponsesEndpointError(errMsg) || looksLikeEndpointMissingError(errMsg) {
			return request.alt(), "upstream chat-completions compatibility", true
		}
		return request, "", false
	}
	if !looksLikeResponsesEndpointError(errMsg) && !looksLikeEndpointMissingError(errMsg) {
		return request, "", false
	}
	return request.alt(), "upstream chat-completions compatibility", true
}

func fallbackRequestAfterCandidateFailure(
	request normalizedRequest,
	candidate *storage.UpstreamGroupKey,
	errMsg string,
	downstreamStarted bool,
) (normalizedRequest, string, bool) {
	if !downstreamStarted && candidate != nil && request.hasAlt() && request.ResponseMode == "responses" &&
		normalizeUpstreamRequestMode(candidate.RequestMode) == "chat" &&
		requestForcedNativeResponsesForChatCandidate(request) &&
		looksLikeExplicitModelNotSupportedFailure(errMsg) {
		return stripCodexResponsesLiteHeader(request.alt()), "chat-marked candidate model fallback", true
	}
	return fallbackRequestAfterFailure(request, errMsg)
}

func requestForcedNativeResponsesForChatCandidate(request normalizedRequest) bool {
	if request.ResponseMode != "responses" {
		return false
	}
	// GPT-5.6, Responses-Lite, tools, and stateful Responses payloads are
	// intentionally tried on the native wire first even for a chat-marked key.
	// Keep this predicate aligned with requestForCandidate so an explicit
	// model_not_supported response can safely retry that same key through its
	// prepared Chat bridge before the scheduler moves on.
	return requestRequiresNativeResponsesWire(request)
}

func responsesBodyHasDeclaredTools(body []byte) bool {
	var raw map[string]any
	if json.Unmarshal(body, &raw) != nil || raw == nil {
		return false
	}
	return meaningfulJSONValue(raw["tools"])
}

func looksLikeExplicitModelNotSupportedFailure(message string) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(message)), "model_not_supported")
}

// rememberSuccessfulProtocolFallback persists only a proven endpoint
// capability transition. It runs after a complete upstream success, so a
// temporary network failure or an unsupported model can never flip a channel
// from native Responses to Chat.
func (s *Service) rememberSuccessfulProtocolFallback(
	candidate *storage.UpstreamGroupKey,
	original normalizedRequest,
	fallback normalizedRequest,
) {
	if s == nil || candidate == nil || normalizeClientFormat(candidate.ClientFormat) != "openai" {
		return
	}
	if original.ResponseMode == "responses" && fallback.ResponseMode == "responses_from_chat" && responsesBodyHasOrphanToolOutputs(original.Body) {
		return
	}
	mode := ""
	switch {
	case original.ResponseMode == "responses" && fallback.ResponseMode == "responses_from_chat":
		mode = "chat"
	case original.ResponseMode == "responses_from_chat" && fallback.ResponseMode == "responses":
		mode = "responses"
	}
	if mode == "" || normalizeUpstreamRequestMode(candidate.RequestMode) == mode {
		return
	}
	if err := s.groupKeys.UpdateRequestMode(candidate.ID, mode); err != nil {
		if s.log != nil {
			s.log.Warn("persist recovered upstream request protocol", "id", candidate.ID, "mode", mode, "err", err)
		}
		return
	}
	candidate.RequestMode = mode
	s.InvalidateSchedulingCache()
}

func looksLikeResponsesToolCallStateError(msg string) bool {
	s := strings.ToLower(strings.TrimSpace(msg))
	if s == "" {
		return false
	}
	return (strings.Contains(s, "no tool call found") && strings.Contains(s, "function call output")) ||
		(strings.Contains(s, "function_call_output") && strings.Contains(s, "call_id") && strings.Contains(s, "not found")) ||
		(strings.Contains(s, "tool_call_id") && strings.Contains(s, "preceding") && strings.Contains(s, "tool_calls")) ||
		(strings.Contains(s, "messages with role") && strings.Contains(s, "tool") && strings.Contains(s, "tool_calls"))
}

func responsesBodyHasOrphanToolOutputs(body []byte) bool {
	var raw map[string]any
	if json.Unmarshal(body, &raw) != nil || raw == nil {
		return false
	}
	input, _ := raw["input"].([]any)
	if len(input) == 0 {
		return false
	}
	seen := map[string]bool{}
	for _, item := range input {
		msg, _ := item.(map[string]any)
		if msg == nil {
			continue
		}
		typ := strings.ToLower(strings.TrimSpace(stringValue(msg["type"])))
		callID := strings.TrimSpace(stringValue(firstNonNil(msg["call_id"], msg["id"])))
		if callID == "" {
			continue
		}
		if typ == "function_call" || typ == "custom_tool_call" {
			seen[callID] = true
			continue
		}
		if typ == "function_call_output" || strings.HasSuffix(typ, "_call_output") {
			if !seen[callID] {
				return true
			}
		}
	}
	return false
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

func (s *Service) orderCandidatesForRequest(candidates []storage.UpstreamGroupKey, request normalizedRequest, gatewayKeys ...*storage.GatewayKey) []storage.UpstreamGroupKey {
	requestModel := routingRequestModel(request)
	ordered := s.orderCandidatesWithRuntime(candidates, requestModel)
	ordered = s.preferSameGroupSchedulableCandidates(ordered, requestModel)
	if len(gatewayKeys) > 0 {
		ordered = s.applyGatewayRoutePreference(ordered, gatewayKeys[0], requestModel)
	}
	ordered = s.rotateEquivalentCandidates(ordered, requestModel, gatewayKeys...)
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
	//   软亲和 —— chat: 前缀，是我们为了上游 prompt 缓存命中率做的"尽量同一台"。
	//            只要原上游仍可调度就继续使用，避免同一上下文在多个上游之间来回跳，
	//            降低 provider 侧 prompt cache 命中率。
	hard := affinityIsHard(request.AffinityKey)
	for i, item := range ordered {
		if item.ID != affinity.GroupKeyID {
			continue
		}
		if !hard {
			// Soft affinity is only a cache-warming hint inside the same dispatch
			// tier. It must not jump over a charity route, a lower ratio, or a
			// cheaper equivalent route.
			if !s.softAffinityCanPromote(ordered, item, requestModel) {
				return ordered
			}
		}
		out := append([]storage.UpstreamGroupKey{item}, ordered[:i]...)
		out = append(out, ordered[i+1:]...)
		out = s.preferSameGroupSchedulableCandidates(out, requestModel)
		if !hard && len(gatewayKeys) > 0 {
			out = s.applyGatewayRoutePreference(out, gatewayKeys[0], requestModel)
		}
		return out
	}
	return ordered
}

func (s *Service) rotateEquivalentCandidates(candidates []storage.UpstreamGroupKey, model string, gatewayKeys ...*storage.GatewayKey) []storage.UpstreamGroupKey {
	if len(candidates) < 2 {
		return candidates
	}
	preference := gatewayRouteRatioFirst
	if len(gatewayKeys) > 0 && gatewayKeys[0] != nil {
		preference = normalizeGatewayRoutePreference(gatewayKeys[0].RoutePreference)
	}
	out := append([]storage.UpstreamGroupKey(nil), candidates...)
	for start := 0; start < len(out); {
		end := start + 1
		for end < len(out) && s.sameRoundRobinTier(out[start], out[end], model, preference) {
			end++
		}
		if size := end - start; size > 1 {
			key := roundRobinTierKey(out[start], model, preference)
			actual, _ := s.dispatchCursors.LoadOrStore(key, &atomic.Uint64{})
			offset := int((actual.(*atomic.Uint64).Add(1) - 1) % uint64(size))
			if offset > 0 {
				block := append([]storage.UpstreamGroupKey(nil), out[start:end]...)
				copy(out[start:end], append(block[offset:], block[:offset]...))
			}
		}
		start = end
	}
	return out
}

func (s *Service) sameRoundRobinTier(a, b storage.UpstreamGroupKey, model, preference string) bool {
	if !sameDispatchOrderTier(a, b) || a.Priority != b.Priority || a.FailureCount != b.FailureCount ||
		s.candidateEffectiveModelCapabilityRank(a, model) != s.candidateEffectiveModelCapabilityRank(b, model) {
		return false
	}
	if preference != gatewayRouteRatioFirst {
		_, poolA := oauthPoolForCandidate(&a)
		_, poolB := oauthPoolForCandidate(&b)
		return poolA == poolB
	}
	return true
}

func roundRobinTierKey(item storage.UpstreamGroupKey, model, preference string) string {
	_, pool := oauthPoolForCandidate(&item)
	return fmt.Sprintf("%d|%t|%d|%.9f|%.9f|%d|%d|%s|%s|%t",
		candidateDispatchLayer(item), item.Charity, statusRank(item.Status), effectiveGroupRatio(item),
		candidateDispatchCostScore(item, model), item.Priority, item.FailureCount,
		normalizeModelCapabilityKey(model), preference, pool,
	)
}

func (s *Service) applyGatewayRoutePreference(candidates []storage.UpstreamGroupKey, key *storage.GatewayKey, model string) []storage.UpstreamGroupKey {
	preference := gatewayRouteRatioFirst
	if key != nil {
		preference = normalizeGatewayRoutePreference(key.RoutePreference)
	}
	if preference == gatewayRouteRatioFirst || len(candidates) < 2 {
		return candidates
	}
	out := append([]storage.UpstreamGroupKey(nil), candidates...)
	for start := 0; start < len(out); {
		end := start + 1
		for end < len(out) && sameDispatchOrderTier(out[start], out[end]) &&
			s.candidateEffectiveModelCapabilityRank(out[start], model) == s.candidateEffectiveModelCapabilityRank(out[end], model) {
			end++
		}
		block := out[start:end]
		preferred := make([]storage.UpstreamGroupKey, 0, len(block))
		other := make([]storage.UpstreamGroupKey, 0, len(block))
		wantPool := preference == gatewayRoutePoolFirst
		for i := range block {
			_, fixed := oauthPoolForCandidate(&block[i])
			if fixed == wantPool {
				preferred = append(preferred, block[i])
			} else {
				other = append(other, block[i])
			}
		}
		copy(out[start:end], append(preferred, other...))
		start = end
	}
	return out
}

func (s *Service) softAffinityCanPromote(ordered []storage.UpstreamGroupKey, sticky storage.UpstreamGroupKey, model string) bool {
	if !candidateSchedulable(sticky) {
		return false
	}
	if len(ordered) == 0 || ordered[0].ID == sticky.ID {
		return true
	}
	best := ordered[0]
	// 粘住的渠道不能服务这个模型（已知不支持）时，不能强行保留——那会必然失败。
	// 只要能力不低于当前最优即可，允许"未知/已支持"的原渠道继续用。
	if s.candidateEffectiveModelCapabilityRank(sticky, model) > s.candidateEffectiveModelCapabilityRank(best, model) {
		return false
	}
	// 缓存粘性优先策略：切上游 = 换 provider 账户 = 上游 prompt cache 前缀失效，
	// 重喂整段上下文既慢又降智。因此只要原渠道仍健康可调度，就默认继续用它，不为了
	// 省一点点倍率差而跳走；仅当出现的更优候选便宜得足够多（成本下降比例达到
	// promoteMinSavingsRatio 逃生阀）时，才放弃缓存去切换。
	if aff := s.appConfig().RouteAffinity; aff.Enabled {
		// 更优候选处在更高的调度层（例如可用公益 vs 非公益）时，尊重公益优先，放弃粘性。
		// 公益额度是共享稀缺资源，不能为了单个会话的缓存把它一直占着不回落。
		if candidateDispatchLayer(best) < candidateDispatchLayer(sticky) {
			return false
		}
		stickyCost := candidateDispatchCostScore(sticky, model)
		bestCost := candidateDispatchCostScore(best, model)
		// 原渠道本身免费/最便宜（成本<=最优候选）时，没有任何省钱理由，直接保留。
		if stickyCost <= bestCost {
			return true
		}
		// 逃生阀：新渠道相对原渠道的成本下降比例达到阈值才值得牺牲一次缓存命中。
		savings := (stickyCost - bestCost) / stickyCost
		threshold := s.routeAffinityPromoteMinSavingsRatio()
		return savings < threshold
	}
	// 未开启缓存粘性：退回历史行为——只在同 tier、同能力时才保留原渠道。
	return sameDispatchOrderTier(best, sticky) &&
		s.candidateEffectiveModelCapabilityRank(best, model) == s.candidateEffectiveModelCapabilityRank(sticky, model)
}

// routeAffinityPromoteMinSavingsRatio 返回缓存粘性"逃生阀"阈值，可在系统设置里调整。
// <=0 或 >=1 时回退默认值，避免 0 阈值让任何微小差价都触发切换、或 >=1 锁死切换。
func (s *Service) routeAffinityPromoteMinSavingsRatio() float64 {
	r := s.appConfig().RouteAffinity.PromoteMinSavingsRatio
	if r <= 0 || r >= 1 {
		return config.DefaultRouteAffinityPromoteMinSavingsRatio
	}
	return r
}

func (s *Service) preferSameGroupSchedulableCandidates(candidates []storage.UpstreamGroupKey, model string) []storage.UpstreamGroupKey {
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
		if candidateSchedulable(item) && dispatchGroupIdentity(item) == target && sameDispatchOrderTier(candidates[0], item) &&
			s.candidateEffectiveModelCapabilityRank(candidates[0], model) == s.candidateEffectiveModelCapabilityRank(item, model) {
			out = append(out, item)
			continue
		}
		rest = append(rest, item)
	}
	out = append(out, rest...)
	return out
}

func sameDispatchOrderTier(a, b storage.UpstreamGroupKey) bool {
	if candidateSchedulable(a) != candidateSchedulable(b) {
		return false
	}
	if a.Charity != b.Charity {
		return false
	}
	if statusRank(a.Status) != statusRank(b.Status) {
		return false
	}
	if !nearlyEqualFloat(effectiveGroupRatio(a), effectiveGroupRatio(b)) {
		return false
	}
	return nearlyEqualFloat(candidateDispatchCostScore(a), candidateDispatchCostScore(b))
}

func nearlyEqualFloat(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9
}

func dispatchGroupIdentity(item storage.UpstreamGroupKey) string {
	name := strings.ToLower(strings.TrimSpace(item.GroupRef))
	if name == "" {
		name = strings.ToLower(strings.TrimSpace(item.GroupName))
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

func (s *Service) orderCandidatesWithRuntime(candidates []storage.UpstreamGroupKey, requestModels ...string) []storage.UpstreamGroupKey {
	out := orderCandidates(candidates)
	requestModel := ""
	if len(requestModels) > 0 {
		requestModel = normalizeModelCapabilityKey(requestModels[0])
	}
	sort.SliceStable(out, func(i, j int) bool {
		schedI := candidateSchedulable(out[i])
		schedJ := candidateSchedulable(out[j])
		if schedI != schedJ {
			return schedI
		}
		supportI, supportJ := s.candidateEffectiveModelCapabilityRank(out[i], requestModel), s.candidateEffectiveModelCapabilityRank(out[j], requestModel)
		if (supportI == 2) != (supportJ == 2) {
			return supportI != 2
		}
		if layerI, layerJ := candidateDispatchLayer(out[i]), candidateDispatchLayer(out[j]); layerI != layerJ {
			return layerI < layerJ
		}
		if rankI, rankJ := statusRank(out[i].Status), statusRank(out[j].Status); rankI != rankJ {
			return rankI < rankJ
		}
		if ratioI, ratioJ := effectiveGroupRatio(out[i]), effectiveGroupRatio(out[j]); ratioI != ratioJ {
			return ratioI < ratioJ
		}
		if costI, costJ := candidateDispatchCostScore(out[i], requestModel), candidateDispatchCostScore(out[j], requestModel); costI != costJ {
			return costI < costJ
		}
		if supportI != supportJ {
			return supportI < supportJ
		}
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		if out[i].FailureCount != out[j].FailureCount {
			return out[i].FailureCount < out[j].FailureCount
		}
		ttftI, okI := s.runtimeFirstTokenLatency(out[i].ID)
		ttftJ, okJ := s.runtimeFirstTokenLatency(out[j].ID)
		switch {
		case okI && okJ && ttftI != ttftJ:
			return ttftI < ttftJ
		case okI != okJ:
			return okI
		}
		latI, latencyI := s.runtimeLatency(out[i].ID)
		latJ, latencyJ := s.runtimeLatency(out[j].ID)
		switch {
		case latencyI && latencyJ && latI != latJ:
			return latI < latJ
		case latencyI != latencyJ:
			return latencyI
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

func routingRequestModel(request normalizedRequest) string {
	return firstNonEmpty(request.RequestModel, modelFromRequestBody(request.Body))
}

func normalizeModelCapabilityKey(model string) string {
	return strings.ToLower(strings.TrimSpace(model))
}

// candidateModelCapabilityRank returns supported (0), unknown (1), or known
// unsupported (2). Once a concrete upstream has proved it cannot serve a model,
// skip it before dispatch so Codex does not pay for a doomed first attempt.
func (s *Service) filterKnownUnsupportedModelCandidates(candidates []storage.UpstreamGroupKey, model string) []storage.UpstreamGroupKey {
	model = normalizeModelCapabilityKey(model)
	if model == "" {
		return candidates
	}
	out := make([]storage.UpstreamGroupKey, 0, len(candidates))
	for _, candidate := range candidates {
		if s.candidateEffectiveModelCapabilityRank(candidate, model) == 2 {
			continue
		}
		out = append(out, candidate)
	}
	return out
}

func filterSchedulableCandidates(candidates []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	out := make([]storage.UpstreamGroupKey, 0, len(candidates))
	for _, candidate := range candidates {
		if candidateSchedulable(candidate) || candidateRecoverableAfterCooldown(candidate) {
			out = append(out, candidate)
		}
	}
	return out
}

func (s *Service) candidateEffectiveModelCapabilityRank(candidate storage.UpstreamGroupKey, model string) int {
	model = normalizeModelCapabilityKey(model)
	if candidate.ModelRestrictionEnabled {
		if supportedModelsContain(candidate.SupportedModels, model) {
			return 0
		}
		// A strict empty allowlist intentionally rejects every model.
		return 2
	}
	rank := s.candidateModelCapabilityRank(candidate.ID, model)
	if rank != 1 {
		return rank
	}
	if probeRank := healthProbeModelCapabilityRank(candidate.HealthProbeModel, model); probeRank != 1 {
		return probeRank
	}
	// Legacy unrestricted rows retain the former positive-hint behavior until
	// an operator explicitly synchronizes or saves a strict policy.
	if supportedModelsContain(candidate.SupportedModels, model) {
		return 0
	}
	return 1
}

// supportedModelsContain 判断请求模型是否命中渠道声明的支持模型清单（JSON 数组文本）。
// 清单为空 / 解析失败 / 未命中都返回 false（调用方据此保持"未知"，不做任何排除）。
func supportedModelsContain(supportedModelsJSON, model string) bool {
	model = normalizeModelCapabilityKey(model)
	if strings.TrimSpace(supportedModelsJSON) == "" || model == "" {
		return false
	}
	var models []string
	if err := json.Unmarshal([]byte(supportedModelsJSON), &models); err != nil {
		return false
	}
	for _, m := range models {
		if normalizeModelCapabilityKey(m) == model {
			return true
		}
	}
	return false
}

func healthProbeModelCapabilityRank(probeModel, requestModel string) int {
	probeModel = normalizeModelCapabilityKey(probeModel)
	requestModel = normalizeModelCapabilityKey(requestModel)
	if probeModel == "" || requestModel == "" {
		return 1
	}
	if probeModel == requestModel {
		return 0
	}
	if probeModel == normalizeModelCapabilityKey(openAIHealthProbeFallbackModel) &&
		requestModel == normalizeModelCapabilityKey(openAIHealthProbePrimaryModel) {
		return 2
	}
	if !healthProbeModelIsOneOf(probeModel, openAIHealthProbePrimaryModel, openAIHealthProbeFallbackModel) &&
		healthProbeModelIsOneOf(requestModel, openAIHealthProbePrimaryModel, openAIHealthProbeFallbackModel) {
		return 2
	}
	return 1
}

func (s *Service) candidateModelCapabilityRank(candidateID uint, model string) int {
	model = normalizeModelCapabilityKey(model)
	if candidateID == 0 || model == "" {
		return 1
	}
	state, ok := s.runtime.Load(candidateID)
	if !ok {
		return 1
	}
	stateValue := state.(*groupRuntimeState)
	stateValue.mu.Lock()
	defer stateValue.mu.Unlock()
	capability, ok := stateValue.modelCapabilities[model]
	if !ok {
		return 1
	}
	if capability.expiresAt.IsZero() || !time.Now().Before(capability.expiresAt) {
		delete(stateValue.modelCapabilities, model)
		return 1
	}
	if capability.supported {
		return 0
	}
	return 2
}

func (s *Service) rememberCandidateModelCapability(candidateID uint, model string, supported bool) {
	model = normalizeModelCapabilityKey(model)
	if candidateID == 0 || model == "" {
		return
	}
	state := s.runtimeState(candidateID)
	ttl := modelSupportNegativeTTL
	if supported {
		ttl = modelSupportPositiveTTL
	}
	state.mu.Lock()
	if state.modelCapabilities == nil {
		state.modelCapabilities = make(map[string]modelCapability)
	}
	if len(state.modelCapabilities) >= 256 {
		for key, capability := range state.modelCapabilities {
			if !time.Now().Before(capability.expiresAt) {
				delete(state.modelCapabilities, key)
			}
		}
		if len(state.modelCapabilities) >= 256 {
			state.modelCapabilities = make(map[string]modelCapability)
		}
	}
	state.modelCapabilities[model] = modelCapability{supported: supported, expiresAt: time.Now().Add(ttl)}
	state.mu.Unlock()
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

func (s *Service) runtimeFirstTokenLatency(id uint) (float64, bool) {
	state, ok := s.runtime.Load(id)
	if !ok {
		return 0, false
	}
	current := state.(*groupRuntimeState)
	current.mu.Lock()
	defer current.mu.Unlock()
	if current.avgFirstTokenMS <= 0 {
		return 0, false
	}
	return current.avgFirstTokenMS, true
}

func (s *Service) tryAcquireScheduledCandidate(candidate *storage.UpstreamGroupKey) (func(), bool) {
	if candidate == nil {
		return nil, false
	}
	id, limit := candidate.ID, candidate.ConcurrencyLimit
	state := s.runtimeState(id)
	state.mu.Lock()
	now := time.Now()
	if candidateNeedsRecoveryProbe(*candidate, now) {
		state.needsRecovery = true
	}
	if state.inFlight >= limit {
		if limit <= 0 {
			// Unlimited candidates do not use the in-flight ceiling.
		} else {
			state.mu.Unlock()
			return nil, false
		}
	}
	recoveryLease := false
	if state.needsRecovery {
		if state.recoveryInFlight {
			state.mu.Unlock()
			return nil, false
		}
		state.recoveryInFlight = true
		recoveryLease = true
	}
	if limit > 0 && state.inFlight >= limit {
		state.mu.Unlock()
		return nil, false
	}
	if limit > 0 {
		state.inFlight++
	}
	state.lastObservedAt = time.Now()
	state.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			state.mu.Lock()
			if limit > 0 && state.inFlight > 0 {
				state.inFlight--
			}
			if recoveryLease {
				state.recoveryInFlight = false
			}
			state.lastObservedAt = time.Now()
			state.mu.Unlock()
		})
	}, true
}

// tryAcquireCandidate preserves the small runtime-concurrency primitive used
// by existing tests and internal callers that do not have a persisted
// candidate snapshot. Recovery-probe semantics require the full candidate and
// are implemented by tryAcquireScheduledCandidate above.
func (s *Service) tryAcquireCandidate(id uint, limit int) (func(), bool) {
	return s.tryAcquireScheduledCandidate(&storage.UpstreamGroupKey{ID: id, ConcurrencyLimit: limit})
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

// lookupRequestIPPolicy checks the forwarded chain and the peer address.  The
// first address remains the canonical address for logs and concurrency; the
// complete check prevents a blacklist from being bypassed by a forged header
// or missed because a reverse proxy exposes both X-Forwarded-For and RemoteAddr.
func (s *Service) lookupRequestIPPolicy(r *http.Request, canonicalIP string) (*storage.IPPolicy, error) {
	if s.ipPolicies == nil {
		return nil, nil
	}
	for _, ip := range clientIPCandidates(r) {
		policy, err := s.lookupIPPolicy(ip)
		if err != nil {
			return nil, err
		}
		if policy != nil && policy.Blocked {
			return policy, nil
		}
	}
	return s.lookupIPPolicy(canonicalIP)
}

// publicIPConcurrencyBudget 返回公益 Key 单 IP 并发上限，可在系统设置的公益 Key
// 配置里调整（存 config.yaml 的 app.publicKey.ipConcurrencyLimit）。<=0 时回退默认值，
// 保持历史行为。app 配置通过 UpdateAppConfig 热更新到网关，无需重启即可生效。
func (s *Service) publicIPConcurrencyBudget() int {
	if limit := s.appConfig().PublicKey.IPConcurrencyLimit; limit > 0 {
		return limit
	}
	return defaultPublicIPConcurrencyLimit
}

func (s *Service) acquirePublicIPSlot(ctx context.Context, key *storage.GatewayKey, ip string, policy *storage.IPPolicy) (func(), error) {
	if key == nil || !key.IsPublic || strings.TrimSpace(ip) == "" || (policy != nil && policy.PublicConcurrencyExempt) {
		return func() {}, nil
	}
	limit := s.publicIPConcurrencyBudget()
	stateAny, _ := s.ipRuntime.LoadOrStore(ip, &keyRuntimeState{})
	state := stateAny.(*keyRuntimeState)
	state.mu.Lock()
	if state.inFlight < limit && len(state.queue) == 0 {
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

func (s *Service) recordRuntimeSuccess(id uint, duration time.Duration, firstToken ...time.Duration) {
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
	firstTokenDuration := duration
	if len(firstToken) > 0 && firstToken[0] > 0 {
		firstTokenDuration = firstToken[0]
	}
	firstTokenMS := float64(firstTokenDuration.Milliseconds())
	if firstTokenMS < 1 {
		firstTokenMS = 1
	}
	if state.avgFirstTokenMS <= 0 {
		state.avgFirstTokenMS = firstTokenMS
	} else {
		state.avgFirstTokenMS = state.avgFirstTokenMS*0.75 + firstTokenMS*0.25
	}
	state.disabledUntil = time.Time{}
	state.needsRecovery = false
	state.recoveryInFlight = false
	state.lastObservedAt = time.Now()
	s.candidateCache.invalidate()
}

func (s *Service) recordRuntimeFailure(id uint, disabledUntil time.Time) {
	state := s.runtimeState(id)
	state.mu.Lock()
	defer state.mu.Unlock()
	if disabledUntil.After(state.disabledUntil) {
		state.disabledUntil = disabledUntil
	}
	state.needsRecovery = !state.disabledUntil.IsZero()
	state.recoveryInFlight = false
	state.lastObservedAt = time.Now()
	s.candidateCache.invalidate()
}

func (s *Service) clearRuntimeDisable(id uint) {
	state := s.runtimeState(id)
	state.mu.Lock()
	defer state.mu.Unlock()
	state.disabledUntil = time.Time{}
	state.needsRecovery = false
	state.recoveryInFlight = false
	state.lastObservedAt = time.Now()
	s.candidateCache.invalidate()
}

func orderCandidates(in []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	out := append([]storage.UpstreamGroupKey(nil), in...)
	sort.SliceStable(out, func(i, j int) bool {
		if layerI, layerJ := candidateDispatchLayer(out[i]), candidateDispatchLayer(out[j]); layerI != layerJ {
			return layerI < layerJ
		}
		if rankI, rankJ := statusRank(out[i].Status), statusRank(out[j].Status); rankI != rankJ {
			return rankI < rankJ
		}
		if ratioI, ratioJ := effectiveGroupRatio(out[i]), effectiveGroupRatio(out[j]); ratioI != ratioJ {
			return ratioI < ratioJ
		}
		if costI, costJ := candidateDispatchCostScore(out[i]), candidateDispatchCostScore(out[j]); costI != costJ {
			return costI < costJ
		}
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		if out[i].FailureCount != out[j].FailureCount {
			return out[i].FailureCount < out[j].FailureCount
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func candidateDispatchLayer(item storage.UpstreamGroupKey) int {
	if candidateSchedulable(item) {
		if item.Charity {
			return 0
		}
		return 1
	}
	if temporaryCooldownFallbackCandidate(item) {
		if item.Charity {
			return 2
		}
		return 3
	}
	return 4
}

func candidateSchedulable(item storage.UpstreamGroupKey) bool {
	switch strings.ToLower(strings.TrimSpace(item.Status)) {
	case "", "alive", "unknown", "network_error", "timeout", "upstream_error", "server_error":
		return true
	default:
		return false
	}
}

func candidateRecoverableAfterCooldown(item storage.UpstreamGroupKey) bool {
	switch strings.ToLower(strings.TrimSpace(item.Status)) {
	case "cooling", "rate_limited":
		return item.DisabledUntil != nil
	default:
		return false
	}
}

func candidateNeedsRecoveryProbe(item storage.UpstreamGroupKey, now time.Time) bool {
	if item.DisabledUntil == nil || item.DisabledUntil.After(now) {
		return false
	}
	if candidateRecoverableAfterCooldown(item) {
		return true
	}
	return item.FailureCount >= healthTransientFailureThreshold
}

func hasSchedulableCharityCandidate(candidates []storage.UpstreamGroupKey) bool {
	for _, candidate := range candidates {
		if candidate.Charity && candidateSchedulable(candidate) {
			return true
		}
	}
	return false
}

// candidateDispatchCostScore is the local ordering price for paid routes. It
// uses the configured per-million input/output prices and ratio, independent
// of whatever a relay reports in a response.  We use equal input/output
// weights for ordering because the exact output size is unknown before a
// request; billing itself still uses the exact locally-counted split.
func candidateDispatchCostScore(candidate storage.UpstreamGroupKey, model ...string) float64 {
	price := priceForModelOrCandidate(firstNonEmpty(model...), candidate)
	return (price.InputPerMillion + price.OutputPerMillion) * effectiveGroupRatio(candidate)
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
		keyExpiresAt := expiresAt
		if strings.HasPrefix(key, "chat:implicit:") {
			// A fallback used only when the client gave no conversation/cache
			// identity should not pin unrelated work for an entire day.
			keyExpiresAt = now.Add(30 * time.Minute)
		}
		if err := s.affinities.Upsert(HashKey(key), groupKeyID, keyExpiresAt, now); err != nil && s.log != nil {
			s.log.Warn("remember gateway affinity failed", "err", err)
		}
	}
}

func implicitRequestAffinityKey(gatewayKey *storage.GatewayKey, request normalizedRequest) string {
	if gatewayKey == nil || gatewayKey.ID == 0 {
		return ""
	}
	model := normalizeModelCapabilityKey(routingRequestModel(request))
	if model == "" {
		return ""
	}
	client := strings.TrimSpace(request.ClientIP)
	if client == "" {
		client = "unknown"
	}
	return fmt.Sprintf("chat:implicit:%d:%s:%s", gatewayKey.ID, client, model)
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
	} else if strings.HasPrefix(id, "resp_") {
		id = "chatcmpl_" + strings.TrimPrefix(id, "resp_")
	}
	message := map[string]any{"role": "assistant", "content": text}
	if reasoning := responsesReasoningSummaryText(raw); reasoning != "" {
		message["reasoning_content"] = reasoning
	}
	toolCalls := responsesOutputToolCalls(raw)
	finishReason := "stop"
	if len(toolCalls) > 0 {
		finishReason = "tool_calls"
		if text == "" {
			message["content"] = nil
		}
		message["tool_calls"] = toolCalls
	}
	resp := map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]any{{
			"index":         0,
			"message":       message,
			"finish_reason": finishReason,
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

func responsesReasoningSummaryText(raw map[string]any) string {
	output, _ := raw["output"].([]any)
	var b strings.Builder
	for _, value := range output {
		item, _ := value.(map[string]any)
		if item == nil || !strings.EqualFold(strings.TrimSpace(stringValue(item["type"])), "reasoning") {
			continue
		}
		summary, _ := item["summary"].([]any)
		for _, partValue := range summary {
			part, _ := partValue.(map[string]any)
			b.WriteString(stringValue(part["text"]))
		}
	}
	return b.String()
}

func responsesOutputToolCalls(raw map[string]any) []any {
	output, _ := raw["output"].([]any)
	toolCalls := make([]any, 0)
	for index, value := range output {
		item, _ := value.(map[string]any)
		if item == nil {
			continue
		}
		typ := strings.ToLower(strings.TrimSpace(stringValue(item["type"])))
		if typ != "function_call" && typ != "custom_tool_call" {
			continue
		}
		name := strings.TrimSpace(stringValue(item["name"]))
		if name == "" {
			continue
		}
		callID := strings.TrimSpace(stringValue(firstNonNil(item["call_id"], item["id"])))
		if callID == "" {
			callID = fmt.Sprintf("call_%d", index)
		}
		toolCalls = append(toolCalls, map[string]any{
			"id": callID, "type": "function",
			"function": map[string]any{
				"name": name, "arguments": stringValue(firstNonNil(item["arguments"], item["input"])),
			},
		})
	}
	return toolCalls
}

// chatToResponsesResponse 把上游返回的 chat.completion（非流式）转换成 Responses 对象，
// 用于 responses→chat 降级后，把 chat 回复还原成客户端期待的 responses 格式。
func chatToResponsesResponse(body []byte, toolKinds ...map[string]string) ([]byte, error) {
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
	output := make([]map[string]any, 0, 2)
	if reasoning := chatCompletionReasoningText(raw); reasoning != "" {
		output = append(output, map[string]any{
			"id": "rs_" + strings.TrimPrefix(id, "resp_"), "type": "reasoning", "status": "completed",
			"summary": []any{map[string]any{"type": "summary_text", "text": reasoning}},
		})
	}
	if text != "" {
		output = append(output, responsesMessageOutputItem(id, len(output), text))
	}
	// Chat compatible upstreams put tool calls in message.tool_calls.  Keeping
	// them only in the transient Chat envelope made Codex treat the converted
	// response as plain text and skip the tool execution turn.
	if choices, _ := raw["choices"].([]any); len(choices) > 0 {
		choice, _ := choices[0].(map[string]any)
		message, _ := choice["message"].(map[string]any)
		calls, _ := message["tool_calls"].([]any)
		for index, value := range calls {
			call, _ := value.(map[string]any)
			fn, _ := call["function"].(map[string]any)
			name := stringValue(fn["name"])
			if name == "" {
				continue
			}
			callID := stringValue(call["id"])
			if callID == "" {
				callID = fmt.Sprintf("call_%s_%d", strings.TrimPrefix(id, "resp_"), index)
			}
			if responseToolKind(toolKinds, name) == "custom" {
				output = append(output, responsesCustomToolCallOutputItem(id, len(output), callID, name, stringValue(fn["arguments"])))
			} else {
				output = append(output, responsesFunctionCallOutputItem(id, len(output), callID, name, stringValue(fn["arguments"])))
			}
		}
		if legacy, _ := message["function_call"].(map[string]any); legacy != nil && stringValue(legacy["name"]) != "" {
			output = append(output, responsesFunctionCallOutputItem(id, len(output), "call_"+strings.TrimPrefix(id, "resp_"), stringValue(legacy["name"]), stringValue(legacy["arguments"])))
		}
	}
	if len(output) == 0 {
		output = append(output, responsesMessageOutputItem(id, 0, ""))
	}
	resp := map[string]any{
		"id":          id,
		"object":      "response",
		"created_at":  time.Now().Unix(),
		"model":       model,
		"status":      "completed",
		"output":      output,
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

func streamNonSSEAsResponsesEvents(w http.ResponseWriter, status int, header http.Header, body []byte, key *storage.UpstreamGroupKey, mode string, toolKinds ...map[string]string) (usageTokens, error) {
	outBody := body
	if mode == "responses_from_chat" || looksLikeChatCompletionResponse(body) {
		if converted, err := chatToResponsesResponse(body, toolKinds...); err == nil {
			outBody = converted
		}
	}
	id, model, text, usage := responsesCompletionPartsFromBody(outBody)
	output := responsesOutputItemsFromBody(outBody)
	copyResponseHeaders(w, header, key)
	setStreamResponseHeaders(w)
	w.WriteHeader(status)
	if err := writeResponsesCreated(w, id, model); err != nil {
		return usage, err
	}
	textStarted := false
	textOutputIndex := 0
	for outputIndex, item := range output {
		switch strings.TrimSpace(stringValue(item["type"])) {
		case "function_call":
			callID := stringValue(item["call_id"])
			name := stringValue(item["name"])
			arguments := stringValue(item["arguments"])
			if err := writeResponsesFunctionCallAdded(w, id, outputIndex, callID, name); err != nil {
				return usage, err
			}
			if arguments != "" {
				if err := writeResponsesFunctionCallArgumentsDelta(w, id, outputIndex, callID, arguments); err != nil {
					return usage, err
				}
			}
			if err := writeResponsesFunctionCallDone(w, id, outputIndex, callID, name, arguments); err != nil {
				return usage, err
			}
		case "message":
			messageText := responseOutputItemText(item)
			if err := writeResponsesOutputStartAtIndex(w, id, outputIndex); err != nil {
				return usage, err
			}
			if messageText != "" {
				if err := writeResponsesTextDeltaAtIndex(w, id, outputIndex, messageText); err != nil {
					return usage, err
				}
			}
			// Chat-compatible non-SSE replies contain at most one assistant text
			// item. Preserve its index for the terminal content-part events.
			if !textStarted {
				textStarted = true
				textOutputIndex = outputIndex
				text = messageText
			}
		}
	}
	if len(output) == 0 {
		output = []map[string]any{responsesMessageOutputItem(id, 0, text)}
		textStarted = true
		textOutputIndex = 0
		if err := writeResponsesOutputStartAtIndex(w, id, textOutputIndex); err != nil {
			return usage, err
		}
		if text != "" {
			if err := writeResponsesTextDeltaAtIndex(w, id, textOutputIndex, text); err != nil {
				return usage, err
			}
		}
	}
	if err := writeResponsesStreamEndWithOutput(w, id, model, text, usage, textOutputIndex, textStarted, output); err != nil {
		return usage, err
	}
	return usage, nil
}

func streamNonSSEAsChatEvents(w http.ResponseWriter, status int, header http.Header, body []byte, key *storage.UpstreamGroupKey) (usageTokens, error) {
	chatBody := body
	if !looksLikeChatCompletionResponse(chatBody) {
		converted, err := responsesToChat(chatBody)
		if err != nil {
			return usageTokens{}, fmt.Errorf("convert non-stream response for Chat streaming client: %w", err)
		}
		chatBody = converted
	}
	var raw map[string]any
	if err := json.Unmarshal(chatBody, &raw); err != nil {
		return usageTokens{}, fmt.Errorf("decode non-stream Chat response: %w", err)
	}
	id := normalizeChatCompletionID(stringValue(raw["id"]))
	model := stringValue(raw["model"])
	created := time.Now().Unix()
	if value, ok := numericValue(raw["created"]); ok && value > 0 {
		created = value
	}
	usage := extractUsage(chatBody)
	usage.ResponseID = firstNonEmpty(usage.ResponseID, id)
	usage.Model = firstNonEmpty(usage.Model, model)
	copyResponseHeaders(w, header, key)
	setStreamResponseHeaders(w)
	if !responseWriterStarted(w) {
		w.WriteHeader(status)
	}
	if err := writeChatStreamChunk(w, id, model, created, map[string]any{"role": "assistant"}, nil); err != nil {
		return usage, err
	}
	finishReason := "stop"
	choices, _ := raw["choices"].([]any)
	if len(choices) > 0 {
		choice, _ := choices[0].(map[string]any)
		message, _ := choice["message"].(map[string]any)
		if reasoning := stringValue(firstNonNil(message["reasoning_content"], message["reasoning"])); reasoning != "" {
			if err := writeChatStreamChunk(w, id, model, created, map[string]any{"reasoning_content": reasoning}, nil); err != nil {
				return usage, err
			}
		}
		if content := chatCompletionText(raw); content != "" {
			usage.GeneratedText = content
			if err := writeChatStreamChunk(w, id, model, created, map[string]any{"content": content}, nil); err != nil {
				return usage, err
			}
		}
		if calls, _ := message["tool_calls"].([]any); len(calls) > 0 {
			finishReason = "tool_calls"
			if err := writeChatStreamChunk(w, id, model, created, map[string]any{"tool_calls": calls}, nil); err != nil {
				return usage, err
			}
		}
		if finish := strings.TrimSpace(stringValue(choice["finish_reason"])); finish != "" {
			finishReason = finish
		}
	}
	if err := writeChatStreamFinalChunk(w, id, model, created, finishReason, usage); err != nil {
		return usage, err
	}
	if err := writeSSEData(w, "[DONE]"); err != nil {
		return usage, err
	}
	return usage, nil
}

func isChatCompletionPath(path string) bool {
	if index := strings.Index(path, "?"); index >= 0 {
		path = path[:index]
	}
	path = "/" + strings.Trim(strings.TrimSpace(path), "/")
	return path == "/v1/chat/completions" || path == "/chat/completions"
}

func responsesOutputItemsFromBody(body []byte) []map[string]any {
	var raw map[string]any
	if json.Unmarshal(body, &raw) != nil || raw == nil {
		return nil
	}
	if nested, ok := raw["response"].(map[string]any); ok && nested != nil {
		raw = nested
	}
	values, _ := raw["output"].([]any)
	items := make([]map[string]any, 0, len(values))
	for _, value := range values {
		if item, ok := value.(map[string]any); ok && item != nil {
			items = append(items, item)
		}
	}
	return items
}

func responseOutputItemText(item map[string]any) string {
	content, _ := item["content"].([]any)
	var text strings.Builder
	for _, value := range content {
		part, _ := value.(map[string]any)
		if part != nil && stringValue(part["type"]) == "output_text" {
			text.WriteString(stringValue(part["text"]))
		}
	}
	return text.String()
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
	usage := usageTokens{ResponseID: id}
	if usageRaw, ok := raw["usage"].(map[string]any); ok {
		usage = usageFromMap(usageRaw)
		usage.ResponseID = id
	}
	usage.Model = model
	return id, model, text, usage
}

func buildResponsesCompletedResponse(id, model, itemID, text string, usage usageTokens) map[string]any {
	if itemID == "" {
		itemID = responseItemID(id)
	}
	return buildResponsesCompletedResponseWithOutput(id, model, []map[string]any{responsesMessageOutputItemWithID(itemID, text)}, text, usage)
}

func buildResponsesCompletedResponseWithOutput(id, model string, output []map[string]any, text string, usage usageTokens) map[string]any {
	if id == "" {
		id = "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	if len(output) == 0 {
		output = []map[string]any{responsesMessageOutputItem(id, 0, text)}
	}
	resp := map[string]any{
		"id":          id,
		"object":      "response",
		"created_at":  time.Now().Unix(),
		"status":      "completed",
		"model":       model,
		"output":      output,
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

func responsesMessageOutputItemWithID(itemID, text string) map[string]any {
	return map[string]any{
		"id": itemID, "type": "message", "role": "assistant", "status": "completed",
		"content": []map[string]any{{"type": "output_text", "text": text}},
	}
}

func responsesMessageOutputItem(responseID string, outputIndex int, text string) map[string]any {
	return responsesMessageOutputItemWithID(responseItemIDForIndex(responseID, outputIndex), text)
}

func responsesFunctionCallOutputItem(responseID string, outputIndex int, callID, name, arguments string) map[string]any {
	return map[string]any{
		"id": responseFunctionItemID(responseID, outputIndex), "type": "function_call", "call_id": callID,
		"name": name, "arguments": arguments, "status": "completed",
	}
}

func responsesCustomToolCallOutputItem(responseID string, outputIndex int, callID, name, input string) map[string]any {
	return map[string]any{
		"id": responseFunctionItemID(responseID, outputIndex), "type": "custom_tool_call", "call_id": callID,
		"name": name, "input": input, "status": "completed",
	}
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
	return writeResponsesOutputStartAtIndex(w, id, 0)
}

func writeResponsesOutputStartAtIndex(w http.ResponseWriter, id string, outputIndex int) error {
	if id == "" {
		id = "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	itemID := responseItemIDForIndex(id, outputIndex)
	added := map[string]any{
		"type":         "response.output_item.added",
		"response_id":  id,
		"output_index": outputIndex,
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
		"output_index":  outputIndex,
		"content_index": 0,
		"part":          map[string]any{"type": "output_text", "text": ""},
	}
	return writeSSEEvent(w, sseEvent{Event: "response.content_part.added", Data: mustJSON(part)})
}

func writeResponsesTextDelta(w http.ResponseWriter, id, delta string) error {
	return writeResponsesTextDeltaAtIndex(w, id, 0, delta)
}

func writeResponsesTextDeltaAtIndex(w http.ResponseWriter, id string, outputIndex int, delta string) error {
	payload := map[string]any{
		"type":          "response.output_text.delta",
		"response_id":   id,
		"item_id":       responseItemIDForIndex(id, outputIndex),
		"output_index":  outputIndex,
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

func writeResponsesCustomToolCallAdded(w http.ResponseWriter, responseID string, outputIndex int, callID, name string) error {
	itemID := responseFunctionItemID(responseID, outputIndex)
	payload := map[string]any{
		"type":         "response.output_item.added",
		"response_id":  responseID,
		"output_index": outputIndex,
		"item": map[string]any{
			"id": itemID, "type": "custom_tool_call", "call_id": callID,
			"name": name, "input": "", "status": "in_progress",
		},
	}
	return writeSSEEvent(w, sseEvent{Event: "response.output_item.added", Data: mustJSON(payload)})
}

func writeResponsesToolCallAdded(w http.ResponseWriter, responseID string, outputIndex int, callID, name, kind string) error {
	if kind == "custom" {
		return writeResponsesCustomToolCallAdded(w, responseID, outputIndex, callID, name)
	}
	return writeResponsesFunctionCallAdded(w, responseID, outputIndex, callID, name)
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

func writeResponsesCustomToolCallInputDelta(w http.ResponseWriter, responseID string, outputIndex int, callID, delta string) error {
	payload := map[string]any{
		"type":         "response.custom_tool_call_input.delta",
		"response_id":  responseID,
		"item_id":      responseFunctionItemID(responseID, outputIndex),
		"output_index": outputIndex,
		"call_id":      callID,
		"delta":        delta,
	}
	return writeSSEEvent(w, sseEvent{Event: "response.custom_tool_call_input.delta", Data: mustJSON(payload)})
}

func writeResponsesToolCallInputDelta(w http.ResponseWriter, responseID string, outputIndex int, callID, delta, kind string) error {
	if kind == "custom" {
		return writeResponsesCustomToolCallInputDelta(w, responseID, outputIndex, callID, delta)
	}
	return writeResponsesFunctionCallArgumentsDelta(w, responseID, outputIndex, callID, delta)
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

func writeResponsesCustomToolCallDone(w http.ResponseWriter, responseID string, outputIndex int, callID, name, input string) error {
	itemID := responseFunctionItemID(responseID, outputIndex)
	inputDone := map[string]any{
		"type": "response.custom_tool_call_input.done", "response_id": responseID, "item_id": itemID,
		"output_index": outputIndex, "call_id": callID, "input": input,
	}
	if err := writeSSEEvent(w, sseEvent{Event: "response.custom_tool_call_input.done", Data: mustJSON(inputDone)}); err != nil {
		return err
	}
	itemDone := map[string]any{
		"type": "response.output_item.done", "response_id": responseID, "output_index": outputIndex,
		"item": responsesCustomToolCallOutputItem(responseID, outputIndex, callID, name, input),
	}
	return writeSSEEvent(w, sseEvent{Event: "response.output_item.done", Data: mustJSON(itemDone)})
}

func writeResponsesToolCallDone(w http.ResponseWriter, responseID string, outputIndex int, callID, name, input, kind string) error {
	if kind == "custom" {
		return writeResponsesCustomToolCallDone(w, responseID, outputIndex, callID, name, input)
	}
	return writeResponsesFunctionCallDone(w, responseID, outputIndex, callID, name, input)
}

func writeResponsesStreamEnd(w http.ResponseWriter, id, model, text string, usage usageTokens) error {
	return writeResponsesStreamEndWithOutput(w, id, model, text, usage, 0, true, nil)
}

// writeResponsesStreamEndWithOutput closes a Responses stream with the exact
// output items that were emitted.  This matters for tool-only turns: the final
// response.completed must contain function_call items, not a fabricated empty
// assistant message.
func writeResponsesStreamEndWithOutput(w http.ResponseWriter, id, model, text string, usage usageTokens, textOutputIndex int, textStarted bool, output []map[string]any) error {
	if id == "" {
		id = "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	if textStarted {
		itemID := responseItemIDForIndex(id, textOutputIndex)
		textDone := map[string]any{"type": "response.output_text.done", "response_id": id, "item_id": itemID, "output_index": textOutputIndex, "content_index": 0, "text": text}
		if err := writeSSEEvent(w, sseEvent{Event: "response.output_text.done", Data: mustJSON(textDone)}); err != nil {
			return err
		}
		partDone := map[string]any{"type": "response.content_part.done", "response_id": id, "item_id": itemID, "output_index": textOutputIndex, "content_index": 0, "part": map[string]any{"type": "output_text", "text": text}}
		if err := writeSSEEvent(w, sseEvent{Event: "response.content_part.done", Data: mustJSON(partDone)}); err != nil {
			return err
		}
		itemDone := map[string]any{"type": "response.output_item.done", "response_id": id, "output_index": textOutputIndex, "item": responsesMessageOutputItemWithID(itemID, text)}
		if err := writeSSEEvent(w, sseEvent{Event: "response.output_item.done", Data: mustJSON(itemDone)}); err != nil {
			return err
		}
	}
	completed := buildResponsesCompletedResponseWithOutput(id, model, output, text, usage)
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
	case strings.Contains(lower, "no configured upstream supports requested model"):
		return "所有可用上游均不支持当前请求模型，请切换模型后重试。"
	case strings.Contains(lower, "no alive upstream group keys available") ||
		strings.Contains(lower, "all upstream group keys failed") ||
		strings.Contains(lower, "upstreams temporarily unavailable"):
		return "当前没有可用上游，请稍后重试；如果持续出现，请检查上游渠道状态。"
	case looksLikeUnsupportedModelError(trimmed):
		return "当前上游不支持请求的模型，已自动尝试其他兼容渠道；请稍后重试或切换模型。"
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
	return responseItemIDForIndex(responseID, 0)
}

func responseItemIDForIndex(responseID string, outputIndex int) string {
	if responseID == "" {
		responseID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	base := "item_" + strings.TrimPrefix(responseID, "resp_")
	if outputIndex <= 0 {
		return base
	}
	return fmt.Sprintf("%s_%d", base, outputIndex)
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

func chatCompletionReasoningText(raw map[string]any) string {
	choices, _ := raw["choices"].([]any)
	var b strings.Builder
	for _, c := range choices {
		choice, _ := c.(map[string]any)
		if choice == nil {
			continue
		}
		message, _ := choice["message"].(map[string]any)
		for _, key := range []string{"reasoning_content", "reasoning"} {
			if text := stringValue(message[key]); text != "" {
				b.WriteString(text)
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

// generatedTextFromResponse is used exclusively by local accounting.  It
// understands both OpenAI Responses and Chat Completions envelopes without
// trusting either envelope's usage fields.
func generatedTextFromResponse(body []byte) string {
	var raw map[string]any
	if json.Unmarshal(body, &raw) != nil {
		return ""
	}
	if text := strings.TrimSpace(stringValue(raw["output_text"])); text != "" {
		return text
	}
	if text := strings.TrimSpace(responseText(raw)); text != "" {
		return text
	}
	return strings.TrimSpace(chatCompletionText(raw))
}

// calculateLocalUsage deliberately owns the accounting boundary.  Compatible
// upstreams often omit usage, return incompatible fields, or report a token
// count for a transformed request.  Charging one caller from those values made
// the same request cost differently depending on the relay.  Keep only the
// response metadata collected from upstream; all token and cache figures are
// calculated from the request and response that passed through this gateway.
func (s *Service) calculateLocalUsage(usage *usageTokens, request normalizedRequest, candidate *storage.UpstreamGroupKey) {
	if usage == nil {
		return
	}
	prompt := approximateRequestTokens(request.Body)
	completion := approximateTokenCount(usage.GeneratedText)
	usage.Prompt = prompt
	usage.Completion = completion
	usage.Total = prompt + completion
	usage.Cached = 0
	usage.Estimated = true
	if model := routingRequestModel(request); model != "" {
		usage.Model = model
	}

	// This is a gateway-side cache-eligibility measurement, not a provider
	// billing claim.  Once a stable conversation/cache key has already been
	// routed to this same upstream, its prompt is eligible for that provider's
	// cache.  The calculation remains fully local and works for every relay.
	if prompt > 0 && candidate != nil && s != nil && s.affinities != nil && request.AffinityKey != "" {
		if affinity, err := s.affinities.Find(HashKey(request.AffinityKey), time.Now()); err == nil && affinity != nil && affinity.GroupKeyID == candidate.ID {
			usage.Cached = prompt
		}
	}
}

// approximateRequestTokens counts semantic request values rather than the
// JSON wire envelope, so model names, transport flags and field names do not
// inflate a caller's local usage.  It intentionally includes instructions,
// messages, input and tool schemas because those all form the model context.
func approximateRequestTokens(body []byte) int64 {
	var raw any
	if json.Unmarshal(body, &raw) != nil {
		return approximateTokenCount(string(body))
	}
	var text strings.Builder
	var walk func(any, string)
	walk = func(value any, field string) {
		switch v := value.(type) {
		case map[string]any:
			for key, item := range v {
				switch strings.ToLower(key) {
				case "model", "stream", "stream_options", "temperature", "top_p", "max_output_tokens", "max_tokens", "n", "seed", "user", "metadata", "store":
					continue
				}
				walk(item, key)
			}
		case []any:
			for _, item := range v {
				walk(item, field)
			}
		case string:
			if strings.TrimSpace(v) != "" {
				text.WriteString(v)
				text.WriteByte('\n')
			}
		case json.Number, float64, float32, int, int64, bool:
			// Tool argument defaults and structured input are also context.
			text.WriteString(fmt.Sprint(v))
			text.WriteByte('\n')
		}
	}
	walk(raw, "")
	return approximateTokenCount(text.String())
}

func approximateTokenCount(text string) int64 {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	var ascii, nonASCII int64
	for _, r := range text {
		if r <= 0x7f {
			ascii++
		} else {
			nonASCII++
		}
	}
	// English-like JSON/text averages about four ASCII characters per token;
	// non-ASCII characters are counted conservatively as one token each.
	return maxInt64(1, (ascii+3)/4+nonASCII)
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
	prompt := firstUsageInt(usageRaw, "prompt_tokens", "input_tokens", "promptTokens", "inputTokens", "input_token_count")
	completion := firstUsageInt(usageRaw, "completion_tokens", "output_tokens", "completionTokens", "outputTokens", "output_token_count")
	total := firstUsageInt(usageRaw, "total_tokens", "totalTokens", "total_token_count")
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

func firstUsageInt(values map[string]any, keys ...string) int64 {
	for _, key := range keys {
		if value := intField(values, key); value > 0 {
			return value
		}
	}
	return 0
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
func streamChatAsResponsesEvents(w http.ResponseWriter, buffered []sseEvent, reader *sseStreamReader, toolKinds ...map[string]string) (usageTokens, error) {
	id := "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	model := ""
	createdSent := false
	textStarted := false
	textOutputIndex := -1
	reasoningStarted := false
	reasoningOutputIndex := -1
	reasoningItemID := ""
	nextOutputIndex := 0
	var best usageTokens
	var textBuf strings.Builder
	var reasoningBuf strings.Builder
	type chatToolCallState struct {
		OutputIndex int
		CallID      string
		Name        string
		Kind        string
		Added       bool
		Args        strings.Builder
	}
	toolCalls := map[int]*chatToolCallState{}
	toolOrder := make([]int, 0)
	sawDone := false
	var interceptBuf strings.Builder

	emitCreated := func() error {
		if createdSent {
			return nil
		}
		createdSent = true
		return writeResponsesCreated(w, id, model)
	}
	emitTextStart := func() error {
		if textStarted {
			return nil
		}
		if err := emitCreated(); err != nil {
			return err
		}
		textOutputIndex = nextOutputIndex
		nextOutputIndex++
		textStarted = true
		return writeResponsesOutputStartAtIndex(w, id, textOutputIndex)
	}
	emitReasoningDelta := func(delta string) error {
		if delta == "" {
			return nil
		}
		if !reasoningStarted {
			if err := emitCreated(); err != nil {
				return err
			}
			reasoningStarted = true
			reasoningOutputIndex = nextOutputIndex
			nextOutputIndex++
			reasoningItemID = "rs_" + strings.TrimPrefix(id, "resp_")
			added := map[string]any{
				"type": "response.output_item.added", "response_id": id, "output_index": reasoningOutputIndex,
				"item": map[string]any{"id": reasoningItemID, "type": "reasoning", "status": "in_progress", "summary": []any{}},
			}
			if err := writeSSEEvent(w, sseEvent{Event: "response.output_item.added", Data: mustJSON(added)}); err != nil {
				return err
			}
			partAdded := map[string]any{
				"type": "response.reasoning_summary_part.added", "response_id": id, "item_id": reasoningItemID,
				"output_index": reasoningOutputIndex, "summary_index": 0,
				"part": map[string]any{"type": "summary_text", "text": ""},
			}
			if err := writeSSEEvent(w, sseEvent{Event: "response.reasoning_summary_part.added", Data: mustJSON(partAdded)}); err != nil {
				return err
			}
		}
		reasoningBuf.WriteString(delta)
		payload := map[string]any{
			"type": "response.reasoning_summary_text.delta", "response_id": id, "item_id": reasoningItemID,
			"output_index": reasoningOutputIndex, "summary_index": 0, "delta": delta,
		}
		return writeSSEEvent(w, sseEvent{Event: "response.reasoning_summary_text.delta", Data: mustJSON(payload)})
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
					state = &chatToolCallState{OutputIndex: nextOutputIndex}
					nextOutputIndex++
					toolCalls[idx] = state
					toolOrder = append(toolOrder, idx)
				}
				if callID := stringValue(call["id"]); callID != "" {
					state.CallID = callID
				}
				fn, _ := call["function"].(map[string]any)
				if name := stringValue(fn["name"]); name != "" {
					state.Name = name
					state.Kind = responseToolKind(toolKinds, name)
				}
				argsDelta := stringValue(fn["arguments"])
				if state.CallID == "" {
					state.CallID = "call_" + strconv.FormatInt(time.Now().UnixNano()+int64(idx), 36)
				}
				if state.Name == "" && argsDelta == "" {
					continue
				}
				if !state.Added {
					if err := emitCreated(); err != nil {
						return err
					}
					if err := writeResponsesToolCallAdded(w, id, state.OutputIndex, state.CallID, state.Name, state.Kind); err != nil {
						return err
					}
					state.Added = true
				}
				if argsDelta != "" {
					state.Args.WriteString(argsDelta)
					if err := writeResponsesToolCallInputDelta(w, id, state.OutputIndex, state.CallID, argsDelta, state.Kind); err != nil {
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
		if failed, message := sanitizedStreamEventFailure(reader, sseEvent{Event: event, Data: data}); failed {
			if err := emitCreated(); err != nil {
				return err
			}
			if err := writeResponsesStreamFailure(w, id, model, "upstream_error", message); err != nil {
				return err
			}
			best.SoftFailure = message
			best.Status = "failed"
			return errResponsesStreamTerminal
		}
		// 正式转发阶段的内容拦截：上游（尤其 grok 中转）可能先吐正常文本，再把"公益 token
		// 休息了 / 当前没有可用上游"之类的软失败话术塞进后续 delta。preflight 只看首字节前的
		// 内容抓不到，这里逐段累积可见文本，命中拦截词就写 response.failed 终态并中断，标记
		// SoftFailure 让上层把该渠道打入冷却，避免把错误话术继续透传给客户端。
		if reader != nil && reader.interceptContent != nil {
			if text := streamEventInterceptableText(sseEvent{Event: event, Data: data}); text != "" {
				interceptBuf.WriteString(text)
				if reader.interceptContent(interceptBuf.String()) {
					message := "response content intercepted"
					if err := emitCreated(); err != nil {
						return err
					}
					if err := writeResponsesStreamFailure(w, id, model, "upstream_error", "upstream stream interrupted"); err != nil {
						return err
					}
					best.SoftFailure = message
					best.Status = "failed"
					return errResponsesStreamTerminal
				}
			}
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
				usage.GeneratedText = best.GeneratedText
				best = usage
			}
		}
		if reasoningDelta := chatChunkReasoningText(raw); reasoningDelta != "" {
			if err := emitReasoningDelta(reasoningDelta); err != nil {
				return err
			}
		}
		// 从 chat chunk 里取出增量文本。
		delta := chatChunkDeltaText(raw)
		if delta != "" {
			if err := emitTextStart(); err != nil {
				return err
			}
			textBuf.WriteString(delta)
			best.GeneratedText = textBuf.String()
			if err := writeResponsesTextDeltaAtIndex(w, id, textOutputIndex, delta); err != nil {
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
	if best.Status == "failed" {
		return best, nil
	}
	if err := emitCreated(); err != nil {
		return best, err
	}
	if streamErr != nil || !sawDone {
		// Some OpenAI-compatible relays end a chat SSE stream with EOF instead
		// of a final finish_reason or [DONE]. If they have already produced
		// usable text or tool-call deltas, close the Responses lifecycle cleanly
		// instead of surfacing a client-visible interrupted stream.
		if streamErr == nil && (textBuf.Len() > 0 || reasoningBuf.Len() > 0 || len(toolOrder) > 0) {
			sawDone = true
		} else {
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
	}
	for _, idx := range toolOrder {
		state := toolCalls[idx]
		if state == nil || !state.Added {
			continue
		}
		if err := writeResponsesToolCallDone(w, id, state.OutputIndex, state.CallID, state.Name, state.Args.String(), state.Kind); err != nil {
			return best, err
		}
	}
	if reasoningStarted {
		reasoningText := reasoningBuf.String()
		textDone := map[string]any{
			"type": "response.reasoning_summary_text.done", "response_id": id, "item_id": reasoningItemID,
			"output_index": reasoningOutputIndex, "summary_index": 0, "text": reasoningText,
		}
		if err := writeSSEEvent(w, sseEvent{Event: "response.reasoning_summary_text.done", Data: mustJSON(textDone)}); err != nil {
			return best, err
		}
		partDone := map[string]any{
			"type": "response.reasoning_summary_part.done", "response_id": id, "item_id": reasoningItemID,
			"output_index": reasoningOutputIndex, "summary_index": 0,
			"part": map[string]any{"type": "summary_text", "text": reasoningText},
		}
		if err := writeSSEEvent(w, sseEvent{Event: "response.reasoning_summary_part.done", Data: mustJSON(partDone)}); err != nil {
			return best, err
		}
		itemDone := map[string]any{
			"type": "response.output_item.done", "response_id": id, "output_index": reasoningOutputIndex,
			"item": map[string]any{
				"id": reasoningItemID, "type": "reasoning", "status": "completed",
				"summary": []any{map[string]any{"type": "summary_text", "text": reasoningText}},
			},
		}
		if err := writeSSEEvent(w, sseEvent{Event: "response.output_item.done", Data: mustJSON(itemDone)}); err != nil {
			return best, err
		}
	}
	outputByIndex := make(map[int]map[string]any, len(toolOrder)+1)
	if reasoningStarted {
		outputByIndex[reasoningOutputIndex] = map[string]any{
			"id": reasoningItemID, "type": "reasoning", "status": "completed",
			"summary": []any{map[string]any{"type": "summary_text", "text": reasoningBuf.String()}},
		}
	}
	if textStarted {
		outputByIndex[textOutputIndex] = responsesMessageOutputItem(id, textOutputIndex, textBuf.String())
	}
	for _, idx := range toolOrder {
		state := toolCalls[idx]
		if state != nil && state.Added {
			if state.Kind == "custom" {
				outputByIndex[state.OutputIndex] = responsesCustomToolCallOutputItem(id, state.OutputIndex, state.CallID, state.Name, state.Args.String())
			} else {
				outputByIndex[state.OutputIndex] = responsesFunctionCallOutputItem(id, state.OutputIndex, state.CallID, state.Name, state.Args.String())
			}
		}
	}
	if len(outputByIndex) == 0 {
		// An empty Chat completion is still a valid Responses terminal.  Start a
		// message only here so a tool-only completion never gets a fake item 0.
		textOutputIndex = 0
		textStarted = true
		nextOutputIndex = 1
		outputByIndex[textOutputIndex] = responsesMessageOutputItem(id, textOutputIndex, "")
		if err := writeResponsesOutputStartAtIndex(w, id, textOutputIndex); err != nil {
			return best, err
		}
	}
	output := make([]map[string]any, 0, len(outputByIndex))
	for outputIndex := 0; outputIndex < nextOutputIndex; outputIndex++ {
		if item, ok := outputByIndex[outputIndex]; ok {
			output = append(output, item)
		}
	}
	// 收尾：补齐 Responses 生命周期终态，保证 Codex 一定能看到 response.completed。
	if err := writeResponsesStreamEndWithOutput(w, id, model, textBuf.String(), best, textOutputIndex, textStarted, output); err != nil {
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

func chatChunkReasoningText(raw map[string]any) string {
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
		for _, key := range []string{"reasoning_content", "reasoning"} {
			if text := stringValue(delta[key]); text != "" {
				b.WriteString(text)
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
	toolCallsSeen := false
	toolIndexByOutput := map[int]int{}
	nextToolIndex := 0
	var best usageTokens
	var interceptBuf strings.Builder
	ensureRole := func() error {
		if roleSent {
			return nil
		}
		if err := writeChatStreamChunk(w, id, model, created, map[string]any{"role": "assistant"}, nil); err != nil {
			return err
		}
		roleSent = true
		return nil
	}
	chatToolIndex := func(outputIndex int) int {
		if index, ok := toolIndexByOutput[outputIndex]; ok {
			return index
		}
		index := nextToolIndex
		nextToolIndex++
		toolIndexByOutput[outputIndex] = index
		return index
	}
	err := readSSEEvents(buffered, reader, func(event, data string) error {
		if data == "" {
			return nil
		}
		if data == "[DONE]" {
			return errResponsesStreamTerminal
		}
		if failed, message := sanitizedStreamEventFailure(reader, sseEvent{Event: event, Data: data}); failed {
			if !roleSent {
				if err := writeChatStreamChunk(w, id, model, created, map[string]any{"role": "assistant"}, nil); err != nil {
					return err
				}
				roleSent = true
			}
			if err := writeChatStreamChunk(w, id, model, created, map[string]any{}, "stop"); err != nil {
				return err
			}
			if err := writeSSEData(w, "[DONE]"); err != nil {
				return err
			}
			doneSent = true
			best.SoftFailure = message
			best.Status = "failed"
			return errResponsesStreamTerminal
		}
		// 正式转发阶段的内容拦截：grok 等中转常把"公益 token 休息了 / 当前没有可用上游"之类
		// 的软失败话术塞进正常文本之后的 delta 里，preflight 只看首字节前的内容抓不到。这里
		// 逐段累积可见文本，命中拦截词就按 chat 格式收尾（stop + [DONE]）并标记 SoftFailure，
		// 让上层把该渠道打入冷却，避免把错误话术继续透传给客户端。
		if reader != nil && reader.interceptContent != nil {
			if text := streamEventInterceptableText(sseEvent{Event: event, Data: data}); text != "" {
				interceptBuf.WriteString(text)
				if reader.interceptContent(interceptBuf.String()) {
					if !roleSent {
						if err := writeChatStreamChunk(w, id, model, created, map[string]any{"role": "assistant"}, nil); err != nil {
							return err
						}
						roleSent = true
					}
					if err := writeChatStreamChunk(w, id, model, created, map[string]any{}, "stop"); err != nil {
						return err
					}
					if err := writeSSEData(w, "[DONE]"); err != nil {
						return err
					}
					doneSent = true
					best.SoftFailure = "response content intercepted"
					best.Status = "failed"
					return errResponsesStreamTerminal
				}
			}
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(data), &raw); err != nil {
			return nil
		}
		if response, ok := raw["response"].(map[string]any); ok {
			if v, ok := response["id"].(string); ok && v != "" {
				id = normalizeChatCompletionID(v)
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
					usage.GeneratedText = best.GeneratedText
					best = usage
				}
			}
		}
		if v, ok := raw["response_id"].(string); ok && v != "" {
			id = normalizeChatCompletionID(v)
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
				usage.GeneratedText = best.GeneratedText
				best = usage
			}
		}
		typ, _ := raw["type"].(string)
		if typ == "" {
			typ = event
		}
		if typ == "response.output_text.delta" {
			if err := ensureRole(); err != nil {
				return err
			}
			delta, _ := raw["delta"].(string)
			if delta != "" {
				best.GeneratedText += delta
				return writeChatStreamChunk(w, id, model, created, map[string]any{"content": delta}, nil)
			}
		}
		if typ == "response.reasoning_summary_text.delta" {
			if err := ensureRole(); err != nil {
				return err
			}
			if delta := stringValue(raw["delta"]); delta != "" {
				return writeChatStreamChunk(w, id, model, created, map[string]any{"reasoning_content": delta}, nil)
			}
		}
		if typ == "response.output_item.added" {
			item, _ := raw["item"].(map[string]any)
			itemType := strings.ToLower(strings.TrimSpace(stringValue(item["type"])))
			if itemType == "function_call" || itemType == "custom_tool_call" {
				if err := ensureRole(); err != nil {
					return err
				}
				outputIndex := int(intField(raw, "output_index"))
				index := chatToolIndex(outputIndex)
				callID := stringValue(firstNonNil(item["call_id"], item["id"]))
				name := stringValue(item["name"])
				toolCallsSeen = true
				return writeChatStreamChunk(w, id, model, created, map[string]any{"tool_calls": []any{map[string]any{
					"index": index, "id": callID, "type": "function",
					"function": map[string]any{"name": name, "arguments": ""},
				}}}, nil)
			}
		}
		if typ == "response.function_call_arguments.delta" || typ == "response.custom_tool_call_input.delta" {
			if err := ensureRole(); err != nil {
				return err
			}
			outputIndex := int(intField(raw, "output_index"))
			index := chatToolIndex(outputIndex)
			toolCallsSeen = true
			return writeChatStreamChunk(w, id, model, created, map[string]any{"tool_calls": []any{map[string]any{
				"index": index, "function": map[string]any{"arguments": stringValue(raw["delta"])},
			}}}, nil)
		}
		if typ == "response.completed" {
			if err := ensureRole(); err != nil {
				return err
			}
			finishReason := "stop"
			if toolCallsSeen {
				finishReason = "tool_calls"
			}
			if err := writeChatStreamFinalChunk(w, id, model, created, finishReason, best); err != nil {
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
		if err := ensureRole(); err != nil {
			return best, err
		}
		finishReason := "stop"
		if toolCallsSeen {
			finishReason = "tool_calls"
		}
		if err := writeChatStreamFinalChunk(w, id, model, created, finishReason, best); err != nil {
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
	var interceptBuf strings.Builder
	err := readSSEEvents(buffered, reader, func(event, data string) error {
		if data == "" {
			return nil
		}
		if data == "[DONE]" {
			return errResponsesStreamTerminal
		}
		if failed, message := sanitizedStreamEventFailure(reader, sseEvent{Event: event, Data: data}); failed {
			if !started {
				if err := writeClaudeStart(w, id, model); err != nil {
					return err
				}
				started = true
			}
			if err := writeClaudeEvent(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": 0}); err != nil {
				return err
			}
			if err := writeClaudeEvent(w, "message_stop", map[string]any{"type": "message_stop"}); err != nil {
				return err
			}
			stopped = true
			best.SoftFailure = message
			best.Status = "failed"
			return errResponsesStreamTerminal
		}
		// 正式转发阶段的内容拦截（Claude 格式）：上游先吐正常文本、再插入软失败话术时
		// preflight 抓不到。逐段累积可见文本，命中拦截词就以 Claude 终态收尾并中断，
		// 标记 SoftFailure 让上层把该渠道打入冷却。
		if reader != nil && reader.interceptContent != nil {
			if text := streamEventInterceptableText(sseEvent{Event: event, Data: data}); text != "" {
				interceptBuf.WriteString(text)
				if reader.interceptContent(interceptBuf.String()) {
					if !started {
						if err := writeClaudeStart(w, id, model); err != nil {
							return err
						}
						started = true
					}
					if err := writeClaudeEvent(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": 0}); err != nil {
						return err
					}
					if err := writeClaudeEvent(w, "message_stop", map[string]any{"type": "message_stop"}); err != nil {
						return err
					}
					stopped = true
					best.SoftFailure = "response content intercepted"
					best.Status = "failed"
					return errResponsesStreamTerminal
				}
			}
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
					usage.GeneratedText = best.GeneratedText
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
				best.GeneratedText += delta
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

func preflightHealthSSEStream(reader *sseStreamReader, closer io.Closer, timeout time.Duration) ([]sseEvent, error) {
	buffered := make([]sseEvent, 0, 4)
	totalBytes := 0
	if timeout <= 0 {
		timeout = healthProbeTimeout
	}
	deadline := time.Now().Add(timeout)
	for len(buffered) < streamPreflightMaxEvents && totalBytes < streamPreflightMaxBytes {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			_ = closer.Close()
			return nil, fmt.Errorf("upstream stream did not send a health generation event within %s", timeout)
		}
		ev, err := readNextSSEWithTimeout(reader, closer, remaining, "health generation event")
		if errors.Is(err, io.EOF) {
			if len(buffered) == 0 {
				return nil, errors.New("upstream stream ended before sending health probe data")
			}
			if healthBufferedHasGeneration(buffered) {
				return buffered, nil
			}
			return buffered, errors.New("upstream stream ended before generating health probe output")
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
		if healthStreamEventReady(ev) {
			return buffered, nil
		}
		if strings.TrimSpace(ev.Data) == "[DONE]" {
			if healthBufferedHasGeneration(buffered) {
				return buffered, nil
			}
			return buffered, errors.New("upstream stream ended before generating health probe output")
		}
	}
	if len(buffered) == 0 {
		return nil, errors.New("upstream stream did not send a usable health probe event")
	}
	if healthBufferedHasGeneration(buffered) {
		return buffered, nil
	}
	return buffered, errors.New("upstream stream did not send a generation health probe event")
}

func healthBufferedHasGeneration(events []sseEvent) bool {
	for _, ev := range events {
		if healthStreamEventReady(ev) {
			return true
		}
	}
	return false
}

func healthStreamEventReady(ev sseEvent) bool {
	data := strings.TrimSpace(ev.Data)
	if data == "" || data == "[DONE]" {
		return false
	}
	return streamEventHasVisibleText(ev)
}

func preflightSSEStream(reader *sseStreamReader, closer io.Closer, timeout time.Duration, hold func([]sseEvent) bool) ([]sseEvent, error) {
	buffered := make([]sseEvent, 0, 4)
	totalBytes := 0
	if timeout <= 0 {
		timeout = streamPreflightTimeout
	}
	deadline := time.Now().Add(timeout)
	for len(buffered) < streamPreflightMaxEvents && totalBytes < streamPreflightMaxBytes {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			_ = closer.Close()
			return nil, fmt.Errorf("upstream stream did not send a usable generation event within %s", timeout)
		}
		ev, err := readNextSSEWithTimeout(reader, closer, remaining, "a usable generation event")
		if errors.Is(err, io.EOF) {
			if len(buffered) == 0 {
				return nil, errors.New("upstream stream ended before sending data")
			}
			// A valid completed event has already returned above. Reaching EOF
			// here therefore means the upstream only emitted lifecycle noise and
			// never produced a visible answer. Do not leak those events to the
			// client: the caller can still retry another direct upstream safely.
			return nil, errors.New("upstream stream ended before sending a usable generation event")
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
		if failed, msg := streamEventPreflightFailure(ev); failed {
			return nil, errors.New(msg)
		}
		buffered = append(buffered, ev)
		totalBytes += len(ev.Event) + len(ev.Data)
		if streamBufferedHasCodexOutput(buffered) {
			if hold != nil && hold(buffered) {
				continue
			}
			return buffered, nil
		}
		if terminal, msg := streamEventTerminalWithoutOutput(ev); terminal {
			return nil, errors.New(msg)
		}
	}
	if len(buffered) == 0 {
		return nil, errors.New("upstream stream did not send a usable event")
	}
	// Lifecycle-only traffic (for example response.created followed by
	// response.in_progress) has not produced anything visible to the caller.
	// Treat it as an unsuccessful preflight rather than pinning the request to
	// a flaky upstream: Proxy can still try the next healthy key without
	// duplicating text, reasoning, or tool calls.
	return nil, errors.New("upstream stream did not send a usable generation event")
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
	return streamEventHasCodexOutput(ev)
}

func streamBufferedHasCodexOutput(events []sseEvent) bool {
	for _, ev := range events {
		if streamEventHasCodexOutput(ev) {
			return true
		}
	}
	return false
}

func streamEventHasCodexOutput(ev sseEvent) bool {
	data := strings.TrimSpace(ev.Data)
	if data == "[DONE]" {
		return false
	}
	if streamEventHasVisibleText(ev) {
		return true
	}
	if data == "" {
		return false
	}
	var raw map[string]any
	if json.Unmarshal([]byte(data), &raw) != nil {
		return false
	}
	if chatChunkHasToolCallDelta(raw) {
		return true
	}
	if responseEventHasToolOutput(raw) {
		return true
	}
	return false
}

func streamEventHasVisibleText(ev sseEvent) bool {
	data := strings.TrimSpace(ev.Data)
	if data == "" || data == "[DONE]" {
		return false
	}
	if looksLikeHealthGenerationSuccess([]byte(data)) {
		return true
	}
	var raw map[string]any
	if json.Unmarshal([]byte(data), &raw) != nil {
		return false
	}
	return strings.TrimSpace(chatChunkDeltaText(raw)) != "" ||
		strings.TrimSpace(responseVisibleText(raw)) != "" ||
		strings.TrimSpace(anthropicEventText(raw)) != ""
}

func responseVisibleText(raw map[string]any) string {
	if raw == nil {
		return ""
	}
	var b strings.Builder
	if text := responseText(raw); strings.TrimSpace(text) != "" {
		b.WriteString(text)
	}
	typ := strings.ToLower(strings.TrimSpace(stringValue(raw["type"])))
	switch typ {
	case "response.output_text.delta":
		b.WriteString(stringValue(raw["delta"]))
	case "response.output_text.done":
		b.WriteString(stringValue(raw["text"]))
	case "response.content_part.done":
		if part, _ := raw["part"].(map[string]any); part != nil {
			b.WriteString(stringValue(part["text"]))
		}
	case "response.output_item.done":
		if item, _ := raw["item"].(map[string]any); item != nil {
			b.WriteString(responseOutputItemText(item))
		}
	}
	if part, _ := raw["part"].(map[string]any); part != nil {
		if text := stringValue(part["text"]); text != "" {
			b.WriteString(text)
		}
	}
	if item, _ := raw["item"].(map[string]any); item != nil {
		if text := responseOutputItemText(item); strings.TrimSpace(text) != "" {
			b.WriteString(text)
		}
	}
	if response, _ := raw["response"].(map[string]any); response != nil {
		if text := responseText(response); strings.TrimSpace(text) != "" {
			b.WriteString(text)
		}
	}
	return b.String()
}

func anthropicEventText(raw map[string]any) string {
	if raw == nil {
		return ""
	}
	var b strings.Builder
	if delta, _ := raw["delta"].(map[string]any); delta != nil {
		b.WriteString(stringValue(delta["text"]))
	}
	if block, _ := raw["content_block"].(map[string]any); block != nil {
		b.WriteString(stringValue(block["text"]))
	}
	b.WriteString(anthropicContentText(raw["content"]))
	if message, _ := raw["message"].(map[string]any); message != nil {
		b.WriteString(anthropicContentText(message["content"]))
	}
	return b.String()
}

func anthropicContentText(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case []any:
		var b strings.Builder
		for _, item := range v {
			b.WriteString(anthropicContentText(item))
		}
		return b.String()
	case map[string]any:
		if text := stringValue(v["text"]); text != "" {
			return text
		}
		return anthropicContentText(v["content"])
	default:
		return ""
	}
}

func chatChunkHasToolCallDelta(raw map[string]any) bool {
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
		if calls, ok := delta["tool_calls"].([]any); ok {
			for _, item := range calls {
				call, _ := item.(map[string]any)
				if chatToolCallDeltaHasContent(call) {
					return true
				}
			}
		}
		if call, ok := delta["function_call"].(map[string]any); ok && chatToolCallDeltaHasContent(call) {
			return true
		}
	}
	return false
}

func chatToolCallDeltaHasContent(call map[string]any) bool {
	if call == nil {
		return false
	}
	for _, field := range []string{"id", "type", "name", "arguments"} {
		if stringValue(call[field]) != "" {
			return true
		}
	}
	if fn, _ := call["function"].(map[string]any); fn != nil {
		return stringValue(fn["name"]) != "" || stringValue(fn["arguments"]) != ""
	}
	return len(call) > 0
}

func responseEventHasToolOutput(raw map[string]any) bool {
	if raw == nil {
		return false
	}
	typ := strings.ToLower(strings.TrimSpace(stringValue(raw["type"])))
	switch typ {
	case "response.function_call_arguments.delta", "response.function_call_arguments.done",
		"response.custom_tool_call_input.delta", "response.custom_tool_call_input.done":
		return true
	}
	if responseToolOutputItem(raw) {
		return true
	}
	if item, _ := raw["item"].(map[string]any); item != nil && responseToolOutputItem(item) {
		return true
	}
	if response, _ := raw["response"].(map[string]any); response != nil && responseEventHasToolOutput(response) {
		return true
	}
	if output, _ := raw["output"].([]any); len(output) > 0 {
		for _, value := range output {
			item, _ := value.(map[string]any)
			if responseToolOutputItem(item) {
				return true
			}
		}
	}
	return false
}

func responseToolOutputItem(item map[string]any) bool {
	if item == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(stringValue(item["type"]))) {
	case "function_call", "custom_tool_call":
		return true
	default:
		return false
	}
}

func streamBufferedHasTerminal(events []sseEvent) bool {
	for _, ev := range events {
		if streamEventIsTerminal(ev) {
			return true
		}
	}
	return false
}

func streamEventTerminalWithoutOutput(ev sseEvent) (bool, string) {
	if !streamEventIsTerminal(ev) {
		return false, ""
	}
	return true, "upstream stream completed before generating usable output"
}

func streamEventIsTerminal(ev sseEvent) bool {
	if strings.TrimSpace(ev.Data) == "[DONE]" {
		return true
	}
	typ := strings.ToLower(strings.TrimSpace(sseEventType(ev)))
	switch typ {
	case "response.completed", "response.done", "response.failed", "response.cancelled", "response.incomplete", "message_stop":
		return true
	}
	if sseEventLooksLikeChatCompletion(ev) {
		var raw map[string]any
		return json.Unmarshal([]byte(strings.TrimSpace(ev.Data)), &raw) == nil && chatChunkHasFinish(raw)
	}
	return false
}

// streamEventPreflightFailure marks terminal upstream events that arrive
// before any visible output. They are safe to fail over from because nothing
// has been written to the downstream ResponseWriter yet.
func streamEventPreflightFailure(ev sseEvent) (bool, string) {
	if strings.TrimSpace(ev.Data) == "[DONE]" {
		return true, "upstream stream ended before response.completed"
	}
	switch strings.ToLower(strings.TrimSpace(sseEventType(ev))) {
	case "response.cancelled":
		return true, "upstream cancelled the response before output"
	case "response.incomplete":
		return true, "upstream returned an incomplete response before output"
	}
	return false, ""
}

func chatCompletionChunkHasGenerationOrTerminal(data string) bool {
	var raw map[string]any
	if err := json.Unmarshal([]byte(data), &raw); err != nil {
		return false
	}
	choices, _ := raw["choices"].([]any)
	for _, item := range choices {
		choice, _ := item.(map[string]any)
		if choice == nil {
			continue
		}
		if finish := strings.TrimSpace(stringValue(choice["finish_reason"])); finish != "" && !strings.EqualFold(finish, "null") {
			return true
		}
		delta, _ := choice["delta"].(map[string]any)
		if delta == nil {
			continue
		}
		for _, field := range []string{"content", "reasoning", "reasoning_content", "analysis"} {
			if strings.TrimSpace(stringValue(delta[field])) != "" {
				return true
			}
		}
		if calls, ok := delta["tool_calls"].([]any); ok && len(calls) > 0 {
			return true
		}
		if call, ok := delta["function_call"].(map[string]any); ok && len(call) > 0 {
			return true
		}
	}
	return false
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

func sanitizedStreamEventFailure(reader *sseStreamReader, ev sseEvent) (bool, string) {
	failed, message := streamEventFailure(ev)
	if !failed || reader == nil || reader.sanitizeFailure == nil {
		return failed, message
	}
	if sanitized, intercepted := reader.sanitizeFailure(message); intercepted {
		return true, sanitized
	}
	return true, message
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
		var textBuf strings.Builder
		var interceptBuf strings.Builder
		err := readSSEEvents(buffered, reader, func(event, data string) error {
			if failed, message := sanitizedStreamEventFailure(reader, sseEvent{Event: event, Data: data}); failed {
				payload := mustJSON(map[string]any{"error": map[string]any{"message": message, "type": "upstream_error"}})
				if err := writeSSEEvent(w, sseEvent{Event: "error", Data: payload}); err != nil {
					return err
				}
				best.SoftFailure = message
				best.Status = "failed"
				return errResponsesStreamTerminal
			}
			// 正式转发阶段的内容拦截：上游可能先吐正常文本、再把"公益 token 休息了 / 当前
			// 没有可用上游"之类的软失败话术塞进后续 delta（grok 尤甚），preflight 只看首字节
			// 前的内容抓不到。这里逐段累积可见文本，命中拦截词就立即中断透传、标记 SoftFailure，
			// 让上层把该渠道打入冷却，避免把错误话术继续喂给客户端。
			if reader != nil && reader.interceptContent != nil {
				if text := streamEventInterceptableText(sseEvent{Event: event, Data: data}); text != "" {
					interceptBuf.WriteString(text)
					if reader.interceptContent(interceptBuf.String()) {
						message := "upstream stream interrupted"
						payload := mustJSON(map[string]any{"error": map[string]any{"message": message, "type": "upstream_error"}})
						if err := writeSSEEvent(w, sseEvent{Event: "error", Data: payload}); err != nil {
							return err
						}
						best.SoftFailure = message
						best.Status = "failed"
						return errResponsesStreamTerminal
					}
				}
			}
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
				usage.GeneratedText = best.GeneratedText
				best = usage
			}
			var raw map[string]any
			if json.Unmarshal([]byte(data), &raw) == nil {
				if delta := chatChunkDeltaText(raw); delta != "" {
					textBuf.WriteString(delta)
					best.GeneratedText = textBuf.String()
				}
			}
			return writeSSEEvent(w, sseEvent{Event: event, Data: data})
		})
		if errors.Is(err, errResponsesStreamTerminal) {
			return best, nil
		}
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
	var interceptBuf strings.Builder

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
			usage.GeneratedText = best.GeneratedText
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
		if failed, msg := sanitizedStreamEventFailure(reader, ev); failed {
			failedSeen = true
			if err := writeResponsesStreamFailure(w, respID, model, "upstream_error", msg); err != nil {
				return err
			}
			best.SoftFailure = msg
			best.Status = "failed"
			doneSent = true
			return errResponsesStreamTerminal
		}
		// 正式转发阶段的内容拦截：上游先吐正常文本、再把软失败话术塞进后续 delta 时，
		// preflight 抓不到。逐段累积可见文本，命中拦截词就写 response.failed 终态并中断，
		// 标记 SoftFailure 让上层把该渠道打入冷却。
		if reader != nil && reader.interceptContent != nil {
			if text := streamEventInterceptableText(ev); text != "" {
				interceptBuf.WriteString(text)
				if reader.interceptContent(interceptBuf.String()) {
					message := "response content intercepted"
					failedSeen = true
					if err := writeResponsesStreamFailure(w, respID, model, "upstream_error", "upstream stream interrupted"); err != nil {
						return err
					}
					best.SoftFailure = message
					best.Status = "failed"
					doneSent = true
					return errResponsesStreamTerminal
				}
			}
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
				best.GeneratedText = textBuf.String()
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

func writeChatStreamFinalChunk(w http.ResponseWriter, id, model string, created int64, finish string, usage usageTokens) error {
	chunk := map[string]any{
		"id": normalizeChatCompletionID(id), "object": "chat.completion.chunk", "created": created, "model": model,
		"choices": []map[string]any{{"index": 0, "delta": map[string]any{}, "finish_reason": finish}},
	}
	if usage.Total > 0 || usage.Prompt > 0 || usage.Completion > 0 {
		chunk["usage"] = map[string]int64{
			"prompt_tokens": usage.Prompt, "completion_tokens": usage.Completion, "total_tokens": usage.Total,
		}
	}
	data, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	return writeSSEData(w, string(data))
}

func normalizeChatCompletionID(id string) string {
	id = strings.TrimSpace(id)
	if strings.HasPrefix(id, "resp_") {
		return "chatcmpl_" + strings.TrimPrefix(id, "resp_")
	}
	if id == "" {
		return "chatcmpl-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return id
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
				usage.GeneratedText = best.GeneratedText
				best = usage
			}
		}
		if responseRaw, ok := raw["response"].(map[string]any); ok {
			if usageRaw, ok := responseRaw["usage"].(map[string]any); ok {
				if usage := usageFromMap(usageRaw); usage.Total > 0 {
					usage.ResponseID = firstNonEmpty(usage.ResponseID, best.ResponseID)
					usage.Model = firstNonEmpty(usage.Model, best.Model)
					usage.GeneratedText = best.GeneratedText
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

// gatewayLimitOrExpiredMessage is used by the direct Responses gateway.  It is
// intentionally not limited to public keys: any automatically disabled local
// key must produce an actionable terminal stream instead of looking invalid.
func (s *Service) gatewayLimitOrExpiredMessage(rawKey string, key *storage.GatewayKey, cause error) (string, bool) {
	if key == nil && s != nil && s.gateway != nil {
		rawKey = strings.TrimSpace(rawKey)
		if rawKey != "" {
			found, err := s.gateway.FindByHash(HashKey(rawKey))
			if err != nil {
				if s.log != nil {
					s.log.Warn("lookup gateway key for friendly stream failed", "err", err)
				}
				return "", false
			}
			key = found
		}
	}
	if key == nil {
		return "", false
	}
	if !key.Enabled && strings.TrimSpace(key.DisabledMessage) != "" {
		return strings.TrimSpace(key.DisabledMessage), true
	}
	now := time.Now()
	lower := ""
	if cause != nil {
		lower = strings.ToLower(cause.Error())
	}
	if publicGatewayKeyExpired(key, now) || strings.Contains(lower, "expired") {
		return publicGatewayExpiredMessage, true
	}
	if publicGatewayKeyQuotaExhausted(key, now) || gatewayQuotaError(cause) {
		return gatewayQuotaExhaustedMessage, true
	}
	return "", false
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
		MaxGroupRatio:      key.MaxGroupRatio,
		RoutePreference:    normalizeGatewayRoutePreference(key.RoutePreference),
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
		DisabledMessage:    key.DisabledMessage,
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
	scoped := candidates
	if scope == gatewayGroupScopeAll {
		return filterCandidatesForGatewayKeyRatio(key, scoped)
	}
	scoped = make([]storage.UpstreamGroupKey, 0, len(candidates))
	allowed := decodeUintSet(key.AllowedGroupIDs)
	for _, candidate := range candidates {
		switch scope {
		case gatewayGroupScopeSelected:
			if allowed[candidate.ID] {
				scoped = append(scoped, candidate)
			}
		case gatewayGroupScopeCharity:
			if candidate.Charity {
				scoped = append(scoped, candidate)
			}
		case gatewayGroupScopeNormal:
			if !candidate.Charity {
				scoped = append(scoped, candidate)
			}
		}
	}
	return filterCandidatesForGatewayKeyRatio(key, scoped)
}

func filterCandidatesForGatewayKeyRatio(key *storage.GatewayKey, candidates []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	if key == nil || key.MaxGroupRatio <= 0 {
		return candidates
	}
	out := make([]storage.UpstreamGroupKey, 0, len(candidates))
	maxRatio := key.MaxGroupRatio + 1e-9
	for _, candidate := range candidates {
		// Public/charity channels are a separate first-tier source.  A maximum
		// paid-channel ratio must not accidentally filter them out, otherwise a
		// gateway key configured as "all" silently spends on paid routes first.
		if candidate.Charity || effectiveGroupRatio(candidate) <= maxRatio {
			out = append(out, candidate)
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

func gatewayKeyScopeEmptyMessage(key *storage.GatewayKey) string {
	if key == nil {
		return "no alive upstream group keys available"
	}
	if key.MaxGroupRatio > 0 {
		return fmt.Sprintf("no upstream group keys at or below %.2f ratio available for this gateway key", key.MaxGroupRatio)
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
	case "messages", "message":
		return "messages"
	case "chat", "chat_completions", "chat-completions", "completions":
		return "chat"
	default:
		return "responses"
	}
}

// defaultRequestModeForClientFormat returns a safe protocol before an automatic
// capability probe completes. It deliberately returns a real forwarding mode:
// "auto" is configuration metadata, never a protocol sent to an upstream.
func defaultRequestModeForClientFormat(format string) string {
	switch normalizeClientFormat(format) {
	case "claude":
		return "messages"
	case "grok":
		return "chat"
	default:
		return "responses"
	}
}

// requestModeConfigForClientFormat validates a protocol override against the
// selected channel format. Supplying "auto" (or omitting the value) restores
// automatic detection while retaining a usable default protocol if the probe
// cannot run or the upstream rejects the probe request.
func requestModeConfigForClientFormat(format, requested string) (mode, source string, err error) {
	requested = strings.ToLower(strings.TrimSpace(requested))
	if requested == "" || requested == "auto" {
		return defaultRequestModeForClientFormat(format), "auto", nil
	}

	mode = normalizeUpstreamRequestMode(requested)
	switch normalizeClientFormat(format) {
	case "openai":
		if mode == "responses" || mode == "chat" {
			return mode, "manual", nil
		}
		return "", "", errors.New("OpenAI channels only support Responses or Chat Completions")
	case "claude":
		if mode == "messages" {
			return mode, "manual", nil
		}
		return "", "", errors.New("Claude channels only support Claude Messages")
	case "grok":
		if mode == "chat" {
			return mode, "manual", nil
		}
		return "", "", errors.New("Grok channels only support Chat Completions")
	default:
		return "", "", errors.New("unsupported channel format for request mode")
	}
}

func defaultAuthModeForClientFormat(format string) string {
	if normalizeClientFormat(format) == "claude" {
		return "x_api_key"
	}
	return "bearer"
}

func normalizeUpstreamAuthMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "x-api-key", "x_api_key", "xapikey", "api_key", "apikey":
		return "x_api_key"
	default:
		return "bearer"
	}
}

func upstreamAuthModeForKey(key *storage.UpstreamGroupKey) string {
	if key == nil || strings.TrimSpace(key.AuthMode) == "" {
		if key != nil {
			return defaultAuthModeForClientFormat(key.ClientFormat)
		}
		return "bearer"
	}
	return normalizeUpstreamAuthMode(key.AuthMode)
}

func upstreamAuthModesForProbe(key *storage.UpstreamGroupKey) []string {
	preferred := upstreamAuthModeForKey(key)
	other := "bearer"
	if preferred == "bearer" {
		other = "x_api_key"
	}
	return []string{preferred, other}
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
	candidates := clientIPCandidates(r)
	if len(candidates) > 0 {
		return candidates[0]
	}
	return ""
}

func clientIPCandidates(r *http.Request) []string {
	if r == nil {
		return nil
	}
	seen := map[string]struct{}{}
	result := make([]string, 0, 4)
	appendIP := func(raw string) {
		ip := net.ParseIP(strings.TrimSpace(raw))
		if ip == nil {
			return
		}
		value := ip.String()
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	for _, raw := range strings.Split(r.Header.Get("X-Forwarded-For"), ",") {
		appendIP(raw)
	}
	appendIP(r.Header.Get("X-Real-IP"))
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil {
		appendIP(host)
	} else {
		appendIP(r.RemoteAddr)
	}
	return result
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

// normalizeUpstreamAPIKey converts a commonly pasted request-header value
// back to its raw credential. Manual-channel forms accept both a plain key
// and snippets copied from documentation or clients, for example
// "Authorization: Bearer sk-..." and "X-Api-Key: sk-...".
func normalizeUpstreamAPIKey(value string) string {
	value = strings.TrimSpace(value)
	for {
		trimmed := strings.TrimSpace(value)
		if len(trimmed) >= 2 && ((trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"') || (trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'')) {
			value = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
			continue
		}
		lower := strings.ToLower(trimmed)
		switch {
		case strings.HasPrefix(lower, "authorization:"):
			value = strings.TrimSpace(trimmed[len("authorization:"):])
		case strings.HasPrefix(lower, "bearer "):
			value = strings.TrimSpace(trimmed[len("bearer "):])
		case strings.HasPrefix(lower, "x-api-key:"):
			value = strings.TrimSpace(trimmed[len("x-api-key:"):])
		case strings.HasPrefix(lower, "x-api-key "):
			value = strings.TrimSpace(trimmed[len("x-api-key "):])
		default:
			return trimmed
		}
	}
}

// sanitizeManualSecret remains as a compatibility alias for callers and old
// tests. All new code should use normalizeUpstreamAPIKey directly.
func sanitizeManualSecret(value string) string {
	return normalizeUpstreamAPIKey(value)
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
	dst.Set("X-Gateway-Channel", key.ChannelName)
	dst.Set("X-Gateway-Group", key.GroupName)
	dst.Set("X-Gateway-Ratio", strconv.FormatFloat(effectiveGroupRatio(*key), 'f', -1, 64))
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
	case "authorization", "x-api-key", "cookie", "set-cookie", "openai-organization", "openai-project",
		"chatgpt-account-id", "x-account-id", "x-user-id", "x-auth-token",
		"x-forwarded-for", "x-real-ip", "forwarded", "cf-connecting-ip", "true-client-ip",
		"host", "content-length", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
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
	if looksLikeResponsesToolCallStateError(msg) {
		return true
	}
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
	if looksLikeResponsesToolCallStateError(msg) {
		return false
	}
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
		"missing messages",
		"missing input",
		"unsupported parameter",
		"unsupported field",
		"unknown parameter",
		"unknown field",
		"unrecognized parameter",
		"unrecognized field",
		"unexpected parameter",
		"unexpected field",
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
			strings.Contains(s, "model is not available") ||
			strings.Contains(s, "model unavailable") ||
			strings.Contains(s, "model not available") ||
			strings.Contains(s, "model not in") ||
			strings.Contains(s, "model is disabled") ||
			strings.Contains(s, "model disabled") ||
			strings.Contains(s, "model access") ||
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

// isDefinitiveUnsupportedModelFailure is intentionally stricter than
// looksLikeUnsupportedModelError. The latter is useful for an inconclusive
// health probe, but live routing must not cache a transient 5xx/router error
// as a permanent model capability miss.
func isDefinitiveUnsupportedModelFailure(msg string) bool {
	lower := strings.ToLower(strings.TrimSpace(msg))
	if lower == "" || looksLikeUpstreamRoutingUnavailable(lower) ||
		strings.Contains(lower, "http 5") || strings.Contains(lower, "status 5") ||
		strings.Contains(lower, "http 429") || strings.Contains(lower, "status 429") ||
		strings.Contains(lower, "timeout") || strings.Contains(lower, "connection ") ||
		strings.Contains(lower, "network") {
		return false
	}
	if !looksLikeUnsupportedModelError(lower) {
		return false
	}
	// Generic “model unavailable” is commonly emitted during a temporary
	// provider-router outage. Cache only explicit model capability/access
	// rejections from a client-error response.
	for _, marker := range []string{
		"unsupported", "not support", "model_not_found", "model_not_supported",
		"no such model", "unknown model", "invalid model", "does not exist",
		"not enabled", "model access", "model_not_available", "not permitted",
		"do not have access", "don't have access", "not have access",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
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

// effectiveGroupRatio is the single ratio source used by display, filtering,
// scheduling and billing. Existing rows have zero for the newly-added scale
// column, which intentionally means the backwards-compatible 100%.
func effectiveGroupRatio(group storage.UpstreamGroupKey) float64 {
	percent := group.RatioScalePercent
	if percent <= 0 {
		percent = 100
	}
	return normalizedRatio(group.Ratio) * percent / 100
}
