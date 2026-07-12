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

	proxyAttemptTimeout       = 60 * time.Second
	healthProbeTimeout        = 8 * time.Second
	healthProbeRetryJitterMax = 15 * time.Second
	// streamFirstEventTimeout 是"等上游吐出第一个 SSE 事件"的最长等待。
	// 这里必须给足：Codex / o1 / o3 这类带 reasoning 的请求，首个可见事件
	// （first token）常常要十几秒甚至更久才出来。之前设 8s 会在慢模型刚开始
	// 思考时就把上游连接 Close 掉，客户端随即报
	// "stream closed before response.completed"。放宽到 90s 覆盖绝大多数推理场景；
	// 真卡死的连接由 preflight 的字节/事件上限和上游自身超时兜底。
	streamFirstEventTimeout = 90 * time.Second
	// streamIdleTimeout 是正式转发阶段"两个事件之间"的最长间隔。给得比首事件更宽，
	// 因为推理模型 token 之间可能有较长停顿；超过则判上游卡死，主动断开让客户端收到错误。
	streamIdleTimeout        = 120 * time.Second
	streamPreflightMaxEvents = 16
	streamPreflightMaxBytes  = 64 << 10
	// proxyFailureCooldown 是"请求失败后该候选临时不可调度"的固定时长。
	proxyFailureCooldown   = 5 * time.Minute
	healthProbeConcurrency = 8
)

type Service struct {
	channels   *storage.Channels
	gateway    *storage.GatewayKeys
	affinities *storage.GatewayAffinities
	groupKeys  *storage.UpstreamGroupKeys
	usageLogs  *storage.UsageLogs
	cipher     *appcrypto.Cipher
	channelSvc *channel.Service
	log        *slog.Logger
	clients    sync.Map
	runtime    sync.Map
	configMu   sync.RWMutex
	upstream   config.UpstreamConfig
}

type CreateGatewayKeyInput struct {
	Name            string `json:"name"`
	ClientFormat    string `json:"client_format"`
	AllowedGroupIDs []uint `json:"allowed_group_ids"`
	DailyLimit      int64  `json:"daily_limit"`
	TotalLimit      int64  `json:"total_limit"`
	ExpiresInDays   int    `json:"expires_in_days"`
}

type UpdateGatewayKeyInput struct {
	Name            *string    `json:"name"`
	Enabled         *bool      `json:"enabled"`
	ClientFormat    *string    `json:"client_format"`
	AllowedGroupIDs []uint     `json:"allowed_group_ids"`
	DailyLimit      *int64     `json:"daily_limit"`
	TotalLimit      *int64     `json:"total_limit"`
	ExpiresInDays   *int       `json:"expires_in_days"`
	ExpiresAt       *time.Time `json:"expires_at"`
}

type GatewayKeyOutput struct {
	ID              uint       `json:"id"`
	Name            string     `json:"name"`
	KeyPrefix       string     `json:"key_prefix"`
	Key             string     `json:"key,omitempty"`
	Enabled         bool       `json:"enabled"`
	ClientFormat    string     `json:"client_format"`
	AllowedGroupIDs []uint     `json:"allowed_group_ids,omitempty"`
	DailyLimit      int64      `json:"daily_limit"`
	TotalLimit      int64      `json:"total_limit"`
	TodayTokens     int64      `json:"today_tokens"`
	TotalTokens     int64      `json:"total_tokens"`
	UsageDate       string     `json:"usage_date,omitempty"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	LastUsedAt      *time.Time `json:"last_used_at,omitempty"`
	LastUsedIP      string     `json:"last_used_ip,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
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
	ResponseMode string
	Stream       bool
	AffinityKey  string
	AltPath      string
	AltBody      []byte
	AltMode      string
	AltStream    bool
}

type usageTokens struct {
	Prompt      int64
	Completion  int64
	Total       int64
	ResponseID  string
	SoftFailure string
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
	// closer/idleTimeout 可选：设置后，正式转发阶段每次读事件都带这个 idle 超时，
	// 避免上游中途卡住导致 reader.Next() 无限阻塞、客户端超时断流。
	closer      io.Closer
	idleTimeout time.Duration
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

// SetUsageLogs 注入使用记录仓库（可选）。为空时不记录，功能降级但不影响主流程。
func (s *Service) SetUsageLogs(logs *storage.UsageLogs) {
	s.usageLogs = logs
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

// ListUsageLogs 分页返回使用记录。
func (s *Service) ListUsageLogs(limit, offset int) ([]storage.UsageLog, int64, error) {
	if s.usageLogs == nil {
		return []storage.UsageLog{}, 0, nil
	}
	return s.usageLogs.List(limit, offset)
}

// recordUsageLog 在请求成功后异步写一条使用记录。失败只记 warn，绝不影响主请求。
func (s *Service) recordUsageLog(gatewayKey *storage.GatewayKey, candidate *storage.UpstreamGroupKey, model string, usage usageTokens) {
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
		Ratio:            candidate.Ratio,
	}
	if gatewayKey != nil {
		entry.GatewayKeyID = gatewayKey.ID
		entry.GatewayKeyName = gatewayKey.Name
	}
	if err := s.usageLogs.Add(entry); err != nil && s.log != nil {
		s.log.Warn("record usage log failed", "err", err)
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
		Name:            name,
		KeyPrefix:       visiblePrefix(key),
		KeyHash:         HashKey(key),
		KeyCipher:       ciphertext,
		Enabled:         true,
		ClientFormat:    normalizeClientFormat(input.ClientFormat),
		AllowedGroupIDs: encodeUintList(input.AllowedGroupIDs),
		DailyLimit:      maxInt64(0, input.DailyLimit),
		TotalLimit:      maxInt64(0, input.TotalLimit),
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
	}
	if input.DailyLimit != nil {
		key.DailyLimit = maxInt64(0, *input.DailyLimit)
	}
	if input.TotalLimit != nil {
		key.TotalLimit = maxInt64(0, *input.TotalLimit)
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
	rawKey := strings.TrimSpace(input.Key)
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
		siteURL := strings.TrimSpace(input.SiteURL)
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
		ChannelID:    ch.ID,
		ChannelName:  ch.Name,
		ChannelType:  ch.Type,
		ClientFormat: format,
		RequestMode:  mode,
		GroupRef:     groupRef,
		GroupName:    groupName,
		GroupDesc:    strings.TrimSpace(input.GroupDesc),
		Ratio:        ratio,
		Priority:     input.Priority,
		Charity:      input.Charity,
		Enabled:      true,
		KeyCipher:    cipher,
		Status:       "unknown",
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

func (s *Service) TestAllGroupKeys(_ context.Context) (*HealthResult, error) {
	list, err := s.groupKeys.List()
	if err != nil {
		return nil, err
	}

	// 关键：测活用独立的、够长的 context，绝不复用调用方（HTTP 请求）的 ctx。
	// 之前直接用 c.Request.Context()，渠道一多、串行探测耗时超过浏览器/网关的请求超时，
	// 整个 ctx 被取消，导致"正在测的 + 还没测的"全部 context canceled 被误判为死亡。
	// 全量测活应当同时发起，而不是按上游数量串行拉长。单个分组最坏为
	// 两次 8 秒探测加最多 15 秒抖动重试，给整批 45 秒的独立预算即可。
	probeCtx := context.Background()

	result := &HealthResult{Items: make([]HealthResultItem, len(list))}

	// 所有启用分组同时测活。每个请求都只有 1 token 的流式 "hi"，
	// 不会因为分组数量多而让后面的分组排队、继承前面的超时。
	var wg sync.WaitGroup
	jobs := make(chan int)
	workers := healthProbeConcurrency
	if workers > len(list) {
		workers = len(list)
	}
	if workers < 1 {
		workers = 1
	}
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				result.Items[idx] = s.testGroupKey(probeCtx, &list[idx])
			}
		}()
	}
	for i := range list {
		if !list[i].Enabled {
			result.Items[i] = HealthResultItem{
				ID:          list[i].ID,
				ChannelID:   list[i].ChannelID,
				ChannelName: list[i].ChannelName,
				GroupRef:    list[i].GroupRef,
				GroupName:   list[i].GroupName,
				Ratio:       list[i].Ratio,
				Status:      "disabled",
			}
			continue
		}
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	// 汇总计数（并发写入各自的 slot，这里统一统计，避免竞态）。
	for i := range result.Items {
		switch result.Items[i].Status {
		case "alive":
			result.Checked++
			result.Alive++
		case "dead":
			result.Checked++
			result.Dead++
		case "disabled":
			// 不计入 checked
		default:
			result.Checked++
		}
	}
	return result, nil
}

// TestGroupKey immediately tests one upstream group. It deliberately owns its
// context so the browser request ending cannot turn a real probe into a false
// "context canceled" death result.
func (s *Service) TestGroupKey(id uint) (*HealthResultItem, error) {
	key, err := s.groupKeys.FindByID(id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	result := s.testGroupKey(ctx, key)
	return &result, nil
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
	if err := validateClientFormat(gatewayKey.ClientFormat, normalized.ResponseMode); err != nil {
		return &GatewayError{Status: http.StatusBadRequest, Body: jsonError(err.Error())}
	}

	candidates, err := s.groupKeys.ListCandidates(time.Now())
	if err != nil {
		return err
	}
	candidates = filterCandidatesForGatewayKey(gatewayKey, candidates)
	candidates = filterCandidatesForClientFormat(gatewayKey.ClientFormat, normalized.ResponseMode, candidates)
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
	switch normalizeUpstreamRequestMode(candidate.RequestMode) {
	case "chat":
		return request.alt()
	default:
		return request
	}
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
		s.recordUsageLog(gatewayKey, candidate, modelFromRequestBody(normalized.Body), usage)
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
		return candOutcome{kind: candFatal, err: err, errMsg: errMsg, markFailure: false}
	}
	if fallback, reason, ok := fallbackRequestAfterFailure(normalized, errMsg); ok {
		start = time.Now()
		retry, usage, err = s.streamProxyCandidate(ctx, fallback, candidate, w)
		if err == nil {
			s.recordRuntimeSuccess(candidate.ID, time.Since(start))
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			s.recordUsageLog(gatewayKey, candidate, modelFromRequestBody(normalized.Body), usage)
			if usage.SoftFailure != "" {
				s.markProxyFailure(candidate.ID, usage.SoftFailure)
			}
			return candOutcome{kind: candSuccess}
		}
		errMsg = reason + " retry failed: " + err.Error()
		if !retry {
			return candOutcome{kind: candFatal, err: err, errMsg: errMsg, markFailure: true}
		}
	}
	if rectified, reason, ok := s.rectifyAfterFailure(normalized, errMsg); ok {
		start = time.Now()
		retry, usage, err = s.streamProxyCandidate(ctx, rectified, candidate, w)
		if err == nil {
			s.recordRuntimeSuccess(candidate.ID, time.Since(start))
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			s.recordUsageLog(gatewayKey, candidate, modelFromRequestBody(normalized.Body), usage)
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
		s.recordUsageLog(gatewayKey, candidate, modelFromRequestBody(normalized.Body), usage)
		writeProxyResponse(w, status, header, respBody, candidate, normalized.ResponseMode)
		return candOutcome{kind: candSuccess}
	}
	errMsg := err.Error()
	if fallback, reason, ok := fallbackRequestAfterFailure(normalized, errMsg); ok {
		start = time.Now()
		status, header, respBody, retry, err = s.tryProxyCandidate(ctx, fallback, candidate)
		if err == nil {
			s.recordRuntimeSuccess(candidate.ID, time.Since(start))
			usage := extractUsage(respBody)
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			s.recordUsageLog(gatewayKey, candidate, modelFromRequestBody(normalized.Body), usage)
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
			s.recordRuntimeSuccess(candidate.ID, time.Since(start))
			usage := extractUsage(respBody)
			_ = s.gateway.AddUsage(gatewayKey.ID, usage.Prompt, usage.Completion, usage.Total, time.Now())
			_ = s.groupKeys.MarkSuccessWithUsage(candidate.ID, usage.Prompt, usage.Completion, usage.Total)
			s.rememberAffinity(normalized, usage.ResponseID, candidate.ID)
			s.recordUsageLog(gatewayKey, candidate, modelFromRequestBody(normalized.Body), usage)
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
		// 正式转发阶段的 idle 读超时：上游连续 streamIdleTimeout 没有任何新事件就判定卡死，
		// 主动关连接返回错误，避免 reader.Next() 无限阻塞导致客户端 stream closed。
		reader.closer = resp.Body
		reader.idleTimeout = streamIdleTimeout
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
		case "responses_from_chat":
			// 降级路径：客户端发的是 responses，但这个上游只支持 chat，我们已把请求转成
			// chat/completions 发出去，此处再把上游的 chat SSE 流转回 responses 事件给客户端。
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
		ClientFormat:     inferGroupClientFormat(group.Name, group.Description),
		RequestMode:      "responses",
		GroupRef:         groupRef,
		GroupName:        strings.TrimSpace(group.Name),
		GroupDesc:        strings.TrimSpace(group.Description),
		Ratio:            normalizedRatio(group.Ratio),
		Enabled:          true,
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
		item.Error = fmt.Sprintf("first probe failed: %v; retry after %s failed: %v", firstErr, delay, healthProbeError(status, body, err))
	} else {
		// 首探已经明确失败，只是"抖动重试的等待"被 ctx 取消（批量扫描超时 / 关机）。
		// 关键取舍：绝不能停在 unknown 就 return —— 那样不落库，DB 会保留上一轮的 alive，
		// 造成"上游已经死了、面板还显示绿色"的僵尸绿（用户明确报过这个 bug）。
		// 首探失败本身就是可信的失败信号，直接落 dead；下一轮 cron 会用退避后重新复活探测，
		// 真的活着会在下轮转回 alive，代价只是一轮的延迟，远好过僵尸绿。
		item.Status = "dead"
		item.Error = fmt.Sprintf("first probe failed: %v; retry wait canceled: %v", firstErr, ctx.Err())
		s.markHealthFailure(key.ID, item.Error, item.LatencyMS)
		return item
	}
	item.Status = "dead"
	s.markHealthFailure(key.ID, item.Error, item.LatencyMS)
	return item
}

func (s *Service) healthProbeCandidate(ctx context.Context, key *storage.UpstreamGroupKey) (int, []byte, int64, error) {
	// Claude 类型渠道：走 Anthropic Messages 格式探测，绝不用 openai 的 /v1/models + /v1/responses，
	// 否则 claude 上游不认这些端点，测活必然失败（这正是"claude 渠道一测就死"的原因）。
	if normalizeClientFormat(key.ClientFormat) == "claude" {
		return s.healthProbeClaude(ctx, key)
	}
	start := time.Now()
	model, status, body, err := s.discoverHealthProbeModel(ctx, key)
	// /v1/models is only a model-selection hint. Some manual/API-key upstreams
	// deliberately block it while accepting Responses, so always perform the
	// real minimal Responses probe instead of marking the channel dead early.
	_ = status
	_ = body
	_ = err
	if strings.TrimSpace(model) == "" {
		model = defaultHealthProbeModel(key.ClientFormat)
	}
	req := healthGenerationProbeRequest(model)
	req = requestForCandidate(req, key)
	status, _, body, err = s.requestHealthProbeCandidate(ctx, req, key, healthProbeTimeout)
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
		AltMode:      "raw",
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
	transport.ResponseHeaderTimeout = proxyAttemptTimeout
	if proxyURL = strings.TrimSpace(proxyURL); proxyURL != "" {
		if parsed, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(parsed)
		}
	}
	return transport
}

func (s *Service) markProxyFailure(id uint, msg string) {
	// 请求失败即冷却：把该候选临时移出调度池，固定 5 分钟后自动恢复。
	// 用户可在面板手动「解除冷却」立即恢复。固定时长比递增退避更符合直觉，
	// 也避免某个渠道偶发抖动后被越锁越久。
	delay := proxyFailureCooldown
	until := time.Now().Add(delay)
	s.recordRuntimeFailure(id, until)
	if err := s.groupKeys.MarkFailure(id, msg, until); err != nil && s.log != nil {
		s.log.Warn("mark upstream group failed", "id", id, "err", err)
	}
}

func (s *Service) markHealthFailure(id uint, msg string, latencyMS int64) {
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
	if err := s.groupKeys.MarkHealthFailure(id, msg, until, latencyMS); err != nil && s.log != nil {
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
		req.AltPath = path
		req.AltBody = append([]byte(nil), body...)
		req.AltMode = "raw"
		req.AltStream = stream
	case responsesPath:
		req.ResponseMode = "responses"
		req.Stream = requestStream(body)
		// 关键：给原生 /v1/responses 请求也准备一个 chat/completions 回退体。
		// Codex 不走 ccswitch 直连网关时发的就是原生 responses，而不少中转站上游只支持
		// chat/completions。RequestMode=chat 的候选会用这个 alt 请求，避免上游不认 responses
		// 直接断流（客户端报 stream closed before response.completed）。
		if converted, altStream, err := responsesToChatRequestBody(body); err == nil {
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
		req.Path = responsesPath
		req.Body = converted
		req.ResponseMode = "claude"
		req.Stream = stream
		// Preserve the native request.  It is selected for Claude upstreams by
		// requestForCandidate; the normalized Responses representation remains
		// useful for the rest of the gateway pipeline.
		req.AltPath = path
		req.AltBody = append([]byte(nil), body...)
		req.AltMode = "raw"
		req.AltStream = stream
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
			"store", "reasoning", "text", "include", "parallel_tool_calls", "truncation":
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
	if mt, ok := raw["max_output_tokens"]; ok {
		out["max_tokens"] = mt
	}
	out["stream"] = stream
	encoded, err := json.Marshal(out)
	return encoded, stream, err
}

// responsesInputToChatMessages 把 Responses 的 input（可能是字符串或消息数组）转回 chat messages。
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
	// responses 模式且备有 chat 回退：只要错误像"上游不支持 responses 端点"，就自动降级到
	// chat/completions 再打一次同一个候选。放宽判定——很多中转站上游根本不认 /v1/responses，
	// 返回的是 gin 默认的 "404 page not found"（不含 responses 字样），不能强求错误里出现 responses。
	if request.ResponseMode == "responses" && looksLikeEndpointMissingError(errMsg) {
		return request.alt(), "upstream chat-completions compatibility", true
	}
	if !looksLikeResponsesEndpointError(errMsg) {
		return request, "", false
	}
	return request.alt(), "upstream chat-completions compatibility", true
}

// looksLikeEndpointMissingError 判断错误是否像"这个 HTTP 端点在上游根本不存在/不被支持"。
// 用于 responses→chat 的自动降级；不强求错误信息里出现具体端点名。
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
		return out
	}
	return ordered
}

// affinityIsHard 判断一个亲和 key 是否是"必须回原上游"的有状态亲和。
// 只有我们自己合成的 chat: 缓存种子是软亲和，其余（response / conversation / metadata）都是硬亲和。
func affinityIsHard(rawKey string) bool {
	return rawKey != "" && !strings.HasPrefix(rawKey, "chat:")
}

func (s *Service) orderCandidatesWithRuntime(candidates []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	out := orderCandidates(candidates)
	sort.SliceStable(out, func(i, j int) bool {
		if rankI, rankJ := statusRank(out[i].Status), statusRank(out[j].Status); rankI != rankJ {
			return rankI < rankJ
		}
		// 公益优先：同为可用状态时，公益渠道永远排在付费渠道前面。
		// 公益全部不可用（被降级到 dead/冷却）后，才会轮到付费渠道。
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

func affinityWouldPromoteCostlier(item, best storage.UpstreamGroupKey) bool {
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
	if id == "" {
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
		// 只有 "failed" 才是真正的上游错误。
		// "incomplete"（触顶 max_output_tokens）和 "cancelled"（客户端主动取消）都是
		// 正常终态，不该当作错误中断整条流——否则带 max_tokens 限制的正常请求会被误杀，
		// 客户端拿到断流后报 "stream closed before response.completed"。
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

// streamChatAsResponsesEvents 把上游的 chat.completion.chunk SSE 流转换成 Responses SSE 事件流，
// 用于 responses→chat 降级：客户端要 responses 格式，上游只会 chat，这里做流式桥接。
// 发出的事件序列：response.created → response.output_text.delta* → response.completed → [DONE]
func streamChatAsResponsesEvents(w http.ResponseWriter, buffered []sseEvent, reader *sseStreamReader) (usageTokens, error) {
	id := "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	model := ""
	createdSent := false
	var best usageTokens
	var textBuf strings.Builder

	emitCreated := func() error {
		if createdSent {
			return nil
		}
		createdSent = true
		payload, _ := json.Marshal(map[string]any{
			"type": "response.created",
			"response": map[string]any{
				"id": id, "object": "response", "status": "in_progress", "model": model,
			},
		})
		if err := writeSSEEvent(w, sseEvent{Event: "response.created", Data: string(payload)}); err != nil {
			return err
		}
		return nil
	}

	err := readSSEEvents(buffered, reader, func(event, data string) error {
		if data == "" || data == "[DONE]" {
			return nil
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(data), &raw); err != nil {
			return nil
		}
		if v, ok := raw["model"].(string); ok && v != "" {
			model = v
		}
		if v, ok := raw["id"].(string); ok && v != "" {
			id = v
			best.ResponseID = v
		}
		if usageRaw, ok := raw["usage"].(map[string]any); ok {
			if usage := usageFromMap(usageRaw); usage.Total > 0 {
				best = usage
				best.ResponseID = id
			}
		}
		// 从 chat chunk 里取出增量文本。
		delta := chatChunkDeltaText(raw)
		if delta != "" {
			if err := emitCreated(); err != nil {
				return err
			}
			textBuf.WriteString(delta)
			payload, _ := json.Marshal(map[string]any{
				"type":        "response.output_text.delta",
				"delta":       delta,
				"item_id":     id,
				"response_id": id,
			})
			if err := writeSSEEvent(w, sseEvent{Event: "response.output_text.delta", Data: string(payload)}); err != nil {
				return err
			}
		}
		return nil
	})
	// 即使上游中途断（err != nil），只要已经开始输出，也补齐 response.completed + [DONE]，
	// 让 Responses 协议的客户端平滑收尾，不报 "stream closed before response.completed"。
	streamErr := err
	if err := emitCreated(); err != nil {
		return best, err
	}
	// 收尾：completed 事件带上完整文本和 usage。
	completed := map[string]any{
		"id": id, "object": "response", "status": "completed", "model": model,
		"output": []map[string]any{{
			"type": "message", "role": "assistant", "status": "completed",
			"content": []map[string]any{{"type": "output_text", "text": textBuf.String()}},
		}},
		"output_text": textBuf.String(),
	}
	if best.Total > 0 {
		completed["usage"] = map[string]int64{
			"input_tokens": best.Prompt, "output_tokens": best.Completion, "total_tokens": best.Total,
		}
	}
	payload, _ := json.Marshal(map[string]any{"type": "response.completed", "response": completed})
	if err := writeSSEEvent(w, sseEvent{Event: "response.completed", Data: string(payload)}); err != nil {
		return best, err
	}
	if err := writeSSEData(w, "[DONE]"); err != nil {
		return best, err
	}
	// streamErr 已经用"补 completed"平滑收尾，且我们从没往调用方写头之外的坏数据，
	// 这里吞掉它（若上游一个字都没发过，textBuf 为空，客户端至少拿到一个空的 completed，
	// 也比断流强）。
	if streamErr != nil {
		best.SoftFailure = "upstream chat stream ended before normal completion: " + streamErr.Error()
	}
	return best, nil
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
		var ev sseEvent
		var err error
		// 有 closer + idleTimeout 的 reader（真实上游转发）走带超时的读，
		// 上游卡住超过 idle 就主动关连接返回错误，避免无限阻塞 → 客户端断流。
		if reader != nil && reader.closer != nil && reader.idleTimeout > 0 {
			ev, err = readNextSSEWithTimeout(reader, reader.closer, reader.idleTimeout)
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

func streamRawSSE(w http.ResponseWriter, buffered []sseEvent, reader *sseStreamReader, responseMode string) (usageTokens, error) {
	var best usageTokens
	needsResponsesCompleted := responseMode == "responses"
	completedSeen := false
	model := ""
	respID := ""
	err := readSSEEvents(buffered, reader, func(event, data string) error {
		if needsResponsesCompleted && strings.TrimSpace(data) == "[DONE]" && !completedSeen {
			return nil
		}
		if usage := usageFromSSEData(data); usage.Total > 0 {
			best = usage
		}
		if strings.TrimSpace(data) != "" && data != "[DONE]" {
			if id, m := sseResponseIDAndModel(data); id != "" || m != "" {
				if id != "" {
					respID = id
					best.ResponseID = id
				}
				if m != "" {
					model = m
				}
			}
		}
		typ := sseEventType(sseEvent{Event: event, Data: data})
		if typ == "response.completed" {
			completedSeen = true
		}
		return writeSSEEvent(w, sseEvent{Event: event, Data: data})
	})
	// 关键容错：上游始终没发 response.completed 时补一个合成终止事件——无论上游是"正常 EOF"还是
	// "中途断开/超时（err != nil）"。
	//
	// 走 Responses 协议的客户端（Codex 直连）必须收到 response.completed 才认为流结束，
	// 否则报 "stream closed before response.completed"。上游中途把 TCP 断掉、或 idle 超时
	// 触发我们主动 Close，都会让 readSSEEvents 返回 err；这些情况下客户端其实已经拿到了
	// 大部分内容，补一个 completed 让它平滑收尾，远好过把断流错误透传出去。
	// 这正是 ccswitch 走 chat 路径时靠 [DONE] 达到的效果。
	if needsResponsesCompleted && !completedSeen {
		if writeErr := writeSyntheticResponseCompleted(w, respID, model, best); writeErr != nil {
			return best, writeErr
		}
		if err != nil {
			best.SoftFailure = "upstream stream ended before response.completed: " + err.Error()
		} else {
			best.SoftFailure = "upstream stream ended before response.completed"
		}
		// 已经给了客户端完整的终止事件，这个"上游中途断"的错误就不再上抛，
		// 否则上层会误判为候选失败并可能重复请求。
		return best, nil
	}
	return best, err
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

// writeSyntheticResponseCompleted 合成一个 response.completed 事件 + [DONE]，用于上游漏发终止事件时兜底。
func writeSyntheticResponseCompleted(w http.ResponseWriter, id, model string, usage usageTokens) error {
	if id == "" {
		id = "resp_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	resp := map[string]any{
		"id":     id,
		"object": "response",
		"status": "completed",
		"model":  model,
	}
	if usage.Total > 0 {
		resp["usage"] = map[string]int64{
			"input_tokens":  usage.Prompt,
			"output_tokens": usage.Completion,
			"total_tokens":  usage.Total,
		}
	}
	payload, _ := json.Marshal(map[string]any{"type": "response.completed", "response": resp})
	if err := writeSSEEvent(w, sseEvent{Event: "response.completed", Data: string(payload)}); err != nil {
		return err
	}
	return writeSSEData(w, "[DONE]")
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
		ID:              key.ID,
		Name:            key.Name,
		KeyPrefix:       key.KeyPrefix,
		Enabled:         key.Enabled,
		DailyLimit:      key.DailyLimit,
		TotalLimit:      key.TotalLimit,
		TodayTokens:     todayTokens,
		TotalTokens:     key.TotalTokens,
		UsageDate:       key.UsageDate,
		ExpiresAt:       key.ExpiresAt,
		LastUsedAt:      key.LastUsedAt,
		LastUsedIP:      key.LastUsedIP,
		ClientFormat:    normalizeClientFormat(key.ClientFormat),
		AllowedGroupIDs: decodeUintList(key.AllowedGroupIDs),
		CreatedAt:       key.CreatedAt,
		UpdatedAt:       key.UpdatedAt,
	}
}

func filterCandidatesForGatewayKey(key *storage.GatewayKey, candidates []storage.UpstreamGroupKey) []storage.UpstreamGroupKey {
	allowed := decodeUintSet(key.AllowedGroupIDs)
	if len(allowed) == 0 {
		return candidates
	}
	out := make([]storage.UpstreamGroupKey, 0, len(candidates))
	for _, candidate := range candidates {
		if allowed[candidate.ID] {
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
