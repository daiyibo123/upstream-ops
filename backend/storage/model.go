package storage

import "time"

// ChannelType 上游渠道类型。
type ChannelType string

const (
	ChannelTypeNewAPI  ChannelType = "newapi"
	ChannelTypeSub2API ChannelType = "sub2api"
)

// CredentialMode 渠道凭据模式：
//   - password: 经典模式，存账号 + 密码，由 Connector 走完整登录流程
//   - token:    跳过登录，存用户已有的 cookie / access_token，直接构造 AuthSession
//
// token 模式不依赖自动验证码 / 不会自动续期，token 失效时表现为 last_error 显示鉴权失败。
type CredentialMode string

const (
	CredentialModePassword CredentialMode = "password"
	CredentialModeToken    CredentialMode = "token"
)

// Channel 上游渠道账号。Password / Turnstile API key 等敏感字段都加密保存。
//
// 注意：会话凭据（access_token / refresh_token / cookie / csrf）单独存放在 AuthSession 表。
//
// CredentialMode + PasswordCipher 的语义重载：
//   - password 模式（默认）：Username + PasswordCipher 存账号密码，由 Connector.Login 用
//   - token    模式：PasswordCipher 存 JSON blob（NewAPI: {cookie,user_id} / Sub2API: {access_token,refresh_token}），
//     channel.Service 解析后直接构造 AuthSession，跳过 Login。Username 字段在 token 模式下保留
//     用户填写的备注（一般是邮箱），仅做展示。
//
// 复用 PasswordCipher 而不新增 TokenCipher 是为了让现有的 GORM 行 / 加密路径 / 迁移流程零变动。
type Channel struct {
	ID        uint        `gorm:"primaryKey" json:"id"`
	Name      string      `gorm:"size:128;not null;uniqueIndex" json:"name"`
	Type      ChannelType `gorm:"size:32;not null;index" json:"type"`
	SiteURL   string      `gorm:"size:512;not null" json:"site_url"`
	Username  string      `gorm:"size:256;not null" json:"username"`
	SortOrder int         `gorm:"not null;default:1" json:"sort_order"`
	// Pinned channels are shown before the normal cost ordering.  This is an
	// operator-facing preference only; it must never alter gateway scheduling.
	Pinned                 bool           `gorm:"not null;default:false;index" json:"pinned"`
	PasswordCipher         string         `gorm:"size:4096;not null" json:"-"`
	CredentialMode         CredentialMode `gorm:"size:16;not null;default:'password'" json:"credential_mode"`
	LoginExtraParams       string         `gorm:"type:text" json:"login_extra_params"`
	TurnstileEnabled       bool           `gorm:"default:false" json:"turnstile_enabled"`
	IgnoreAnnouncements    bool           `gorm:"default:false" json:"ignore_announcements"`
	SubscriptionEnabled    bool           `gorm:"default:false" json:"subscription_enabled"`
	ProxyEnabled           bool           `gorm:"default:false" json:"proxy_enabled"`
	CaptchaConfigID        *uint          `json:"captcha_config_id,omitempty"`
	BalanceThreshold       float64        `gorm:"default:0" json:"balance_threshold"`
	RechargeMultiplier     *float64       `json:"recharge_multiplier,omitempty"`
	RechargeMultiplierMode string         `gorm:"size:16;not null;default:'divide'" json:"recharge_multiplier_mode"`
	MonitorEnabled         bool           `gorm:"default:true" json:"monitor_enabled"`

	// 最近一次采集结果（聚合视图，便于列表页直接展示）
	LastBalance   *float64   `json:"last_balance,omitempty"`
	LastBalanceAt *time.Time `json:"last_balance_at,omitempty"`
	TodayCost     *float64   `json:"today_cost,omitempty"`
	TotalCost     *float64   `json:"total_cost,omitempty"`
	LastError     string     `gorm:"type:text" json:"last_error,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (Channel) TableName() string { return "channels" }

// AuthSession 渠道登录后保存的凭据，按 ChannelID 一对一关联。
// *Cipher 字段都用 AES-GCM 加密；UserID 是上游账号 ID 字符串（非敏感），明文存放。
type AuthSession struct {
	ChannelID          uint       `gorm:"primaryKey" json:"channel_id"`
	UserID             string     `gorm:"size:64" json:"user_id,omitempty"`
	AccessTokenCipher  string     `gorm:"type:text" json:"-"`
	RefreshTokenCipher string     `gorm:"type:text" json:"-"`
	CookieCipher       string     `gorm:"type:text" json:"-"`
	CSRFTokenCipher    string     `gorm:"size:1024" json:"-"`
	ExpiresAt          *time.Time `json:"expires_at,omitempty"`
	LastLoginAt        *time.Time `json:"last_login_at,omitempty"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (AuthSession) TableName() string { return "auth_sessions" }

// CaptchaProviderType 历史自动验证码平台类型，保留用于旧数据库兼容。
type CaptchaProviderType string

const (
	CaptchaCapSolver   CaptchaProviderType = "capsolver"
	CaptchaTwoCaptcha  CaptchaProviderType = "2captcha"
	CaptchaAntiCaptcha CaptchaProviderType = "anticaptcha"
	CaptchaYesCaptcha  CaptchaProviderType = "yescaptcha"
)

// CaptchaConfig 历史自动验证码配置。当前版本不再暴露配置入口，结构保留用于旧数据库兼容。
type CaptchaConfig struct {
	ID           uint                `gorm:"primaryKey" json:"id"`
	Name         string              `gorm:"size:128;not null;uniqueIndex" json:"name"`
	Type         CaptchaProviderType `gorm:"size:32;not null;index" json:"type"`
	APIKeyCipher string              `gorm:"size:1024" json:"-"`
	Endpoint     string              `gorm:"size:512" json:"endpoint,omitempty"`
	Extra        string              `gorm:"type:text" json:"extra,omitempty"`
	Enabled      bool                `gorm:"default:true" json:"enabled"`
	ProxyEnabled bool                `gorm:"default:false" json:"proxy_enabled"`
	LastBalance  *float64            `json:"last_balance,omitempty"`
	BalanceUnit  string              `gorm:"size:32" json:"balance_unit,omitempty"`
	BalanceAt    *time.Time          `json:"balance_at,omitempty"`
	BalanceError string              `gorm:"type:text" json:"balance_error,omitempty"`
	CreatedAt    time.Time           `json:"created_at"`
	UpdatedAt    time.Time           `json:"updated_at"`
}

func (CaptchaConfig) TableName() string { return "captcha_configs" }

// RateSnapshot 渠道当前观察到的模型 / 分组倍率快照。upsert per (channel_id, model_name)。
// 实际的"变化历史"在 RateChangeLog；此表只保存当前状态。
type RateSnapshot struct {
	ID              uint    `gorm:"primaryKey" json:"id"`
	ChannelID       uint    `gorm:"not null;uniqueIndex:idx_rate_chan_model" json:"channel_id"`
	ModelName       string  `gorm:"size:256;not null;uniqueIndex:idx_rate_chan_model" json:"model_name"`
	Description     string  `gorm:"size:512" json:"description,omitempty"`
	Ratio           float64 `gorm:"not null" json:"ratio"`
	CompletionRatio float64 `json:"completion_ratio"`

	FirstSeenAt time.Time `json:"first_seen_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
}

func (RateSnapshot) TableName() string { return "rate_snapshots" }

// RateChangeLog 倍率变化历史。每次扫描发现差异时写入一行。
type RateChangeLog struct {
	ID                 uint      `gorm:"primaryKey" json:"id"`
	ChannelID          uint      `gorm:"not null;index" json:"channel_id"`
	ModelName          string    `gorm:"size:256;not null;index" json:"model_name"`
	OldRatio           *float64  `json:"old_ratio,omitempty"`
	NewRatio           float64   `gorm:"not null" json:"new_ratio"`
	OldCompletionRatio *float64  `json:"old_completion_ratio,omitempty"`
	NewCompletionRatio float64   `json:"new_completion_ratio"`
	ChangedAt          time.Time `gorm:"not null;index" json:"changed_at"`
}

func (RateChangeLog) TableName() string { return "rate_change_logs" }

// UpstreamAnnouncement 保存从上游渠道同步到的公告。
type UpstreamAnnouncement struct {
	ID              uint       `gorm:"primaryKey" json:"id"`
	ChannelID       uint       `gorm:"not null;uniqueIndex:idx_announcement_chan_source;index" json:"channel_id"`
	SourceKey       string     `gorm:"size:512;not null;uniqueIndex:idx_announcement_chan_source" json:"source_key"`
	Title           string     `gorm:"size:512" json:"title,omitempty"`
	Content         string     `gorm:"type:text;not null" json:"content"`
	Type            string     `gorm:"size:64" json:"type,omitempty"`
	Link            string     `gorm:"size:512" json:"link,omitempty"`
	PublishedAt     *time.Time `json:"published_at,omitempty"`
	SourceUpdatedAt *time.Time `json:"source_updated_at,omitempty"`
	FirstSeenAt     time.Time  `gorm:"not null;index" json:"first_seen_at"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (UpstreamAnnouncement) TableName() string { return "upstream_announcements" }

// BalanceSnapshot 周期性余额采样，用于图表展示。
type BalanceSnapshot struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	ChannelID uint      `gorm:"not null;index" json:"channel_id"`
	Balance   float64   `gorm:"not null" json:"balance"`
	SampledAt time.Time `gorm:"not null;index" json:"sampled_at"`
}

func (BalanceSnapshot) TableName() string { return "balance_snapshots" }

// CostSnapshot 周期性消费采样，用于图表展示。
type CostSnapshot struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	ChannelID uint      `gorm:"not null;index" json:"channel_id"`
	TodayCost float64   `gorm:"not null" json:"today_cost"`
	SampledAt time.Time `gorm:"not null;index" json:"sampled_at"`
}

func (CostSnapshot) TableName() string { return "cost_snapshots" }

// NotificationChannelType 通知渠道类型。旧类型常量保留用于读取历史数据。
type NotificationChannelType string

const (
	NotifyTelegram    NotificationChannelType = "telegram"
	NotifyWebhook     NotificationChannelType = "webhook"
	NotifyEmail       NotificationChannelType = "email"
	NotifyWecom       NotificationChannelType = "wecom"
	NotifyDingTalk    NotificationChannelType = "dingtalk"
	NotifyFeishu      NotificationChannelType = "feishu"
	NotifyServerChan3 NotificationChannelType = "serverchan3"
)

// NotificationChannel 通知渠道配置。ConfigCipher 加密保存 JSON 配置（含 token / webhook url / 密码等）。
//
// Subscriptions 是 JSON 数组，记录该渠道关心的上游、事件和分组过滤；为空 / "[]" 表示订阅一切。
// 非敏感数据，明文保存，方便 Dispatcher 直接读取过滤而不解密。
type NotificationChannel struct {
	ID            uint                    `gorm:"primaryKey" json:"id"`
	Name          string                  `gorm:"size:128;not null;uniqueIndex" json:"name"`
	Type          NotificationChannelType `gorm:"size:32;not null;index" json:"type"`
	ConfigCipher  string                  `gorm:"type:text;not null" json:"-"`
	Subscriptions string                  `gorm:"size:4096;not null;default:'[]'" json:"subscriptions"`
	Enabled       bool                    `gorm:"default:true" json:"enabled"`
	ProxyEnabled  bool                    `gorm:"default:false" json:"proxy_enabled"`
	CreatedAt     time.Time               `json:"created_at"`
	UpdatedAt     time.Time               `json:"updated_at"`
}

func (NotificationChannel) TableName() string { return "notification_channels" }

// NotificationEvent 系统内部触发的通知事件类型。
type NotificationEvent string

const (
	EventBalanceLow             NotificationEvent = "balance_low"
	EventRateChanged            NotificationEvent = "rate_changed"
	EventRateStructureChanged   NotificationEvent = "rate_structure_changed"
	EventRateAdded              NotificationEvent = "rate_added"
	EventRateRemoved            NotificationEvent = "rate_removed"
	EventAnnouncement           NotificationEvent = "announcement"
	EventLoginFailed            NotificationEvent = "login_failed"
	EventCaptchaFailed          NotificationEvent = "captcha_failed"
	EventMonitorFailed          NotificationEvent = "monitor_failed"
	EventSubscriptionDailyLow   NotificationEvent = "subscription_daily_remaining_low"
	EventSubscriptionWeeklyLow  NotificationEvent = "subscription_weekly_remaining_low"
	EventSubscriptionMonthlyLow NotificationEvent = "subscription_monthly_remaining_low"
	EventSubscriptionExpiring   NotificationEvent = "subscription_expiring"
)

// NotificationLog 通知发送记录。
type NotificationLog struct {
	ID                uint              `gorm:"primaryKey" json:"id"`
	ChannelID         uint              `gorm:"not null;index" json:"channel_id"`
	UpstreamChannelID uint              `gorm:"not null;default:0;index" json:"upstream_channel_id,omitempty"`
	Event             NotificationEvent `gorm:"size:64;not null;index" json:"event"`
	Subject           string            `gorm:"size:512;not null" json:"subject"`
	Body              string            `gorm:"type:text" json:"body"`
	Success           bool              `gorm:"not null" json:"success"`
	ErrorMessage      string            `gorm:"type:text" json:"error_message,omitempty"`
	SentAt            time.Time         `gorm:"not null;index" json:"sent_at"`
}

func (NotificationLog) TableName() string { return "notification_logs" }

// NotificationCooldown 跨重启持久化的通知冷却记录。
//
// 业务键 (ChannelID, Event)：标记某渠道某类事件最近一次发送时间。
// Dispatcher 在发送 cooldown-aware 事件（如 balance_low）前查这张表，
// 命中且未过 cooldown 就跳过。
//
// 不和 NotificationLog 合并是因为：
//   - NotificationLog 是审计/历史日志（用户可见、可清理）
//   - NotificationCooldown 是去抖控制平面（仅最新一条、原子 upsert）
//
// ChannelID 这里指的是**上游渠道**（storage.Channel），不是通知渠道。
type NotificationCooldown struct {
	ChannelID  uint              `gorm:"primaryKey" json:"channel_id"`
	Event      NotificationEvent `gorm:"primaryKey;size:64" json:"event"`
	LastSentAt time.Time         `gorm:"not null" json:"last_sent_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

func (NotificationCooldown) TableName() string { return "notification_cooldowns" }

// MonitorJob 监控任务类型。
type MonitorJob string

const (
	MonitorJobLogin   MonitorJob = "login"
	MonitorJobBalance MonitorJob = "balance"
	MonitorJobRates   MonitorJob = "rates"
)

// MonitorLog 每次扫描 / 登录尝试的结果，便于诊断失败。
type MonitorLog struct {
	ID           uint       `gorm:"primaryKey" json:"id"`
	ChannelID    uint       `gorm:"not null;index" json:"channel_id"`
	Job          MonitorJob `gorm:"size:32;not null;index" json:"job"`
	Success      bool       `gorm:"not null" json:"success"`
	ErrorMessage string     `gorm:"type:text" json:"error_message,omitempty"`
	DurationMS   int64      `json:"duration_ms"`
	StartedAt    time.Time  `gorm:"not null;index" json:"started_at"`
	FinishedAt   time.Time  `json:"finished_at"`
}

func (MonitorLog) TableName() string { return "monitor_logs" }

// UsageLog 记录每次通过网关的请求，用于"使用记录"页展示（渠道/分组/模型/token/时间）。
// 只在请求成功后写入，保持精简；保留天数由清理任务控制。
type UsageLog struct {
	ID               uint      `gorm:"primaryKey" json:"id"`
	GatewayKeyID     uint      `gorm:"index" json:"gateway_key_id"`
	GatewayKeyName   string    `gorm:"size:128" json:"gateway_key_name,omitempty"`
	RequestIP        string    `gorm:"size:64;index" json:"request_ip,omitempty"`
	ChannelID        uint      `gorm:"index" json:"channel_id"`
	ChannelName      string    `gorm:"size:128" json:"channel_name,omitempty"`
	GroupName        string    `gorm:"size:128" json:"group_name,omitempty"`
	Model            string    `gorm:"size:256;index" json:"model,omitempty"`
	ClientFormat     string    `gorm:"size:16" json:"client_format,omitempty"`
	PromptTokens     int64     `json:"prompt_tokens"`
	CompletionTokens int64     `json:"completion_tokens"`
	TotalTokens      int64     `json:"total_tokens"`
	CachedTokens     int64     `gorm:"not null;default:0" json:"cached_tokens"`
	Ratio            float64   `json:"ratio"`
	Status           string    `gorm:"size:32;not null;default:'success';index" json:"status"`
	FirstTokenMS     int64     `gorm:"not null;default:0" json:"first_token_ms"`
	DurationMS       int64     `gorm:"not null;default:0" json:"duration_ms"`
	CreatedAt        time.Time `gorm:"index" json:"created_at"`
}

func (UsageLog) TableName() string { return "usage_logs" }

// IPPolicy is a global abuse-control rule for gateway callers. The exemption
// only applies to the public-key per-IP concurrency guard; blacklisting always
// takes precedence.
type IPPolicy struct {
	ID                      uint      `gorm:"primaryKey" json:"id"`
	IP                      string    `gorm:"size:64;not null;uniqueIndex" json:"ip"`
	Blocked                 bool      `gorm:"not null;default:false;index" json:"blocked"`
	PublicConcurrencyExempt bool      `gorm:"not null;default:false" json:"public_concurrency_exempt"`
	Note                    string    `gorm:"size:256" json:"note,omitempty"`
	CreatedAt               time.Time `json:"created_at"`
	UpdatedAt               time.Time `json:"updated_at"`
}

func (IPPolicy) TableName() string { return "ip_policies" }

const (
	DefaultInputPricePerMillion  = 5.0
	DefaultOutputPricePerMillion = 30.0
)

// GatewayKey is the local API key a client uses against this deployment's
// OpenAI-compatible /v1/* gateway. The full key is encrypted for reveal/copy;
// authentication uses KeyHash so request handling does not need to decrypt.
type GatewayKey struct {
	ID                   uint       `gorm:"primaryKey" json:"id"`
	Name                 string     `gorm:"size:128;not null" json:"name"`
	KeyPrefix            string     `gorm:"size:24;not null;index" json:"key_prefix"`
	KeyHash              string     `gorm:"size:64;not null;uniqueIndex" json:"-"`
	KeyCipher            string     `gorm:"type:text;not null" json:"-"`
	Enabled              bool       `gorm:"not null;default:true" json:"enabled"`
	ClientFormat         string     `gorm:"size:16;not null;default:'openai'" json:"client_format"`
	AllowedGroupScope    string     `gorm:"size:16;not null;default:'all';index" json:"allowed_group_scope"`
	AllowedGroupIDs      string     `gorm:"type:text" json:"allowed_group_ids,omitempty"`
	DailyLimit           int64      `gorm:"not null;default:0" json:"daily_limit"`
	TotalLimit           int64      `gorm:"not null;default:0" json:"total_limit"`
	TodayTokens          int64      `gorm:"not null;default:0" json:"today_tokens"`
	TotalTokens          int64      `gorm:"not null;default:0" json:"total_tokens"`
	TodayPromptTokens    int64      `gorm:"not null;default:0" json:"today_prompt_tokens"`
	TotalPromptTokens    int64      `gorm:"not null;default:0" json:"total_prompt_tokens"`
	TodayCachedTokens    int64      `gorm:"not null;default:0" json:"today_cached_tokens"`
	TotalCachedTokens    int64      `gorm:"not null;default:0" json:"total_cached_tokens"`
	CostPerMillion       float64    `gorm:"not null;default:0" json:"cost_per_million"`
	BalanceLimit         float64    `gorm:"not null;default:0" json:"balance_limit"`
	ConcurrencyLimit     int        `gorm:"not null;default:0" json:"concurrency_limit"`
	MaxGroupRatio        float64    `gorm:"not null;default:0" json:"max_group_ratio"`
	TodayCost            float64    `gorm:"not null;default:0" json:"today_cost"`
	TotalCost            float64    `gorm:"not null;default:0" json:"total_cost"`
	UsageDate            string     `gorm:"size:10;index" json:"usage_date,omitempty"`
	ExpiresAt            *time.Time `json:"expires_at,omitempty"`
	IsPublic             bool       `gorm:"not null;default:false;index" json:"is_public"`
	PublicName           string     `gorm:"size:128" json:"public_name,omitempty"`
	PublicPasswordCipher string     `gorm:"type:text" json:"-"`
	PublicPasswordHint   string     `gorm:"size:256" json:"public_password_hint,omitempty"`
	LastUsedAt           *time.Time `json:"last_used_at,omitempty"`
	LastUsedIP           string     `gorm:"size:128" json:"last_used_ip,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

func (GatewayKey) TableName() string { return "gateway_keys" }

// GatewayAffinity remembers which upstream handled a stateful Responses item.
// It is intentionally keyed by a hash so upstream response IDs are not exposed
// in the local database.
type GatewayAffinity struct {
	ID           uint      `gorm:"primaryKey" json:"id"`
	AffinityHash string    `gorm:"size:64;not null;uniqueIndex" json:"-"`
	GroupKeyID   uint      `gorm:"not null;index" json:"group_key_id"`
	ExpiresAt    time.Time `gorm:"not null;index" json:"expires_at"`
	LastUsedAt   time.Time `gorm:"not null;index" json:"last_used_at"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (GatewayAffinity) TableName() string { return "gateway_affinities" }

// UpstreamGroupKey stores one upstream API key bound to a channel group. The
// gateway scheduler chooses among these records by live status and ratio.
type UpstreamGroupKey struct {
	ID                    uint        `gorm:"primaryKey" json:"id"`
	ChannelID             uint        `gorm:"not null;uniqueIndex:idx_upstream_group_key;index" json:"channel_id"`
	ChannelName           string      `gorm:"size:128" json:"channel_name,omitempty"`
	ChannelURL            string      `gorm:"size:1024" json:"channel_url,omitempty"`
	ChannelType           ChannelType `gorm:"size:32;not null" json:"channel_type"`
	ClientFormat          string      `gorm:"size:16;not null;default:'openai';index" json:"client_format"`
	RequestMode           string      `gorm:"size:16;not null;default:'responses';index" json:"request_mode"`
	GroupRef              string      `gorm:"size:128;not null;uniqueIndex:idx_upstream_group_key" json:"group_ref"`
	GroupName             string      `gorm:"size:256;not null" json:"group_name"`
	GroupDesc             string      `gorm:"size:512" json:"group_description,omitempty"`
	Ratio                 float64     `gorm:"not null;default:1" json:"ratio"`
	InputPricePerMillion  float64     `gorm:"not null;default:5" json:"input_price_per_million"`
	OutputPricePerMillion float64     `gorm:"not null;default:30" json:"output_price_per_million"`
	Priority              int         `gorm:"not null;default:0;index" json:"priority"`
	// Charity 标记这个分组是"公益/免费"渠道。调度时公益永远优先于付费，
	// 公益内部再按倍率高低（这里沿用 ratio 低者优先）排序，公益全挂了才轮到付费。
	Charity          bool       `gorm:"not null;default:false;index" json:"charity"`
	UpstreamKeyID    int64      `gorm:"not null;default:0" json:"upstream_key_id"`
	KeyCipher        string     `gorm:"type:text;not null" json:"-"`
	Enabled          bool       `gorm:"not null;default:true;index" json:"enabled"`
	Status           string     `gorm:"size:16;not null;default:'unknown';index" json:"status"`
	ConcurrencyLimit int        `gorm:"not null;default:0" json:"concurrency_limit"`
	FailureCount     int        `gorm:"not null;default:0" json:"failure_count"`
	PromptTokens     int64      `gorm:"not null;default:0" json:"prompt_tokens"`
	CompletionTokens int64      `gorm:"not null;default:0" json:"completion_tokens"`
	TotalTokens      int64      `gorm:"not null;default:0" json:"total_tokens"`
	LastCheckedAt    *time.Time `json:"last_checked_at,omitempty"`
	LastLatencyMS    int64      `gorm:"not null;default:0" json:"last_latency_ms"`
	LastSuccessAt    *time.Time `json:"last_success_at,omitempty"`
	LastUsedAt       *time.Time `json:"last_used_at,omitempty"`
	DisabledUntil    *time.Time `json:"disabled_until,omitempty"`
	LastError        string     `gorm:"type:text" json:"last_error,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

func (UpstreamGroupKey) TableName() string { return "upstream_group_keys" }
