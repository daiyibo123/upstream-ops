package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

type Config struct {
	App           AppConfig           `mapstructure:"app" yaml:"app" json:"app"`
	Server        ServerConfig        `mapstructure:"server" yaml:"server" json:"server"`
	Database      DatabaseConfig      `mapstructure:"database" yaml:"database" json:"database"`
	Security      SecurityConfig      `mapstructure:"security" yaml:"security" json:"security"`
	Auth          AuthConfig          `mapstructure:"auth" yaml:"auth" json:"auth"`
	Scheduler     SchedulerConfig     `mapstructure:"scheduler" yaml:"scheduler" json:"scheduler"`
	Notifications NotificationsConfig `mapstructure:"notifications" yaml:"notifications" json:"notifications"`
	Proxy         ProxyConfig         `mapstructure:"proxy" yaml:"proxy" json:"proxy"`
	Upstream      UpstreamConfig      `mapstructure:"upstream" yaml:"upstream" json:"upstream"`
	Log           LogConfig           `mapstructure:"log" yaml:"log" json:"log"`
}

type AppConfig struct {
	Title                     string                     `mapstructure:"title" yaml:"title" json:"title"`
	NotificationPrefix        string                     `mapstructure:"notificationPrefix" yaml:"notificationPrefix" json:"notificationPrefix"`
	HomepageCheapestEnabled   bool                       `mapstructure:"homepageCheapestEnabled" yaml:"homepageCheapestEnabled" json:"homepageCheapestEnabled"`
	PublicKey                 PublicKeyConfig            `mapstructure:"publicKey" yaml:"publicKey" json:"publicKey"`
	RouteAffinity             RouteAffinityConfig        `mapstructure:"routeAffinity" yaml:"routeAffinity" json:"routeAffinity"`
	ResponseInterceptionRules []ResponseInterceptionRule `mapstructure:"responseInterceptionRules" yaml:"responseInterceptionRules" json:"responseInterceptionRules"`
}

// RouteAffinityConfig 控制"无状态普通对话"的路由缓存粘性（软亲和）。
//
// 背景：切换上游 = 换 provider 账户 = 上游 prompt cache 前缀必然失效，网关要把
// 整段上下文重新喂给新上游，既慢（首字节变长）又"降智"（模型丢失已缓存的推理上下文）。
// 这个代价通常远大于切到便宜一点点渠道省下的倍率差，因此默认策略是"缓存粘性优先"：
// 只要原渠道仍健康可调度，就继续用它，不为了省一点钱而跳走；只有当出现的更优候选
// 便宜得足够多（省钱收益超过 PromoteMinSavingsRatio）时，才允许放弃缓存去切换。
//
//   - Enabled=false 时退回历史行为（只在同 tier 才保留原渠道，一有更优就切走）。
//
// 失败重调度时"优先回到同 provider 的其它 key"这一保缓存行为不需要单独开关：
// 软亲和把原渠道顶到最前后，preferSameGroupSchedulableCandidates 会把同 provider
// 的兄弟 key 紧随其后聚拢，因此原渠道失败时自然优先落到同 provider（缓存前缀可复用）。
type RouteAffinityConfig struct {
	// Enabled 打开缓存粘性优先策略。默认开启（见 setDefaults）。
	Enabled bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	// PromoteMinSavingsRatio 是"逃生阀"：仅当更优候选相对原渠道的成本下降比例
	// 达到该阈值时，才允许为省钱放弃缓存切换。取值 [0,1)，例如 0.3 表示"新渠道
	// 至少便宜 30% 才切"。<=0 或 >=1 时取默认值 DefaultRouteAffinityPromoteMinSavingsRatio。
	PromoteMinSavingsRatio float64 `mapstructure:"promoteMinSavingsRatio" yaml:"promoteMinSavingsRatio" json:"promoteMinSavingsRatio"`
}

// DefaultRouteAffinityPromoteMinSavingsRatio 是缓存粘性"逃生阀"的默认阈值：
// 新渠道要比当前粘住的渠道便宜 30% 以上，才值得为省钱牺牲一次缓存命中。
const DefaultRouteAffinityPromoteMinSavingsRatio = 0.3

// WithDefaults 兜底路由缓存粘性配置。注意：Enabled 是 bool，无法从"零值"区分
// "用户显式关"和"未设置"，因此它的默认值只在 setDefaults(viper) 层设置；这里仅
// 兜底数值阈值，避免 0 阈值让任何微小差价都触发切换、或 >=1 的不可达阈值锁死切换。
func (r RouteAffinityConfig) WithDefaults() RouteAffinityConfig {
	if r.PromoteMinSavingsRatio <= 0 || r.PromoteMinSavingsRatio >= 1 {
		r.PromoteMinSavingsRatio = DefaultRouteAffinityPromoteMinSavingsRatio
	}
	return r
}

type ResponseInterceptionRule struct {
	Enabled   bool   `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	ChannelID uint   `mapstructure:"channelId" yaml:"channelId" json:"channelId"`
	Content   string `mapstructure:"content" yaml:"content" json:"content"`
}

type PublicKeyConfig struct {
	Enabled      bool   `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	Name         string `mapstructure:"name" yaml:"name" json:"name"`
	Key          string `mapstructure:"key" yaml:"key" json:"key"`
	Password     string `mapstructure:"password" yaml:"password" json:"password"`
	PasswordHint string `mapstructure:"passwordHint" yaml:"passwordHint" json:"passwordHint"`
	ExpiresAt    string `mapstructure:"expiresAt" yaml:"expiresAt" json:"expiresAt"`
	// IPConcurrencyLimit 限制公益 Key 对"同一个客户端 IP"的并发路数，防止单个 IP
	// 把公益额度占满导致其他人排队。<=0 时取默认值 DefaultPublicIPConcurrencyLimit。
	// 命中 IP 白名单（public_concurrency_exempt）的地址不受此限制。
	IPConcurrencyLimit int `mapstructure:"ipConcurrencyLimit" yaml:"ipConcurrencyLimit" json:"ipConcurrencyLimit"`
}

// DefaultPublicIPConcurrencyLimit 是公益 Key 单 IP 并发的默认值，保持历史行为（3 路）。
const DefaultPublicIPConcurrencyLimit = 3

// WithDefaults 兜底公益 Key 配置：单 IP 并发未设置（<=0）时回退到默认值。
func (p PublicKeyConfig) WithDefaults() PublicKeyConfig {
	if p.IPConcurrencyLimit <= 0 {
		p.IPConcurrencyLimit = DefaultPublicIPConcurrencyLimit
	}
	return p
}

type ServerConfig struct {
	Port           int      `mapstructure:"port" yaml:"port" json:"port"`
	Mode           string   `mapstructure:"mode" yaml:"mode" json:"mode"`
	TrustedProxies []string `mapstructure:"trustedProxies" yaml:"trustedProxies" json:"trustedProxies"`
	BaseURL        string   `mapstructure:"baseURL" yaml:"baseURL" json:"baseURL"`
}

type DatabaseConfig struct {
	Driver       string `mapstructure:"driver" yaml:"driver" json:"driver"`
	Path         string `mapstructure:"path" yaml:"path" json:"path"`
	Host         string `mapstructure:"host" yaml:"host" json:"host"`
	Port         int    `mapstructure:"port" yaml:"port" json:"port"`
	User         string `mapstructure:"user" yaml:"user" json:"user"`
	Password     string `mapstructure:"password" yaml:"password" json:"password"`
	Name         string `mapstructure:"name" yaml:"name" json:"name"`
	MaxOpenConns int    `mapstructure:"maxOpenConns" yaml:"maxOpenConns" json:"maxOpenConns"`
	MaxIdleConns int    `mapstructure:"maxIdleConns" yaml:"maxIdleConns" json:"maxIdleConns"`
}

func (d DatabaseConfig) ToStorageConfig() storage.DBConfig {
	return storage.DBConfig{
		Driver:       storage.DBDriver(d.Driver),
		Path:         d.Path,
		Host:         d.Host,
		Port:         d.Port,
		User:         d.User,
		Password:     d.Password,
		Name:         d.Name,
		MaxOpenConns: d.MaxOpenConns,
		MaxIdleConns: d.MaxIdleConns,
	}
}

type SecurityConfig struct {
	// AppSecret 主密钥，用于 AES-GCM。优先从 APP_SECRET 环境变量读取。
	AppSecret string `mapstructure:"appSecret" yaml:"appSecret" json:"appSecret"`
}

// AuthConfig 后台单用户登录配置。
//
// Enabled = false（默认）时整套鉴权被关掉：/api/* 全部免 token，前端检测后跳过登录页。
// 适合纯内网 / 反代后面的部署。需要公网暴露时必须显式 Enabled=true 并设强密码。
//
// Enabled=true 时 Username/Password 是写死的管理员凭据，TokenSecret 用于签发 HMAC token。
// 如果 TokenSecret 为空，会回退使用 Security.AppSecret，保证有合理默认。
type AuthConfig struct {
	Enabled         bool   `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	Username        string `mapstructure:"username" yaml:"username" json:"username"`
	Password        string `mapstructure:"password" yaml:"password" json:"password"`
	TokenSecret     string `mapstructure:"tokenSecret" yaml:"tokenSecret" json:"tokenSecret"`
	SessionTTLHours int    `mapstructure:"sessionTTLHours" yaml:"sessionTTLHours" json:"sessionTTLHours"`
}

type SchedulerConfig struct {
	BalanceCron       string          `mapstructure:"balanceCron" yaml:"balanceCron" json:"balanceCron"`
	RateCron          string          `mapstructure:"rateCron" yaml:"rateCron" json:"rateCron"`
	GatewayHealthCron string          `mapstructure:"gatewayHealthCron" yaml:"gatewayHealthCron" json:"gatewayHealthCron"`
	Concurrency       int             `mapstructure:"concurrency" yaml:"concurrency" json:"concurrency"`
	Retention         RetentionConfig `mapstructure:"retention" yaml:"retention" json:"retention"`
}

// RetentionConfig 历史数据保留策略。
//
// 字段为 0 表示该表不清理，永久保留（默认 rate_change_logs 永远保留，是核心业务数据）。
// Cron 为空时不启动清理任务。
type RetentionConfig struct {
	Cron                 string `mapstructure:"cron" yaml:"cron" json:"cron"`
	MonitorLogsDays      int    `mapstructure:"monitorLogsDays" yaml:"monitorLogsDays" json:"monitorLogsDays"`
	BalanceSnapshotsDays int    `mapstructure:"balanceSnapshotsDays" yaml:"balanceSnapshotsDays" json:"balanceSnapshotsDays"`
	NotificationLogsDays int    `mapstructure:"notificationLogsDays" yaml:"notificationLogsDays" json:"notificationLogsDays"`
	AnnouncementsDays    int    `mapstructure:"announcementsDays" yaml:"announcementsDays" json:"announcementsDays"`
	UsageLogsDays        int    `mapstructure:"usageLogsDays" yaml:"usageLogsDays" json:"usageLogsDays"`
}

// NotificationsConfig 通知去抖策略。所有字段都是"少烦我"取向，默认不丢消息只合并。
//
//   - BatchRateChanges：同次扫描中将多个分组的变化合并成 1 条消息，避免上游一次大调价
//     瞬间发出 30+ 条通知刷屏。默认 true。
//   - MinChangePct：涨跌幅 < X% 的 rate_changed 跳过推送（仍会写入 rate_change_logs）。
//     0 = 全发，对应原始行为。
//   - BalanceLowCooldownMinutes：同一渠道的 balance_low 在 X 分钟内不重复推送。
//     0 = 不冷却（每次扫描发现仍 < 阈值都发）。冷却状态持久化在数据库的
//     notification_cooldowns 表，跨重启生效。
//   - SendMaxAttempts：单条通知发送失败时最多尝试次数（含首次）。
//     1 = 不重试。重试采用指数退避：1s / 2s / 4s …，上限 30s。
type NotificationsConfig struct {
	BatchRateChanges                         bool    `mapstructure:"batchRateChanges" yaml:"batchRateChanges" json:"batchRateChanges"`
	MinChangePct                             float64 `mapstructure:"minChangePct" yaml:"minChangePct" json:"minChangePct"`
	BalanceLowCooldownMinutes                int     `mapstructure:"balanceLowCooldownMinutes" yaml:"balanceLowCooldownMinutes" json:"balanceLowCooldownMinutes"`
	SubscriptionDailyRemainingThresholdPct   float64 `mapstructure:"subscriptionDailyRemainingThresholdPct" yaml:"subscriptionDailyRemainingThresholdPct" json:"subscriptionDailyRemainingThresholdPct"`
	SubscriptionWeeklyRemainingThresholdPct  float64 `mapstructure:"subscriptionWeeklyRemainingThresholdPct" yaml:"subscriptionWeeklyRemainingThresholdPct" json:"subscriptionWeeklyRemainingThresholdPct"`
	SubscriptionMonthlyRemainingThresholdPct float64 `mapstructure:"subscriptionMonthlyRemainingThresholdPct" yaml:"subscriptionMonthlyRemainingThresholdPct" json:"subscriptionMonthlyRemainingThresholdPct"`
	SubscriptionExpiryThresholdHours         int     `mapstructure:"subscriptionExpiryThresholdHours" yaml:"subscriptionExpiryThresholdHours" json:"subscriptionExpiryThresholdHours"`
	SubscriptionAlertCooldownMinutes         int     `mapstructure:"subscriptionAlertCooldownMinutes" yaml:"subscriptionAlertCooldownMinutes" json:"subscriptionAlertCooldownMinutes"`
	SendMaxAttempts                          int     `mapstructure:"sendMaxAttempts" yaml:"sendMaxAttempts" json:"sendMaxAttempts"`
}

type ProxyConfig struct {
	Enabled             bool   `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	VersionCheckEnabled bool   `mapstructure:"versionCheckEnabled" yaml:"versionCheckEnabled" json:"versionCheckEnabled"`
	Protocol            string `mapstructure:"protocol" yaml:"protocol" json:"protocol"`
	Host                string `mapstructure:"host" yaml:"host" json:"host"`
	Port                int    `mapstructure:"port" yaml:"port" json:"port"`
	Username            string `mapstructure:"username" yaml:"username" json:"username"`
	Password            string `mapstructure:"password" yaml:"password" json:"password"`
}

const (
	DefaultUpstreamTimeoutSeconds  = 30
	DefaultCodexOriginator         = "codex_cli_rs"
	DefaultCodexVersion            = "0.144.1"
	DefaultUpstreamUserAgent       = DefaultCodexOriginator + "/" + DefaultCodexVersion + " (Ubuntu 22.4.0; x86_64) xterm-256color"
	legacyDefaultUpstreamUserAgent = "upstream-ops/0.1"
	// DefaultStreamFirstEventTimeoutSeconds 是"等上游吐出第一个可见生成事件"的窗口。
	// 推理模型（gpt-5.5/5.6 等）在真正出字前会先经历 reasoning 阶段，常需 5-30s，
	// 秒级窗口会把完全可用的渠道误判成卡死并送去冷却。放宽到 45s 与成熟网关对齐
	// （new-api StreamingTimeout 默认 300s、sub2api 默认禁用本地首字节截断），
	// 45s 已足够覆盖推理模型的 reasoning 阶段，同时把"真卡死渠道"的最坏切换延迟
	// 控制在一个更稳的时间点。等待期间仍按心跳间隔向客户端发 SSE 心跳，避免被下游断连。
	DefaultStreamFirstEventTimeoutSeconds = 45
	// DefaultHealthProbeTimeoutSeconds 是单次测活"等一个可见生成事件"的窗口。
	// 推理模型 6s 内产不出 1+1= 的可见答案就会被判死，同样需要放宽。
	DefaultHealthProbeTimeoutSeconds = 30
	// DefaultHealthProbeMaxRatio 是一键/定时"全量兜底扫描"时的倍率成本上限：
	// 只测 <=0.1 倍率的低倍率/公益渠道，避免全量测活把高倍率渠道也烧一遍。
	// 明确勾选分组测活时不套用此上限（见 gateway.OneClickHealthTestOptions）。
	DefaultHealthProbeMaxRatio = 0.1
	// DefaultTemporaryFailureCooldownSeconds 是真实上游故障（503、内容拦截、
	// 网络错误等）首次出现后退出调度池的默认时长。与 Sub2API 的临时不可调度
	// 语义一致：冷却期间不再把用户请求发送给该上游。
	DefaultTemporaryFailureCooldownSeconds = 300
)

// DefaultOpenAIHealthProbeModels 是 OpenAI 渠道一键测活默认依次尝试的模型：
// 先用主模型，失败再退到次模型。保持历史行为（gpt-5.4 → gpt-5.5）。
// 可在系统设置里覆盖，方便后续上游上线新模型时无需改代码即可纳入测活。
var DefaultOpenAIHealthProbeModels = []string{"gpt-5.4", "gpt-5.5"}

type UpstreamConfig struct {
	TimeoutSeconds int    `mapstructure:"timeoutSeconds" yaml:"timeoutSeconds" json:"timeoutSeconds"`
	UserAgent      string `mapstructure:"userAgent" yaml:"userAgent" json:"userAgent"`
	// StreamFirstEventTimeoutSeconds 覆盖流式请求等待首个可见生成事件的秒数。<=0 时取默认值。
	StreamFirstEventTimeoutSeconds int `mapstructure:"streamFirstEventTimeoutSeconds" yaml:"streamFirstEventTimeoutSeconds" json:"streamFirstEventTimeoutSeconds"`
	// HealthProbeTimeoutSeconds 覆盖测活等待可见生成事件的秒数。<=0 时取默认值。
	HealthProbeTimeoutSeconds int `mapstructure:"healthProbeTimeoutSeconds" yaml:"healthProbeTimeoutSeconds" json:"healthProbeTimeoutSeconds"`
	// HealthProbeModels 是一键测活/定时测活对 OpenAI 渠道按顺序尝试的模型清单。
	// 探测按顺序逐个尝试：前一个不行（不支持/超时/非生成响应）才试下一个，命中即停。
	// 留空时回退到内置默认清单 DefaultOpenAIHealthProbeModels（gpt-5.4 → gpt-5.5），
	// 保持历史行为。后期上游上新模型时，运维可在系统设置里补进清单，无需改代码发版。
	HealthProbeModels []string `mapstructure:"healthProbeModels" yaml:"healthProbeModels" json:"healthProbeModels"`
	// HealthProbeMaxRatio 是"全量兜底扫描"（不指定分组的一键测活 / 定时任务）用来
	// 控制成本的倍率上限：只测有效倍率 <= 该值的低倍率/公益渠道。<=0 时取默认值。
	// 明确勾选分组的一键测活不受此限制（尊重用户意图，见 OneClickHealthTestOptions）。
	HealthProbeMaxRatio float64 `mapstructure:"healthProbeMaxRatio" yaml:"healthProbeMaxRatio" json:"healthProbeMaxRatio"`
	// TemporaryFailureCooldownSeconds 控制请求转发中真实上游故障的临时不可调度时长。
	// <=0 时使用默认 300 秒；明确的 Retry-After 仍优先使用上游给出的时间。
	TemporaryFailureCooldownSeconds int                    `mapstructure:"temporaryFailureCooldownSeconds" yaml:"temporaryFailureCooldownSeconds" json:"temporaryFailureCooldownSeconds"`
	RequestRectifier                RequestRectifierConfig `mapstructure:"requestRectifier" yaml:"requestRectifier" json:"requestRectifier"`
}

type RequestRectifierConfig struct {
	Enabled                  bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	ThinkingSignature        bool `mapstructure:"thinkingSignature" yaml:"thinkingSignature" json:"thinkingSignature"`
	ThinkingBudget           bool `mapstructure:"thinkingBudget" yaml:"thinkingBudget" json:"thinkingBudget"`
	UnsupportedImageFallback bool `mapstructure:"unsupportedImageFallback" yaml:"unsupportedImageFallback" json:"unsupportedImageFallback"`
	HeuristicTextOnlyModels  bool `mapstructure:"heuristicTextOnlyModels" yaml:"heuristicTextOnlyModels" json:"heuristicTextOnlyModels"`
}

func (u UpstreamConfig) WithDefaults() UpstreamConfig {
	if u.TimeoutSeconds <= 0 {
		u.TimeoutSeconds = DefaultUpstreamTimeoutSeconds
	}
	if u.StreamFirstEventTimeoutSeconds <= 0 {
		u.StreamFirstEventTimeoutSeconds = DefaultStreamFirstEventTimeoutSeconds
	}
	if u.HealthProbeTimeoutSeconds <= 0 {
		u.HealthProbeTimeoutSeconds = DefaultHealthProbeTimeoutSeconds
	}
	if u.HealthProbeMaxRatio <= 0 {
		u.HealthProbeMaxRatio = DefaultHealthProbeMaxRatio
	}
	if u.TemporaryFailureCooldownSeconds <= 0 {
		u.TemporaryFailureCooldownSeconds = DefaultTemporaryFailureCooldownSeconds
	}
	// 清洗测活模型清单：去空白、去重、丢弃空串；清洗后为空则回退内置默认清单。
	// 这样前端传来的脏数据（空行、重复模型）不会污染探测顺序，也保证探测清单永不为空。
	cleanedModels := make([]string, 0, len(u.HealthProbeModels))
	seenModels := make(map[string]struct{}, len(u.HealthProbeModels))
	for _, m := range u.HealthProbeModels {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		key := strings.ToLower(m)
		if _, ok := seenModels[key]; ok {
			continue
		}
		seenModels[key] = struct{}{}
		cleanedModels = append(cleanedModels, m)
	}
	if len(cleanedModels) == 0 {
		cleanedModels = append(cleanedModels, DefaultOpenAIHealthProbeModels...)
	}
	u.HealthProbeModels = cleanedModels
	userAgent := strings.TrimSpace(u.UserAgent)
	if userAgent == "" || userAgent == legacyDefaultUpstreamUserAgent {
		u.UserAgent = DefaultUpstreamUserAgent
	} else {
		u.UserAgent = userAgent
	}
	return u
}

type LogConfig struct {
	Level  string `mapstructure:"level" yaml:"level" json:"level"`
	Format string `mapstructure:"format" yaml:"format" json:"format"`
}

// Load 读取 config.yaml（可选）+ APP_SECRET / * 环境变量覆盖。
//
// 关键映射：
//
//	APP_SECRET                       -> security.appSecret
//	DATABASE_DRIVER      -> database.driver
//	DATABASE_PATH        -> database.path
//	DATABASE_HOST        -> database.host
//	SERVER_PORT          -> server.port
//	SCHEDULER_BALANCECRON-> scheduler.balanceCron
func Load(path string) (*Config, error) {
	cfg, _, err := load(path, true)
	return cfg, err
}

func LoadWithPath(path string) (*Config, string, error) {
	return load(path, true)
}

func LoadFile(path string) (*Config, error) {
	cfg, _, err := load(path, false)
	return cfg, err
}

func load(path string, withEnv bool) (*Config, string, error) {
	v := viper.New()
	v.SetConfigType("yaml")

	if path != "" {
		v.SetConfigFile(path)
	} else {
		v.SetConfigName("config")
		for _, p := range configSearchPaths() {
			v.AddConfigPath(p)
		}
		v.AddConfigPath("/etc/upstream-ops")
	}

	setDefaults(v)

	if withEnv {
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		v.AutomaticEnv()
		// APP_SECRET / ADMIN_USERNAME / ADMIN_PASSWORD / AUTH_ENABLED 是独立约定的环境变量名，不带前缀。
		_ = v.BindEnv("security.appSecret", "APP_SECRET")
		_ = v.BindEnv("auth.enabled", "AUTH_ENABLED")
		_ = v.BindEnv("auth.username", "ADMIN_USERNAME")
		_ = v.BindEnv("auth.password", "ADMIN_PASSWORD")
		_ = v.BindEnv("auth.tokenSecret", "AUTH_TOKEN_SECRET")
		// Viper 坑：AutomaticEnv 只对已通过 SetDefault / BindEnv / 配置文件注册过的 key 生效；
		// 数据库的 user/password 没有合理的默认值（拒绝写"change-me"作默认），
		// 因此显式 BindEnv 以确保从环境变量读取。
		_ = v.BindEnv("database.driver", "DATABASE_DRIVER")
		_ = v.BindEnv("database.path", "DATABASE_PATH")
		_ = v.BindEnv("database.host", "DATABASE_HOST")
		_ = v.BindEnv("database.port", "DATABASE_PORT")
		_ = v.BindEnv("database.user", "DATABASE_USER")
		_ = v.BindEnv("database.password", "DATABASE_PASSWORD")
		_ = v.BindEnv("database.name", "DATABASE_NAME")
		_ = v.BindEnv("server.port", "SERVER_PORT")
		_ = v.BindEnv("server.mode", "SERVER_MODE")
		_ = v.BindEnv("log.level", "LOG_LEVEL")
	}

	if err := v.ReadInConfig(); err != nil {
		if path != "" {
			if !os.IsNotExist(err) {
				return nil, "", fmt.Errorf("read config: %w", err)
			}
		} else {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return nil, "", fmt.Errorf("read config: %w", err)
			}
		}
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, "", fmt.Errorf("unmarshal config: %w", err)
	}
	cfg.Upstream = cfg.Upstream.WithDefaults()
	cfg.App.PublicKey = cfg.App.PublicKey.WithDefaults()
	cfg.App.RouteAffinity = cfg.App.RouteAffinity.WithDefaults()
	return cfg, v.ConfigFileUsed(), nil
}

func Save(path string, cfg *Config) error {
	if path == "" {
		return fmt.Errorf("config path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir config dir: %w", err)
	}
	body, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

func ResolvePath(requested, used string) string {
	if requested != "" {
		return requested
	}
	if used != "" {
		return used
	}
	for _, candidate := range configSearchPaths() {
		candidate = filepath.Join(candidate, "config.yaml")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	if wd, err := os.Getwd(); err == nil && filepath.Base(wd) == "backend" {
		return "../config.yaml"
	}
	return "config.yaml"
}

func configSearchPaths() []string {
	if wd, err := os.Getwd(); err == nil && filepath.Base(wd) == "backend" {
		return []string{"..", "."}
	}
	return []string{"."}
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("app.title", "AI Gateway")
	v.SetDefault("app.notificationPrefix", "[AI 聚合监控] ")
	v.SetDefault("app.publicKey.enabled", false)
	v.SetDefault("app.publicKey.name", "公益 Key")
	v.SetDefault("app.publicKey.ipConcurrencyLimit", DefaultPublicIPConcurrencyLimit)
	// 缓存粘性默认开启：切上游会让上游 prompt cache 前缀失效，重喂上下文既慢又降智，
	// 默认保住缓存优先，仅当新渠道便宜足够多（见 promoteMinSavingsRatio）才切走。
	v.SetDefault("app.routeAffinity.enabled", true)
	v.SetDefault("app.routeAffinity.promoteMinSavingsRatio", DefaultRouteAffinityPromoteMinSavingsRatio)

	v.SetDefault("server.port", 8418)
	v.SetDefault("server.mode", "debug")
	v.SetDefault("server.baseURL", "http://localhost:8418")

	v.SetDefault("database.driver", "sqlite")
	v.SetDefault("database.path", "./data/upstream-ops.db")
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 3306)
	v.SetDefault("database.name", "upstreamops")
	v.SetDefault("database.maxOpenConns", 20)
	v.SetDefault("database.maxIdleConns", 5)

	// CLAUDE.md 默认建议：余额 15 分钟，倍率 30 分钟。
	v.SetDefault("scheduler.balanceCron", "37 */15 * * * *")
	v.SetDefault("scheduler.rateCron", "13 */30 * * * *")
	v.SetDefault("scheduler.gatewayHealthCron", "7 */10 * * * *")
	v.SetDefault("scheduler.concurrency", 4)

	// 历史清理：每天凌晨 3:17 跑一次（6 字段 cron 含秒），
	// monitor 30 天 / balance 90 天 / notify 90 天 / usage 1 天。rate_change_logs 不清理（业务核心数据）。
	v.SetDefault("scheduler.retention.cron", "0 17 3 * * *")
	v.SetDefault("scheduler.retention.monitorLogsDays", 30)
	v.SetDefault("scheduler.retention.balanceSnapshotsDays", 90)
	v.SetDefault("scheduler.retention.notificationLogsDays", 90)
	v.SetDefault("scheduler.retention.announcementsDays", 90)
	v.SetDefault("scheduler.retention.usageLogsDays", 1)

	v.SetDefault("auth.enabled", false)
	v.SetDefault("auth.username", "admin")
	v.SetDefault("auth.sessionTTLHours", 168) // 7 天

	// 通知去抖：默认开合并、不过滤涨跌幅、balance_low 1h 内不重复、失败重试 3 次。
	// 即"默认行为是合并刷屏 + 不重复 balance_low + 抗短时网络抖动"，不丢任何 rate_changed 事件。
	v.SetDefault("notifications.batchRateChanges", true)
	v.SetDefault("notifications.minChangePct", 0)
	v.SetDefault("notifications.balanceLowCooldownMinutes", 60)
	v.SetDefault("notifications.subscriptionDailyRemainingThresholdPct", 0)
	v.SetDefault("notifications.subscriptionWeeklyRemainingThresholdPct", 0)
	v.SetDefault("notifications.subscriptionMonthlyRemainingThresholdPct", 0)
	v.SetDefault("notifications.subscriptionExpiryThresholdHours", 0)
	v.SetDefault("notifications.subscriptionAlertCooldownMinutes", 1440)
	v.SetDefault("notifications.sendMaxAttempts", 3)

	v.SetDefault("proxy.protocol", "http")
	v.SetDefault("proxy.port", 0)
	v.SetDefault("proxy.enabled", false)
	v.SetDefault("proxy.versionCheckEnabled", false)

	v.SetDefault("upstream.timeoutSeconds", DefaultUpstreamTimeoutSeconds)
	v.SetDefault("upstream.userAgent", DefaultUpstreamUserAgent)
	v.SetDefault("upstream.streamFirstEventTimeoutSeconds", DefaultStreamFirstEventTimeoutSeconds)
	v.SetDefault("upstream.healthProbeTimeoutSeconds", DefaultHealthProbeTimeoutSeconds)
	v.SetDefault("upstream.healthProbeModels", DefaultOpenAIHealthProbeModels)
	v.SetDefault("upstream.healthProbeMaxRatio", DefaultHealthProbeMaxRatio)
	v.SetDefault("upstream.temporaryFailureCooldownSeconds", DefaultTemporaryFailureCooldownSeconds)
	v.SetDefault("app.homepageCheapestEnabled", true)
	v.SetDefault("upstream.requestRectifier.enabled", true)
	v.SetDefault("upstream.requestRectifier.thinkingSignature", true)
	v.SetDefault("upstream.requestRectifier.thinkingBudget", true)
	v.SetDefault("upstream.requestRectifier.unsupportedImageFallback", true)
	v.SetDefault("upstream.requestRectifier.heuristicTextOnlyModels", false)

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "text")
}
