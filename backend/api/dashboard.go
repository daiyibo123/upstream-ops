package api

import (
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/gin-gonic/gin"
)

var dashboardStartedAt = time.Now()

// registerDashboard 提供首页所需聚合视图。
func registerDashboard(g *gin.RouterGroup, d *Deps) {
	g.GET("/dashboard/summary", func(c *gin.Context) { dashboardSummary(c, d) })
	g.GET("/dashboard/balance-trend", func(c *gin.Context) { dashboardBalanceTrend(c, d) })
	g.GET("/dashboard/cost-trend", func(c *gin.Context) { dashboardCostTrend(c, d) })
}

func registerPublicDashboard(g *gin.RouterGroup, d *Deps) {
	g.GET("/summary", func(c *gin.Context) {
		channels, _ := d.Channels.List()
		gateway := dashboardGateway(d)
		publicKey := publicKeySummary(d)
		title := publicDashboardTitle(d)
		homepageCheapestEnabled := true
		if d != nil && d.Runtime != nil {
			if cfg, err := config.LoadFile(d.Runtime.ConfigPath()); err == nil {
				homepageCheapestEnabled = cfg.App.HomepageCheapestEnabled
			}
		}
		// 公开页展示所有当前可调度 OpenAI 分组中倍率最低的前五个。
		// 这和实际调度的优先级策略分开：这里展示的是可对外说明的成本顺序。
		publicGroups := dashboardPublicDispatchPreview(gateway.Groups)
		openaiCount := 0
		claudeCount := 0
		grokCount := 0
		for _, group := range gateway.Groups {
			if !group.Enabled {
				continue
			}
			switch group.ClientFormat {
			case "claude":
				claudeCount++
			case "grok":
				grokCount++
			default:
				openaiCount++
			}
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"title":                     title,
			"total_channels":            len(channels),
			"active_channels":           gateway.AliveGroups,
			"upstream_groups":           gateway.TotalGroups,
			"available_groups":          gateway.AliveGroups + gateway.UnknownGroups,
			"zero_balance_groups":       gateway.ZeroBalanceGroups,
			"rate_limited_groups":       gateway.RateLimitedGroups,
			"forbidden_groups":          gateway.ForbiddenGroups,
			"non_generation_groups":     gateway.NonGenerationGroups,
			"error_groups":              gateway.ErrorGroups,
			"openai_groups":             openaiCount,
			"claude_groups":             claudeCount,
			"grok_groups":               grokCount,
			"today_tokens":              gateway.TodayTokens,
			"total_tokens":              gateway.TotalTokens,
			"cheapest":                  gateway.Cheapest,
			"dispatch_preview":          publicGroups,
			"homepage_cheapest_enabled": homepageCheapestEnabled,
			"supported_formats":         []string{"OpenAI /v1/chat/completions", "OpenAI /v1/responses", "Claude Messages 自动转 Responses"},
			"gateway_status":            "online",
			"public_key":                publicKey,
			"public_key_enabled":        publicKey.Enabled,
		}})
	})
	g.POST("/key/reveal", func(c *gin.Context) {
		var in struct {
			Password string `json:"password"`
		}
		if err := c.ShouldBindJSON(&in); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		if d.Gateway == nil {
			fail(c, http.StatusNotFound, errPublicKeyUnavailable())
			return
		}
		raw, key, err := d.Gateway.RevealPublicGatewayKey(in.Password)
		if err != nil {
			status := http.StatusNotFound
			if err.Error() == "public key expired" {
				status = http.StatusGone
			} else if err.Error() == "public key password mismatch" {
				status = http.StatusUnauthorized
			}
			fail(c, status, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"key":        raw,
			"name":       key.Name,
			"expires_at": key.ExpiresAt,
		}})
	})
}

// dashboardPublicDispatchPreview returns the five cheapest available OpenAI
// websites. A website can expose multiple groups (and even multiple imported
// channels), but the public overview keeps only that website's cheapest group.
func dashboardPublicDispatchPreview(groups []dashboardGatewayGroup) []dashboardGatewayGroup {
	cheapestBySite := make(map[string]dashboardGatewayGroup, len(groups))
	for _, group := range groups {
		if !dashboardGroupIsOpenAI(group.ClientFormat) {
			continue
		}
		if !group.Enabled || (group.Status != "alive" && group.Status != "unknown") {
			continue
		}
		site := dashboardPublicPreviewSiteKey(group)
		best, exists := cheapestBySite[site]
		if !exists || group.Ratio < best.Ratio || (group.Ratio == best.Ratio && group.ID < best.ID) {
			cheapestBySite[site] = group
		}
	}
	publicGroups := make([]dashboardGatewayGroup, 0, len(cheapestBySite))
	for _, group := range cheapestBySite {
		publicGroups = append(publicGroups, group)
	}
	sort.SliceStable(publicGroups, func(i, j int) bool {
		if publicGroups[i].Ratio != publicGroups[j].Ratio {
			return publicGroups[i].Ratio < publicGroups[j].Ratio
		}
		return publicGroups[i].ID < publicGroups[j].ID
	})
	if len(publicGroups) > 5 {
		return publicGroups[:5]
	}
	return publicGroups
}

func dashboardPublicPreviewSiteKey(group dashboardGatewayGroup) string {
	if domain := strings.ToLower(strings.TrimSpace(group.SiteDomain)); domain != "" {
		return "domain:" + domain
	}
	if name := strings.ToLower(strings.TrimSpace(group.ChannelName)); name != "" {
		return "channel:" + name
	}
	return fmt.Sprintf("channel-id:%d", group.ChannelID)
}

func publicDashboardTitle(d *Deps) string {
	if d == nil || d.Runtime == nil {
		return "AI Gateway"
	}
	cfg, err := config.LoadFile(d.Runtime.ConfigPath())
	if err != nil {
		return "AI Gateway"
	}
	if title := strings.TrimSpace(cfg.App.Title); title != "" {
		return title
	}
	return "AI Gateway"
}

type publicKeyStat struct {
	Enabled           bool       `json:"enabled"`
	Name              string     `json:"name"`
	KeyPrefix         string     `json:"key_prefix,omitempty"`
	MaskedKey         string     `json:"masked_key,omitempty"`
	PasswordRequired  bool       `json:"password_required"`
	PasswordHint      string     `json:"password_hint,omitempty"`
	ExpiresAt         string     `json:"expires_at,omitempty"`
	Status            string     `json:"status"`
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

func publicKeySummary(d *Deps) publicKeyStat {
	if d == nil || d.Gateway == nil {
		return publicKeyStat{Status: "disabled"}
	}
	key, err := d.Gateway.GetPublicGatewayKey()
	if err != nil || key == nil {
		return publicKeyStat{Status: "disabled"}
	}
	stat := publicKeyStat{
		Enabled:          key.Enabled,
		Name:             key.Name,
		KeyPrefix:        key.KeyPrefix,
		MaskedKey:        key.MaskedKey,
		PasswordRequired: key.PasswordRequired,
		PasswordHint:     key.PasswordHint,
		Status:           "disabled",
	}
	stat.TodayTokens = key.TodayTokens
	stat.TotalTokens = key.TotalTokens
	stat.TodayPromptTokens = key.TodayPromptTokens
	stat.TotalPromptTokens = key.TotalPromptTokens
	stat.TodayCachedTokens = key.TodayCachedTokens
	stat.TotalCachedTokens = key.TotalCachedTokens
	stat.TodayCacheHitRate = key.TodayCacheHitRate
	stat.TotalCacheHitRate = key.TotalCacheHitRate
	stat.LastUsedAt = key.LastUsedAt
	if !stat.Enabled {
		return stat
	}
	if key.ExpiresAt != nil && time.Now().After(*key.ExpiresAt) {
		stat.Status = "expired"
		return stat
	}
	if key.ExpiresAt != nil {
		// 用完整 RFC3339 时间戳（含时区），而不是截断成纯日期。前端 new Date()
		// 解析纯日期会按 UTC 零点处理，在东八区就渲染成"次日早八点"，与后端实际的
		// 精确 24 小时过期时刻不符。返回带时区的完整时间戳即可显示真实过期时刻。
		stat.ExpiresAt = key.ExpiresAt.Format(time.RFC3339)
	}
	stat.Status = "available"
	return stat
}

func errPublicKeyUnavailable() error {
	return publicKeyError("public key is not available")
}

func errPublicKeyExpired() error {
	return publicKeyError("public key expired")
}

func errPublicKeyPassword() error {
	return publicKeyError("public key password mismatch")
}

type publicKeyError string

func (e publicKeyError) Error() string { return string(e) }

type dashboardLowest struct {
	ChannelID uint     `json:"channel_id"`
	Name      string   `json:"name"`
	Balance   *float64 `json:"balance"`
}

type dashboardChannelStat struct {
	ID             uint     `json:"id"`
	Name           string   `json:"name"`
	Type           string   `json:"type"`
	MonitorEnabled bool     `json:"monitor_enabled"`
	LastBalance    *float64 `json:"last_balance,omitempty"`
	TodayCost      *float64 `json:"today_cost,omitempty"`
	TotalCost      *float64 `json:"total_cost,omitempty"`
	LastError      string   `json:"last_error,omitempty"`
}

type dashboardGatewayStat struct {
	TotalKeys           int64                   `json:"total_keys"`
	EnabledKeys         int64                   `json:"enabled_keys"`
	TotalGroups         int                     `json:"total_groups"`
	AliveGroups         int                     `json:"alive_groups"`
	DeadGroups          int                     `json:"dead_groups"`
	ZeroBalanceGroups   int                     `json:"zero_balance_groups"`
	RateLimitedGroups   int                     `json:"rate_limited_groups"`
	ForbiddenGroups     int                     `json:"forbidden_groups"`
	NonGenerationGroups int                     `json:"non_generation_groups"`
	ErrorGroups         int                     `json:"error_groups"`
	UnknownGroups       int                     `json:"unknown_groups"`
	TodayTokens         int64                   `json:"today_tokens"`
	TotalTokens         int64                   `json:"total_tokens"`
	PromptTokens        int64                   `json:"prompt_tokens"`
	CompletionTokens    int64                   `json:"completion_tokens"`
	Cheapest            *dashboardGatewayGroup  `json:"cheapest,omitempty"`
	Groups              []dashboardGatewayGroup `json:"groups"`
	Keys                []dashboardGatewayKey   `json:"keys"`
}

type dashboardGatewayGroup struct {
	ID                    uint       `json:"id"`
	ChannelID             uint       `json:"channel_id"`
	ChannelName           string     `json:"channel_name"`
	SiteDomain            string     `json:"site_domain,omitempty"`
	ClientFormat          string     `json:"client_format"`
	GroupName             string     `json:"group_name"`
	Ratio                 float64    `json:"ratio"`
	InputPricePerMillion  float64    `json:"input_price_per_million"`
	OutputPricePerMillion float64    `json:"output_price_per_million"`
	Priority              int        `json:"priority"`
	Charity               bool       `json:"charity"`
	Enabled               bool       `json:"enabled"`
	Status                string     `json:"status"`
	FailureCount          int        `json:"failure_count"`
	TotalTokens           int64      `json:"total_tokens"`
	LastCheckedAt         *time.Time `json:"last_checked_at,omitempty"`
	LastUsedAt            *time.Time `json:"last_used_at,omitempty"`
	LastError             string     `json:"last_error,omitempty"`
}

type dashboardGatewayKey struct {
	ID                uint       `json:"id"`
	Name              string     `json:"name"`
	KeyPrefix         string     `json:"key_prefix"`
	Enabled           bool       `json:"enabled"`
	DailyLimit        int64      `json:"daily_limit"`
	TotalLimit        int64      `json:"total_limit"`
	TodayTokens       int64      `json:"today_tokens"`
	TotalTokens       int64      `json:"total_tokens"`
	TodayPromptTokens int64      `json:"today_prompt_tokens"`
	TotalPromptTokens int64      `json:"total_prompt_tokens"`
	TodayCachedTokens int64      `json:"today_cached_tokens"`
	TotalCachedTokens int64      `json:"total_cached_tokens"`
	TodayCacheHitRate float64    `json:"today_cache_hit_rate"`
	TotalCacheHitRate float64    `json:"total_cache_hit_rate"`
	CostPerMillion    float64    `json:"cost_per_million"`
	BalanceLimit      float64    `json:"balance_limit"`
	ConcurrencyLimit  int        `json:"concurrency_limit"`
	MaxGroupRatio     float64    `json:"max_group_ratio"`
	BalanceRemaining  float64    `json:"balance_remaining"`
	TodayCost         float64    `json:"today_cost"`
	TotalCost         float64    `json:"total_cost"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	LastUsedAt        *time.Time `json:"last_used_at,omitempty"`
}

type dashboardServerStat struct {
	Status        string    `json:"status"`
	Database      string    `json:"database"`
	UptimeSeconds int64     `json:"uptime_seconds"`
	StartedAt     time.Time `json:"started_at"`
	ServerTime    time.Time `json:"server_time"`
	GoVersion     string    `json:"go_version"`
	NumGoroutine  int       `json:"num_goroutine"`
	AllocBytes    uint64    `json:"alloc_bytes"`
	SysBytes      uint64    `json:"sys_bytes"`
	LastError     string    `json:"last_error,omitempty"`
}

func dashboardSummary(c *gin.Context, d *Deps) {
	channels, err := d.Channels.List()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}

	stats := make([]dashboardChannelStat, 0, len(channels))
	var totalBalance float64
	var todayTotalCost float64
	var totalCost float64
	var lowest *dashboardLowest
	var activeCount, failedCount int

	for _, ch := range channels {
		stat := dashboardChannelStat{
			ID:             ch.ID,
			Name:           ch.Name,
			Type:           string(ch.Type),
			MonitorEnabled: ch.MonitorEnabled,
			LastBalance:    ch.LastBalance,
			TodayCost:      ch.TodayCost,
			TotalCost:      ch.TotalCost,
			LastError:      ch.LastError,
		}
		stats = append(stats, stat)
		if ch.LastError != "" {
			failedCount++
		} else if ch.MonitorEnabled {
			activeCount++
		}
		if ch.LastBalance != nil {
			totalBalance += *ch.LastBalance
			if lowest == nil || (lowest.Balance == nil) || (*ch.LastBalance < *lowest.Balance) {
				bal := *ch.LastBalance
				lowest = &dashboardLowest{ChannelID: ch.ID, Name: ch.Name, Balance: &bal}
			}
		}
		if ch.TodayCost != nil {
			todayTotalCost += *ch.TodayCost
		}
		if ch.TotalCost != nil {
			totalCost += *ch.TotalCost
		}
	}

	recentChanges, err := d.Rates.ListChanges(0, 10)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"total_channels":      len(channels),
			"active_channels":     activeCount,
			"failed_channels":     failedCount,
			"total_balance":       totalBalance,
			"today_total_cost":    todayTotalCost,
			"total_cost":          totalCost,
			"lowest_balance":      lowest,
			"channels":            stats,
			"recent_rate_changes": recentChanges,
			"gateway":             dashboardGateway(d),
			"server":              dashboardServer(d),
		},
	})
}

func dashboardGateway(d *Deps) dashboardGatewayStat {
	stat := dashboardGatewayStat{}
	if d.Gateway == nil {
		return stat
	}
	keys, _ := d.Gateway.ListGatewayKeys()
	groups, _ := d.Gateway.ListGroupKeys()
	siteDomains := make(map[uint]string)
	if d.Channels != nil {
		channels, _ := d.Channels.List()
		for _, channel := range channels {
			siteDomains[channel.ID] = channelSiteDomain(channel.SiteURL)
		}
	}
	stat.TotalKeys = int64(len(keys))
	today := time.Now().Format("2006-01-02")
	for _, key := range keys {
		todayTokens := key.TodayTokens
		if key.UsageDate != "" && key.UsageDate != today {
			todayTokens = 0
		}
		if key.Enabled {
			stat.EnabledKeys++
		}
		stat.TodayTokens += todayTokens
		stat.TotalTokens += key.TotalTokens
		stat.Keys = append(stat.Keys, dashboardGatewayKey{
			ID:                key.ID,
			Name:              key.Name,
			KeyPrefix:         key.KeyPrefix,
			Enabled:           key.Enabled,
			DailyLimit:        key.DailyLimit,
			TotalLimit:        key.TotalLimit,
			TodayTokens:       todayTokens,
			TotalTokens:       key.TotalTokens,
			TodayPromptTokens: key.TodayPromptTokens,
			TotalPromptTokens: key.TotalPromptTokens,
			TodayCachedTokens: key.TodayCachedTokens,
			TotalCachedTokens: key.TotalCachedTokens,
			TodayCacheHitRate: key.TodayCacheHitRate,
			TotalCacheHitRate: key.TotalCacheHitRate,
			CostPerMillion:    key.CostPerMillion,
			BalanceLimit:      key.BalanceLimit,
			ConcurrencyLimit:  key.ConcurrencyLimit,
			MaxGroupRatio:     key.MaxGroupRatio,
			BalanceRemaining:  key.BalanceRemaining,
			TodayCost:         key.TodayCost,
			TotalCost:         key.TotalCost,
			ExpiresAt:         key.ExpiresAt,
			LastUsedAt:        key.LastUsedAt,
		})
	}
	for _, group := range groups {
		status := dashboardEffectiveGroupStatus(group.Status)
		if !group.Enabled {
			status = "disabled"
		}
		isOpenAI := dashboardGroupIsOpenAI(group.ClientFormat)
		if isOpenAI {
			stat.TotalGroups++
			switch status {
			case "disabled":
			case "alive":
				stat.AliveGroups++
			case "dead":
				stat.DeadGroups++
			case "zero_balance":
				stat.ZeroBalanceGroups++
			case "rate_limited":
				stat.RateLimitedGroups++
			case "forbidden", "auth_failed":
				stat.ForbiddenGroups++
			case "non_generation":
				stat.NonGenerationGroups++
			case "timeout", "network_error", "upstream_error", "model_error", "invalid_request", "server_error":
				stat.ErrorGroups++
			default:
				stat.UnknownGroups++
			}
		}
		stat.PromptTokens += group.PromptTokens
		stat.CompletionTokens += group.CompletionTokens
		// 展示与前五名比较统一用有效倍率（EffectiveRatioValue），与调度器口径一致。
		// 否则首页按标称 Ratio 显示便宜（如 0.02/0.03），调度却按含 RatioScalePercent
		// 缩放的有效倍率选中更贵的渠道（0.04），造成"页面显示便宜实际调度贵"的错位。
		g := dashboardGatewayGroup{
			ID:                    group.ID,
			ChannelID:             group.ChannelID,
			ChannelName:           group.ChannelName,
			SiteDomain:            siteDomains[group.ChannelID],
			ClientFormat:          group.ClientFormat,
			GroupName:             group.GroupName,
			Ratio:                 group.EffectiveRatioValue(),
			InputPricePerMillion:  group.InputPricePerMillion,
			OutputPricePerMillion: group.OutputPricePerMillion,
			Priority:              group.Priority,
			Charity:               group.Charity,
			Enabled:               group.Enabled,
			Status:                status,
			FailureCount:          group.FailureCount,
			TotalTokens:           group.TotalTokens,
			LastCheckedAt:         group.LastCheckedAt,
			LastUsedAt:            group.LastUsedAt,
			LastError:             group.LastError,
		}
		stat.Groups = append(stat.Groups, g)
		if isOpenAI && group.Enabled && (status == "alive" || status == "unknown") {
			if stat.Cheapest == nil || g.Ratio < stat.Cheapest.Ratio {
				copy := g
				stat.Cheapest = &copy
			}
		}
	}
	sort.SliceStable(stat.Groups, func(i, j int) bool {
		return dashboardGroupLess(stat.Groups[i], stat.Groups[j])
	})
	return stat
}

func dashboardGroupIsOpenAI(format string) bool {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "openai", "any":
		// Both native Responses and Chat-completions compatibility upstreams are
		// OpenAI/GPT channels. Excluding the latter made the dashboard health
		// numbers disagree with one-click health checks and hide real failures.
		return true
	default:
		return false
	}
}

func channelSiteDomain(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Hostname() == "" {
		return strings.TrimSpace(raw)
	}
	return u.Hostname()
}

func dashboardGroupLess(a, b dashboardGatewayGroup) bool {
	if rankA, rankB := dashboardStatusRank(a.Status), dashboardStatusRank(b.Status); rankA != rankB {
		return rankA < rankB
	}
	if a.Charity != b.Charity {
		return a.Charity
	}
	if a.Ratio != b.Ratio {
		return a.Ratio < b.Ratio
	}
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if a.FailureCount != b.FailureCount {
		return a.FailureCount < b.FailureCount
	}
	return a.ID < b.ID
}

func dashboardEffectiveGroupStatus(status string) string {
	switch status {
	case "rate_limited", "network_error", "timeout", "upstream_error", "server_error":
		// Transient transport/pressure outcomes are retained in last_error and
		// cooldown metadata. They must not paint an otherwise enabled route as
		// dead in the overview; dispatch still honors disabled_until.
		return "alive"
	default:
		return status
	}
}

func dashboardStatusRank(status string) int {
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
		return 5
	}
}

func dashboardServer(d *Deps) dashboardServerStat {
	now := time.Now()
	stat := dashboardServerStat{
		Status:        "ok",
		Database:      "ok",
		UptimeSeconds: int64(now.Sub(dashboardStartedAt).Seconds()),
		StartedAt:     dashboardStartedAt,
		ServerTime:    now,
		GoVersion:     runtime.Version(),
		NumGoroutine:  runtime.NumGoroutine(),
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	stat.AllocBytes = mem.Alloc
	stat.SysBytes = mem.Sys
	if d.DB == nil {
		stat.Status = "degraded"
		stat.Database = "missing"
		stat.LastError = "database handle is nil"
		return stat
	}
	sqlDB, err := d.DB.DB()
	if err != nil {
		stat.Status = "degraded"
		stat.Database = "down"
		stat.LastError = err.Error()
		return stat
	}
	if err := sqlDB.Ping(); err != nil {
		stat.Status = "degraded"
		stat.Database = "down"
		stat.LastError = err.Error()
	}
	return stat
}

func dashboardBalanceTrend(c *gin.Context, d *Deps) {
	days, _ := strconv.Atoi(c.DefaultQuery("days", "7"))
	if days <= 0 {
		days = 7
	}
	trend, err := d.Rates.AggregateBalanceTrend(days)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": trend})
}

func dashboardCostTrend(c *gin.Context, d *Deps) {
	days, _ := strconv.Atoi(c.DefaultQuery("days", "7"))
	if days <= 0 {
		days = 7
	}
	trend, err := d.Rates.AggregateCostTrend(days)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": trend})
}
