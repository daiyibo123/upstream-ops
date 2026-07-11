package api

import (
	"crypto/subtle"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	gatewaySvc "github.com/bejix/upstream-ops/backend/gateway"
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
		publicGroups := make([]dashboardGatewayGroup, 0, 8)
		for _, group := range gateway.Groups {
			if !group.Enabled || group.Status == "dead" || group.Status == "disabled" {
				continue
			}
			publicGroups = append(publicGroups, group)
			if len(publicGroups) >= 8 {
				break
			}
		}
		openaiCount := 0
		claudeCount := 0
		for _, group := range gateway.Groups {
			if !group.Enabled {
				continue
			}
			switch group.ClientFormat {
			case "claude":
				claudeCount++
			default:
				openaiCount++
			}
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"title":              "UpstreamOps",
			"total_channels":     len(channels),
			"active_channels":    gateway.AliveGroups,
			"upstream_groups":    gateway.TotalGroups,
			"available_groups":   gateway.AliveGroups + gateway.UnknownGroups,
			"openai_groups":      openaiCount,
			"claude_groups":      claudeCount,
			"today_tokens":       gateway.TodayTokens,
			"total_tokens":       gateway.TotalTokens,
			"cheapest":           gateway.Cheapest,
			"dispatch_preview":   publicGroups,
			"supported_formats":  []string{"OpenAI /v1/chat/completions", "OpenAI /v1/responses", "Claude Messages 自动转 Responses"},
			"gateway_status":     "online",
			"public_key":         publicKey,
			"public_key_enabled": publicKey.Enabled,
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
		cfg, gatewayKey, ok := loadPublicKeyConfig(d)
		if !ok || !cfg.Enabled || cfg.Key == "" || gatewayKey == nil {
			fail(c, http.StatusNotFound, errPublicKeyUnavailable())
			return
		}
		if publicKeyExpired(cfg.ExpiresAt) {
			fail(c, http.StatusGone, errPublicKeyExpired())
			return
		}
		if cfg.Password != "" && subtle.ConstantTimeCompare([]byte(in.Password), []byte(cfg.Password)) != 1 {
			fail(c, http.StatusUnauthorized, errPublicKeyPassword())
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"key":        cfg.Key,
			"name":       publicKeyName(cfg),
			"expires_at": cfg.ExpiresAt,
		}})
	})
}

type publicKeyStat struct {
	Enabled          bool       `json:"enabled"`
	Name             string     `json:"name"`
	PasswordRequired bool       `json:"password_required"`
	PasswordHint     string     `json:"password_hint,omitempty"`
	ExpiresAt        string     `json:"expires_at,omitempty"`
	Status           string     `json:"status"`
	TodayTokens      int64      `json:"today_tokens"`
	TotalTokens      int64      `json:"total_tokens"`
	LastUsedAt       *time.Time `json:"last_used_at,omitempty"`
}

func publicKeySummary(d *Deps) publicKeyStat {
	cfg, gatewayKey, ok := loadPublicKeyConfig(d)
	if !ok {
		return publicKeyStat{Status: "disabled"}
	}
	stat := publicKeyStat{
		Enabled:          cfg.Enabled && cfg.Key != "",
		Name:             publicKeyName(cfg),
		PasswordRequired: cfg.Password != "",
		PasswordHint:     cfg.PasswordHint,
		ExpiresAt:        cfg.ExpiresAt,
		Status:           "disabled",
	}
	if !stat.Enabled {
		return stat
	}
	if publicKeyExpired(cfg.ExpiresAt) {
		stat.Status = "expired"
		return stat
	}
	if gatewayKey == nil {
		stat.Status = "unavailable"
		return stat
	}
	stat.Status = "available"
	todayTokens := gatewayKey.TodayTokens
	if gatewayKey.UsageDate != "" && gatewayKey.UsageDate != time.Now().Format("2006-01-02") {
		todayTokens = 0
	}
	stat.TodayTokens = todayTokens
	stat.TotalTokens = gatewayKey.TotalTokens
	stat.LastUsedAt = gatewayKey.LastUsedAt
	return stat
}

func loadPublicKeyConfig(d *Deps) (config.PublicKeyConfig, *gatewaySvc.GatewayKeyOutput, bool) {
	if d == nil || d.Runtime == nil {
		return config.PublicKeyConfig{}, nil, false
	}
	cfg, err := config.LoadFile(d.Runtime.ConfigPath())
	if err != nil {
		return config.PublicKeyConfig{}, nil, false
	}
	publicKey := cfg.App.PublicKey
	if d.Gateway == nil || publicKey.Key == "" {
		return publicKey, nil, true
	}
	key, err := d.Gateway.FindGatewayKeyByRaw(publicKey.Key)
	if err != nil {
		return publicKey, nil, true
	}
	return publicKey, key, true
}

func publicKeyName(cfg config.PublicKeyConfig) string {
	if cfg.Name != "" {
		return cfg.Name
	}
	return "公益 Key"
}

func publicKeyExpired(raw string) bool {
	expiresAt, ok := parsePublicKeyExpiry(raw)
	return ok && time.Now().After(expiresAt)
}

func parsePublicKeyExpiry(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, true
	}
	if t, err := time.Parse("2006-01-02", raw); err == nil {
		return t.Add(24*time.Hour - time.Nanosecond), true
	}
	return time.Time{}, false
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
	TotalKeys        int64                   `json:"total_keys"`
	EnabledKeys      int64                   `json:"enabled_keys"`
	TotalGroups      int                     `json:"total_groups"`
	AliveGroups      int                     `json:"alive_groups"`
	DeadGroups       int                     `json:"dead_groups"`
	UnknownGroups    int                     `json:"unknown_groups"`
	TodayTokens      int64                   `json:"today_tokens"`
	TotalTokens      int64                   `json:"total_tokens"`
	PromptTokens     int64                   `json:"prompt_tokens"`
	CompletionTokens int64                   `json:"completion_tokens"`
	Cheapest         *dashboardGatewayGroup  `json:"cheapest,omitempty"`
	Groups           []dashboardGatewayGroup `json:"groups"`
	Keys             []dashboardGatewayKey   `json:"keys"`
}

type dashboardGatewayGroup struct {
	ID            uint       `json:"id"`
	ChannelID     uint       `json:"channel_id"`
	ChannelName   string     `json:"channel_name"`
	ClientFormat  string     `json:"client_format"`
	GroupName     string     `json:"group_name"`
	Ratio         float64    `json:"ratio"`
	Priority      int        `json:"priority"`
	Enabled       bool       `json:"enabled"`
	Status        string     `json:"status"`
	FailureCount  int        `json:"failure_count"`
	TotalTokens   int64      `json:"total_tokens"`
	LastCheckedAt *time.Time `json:"last_checked_at,omitempty"`
	LastUsedAt    *time.Time `json:"last_used_at,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
}

type dashboardGatewayKey struct {
	ID          uint       `json:"id"`
	Name        string     `json:"name"`
	KeyPrefix   string     `json:"key_prefix"`
	Enabled     bool       `json:"enabled"`
	DailyLimit  int64      `json:"daily_limit"`
	TotalLimit  int64      `json:"total_limit"`
	TodayTokens int64      `json:"today_tokens"`
	TotalTokens int64      `json:"total_tokens"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	LastUsedAt  *time.Time `json:"last_used_at,omitempty"`
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
			ID:          key.ID,
			Name:        key.Name,
			KeyPrefix:   key.KeyPrefix,
			Enabled:     key.Enabled,
			DailyLimit:  key.DailyLimit,
			TotalLimit:  key.TotalLimit,
			TodayTokens: todayTokens,
			TotalTokens: key.TotalTokens,
			ExpiresAt:   key.ExpiresAt,
			LastUsedAt:  key.LastUsedAt,
		})
	}
	stat.TotalGroups = len(groups)
	for _, group := range groups {
		status := group.Status
		if !group.Enabled {
			status = "disabled"
		}
		switch status {
		case "disabled":
		case "alive":
			stat.AliveGroups++
		case "dead":
			stat.DeadGroups++
		default:
			stat.UnknownGroups++
		}
		stat.PromptTokens += group.PromptTokens
		stat.CompletionTokens += group.CompletionTokens
		g := dashboardGatewayGroup{
			ID:            group.ID,
			ChannelID:     group.ChannelID,
			ChannelName:   group.ChannelName,
			ClientFormat:  group.ClientFormat,
			GroupName:     group.GroupName,
			Ratio:         group.Ratio,
			Priority:      group.Priority,
			Enabled:       group.Enabled,
			Status:        status,
			FailureCount:  group.FailureCount,
			TotalTokens:   group.TotalTokens,
			LastCheckedAt: group.LastCheckedAt,
			LastUsedAt:    group.LastUsedAt,
			LastError:     group.LastError,
		}
		stat.Groups = append(stat.Groups, g)
		if group.Enabled && (group.Status == "alive" || group.Status == "unknown") {
			if stat.Cheapest == nil || group.Ratio < stat.Cheapest.Ratio {
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

func dashboardGroupLess(a, b dashboardGatewayGroup) bool {
	if rankA, rankB := dashboardStatusRank(a.Status), dashboardStatusRank(b.Status); rankA != rankB {
		return rankA < rankB
	}
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if a.Ratio != b.Ratio {
		return a.Ratio < b.Ratio
	}
	if a.FailureCount != b.FailureCount {
		return a.FailureCount < b.FailureCount
	}
	return a.ID < b.ID
}

func dashboardStatusRank(status string) int {
	switch status {
	case "alive":
		return 0
	case "unknown":
		return 1
	case "dead":
		return 2
	default:
		return 3
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
