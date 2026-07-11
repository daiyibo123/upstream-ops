package api

import (
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

var dashboardStartedAt = time.Now()

// registerDashboard 提供首页所需聚合视图。
func registerDashboard(g *gin.RouterGroup, d *Deps) {
	g.GET("/dashboard/summary", func(c *gin.Context) { dashboardSummary(c, d) })
	g.GET("/dashboard/balance-trend", func(c *gin.Context) { dashboardBalanceTrend(c, d) })
	g.GET("/dashboard/cost-trend", func(c *gin.Context) { dashboardCostTrend(c, d) })
}

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
	GroupName     string     `json:"group_name"`
	Ratio         float64    `json:"ratio"`
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
		switch group.Status {
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
			GroupName:     group.GroupName,
			Ratio:         group.Ratio,
			Status:        group.Status,
			FailureCount:  group.FailureCount,
			TotalTokens:   group.TotalTokens,
			LastCheckedAt: group.LastCheckedAt,
			LastUsedAt:    group.LastUsedAt,
			LastError:     group.LastError,
		}
		stat.Groups = append(stat.Groups, g)
		if group.Status == "alive" || group.Status == "unknown" {
			if stat.Cheapest == nil || group.Ratio < stat.Cheapest.Ratio {
				copy := g
				stat.Cheapest = &copy
			}
		}
	}
	return stat
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
