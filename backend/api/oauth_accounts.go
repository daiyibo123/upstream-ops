package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const maxOAuthImportBody = 10 << 20

func registerOAuthAccounts(g *gin.RouterGroup, d *Deps) {
	if d == nil || d.OAuthAdmin == nil || d.OAuthAdmin.Accounts() == nil {
		return
	}
	repository := d.OAuthAdmin.Accounts()

	g.GET("/oauth-pools", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"data": []gin.H{
			{"id": storage.OAuthPoolChatGPT, "name": "chatgpt号池", "fixed": true, "deletable": false},
			{"id": storage.OAuthPoolGrok, "name": "grok号池", "fixed": true, "deletable": false},
		}})
	})

	accounts := g.Group("/oauth-accounts/:pool")
	accounts.Use(func(c *gin.Context) {
		pool, err := storage.ParseOAuthPool(c.Param("pool"))
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			c.Abort()
			return
		}
		c.Set("oauth_pool", pool)
		c.Next()
	})

	accounts.GET("", func(c *gin.Context) {
		pool := oauthPoolFromContext(c)
		page := atoiDefault(c.Query("page"), 1)
		pageSize := atoiDefault(c.Query("page_size"), 50)
		if page < 1 {
			page = 1
		}
		pageSize = normalizedOAuthPageSize(pageSize)
		status, err := parseOAuthStatusFilter(c.Query("status"))
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		items, total, err := repository.List(pool, page, pageSize, status, c.Query("search"))
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		views := make([]gin.H, 0, len(items))
		for i := range items {
			views = append(views, oauthAccountView(items[i]))
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": views, "total": total, "page": page, "page_size": pageSize}})
	})

	accounts.GET("/stats", func(c *gin.Context) {
		stats, err := repository.Stats(oauthPoolFromContext(c), time.Now().UTC())
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"total": stats.Total, "available": stats.Available, "schedulable": stats.Available,
			"rate_limited": stats.RateLimited, "dead": stats.Dead, "cooling": stats.Cooling,
			"unchecked": stats.Unchecked, "status": poolAvailability(stats.Available),
		}})
	})

	accounts.POST("/import", func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxOAuthImportBody)
		raw, err := c.GetRawData()
		if err != nil {
			fail(c, http.StatusBadRequest, errors.New("OAuth import JSON is too large or unreadable"))
			return
		}
		result, job, err := d.OAuthAdmin.Import(oauthPoolFromContext(c), raw)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		items := make([]gin.H, 0, len(result.Items))
		for _, item := range result.Items {
			status := "failed"
			if item.AccountID != 0 && item.Status != "failed" {
				status = "success"
			}
			items = append(items, gin.H{
				"index": item.Index, "status": status, "action": item.Status,
				"account_id": item.AccountID, "reference": importAccountReference(item.AccountID, item.Index),
				"source_format": item.SourceFormat, "weak_identity": item.WeakIdentity, "reason": item.Reason,
			})
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"total": result.Total, "success": result.Succeeded, "succeeded": result.Succeeded,
			"created": result.Created, "updated": result.Updated, "duplicate": result.Duplicates,
			"duplicates": result.Duplicates, "failed": result.Failed, "items": items,
			"failures": result.Failures, "inspection": job,
		}})
	})

	accounts.POST("/batch-delete", func(c *gin.Context) {
		var input struct {
			IDs []uint `json:"ids" binding:"required"`
		}
		if err := c.ShouldBindJSON(&input); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		result, err := d.OAuthAdmin.BatchDelete(oauthPoolFromContext(c), input.IDs)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		failures := make([]gin.H, 0, len(result.Failures))
		for id, reason := range result.Failures {
			failures = append(failures, gin.H{"id": id, "reason": reason})
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"requested": result.Requested, "success": result.Succeeded, "succeeded": result.Succeeded, "failed": result.Failed, "failures": failures}})
	})

	accounts.POST("/inspect", func(c *gin.Context) {
		job, started := d.OAuthAdmin.StartInspection(oauthPoolFromContext(c), nil)
		if job == nil {
			fail(c, http.StatusServiceUnavailable, errors.New("OAuth inspection service is unavailable"))
			return
		}
		status := http.StatusAccepted
		if !started {
			status = http.StatusOK
		}
		c.JSON(status, gin.H{"data": job})
	})
	accounts.GET("/inspect", func(c *gin.Context) {
		job := d.OAuthAdmin.Inspection(oauthPoolFromContext(c))
		if job == nil {
			c.JSON(http.StatusOK, gin.H{"data": nil})
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": job})
	})

	accounts.GET("/:id", func(c *gin.Context) {
		id, ok := oauthAccountID(c)
		if !ok {
			return
		}
		account, err := repository.Find(oauthPoolFromContext(c), id)
		if err != nil {
			writeOAuthRepositoryError(c, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": oauthAccountView(account)})
	})
	accounts.DELETE("/:id", func(c *gin.Context) {
		id, ok := oauthAccountID(c)
		if !ok {
			return
		}
		if err := d.OAuthAdmin.Delete(oauthPoolFromContext(c), id); err != nil {
			writeOAuthRepositoryError(c, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"deleted": true, "id": id}})
	})
	accounts.POST("/:id/check", func(c *gin.Context) {
		id, ok := oauthAccountID(c)
		if !ok {
			return
		}
		account, result, err := d.OAuthAdmin.CheckOne(c.Request.Context(), oauthPoolFromContext(c), id)
		if err != nil {
			writeOAuthRepositoryError(c, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"success": result.Status == storage.OAuthStatusAlive, "status": result.Status, "error": result.Error, "account": oauthAccountView(account)}})
	})
	accounts.POST("/:id/quota", func(c *gin.Context) {
		id, ok := oauthAccountID(c)
		if !ok {
			return
		}
		account, quota, err := d.OAuthAdmin.QueryQuota(c.Request.Context(), oauthPoolFromContext(c), id)
		if err != nil {
			writeOAuthRepositoryError(c, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"account": oauthAccountView(account), "quota": quota}})
	})
}

func importAccountReference(accountID uint, index int) string {
	if accountID != 0 {
		return "account#" + strconv.FormatUint(uint64(accountID), 10)
	}
	return "item#" + strconv.Itoa(index+1)
}

func oauthPoolFromContext(c *gin.Context) storage.OAuthPool {
	value, _ := c.Get("oauth_pool")
	pool, _ := value.(storage.OAuthPool)
	return pool
}

func oauthAccountID(c *gin.Context) (uint, bool) {
	value, err := strconv.ParseUint(strings.TrimSpace(c.Param("id")), 10, 64)
	if err != nil || value == 0 {
		fail(c, http.StatusBadRequest, errors.New("invalid OAuth account id"))
		return 0, false
	}
	return uint(value), true
}

func parseOAuthStatusFilter(value string) (storage.OAuthAccountStatus, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "all":
		return "", nil
	case "alive":
		return storage.OAuthStatusAlive, nil
	case "rate_limited":
		return storage.OAuthStatusRateLimited, nil
	case "dead":
		return storage.OAuthStatusDead, nil
	case "cooling", "temporary_unavailable":
		return storage.OAuthStatusCooling, nil
	case "unchecked":
		return storage.OAuthStatusUnchecked, nil
	default:
		return "", errors.New("unsupported OAuth account status filter")
	}
}

func oauthAccountView(account storage.OAuthAccount) gin.H {
	now := time.Now().UTC()
	identifier := strings.TrimSpace(account.Email)
	if identifier == "" {
		identifier = strings.TrimSpace(account.ExternalID)
	}
	return gin.H{
		"id": account.ID, "pool": account.Pool, "display_name": account.DisplayName,
		"masked_identifier": maskAccountIdentifier(identifier), "source_format": account.SourceFormat,
		"status": account.Status, "enabled": account.Enabled, "in_rotation": account.InRotation,
		"schedulable": account.CurrentlySchedulable(now), "schedulable_reason": oauthUnschedulableReason(account, now),
		"quota_used": account.QuotaUsed, "quota_limit": account.QuotaLimit, "quota_unit": account.QuotaUnit,
		"quota_reset_at": account.QuotaResetAt, "last_checked_at": account.LastCheckedAt,
		"last_error": account.LastError, "disabled_until": account.DisabledUntil,
		"consecutive_failures": account.ConsecutiveFails, "created_at": account.CreatedAt, "updated_at": account.UpdatedAt,
	}
}

func maskAccountIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if at := strings.Index(value, "@"); at > 0 {
		return value[:1] + "***" + value[at:]
	}
	runes := []rune(value)
	if len(runes) <= 6 {
		return "***"
	}
	return string(runes[:3]) + "***" + string(runes[len(runes)-2:])
}

func oauthUnschedulableReason(account storage.OAuthAccount, now time.Time) string {
	if !account.Enabled {
		return "disabled"
	}
	if account.DisabledUntil != nil && account.DisabledUntil.After(now) {
		return "cooldown"
	}
	if account.Status != storage.OAuthStatusAlive {
		return string(account.Status)
	}
	if !account.InRotation {
		return "health_check_required"
	}
	return ""
}

func normalizedOAuthPageSize(value int) int {
	switch value {
	case 10, 50, 100, 200:
		return value
	default:
		return 10
	}
}

func poolAvailability(available int64) string {
	if available > 0 {
		return "available"
	}
	return "unavailable"
}

func writeOAuthRepositoryError(c *gin.Context, err error) {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		fail(c, http.StatusNotFound, errors.New("OAuth account not found"))
		return
	}
	fail(c, http.StatusInternalServerError, err)
}
