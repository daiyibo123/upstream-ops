package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	gatewaySvc "github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/progress"
	"github.com/bejix/upstream-ops/backend/sanitize"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func registerGatewayAPI(g *gin.RouterGroup, d *Deps) {
	if d.Gateway == nil {
		return
	}
	gp := g.Group("/gateway")
	gp.GET("/route-preferences", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"data": []gin.H{
			{"id": "ratio_first", "name": "倍率优先", "default": true},
			{"id": "pool_first", "name": "号池优先"},
			{"id": "upstream_first", "name": "上游优先"},
		}})
	})
	gp.GET("/ip-policies", func(c *gin.Context) {
		items, err := d.Gateway.ListIPPolicies()
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": items})
	})
	gp.PUT("/ip-policies", func(c *gin.Context) {
		var in gatewaySvc.IPPolicyInput
		if err := c.ShouldBindJSON(&in); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		item, err := d.Gateway.UpdateIPPolicy(in.IP, in.Blocked, in.PublicConcurrencyExempt, in.Note, in.BlockedMessage)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": item})
	})
	gp.DELETE("/ip-policies/:ip", func(c *gin.Context) {
		if err := d.Gateway.DeleteIPPolicy(c.Param("ip")); err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	gp.GET("/keys", func(c *gin.Context) {
		list, err := d.Gateway.ListGatewayKeys()
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": list})
	})
	gp.GET("/public-key", func(c *gin.Context) {
		key, err := d.Gateway.GetPublicGatewayKey()
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": key})
	})
	gp.PUT("/public-key", func(c *gin.Context) {
		var in gatewaySvc.ConfigurePublicGatewayKeyInput
		if err := c.ShouldBindJSON(&in); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		key, err := d.Gateway.ConfigurePublicGatewayKey(in)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": key})
	})
	gp.POST("/public-key/reset-verification", func(c *gin.Context) {
		key, err := d.Gateway.ResetPublicGatewayKeyVerification()
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": key})
	})
	gp.POST("/keys", func(c *gin.Context) {
		var in gatewaySvc.CreateGatewayKeyInput
		if err := c.ShouldBindJSON(&in); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		key, err := d.Gateway.CreateGatewayKey(in)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": key})
	})
	gp.POST("/keys/batch-disable", func(c *gin.Context) {
		var in gatewaySvc.BatchDisableGatewayKeysInput
		if err := c.ShouldBindJSON(&in); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		items, err := d.Gateway.BatchDisableGatewayKeys(in)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": items})
	})
	gp.PATCH("/keys/:id", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		var in gatewaySvc.UpdateGatewayKeyInput
		if err := c.ShouldBindJSON(&in); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		key, err := d.Gateway.UpdateGatewayKey(id, in)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": key})
	})
	gp.POST("/keys/:id/reveal", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		key, err := d.Gateway.RevealGatewayKey(id)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"key": key}})
	})
	gp.GET("/keys/:id/usage", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		usage, err := d.Gateway.GatewayKeyUsage(id)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": usage})
	})
	gp.DELETE("/keys/:id", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		if err := d.Gateway.DeleteGatewayKey(id); err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})
	gp.GET("/group-keys", func(c *gin.Context) {
		if c.Query("page") != "" || c.Query("page_size") != "" {
			page, pageSize, err := parsePageQuery(c)
			if err != nil {
				fail(c, http.StatusBadRequest, err)
				return
			}
			items, total, err := d.Gateway.ListGroupKeysPage(pageSize, (page-1)*pageSize, c.Query("search"))
			if err != nil {
				fail(c, http.StatusInternalServerError, err)
				return
			}
			counts, err := d.Gateway.GroupKeyCounts()
			if err != nil {
				fail(c, http.StatusInternalServerError, err)
				return
			}
			pages := 1
			if total > 0 {
				pages = int((total + int64(pageSize) - 1) / int64(pageSize))
			}
			c.JSON(http.StatusOK, gin.H{"data": gin.H{
				"items":     items,
				"total":     total,
				"alive":     counts.Alive,
				"dead":      counts.Dead,
				"enabled":   counts.Enabled,
				"page":      page,
				"page_size": pageSize,
				"pages":     pages,
			}})
			return
		}
		list, err := d.Gateway.ListGroupKeys()
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": list})
	})
	gp.PATCH("/group-keys/:id", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		var in gatewaySvc.UpdateGroupKeyInput
		if err := c.ShouldBindJSON(&in); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		item, err := d.Gateway.UpdateGroupKey(id, in)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": item})
	})
	gp.DELETE("/group-keys/:id", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		if err := d.Gateway.DeleteGroupKey(id); err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"deleted": true}})
	})
	gp.POST("/group-keys/:id/clear-cooldown", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		item, err := d.Gateway.ClearGroupKeyCooldown(id)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": item})
	})
	gp.POST("/group-keys/:id/test", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		item, err := d.Gateway.TestGroupKey(id)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": item})
	})
	gp.POST("/group-keys/:id/reveal", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		key, err := d.Gateway.RevealManualGroupKey(id)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"key": key}})
	})
	gp.POST("/group-keys/:id/detect-request-mode", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		item, err := d.Gateway.DetectGroupRequestMode(c.Request.Context(), id)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": item})
	})
	gp.GET("/group-keys/test/jobs/:id", func(c *gin.Context) {
		job, err := d.Gateway.HealthJob(c.Param("id"))
		if err != nil {
			fail(c, http.StatusNotFound, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": job})
	})
	gp.POST("/group-keys/bootstrap", func(c *gin.Context) {
		result, err := d.Gateway.BootstrapGroupKeys(c.Request.Context())
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": result})
	})
	gp.POST("/group-keys/manual", func(c *gin.Context) {
		var in gatewaySvc.ManualGroupKeyInput
		if err := c.ShouldBindJSON(&in); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		item, err := d.Gateway.CreateManualGroupKey(c.Request.Context(), in)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": item})
	})
	gp.GET("/group-keys/:id/models", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		policy, err := d.Gateway.GroupKeyModels(id)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": policy})
	})
	// Discover the upstream catalog and preserve the operator's explicit
	// allowlist. Newly discovered models are not silently enabled after the
	// first synchronization.
	gp.POST("/group-keys/:id/models/sync", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		policy, err := d.Gateway.SyncGroupKeyModels(c.Request.Context(), id)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": policy})
	})
	// Save a strict allowlist. An empty selection intentionally rejects every
	// model for this route.
	gp.PUT("/group-keys/:id/models", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		var body struct {
			Models []string `json:"models"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		policy, err := d.Gateway.SetGroupKeyModels(id, body.Models)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": policy})
	})
	gp.POST("/group-keys/test", func(c *gin.Context) {
		groupIDs := parseUintCSV(c.Query("ids"))
		// A dashboard one-click check must never burst a shared upstream.  The
		// service also enforces the OpenAI/effective-ratio policy server-side, so
		// callers cannot accidentally test costly or non-OpenAI groups.
		opts := d.Gateway.OneClickHealthTestOptions(groupIDs)
		if !wantsSSE(c) {
			job, err := d.Gateway.StartOneClickHealthJob(groupIDs)
			if err != nil {
				fail(c, http.StatusInternalServerError, err)
				return
			}
			c.JSON(http.StatusAccepted, gin.H{"data": job})
			return
		}
		if wantsSSE(c) {
			obs := setupSSE(c)
			ctx := progress.WithObserver(context.Background(), obs)
			_, err := d.Gateway.TestGroupKeys(ctx, opts)
			if err != nil {
				progress.Fail(ctx, progress.StageError, err.Error())
				return
			}
			return
		}
		result, err := d.Gateway.TestGroupKeys(context.Background(), opts)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": result})
	})
	gp.GET("/usage-logs", func(c *gin.Context) {
		limit := atoiDefault(c.Query("limit"), 50)
		offset := atoiDefault(c.Query("offset"), 0)
		view := strings.TrimSpace(c.DefaultQuery("view", "usage"))
		items, total, err := d.Gateway.ListUsageLogs(limit, offset, view)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		stats, err := d.Gateway.UsageLogStats("usage")
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		keys, err := d.Gateway.ListGatewayKeys()
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": items, "total": total, "stats": stats, "keys": keys}})
	})
	gp.GET("/usage-logs/:id", func(c *gin.Context) {
		value, err := strconv.ParseUint(strings.TrimSpace(c.Param("id")), 10, 64)
		if err != nil || value == 0 {
			fail(c, http.StatusBadRequest, errors.New("invalid dispatch event id"))
			return
		}
		entry, err := d.Gateway.UsageLogDetail(uint(value))
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				fail(c, http.StatusNotFound, errors.New("dispatch event not found"))
				return
			}
			fail(c, http.StatusInternalServerError, err)
			return
		}
		var detail any
		if strings.TrimSpace(entry.ErrorDetail) != "" {
			if json.Unmarshal([]byte(entry.ErrorDetail), &detail) != nil {
				detail = gin.H{"error": entry.ErrorDetail}
			}
		}
		if fields, ok := detail.(map[string]any); ok {
			if entry.OAuthPool != "" {
				fields["pool"] = entry.OAuthPool
			}
			if entry.OAuthAccount != "" {
				fields["account"] = entry.OAuthAccount
			}
			if entry.DispatchAttempt > 0 {
				fields["attempt"] = entry.DispatchAttempt
			}
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"event": entry, "detail": detail}})
	})
	gp.DELETE("/usage-logs", func(c *gin.Context) {
		deleted, err := d.Gateway.ClearUsageLogs()
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"deleted": deleted}})
	})
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func parseUintCSV(raw string) []uint {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]uint, 0, len(parts))
	seen := map[uint]bool{}
	for _, part := range parts {
		n, err := strconv.ParseUint(strings.TrimSpace(part), 10, 64)
		if err != nil || n == 0 {
			continue
		}
		id := uint(n)
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}

func wantsSSE(c *gin.Context) bool {
	return c.Query("stream") == "1" || strings.Contains(strings.ToLower(c.GetHeader("Accept")), "text/event-stream")
}

func gatewayProxyPathWithQuery(c *gin.Context, path string) string {
	if raw := c.Request.URL.RawQuery; raw != "" {
		return path + "?" + raw
	}
	return path
}

func handleGatewayProxy(c *gin.Context, d *Deps, path string) {
	if d == nil || d.Gateway == nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	if err := d.Gateway.Proxy(c.Writer, c.Request, path); err != nil {
		if c.Writer.Written() {
			return
		}
		var gerr *gatewaySvc.GatewayError
		if errors.As(err, &gerr) {
			for k, values := range gerr.Header {
				for _, v := range values {
					c.Writer.Header().Add(k, v)
				}
			}
			if c.Writer.Header().Get("Content-Type") == "" {
				c.Writer.Header().Set("Content-Type", "application/json")
			}
			c.Writer.WriteHeader(gerr.Status)
			safeBody := []byte(sanitize.Truncate(sanitize.RedactText(string(gerr.Body)), 64<<10))
			_, _ = c.Writer.Write(safeBody)
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": sanitize.Truncate(sanitize.RedactText(err.Error()), 2000)})
	}
}

func registerGatewayProxy(r *gin.Engine, d *Deps) {
	if d.Gateway == nil {
		return
	}
	r.POST("/responses", func(c *gin.Context) {
		handleGatewayProxy(c, d, gatewayProxyPathWithQuery(c, "/v1/responses"))
	})
	for i := 0; i < 100; i++ {
		r.POST("/"+strconv.Itoa(i)+"/responses", func(c *gin.Context) {
			handleGatewayProxy(c, d, gatewayProxyPathWithQuery(c, "/v1/responses"))
		})
	}
	r.Any("/v1/*path", func(c *gin.Context) {
		path := "/v1" + c.Param("path")
		handleGatewayProxy(c, d, gatewayProxyPathWithQuery(c, path))
	})
}
