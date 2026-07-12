package api

import (
	"errors"
	"net/http"
	"strconv"

	gatewaySvc "github.com/bejix/upstream-ops/backend/gateway"
	"github.com/gin-gonic/gin"
)

func registerGatewayAPI(g *gin.RouterGroup, d *Deps) {
	if d.Gateway == nil {
		return
	}
	gp := g.Group("/gateway")
	gp.GET("/keys", func(c *gin.Context) {
		list, err := d.Gateway.ListGatewayKeys()
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": list})
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
	gp.POST("/group-keys/test", func(c *gin.Context) {
		result, err := d.Gateway.TestAllGroupKeys(c.Request.Context())
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": result})
	})
	gp.GET("/usage-logs", func(c *gin.Context) {
		limit := atoiDefault(c.Query("limit"), 50)
		offset := atoiDefault(c.Query("offset"), 0)
		items, total, err := d.Gateway.ListUsageLogs(limit, offset)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": items, "total": total}})
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

func registerGatewayProxy(r *gin.Engine, d *Deps) {
	if d.Gateway == nil {
		return
	}
	r.Any("/v1/*path", func(c *gin.Context) {
		path := "/v1" + c.Param("path")
		if raw := c.Request.URL.RawQuery; raw != "" {
			path += "?" + raw
		}
		if err := d.Gateway.Proxy(c.Writer, c.Request, path); err != nil {
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
				_, _ = c.Writer.Write(gerr.Body)
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})
}
