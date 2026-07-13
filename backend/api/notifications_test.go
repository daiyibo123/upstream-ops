package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestCreateNotifyChannelRejectsInvalidConfig(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	registerNotifications(r.Group("/api"), &Deps{})

	body := `{"name":"ops","type":"wecom","enabled":true,"config":"{\"webhook_url\":\"\"}"}`
	req := httptest.NewRequest(http.MethodPost, "/api/notifications/channels", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "invalid notification config") {
		t.Fatalf("body = %s, want invalid config error", rec.Body.String())
	}
}
