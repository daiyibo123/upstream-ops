package api

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/auth"
	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/runtimeconfig"
	"github.com/gin-gonic/gin"
)

func TestLoginLocksIPAfterRepeatedFailures(t *testing.T) {
	gin.SetMode(gin.TestMode)

	now := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
	oldProtector := authLoginProtector
	authLoginProtector = newLoginProtector(5, 5*time.Minute, func() time.Time { return now })
	t.Cleanup(func() { authLoginProtector = oldProtector })

	r := newAuthTestRouter(t)
	ip := "203.0.113.10"

	for i := 0; i < 5; i++ {
		rec := performLogin(t, r, ip, "admin", "wrong")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("failure %d status = %d, body = %s", i+1, rec.Code, rec.Body.String())
		}
	}

	rec := performLogin(t, r, ip, "admin", "wrong")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("lock status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got != "300" {
		t.Fatalf("Retry-After = %q, want 300", got)
	}

	rec = performLogin(t, r, ip, "admin", "secret")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("locked correct-login status = %d, body = %s", rec.Code, rec.Body.String())
	}

	rec = performLogin(t, r, "203.0.113.11", "admin", "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("different IP status = %d, body = %s", rec.Code, rec.Body.String())
	}

	now = now.Add(5*time.Minute + time.Second)
	rec = performLogin(t, r, ip, "admin", "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("after lock status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestLoginSuccessResetsFailedAttempts(t *testing.T) {
	gin.SetMode(gin.TestMode)

	oldProtector := authLoginProtector
	authLoginProtector = newLoginProtector(5, 5*time.Minute, time.Now)
	t.Cleanup(func() { authLoginProtector = oldProtector })

	r := newAuthTestRouter(t)
	ip := "203.0.113.20"

	for i := 0; i < 5; i++ {
		rec := performLogin(t, r, ip, "admin", "wrong")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("initial failure %d status = %d, body = %s", i+1, rec.Code, rec.Body.String())
		}
	}

	rec := performLogin(t, r, ip, "admin", "secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("success status = %d, body = %s", rec.Code, rec.Body.String())
	}

	for i := 0; i < 5; i++ {
		rec = performLogin(t, r, ip, "admin", "wrong")
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("post-reset failure %d status = %d, body = %s", i+1, rec.Code, rec.Body.String())
		}
	}
}

func newAuthTestRouter(t *testing.T) *gin.Engine {
	t.Helper()

	authSvc, err := auth.New("admin", "secret", "test-token-secret", time.Hour)
	if err != nil {
		t.Fatalf("auth service: %v", err)
	}
	runtime := runtimeconfig.New(
		"",
		"",
		nil,
		nil,
		nil,
		nil,
		authSvc,
		nil,
		config.ProxyConfig{},
		config.UpstreamConfig{},
		nil,
	)
	r := gin.New()
	registerAuth(r.Group("/api"), &Deps{Runtime: runtime})
	return r
}

func performLogin(t *testing.T, r *gin.Engine, ip, username, password string) *httptest.ResponseRecorder {
	t.Helper()

	body := `{"username":"` + username + `","password":"` + password + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = net.JoinHostPort(ip, "12345")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}
