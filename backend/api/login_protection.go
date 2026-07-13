package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	loginFailureLimit = 5
	loginLockDuration = 5 * time.Minute
)

var authLoginProtector = newLoginProtector(loginFailureLimit, loginLockDuration, time.Now)

type loginProtector struct {
	mu           sync.Mutex
	maxFailures  int
	lockDuration time.Duration
	now          func() time.Time
	attempts     map[string]*loginAttemptState
}

type loginAttemptState struct {
	failures    int
	lockedUntil time.Time
}

func newLoginProtector(maxFailures int, lockDuration time.Duration, now func() time.Time) *loginProtector {
	if maxFailures <= 0 {
		maxFailures = 1
	}
	if lockDuration <= 0 {
		lockDuration = 5 * time.Minute
	}
	if now == nil {
		now = time.Now
	}
	return &loginProtector{
		maxFailures:  maxFailures,
		lockDuration: lockDuration,
		now:          now,
		attempts:     make(map[string]*loginAttemptState),
	}
}

func (p *loginProtector) locked(ip string) (time.Duration, bool) {
	ip = normalizeLoginIP(ip)
	now := p.now()

	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.attempts[ip]
	if state == nil {
		return 0, false
	}
	if state.lockedUntil.After(now) {
		return state.lockedUntil.Sub(now), true
	}
	if !state.lockedUntil.IsZero() {
		delete(p.attempts, ip)
	}
	return 0, false
}

func (p *loginProtector) recordFailure(ip string) (time.Duration, bool) {
	ip = normalizeLoginIP(ip)
	now := p.now()

	p.mu.Lock()
	defer p.mu.Unlock()

	state := p.attempts[ip]
	if state == nil {
		state = &loginAttemptState{}
		p.attempts[ip] = state
	}
	if state.lockedUntil.After(now) {
		return state.lockedUntil.Sub(now), true
	}
	if !state.lockedUntil.IsZero() {
		state.failures = 0
		state.lockedUntil = time.Time{}
	}

	state.failures++
	if state.failures > p.maxFailures {
		state.failures = 0
		state.lockedUntil = now.Add(p.lockDuration)
		return p.lockDuration, true
	}
	return 0, false
}

func (p *loginProtector) reset(ip string) {
	ip = normalizeLoginIP(ip)

	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.attempts, ip)
}

func normalizeLoginIP(ip string) string {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return "unknown"
	}
	return ip
}

func respondLoginLocked(c *gin.Context, retryAfter time.Duration) {
	seconds := retryAfterSeconds(retryAfter)
	c.Header("Retry-After", strconv.Itoa(seconds))
	c.JSON(http.StatusTooManyRequests, gin.H{
		"error":               fmt.Sprintf("登录失败次数过多，请 %s 后再试。", retryAfterText(seconds)),
		"retry_after_seconds": seconds,
	})
}

func retryAfterSeconds(d time.Duration) int {
	if d <= 0 {
		return 1
	}
	seconds := int((d + time.Second - 1) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}

func retryAfterText(seconds int) string {
	if seconds >= 60 {
		minutes := (seconds + 59) / 60
		return fmt.Sprintf("%d 分钟", minutes)
	}
	return fmt.Sprintf("%d 秒", seconds)
}
