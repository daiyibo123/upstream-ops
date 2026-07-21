package oauthpool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

var (
	ErrNoAvailableAccount = errors.New("oauth pool has no schedulable account")
	ErrUnsupportedModel   = errors.New("model is not supported by oauth pool")
)

const (
	defaultSnapshotTTL = 2 * time.Second
	defaultCooldown    = time.Minute
	defaultRateLimit   = time.Minute
	failureThreshold   = 3
)

// Repository is deliberately smaller than the concrete storage repository so
// the hot-path scheduler can be tested without a database or API handler.
type Repository interface {
	ListSchedulable(pool storage.OAuthPool, now time.Time, limit int) ([]storage.OAuthAccount, error)
	Credentials(pool storage.OAuthPool, id uint) (storage.OAuthCredentials, error)
}

type breakerMode int32

const (
	breakerAvailable breakerMode = iota
	breakerCooling
	breakerAuthFailed
	breakerDead
)

type runtimeState struct {
	inflight      atomic.Int64
	failures      atomic.Int32
	mode          atomic.Int32
	cooldownUntil atomic.Int64
	halfOpen      atomic.Bool
}

type candidate struct {
	account     storage.OAuthAccount
	credentials storage.OAuthCredentials
	runtime     *runtimeState
}

type poolSnapshot struct {
	values    []candidate
	expiresAt time.Time
}

type poolState struct {
	cursor   atomic.Uint64
	snapshot atomic.Pointer[poolSnapshot]
	load     flightGroup
	mu       sync.Mutex
	runtime  map[uint]*runtimeState
}

type Service struct {
	repository Repository
	chatgpt    poolState
	grok       poolState

	snapshotTTL atomic.Int64
	cooldownNS  atomic.Int64
	rateLimitNS atomic.Int64
	now         func() time.Time

	transport transportManager
	endpoints atomic.Pointer[Endpoints]
}

type Option func(*Service)

func WithSnapshotTTL(value time.Duration) Option {
	return func(service *Service) {
		if value > 0 {
			service.snapshotTTL.Store(int64(value))
		}
	}
}

func WithCooldown(value time.Duration) Option {
	return func(service *Service) {
		if value > 0 {
			service.cooldownNS.Store(int64(value))
		}
	}
}

func WithRateLimitCooldown(value time.Duration) Option {
	return func(service *Service) {
		if value > 0 {
			service.rateLimitNS.Store(int64(value))
		}
	}
}

func WithEndpoints(value Endpoints) Option {
	return func(service *Service) {
		normalized := value.withDefaults()
		service.endpoints.Store(&normalized)
	}
}

func NewService(repository Repository, options ...Option) *Service {
	service := &Service{repository: repository, now: time.Now}
	service.chatgpt.runtime = make(map[uint]*runtimeState)
	service.grok.runtime = make(map[uint]*runtimeState)
	service.snapshotTTL.Store(int64(defaultSnapshotTTL))
	service.cooldownNS.Store(int64(defaultCooldown))
	service.rateLimitNS.Store(int64(defaultRateLimit))
	endpoints := (Endpoints{}).withDefaults()
	service.endpoints.Store(&endpoints)
	service.transport.init()
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *Service) state(pool storage.OAuthPool) (*poolState, error) {
	switch pool {
	case storage.OAuthPoolChatGPT:
		return &s.chatgpt, nil
	case storage.OAuthPoolGrok:
		return &s.grok, nil
	default:
		return nil, fmt.Errorf("unsupported OAuth pool %q", pool)
	}
}

// Invalidate discards the immutable account snapshot. Supplying account IDs
// also forgets their process-local breaker state, which is appropriate after a
// delete, credential replacement, or an explicit health-state transition.
func (s *Service) Invalidate(pool storage.OAuthPool, accountIDs ...uint) {
	state, err := s.state(pool)
	if err != nil {
		return
	}
	state.snapshot.Store(nil)
	if len(accountIDs) == 0 {
		return
	}
	state.mu.Lock()
	for _, id := range accountIDs {
		delete(state.runtime, id)
	}
	state.mu.Unlock()
}

func (s *Service) Acquire(pool storage.OAuthPool, model string, exclude map[uint]bool) (*DispatchLease, error) {
	return s.AcquireContext(context.Background(), pool, model, exclude)
}

// HasAvailable reports whether the pool currently contains an account that
// could be acquired for the model. It is intentionally a read-only scheduler
// hint: it reuses the immutable snapshot but does not advance the round-robin
// cursor, increment inflight counters, or reserve a half-open probe.
func (s *Service) HasAvailable(ctx context.Context, pool storage.OAuthPool, model string) (bool, error) {
	if !supportsPoolModel(pool, model) {
		return false, ErrUnsupportedModel
	}
	state, err := s.state(pool)
	if err != nil {
		return false, err
	}
	snapshot, err := s.loadSnapshot(ctx, pool, state)
	if err != nil {
		return false, err
	}
	now := s.now().UTC()
	for index := range snapshot.values {
		if _, eligible := runtimeEligibility(snapshot.values[index].runtime, now); eligible {
			return true, nil
		}
	}
	return false, nil
}

func (s *Service) AcquireContext(ctx context.Context, pool storage.OAuthPool, model string, exclude map[uint]bool) (*DispatchLease, error) {
	if !supportsPoolModel(pool, model) {
		return nil, ErrUnsupportedModel
	}
	state, err := s.state(pool)
	if err != nil {
		return nil, err
	}
	snapshot, err := s.loadSnapshot(ctx, pool, state)
	if err != nil {
		return nil, err
	}
	if len(snapshot.values) == 0 {
		return nil, ErrNoAvailableAccount
	}

	start := int((state.cursor.Add(1) - 1) % uint64(len(snapshot.values)))
	now := s.now().UTC()
	selected := -1
	selectedInflight := int64(^uint64(0) >> 1)
	selectedHalfOpen := false
	for offset := 0; offset < len(snapshot.values); offset++ {
		index := (start + offset) % len(snapshot.values)
		value := &snapshot.values[index]
		if exclude != nil && exclude[value.account.ID] {
			continue
		}
		halfOpen, eligible := runtimeEligibility(value.runtime, now)
		if !eligible {
			continue
		}
		inflight := value.runtime.inflight.Load()
		if selected < 0 || inflight < selectedInflight {
			selected = index
			selectedInflight = inflight
			selectedHalfOpen = halfOpen
			if inflight == 0 && !halfOpen {
				break
			}
		}
	}
	if selected < 0 {
		return nil, ErrNoAvailableAccount
	}

	value := snapshot.values[selected]
	if selectedHalfOpen && !value.runtime.halfOpen.CompareAndSwap(false, true) {
		// Another request won the half-open probe. Retry once against the rest of
		// the immutable snapshot without recursively reloading the repository.
		nextExclude := make(map[uint]bool, len(exclude)+1)
		for id, excluded := range exclude {
			nextExclude[id] = excluded
		}
		nextExclude[value.account.ID] = true
		return s.acquireFromSnapshot(pool, model, state, snapshot, nextExclude)
	}
	value.runtime.inflight.Add(1)
	return newDispatchLease(s, pool, model, value, selectedHalfOpen), nil
}

func (s *Service) acquireFromSnapshot(pool storage.OAuthPool, model string, state *poolState, snapshot *poolSnapshot, exclude map[uint]bool) (*DispatchLease, error) {
	if len(snapshot.values) == 0 {
		return nil, ErrNoAvailableAccount
	}
	start := int((state.cursor.Add(1) - 1) % uint64(len(snapshot.values)))
	now := s.now().UTC()
	for offset := 0; offset < len(snapshot.values); offset++ {
		value := snapshot.values[(start+offset)%len(snapshot.values)]
		if exclude[value.account.ID] {
			continue
		}
		halfOpen, eligible := runtimeEligibility(value.runtime, now)
		if !eligible || (halfOpen && !value.runtime.halfOpen.CompareAndSwap(false, true)) {
			continue
		}
		value.runtime.inflight.Add(1)
		return newDispatchLease(s, pool, model, value, halfOpen), nil
	}
	return nil, ErrNoAvailableAccount
}

func runtimeEligibility(runtime *runtimeState, now time.Time) (halfOpen bool, eligible bool) {
	mode := breakerMode(runtime.mode.Load())
	switch mode {
	case breakerAuthFailed, breakerDead:
		return false, false
	case breakerCooling:
		until := time.Unix(0, runtime.cooldownUntil.Load())
		if until.After(now) {
			return false, false
		}
		if runtime.halfOpen.Load() {
			return false, false
		}
		return true, true
	default:
		return false, true
	}
}

func (s *Service) loadSnapshot(ctx context.Context, pool storage.OAuthPool, state *poolState) (*poolSnapshot, error) {
	now := s.now().UTC()
	if snapshot := state.snapshot.Load(); snapshot != nil && now.Before(snapshot.expiresAt) {
		return snapshot, nil
	}
	loaded, err, _ := state.load.Do(string(pool), func() (any, error) {
		checkTime := s.now().UTC()
		if snapshot := state.snapshot.Load(); snapshot != nil && checkTime.Before(snapshot.expiresAt) {
			return snapshot, nil
		}
		if s.repository == nil {
			return nil, errors.New("oauth pool repository is not configured")
		}
		accounts, err := s.repository.ListSchedulable(pool, checkTime, 0)
		if err != nil {
			return nil, err
		}
		values := make([]candidate, 0, len(accounts))
		seen := make(map[uint]struct{}, len(accounts))
		for _, account := range accounts {
			if !accountSchedulable(account, checkTime) {
				continue
			}
			credentials, credentialErr := s.repository.Credentials(pool, account.ID)
			if credentialErr != nil || !credentialsUsable(pool, account, credentials) {
				continue
			}
			runtime := s.runtimeFor(state, account, checkTime)
			values = append(values, candidate{account: account, credentials: credentials, runtime: runtime})
			seen[account.ID] = struct{}{}
		}
		state.mu.Lock()
		for id := range state.runtime {
			if _, exists := seen[id]; !exists {
				delete(state.runtime, id)
			}
		}
		state.mu.Unlock()
		snapshot := &poolSnapshot{values: values, expiresAt: checkTime.Add(time.Duration(s.snapshotTTL.Load()))}
		state.snapshot.Store(snapshot) // Empty pools are cached too.
		return snapshot, nil
	})
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return loaded.(*poolSnapshot), nil
}

func (s *Service) runtimeFor(state *poolState, account storage.OAuthAccount, now time.Time) *runtimeState {
	state.mu.Lock()
	defer state.mu.Unlock()
	if current := state.runtime[account.ID]; current != nil {
		return current
	}
	current := &runtimeState{}
	if (account.Status == storage.OAuthStatusCooling || account.Status == storage.OAuthStatusRateLimited) && account.DisabledUntil != nil {
		current.mode.Store(int32(breakerCooling))
		current.cooldownUntil.Store(account.DisabledUntil.UTC().UnixNano())
	} else {
		current.mode.Store(int32(breakerAvailable))
	}
	state.runtime[account.ID] = current
	return current
}

func accountSchedulable(account storage.OAuthAccount, now time.Time) bool {
	if !account.Enabled || strings.TrimSpace(account.CredentialCipher) == "" {
		return false
	}
	switch account.Status {
	case storage.OAuthStatusAlive:
		return account.InRotation && (account.DisabledUntil == nil || !account.DisabledUntil.After(now))
	case storage.OAuthStatusCooling, storage.OAuthStatusRateLimited:
		return account.DisabledUntil != nil
	default:
		return false
	}
}

func credentialsUsable(pool storage.OAuthPool, account storage.OAuthAccount, value storage.OAuthCredentials) bool {
	if pool == storage.OAuthPoolChatGPT {
		// The Codex backend requires an access token. Browser session cookies are
		// importable metadata but are not silently promoted to bearer credentials.
		return strings.TrimSpace(value.AccessToken) != ""
	}
	if strings.Contains(strings.ToLower(account.SourceFormat), "sso") {
		return normalizedSSOToken(value) != ""
	}
	return strings.TrimSpace(value.AccessToken) != "" || strings.TrimSpace(value.RefreshToken) != "" || normalizedSSOToken(value) != ""
}

func supportsPoolModel(pool storage.OAuthPool, model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	if model == "" {
		return true
	}
	switch pool {
	case storage.OAuthPoolChatGPT:
		return !strings.Contains(model, "grok")
	case storage.OAuthPoolGrok:
		return strings.Contains(model, "grok")
	default:
		return false
	}
}

type FailureKind string

const (
	FailureTemporary FailureKind = "temporary"
	FailureAuth      FailureKind = "auth"
	FailureRateLimit FailureKind = "rate_limited"
	FailurePermanent FailureKind = "permanent"
)

type Failure struct {
	Kind       FailureKind
	StatusCode int
	RetryAfter time.Duration
	Err        error
}

// FailureFromHTTP gives gateway integrations one consistent account-state
// policy: 401 is credential rejection, 429 is immediate rate-limit cooling,
// other 4xx/5xx and transport failures are temporary unless explicitly marked
// permanent by the caller.
func FailureFromHTTP(status int, retryAfter time.Duration, err error) Failure {
	value := Failure{Kind: FailureTemporary, StatusCode: status, RetryAfter: retryAfter, Err: err}
	switch status {
	case 401:
		value.Kind = FailureAuth
	case 429:
		value.Kind = FailureRateLimit
	}
	return value
}

func (s *Service) ReportSuccess(pool storage.OAuthPool, accountID uint) {
	runtime := s.findRuntime(pool, accountID)
	if runtime == nil {
		return
	}
	runtime.failures.Store(0)
	runtime.cooldownUntil.Store(0)
	runtime.mode.Store(int32(breakerAvailable))
	runtime.halfOpen.Store(false)
}

func (s *Service) ReportFailure(pool storage.OAuthPool, accountID uint, failure Failure) {
	runtime := s.findRuntime(pool, accountID)
	if runtime == nil {
		return
	}
	now := s.now().UTC()
	switch failure.Kind {
	case FailureAuth:
		runtime.mode.Store(int32(breakerAuthFailed))
		runtime.cooldownUntil.Store(0)
		runtime.halfOpen.Store(false)
	case FailurePermanent:
		runtime.mode.Store(int32(breakerDead))
		runtime.cooldownUntil.Store(0)
		runtime.halfOpen.Store(false)
	case FailureRateLimit:
		cooldown := failure.RetryAfter
		if cooldown <= 0 {
			cooldown = time.Duration(s.rateLimitNS.Load())
		}
		runtime.failures.Add(1)
		runtime.mode.Store(int32(breakerCooling))
		extendAtomicDeadline(&runtime.cooldownUntil, now.Add(cooldown).UnixNano())
		runtime.halfOpen.Store(false)
	default:
		count := runtime.failures.Add(1)
		// A failed half-open probe reopens immediately. Normal traffic requires
		// three consecutive temporary failures before it is removed.
		if runtime.halfOpen.Load() || count >= failureThreshold {
			cooldown := failure.RetryAfter
			if cooldown <= 0 {
				cooldown = time.Duration(s.cooldownNS.Load())
			}
			runtime.mode.Store(int32(breakerCooling))
			extendAtomicDeadline(&runtime.cooldownUntil, now.Add(cooldown).UnixNano())
		}
		runtime.halfOpen.Store(false)
	}
}

func extendAtomicDeadline(value *atomic.Int64, proposed int64) {
	for {
		current := value.Load()
		if current >= proposed {
			return
		}
		if value.CompareAndSwap(current, proposed) {
			return
		}
	}
}

func (s *Service) findRuntime(pool storage.OAuthPool, accountID uint) *runtimeState {
	state, err := s.state(pool)
	if err != nil {
		return nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.runtime[accountID]
}

type DispatchLease struct {
	service     *Service
	runtime     *runtimeState
	Pool        storage.OAuthPool
	Model       string
	Account     storage.OAuthAccount
	Credentials storage.OAuthCredentials
	HalfOpen    bool

	reported atomic.Bool
	released atomic.Bool
}

func newDispatchLease(service *Service, pool storage.OAuthPool, model string, value candidate, halfOpen bool) *DispatchLease {
	return &DispatchLease{
		service: service, runtime: value.runtime, Pool: pool, Model: model,
		Account: value.account, Credentials: value.credentials, HalfOpen: halfOpen,
	}
}

func (l *DispatchLease) Release() {
	if l == nil || l.runtime == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	if l.HalfOpen && !l.reported.Load() && l.service != nil {
		l.service.ReportFailure(l.Pool, l.Account.ID, Failure{Kind: FailureTemporary})
	}
	l.runtime.inflight.Add(-1)
}

func (l *DispatchLease) ReportSuccess() {
	if l == nil || l.service == nil || !l.reported.CompareAndSwap(false, true) {
		return
	}
	l.service.ReportSuccess(l.Pool, l.Account.ID)
}

func (l *DispatchLease) ReportFailure(failure Failure) {
	if l == nil || l.service == nil || !l.reported.CompareAndSwap(false, true) {
		return
	}
	l.service.ReportFailure(l.Pool, l.Account.ID, failure)
}

// Ignore completes a lease for a request error that does not describe account
// health (for example a caller-side 400). It prevents a half-open lease from
// being treated as a failed recovery probe when released.
func (l *DispatchLease) Ignore() {
	if l != nil {
		l.reported.CompareAndSwap(false, true)
	}
}

func (l *DispatchLease) Inflight() int64 {
	if l == nil || l.runtime == nil {
		return 0
	}
	return l.runtime.inflight.Load()
}
