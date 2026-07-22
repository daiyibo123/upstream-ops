package oauthpool

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

type fakeRepository struct {
	mu          sync.Mutex
	accounts    map[storage.OAuthPool][]storage.OAuthAccount
	credentials map[uint]storage.OAuthCredentials
	loads       int
}

func (r *fakeRepository) ListSchedulable(pool storage.OAuthPool, _ time.Time, _ int) ([]storage.OAuthAccount, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loads++
	return append([]storage.OAuthAccount(nil), r.accounts[pool]...), nil
}

func (r *fakeRepository) Credentials(_ storage.OAuthPool, id uint) (storage.OAuthCredentials, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.credentials[id]
	if !ok {
		return storage.OAuthCredentials{}, errors.New("missing credentials")
	}
	return value, nil
}

func aliveAccount(id uint, pool storage.OAuthPool) storage.OAuthAccount {
	return storage.OAuthAccount{ID: id, Pool: pool, Status: storage.OAuthStatusAlive, Enabled: true, InRotation: true, CredentialCipher: "encrypted"}
}

func chatGPTRepository(count int) *fakeRepository {
	repository := &fakeRepository{accounts: map[storage.OAuthPool][]storage.OAuthAccount{}, credentials: make(map[uint]storage.OAuthCredentials)}
	for index := 1; index <= count; index++ {
		id := uint(index)
		repository.accounts[storage.OAuthPoolChatGPT] = append(repository.accounts[storage.OAuthPoolChatGPT], aliveAccount(id, storage.OAuthPoolChatGPT))
		repository.credentials[id] = storage.OAuthCredentials{AccessToken: "token", AccountID: "account"}
	}
	return repository
}

func TestAcquireUsesSharedRoundRobinCursor(t *testing.T) {
	repository := chatGPTRepository(3)
	service := NewService(repository, WithSnapshotTTL(time.Hour))
	var got []uint
	for range 9 {
		lease, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, lease.Account.ID)
		lease.ReportSuccess()
		lease.Release()
	}
	want := []uint{1, 2, 3, 1, 2, 3, 1, 2, 3}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("round robin = %v, want %v", got, want)
		}
	}
	if repository.loads != 1 {
		t.Fatalf("repository loads = %d, want 1", repository.loads)
	}
}

func TestHasAvailableReusesSnapshotWithoutAdvancingOrReserving(t *testing.T) {
	repository := chatGPTRepository(2)
	service := NewService(repository, WithSnapshotTTL(time.Hour))

	for range 2 {
		available, err := service.HasAvailable(context.Background(), storage.OAuthPoolChatGPT, "gpt-5.4")
		if err != nil || !available {
			t.Fatalf("available = %v, err = %v", available, err)
		}
	}
	if repository.loads != 1 {
		t.Fatalf("repository loads = %d, want 1", repository.loads)
	}

	lease, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if lease.Account.ID != 1 {
		t.Fatalf("first acquired account = %d, want 1", lease.Account.ID)
	}
}

func TestHasAvailableObservesBreakerWithoutClaimingHalfOpenProbe(t *testing.T) {
	repository := chatGPTRepository(1)
	service := NewService(repository, WithCooldown(time.Minute), WithSnapshotTTL(time.Hour))
	var now atomic.Int64
	base := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	now.Store(base.UnixNano())
	service.now = func() time.Time { return time.Unix(0, now.Load()) }

	for range failureThreshold {
		lease, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
		if err != nil {
			t.Fatal(err)
		}
		lease.ReportFailure(Failure{Kind: FailureTemporary})
		lease.Release()
	}
	available, err := service.HasAvailable(context.Background(), storage.OAuthPoolChatGPT, "gpt-5.4")
	if err != nil || available {
		t.Fatalf("cooling availability = %v, err = %v", available, err)
	}

	now.Store(base.Add(time.Minute + time.Second).UnixNano())
	for range 2 {
		available, err = service.HasAvailable(context.Background(), storage.OAuthPoolChatGPT, "gpt-5.4")
		if err != nil || !available {
			t.Fatalf("half-open availability = %v, err = %v", available, err)
		}
	}
	lease, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
	if err != nil || !lease.HalfOpen {
		t.Fatalf("half-open lease = %#v, err = %v", lease, err)
	}
	defer lease.Release()
	available, err = service.HasAvailable(context.Background(), storage.OAuthPoolChatGPT, "gpt-5.4")
	if err != nil || available {
		t.Fatalf("claimed half-open availability = %v, err = %v", available, err)
	}
}

func TestHasAvailableRejectsUnsupportedModelAndCachesEmptyPool(t *testing.T) {
	repository := chatGPTRepository(0)
	service := NewService(repository, WithSnapshotTTL(time.Hour))
	available, err := service.HasAvailable(context.Background(), storage.OAuthPoolChatGPT, "grok-4")
	if !errors.Is(err, ErrUnsupportedModel) || available {
		t.Fatalf("unsupported availability = %v, err = %v", available, err)
	}
	for range 2 {
		available, err = service.HasAvailable(context.Background(), storage.OAuthPoolChatGPT, "gpt-5.4")
		if err != nil || available {
			t.Fatalf("empty availability = %v, err = %v", available, err)
		}
	}
	if repository.loads != 1 {
		t.Fatalf("empty repository loads = %d, want 1", repository.loads)
	}
}

func TestConcurrentAcquireDoesNotConcentrateOnOneAccount(t *testing.T) {
	repository := chatGPTRepository(4)
	service := NewService(repository, WithSnapshotTTL(time.Hour))
	const requests = 80
	leases := make(chan *DispatchLease, requests)
	var workers sync.WaitGroup
	workers.Add(requests)
	for range requests {
		go func() {
			defer workers.Done()
			lease, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
			if err == nil {
				leases <- lease
			}
		}()
	}
	workers.Wait()
	close(leases)
	counts := map[uint]int{}
	for lease := range leases {
		counts[lease.Account.ID]++
		lease.ReportSuccess()
		lease.Release()
	}
	if len(counts) != 4 {
		t.Fatalf("selected accounts = %v", counts)
	}
	values := make([]int, 0, 4)
	for _, count := range counts {
		values = append(values, count)
	}
	sort.Ints(values)
	if values[len(values)-1]-values[0] > 2 {
		t.Fatalf("concurrent distribution is imbalanced: %v", counts)
	}
}

func TestSnapshotSkipsNonSchedulableStatuses(t *testing.T) {
	repository := chatGPTRepository(0)
	statuses := []storage.OAuthAccountStatus{storage.OAuthStatusDead, storage.OAuthStatusRateLimited, storage.OAuthStatusCooling, storage.OAuthStatusUnchecked, storage.OAuthStatusAlive}
	for index, status := range statuses {
		account := aliveAccount(uint(index+1), storage.OAuthPoolChatGPT)
		account.Status = status
		repository.accounts[storage.OAuthPoolChatGPT] = append(repository.accounts[storage.OAuthPoolChatGPT], account)
		repository.credentials[account.ID] = storage.OAuthCredentials{AccessToken: "token"}
	}
	lease, err := NewService(repository).Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if lease.Account.ID != 5 {
		t.Fatalf("selected account = %d, want alive account 5", lease.Account.ID)
	}
}

func TestThreeTemporaryFailuresOpenCircuitAndHalfOpenIsSingleFlight(t *testing.T) {
	repository := chatGPTRepository(1)
	service := NewService(repository, WithCooldown(time.Minute), WithSnapshotTTL(time.Hour))
	var now atomic.Int64
	base := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	now.Store(base.UnixNano())
	service.now = func() time.Time { return time.Unix(0, now.Load()) }
	for attempt := 1; attempt <= 3; attempt++ {
		lease, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
		if err != nil {
			t.Fatalf("attempt %d acquire: %v", attempt, err)
		}
		lease.ReportFailure(Failure{Kind: FailureTemporary})
		lease.Release()
	}
	if _, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil); !errors.Is(err, ErrNoAvailableAccount) {
		t.Fatalf("open circuit error = %v", err)
	}
	now.Store(base.Add(time.Minute + time.Second).UnixNano())
	first, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
	if err != nil || !first.HalfOpen {
		t.Fatalf("half-open lease = %#v, err = %v", first, err)
	}
	if _, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil); !errors.Is(err, ErrNoAvailableAccount) {
		t.Fatalf("second half-open acquire = %v", err)
	}
	first.ReportSuccess()
	first.Release()
	lease, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
	if err != nil || lease.HalfOpen {
		t.Fatalf("recovered lease = %#v, err = %v", lease, err)
	}
	lease.Release()
}

func TestAuthAndRateLimitImmediatelyRemoveAccount(t *testing.T) {
	for _, test := range []struct {
		name    string
		failure Failure
	}{
		{name: "auth", failure: Failure{Kind: FailureAuth, StatusCode: http.StatusUnauthorized}},
		{name: "rate", failure: Failure{Kind: FailureRateLimit, StatusCode: http.StatusTooManyRequests, RetryAfter: time.Hour}},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := NewService(chatGPTRepository(1), WithSnapshotTTL(time.Hour))
			lease, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
			if err != nil {
				t.Fatal(err)
			}
			lease.ReportFailure(test.failure)
			lease.Release()
			if _, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil); !errors.Is(err, ErrNoAvailableAccount) {
				t.Fatalf("account remained schedulable: %v", err)
			}
		})
	}
}

func TestConcurrentFailureCannotShortenOAuthCooldown(t *testing.T) {
	repository := chatGPTRepository(1)
	service := NewService(repository, WithSnapshotTTL(time.Hour))
	lease, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	service.ReportFailure(storage.OAuthPoolChatGPT, 1, Failure{Kind: FailureRateLimit, RetryAfter: time.Hour})
	service.ReportFailure(storage.OAuthPoolChatGPT, 1, Failure{Kind: FailureRateLimit, RetryAfter: time.Minute})
	runtime := service.findRuntime(storage.OAuthPoolChatGPT, 1)
	if runtime == nil {
		t.Fatal("missing runtime state")
	}
	remaining := time.Until(time.Unix(0, runtime.cooldownUntil.Load()))
	if remaining < 59*time.Minute {
		t.Fatalf("OAuth cooldown was shortened: %s", remaining)
	}
}

func TestEmptyPoolSnapshotIsCachedUntilInvalidated(t *testing.T) {
	repository := chatGPTRepository(0)
	service := NewService(repository, WithSnapshotTTL(time.Hour))
	for range 2 {
		if _, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil); !errors.Is(err, ErrNoAvailableAccount) {
			t.Fatalf("empty pool error = %v", err)
		}
	}
	if repository.loads != 1 {
		t.Fatalf("empty pool loads = %d, want 1", repository.loads)
	}
	service.Invalidate(storage.OAuthPoolChatGPT)
	_, _ = service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
	if repository.loads != 2 {
		t.Fatalf("loads after invalidate = %d, want 2", repository.loads)
	}
}

func TestCheckRequiresMeaningfulStreamOutput(t *testing.T) {
	for _, test := range []struct {
		name  string
		body  string
		alive bool
	}{
		{name: "delta", body: "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\n" +
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n", alive: true},
		{name: "empty completed", body: "data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\ndata: [DONE]\n\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "text/event-stream")
				_, _ = writer.Write([]byte(test.body))
			}))
			defer server.Close()
			service := NewService(chatGPTRepository(1), WithEndpoints(Endpoints{ChatGPTCodex: server.URL}))
			account := aliveAccount(1, storage.OAuthPoolChatGPT)
			result := service.Check(context.Background(), storage.OAuthPoolChatGPT, account, storage.OAuthCredentials{AccessToken: "token"})
			if got := result.Status == storage.OAuthStatusAlive && result.Schedulable; got != test.alive {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestCheckHTTPStatusImmediatelyUpdatesSchedulerState(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     int
		wantStatus storage.OAuthAccountStatus
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, wantStatus: storage.OAuthStatusDead},
		{name: "rate limited", status: http.StatusTooManyRequests, wantStatus: storage.OAuthStatusRateLimited},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if test.status == http.StatusTooManyRequests {
					writer.Header().Set("Retry-After", "120")
				}
				writer.WriteHeader(test.status)
				_, _ = writer.Write([]byte(`{"error":{"message":"rejected"}}`))
			}))
			defer server.Close()
			repository := chatGPTRepository(1)
			service := NewService(repository, WithEndpoints(Endpoints{ChatGPTCodex: server.URL}), WithSnapshotTTL(time.Hour))
			lease, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil)
			if err != nil {
				t.Fatal(err)
			}
			lease.Release()
			account := aliveAccount(1, storage.OAuthPoolChatGPT)
			result := service.Check(context.Background(), storage.OAuthPoolChatGPT, account, storage.OAuthCredentials{AccessToken: "token"})
			if result.Status != test.wantStatus || result.Schedulable {
				t.Fatalf("health result = %#v", result)
			}
			if _, err := service.Acquire(storage.OAuthPoolChatGPT, "gpt-5.4", nil); !errors.Is(err, ErrNoAvailableAccount) {
				t.Fatalf("failed account remained schedulable: %v", err)
			}
		})
	}
}

func TestProxyConfigurationFailsClosed(t *testing.T) {
	service := NewService(chatGPTRepository(1))
	err := service.UpdateProxyConfig(config.ProxyConfig{Enabled: true, Protocol: "http", Port: 8080, SelectedTargets: []string{config.ProxyTargetChatGPTPool}})
	if err == nil {
		t.Fatal("invalid selected proxy was accepted")
	}
	_, err = service.Do(context.Background(), storage.OAuthPoolChatGPT, ResolvedRequest{Method: http.MethodGet, URL: "http://example.test"})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "proxy") {
		t.Fatalf("fail-closed error = %v", err)
	}
}

func TestProxySelectionAppliesOnlyToSelectedPool(t *testing.T) {
	var proxyHits atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		proxyHits.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer proxy.Close()
	parsed, _ := url.Parse(proxy.URL)
	host, portText, _ := net.SplitHostPort(parsed.Host)
	port, _ := strconv.Atoi(portText)
	destinationHits := atomic.Int64{}
	destination := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		destinationHits.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer destination.Close()

	service := NewService(chatGPTRepository(1))
	if err := service.UpdateProxyConfig(config.ProxyConfig{Enabled: true, Protocol: "http", Host: host, Port: port, SelectedTargets: []string{config.ProxyTargetChatGPTPool}}); err != nil {
		t.Fatal(err)
	}
	response, err := service.Do(context.Background(), storage.OAuthPoolChatGPT, ResolvedRequest{Method: http.MethodGet, URL: destination.URL})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if proxyHits.Load() != 1 || destinationHits.Load() != 0 {
		t.Fatalf("selected proxy hits=%d destination=%d", proxyHits.Load(), destinationHits.Load())
	}

	if err := service.UpdateProxyConfig(config.ProxyConfig{Enabled: true, Protocol: "http", Host: host, Port: port, SelectedTargets: []string{config.ProxyTargetGrokPool}}); err != nil {
		t.Fatal(err)
	}
	response, err = service.Do(context.Background(), storage.OAuthPoolChatGPT, ResolvedRequest{Method: http.MethodGet, URL: destination.URL})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if proxyHits.Load() != 1 || destinationHits.Load() != 1 {
		t.Fatalf("unselected pool proxy hits=%d destination=%d", proxyHits.Load(), destinationHits.Load())
	}
}

func TestOAuthPoolProxyIgnoresOrdinaryChannelFamilyScopes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pool   storage.OAuthPool
		target string
	}{
		{name: "gpt", pool: storage.OAuthPoolChatGPT, target: config.ProxyTargetGPTPoolChannel},
		{name: "grok", pool: storage.OAuthPoolGrok, target: config.ProxyTargetGrokPoolChannel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proxyURL, err := proxyURLForPool(config.ProxyConfig{
				Enabled: true, Protocol: "http", Host: "127.0.0.1", Port: 18080,
				SelectedTargets: []string{tc.target},
			}, tc.pool)
			if err != nil || proxyURL != "" {
				t.Fatalf("OAuth pool unexpectedly used channel-family proxy: url=%q err=%v", proxyURL, err)
			}
		})
	}
}
