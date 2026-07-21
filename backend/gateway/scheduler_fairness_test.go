package gateway

import (
	"sync"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestEquivalentUpstreamsUseConcurrentSafeRoundRobin(t *testing.T) {
	service := &Service{}
	candidates := []storage.UpstreamGroupKey{
		{ID: 1, Status: "alive", ClientFormat: "openai", RequestMode: "responses", GroupRef: "a", Ratio: 1, Priority: 10},
		{ID: 2, Status: "alive", ClientFormat: "openai", RequestMode: "responses", GroupRef: "b", Ratio: 1, Priority: 10},
	}
	request := normalizedRequest{RequestModel: "gpt-test", ResponseMode: "responses"}
	var sequence []uint
	for range 4 {
		ordered := service.orderCandidatesForRequest(candidates, request)
		sequence = append(sequence, ordered[0].ID)
	}
	want := []uint{1, 2, 1, 2}
	for index := range want {
		if sequence[index] != want[index] {
			t.Fatalf("round-robin sequence=%v want=%v", sequence, want)
		}
	}

	service = &Service{}
	const requests = 64
	counts := map[uint]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(requests)
	for range requests {
		go func() {
			defer wg.Done()
			ordered := service.orderCandidatesForRequest(candidates, request)
			mu.Lock()
			counts[ordered[0].ID]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if counts[1] != requests/2 || counts[2] != requests/2 {
		t.Fatalf("concurrent round-robin distribution=%v", counts)
	}
}

func TestRateLimitedCandidateUsesSingleHalfOpenProbeAfterCooldown(t *testing.T) {
	service := &Service{}
	past := time.Now().Add(-time.Second)
	candidate := storage.UpstreamGroupKey{
		ID: 7, Status: "rate_limited", FailureCount: 3, DisabledUntil: &past,
		ClientFormat: "openai", RequestMode: "responses", Ratio: 1,
	}
	if filtered := filterSchedulableCandidates([]storage.UpstreamGroupKey{candidate}); len(filtered) != 1 {
		t.Fatalf("expired rate-limited candidate was permanently filtered: %#v", filtered)
	}
	release, ok := service.tryAcquireScheduledCandidate(&candidate)
	if !ok {
		t.Fatal("first half-open probe was not acquired")
	}
	if secondRelease, secondOK := service.tryAcquireScheduledCandidate(&candidate); secondOK {
		secondRelease()
		t.Fatal("concurrent half-open probe was allowed")
	}
	release()
}

func TestRuntimeCooldownCanOnlyBeExtended(t *testing.T) {
	service := &Service{}
	long := time.Now().Add(time.Hour)
	short := time.Now().Add(time.Minute)
	service.recordRuntimeFailure(9, long)
	service.recordRuntimeFailure(9, short)
	got, ok := service.runtimeDisabledUntil(9)
	if !ok || got.Before(long.Add(-time.Millisecond)) {
		t.Fatalf("runtime cooldown shortened: got=%v want>=%v", got, long)
	}
}
