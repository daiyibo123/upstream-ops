package gateway

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestCandidateSnapshotCacheAvoidsPerRequestReload(t *testing.T) {
	var cache candidateSnapshotCache
	var loads atomic.Int32
	loader := func(time.Time) ([]storage.UpstreamGroupKey, error) {
		loads.Add(1)
		return []storage.UpstreamGroupKey{{ID: 1, GroupName: "primary"}}, nil
	}
	now := time.Now()
	first, err := cache.load(now, loader)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.load(now.Add(time.Second), loader)
	if err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 1 {
		t.Fatalf("loader called %d times, want 1", loads.Load())
	}
	if len(first) != 1 || len(second) != 1 || first[0].ID != second[0].ID {
		t.Fatalf("unexpected cached candidates: %#v %#v", first, second)
	}
}

func TestCandidateSnapshotCacheInvalidationReloadsOnceConcurrently(t *testing.T) {
	var cache candidateSnapshotCache
	var loads atomic.Int32
	loader := func(time.Time) ([]storage.UpstreamGroupKey, error) {
		count := loads.Add(1)
		time.Sleep(5 * time.Millisecond)
		return []storage.UpstreamGroupKey{{ID: uint(count)}}, nil
	}
	if _, err := cache.load(time.Now(), loader); err != nil {
		t.Fatal(err)
	}
	cache.invalidate()

	const readers = 32
	var wg sync.WaitGroup
	results := make(chan uint, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			items, err := cache.load(time.Now(), loader)
			if err != nil {
				t.Errorf("load: %v", err)
				return
			}
			results <- items[0].ID
		}()
	}
	wg.Wait()
	close(results)
	if loads.Load() != 2 {
		t.Fatalf("loader called %d times after concurrent invalidation, want 2 total", loads.Load())
	}
	for id := range results {
		if id != 2 {
			t.Fatalf("reader observed snapshot %d, want refreshed snapshot 2", id)
		}
	}
}

func TestCandidateSnapshotCacheCachesEmptySnapshot(t *testing.T) {
	var cache candidateSnapshotCache
	var loads atomic.Int32
	loader := func(time.Time) ([]storage.UpstreamGroupKey, error) {
		loads.Add(1)
		return nil, nil
	}
	if _, err := cache.load(time.Now(), loader); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.load(time.Now(), loader); err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 1 {
		t.Fatalf("empty snapshot loader called %d times, want 1", loads.Load())
	}
}

func TestCandidateSnapshotCacheDoesNotLoseInvalidationDuringRefresh(t *testing.T) {
	var cache candidateSnapshotCache
	started := make(chan struct{})
	release := make(chan struct{})
	var loads atomic.Int32
	loader := func(time.Time) ([]storage.UpstreamGroupKey, error) {
		count := loads.Add(1)
		if count == 1 {
			close(started)
			<-release
		}
		return []storage.UpstreamGroupKey{{ID: uint(count)}}, nil
	}
	done := make(chan struct{})
	go func() {
		_, _ = cache.load(time.Now(), loader)
		close(done)
	}()
	<-started
	cache.invalidate()
	close(release)
	<-done
	items, err := cache.load(time.Now(), loader)
	if err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 2 || len(items) != 1 || items[0].ID != 2 {
		t.Fatalf("invalidation during refresh was lost: loads=%d items=%#v", loads.Load(), items)
	}
}

func BenchmarkCandidateSnapshotCacheHit(b *testing.B) {
	var cache candidateSnapshotCache
	loader := func(time.Time) ([]storage.UpstreamGroupKey, error) {
		return []storage.UpstreamGroupKey{{ID: 1}, {ID: 2}}, nil
	}
	if _, err := cache.load(time.Now(), loader); err != nil {
		b.Fatal(err)
	}
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cache.load(now, loader); err != nil {
			b.Fatal(err)
		}
	}
}
