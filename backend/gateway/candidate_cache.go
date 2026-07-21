package gateway

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/oauthpool"
	"github.com/bejix/upstream-ops/backend/storage"
)

const candidateSnapshotTTL = 30 * time.Second

// candidateSnapshot is immutable after publication. Request handlers may read
// the slice without locking; filtering/order helpers must allocate before
// changing its order.
type candidateSnapshot struct {
	loadedAt   time.Time
	generation uint64
	items      []storage.UpstreamGroupKey
}

type candidateSnapshotCache struct {
	mu      sync.Mutex
	value   atomic.Pointer[candidateSnapshot]
	version atomic.Uint64
}

func (c *candidateSnapshotCache) invalidate() {
	if c != nil {
		c.version.Add(1)
	}
}

func (c *candidateSnapshotCache) load(now time.Time, loader func(time.Time) ([]storage.UpstreamGroupKey, error)) ([]storage.UpstreamGroupKey, error) {
	if now.IsZero() {
		now = time.Now()
	}
	version := c.version.Load()
	if current := c.value.Load(); current != nil && current.generation == version && now.Sub(current.loadedAt) < candidateSnapshotTTL {
		return current.items, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	version = c.version.Load()
	if current := c.value.Load(); current != nil && current.generation == version && now.Sub(current.loadedAt) < candidateSnapshotTTL {
		return current.items, nil
	}
	items, err := loader(now)
	if err != nil {
		return nil, err
	}
	// Own the backing array so a repository/test caller cannot mutate a
	// published snapshot after the lock is released.
	snapshot := &candidateSnapshot{loadedAt: now, generation: version, items: append([]storage.UpstreamGroupKey(nil), items...)}
	c.value.Store(snapshot)
	return snapshot.items, nil
}

// InvalidateSchedulingCache makes the next request rebuild the immutable
// candidate snapshot once. It is intentionally cheap and safe to call after
// every channel/group state mutation.
func (s *Service) InvalidateSchedulingCache() {
	if s != nil {
		s.candidateCache.invalidate()
	}
}

func (s *Service) schedulingCandidates(now time.Time, model string) ([]storage.UpstreamGroupKey, error) {
	items, err := s.candidateCache.load(now, s.groupKeys.ListCandidates)
	if err != nil {
		return nil, err
	}
	poolService := s.oauthPoolService()
	if poolService == nil {
		return items, nil
	}
	filtered := make([]storage.UpstreamGroupKey, 0, len(items))
	for i := range items {
		candidate := items[i]
		pool, fixed := oauthPoolForCandidate(&candidate)
		if !fixed {
			filtered = append(filtered, candidate)
			continue
		}
		available, checkErr := poolService.HasAvailable(context.Background(), pool, model)
		if checkErr != nil {
			if errors.Is(checkErr, oauthpool.ErrUnsupportedModel) {
				continue
			}
			// Keep the candidate so the normal attempt path returns a recognizable
			// pool/repository error instead of hiding an infrastructure fault.
			filtered = append(filtered, candidate)
			continue
		}
		if available {
			filtered = append(filtered, candidate)
		}
	}
	return filtered, nil
}
