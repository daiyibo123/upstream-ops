package oauthadmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/oauthpool"
	"github.com/bejix/upstream-ops/backend/storage"
	"gorm.io/gorm"
)

const inspectionWorkers = 2

// Service coordinates operator-facing account mutations with the hot-path
// pool snapshot. It deliberately owns only two inspection workers per job so
// a manual click cannot create an unbounded number of goroutines on a small
// server.
type Service struct {
	accounts *storage.OAuthAccounts
	pool     *oauthpool.Service

	mu              sync.Mutex
	jobs            map[storage.OAuthPool]*InspectionJob
	seq             atomic.Uint64
	inspectionSlots chan struct{}
}

type InspectionJob struct {
	ID        uint64            `json:"id"`
	Pool      storage.OAuthPool `json:"pool"`
	Status    string            `json:"status"`
	Total     int               `json:"total"`
	Completed int               `json:"completed"`
	Alive     int               `json:"alive"`
	Limited   int               `json:"limited"`
	Dead      int               `json:"dead"`
	Cooling   int               `json:"cooling"`
	Failed    int               `json:"failed"`
	StartedAt time.Time         `json:"started_at"`
	EndedAt   *time.Time        `json:"ended_at,omitempty"`
	LastError string            `json:"last_error,omitempty"`

	mu sync.RWMutex
}

func (j *InspectionJob) MarshalJSON() ([]byte, error) {
	if j == nil {
		return []byte("null"), nil
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	type view struct {
		ID        uint64            `json:"id"`
		Pool      storage.OAuthPool `json:"pool"`
		Status    string            `json:"status"`
		Total     int               `json:"total"`
		Completed int               `json:"completed"`
		Alive     int               `json:"alive"`
		Succeeded int               `json:"succeeded"`
		Limited   int               `json:"limited"`
		Dead      int               `json:"dead"`
		Cooling   int               `json:"cooling"`
		Failed    int               `json:"failed"`
		StartedAt time.Time         `json:"started_at"`
		EndedAt   *time.Time        `json:"ended_at,omitempty"`
		LastError string            `json:"last_error,omitempty"`
		Error     string            `json:"error,omitempty"`
	}
	return json.Marshal(view{
		ID: j.ID, Pool: j.Pool, Status: j.Status, Total: j.Total, Completed: j.Completed,
		Alive: j.Alive, Succeeded: j.Alive, Limited: j.Limited, Dead: j.Dead, Cooling: j.Cooling, Failed: j.Failed,
		StartedAt: j.StartedAt, EndedAt: j.EndedAt, LastError: j.LastError, Error: j.LastError,
	})
}

func New(accounts *storage.OAuthAccounts, pool *oauthpool.Service) *Service {
	return &Service{
		accounts: accounts, pool: pool, jobs: make(map[storage.OAuthPool]*InspectionJob),
		inspectionSlots: make(chan struct{}, inspectionWorkers),
	}
}

func (s *Service) Accounts() *storage.OAuthAccounts { return s.accounts }

func (s *Service) Import(pool storage.OAuthPool, raw []byte) (storage.OAuthImportResult, *InspectionJob, error) {
	if s == nil || s.accounts == nil {
		return storage.OAuthImportResult{}, nil, errors.New("OAuth account service is not configured")
	}
	result, err := s.accounts.ImportJSON(pool, raw)
	if err != nil {
		return result, nil, err
	}
	ids := make([]uint, 0, result.Succeeded)
	for _, item := range result.Items {
		if item.AccountID != 0 && item.Status != "failed" {
			ids = append(ids, item.AccountID)
		}
	}
	s.invalidate(pool, ids...)
	job, _ := s.StartInspection(pool, ids)
	return result, job, nil
}

func (s *Service) Delete(pool storage.OAuthPool, id uint) error {
	if s == nil || s.accounts == nil {
		return errors.New("OAuth account service is not configured")
	}
	err := s.accounts.Delete(pool, id)
	if err == nil {
		s.invalidate(pool, id)
	}
	return err
}

func (s *Service) BatchDelete(pool storage.OAuthPool, ids []uint) (storage.OAuthBatchDeleteResult, error) {
	if s == nil || s.accounts == nil {
		return storage.OAuthBatchDeleteResult{}, errors.New("OAuth account service is not configured")
	}
	result, err := s.accounts.BatchDelete(pool, ids)
	if err == nil {
		s.invalidate(pool, ids...)
	}
	return result, err
}

func (s *Service) CheckOne(ctx context.Context, pool storage.OAuthPool, id uint) (storage.OAuthAccount, storage.OAuthHealthResult, error) {
	if s == nil || s.accounts == nil || s.pool == nil {
		return storage.OAuthAccount{}, storage.OAuthHealthResult{}, errors.New("OAuth account service is not configured")
	}
	account, err := s.accounts.Find(pool, id)
	if err != nil {
		return storage.OAuthAccount{}, storage.OAuthHealthResult{}, err
	}
	credentials, err := s.accounts.Credentials(pool, id)
	if err != nil {
		return storage.OAuthAccount{}, storage.OAuthHealthResult{}, err
	}
	result := s.pool.Check(ctx, pool, account, credentials)
	checkedAt := time.Now().UTC()
	if result.Transient {
		if err := s.accounts.RecordRuntimeFailure(pool, id, storage.OAuthStatusCooling, result.Error, result.DisabledUntil, 3); err != nil {
			return storage.OAuthAccount{}, result, err
		}
	} else if err := s.accounts.ApplyHealthResult(pool, id, result, checkedAt); err != nil {
		return storage.OAuthAccount{}, result, err
	}
	s.invalidate(pool, id)
	updated, err := s.accounts.Find(pool, id)
	if err == nil && result.Transient {
		result.Status = updated.Status
		result.Schedulable = updated.CurrentlySchedulable(checkedAt)
		result.DisabledUntil = updated.DisabledUntil
	}
	return updated, result, err
}

func (s *Service) QueryQuota(ctx context.Context, pool storage.OAuthPool, id uint) (storage.OAuthAccount, oauthpool.QuotaResult, error) {
	if s == nil || s.accounts == nil || s.pool == nil {
		return storage.OAuthAccount{}, oauthpool.QuotaResult{}, errors.New("OAuth account service is not configured")
	}
	account, err := s.accounts.Find(pool, id)
	if err != nil {
		return storage.OAuthAccount{}, oauthpool.QuotaResult{}, err
	}
	credentials, err := s.accounts.Credentials(pool, id)
	if err != nil {
		return storage.OAuthAccount{}, oauthpool.QuotaResult{}, err
	}
	quota, err := s.pool.QueryQuota(ctx, pool, account, credentials)
	if err != nil {
		return storage.OAuthAccount{}, oauthpool.QuotaResult{}, err
	}
	if err := s.accounts.UpdateQuota(pool, id, quota.Used, quota.Limit, quota.Unit, quota.ResetAt); err != nil {
		return storage.OAuthAccount{}, quota, err
	}
	updated, err := s.accounts.Find(pool, id)
	return updated, quota, err
}

// StartInspection is idempotent per pool while a job is running. Supplying no
// IDs inspects the whole pool; imports pass only the affected IDs.
func (s *Service) StartInspection(pool storage.OAuthPool, ids []uint) (*InspectionJob, bool) {
	if s == nil || s.accounts == nil || s.pool == nil {
		return nil, false
	}
	s.mu.Lock()
	if current := s.jobs[pool]; current != nil {
		current.mu.RLock()
		running := current.Status == "running"
		current.mu.RUnlock()
		if running {
			s.mu.Unlock()
			return current, false
		}
	}
	job := &InspectionJob{ID: s.seq.Add(1), Pool: pool, Status: "running", StartedAt: time.Now().UTC()}
	s.jobs[pool] = job
	s.mu.Unlock()

	go s.runInspection(job, append([]uint(nil), ids...))
	return job, true
}

func (s *Service) Inspection(pool storage.OAuthPool) *InspectionJob {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	job := s.jobs[pool]
	s.mu.Unlock()
	return job
}

func (s *Service) runInspection(job *InspectionJob, ids []uint) {
	if len(ids) == 0 {
		var err error
		ids, err = s.accounts.ListIDs(job.Pool)
		if err != nil {
			s.finishJobWithError(job, err)
			return
		}
	}
	ids = uniqueIDs(ids)
	job.mu.Lock()
	job.Total = len(ids)
	job.mu.Unlock()
	if len(ids) == 0 {
		s.finishJob(job)
		return
	}

	queue := make(chan uint)
	var workers sync.WaitGroup
	for worker := 0; worker < inspectionWorkers; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for id := range queue {
				s.inspectionSlots <- struct{}{}
				ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
				_, result, err := s.CheckOne(ctx, job.Pool, id)
				cancel()
				<-s.inspectionSlots
				s.recordInspectionResult(job, result, err)
			}
		}()
	}
	for _, id := range ids {
		queue <- id
	}
	close(queue)
	workers.Wait()
	s.finishJob(job)
}

func (s *Service) recordInspectionResult(job *InspectionJob, result storage.OAuthHealthResult, err error) {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.Completed++
	if err != nil {
		job.Failed++
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			job.LastError = fmt.Sprintf("account check failed: %v", err)
		}
		return
	}
	switch result.Status {
	case storage.OAuthStatusAlive:
		job.Alive++
	case storage.OAuthStatusRateLimited:
		job.Limited++
	case storage.OAuthStatusDead:
		job.Dead++
	case storage.OAuthStatusCooling:
		job.Cooling++
	default:
		job.Failed++
	}
}

func (s *Service) finishJobWithError(job *InspectionJob, err error) {
	job.mu.Lock()
	job.Status = "failed"
	job.LastError = err.Error()
	now := time.Now().UTC()
	job.EndedAt = &now
	job.mu.Unlock()
}

func (s *Service) finishJob(job *InspectionJob) {
	job.mu.Lock()
	job.Status = "completed"
	now := time.Now().UTC()
	job.EndedAt = &now
	job.mu.Unlock()
}

func (s *Service) invalidate(pool storage.OAuthPool, ids ...uint) {
	if s.pool != nil {
		s.pool.Invalidate(pool, ids...)
	}
}

func uniqueIDs(ids []uint) []uint {
	out := make([]uint, 0, len(ids))
	seen := make(map[uint]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
