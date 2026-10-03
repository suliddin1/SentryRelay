package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/suliddin1/SentryRelay/internal/delivery"
	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/retry"
	"github.com/suliddin1/SentryRelay/internal/storage/sqlite"
)

// Config configures the worker pool and lease reaper.
type Config struct {
	NumWorkers     int
	BatchSize      int
	PollInterval   time.Duration
	LeaseDuration  time.Duration
	ReaperInterval time.Duration
	RetryPolicy    retry.Policy
}

// DefaultConfig provides recommended production settings.
func DefaultConfig() Config {
	return Config{
		NumWorkers:     5,
		BatchSize:      10,
		PollInterval:   100 * time.Millisecond,
		LeaseDuration:  30 * time.Second,
		ReaperInterval: 5 * time.Second,
		RetryPolicy:    retry.DefaultPolicy(),
	}
}

// Pool manages concurrent dispatch of delivery jobs and lease reclamation.
type Pool struct {
	cfg      Config
	db       *sqlite.DB
	client   *delivery.Client
	stopCh   chan struct{}
	wg       sync.WaitGroup
	startMut sync.Mutex
	running  bool
}

// NewPool initializes a new worker pool.
func NewPool(cfg Config, db *sqlite.DB, client *delivery.Client) *Pool {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 5
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.ReaperInterval <= 0 {
		cfg.ReaperInterval = 5 * time.Second
	}

	return &Pool{
		cfg:    cfg,
		db:     db,
		client: client,
		stopCh: make(chan struct{}),
	}
}

// Start spawns the background worker dispatchers and the stale lease reaper.
func (p *Pool) Start(ctx context.Context) error {
	p.startMut.Lock()
	defer p.startMut.Unlock()

	if p.running {
		return fmt.Errorf("worker pool is already running")
	}
	p.running = true

	// Channel for distributing claimed jobs among workers
	jobQueue := make(chan *model.DeliveryJob, p.cfg.BatchSize*2)

	// 1. Start worker goroutines
	for i := 0; i < p.cfg.NumWorkers; i++ {
		p.wg.Add(1)
		go p.workerLoop(ctx, jobQueue)
	}

	// 2. Start dispatcher goroutine
	p.wg.Add(1)
	go p.dispatcherLoop(ctx, jobQueue)

	// 3. Start lease reaper goroutine
	p.wg.Add(1)
	go p.reaperLoop(ctx)

	return nil
}

// Stop gracefully signals all workers and waits for in-flight tasks to complete.
func (p *Pool) Stop() {
	p.startMut.Lock()
	if !p.running {
		p.startMut.Unlock()
		return
	}
	p.running = false
	close(p.stopCh)
	p.startMut.Unlock()

	p.wg.Wait()
}

func (p *Pool) dispatcherLoop(ctx context.Context, jobQueue chan<- *model.DeliveryJob) {
	defer p.wg.Done()
	defer close(jobQueue)

	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Claim up to batch size
			now := time.Now().UTC()
			jobs, err := p.db.ClaimJobs(ctx, p.cfg.BatchSize, p.cfg.LeaseDuration, now)
			if err != nil {
				// Back off slightly on database error
				time.Sleep(100 * time.Millisecond)
				continue
			}

			for _, job := range jobs {
				select {
				case jobQueue <- job:
				case <-p.stopCh:
					return
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (p *Pool) workerLoop(ctx context.Context, jobQueue <-chan *model.DeliveryJob) {
	defer p.wg.Done()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ctx.Done():
			return
		case job, ok := <-jobQueue:
			if !ok {
				return
			}
			// Skip jobs whose visibility lease expired while waiting in memory
			if job.LeasedUntil != nil && time.Now().UTC().After(*job.LeasedUntil) {
				continue
			}
			p.processJob(ctx, job)
		}
	}
}

func (p *Pool) processJob(ctx context.Context, job *model.DeliveryJob) {
	// 1. Fetch parent event
	jobWithEvent, event, err := p.db.GetJobWithEvent(ctx, job.ID)
	if err != nil {
		return
	}

	// 2. Deliver payload to destination
	res := p.client.Deliver(ctx, jobWithEvent, event)

	// 3. Determine next state based on classification and attempt limit
	var nextStatus model.DeliveryStatus
	var nextRetryAt time.Time
	var errCode, errMsg string

	attemptNum := jobWithEvent.AttemptCount + 1

	switch res.Classification {
	case retry.ClassificationSuccess:
		nextStatus = model.StatusDelivered

	case retry.ClassificationPermanent:
		nextStatus = model.StatusDeadLetter
		errCode = "PERMANENT_ERROR"
		errMsg = res.Attempt.ErrorMessage

	case retry.ClassificationTransient:
		if attemptNum >= jobWithEvent.MaxAttempts {
			nextStatus = model.StatusDeadLetter
			errCode = "MAX_ATTEMPTS_EXCEEDED"
			errMsg = fmt.Sprintf("Exceeded max retry attempts (%d): %s", jobWithEvent.MaxAttempts, res.Attempt.ErrorMessage)
		} else {
			nextStatus = model.StatusRetryPending
			backoff := p.cfg.RetryPolicy.BackoffDuration(attemptNum)
			nextRetryAt = time.Now().UTC().Add(backoff)
			errCode = "TRANSIENT_ERROR"
			errMsg = res.Attempt.ErrorMessage
		}
	}

	// 4. Persist delivery attempt and state transition atomically with lease fencing
	err = p.db.RecordAttempt(ctx, res.Attempt, jobWithEvent.LeasedUntil, nextStatus, nextRetryAt, errCode, errMsg)
	if err != nil {
		if errors.Is(err, model.ErrLeaseLost) {
			log.Printf("[WARN] Delivery attempt for job %s completed after lease expiration; state transition discarded", job.ID)
			return
		}
		log.Printf("[ERROR] Failed to record delivery attempt for job %s: %v", job.ID, err)
	}
}

func (p *Pool) reaperLoop(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.cfg.ReaperInterval)
	defer ticker.Stop()

	// Run initial reap immediately on startup to recover any crashed worker leases
	_, _ = p.db.ReapStaleLeases(ctx, time.Now().UTC())

	for {
		select {
		case <-p.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = p.db.ReapStaleLeases(ctx, time.Now().UTC())
		}
	}
}
