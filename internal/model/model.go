package model

import (
	"errors"
	"time"
)

// DeliveryStatus represents the distinct lifecycle states of a delivery job.
type DeliveryStatus string

const (
	StatusPending      DeliveryStatus = "pending"
	StatusInFlight     DeliveryStatus = "in_flight"
	StatusDelivered    DeliveryStatus = "delivered"
	StatusRetryPending DeliveryStatus = "retry_pending"
	StatusDeadLetter   DeliveryStatus = "dead_letter"
)

var (
	ErrEventNotFound       = errors.New("event not found")
	ErrJobNotFound         = errors.New("delivery job not found")
	ErrDuplicateEvent      = errors.New("duplicate event with same idempotency key")
	ErrInvalidSignature    = errors.New("invalid signature")
	ErrTimestampOutOfRange = errors.New("timestamp outside acceptable replay window")
	ErrTenantNotFound      = errors.New("tenant not found")
	ErrTenantDisabled      = errors.New("tenant disabled")
)

// Tenant represents an authorized webhook publisher.
type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Secret    string    `json:"secret"` // Shared secret for HMAC-SHA256 signature verification
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
}

// Event represents an ingested webhook event.
type Event struct {
	ID             string            `json:"id"`
	TenantID       string            `json:"tenant_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	DestinationURL string            `json:"destination_url"`
	Payload        []byte            `json:"payload"`
	Headers        map[string]string `json:"headers"`
	CreatedAt      time.Time         `json:"created_at"`
}

// DeliveryJob represents the state and retry tracking for delivering an event to its destination.
type DeliveryJob struct {
	ID               string         `json:"id"`
	EventID          string         `json:"event_id"`
	Status           DeliveryStatus `json:"status"`
	AttemptCount     int            `json:"attempt_count"`
	MaxAttempts      int            `json:"max_attempts"`
	NextRetryAt      time.Time      `json:"next_retry_at"`
	LeasedAt         *time.Time     `json:"leased_at,omitempty"`
	LeasedUntil      *time.Time     `json:"leased_until,omitempty"`
	LastErrorCode    string         `json:"last_error_code,omitempty"`
	LastErrorMessage string         `json:"last_error_message,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

// DeliveryAttempt records an individual HTTP attempt to deliver a webhook payload.
type DeliveryAttempt struct {
	ID                  string    `json:"id"`
	JobID               string    `json:"job_id"`
	AttemptNumber       int       `json:"attempt_number"`
	StatusCode          int       `json:"status_code"`
	ExecutionDurationMs int64     `json:"execution_duration_ms"`
	ErrorMessage        string    `json:"error_message,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
}
