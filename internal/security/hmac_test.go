package security

import (
	"errors"
	"testing"
	"time"

	"github.com/suliddin1/SentryRelay/internal/model"
)

func TestSignAndVerify(t *testing.T) {
	secret := "whsec_test_secret_key_12345"
	payload := []byte(`{"event":"order.created","amount":9900}`)
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	timestamp := now.Unix()

	sig := Sign(secret, timestamp, payload)
	if sig == "" {
		t.Fatal("expected non-empty signature")
	}

	// 1. Valid signature
	err := Verify(secret, sig, timestamp, payload, 5*time.Minute, now)
	if err != nil {
		t.Fatalf("expected signature to be valid, got: %v", err)
	}

	// 2. Valid with prefix
	err = Verify(secret, "sha256="+sig, timestamp, payload, 5*time.Minute, now)
	if err != nil {
		t.Fatalf("expected prefixed signature to be valid, got: %v", err)
	}

	// 3. Wrong secret
	err = Verify("wrong_secret", sig, timestamp, payload, 5*time.Minute, now)
	if !errors.Is(err, model.ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature for wrong secret, got: %v", err)
	}

	// 4. Altered payload
	tamperedPayload := []byte(`{"event":"order.created","amount":10000}`)
	err = Verify(secret, sig, timestamp, tamperedPayload, 5*time.Minute, now)
	if !errors.Is(err, model.ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature for tampered payload, got: %v", err)
	}

	// 5. Expired timestamp (older than tolerance)
	oldTimestamp := now.Add(-10 * time.Minute).Unix()
	oldSig := Sign(secret, oldTimestamp, payload)
	err = Verify(secret, oldSig, oldTimestamp, payload, 5*time.Minute, now)
	if !errors.Is(err, model.ErrTimestampOutOfRange) {
		t.Fatalf("expected ErrTimestampOutOfRange for expired timestamp, got: %v", err)
	}

	// 6. Future timestamp (beyond tolerance)
	futureTimestamp := now.Add(10 * time.Minute).Unix()
	futureSig := Sign(secret, futureTimestamp, payload)
	err = Verify(secret, futureSig, futureTimestamp, payload, 5*time.Minute, now)
	if !errors.Is(err, model.ErrTimestampOutOfRange) {
		t.Fatalf("expected ErrTimestampOutOfRange for future timestamp, got: %v", err)
	}
}
