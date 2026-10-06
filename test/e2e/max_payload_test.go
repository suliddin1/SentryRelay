package e2e

import (
"github.com/suliddin1/SentryRelay/internal/retry"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
		"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	)

func TestIngest_MaxPayloadSize(t *testing.T) {
	env := setupE2E(t, 3, retry.DefaultPolicy())
	defer env.srv.Close(); defer env.db.Close()

	// Create 1MB exact payload
	exactSize := 1024 * 1024
	exactPayload := []byte(strings.Repeat("a", exactSize))

	// Create 1MB + 1 byte payload
	overPayload := []byte(strings.Repeat("a", exactSize+1))

	timestamp := time.Now().Unix()
	
	sendReq := func(payload []byte) int {
		req, _ := http.NewRequest(http.MethodPost, env.srv.URL+"/v1/ingest", bytes.NewReader(payload))
		req.Header.Set("X-SentryRelay-Tenant-ID", env.tenant.ID)
		req.Header.Set("X-SentryRelay-Idempotency-Key", uuid.NewString())
		req.Header.Set("X-SentryRelay-Destination-URL", "http://example.com")
		req.Header.Set("X-SentryRelay-Timestamp", fmt.Sprintf("%d", timestamp))

		mac := hmac.New(sha256.New, []byte(env.tenant.Secret))
		mac.Write([]byte(fmt.Sprintf("%d.", timestamp)))
		mac.Write(payload)
		signature := hex.EncodeToString(mac.Sum(nil))
		req.Header.Set("X-SentryRelay-Signature", signature)

		resp, _ := http.DefaultClient.Do(req)
		return resp.StatusCode
	}

	// 1. Exact Size
	if code := sendReq(exactPayload); code != http.StatusAccepted {
		t.Errorf("Expected 202 for exact size, got %d", code)
	}

	// 2. Over Size
	if code := sendReq(overPayload); code != http.StatusRequestEntityTooLarge {
		t.Errorf("Expected 413 for oversized payload, got %d", code)
	}
}
