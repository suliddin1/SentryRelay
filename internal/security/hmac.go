package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/suliddin1/SentryRelay/internal/model"
)

// DefaultTimestampTolerance is the default allowable drift for webhook request timestamps.
const DefaultTimestampTolerance = 5 * time.Minute

// Sign computes the HMAC-SHA256 signature for a timestamped payload using the secret.
// Format signed: "{timestamp}.{payload}"
func Sign(secret string, timestamp int64, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte("."))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// Verify checks both the timestamp freshness and cryptographic validity of an HMAC signature.
func Verify(secret string, signature string, timestamp int64, payload []byte, tolerance time.Duration, now time.Time) error {
	if tolerance <= 0 {
		tolerance = DefaultTimestampTolerance
	}

	reqTime := time.Unix(timestamp, 0)
	diff := now.Sub(reqTime)
	if diff < -tolerance || diff > tolerance {
		return fmt.Errorf("%w: timestamp drift of %s exceeds allowed tolerance %s", model.ErrTimestampOutOfRange, diff, tolerance)
	}

	// Clean any prefix like "sha256=" or "v1="
	cleanSig := signature
	if idx := strings.Index(cleanSig, "="); idx != -1 {
		cleanSig = cleanSig[idx+1:]
	}

	expectedSig := Sign(secret, timestamp, payload)

	if subtle.ConstantTimeCompare([]byte(cleanSig), []byte(expectedSig)) != 1 {
		return model.ErrInvalidSignature
	}

	return nil
}
