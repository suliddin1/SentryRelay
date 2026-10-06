package security

import (
	"context"
		"testing"
)

func TestSSRF_0000(t *testing.T) {
	dialer := SafeDialContext(false)
	_, err := dialer(context.Background(), "tcp", "0.0.0.0:8080")
	if err == nil {
		t.Fatalf("CRITICAL: 0.0.0.0 was NOT blocked!")
	}
	t.Logf("Blocked 0.0.0.0 successfully: %v", err)
}
