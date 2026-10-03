package security

import (
	"errors"
	"testing"
)

func TestValidateDestinationURL(t *testing.T) {
	cases := []struct {
		url        string
		allowLocal bool
		expectErr  error
	}{
		// Valid public endpoints
		{"https://example.com/webhook", false, nil},
		{"http://example.com:8080/events", false, nil},

		// Loopback blocked by default
		{"http://127.0.0.1:8080/hook", false, ErrSSRFBlocked},
		{"http://localhost/hook", false, ErrSSRFBlocked},

		// Loopback allowed when explicitly requested
		{"http://127.0.0.1:8080/hook", true, nil},
		{"http://localhost:8080/hook", true, nil},

		// Cloud metadata blocked
		{"http://169.254.169.254/latest/meta-data/", false, ErrSSRFBlocked},
		{"http://169.254.169.254/latest/meta-data/", true, ErrSSRFBlocked},

		// Private RFC1918 networks blocked
		{"http://10.0.1.5/webhook", false, ErrSSRFBlocked},
		{"http://192.168.1.1/webhook", false, ErrSSRFBlocked},
		{"http://172.16.0.5/webhook", false, ErrSSRFBlocked},

		// Invalid schemes
		{"ftp://example.com/file", false, ErrInvalidDestinationScheme},
		{"file:///etc/passwd", false, ErrInvalidDestinationScheme},
		{"javascript:alert(1)", false, ErrInvalidDestinationScheme},
	}

	for _, tc := range cases {
		err := ValidateDestinationURL(tc.url, tc.allowLocal)
		if tc.expectErr == nil {
			if err != nil {
				t.Errorf("ValidateDestinationURL(%q, %v) expected nil, got: %v", tc.url, tc.allowLocal, err)
			}
		} else {
			if !errors.Is(err, tc.expectErr) {
				t.Errorf("ValidateDestinationURL(%q, %v) expected %v, got: %v", tc.url, tc.allowLocal, tc.expectErr, err)
			}
		}
	}
}
