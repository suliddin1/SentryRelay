package security

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

var (
	ErrInvalidDestinationScheme = errors.New("destination URL must have http or https scheme")
	ErrSSRFBlocked              = errors.New("destination URL resolves to a private, loopback, or cloud-metadata IP address")
	ErrEmptyDestinationHost     = errors.New("destination URL must have a valid host")
)

var blockedCIDRs []*net.IPNet

func init() {
	cidrs := []string{
		"127.0.0.0/8",     // IPv4 loopback
		"::1/128",         // IPv6 loopback
		"10.0.0.0/8",      // RFC1918
		"172.16.0.0/12",   // RFC1918
		"192.168.0.0/16",  // RFC1918
		"169.254.0.0/16",  // Link-local / AWS & GCP cloud metadata (169.254.169.254)
		"fc00::/7",        // Unique local IPv6
		"fe80::/10",       // Link-local IPv6
		"0.0.0.0/8",       // Current network
		"100.64.0.0/10",   // Carrier-grade NAT
		"192.0.0.0/24",    // IETF Protocol Assignments
		"192.0.2.0/24",    // TEST-NET-1
		"198.51.100.0/24", // TEST-NET-2
		"203.0.113.0/24",  // TEST-NET-3
		"240.0.0.0/4",     // Reserved
	}

	for _, cidr := range cidrs {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err == nil {
			blockedCIDRs = append(blockedCIDRs, ipNet)
		}
	}
}

// ValidateDestinationURL verifies that the target URL is a valid HTTP/HTTPS endpoint
// and is not pointing to private, loopback, or cloud-metadata networks (SSRF defense).
func ValidateDestinationURL(rawURL string, allowLocal bool) error {
	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return fmt.Errorf("invalid destination URL: %w", err)
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return ErrInvalidDestinationScheme
	}

	host := parsed.Hostname()
	if host == "" {
		return ErrEmptyDestinationHost
	}

	// In development or automated test mode, loopback/local can be explicitly permitted
	isLoopbackHost := strings.EqualFold(host, "localhost") || host == "127.0.0.1" || host == "::1"
	if isLoopbackHost {
		if allowLocal {
			return nil
		}
		return fmt.Errorf("%w: loopback address %s blocked", ErrSSRFBlocked, host)
	}

	// If host is a direct IP address
	if ip := net.ParseIP(host); ip != nil {
		// Cloud metadata (169.254.169.254) is ALWAYS blocked, even with allowLocal
		if isMetadataIP(ip) {
			return fmt.Errorf("%w: cloud metadata address %s", ErrSSRFBlocked, ip.String())
		}
		if ip.IsLoopback() {
			if allowLocal {
				return nil
			}
			return fmt.Errorf("%w: loopback address %s", ErrSSRFBlocked, ip.String())
		}
		if isBlockedIP(ip) {
			return fmt.Errorf("%w: private network address %s", ErrSSRFBlocked, ip.String())
		}
		return nil
	}

	// Resolve hostname to check for DNS rebinding / private IP resolution
	ips, err := net.LookupIP(host)
	if err != nil {
		// When allowLocal is enabled (e.g. unit/integration testing or offline development),
		// allow synthetic hostnames if DNS resolution is unavailable
		if allowLocal {
			return nil
		}
		return fmt.Errorf("failed to resolve destination host: %w", err)
	}

	for _, ip := range ips {
		if isMetadataIP(ip) {
			return fmt.Errorf("%w: host %s resolves to metadata IP %s", ErrSSRFBlocked, host, ip.String())
		}
		if ip.IsLoopback() {
			if !allowLocal {
				return fmt.Errorf("%w: host %s resolves to loopback IP %s", ErrSSRFBlocked, host, ip.String())
			}
			continue
		}
		if isBlockedIP(ip) {
			return fmt.Errorf("%w: host %s resolves to private IP %s", ErrSSRFBlocked, host, ip.String())
		}
	}

	return nil
}

func isMetadataIP(ip net.IP) bool {
	if ip.IsLinkLocalUnicast() {
		return true
	}
	return ip.Equal(net.ParseIP("169.254.169.254"))
}

func isBlockedIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsPrivate() {
		return true
	}

	for _, block := range blockedCIDRs {
		if block.Contains(ip) {
			return true
		}
	}

	return false
}
