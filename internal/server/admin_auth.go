package server

import (
	"net/http"
	"strings"
)

func (s *Server) adminAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.adminToken == "" {
			// If no token configured, block all access by default
			writeError(w, http.StatusUnauthorized, "admin endpoints disabled")
			return
		}
		
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			writeError(w, http.StatusUnauthorized, "missing authorization header")
			return
		}
		
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			writeError(w, http.StatusUnauthorized, "invalid authorization format")
			return
		}
		
		if parts[1] != s.adminToken {
			writeError(w, http.StatusUnauthorized, "invalid admin token")
			return
		}
		
		next.ServeHTTP(w, r)
	})
}
