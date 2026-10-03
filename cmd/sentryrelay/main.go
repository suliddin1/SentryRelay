package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/suliddin1/SentryRelay/internal/delivery"
	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/retry"
	"github.com/suliddin1/SentryRelay/internal/server"
	"github.com/suliddin1/SentryRelay/internal/storage/sqlite"
	"github.com/suliddin1/SentryRelay/internal/worker"
)

func main() {
	var (
		port           = flag.Int("port", 8080, "HTTP server port")
		dbPath         = flag.String("db", "sentryrelay.db", "Path to SQLite database file")
		numWorkers     = flag.Int("workers", 5, "Number of concurrent delivery workers")
		batchSize      = flag.Int("batch-size", 10, "Job claim batch size")
		leaseDuration  = flag.Duration("lease-duration", 30*time.Second, "Visibility lease duration for in-flight jobs")
		reaperInterval = flag.Duration("reaper-interval", 5*time.Second, "Interval for stale lease recovery reaper")
		seedDevTenant  = flag.Bool("seed-dev-tenant", false, "Seed a default development tenant if absent")
		allowLocalDest = flag.Bool("allow-local-destinations", false, "Allow delivery to localhost/loopback destinations (dev/test only)")
	)
	flag.Parse()

	log.Printf("[INFO] Initializing SentryRelay (Storage: %s, Workers: %d)...", *dbPath, *numWorkers)

	db, err := sqlite.Open(*dbPath)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize SQLite storage: %v", err)
	}
	defer db.Close()

	// Seed dev tenant if requested
	if *seedDevTenant {
		seedTenant(db)
	}

	// Delivery client
	client := delivery.NewClient(delivery.WithTimeout(15 * time.Second))

	// Worker Pool & Lease Reaper
	workerCfg := worker.Config{
		NumWorkers:     *numWorkers,
		BatchSize:      *batchSize,
		PollInterval:   100 * time.Millisecond,
		LeaseDuration:  *leaseDuration,
		ReaperInterval: *reaperInterval,
		RetryPolicy:    retry.DefaultPolicy(),
	}
	pool := worker.NewPool(workerCfg, db, client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		log.Fatalf("[FATAL] Failed to start worker pool: %v", err)
	}
	log.Printf("[INFO] Worker pool and lease reaper started successfully")

	// Ingestion HTTP Server
	srvConfig := server.Config{
		DefaultMaxRetry:        5,
		ReplayTolerance:        5 * time.Minute,
		AllowLocalDestinations: *allowLocalDest || *seedDevTenant,
	}
	srv := server.NewServer(srvConfig, db)

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      srv.Handler(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Background HTTP server listener
	go func() {
		log.Printf("[INFO] SentryRelay HTTP server listening on http://localhost:%d", *port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] HTTP server error: %v", err)
		}
	}()

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	sig := <-sigChan
	log.Printf("[INFO] Received signal %v, commencing graceful shutdown...", sig)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[WARN] HTTP server shutdown error: %v", err)
	}

	pool.Stop()
	log.Printf("[INFO] SentryRelay terminated cleanly.")
}

func seedTenant(db *sqlite.DB) {
	ctx := context.Background()
	tenantID := "dev_tenant"
	_, err := db.GetTenant(ctx, tenantID)
	if err == nil {
		return
	}

	secret := "dev_secret_key_" + uuid.NewString()[:8]
	tenant := &model.Tenant{
		ID:        tenantID,
		Name:      "Development Tenant",
		Secret:    secret,
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
	}
	if err := db.CreateTenant(ctx, tenant); err != nil {
		log.Printf("[WARN] Could not seed dev tenant: %v", err)
		return
	}

	log.Printf("[INFO] Seeded development tenant:")
	log.Printf("       Tenant ID: %s", tenant.ID)
	log.Printf("       Secret:    %s", tenant.Secret)
}
