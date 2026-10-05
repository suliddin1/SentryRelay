package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	adminToken = flag.String("admin-token", os.Getenv("SENTRYRELAY_ADMIN_TOKEN"), "Admin token for operator endpoints")
	endpoint = flag.String("endpoint", "http://localhost:8080", "SentryRelay API endpoint")
	client   = &http.Client{Timeout: 5 * time.Second}
)

func doRequest(method, path string, body []byte) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, fmt.Sprintf("%s%s", *endpoint, path), bodyReader)
	if err != nil {
		return nil, err
	}
	if *adminToken != "" {
		req.Header.Set("Authorization", "Bearer " + *adminToken)
	}
	return client.Do(req)
}

func main() {
	flag.Parse()
	args := flag.Args()

	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]
	switch command {
	case "status":
		handleStatus()
	case "queue":
		if len(args) < 2 {
			fmt.Println("Usage: sentryrelay-ctl queue inspect")
			os.Exit(1)
		}
		if args[1] == "inspect" {
			handleQueueInspect()
		} else {
			fmt.Println("Unknown queue command:", args[1])
			os.Exit(1)
		}
	case "dlq":
		if len(args) < 2 {
			fmt.Println("Usage: sentryrelay-ctl dlq list | replay <id>")
			os.Exit(1)
		}
		if args[1] == "list" {
			handleDLQList()
		} else if args[1] == "replay" {
			if len(args) < 3 {
				fmt.Println("Usage: sentryrelay-ctl dlq replay <event_id>")
				os.Exit(1)
			}
			handleDLQReplay(args[2])
		} else {
			fmt.Println("Unknown dlq command:", args[1])
			os.Exit(1)
		}
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`SentryRelay Operational CLI

Usage:
  sentryrelay-ctl [options] <command> [args...]

Commands:
  status                   Display system stats and active queue depth
  queue inspect            View pending and in-flight jobs
  dlq list                 List dead-lettered events with failure diagnostics
  dlq replay <event_id>    Replay a dead-lettered event

Options:`)
	flag.PrintDefaults()
}

func getJSON(path string, target interface{}) error {
	resp, err := doRequest(http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return json.NewDecoder(resp.Body).Decode(target)
}

func handleStatus() {
	var result map[string]interface{}
	if err := getJSON("/v1/status", &result); err != nil {
		fmt.Printf("Failed to get status: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("=== SentryRelay Status ===")
	fmt.Printf("Status:      %v\n", result["status"])
	fmt.Printf("Uptime:      %vs\n", result["uptime_seconds"])
	fmt.Printf("Active Jobs: %v (PENDING + RETRY_PENDING)\n", result["queue_depth_active"])
}

func handleQueueInspect() {
	var result struct {
		QueueJobs []map[string]interface{} `json:"queue_jobs"`
		Count     int                      `json:"count"`
	}

	if err := getJSON("/v1/queue?limit=20", &result); err != nil {
		fmt.Printf("Failed to inspect queue: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("=== Active Queue (Top %d) ===\n", result.Count)
	if result.Count == 0 {
		fmt.Println("Queue is empty.")
		return
	}

	for _, job := range result.QueueJobs {
		fmt.Printf("- JobID: %v | Status: %v | Attempts: %v/%v | Next Retry: %v\n",
			job["id"], job["status"], job["attempt_count"], job["max_attempts"], job["next_retry_at"])
	}
}

func handleDLQList() {
	var result struct {
		DeadLetterJobs []map[string]interface{} `json:"dead_letter_jobs"`
		Count          int                      `json:"count"`
	}

	if err := getJSON("/v1/dlq?limit=20", &result); err != nil {
		fmt.Printf("Failed to list DLQ: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("=== Dead-Letter Queue (Top %d) ===\n", result.Count)
	if result.Count == 0 {
		fmt.Println("DLQ is empty.")
		return
	}

	for _, job := range result.DeadLetterJobs {
		fmt.Printf("- JobID: %v\n", job["id"])
		fmt.Printf("  EventID: %v\n", job["event_id"])
		fmt.Printf("  Attempts: %v/%v\n", job["attempt_count"], job["max_attempts"])
		fmt.Printf("  Error Code: %v\n", job["last_error_code"])
		fmt.Printf("  Error Message: %v\n", job["last_error_message"])
		fmt.Printf("  Failed At: %v\n\n", job["updated_at"])
	}
}

func handleDLQReplay(jobID string) {
	resp, err := doRequest(http.MethodPost, fmt.Sprintf("/v1/dlq/%s/replay", jobID), nil)
	if err != nil {
		fmt.Printf("Request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Replay failed with status %d: %s\n", resp.StatusCode, strings.TrimSpace(string(body)))
		os.Exit(1)
	}

	fmt.Printf("Successfully replayed DLQ job: %s\n", jobID)
}
