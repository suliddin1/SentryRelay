# Contributing to SentryRelay

Thank you for your interest in contributing to SentryRelay!

## Development Setup

### Prerequisites
- Go 1.24+ (pure Go SQLite driver, no CGO or C compiler required)
- Git

### Running Tests
Ensure all unit and integration tests pass cleanly:
```bash
go test -v -race ./...
```

### Running Benchmarks
Verify throughput and latency performance invariants:
```bash
go test -bench . -benchmem ./test/benchmark
```

### Building the Project
Compile the server daemon and the CLI utility:
```bash
go build -v ./cmd/sentryrelay
go build -v ./cmd/sentryrelay-ctl
```

## Pull Request Guidelines

1. **Keep Changes Focused**: Each pull request should address a single bug fix, feature, or documentation enhancement.
2. **Preserve Invariants**: Ensure architectural invariants (WAL transactions, lease fencing, idempotency keys, integer epoch timestamps) are maintained.
3. **Verify Tests**: Run `go test -v ./...` before submitting your PR.
4. **Follow Formatting**: Keep code formatted with standard `gofmt` and adhere to [.editorconfig](.editorconfig).
