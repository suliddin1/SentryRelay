.PHONY: all build test test-race bench clean tidy

BINARY_DIR := bin
SERVER_BIN := $(BINARY_DIR)/sentryrelay
CTL_BIN := $(BINARY_DIR)/sentryrelay-ctl

all: build

build:
	@mkdir -p $(BINARY_DIR)
	go build -o $(SERVER_BIN) ./cmd/sentryrelay
	go build -o $(CTL_BIN) ./cmd/sentryrelay-ctl

test:
	go test -v ./...

test-race:
	go test -v -race ./...

bench:
	go test -bench . -benchmem ./test/benchmark

tidy:
	go mod tidy

clean:
	rm -rf $(BINARY_DIR)
