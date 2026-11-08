# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Statically compile pure Go binaries (modernc.org/sqlite requires no CGO)
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/sentryrelay ./cmd/sentryrelay && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/sentryrelay-ctl ./cmd/sentryrelay-ctl

# Runtime stage
FROM alpine:3.20

WORKDIR /app

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /bin/sentryrelay /usr/local/bin/sentryrelay
COPY --from=builder /bin/sentryrelay-ctl /usr/local/bin/sentryrelay-ctl

EXPOSE 8080

RUN adduser -D -u 10001 appuser && \
    mkdir -p /data && chown -R appuser:appuser /data /app

USER appuser

VOLUME ["/data"]

ENTRYPOINT ["sentryrelay"]
CMD ["-port", "8080", "-db", "/data/sentryrelay.db"]
