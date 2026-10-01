// Package config loads and validates the ingestion service configuration
// from a .env file in the same directory as the running binary.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds all runtime configuration for the ingestion service.
type Config struct {
	// GRPCPort is the TCP port the gRPC server listens on (e.g. "50051").
	GRPCPort string

	// LogLevel controls slog output verbosity: debug | info | warn | error.
	LogLevel slog.Level

	// KafkaBrokers is a list of Kafka broker addresses parsed from the
	// comma-separated KAFKA_BROKER environment variable.
	// Default: ["localhost:9092"]
	KafkaBrokers []string

	// KafkaTopic is the Kafka topic name to publish location events to.
	// Default: "location.events"
	KafkaTopic string
}

// Load reads .env from the current working directory and returns a Config.
// Missing optional variables fall back to documented defaults.
func Load() (*Config, error) {
	// godotenv silently ignores a missing .env file; that is intentional so
	// the service can also be configured via real environment variables.
	if err := godotenv.Load(); err != nil {
		slog.Warn("no .env file found, falling back to environment variables")
	}

	cfg := &Config{}

	// ── GRPC_PORT ────────────────────────────────────────────────────────────
	cfg.GRPCPort = getEnvOrDefault("GRPC_PORT", "50051")
	if _, err := strconv.Atoi(cfg.GRPCPort); err != nil {
		return nil, fmt.Errorf("GRPC_PORT %q is not a valid port number: %w", cfg.GRPCPort, err)
	}

	// ── LOG_LEVEL ────────────────────────────────────────────────────────────
	cfg.LogLevel = parseLogLevel(getEnvOrDefault("LOG_LEVEL", "info"))

	// ── KAFKA_BROKER ─────────────────────────────────────────────────────────
	// Accepts a single address or a comma-separated list.
	// Example: "localhost:9092" or "broker1:9092,broker2:9092"
	rawBrokers := getEnvOrDefault("KAFKA_BROKER", "localhost:9092")
	cfg.KafkaBrokers = parseBrokers(rawBrokers)
	if len(cfg.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKER resolved to an empty broker list (raw: %q)", rawBrokers)
	}

	// ── KAFKA_TOPIC ──────────────────────────────────────────────────────────
	cfg.KafkaTopic = getEnvOrDefault("KAFKA_TOPIC", "location.events")

	return cfg, nil
}

// parseBrokers splits a comma-separated broker string and trims whitespace.
func parseBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if b := strings.TrimSpace(p); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// getEnvOrDefault returns the value of the named environment variable or
// the provided default when the variable is empty or unset.
func getEnvOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); strings.TrimSpace(v) != "" {
		return v
	}
	return defaultVal
}

// parseLogLevel converts a string level name to a slog.Level.
// Unknown values default to slog.LevelInfo.
func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
