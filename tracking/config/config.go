// Package config loads and validates the tracking service configuration
// from environment variables (and optionally a .env file).
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds all runtime configuration for the tracking service.
type Config struct {
	// ── App ───────────────────────────────────────────────────────────────
	AppPort     string
	AppTimezone string
	CORSOrigin  string

	// ── MySQL ─────────────────────────────────────────────────────────────
	DBHost string
	DBPort string
	DBUser string
	DBPass string
	DBName string

	// ── Redis ─────────────────────────────────────────────────────────────
	RedisHost string
	RedisPort string
	RedisPass string

	// ── Kafka ─────────────────────────────────────────────────────────────
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string

	// ── Logging ───────────────────────────────────────────────────────────
	LogLevel slog.Level
}

// Load reads .env from the current working directory and returns a Config.
// Missing optional variables fall back to documented defaults.
func Load() (*Config, error) {
	// godotenv silently ignores a missing .env file so the service works
	// with real environment variables in production too.
	if err := godotenv.Load(); err != nil {
		slog.Warn("no .env file found, falling back to environment variables")
	}

	cfg := &Config{}

	// ── App ───────────────────────────────────────────────────────────────
	cfg.AppPort = getEnvOrDefault("APP_PORT", "8080")
	cfg.AppTimezone = getEnvOrDefault("APP_TIMEZONE", "UTC")
	cfg.CORSOrigin = getEnvOrDefault("CORS_ORIGIN", "http://localhost:3000")

	// ── MySQL ─────────────────────────────────────────────────────────────
	cfg.DBHost = getEnvOrDefault("DB_HOST", "localhost")
	cfg.DBPort = getEnvOrDefault("DB_PORT", "3306")
	cfg.DBUser = getEnvOrDefault("DB_USER", "root")
	cfg.DBPass = os.Getenv("DB_PASS") // intentionally blank by default
	cfg.DBName = getEnvOrDefault("DB_NAME", "fleet_tracking")

	if cfg.DBUser == "" {
		return nil, fmt.Errorf("DB_USER must not be empty")
	}
	if cfg.DBName == "" {
		return nil, fmt.Errorf("DB_NAME must not be empty")
	}

	// ── Redis ─────────────────────────────────────────────────────────────
	cfg.RedisHost = getEnvOrDefault("REDIS_HOST", "localhost")
	cfg.RedisPort = getEnvOrDefault("REDIS_PORT", "6379")
	cfg.RedisPass = os.Getenv("REDIS_PASS") // blank by default

	// ── Kafka ─────────────────────────────────────────────────────────────
	rawBrokers := getEnvOrDefault("KAFKA_BROKER", "localhost:9092")
	cfg.KafkaBrokers = parseBrokers(rawBrokers)
	if len(cfg.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("KAFKA_BROKER resolved to an empty list (raw: %q)", rawBrokers)
	}
	cfg.KafkaTopic = getEnvOrDefault("KAFKA_TOPIC", "location.events")
	cfg.KafkaGroupID = getEnvOrDefault("KAFKA_GROUP_ID", "tracking-service")

	// ── Logging ───────────────────────────────────────────────────────────
	cfg.LogLevel = parseLogLevel(getEnvOrDefault("LOG_LEVEL", "info"))

	return cfg, nil
}

// ── DSN helpers ───────────────────────────────────────────────────────────

// MySQLDSN returns the GORM/MySQL DSN string.
// parseTime=true converts DATETIME/TIMESTAMP to time.Time.
// loc=UTC ensures all timestamps are treated as UTC regardless of server tz.
func (c *Config) MySQLDSN() string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true&loc=UTC&charset=utf8mb4",
		c.DBUser, c.DBPass, c.DBHost, c.DBPort, c.DBName,
	)
}

// RedisAddr returns the host:port string for the Redis client.
func (c *Config) RedisAddr() string {
	return c.RedisHost + ":" + c.RedisPort
}

// ── Private helpers ───────────────────────────────────────────────────────

func getEnvOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); strings.TrimSpace(v) != "" {
		return v
	}
	return defaultVal
}

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
