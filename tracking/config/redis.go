// Package config provides Redis client initialisation for the tracking service.
package config

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewRedis creates and validates a Redis client using the address and password
// from cfg. A Ping is issued immediately to verify connectivity.
func NewRedis(cfg *Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr(),
		Password: cfg.RedisPass,
		DB:       0, // use the default Redis database

		// Connection pool sizing.
		PoolSize:     10,
		MinIdleConns: 3,

		// Timeouts.
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	// Verify connectivity at startup.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: ping failed (%s): %w", cfg.RedisAddr(), err)
	}

	slog.Info("Redis connected", "addr", cfg.RedisAddr())
	return client, nil
}
