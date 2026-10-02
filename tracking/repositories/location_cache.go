// Package repositories implements the LocationCache interface using Redis.
package repositories

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tento03/fleet-tracking/tracking/models"
	"github.com/redis/go-redis/v9"
)

const locationKeyPrefix = "driver:location:"

// redisLocationCache is the production Redis implementation of LocationCache.
type redisLocationCache struct {
	client *redis.Client
}

// NewLocationCache constructs a LocationCache backed by the given Redis client.
func NewLocationCache(client *redis.Client) LocationCache {
	return &redisLocationCache{client: client}
}

// locationKey returns the Redis key for a driver's last-known position.
func locationKey(driverID string) string {
	return locationKeyPrefix + driverID
}

// ── SetLast ───────────────────────────────────────────────────────────────

// SetLast serialises snapshot to JSON and stores it in Redis with the given
// TTL. A TTL of 0 means the key persists indefinitely (not recommended for
// GPS data; the service always passes 300 s).
func (c *redisLocationCache) SetLast(
	ctx context.Context,
	snapshot *models.LocationSnapshot,
	ttl time.Duration,
) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("location cache marshal: %w", err)
	}
	if err := c.client.Set(ctx, locationKey(snapshot.DriverID), data, ttl).Err(); err != nil {
		return fmt.Errorf("location cache set: %w", err)
	}
	return nil
}

// ── GetLast ───────────────────────────────────────────────────────────────

// GetLast retrieves and deserialises the last-known position for driverID.
// Returns (nil, nil) when the key does not exist or has expired.
func (c *redisLocationCache) GetLast(
	ctx context.Context,
	driverID string,
) (*models.LocationSnapshot, error) {
	data, err := c.client.Get(ctx, locationKey(driverID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("location cache get: %w", err)
	}
	var snap models.LocationSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("location cache unmarshal: %w", err)
	}
	return &snap, nil
}

// ── GetManyLast ───────────────────────────────────────────────────────────

// GetManyLast retrieves snapshots for multiple drivers using a single Redis
// MGET command. The returned slice is index-aligned with ids; nil entries
// indicate a cache miss for that driver.
func (c *redisLocationCache) GetManyLast(
	ctx context.Context,
	ids []string,
) ([]*models.LocationSnapshot, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = locationKey(id)
	}

	vals, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("location cache mget: %w", err)
	}

	snapshots := make([]*models.LocationSnapshot, len(ids))
	for i, val := range vals {
		if val == nil {
			snapshots[i] = nil
			continue
		}
		raw, ok := val.(string)
		if !ok {
			snapshots[i] = nil
			continue
		}
		var snap models.LocationSnapshot
		if err := json.Unmarshal([]byte(raw), &snap); err != nil {
			// Corrupted entry — treat as miss rather than halting.
			snapshots[i] = nil
			continue
		}
		snapshots[i] = &snap
	}
	return snapshots, nil
}

// ── Exists ────────────────────────────────────────────────────────────────

// Exists reports whether the cache key for driverID is present and has not
// expired. The stale sweeper uses this to detect drivers that have gone silent.
func (c *redisLocationCache) Exists(ctx context.Context, driverID string) (bool, error) {
	n, err := c.client.Exists(ctx, locationKey(driverID)).Result()
	if err != nil {
		return false, fmt.Errorf("location cache exists: %w", err)
	}
	return n > 0, nil
}

// ── Compile-time assertion ────────────────────────────────────────────────

var _ LocationCache = (*redisLocationCache)(nil)
