// Package repositories defines the data-access interfaces and their
// implementations for the tracking service.
package repositories

import (
	"context"
	"time"

	"github.com/Tento03/fleet-tracking/tracking/models"
)

// ── DriverRepository ──────────────────────────────────────────────────────

// DriverRepository defines all persistence operations for the Driver entity.
// The GORM implementation lives in driver_repository.go.
type DriverRepository interface {
	// Create inserts a new driver record. The BeforeCreate hook assigns a UUID
	// when driver.ID is empty.
	Create(ctx context.Context, driver *models.Driver) error

	// FindByID retrieves a driver by primary key.
	// Returns utils.ErrDriverNotFound when no row matches.
	FindByID(ctx context.Context, id string) (*models.Driver, error)

	// FindAll returns every driver ordered by created_at DESC.
	FindAll(ctx context.Context) ([]models.Driver, error)

	// FindByStatuses returns drivers whose status is in the given list.
	FindByStatuses(ctx context.Context, statuses []models.DriverStatus) ([]models.Driver, error)

	// UpdateStatus sets the status column for a single driver.
	UpdateStatus(ctx context.Context, id string, status models.DriverStatus) error

	// FirstOrCreate fetches the driver with the given ID or inserts it if it
	// does not exist. Returns (driver, created, error).
	FirstOrCreate(ctx context.Context, driver *models.Driver) (*models.Driver, bool, error)

	// CountByStatus counts drivers in each status group.
	// Returns a map of status → count.
	CountByStatus(ctx context.Context) (map[models.DriverStatus]int64, error)

	// FindOnlineStale returns drivers whose IDs are in the provided slice
	// and whose status is online or on_trip. Used by the stale sweeper to
	// cross-reference against Redis key existence.
	FindOnlineStale(ctx context.Context, ids []string) ([]models.Driver, error)
}

// ── LocationRepository ────────────────────────────────────────────────────

// LocationRepository defines persistence operations for location_history rows.
type LocationRepository interface {
	// Insert appends a new GPS reading to location_history.
	Insert(ctx context.Context, loc *models.LocationHistory) error

	// FindHistory returns all location_history rows for a driver between from
	// and to (inclusive), ordered by timestamp ASC.
	FindHistory(ctx context.Context, driverID string, from, to time.Time) ([]models.LocationHistory, error)

	// CountBetween counts the total number of location_history rows recorded
	// within the given time window (across all drivers).
	CountBetween(ctx context.Context, from, to time.Time) (int64, error)
}

// ── LocationCache ─────────────────────────────────────────────────────────

// LocationCache defines the Redis caching operations for last-known positions.
type LocationCache interface {
	// SetLast serialises snapshot to JSON and stores it under the key
	// "driver:location:{driverID}" with the given TTL.
	SetLast(ctx context.Context, snapshot *models.LocationSnapshot, ttl time.Duration) error

	// GetLast retrieves and deserialises the last-known position for driverID.
	// Returns (nil, nil) when the key does not exist.
	GetLast(ctx context.Context, driverID string) (*models.LocationSnapshot, error)

	// GetManyLast retrieves snapshots for multiple drivers in a single MGET.
	// Missing keys are represented as nil entries in the returned slice
	// (index-aligned with ids).
	GetManyLast(ctx context.Context, ids []string) ([]*models.LocationSnapshot, error)

	// Exists reports whether the cache key for driverID is present (and not
	// expired). Used by the stale sweeper to detect drivers that have gone
	// silent.
	Exists(ctx context.Context, driverID string) (bool, error)
}
