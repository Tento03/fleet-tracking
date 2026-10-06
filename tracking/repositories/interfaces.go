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
type DriverRepository interface {
	// Create inserts a new driver record. The BeforeCreate hook assigns a UUID
	// when driver.ID is empty.
	Create(ctx context.Context, driver *models.Driver) error

	// FindByID retrieves a driver by primary key UUID.
	// Returns utils.ErrDriverNotFound when no row matches.
	FindByID(ctx context.Context, id string) (*models.Driver, error)

	// FindByCode retrieves a driver by its unique business code (e.g. "driver-001").
	// Returns utils.ErrDriverNotFound when no row matches.
	FindByCode(ctx context.Context, code string) (*models.Driver, error)

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
	CountByStatus(ctx context.Context) (map[models.DriverStatus]int64, error)

	// FindOnlineStale returns drivers whose IDs are in the provided slice
	// and whose status is online or on_trip.
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
	GetManyLast(ctx context.Context, ids []string) ([]*models.LocationSnapshot, error)

	// Exists reports whether the cache key for driverID is present.
	Exists(ctx context.Context, driverID string) (bool, error)
}

// ── GeofenceRepository ────────────────────────────────────────────────────

// GeofenceRepository defines all persistence operations for geofences,
// geofence state, and geofence alerts.
type GeofenceRepository interface {
	// ── CRUD ──────────────────────────────────────────────────────────────

	// Create inserts a new geofence. The BeforeCreate hook assigns a UUID.
	Create(ctx context.Context, g *models.Geofence) error

	// FindByID retrieves a geofence by UUID primary key.
	// Returns utils.ErrGeofenceNotFound when no row matches.
	FindByID(ctx context.Context, id string) (*models.Geofence, error)

	// FindAll returns all geofences ordered by created_at DESC.
	// Pass activeOnly = true to filter inactive (soft-deleted) geofences.
	FindAll(ctx context.Context, activeOnly bool) ([]models.Geofence, error)

	// Update persists changes to an existing geofence.
	Update(ctx context.Context, g *models.Geofence) error

	// Delete soft-deletes a geofence by setting active = false.
	// Returns utils.ErrGeofenceNotFound when no row matches.
	Delete(ctx context.Context, id string) error

	// ── State ─────────────────────────────────────────────────────────────

	// GetState returns the current containment state for a driver-geofence pair.
	// Returns (nil, nil) when the pair has no state row yet.
	GetState(ctx context.Context, driverID, geofenceID string) (*models.GeofenceState, error)

	// UpsertState inserts or updates the containment state row.
	UpsertState(ctx context.Context, s *models.GeofenceState) error

	// ── Alerts ────────────────────────────────────────────────────────────

	// CreateAlert inserts a new geofence crossing alert record.
	CreateAlert(ctx context.Context, a *models.GeofenceAlert) error

	// FindAlerts returns alerts filtered by optional driverID / geofenceID
	// and time window [from, to], ordered by triggered_at DESC.
	// Pass empty strings / zero times to skip individual filters.
	FindAlerts(ctx context.Context, driverID, geofenceID string, from, to time.Time) ([]models.GeofenceAlert, error)
}

