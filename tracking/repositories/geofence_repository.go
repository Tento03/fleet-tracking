// Package repositories provides data-access implementations for the tracking service.
package repositories

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/Tento03/fleet-tracking/tracking/models"
	"github.com/Tento03/fleet-tracking/tracking/utils"
)

// geofenceRepository is the GORM-backed implementation of GeofenceRepository.
type geofenceRepository struct {
	db *gorm.DB
}

// NewGeofenceRepository constructs a GeofenceRepository backed by the given
// GORM database connection.
func NewGeofenceRepository(db *gorm.DB) GeofenceRepository {
	return &geofenceRepository{db: db}
}

// Create inserts a new geofence. The BeforeCreate hook assigns a UUID.
func (r *geofenceRepository) Create(ctx context.Context, g *models.Geofence) error {
	return r.db.WithContext(ctx).Create(g).Error
}

// FindByID retrieves a geofence by UUID primary key.
func (r *geofenceRepository) FindByID(ctx context.Context, id string) (*models.Geofence, error) {
	var g models.Geofence
	err := r.db.WithContext(ctx).First(&g, "id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, utils.ErrGeofenceNotFound
	}
	return &g, err
}

// FindAll returns all geofences ordered by created_at DESC.
// If activeOnly is true, only active geofences are returned.
func (r *geofenceRepository) FindAll(ctx context.Context, activeOnly bool) ([]models.Geofence, error) {
	q := r.db.WithContext(ctx).Order("created_at DESC")
	if activeOnly {
		q = q.Where("active = ?", true)
	}
	var gs []models.Geofence
	return gs, q.Find(&gs).Error
}

// Update saves changes to an existing geofence (all non-zero fields).
func (r *geofenceRepository) Update(ctx context.Context, g *models.Geofence) error {
	return r.db.WithContext(ctx).Save(g).Error
}

// Delete soft-deletes by setting active = false (logical delete).
// Physical rows are kept for alert history integrity.
func (r *geofenceRepository) Delete(ctx context.Context, id string) error {
	res := r.db.WithContext(ctx).
		Model(&models.Geofence{}).
		Where("id = ?", id).
		Updates(map[string]any{"active": false, "updated_at": time.Now().UTC()})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return utils.ErrGeofenceNotFound
	}
	return nil
}

// ── State operations ──────────────────────────────────────────────────────

// GetState retrieves the current containment state for a driver-geofence pair.
// Returns (nil, nil) when no state row exists yet.
func (r *geofenceRepository) GetState(ctx context.Context, driverID, geofenceID string) (*models.GeofenceState, error) {
	var s models.GeofenceState
	err := r.db.WithContext(ctx).
		Where("driver_id = ? AND geofence_id = ?", driverID, geofenceID).
		First(&s).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &s, err
}

// UpsertState inserts or updates the containment state for a driver-geofence pair.
func (r *geofenceRepository) UpsertState(ctx context.Context, s *models.GeofenceState) error {
	return r.db.WithContext(ctx).Save(s).Error
}

// ── Alert operations ──────────────────────────────────────────────────────

// CreateAlert inserts a new geofence crossing alert record.
func (r *geofenceRepository) CreateAlert(ctx context.Context, a *models.GeofenceAlert) error {
	return r.db.WithContext(ctx).Create(a).Error
}

// FindAlerts returns alerts filtered by optional driverID / geofenceID and
// time window [from, to], ordered by triggered_at DESC.
func (r *geofenceRepository) FindAlerts(
	ctx context.Context,
	driverID, geofenceID string,
	from, to time.Time,
) ([]models.GeofenceAlert, error) {
	q := r.db.WithContext(ctx).
		Preload("Geofence").
		Order("triggered_at DESC")

	if driverID != "" {
		q = q.Where("driver_id = ?", driverID)
	}
	if geofenceID != "" {
		q = q.Where("geofence_id = ?", geofenceID)
	}
	if !from.IsZero() {
		q = q.Where("triggered_at >= ?", from)
	}
	if !to.IsZero() {
		q = q.Where("triggered_at <= ?", to)
	}

	var alerts []models.GeofenceAlert
	return alerts, q.Find(&alerts).Error
}
