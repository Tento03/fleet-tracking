// Package repositories implements the LocationRepository interface using GORM.
package repositories

import (
	"context"
	"fmt"
	"time"

	"github.com/Tento03/fleet-tracking/tracking/models"
	"github.com/Tento03/fleet-tracking/tracking/utils"
	"gorm.io/gorm"
)

// gormLocationRepository is the production GORM implementation for
// location_history rows.
type gormLocationRepository struct {
	db *gorm.DB
}

// NewLocationRepository constructs a LocationRepository backed by the given
// *gorm.DB instance.
func NewLocationRepository(db *gorm.DB) LocationRepository {
	return &gormLocationRepository{db: db}
}

// ── Insert ────────────────────────────────────────────────────────────────

// Insert appends a new GPS reading to location_history. The BeforeCreate hook
// in LocationHistory assigns a UUID when ID is empty.
func (r *gormLocationRepository) Insert(
	ctx context.Context,
	loc *models.LocationHistory,
) error {
	if err := r.db.WithContext(ctx).Create(loc).Error; err != nil {
		return fmt.Errorf("%w: %w", utils.ErrDatabase, err)
	}
	return nil
}

// ── FindHistory ───────────────────────────────────────────────────────────

// FindHistory returns all location_history rows for driverID where Timestamp
// falls within [from, to], ordered by Timestamp ASC. This is designed for the
// history / replay endpoint (added in a later prompt).
func (r *gormLocationRepository) FindHistory(
	ctx context.Context,
	driverID string,
	from, to time.Time,
) ([]models.LocationHistory, error) {
	var history []models.LocationHistory
	if err := r.db.WithContext(ctx).
		Where("driver_id = ? AND timestamp BETWEEN ? AND ?", driverID, from, to).
		Order("timestamp ASC").
		Find(&history).Error; err != nil {
		return nil, fmt.Errorf("%w: %w", utils.ErrDatabase, err)
	}
	return history, nil
}

// ── CountBetween ──────────────────────────────────────────────────────────

// CountBetween counts the total number of location_history rows recorded
// across all drivers within the given time window. Useful for analytics /
// dashboard counters.
func (r *gormLocationRepository) CountBetween(
	ctx context.Context,
	from, to time.Time,
) (int64, error) {
	var count int64
	if err := r.db.WithContext(ctx).
		Model(&models.LocationHistory{}).
		Where("timestamp BETWEEN ? AND ?", from, to).
		Count(&count).Error; err != nil {
		return 0, fmt.Errorf("%w: %w", utils.ErrDatabase, err)
	}
	return count, nil
}

// ── Compile-time assertion ────────────────────────────────────────────────

var _ LocationRepository = (*gormLocationRepository)(nil)
