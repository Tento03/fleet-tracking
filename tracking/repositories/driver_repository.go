// Package repositories implements the DriverRepository interface using GORM.
package repositories

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tento03/fleet-tracking/tracking/models"
	"github.com/Tento03/fleet-tracking/tracking/utils"
	"gorm.io/gorm"
)

// gormDriverRepository is the production GORM implementation.
type gormDriverRepository struct {
	db *gorm.DB
}

// NewDriverRepository constructs a DriverRepository backed by the given
// *gorm.DB instance.
func NewDriverRepository(db *gorm.DB) DriverRepository {
	return &gormDriverRepository{db: db}
}

// ── Create ────────────────────────────────────────────────────────────────

func (r *gormDriverRepository) Create(ctx context.Context, driver *models.Driver) error {
	if err := r.db.WithContext(ctx).Create(driver).Error; err != nil {
		return fmt.Errorf("%w: %w", utils.ErrDatabase, err)
	}
	return nil
}

// ── FindByID ──────────────────────────────────────────────────────────────

func (r *gormDriverRepository) FindByID(ctx context.Context, id string) (*models.Driver, error) {
	var driver models.Driver
	err := r.db.WithContext(ctx).First(&driver, "id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, utils.ErrDriverNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", utils.ErrDatabase, err)
	}
	return &driver, nil
}

// ── FindAll ───────────────────────────────────────────────────────────────

func (r *gormDriverRepository) FindAll(ctx context.Context) ([]models.Driver, error) {
	var drivers []models.Driver
	if err := r.db.WithContext(ctx).Order("created_at DESC").Find(&drivers).Error; err != nil {
		return nil, fmt.Errorf("%w: %w", utils.ErrDatabase, err)
	}
	return drivers, nil
}

// ── FindByStatuses ────────────────────────────────────────────────────────

func (r *gormDriverRepository) FindByStatuses(
	ctx context.Context,
	statuses []models.DriverStatus,
) ([]models.Driver, error) {
	var drivers []models.Driver
	if err := r.db.WithContext(ctx).
		Where("status IN ?", statuses).
		Find(&drivers).Error; err != nil {
		return nil, fmt.Errorf("%w: %w", utils.ErrDatabase, err)
	}
	return drivers, nil
}

// ── UpdateStatus ──────────────────────────────────────────────────────────

func (r *gormDriverRepository) UpdateStatus(
	ctx context.Context,
	id string,
	status models.DriverStatus,
) error {
	result := r.db.WithContext(ctx).
		Model(&models.Driver{}).
		Where("id = ?", id).
		Update("status", status)
	if result.Error != nil {
		return fmt.Errorf("%w: %w", utils.ErrDatabase, result.Error)
	}
	if result.RowsAffected == 0 {
		return utils.ErrDriverNotFound
	}
	return nil
}

// ── FirstOrCreate ─────────────────────────────────────────────────────────

// FirstOrCreate fetches the driver with driver.ID or inserts a new row when
// it does not exist. The bool return value is true when a new row was created.
func (r *gormDriverRepository) FirstOrCreate(
	ctx context.Context,
	driver *models.Driver,
) (*models.Driver, bool, error) {
	existing := &models.Driver{}
	result := r.db.WithContext(ctx).
		Where("id = ?", driver.ID).
		Attrs(driver).
		FirstOrCreate(existing)
	if result.Error != nil {
		return nil, false, fmt.Errorf("%w: %w", utils.ErrDatabase, result.Error)
	}
	created := result.RowsAffected > 0
	return existing, created, nil
}

// ── CountByStatus ─────────────────────────────────────────────────────────

func (r *gormDriverRepository) CountByStatus(ctx context.Context) (map[models.DriverStatus]int64, error) {
	type row struct {
		Status models.DriverStatus
		Count  int64
	}
	var rows []row
	if err := r.db.WithContext(ctx).
		Model(&models.Driver{}).
		Select("status, COUNT(*) as count").
		Group("status").
		Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("%w: %w", utils.ErrDatabase, err)
	}
	result := make(map[models.DriverStatus]int64, len(rows))
	for _, r := range rows {
		result[r.Status] = r.Count
	}
	return result, nil
}

// ── FindOnlineStale ───────────────────────────────────────────────────────

// FindOnlineStale returns the subset of drivers (from ids) that are
// currently online or on_trip. The stale sweeper calls this to know which
// drivers should have an active Redis key.
func (r *gormDriverRepository) FindOnlineStale(
	ctx context.Context,
	ids []string,
) ([]models.Driver, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var drivers []models.Driver
	statuses := []models.DriverStatus{models.StatusOnline, models.StatusOnTrip}
	if err := r.db.WithContext(ctx).
		Where("id IN ? AND status IN ?", ids, statuses).
		Find(&drivers).Error; err != nil {
		return nil, fmt.Errorf("%w: %w", utils.ErrDatabase, err)
	}
	return drivers, nil
}

// ── Ensure at compile time ────────────────────────────────────────────────

// Compile-time assertion that gormDriverRepository satisfies DriverRepository.
var _ DriverRepository = (*gormDriverRepository)(nil)

// ── stale-sweeper helpers ─────────────────────────────────────────────────

// GetAllActiveIDs returns the IDs of all online / on_trip drivers.
// This is a convenience used by the stale sweeper goroutine.
func GetAllActiveIDs(ctx context.Context, repo DriverRepository) ([]string, error) {
	drivers, err := repo.FindByStatuses(ctx, []models.DriverStatus{
		models.StatusOnline,
		models.StatusOnTrip,
	})
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(drivers))
	for _, d := range drivers {
		ids = append(ids, d.ID)
	}
	return ids, nil
}

// ── timestamp guard ────────────────────────────────────────────────────────

// driverUpdatedAt returns the UpdatedAt time for use in stale detection.
// Uses a raw query so we do not need to add UpdatedAt to the model struct yet.
func driverUpdatedAt(ctx context.Context, db *gorm.DB, id string) (time.Time, error) {
	var t time.Time
	err := db.WithContext(ctx).
		Model(&models.Driver{}).
		Where("id = ?", id).
		Select("created_at").
		Scan(&t).Error
	return t, err
}

// suppress unused import lint when driverUpdatedAt is not yet called outside tests.
var _ = driverUpdatedAt
