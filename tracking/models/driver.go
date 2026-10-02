// Package models defines the persistent domain entities for the tracking
// service. Each struct maps 1-to-1 to a MySQL table managed by GORM.
package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ── DriverStatus ──────────────────────────────────────────────────────────

// DriverStatus represents the operational state of a driver.
type DriverStatus string

const (
	// StatusOffline means the driver app is closed / no recent GPS ping.
	StatusOffline DriverStatus = "offline"

	// StatusOnline means the driver is active and sending GPS updates.
	StatusOnline DriverStatus = "online"

	// StatusOnTrip means the driver is currently executing a trip.
	StatusOnTrip DriverStatus = "on_trip"
)

// ── Driver ────────────────────────────────────────────────────────────────

// Driver is the persistent record for a fleet driver.
// The simulator uses its own fixed IDs (e.g. "driver-001"), so BeforeCreate
// only generates a UUID when the ID field is blank — it never overwrites an
// existing value supplied by the caller.
type Driver struct {
	// ID is a UUID v4 string (varchar 36). The simulator populates this field
	// with its own stable identifier; the hook fills it only when blank.
	ID string `gorm:"type:varchar(36);primaryKey" json:"id"`

	// Name is the driver's display name (required).
	Name string `gorm:"type:varchar(100);not null" json:"name"`

	// Phone is an optional contact number.
	Phone string `gorm:"type:varchar(20)" json:"phone"`

	// Vehicle is an optional vehicle description / plate number.
	Vehicle string `gorm:"type:varchar(50)" json:"vehicle"`

	// Status holds the driver's current operational state.
	// The MySQL enum enforces the domain constraint at the database level.
	Status DriverStatus `gorm:"type:enum('offline','online','on_trip');default:'offline';index" json:"status"`

	// CreatedAt is set once by GORM on insert.
	CreatedAt time.Time `json:"created_at"`
}

// BeforeCreate is a GORM hook that auto-assigns a UUID v4 to ID when the
// field is empty. Drivers registered by the simulator arrive with their own
// stable ID (e.g. "driver-001") and are not modified.
func (d *Driver) BeforeCreate(_ *gorm.DB) error {
	if d.ID == "" {
		d.ID = uuid.New().String()
	}
	return nil
}
