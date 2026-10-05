// Package models defines the persistent domain entities for the tracking service.
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
// The primary key ID is always a UUID v4 string (varchar 36).
// The business code (e.g. "driver-001") is stored in the unique Code column.
type Driver struct {
	// ID is the database UUID v4 primary key.
	ID string `gorm:"type:varchar(36);primaryKey" json:"id"`

	// Code is the unique business code (e.g. "driver-001").
	Code string `gorm:"type:varchar(50);uniqueIndex;not null" json:"code"`

	// Name is the driver's display name (required).
	Name string `gorm:"type:varchar(100);not null" json:"name"`

	// Phone is an optional contact number.
	Phone string `gorm:"type:varchar(20)" json:"phone"`

	// Vehicle is an optional vehicle description / plate number.
	Vehicle string `gorm:"type:varchar(50)" json:"vehicle"`

	// Status holds the driver's current operational state.
	Status DriverStatus `gorm:"type:enum('offline','online','on_trip');default:'offline';index" json:"status"`

	// CreatedAt is set once by GORM on insert.
	CreatedAt time.Time `json:"created_at"`
}

// BeforeCreate is a GORM hook that auto-assigns a UUID v4 to ID when blank.
func (d *Driver) BeforeCreate(_ *gorm.DB) error {
	if d.ID == "" {
		d.ID = uuid.New().String()
	}
	return nil
}
