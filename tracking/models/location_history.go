// Package models defines the persistent domain entities for the tracking
// service.
package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ── LocationHistory ───────────────────────────────────────────────────────

// LocationHistory is the append-only GPS audit trail stored in MySQL.
// The composite index idx_driver_time enables efficient time-range queries
// scoped to a single driver.
type LocationHistory struct {
	// ID is a UUID v4 primary key (varchar 36).
	ID string `gorm:"type:varchar(36);primaryKey" json:"id"`

	// DriverID is a foreign-key reference to drivers.id.
	// priority:1 makes it the leading column in the composite index.
	DriverID string `gorm:"type:varchar(36);not null;index:idx_driver_time,priority:1" json:"driver_id"`

	// Latitude stored with 8 decimal places (sub-metre precision).
	Latitude float64 `gorm:"type:decimal(10,8);not null" json:"latitude"`

	// Longitude stored with 8 decimal places (sub-metre precision).
	Longitude float64 `gorm:"type:decimal(11,8);not null" json:"longitude"`

	// Speed in km/h reported by the simulator.
	Speed float32 `gorm:"type:float" json:"speed"`

	// Timestamp is the GPS reading time (from the simulator frame).
	// priority:2 makes it the secondary column in the composite index.
	Timestamp time.Time `gorm:"not null;index:idx_driver_time,priority:2" json:"timestamp"`

	// Driver is the associated driver loaded via GORM preload when needed.
	// CASCADE delete ensures history is removed when the driver is deleted.
	Driver Driver `gorm:"foreignKey:DriverID;constraint:OnDelete:CASCADE" json:"-"`
}

// BeforeCreate assigns a UUID v4 if the ID field is blank.
func (l *LocationHistory) BeforeCreate(_ *gorm.DB) error {
	if l.ID == "" {
		l.ID = uuid.New().String()
	}
	return nil
}

// ── LocationEvent ─────────────────────────────────────────────────────────

// LocationEvent is the JSON payload consumed from the Kafka topic
// "location.events". The ingestion service (handler/location_handler.go)
// produces this exact schema.
//
// Example:
//
//	{
//	  "driver_id":  "driver-001",
//	  "latitude":   -3.5952,
//	  "longitude":  98.6722,
//	  "speed":      42.5,
//	  "timestamp":  "2026-09-26T08:00:00Z"
//	}
type LocationEvent struct {
	DriverID  string  `json:"driver_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Speed     float32 `json:"speed"`
	Timestamp string  `json:"timestamp"` // RFC3339 UTC, matches ingestion output
}

// ── LocationSnapshot ──────────────────────────────────────────────────────

// LocationSnapshot is the Redis-cached representation of a driver's most
// recent GPS position. It is written by LocationCache.SetLast and read by
// LocationCache.GetLast / GetManyLast.
//
// The Redis key convention is: "driver:location:{driver_id}"
type LocationSnapshot struct {
	DriverID  string       `json:"driver_id"`
	Latitude  float64      `json:"latitude"`
	Longitude float64      `json:"longitude"`
	Speed     float32      `json:"speed"`
	Status    DriverStatus `json:"status"`
	Timestamp time.Time    `json:"timestamp"`
	UpdatedAt time.Time    `json:"updated_at"`
}
