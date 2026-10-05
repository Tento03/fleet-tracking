// Package models defines the persistent domain entities for the tracking service.
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

	// DriverID is a foreign-key reference to drivers.id (UUID).
	DriverID string `gorm:"type:varchar(36);not null;index:idx_driver_time,priority:1" json:"driver_id"`

	// Latitude stored with 8 decimal places (sub-metre precision).
	Latitude float64 `gorm:"type:decimal(10,8);not null" json:"latitude"`

	// Longitude stored with 8 decimal places (sub-metre precision).
	Longitude float64 `gorm:"type:decimal(11,8);not null" json:"longitude"`

	// Speed in km/h.
	Speed float32 `gorm:"type:float" json:"speed"`

	// Heading in degrees [0, 360).
	Heading float32 `gorm:"type:float" json:"heading"`

	// Timestamp is the GPS reading time (UTC).
	Timestamp time.Time `gorm:"not null;index:idx_driver_time,priority:2" json:"timestamp"`

	// Driver is the associated driver loaded via GORM preload when needed.
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

// LocationEvent is the JSON payload consumed from the Kafka topic "location.events".
type LocationEvent struct {
	EventID    string  `json:"event_id"`
	DriverCode string  `json:"driver_code"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	Speed      float32 `json:"speed"`
	Heading    float32 `json:"heading"`
	Timestamp  string  `json:"timestamp"`   // RFC3339 UTC from client
	ReceivedAt string  `json:"received_at"` // RFC3339 UTC from ingestion server
}

// ── LocationSnapshot ──────────────────────────────────────────────────────

// LocationSnapshot is the Redis-cached representation of a driver's most
// recent GPS position.
// Redis key convention: "driver:location:{driver_id}"
type LocationSnapshot struct {
	DriverID   string       `json:"driver_id"`   // Driver UUID
	DriverCode string       `json:"driver_code"` // Driver business code (e.g. "driver-001")
	Latitude   float64      `json:"latitude"`
	Longitude  float64      `json:"longitude"`
	Speed      float32      `json:"speed"`
	Heading    float32      `json:"heading"`
	Status     DriverStatus `json:"status"`
	Timestamp  time.Time    `json:"timestamp"`
	UpdatedAt  time.Time    `json:"updated_at"`
}
