// Package models defines the WebSocket broadcast event payloads.
// All events use the flat JSON format from KONTEKS GLOBAL.
package models

import "time"

// Event type constants.
const (
	EventLocationUpdated     = "location_updated"
	EventDriverStatusChanged = "driver_status_changed"
	EventDriverRegistered    = "driver_registered"
)

// LocationUpdatedEvent is the flat JSON broadcast for location_updated.
type LocationUpdatedEvent struct {
	Event      string       `json:"event"`
	DriverID   string       `json:"driver_id"`
	DriverCode string       `json:"driver_code"`
	Latitude   float64      `json:"latitude"`
	Longitude  float64      `json:"longitude"`
	Speed      float32      `json:"speed"`
	Heading    float32      `json:"heading"`
	Status     DriverStatus `json:"status"`
	Timestamp  time.Time    `json:"timestamp"`
}

// DriverStatusChangedEvent is the flat JSON broadcast for driver_status_changed.
type DriverStatusChangedEvent struct {
	Event      string       `json:"event"`
	DriverID   string       `json:"driver_id"`
	DriverCode string       `json:"driver_code"`
	OldStatus  DriverStatus `json:"old_status"`
	NewStatus  DriverStatus `json:"new_status"`
	Timestamp  time.Time    `json:"timestamp"`
}

// DriverInfo is the nested driver object inside DriverRegisteredEvent.
type DriverInfo struct {
	ID        string       `json:"id"`
	Code      string       `json:"code"`
	Name      string       `json:"name"`
	Phone     string       `json:"phone"`
	Vehicle   string       `json:"vehicle"`
	Status    DriverStatus `json:"status"`
	CreatedAt time.Time    `json:"created_at"`
}

// DriverRegisteredEvent is the flat JSON broadcast for driver_registered.
type DriverRegisteredEvent struct {
	Event  string     `json:"event"`
	Driver DriverInfo `json:"driver"`
}
