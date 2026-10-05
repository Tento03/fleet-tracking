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
//
//   {"event":"location_updated","driver_id":"driver-001","latitude":-3.5952,
//    "longitude":98.6722,"speed":40.5,"status":"online","timestamp":"..."}
type LocationUpdatedEvent struct {
	Event     string       `json:"event"`
	DriverID  string       `json:"driver_id"`
	Latitude  float64      `json:"latitude"`
	Longitude float64      `json:"longitude"`
	Speed     float32      `json:"speed"`
	Status    DriverStatus `json:"status"`
	Timestamp time.Time    `json:"timestamp"`
}

// DriverStatusChangedEvent is the flat JSON broadcast for driver_status_changed.
//
//   {"event":"driver_status_changed","driver_id":"driver-001",
//    "old_status":"offline","new_status":"online","timestamp":"..."}
type DriverStatusChangedEvent struct {
	Event     string       `json:"event"`
	DriverID  string       `json:"driver_id"`
	OldStatus DriverStatus `json:"old_status"`
	NewStatus DriverStatus `json:"new_status"`
	Timestamp time.Time    `json:"timestamp"`
}

// DriverInfo is the nested driver object inside DriverRegisteredEvent.
type DriverInfo struct {
	ID        string       `json:"id"`
	Name      string       `json:"name"`
	Phone     string       `json:"phone"`
	Vehicle   string       `json:"vehicle"`
	Status    DriverStatus `json:"status"`
	CreatedAt time.Time    `json:"created_at"`
}

// DriverRegisteredEvent is the flat JSON broadcast for driver_registered.
//
//   {"event":"driver_registered","driver":{"id":"...","name":"...","status":"offline",...}}
type DriverRegisteredEvent struct {
	Event  string     `json:"event"`
	Driver DriverInfo `json:"driver"`
}
