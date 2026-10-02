// Package models defines the WebSocket broadcast event payloads used by
// the tracking service. All events share a common envelope so the frontend
// can dispatch on "type" before reading "payload".
package models

import "time"

// ── WebSocket Event Envelope ──────────────────────────────────────────────

// WSEvent is the top-level JSON envelope broadcast to every WebSocket client.
//
// Example:
//
//	{"type":"location_updated","payload":{...}}
type WSEvent struct {
	Type    string `json:"type"`
	Payload any    `json:"payload"`
}

// ── Event types (string constants) ───────────────────────────────────────

const (
	// EventLocationUpdated is broadcast on every successful GPS update.
	EventLocationUpdated = "location_updated"

	// EventDriverStatusChanged is broadcast when a driver's status changes
	// (offline → online, online → offline, etc.).
	EventDriverStatusChanged = "driver_status_changed"

	// EventDriverRegistered is broadcast the first time a driver is seen
	// (auto-registered via FirstOrCreate).
	EventDriverRegistered = "driver_registered"
)

// ── Payload structs ───────────────────────────────────────────────────────

// LocationUpdatedPayload is the payload for EventLocationUpdated.
type LocationUpdatedPayload struct {
	DriverID  string       `json:"driver_id"`
	Latitude  float64      `json:"latitude"`
	Longitude float64      `json:"longitude"`
	Speed     float32      `json:"speed"`
	Status    DriverStatus `json:"status"`
	Timestamp time.Time    `json:"timestamp"`
}

// DriverStatusChangedPayload is the payload for EventDriverStatusChanged.
type DriverStatusChangedPayload struct {
	DriverID  string       `json:"driver_id"`
	OldStatus DriverStatus `json:"old_status"`
	NewStatus DriverStatus `json:"new_status"`
	ChangedAt time.Time    `json:"changed_at"`
}

// DriverRegisteredPayload is the payload for EventDriverRegistered.
type DriverRegisteredPayload struct {
	DriverID    string    `json:"driver_id"`
	RegisteredAt time.Time `json:"registered_at"`
}
