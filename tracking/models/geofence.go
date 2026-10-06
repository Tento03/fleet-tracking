// Package models defines the persistent domain entities for the tracking service.
package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ── GeofenceType ──────────────────────────────────────────────────────────

// GeofenceType describes the shape of a geofence boundary.
type GeofenceType string

const (
	GeofenceCircle  GeofenceType = "circle"
	GeofencePolygon GeofenceType = "polygon"
)

// ── GeoJSONPolygon ────────────────────────────────────────────────────────

// GeoJSONPolygon wraps a GeoJSON polygon coordinate ring stored as TEXT.
// It implements driver.Valuer / sql.Scanner for GORM compatibility.
type GeoJSONPolygon struct {
	// Coordinates is a GeoJSON LinearRing: [][lng, lat] where the first and
	// last points are identical.
	Coordinates [][][2]float64 `json:"coordinates"`
}

// Value serialises to JSON for storage.
func (g GeoJSONPolygon) Value() (driver.Value, error) {
	if len(g.Coordinates) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(g)
	return string(b), err
}

// Scan deserialises from a database TEXT/JSON column.
func (g *GeoJSONPolygon) Scan(src any) error {
	if src == nil {
		return nil
	}
	var b []byte
	switch v := src.(type) {
	case string:
		b = []byte(v)
	case []byte:
		b = v
	default:
		return fmt.Errorf("geojson: unsupported scan type %T", src)
	}
	return json.Unmarshal(b, g)
}

// ── Geofence ──────────────────────────────────────────────────────────────

// Geofence is a named geographic boundary (circle or polygon).
//
// Circle geofences use CenterLat, CenterLng, and RadiusM.
// Polygon geofences store a GeoJSON LinearRing in the Polygon column.
type Geofence struct {
	// ID is a UUID v4 primary key.
	ID string `gorm:"type:varchar(36);primaryKey" json:"id"`

	// Name is a human-readable label for this geofence.
	Name string `gorm:"type:varchar(100);not null" json:"name"`

	// Type determines the shape used for containment checks.
	Type GeofenceType `gorm:"type:enum('circle','polygon');not null" json:"type"`

	// CenterLat is the latitude of the circle's centre (circle only).
	CenterLat float64 `gorm:"type:decimal(10,8)" json:"center_lat"`

	// CenterLng is the longitude of the circle's centre (circle only).
	CenterLng float64 `gorm:"type:decimal(11,8)" json:"center_lng"`

	// RadiusM is the radius in metres (circle only, >0).
	RadiusM float64 `gorm:"type:float" json:"radius_m"`

	// Polygon stores the GeoJSON polygon ring as TEXT (polygon only).
	Polygon *GeoJSONPolygon `gorm:"type:text" json:"polygon,omitempty"`

	// Active controls whether this geofence triggers alerts.
	Active bool `gorm:"default:true;not null" json:"active"`

	// CreatedAt is set once by GORM on insert.
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is updated on every save.
	UpdatedAt time.Time `json:"updated_at"`
}

// BeforeCreate assigns a UUID v4 when ID is blank.
func (g *Geofence) BeforeCreate(_ *gorm.DB) error {
	if g.ID == "" {
		g.ID = uuid.New().String()
	}
	return nil
}

// ── GeofenceState ─────────────────────────────────────────────────────────

// GeofenceStateValue is the driver-geofence containment state.
type GeofenceStateValue string

const (
	GeofenceStateInside  GeofenceStateValue = "inside"
	GeofenceStateOutside GeofenceStateValue = "outside"
)

// GeofenceState tracks whether a specific driver is inside or outside a
// geofence. The composite unique index prevents duplicate state rows.
type GeofenceState struct {
	// ID is a UUID v4 primary key.
	ID string `gorm:"type:varchar(36);primaryKey" json:"id"`

	// DriverID references drivers.id.
	DriverID string `gorm:"type:varchar(36);not null;uniqueIndex:udx_driver_fence" json:"driver_id"`

	// GeofenceID references geofences.id.
	GeofenceID string `gorm:"type:varchar(36);not null;uniqueIndex:udx_driver_fence" json:"geofence_id"`

	// State is the latest containment state.
	State GeofenceStateValue `gorm:"type:enum('inside','outside');not null" json:"state"`

	// UpdatedAt is refreshed on every state change.
	UpdatedAt time.Time `json:"updated_at"`
}

// BeforeCreate assigns a UUID v4 when ID is blank.
func (s *GeofenceState) BeforeCreate(_ *gorm.DB) error {
	if s.ID == "" {
		s.ID = uuid.New().String()
	}
	return nil
}

// ── GeofenceAlertType ─────────────────────────────────────────────────────

// GeofenceAlertType describes the direction of a containment crossing.
type GeofenceAlertType string

const (
	AlertEnter GeofenceAlertType = "enter"
	AlertExit  GeofenceAlertType = "exit"
)

// ── GeofenceAlert ─────────────────────────────────────────────────────────

// GeofenceAlert is an immutable audit record created every time a driver
// crosses a geofence boundary.
type GeofenceAlert struct {
	// ID is a UUID v4 primary key.
	ID string `gorm:"type:varchar(36);primaryKey" json:"id"`

	// DriverID references drivers.id.
	DriverID string `gorm:"type:varchar(36);not null;index" json:"driver_id"`

	// GeofenceID references geofences.id.
	GeofenceID string `gorm:"type:varchar(36);not null;index" json:"geofence_id"`

	// AlertType is the crossing direction (enter | exit).
	AlertType GeofenceAlertType `gorm:"type:enum('enter','exit');not null" json:"alert_type"`

	// Latitude at the moment the boundary was crossed.
	Latitude float64 `gorm:"type:decimal(10,8);not null" json:"latitude"`

	// Longitude at the moment the boundary was crossed.
	Longitude float64 `gorm:"type:decimal(11,8);not null" json:"longitude"`

	// TriggeredAt is the GPS timestamp of the crossing event.
	TriggeredAt time.Time `gorm:"not null;index" json:"triggered_at"`

	// Geofence is the associated geofence (preloaded when needed).
	Geofence Geofence `gorm:"foreignKey:GeofenceID" json:"geofence,omitempty"`
}

// BeforeCreate assigns a UUID v4 when ID is blank.
func (a *GeofenceAlert) BeforeCreate(_ *gorm.DB) error {
	if a.ID == "" {
		a.ID = uuid.New().String()
	}
	return nil
}

// ── WebSocket event ───────────────────────────────────────────────────────

const EventGeofenceAlert = "geofence_alert"

// GeofenceAlertEvent is the WebSocket broadcast payload emitted on every
// geofence crossing.
type GeofenceAlertEvent struct {
	Event        string            `json:"event"`
	DriverID     string            `json:"driver_id"`
	DriverCode   string            `json:"driver_code"`
	GeofenceID   string            `json:"geofence_id"`
	GeofenceName string            `json:"geofence_name"`
	AlertType    GeofenceAlertType `json:"alert_type"`
	Latitude     float64           `json:"latitude"`
	Longitude    float64           `json:"longitude"`
	TriggeredAt  time.Time         `json:"triggered_at"`
}
