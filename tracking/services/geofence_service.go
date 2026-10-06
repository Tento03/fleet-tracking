// Package services contains the core business logic for the tracking service.
package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/Tento03/fleet-tracking/tracking/models"
	"github.com/Tento03/fleet-tracking/tracking/repositories"
	"github.com/Tento03/fleet-tracking/tracking/websocket"
)

// ── constants ──────────────────────────────────────────────────────────────

// earthRadiusM is the mean radius of the Earth in metres (WGS-84).
const earthRadiusM = 6_371_000.0

// ── GeofenceService ───────────────────────────────────────────────────────

// GeofenceService manages geofence CRUD and evaluates boundary crossings for
// every incoming GPS position. It is called from LocationService.CheckGeofences
// after each location event is processed.
type GeofenceService struct {
	repo        repositories.GeofenceRepository
	broadcaster websocket.Broadcaster
	log         *slog.Logger
}

// NewGeofenceService constructs a GeofenceService with all required dependencies.
func NewGeofenceService(
	repo repositories.GeofenceRepository,
	broadcaster websocket.Broadcaster,
	logger *slog.Logger,
) *GeofenceService {
	if logger == nil {
		logger = slog.Default()
	}
	return &GeofenceService{
		repo:        repo,
		broadcaster: broadcaster,
		log:         logger,
	}
}

// ── CRUD operations ───────────────────────────────────────────────────────

// CreateGeofence validates and persists a new geofence.
func (s *GeofenceService) CreateGeofence(ctx context.Context, g *models.Geofence) error {
	if err := validateGeofence(g); err != nil {
		return err
	}
	g.Active = true
	if err := s.repo.Create(ctx, g); err != nil {
		return fmt.Errorf("geofence service: create: %w", err)
	}
	s.log.Info("geofence created", "id", g.ID, "name", g.Name, "type", g.Type)
	return nil
}

// GetGeofence retrieves a single geofence by UUID.
func (s *GeofenceService) GetGeofence(ctx context.Context, id string) (*models.Geofence, error) {
	return s.repo.FindByID(ctx, id)
}

// ListGeofences returns all geofences. Pass activeOnly = true to skip inactive ones.
func (s *GeofenceService) ListGeofences(ctx context.Context, activeOnly bool) ([]models.Geofence, error) {
	return s.repo.FindAll(ctx, activeOnly)
}

// UpdateGeofence validates and applies changes to an existing geofence.
func (s *GeofenceService) UpdateGeofence(ctx context.Context, g *models.Geofence) error {
	if err := validateGeofence(g); err != nil {
		return err
	}
	if err := s.repo.Update(ctx, g); err != nil {
		return fmt.Errorf("geofence service: update: %w", err)
	}
	s.log.Info("geofence updated", "id", g.ID, "name", g.Name)
	return nil
}

// DeleteGeofence soft-deletes a geofence by ID.
func (s *GeofenceService) DeleteGeofence(ctx context.Context, id string) error {
	if err := s.repo.Delete(ctx, id); err != nil {
		return fmt.Errorf("geofence service: delete: %w", err)
	}
	s.log.Info("geofence deactivated", "id", id)
	return nil
}

// GetAlerts returns geofence crossing alerts with optional filters.
func (s *GeofenceService) GetAlerts(
	ctx context.Context,
	driverID, geofenceID string,
	from, to time.Time,
) ([]models.GeofenceAlert, error) {
	return s.repo.FindAlerts(ctx, driverID, geofenceID, from, to)
}

// ── Geofence evaluation ───────────────────────────────────────────────────

// CheckGeofences evaluates all active geofences against the driver's new
// position and fires enter/exit alerts when the containment state changes.
//
// This is called by LocationService.ProcessLocation after each Kafka event.
func (s *GeofenceService) CheckGeofences(
	ctx context.Context,
	driver *models.Driver,
	lat, lng float64,
	ts time.Time,
) {
	geofences, err := s.repo.FindAll(ctx, true) // active only
	if err != nil {
		s.log.Warn("geofence check: failed to load geofences", "error", err)
		return
	}

	for i := range geofences {
		gf := &geofences[i]
		s.evalGeofence(ctx, driver, gf, lat, lng, ts)
	}
}

// evalGeofence runs the containment check for a single geofence and triggers
// state transitions / alerts when the driver crosses a boundary.
func (s *GeofenceService) evalGeofence(
	ctx context.Context,
	driver *models.Driver,
	gf *models.Geofence,
	lat, lng float64,
	ts time.Time,
) {
	inside := s.isInside(gf, lat, lng)

	prevState, err := s.repo.GetState(ctx, driver.ID, gf.ID)
	if err != nil {
		s.log.Warn("geofence check: GetState error",
			"driver_id", driver.ID,
			"geofence_id", gf.ID,
			"error", err,
		)
		return
	}

	// Determine previous containment: treat nil (first seen) as outside.
	wasInside := prevState != nil && prevState.State == models.GeofenceStateInside

	// No state change → nothing to do.
	if inside == wasInside {
		return
	}

	// ── State transition ──────────────────────────────────────────────────
	newStateVal := models.GeofenceStateOutside
	alertType := models.AlertExit
	if inside {
		newStateVal = models.GeofenceStateInside
		alertType = models.AlertEnter
	}

	// Persist new state.
	state := prevState
	if state == nil {
		state = &models.GeofenceState{
			DriverID:   driver.ID,
			GeofenceID: gf.ID,
		}
	}
	state.State = newStateVal
	state.UpdatedAt = time.Now().UTC()
	if err := s.repo.UpsertState(ctx, state); err != nil {
		s.log.Warn("geofence check: UpsertState error",
			"driver_id", driver.ID,
			"geofence_id", gf.ID,
			"error", err,
		)
	}

	// Persist alert record.
	alert := &models.GeofenceAlert{
		DriverID:    driver.ID,
		GeofenceID:  gf.ID,
		AlertType:   alertType,
		Latitude:    lat,
		Longitude:   lng,
		TriggeredAt: ts,
	}
	if err := s.repo.CreateAlert(ctx, alert); err != nil {
		s.log.Warn("geofence check: CreateAlert error",
			"driver_id", driver.ID,
			"geofence_id", gf.ID,
			"error", err,
		)
	}

	// Broadcast WebSocket event.
	s.broadcastAlert(driver, gf, alertType, lat, lng, ts)

	s.log.Info("geofence boundary crossed",
		"driver_id", driver.ID,
		"driver_code", driver.Code,
		"geofence_id", gf.ID,
		"geofence_name", gf.Name,
		"alert_type", alertType,
		"lat", lat,
		"lng", lng,
	)
}

// ── Geometry helpers ──────────────────────────────────────────────────────

// isInside returns true when (lat, lng) is contained by the geofence.
func (s *GeofenceService) isInside(gf *models.Geofence, lat, lng float64) bool {
	switch gf.Type {
	case models.GeofenceCircle:
		return haversineM(lat, lng, gf.CenterLat, gf.CenterLng) <= gf.RadiusM
	case models.GeofencePolygon:
		if gf.Polygon == nil || len(gf.Polygon.Coordinates) == 0 {
			return false
		}
		// GeoJSON: coordinates[0] is the outer ring; [lng, lat] pairs.
		return pointInRing(lat, lng, gf.Polygon.Coordinates[0])
	default:
		return false
	}
}

// haversineM returns the great-circle distance in metres between two WGS-84
// coordinates.
func haversineM(lat1, lng1, lat2, lng2 float64) float64 {
	const toRad = math.Pi / 180
	φ1 := lat1 * toRad
	φ2 := lat2 * toRad
	Δφ := (lat2 - lat1) * toRad
	Δλ := (lng2 - lng1) * toRad

	a := math.Sin(Δφ/2)*math.Sin(Δφ/2) +
		math.Cos(φ1)*math.Cos(φ2)*
			math.Sin(Δλ/2)*math.Sin(Δλ/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return earthRadiusM * c
}

// pointInRing uses the ray-casting algorithm to determine whether the point
// (lat, lng) is inside the polygon ring. The ring is a GeoJSON outer ring:
// [][lng, lat] where ring[i][0] = longitude, ring[i][1] = latitude.
func pointInRing(lat, lng float64, ring [][2]float64) bool {
	n := len(ring)
	inside := false
	j := n - 1
	for i := 0; i < n; i++ {
		xi, yi := ring[i][0], ring[i][1] // lng, lat
		xj, yj := ring[j][0], ring[j][1]
		if ((yi > lat) != (yj > lat)) &&
			(lng < (xj-xi)*(lat-yi)/(yj-yi)+xi) {
			inside = !inside
		}
		j = i
	}
	return inside
}

// ── Broadcast helper ──────────────────────────────────────────────────────

func (s *GeofenceService) broadcastAlert(
	driver *models.Driver,
	gf *models.Geofence,
	alertType models.GeofenceAlertType,
	lat, lng float64,
	ts time.Time,
) {
	evt := models.GeofenceAlertEvent{
		Event:        models.EventGeofenceAlert,
		DriverID:     driver.ID,
		DriverCode:   driver.Code,
		GeofenceID:   gf.ID,
		GeofenceName: gf.Name,
		AlertType:    alertType,
		Latitude:     lat,
		Longitude:    lng,
		TriggeredAt:  ts,
	}
	data, err := json.Marshal(evt)
	if err != nil {
		s.log.Error("geofence: failed to marshal WebSocket event", "error", err)
		return
	}
	s.broadcaster.Broadcast(data)
}

// ── Validation ────────────────────────────────────────────────────────────

func validateGeofence(g *models.Geofence) error {
	if g.Name == "" {
		return fmt.Errorf("geofence name is required")
	}
	switch g.Type {
	case models.GeofenceCircle:
		if g.RadiusM <= 0 {
			return fmt.Errorf("circle geofence requires radius_m > 0")
		}
	case models.GeofencePolygon:
		if g.Polygon == nil || len(g.Polygon.Coordinates) == 0 {
			return fmt.Errorf("polygon geofence requires at least one coordinate ring")
		}
		if len(g.Polygon.Coordinates[0]) < 3 {
			return fmt.Errorf("polygon outer ring requires at least 3 vertices")
		}
	default:
		return fmt.Errorf("geofence type must be 'circle' or 'polygon'")
	}
	return nil
}
