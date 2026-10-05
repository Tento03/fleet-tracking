// Package services contains the core business logic for the tracking service.
package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Tento03/fleet-tracking/tracking/models"
	"github.com/Tento03/fleet-tracking/tracking/repositories"
	"github.com/Tento03/fleet-tracking/tracking/websocket"
)

const locationTTL = 300 * time.Second

// LocationService orchestrates the processing of a single location event:
//
//  1. Ensure the driver exists (auto-register if new).
//  2. Cache the position in Redis.
//  3. Append to MySQL location_history.
//  4. Emit WebSocket broadcast events.
type LocationService struct {
	driverRepo    repositories.DriverRepository
	locationRepo  repositories.LocationRepository
	locationCache repositories.LocationCache
	broadcaster   websocket.Broadcaster
	log           *slog.Logger
}

// NewLocationService constructs a LocationService with all required
// dependencies. All arguments are required and must be non-nil.
func NewLocationService(
	driverRepo repositories.DriverRepository,
	locationRepo repositories.LocationRepository,
	locationCache repositories.LocationCache,
	broadcaster websocket.Broadcaster,
	logger *slog.Logger,
) *LocationService {
	if logger == nil {
		logger = slog.Default()
	}
	return &LocationService{
		driverRepo:    driverRepo,
		locationRepo:  locationRepo,
		locationCache: locationCache,
		broadcaster:   broadcaster,
		log:           logger,
	}
}

// ── ProcessLocation ───────────────────────────────────────────────────────────

// ProcessLocation is the hot path called once per Kafka message.
//
// Processing order:
//
//	a) FirstOrCreate the driver. If created → broadcast driver_registered.
//	b) Redis SetLast (TTL 300 s). Failure = warn + continue.
//	c) Insert into location_history. Failure = return error.
//	d) If driver was offline → update to online + broadcast driver_status_changed.
//	e) Broadcast location_updated with the current status.
func (s *LocationService) ProcessLocation(ctx context.Context, event *models.LocationEvent) error {
	// ── Parse timestamp ───────────────────────────────────────────────────
	ts, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		ts = time.Now().UTC()
		s.log.Warn("invalid event timestamp – using UTC now",
			"driver_id", event.DriverID,
			"raw_timestamp", event.Timestamp,
		)
	}

	// ── a) Ensure driver exists ───────────────────────────────────────────
	candidate := &models.Driver{
		ID:     event.DriverID,
		Name:   "Driver " + event.DriverID,
		Status: models.StatusOffline,
	}
	driver, created, err := s.driverRepo.FirstOrCreate(ctx, candidate)
	if err != nil {
		return fmt.Errorf("location service: ensure driver: %w", err)
	}
	if created {
		s.log.Info("auto-registered new driver", "driver_id", driver.ID)
		s.broadcastDriverRegistered(driver)
	}

	// ── b) Redis SetLast (non-fatal) ──────────────────────────────────────
	snapshot := &models.LocationSnapshot{
		DriverID:  event.DriverID,
		Latitude:  event.Latitude,
		Longitude: event.Longitude,
		Speed:     event.Speed,
		Status:    driver.Status,
		Timestamp: ts,
		UpdatedAt: time.Now().UTC(),
	}
	if cacheErr := s.locationCache.SetLast(ctx, snapshot, locationTTL); cacheErr != nil {
		s.log.Warn("failed to update location cache – continuing",
			"driver_id", event.DriverID,
			"error", cacheErr,
		)
	}

	// ── c) Insert location_history (fatal on error) ────────────────────────
	history := &models.LocationHistory{
		DriverID:  event.DriverID,
		Latitude:  event.Latitude,
		Longitude: event.Longitude,
		Speed:     event.Speed,
		Timestamp: ts,
	}
	if err := s.locationRepo.Insert(ctx, history); err != nil {
		return fmt.Errorf("location service: insert history: %w", err)
	}

	// ── d) Auto-online: offline → online ──────────────────────────────────
	currentStatus := driver.Status
	if driver.Status == models.StatusOffline {
		if err := s.driverRepo.UpdateStatus(ctx, driver.ID, models.StatusOnline); err != nil {
			s.log.Warn("failed to update driver status to online",
				"driver_id", driver.ID,
				"error", err,
			)
		} else {
			s.log.Info("driver came online",
				"driver_id", driver.ID,
				"old_status", models.StatusOffline,
			)
			s.broadcastStatusChanged(driver.ID, models.StatusOffline, models.StatusOnline)
			currentStatus = models.StatusOnline
		}
	}

	// ── e) Broadcast location_updated ─────────────────────────────────────
	s.broadcastLocationUpdated(event, ts, currentStatus)

	return nil
}

// ── broadcast helpers ─────────────────────────────────────────────────────────

func (s *LocationService) broadcastDriverRegistered(d *models.Driver) {
	evt := models.DriverRegisteredEvent{
		Event: models.EventDriverRegistered,
		Driver: models.DriverInfo{
			ID:        d.ID,
			Name:      d.Name,
			Phone:     d.Phone,
			Vehicle:   d.Vehicle,
			Status:    d.Status,
			CreatedAt: d.CreatedAt,
		},
	}
	s.broadcast(evt)
}

func (s *LocationService) broadcastStatusChanged(
	driverID string,
	oldStatus, newStatus models.DriverStatus,
) {
	evt := models.DriverStatusChangedEvent{
		Event:     models.EventDriverStatusChanged,
		DriverID:  driverID,
		OldStatus: oldStatus,
		NewStatus: newStatus,
		Timestamp: time.Now().UTC(),
	}
	s.broadcast(evt)
}

func (s *LocationService) broadcastLocationUpdated(
	event *models.LocationEvent,
	ts time.Time,
	status models.DriverStatus,
) {
	evt := models.LocationUpdatedEvent{
		Event:     models.EventLocationUpdated,
		DriverID:  event.DriverID,
		Latitude:  event.Latitude,
		Longitude: event.Longitude,
		Speed:     event.Speed,
		Status:    status,
		Timestamp: ts,
	}
	s.broadcast(evt)
}

func (s *LocationService) broadcast(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		s.log.Error("failed to marshal WebSocket event", "error", err)
		return
	}
	s.broadcaster.Broadcast(data)
}
