// Package services contains the core business logic for the tracking service.
package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Tento03/fleet-tracking/tracking/models"
	"github.com/Tento03/fleet-tracking/tracking/repositories"
	"github.com/Tento03/fleet-tracking/tracking/utils"
	"github.com/Tento03/fleet-tracking/tracking/websocket"
)

const locationTTL = 300 * time.Second

// LocationService orchestrates the processing of a single location event:
//  1. Resolves driver_code → driver UUID using in-memory cache / DB.
//  2. If driver is unregistered → logs warning and rejects event (safe, no crash).
//  3. Caches position snapshot in Redis.
//  4. Appends to MySQL location_history (FK to drivers.id).
//  5. Auto-sets status from offline to online.
//  6. Emits WebSocket broadcast events.
type LocationService struct {
	driverRepo    repositories.DriverRepository
	locationRepo  repositories.LocationRepository
	locationCache repositories.LocationCache
	broadcaster   websocket.Broadcaster
	codeCache     sync.Map // key: driver_code (string) → value: *models.Driver
	log           *slog.Logger
}

// NewLocationService constructs a LocationService with all required dependencies.
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

// ProcessLocation is called once per Kafka message.
func (s *LocationService) ProcessLocation(ctx context.Context, event *models.LocationEvent) error {
	driverCode := event.DriverCode
	if driverCode == "" {
		s.log.Warn("received event without driver_code – rejected", "event_id", event.EventID)
		return nil
	}

	// ── 1. Resolve driver_code → driver UUID ──────────────────────────────
	driver, err := s.resolveDriver(ctx, driverCode)
	if err != nil {
		if errors.Is(err, utils.ErrDriverNotFound) {
			s.log.Warn("unregistered driver code – event rejected",
				"driver_code", driverCode,
				"event_id", event.EventID,
			)
			return nil // Reject safely without crashing or throwing unhandled errors
		}
		return fmt.Errorf("failed to resolve driver %q: %w", driverCode, err)
	}

	// ── 2. Parse timestamp ────────────────────────────────────────────────
	ts, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		ts = time.Now().UTC()
		s.log.Warn("invalid event timestamp – using current UTC",
			"driver_code", driverCode,
			"raw_timestamp", event.Timestamp,
		)
	}

	// ── 3. Redis SetLast (non-fatal) ──────────────────────────────────────
	snapshot := &models.LocationSnapshot{
		DriverID:   driver.ID,
		DriverCode: driver.Code,
		Latitude:   event.Latitude,
		Longitude:  event.Longitude,
		Speed:      event.Speed,
		Heading:    event.Heading,
		Status:     driver.Status,
		Timestamp:  ts,
		UpdatedAt:  time.Now().UTC(),
	}
	if cacheErr := s.locationCache.SetLast(ctx, snapshot, locationTTL); cacheErr != nil {
		s.log.Warn("failed to update location cache – continuing",
			"driver_id", driver.ID,
			"driver_code", driver.Code,
			"error", cacheErr,
		)
	}

	// ── 4. Insert location_history (FK to drivers.id UUID) ─────────────────
	history := &models.LocationHistory{
		DriverID:  driver.ID,
		Latitude:  event.Latitude,
		Longitude: event.Longitude,
		Speed:     event.Speed,
		Heading:   event.Heading,
		Timestamp: ts,
	}
	if err := s.locationRepo.Insert(ctx, history); err != nil {
		return fmt.Errorf("location service: insert history: %w", err)
	}

	// ── 5. Auto-online: offline → online ──────────────────────────────────
	currentStatus := driver.Status
	if driver.Status == models.StatusOffline {
		if err := s.driverRepo.UpdateStatus(ctx, driver.ID, models.StatusOnline); err != nil {
			s.log.Warn("failed to update driver status to online",
				"driver_id", driver.ID,
				"driver_code", driver.Code,
				"error", err,
			)
		} else {
			s.log.Info("driver came online",
				"driver_id", driver.ID,
				"driver_code", driver.Code,
				"old_status", models.StatusOffline,
			)
			driver.Status = models.StatusOnline
			currentStatus = models.StatusOnline
			// Update local cache
			s.codeCache.Store(driverCode, driver)
			s.broadcastStatusChanged(driver, models.StatusOffline, models.StatusOnline)
		}
	}

	// ── 6. Broadcast location_updated ─────────────────────────────────────
	s.broadcastLocationUpdated(driver, event, ts, currentStatus)

	return nil
}

// resolveDriver finds driver by code using in-memory cache or DB query.
func (s *LocationService) resolveDriver(ctx context.Context, code string) (*models.Driver, error) {
	if val, ok := s.codeCache.Load(code); ok {
		if d, ok := val.(*models.Driver); ok {
			return d, nil
		}
	}

	driver, err := s.driverRepo.FindByCode(ctx, code)
	if err != nil {
		return nil, err
	}

	s.codeCache.Store(code, driver)
	return driver, nil
}

// InvalidateDriverCodeCache removes a cached code mapping when needed.
func (s *LocationService) InvalidateDriverCodeCache(code string) {
	s.codeCache.Delete(code)
}

// ── broadcast helpers ─────────────────────────────────────────────────────────

func (s *LocationService) broadcastStatusChanged(
	driver *models.Driver,
	oldStatus, newStatus models.DriverStatus,
) {
	evt := models.DriverStatusChangedEvent{
		Event:      models.EventDriverStatusChanged,
		DriverID:   driver.ID,
		DriverCode: driver.Code,
		OldStatus:  oldStatus,
		NewStatus:  newStatus,
		Timestamp:  time.Now().UTC(),
	}
	s.broadcast(evt)
}

func (s *LocationService) broadcastLocationUpdated(
	driver *models.Driver,
	event *models.LocationEvent,
	ts time.Time,
	status models.DriverStatus,
) {
	evt := models.LocationUpdatedEvent{
		Event:      models.EventLocationUpdated,
		DriverID:   driver.ID,
		DriverCode: driver.Code,
		Latitude:   event.Latitude,
		Longitude:  event.Longitude,
		Speed:      event.Speed,
		Heading:    event.Heading,
		Status:     status,
		Timestamp:  ts,
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
