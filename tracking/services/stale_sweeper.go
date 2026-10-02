// Package services contains the stale-driver sweeper background goroutine.
package services

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/Tento03/fleet-tracking/tracking/models"
	"github.com/Tento03/fleet-tracking/tracking/repositories"
	"github.com/Tento03/fleet-tracking/tracking/websocket"
)

const sweeperInterval = 30 * time.Second

// StaleSweeper is a background goroutine that periodically detects drivers
// whose Redis key has expired (i.e. no GPS ping in the last 300 s) and marks
// them offline.
//
// A driver is considered stale when:
//   - Their status is online OR on_trip (they should be sending GPS).
//   - Their Redis key "driver:location:{id}" no longer exists.
//
// On detection the sweeper:
//  1. Updates MySQL status → offline.
//  2. Broadcasts driver_status_changed to all WebSocket clients.
type StaleSweeper struct {
	driverRepo    repositories.DriverRepository
	locationCache repositories.LocationCache
	broadcaster   websocket.Broadcaster
	log           *slog.Logger
}

// NewStaleSweeper constructs a StaleSweeper. All arguments must be non-nil.
func NewStaleSweeper(
	driverRepo repositories.DriverRepository,
	locationCache repositories.LocationCache,
	broadcaster websocket.Broadcaster,
	logger *slog.Logger,
) *StaleSweeper {
	if logger == nil {
		logger = slog.Default()
	}
	return &StaleSweeper{
		driverRepo:    driverRepo,
		locationCache: locationCache,
		broadcaster:   broadcaster,
		log:           logger,
	}
}

// Run starts the sweeper ticker. It blocks until ctx is cancelled; intended to
// be launched as a goroutine from main.
func (s *StaleSweeper) Run(ctx context.Context) {
	s.log.Info("stale sweeper started", "interval", sweeperInterval)
	defer s.log.Info("stale sweeper stopped")

	ticker := time.NewTicker(sweeperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

// sweep performs one pass: load active drivers → check Redis → mark stale.
func (s *StaleSweeper) sweep(ctx context.Context) {
	// Load all active (online + on_trip) drivers from MySQL.
	activeDrivers, err := s.driverRepo.FindByStatuses(ctx, []models.DriverStatus{
		models.StatusOnline,
		models.StatusOnTrip,
	})
	if err != nil {
		s.log.Error("stale sweeper: failed to load active drivers", "error", err)
		return
	}

	if len(activeDrivers) == 0 {
		return
	}

	s.log.Debug("stale sweeper: checking drivers", "count", len(activeDrivers))

	for _, driver := range activeDrivers {
		// Skip if a new context cancellation happened mid-sweep.
		if ctx.Err() != nil {
			return
		}

		exists, err := s.locationCache.Exists(ctx, driver.ID)
		if err != nil {
			s.log.Warn("stale sweeper: cache exists check failed",
				"driver_id", driver.ID,
				"error", err,
			)
			// Can't determine staleness — be conservative and skip.
			continue
		}

		if exists {
			// Driver is alive; GPS key still in Redis.
			continue
		}

		// ── Driver is stale ───────────────────────────────────────────
		s.log.Info("stale sweeper: driver went offline",
			"driver_id", driver.ID,
			"last_status", driver.Status,
		)

		oldStatus := driver.Status
		if err := s.driverRepo.UpdateStatus(ctx, driver.ID, models.StatusOffline); err != nil {
			s.log.Error("stale sweeper: failed to update driver status",
				"driver_id", driver.ID,
				"error", err,
			)
			continue
		}

		// Broadcast the status change to all WebSocket clients.
		s.broadcastStatusChanged(driver.ID, oldStatus, models.StatusOffline)
	}
}

// broadcastStatusChanged marshals and broadcasts a driver_status_changed event.
func (s *StaleSweeper) broadcastStatusChanged(
	driverID string,
	oldStatus, newStatus models.DriverStatus,
) {
	envelope := models.WSEvent{
		Type: models.EventDriverStatusChanged,
		Payload: models.DriverStatusChangedPayload{
			DriverID:  driverID,
			OldStatus: oldStatus,
			NewStatus: newStatus,
			ChangedAt: time.Now().UTC(),
		},
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		s.log.Error("stale sweeper: failed to marshal event", "error", err)
		return
	}
	s.broadcaster.Broadcast(data)
}
