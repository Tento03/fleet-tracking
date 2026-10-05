// Package controllers implements the HTTP handlers for the tracking service.
package controllers

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Tento03/fleet-tracking/tracking/dto"
	"github.com/Tento03/fleet-tracking/tracking/models"
	"github.com/Tento03/fleet-tracking/tracking/repositories"
	"github.com/Tento03/fleet-tracking/tracking/utils"
	"github.com/Tento03/fleet-tracking/tracking/websocket"
)

// DriverController handles REST operations on the /drivers resource.
type DriverController struct {
	driverRepo    repositories.DriverRepository
	locationCache repositories.LocationCache
	broadcaster   websocket.Broadcaster
	log           *slog.Logger
}

// NewDriverController constructs a DriverController with its dependencies.
func NewDriverController(
	driverRepo repositories.DriverRepository,
	locationCache repositories.LocationCache,
	broadcaster websocket.Broadcaster,
	logger *slog.Logger,
) *DriverController {
	if logger == nil {
		logger = slog.Default()
	}
	return &DriverController{
		driverRepo:    driverRepo,
		locationCache: locationCache,
		broadcaster:   broadcaster,
		log:           logger,
	}
}

// ── POST /drivers ─────────────────────────────────────────────────────────────

// Create registers a new driver (UUID, status offline) and broadcasts
// driver_registered.
//
// Request : { "name": "...", "phone": "...", "vehicle": "..." }
// Response: 201 { "data": DriverResponse }
func (dc *DriverController) Create(c *gin.Context) {
	var req dto.CreateDriverRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		utils.Fail(c, http.StatusBadRequest, "INVALID_INPUT", err.Error())
		return
	}

	driver := &models.Driver{
		// ID left blank — BeforeCreate hook assigns UUID v4.
		Name:    req.Name,
		Phone:   req.Phone,
		Vehicle: req.Vehicle,
		Status:  models.StatusOffline,
	}

	if err := dc.driverRepo.Create(c.Request.Context(), driver); err != nil {
		dc.log.Error("create driver: db error", "error", err)
		utils.Error(c, err)
		return
	}

	// Broadcast driver_registered event.
	dc.broadcastDriverRegistered(driver)

	utils.Created(c, toDriverResponse(driver))
}

// ── GET /drivers ──────────────────────────────────────────────────────────────

// List returns all drivers ordered by created_at DESC.
//
// Response: 200 { "data": [ DriverResponse, ... ] }
func (dc *DriverController) List(c *gin.Context) {
	drivers, err := dc.driverRepo.FindAll(c.Request.Context())
	if err != nil {
		utils.Error(c, err)
		return
	}
	resp := make([]dto.DriverResponse, len(drivers))
	for i := range drivers {
		resp[i] = toDriverResponse(&drivers[i])
	}
	utils.OK(c, resp)
}

// ── GET /drivers/active ───────────────────────────────────────────────────────

// ListActive returns drivers whose status is online or on_trip.
// Each item includes the last-known location from Redis (null when absent).
//
// Response: 200 { "data": [ ActiveDriverResponse, ... ] }
func (dc *DriverController) ListActive(c *gin.Context) {
	ctx := c.Request.Context()

	drivers, err := dc.driverRepo.FindByStatuses(ctx, []models.DriverStatus{
		models.StatusOnline,
		models.StatusOnTrip,
	})
	if err != nil {
		utils.Error(c, err)
		return
	}

	// Bulk-fetch last-known locations via MGET.
	ids := make([]string, len(drivers))
	for i, d := range drivers {
		ids[i] = d.ID
	}

	snapshots, err := dc.locationCache.GetManyLast(ctx, ids)
	if err != nil {
		dc.log.Warn("list active: cache mget failed – continuing without locations", "error", err)
		snapshots = make([]*models.LocationSnapshot, len(drivers))
	}

	resp := make([]dto.ActiveDriverResponse, len(drivers))
	for i, d := range drivers {
		resp[i] = dto.ActiveDriverResponse{
			DriverResponse: toDriverResponse(&d),
		}
		if i < len(snapshots) && snapshots[i] != nil {
			resp[i].LastLocation = toLocationSnapshotResponse(snapshots[i])
		}
	}

	utils.OK(c, resp)
}

// ── GET /drivers/:id ──────────────────────────────────────────────────────────

// GetByID returns a single driver by its ID.
//
// Response: 200 { "data": DriverResponse } | 404
func (dc *DriverController) GetByID(c *gin.Context) {
	id := c.Param("id")
	driver, err := dc.driverRepo.FindByID(c.Request.Context(), id)
	if err != nil {
		utils.Error(c, err)
		return
	}
	utils.OK(c, toDriverResponse(driver))
}

// ── PATCH /drivers/:id/status ─────────────────────────────────────────────────

// UpdateStatus changes a driver's status and broadcasts driver_status_changed.
//
// Request : { "status": "online" | "offline" | "on_trip" }
// Response: 200 { "data": DriverResponse } | 400 | 404
func (dc *DriverController) UpdateStatus(c *gin.Context) {
	id := c.Param("id")

	var req dto.UpdateDriverStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		utils.Fail(c, http.StatusBadRequest, "INVALID_INPUT", err.Error())
		return
	}

	newStatus := models.DriverStatus(req.Status)

	ctx := c.Request.Context()

	// Fetch the current driver to capture oldStatus for the broadcast.
	driver, err := dc.driverRepo.FindByID(ctx, id)
	if err != nil {
		utils.Error(c, err)
		return
	}

	if driver.Status == newStatus {
		// No-op — return current state.
		utils.OK(c, toDriverResponse(driver))
		return
	}

	oldStatus := driver.Status

	if err := dc.driverRepo.UpdateStatus(ctx, id, newStatus); err != nil {
		utils.Error(c, err)
		return
	}
	driver.Status = newStatus

	// Broadcast driver_status_changed.
	dc.broadcastStatusChanged(id, oldStatus, newStatus)

	utils.OK(c, toDriverResponse(driver))
}

// ── private helpers ───────────────────────────────────────────────────────────

func toDriverResponse(d *models.Driver) dto.DriverResponse {
	return dto.DriverResponse{
		ID:        d.ID,
		Name:      d.Name,
		Phone:     d.Phone,
		Vehicle:   d.Vehicle,
		Status:    string(d.Status),
		CreatedAt: d.CreatedAt.UTC().Format(time.RFC3339),
	}
}

func toLocationSnapshotResponse(s *models.LocationSnapshot) *dto.LocationSnapshotResponse {
	return &dto.LocationSnapshotResponse{
		DriverID:  s.DriverID,
		Latitude:  s.Latitude,
		Longitude: s.Longitude,
		Speed:     s.Speed,
		UpdatedAt: s.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func (dc *DriverController) broadcastDriverRegistered(d *models.Driver) {
	payload := map[string]any{
		"event": models.EventDriverRegistered,
		"driver": map[string]any{
			"id":         d.ID,
			"name":       d.Name,
			"phone":      d.Phone,
			"vehicle":    d.Vehicle,
			"status":     string(d.Status),
			"created_at": d.CreatedAt.UTC().Format(time.RFC3339),
		},
	}
	dc.broadcastJSON(payload)
}

func (dc *DriverController) broadcastStatusChanged(
	driverID string,
	oldStatus, newStatus models.DriverStatus,
) {
	payload := map[string]any{
		"event":      models.EventDriverStatusChanged,
		"driver_id":  driverID,
		"old_status": string(oldStatus),
		"new_status": string(newStatus),
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	}
	dc.broadcastJSON(payload)
}

func (dc *DriverController) broadcastJSON(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		dc.log.Error("driver controller: broadcast marshal error", "error", err)
		return
	}
	dc.broadcaster.Broadcast(data)
}
