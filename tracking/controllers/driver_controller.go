// Package controllers implements the HTTP handlers for the tracking service.
package controllers

import (
	"encoding/json"
	"errors"
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

// Create registers a driver or returns existing if code matches (idempotent).
//
// Request : { "code": "driver-001", "name": "...", "phone": "...", "vehicle": "..." }
// Response: 201 { "data": DriverResponse } (or 200 if already exists)
func (dc *DriverController) Create(c *gin.Context) {
	ctx := c.Request.Context()

	var req dto.CreateDriverRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		utils.Fail(c, http.StatusBadRequest, "INVALID_INPUT", err.Error())
		return
	}

	// Idempotency check: if driver with this business code already exists, return it
	existing, err := dc.driverRepo.FindByCode(ctx, req.Code)
	if err == nil && existing != nil {
		dc.log.Info("driver code already registered, returning existing", "code", req.Code, "id", existing.ID)
		utils.OK(c, toDriverResponse(existing))
		return
	}
	if err != nil && !errors.Is(err, utils.ErrDriverNotFound) {
		dc.log.Error("check existing driver by code failed", "code", req.Code, "error", err)
		utils.Error(c, err)
		return
	}

	driver := &models.Driver{
		// ID left blank — BeforeCreate hook assigns UUID v4.
		Code:    req.Code,
		Name:    req.Name,
		Phone:   req.Phone,
		Vehicle: req.Vehicle,
		Status:  models.StatusOffline,
	}

	if err := dc.driverRepo.Create(ctx, driver); err != nil {
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

// GetByID returns a single driver by primary UUID.
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
func (dc *DriverController) UpdateStatus(c *gin.Context) {
	id := c.Param("id")

	var req dto.UpdateDriverStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		utils.Fail(c, http.StatusBadRequest, "INVALID_INPUT", err.Error())
		return
	}

	newStatus := models.DriverStatus(req.Status)
	ctx := c.Request.Context()

	driver, err := dc.driverRepo.FindByID(ctx, id)
	if err != nil {
		utils.Error(c, err)
		return
	}

	if driver.Status == newStatus {
		utils.OK(c, toDriverResponse(driver))
		return
	}

	oldStatus := driver.Status

	if err := dc.driverRepo.UpdateStatus(ctx, id, newStatus); err != nil {
		utils.Error(c, err)
		return
	}
	driver.Status = newStatus

	dc.broadcastStatusChanged(driver, oldStatus, newStatus)

	utils.OK(c, toDriverResponse(driver))
}

// ── private helpers ───────────────────────────────────────────────────────────

func toDriverResponse(d *models.Driver) dto.DriverResponse {
	return dto.DriverResponse{
		ID:        d.ID,
		Code:      d.Code,
		Name:      d.Name,
		Phone:     d.Phone,
		Vehicle:   d.Vehicle,
		Status:    string(d.Status),
		CreatedAt: d.CreatedAt.UTC().Format(time.RFC3339),
	}
}

func toLocationSnapshotResponse(s *models.LocationSnapshot) *dto.LocationSnapshotResponse {
	return &dto.LocationSnapshotResponse{
		DriverID:   s.DriverID,
		DriverCode: s.DriverCode,
		Latitude:   s.Latitude,
		Longitude:  s.Longitude,
		Speed:      s.Speed,
		Heading:    s.Heading,
		UpdatedAt:  s.UpdatedAt.UTC().Format(time.RFC3339),
	}
}

func (dc *DriverController) broadcastDriverRegistered(d *models.Driver) {
	payload := models.DriverRegisteredEvent{
		Event: models.EventDriverRegistered,
		Driver: models.DriverInfo{
			ID:        d.ID,
			Code:      d.Code,
			Name:      d.Name,
			Phone:     d.Phone,
			Vehicle:   d.Vehicle,
			Status:    d.Status,
			CreatedAt: d.CreatedAt,
		},
	}
	dc.broadcastJSON(payload)
}

func (dc *DriverController) broadcastStatusChanged(
	driver *models.Driver,
	oldStatus, newStatus models.DriverStatus,
) {
	payload := models.DriverStatusChangedEvent{
		Event:      models.EventDriverStatusChanged,
		DriverID:   driver.ID,
		DriverCode: driver.Code,
		OldStatus:  oldStatus,
		NewStatus:  newStatus,
		Timestamp:  time.Now().UTC(),
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
