// Package controllers implements the HTTP handlers for the tracking service.
package controllers

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Tento03/fleet-tracking/tracking/dto"
	"github.com/Tento03/fleet-tracking/tracking/models"
	"github.com/Tento03/fleet-tracking/tracking/repositories"
	"github.com/Tento03/fleet-tracking/tracking/utils"
	trackingws "github.com/Tento03/fleet-tracking/tracking/websocket"
)

// DashboardController handles analytics and WebSocket upgrade endpoints.
type DashboardController struct {
	driverRepo   repositories.DriverRepository
	locationRepo repositories.LocationRepository
	hub          *trackingws.Hub
	corsOrigin   string
	timezone     *time.Location
	log          *slog.Logger
}

// NewDashboardController constructs a DashboardController.
func NewDashboardController(
	driverRepo repositories.DriverRepository,
	locationRepo repositories.LocationRepository,
	hub *trackingws.Hub,
	corsOrigin string,
	tz *time.Location,
	logger *slog.Logger,
) *DashboardController {
	if logger == nil {
		logger = slog.Default()
	}
	return &DashboardController{
		driverRepo:   driverRepo,
		locationRepo: locationRepo,
		hub:          hub,
		corsOrigin:   corsOrigin,
		timezone:     tz,
		log:          logger,
	}
}

// ── GET /dashboard/stats ──────────────────────────────────────────────────────

// Stats returns fleet-wide counters.
//
// Response: 200 { "data": DashboardStatsResponse }
func (dc *DashboardController) Stats(c *gin.Context) {
	ctx := c.Request.Context()

	// Driver counts per status.
	counts, err := dc.driverRepo.CountByStatus(ctx)
	if err != nil {
		dc.log.Error("dashboard stats: count by status failed", "error", err)
		utils.Error(c, err)
		return
	}

	total := counts[models.StatusOnline] +
		counts[models.StatusOffline] +
		counts[models.StatusOnTrip]

	// Total location rows for "today" in APP_TIMEZONE.
	tz := dc.timezone
	now := time.Now().In(tz)
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, tz).UTC()
	todayEnd := todayStart.Add(24 * time.Hour)

	locCount, err := dc.locationRepo.CountBetween(ctx, todayStart, todayEnd)
	if err != nil {
		dc.log.Warn("dashboard stats: location count failed", "error", err)
		// Non-fatal — return 0 rather than an error.
		locCount = 0
	}

	utils.OK(c, dto.DashboardStatsResponse{
		TotalDrivers:        total,
		Online:              counts[models.StatusOnline],
		Offline:             counts[models.StatusOffline],
		OnTrip:              counts[models.StatusOnTrip],
		TotalLocationsToday: locCount,
		AsOf:                time.Now().UTC(),
	})
}

// ── GET /ws ───────────────────────────────────────────────────────────────────

// ServeWS upgrades the connection to WebSocket and registers the client.
func (dc *DashboardController) ServeWS(c *gin.Context) {
	trackingws.ServeWS(dc.hub, dc.corsOrigin, c.Writer, c.Request)
}
