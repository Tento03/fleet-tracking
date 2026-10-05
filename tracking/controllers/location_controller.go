// Package controllers implements the HTTP handlers for the tracking service.
package controllers

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Tento03/fleet-tracking/tracking/dto"
	"github.com/Tento03/fleet-tracking/tracking/repositories"
	"github.com/Tento03/fleet-tracking/tracking/utils"
)

const (
	defaultHistoryLimit = 5000
	maxHistoryLimit     = 20000
)

// LocationController handles REST operations on /drivers/:id/location and /drivers/:id/history.
type LocationController struct {
	driverRepo    repositories.DriverRepository
	locationRepo  repositories.LocationRepository
	locationCache repositories.LocationCache
	timezone      *time.Location
	log           *slog.Logger
}

// NewLocationController constructs a LocationController.
func NewLocationController(
	driverRepo repositories.DriverRepository,
	locationRepo repositories.LocationRepository,
	locationCache repositories.LocationCache,
	tz *time.Location,
	logger *slog.Logger,
) *LocationController {
	if logger == nil {
		logger = slog.Default()
	}
	return &LocationController{
		driverRepo:    driverRepo,
		locationRepo:  locationRepo,
		locationCache: locationCache,
		timezone:      tz,
		log:           logger,
	}
}

// ── GET /drivers/:id/location ─────────────────────────────────────────────────

// GetLastLocation returns the last-known GPS position from Redis.
func (lc *LocationController) GetLastLocation(c *gin.Context) {
	driverID := c.Param("id")

	snap, err := lc.locationCache.GetLast(c.Request.Context(), driverID)
	if err != nil {
		utils.Error(c, err)
		return
	}
	if snap == nil {
		utils.Fail(c, http.StatusNotFound, "LOCATION_NOT_FOUND",
			fmt.Sprintf("no location found for driver %s", driverID))
		return
	}

	utils.OK(c, dto.LocationSnapshotResponse{
		DriverID:   snap.DriverID,
		DriverCode: snap.DriverCode,
		Latitude:   snap.Latitude,
		Longitude:  snap.Longitude,
		Speed:      snap.Speed,
		Heading:    snap.Heading,
		UpdatedAt:  snap.UpdatedAt.UTC().Format(time.RFC3339),
	})
}

// ── GET /drivers/:id/history ──────────────────────────────────────────────────

// GetHistory returns GPS history for a driver within a date/time window.
func (lc *LocationController) GetHistory(c *gin.Context) {
	driverID := c.Param("id")
	tz := lc.timezone

	nowLocal := time.Now().In(tz)

	// parse date
	dateStr := c.DefaultQuery("date", nowLocal.Format("2006-01-02"))
	date, err := time.ParseInLocation("2006-01-02", dateStr, tz)
	if err != nil {
		utils.Fail(c, http.StatusBadRequest, "INVALID_INPUT",
			fmt.Sprintf("invalid date format %q, expected YYYY-MM-DD", dateStr))
		return
	}

	// parse start
	startStr := c.DefaultQuery("start", "00:00")
	startTime, err := parseHHMM(startStr, date, tz)
	if err != nil {
		utils.Fail(c, http.StatusBadRequest, "INVALID_INPUT",
			fmt.Sprintf("invalid start format %q, expected HH:MM", startStr))
		return
	}

	// parse end
	endStr := c.DefaultQuery("end", "23:59")
	endTime, err := parseHHMM(endStr, date, tz)
	if err != nil {
		utils.Fail(c, http.StatusBadRequest, "INVALID_INPUT",
			fmt.Sprintf("invalid end format %q, expected HH:MM", endStr))
		return
	}

	if !endTime.After(startTime) {
		utils.Fail(c, http.StatusBadRequest, "INVALID_INPUT",
			"end must be after start")
		return
	}

	// parse limit
	limit := defaultHistoryLimit
	if limitStr := c.Query("limit"); limitStr != "" {
		parsed, err := strconv.Atoi(limitStr)
		if err != nil || parsed <= 0 {
			utils.Fail(c, http.StatusBadRequest, "INVALID_INPUT", "limit must be a positive integer")
			return
		}
		if parsed > maxHistoryLimit {
			parsed = maxHistoryLimit
		}
		limit = parsed
	}

	// ensure driver exists
	if _, err := lc.driverRepo.FindByID(c.Request.Context(), driverID); err != nil {
		utils.Error(c, err)
		return
	}

	// query history (UTC)
	fromUTC := startTime.UTC()
	toUTC := endTime.UTC()

	history, err := lc.locationRepo.FindHistory(c.Request.Context(), driverID, fromUTC, toUTC)
	if err != nil {
		utils.Error(c, err)
		return
	}

	if len(history) > limit {
		history = history[:limit]
	}

	items := make([]dto.LocationHistoryItem, len(history))
	for i, h := range history {
		items[i] = dto.LocationHistoryItem{
			Latitude:  h.Latitude,
			Longitude: h.Longitude,
			Speed:     h.Speed,
			Heading:   h.Heading,
			Timestamp: h.Timestamp.UTC().Format(time.RFC3339),
		}
	}

	utils.OK(c, dto.LocationHistoryResponse{
		DriverID:    driverID,
		Date:        dateStr,
		TotalPoints: len(items),
		Locations:   items,
	})
}

func parseHHMM(s string, date time.Time, tz *time.Location) (time.Time, error) {
	combined := date.Format("2006-01-02") + "T" + s + ":00"
	t, err := time.ParseInLocation("2006-01-02T15:04:05", combined, tz)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}
