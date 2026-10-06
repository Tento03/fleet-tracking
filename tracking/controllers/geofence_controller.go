// Package controllers provides the HTTP handlers for the tracking service.
package controllers

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Tento03/fleet-tracking/tracking/models"
	"github.com/Tento03/fleet-tracking/tracking/services"
	"github.com/Tento03/fleet-tracking/tracking/utils"
)

// GeofenceController handles all /geofences HTTP endpoints.
type GeofenceController struct {
	svc *services.GeofenceService
	log *slog.Logger
}

// NewGeofenceController constructs a GeofenceController.
func NewGeofenceController(svc *services.GeofenceService, logger *slog.Logger) *GeofenceController {
	if logger == nil {
		logger = slog.Default()
	}
	return &GeofenceController{svc: svc, log: logger}
}

// ── POST /geofences ───────────────────────────────────────────────────────

// Create godoc
//
//	@Summary     Create a new geofence
//	@Tags        geofences
//	@Accept      json
//	@Produce     json
//	@Param       body body models.Geofence true "Geofence payload"
//	@Success     201 {object} models.Geofence
//	@Failure     400 {object} utils.ErrorResponse
//	@Router      /geofences [post]
func (ctrl *GeofenceController) Create(c *gin.Context) {
	var g models.Geofence
	if err := c.ShouldBindJSON(&g); err != nil {
		c.JSON(http.StatusBadRequest, utils.ErrorResponse{
			Error:   http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	if err := ctrl.svc.CreateGeofence(c.Request.Context(), &g); err != nil {
		ctrl.log.Warn("geofence create failed", "error", err)
		c.JSON(http.StatusBadRequest, utils.ErrorResponse{
			Error:   http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, g)
}

// ── GET /geofences ────────────────────────────────────────────────────────

// List godoc
//
//	@Summary     List geofences
//	@Tags        geofences
//	@Produce     json
//	@Param       active query bool false "Filter to active only (default true)"
//	@Success     200 {array} models.Geofence
//	@Router      /geofences [get]
func (ctrl *GeofenceController) List(c *gin.Context) {
	activeOnly := true
	if c.Query("active") == "false" {
		activeOnly = false
	}

	geofences, err := ctrl.svc.ListGeofences(c.Request.Context(), activeOnly)
	if err != nil {
		utils.RespondError(c, err)
		return
	}
	c.JSON(http.StatusOK, geofences)
}

// ── GET /geofences/:id ────────────────────────────────────────────────────

// GetByID godoc
//
//	@Summary     Get a geofence by ID
//	@Tags        geofences
//	@Produce     json
//	@Param       id path string true "Geofence UUID"
//	@Success     200 {object} models.Geofence
//	@Failure     404 {object} utils.ErrorResponse
//	@Router      /geofences/{id} [get]
func (ctrl *GeofenceController) GetByID(c *gin.Context) {
	g, err := ctrl.svc.GetGeofence(c.Request.Context(), c.Param("id"))
	if err != nil {
		utils.RespondError(c, err)
		return
	}
	c.JSON(http.StatusOK, g)
}

// ── PUT /geofences/:id ────────────────────────────────────────────────────

// Update godoc
//
//	@Summary     Update a geofence
//	@Tags        geofences
//	@Accept      json
//	@Produce     json
//	@Param       id   path string          true "Geofence UUID"
//	@Param       body body models.Geofence true "Updated geofence payload"
//	@Success     200 {object} models.Geofence
//	@Failure     400,404 {object} utils.ErrorResponse
//	@Router      /geofences/{id} [put]
func (ctrl *GeofenceController) Update(c *gin.Context) {
	id := c.Param("id")

	existing, err := ctrl.svc.GetGeofence(c.Request.Context(), id)
	if err != nil {
		utils.RespondError(c, err)
		return
	}

	if err := c.ShouldBindJSON(existing); err != nil {
		c.JSON(http.StatusBadRequest, utils.ErrorResponse{
			Error:   http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	// Ensure the ID from the URL takes precedence.
	existing.ID = id

	if err := ctrl.svc.UpdateGeofence(c.Request.Context(), existing); err != nil {
		ctrl.log.Warn("geofence update failed", "id", id, "error", err)
		c.JSON(http.StatusBadRequest, utils.ErrorResponse{
			Error:   http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, existing)
}

// ── DELETE /geofences/:id ─────────────────────────────────────────────────

// Delete godoc
//
//	@Summary     Deactivate (soft-delete) a geofence
//	@Tags        geofences
//	@Produce     json
//	@Param       id path string true "Geofence UUID"
//	@Success     200 {object} gin.H
//	@Failure     404 {object} utils.ErrorResponse
//	@Router      /geofences/{id} [delete]
func (ctrl *GeofenceController) Delete(c *gin.Context) {
	id := c.Param("id")
	if err := ctrl.svc.DeleteGeofence(c.Request.Context(), id); err != nil {
		utils.RespondError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "geofence deactivated", "id": id})
}

// ── GET /geofences/alerts ─────────────────────────────────────────────────

// GetAlerts godoc
//
//	@Summary     List geofence crossing alerts
//	@Tags        geofences
//	@Produce     json
//	@Param       driver_id    query string false "Filter by driver UUID"
//	@Param       geofence_id  query string false "Filter by geofence UUID"
//	@Param       from         query string false "RFC3339 start (e.g. 2024-01-01T00:00:00Z)"
//	@Param       to           query string false "RFC3339 end"
//	@Success     200 {array}  models.GeofenceAlert
//	@Failure     400 {object} utils.ErrorResponse
//	@Router      /geofences/alerts [get]
func (ctrl *GeofenceController) GetAlerts(c *gin.Context) {
	driverID := c.Query("driver_id")
	geofenceID := c.Query("geofence_id")

	var from, to time.Time
	var parseErr error

	if raw := c.Query("from"); raw != "" {
		from, parseErr = time.Parse(time.RFC3339, raw)
		if parseErr != nil {
			c.JSON(http.StatusBadRequest, utils.ErrorResponse{
				Error:   http.StatusText(http.StatusBadRequest),
				Message: "invalid 'from' timestamp; use RFC3339 format",
			})
			return
		}
	}
	if raw := c.Query("to"); raw != "" {
		to, parseErr = time.Parse(time.RFC3339, raw)
		if parseErr != nil {
			c.JSON(http.StatusBadRequest, utils.ErrorResponse{
				Error:   http.StatusText(http.StatusBadRequest),
				Message: "invalid 'to' timestamp; use RFC3339 format",
			})
			return
		}
	}

	alerts, err := ctrl.svc.GetAlerts(c.Request.Context(), driverID, geofenceID, from, to)
	if err != nil {
		utils.RespondError(c, err)
		return
	}
	c.JSON(http.StatusOK, alerts)
}

// respondGeofenceError maps geofence-specific errors to HTTP codes.
// Currently delegated to utils.RespondError via the ErrGeofenceNotFound sentinel.
func respondGeofenceError(c *gin.Context, err error) {
	if errors.Is(err, utils.ErrGeofenceNotFound) {
		c.JSON(http.StatusNotFound, utils.ErrorResponse{
			Error:   http.StatusText(http.StatusNotFound),
			Message: err.Error(),
		})
		return
	}
	utils.RespondError(c, err)
}
