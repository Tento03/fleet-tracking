// Package dto defines request and response data-transfer objects.
package dto

import "time"

// CreateDriverRequest is the JSON body for POST /drivers.
type CreateDriverRequest struct {
	Name    string `json:"name"    binding:"required,min=1,max=100"`
	Phone   string `json:"phone"   binding:"omitempty,max=20"`
	Vehicle string `json:"vehicle" binding:"omitempty,max=50"`
}

// UpdateDriverStatusRequest is the JSON body for PATCH /drivers/:id/status.
type UpdateDriverStatusRequest struct {
	Status string `json:"status" binding:"required,oneof=offline online on_trip"`
}

// DriverResponse is the JSON representation of a driver.
type DriverResponse struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Phone     string `json:"phone"`
	Vehicle   string `json:"vehicle"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

// ActiveDriverResponse extends DriverResponse with last-known location.
type ActiveDriverResponse struct {
	DriverResponse
	LastLocation *LocationSnapshotResponse `json:"last_location"`
}

// LocationSnapshotResponse is returned by GET /drivers/:id/location.
type LocationSnapshotResponse struct {
	DriverID  string  `json:"driver_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Speed     float32 `json:"speed"`
	UpdatedAt string  `json:"updated_at"`
}

// LocationHistoryItem is one GPS point in the history response.
type LocationHistoryItem struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Speed     float32 `json:"speed"`
	Timestamp string  `json:"timestamp"`
}

// LocationHistoryResponse is returned by GET /drivers/:id/history.
type LocationHistoryResponse struct {
	DriverID    string                `json:"driver_id"`
	Date        string                `json:"date"`
	TotalPoints int                   `json:"total_points"`
	Locations   []LocationHistoryItem `json:"locations"`
}

// DashboardStatsResponse is returned by GET /dashboard/stats.
type DashboardStatsResponse struct {
	TotalDrivers        int64     `json:"total_drivers"`
	Online              int64     `json:"online"`
	Offline             int64     `json:"offline"`
	OnTrip              int64     `json:"on_trip"`
	TotalLocationsToday int64     `json:"total_locations_today"`
	AsOf                time.Time `json:"as_of"`
}
