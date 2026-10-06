// Package utils provides shared error types and HTTP response helpers
// for the tracking service.
package utils

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

// ── AppError ──────────────────────────────────────────────────────────────

// AppError is a domain error that carries an HTTP status code alongside the
// underlying cause. It allows the transport layer to map business errors to
// appropriate HTTP responses without scattering status codes throughout the
// application logic.
type AppError struct {
	Code    int    // HTTP status code
	Message string // human-readable message sent to the client
	Err     error  // underlying cause (may be nil)
}

func (e *AppError) Error() string {
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

// Unwrap allows errors.Is / errors.As to traverse the chain.
func (e *AppError) Unwrap() error { return e.Err }

// New creates a new AppError.
func New(code int, message string, cause error) *AppError {
	return &AppError{Code: code, Message: message, Err: cause}
}

// ── Sentinel errors ───────────────────────────────────────────────────────

// Sentinel errors for the tracking service domain. Use errors.Is to check
// these; never compare error values directly.
var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = errors.New("resource not found")

	// ErrInvalidInput is returned when the caller supplies malformed data.
	ErrInvalidInput = errors.New("invalid input")

	// ErrDriverNotFound is returned when a driver ID resolves to no record.
	ErrDriverNotFound = errors.New("driver not found")

	// ErrLocationNotFound is returned when no location history matches the query.
	ErrLocationNotFound = errors.New("location history not found")

	// ErrGeofenceNotFound is returned when a geofence ID resolves to no record.
	ErrGeofenceNotFound = errors.New("geofence not found")

	// ErrDatabase is returned on unrecoverable MySQL/GORM failures.
	ErrDatabase = errors.New("database error")

	// ErrCache is returned on Redis failures that should not halt processing.
	ErrCache = errors.New("cache error")

	// ErrConflict is returned when a create operation would violate uniqueness.
	ErrConflict = errors.New("resource conflict")

	// ErrKafka is returned when the Kafka consumer encounters a fatal error.
	ErrKafka = errors.New("kafka error")
)

// ── HTTP helpers ──────────────────────────────────────────────────────────

// ErrorResponse is the standardised JSON body for all error responses.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// RespondError maps an error to the appropriate HTTP status code and writes
// a JSON ErrorResponse body. If err is an *AppError its Code and Message are
// used directly; otherwise the error is treated as an internal server error.
func RespondError(c *gin.Context, err error) {
	var appErr *AppError
	if errors.As(err, &appErr) {
		c.JSON(appErr.Code, ErrorResponse{
			Error:   http.StatusText(appErr.Code),
			Message: appErr.Message,
		})
		return
	}

	// Fallback: map well-known sentinel errors.
	switch {
	case errors.Is(err, ErrNotFound),
		errors.Is(err, ErrDriverNotFound),
		errors.Is(err, ErrLocationNotFound),
		errors.Is(err, ErrGeofenceNotFound):
		c.JSON(http.StatusNotFound, ErrorResponse{
			Error:   http.StatusText(http.StatusNotFound),
			Message: err.Error(),
		})
	case errors.Is(err, ErrInvalidInput):
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
	case errors.Is(err, ErrConflict):
		c.JSON(http.StatusConflict, ErrorResponse{
			Error:   http.StatusText(http.StatusConflict),
			Message: err.Error(),
		})
	default:
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   http.StatusText(http.StatusInternalServerError),
			Message: "an unexpected error occurred",
		})
	}
}

// RespondOK writes a 200 JSON response.
func RespondOK(c *gin.Context, data any) {
	c.JSON(http.StatusOK, data)
}

// RespondCreated writes a 201 JSON response.
func RespondCreated(c *gin.Context, data any) {
	c.JSON(http.StatusCreated, data)
}
