// Package utils — response helpers aligned with KONTEKS GLOBAL envelope.
// { "data": ... } for success, { "error": { "code": "...", "message": "..." } } for errors.
package utils

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

// ── Standard envelope helpers ─────────────────────────────────────────────────

// OK writes { "data": data } with HTTP 200.
func OK(c *gin.Context, data any) {
	c.JSON(http.StatusOK, gin.H{"data": data})
}

// Created writes { "data": data } with HTTP 201.
func Created(c *gin.Context, data any) {
	c.JSON(http.StatusCreated, gin.H{"data": data})
}

// Fail writes { "error": { "code": code, "message": msg } } with the given status.
func Fail(c *gin.Context, status int, code, msg string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": msg}})
}

// Error maps an error value to the correct HTTP status and envelope.
func Error(c *gin.Context, err error) {
	var appErr *AppError
	if errors.As(err, &appErr) {
		Fail(c, appErr.Code, http.StatusText(appErr.Code), appErr.Message)
		return
	}
	switch {
	case errors.Is(err, ErrDriverNotFound):
		Fail(c, http.StatusNotFound, "DRIVER_NOT_FOUND", err.Error())
	case errors.Is(err, ErrLocationNotFound):
		Fail(c, http.StatusNotFound, "LOCATION_NOT_FOUND", err.Error())
	case errors.Is(err, ErrNotFound):
		Fail(c, http.StatusNotFound, "NOT_FOUND", err.Error())
	case errors.Is(err, ErrInvalidInput):
		Fail(c, http.StatusBadRequest, "INVALID_INPUT", err.Error())
	case errors.Is(err, ErrConflict):
		Fail(c, http.StatusConflict, "CONFLICT", err.Error())
	default:
		Fail(c, http.StatusInternalServerError, "INTERNAL_ERROR", "an unexpected error occurred")
	}
}
