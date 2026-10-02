// Package routes registers the Gin HTTP routes for the tracking service.
package routes

import (
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// KafkaChecker checks if the Kafka consumer is active.
type KafkaChecker interface {
	IsConnected() bool
}

// Register wires all HTTP routes onto the given Gin engine.
func Register(r *gin.Engine, sqlDB *sql.DB, rdb *redis.Client, kc KafkaChecker) {
	// ── Ping ──────────────────────────────────────────────────────────────
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "tracking"})
	})

	// ── Health ────────────────────────────────────────────────────────────
	r.GET("/health", func(c *gin.Context) {
		status := gin.H{
			"status":  "ok",
			"service": "tracking",
		}

		allHealthy := true

		// Check MySQL
		if sqlDB != nil {
			if err := sqlDB.PingContext(c.Request.Context()); err != nil {
				status["mysql"] = "down"
				allHealthy = false
			} else {
				status["mysql"] = "up"
			}
		}

		// Check Redis
		if rdb != nil {
			if err := rdb.Ping(c.Request.Context()).Err(); err != nil {
				status["redis"] = "down"
				allHealthy = false
			} else {
				status["redis"] = "up"
			}
		}

		// Check Kafka
		if kc != nil {
			if kc.IsConnected() {
				status["kafka"] = "up"
			} else {
				status["kafka"] = "connecting"
			}
		}

		httpStatus := http.StatusOK
		if !allHealthy {
			httpStatus = http.StatusServiceUnavailable
			status["status"] = "degraded"
		}

		c.JSON(httpStatus, status)
	})
}

