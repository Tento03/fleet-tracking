// Package routes registers the Gin HTTP routes for the tracking service.
package routes

import (
	"database/sql"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/Tento03/fleet-tracking/tracking/controllers"
	"github.com/Tento03/fleet-tracking/tracking/repositories"
	trackingws "github.com/Tento03/fleet-tracking/tracking/websocket"
)

// KafkaChecker checks if the Kafka consumer is active.
type KafkaChecker interface {
	IsConnected() bool
}

// Config holds everything the router needs from the outside world.
type Config struct {
	// External deps
	DriverRepo    repositories.DriverRepository
	LocationRepo  repositories.LocationRepository
	LocationCache repositories.LocationCache
	Hub           *trackingws.Hub
	KafkaChecker  KafkaChecker

	// Config values
	CORSOrigin string
	Timezone   *time.Location
	Logger     *slog.Logger

	// DB/Redis for health-check
	SQLDB  *sql.DB
	RedisCl *redis.Client
}

// Setup wires all HTTP routes and middleware onto the given Gin engine.
// Call this from main after all dependencies are initialised.
func Setup(r *gin.Engine, cfg Config) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// ── Request logger middleware ──────────────────────────────────────────
	r.Use(func(c *gin.Context) {
		start := time.Now()
		c.Next()
		logger.Info("request",
			"method",  c.Request.Method,
			"path",    c.Request.URL.Path,
			"status",  c.Writer.Status(),
			"latency", time.Since(start),
			"ip",      c.ClientIP(),
		)
	})

	// ── Build controllers ──────────────────────────────────────────────────
	driverCtrl := controllers.NewDriverController(
		cfg.DriverRepo, cfg.LocationCache, cfg.Hub, logger,
	)
	locationCtrl := controllers.NewLocationController(
		cfg.DriverRepo, cfg.LocationRepo, cfg.LocationCache, cfg.Timezone, logger,
	)
	dashCtrl := controllers.NewDashboardController(
		cfg.DriverRepo, cfg.LocationRepo, cfg.Hub, cfg.CORSOrigin, cfg.Timezone, logger,
	)

	// ── Ping ──────────────────────────────────────────────────────────────
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "tracking"})
	})

	// ── Health ────────────────────────────────────────────────────────────
	r.GET("/health", func(c *gin.Context) {
		status := gin.H{"status": "ok", "service": "tracking"}
		allHealthy := true

		if cfg.SQLDB != nil {
			if err := cfg.SQLDB.PingContext(c.Request.Context()); err != nil {
				status["mysql"] = "down"
				allHealthy = false
			} else {
				status["mysql"] = "up"
			}
		}
		if cfg.RedisCl != nil {
			if err := cfg.RedisCl.Ping(c.Request.Context()).Err(); err != nil {
				status["redis"] = "down"
				allHealthy = false
			} else {
				status["redis"] = "up"
			}
		}
		if cfg.KafkaChecker != nil {
			if cfg.KafkaChecker.IsConnected() {
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

	// ── WebSocket ─────────────────────────────────────────────────────────
	r.GET("/ws", dashCtrl.ServeWS)

	// ── Dashboard ─────────────────────────────────────────────────────────
	r.GET("/dashboard/stats", dashCtrl.Stats)

	// ── Drivers ───────────────────────────────────────────────────────────
	// IMPORTANT: /drivers/active must be registered BEFORE /drivers/:id so Gin
	// does not swallow the literal "active" segment as a path parameter.
	r.GET("/drivers/active", driverCtrl.ListActive)

	r.POST("/drivers", driverCtrl.Create)
	r.GET("/drivers", driverCtrl.List)
	r.GET("/drivers/:id", driverCtrl.GetByID)
	r.PATCH("/drivers/:id/status", driverCtrl.UpdateStatus)

	// ── Locations ─────────────────────────────────────────────────────────
	r.GET("/drivers/:id/location", locationCtrl.GetLastLocation)
	r.GET("/drivers/:id/history", locationCtrl.GetHistory)
}

// Register is kept for backward compatibility with the old signature used in
// main.go from Prompt 3.  New callers should use Setup.
func Register(r *gin.Engine, sqlDB *sql.DB, rdb *redis.Client, kc KafkaChecker) {
	// No-op shim: real wiring is done via Setup in the updated main.go.
	_ = sqlDB
	_ = rdb
	_ = kc
}
