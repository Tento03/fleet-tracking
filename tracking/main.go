// Package main is the entry point for the Fleet-Tracking tracking service.
//
// Startup order:
//  1. Load configuration from .env / environment variables.
//  2. Connect to MySQL (GORM) and run AutoMigrate.
//  3. Connect to Redis and verify with Ping.
//  4. Build repositories (DriverRepository, LocationRepository, LocationCache).
//  5. Build WebSocket hub → start hub.Run in goroutine.
//  6. Build LocationService and StaleSweeper.
//  7. Build Kafka consumer → start consumer in goroutine.
//  8. Start StaleSweeper in goroutine.
//  9. Start Gin HTTP server on :APP_PORT with all REST + WebSocket routes.
// 10. Graceful shutdown: cancel ctx → server.Shutdown → consumer.Close → close DB & Redis.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"

	"github.com/Tento03/fleet-tracking/tracking/config"
	trackingkafka "github.com/Tento03/fleet-tracking/tracking/kafka"
	"github.com/Tento03/fleet-tracking/tracking/repositories"
	"github.com/Tento03/fleet-tracking/tracking/routes"
	"github.com/Tento03/fleet-tracking/tracking/services"
	trackingws "github.com/Tento03/fleet-tracking/tracking/websocket"
)

func main() {
	// ── 1. Configuration ───────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// ── Logging (JSON) ─────────────────────────────────────────────────────
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	// ── Background context with cancellation ───────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── 2. MySQL (GORM) ────────────────────────────────────────────────────
	logger.Info("connecting to MySQL", "host", cfg.DBHost, "db", cfg.DBName)
	db, err := config.NewDB(cfg)
	if err != nil {
		logger.Error("failed to connect to MySQL", "error", err)
		os.Exit(1)
	}
	sqlDB, _ := db.DB()

	// ── 3. Redis ───────────────────────────────────────────────────────────
	logger.Info("connecting to Redis", "addr", cfg.RedisAddr())
	redisClient, err := config.NewRedis(cfg)
	if err != nil {
		logger.Error("failed to connect to Redis", "error", err)
		os.Exit(1)
	}

	// ── 4. Repositories ────────────────────────────────────────────────────
	driverRepo := repositories.NewDriverRepository(db)
	locationRepo := repositories.NewLocationRepository(db)
	locationCache := repositories.NewLocationCache(redisClient)

	// ── 5. WebSocket Hub ───────────────────────────────────────────────────
	hub := trackingws.NewHub(logger)
	go hub.Run(ctx)

	// ── 6. Services ────────────────────────────────────────────────────────
	locationService := services.NewLocationService(
		driverRepo,
		locationRepo,
		locationCache,
		hub,
		logger,
	)
	staleSweeper := services.NewStaleSweeper(
		driverRepo,
		locationCache,
		hub,
		logger,
	)

	// ── 7. Kafka Consumer ──────────────────────────────────────────────────
	logger.Info("connecting to Kafka", "brokers", cfg.KafkaBrokers)
	consumer, err := trackingkafka.NewConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaGroupID,
		cfg.KafkaTopic,
		locationService,
		logger,
	)
	if err != nil {
		logger.Error("failed to create Kafka consumer", "error", err)
		os.Exit(1)
	}
	consumer.Start(ctx)
	logger.Info("Kafka consumer started",
		"group", cfg.KafkaGroupID,
		"topic", cfg.KafkaTopic,
	)

	// ── 8. Stale sweeper ───────────────────────────────────────────────────
	go staleSweeper.Run(ctx)

	// ── 9. Timezone ────────────────────────────────────────────────────────
	tz, err := time.LoadLocation(cfg.AppTimezone)
	if err != nil {
		logger.Warn("unknown APP_TIMEZONE – falling back to UTC", "tz", cfg.AppTimezone)
		tz = time.UTC
	}

	// ── Gin HTTP server ────────────────────────────────────────────────────
	if cfg.LogLevel != slog.LevelDebug {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.Recovery())

	if cfg.LogLevel == slog.LevelDebug {
		r.Use(gin.Logger())
	}

	// CORS — allow the configured frontend origin.
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{cfg.CORSOrigin},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// Wire all routes.
	routes.Setup(r, routes.Config{
		DriverRepo:    driverRepo,
		LocationRepo:  locationRepo,
		LocationCache: locationCache,
		Hub:           hub,
		KafkaChecker:  consumer,
		CORSOrigin:    cfg.CORSOrigin,
		Timezone:      tz,
		Logger:        logger,
		SQLDB:         sqlDB,
		RedisCl:       redisClient,
	})

	server := &http.Server{
		Addr:         ":" + cfg.AppPort,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Run HTTP server in background goroutine.
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("Tracking service started", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	// ── 10. Graceful shutdown ──────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		logger.Info("shutdown signal received", "signal", sig)
	case err := <-serverErr:
		logger.Error("HTTP server fatal error", "error", err)
	}

	// Cancel the shared context → stops hub, sweeper, consumer loop.
	cancel()

	// Give in-flight HTTP requests up to 10 s to complete.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	logger.Info("shutting down HTTP server")
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	logger.Info("closing Kafka consumer")
	if err := consumer.Close(); err != nil {
		logger.Error("Kafka consumer close error", "error", err)
	}

	logger.Info("closing Redis")
	if err := redisClient.Close(); err != nil {
		logger.Error("Redis close error", "error", err)
	}

	logger.Info("closing MySQL")
	if err := sqlDB.Close(); err != nil {
		logger.Error("MySQL close error", "error", err)
	}

	logger.Info("tracking service stopped")
}
