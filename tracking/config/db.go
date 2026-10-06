// Package config provides database initialisation for the tracking service.
package config

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/Tento03/fleet-tracking/tracking/models"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// NewDB opens a GORM connection to MySQL using the DSN built from cfg,
// configures connection-pool settings, and runs AutoMigrate for all models.
//
// DSN options:
//   - parseTime=true  → DATETIME columns decoded as time.Time
//   - loc=UTC         → all timestamps treated as UTC
//   - charset=utf8mb4 → full Unicode support (emoji, CJK, etc.)
func NewDB(cfg *Config) (*gorm.DB, error) {
	dsn := cfg.MySQLDSN()

	// Set GORM log level based on the application log level.
	gormLogLevel := logger.Warn
	if cfg.LogLevel == slog.LevelDebug {
		gormLogLevel = logger.Info // prints every SQL statement
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(gormLogLevel),
		// PrepareStmt caches prepared statements for reuse, reducing round-trips.
		PrepareStmt: true,
	})
	if err != nil {
		return nil, fmt.Errorf("gorm: failed to connect to MySQL: %w", err)
	}

	// ── Connection pool tuning ────────────────────────────────────────────
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("gorm: failed to get underlying sql.DB: %w", err)
	}

	// Maximum number of open connections to the database.
	sqlDB.SetMaxOpenConns(25)
	// Maximum number of idle connections in the pool.
	sqlDB.SetMaxIdleConns(10)
	// Maximum time a connection may be reused (prevents stale conns).
	sqlDB.SetConnMaxLifetime(5 * time.Minute)

	// ── AutoMigrate ───────────────────────────────────────────────────────
	// AutoMigrate creates missing tables and adds missing columns / indexes.
	// It never drops existing columns, making it safe to run on an existing DB.
	if err := db.AutoMigrate(
		&models.Driver{},
		&models.LocationHistory{},
		&models.Geofence{},
		&models.GeofenceState{},
		&models.GeofenceAlert{},
	); err != nil {
		return nil, fmt.Errorf("gorm: AutoMigrate failed: %w", err)
	}

	slog.Info("MySQL connected and schema migrated",
		"host", cfg.DBHost,
		"port", cfg.DBPort,
		"database", cfg.DBName,
	)
	return db, nil
}
