// Package main is the GPS simulator.
// It spawns one goroutine per driver, each maintaining a single long-lived
// client-streaming gRPC call to the ingestion service.
// Drivers perform a realistic random-walk with smooth heading changes.
// On SIGINT / SIGTERM each stream is closed cleanly via CloseAndRecv.
// On transient stream errors the goroutine reconnects with exponential backoff.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	locationpb "github.com/Tento03/fleet-tracking/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ── Constants ─────────────────────────────────────────────────────────────

const (
	serverAddr    = "localhost:50051"
	sendInterval  = 3 * time.Second
	minSpeed      = 20.0 // km/h
	maxSpeed      = 60.0 // km/h
	headingDelta  = 15.0 // max heading change per tick (degrees)
	backoffBase   = 1 * time.Second
	backoffMax    = 30 * time.Second
	backoffFactor = 2.0
)

// ── Driver seed positions (Medan area) ────────────────────────────────────

type driverSeed struct {
	id  string
	lat float64
	lng float64
}

var drivers = []driverSeed{
	{"driver-001", -3.5952, 98.6722},
	{"driver-002", -3.6012, 98.6800},
	{"driver-003", -3.5880, 98.6650},
}

// ── driverState tracks the current simulated position and heading ─────────

type driverState struct {
	id      string
	lat     float64
	lng     float64
	heading float64 // degrees, 0 = North, clockwise
	speed   float64 // km/h
	rng     *rand.Rand
}

func newDriverState(seed driverSeed) *driverState {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(seed.lat*1e6)))
	return &driverState{
		id:      seed.id,
		lat:     seed.lat,
		lng:     seed.lng,
		heading: rng.Float64() * 360,
		speed:   minSpeed + rng.Float64()*(maxSpeed-minSpeed),
		rng:     rng,
	}
}

// advance moves the driver one tick forward.
// heading changes smoothly (±headingDelta per tick).
// speed varies randomly within [minSpeed, maxSpeed].
func (d *driverState) advance() {
	// Smooth heading change.
	delta := (d.rng.Float64()*2 - 1) * headingDelta
	d.heading = math.Mod(d.heading+delta+360, 360)

	// Randomise speed within band.
	d.speed = minSpeed + d.rng.Float64()*(maxSpeed-minSpeed)

	// Distance covered in one tick (km).
	distKm := d.speed * sendInterval.Hours()

	// Convert to lat/lng delta.
	// 1 degree latitude ≈ 111.32 km.
	// 1 degree longitude ≈ 111.32 * cos(lat) km.
	headingRad := d.heading * math.Pi / 180
	d.lat += (distKm / 111.32) * math.Cos(headingRad)
	d.lng += (distKm / (111.32 * math.Cos(d.lat*math.Pi/180))) * math.Sin(headingRad)

	// Clamp to valid ranges.
	d.lat = math.Max(-90, math.Min(90, d.lat))
	d.lng = math.Max(-180, math.Min(180, d.lng))
}

// ── gRPC connection helper ────────────────────────────────────────────────

func dialGRPC() (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	return conn, err
}

// ── runDriver manages the lifecycle for a single driver ──────────────────

func runDriver(ctx context.Context, seed driverSeed, logger *slog.Logger) {
	state := newDriverState(seed)
	backoff := backoffBase

	for {
		// Check for shutdown before attempting (re)connection.
		select {
		case <-ctx.Done():
			logger.Info("driver shutting down", "driver_id", state.id)
			return
		default:
		}

		// ── Dial ──────────────────────────────────────────────────────────
		conn, err := dialGRPC()
		if err != nil {
			logger.Error("dial failed – will retry",
				"driver_id", state.id,
				"error", err,
				"backoff", backoff,
			)
			sleepWithContext(ctx, backoff)
			backoff = minDuration(time.Duration(float64(backoff)*backoffFactor), backoffMax)
			continue
		}

		// Reset backoff on successful connection.
		backoff = backoffBase

		client := locationpb.NewLocationServiceClient(conn)

		// ── Open stream ───────────────────────────────────────────────────
		stream, err := client.StreamLocation(ctx)
		if err != nil {
			logger.Error("failed to open stream – will retry",
				"driver_id", state.id,
				"error", err,
				"backoff", backoff,
			)
			conn.Close()
			sleepWithContext(ctx, backoff)
			backoff = minDuration(time.Duration(float64(backoff)*backoffFactor), backoffMax)
			continue
		}

		logger.Info("stream opened", "driver_id", state.id)

		// ── Send loop ─────────────────────────────────────────────────────
		streamErr := sendLoop(ctx, stream, state, logger)

		// Close stream regardless of reason.
		resp, closeErr := stream.CloseAndRecv()
		if closeErr != nil {
			logger.Warn("CloseAndRecv error", "driver_id", state.id, "error", closeErr)
		} else {
			logger.Info("stream closed by client",
				"driver_id", state.id,
				"server_response", resp.GetMessage(),
			)
		}
		conn.Close()

		if streamErr == nil {
			// Clean shutdown requested via context.
			return
		}

		// Transient error – reconnect with backoff.
		logger.Warn("stream error – reconnecting",
			"driver_id", state.id,
			"error", streamErr,
			"backoff", backoff,
		)
		sleepWithContext(ctx, backoff)
		backoff = minDuration(time.Duration(float64(backoff)*backoffFactor), backoffMax)
	}
}

// sendLoop sends GPS frames at sendInterval until ctx is cancelled or the
// stream returns an error. Returns nil on clean shutdown, error otherwise.
func sendLoop(
	ctx context.Context,
	stream locationpb.LocationService_StreamLocationClient,
	state *driverState,
	logger *slog.Logger,
) error {
	ticker := time.NewTicker(sendInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil // clean shutdown

		case <-ticker.C:
			state.advance()

			req := &locationpb.LocationRequest{
				DriverId:  state.id,
				Latitude:  state.lat,
				Longitude: state.lng,
				Speed:     float32(state.speed),
				Timestamp: time.Now().UnixMilli(),
			}

			if err := stream.Send(req); err != nil {
				return fmt.Errorf("send failed: %w", err)
			}

			logger.Debug("location sent",
				"driver_id", state.id,
				"lat", state.lat,
				"lng", state.lng,
				"speed", state.speed,
			)
		}
	}
}

// ── Utility helpers ───────────────────────────────────────────────────────

func sleepWithContext(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// ── main ──────────────────────────────────────────────────────────────────

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	for _, seed := range drivers {
		wg.Add(1)
		seed := seed // capture loop variable
		go func() {
			defer wg.Done()
			runDriver(ctx, seed, logger)
		}()
	}

	logger.Info("simulator started", "drivers", len(drivers), "server", serverAddr)

	// Wait for all driver goroutines to finish.
	wg.Wait()
	logger.Info("simulator stopped")
}
