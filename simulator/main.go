// Package main is the GPS simulator.
// It registers each simulated driver via POST /drivers (idempotent), then
// opens a bidirectional streaming gRPC connection to the ingestion service
// for each driver, sending realistic GPS updates and reading LocationAcks.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	locationpb "github.com/Tento03/fleet-tracking/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ── Constants & Configuration ─────────────────────────────────────────────

const (
	defaultServerAddr  = "localhost:50051"
	defaultTrackingURL = "http://localhost:8080"
	sendInterval       = 3 * time.Second
	minSpeed           = 20.0 // km/h
	maxSpeed           = 60.0 // km/h
	headingDelta       = 15.0 // max heading change per tick (degrees)
	backoffBase        = 1 * time.Second
	backoffMax         = 30 * time.Second
	backoffFactor      = 2.0
)

// ── Driver seed positions (Medan area) ────────────────────────────────────

type driverSeed struct {
	code    string
	name    string
	phone   string
	vehicle string
	lat     float64
	lng     float64
}

var drivers = []driverSeed{
	{
		code:    "driver-001",
		name:    "Budi Santoso",
		phone:   "081234567801",
		vehicle: "BK 1001 AA",
		lat:     -3.5952,
		lng:     98.6722,
	},
	{
		code:    "driver-002",
		name:    "Rian Hidayat",
		phone:   "081234567802",
		vehicle: "BK 2002 BB",
		lat:     -3.6012,
		lng:     98.6800,
	},
	{
		code:    "driver-003",
		name:    "Dewi Lestari",
		phone:   "081234567803",
		vehicle: "BK 3003 CC",
		lat:     -3.5880,
		lng:     98.6650,
	},
}

// ── driverState tracks simulated movement ──────────────────────────────────

type driverState struct {
	code    string
	lat     float64
	lng     float64
	heading float64 // degrees, 0 = North, clockwise
	speed   float64 // km/h
	rng     *rand.Rand
}

func newDriverState(seed driverSeed) *driverState {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(seed.lat*1e6)))
	return &driverState{
		code:    seed.code,
		lat:     seed.lat,
		lng:     seed.lng,
		heading: rng.Float64() * 360,
		speed:   minSpeed + rng.Float64()*(maxSpeed-minSpeed),
		rng:     rng,
	}
}

func (d *driverState) advance() {
	delta := (d.rng.Float64()*2 - 1) * headingDelta
	d.heading = math.Mod(d.heading+delta+360, 360)
	d.speed = minSpeed + d.rng.Float64()*(maxSpeed-minSpeed)

	distKm := d.speed * sendInterval.Hours()
	headingRad := d.heading * math.Pi / 180
	d.lat += (distKm / 111.32) * math.Cos(headingRad)
	d.lng += (distKm / (111.32 * math.Cos(d.lat*math.Pi/180))) * math.Sin(headingRad)

	d.lat = math.Max(-90, math.Min(90, d.lat))
	d.lng = math.Max(-180, math.Min(180, d.lng))
}

// ── Driver Registration (idempotent POST /drivers) ────────────────────────

func registerDriver(ctx context.Context, trackingURL string, seed driverSeed, logger *slog.Logger) error {
	url := fmt.Sprintf("%s/drivers", trackingURL)
	payload := map[string]string{
		"code":    seed.code,
		"name":    seed.name,
		"phone":   seed.phone,
		"vehicle": seed.vehicle,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		logger.Info("driver registered successfully (or already exists)",
			"code", seed.code,
			"status_code", resp.StatusCode,
		)
		return nil
	}

	return fmt.Errorf("registration returned status %d: %s", resp.StatusCode, string(respBytes))
}

// ── gRPC connection helper ────────────────────────────────────────────────

func dialGRPC(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

// ── runDriver manages lifecycle for a single driver ───────────────────────

func runDriver(ctx context.Context, seed driverSeed, serverAddr, apiKey string, logger *slog.Logger) {
	state := newDriverState(seed)
	backoff := backoffBase

	for {
		select {
		case <-ctx.Done():
			logger.Info("driver worker stopping", "driver_code", state.code)
			return
		default:
		}

		conn, err := dialGRPC(serverAddr)
		if err != nil {
			logger.Error("dial failed – will retry",
				"driver_code", state.code,
				"error", err,
				"backoff", backoff,
			)
			sleepWithContext(ctx, backoff)
			backoff = minDuration(time.Duration(float64(backoff)*backoffFactor), backoffMax)
			continue
		}

		backoff = backoffBase
		client := locationpb.NewLocationServiceClient(conn)

		// Attach auth metadata
		streamCtx := ctx
		if apiKey != "" {
			md := metadata.Pairs("authorization", "Bearer "+apiKey)
			streamCtx = metadata.NewOutgoingContext(ctx, md)
		}

		stream, err := client.StreamLocation(streamCtx)
		if err != nil {
			logger.Error("failed to open stream – will retry",
				"driver_code", state.code,
				"error", err,
				"backoff", backoff,
			)
			conn.Close()
			sleepWithContext(ctx, backoff)
			backoff = minDuration(time.Duration(float64(backoff)*backoffFactor), backoffMax)
			continue
		}

		logger.Info("bidirectional stream opened", "driver_code", state.code)

		// Send & receive loop
		streamErr := runBidirectionalStream(streamCtx, stream, state, logger)
		_ = stream.CloseSend()
		conn.Close()

		if streamErr == nil {
			return
		}

		logger.Warn("stream error – reconnecting",
			"driver_code", state.code,
			"error", streamErr,
			"backoff", backoff,
		)
		sleepWithContext(ctx, backoff)
		backoff = minDuration(time.Duration(float64(backoff)*backoffFactor), backoffMax)
	}
}

func runBidirectionalStream(
	ctx context.Context,
	stream locationpb.LocationService_StreamLocationClient,
	state *driverState,
	logger *slog.Logger,
) error {
	errChan := make(chan error, 2)

	// Goroutine reading LocationAcks
	go func() {
		for {
			ack, err := stream.Recv()
			if err != nil {
				errChan <- fmt.Errorf("recv ack error: %w", err)
				return
			}
			logger.Debug("received location ack",
				"driver_code", state.code,
				"event_id", ack.GetEventId(),
				"accepted", ack.GetAccepted(),
				"message", ack.GetMessage(),
			)
		}
	}()

	// Goroutine sending LocationRequests
	go func() {
		ticker := time.NewTicker(sendInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				errChan <- nil
				return
			case <-ticker.C:
				state.advance()
				eventID := uuid.New().String()

				req := &locationpb.LocationRequest{
					EventId:    eventID,
					DriverCode: state.code,
					Latitude:   state.lat,
					Longitude:  state.lng,
					Speed:      float32(state.speed),
					Heading:    float32(state.heading),
					Timestamp:  timestamppb.Now(),
				}

				if err := stream.Send(req); err != nil {
					errChan <- fmt.Errorf("send frame error: %w", err)
					return
				}

				logger.Info("sent GPS frame",
					"driver_code", state.code,
					"event_id", eventID,
					"lat", state.lat,
					"lng", state.lng,
					"speed", state.speed,
					"heading", state.heading,
				)
			}
		}
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errChan:
		return err
	}
}

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

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// ── main ──────────────────────────────────────────────────────────────────

func main() {
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	serverAddr := getEnv("INGESTION_SERVER_ADDR", defaultServerAddr)
	trackingURL := getEnv("TRACKING_API_URL", defaultTrackingURL)
	apiKey := getEnv("INGESTION_API_KEY", "")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("simulator starting: registering drivers with tracking service",
		"tracking_url", trackingURL,
		"drivers_count", len(drivers),
	)

	// Step 1: Idempotent registration with tracking service
	for _, seed := range drivers {
		if err := registerDriver(ctx, trackingURL, seed, logger); err != nil {
			logger.Warn("could not register driver (tracking service may be down, continuing)",
				"driver_code", seed.code,
				"error", err,
			)
		}
	}

	// Step 2: Start GPS streaming workers
	var wg sync.WaitGroup
	for _, seed := range drivers {
		wg.Add(1)
		seed := seed
		go func() {
			defer wg.Done()
			runDriver(ctx, seed, serverAddr, apiKey, logger)
		}()
	}

	logger.Info("simulator running",
		"drivers", len(drivers),
		"ingestion_server", serverAddr,
	)

	wg.Wait()
	logger.Info("simulator stopped")
}
