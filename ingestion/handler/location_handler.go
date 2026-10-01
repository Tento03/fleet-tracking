// Package handler implements the gRPC LocationService server.
package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	locationpb "github.com/Tento03/fleet-tracking/proto"
)

// EventPublisher is the interface any event broker must implement.
// The handler depends only on this interface, never on Kafka directly,
// which keeps the handler testable without a running broker.
type EventPublisher interface {
	// Publish sends a raw JSON payload to the given topic, using key for
	// partition routing. Implementations must be safe for concurrent use.
	Publish(topic, key string, value []byte) error
}

// noopPublisher silently discards every event.
// Used as a safe default when no real publisher is injected.
type noopPublisher struct{}

func (noopPublisher) Publish(_, _ string, _ []byte) error { return nil }

// ── LocationEvent ─────────────────────────────────────────────────────────

// LocationEvent is the JSON payload written to the Kafka topic.
// timestamp is serialised as RFC3339 UTC ("2026-09-26T08:00:00Z").
type LocationEvent struct {
	DriverID  string  `json:"driver_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Speed     float32 `json:"speed"`
	Timestamp string  `json:"timestamp"` // RFC3339 UTC
}

// ── LocationHandler ───────────────────────────────────────────────────────

// LocationHandler implements locationpb.LocationServiceServer.
// It validates incoming GPS frames, serialises them to JSON, and publishes
// them to the event bus via EventPublisher.
type LocationHandler struct {
	locationpb.UnimplementedLocationServiceServer

	publisher EventPublisher
	topic     string
	log       *slog.Logger
}

// NewLocationHandler constructs a LocationHandler.
// - publisher: if nil, a no-op stub is used (safe for tests / early dev).
// - topic: Kafka topic name (e.g. "location.events").
// - logger: if nil, slog.Default() is used.
func NewLocationHandler(publisher EventPublisher, topic string, logger *slog.Logger) *LocationHandler {
	if publisher == nil {
		publisher = noopPublisher{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	if topic == "" {
		topic = "location.events"
	}
	return &LocationHandler{
		publisher: publisher,
		topic:     topic,
		log:       logger,
	}
}

// StreamLocation implements the client-streaming RPC.
// It loops over incoming LocationRequest frames until the client closes the
// stream (io.EOF), then replies with a summary LocationResponse that reports
// how many events were published successfully and how many were skipped/failed.
func (h *LocationHandler) StreamLocation(
	stream locationpb.LocationService_StreamLocationServer,
) error {
	var (
		received  int // frames accepted (valid)
		skipped   int // frames rejected by validation
		published int // frames successfully published to broker
		failed    int // frames that passed validation but failed to publish
	)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// Client closed the stream gracefully – send final summary.
			msg := fmt.Sprintf(
				"%d received, %d published, %d failed, %d skipped",
				received, published, failed, skipped,
			)
			h.log.Info("stream closed by client",
				"received", received,
				"published", published,
				"failed", failed,
				"skipped", skipped,
			)
			return stream.SendAndClose(&locationpb.LocationResponse{
				Success: true,
				Message: msg,
			})
		}
		if err != nil {
			h.log.Error("stream recv error", "error", err)
			return err
		}

		// ── Validation ───────────────────────────────────────────────────
		if reason := validateRequest(req); reason != "" {
			h.log.Warn("invalid GPS frame – skipping",
				"driver_id", req.GetDriverId(),
				"reason", reason,
			)
			skipped++
			continue
		}
		received++

		// ── Logging ──────────────────────────────────────────────────────
		h.log.Info("GPS received",
			"driver_id", req.GetDriverId(),
			"lat", req.GetLatitude(),
			"lng", req.GetLongitude(),
			"speed", req.GetSpeed(),
		)

		// ── Build Kafka payload ───────────────────────────────────────────
		payload := h.buildEvent(req)
		bytes, err := json.Marshal(payload)
		if err != nil {
			// json.Marshal should never fail for this struct; log and skip.
			h.log.Error("failed to marshal location event",
				"driver_id", req.GetDriverId(),
				"error", err,
			)
			failed++
			continue
		}

		// ── Publish ──────────────────────────────────────────────────────
		if err := h.publisher.Publish(h.topic, req.GetDriverId(), bytes); err != nil {
			// A publish error must not kill the gRPC stream.  The driver
			// should keep sending and Kafka will (hopefully) recover soon.
			h.log.Error("failed to publish to kafka",
				"driver_id", req.GetDriverId(),
				"topic", h.topic,
				"error", err,
			)
			failed++
			continue
		}

		h.log.Info("Published to "+h.topic, "driver_id", req.GetDriverId())
		published++
	}
}

// buildEvent converts a LocationRequest into the Kafka JSON payload.
// If req.Timestamp is zero the current wall clock (UTC) is used.
func (h *LocationHandler) buildEvent(req *locationpb.LocationRequest) LocationEvent {
	var ts time.Time
	if req.GetTimestamp() == 0 {
		ts = time.Now().UTC()
	} else {
		ts = time.UnixMilli(req.GetTimestamp()).UTC()
	}

	return LocationEvent{
		DriverID:  req.GetDriverId(),
		Latitude:  req.GetLatitude(),
		Longitude: req.GetLongitude(),
		Speed:     req.GetSpeed(),
		Timestamp: ts.Format(time.RFC3339),
	}
}

// ── validateRequest ───────────────────────────────────────────────────────

// validateRequest checks the mandatory fields of a LocationRequest.
// Returns an empty string when the request is valid, otherwise a human-readable
// reason describing which field is invalid.
func validateRequest(req *locationpb.LocationRequest) string {
	if req.GetDriverId() == "" {
		return "driver_id is empty"
	}
	lat := req.GetLatitude()
	if lat < -90 || lat > 90 {
		return fmt.Sprintf("latitude %f out of range [-90, 90]", lat)
	}
	lng := req.GetLongitude()
	if lng < -180 || lng > 180 {
		return fmt.Sprintf("longitude %f out of range [-180, 180]", lng)
	}
	return ""
}
