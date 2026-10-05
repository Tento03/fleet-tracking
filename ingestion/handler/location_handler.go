// Package handler implements the bidirectional gRPC LocationService server.
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/Tento03/fleet-tracking/ingestion/kafka"
	locationpb "github.com/Tento03/fleet-tracking/proto"
)

// EventPublisher is the interface any event broker must implement.
type EventPublisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// noopPublisher silently discards every event.
type noopPublisher struct{}

func (noopPublisher) Publish(_ context.Context, _, _ string, _ []byte) error { return nil }

// ── LocationEvent ─────────────────────────────────────────────────────────

// LocationEvent is the JSON payload published to the Kafka topic.
type LocationEvent struct {
	EventID    string  `json:"event_id"`
	DriverCode string  `json:"driver_code"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	Speed      float32 `json:"speed"`
	Heading    float32 `json:"heading"`
	Timestamp  string  `json:"timestamp"`   // RFC3339 UTC from client
	ReceivedAt string  `json:"received_at"` // RFC3339 UTC server wall-clock
}

// ── LocationHandler ───────────────────────────────────────────────────────

// LocationHandler implements locationpb.LocationServiceServer.
type LocationHandler struct {
	locationpb.UnimplementedLocationServiceServer

	publisher EventPublisher
	topic     string
	log       *slog.Logger
}

// NewLocationHandler constructs a LocationHandler.
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

// StreamLocation implements bidirectional streaming.
// It loops over stream.Recv() until io.EOF and responds with LocationAck per event.
func (h *LocationHandler) StreamLocation(
	stream locationpb.LocationService_StreamLocationServer,
) error {
	ctx := stream.Context()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			h.log.Info("gRPC client stream closed normally")
			return nil
		}
		if err != nil {
			h.log.Error("gRPC stream receive error", "error", err)
			return err
		}

		eventID := req.GetEventId()
		driverCode := req.GetDriverCode()

		// ── 1. Validation ────────────────────────────────────────────────
		if reason := validateRequest(req); reason != "" {
			h.log.Warn("rejected invalid GPS frame",
				"event_id", eventID,
				"driver_code", driverCode,
				"reason", reason,
			)
			if sendErr := stream.Send(&locationpb.LocationAck{
				EventId:  eventID,
				Accepted: false,
				Message:  reason,
			}); sendErr != nil {
				return sendErr
			}
			continue
		}

		// ── 2. Build Kafka payload with received_at ──────────────────────
		payload := h.buildEvent(req)
		bytes, err := json.Marshal(payload)
		if err != nil {
			h.log.Error("failed to marshal location event",
				"event_id", eventID,
				"driver_code", driverCode,
				"error", err,
			)
			if sendErr := stream.Send(&locationpb.LocationAck{
				EventId:  eventID,
				Accepted: false,
				Message:  "failed to marshal event payload",
			}); sendErr != nil {
				return sendErr
			}
			continue
		}

		// ── 3. Publish to Kafka (non-blocking) ───────────────────────────
		if err := h.publisher.Publish(ctx, h.topic, driverCode, bytes); err != nil {
			if errors.Is(err, kafka.ErrBackpressure) {
				h.log.Warn("dropping frame due to backpressure",
					"event_id", eventID,
					"driver_code", driverCode,
				)
				if sendErr := stream.Send(&locationpb.LocationAck{
					EventId:  eventID,
					Accepted: false,
					Message:  "backpressure",
				}); sendErr != nil {
					return sendErr
				}
				continue
			}

			h.log.Error("failed to publish to kafka",
				"event_id", eventID,
				"driver_code", driverCode,
				"error", err,
			)
			if sendErr := stream.Send(&locationpb.LocationAck{
				EventId:  eventID,
				Accepted: false,
				Message:  "internal publish failure",
			}); sendErr != nil {
				return sendErr
			}
			continue
		}

		// ── 4. Success Ack ───────────────────────────────────────────────
		h.log.Debug("GPS frame accepted",
			"event_id", eventID,
			"driver_code", driverCode,
			"lat", req.GetLatitude(),
			"lng", req.GetLongitude(),
			"speed", req.GetSpeed(),
		)

		if sendErr := stream.Send(&locationpb.LocationAck{
			EventId:  eventID,
			Accepted: true,
			Message:  "accepted",
		}); sendErr != nil {
			return sendErr
		}
	}
}

// buildEvent converts a LocationRequest into the Kafka LocationEvent JSON payload.
func (h *LocationHandler) buildEvent(req *locationpb.LocationRequest) LocationEvent {
	var clientTime time.Time
	if req.GetTimestamp() != nil {
		clientTime = req.GetTimestamp().AsTime().UTC()
	} else {
		clientTime = time.Now().UTC()
	}

	return LocationEvent{
		EventID:    req.GetEventId(),
		DriverCode: req.GetDriverCode(),
		Latitude:   req.GetLatitude(),
		Longitude:  req.GetLongitude(),
		Speed:      req.GetSpeed(),
		Heading:    req.GetHeading(),
		Timestamp:  clientTime.Format(time.RFC3339),
		ReceivedAt: time.Now().UTC().Format(time.RFC3339),
	}
}

// validateRequest validates mandatory constraints.
// Returns an empty string if valid, otherwise a rejection reason.
func validateRequest(req *locationpb.LocationRequest) string {
	if req.GetDriverCode() == "" {
		return "driver_code is empty"
	}
	lat := req.GetLatitude()
	if lat < -90 || lat > 90 {
		return fmt.Sprintf("latitude %f out of range [-90, 90]", lat)
	}
	lng := req.GetLongitude()
	if lng < -180 || lng > 180 {
		return fmt.Sprintf("longitude %f out of range [-180, 180]", lng)
	}
	if req.GetSpeed() < 0 {
		return fmt.Sprintf("speed %f cannot be negative", req.GetSpeed())
	}
	if req.GetTimestamp() != nil {
		ts := req.GetTimestamp().AsTime()
		if ts.After(time.Now().Add(5 * time.Minute)) {
			return fmt.Sprintf("timestamp is more than 5 minutes in the future: %s", ts.Format(time.RFC3339))
		}
	}
	return ""
}
