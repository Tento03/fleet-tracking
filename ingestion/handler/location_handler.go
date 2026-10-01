// Package handler implements the gRPC LocationService server.
package handler

import (
	"fmt"
	"io"
	"log/slog"

	locationpb "github.com/Tento03/fleet-tracking/proto"
)

// EventPublisher is the interface that Kafka (or any other broker) must
// implement. Prompt 2 will provide the real implementation; for now a
// no-op stub is used so the ingestion service compiles without Kafka.
type EventPublisher interface {
	// PublishLocation forwards a validated location request to the event bus.
	// Implementations must be safe for concurrent use.
	PublishLocation(req *locationpb.LocationRequest) error
}

// noopPublisher is a stub that discards every event.
// It is used when no real publisher is injected.
type noopPublisher struct{}

func (noopPublisher) PublishLocation(_ *locationpb.LocationRequest) error { return nil }

// LocationHandler implements locationpb.LocationServiceServer.
// It validates incoming GPS frames and forwards valid ones to publisher.
type LocationHandler struct {
	locationpb.UnimplementedLocationServiceServer

	publisher EventPublisher
	log       *slog.Logger
}

// NewLocationHandler constructs a LocationHandler.
// If publisher is nil a no-op stub is used (safe for early development).
func NewLocationHandler(publisher EventPublisher, logger *slog.Logger) *LocationHandler {
	if publisher == nil {
		publisher = noopPublisher{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &LocationHandler{
		publisher: publisher,
		log:       logger,
	}
}

// StreamLocation implements the client-streaming RPC.
// It loops over incoming LocationRequest frames until the client closes the
// stream (io.EOF), then replies with a summary LocationResponse.
func (h *LocationHandler) StreamLocation(
	stream locationpb.LocationService_StreamLocationServer,
) error {
	var received int
	var skipped int

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// Client closed the stream – send summary and finish cleanly.
			msg := fmt.Sprintf("%d locations received (%d skipped)", received, skipped)
			h.log.Info("stream closed by client", "received", received, "skipped", skipped)
			return stream.SendAndClose(&locationpb.LocationResponse{
				Success: true,
				Message: msg,
			})
		}
		if err != nil {
			h.log.Error("stream recv error", "error", err)
			return err
		}

		// ── Validation ───────────────────────────────────────────────────────
		if validationErr := validateRequest(req); validationErr != "" {
			h.log.Warn("invalid GPS frame – skipping",
				"driver_id", req.GetDriverId(),
				"reason", validationErr,
			)
			skipped++
			continue
		}

		// ── Logging ──────────────────────────────────────────────────────────
		h.log.Info("GPS received",
			"driver_id", req.GetDriverId(),
			"lat", req.GetLatitude(),
			"lng", req.GetLongitude(),
			"speed", req.GetSpeed(),
		)

		// ── Publish ──────────────────────────────────────────────────────────
		if err := h.publisher.PublishLocation(req); err != nil {
			// Log the publish failure but do NOT drop the stream – the driver
			// should not be penalised for a transient broker outage.
			h.log.Error("failed to publish location event",
				"driver_id", req.GetDriverId(),
				"error", err,
			)
		}

		received++
	}
}

// validateRequest checks the fields of a LocationRequest and returns an
// error description string. An empty string means the request is valid.
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
