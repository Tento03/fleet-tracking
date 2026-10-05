package handler

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	locationpb "github.com/Tento03/fleet-tracking/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockPublisher records published events for test assertions.
type mockPublisher struct {
	events []struct {
		Topic string
		Key   string
		Value []byte
	}
	shouldFail bool
}

func (m *mockPublisher) Publish(_ context.Context, topic, key string, value []byte) error {
	if m.shouldFail {
		return &mockError{msg: "simulated broker down"}
	}
	m.events = append(m.events, struct {
		Topic string
		Key   string
		Value []byte
	}{Topic: topic, Key: key, Value: value})
	return nil
}

type mockError struct{ msg string }

func (e *mockError) Error() string { return e.msg }

func TestLocationHandler_StreamLocation_Bidirectional(t *testing.T) {
	mockPub := &mockPublisher{}
	h := NewLocationHandler(mockPub, "location.events", nil)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	srv := grpc.NewServer()
	locationpb.RegisterLocationServiceServer(srv, h)
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := locationpb.NewLocationServiceClient(conn)
	stream, err := client.StreamLocation(context.Background())
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	// 1. Send valid frame
	err = stream.Send(&locationpb.LocationRequest{
		EventId:    "evt-001",
		DriverCode: "driver-001",
		Latitude:   -3.5952,
		Longitude:  98.6722,
		Speed:      45.5,
		Heading:    90.0,
		Timestamp:  timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("failed to send frame 1: %v", err)
	}

	ack1, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to receive ack 1: %v", err)
	}
	if !ack1.GetAccepted() || ack1.GetEventId() != "evt-001" {
		t.Fatalf("expected ack accepted for evt-001, got: %+v", ack1)
	}

	// 2. Send invalid frame (latitude > 90) -> should be rejected with accepted=false without breaking stream
	err = stream.Send(&locationpb.LocationRequest{
		EventId:    "evt-002",
		DriverCode: "driver-001",
		Latitude:   120.0,
		Longitude:  98.6722,
		Speed:      20.0,
	})
	if err != nil {
		t.Fatalf("failed to send frame 2: %v", err)
	}

	ack2, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to receive ack 2: %v", err)
	}
	if ack2.GetAccepted() {
		t.Fatalf("expected ack rejected for invalid latitude, got accepted")
	}

	// Close stream
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("failed to close send: %v", err)
	}

	// Verify published events
	if len(mockPub.events) != 1 {
		t.Fatalf("expected 1 event published, got %d", len(mockPub.events))
	}

	ev := mockPub.events[0]
	if ev.Topic != "location.events" {
		t.Errorf("expected topic location.events, got %s", ev.Topic)
	}
	if ev.Key != "driver-001" {
		t.Errorf("expected key driver-001, got %s", ev.Key)
	}

	var parsed LocationEvent
	if err := json.Unmarshal(ev.Value, &parsed); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}
	if parsed.DriverCode != "driver-001" || parsed.Speed != 45.5 || parsed.ReceivedAt == "" {
		t.Errorf("unexpected parsed payload: %+v", parsed)
	}
}
