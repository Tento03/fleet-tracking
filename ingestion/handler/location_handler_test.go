package handler

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	locationpb "github.com/Tento03/fleet-tracking/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// mockPublisher merekam event yang dipublish untuk validasi test.
type mockPublisher struct {
	events []struct {
		Topic string
		Key   string
		Value []byte
	}
	shouldFail bool
}

func (m *mockPublisher) Publish(topic, key string, value []byte) error {
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

func TestLocationHandler_StreamLocation_Success(t *testing.T) {
	mockPub := &mockPublisher{}
	h := NewLocationHandler(mockPub, "location.events", nil)

	// Buat in-memory gRPC server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	srv := grpc.NewServer()
	locationpb.RegisterLocationServiceServer(srv, h)
	go srv.Serve(lis)
	defer srv.Stop()

	// Client gRPC
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

	// 1. Kirim frame valid
	err = stream.Send(&locationpb.LocationRequest{
		DriverId:  "driver-01",
		Latitude:  -6.2088,
		Longitude: 106.8456,
		Speed:     45.5,
		Timestamp: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("failed to send frame 1: %v", err)
	}

	// 2. Kirim frame tidak valid (latitude di luar jangkauan > 90) -> harus di-skip
	err = stream.Send(&locationpb.LocationRequest{
		DriverId:  "driver-01",
		Latitude:  95.0, // Invalid!
		Longitude: 106.8456,
		Speed:     20.0,
	})
	if err != nil {
		t.Fatalf("failed to send invalid frame: %v", err)
	}

	// Tutup stream dan dapatkan respons summary
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("failed to close and recv: %v", err)
	}

	if !resp.GetSuccess() {
		t.Errorf("expected success true, got false")
	}

	// Verifikasi event yang terkirim ke mock publisher
	if len(mockPub.events) != 1 {
		t.Fatalf("expected 1 event published, got %d", len(mockPub.events))
	}

	ev := mockPub.events[0]
	if ev.Topic != "location.events" {
		t.Errorf("expected topic location.events, got %s", ev.Topic)
	}
	if ev.Key != "driver-01" {
		t.Errorf("expected key driver-01, got %s", ev.Key)
	}

	var parsed LocationEvent
	if err := json.Unmarshal(ev.Value, &parsed); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}
	if parsed.DriverID != "driver-01" || parsed.Speed != 45.5 {
		t.Errorf("unexpected parsed payload: %+v", parsed)
	}

	t.Logf("Response Message dari Ingestion Server: %s", resp.GetMessage())
}
