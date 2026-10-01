// Package main is the entry point for the ingestion gRPC service.
// It loads configuration, sets up structured logging, initialises the Kafka
// producer, registers the LocationService handler, and manages graceful
// shutdown (SIGINT / SIGTERM → GracefulStop → producer.Close).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Tento03/fleet-tracking/ingestion/config"
	"github.com/Tento03/fleet-tracking/ingestion/handler"
	"github.com/Tento03/fleet-tracking/ingestion/kafka"
	locationpb "github.com/Tento03/fleet-tracking/proto"
	"google.golang.org/grpc"
)

func main() {
	// ── Configuration ─────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// ── Logging ───────────────────────────────────────────────────────────
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	// ── Kafka Producer ────────────────────────────────────────────────────
	// NewKafkaProducer retries internally (up to 10×, 2 s apart), so this
	// call can block for up to ~20 s when Kafka is slow to start.
	logger.Info("connecting to Kafka", "brokers", cfg.KafkaBrokers)
	producer, err := kafka.NewKafkaProducer(cfg.KafkaBrokers, logger)
	if err != nil {
		logger.Error("failed to initialise Kafka producer", "error", err)
		os.Exit(1)
	}
	logger.Info("Kafka producer ready", "topic", cfg.KafkaTopic)

	// ── gRPC server ───────────────────────────────────────────────────────
	addr := ":" + cfg.GRPCPort
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("failed to listen", "addr", addr, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()

	// Inject the Kafka producer as the EventPublisher.
	locationHandler := handler.NewLocationHandler(producer, cfg.KafkaTopic, logger)
	locationpb.RegisterLocationServiceServer(grpcServer, locationHandler)

	// ── Signal handling ───────────────────────────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Serve in a background goroutine so we can wait for the signal.
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("Ingestion service started", "addr", addr)
		if err := grpcServer.Serve(lis); err != nil {
			serveErr <- err
		}
	}()

	// ── Block until shutdown signal or fatal server error ─────────────────
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")

		// 1. Stop accepting new gRPC connections and wait for active streams.
		logger.Info("stopping gRPC server gracefully")
		grpcServer.GracefulStop()

		// 2. Flush and close the Kafka producer.
		logger.Info("closing Kafka producer")
		if err := producer.Close(); err != nil {
			logger.Error("error closing Kafka producer", "error", err)
		}

	case err := <-serveErr:
		logger.Error("gRPC server fatal error", "error", err)
		_ = producer.Close()
		os.Exit(1)
	}

	logger.Info("ingestion service stopped")
}
