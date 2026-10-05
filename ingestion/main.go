// Package main is the entry point for the ingestion gRPC service.
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

	// ── Structured Logging (JSON) ─────────────────────────────────────────
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	// ── Kafka AsyncProducer ───────────────────────────────────────────────
	logger.Info("connecting to Kafka async producer", "brokers", cfg.KafkaBrokers)
	producer, err := kafka.NewKafkaProducer(cfg.KafkaBrokers, 2000, logger)
	if err != nil {
		logger.Error("failed to initialise Kafka producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	// ── gRPC server setup ─────────────────────────────────────────────────
	addr := ":" + cfg.GRPCPort
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("failed to listen", "addr", addr, "error", err)
		os.Exit(1)
	}

	// Register auth interceptors
	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(handler.StreamAuthInterceptor(cfg.IngestionAPIKey)),
		grpc.UnaryInterceptor(handler.UnaryAuthInterceptor(cfg.IngestionAPIKey)),
	)

	locationHandler := handler.NewLocationHandler(producer, cfg.KafkaTopic, logger)
	locationpb.RegisterLocationServiceServer(grpcServer, locationHandler)

	// ── Signal handling & graceful shutdown ───────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("Ingestion service started",
			"addr", addr,
			"auth_enabled", cfg.IngestionAPIKey != "",
		)
		if err := grpcServer.Serve(lis); err != nil {
			serveErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
		logger.Info("stopping gRPC server gracefully")
		grpcServer.GracefulStop()
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
