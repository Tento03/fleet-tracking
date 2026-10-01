// Package kafka provides a Kafka SyncProducer wrapper for the ingestion service.
// It implements handler.EventPublisher so it can be injected directly into
// LocationHandler without the handler knowing about Kafka internals.
package kafka

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// ── Sentinel / category errors ────────────────────────────────────────────

// isRetriableError returns true when the Kafka error is likely caused by a
// transient broker connectivity problem (not a bad message).
func isRetriableError(err error) bool {
	if err == nil {
		return false
	}
	// Check package-level sentinel errors.
	if errors.Is(err, sarama.ErrOutOfBrokers) ||
		errors.Is(err, sarama.ErrClosedClient) {
		return true
	}
	// Check sarama typed protocol errors.
	var kErr sarama.KError
	if errors.As(err, &kErr) {
		switch kErr {
		case sarama.ErrBrokerNotAvailable,
			sarama.ErrLeaderNotAvailable,
			sarama.ErrNotLeaderForPartition,
			sarama.ErrNetworkException:
			return true
		}
	}
	return false
}

// ── KafkaProducer ─────────────────────────────────────────────────────────

// KafkaProducer wraps a sarama.SyncProducer with automatic reconnection
// on transient network / broker failures. All public methods are safe for
// concurrent use.
type KafkaProducer struct {
	mu       sync.RWMutex
	producer sarama.SyncProducer
	client   sarama.Client // kept alive to check connectivity
	brokers  []string
	cfg      *sarama.Config
	log      *slog.Logger
}

// buildSaramaConfig returns a sarama.Config tuned for reliable, ordered
// delivery keyed by driver_id.
func buildSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()

	// Producer must confirm all in-sync replicas have written the message.
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	// Return success / error via channels so SyncProducer can work.
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	// Up to 5 automatic retries on transient send failures.
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 250 * time.Millisecond

	// HashPartitioner ensures the same driver_id always lands on the
	// same partition, which keeps per-driver message ordering intact.
	cfg.Producer.Partitioner = sarama.NewHashPartitioner

	// Compression reduces bandwidth between ingestion and Kafka.
	cfg.Producer.Compression = sarama.CompressionSnappy

	// Version — Kafka 2.x+ is required for sticky partitioning.
	cfg.Version = sarama.V2_6_0_0

	return cfg
}

// NewKafkaProducer connects to Kafka, retrying up to maxRetries times with
// a fixed backoff between attempts. This makes the ingestion service
// resilient to Kafka being temporarily unavailable at startup.
func NewKafkaProducer(brokers []string, logger *slog.Logger) (*KafkaProducer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	const maxRetries = 10
	const retryBackoff = 2 * time.Second

	cfg := buildSaramaConfig()

	var (
		client   sarama.Client
		producer sarama.SyncProducer
		lastErr  error
	)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		var err error

		client, err = sarama.NewClient(brokers, cfg)
		if err != nil {
			lastErr = err
			logger.Warn("kafka: connect attempt failed – retrying",
				"attempt", attempt,
				"max", maxRetries,
				"brokers", brokers,
				"error", err,
				"backoff", retryBackoff,
			)
			time.Sleep(retryBackoff)
			continue
		}

		producer, err = sarama.NewSyncProducerFromClient(client)
		if err != nil {
			client.Close() //nolint:errcheck
			lastErr = err
			logger.Warn("kafka: producer init failed – retrying",
				"attempt", attempt,
				"error", err,
			)
			time.Sleep(retryBackoff)
			continue
		}

		logger.Info("kafka: producer connected",
			"brokers", brokers,
			"attempt", attempt,
		)
		return &KafkaProducer{
			producer: producer,
			client:   client,
			brokers:  brokers,
			cfg:      cfg,
			log:      logger,
		}, nil
	}

	return nil, fmt.Errorf("kafka: failed to connect after %d attempts: %w", maxRetries, lastErr)
}

// ── reconnect (internal, must be called with mu write-locked) ────────────

func (kp *KafkaProducer) reconnect() error {
	kp.log.Warn("kafka: attempting producer reconnect")

	// Best-effort close of old resources.
	if kp.producer != nil {
		_ = kp.producer.Close()
	}
	if kp.client != nil {
		_ = kp.client.Close()
	}

	client, err := sarama.NewClient(kp.brokers, kp.cfg)
	if err != nil {
		return fmt.Errorf("kafka reconnect – client: %w", err)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		return fmt.Errorf("kafka reconnect – producer: %w", err)
	}

	kp.client = client
	kp.producer = producer
	kp.log.Info("kafka: producer reconnected successfully")
	return nil
}

// ── Publish ───────────────────────────────────────────────────────────────

// Publish sends a single message to the given topic, using key as the
// partition routing key (driver_id in our case).
//
// On transient connectivity errors the producer is recreated and the send
// is retried exactly once. If the retry also fails, the error is returned
// and the caller decides whether to continue or abort.
func (kp *KafkaProducer) Publish(topic, key string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	// ── First attempt (read-locked) ───────────────────────────────────────
	kp.mu.RLock()
	_, _, err := kp.producer.SendMessage(msg)
	kp.mu.RUnlock()

	if err == nil {
		return nil
	}

	// ── Retriable error → reconnect and retry once ────────────────────────
	if !isRetriableError(err) {
		return fmt.Errorf("kafka publish: %w", err)
	}

	kp.log.Warn("kafka: retriable send error – reconnecting",
		"topic", topic,
		"key", key,
		"error", err,
	)

	kp.mu.Lock()
	reconnErr := kp.reconnect()
	kp.mu.Unlock()

	if reconnErr != nil {
		return fmt.Errorf("kafka publish: reconnect failed: %w", reconnErr)
	}

	// Retry once after successful reconnect.
	kp.mu.RLock()
	_, _, retryErr := kp.producer.SendMessage(msg)
	kp.mu.RUnlock()

	if retryErr != nil {
		return fmt.Errorf("kafka publish retry: %w", retryErr)
	}
	return nil
}

// ── IsConnected ───────────────────────────────────────────────────────────

// IsConnected reports whether the underlying sarama client and at least one
// broker are reachable. Useful for health-check endpoints.
func (kp *KafkaProducer) IsConnected() bool {
	kp.mu.RLock()
	defer kp.mu.RUnlock()

	if kp.client == nil || kp.client.Closed() {
		return false
	}
	brokers := kp.client.Brokers()
	for _, b := range brokers {
		if connected, _ := b.Connected(); connected {
			return true
		}
	}
	return false
}

// ── Close ─────────────────────────────────────────────────────────────────

// Close flushes any pending messages and shuts down the producer and client.
// It should be called exactly once during service shutdown.
func (kp *KafkaProducer) Close() error {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	var errs []error
	if kp.producer != nil {
		if err := kp.producer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("producer close: %w", err))
		}
	}
	if kp.client != nil {
		if err := kp.client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("client close: %w", err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
