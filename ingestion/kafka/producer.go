// Package kafka provides a non-blocking Sarama AsyncProducer wrapper for the ingestion service.
// It implements handler.EventPublisher so it can be injected directly into LocationHandler.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

var (
	// ErrBackpressure is returned when the internal producer channel is full.
	ErrBackpressure = errors.New("backpressure: producer buffer full")
	// ErrClosed is returned when attempting to publish to a closed producer.
	ErrClosed = errors.New("kafka producer is closed")
)

// KafkaProducer wraps a sarama.AsyncProducer with non-blocking publish
// and background goroutines to drain successes and errors.
type KafkaProducer struct {
	producer sarama.AsyncProducer
	buffer   chan *sarama.ProducerMessage
	log      *slog.Logger
	wg       sync.WaitGroup
	closed   chan struct{}
	once     sync.Once
}

// buildSaramaConfig returns an idempotent, strictly ordered Sarama configuration.
func buildSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()

	// acks=all
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	// Idempotent producer guarantees exactly-once delivery within a partition
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1

	// Must enable Return.Successes and Return.Errors for AsyncProducer
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	// Retries with exponential-like backoff
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 250 * time.Millisecond

	// Hash partitioner routes identical keys (driver_code) to the exact same partition
	cfg.Producer.Partitioner = sarama.NewHashPartitioner

	// Snappy compression for optimal throughput and low CPU overhead
	cfg.Producer.Compression = sarama.CompressionSnappy

	// Kafka 2.6+ required for idempotent producer
	cfg.Version = sarama.V2_6_0_0

	return cfg
}

// NewKafkaProducer connects to Kafka using Sarama's AsyncProducer.
// bufferSize controls the capacity of the non-blocking ingress channel.
func NewKafkaProducer(brokers []string, bufferSize int, logger *slog.Logger) (*KafkaProducer, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if bufferSize <= 0 {
		bufferSize = 1000
	}

	cfg := buildSaramaConfig()
	producer, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create async producer: %w", err)
	}

	kp := &KafkaProducer{
		producer: producer,
		buffer:   make(chan *sarama.ProducerMessage, bufferSize),
		log:      logger,
		closed:   make(chan struct{}),
	}

	// 1. Goroutine feeding buffer into producer.Input()
	kp.wg.Add(1)
	go func() {
		defer kp.wg.Done()
		for {
			select {
			case <-kp.closed:
				// Drain remaining messages before exiting
				for {
					select {
					case msg := <-kp.buffer:
						producer.Input() <- msg
					default:
						return
					}
				}
			case msg, ok := <-kp.buffer:
				if !ok {
					return
				}
				producer.Input() <- msg
			}
		}
	}()

	// 2. Goroutine draining successes
	kp.wg.Add(1)
	go func() {
		defer kp.wg.Done()
		for succ := range producer.Successes() {
			logger.Debug("kafka message acknowledged",
				"topic", succ.Topic,
				"partition", succ.Partition,
				"offset", succ.Offset,
				"key", string(succ.Key.(sarama.StringEncoder)),
			)
		}
	}()

	// 3. Goroutine draining errors
	kp.wg.Add(1)
	go func() {
		defer kp.wg.Done()
		for prodErr := range producer.Errors() {
			logger.Error("kafka message delivery failed",
				"topic", prodErr.Msg.Topic,
				"key", string(prodErr.Msg.Key.(sarama.StringEncoder)),
				"error", prodErr.Err,
			)
		}
	}()

	logger.Info("kafka async producer initialized",
		"brokers", brokers,
		"buffer_size", bufferSize,
	)

	return kp, nil
}

// Publish enqueues a message without blocking. If the internal buffer is full,
// it immediately returns ErrBackpressure.
func (kp *KafkaProducer) Publish(ctx context.Context, topic, key string, value []byte) error {
	select {
	case <-kp.closed:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	select {
	case kp.buffer <- msg:
		return nil
	default:
		kp.log.Warn("kafka producer backpressure: buffer full",
			"topic", topic,
			"key", key,
		)
		return ErrBackpressure
	}
}

// Close gracefully flushes pending messages and stops the producer.
func (kp *KafkaProducer) Close() error {
	var closeErr error
	kp.once.Do(func() {
		close(kp.closed)
		kp.wg.Wait()
		closeErr = kp.producer.Close()
	})
	return closeErr
}
