// Package kafka provides the Sarama consumer-group handler for the tracking
// service. It consumes location events from "location.events" and delegates
// processing to LocationService.
package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/Tento03/fleet-tracking/tracking/models"
	"github.com/Tento03/fleet-tracking/tracking/services"
)

// ── LocationProcessor ─────────────────────────────────────────────────────

// LocationProcessor is the interface the consumer depends on. Keeping it as
// an interface enables unit-testing the consumer without a real Kafka broker.
type LocationProcessor interface {
	ProcessLocation(ctx context.Context, event *models.LocationEvent) error
}

// ── Consumer ──────────────────────────────────────────────────────────────

// Consumer wraps a sarama ConsumerGroup and implements
// sarama.ConsumerGroupHandler. It loops Consume inside a goroutine,
// auto-rejoins after every rebalance, and respects ctx cancellation.
type Consumer struct {
	group     sarama.ConsumerGroup
	topics    []string
	service   LocationProcessor
	connected atomic.Bool // true between Setup and Cleanup
	log       *slog.Logger
}

// NewConsumer creates a Consumer that subscribes to topic using groupID.
// It connects to brokers with OffsetNewest (skip historic messages on first
// start) and retries until the broker is reachable (up to 10 × 2 s).
func NewConsumer(
	brokers []string,
	groupID string,
	topic string,
	locationService *services.LocationService,
	logger *slog.Logger,
) (*Consumer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	// Start from newest offset so we do not replay historical events on
	// service restart.
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Return.Errors = true

	const maxRetries = 10
	const retryBackoff = 2 * time.Second

	var (
		group   sarama.ConsumerGroup
		lastErr error
	)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		var err error
		group, err = sarama.NewConsumerGroup(brokers, groupID, cfg)
		if err == nil {
			logger.Info("kafka consumer connected",
				"brokers", brokers,
				"group", groupID,
				"topic", topic,
				"attempt", attempt,
			)
			break
		}
		lastErr = err
		logger.Warn("kafka consumer: connect attempt failed – retrying",
			"attempt", attempt,
			"max", maxRetries,
			"error", err,
			"backoff", retryBackoff,
		)
		time.Sleep(retryBackoff)
	}
	if group == nil {
		return nil, lastErr
	}

	c := &Consumer{
		group:   group,
		topics:  []string{topic},
		service: locationService,
		log:     logger,
	}
	return c, nil
}

// ── Start ─────────────────────────────────────────────────────────────────

// Start launches the consume loop in the background. The loop automatically
// rejoins the group after each rebalance (sarama requires re-calling Consume
// after every session ends). Stops when ctx is cancelled.
//
// Errors from the consumer-group's internal error channel are logged; they
// do not terminate the loop.
func (c *Consumer) Start(ctx context.Context) {
	// Drain the error channel in a separate goroutine to avoid blocking the
	// consume loop when errors arrive during rebalance.
	go func() {
		for err := range c.group.Errors() {
			c.log.Error("kafka consumer group error", "error", err)
		}
	}()

	go func() {
		for {
			// Consume blocks until the session ends (rebalance or ctx cancel).
			if err := c.group.Consume(ctx, c.topics, c); err != nil {
				if ctx.Err() != nil {
					// Normal shutdown — context cancelled.
					return
				}
				c.log.Error("kafka consume error – will rejoin", "error", err)
			}
			// Context cancelled → exit the loop.
			if ctx.Err() != nil {
				return
			}
			// Otherwise: rebalance happened; loop back and rejoin.
		}
	}()
}

// ── Close ─────────────────────────────────────────────────────────────────

// Close flushes any in-flight work and shuts down the consumer group.
// Should be called once during graceful shutdown.
func (c *Consumer) Close() error {
	return c.group.Close()
}

// ── IsConnected ───────────────────────────────────────────────────────────

// IsConnected returns true while a consumer-group session is active (between
// Setup and Cleanup). Used by the health-check endpoint added in Prompt 6.
func (c *Consumer) IsConnected() bool {
	return c.connected.Load()
}

// ── sarama.ConsumerGroupHandler ───────────────────────────────────────────

// Setup is called by sarama at the beginning of every consumer-group session,
// before ConsumeClaim. We set the connected flag here.
func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	c.connected.Store(true)
	c.log.Info("kafka consumer session started")
	return nil
}

// Cleanup is called by sarama at the end of every session, after all
// ConsumeClaim goroutines have returned. We clear the connected flag here.
func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	c.connected.Store(false)
	c.log.Info("kafka consumer session ended")
	return nil
}

// ConsumeClaim is called once per topic-partition claim in a session.
// It reads messages from claim.Messages(), processes each one through the
// LocationService, and always marks the message regardless of processing
// outcome to avoid poison-pill scenarios.
func (c *Consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			c.log.Info("Processing location event",
				"driver_id", extractDriverID(msg.Value),
				"partition", msg.Partition,
				"offset", msg.Offset,
			)

			// Unmarshal the Kafka message payload.
			var event models.LocationEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				c.log.Error("kafka: malformed JSON – skipping",
					"partition", msg.Partition,
					"offset", msg.Offset,
					"error", err,
					"raw", string(msg.Value),
				)
				// Mark the message to advance the offset even though we
				// cannot process it (poison-pill guard).
				session.MarkMessage(msg, "")
				continue
			}

			// Delegate to the service. On failure, log and mark anyway so
			// a single bad message cannot stall the consumer.
			if err := c.service.ProcessLocation(session.Context(), &event); err != nil {
				c.log.Error("kafka: ProcessLocation failed",
					"driver_id", event.DriverID,
					"partition", msg.Partition,
					"offset", msg.Offset,
					"error", err,
				)
			}

			// Always mark the message to commit the offset.
			session.MarkMessage(msg, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────

// extractDriverID is a best-effort helper that peeks at the driver_id field
// in a raw JSON payload for logging purposes, without fully unmarshalling it.
func extractDriverID(raw []byte) string {
	var partial struct {
		DriverID string `json:"driver_id"`
	}
	if err := json.Unmarshal(raw, &partial); err != nil {
		return "<unknown>"
	}
	return partial.DriverID
}

// Compile-time assertion: Consumer implements sarama.ConsumerGroupHandler.
var _ sarama.ConsumerGroupHandler = (*Consumer)(nil)
