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

// LocationProcessor is the interface the consumer depends on.
type LocationProcessor interface {
	ProcessLocation(ctx context.Context, event *models.LocationEvent) error
}

// ── Consumer ──────────────────────────────────────────────────────────────

// Consumer wraps a sarama ConsumerGroup and implements sarama.ConsumerGroupHandler.
type Consumer struct {
	group     sarama.ConsumerGroup
	topics    []string
	service   LocationProcessor
	connected atomic.Bool
	log       *slog.Logger
}

// NewConsumer creates a Consumer that subscribes to topic using groupID.
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

// Start launches the consume loop in the background.
func (c *Consumer) Start(ctx context.Context) {
	go func() {
		for err := range c.group.Errors() {
			c.log.Error("kafka consumer group error", "error", err)
		}
	}()

	go func() {
		for {
			if err := c.group.Consume(ctx, c.topics, c); err != nil {
				if ctx.Err() != nil {
					return
				}
				c.log.Error("kafka consume error – will rejoin", "error", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
}

// Close flushes any in-flight work and shuts down the consumer group.
func (c *Consumer) Close() error {
	return c.group.Close()
}

// IsConnected returns true while a consumer-group session is active.
func (c *Consumer) IsConnected() bool {
	return c.connected.Load()
}

// ── sarama.ConsumerGroupHandler ───────────────────────────────────────────

func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	c.connected.Store(true)
	c.log.Info("kafka consumer session started")
	return nil
}

func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	c.connected.Store(false)
	c.log.Info("kafka consumer session ended")
	return nil
}

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

			driverKey := extractDriverIdentifier(msg.Value)

			c.log.Info("processing location event from kafka",
				"driver", driverKey,
				"partition", msg.Partition,
				"offset", msg.Offset,
			)

			var event models.LocationEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				c.log.Error("kafka: malformed JSON – skipping",
					"partition", msg.Partition,
					"offset", msg.Offset,
					"error", err,
					"raw", string(msg.Value),
				)
				session.MarkMessage(msg, "")
				continue
			}

			// If event.DriverCode is missing, fallback to string(msg.Key)
			if event.DriverCode == "" && len(msg.Key) > 0 {
				event.DriverCode = string(msg.Key)
			}

			if err := c.service.ProcessLocation(session.Context(), &event); err != nil {
				c.log.Error("kafka: ProcessLocation failed",
					"driver_code", event.DriverCode,
					"partition", msg.Partition,
					"offset", msg.Offset,
					"error", err,
				)
			}

			session.MarkMessage(msg, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

func extractDriverIdentifier(raw []byte) string {
	var partial struct {
		DriverCode string `json:"driver_code"`
		DriverID   string `json:"driver_id"`
	}
	if err := json.Unmarshal(raw, &partial); err != nil {
		return "<unknown>"
	}
	if partial.DriverCode != "" {
		return partial.DriverCode
	}
	if partial.DriverID != "" {
		return partial.DriverID
	}
	return "<unknown>"
}

var _ sarama.ConsumerGroupHandler = (*Consumer)(nil)
