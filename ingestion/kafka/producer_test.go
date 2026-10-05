package kafka

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
)

func TestKafkaProducer_ClosedBehavior(t *testing.T) {
	kp := &KafkaProducer{
		buffer: make(chan *sarama.ProducerMessage, 1),
		closed: make(chan struct{}),
	}
	close(kp.closed)

	err := kp.Publish(context.Background(), "topic", "key", []byte("val"))
	if err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}
