package kafka

import (
	"errors"
	"testing"

	"github.com/IBM/sarama"
)

func TestIsRetriableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "out of brokers error",
			err:      sarama.ErrOutOfBrokers,
			expected: true,
		},
		{
			name:     "closed client error",
			err:      sarama.ErrClosedClient,
			expected: true,
		},
		{
			name:     "leader not available",
			err:      sarama.ErrLeaderNotAvailable,
			expected: true,
		},
		{
			name:     "arbitrary business error",
			err:      errors.New("something bad happened"),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isRetriableError(tc.err)
			if got != tc.expected {
				t.Errorf("isRetriableError(%v) = %v; want %v", tc.err, got, tc.expected)
			}
		})
	}
}
