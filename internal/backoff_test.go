package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNextBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		attempt    uint32
		initial    time.Duration
		max        time.Duration
		multiplier float64
		want       time.Duration
	}{
		{
			name:       "first attempt",
			attempt:    0,
			initial:    time.Second,
			max:        time.Minute,
			multiplier: 2.0,
			want:       time.Second,
		},
		{
			name:       "second attempt",
			attempt:    1,
			initial:    time.Second,
			max:        time.Minute,
			multiplier: 2.0,
			want:       2 * time.Second,
		},
		{
			name:       "third attempt",
			attempt:    2,
			initial:    time.Second,
			max:        time.Minute,
			multiplier: 2.0,
			want:       4 * time.Second,
		},
		{
			name:       "max delay cap",
			attempt:    10,
			initial:    time.Second,
			max:        5 * time.Second,
			multiplier: 2.0,
			want:       5 * time.Second,
		},
		{
			name:       "default multiplier when less or equal one",
			attempt:    1,
			initial:    time.Second,
			max:        time.Minute,
			multiplier: 1.0,
			want:       2 * time.Second,
		},
		{
			name:       "default initial when zero",
			attempt:    0,
			initial:    0,
			max:        time.Minute,
			multiplier: 2.0,
			want:       time.Second,
		},
		{
			name:       "default max when zero",
			attempt:    0,
			initial:    time.Second,
			max:        0,
			multiplier: 2.0,
			want:       time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// act
			got := NextBackoff(tt.attempt, tt.initial, tt.max, tt.multiplier)

			// assert
			assert.Equal(t, tt.want, got)
		})
	}
}
