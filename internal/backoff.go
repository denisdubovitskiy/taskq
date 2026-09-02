package internal

import (
	"math"
	"time"
)

// DefaultRetryMultiplier — множитель по умолчанию для экспоненциального backoff.
const DefaultRetryMultiplier = 2.0

// NextBackoff вычисляет следующую задержку на основе попытки и политики.
func NextBackoff(attempt uint32, initial, maxDelay time.Duration, multiplier float64) time.Duration {
	if multiplier <= 1 {
		multiplier = DefaultRetryMultiplier
	}
	if initial <= 0 {
		initial = time.Second
	}
	if maxDelay <= 0 {
		maxDelay = 5 * time.Minute
	}

	factor := math.Pow(multiplier, float64(attempt))
	delay := time.Duration(float64(initial) * factor)
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}
