package retry

import (
"math"
"math/rand"
"time"
)

// Policy defines exponential backoff configuration
type Policy struct {
InitialInterval time.Duration
MaxInterval     time.Duration
Multiplier      float64
MaxRetries      int
}

// DefaultPolicy returns standard enterprise retry settings
func DefaultPolicy() Policy {
return Policy{
InitialInterval: 500 * time.Millisecond,
MaxInterval:     30 * time.Second,
Multiplier:      2.0,
MaxRetries:      5,
}
}

// NextBackoff calculates the delay for a given attempt with randomized jitter
func (p Policy) NextBackoff(attempt int) time.Duration {
if attempt < 1 {
attempt = 1
}

// Calculate exponential interval: initial * multiplier^(attempt-1)
factor := math.Pow(p.Multiplier, float64(attempt-1))
interval := float64(p.InitialInterval) * factor

if interval > float64(p.MaxInterval) {
interval = float64(p.MaxInterval)
}

// Add ±20% randomized jitter to prevent thundering herd problem
jitter := (rand.Float64()*0.4 - 0.2) * interval
backoff := time.Duration(interval + jitter)

if backoff < 0 {
return p.InitialInterval
}
return backoff
}
