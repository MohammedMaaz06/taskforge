package retry

import (
"testing"
"time"
)

func TestExponentialBackoffProgression(t *testing.T) {
policy := DefaultPolicy()

d1 := policy.NextBackoff(1)
d2 := policy.NextBackoff(2)
d3 := policy.NextBackoff(3)

t.Logf("Attempt 1 Backoff: %v", d1)
t.Logf("Attempt 2 Backoff: %v", d2)
t.Logf("Attempt 3 Backoff: %v", d3)

if d1 >= d2 {
t.Errorf("Expected backoff to increase: attempt 1 (%v) vs attempt 2 (%v)", d1, d2)
}
if d2 >= d3 {
t.Errorf("Expected backoff to increase: attempt 2 (%v) vs attempt 3 (%v)", d2, d3)
}
}

func TestMaxIntervalCap(t *testing.T) {
policy := Policy{
InitialInterval: 1 * time.Second,
MaxInterval:     5 * time.Second,
Multiplier:      2.0,
MaxRetries:      10,
}

// Attempt 10 should be capped near MaxInterval
d10 := policy.NextBackoff(10)
if d10 > 7*time.Second { // allowing headroom for jitter
t.Errorf("Backoff exceeded max interval cap: got %v", d10)
}
}
