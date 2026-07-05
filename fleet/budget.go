// Package fleet provides multi-device convergence management for billet. It
// orchestrates a fleet of devices, fans out desired-state changes, enforces
// per-device downlink budgets, and surfaces observability events.
package fleet

import "time"

// BudgetConfig configures the token-bucket rate limiter for a device's
// downlink slot. MaxTokens sets the burst capacity, and RefillInterval sets
// how long it takes to regenerate one token.
type BudgetConfig struct {
	// MaxTokens is the maximum number of tokens the bucket can hold (burst
	// capacity). A device with MaxTokens=3 can flush three times in rapid
	// succession before being throttled.
	MaxTokens int

	// RefillInterval is the duration per token refill. For example, a 1%
	// LoRaWAN duty cycle with 1-second airtime implies RefillInterval of
	// 100 seconds.
	RefillInterval time.Duration
}

// budget is a per-device token-bucket rate limiter. It is not safe for
// concurrent use — the Manager serializes access.
type budget struct {
	tokens     int
	max        int
	refillNs   int64 // RefillInterval in nanoseconds
	lastRefill int64 // physical nanoseconds of last refill
}

// newBudget creates a budget with a full token bucket.
func newBudget(cfg BudgetConfig, nowNs int64) budget {
	return budget{
		tokens:     cfg.MaxTokens,
		max:        cfg.MaxTokens,
		refillNs:   cfg.RefillInterval.Nanoseconds(),
		lastRefill: nowNs,
	}
}

// refill adds tokens based on elapsed time since the last refill. Tokens are
// capped at the bucket maximum.
func (b *budget) refill(nowNs int64) {
	if b.refillNs <= 0 {
		return
	}
	elapsed := nowNs - b.lastRefill
	if elapsed < b.refillNs {
		return
	}
	add := int(elapsed / b.refillNs)
	b.tokens += add
	if b.tokens > b.max {
		b.tokens = b.max
	}
	// Advance lastRefill by the number of whole intervals consumed, not to
	// nowNs, so fractional time carries over.
	b.lastRefill += int64(add) * b.refillNs
}

// consume attempts to take one token from the bucket. Returns true if a token
// was available and consumed.
func (b *budget) consume() bool {
	if b.tokens <= 0 {
		return false
	}
	b.tokens--
	return true
}
