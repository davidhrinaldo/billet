package fleet

import (
	"testing"
	"time"
)

func TestBudget(t *testing.T) {
	tests := []struct {
		name       string
		cfg        BudgetConfig
		startNs    int64
		ops        func(b *budget) // sequence of operations
		wantTokens int
	}{
		{
			name:    "starts full",
			cfg:     BudgetConfig{MaxTokens: 3, RefillInterval: time.Second},
			startNs: 0,
			ops:     func(b *budget) {},
			wantTokens: 3,
		},
		{
			name:    "consume drains tokens",
			cfg:     BudgetConfig{MaxTokens: 3, RefillInterval: time.Second},
			startNs: 0,
			ops: func(b *budget) {
				b.consume()
				b.consume()
			},
			wantTokens: 1,
		},
		{
			name:    "consume returns false when empty",
			cfg:     BudgetConfig{MaxTokens: 1, RefillInterval: time.Second},
			startNs: 0,
			ops: func(b *budget) {
				if !b.consume() {
					t.Error("first consume should succeed")
				}
				if b.consume() {
					t.Error("second consume should fail")
				}
			},
			wantTokens: 0,
		},
		{
			name:    "refill adds tokens after interval",
			cfg:     BudgetConfig{MaxTokens: 3, RefillInterval: time.Second},
			startNs: 0,
			ops: func(b *budget) {
				b.consume()
				b.consume()
				b.consume()
				b.refill(int64(2 * time.Second))
			},
			wantTokens: 2,
		},
		{
			name:    "refill caps at max",
			cfg:     BudgetConfig{MaxTokens: 3, RefillInterval: time.Second},
			startNs: 0,
			ops: func(b *budget) {
				b.consume()
				b.refill(int64(10 * time.Second))
			},
			wantTokens: 3,
		},
		{
			name:    "refill carries fractional time",
			cfg:     BudgetConfig{MaxTokens: 5, RefillInterval: time.Second},
			startNs: 0,
			ops: func(b *budget) {
				b.consume()
				b.consume()
				b.consume()
				// 1.5 seconds elapsed — should add 1 token, carry 0.5s
				b.refill(int64(1500 * time.Millisecond))
			},
			wantTokens: 3,
		},
		{
			name:    "no refill before interval elapses",
			cfg:     BudgetConfig{MaxTokens: 3, RefillInterval: time.Second},
			startNs: 0,
			ops: func(b *budget) {
				b.consume()
				b.refill(int64(500 * time.Millisecond))
			},
			wantTokens: 2,
		},
		{
			name:    "zero refill interval never refills",
			cfg:     BudgetConfig{MaxTokens: 3, RefillInterval: 0},
			startNs: 0,
			ops: func(b *budget) {
				b.consume()
				b.refill(int64(10 * time.Second))
			},
			wantTokens: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBudget(tt.cfg, tt.startNs)
			tt.ops(&b)
			if b.tokens != tt.wantTokens {
				t.Errorf("tokens = %d, want %d", b.tokens, tt.wantTokens)
			}
		})
	}
}
