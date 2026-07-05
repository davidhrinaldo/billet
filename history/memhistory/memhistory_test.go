package memhistory

import (
	"testing"
	"time"

	"github.com/davidhrinaldo/billet/history"
)

var t0 = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func TestRecord(t *testing.T) {
	tests := []struct {
		name    string
		samples []history.Sample
		wantErr error
	}{
		{
			name:    "single sample",
			samples: []history.Sample{{Time: t0, Value: 22.5}},
			wantErr: nil,
		},
		{
			name: "multiple samples",
			samples: []history.Sample{
				{Time: t0, Value: 22.5},
				{Time: t0.Add(time.Minute), Value: 23.0},
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := New()
			var lastErr error
			for _, s := range tt.samples {
				lastErr = h.Record("dev-1", "temp", s)
			}
			if lastErr != tt.wantErr {
				t.Errorf("Record error = %v, want %v", lastErr, tt.wantErr)
			}
		})
	}
}

func TestQuery(t *testing.T) {
	tests := []struct {
		name     string
		device   string
		key      string
		seed     []history.Sample
		start    time.Time
		end      time.Time
		wantLen  int
		wantErr  error
	}{
		{
			name:    "no data returns ErrNoData",
			device:  "dev-1",
			key:     "temp",
			seed:    nil,
			start:   t0,
			end:     t0.Add(time.Hour),
			wantLen: 0,
			wantErr: history.ErrNoData,
		},
		{
			name:   "query outside range returns ErrNoData",
			device: "dev-1",
			key:    "temp",
			seed: []history.Sample{
				{Time: t0, Value: 22.5},
			},
			start:   t0.Add(time.Hour),
			end:     t0.Add(2 * time.Hour),
			wantLen: 0,
			wantErr: history.ErrNoData,
		},
		{
			name:   "query returns samples in range",
			device: "dev-1",
			key:    "temp",
			seed: []history.Sample{
				{Time: t0, Value: 1.0},
				{Time: t0.Add(time.Minute), Value: 2.0},
				{Time: t0.Add(2 * time.Minute), Value: 3.0},
				{Time: t0.Add(3 * time.Minute), Value: 4.0},
			},
			start:   t0.Add(time.Minute),
			end:     t0.Add(3 * time.Minute),
			wantLen: 2,
			wantErr: nil,
		},
		{
			name:   "different device isolation",
			device: "dev-1",
			key:    "temp",
			seed: []history.Sample{
				{Time: t0, Value: 99.0},
			},
			start:   t0,
			end:     t0.Add(time.Hour),
			wantLen: 0,
			wantErr: history.ErrNoData,
		},
		{
			name:   "different key isolation",
			device: "dev-1",
			key:    "humidity",
			seed: []history.Sample{
				{Time: t0, Value: 50.0},
			},
			start:   t0,
			end:     t0.Add(time.Hour),
			wantLen: 0,
			wantErr: history.ErrNoData,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := New()

			// Seed data — use the device/key from the test case name to determine
			// isolation tests. For isolation tests, seed under a different identity.
			seedDevice := tt.device
			seedKey := tt.key
			if tt.name == "different device isolation" {
				seedDevice = "dev-2"
			}
			if tt.name == "different key isolation" {
				seedKey = "temp"
			}

			for _, s := range tt.seed {
				if err := h.Record(seedDevice, seedKey, s); err != nil {
					t.Fatalf("Record: %v", err)
				}
			}

			results, err := h.Query(tt.device, tt.key, tt.start, tt.end)
			if err != tt.wantErr {
				t.Fatalf("Query error = %v, want %v", err, tt.wantErr)
			}
			if len(results) != tt.wantLen {
				t.Errorf("got %d results, want %d", len(results), tt.wantLen)
			}

			// Verify time ordering.
			for i := 1; i < len(results); i++ {
				if results[i].Time.Before(results[i-1].Time) {
					t.Errorf("results not in order: [%d]=%v before [%d]=%v",
						i, results[i].Time, i-1, results[i-1].Time)
				}
			}
		})
	}
}
