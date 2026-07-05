// Package historytest provides a conformance test suite for history.History
// implementations. Call Suite(t, factory) from your implementation's test file.
package historytest

import (
	"testing"
	"time"

	"github.com/davidhrinaldo/billet/history"
)

var t0 = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// Factory creates a fresh History for each test case and returns an optional
// cleanup function.
type Factory func(t *testing.T) (history.History, func())

// Suite runs the full conformance suite against a History implementation.
func Suite(t *testing.T, f Factory) {
	t.Helper()
	t.Run("Record", func(t *testing.T) { testRecord(t, f) })
	t.Run("Query", func(t *testing.T) { testQuery(t, f) })
}

func testRecord(t *testing.T, f Factory) {
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
			h, cleanup := f(t)
			defer cleanup()

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

func testQuery(t *testing.T, f Factory) {
	tests := []struct {
		name       string
		seedDevice string
		seedKey    string
		seed       []history.Sample
		queryDev   string
		queryKey   string
		start      time.Time
		end        time.Time
		wantLen    int
		wantErr    error
	}{
		{
			name:     "no data returns ErrNoData",
			queryDev: "dev-1",
			queryKey: "temp",
			start:    t0,
			end:      t0.Add(time.Hour),
			wantLen:  0,
			wantErr:  history.ErrNoData,
		},
		{
			name:       "query outside range returns ErrNoData",
			seedDevice: "dev-1",
			seedKey:    "temp",
			seed: []history.Sample{
				{Time: t0, Value: 22.5},
			},
			queryDev: "dev-1",
			queryKey: "temp",
			start:    t0.Add(time.Hour),
			end:      t0.Add(2 * time.Hour),
			wantLen:  0,
			wantErr:  history.ErrNoData,
		},
		{
			name:       "query returns samples in range",
			seedDevice: "dev-1",
			seedKey:    "temp",
			seed: []history.Sample{
				{Time: t0, Value: 1.0},
				{Time: t0.Add(time.Minute), Value: 2.0},
				{Time: t0.Add(2 * time.Minute), Value: 3.0},
				{Time: t0.Add(3 * time.Minute), Value: 4.0},
			},
			queryDev: "dev-1",
			queryKey: "temp",
			start:    t0.Add(time.Minute),
			end:      t0.Add(3 * time.Minute),
			wantLen:  2,
			wantErr:  nil,
		},
		{
			name:       "different device isolation",
			seedDevice: "dev-2",
			seedKey:    "temp",
			seed: []history.Sample{
				{Time: t0, Value: 99.0},
			},
			queryDev: "dev-1",
			queryKey: "temp",
			start:    t0,
			end:      t0.Add(time.Hour),
			wantLen:  0,
			wantErr:  history.ErrNoData,
		},
		{
			name:       "different key isolation",
			seedDevice: "dev-1",
			seedKey:    "temp",
			seed: []history.Sample{
				{Time: t0, Value: 50.0},
			},
			queryDev: "dev-1",
			queryKey: "humidity",
			start:    t0,
			end:      t0.Add(time.Hour),
			wantLen:  0,
			wantErr:  history.ErrNoData,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, cleanup := f(t)
			defer cleanup()

			for _, s := range tt.seed {
				if err := h.Record(tt.seedDevice, tt.seedKey, s); err != nil {
					t.Fatalf("Record: %v", err)
				}
			}

			results, err := h.Query(tt.queryDev, tt.queryKey, tt.start, tt.end)
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
