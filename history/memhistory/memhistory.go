// Package memhistory provides an in-memory implementation of the history.History
// interface. It is a stub for testing; the production implementation wraps ingot.
package memhistory

import (
	"sync"
	"time"

	"github.com/davidhrinaldo/billet/history"
)

// seriesKey identifies a unique time series.
type seriesKey struct {
	deviceID string
	key      string
}

// MemHistory is a concurrency-safe in-memory History implementation.
type MemHistory struct {
	mu     sync.RWMutex
	series map[seriesKey][]history.Sample
}

// New creates an empty MemHistory.
func New() *MemHistory {
	return &MemHistory{
		series: make(map[seriesKey][]history.Sample),
	}
}

// Record stores a sample for the given device and key.
func (h *MemHistory) Record(deviceID, key string, s history.Sample) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	k := seriesKey{deviceID: deviceID, key: key}
	h.series[k] = append(h.series[k], s)
	return nil
}

// Query returns samples for a device/key within [start, end).
func (h *MemHistory) Query(deviceID, key string, start, end time.Time) ([]history.Sample, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	k := seriesKey{deviceID: deviceID, key: key}
	all, ok := h.series[k]
	if !ok || len(all) == 0 {
		return nil, history.ErrNoData
	}

	var result []history.Sample
	for _, s := range all {
		if (s.Time.Equal(start) || s.Time.After(start)) && s.Time.Before(end) {
			result = append(result, s)
		}
	}

	if len(result) == 0 {
		return nil, history.ErrNoData
	}

	return result, nil
}
