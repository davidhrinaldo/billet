// Package history defines the interface for numeric time-series storage of
// reported values. The production implementation wraps ingot; the in-memory
// stub in memhistory is for testing.
package history

import (
	"errors"
	"time"
)

// ErrNoData is returned when a query matches no recorded samples.
var ErrNoData = errors.New("history: no data")

// Sample is a single timestamped numeric value.
type Sample struct {
	Time  time.Time
	Value float64
}

// History is the interface for recording and querying numeric reported-value
// time series. Each series is identified by a device ID and a key (channel name).
type History interface {
	// Record stores a sample for the given device and key.
	Record(deviceID, key string, s Sample) error

	// Query returns samples for a device/key within the given time range,
	// inclusive of start, exclusive of end. Results are in time order.
	// Returns ErrNoData if no samples exist in the range.
	Query(deviceID, key string, start, end time.Time) ([]Sample, error)
}
