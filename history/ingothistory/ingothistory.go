// Package ingothistory provides an ingot-backed implementation of the
// history.History interface. It lives in a separate Go module to isolate
// the ingot dependency from the core billet module.
package ingothistory

import (
	"fmt"
	"sync"
	"time"

	"github.com/davidhrinaldo/billet/history"
	"github.com/davidhrinaldo/ingot"
	"github.com/davidhrinaldo/ingot/labels"
)

// labelDevice is the label name for device ID.
const labelDevice = "device"

// labelKey is the label name for the series key.
const labelKey = "key"

// IngotHistory wraps an ingot.DB as a history.History.
type IngotHistory struct {
	db *ingot.DB

	// refs caches series references for fast appends after first resolution.
	mu   sync.RWMutex
	refs map[seriesID]uint64
}

type seriesID struct {
	deviceID string
	key      string
}

// New creates an IngotHistory wrapping the given ingot.DB. The caller is
// responsible for opening and closing the DB.
func New(db *ingot.DB) *IngotHistory {
	return &IngotHistory{
		db:   db,
		refs: make(map[seriesID]uint64),
	}
}

// Record stores a sample for the given device and key.
func (h *IngotHistory) Record(deviceID, key string, s history.Sample) error {
	sid := seriesID{deviceID: deviceID, key: key}

	// Fast path: check cached ref.
	h.mu.RLock()
	ref, ok := h.refs[sid]
	h.mu.RUnlock()

	ls := labels.Sort([]labels.Label{
		{Name: labelDevice, Value: deviceID},
		{Name: labelKey, Value: key},
	})

	app := h.db.Appender()
	newRef, err := app.Append(ref, ls, s.Time.UnixMilli(), s.Value)
	if err != nil {
		app.Rollback()
		return fmt.Errorf("ingothistory: append: %w", err)
	}
	if err := app.Commit(); err != nil {
		return fmt.Errorf("ingothistory: commit: %w", err)
	}

	// Cache the ref if new.
	if !ok || newRef != ref {
		h.mu.Lock()
		h.refs[sid] = newRef
		h.mu.Unlock()
	}

	return nil
}

// Query returns samples for a device/key within [start, end).
func (h *IngotHistory) Query(deviceID, key string, start, end time.Time) ([]history.Sample, error) {
	mintMs := start.UnixMilli()
	// end is exclusive in the History interface; ingot Querier range is
	// [mint, maxt] inclusive, so subtract 1ms.
	maxtMs := end.UnixMilli() - 1
	if maxtMs < mintMs {
		return nil, history.ErrNoData
	}

	q, err := h.db.Querier(mintMs, maxtMs)
	if err != nil {
		return nil, fmt.Errorf("ingothistory: querier: %w", err)
	}
	defer q.Close()

	ss := q.Select(
		labels.MustNewMatcher(labels.MatchEqual, labelDevice, deviceID),
		labels.MustNewMatcher(labels.MatchEqual, labelKey, key),
	)

	var samples []history.Sample
	for ss.Next() {
		series := ss.At()
		it := series.Iterator()
		for it.Next() {
			t, v := it.At()
			samples = append(samples, history.Sample{
				Time:  time.UnixMilli(t).UTC(),
				Value: v,
			})
		}
		if err := it.Err(); err != nil {
			return nil, fmt.Errorf("ingothistory: iterator: %w", err)
		}
	}
	if err := ss.Err(); err != nil {
		return nil, fmt.Errorf("ingothistory: series set: %w", err)
	}

	if len(samples) == 0 {
		return nil, history.ErrNoData
	}

	return samples, nil
}
