// Package memstore provides an in-memory implementation of the store.Store
// interface. It is intended for testing and development; it is not durable.
package memstore

import (
	"slices"
	"sync"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store"
)

// MemStore is a concurrency-safe in-memory Store.
type MemStore struct {
	mu   sync.RWMutex
	docs map[shadow.DeviceID]*shadow.Document
	ops  []shadow.Op
	seen map[shadow.OpID]struct{}
}

// New creates an empty MemStore.
func New() *MemStore {
	return &MemStore{
		docs: make(map[shadow.DeviceID]*shadow.Document),
		seen: make(map[shadow.OpID]struct{}),
	}
}

// GetDocument retrieves a device's shadow document.
func (s *MemStore) GetDocument(deviceID shadow.DeviceID) (*shadow.Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	doc, ok := s.docs[deviceID]
	if !ok {
		return nil, store.ErrNotFound
	}
	// Return a shallow copy so callers don't mutate internal state.
	cp := *doc
	return &cp, nil
}

// PutDocument persists a shadow document, creating or replacing it.
func (s *MemStore) PutDocument(doc *shadow.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := *doc
	s.docs[doc.DeviceID] = &cp
	return nil
}

// AppendOp appends an operation to the log.
func (s *MemStore) AppendOp(op shadow.Op) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.seen[op.ID]; exists {
		return store.ErrOpExists
	}
	s.seen[op.ID] = struct{}{}
	s.ops = append(s.ops, op)
	return nil
}

// ListOps returns all ops for a device in timestamp order.
func (s *MemStore) ListOps(deviceID shadow.DeviceID) ([]shadow.Op, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []shadow.Op
	for _, op := range s.ops {
		if op.DeviceID == deviceID {
			result = append(result, op)
		}
	}

	slices.SortFunc(result, func(a, b shadow.Op) int {
		return hlc.Compare(a.Timestamp, b.Timestamp)
	})

	return result, nil
}

// HasOp reports whether an op with the given ID has been recorded.
func (s *MemStore) HasOp(id shadow.OpID) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.seen[id]
	return exists, nil
}

// TruncateOps removes all ops for a device with timestamps strictly before
// the given cutoff. It returns the number of ops removed.
func (s *MemStore) TruncateOps(deviceID shadow.DeviceID, before hlc.Timestamp) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	kept := s.ops[:0]
	for _, op := range s.ops {
		if op.DeviceID == deviceID && op.Timestamp.Before(before) {
			delete(s.seen, op.ID)
			removed++
		} else {
			kept = append(kept, op)
		}
	}
	s.ops = kept
	return removed, nil
}
