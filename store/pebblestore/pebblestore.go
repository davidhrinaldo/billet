// Package pebblestore provides a Pebble-backed implementation of the
// store.Store interface. It lives in a separate Go module to isolate
// the CGo dependency on Pebble from the core billet module.
package pebblestore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store"
)

// Key prefixes partition the keyspace.
var (
	prefixDoc  = []byte("d/") // d/<deviceID>
	prefixOp   = []byte("o/") // o/<deviceID>/<physical8>/<logical2>/<nodeID2>
	prefixDedup = []byte("i/") // i/<nodeID2>/<seq8>
)

// PebbleStore is a durable, Pebble-backed Store.
type PebbleStore struct {
	db *pebble.DB
}

// Open opens or creates a PebbleStore at the given directory.
func Open(dir string, opts *pebble.Options) (*PebbleStore, error) {
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("pebblestore: open: %w", err)
	}
	return &PebbleStore{db: db}, nil
}

// Close closes the underlying Pebble database.
func (s *PebbleStore) Close() error {
	return s.db.Close()
}

// Metrics returns the underlying Pebble metrics for flash-budget instrumentation.
func (s *PebbleStore) Metrics() *pebble.Metrics {
	return s.db.Metrics()
}

// GetDocument retrieves a device's shadow document.
func (s *PebbleStore) GetDocument(deviceID shadow.DeviceID) (*shadow.Document, error) {
	val, closer, err := s.db.Get(docKey(deviceID))
	if err == pebble.ErrNotFound {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("pebblestore: get document: %w", err)
	}
	defer closer.Close()

	var doc shadow.Document
	if err := json.Unmarshal(val, &doc); err != nil {
		return nil, fmt.Errorf("pebblestore: unmarshal document: %w", err)
	}
	return &doc, nil
}

// PutDocument persists a shadow document, creating or replacing it.
func (s *PebbleStore) PutDocument(doc *shadow.Document) error {
	val, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("pebblestore: marshal document: %w", err)
	}
	if err := s.db.Set(docKey(doc.DeviceID), val, pebble.Sync); err != nil {
		return fmt.Errorf("pebblestore: put document: %w", err)
	}
	return nil
}

// AppendOp appends an operation to the log. Returns store.ErrOpExists if the
// op ID has already been recorded.
func (s *PebbleStore) AppendOp(op shadow.Op) error {
	dk := dedupKey(op.ID)

	// Check dedup index first.
	_, closer, err := s.db.Get(dk)
	if err == nil {
		closer.Close()
		return store.ErrOpExists
	}
	if err != pebble.ErrNotFound {
		return fmt.Errorf("pebblestore: dedup check: %w", err)
	}

	val, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("pebblestore: marshal op: %w", err)
	}

	// Write op + dedup key atomically.
	batch := s.db.NewBatch()
	if err := batch.Set(opKey(op.DeviceID, op.Timestamp), val, nil); err != nil {
		batch.Close()
		return fmt.Errorf("pebblestore: batch set op: %w", err)
	}
	if err := batch.Set(dk, nil, nil); err != nil {
		batch.Close()
		return fmt.Errorf("pebblestore: batch set dedup: %w", err)
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("pebblestore: commit op: %w", err)
	}
	return nil
}

// ListOps returns all ops for a device in timestamp order.
func (s *PebbleStore) ListOps(deviceID shadow.DeviceID) ([]shadow.Op, error) {
	prefix := opDevicePrefix(deviceID)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixSuccessor(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("pebblestore: new iter: %w", err)
	}
	defer iter.Close()

	var ops []shadow.Op
	for iter.First(); iter.Valid(); iter.Next() {
		var op shadow.Op
		if err := json.Unmarshal(iter.Value(), &op); err != nil {
			return nil, fmt.Errorf("pebblestore: unmarshal op: %w", err)
		}
		ops = append(ops, op)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("pebblestore: iter error: %w", err)
	}
	return ops, nil
}

// HasOp reports whether an op with the given ID has been recorded.
func (s *PebbleStore) HasOp(id shadow.OpID) (bool, error) {
	_, closer, err := s.db.Get(dedupKey(id))
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("pebblestore: has op: %w", err)
	}
	closer.Close()
	return true, nil
}

// TruncateOps removes all ops for a device with timestamps strictly before
// the given cutoff. It returns the number of ops removed.
func (s *PebbleStore) TruncateOps(deviceID shadow.DeviceID, before hlc.Timestamp) (int, error) {
	prefix := opDevicePrefix(deviceID)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: opKey(deviceID, before),
	})
	if err != nil {
		return 0, fmt.Errorf("pebblestore: new iter: %w", err)
	}

	// Collect keys to delete (op keys + their dedup keys).
	type toDelete struct {
		opKey    []byte
		dedupKey []byte
	}
	var deletions []toDelete
	for iter.First(); iter.Valid(); iter.Next() {
		var op shadow.Op
		if err := json.Unmarshal(iter.Value(), &op); err != nil {
			iter.Close()
			return 0, fmt.Errorf("pebblestore: unmarshal op: %w", err)
		}
		// Copy the key since the iterator reuses the buffer.
		k := make([]byte, len(iter.Key()))
		copy(k, iter.Key())
		deletions = append(deletions, toDelete{
			opKey:    k,
			dedupKey: dedupKey(op.ID),
		})
	}
	if err := iter.Error(); err != nil {
		iter.Close()
		return 0, fmt.Errorf("pebblestore: iter error: %w", err)
	}
	iter.Close()

	if len(deletions) == 0 {
		return 0, nil
	}

	batch := s.db.NewBatch()
	for _, d := range deletions {
		if err := batch.Delete(d.opKey, nil); err != nil {
			batch.Close()
			return 0, fmt.Errorf("pebblestore: batch delete op: %w", err)
		}
		if err := batch.Delete(d.dedupKey, nil); err != nil {
			batch.Close()
			return 0, fmt.Errorf("pebblestore: batch delete dedup: %w", err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, fmt.Errorf("pebblestore: commit truncate: %w", err)
	}
	return len(deletions), nil
}

// --- key encoding ---

// docKey returns the Pebble key for a document: d/<deviceID>
func docKey(deviceID shadow.DeviceID) []byte {
	return append(append([]byte(nil), prefixDoc...), []byte(deviceID)...)
}

// opKey returns the Pebble key for an op: o/<deviceID>\x00<physical8><logical2><nodeID2>
// The null byte separates the variable-length deviceID from the fixed-width
// timestamp suffix, ensuring correct lexicographic ordering.
func opKey(deviceID shadow.DeviceID, ts hlc.Timestamp) []byte {
	buf := make([]byte, 0, len(prefixOp)+len(deviceID)+1+12)
	buf = append(buf, prefixOp...)
	buf = append(buf, []byte(deviceID)...)
	buf = append(buf, 0) // separator
	buf = appendTimestamp(buf, ts)
	return buf
}

// opDevicePrefix returns the key prefix for all ops of a device: o/<deviceID>\x00
func opDevicePrefix(deviceID shadow.DeviceID) []byte {
	buf := make([]byte, 0, len(prefixOp)+len(deviceID)+1)
	buf = append(buf, prefixOp...)
	buf = append(buf, []byte(deviceID)...)
	buf = append(buf, 0)
	return buf
}

// dedupKey returns the dedup index key: i/<nodeID2><seq8>
func dedupKey(id shadow.OpID) []byte {
	buf := make([]byte, 0, len(prefixDedup)+10)
	buf = append(buf, prefixDedup...)
	buf = binary.BigEndian.AppendUint16(buf, id.NodeID)
	buf = binary.BigEndian.AppendUint64(buf, id.Seq)
	return buf
}

// appendTimestamp appends an HLC timestamp as 12 big-endian bytes (physical8 + logical2 + nodeID2).
func appendTimestamp(buf []byte, ts hlc.Timestamp) []byte {
	buf = binary.BigEndian.AppendUint64(buf, uint64(ts.Physical))
	buf = binary.BigEndian.AppendUint16(buf, ts.Logical)
	buf = binary.BigEndian.AppendUint16(buf, ts.NodeID)
	return buf
}

// prefixSuccessor returns the lexicographic successor of a prefix, used as an
// exclusive upper bound for prefix iteration.
func prefixSuccessor(prefix []byte) []byte {
	succ := make([]byte, len(prefix))
	copy(succ, prefix)
	for i := len(succ) - 1; i >= 0; i-- {
		succ[i]++
		if succ[i] != 0 {
			return succ
		}
	}
	// All 0xFF — return nil to mean "no upper bound" (won't happen in practice).
	return nil
}
