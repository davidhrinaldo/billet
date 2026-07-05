// Package oplog provides higher-level op-log operations on top of store.Store.
// It coordinates append, replay, snapshot, and truncation of operations for a
// single device's shadow document.
package oplog

import (
	"errors"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store"
)

// ErrNoOps is returned when a snapshot is requested but the device has no ops.
var ErrNoOps = errors.New("oplog: no ops to snapshot")

// Policy configures automatic op-log compaction thresholds. When either
// threshold is exceeded after an Append, the log triggers an automatic
// Snapshot for the affected device.
type Policy struct {
	// MaxOps triggers compaction when the device's op count exceeds this value.
	// Zero means no op-count limit.
	MaxOps int

	// MaxBytes triggers compaction when the approximate total data size of a
	// device's ops exceeds this value. Size is estimated as the sum of
	// len(op.Data) across the device's ops. Zero means no byte limit.
	MaxBytes int64
}

// Log provides append, replay, snapshot, and truncate operations over a Store.
type Log struct {
	store  store.Store
	policy *Policy
}

// New creates an op-log backed by the given Store.
func New(s store.Store, opts ...Option) *Log {
	l := &Log{store: s}
	for _, o := range opts {
		o(l)
	}
	return l
}

// Option configures a Log.
type Option func(*Log)

// WithPolicy sets the auto-compaction policy for the log.
func WithPolicy(p Policy) Option {
	return func(l *Log) {
		l.policy = &p
	}
}

// Append adds an op to the log. Duplicate ops (same OpID) are silently ignored.
// If a Policy is set and the append causes thresholds to be exceeded, an
// automatic Snapshot is triggered for the device.
func (l *Log) Append(op shadow.Op) error {
	err := l.store.AppendOp(op)
	if errors.Is(err, store.ErrOpExists) {
		return nil
	}
	if err != nil {
		return err
	}

	if l.policy != nil {
		if err := l.maybeCompact(op.DeviceID); err != nil {
			return err
		}
	}
	return nil
}

// Replay returns all ops for a device in timestamp order.
func (l *Log) Replay(deviceID shadow.DeviceID) ([]shadow.Op, error) {
	return l.store.ListOps(deviceID)
}

// Apply replays all ops for a device onto its stored document and returns the
// result. If no document exists yet, a new one is created. The document is not
// persisted — call Snapshot for that.
func (l *Log) Apply(deviceID shadow.DeviceID) (*shadow.Document, error) {
	doc, err := l.store.GetDocument(deviceID)
	if errors.Is(err, store.ErrNotFound) {
		doc = &shadow.Document{
			DeviceID: deviceID,
			Reported: shadow.Section{Values: make(map[string]shadow.Value)},
			Desired:  shadow.Section{Values: make(map[string]shadow.Value)},
		}
	} else if err != nil {
		return nil, err
	}

	ops, err := l.store.ListOps(deviceID)
	if err != nil {
		return nil, err
	}

	applyOps(doc, ops)
	return doc, nil
}

// Snapshot replays all ops into the device's document, persists it, and
// truncates the applied ops. Returns the snapshotted document.
func (l *Log) Snapshot(deviceID shadow.DeviceID) (*shadow.Document, error) {
	ops, err := l.store.ListOps(deviceID)
	if err != nil {
		return nil, err
	}
	if len(ops) == 0 {
		return nil, ErrNoOps
	}

	doc, err := l.store.GetDocument(deviceID)
	if errors.Is(err, store.ErrNotFound) {
		doc = &shadow.Document{
			DeviceID: deviceID,
			Reported: shadow.Section{Values: make(map[string]shadow.Value)},
			Desired:  shadow.Section{Values: make(map[string]shadow.Value)},
		}
	} else if err != nil {
		return nil, err
	}

	applyOps(doc, ops)

	if err := l.store.PutDocument(doc); err != nil {
		return nil, err
	}

	// Truncate ops up to and including the last applied op. Use a cutoff
	// just after the last op's timestamp so the last op is included.
	last := ops[len(ops)-1].Timestamp
	cutoff := hlc.Timestamp{
		Physical: last.Physical,
		Logical:  last.Logical + 1,
		NodeID:   last.NodeID,
	}
	if _, err := l.store.TruncateOps(deviceID, cutoff); err != nil {
		return nil, err
	}

	return doc, nil
}

// Truncate removes ops for a device with timestamps before the cutoff.
func (l *Log) Truncate(deviceID shadow.DeviceID, before hlc.Timestamp) (int, error) {
	return l.store.TruncateOps(deviceID, before)
}

// maybeCompact checks whether the device's op-log exceeds the configured policy
// thresholds and triggers a Snapshot if so.
func (l *Log) maybeCompact(deviceID shadow.DeviceID) error {
	ops, err := l.store.ListOps(deviceID)
	if err != nil {
		return err
	}

	exceeded := false
	if l.policy.MaxOps > 0 && len(ops) > l.policy.MaxOps {
		exceeded = true
	}
	if !exceeded && l.policy.MaxBytes > 0 {
		var total int64
		for _, op := range ops {
			total += int64(len(op.Data))
		}
		if total > l.policy.MaxBytes {
			exceeded = true
		}
	}

	if exceeded {
		_, err := l.Snapshot(deviceID)
		return err
	}
	return nil
}

// applyOps applies ops to a document in order. Ops must be sorted by timestamp.
func applyOps(doc *shadow.Document, ops []shadow.Op) {
	for _, op := range ops {
		section := &doc.Reported
		if op.Section == shadow.SectionDesired {
			section = &doc.Desired
		}
		if section.Values == nil {
			section.Values = make(map[string]shadow.Value)
		}
		section.Values[op.Key] = shadow.Value{
			Data:      op.Data,
			Timestamp: op.Timestamp,
		}
		if op.Timestamp.Before(doc.Version) {
			continue
		}
		doc.Version = op.Timestamp
	}
}
