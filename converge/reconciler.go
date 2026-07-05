package converge

import (
	"errors"
	"time"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/oplog"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store"
	"github.com/davidhrinaldo/billet/transport"
)

// State represents the convergence state of a single device.
type State int

const (
	// Synced means reported matches desired — no outstanding work.
	Synced State = iota
	// Pending means desired has changed but ops have not been sent yet.
	Pending
	// Inflight means ops have been sent and are awaiting acknowledgment.
	Inflight
	// TimedOut means in-flight ops exceeded the timeout without ack.
	TimedOut
	// Diverged means a nack was received — manual intervention required.
	Diverged
)

// String returns the name of the state.
func (s State) String() string {
	switch s {
	case Synced:
		return "Synced"
	case Pending:
		return "Pending"
	case Inflight:
		return "Inflight"
	case TimedOut:
		return "TimedOut"
	case Diverged:
		return "Diverged"
	default:
		return "Unknown"
	}
}

// ErrDiverged is returned when an operation is attempted on a diverged
// reconciler that requires a reset first.
var ErrDiverged = errors.New("converge: device has diverged, reset required")

// Reconciler drives a single device toward convergence between its desired and
// reported state. It is not safe for concurrent use — the caller must
// serialize access.
type Reconciler struct {
	deviceID shadow.DeviceID
	state    State
	log      *oplog.Log
	st       store.Store
	tr       transport.Transport
	clock    *hlc.Clock
	acks     *AckTracker
	timeout  time.Duration
	seq      uint64 // monotonic sequence counter for OpIDs
}

// ReconcilerConfig holds the parameters for creating a Reconciler.
type ReconcilerConfig struct {
	DeviceID  shadow.DeviceID
	Log       *oplog.Log
	Store     store.Store
	Transport transport.Transport
	Clock     *hlc.Clock
	Timeout   time.Duration
}

// NewReconciler creates a Reconciler for a single device.
func NewReconciler(cfg ReconcilerConfig) *Reconciler {
	return &Reconciler{
		deviceID: cfg.DeviceID,
		state:    Synced,
		log:      cfg.Log,
		st:       cfg.Store,
		tr:       cfg.Transport,
		clock:    cfg.Clock,
		acks:     NewAckTracker(),
		timeout:  cfg.Timeout,
	}
}

// State returns the current convergence state.
func (r *Reconciler) CurrentState() State {
	return r.state
}

// SetDesired writes a desired-section op to the op-log and transitions to
// Pending if currently Synced or Inflight. Returns ErrDiverged if the device
// has diverged.
func (r *Reconciler) SetDesired(key string, data []byte) error {
	if r.state == Diverged {
		return ErrDiverged
	}

	r.seq++
	ts := r.clock.Now()
	op := shadow.Op{
		ID:        shadow.OpID{NodeID: ts.NodeID, Seq: r.seq},
		DeviceID:  r.deviceID,
		Section:   shadow.SectionDesired,
		Key:       key,
		Data:      data,
		Timestamp: ts,
	}

	if err := r.log.Append(op); err != nil {
		return err
	}

	// Apply the op to the stored document so Delta() reflects the new desired.
	doc, err := r.getOrCreateDoc()
	if err != nil {
		return err
	}
	if doc.Desired.Values == nil {
		doc.Desired.Values = make(map[string]shadow.Value)
	}
	doc.Desired.Values[key] = shadow.Value{Data: data, Timestamp: ts}
	doc.Version = ts
	if err := r.st.PutDocument(doc); err != nil {
		return err
	}

	if r.state == Synced || r.state == Inflight {
		r.state = Pending
	}
	return nil
}

// OnReported processes an inbound reported-section op from the device. It
// applies the op, recomputes the delta, and transitions state accordingly.
func (r *Reconciler) OnReported(op shadow.Op) error {
	if err := r.log.Append(op); err != nil {
		return err
	}

	doc, err := r.getOrCreateDoc()
	if err != nil {
		return err
	}
	if doc.Reported.Values == nil {
		doc.Reported.Values = make(map[string]shadow.Value)
	}
	doc.Reported.Values[op.Key] = shadow.Value{Data: op.Data, Timestamp: op.Timestamp}
	if !op.Timestamp.Before(doc.Version) {
		doc.Version = op.Timestamp
	}
	if err := r.st.PutDocument(doc); err != nil {
		return err
	}

	// If all in-flight ops for this key are now reflected, ack them.
	// Simplified: ack any in-flight op whose desired value now matches reported.
	delta := doc.Delta()
	if delta.IsEmpty() && (r.state == Inflight || r.state == Pending) {
		// All desired keys now match reported. Clear acks, go Synced.
		r.acks = NewAckTracker()
		r.state = Synced
	}

	return nil
}

// OnAck acknowledges successful delivery of an op. If the delta is now empty,
// transitions to Synced.
func (r *Reconciler) OnAck(id shadow.OpID) error {
	r.acks.Ack(id)

	doc, err := r.getOrCreateDoc()
	if err != nil {
		return err
	}

	delta := doc.Delta()
	if delta.IsEmpty() && r.acks.Pending() == 0 && r.state == Inflight {
		r.state = Synced
	}
	return nil
}

// OnNack records that the device rejected an op. Transitions to Diverged.
func (r *Reconciler) OnNack(id shadow.OpID) {
	r.acks.Nack(id)
	r.state = Diverged
}

// Flush sends all pending desired ops over the transport. Fragments ops that
// exceed the transport's MaxFrameBytes. Transitions Pending → Inflight.
func (r *Reconciler) Flush() error {
	if r.state != Pending {
		return nil
	}

	doc, err := r.getOrCreateDoc()
	if err != nil {
		return err
	}
	delta := doc.Delta()
	if delta.IsEmpty() {
		r.state = Synced
		return nil
	}

	caps := r.tr.Caps()
	ch := transport.Channel(r.deviceID)

	// Build and send an op for each diff.
	for _, diff := range delta.Diffs {
		r.seq++
		ts := r.clock.Now()
		op := shadow.Op{
			ID:        shadow.OpID{NodeID: ts.NodeID, Seq: r.seq},
			DeviceID:  r.deviceID,
			Section:   shadow.SectionDesired,
			Key:       diff.Key,
			Data:      diff.Desired,
			Timestamp: ts,
		}

		payload := EncodeOp(op)
		frames, err := Fragment(op.ID, payload, caps.MaxFrameBytes)
		if err != nil {
			return err
		}

		for _, frame := range frames {
			if err := r.tr.Send(ch, frame); err != nil {
				return err
			}
		}

		r.acks.MarkSent(op.ID, ts)
	}

	r.state = Inflight
	return nil
}

// Tick checks for timed-out in-flight ops and re-sends them. Transitions
// Inflight → TimedOut on first detection, TimedOut → Inflight on retry.
func (r *Reconciler) Tick(nowPhysicalNs int64) error {
	if r.state != Inflight && r.state != TimedOut {
		return nil
	}

	timedOut := r.acks.TimedOut(nowPhysicalNs, r.timeout)
	if len(timedOut) == 0 {
		return nil
	}

	if r.state == Inflight {
		r.state = TimedOut
	}

	// Re-send timed-out ops from the op-log.
	ops, err := r.log.Replay(r.deviceID)
	if err != nil {
		return err
	}

	caps := r.tr.Caps()
	ch := transport.Channel(r.deviceID)

	// Build a set of timed-out IDs for fast lookup.
	timedOutSet := make(map[shadow.OpID]struct{}, len(timedOut))
	for _, id := range timedOut {
		timedOutSet[id] = struct{}{}
	}

	// Re-send ops that match timed-out IDs. In practice the ack tracker
	// holds OpIDs we generated in Flush, which may not be in the op-log
	// (Flush sends delta-derived ops over the wire but doesn't persist them
	// to the log). So we rebuild and resend from the current delta instead.
	doc, err := r.getOrCreateDoc()
	if err != nil {
		return err
	}
	_ = ops // ops used for log-based retry in future; delta-based for now

	delta := doc.Delta()
	for _, diff := range delta.Diffs {
		r.seq++
		ts := r.clock.Now()
		op := shadow.Op{
			ID:        shadow.OpID{NodeID: ts.NodeID, Seq: r.seq},
			DeviceID:  r.deviceID,
			Section:   shadow.SectionDesired,
			Key:       diff.Key,
			Data:      diff.Desired,
			Timestamp: ts,
		}

		payload := EncodeOp(op)
		frames, err := Fragment(op.ID, payload, caps.MaxFrameBytes)
		if err != nil {
			return err
		}
		for _, frame := range frames {
			if err := r.tr.Send(ch, frame); err != nil {
				return err
			}
		}
		r.acks.MarkSent(op.ID, ts)
	}

	// Clear old timed-out entries (they've been superseded by new sends).
	for id := range timedOutSet {
		r.acks.Ack(id)
	}

	if delta.IsEmpty() {
		r.state = Synced
	} else {
		r.state = Inflight
	}
	return nil
}

// Reset clears the Diverged state back to Pending so the reconciler can retry.
func (r *Reconciler) Reset() {
	if r.state == Diverged {
		r.acks = NewAckTracker()
		r.state = Pending
	}
}

// getOrCreateDoc retrieves the device document from the store, creating an
// empty one if it doesn't exist.
func (r *Reconciler) getOrCreateDoc() (*shadow.Document, error) {
	doc, err := r.st.GetDocument(r.deviceID)
	if errors.Is(err, store.ErrNotFound) {
		return &shadow.Document{
			DeviceID: r.deviceID,
			Reported: shadow.Section{Values: make(map[string]shadow.Value)},
			Desired:  shadow.Section{Values: make(map[string]shadow.Value)},
		}, nil
	}
	return doc, err
}
