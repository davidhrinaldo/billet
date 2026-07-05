package converge

import (
	"time"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
)

// AckState tracks the in-flight status of a single operation.
type AckState struct {
	// SentAt is the local HLC timestamp when the op was last sent.
	SentAt hlc.Timestamp
	// Attempts is the number of times this op has been sent.
	Attempts int
}

// AckTracker tracks which operations are in-flight awaiting acknowledgment.
type AckTracker struct {
	inflight map[shadow.OpID]AckState
}

// NewAckTracker creates an empty AckTracker.
func NewAckTracker() *AckTracker {
	return &AckTracker{
		inflight: make(map[shadow.OpID]AckState),
	}
}

// MarkSent records that an op was sent at the given timestamp. If the op was
// already tracked, its SentAt is updated and Attempts is incremented.
func (a *AckTracker) MarkSent(id shadow.OpID, sentAt hlc.Timestamp) {
	state, ok := a.inflight[id]
	if ok {
		state.SentAt = sentAt
		state.Attempts++
	} else {
		state = AckState{SentAt: sentAt, Attempts: 1}
	}
	a.inflight[id] = state
}

// Ack removes an op from tracking, indicating successful delivery.
func (a *AckTracker) Ack(id shadow.OpID) {
	delete(a.inflight, id)
}

// Nack marks an op for retry by keeping it in the tracker. The caller is
// responsible for re-sending and calling MarkSent again.
func (a *AckTracker) Nack(id shadow.OpID) {
	// Nack is a no-op on the tracker state — the op remains in-flight.
	// The caller decides when to retry and calls MarkSent again.
}

// TimedOut returns the OpIDs of all in-flight ops whose SentAt timestamp is
// older than now minus timeout (comparing physical time only).
func (a *AckTracker) TimedOut(nowPhysicalNs int64, timeout time.Duration) []shadow.OpID {
	cutoff := nowPhysicalNs - timeout.Nanoseconds()
	var result []shadow.OpID
	for id, state := range a.inflight {
		if state.SentAt.Physical <= cutoff {
			result = append(result, id)
		}
	}
	return result
}

// Pending returns the number of ops currently in-flight.
func (a *AckTracker) Pending() int {
	return len(a.inflight)
}

// Get returns the AckState for an op, and whether it is being tracked.
func (a *AckTracker) Get(id shadow.OpID) (AckState, bool) {
	s, ok := a.inflight[id]
	return s, ok
}
