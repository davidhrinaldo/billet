package fleet

import (
	"github.com/davidhrinaldo/billet/converge"
	"github.com/davidhrinaldo/billet/shadow"
)

// EventKind identifies the type of fleet event.
type EventKind int

const (
	// EventStateChange indicates a device changed convergence state.
	EventStateChange EventKind = iota
	// EventConverged indicates a device reached the Synced state.
	EventConverged
	// EventDiverged indicates a device entered the Diverged state.
	EventDiverged
	// EventError indicates a per-device error during a fleet operation.
	// The Err field carries the underlying error. The device is skipped
	// for this tick but remains registered — the manager does not abort
	// the entire tick on a single device's failure.
	EventError
)

// String returns the name of the event kind.
func (k EventKind) String() string {
	switch k {
	case EventStateChange:
		return "StateChange"
	case EventConverged:
		return "Converged"
	case EventDiverged:
		return "Diverged"
	case EventError:
		return "Error"
	default:
		return "Unknown"
	}
}

// Event is an observable occurrence in the fleet manager, emitted to the
// events channel for state transitions and per-device errors.
type Event struct {
	// Kind identifies the event type.
	Kind EventKind
	// DeviceID is the affected device.
	DeviceID shadow.DeviceID
	// From is the previous convergence state (state-change events only).
	From converge.State
	// To is the new convergence state (state-change events only).
	To converge.State
	// At is the physical nanosecond timestamp when the event was generated.
	At int64
	// Err is the error that occurred (EventError only).
	Err error
}
