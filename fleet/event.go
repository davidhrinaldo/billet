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
	default:
		return "Unknown"
	}
}

// Event is an observable occurrence in the fleet manager, emitted to the
// events channel when a device transitions between convergence states.
type Event struct {
	// Kind identifies the event type.
	Kind EventKind
	// DeviceID is the device that transitioned.
	DeviceID shadow.DeviceID
	// From is the previous convergence state.
	From converge.State
	// To is the new convergence state.
	To converge.State
	// At is the physical nanosecond timestamp when the event was generated.
	At int64
}
