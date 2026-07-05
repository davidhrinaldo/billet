package fleet

import (
	"github.com/davidhrinaldo/billet/converge"
	"github.com/davidhrinaldo/billet/shadow"
)

// StalledDevice describes a single device that has not converged.
type StalledDevice struct {
	// DeviceID identifies the device.
	DeviceID shadow.DeviceID
	// State is the device's current convergence state.
	State converge.State
	// Since is the physical nanosecond timestamp when the device entered its
	// current non-Synced state.
	Since int64
}

// StallReport is a snapshot of all devices that have not reached the Synced
// state. An empty Stalled slice means the fleet is fully converged.
type StallReport struct {
	// Stalled lists every device that is not in the Synced state.
	Stalled []StalledDevice
}
