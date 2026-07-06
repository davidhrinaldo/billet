package fleet

import (
	"github.com/davidhrinaldo/billet/converge"
	"github.com/davidhrinaldo/billet/shadow"
)

// DeviceSnapshot is a point-in-time view of a device's convergence state and
// shadow document. It consolidates state, reported/desired values, and delta
// into a single value for observability consumers.
type DeviceSnapshot struct {
	// DeviceID identifies the device.
	DeviceID shadow.DeviceID
	// State is the current convergence state.
	State converge.State
	// Since is the physical nanosecond timestamp when the device entered its
	// current non-Synced state. Zero when the device is Synced.
	Since int64
	// Reported maps keys to their reported (device-owned) values.
	Reported map[string][]byte
	// Desired maps keys to their desired (controller-owned) values.
	Desired map[string][]byte
	// Delta is the set of keys where desired differs from reported.
	Delta shadow.Delta
}
