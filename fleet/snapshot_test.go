package fleet

import (
	"fmt"
	"testing"

	"github.com/davidhrinaldo/billet/converge"
	"github.com/davidhrinaldo/billet/shadow"
)

func TestDeviceIDs(t *testing.T) {
	tests := []struct {
		name    string
		count   int
		wantLen int
	}{
		{name: "empty fleet", count: 0, wantLen: 0},
		{name: "single device", count: 1, wantLen: 1},
		{name: "multiple devices", count: 5, wantLen: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, _, _ := newTestManager(t, tt.count)

			ids := mgr.DeviceIDs()
			if len(ids) != tt.wantLen {
				t.Fatalf("DeviceIDs() returned %d ids, want %d", len(ids), tt.wantLen)
			}

			// Verify sorted order.
			for i := 1; i < len(ids); i++ {
				if ids[i-1] >= ids[i] {
					t.Errorf("DeviceIDs() not sorted: %q >= %q at index %d", ids[i-1], ids[i], i)
				}
			}
		})
	}
}

func TestSnapshot(t *testing.T) {
	tests := []struct {
		name       string
		id         shadow.DeviceID
		setDesired bool // whether to set desired state before snapshot
		wantOK     bool
		wantState  converge.State
		wantEmpty  bool // whether delta should be empty
	}{
		{
			name:   "unknown device",
			id:     "dev-unknown",
			wantOK: false,
		},
		{
			name:      "synced device with no desired",
			id:        "dev-0000",
			wantOK:    true,
			wantState: converge.Synced,
			wantEmpty: true,
		},
		{
			name:       "pending device with desired",
			id:         "dev-0000",
			setDesired: true,
			wantOK:     true,
			wantState:  converge.Pending,
			wantEmpty:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, _, _ := newTestManager(t, 2)

			if tt.setDesired {
				if err := mgr.SetDesired(tt.id, "mode", []byte("eco")); err != nil {
					t.Fatalf("SetDesired: %v", err)
				}
			}

			snap, ok := mgr.Snapshot(tt.id)
			if ok != tt.wantOK {
				t.Fatalf("Snapshot(%q) ok = %v, want %v", tt.id, ok, tt.wantOK)
			}
			if !ok {
				return
			}

			if snap.DeviceID != tt.id {
				t.Errorf("DeviceID = %q, want %q", snap.DeviceID, tt.id)
			}
			if snap.State != tt.wantState {
				t.Errorf("State = %v, want %v", snap.State, tt.wantState)
			}
			if snap.State == converge.Synced && snap.Since != 0 {
				t.Errorf("Since = %d for Synced device, want 0", snap.Since)
			}
			if tt.wantEmpty && !snap.Delta.IsEmpty() {
				t.Errorf("Delta should be empty, got %d diffs", len(snap.Delta.Diffs))
			}
			if !tt.wantEmpty && snap.Delta.IsEmpty() {
				t.Error("Delta should not be empty")
			}
			if tt.setDesired {
				if string(snap.Desired["mode"]) != "eco" {
					t.Errorf("Desired[mode] = %q, want %q", snap.Desired["mode"], "eco")
				}
			}
		})
	}
}

func TestFleetSnapshot(t *testing.T) {
	tests := []struct {
		name      string
		count     int
		wantCount int
	}{
		{name: "empty fleet", count: 0, wantCount: 0},
		{name: "multiple devices", count: 3, wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, _, _ := newTestManager(t, tt.count)

			snaps := mgr.FleetSnapshot()
			if len(snaps) != tt.wantCount {
				t.Fatalf("FleetSnapshot() returned %d snapshots, want %d", len(snaps), tt.wantCount)
			}

			// Verify sorted order.
			for i := 1; i < len(snaps); i++ {
				if snaps[i-1].DeviceID >= snaps[i].DeviceID {
					t.Errorf("FleetSnapshot() not sorted: %q >= %q", snaps[i-1].DeviceID, snaps[i].DeviceID)
				}
			}

			// Verify each snapshot has the expected device ID.
			for i, snap := range snaps {
				wantID := shadow.DeviceID(fmt.Sprintf("dev-%04d", i))
				if snap.DeviceID != wantID {
					t.Errorf("snaps[%d].DeviceID = %q, want %q", i, snap.DeviceID, wantID)
				}
			}
		})
	}
}
