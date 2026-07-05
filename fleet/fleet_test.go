package fleet

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/davidhrinaldo/billet/converge"
	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/internal/testutil"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store/memstore"
	"github.com/davidhrinaldo/billet/transport"
)

// TestFleetConvergence400Devices simulates a fleet of 400 devices converging
// over a lossy transport. This is the M4 acceptance test.
//
// Scenario: controller sets desired "mode"="auto" for all 400 devices. The
// lossy transport drops 20% of frames in both directions. The test drives
// time forward in 1-second ticks, simulating device-side behavior (receive
// desired op, report back). It asserts:
//   - Convergence within a bounded number of ticks (< 500)
//   - StallReport correctly identifies non-converged devices at intermediate points
//   - All 400 devices emit EventConverged by the end
func TestFleetConvergence400Devices(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fleet simulation in -short mode")
	}

	const (
		numDevices = 400
		maxTicks   = 500
		tickStep   = int64(time.Second)
		lossRate   = 0.20
	)

	// Fixed-seed PRNG for deterministic loss.
	rng := rand.New(rand.NewSource(42))
	lossFn := func(_ transport.Channel) bool {
		return rng.Float64() < lossRate
	}

	ft := &fixedTime{ns: int64(time.Second)}
	clock := hlc.NewClock(1, ft)
	s := memstore.New()
	controller, deviceMux := testutil.NewLossyMuxPair(1024, lossFn)

	mgr := NewManager(ManagerConfig{
		Store:     s,
		Transport: controller,
		Clock:     clock,
		Timeout:   5 * time.Second,
		Budget:    BudgetConfig{MaxTokens: 5, RefillInterval: 500 * time.Millisecond},
		EventSize: 8192,
	})

	// Register all devices.
	deviceIDs := make([]shadow.DeviceID, numDevices)
	for i := range numDevices {
		id := shadow.DeviceID(fmt.Sprintf("dev-%04d", i))
		deviceIDs[i] = id
		mgr.Add(id)
	}

	// Set desired for all devices.
	gerr := mgr.SetDesiredGroup(deviceIDs, "mode", []byte("auto"))
	if gerr != nil {
		t.Fatalf("SetDesiredGroup: %v", gerr)
	}

	// Track which devices have converged via events.
	convergedSet := make(map[shadow.DeviceID]bool)
	stallChecked := false

	for tick := 0; tick < maxTicks; tick++ {
		ft.advance(time.Duration(tickStep))

		// Phase 1: drain controller inbound (device reports from previous tick).
		mgr.DrainInbound()

		// Phase 2: tick the manager (refill budgets, flush, retry timeouts).
		mgr.Tick(ft.ns)

		// Phase 3: simulate device side — read desired ops, report back.
		simulateDevices(t, deviceMux, ft.ns)

		// Phase 4: drain events and track convergence.
		drainEvents(mgr, convergedSet)

		// Intermediate stall check: around tick 10, verify StallReport is
		// non-empty (some devices should still be lagging due to loss).
		if tick == 10 && !stallChecked {
			report := mgr.StallReport()
			if len(report.Stalled) == 0 {
				t.Error("tick 10: StallReport is empty, expected some devices still lagging")
			}
			lagging := mgr.Lagging()
			if len(lagging) == 0 {
				t.Error("tick 10: Lagging() is empty")
			}
			stallChecked = true
		}

		// Early exit if fully converged.
		if len(convergedSet) == numDevices {
			t.Logf("fleet converged at tick %d", tick)
			break
		}
	}

	// Final assertions.
	report := mgr.StallReport()
	if len(report.Stalled) > 0 {
		t.Errorf("fleet did not fully converge: %d devices still stalled", len(report.Stalled))
		// Log a few for debugging.
		for i, sd := range report.Stalled {
			if i >= 5 {
				t.Logf("  ... and %d more", len(report.Stalled)-5)
				break
			}
			t.Logf("  stalled: %s (state=%v, since=%d)", sd.DeviceID, sd.State, sd.Since)
		}
	}

	if len(convergedSet) != numDevices {
		t.Errorf("converged %d/%d devices via events", len(convergedSet), numDevices)
	}

	if !stallChecked {
		t.Error("stall check at tick 10 was not reached")
	}
}

// simulateDevices reads all pending frames from the device-side transport,
// decodes them as desired ops, and sends matching reported ops back.
func simulateDevices(t *testing.T, deviceMux *testutil.LossyMux, nowNs int64) {
	t.Helper()
	for {
		select {
		case d := <-deviceMux.Inbound():
			// Decode the frame to extract the desired op.
			opID, _, _, payload, err := converge.ParseFragmentHeader(d.Frame)
			if err != nil {
				continue // silently skip malformed frames
			}
			op, err := converge.DecodeOp(opID, payload)
			if err != nil {
				continue
			}

			// "Device" applies the desired state and reports back.
			reportOp := shadow.Op{
				ID:        shadow.OpID{NodeID: 2, Seq: opID.Seq},
				DeviceID:  op.DeviceID,
				Section:   shadow.SectionReported,
				Key:       op.Key,
				Data:      op.Data,
				Timestamp: hlc.Timestamp{Physical: nowNs, NodeID: 2},
			}
			reportPayload := converge.EncodeOp(reportOp)
			frames, err := converge.Fragment(reportOp.ID, reportPayload, 1024)
			if err != nil {
				t.Fatalf("simulateDevices: Fragment: %v", err)
			}
			for _, frame := range frames {
				// Send back to controller — may be lost per lossFn.
				_ = deviceMux.Send(transport.Channel(op.DeviceID), frame)
			}
		default:
			return
		}
	}
}

// drainEvents reads all pending events from the manager and records converged
// devices.
func drainEvents(mgr *Manager, converged map[shadow.DeviceID]bool) {
	for {
		select {
		case ev := <-mgr.Events():
			if ev.Kind == EventConverged {
				converged[ev.DeviceID] = true
			}
		default:
			return
		}
	}
}
