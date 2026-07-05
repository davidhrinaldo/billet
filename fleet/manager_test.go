package fleet

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/davidhrinaldo/billet/converge"
	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/internal/testutil"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store/memstore"
	"github.com/davidhrinaldo/billet/transport"
)

// fixedTime is a TimeSource that returns a manually-advanced clock.
type fixedTime struct {
	ns int64
}

func (f *fixedTime) Now() time.Time { return time.Unix(0, f.ns) }

func (f *fixedTime) advance(d time.Duration) { f.ns += int64(d) }

// newTestManager creates a Manager with n pre-registered devices using a
// LossyMuxPair with no loss. Returns the manager, the device-side transport,
// and the fixedTime source.
func newTestManager(t *testing.T, n int) (*Manager, *testutil.LossyMux, *fixedTime) {
	t.Helper()
	ft := &fixedTime{ns: int64(time.Second)}
	clock := hlc.NewClock(1, ft)
	s := memstore.New()
	controller, device := testutil.NewLossyMuxPair(1024, nil)

	mgr := NewManager(ManagerConfig{
		Store:     s,
		Transport: controller,
		Clock:     clock,
		Timeout:   5 * time.Second,
		Budget:    BudgetConfig{MaxTokens: 3, RefillInterval: time.Second},
		EventSize: 128,
	})

	for i := range n {
		mgr.Add(shadow.DeviceID(fmt.Sprintf("dev-%04d", i)))
	}
	return mgr, device, ft
}

func TestManagerAddRemove(t *testing.T) {
	mgr, _, _ := newTestManager(t, 0)

	mgr.Add("dev-a")
	mgr.Add("dev-b")
	if mgr.Devices() != 2 {
		t.Fatalf("Devices() = %d, want 2", mgr.Devices())
	}

	// Duplicate add is a no-op.
	mgr.Add("dev-a")
	if mgr.Devices() != 2 {
		t.Fatalf("Devices() after dup = %d, want 2", mgr.Devices())
	}

	mgr.Remove("dev-a")
	if mgr.Devices() != 1 {
		t.Fatalf("Devices() after remove = %d, want 1", mgr.Devices())
	}

	// Remove unknown is a no-op.
	mgr.Remove("dev-z")
	if mgr.Devices() != 1 {
		t.Fatalf("Devices() after remove unknown = %d, want 1", mgr.Devices())
	}
}

func TestManagerSetDesiredUnknownDevice(t *testing.T) {
	mgr, _, _ := newTestManager(t, 1)

	err := mgr.SetDesired("dev-unknown", "key", []byte("val"))
	if !errors.Is(err, ErrUnknownDevice) {
		t.Fatalf("SetDesired(unknown) = %v, want ErrUnknownDevice", err)
	}
}

func TestManagerSetDesiredTransitionsToPending(t *testing.T) {
	mgr, _, _ := newTestManager(t, 1)

	if err := mgr.SetDesired("dev-0000", "mode", []byte("auto")); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}

	state, ok := mgr.DeviceState("dev-0000")
	if !ok {
		t.Fatal("DeviceState returned false for known device")
	}
	if state != converge.Pending {
		t.Errorf("state = %v, want Pending", state)
	}
}

func TestManagerSetDesiredGroup(t *testing.T) {
	tests := []struct {
		name      string
		ids       []shadow.DeviceID
		wantErr   bool
		wantCount int // number of errors in GroupError
	}{
		{
			name:    "all known devices succeed",
			ids:     []shadow.DeviceID{"dev-0000", "dev-0001"},
			wantErr: false,
		},
		{
			name:      "unknown device produces GroupError",
			ids:       []shadow.DeviceID{"dev-0000", "dev-unknown"},
			wantErr:   true,
			wantCount: 1,
		},
		{
			name:      "all unknown",
			ids:       []shadow.DeviceID{"dev-x", "dev-y"},
			wantErr:   true,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, _, _ := newTestManager(t, 2)

			gerr := mgr.SetDesiredGroup(tt.ids, "mode", []byte("auto"))
			if tt.wantErr {
				if gerr == nil {
					t.Fatal("expected GroupError, got nil")
				}
				if len(gerr.Errors) != tt.wantCount {
					t.Errorf("GroupError.Errors has %d entries, want %d", len(gerr.Errors), tt.wantCount)
				}
				// Verify Error() produces non-empty string.
				if gerr.Error() == "" {
					t.Error("GroupError.Error() returned empty string")
				}
			} else {
				if gerr != nil {
					t.Fatalf("unexpected GroupError: %v", gerr)
				}
			}
		})
	}
}

func TestManagerTickFlushesWithBudget(t *testing.T) {
	mgr, device, ft := newTestManager(t, 1)

	if err := mgr.SetDesired("dev-0000", "mode", []byte("auto")); err != nil {
		t.Fatal(err)
	}

	// Tick should flush (budget starts full).
	if err := mgr.Tick(ft.ns); err != nil {
		t.Fatal(err)
	}

	state, _ := mgr.DeviceState("dev-0000")
	if state != converge.Inflight {
		t.Errorf("state after Tick = %v, want Inflight", state)
	}

	// Verify frame arrived on device side.
	select {
	case <-device.Inbound():
		// Good.
	default:
		t.Error("no frame delivered to device after Tick")
	}
}

func TestManagerTickRespectsEmptyBudget(t *testing.T) {
	ft := &fixedTime{ns: int64(time.Second)}
	clock := hlc.NewClock(1, ft)
	s := memstore.New()
	controller, device := testutil.NewLossyMuxPair(1024, nil)

	mgr := NewManager(ManagerConfig{
		Store:     s,
		Transport: controller,
		Clock:     clock,
		Timeout:   5 * time.Second,
		Budget:    BudgetConfig{MaxTokens: 1, RefillInterval: 10 * time.Second},
		EventSize: 128,
	})
	mgr.Add("dev-0000")

	// First SetDesired + Tick consumes the single budget token.
	mgr.SetDesired("dev-0000", "a", []byte("1"))
	mgr.Tick(ft.ns)

	state, _ := mgr.DeviceState("dev-0000")
	if state != converge.Inflight {
		t.Fatalf("state after first tick = %v, want Inflight", state)
	}

	// Drain device side and simulate reported match → Synced.
	drainAll(device)
	ft.advance(time.Second)
	sendReport(t, device, "dev-0000", "a", []byte("1"), ft.ns)
	mgr.DrainInbound()
	// Tick to detect state change.
	mgr.Tick(ft.ns)

	state, _ = mgr.DeviceState("dev-0000")
	if state != converge.Synced {
		t.Fatalf("state after report = %v, want Synced", state)
	}

	// Second SetDesired. Budget is empty (only 1s elapsed, refill needs 10s).
	mgr.SetDesired("dev-0000", "b", []byte("2"))
	ft.advance(time.Second)
	mgr.Tick(ft.ns)

	state, _ = mgr.DeviceState("dev-0000")
	if state != converge.Pending {
		t.Errorf("state after second tick with empty budget = %v, want Pending", state)
	}

	// Advance time past refill interval, tick again — now it should flush.
	ft.advance(10 * time.Second)
	mgr.Tick(ft.ns)

	state, _ = mgr.DeviceState("dev-0000")
	if state != converge.Inflight {
		t.Errorf("state after refill tick = %v, want Inflight", state)
	}
}

// drainAll reads all pending deliveries from a transport.
func drainAll(tr *testutil.LossyMux) {
	for {
		select {
		case <-tr.Inbound():
		default:
			return
		}
	}
}

// sendReport sends a reported op from the device side back to the controller.
func sendReport(t *testing.T, device *testutil.LossyMux, deviceID shadow.DeviceID, key string, data []byte, nowNs int64) {
	t.Helper()
	op := shadow.Op{
		ID:        shadow.OpID{NodeID: 2, Seq: uint64(nowNs)},
		DeviceID:  deviceID,
		Section:   shadow.SectionReported,
		Key:       key,
		Data:      data,
		Timestamp: hlc.Timestamp{Physical: nowNs, NodeID: 2},
	}
	payload := converge.EncodeOp(op)
	frames, err := converge.Fragment(op.ID, payload, 1024)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	for _, frame := range frames {
		if err := device.Send(transport.Channel(deviceID), frame); err != nil {
			t.Fatalf("device.Send: %v", err)
		}
	}
}

func TestManagerDrainInboundRoutesToDevice(t *testing.T) {
	mgr, device, ft := newTestManager(t, 1)

	if err := mgr.SetDesired("dev-0000", "mode", []byte("auto")); err != nil {
		t.Fatal(err)
	}
	mgr.Tick(ft.ns)

	// Simulate device reporting back: send a reported op through the device
	// transport (which delivers to the controller's inbound).
	ft.advance(time.Second)
	reportOp := shadow.Op{
		ID:        shadow.OpID{NodeID: 2, Seq: 1},
		DeviceID:  "dev-0000",
		Section:   shadow.SectionReported,
		Key:       "mode",
		Data:      []byte("auto"),
		Timestamp: hlc.Timestamp{Physical: ft.ns, NodeID: 2},
	}
	payload := converge.EncodeOp(reportOp)
	frames, err := converge.Fragment(reportOp.ID, payload, 1024)
	if err != nil {
		t.Fatalf("Fragment: %v", err)
	}
	for _, frame := range frames {
		if err := device.Send("dev-0000", frame); err != nil {
			t.Fatalf("device.Send: %v", err)
		}
	}

	// Drain inbound on the manager side.
	mgr.DrainInbound()

	// The device should have converged to Synced.
	state, _ := mgr.DeviceState("dev-0000")
	if state != converge.Synced {
		t.Errorf("state after DrainInbound = %v, want Synced", state)
	}
}

func TestManagerStallReport(t *testing.T) {
	mgr, _, ft := newTestManager(t, 3)

	// All synced initially.
	report := mgr.StallReport()
	if len(report.Stalled) != 0 {
		t.Fatalf("StallReport should be empty initially, got %d", len(report.Stalled))
	}

	// Set desired for two of three.
	mgr.SetDesired("dev-0000", "a", []byte("1"))
	mgr.SetDesired("dev-0001", "a", []byte("1"))

	mgr.Tick(ft.ns)

	report = mgr.StallReport()
	if len(report.Stalled) != 2 {
		t.Errorf("StallReport.Stalled = %d, want 2", len(report.Stalled))
	}

	// Lagging should match.
	lagging := mgr.Lagging()
	if len(lagging) != 2 {
		t.Errorf("Lagging() = %d, want 2", len(lagging))
	}
}

func TestManagerEventsEmitted(t *testing.T) {
	mgr, _, ft := newTestManager(t, 1)

	mgr.SetDesired("dev-0000", "mode", []byte("auto"))
	mgr.Tick(ft.ns)

	// Should have emitted a state change event (Synced → Pending from
	// SetDesired, then Pending → Inflight from Tick/Flush).
	// Actually, events are emitted in Tick phase 4, so we get:
	// Pending → Inflight (the SetDesired transition was before Tick).
	// The event captures the state at Tick time.
	var events []Event
	for {
		select {
		case ev := <-mgr.Events():
			events = append(events, ev)
		default:
			goto done
		}
	}
done:
	if len(events) == 0 {
		t.Fatal("expected at least one event, got none")
	}

	// The last event should show transition to Inflight.
	last := events[len(events)-1]
	if last.DeviceID != "dev-0000" {
		t.Errorf("event.DeviceID = %q, want dev-0000", last.DeviceID)
	}
	if last.To != converge.Inflight {
		t.Errorf("event.To = %v, want Inflight", last.To)
	}
}

func TestManagerDeviceStateUnknown(t *testing.T) {
	mgr, _, _ := newTestManager(t, 0)
	_, ok := mgr.DeviceState("dev-nope")
	if ok {
		t.Error("DeviceState returned true for unknown device")
	}
}
