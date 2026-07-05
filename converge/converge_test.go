package converge

import (
	"testing"
	"time"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/internal/testutil"
	"github.com/davidhrinaldo/billet/oplog"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store/memstore"
)

// TestIntegrationOfflineReconnectConverge exercises the full M2 cycle:
//
//  1. Controller sets desired state.
//  2. Reconciler flushes ops over loopback → Inflight.
//  3. Device goes offline (ack not delivered) → Tick → TimedOut → retry → Inflight.
//  4. Device reconnects, reports matching state → Synced.
//  5. Document in store has reported == desired, delta empty.
func TestIntegrationOfflineReconnectConverge(t *testing.T) {
	// --- Setup ---
	s := memstore.New()
	log := oplog.New(s)
	controller, device := testutil.NewLoopbackPair(256)
	ft := &fixedTime{ns: 1_000_000_000}
	clock := hlc.NewClock(1, ft)

	r := NewReconciler(ReconcilerConfig{
		DeviceID:  "dev-1",
		Log:       log,
		Store:     s,
		Transport: controller,
		Clock:     clock,
		Timeout:   5 * time.Second,
	})

	// --- Step 1: Set desired state ---
	if err := r.SetDesired("mode", []byte("auto")); err != nil {
		t.Fatalf("SetDesired(mode): %v", err)
	}
	if err := r.SetDesired("interval", []byte("60")); err != nil {
		t.Fatalf("SetDesired(interval): %v", err)
	}
	assertState(t, r, Pending, "after SetDesired")

	// --- Step 2: Flush → Inflight ---
	if err := r.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	assertState(t, r, Inflight, "after Flush")

	// Drain device-side frames (the device would receive these).
	drainFrames(t, device)

	// --- Step 3: Device goes offline — no ack arrives ---
	// Advance time past the timeout.
	ft.advance(10 * time.Second)

	// Tick detects timeout, re-sends, transitions to Inflight.
	if err := r.Tick(ft.ns); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	assertState(t, r, Inflight, "after timeout retry")

	// Drain the retried frames on the device side.
	drainFrames(t, device)

	// --- Step 4: Device reconnects, reports matching state ---
	ft.advance(1 * time.Second)
	for _, kv := range []struct{ k, v string }{
		{"mode", "auto"},
		{"interval", "60"},
	} {
		reportOp := shadow.Op{
			ID:        shadow.OpID{NodeID: 2, Seq: uint64(ft.ns)},
			DeviceID:  "dev-1",
			Section:   shadow.SectionReported,
			Key:       kv.k,
			Data:      []byte(kv.v),
			Timestamp: hlc.Timestamp{Physical: ft.ns, NodeID: 2},
		}
		if err := r.OnReported(reportOp); err != nil {
			t.Fatalf("OnReported(%s): %v", kv.k, err)
		}
		ft.advance(1 * time.Millisecond)
	}

	assertState(t, r, Synced, "after device reports matching state")

	// --- Step 5: Verify document state ---
	doc, err := s.GetDocument("dev-1")
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}

	delta := doc.Delta()
	if !delta.IsEmpty() {
		t.Errorf("expected empty delta, got %d diffs", len(delta.Diffs))
	}

	// Verify reported values match desired.
	for _, key := range []string{"mode", "interval"} {
		desired, ok := doc.Desired.Values[key]
		if !ok {
			t.Errorf("missing desired key %q", key)
			continue
		}
		reported, ok := doc.Reported.Values[key]
		if !ok {
			t.Errorf("missing reported key %q", key)
			continue
		}
		if string(desired.Data) != string(reported.Data) {
			t.Errorf("key %q: desired=%q, reported=%q", key, desired.Data, reported.Data)
		}
	}
}

// TestIntegrationNackAndReset exercises the diverged→reset→converge path.
func TestIntegrationNackAndReset(t *testing.T) {
	s := memstore.New()
	log := oplog.New(s)
	controller, device := testutil.NewLoopbackPair(256)
	ft := &fixedTime{ns: 1_000_000_000}
	clock := hlc.NewClock(1, ft)

	r := NewReconciler(ReconcilerConfig{
		DeviceID:  "dev-1",
		Log:       log,
		Store:     s,
		Transport: controller,
		Clock:     clock,
		Timeout:   5 * time.Second,
	})

	// Set desired, flush.
	if err := r.SetDesired("fw", []byte("2.0")); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}
	if err := r.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	drainFrames(t, device)
	assertState(t, r, Inflight, "after Flush")

	// Device nacks.
	r.OnNack(shadow.OpID{NodeID: 1, Seq: 1})
	assertState(t, r, Diverged, "after Nack")

	// SetDesired while diverged fails.
	err := r.SetDesired("fw", []byte("2.1"))
	if err != ErrDiverged {
		t.Fatalf("SetDesired on Diverged = %v, want ErrDiverged", err)
	}

	// Reset → Pending.
	r.Reset()
	assertState(t, r, Pending, "after Reset")

	// Flush again with corrected desired.
	ft.advance(1 * time.Second)
	if err := r.SetDesired("fw", []byte("2.1")); err != nil {
		t.Fatalf("SetDesired after reset: %v", err)
	}
	if err := r.Flush(); err != nil {
		t.Fatalf("Flush after reset: %v", err)
	}
	drainFrames(t, device)
	assertState(t, r, Inflight, "after re-Flush")

	// Device reports success.
	ft.advance(1 * time.Second)
	if err := r.OnReported(shadow.Op{
		ID:        shadow.OpID{NodeID: 2, Seq: 100},
		DeviceID:  "dev-1",
		Section:   shadow.SectionReported,
		Key:       "fw",
		Data:      []byte("2.1"),
		Timestamp: hlc.Timestamp{Physical: ft.ns, NodeID: 2},
	}); err != nil {
		t.Fatalf("OnReported: %v", err)
	}
	assertState(t, r, Synced, "after reported matches desired")
}

// TestIntegrationMultipleDesiredUpdatesBeforeFlush verifies that setting
// desired multiple times before flushing sends the latest value.
func TestIntegrationMultipleDesiredUpdatesBeforeFlush(t *testing.T) {
	s := memstore.New()
	log := oplog.New(s)
	controller, device := testutil.NewLoopbackPair(256)
	ft := &fixedTime{ns: 1_000_000_000}
	clock := hlc.NewClock(1, ft)

	r := NewReconciler(ReconcilerConfig{
		DeviceID:  "dev-1",
		Log:       log,
		Store:     s,
		Transport: controller,
		Clock:     clock,
		Timeout:   5 * time.Second,
	})

	// Set desired twice for the same key.
	ft.advance(1 * time.Millisecond)
	if err := r.SetDesired("temp", []byte("20")); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}
	ft.advance(1 * time.Millisecond)
	if err := r.SetDesired("temp", []byte("25")); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}

	if err := r.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	drainFrames(t, device)

	// Verify the desired value in the store is the latest.
	doc, err := s.GetDocument("dev-1")
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if string(doc.Desired.Values["temp"].Data) != "25" {
		t.Errorf("desired temp = %q, want 25", doc.Desired.Values["temp"].Data)
	}
}

func assertState(t *testing.T, r *Reconciler, want State, context string) {
	t.Helper()
	if r.CurrentState() != want {
		t.Errorf("%s: state = %v, want %v", context, r.CurrentState(), want)
	}
}

func drainFrames(t *testing.T, lb *testutil.Loopback) {
	t.Helper()
	for {
		select {
		case <-lb.Inbound():
		default:
			return
		}
	}
}
