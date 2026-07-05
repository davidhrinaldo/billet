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

// fixedTime is a TimeSource that always returns the same time, advancing only
// when explicitly set.
type fixedTime struct {
	ns int64
}

func (f *fixedTime) Now() time.Time {
	return time.Unix(0, f.ns)
}

func (f *fixedTime) advance(d time.Duration) {
	f.ns += int64(d)
}

func newTestReconciler(t *testing.T) (*Reconciler, *memstore.MemStore, *testutil.Loopback, *testutil.Loopback, *fixedTime) {
	t.Helper()
	s := memstore.New()
	l := oplog.New(s)
	controller, device := testutil.NewLoopbackPair(1024)
	ft := &fixedTime{ns: 1_000_000_000} // start at 1s
	clock := hlc.NewClock(1, ft)

	r := NewReconciler(ReconcilerConfig{
		DeviceID:  "dev-1",
		Log:       l,
		Store:     s,
		Transport: controller,
		Clock:     clock,
		Timeout:   5 * time.Second,
	})
	return r, s, controller, device, ft
}

func TestReconcilerInitialState(t *testing.T) {
	r, _, _, _, _ := newTestReconciler(t)
	if r.CurrentState() != Synced {
		t.Errorf("initial state = %v, want Synced", r.CurrentState())
	}
}

func TestReconcilerSetDesired(t *testing.T) {
	tests := []struct {
		name      string
		initial   State
		wantState State
		wantErr   error
	}{
		{
			name:      "synced to pending",
			initial:   Synced,
			wantState: Pending,
		},
		{
			name:      "diverged returns error",
			initial:   Diverged,
			wantState: Diverged,
			wantErr:   ErrDiverged,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _, _, _, _ := newTestReconciler(t)
			r.state = tt.initial

			err := r.SetDesired("mode", []byte("auto"))
			if err != tt.wantErr {
				t.Fatalf("SetDesired error = %v, want %v", err, tt.wantErr)
			}
			if r.CurrentState() != tt.wantState {
				t.Errorf("state = %v, want %v", r.CurrentState(), tt.wantState)
			}
		})
	}
}

func TestReconcilerFlush(t *testing.T) {
	tests := []struct {
		name       string
		setDesired bool
		wantState  State
		wantFrames bool
	}{
		{
			name:       "flush with pending ops",
			setDesired: true,
			wantState:  Inflight,
			wantFrames: true,
		},
		{
			name:       "flush with no pending ops is no-op",
			setDesired: false,
			wantState:  Synced,
			wantFrames: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _, _, device, _ := newTestReconciler(t)

			if tt.setDesired {
				if err := r.SetDesired("mode", []byte("auto")); err != nil {
					t.Fatalf("SetDesired: %v", err)
				}
			}

			if err := r.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}

			if r.CurrentState() != tt.wantState {
				t.Errorf("state = %v, want %v", r.CurrentState(), tt.wantState)
			}

			if tt.wantFrames {
				select {
				case d := <-device.Inbound():
					if len(d.Frame) == 0 {
						t.Error("received empty frame")
					}
				default:
					t.Error("expected frame on device transport, got none")
				}
			}
		})
	}
}

func TestReconcilerOnReported(t *testing.T) {
	r, _, _, _, ft := newTestReconciler(t)

	// Set desired, flush.
	if err := r.SetDesired("mode", []byte("auto")); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}
	if err := r.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if r.CurrentState() != Inflight {
		t.Fatalf("state after flush = %v, want Inflight", r.CurrentState())
	}

	// Device reports the matching value.
	ft.advance(1 * time.Second)
	reportedOp := shadow.Op{
		ID:        shadow.OpID{NodeID: 2, Seq: 1},
		DeviceID:  "dev-1",
		Section:   shadow.SectionReported,
		Key:       "mode",
		Data:      []byte("auto"),
		Timestamp: hlc.Timestamp{Physical: ft.ns, NodeID: 2},
	}

	if err := r.OnReported(reportedOp); err != nil {
		t.Fatalf("OnReported: %v", err)
	}
	if r.CurrentState() != Synced {
		t.Errorf("state after matching reported = %v, want Synced", r.CurrentState())
	}
}

func TestReconcilerOnNack(t *testing.T) {
	r, _, _, _, _ := newTestReconciler(t)

	if err := r.SetDesired("mode", []byte("auto")); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}
	if err := r.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	r.OnNack(shadow.OpID{NodeID: 1, Seq: 1})
	if r.CurrentState() != Diverged {
		t.Errorf("state after nack = %v, want Diverged", r.CurrentState())
	}
}

func TestReconcilerReset(t *testing.T) {
	r, _, _, _, _ := newTestReconciler(t)
	r.state = Diverged

	r.Reset()
	if r.CurrentState() != Pending {
		t.Errorf("state after reset = %v, want Pending", r.CurrentState())
	}
}

func TestReconcilerTick(t *testing.T) {
	r, _, _, _, ft := newTestReconciler(t)

	if err := r.SetDesired("temp", []byte("22")); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}
	if err := r.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if r.CurrentState() != Inflight {
		t.Fatalf("state = %v, want Inflight", r.CurrentState())
	}

	// Tick before timeout — no change.
	if err := r.Tick(ft.ns + int64(2*time.Second)); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if r.CurrentState() != Inflight {
		t.Errorf("state after early tick = %v, want Inflight", r.CurrentState())
	}

	// Tick past timeout — transitions to TimedOut then back to Inflight after retry.
	ft.advance(10 * time.Second)
	if err := r.Tick(ft.ns); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	// After retry, state should be Inflight (ops were re-sent).
	if r.CurrentState() != Inflight {
		t.Errorf("state after timeout tick = %v, want Inflight", r.CurrentState())
	}
}

func TestStateString(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{Synced, "Synced"},
		{Pending, "Pending"},
		{Inflight, "Inflight"},
		{TimedOut, "TimedOut"},
		{Diverged, "Diverged"},
		{State(99), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.state.String(); got != tt.want {
				t.Errorf("State(%d).String() = %q, want %q", tt.state, got, tt.want)
			}
		})
	}
}
