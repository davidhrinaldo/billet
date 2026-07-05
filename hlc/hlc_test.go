package hlc

import (
	"testing"
	"time"
)

func TestTimestampCompare(t *testing.T) {
	tests := []struct {
		name string
		a, b Timestamp
		want int
	}{
		{
			name: "equal timestamps",
			a:    Timestamp{Physical: 100, Logical: 1, NodeID: 1},
			b:    Timestamp{Physical: 100, Logical: 1, NodeID: 1},
			want: 0,
		},
		{
			name: "a has greater physical",
			a:    Timestamp{Physical: 200, Logical: 0, NodeID: 1},
			b:    Timestamp{Physical: 100, Logical: 5, NodeID: 1},
			want: 1,
		},
		{
			name: "b has greater physical",
			a:    Timestamp{Physical: 100, Logical: 5, NodeID: 1},
			b:    Timestamp{Physical: 200, Logical: 0, NodeID: 1},
			want: -1,
		},
		{
			name: "same physical a has greater logical",
			a:    Timestamp{Physical: 100, Logical: 5, NodeID: 1},
			b:    Timestamp{Physical: 100, Logical: 3, NodeID: 1},
			want: 1,
		},
		{
			name: "same physical b has greater logical",
			a:    Timestamp{Physical: 100, Logical: 3, NodeID: 1},
			b:    Timestamp{Physical: 100, Logical: 5, NodeID: 1},
			want: -1,
		},
		{
			name: "same physical and logical different node breaks tie by node",
			a:    Timestamp{Physical: 100, Logical: 3, NodeID: 2},
			b:    Timestamp{Physical: 100, Logical: 3, NodeID: 1},
			want: 1,
		},
		{
			name: "zero timestamps are equal",
			a:    Timestamp{},
			b:    Timestamp{},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Compare(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("Compare(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestTimestampBefore(t *testing.T) {
	tests := []struct {
		name string
		a, b Timestamp
		want bool
	}{
		{
			name: "a before b",
			a:    Timestamp{Physical: 100, Logical: 0, NodeID: 1},
			b:    Timestamp{Physical: 200, Logical: 0, NodeID: 1},
			want: true,
		},
		{
			name: "a equal b",
			a:    Timestamp{Physical: 100, Logical: 0, NodeID: 1},
			b:    Timestamp{Physical: 100, Logical: 0, NodeID: 1},
			want: false,
		},
		{
			name: "a after b",
			a:    Timestamp{Physical: 200, Logical: 0, NodeID: 1},
			b:    Timestamp{Physical: 100, Logical: 0, NodeID: 1},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Before(tt.b)
			if got != tt.want {
				t.Errorf("%v.Before(%v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// fakeClock is a controllable time source for testing.
type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time { return f.now }
func (f *fakeClock) advance(d time.Duration) { f.now = f.now.Add(d) }

func TestClockNow_Monotonicity(t *testing.T) {
	fc := &fakeClock{now: time.Unix(0, 1000)}
	c := NewClock(1, fc)

	tests := []struct {
		name        string
		advance     time.Duration
		wantPhysGT  int64 // physical must be > this (use -1 to skip)
		wantLogical uint16
	}{
		{
			name:        "first tick",
			advance:     0,
			wantPhysGT:  -1,
			wantLogical: 0,
		},
		{
			name:        "same wall time increments logical",
			advance:     0,
			wantPhysGT:  -1,
			wantLogical: 1,
		},
		{
			name:        "same wall time increments logical again",
			advance:     0,
			wantPhysGT:  -1,
			wantLogical: 2,
		},
		{
			name:        "advancing wall resets logical",
			advance:     time.Millisecond,
			wantPhysGT:  1000,
			wantLogical: 0,
		},
	}

	var prev Timestamp
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc.advance(tt.advance)
			ts := c.Now()

			// Monotonicity: every new timestamp must be > previous.
			if prev != (Timestamp{}) && !prev.Before(ts) {
				t.Errorf("monotonicity violated: prev=%v >= now=%v", prev, ts)
			}

			if tt.wantPhysGT >= 0 && ts.Physical <= tt.wantPhysGT {
				t.Errorf("physical=%d, want > %d", ts.Physical, tt.wantPhysGT)
			}

			if ts.Logical != tt.wantLogical {
				t.Errorf("logical=%d, want %d", ts.Logical, tt.wantLogical)
			}

			if ts.NodeID != 1 {
				t.Errorf("nodeID=%d, want 1", ts.NodeID)
			}

			prev = ts
		})
	}
}

func TestClockNow_WallClockRegression(t *testing.T) {
	// Simulates wall clock going backward (e.g., NTP correction).
	// HLC must not go backward.
	fc := &fakeClock{now: time.Unix(0, 5000)}
	c := NewClock(1, fc)

	ts1 := c.Now()

	// Wall clock regresses.
	fc.now = time.Unix(0, 3000)
	ts2 := c.Now()

	if !ts1.Before(ts2) {
		t.Errorf("clock went backward: ts1=%v, ts2=%v", ts1, ts2)
	}

	// Physical should not regress.
	if ts2.Physical < ts1.Physical {
		t.Errorf("physical regressed: %d < %d", ts2.Physical, ts1.Physical)
	}
}

func TestClockUpdate_RemoteAhead(t *testing.T) {
	fc := &fakeClock{now: time.Unix(0, 1000)}
	c := NewClock(1, fc)

	tests := []struct {
		name       string
		remote     Timestamp
		wantPhysGE int64
		wantLogGT  uint16 // result logical must be > remote logical when physical matches
	}{
		{
			name:       "remote physical far ahead",
			remote:     Timestamp{Physical: 9000, Logical: 5, NodeID: 2},
			wantPhysGE: 9000,
		},
		{
			name:       "remote same physical as local max",
			remote:     Timestamp{Physical: 9000, Logical: 10, NodeID: 2},
			wantPhysGE: 9000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := c.Update(tt.remote)

			if ts.Physical < tt.wantPhysGE {
				t.Errorf("physical=%d, want >= %d", ts.Physical, tt.wantPhysGE)
			}

			// Must be strictly after the remote.
			if !tt.remote.Before(ts) {
				t.Errorf("result %v is not after remote %v", ts, tt.remote)
			}

			if ts.NodeID != 1 {
				t.Errorf("nodeID=%d, want 1", ts.NodeID)
			}
		})
	}
}

func TestClockUpdate_RemoteBehind(t *testing.T) {
	// When remote is behind local, Update still advances local monotonically.
	fc := &fakeClock{now: time.Unix(0, 5000)}
	c := NewClock(1, fc)

	ts1 := c.Now()

	remote := Timestamp{Physical: 1000, Logical: 0, NodeID: 2}
	ts2 := c.Update(remote)

	if !ts1.Before(ts2) {
		t.Errorf("Update with stale remote did not advance: ts1=%v, ts2=%v", ts1, ts2)
	}
}

func TestClockUpdate_CausalityAcrossNodes(t *testing.T) {
	// Two clocks with skewed physical times maintain causality through Update.
	fc1 := &fakeClock{now: time.Unix(0, 1000)}
	fc2 := &fakeClock{now: time.Unix(0, 5000)}

	c1 := NewClock(1, fc1)
	c2 := NewClock(2, fc2)

	// c2 is ahead. It generates a timestamp.
	send := c2.Now()

	// c1 receives. Its wall clock is behind, but it must produce a timestamp after send.
	recv := c1.Update(send)

	if !send.Before(recv) {
		t.Errorf("causality violated: send=%v not before recv=%v", send, recv)
	}

	// c1 sends back to c2.
	reply := c2.Update(recv)

	if !recv.Before(reply) {
		t.Errorf("causality violated: recv=%v not before reply=%v", recv, reply)
	}
}

func TestClockLogicalOverflow(t *testing.T) {
	// If logical counter would overflow uint16, Now must return an error or panic.
	// We choose to return an error via a second return value.
	fc := &fakeClock{now: time.Unix(0, 1000)}
	c := NewClock(1, fc)

	// Drive logical to max.
	c.setStateForTest(1000, maxLogical, 1)

	_, err := c.NowE()
	if err == nil {
		t.Error("expected error on logical overflow, got nil")
	}
}
