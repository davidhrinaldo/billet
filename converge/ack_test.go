package converge

import (
	"testing"
	"time"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
)

func TestAckTrackerMarkSent(t *testing.T) {
	tests := []struct {
		name         string
		sends        []shadow.OpID
		wantPending  int
		wantAttempts int // attempts for the first OpID
	}{
		{
			name:         "single send",
			sends:        []shadow.OpID{{NodeID: 1, Seq: 1}},
			wantPending:  1,
			wantAttempts: 1,
		},
		{
			name:         "two distinct ops",
			sends:        []shadow.OpID{{NodeID: 1, Seq: 1}, {NodeID: 1, Seq: 2}},
			wantPending:  2,
			wantAttempts: 1,
		},
		{
			name:         "resend increments attempts",
			sends:        []shadow.OpID{{NodeID: 1, Seq: 1}, {NodeID: 1, Seq: 1}},
			wantPending:  1,
			wantAttempts: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := NewAckTracker()
			for i, id := range tt.sends {
				tracker.MarkSent(id, hlc.Timestamp{Physical: int64(i * 100)})
			}

			if tracker.Pending() != tt.wantPending {
				t.Errorf("Pending = %d, want %d", tracker.Pending(), tt.wantPending)
			}

			state, ok := tracker.Get(tt.sends[0])
			if !ok {
				t.Fatal("expected op to be tracked")
			}
			if state.Attempts != tt.wantAttempts {
				t.Errorf("Attempts = %d, want %d", state.Attempts, tt.wantAttempts)
			}
		})
	}
}

func TestAckTrackerAck(t *testing.T) {
	tests := []struct {
		name        string
		send        []shadow.OpID
		ack         []shadow.OpID
		wantPending int
	}{
		{
			name:        "ack removes from tracking",
			send:        []shadow.OpID{{NodeID: 1, Seq: 1}},
			ack:         []shadow.OpID{{NodeID: 1, Seq: 1}},
			wantPending: 0,
		},
		{
			name:        "ack unknown op is harmless",
			send:        []shadow.OpID{{NodeID: 1, Seq: 1}},
			ack:         []shadow.OpID{{NodeID: 1, Seq: 99}},
			wantPending: 1,
		},
		{
			name:        "partial ack",
			send:        []shadow.OpID{{NodeID: 1, Seq: 1}, {NodeID: 1, Seq: 2}},
			ack:         []shadow.OpID{{NodeID: 1, Seq: 1}},
			wantPending: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := NewAckTracker()
			for _, id := range tt.send {
				tracker.MarkSent(id, hlc.Timestamp{Physical: 100})
			}
			for _, id := range tt.ack {
				tracker.Ack(id)
			}
			if tracker.Pending() != tt.wantPending {
				t.Errorf("Pending = %d, want %d", tracker.Pending(), tt.wantPending)
			}
		})
	}
}

func TestAckTrackerTimedOut(t *testing.T) {
	tests := []struct {
		name       string
		sentAt     int64 // physical nanoseconds
		now        int64 // physical nanoseconds
		timeout    time.Duration
		wantCount  int
	}{
		{
			name:      "not timed out yet",
			sentAt:    1000,
			now:       1500,
			timeout:   1 * time.Second,
			wantCount: 0,
		},
		{
			name:      "exactly at timeout boundary",
			sentAt:    1000,
			now:       1000 + int64(time.Second),
			timeout:   1 * time.Second,
			wantCount: 1,
		},
		{
			name:      "well past timeout",
			sentAt:    1000,
			now:       1000 + int64(10*time.Second),
			timeout:   1 * time.Second,
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := NewAckTracker()
			tracker.MarkSent(shadow.OpID{NodeID: 1, Seq: 1}, hlc.Timestamp{Physical: tt.sentAt})

			timedOut := tracker.TimedOut(tt.now, tt.timeout)
			if len(timedOut) != tt.wantCount {
				t.Errorf("TimedOut count = %d, want %d", len(timedOut), tt.wantCount)
			}
		})
	}
}

func TestAckTrackerNack(t *testing.T) {
	tracker := NewAckTracker()
	id := shadow.OpID{NodeID: 1, Seq: 1}

	tracker.MarkSent(id, hlc.Timestamp{Physical: 100})
	tracker.Nack(id)

	// Op should still be tracked after nack.
	if tracker.Pending() != 1 {
		t.Errorf("Pending after Nack = %d, want 1", tracker.Pending())
	}

	state, ok := tracker.Get(id)
	if !ok {
		t.Fatal("expected op to still be tracked after Nack")
	}
	if state.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1 (Nack should not change attempts)", state.Attempts)
	}
}
