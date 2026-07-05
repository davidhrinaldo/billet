package fleet

import "testing"

func TestEventKindString(t *testing.T) {
	tests := []struct {
		kind EventKind
		want string
	}{
		{EventStateChange, "StateChange"},
		{EventConverged, "Converged"},
		{EventDiverged, "Diverged"},
		{EventKind(99), "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.kind.String(); got != tt.want {
				t.Errorf("EventKind(%d).String() = %q, want %q", tt.kind, got, tt.want)
			}
		})
	}
}
