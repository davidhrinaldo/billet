package oplog

import (
	"testing"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store"
	"github.com/davidhrinaldo/billet/store/memstore"
)

func TestAppend(t *testing.T) {
	tests := []struct {
		name    string
		ops     []shadow.Op
		wantErr bool
		wantLen int
	}{
		{
			name: "single op",
			ops: []shadow.Op{
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 100}},
			},
			wantLen: 1,
		},
		{
			name: "duplicate is silently ignored",
			ops: []shadow.Op{
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 100}},
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 100}},
			},
			wantLen: 1,
		},
		{
			name: "multiple distinct ops",
			ops: []shadow.Op{
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 100}},
				{ID: shadow.OpID{NodeID: 1, Seq: 2}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 200}},
			},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := memstore.New()
			l := New(s)

			for _, op := range tt.ops {
				err := l.Append(op)
				if (err != nil) != tt.wantErr {
					t.Fatalf("Append error = %v, wantErr = %v", err, tt.wantErr)
				}
			}

			ops, err := l.Replay("dev-1")
			if err != nil {
				t.Fatalf("Replay: %v", err)
			}
			if len(ops) != tt.wantLen {
				t.Errorf("got %d ops, want %d", len(ops), tt.wantLen)
			}
		})
	}
}

func TestReplay(t *testing.T) {
	tests := []struct {
		name    string
		ops     []shadow.Op
		device  shadow.DeviceID
		wantLen int
	}{
		{
			name:    "empty log",
			device:  "dev-1",
			wantLen: 0,
		},
		{
			name: "filters by device",
			ops: []shadow.Op{
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 100}},
				{ID: shadow.OpID{NodeID: 1, Seq: 2}, DeviceID: "dev-2", Timestamp: hlc.Timestamp{Physical: 200}},
			},
			device:  "dev-1",
			wantLen: 1,
		},
		{
			name: "returns in timestamp order",
			ops: []shadow.Op{
				{ID: shadow.OpID{NodeID: 1, Seq: 2}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 200}},
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 100}},
			},
			device:  "dev-1",
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := memstore.New()
			l := New(s)
			for _, op := range tt.ops {
				if err := l.Append(op); err != nil {
					t.Fatalf("Append: %v", err)
				}
			}

			ops, err := l.Replay(tt.device)
			if err != nil {
				t.Fatalf("Replay: %v", err)
			}
			if len(ops) != tt.wantLen {
				t.Errorf("got %d ops, want %d", len(ops), tt.wantLen)
			}

			// Verify ordering.
			for i := 1; i < len(ops); i++ {
				if ops[i].Timestamp.Before(ops[i-1].Timestamp) {
					t.Errorf("ops[%d] before ops[%d]: %v < %v", i, i-1,
						ops[i].Timestamp, ops[i-1].Timestamp)
				}
			}
		})
	}
}

func TestApply(t *testing.T) {
	tests := []struct {
		name       string
		seedDoc    *shadow.Document
		ops        []shadow.Op
		wantKeys   map[string]string // section:key → expected data
		wantVersion hlc.Timestamp
	}{
		{
			name: "apply to new document",
			ops: []shadow.Op{
				{
					ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1",
					Section: shadow.SectionReported, Key: "temp",
					Data: []byte("22"), Timestamp: hlc.Timestamp{Physical: 100},
				},
			},
			wantKeys:    map[string]string{"reported:temp": "22"},
			wantVersion: hlc.Timestamp{Physical: 100},
		},
		{
			name: "apply desired op",
			ops: []shadow.Op{
				{
					ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1",
					Section: shadow.SectionDesired, Key: "mode",
					Data: []byte("auto"), Timestamp: hlc.Timestamp{Physical: 100},
				},
			},
			wantKeys:    map[string]string{"desired:mode": "auto"},
			wantVersion: hlc.Timestamp{Physical: 100},
		},
		{
			name: "later op overwrites earlier",
			ops: []shadow.Op{
				{
					ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1",
					Section: shadow.SectionReported, Key: "temp",
					Data: []byte("20"), Timestamp: hlc.Timestamp{Physical: 100},
				},
				{
					ID: shadow.OpID{NodeID: 1, Seq: 2}, DeviceID: "dev-1",
					Section: shadow.SectionReported, Key: "temp",
					Data: []byte("25"), Timestamp: hlc.Timestamp{Physical: 200},
				},
			},
			wantKeys:    map[string]string{"reported:temp": "25"},
			wantVersion: hlc.Timestamp{Physical: 200},
		},
		{
			name: "apply on top of existing document",
			seedDoc: &shadow.Document{
				DeviceID: "dev-1",
				Reported: shadow.Section{Values: map[string]shadow.Value{
					"humidity": {Data: []byte("60"), Timestamp: hlc.Timestamp{Physical: 50}},
				}},
				Desired: shadow.Section{Values: make(map[string]shadow.Value)},
				Version: hlc.Timestamp{Physical: 50},
			},
			ops: []shadow.Op{
				{
					ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1",
					Section: shadow.SectionReported, Key: "temp",
					Data: []byte("22"), Timestamp: hlc.Timestamp{Physical: 100},
				},
			},
			wantKeys:    map[string]string{"reported:temp": "22", "reported:humidity": "60"},
			wantVersion: hlc.Timestamp{Physical: 100},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := memstore.New()
			l := New(s)

			if tt.seedDoc != nil {
				if err := s.PutDocument(tt.seedDoc); err != nil {
					t.Fatalf("PutDocument: %v", err)
				}
			}
			for _, op := range tt.ops {
				if err := l.Append(op); err != nil {
					t.Fatalf("Append: %v", err)
				}
			}

			doc, err := l.Apply("dev-1")
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}

			for composite, want := range tt.wantKeys {
				var section shadow.Section
				switch composite[:8] {
				case "reported":
					section = doc.Reported
				case "desired:":
					section = doc.Desired
				}
				key := composite[len("reported:"):]
				if composite[:7] == "desired" {
					key = composite[len("desired:"):]
					section = doc.Desired
				}
				v, ok := section.Values[key]
				if !ok {
					t.Errorf("missing key %q", composite)
					continue
				}
				if string(v.Data) != want {
					t.Errorf("key %q = %q, want %q", composite, v.Data, want)
				}
			}

			if hlc.Compare(doc.Version, tt.wantVersion) != 0 {
				t.Errorf("version = %v, want %v", doc.Version, tt.wantVersion)
			}
		})
	}
}

func TestSnapshot(t *testing.T) {
	tests := []struct {
		name         string
		ops          []shadow.Op
		wantErr      error
		wantReported map[string]string
		wantOpsAfter int
	}{
		{
			name:    "no ops returns ErrNoOps",
			wantErr: ErrNoOps,
		},
		{
			name: "snapshot persists document and truncates ops",
			ops: []shadow.Op{
				{
					ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1",
					Section: shadow.SectionReported, Key: "temp",
					Data: []byte("22"), Timestamp: hlc.Timestamp{Physical: 100},
				},
				{
					ID: shadow.OpID{NodeID: 1, Seq: 2}, DeviceID: "dev-1",
					Section: shadow.SectionReported, Key: "humidity",
					Data: []byte("55"), Timestamp: hlc.Timestamp{Physical: 200},
				},
			},
			wantReported: map[string]string{"temp": "22", "humidity": "55"},
			wantOpsAfter: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := memstore.New()
			l := New(s)
			for _, op := range tt.ops {
				if err := l.Append(op); err != nil {
					t.Fatalf("Append: %v", err)
				}
			}

			doc, err := l.Snapshot("dev-1")
			if err != tt.wantErr {
				t.Fatalf("Snapshot error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}

			for key, want := range tt.wantReported {
				v, ok := doc.Reported.Values[key]
				if !ok {
					t.Errorf("missing reported key %q", key)
					continue
				}
				if string(v.Data) != want {
					t.Errorf("reported[%q] = %q, want %q", key, v.Data, want)
				}
			}

			// Verify document was persisted.
			stored, err := s.GetDocument("dev-1")
			if err != nil {
				t.Fatalf("GetDocument after snapshot: %v", err)
			}
			if stored.DeviceID != "dev-1" {
				t.Errorf("stored DeviceID = %q, want dev-1", stored.DeviceID)
			}

			// Verify ops were truncated.
			ops, err := l.Replay("dev-1")
			if err != nil {
				t.Fatalf("Replay after snapshot: %v", err)
			}
			if len(ops) != tt.wantOpsAfter {
				t.Errorf("ops after snapshot = %d, want %d", len(ops), tt.wantOpsAfter)
			}
		})
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		name        string
		ops         []shadow.Op
		before      hlc.Timestamp
		wantRemoved int
		wantLeft    int
	}{
		{
			name:        "empty log",
			before:      hlc.Timestamp{Physical: 100},
			wantRemoved: 0,
			wantLeft:    0,
		},
		{
			name: "partial truncate",
			ops: []shadow.Op{
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 100}},
				{ID: shadow.OpID{NodeID: 1, Seq: 2}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 200}},
				{ID: shadow.OpID{NodeID: 1, Seq: 3}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 300}},
			},
			before:      hlc.Timestamp{Physical: 250},
			wantRemoved: 2,
			wantLeft:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := memstore.New()
			l := New(s)
			for _, op := range tt.ops {
				if err := l.Append(op); err != nil {
					t.Fatalf("Append: %v", err)
				}
			}

			removed, err := l.Truncate("dev-1", tt.before)
			if err != nil {
				t.Fatalf("Truncate: %v", err)
			}
			if removed != tt.wantRemoved {
				t.Errorf("removed = %d, want %d", removed, tt.wantRemoved)
			}

			ops, err := l.Replay("dev-1")
			if err != nil {
				t.Fatalf("Replay: %v", err)
			}
			if len(ops) != tt.wantLeft {
				t.Errorf("ops left = %d, want %d", len(ops), tt.wantLeft)
			}
		})
	}
}

// Verify the interface is satisfied at compile time.
var _ store.Store = (*memstore.MemStore)(nil)
