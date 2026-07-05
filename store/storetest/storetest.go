// Package storetest provides a conformance test suite for store.Store
// implementations. Call Suite(t, factory) from your implementation's test file.
package storetest

import (
	"testing"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store"
)

// Factory creates a fresh Store for each test case and returns an optional
// cleanup function.
type Factory func(t *testing.T) (store.Store, func())

// Suite runs the full conformance suite against a Store implementation.
func Suite(t *testing.T, f Factory) {
	t.Helper()
	t.Run("GetDocument", func(t *testing.T) { testGetDocument(t, f) })
	t.Run("PutDocumentOverwrites", func(t *testing.T) { testPutDocumentOverwrites(t, f) })
	t.Run("AppendOp", func(t *testing.T) { testAppendOp(t, f) })
	t.Run("ListOps", func(t *testing.T) { testListOps(t, f) })
	t.Run("TruncateOps", func(t *testing.T) { testTruncateOps(t, f) })
	t.Run("HasOp", func(t *testing.T) { testHasOp(t, f) })
}

func testGetDocument(t *testing.T, f Factory) {
	tests := []struct {
		name     string
		seed     []*shadow.Document
		queryID  shadow.DeviceID
		wantErr  error
		wantData bool
	}{
		{
			name:    "not found in empty store",
			seed:    nil,
			queryID: "dev-1",
			wantErr: store.ErrNotFound,
		},
		{
			name: "not found when other devices exist",
			seed: []*shadow.Document{
				{DeviceID: "dev-2"},
			},
			queryID: "dev-1",
			wantErr: store.ErrNotFound,
		},
		{
			name: "found after put",
			seed: []*shadow.Document{
				{DeviceID: "dev-1", Reported: shadow.Section{Values: map[string]shadow.Value{
					"temp": {Data: []byte("22")},
				}}},
			},
			queryID:  "dev-1",
			wantErr:  nil,
			wantData: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, cleanup := f(t)
			defer cleanup()

			for _, doc := range tt.seed {
				if err := s.PutDocument(doc); err != nil {
					t.Fatalf("seed PutDocument: %v", err)
				}
			}

			got, err := s.GetDocument(tt.queryID)
			if err != tt.wantErr {
				t.Fatalf("GetDocument(%q) error = %v, want %v", tt.queryID, err, tt.wantErr)
			}
			if tt.wantData && got == nil {
				t.Fatal("expected document, got nil")
			}
			if tt.wantData && got.DeviceID != tt.queryID {
				t.Errorf("got DeviceID = %q, want %q", got.DeviceID, tt.queryID)
			}
		})
	}
}

func testPutDocumentOverwrites(t *testing.T, f Factory) {
	s, cleanup := f(t)
	defer cleanup()

	doc1 := &shadow.Document{
		DeviceID: "dev-1",
		Reported: shadow.Section{Values: map[string]shadow.Value{
			"temp": {Data: []byte("20")},
		}},
	}
	doc2 := &shadow.Document{
		DeviceID: "dev-1",
		Reported: shadow.Section{Values: map[string]shadow.Value{
			"temp": {Data: []byte("25")},
		}},
	}

	if err := s.PutDocument(doc1); err != nil {
		t.Fatalf("PutDocument: %v", err)
	}
	if err := s.PutDocument(doc2); err != nil {
		t.Fatalf("PutDocument: %v", err)
	}

	got, err := s.GetDocument("dev-1")
	if err != nil {
		t.Fatalf("GetDocument: %v", err)
	}
	if string(got.Reported.Values["temp"].Data) != "25" {
		t.Errorf("expected overwritten value '25', got %q", got.Reported.Values["temp"].Data)
	}
}

func testAppendOp(t *testing.T, f Factory) {
	tests := []struct {
		name    string
		ops     []shadow.Op
		wantErr error
	}{
		{
			name: "append single op",
			ops: []shadow.Op{
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 100}},
			},
			wantErr: nil,
		},
		{
			name: "duplicate op returns ErrOpExists",
			ops: []shadow.Op{
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 100}},
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 200}},
			},
			wantErr: store.ErrOpExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, cleanup := f(t)
			defer cleanup()

			var lastErr error
			for _, op := range tt.ops {
				lastErr = s.AppendOp(op)
			}
			if lastErr != tt.wantErr {
				t.Errorf("last AppendOp error = %v, want %v", lastErr, tt.wantErr)
			}
		})
	}
}

func testListOps(t *testing.T, f Factory) {
	tests := []struct {
		name    string
		ops     []shadow.Op
		queryID shadow.DeviceID
		wantLen int
	}{
		{
			name:    "empty store returns empty slice",
			ops:     nil,
			queryID: "dev-1",
			wantLen: 0,
		},
		{
			name: "returns only ops for queried device",
			ops: []shadow.Op{
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 100}},
				{ID: shadow.OpID{NodeID: 1, Seq: 2}, DeviceID: "dev-2", Timestamp: hlc.Timestamp{Physical: 200}},
				{ID: shadow.OpID{NodeID: 1, Seq: 3}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 300}},
			},
			queryID: "dev-1",
			wantLen: 2,
		},
		{
			name: "returns ops in timestamp order",
			ops: []shadow.Op{
				{ID: shadow.OpID{NodeID: 1, Seq: 3}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 300}},
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 100}},
				{ID: shadow.OpID{NodeID: 1, Seq: 2}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 200}},
			},
			queryID: "dev-1",
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, cleanup := f(t)
			defer cleanup()

			for _, op := range tt.ops {
				if err := s.AppendOp(op); err != nil {
					t.Fatalf("AppendOp: %v", err)
				}
			}

			ops, err := s.ListOps(tt.queryID)
			if err != nil {
				t.Fatalf("ListOps: %v", err)
			}
			if len(ops) != tt.wantLen {
				t.Fatalf("got %d ops, want %d", len(ops), tt.wantLen)
			}

			// Verify timestamp ordering.
			for i := 1; i < len(ops); i++ {
				if ops[i].Timestamp.Before(ops[i-1].Timestamp) {
					t.Errorf("ops not in order: [%d].Timestamp=%v < [%d].Timestamp=%v",
						i, ops[i].Timestamp, i-1, ops[i-1].Timestamp)
				}
			}
		})
	}
}

func testTruncateOps(t *testing.T, f Factory) {
	allOps := []shadow.Op{
		{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 100}},
		{ID: shadow.OpID{NodeID: 1, Seq: 2}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 200}},
		{ID: shadow.OpID{NodeID: 1, Seq: 3}, DeviceID: "dev-1", Timestamp: hlc.Timestamp{Physical: 300}},
		{ID: shadow.OpID{NodeID: 1, Seq: 4}, DeviceID: "dev-2", Timestamp: hlc.Timestamp{Physical: 150}},
	}

	tests := []struct {
		name        string
		device      shadow.DeviceID
		before      hlc.Timestamp
		wantRemoved int
		wantDev1Ops int
		wantDev2Ops int
		wantHasSeq1 bool
		wantHasSeq2 bool
	}{
		{
			name:        "truncate none when cutoff is before all ops",
			device:      "dev-1",
			before:      hlc.Timestamp{Physical: 50},
			wantRemoved: 0,
			wantDev1Ops: 3,
			wantDev2Ops: 1,
			wantHasSeq1: true,
			wantHasSeq2: true,
		},
		{
			name:        "truncate first two ops",
			device:      "dev-1",
			before:      hlc.Timestamp{Physical: 300},
			wantRemoved: 2,
			wantDev1Ops: 1,
			wantDev2Ops: 1,
			wantHasSeq1: false,
			wantHasSeq2: false,
		},
		{
			name:        "truncate does not affect other devices",
			device:      "dev-1",
			before:      hlc.Timestamp{Physical: 500},
			wantRemoved: 3,
			wantDev1Ops: 0,
			wantDev2Ops: 1,
			wantHasSeq1: false,
			wantHasSeq2: false,
		},
		{
			name:        "truncate nonexistent device removes nothing",
			device:      "dev-99",
			before:      hlc.Timestamp{Physical: 500},
			wantRemoved: 0,
			wantDev1Ops: 3,
			wantDev2Ops: 1,
			wantHasSeq1: true,
			wantHasSeq2: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, cleanup := f(t)
			defer cleanup()

			for _, op := range allOps {
				if err := s.AppendOp(op); err != nil {
					t.Fatalf("AppendOp: %v", err)
				}
			}

			removed, err := s.TruncateOps(tt.device, tt.before)
			if err != nil {
				t.Fatalf("TruncateOps: %v", err)
			}
			if removed != tt.wantRemoved {
				t.Errorf("removed = %d, want %d", removed, tt.wantRemoved)
			}

			dev1Ops, err := s.ListOps("dev-1")
			if err != nil {
				t.Fatalf("ListOps(dev-1): %v", err)
			}
			if len(dev1Ops) != tt.wantDev1Ops {
				t.Errorf("dev-1 ops = %d, want %d", len(dev1Ops), tt.wantDev1Ops)
			}

			dev2Ops, err := s.ListOps("dev-2")
			if err != nil {
				t.Fatalf("ListOps(dev-2): %v", err)
			}
			if len(dev2Ops) != tt.wantDev2Ops {
				t.Errorf("dev-2 ops = %d, want %d", len(dev2Ops), tt.wantDev2Ops)
			}

			hasSeq1, _ := s.HasOp(shadow.OpID{NodeID: 1, Seq: 1})
			if hasSeq1 != tt.wantHasSeq1 {
				t.Errorf("HasOp(seq=1) = %v, want %v", hasSeq1, tt.wantHasSeq1)
			}
			hasSeq2, _ := s.HasOp(shadow.OpID{NodeID: 1, Seq: 2})
			if hasSeq2 != tt.wantHasSeq2 {
				t.Errorf("HasOp(seq=2) = %v, want %v", hasSeq2, tt.wantHasSeq2)
			}
		})
	}
}

func testHasOp(t *testing.T, f Factory) {
	tests := []struct {
		name    string
		ops     []shadow.Op
		queryID shadow.OpID
		want    bool
	}{
		{
			name:    "not found in empty store",
			ops:     nil,
			queryID: shadow.OpID{NodeID: 1, Seq: 1},
			want:    false,
		},
		{
			name: "found after append",
			ops: []shadow.Op{
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1"},
			},
			queryID: shadow.OpID{NodeID: 1, Seq: 1},
			want:    true,
		},
		{
			name: "different seq not found",
			ops: []shadow.Op{
				{ID: shadow.OpID{NodeID: 1, Seq: 1}, DeviceID: "dev-1"},
			},
			queryID: shadow.OpID{NodeID: 1, Seq: 2},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, cleanup := f(t)
			defer cleanup()

			for _, op := range tt.ops {
				_ = s.AppendOp(op)
			}

			got, err := s.HasOp(tt.queryID)
			if err != nil {
				t.Fatalf("HasOp: %v", err)
			}
			if got != tt.want {
				t.Errorf("HasOp(%v) = %v, want %v", tt.queryID, got, tt.want)
			}
		})
	}
}
