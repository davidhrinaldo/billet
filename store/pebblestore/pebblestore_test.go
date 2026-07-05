package pebblestore

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store"
	"github.com/davidhrinaldo/billet/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Suite(t, func(t *testing.T) (store.Store, func()) {
		t.Helper()
		s, err := Open("", &pebble.Options{FS: vfs.NewMem()})
		if err != nil {
			t.Fatalf("open pebblestore: %v", err)
		}
		return s, func() { s.Close() }
	})
}

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	// Open, write, close.
	s, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	doc := &shadow.Document{
		DeviceID: "dev-1",
		Reported: shadow.Section{Values: map[string]shadow.Value{
			"temp": {Data: []byte("22")},
		}},
	}
	if err := s.PutDocument(doc); err != nil {
		t.Fatalf("PutDocument: %v", err)
	}

	op := shadow.Op{
		ID:        shadow.OpID{NodeID: 1, Seq: 1},
		DeviceID:  "dev-1",
		Section:   shadow.SectionReported,
		Key:       "temp",
		Data:      []byte("22"),
		Timestamp: hlc.Timestamp{Physical: 100},
	}
	if err := s.AppendOp(op); err != nil {
		t.Fatalf("AppendOp: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and verify.
	s2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	got, err := s2.GetDocument("dev-1")
	if err != nil {
		t.Fatalf("GetDocument after reopen: %v", err)
	}
	if string(got.Reported.Values["temp"].Data) != "22" {
		t.Errorf("expected value '22', got %q", got.Reported.Values["temp"].Data)
	}

	ops, err := s2.ListOps("dev-1")
	if err != nil {
		t.Fatalf("ListOps after reopen: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected 1 op after reopen, got %d", len(ops))
	}

	has, err := s2.HasOp(shadow.OpID{NodeID: 1, Seq: 1})
	if err != nil {
		t.Fatalf("HasOp after reopen: %v", err)
	}
	if !has {
		t.Error("expected HasOp=true after reopen")
	}
}

func TestLargeKeySpace(t *testing.T) {
	s, err := Open("", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// Write ops across many devices.
	const numDevices = 100
	const opsPerDevice = 50
	for d := range numDevices {
		deviceID := shadow.DeviceID(fmt.Sprintf("dev-%04d", d))
		for seq := range opsPerDevice {
			op := shadow.Op{
				ID:        shadow.OpID{NodeID: 1, Seq: uint64(d*opsPerDevice + seq)},
				DeviceID:  deviceID,
				Section:   shadow.SectionReported,
				Key:       "temp",
				Data:      []byte("42"),
				Timestamp: hlc.Timestamp{Physical: int64(seq * 1000)},
			}
			if err := s.AppendOp(op); err != nil {
				t.Fatalf("AppendOp device=%s seq=%d: %v", deviceID, seq, err)
			}
		}
	}

	// Verify each device sees only its own ops, in order.
	for d := range numDevices {
		deviceID := shadow.DeviceID(fmt.Sprintf("dev-%04d", d))
		ops, err := s.ListOps(deviceID)
		if err != nil {
			t.Fatalf("ListOps(%s): %v", deviceID, err)
		}
		if len(ops) != opsPerDevice {
			t.Errorf("device %s: got %d ops, want %d", deviceID, len(ops), opsPerDevice)
			continue
		}
		for i := 1; i < len(ops); i++ {
			if ops[i].Timestamp.Before(ops[i-1].Timestamp) {
				t.Errorf("device %s: ops not ordered at index %d", deviceID, i)
				break
			}
		}
	}
}
