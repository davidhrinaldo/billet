package pebblestore

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
)

func BenchmarkAppendOp(b *testing.B) {
	s, err := Open("", &pebble.Options{FS: vfs.NewMem()})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer s.Close()

	b.ResetTimer()
	for i := range b.N {
		op := shadow.Op{
			ID:        shadow.OpID{NodeID: 1, Seq: uint64(i + 1)},
			DeviceID:  "dev-1",
			Section:   shadow.SectionReported,
			Key:       "temp",
			Data:      []byte("value-placeholder"),
			Timestamp: hlc.Timestamp{Physical: int64(i * 1000)},
		}
		if err := s.AppendOp(op); err != nil {
			b.Fatalf("AppendOp: %v", err)
		}
	}
}

func BenchmarkListOps(b *testing.B) {
	for _, n := range []int{1000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("ops=%d", n), func(b *testing.B) {
			s, err := Open("", &pebble.Options{FS: vfs.NewMem()})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			defer s.Close()

			for i := range n {
				op := shadow.Op{
					ID:        shadow.OpID{NodeID: 1, Seq: uint64(i + 1)},
					DeviceID:  "dev-1",
					Section:   shadow.SectionReported,
					Key:       fmt.Sprintf("k-%d", i%64),
					Data:      []byte("value-placeholder"),
					Timestamp: hlc.Timestamp{Physical: int64(i * 1000)},
				}
				if err := s.AppendOp(op); err != nil {
					b.Fatalf("AppendOp: %v", err)
				}
			}

			b.ResetTimer()
			for range b.N {
				ops, err := s.ListOps("dev-1")
				if err != nil {
					b.Fatalf("ListOps: %v", err)
				}
				if len(ops) != n {
					b.Fatalf("got %d ops, want %d", len(ops), n)
				}
			}
		})
	}
}

func BenchmarkFlashBudget(b *testing.B) {
	// Measures total bytes written to Pebble under sustained append load.
	dir := b.TempDir()
	s, err := Open(dir, nil)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer s.Close()

	m0 := s.Metrics()
	walWritten0 := m0.WAL.BytesWritten

	const opsPerDevice = 1000
	const numDevices = 10

	for d := range numDevices {
		deviceID := shadow.DeviceID(fmt.Sprintf("dev-%04d", d))
		for i := range opsPerDevice {
			op := shadow.Op{
				ID:        shadow.OpID{NodeID: 1, Seq: uint64(d*opsPerDevice + i)},
				DeviceID:  deviceID,
				Section:   shadow.SectionReported,
				Key:       fmt.Sprintf("k-%d", i%64),
				Data:      []byte("value-placeholder-data"),
				Timestamp: hlc.Timestamp{Physical: int64(i * 60000)},
			}
			if err := s.AppendOp(op); err != nil {
				b.Fatalf("AppendOp: %v", err)
			}
		}
	}

	m1 := s.Metrics()
	walWritten := m1.WAL.BytesWritten - walWritten0
	totalOps := numDevices * opsPerDevice

	b.ReportMetric(float64(walWritten), "wal_bytes_written")
	b.ReportMetric(float64(walWritten)/float64(totalOps), "wal_bytes/op")
	b.ReportMetric(float64(m1.DiskSpaceUsage()), "disk_usage_bytes")
}
