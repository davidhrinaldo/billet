package oplog

import (
	"fmt"
	"testing"

	"github.com/davidhrinaldo/billet/hlc"
	"github.com/davidhrinaldo/billet/shadow"
	"github.com/davidhrinaldo/billet/store/memstore"
)

func BenchmarkReplay(b *testing.B) {
	for _, n := range []int{1000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("ops=%d", n), func(b *testing.B) {
			s := memstore.New()
			l := New(s)
			for i := range n {
				op := shadow.Op{
					ID:        shadow.OpID{NodeID: 1, Seq: uint64(i + 1)},
					DeviceID:  "dev-1",
					Section:   shadow.SectionReported,
					Key:       fmt.Sprintf("k-%d", i%64),
					Data:      []byte("value-placeholder"),
					Timestamp: hlc.Timestamp{Physical: int64(i * 1000)},
				}
				if err := l.Append(op); err != nil {
					b.Fatalf("Append: %v", err)
				}
			}

			b.ResetTimer()
			for range b.N {
				ops, err := l.Replay("dev-1")
				if err != nil {
					b.Fatalf("Replay: %v", err)
				}
				if len(ops) != n {
					b.Fatalf("got %d ops, want %d", len(ops), n)
				}
			}
		})
	}
}

func BenchmarkSnapshot(b *testing.B) {
	for _, n := range []int{100, 1000, 10_000} {
		b.Run(fmt.Sprintf("ops=%d", n), func(b *testing.B) {
			for range b.N {
				b.StopTimer()
				s := memstore.New()
				l := New(s)
				for i := range n {
					op := shadow.Op{
						ID:        shadow.OpID{NodeID: 1, Seq: uint64(i + 1)},
						DeviceID:  "dev-1",
						Section:   shadow.SectionReported,
						Key:       fmt.Sprintf("k-%d", i%64),
						Data:      []byte("value-placeholder"),
						Timestamp: hlc.Timestamp{Physical: int64(i * 1000)},
					}
					if err := l.Append(op); err != nil {
						b.Fatalf("Append: %v", err)
					}
				}
				b.StartTimer()

				if _, err := l.Snapshot("dev-1"); err != nil {
					b.Fatalf("Snapshot: %v", err)
				}
			}
		})
	}
}
