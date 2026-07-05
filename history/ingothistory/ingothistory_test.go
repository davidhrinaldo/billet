package ingothistory

import (
	"testing"
	"time"

	"github.com/davidhrinaldo/billet/history"
	"github.com/davidhrinaldo/billet/history/historytest"
	"github.com/davidhrinaldo/ingot"
)

func TestConformance(t *testing.T) {
	historytest.Suite(t, func(t *testing.T) (history.History, func()) {
		t.Helper()
		dir := t.TempDir()
		db, err := ingot.Open(dir, ingot.Options{
			BlockDuration: 1 * time.Hour,
		})
		if err != nil {
			t.Fatalf("open ingot: %v", err)
		}
		h := New(db)
		return h, func() { db.Close() }
	})
}

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	// Open, write, close.
	db, err := ingot.Open(dir, ingot.Options{BlockDuration: 1 * time.Hour})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	h := New(db)

	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := h.Record("dev-1", "temp", history.Sample{Time: t0, Value: 22.5}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen and verify.
	db2, err := ingot.Open(dir, ingot.Options{BlockDuration: 1 * time.Hour})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	h2 := New(db2)
	samples, err := h2.Query("dev-1", "temp", t0, t0.Add(time.Hour))
	if err != nil {
		t.Fatalf("Query after reopen: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(samples))
	}
	if samples[0].Value != 22.5 {
		t.Errorf("expected value 22.5, got %f", samples[0].Value)
	}
}

func TestMultiChannelWrite(t *testing.T) {
	dir := t.TempDir()
	db, err := ingot.Open(dir, ingot.Options{BlockDuration: 1 * time.Hour})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	h := New(db)
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Write 64 channels for a single device.
	const numChannels = 64
	const numSamples = 10
	for ch := range numChannels {
		key := channelKey(ch)
		for s := range numSamples {
			sample := history.Sample{
				Time:  t0.Add(time.Duration(s) * time.Minute),
				Value: float64(ch*1000 + s),
			}
			if err := h.Record("dev-1", key, sample); err != nil {
				t.Fatalf("Record ch=%d s=%d: %v", ch, s, err)
			}
		}
	}

	// Query each channel and verify isolation.
	for ch := range numChannels {
		key := channelKey(ch)
		samples, err := h.Query("dev-1", key, t0, t0.Add(time.Hour))
		if err != nil {
			t.Fatalf("Query ch=%d: %v", ch, err)
		}
		if len(samples) != numSamples {
			t.Errorf("ch=%d: got %d samples, want %d", ch, len(samples), numSamples)
			continue
		}
		for i, s := range samples {
			want := float64(ch*1000 + i)
			if s.Value != want {
				t.Errorf("ch=%d sample=%d: got %f, want %f", ch, i, s.Value, want)
			}
		}
	}
}

func channelKey(ch int) string {
	return "ch-" + itoa(ch)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 4)
	for n > 0 {
		buf = append(buf, byte('0'+n%10))
		n /= 10
	}
	// Reverse.
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return string(buf)
}
