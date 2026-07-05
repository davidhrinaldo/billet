package ingothistory

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davidhrinaldo/billet/history"
	"github.com/davidhrinaldo/ingot"
)

func BenchmarkRecord(b *testing.B) {
	dir := b.TempDir()
	db, err := ingot.Open(dir, ingot.Options{BlockDuration: 2 * time.Hour})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	h := New(db)
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	b.ResetTimer()
	for i := range b.N {
		s := history.Sample{
			Time:  t0.Add(time.Duration(i) * time.Minute),
			Value: float64(i) * 0.1,
		}
		if err := h.Record("dev-1", "temp", s); err != nil {
			b.Fatalf("Record: %v", err)
		}
	}
}

func BenchmarkSustainedWrite(b *testing.B) {
	// Simulates the target workload: 64 channels, 1-minute interval, measures
	// bytes on disk after the run.
	dir := b.TempDir()
	db, err := ingot.Open(dir, ingot.Options{
		BlockDuration: 2 * time.Hour,
		Retention:     90 * 24 * time.Hour,
	})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()

	h := New(db)
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	const numChannels = 64
	// Write 1 day of 1-minute data per channel = 1440 samples/channel.
	const samplesPerChannel = 1440

	b.ResetTimer()
	for ch := range numChannels {
		key := fmt.Sprintf("ch-%02d", ch)
		for s := range samplesPerChannel {
			sample := history.Sample{
				Time:  t0.Add(time.Duration(s) * time.Minute),
				Value: float64(ch) + float64(s)*0.01,
			}
			if err := h.Record("dev-1", key, sample); err != nil {
				b.Fatalf("Record ch=%d s=%d: %v", ch, s, err)
			}
		}
	}
	b.StopTimer()

	// Report disk usage.
	var totalBytes int64
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalBytes += info.Size()
		}
		return nil
	})

	totalSamples := numChannels * samplesPerChannel
	b.ReportMetric(float64(totalBytes), "disk_bytes")
	b.ReportMetric(float64(totalBytes)/float64(totalSamples), "bytes/sample")
	b.ReportMetric(float64(totalSamples), "total_samples")

	stats := db.Stats()
	b.ReportMetric(float64(stats.HeadSeries), "head_series")
	b.ReportMetric(float64(stats.HeadChunks), "head_chunks")
}

// TestCompacted90DayStorage simulates the M3 target workload — 64 channels of
// 1-minute data for 90 days — with ingot's flush and compaction running, then
// measures final on-disk size. The M3 target is <100 MB.
//
// This test is skipped with -short because it writes ~8.3M samples and takes
// several minutes.
func TestCompacted90DayStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compaction soak test in -short mode")
	}

	dir := t.TempDir()

	// Simulated clock: ingot uses this to decide flush boundaries.
	var nowMs atomic.Int64
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	nowMs.Store(t0.UnixMilli())

	db, err := ingot.Open(dir, ingot.Options{
		BlockDuration: 2 * time.Hour,
		Retention:     91 * 24 * time.Hour, // slightly over 90 to avoid edge trimming
		Clock:         func() int64 { return nowMs.Load() },
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	h := New(db)

	const (
		numChannels      = 64
		daysToSimulate   = 90
		samplesPerDay    = 1440 // 1 per minute
		flushIntervalMin = 120  // flush every 2 hours of simulated time
	)

	totalSamples := 0
	for day := range daysToSimulate {
		dayStart := t0.Add(time.Duration(day) * 24 * time.Hour)

		for s := range samplesPerDay {
			sampleTime := dayStart.Add(time.Duration(s) * time.Minute)
			nowMs.Store(sampleTime.UnixMilli())

			for ch := range numChannels {
				sample := history.Sample{
					Time:  sampleTime,
					Value: float64(ch) + float64(s)*0.001,
				}
				if err := h.Record("dev-1", fmt.Sprintf("ch-%02d", ch), sample); err != nil {
					t.Fatalf("Record day=%d s=%d ch=%d: %v", day, s, ch, err)
				}
				totalSamples++
			}

			// Flush completed blocks every flushIntervalMin minutes of
			// simulated time.
			if s > 0 && s%flushIntervalMin == 0 {
				if _, err := db.FlushOlderThan(sampleTime.UnixMilli()); err != nil {
					t.Fatalf("FlushOlderThan day=%d s=%d: %v", day, s, err)
				}
			}
		}

		// Compact once per simulated day.
		if err := db.RunCompaction(); err != nil {
			t.Fatalf("RunCompaction day=%d: %v", day, err)
		}

		if (day+1)%10 == 0 {
			size := dirSize(t, dir)
			t.Logf("day %3d: %d samples written, disk = %.1f MB, %d blocks",
				day+1, totalSamples, float64(size)/(1024*1024), db.Stats().Blocks)
		}
	}

	// Final flush and compaction.
	endTime := t0.Add(90 * 24 * time.Hour)
	nowMs.Store(endTime.UnixMilli())
	if _, err := db.FlushOlderThan(endTime.UnixMilli()); err != nil {
		t.Fatalf("final FlushOlderThan: %v", err)
	}
	if err := db.RunCompaction(); err != nil {
		t.Fatalf("final RunCompaction: %v", err)
	}

	finalSize := dirSize(t, dir)
	stats := db.Stats()

	t.Logf("=== 90-day compacted storage result ===")
	t.Logf("channels:      %d", numChannels)
	t.Logf("total samples: %d", totalSamples)
	t.Logf("disk size:     %.2f MB", float64(finalSize)/(1024*1024))
	t.Logf("bytes/sample:  %.2f", float64(finalSize)/float64(totalSamples))
	t.Logf("blocks:        %d", stats.Blocks)
	t.Logf("head series:   %d", stats.HeadSeries)
	t.Logf("head chunks:   %d", stats.HeadChunks)
	t.Logf("compactions:   %d", stats.Compactions)

	const targetMB = 100.0
	sizeMB := float64(finalSize) / (1024 * 1024)
	if sizeMB > targetMB {
		t.Errorf("disk size %.2f MB exceeds target %.0f MB", sizeMB, targetMB)
	}
}

// dirSize returns the total size of all files under dir.
func dirSize(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}
