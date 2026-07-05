package ingothistory

import (
	"fmt"
	"os"
	"path/filepath"
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
