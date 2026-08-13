package lord

import (
	"math"
	"testing"
	"time"
)

func TestParseCPUTime(t *testing.T) {
	cases := map[string]float64{
		"0:00.50":    0.50,
		"117:08.37":  117*60 + 8.37,                 // macOS: MMM:SS.ss (minutes unbounded)
		"01:02:03":   1*3600 + 2*60 + 3,             // Linux: HH:MM:SS
		"2-03:04:05": 2*86400 + 3*3600 + 4*60 + 5,   // Linux: DD-HH:MM:SS
		"":           0,
	}
	for in, want := range cases {
		if got := parseCPUTime(in); math.Abs(got-want) > 0.01 {
			t.Errorf("parseCPUTime(%q) = %v, want %v", in, got, want)
		}
	}
}

// TestParseProcStatsSumsByPgid covers the core reason for aggregating by pgid: a thrall's real
// resource use is the SUM over its process group (sh leader + interpreter grandchild), not a
// single PID.
func TestParseProcStatsSumsByPgid(t *testing.T) {
	// pgid 100 has two processes (sh + interpreter); pgid 200 is alone. Leading spaces mimic ps.
	out := "  100   1024 0:01.00\n" +
		"  100  51200 0:10.00\n" +
		"  200   8192 0:02.00\n"

	agg := parseProcStats(out)
	if len(agg) != 2 {
		t.Fatalf("groups = %d, want 2", len(agg))
	}
	if got, want := agg[100].RSSBytes, int64((1024+51200)*1024); got != want {
		t.Errorf("pgid 100 RSS = %d bytes, want %d (summed over the group)", got, want)
	}
	if got := agg[100].CPUSeconds; math.Abs(got-11.0) > 0.01 {
		t.Errorf("pgid 100 CPU = %v s, want 11.0 (summed)", got)
	}
	if got, want := agg[200].RSSBytes, int64(8192*1024); got != want {
		t.Errorf("pgid 200 RSS = %d bytes, want %d", got, want)
	}
}

// TestParseProcStatsSkipsGarbage: malformed lines (a header, short lines) are skipped, not fatal.
func TestParseProcStatsSkipsGarbage(t *testing.T) {
	out := "PGID  RSS TIME\n" + // a header row (non-numeric) must be ignored
		"  50  2048 0:03.00\n" +
		"garbage\n"
	agg := parseProcStats(out)
	if len(agg) != 1 {
		t.Fatalf("groups = %d, want 1 (garbage skipped)", len(agg))
	}
	if agg[50].RSSBytes != 2048*1024 {
		t.Errorf("pgid 50 RSS = %d", agg[50].RSSBytes)
	}
}

func TestCPUPercent(t *testing.T) {
	// 1 CPU-second over a 2s interval = 50%.
	if got := cpuPercent(10.0, 11.0, 2*time.Second); math.Abs(got-50.0) > 0.01 {
		t.Errorf("cpuPercent(10,11,2s) = %v, want 50", got)
	}
	// 2 CPU-seconds over 2s = 100% (one fully-busy core).
	if got := cpuPercent(0, 2, 2*time.Second); math.Abs(got-100.0) > 0.01 {
		t.Errorf("cpuPercent(0,2,2s) = %v, want 100", got)
	}
	// Zero interval (no previous sample) -> 0, not a divide-by-zero.
	if got := cpuPercent(5, 6, 0); got != 0 {
		t.Errorf("cpuPercent with zero elapsed = %v, want 0", got)
	}
	// CPU-time decrease (a restarted process whose counter reset) -> 0, not negative.
	if got := cpuPercent(100, 1, 2*time.Second); got != 0 {
		t.Errorf("cpuPercent on a counter reset = %v, want 0", got)
	}
}
