package soak

import (
	"testing"
	"time"
)

// nearBucket asserts got is within one histogram resolution above want (percentiles
// report a bucket upper bound, a conservative over-estimate).
func nearBucket(t *testing.T, label string, got, want time.Duration) {
	t.Helper()
	if got < want || got > want+2*histResolution {
		t.Errorf("%s = %s, want ~%s (within +%s)", label, got, want, 2*histResolution)
	}
}

func TestPercentileNearestRank(t *testing.T) {
	r := NewLatencyRecorder(time.Hour, 1)
	// Samples 1ms..100ms, added out of order to prove the histogram sorts by bucket.
	for _, v := range []int{50, 1, 100, 2, 99} {
		r.Add(time.Duration(v) * time.Millisecond)
	}
	for i := 3; i <= 98; i++ {
		if i == 50 || i == 99 {
			continue
		}
		r.Add(time.Duration(i) * time.Millisecond)
	}
	if got, want := r.Count(), uint64(100); got != want {
		t.Fatalf("Count = %d, want %d", got, want)
	}
	// nearest-rank: p99 -> the 99th smallest = 99ms, p50 -> 50ms.
	nearBucket(t, "p99", r.Percentile(99), 99*time.Millisecond)
	nearBucket(t, "p50", r.Percentile(50), 50*time.Millisecond)
	if got, want := r.Max(), 100*time.Millisecond; got != want {
		t.Errorf("Max = %s, want %s (exact)", got, want)
	}
}

func TestPercentileOverflowUsesExactMax(t *testing.T) {
	r := NewLatencyRecorder(time.Hour, 1)
	// A sample past the histogram ceiling must still report an exact max and a
	// percentile that reaches it rather than silently capping at histMax.
	r.Add(5 * time.Millisecond)
	r.Add(2 * time.Second) // overflow
	if got := r.Max(); got != 2*time.Second {
		t.Errorf("Max = %s, want 2s", got)
	}
	if got := r.Percentile(100); got != 2*time.Second {
		t.Errorf("p100 = %s, want 2s (from overflow max)", got)
	}
}

func TestPercentileEmpty(t *testing.T) {
	r := NewLatencyRecorder(time.Hour, 1)
	if got := r.Percentile(99); got != 0 {
		t.Errorf("empty p99 = %s, want 0", got)
	}
	if got := r.Max(); got != 0 {
		t.Errorf("empty Max = %s, want 0", got)
	}
}

func TestTrendRisingAndFlat(t *testing.T) {
	// Window 0 at 10ms, the last window at 30ms -> a clear upward trend. addAt lets
	// the test place samples in windows without waiting on wall-clock time.
	r := NewLatencyRecorder(time.Hour, 10)
	for i := 0; i < 10; i++ {
		r.addAt(0, 10*time.Millisecond)
		r.addAt(9, 30*time.Millisecond)
	}
	tr := r.Trend(99)
	nearBucket(t, "trend.First", tr.First, 10*time.Millisecond)
	nearBucket(t, "trend.Last", tr.Last, 30*time.Millisecond)
	if tr.OK(0.10) {
		t.Errorf("rising trend must fail a 10%% tolerance")
	}
	if !tr.OK(3.0) {
		t.Errorf("rising trend within a 300%% tolerance must pass")
	}

	// A flat run must pass a 0% tolerance.
	flat := NewLatencyRecorder(time.Hour, 10)
	for i := 0; i < 20; i++ {
		flat.addAt(0, 15*time.Millisecond)
		flat.addAt(9, 15*time.Millisecond)
	}
	if ft := flat.Trend(99); !ft.OK(0) {
		t.Errorf("flat trend must pass a 0%% tolerance, got %+v", ft)
	}
}

func TestTrendEmptyPasses(t *testing.T) {
	r := NewLatencyRecorder(time.Hour, 10)
	if !r.Trend(99).OK(0) {
		t.Errorf("empty trend must pass (nothing to regress against)")
	}
}

func TestTrendSingleWindowIsFlat(t *testing.T) {
	// Only one window saw traffic -> First == Last, always passes.
	r := NewLatencyRecorder(time.Hour, 10)
	r.addAt(3, 42*time.Millisecond)
	tr := r.Trend(99)
	if tr.First != tr.Last {
		t.Errorf("single populated window must give First==Last, got %+v", tr)
	}
	if !tr.OK(0) {
		t.Errorf("single populated window must pass, got %+v", tr)
	}
}

func TestAddDoesNotDivideByZeroOnTinyTotal(t *testing.T) {
	// total < windows would round the window width to 0; Add must not panic.
	r := NewLatencyRecorder(5, 10) // 5ns over 10 windows
	r.Add(1 * time.Millisecond)
	if r.Count() != 1 {
		t.Fatalf("Count = %d, want 1", r.Count())
	}
}

func TestGrowthOK(t *testing.T) {
	cases := []struct {
		name            string
		start, end, tol float64
		want            bool
	}{
		{"within tolerance", 100, 109, 0.10, true},
		{"at the bar", 100, 110, 0.10, true},
		{"over the bar", 100, 111, 0.10, false},
		{"shrink passes", 100, 50, 0.10, true},
		{"flat passes", 100, 100, 0.0, true},
		{"zero start, zero end", 0, 0, 0.10, true},
		{"zero start, positive end fails", 0, 5, 0.10, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := GrowthOK(tc.start, tc.end, tc.tol); got != tc.want {
				t.Errorf("GrowthOK(%v, %v, %v) = %v, want %v", tc.start, tc.end, tc.tol, got, tc.want)
			}
		})
	}
}

func TestGrowthPct(t *testing.T) {
	if got := GrowthPct(100, 110); got != 10 {
		t.Errorf("GrowthPct(100,110) = %v, want 10", got)
	}
	if got := GrowthPct(0, 5); got != 0 {
		t.Errorf("GrowthPct(0,5) = %v, want 0 (guarded div-by-zero)", got)
	}
}
