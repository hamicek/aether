package soak

import (
	"testing"
	"time"
)

func TestPercentileNearestRank(t *testing.T) {
	r := NewLatencyRecorder(100)
	// Samples 1ms..100ms in a deliberately shuffled order to prove sorting.
	for _, v := range []int{50, 1, 100, 2, 99, 3, 98, 51} {
		r.Add(time.Duration(v) * time.Millisecond)
	}
	for i := 4; i <= 97; i++ {
		if i == 50 || i == 51 || i == 98 || i == 99 {
			continue
		}
		r.Add(time.Duration(i) * time.Millisecond)
	}
	if got, want := r.Count(), 100; got != want {
		t.Fatalf("Count = %d, want %d", got, want)
	}
	// nearest-rank: p99 -> ceil(0.99*100)=99 -> the 99th smallest = 99ms.
	if got, want := r.Percentile(99), 99*time.Millisecond; got != want {
		t.Errorf("p99 = %s, want %s", got, want)
	}
	if got, want := r.Percentile(50), 50*time.Millisecond; got != want {
		t.Errorf("p50 = %s, want %s", got, want)
	}
	if got, want := r.Percentile(100), 100*time.Millisecond; got != want {
		t.Errorf("p100 = %s, want %s", got, want)
	}
	if got, want := r.Max(), 100*time.Millisecond; got != want {
		t.Errorf("Max = %s, want %s", got, want)
	}
}

func TestPercentileEmpty(t *testing.T) {
	r := NewLatencyRecorder(0)
	if got := r.Percentile(99); got != 0 {
		t.Errorf("empty p99 = %s, want 0", got)
	}
	if got := r.Max(); got != 0 {
		t.Errorf("empty Max = %s, want 0", got)
	}
}

func TestTrendFlatAndRising(t *testing.T) {
	// First half at 10ms, second half at 30ms -> a clear upward trend.
	r := NewLatencyRecorder(20)
	for i := 0; i < 10; i++ {
		r.Add(10 * time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		r.Add(30 * time.Millisecond)
	}
	tr := r.Trend(99, 0.5)
	if tr.First != 10*time.Millisecond || tr.Last != 30*time.Millisecond {
		t.Fatalf("Trend = %+v, want first=10ms last=30ms", tr)
	}
	if tr.OK(0.10) {
		t.Errorf("rising trend must fail a 10%% tolerance")
	}
	if !tr.OK(3.0) {
		t.Errorf("rising trend within a 300%% tolerance must pass")
	}

	// A flat run must pass any non-negative tolerance.
	flat := NewLatencyRecorder(20)
	for i := 0; i < 20; i++ {
		flat.Add(15 * time.Millisecond)
	}
	if ft := flat.Trend(99, 0.25); !ft.OK(0) {
		t.Errorf("flat trend must pass a 0%% tolerance, got %+v", ft)
	}
}

func TestTrendEmptyPasses(t *testing.T) {
	r := NewLatencyRecorder(0)
	if !r.Trend(99, 0.1).OK(0) {
		t.Errorf("empty trend must pass (nothing to regress against)")
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
