// Package soak provides the pure metric primitives for the aether soak/chaos suite:
// latency percentiles, a windowed latency-trend check and a growth (leak) check.
// Nothing here touches NATS or an OS process, so it is fast to unit-test in normal
// CI, while the long-running orchestration that uses it lives behind the `soak`
// build tag in internal/lord.
package soak

import (
	"math"
	"sort"
	"sync"
	"time"
)

// LatencyRecorder collects call latencies and computes percentiles over them. It is
// safe for concurrent use: the soak load generator records from many goroutines at
// once. Samples are kept in arrival order so a windowed trend can compare an early
// slice of the run against a late one.
type LatencyRecorder struct {
	mu      sync.Mutex
	samples []time.Duration
}

// NewLatencyRecorder returns a recorder pre-sized for capacity samples (a hint; it
// grows as needed).
func NewLatencyRecorder(capacity int) *LatencyRecorder {
	if capacity < 0 {
		capacity = 0
	}
	return &LatencyRecorder{samples: make([]time.Duration, 0, capacity)}
}

// Add records one latency sample.
func (r *LatencyRecorder) Add(d time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, d)
	r.mu.Unlock()
}

// Count returns the number of recorded samples.
func (r *LatencyRecorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.samples)
}

// Percentile returns the p-th percentile (p in 0..100) using the nearest-rank
// method. An empty recorder returns 0.
func (r *LatencyRecorder) Percentile(p float64) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return percentile(r.samples, p)
}

// Max returns the largest recorded sample, or 0 when empty.
func (r *LatencyRecorder) Max() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	var max time.Duration
	for _, d := range r.samples {
		if d > max {
			max = d
		}
	}
	return max
}

// Trend describes how a percentile moved from the start of the run to the end.
type Trend struct {
	First time.Duration // percentile of the leading window
	Last  time.Duration // percentile of the trailing window
}

// Trend splits the recorded samples into a leading and a trailing window, each
// holding `fraction` of the samples (0 < fraction <= 0.5), and reports the p-th
// percentile of each. Samples are in arrival order, so this compares the early part
// of the run against the late part - a rising Last is a degradation signal.
func (r *LatencyRecorder) Trend(p, fraction float64) Trend {
	r.mu.Lock()
	n := len(r.samples)
	window := make([]time.Duration, n)
	copy(window, r.samples)
	r.mu.Unlock()

	if n == 0 {
		return Trend{}
	}
	w := int(float64(n) * fraction)
	if w < 1 {
		w = 1
	}
	if w > n {
		w = n
	}
	return Trend{
		First: percentile(window[:w], p),
		Last:  percentile(window[n-w:], p),
	}
}

// OK reports whether the trailing window stayed within tolerance of the leading one
// (Last <= First * (1+tolerance)). A flat or improving trend passes. With no leading
// data it passes - there is nothing to regress against.
func (t Trend) OK(tolerance float64) bool {
	if t.First <= 0 {
		return true
	}
	return float64(t.Last) <= float64(t.First)*(1+tolerance)
}

// GrowthOK reports whether `end` stayed within tolerance above `start`
// (end <= start * (1+tolerance)). Used for goroutine counts, heap and thrall RSS: a
// leak shows up as unbounded growth over the run. A shrink or flat value passes.
// A non-positive start with a positive end is unbounded growth and fails.
func GrowthOK(start, end, tolerance float64) bool {
	if start <= 0 {
		return end <= 0
	}
	return end <= start*(1+tolerance)
}

// GrowthPct returns the growth of end over start as a percentage. A non-positive
// start yields 0 to avoid a division by zero in the report.
func GrowthPct(start, end float64) float64 {
	if start <= 0 {
		return 0
	}
	return (end - start) / start * 100
}

// percentile returns the p-th percentile of the samples using nearest-rank. It
// operates on a copy, so it never reorders the caller's slice (windows share a
// backing array).
func percentile(samples []time.Duration, p float64) time.Duration {
	n := len(samples)
	if n == 0 {
		return 0
	}
	sorted := make([]time.Duration, n)
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	rank := int(math.Ceil(p / 100 * float64(n)))
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}
