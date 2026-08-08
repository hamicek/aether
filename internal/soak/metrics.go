// Package soak provides the pure metric primitives for the aether soak/chaos suite:
// bounded-memory latency percentiles, a windowed latency-trend check and a growth
// (leak) check. Nothing here touches NATS or an OS process, so it is fast to
// unit-test in normal CI, while the long-running orchestration that uses it lives
// behind the `soak` build tag in internal/lord.
//
// Latencies are kept in fixed-bucket histograms rather than a growing slice: a soak
// run makes an unbounded number of calls, and because the lord runs in the same
// process as the test, an unbounded recorder would inflate the process heap and
// corrupt the very leak measurement the suite performs. Histograms make the
// recorder's footprint constant.
package soak

import (
	"math"
	"sync"
	"time"
)

// Histogram resolution and ceiling. Buckets are histResolution wide up to histMax;
// anything above lands in an overflow tally and is reported via the exact max. The
// ceiling comfortably clears the sustained-load bar (p99 < 50ms) while the 100us
// resolution keeps percentiles near the bar precise.
const (
	histResolution = 100 * time.Microsecond
	histMax        = 200 * time.Millisecond
	histBuckets    = int(histMax / histResolution)
)

// hist is a fixed-bucket latency histogram with constant memory.
type hist struct {
	counts   []uint64
	total    uint64
	overflow uint64
	max      time.Duration
}

func newHist() *hist {
	return &hist{counts: make([]uint64, histBuckets)}
}

func (h *hist) add(d time.Duration) {
	if d < 0 {
		d = 0
	}
	if d > h.max {
		h.max = d
	}
	h.total++
	idx := int(d / histResolution)
	if idx >= len(h.counts) {
		h.overflow++
		return
	}
	h.counts[idx]++
}

// percentile returns the p-th percentile (0..100) using nearest-rank over the
// bucket counts. It returns a bucket's upper bound (a conservative, never-under
// estimate for an SLO); a rank landing in the overflow returns the exact max.
func (h *hist) percentile(p float64) time.Duration {
	if h.total == 0 {
		return 0
	}
	rank := uint64(math.Ceil(p / 100 * float64(h.total)))
	if rank < 1 {
		rank = 1
	}
	var cum uint64
	for i, c := range h.counts {
		cum += c
		if cum >= rank {
			return time.Duration(i+1) * histResolution
		}
	}
	return h.max // rank falls in the overflow -> exact largest sample
}

// LatencyRecorder collects call latencies into an overall histogram plus per-window
// histograms so a trend (early vs late in the run) can be computed. It is safe for
// concurrent use: the load generator records from many goroutines at once. Memory is
// bounded by (windows+1) * histBuckets regardless of how many calls are recorded.
type LatencyRecorder struct {
	mu      sync.Mutex
	start   time.Time
	window  time.Duration
	overall *hist
	windows []*hist
}

// NewLatencyRecorder returns a recorder that spreads samples across `windows` equal
// time windows over the expected total run duration. Samples arriving after the last
// window fold into it, so an over-running phase never panics.
func NewLatencyRecorder(total time.Duration, windows int) *LatencyRecorder {
	if windows < 1 {
		windows = 1
	}
	if total <= 0 {
		total = time.Second
	}
	ws := make([]*hist, windows)
	for i := range ws {
		ws[i] = newHist()
	}
	return &LatencyRecorder{
		start:   time.Now(),
		window:  total / time.Duration(windows),
		overall: newHist(),
		windows: ws,
	}
}

// Add records one latency sample, placing it in the window for the current elapsed
// time.
func (r *LatencyRecorder) Add(d time.Duration) {
	r.mu.Lock()
	r.addAt(int(time.Since(r.start)/r.window), d)
	r.mu.Unlock()
}

// addAt records a sample into a specific window (and the overall histogram). It
// assumes the caller holds the lock; tests call it to populate windows
// deterministically without waiting for wall-clock time to pass.
func (r *LatencyRecorder) addAt(window int, d time.Duration) {
	r.overall.add(d)
	if window < 0 {
		window = 0
	}
	if window >= len(r.windows) {
		window = len(r.windows) - 1
	}
	r.windows[window].add(d)
}

// Count returns the number of recorded samples.
func (r *LatencyRecorder) Count() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.overall.total
}

// Percentile returns the p-th percentile (0..100) over all samples.
func (r *LatencyRecorder) Percentile(p float64) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.overall.percentile(p)
}

// Max returns the largest recorded sample, or 0 when empty.
func (r *LatencyRecorder) Max() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.overall.max
}

// Trend describes how a percentile moved from the start of the run to the end.
type Trend struct {
	First time.Duration // percentile of the first window that saw traffic
	Last  time.Duration // percentile of the last window that saw traffic
}

// Trend reports the p-th percentile of the earliest and latest windows that received
// samples. Comparing them surfaces a runtime that degrades over time. A run short
// enough to fill only one window yields First == Last (a flat, passing trend).
func (r *LatencyRecorder) Trend(p float64) Trend {
	r.mu.Lock()
	defer r.mu.Unlock()
	var first, last *hist
	for _, h := range r.windows {
		if h.total > 0 {
			if first == nil {
				first = h
			}
			last = h
		}
	}
	if first == nil {
		return Trend{}
	}
	return Trend{First: first.percentile(p), Last: last.percentile(p)}
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
