package soak

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Bars holds the pass/fail thresholds for a soak run. The defaults are the story's
// approved values; a run may tighten or loosen them per profile.
type Bars struct {
	LatP99Max time.Duration // sustained-load p99 ceiling (story: < 50ms)
	TrendTol  float64       // allowed rise of the last latency window over the first (fraction)
	GrowthTol float64       // allowed growth of goroutines / heap / RSS end-vs-start (fraction)
}

// DefaultBars returns the story's approved bars: p99 < 50ms, no upward latency
// trend beyond 20%, and less than 10% resource growth after warm-up.
func DefaultBars() Bars {
	return Bars{
		LatP99Max: 50 * time.Millisecond,
		TrendTol:  0.20,
		GrowthTol: 0.10,
	}
}

// Report is the structured outcome of a soak run. Fields for a phase that did not
// run stay at their zero value, and Breaches skips a bar whose phase produced no
// data (e.g. a load-only smoke run leaves the durable and leak sections empty).
type Report struct {
	Profile  string
	Duration time.Duration
	Seed     int64
	Bars     Bars

	// Sustained load.
	CallCount   int
	LatP99      time.Duration
	LatMax      time.Duration
	LatP99First time.Duration // p99 of the first latency window (trend baseline)
	LatP99Last  time.Duration // p99 of the last latency window

	// Durable no-loss.
	Published  int
	Distinct   int
	Duplicates int

	// Leak deltas. LeakEvaluated is false when the run was too short for a valid
	// warm-up: the deltas are then reported for information but not held to the growth
	// bars (a cold-start ramp is not a leak).
	LeakEvaluated  bool
	GoroutineStart int
	GoroutineEnd   int
	HeapStart      uint64           // bytes
	HeapEnd        uint64           // bytes
	ThrallRSSStart map[string]int64 // KB, per thrall name
	ThrallRSSEnd   map[string]int64 // KB, per thrall name
}

// latencyTrend rebuilds the Trend view from the recorded first/last window p99.
func (r Report) latencyTrend() Trend {
	return Trend{First: r.LatP99First, Last: r.LatP99Last}
}

// Breaches returns a human-readable list of bar violations; an empty slice means
// every bar held. The caller turns a non-empty result into a non-zero exit.
func (r Report) Breaches() []string {
	var breaches []string

	if r.CallCount > 0 {
		if r.LatP99 >= r.Bars.LatP99Max {
			breaches = append(breaches, fmt.Sprintf(
				"call p99 %s >= bar %s", ms(r.LatP99), ms(r.Bars.LatP99Max)))
		}
		if !r.latencyTrend().OK(r.Bars.TrendTol) {
			breaches = append(breaches, fmt.Sprintf(
				"latency trend: last window p99 %s over first %s (bar +%.0f%%)",
				ms(r.LatP99Last), ms(r.LatP99First), r.Bars.TrendTol*100))
		}
	}

	if r.Published > 0 && r.Distinct != r.Published {
		breaches = append(breaches, fmt.Sprintf(
			"durable loss: published %d, delivered %d distinct (lost %d)",
			r.Published, r.Distinct, r.Published-r.Distinct))
	}

	if r.LeakEvaluated {
		if r.GoroutineStart > 0 && !GrowthOK(float64(r.GoroutineStart), float64(r.GoroutineEnd), r.Bars.GrowthTol) {
			breaches = append(breaches, fmt.Sprintf(
				"goroutine growth: %d -> %d (+%.1f%%, bar +%.0f%%)",
				r.GoroutineStart, r.GoroutineEnd,
				GrowthPct(float64(r.GoroutineStart), float64(r.GoroutineEnd)), r.Bars.GrowthTol*100))
		}
		if r.HeapStart > 0 && !GrowthOK(float64(r.HeapStart), float64(r.HeapEnd), r.Bars.GrowthTol) {
			breaches = append(breaches, fmt.Sprintf(
				"lord heap growth: %s -> %s (+%.1f%%, bar +%.0f%%)",
				bytes(r.HeapStart), bytes(r.HeapEnd),
				GrowthPct(float64(r.HeapStart), float64(r.HeapEnd)), r.Bars.GrowthTol*100))
		}
		for _, name := range sortedRSSNames(r.ThrallRSSStart) {
			start := r.ThrallRSSStart[name]
			end := r.ThrallRSSEnd[name]
			if start > 0 && !GrowthOK(float64(start), float64(end), r.Bars.GrowthTol) {
				breaches = append(breaches, fmt.Sprintf(
					"thrall %s RSS growth: %d -> %d KB (+%.1f%%, bar +%.0f%%)",
					name, start, end,
					GrowthPct(float64(start), float64(end)), r.Bars.GrowthTol*100))
			}
		}
	}

	return breaches
}

// Format renders the report as a structured, human-readable block. Only sections
// whose phase ran are printed.
func (r Report) Format() string {
	var b strings.Builder
	fmt.Fprintf(&b, "soak report - profile=%s duration=%s seed=%d\n",
		r.Profile, r.Duration.Round(time.Second), r.Seed)

	if r.CallCount > 0 {
		fmt.Fprintf(&b, "  sustained load: calls=%d p99=%s max=%s trend[first=%s last=%s]\n",
			r.CallCount, ms(r.LatP99), ms(r.LatMax), ms(r.LatP99First), ms(r.LatP99Last))
	}
	if r.Published > 0 {
		fmt.Fprintf(&b, "  durable: published=%d delivered=%d lost=%d duplicates=%d\n",
			r.Published, r.Distinct, r.Published-r.Distinct, r.Duplicates)
	}
	if r.GoroutineStart > 0 {
		fmt.Fprintf(&b, "  goroutines: %d -> %d (%+.1f%%)\n",
			r.GoroutineStart, r.GoroutineEnd,
			GrowthPct(float64(r.GoroutineStart), float64(r.GoroutineEnd)))
	}
	if r.HeapStart > 0 {
		fmt.Fprintf(&b, "  lord heap: %s -> %s (%+.1f%%)\n",
			bytes(r.HeapStart), bytes(r.HeapEnd),
			GrowthPct(float64(r.HeapStart), float64(r.HeapEnd)))
	}
	for _, name := range sortedRSSNames(r.ThrallRSSStart) {
		start := r.ThrallRSSStart[name]
		end := r.ThrallRSSEnd[name]
		fmt.Fprintf(&b, "  thrall %s RSS: %d -> %d KB (%+.1f%%)\n",
			name, start, end, GrowthPct(float64(start), float64(end)))
	}
	if r.GoroutineStart > 0 && !r.LeakEvaluated {
		b.WriteString("  leak: informational only (run too short to hold the growth bars)\n")
	}

	if breaches := r.Breaches(); len(breaches) == 0 {
		b.WriteString("  bars: all held\n")
	} else {
		fmt.Fprintf(&b, "  bars: %d BREACH(es)\n", len(breaches))
		for _, br := range breaches {
			fmt.Fprintf(&b, "    - %s\n", br)
		}
	}
	return b.String()
}

// sortedRSSNames returns the thrall names in a map in deterministic order, so the
// report and breach list are stable across runs.
func sortedRSSNames(m map[string]int64) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
}

func bytes(n uint64) string {
	const mib = 1 << 20
	return fmt.Sprintf("%.1fMiB", float64(n)/float64(mib))
}
