package soak

import (
	"strings"
	"testing"
	"time"
)

// goodReport is a run where every phase ran and every bar held.
func goodReport() Report {
	return Report{
		Profile:  "smoke",
		Duration: 2 * time.Minute,
		Seed:     42,
		Bars:     DefaultBars(),

		CallCount:   10000,
		LatP99:      20 * time.Millisecond,
		LatMax:      40 * time.Millisecond,
		LatP99First: 18 * time.Millisecond,
		LatP99Last:  19 * time.Millisecond,

		Published:  5000,
		Distinct:   5000,
		Duplicates: 7,

		LeakEvaluated:  true,
		GoroutineStart: 40,
		GoroutineEnd:   41,
		HeapStart:      10 << 20,
		HeapEnd:        10 << 20,
		ThrallRSSStart: map[string]int64{"probe": 12000},
		ThrallRSSEnd:   map[string]int64{"probe": 12500},
	}
}

func TestBreachesNoneWhenHealthy(t *testing.T) {
	if b := goodReport().Breaches(); len(b) != 0 {
		t.Fatalf("healthy report should have no breaches, got %v", b)
	}
}

func TestBreachesLatencyP99(t *testing.T) {
	r := goodReport()
	r.LatP99 = 60 * time.Millisecond // over the 50ms bar
	b := r.Breaches()
	if len(b) != 1 || !strings.Contains(b[0], "call p99") {
		t.Fatalf("expected a single p99 breach, got %v", b)
	}
}

func TestBreachesLatencyTrend(t *testing.T) {
	r := goodReport()
	r.LatP99First = 10 * time.Millisecond
	r.LatP99Last = 40 * time.Millisecond // +300%, far past the 20% trend bar
	assertSingleBreach(t, r, "latency trend")
}

func TestBreachesDurableLoss(t *testing.T) {
	r := goodReport()
	r.Distinct = 4998 // 2 lost
	assertSingleBreach(t, r, "durable loss")
}

func TestBreachesGoroutineGrowth(t *testing.T) {
	r := goodReport()
	r.GoroutineEnd = 80 // +100%
	assertSingleBreach(t, r, "goroutine growth")
}

func TestBreachesHeapGrowth(t *testing.T) {
	r := goodReport()
	r.HeapEnd = 20 << 20 // +100%
	assertSingleBreach(t, r, "lord heap growth")
}

func TestBreachesThrallRSSGrowth(t *testing.T) {
	r := goodReport()
	r.ThrallRSSEnd = map[string]int64{"probe": 20000} // +66%
	assertSingleBreach(t, r, "thrall probe RSS growth")
}

func TestBreachesSkipsLeakWhenNotEvaluated(t *testing.T) {
	// A run too short to evaluate leaks reports the deltas but must not breach on them.
	r := goodReport()
	r.LeakEvaluated = false
	r.GoroutineEnd = 200 // huge growth, but leak bars are not enforced
	r.HeapEnd = 100 << 20
	r.ThrallRSSEnd = map[string]int64{"probe": 99000}
	if b := r.Breaches(); len(b) != 0 {
		t.Fatalf("leak bars must be skipped when not evaluated, got %v", b)
	}
	if out := r.Format(); !strings.Contains(out, "informational only") {
		t.Errorf("Format() should flag leak as informational when not evaluated\n%s", out)
	}
}

func TestBreachesSkipsPhasesThatDidNotRun(t *testing.T) {
	// A load-only run: durable and leak sections are zero and must not breach.
	r := Report{
		Bars:        DefaultBars(),
		CallCount:   100,
		LatP99:      10 * time.Millisecond,
		LatP99First: 10 * time.Millisecond,
		LatP99Last:  10 * time.Millisecond,
	}
	if b := r.Breaches(); len(b) != 0 {
		t.Fatalf("phases that did not run must not breach, got %v", b)
	}
}

func TestFormatContainsSectionsAndVerdict(t *testing.T) {
	out := goodReport().Format()
	for _, want := range []string{"sustained load", "durable", "goroutines", "lord heap", "thrall probe RSS", "bars: all held"} {
		if !strings.Contains(out, want) {
			t.Errorf("Format() missing %q\n%s", want, out)
		}
	}

	r := goodReport()
	r.LatP99 = 99 * time.Millisecond
	if out := r.Format(); !strings.Contains(out, "BREACH") {
		t.Errorf("Format() of a breaching report should say BREACH\n%s", out)
	}
}

func assertSingleBreach(t *testing.T, r Report, want string) {
	t.Helper()
	b := r.Breaches()
	if len(b) != 1 || !strings.Contains(b[0], want) {
		t.Fatalf("expected a single %q breach, got %v", want, b)
	}
}
