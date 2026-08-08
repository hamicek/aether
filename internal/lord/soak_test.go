//go:build soak

// This file is the aether soak/chaos suite (v1: sustained load, durable-no-loss and
// leak detection). It is gated behind the `soak` build tag so normal CI - which runs
// `go test ./...` without tags - never compiles or runs it. Run it explicitly:
//
//	go test -tags soak -run TestSoak -timeout 10m ./internal/lord/ -args -soak.profile smoke
//
// or via scripts/soak.sh. Configuration is by flag or the matching AETHER_SOAK_* env
// var; a profile (smoke|default|overnight) sets the run length and load width, and
// -soak.duration / -soak.seed override for ad-hoc runs. The seed is logged so a run
// is reproducible.
//
// The suite reuses the AE-003 harness (startEmbedded, startLord, waitReady, waitFor,
// spec) and drives a richer probe (runSoakProbe) that counts casts idempotently so
// durable delivery can be checked for loss.
package lord

import (
	"context"
	"encoding/json"
	"flag"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/soak"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// Number of trend windows the run's latency is split across for the first-vs-last
// comparison, and the per-call request timeout used by the load generator.
const (
	soakTrendWindows = 10
	soakCallTimeout  = 1 * time.Second

	// Durable publishers and their per-cast pacing. Paced well below the single-message
	// durable consumer's drain rate so the JetStream backlog stays near empty and the
	// end-of-run drain is quick.
	soakDurableWorkers = 2
	soakDurablePace    = 5 * time.Millisecond
	soakDrainStall     = 20 // consecutive no-progress polls (~10s) that end the drain
)

var (
	soakProfileFlag  = flag.String("soak.profile", "", "soak profile: smoke|default|overnight")
	soakDurationFlag = flag.Duration("soak.duration", 0, "override the profile run duration")
	soakSeedFlag     = flag.Int64("soak.seed", 0, "PRNG seed (0 -> profile default)")
	soakReportFlag   = flag.String("soak.report", "", "optional path to also write the report to")
)

// loadProfile is the shape of one named profile: how long to run and how wide the
// concurrent load is.
type loadProfile struct {
	duration    time.Duration
	loadWorkers int
	defaultSeed int64
}

var soakProfiles = map[string]loadProfile{
	"smoke":     {duration: 2 * time.Minute, loadWorkers: 8, defaultSeed: 1},
	"default":   {duration: 45 * time.Minute, loadWorkers: 16, defaultSeed: 1},
	"overnight": {duration: 8 * time.Hour, loadWorkers: 16, defaultSeed: 1},
}

// soakConfig is the resolved configuration for one run (profile + overrides).
type soakConfig struct {
	profile     string
	duration    time.Duration
	loadWorkers int
	seed        int64
	reportPath  string
}

// --- soak probe (the thrall behind AETHER_SOAK_PROBE=1) ---

func init() { soakDispatch = runSoakDispatch }

// runSoakDispatch is the seam TestMain (integration_test.go) calls: when this process
// is a soak-probe re-exec it runs the probe and reports true so the caller returns
// without entering the test runner.
func runSoakDispatch() bool {
	if os.Getenv("AETHER_SOAK_PROBE") != "1" {
		return false
	}
	runSoakProbe()
	return true
}

// soakState is the soak probe's state for idempotent (at-least-once) cast counting.
// The publisher sends a contiguous 0..N-1 sequence, so instead of a full seen-set
// (which would grow to N entries and inflate the probe's RSS on a long run) the state
// tracks the contiguous watermark plus a small set of out-of-order arrivals above it.
// Memory is bounded by the reorder/redelivery window, not by the total cast count.
// Held behind a pointer so the serialized-mailbox handlers mutate it in place.
type soakState struct {
	next     int          // every sequence number in [0, next) has been seen
	ahead    map[int]bool // seen sequence numbers >= next, not yet contiguous
	distinct int
	dups     int
	max      int
}

// record folds one cast sequence number into the state: first sight counts as
// distinct, a re-delivery counts as a duplicate. This is what lets the suite assert
// zero loss (distinct == stored) while tolerating a finite number of duplicates.
func (s *soakState) record(seq int) {
	if seq < s.next || s.ahead[seq] {
		s.dups++
		return
	}
	s.distinct++
	if seq > s.max {
		s.max = seq
	}
	if seq == s.next {
		s.next++
		for s.ahead[s.next] {
			delete(s.ahead, s.next)
			s.next++
		}
	} else {
		s.ahead[seq] = true
	}
}

// runSoakProbe runs the richer thrall through the real Go SDK. `getlat` is a cheap
// call used only to measure request/reply latency; `seq` is an idempotent cast that
// carries a sequence number; `stats` reports the counters for the no-loss check.
func runSoakProbe() {
	def := thrall.Def[*soakState]{
		// Name empty -> taken from AETHER_NAME injected by the lord.
		Init: func(ctx *thrall.Ctx) (*soakState, error) {
			_ = ctx.NATS.Publish("test.probe.started", []byte(ctx.Name))
			_ = ctx.NATS.Flush()
			return &soakState{ahead: make(map[int]bool)}, nil
		},
		HandleCall: map[string]thrall.CallFn[*soakState]{
			"getlat": func(_ json.RawMessage, s *soakState, _ *thrall.Ctx) (any, *soakState, error) {
				return s.distinct, s, nil
			},
			"stats": func(_ json.RawMessage, s *soakState, _ *thrall.Ctx) (any, *soakState, error) {
				return map[string]int{"distinct": s.distinct, "dups": s.dups, "max": s.max}, s, nil
			},
		},
		HandleCast: map[string]thrall.CastFn[*soakState]{
			"seq": func(raw json.RawMessage, s *soakState, _ *thrall.Ctx) (*soakState, error) {
				var p struct {
					Seq int `json:"seq"`
				}
				if err := json.Unmarshal(raw, &p); err != nil {
					return s, nil // ignore a malformed cast rather than crash the mailbox
				}
				s.record(p.Seq)
				return s, nil
			},
		},
	}
	if err := thrall.Start(def); err != nil {
		os.Exit(2)
	}
}

// --- harness helpers specific to the soak probe ---

func soakProbeCmd(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	return "AETHER_SOAK_PROBE=1 " + exe
}

func soakManifest(t *testing.T, app string, specs ...ThrallSpec) *Manifest {
	t.Helper()
	cmd := soakProbeCmd(t)
	for i := range specs {
		specs[i].Cmd = cmd
	}
	return &Manifest{App: app, Strategy: "one_for_one", Thralls: specs}
}

type probeStats struct {
	Distinct int `json:"distinct"`
	Dups     int `json:"dups"`
	Max      int `json:"max"`
}

// trySoakStats calls `stats` and returns the counters; ok is false on any transport
// or decode error so it is safe to use inside a waitFor poll.
func trySoakStats(nc *nats.Conn, app, name string) (probeStats, bool) {
	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: name, Op: "stats",
		Payload: json.RawMessage("{}"), TS: time.Now().UnixMilli()}
	data, _ := json.Marshal(req)
	msg, err := nc.Request(wire.Call(app, name), data, 2*time.Second)
	if err != nil {
		return probeStats{}, false
	}
	var reply wire.Envelope
	if json.Unmarshal(msg.Data, &reply) != nil || reply.Status == "error" {
		return probeStats{}, false
	}
	var st probeStats
	if json.Unmarshal(reply.Payload, &st) != nil {
		return probeStats{}, false
	}
	return st, true
}

// --- sustained load ---

// loadResult is the outcome of the sustained-load phase.
type loadResult struct {
	calls  int64
	errors int64
	p99    time.Duration
	max    time.Duration
	trend  soak.Trend
}

// callLatency issues one `getlat` request and returns the round-trip time. ok is
// false on a transport or handler error; the elapsed time is returned either way so
// a timeout still counts against the latency bars (a slow runtime is a failing one).
func callLatency(nc *nats.Conn, subject string, req []byte) (time.Duration, bool) {
	start := time.Now()
	msg, err := nc.Request(subject, req, soakCallTimeout)
	elapsed := time.Since(start)
	if err != nil {
		return elapsed, false
	}
	var reply wire.Envelope
	if json.Unmarshal(msg.Data, &reply) != nil || reply.Status == "error" {
		return elapsed, false
	}
	return elapsed, true
}

// runLoad drives a concurrent `getlat` request/reply stream against the targets for
// the whole run and records every latency. Each worker has its own seeded RNG (so a
// run is reproducible) and picks a target with it. The recorder's memory is bounded,
// so this is safe to run for hours.
func runLoad(ctx context.Context, nc *nats.Conn, app string, targets []string, workers int, seed int64, rec *soak.LatencyRecorder) loadResult {
	type target struct {
		subject string
		req     []byte
	}
	tmpls := make([]target, len(targets))
	for i, name := range targets {
		e := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: name, Op: "getlat",
			Payload: json.RawMessage("{}"), TS: time.Now().UnixMilli()}
		data, _ := json.Marshal(e)
		tmpls[i] = target{subject: wire.Call(app, name), req: data}
	}

	var calls, errors int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed + int64(w)))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				tmpl := tmpls[rng.Intn(len(tmpls))]
				elapsed, ok := callLatency(nc, tmpl.subject, tmpl.req)
				rec.Add(elapsed)
				atomic.AddInt64(&calls, 1)
				if !ok {
					atomic.AddInt64(&errors, 1)
				}
			}
		}(w)
	}
	wg.Wait()

	return loadResult{
		calls:  calls,
		errors: errors,
		p99:    rec.Percentile(99),
		max:    rec.Max(),
		trend:  rec.Trend(99),
	}
}

// --- durable no-loss ---

// durableResult is the outcome of the durable-no-loss phase after the stream drains.
type durableResult struct {
	stored     int // casts the server acked into the stream (the no-loss denominator)
	attempted  int // publishes tried (stored plus any publish failures -> gaps)
	distinct   int // distinct casts the durable thrall received
	duplicates int
}

// runDurable publishes a contiguous 0..N-1 sequence of casts through JetStream (with
// publish acks, so a counted cast is a persisted one) for the whole run. Publishers
// share an atomic sequence counter; the returned counts feed the zero-loss check.
func runDurable(ctx context.Context, nc *nats.Conn, app, name string, workers int, pace time.Duration) (stored, attempted int64) {
	js, err := nc.JetStream()
	if err != nil {
		return 0, 0
	}
	subject := wire.Cast(app, name)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				seq := atomic.AddInt64(&attempted, 1) - 1
				body, _ := json.Marshal(struct {
					Seq int64 `json:"seq"`
				}{seq})
				e := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCast, To: name, Op: "seq",
					Payload: body, TS: time.Now().UnixMilli()}
				data, _ := json.Marshal(e)
				if _, err := js.Publish(subject, data); err == nil {
					atomic.AddInt64(&stored, 1)
				}
				// A publish failure leaves a gap at this seq; it was never stored, so
				// it is excluded from the no-loss denominator (stored) by design.
				if pace > 0 {
					time.Sleep(pace)
				}
			}
		}()
	}
	wg.Wait()
	return atomic.LoadInt64(&stored), atomic.LoadInt64(&attempted)
}

// drainDurable waits for the durable thrall to consume the stream, polling its stats
// until every stored cast is delivered or progress stalls (a genuine loss). It
// returns the final counts.
func drainDurable(nc *nats.Conn, app, name string, stored int) durableResult {
	res := durableResult{stored: stored}
	lastDistinct := -1
	stall := 0
	for stall < soakDrainStall {
		st, ok := trySoakStats(nc, app, name)
		if ok {
			res.distinct = st.Distinct
			res.duplicates = st.Dups
			if st.Distinct >= stored {
				return res
			}
			if st.Distinct == lastDistinct {
				stall++
			} else {
				stall = 0
				lastDistinct = st.Distinct
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return res
}

// resolveSoakConfig turns the profile plus any flag/env overrides into a concrete
// run configuration. Precedence: explicit flag > env var > profile default.
func resolveSoakConfig(t *testing.T) soakConfig {
	t.Helper()

	name := *soakProfileFlag
	if name == "" {
		name = os.Getenv("AETHER_SOAK_PROFILE")
	}
	if name == "" {
		name = "smoke"
	}
	p, ok := soakProfiles[name]
	if !ok {
		t.Fatalf("unknown soak profile %q (want smoke|default|overnight)", name)
	}
	cfg := soakConfig{profile: name, duration: p.duration, loadWorkers: p.loadWorkers, seed: p.defaultSeed}

	if *soakDurationFlag > 0 {
		cfg.duration = *soakDurationFlag
	} else if env := os.Getenv("AETHER_SOAK_DURATION"); env != "" {
		d, err := time.ParseDuration(env)
		if err != nil {
			t.Fatalf("AETHER_SOAK_DURATION %q: %v", env, err)
		}
		cfg.duration = d
	}

	if *soakSeedFlag != 0 {
		cfg.seed = *soakSeedFlag
	} else if env := os.Getenv("AETHER_SOAK_SEED"); env != "" {
		s, err := strconv.ParseInt(env, 10, 64)
		if err != nil {
			t.Fatalf("AETHER_SOAK_SEED %q: %v", env, err)
		}
		cfg.seed = s
	}

	cfg.reportPath = *soakReportFlag
	if cfg.reportPath == "" {
		cfg.reportPath = os.Getenv("AETHER_SOAK_REPORT")
	}
	return cfg
}

// --- the soak test ---

func TestSoak(t *testing.T) {
	cfg := resolveSoakConfig(t)
	t.Logf("soak: profile=%s duration=%s workers=%d seed=%d",
		cfg.profile, cfg.duration, cfg.loadWorkers, cfg.seed)

	const app = "soak"
	durSpec := spec("probe-dur", "permanent", "local")
	durSpec.Durable = true

	eth := startEmbedded(t)
	startLord(t, eth, soakManifest(t, app, spec("probe", "permanent", "local"), durSpec))
	nc := eth.Conn()
	waitReady(t, eth, "probe")
	waitReady(t, eth, "probe-dur")

	report := soak.Report{
		Profile:  cfg.profile,
		Duration: cfg.duration,
		Seed:     cfg.seed,
		Bars:     soak.DefaultBars(),
	}

	// Run the sustained-load and durable-no-loss phases together for the whole window:
	// a concurrent call stream (latency bars) against `probe` and a durable cast stream
	// (zero-loss bar) against `probe-dur`.
	rec := soak.NewLatencyRecorder(cfg.duration, soakTrendWindows)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration)

	var load loadResult
	var durStored, durAttempted int64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		load = runLoad(ctx, nc, app, []string{"probe"}, cfg.loadWorkers, cfg.seed, rec)
	}()
	go func() {
		defer wg.Done()
		durStored, durAttempted = runDurable(ctx, nc, app, "probe-dur", soakDurableWorkers, soakDurablePace)
	}()
	wg.Wait()
	cancel()

	report.CallCount = int(load.calls)
	report.LatP99 = load.p99
	report.LatMax = load.max
	report.LatP99First = load.trend.First
	report.LatP99Last = load.trend.Last
	if load.errors > 0 {
		t.Logf("sustained load: %d/%d calls errored", load.errors, load.calls)
	}

	// Wait for the durable stream to drain, then record delivered vs stored.
	dur := drainDurable(nc, app, "probe-dur", int(durStored))
	report.Published = dur.stored
	report.Distinct = dur.distinct
	report.Duplicates = dur.duplicates
	if durAttempted != durStored {
		t.Logf("durable: %d/%d publishes failed (gaps, excluded from no-loss)", durAttempted-durStored, durAttempted)
	}

	// The leak section is filled in the next step before the verdict.
	finishSoak(t, report)
}

// finishSoak formats the report, logs it and fails the run on any bar breach (a
// non-zero exit, per the story). Later steps extend it to also persist the report.
func finishSoak(t *testing.T, report soak.Report) {
	t.Helper()
	t.Logf("\n%s", report.Format())
	if b := report.Breaches(); len(b) > 0 {
		t.Fatalf("soak bars breached:\n  %s", strings.Join(b, "\n  "))
	}
}
