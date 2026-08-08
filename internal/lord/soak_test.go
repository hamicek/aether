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
	"encoding/json"
	"flag"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/soak"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
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

// soakState is the soak probe's state: a set of seen sequence numbers for idempotent
// (at-least-once) counting, plus a duplicate tally and the observed range. Held
// behind a pointer so the serialized-mailbox handlers mutate it in place.
type soakState struct {
	seen     map[int]bool
	distinct int
	dups     int
	min, max int
}

// record folds one cast sequence number into the state: first sight counts as
// distinct, a re-delivery counts as a duplicate. This is what lets the suite assert
// zero loss (distinct == published) while tolerating finite duplicates.
func (s *soakState) record(seq int) {
	if s.seen[seq] {
		s.dups++
		return
	}
	s.seen[seq] = true
	if s.distinct == 0 {
		s.min, s.max = seq, seq
	} else {
		if seq < s.min {
			s.min = seq
		}
		if seq > s.max {
			s.max = seq
		}
	}
	s.distinct++
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
			return &soakState{seen: make(map[int]bool)}, nil
		},
		HandleCall: map[string]thrall.CallFn[*soakState]{
			"getlat": func(_ json.RawMessage, s *soakState, _ *thrall.Ctx) (any, *soakState, error) {
				return s.distinct, s, nil
			},
			"stats": func(_ json.RawMessage, s *soakState, _ *thrall.Ctx) (any, *soakState, error) {
				return map[string]int{"distinct": s.distinct, "dups": s.dups, "min": s.min, "max": s.max}, s, nil
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

// soakCast publishes a cast with a JSON payload (the AE-003 `cast` helper only sends
// an empty body).
func soakCast(t *testing.T, nc *nats.Conn, app, name, op string, payload any) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s.%s payload: %v", name, op, err)
	}
	e := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCast, To: name, Op: op,
		Payload: body, TS: time.Now().UnixMilli()}
	data, _ := json.Marshal(e)
	if err := nc.Publish(wire.Cast(app, name), data); err != nil {
		t.Fatalf("cast %s.%s: %v", name, op, err)
	}
	_ = nc.Flush()
}

type probeStats struct {
	Distinct int `json:"distinct"`
	Dups     int `json:"dups"`
	Min      int `json:"min"`
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
	t.Logf("soak: profile=%s duration=%s seed=%d", cfg.profile, cfg.duration, cfg.seed)

	const app = "soak"
	eth := startEmbedded(t)
	startLord(t, eth, soakManifest(t, app, spec("probe", "permanent", "local")))
	nc := eth.Conn()
	waitReady(t, eth, "probe")

	// Scaffold check: the soak probe's idempotent counting works end to end - two
	// distinct sequences and one duplicate. The load/durable/leak phases build on
	// this in the following steps.
	soakCast(t, nc, app, "probe", "seq", map[string]int{"seq": 1})
	soakCast(t, nc, app, "probe", "seq", map[string]int{"seq": 1}) // duplicate
	soakCast(t, nc, app, "probe", "seq", map[string]int{"seq": 2})

	var st probeStats
	waitFor(t, 2*time.Second, "soak probe to record two distinct sequences", func() bool {
		var ok bool
		st, ok = trySoakStats(nc, app, "probe")
		return ok && st.Distinct == 2
	})
	if st.Dups != 1 {
		t.Fatalf("idempotent counting: expected 1 duplicate, got %d", st.Dups)
	}

	report := soak.Report{
		Profile:  cfg.profile,
		Duration: cfg.duration,
		Seed:     cfg.seed,
		Bars:     soak.DefaultBars(),
	}
	// Later steps fill the load, durable and leak sections before formatting.
	t.Logf("\n%s", report.Format())
	if b := report.Breaches(); len(b) != 0 {
		t.Fatalf("unexpected breaches in scaffold run: %v", b)
	}
}
