//go:build scadaspike

// SCADA spike bench harness (AE-014). Gated behind the `scadaspike` build tag so normal
// `go test ./...` never compiles it. Run it explicitly:
//
//	mise exec go@latest -- go test -tags scadaspike -run TestSpike -v -timeout 5m ./examples/scada-spike/
//
// It brings up an embedded ether, runs the site thrall in-process (the real thrall
// code via siteDef), drives telemetry from a Go load generator (so Bun is not the
// bottleneck), and measures three things for the report: image throughput + ceiling,
// snapshot call latency, and alarm latency. Numbers are logged, not asserted as a
// pass/fail gate - a spike measures reality.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

const (
	benchApp       = "scada"
	benchSite      = "site"
	benchThreshold = 100.0
)

// harness holds the running bus and a client connection for load and measurement.
type harness struct {
	eth *ether.Ether
	nc  *nats.Conn
	st  *site
}

func setup(t *testing.T) *harness {
	t.Helper()
	eth, err := ether.Start(context.Background(), ether.Config{Mode: "embedded"})
	if err != nil {
		t.Fatalf("ether start: %v", err)
	}
	// The in-process thrall reads its wiring from the environment, exactly as a
	// spawned OS process would.
	os.Setenv("AETHER_NATS_URL", eth.URL())
	os.Setenv("AETHER_APP", benchApp)
	os.Setenv("AETHER_NAME", benchSite)

	st := newSite(benchThreshold)
	go func() { _ = thrall.Start(siteDef(benchSite, st)) }()

	nc, err := nats.Connect(eth.URL())
	if err != nil {
		eth.Stop()
		t.Fatalf("client connect: %v", err)
	}

	h := &harness{eth: eth, nc: nc, st: st}
	// Wait until the site answers a stats call - it is then subscribed and ready.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := h.callStats(200 * time.Millisecond); err == nil {
			break
		}
		if time.Now().After(deadline) {
			h.stop()
			t.Fatal("site did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return h
}

func (h *harness) stop() {
	if h.nc != nil {
		h.nc.Drain()
	}
	if h.eth != nil {
		h.eth.Stop()
	}
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// castTele publishes one telemetry sample as a wire cast envelope.
func (h *harness) castTele(t tele) error {
	e := wire.Envelope{V: 1, Kind: wire.KindCast, To: benchSite, Op: "tele", Payload: mustJSON(t)}
	return h.nc.Publish(wire.Cast(benchApp, benchSite), mustJSON(e))
}

// call issues a wire call and returns the reply payload.
func (h *harness) call(op string, timeout time.Duration) (json.RawMessage, error) {
	req := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: benchSite, Op: op}
	msg, err := h.nc.Request(wire.Call(benchApp, benchSite), mustJSON(req), timeout)
	if err != nil {
		return nil, err
	}
	var reply wire.Envelope
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return nil, err
	}
	if reply.Status == "error" {
		return nil, fmt.Errorf("call %s error", op)
	}
	return reply.Payload, nil
}

func (h *harness) callStats(timeout time.Duration) (stats, error) {
	payload, err := h.call("stats", timeout)
	if err != nil {
		return stats{}, err
	}
	var s stats
	err = json.Unmarshal(payload, &s)
	return s, err
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

// TestSpike runs the full measurement and logs a machine-readable block for REPORT.md.
func TestSpike(t *testing.T) {
	h := setup(t)
	defer h.stop()

	t.Log("=== SCADA SPIKE MEASUREMENT ===")

	ceiling := h.measureThroughput(t)
	h.measureSnapshotLatency(t)
	h.measureAlarmLatency(t)

	t.Logf("RESULT ceiling_values_per_sec=%d", ceiling)
}

// measureThroughput sweeps target rates and finds the ceiling: the highest sustained
// rate with no sequence gaps and stable processing latency. Returns the ceiling.
func (h *harness) measureThroughput(t *testing.T) int {
	rates := []int{1000, 2000, 5000, 10000, 20000, 50000}
	const window = 2 * time.Second

	ceiling := 0
	for _, rate := range rates {
		before, err := h.callStats(time.Second)
		if err != nil {
			t.Fatalf("stats before: %v", err)
		}
		sent := h.pump(rate, window)
		time.Sleep(200 * time.Millisecond) // let the mailbox settle
		after, err := h.callStats(time.Second)
		if err != nil {
			t.Fatalf("stats after: %v", err)
		}

		recv := after.Received - before.Received
		gaps := after.Gaps - before.Gaps
		var avgProcUs float64
		if d := after.Received - before.Received; d > 0 {
			avgProcUs = float64(after.SumProcNs-before.SumProcNs) / float64(d) / 1000
		}
		achieved := float64(recv) / window.Seconds()
		ok := gaps == 0
		t.Logf("THROUGHPUT target=%d sent=%d recv=%d achieved=%.0f/s gaps=%d avg_proc_us=%.1f max_proc_us=%.1f ok=%v",
			rate, sent, recv, achieved, gaps, avgProcUs, float64(after.MaxProcNs)/1000, ok)
		if ok {
			ceiling = rate
		} else {
			break // first rate with drops - ceiling is the previous clean rate
		}
	}
	return ceiling
}

// pump sends telemetry at ~rate values/s for the given duration, round-robin over
// tags, and returns how many it sent. Paced in 10ms slices to approximate the rate.
func (h *harness) pump(rate int, dur time.Duration) int {
	const tags = 10
	perSlice := rate / 100 // 10ms slices -> 100 per second
	if perSlice < 1 {
		perSlice = 1
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var seq [tags]uint64
	sent := 0
	deadline := time.Now().Add(dur)
	k := 0
	for range ticker.C {
		for i := 0; i < perSlice; i++ {
			tag := k % tags
			seq[tag]++
			_ = h.castTele(tele{
				Tag:   fmt.Sprintf("tag%d", tag),
				Value: 40 + float64(k%20),
				Seq:   seq[tag],
				TsNs:  nowNs(),
			})
			sent++
			k++
		}
		if time.Now().After(deadline) {
			break
		}
	}
	return sent
}

// measureSnapshotLatency measures the snapshot call RTT under a steady background load.
func (h *harness) measureSnapshotLatency(t *testing.T) {
	stop := make(chan struct{})
	var running atomic.Bool
	running.Store(true)
	go func() {
		var seq uint64
		for running.Load() {
			seq++
			_ = h.castTele(tele{Tag: "bg", Value: 50, Seq: seq, TsNs: nowNs()})
			time.Sleep(time.Millisecond) // ~1000/s background
		}
		close(stop)
	}()

	const n = 300
	lat := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		t0 := time.Now()
		if _, err := h.call("snapshot", time.Second); err != nil {
			t.Fatalf("snapshot call: %v", err)
		}
		lat = append(lat, time.Since(t0))
		time.Sleep(5 * time.Millisecond)
	}
	running.Store(false)
	<-stop

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	t.Logf("SNAPSHOT n=%d p50_us=%.0f p99_us=%.0f max_us=%.0f",
		n, float64(pct(lat, 0.50).Microseconds()), float64(pct(lat, 0.99).Microseconds()),
		float64(lat[len(lat)-1].Microseconds()))
}

// measureAlarmLatency measures end-to-end latency from sending a threshold-breaching
// sample to receiving the alarm event, over many unique probe tags.
func (h *harness) measureAlarmLatency(t *testing.T) {
	type ev struct {
		tag string
		at  time.Time
	}
	got := make(chan ev, 256)
	sub, err := h.nc.Subscribe(alarmSubject, func(m *nats.Msg) {
		var a alarm
		if json.Unmarshal(m.Data, &a) == nil {
			got <- ev{tag: a.Tag, at: time.Now()}
		}
	})
	if err != nil {
		t.Fatalf("alarm subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	const n = 100
	lat := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		tag := fmt.Sprintf("alarmprobe%d", i) // unique tag -> fresh below->above cross
		t0 := time.Now()
		_ = h.castTele(tele{Tag: tag, Value: benchThreshold + 50, Seq: 1, TsNs: nowNs()})
		select {
		case e := <-got:
			if e.tag == tag {
				lat = append(lat, e.at.Sub(t0))
			}
		case <-time.After(time.Second):
			t.Fatalf("alarm %s not received in time", tag)
		}
		time.Sleep(2 * time.Millisecond)
	}

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	t.Logf("ALARM n=%d p50_us=%.0f p99_us=%.0f max_us=%.0f",
		len(lat), float64(pct(lat, 0.50).Microseconds()), float64(pct(lat, 0.99).Microseconds()),
		float64(lat[len(lat)-1].Microseconds()))
}
