package lord

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/wire"
)

// newTestLord builds a Lord with just the fields the metric path needs - no NATS. The
// registry/HTTP surface is independent of the bus, which is exactly the point (it works
// the same embedded or external).
func newTestLord() *Lord {
	return &Lord{log: obs.NewLogger(), metrics: newLordMetrics()}
}

func scrape(t *testing.T, l *Lord) string {
	t.Helper()
	srv := httptest.NewServer(l.metricsHandler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func TestMetricsEndpointExposesBaseline(t *testing.T) {
	l := newTestLord()
	out := scrape(t, l)
	for _, want := range []string{
		"aether_up 1",
		"# TYPE aether_thralls gauge",
		`aether_thralls{status="ready"} 0`,
		"# TYPE aether_restarts_total counter",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("/metrics missing %q\n---\n%s", want, out)
		}
	}
}

func TestMetricsReflectStatusAndRestarts(t *testing.T) {
	l := newTestLord()
	l.metrics.setStatus("worker", "ready")
	l.metrics.setStatus("logger", "ready")
	l.metrics.setStatus("flaky", "down")
	l.metrics.incRestart("flaky")
	l.metrics.incRestart("flaky")
	l.metrics.incGaveUp("flaky")

	out := scrape(t, l)
	for _, want := range []string{
		`aether_thralls{status="ready"} 2`,
		`aether_thralls{status="down"} 1`,
		`aether_restarts_total{name="flaky"} 2`,
		`aether_gave_up_total{name="flaky"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("/metrics missing %q\n---\n%s", want, out)
		}
	}
}

func TestForgetDropsThrallFromStatusGauge(t *testing.T) {
	l := newTestLord()
	l.metrics.setStatus("dyn", "ready")
	if out := scrape(t, l); !strings.Contains(out, `aether_thralls{status="ready"} 1`) {
		t.Fatalf("setup: ready count not 1\n%s", out)
	}
	l.metrics.forget("dyn")
	if out := scrape(t, l); !strings.Contains(out, `aether_thralls{status="ready"} 0`) {
		t.Errorf("forget did not drop the thrall\n%s", out)
	}
}

// TestForgetDeletesPerThrallSeries covers the leak fix: a stopped dynamic thrall's per-name
// series must not linger in the exposition with a frozen value.
func TestForgetDeletesPerThrallSeries(t *testing.T) {
	l := newTestLord()
	l.metrics.setStatus("dyn", "ready")
	l.metrics.incRestart("dyn")
	l.metrics.recordHeartbeat("dyn", wire.HeartbeatMetrics{MailboxDepth: 5, MailboxLatencyMs: 2, ProcessedTotal: 7})
	if out := scrape(t, l); !strings.Contains(out, `aether_mailbox_depth{name="dyn"} 5`) ||
		!strings.Contains(out, `aether_restarts_total{name="dyn"} 1`) {
		t.Fatalf("setup: per-name series missing\n%s", out)
	}

	l.metrics.forget("dyn")

	out := scrape(t, l)
	for _, series := range []string{
		`aether_mailbox_depth{name="dyn"}`,
		`aether_mailbox_latency_ms{name="dyn"}`,
		`aether_processed_total{name="dyn"}`,
		`aether_restarts_total{name="dyn"}`,
	} {
		if strings.Contains(out, series) {
			t.Errorf("forget left behind %q\n%s", series, out)
		}
	}
}

// TestStartMetricsServerReportsBindFailure covers the fail-fast fix: an unusable metrics_addr
// surfaces as an error from Start, not a silently-dead endpoint.
func TestStartMetricsServerReportsBindFailure(t *testing.T) {
	l := newTestLord()
	if err := l.startMetricsServer("127.0.0.1:notaport"); err == nil {
		t.Error("expected an error for an invalid metrics_addr, got nil")
	}
	// An empty address is the opt-out and must not error.
	if err := l.startMetricsServer(""); err != nil {
		t.Errorf("empty metrics_addr should be a no-op, got %v", err)
	}
}

// TestMetricsSnapshotReflectsRecordedValues covers the dashboard read-back layer: the raw
// snapshot must mirror what the metric mutators recorded, without parsing the Prometheus text.
func TestMetricsSnapshotReflectsRecordedValues(t *testing.T) {
	lm := newLordMetrics()
	lm.setStatus("worker", "ready")
	lm.incRestart("worker")
	lm.incRestart("worker")
	lm.incGaveUp("worker")
	lm.incHeartbeatMiss("worker")
	lm.recordHeartbeat("worker", wire.HeartbeatMetrics{MailboxDepth: 3, MailboxLatencyMs: 1.5, ProcessedTotal: 100})
	lm.recordBacklog("worker", 7)

	w, ok := lm.snapshot()["worker"]
	if !ok {
		t.Fatal("worker missing from snapshot")
	}
	if w.Status != "ready" {
		t.Errorf("status = %q, want ready", w.Status)
	}
	if w.Restarts != 2 || w.GaveUp != 1 || w.HeartbeatMiss != 1 {
		t.Errorf("counters = restarts %d/gaveUp %d/hbMiss %d, want 2/1/1", w.Restarts, w.GaveUp, w.HeartbeatMiss)
	}
	if w.MailboxDepth != 3 || w.MailboxLatMs != 1.5 || w.Processed != 100 {
		t.Errorf("heartbeat metrics = %+v, want depth 3 / lat 1.5 / processed 100", w)
	}
	if w.DurableBacklog != 7 {
		t.Errorf("durable backlog = %v, want 7", w.DurableBacklog)
	}
}

// TestMetricsSnapshotForgetsThrall covers churn cleanup: a forgotten thrall must leave no raw
// snapshot entry (mirroring the per-name series deletion in the exposition).
func TestMetricsSnapshotForgetsThrall(t *testing.T) {
	lm := newLordMetrics()
	lm.setStatus("gone", "ready")
	lm.incRestart("gone")
	lm.forget("gone")
	if _, ok := lm.snapshot()["gone"]; ok {
		t.Error("forgotten thrall still present in snapshot")
	}
}

// TestMetricsSnapshotIncludesProcStats covers the RSS/CPU read-back for the dashboard.
func TestMetricsSnapshotIncludesProcStats(t *testing.T) {
	lm := newLordMetrics()
	lm.recordProcStats("worker", 52<<20, 12.5) // 52 MB, 12.5% CPU
	w := lm.snapshot()["worker"]
	if w.RSSBytes != 52<<20 || w.CPUPercent != 12.5 {
		t.Errorf("procstats snapshot = rss %d / cpu %v, want %d / 12.5", w.RSSBytes, w.CPUPercent, int64(52<<20))
	}
	lm.forget("worker")
	if _, ok := lm.snapshot()["worker"]; ok {
		t.Error("forgotten thrall still in snapshot after recordProcStats")
	}
}
