package lord

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hamicek/aether/internal/obs"
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
