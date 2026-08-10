package obs

import (
	"bytes"
	"strings"
	"testing"
)

func render(t *testing.T, m *Metrics) string {
	t.Helper()
	var buf bytes.Buffer
	if err := m.WriteText(&buf); err != nil {
		t.Fatalf("WriteText: %v", err)
	}
	return buf.String()
}

func TestCounterAndGaugeExposition(t *testing.T) {
	m := NewMetrics()
	m.Counter("aether_restarts_total", "thrall restarts")
	m.Gauge("aether_thralls", "live thralls by status")

	m.Inc("aether_restarts_total", map[string]string{"name": "worker"})
	m.Inc("aether_restarts_total", map[string]string{"name": "worker"})
	m.Set("aether_thralls", map[string]string{"status": "ready"}, 3)

	out := render(t, m)
	for _, want := range []string{
		"# TYPE aether_restarts_total counter",
		`aether_restarts_total{name="worker"} 2`,
		"# TYPE aether_thralls gauge",
		`aether_thralls{status="ready"} 3`,
		"# HELP aether_restarts_total thrall restarts",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition missing %q\n---\n%s", want, out)
		}
	}
}

func TestSetOverwritesGauge(t *testing.T) {
	m := NewMetrics()
	m.Gauge("aether_durable_backlog", "pending casts")
	labels := map[string]string{"name": "q"}
	m.Set("aether_durable_backlog", labels, 10)
	m.Set("aether_durable_backlog", labels, 4)

	if out := render(t, m); !strings.Contains(out, `aether_durable_backlog{name="q"} 4`) {
		t.Errorf("Set did not overwrite: %s", out)
	}
}

func TestSeparateSeriesPerLabelSet(t *testing.T) {
	m := NewMetrics()
	m.Counter("aether_restarts_total", "restarts")
	m.Inc("aether_restarts_total", map[string]string{"name": "a"})
	m.Add("aether_restarts_total", map[string]string{"name": "b"}, 5)

	out := render(t, m)
	if !strings.Contains(out, `aether_restarts_total{name="a"} 1`) ||
		!strings.Contains(out, `aether_restarts_total{name="b"} 5`) {
		t.Errorf("label sets not tracked separately:\n%s", out)
	}
}

func TestDeterministicOrdering(t *testing.T) {
	m := NewMetrics()
	m.Counter("aether_restarts_total", "restarts")
	m.Inc("aether_restarts_total", map[string]string{"name": "z"})
	m.Inc("aether_restarts_total", map[string]string{"name": "a"})

	first := render(t, m)
	if second := render(t, m); first != second {
		t.Errorf("output not deterministic:\n%q\nvs\n%q", first, second)
	}
	// series sorted by labels -> "a" precedes "z"
	if strings.Index(first, `name="a"`) > strings.Index(first, `name="z"`) {
		t.Errorf("series not sorted by labels:\n%s", first)
	}
}

func TestLabelValueEscaping(t *testing.T) {
	m := NewMetrics()
	m.Counter("aether_errors_total", "errors")
	m.Inc("aether_errors_total", map[string]string{"msg": `a "b"\c`})
	if out := render(t, m); !strings.Contains(out, `msg="a \"b\"\\c"`) {
		t.Errorf("label value not escaped: %s", out)
	}
}

func TestFloatValueFormatting(t *testing.T) {
	m := NewMetrics()
	m.Gauge("aether_mailbox_latency_ms", "latency")
	m.Set("aether_mailbox_latency_ms", map[string]string{"name": "w"}, 1.5)
	if out := render(t, m); !strings.Contains(out, `aether_mailbox_latency_ms{name="w"} 1.5`) {
		t.Errorf("float not formatted: %s", out)
	}
}
