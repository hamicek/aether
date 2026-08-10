package obs

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// Metrics is a minimal in-process metric registry that renders Prometheus text exposition.
// It deliberately avoids a client library: the runtime needs only counters and gauges with
// a small label set, and hand-rolling the exposition keeps the dependency surface thin and
// the model exporter-agnostic (an OTLP bridge could read the same registry later).
//
// A metric is identified by its name plus its label set; the same name may carry many label
// combinations (one series each). All operations are safe for concurrent use.
type Metrics struct {
	mu     sync.Mutex
	kinds  map[string]metricKind // name -> counter|gauge (+ help), set on first registration
	help   map[string]string
	series map[string]*series // series key -> value + labels
}

type metricKind int

const (
	kindCounter metricKind = iota
	kindGauge
)

func (k metricKind) String() string {
	if k == kindGauge {
		return "gauge"
	}
	return "counter"
}

// series is one labelled sample of a metric.
type series struct {
	name   string
	labels map[string]string
	value  float64
}

// NewMetrics creates an empty registry.
func NewMetrics() *Metrics {
	return &Metrics{
		kinds:  map[string]metricKind{},
		help:   map[string]string{},
		series: map[string]*series{},
	}
}

// Counter registers (once) a counter metric with help text. Registering the same name twice
// is a no-op, so callers can declare metrics wherever they are first used.
func (m *Metrics) Counter(name, help string) {
	m.register(name, kindCounter, help)
}

// Gauge registers (once) a gauge metric with help text.
func (m *Metrics) Gauge(name, help string) {
	m.register(name, kindGauge, help)
}

func (m *Metrics) register(name string, kind metricKind, help string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.kinds[name]; ok {
		return
	}
	m.kinds[name] = kind
	m.help[name] = help
}

// Inc adds 1 to the counter series identified by name+labels.
func (m *Metrics) Inc(name string, labels map[string]string) { m.Add(name, labels, 1) }

// Add adds delta to the series identified by name+labels (creating it at zero first).
func (m *Metrics) Add(name string, labels map[string]string, delta float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.at(name, labels).value += delta
}

// Set assigns an absolute value to the gauge series identified by name+labels.
func (m *Metrics) Set(name string, labels map[string]string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.at(name, labels).value = value
}

// at returns the series for name+labels, creating it at zero if absent. Caller holds mu.
func (m *Metrics) at(name string, labels map[string]string) *series {
	key := seriesKey(name, labels)
	s, ok := m.series[key]
	if !ok {
		s = &series{name: name, labels: cloneLabels(labels)}
		m.series[key] = s
	}
	return s
}

// WriteText renders the registry in Prometheus text exposition format, with metrics and
// their series in a stable (sorted) order so the output is deterministic.
func (m *Metrics) WriteText(w io.Writer) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	names := make([]string, 0, len(m.kinds))
	for name := range m.kinds {
		names = append(names, name)
	}
	sort.Strings(names)

	byName := map[string][]*series{}
	for _, s := range m.series {
		byName[s.name] = append(byName[s.name], s)
	}

	for _, name := range names {
		if help := m.help[name]; help != "" {
			if _, err := fmt.Fprintf(w, "# HELP %s %s\n", name, help); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "# TYPE %s %s\n", name, m.kinds[name]); err != nil {
			return err
		}
		lines := byName[name]
		sort.Slice(lines, func(i, j int) bool {
			return formatLabels(lines[i].labels) < formatLabels(lines[j].labels)
		})
		for _, s := range lines {
			if _, err := fmt.Fprintf(w, "%s%s %s\n", name, formatLabels(s.labels), formatValue(s.value)); err != nil {
				return err
			}
		}
	}
	return nil
}

func seriesKey(name string, labels map[string]string) string {
	return name + formatLabels(labels)
}

// formatLabels renders a label set as `{k="v",...}` with keys sorted, or "" when empty. Label
// values are escaped per the exposition format (backslash, quote, newline).
func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(labels[k]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func escapeLabelValue(v string) string {
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(v)
}

// formatValue renders a float without a trailing ".0" for integers, matching Prometheus habits.
func formatValue(v float64) string {
	if v == float64(int64(v)) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}

func cloneLabels(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}
