package lord

import (
	"github.com/BurntSushi/toml"

	"github.com/hamicek/aether/internal/ether"
)

// Manifest = the contents of aether.toml (the supervision tree topology).
type Manifest struct {
	App              string        `toml:"app"`
	Strategy         string        `toml:"strategy"` // one_for_one | one_for_all | rest_for_one
	RestartIntensity Intensity     `toml:"restart_intensity"`
	Nats             ether.Config  `toml:"nats"`
	Observability    Observability `toml:"observability"`
	Thralls          []ThrallSpec  `toml:"thrall"`
}

// Observability configures the runtime's telemetry. An empty MetricsAddr keeps the
// Prometheus endpoint off (opt-in), so plain runs stay free of an open HTTP port.
type Observability struct {
	MetricsAddr string `toml:"metrics_addr"` // host:port for the Prometheus /metrics endpoint (empty = disabled)
}

// Intensity = the restart-intensity window (max restarts within a given time).
type Intensity struct {
	Max      int `toml:"max"`
	WithinMs int `toml:"within_ms"`
}

// ThrallSpec = a single child in the manifest.
type ThrallSpec struct {
	Name     string `toml:"name"`
	Cmd      string `toml:"cmd"`
	Restart  string `toml:"restart"`  // permanent | transient | temporary
	Scope    string `toml:"scope"`    // local | singleton
	Replicas int    `toml:"replicas"` // >1 -> queue group (a pool of workers)
	Durable  bool   `toml:"durable"`  // true -> casts go through JetStream (survive a crash)

	// EventLog opts the thrall into an event-sourcing log: a separate RETENTION stream the
	// thrall appends events to and replays in init to rebuild state. Independent of Durable.
	EventLog         bool  `toml:"event_log"`
	EventLogMaxMsgs  int64 `toml:"event_log_max_msgs"`   // 0 = unbounded (message count)
	EventLogMaxAgeMs int64 `toml:"event_log_max_age_ms"` // 0 = unbounded (age)
}

// LoadManifest reads, parses and fills in the defaults of aether.toml.
func LoadManifest(path string) (*Manifest, error) {
	var m Manifest
	if _, err := toml.DecodeFile(path, &m); err != nil {
		return nil, err
	}
	m.applyDefaults()
	return &m, nil
}

func (m *Manifest) applyDefaults() {
	if m.Strategy == "" {
		m.Strategy = "one_for_one"
	}
	if m.Nats.Mode == "" {
		m.Nats.Mode = "embedded"
	}
	for i := range m.Thralls {
		if m.Thralls[i].Restart == "" {
			m.Thralls[i].Restart = "permanent"
		}
		if m.Thralls[i].Scope == "" {
			m.Thralls[i].Scope = "local"
		}
	}
}
