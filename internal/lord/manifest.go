package lord

import (
	"github.com/BurntSushi/toml"

	"github.com/hamicek/aether/internal/ether"
)

// Manifest = the contents of aether.toml (the supervision tree topology).
type Manifest struct {
	App              string       `toml:"app"`
	Strategy         string       `toml:"strategy"` // one_for_one | one_for_all | rest_for_one
	RestartIntensity Intensity    `toml:"restart_intensity"`
	Nats             ether.Config `toml:"nats"`
	Thralls          []ThrallSpec `toml:"thrall"`
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
