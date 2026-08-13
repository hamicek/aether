package lord

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/BurntSushi/toml"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/obs"
)

// Manifest = the contents of aether.toml (the supervision tree topology).
type Manifest struct {
	App              string        `toml:"app"`
	Strategy         string        `toml:"strategy"` // one_for_one | one_for_all | rest_for_one
	RestartIntensity Intensity     `toml:"restart_intensity"`
	Nats             ether.Config  `toml:"nats"`
	Observability    Observability `toml:"observability"`
	Liveness         Liveness      `toml:"liveness"`
	Thralls          []ThrallSpec  `toml:"thrall"`
	Edge             Edge          `toml:"edge"`
}

// Observability configures the runtime's telemetry. An empty MetricsAddr keeps the
// Prometheus endpoint off (opt-in), so plain runs stay free of an open HTTP port.
type Observability struct {
	MetricsAddr string `toml:"metrics_addr"` // host:port for the Prometheus /metrics endpoint (empty = disabled)
	// Dashboard serves the read-only observer dashboard (live supervision tree + event stream)
	// on the same HTTP server as /metrics; it therefore requires MetricsAddr. Off by default.
	Dashboard bool `toml:"dashboard"`
}

// Liveness tunes how fast a hung-but-alive thrall is detected. The interval is propagated to the
// thralls (they heartbeat at it) and the lord derives its reaper threshold from the same values,
// so the two never drift. Defaults preserve the historical 2s / 3 misses (~6s).
type Liveness struct {
	HeartbeatIntervalMs int `toml:"heartbeat_interval_ms"` // how often a thrall heartbeats (default 2000)
	StaleAfterMisses    int `toml:"stale_after_misses"`    // missed intervals before "stale" (default 3)
}

// Intensity = the restart-intensity window (max restarts within a given time).
type Intensity struct {
	Max      int `toml:"max"`
	WithinMs int `toml:"within_ms"`
}

// Edge groups the built-in edge servers: processes whose input arrives from outside the ether
// (a push, e.g. HTTP) rather than from a mailbox (a pull). Each edge is spawned and supervised
// as an ordinary thrall; the runtime that translates the external protocol into ether call/cast
// is built into the aether binary (the internal `_edge` subcommand), so it does not multiply
// across the language SDKs.
type Edge struct {
	HTTP []EdgeHTTPSpec `toml:"http"` // [[edge.http]] - HTTP ingress servers
}

// EdgeHTTPSpec = one HTTP ingress server. It binds a single OS port (singleton fit) and maps
// HTTP routes to a thrall operation over the ether.
type EdgeHTTPSpec struct {
	Name string `toml:"name"` // unique name; the edge is supervised under it
	Addr string `toml:"addr"` // host:port to bind (e.g. ":8080")
	// Routes maps a "METHOD /path" key to a target thrall operation, e.g.
	//   route."POST /increment" = { thrall = "counter", op = "increment", kind = "cast" }
	Routes map[string]EdgeRoute `toml:"route"`
}

// EdgeRoute = a single HTTP route's target on the ether.
type EdgeRoute struct {
	Thrall string `toml:"thrall"` // target thrall name
	Op     string `toml:"op"`     // operation invoked on the thrall
	Kind   string `toml:"kind"`   // call (wait for reply -> body) | cast (fire-and-forget -> 202); default call
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

// LoadManifest reads, parses, fills in the defaults and validates aether.toml.
func LoadManifest(path string) (*Manifest, error) {
	var m Manifest
	if _, err := toml.DecodeFile(path, &m); err != nil {
		return nil, err
	}
	m.applyDefaults()
	if err := m.validate(); err != nil {
		return nil, err
	}
	return &m, nil
}

func (m *Manifest) applyDefaults() {
	if m.Strategy == "" {
		m.Strategy = "one_for_one"
	}
	if m.Nats.Mode == "" {
		m.Nats.Mode = "embedded"
	}
	// Liveness: clamp the interval (non-positive -> default, too-small -> floor) and default the
	// miss count, so a missing or nonsensical [liveness] section keeps today's 2s / 3-miss behaviour.
	m.Liveness.HeartbeatIntervalMs = obs.ClampHeartbeatIntervalMs(m.Liveness.HeartbeatIntervalMs)
	if m.Liveness.StaleAfterMisses <= 0 {
		m.Liveness.StaleAfterMisses = 3
	}
	for i := range m.Thralls {
		if m.Thralls[i].Restart == "" {
			m.Thralls[i].Restart = "permanent"
		}
		if m.Thralls[i].Scope == "" {
			m.Thralls[i].Scope = "local"
		}
	}
	// Edge routes default to call (request/reply -> body); cast is opt-in per route.
	for i := range m.Edge.HTTP {
		for key, r := range m.Edge.HTTP[i].Routes {
			if r.Kind == "" {
				r.Kind = "call"
				m.Edge.HTTP[i].Routes[key] = r
			}
		}
	}
}

// validate rejects a structurally parseable but semantically broken manifest. It runs after
// applyDefaults, so defaulted fields (route kind, thrall scope) are already in place. A returned
// error fails `aether up` loudly, the same as an unparseable manifest.
func (m *Manifest) validate() error {
	// Names must be unique across thralls and edge servers - they share the supervision namespace
	// (registry key, control subject, fencing) and a collision would cross their wires.
	seen := make(map[string]string) // name -> where it was first defined
	for _, t := range m.Thralls {
		if t.Name == "" {
			return fmt.Errorf("thrall with empty name")
		}
		if where, dup := seen[t.Name]; dup {
			return fmt.Errorf("duplicate name %q (already used by %s)", t.Name, where)
		}
		seen[t.Name] = "thrall"
	}
	addrs := make(map[string]string) // addr -> edge server that bound it first
	for _, e := range m.Edge.HTTP {
		if e.Name == "" {
			return fmt.Errorf("edge.http server with empty name")
		}
		if where, dup := seen[e.Name]; dup {
			return fmt.Errorf("duplicate name %q (already used by %s)", e.Name, where)
		}
		seen[e.Name] = "edge.http"
		if e.Addr == "" {
			return fmt.Errorf("edge.http %q: empty addr", e.Name)
		}
		// Two servers on one addr would both parse but the second crash-loops on net.Listen; reject early.
		if other, dup := addrs[e.Addr]; dup {
			return fmt.Errorf("edge.http %q: addr %q already used by %q", e.Name, e.Addr, other)
		}
		addrs[e.Addr] = e.Name
		if len(e.Routes) == 0 {
			return fmt.Errorf("edge.http %q: no routes", e.Name)
		}
		for key, r := range e.Routes {
			if err := validateRouteKey(key); err != nil {
				return fmt.Errorf("edge.http %q: route %q: %w", e.Name, key, err)
			}
			if r.Thrall == "" || r.Op == "" {
				return fmt.Errorf("edge.http %q: route %q: thrall and op are required", e.Name, key)
			}
			if r.Kind != "call" && r.Kind != "cast" {
				return fmt.Errorf("edge.http %q: route %q: kind must be call or cast, got %q", e.Name, key, r.Kind)
			}
		}
	}
	return nil
}

// validateRouteKey checks a route key of the form "METHOD /path" (e.g. "POST /increment").
func validateRouteKey(key string) error {
	method, path, ok := strings.Cut(key, " ")
	if !ok {
		return fmt.Errorf("expected \"METHOD /path\"")
	}
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
		http.MethodPatch, http.MethodDelete, http.MethodOptions:
	default:
		return fmt.Errorf("unsupported HTTP method %q", method)
	}
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("path must start with \"/\"")
	}
	return nil
}
