package lord

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/sdk/go/schema"
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
	// Schema is an optional path to a JSON Schema (relative to the manifest). When set, the edge
	// validates the request body against it and rejects a malformed body with a 400 before the
	// message reaches the ether. Empty = today's "is it valid JSON" check only.
	Schema string `toml:"schema"`

	// schemaJSON is the schema's content, read and compile-checked at manifest load (so a bad
	// schema fails fast) and inlined into the runtime spec - the edge process does not re-read it.
	schemaJSON string
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
	// EventLogDedupWindowMs bounds how long JetStream remembers a Nats-Msg-Id for dedup. 0 uses
	// wire.DefaultEventLogDedupWindowMs. Raise it if command-key idempotence must survive slow
	// retries (the trade-off is a larger dedup index).
	EventLogDedupWindowMs int64 `toml:"event_log_dedup_window_ms"`

	// Fencing opts the thrall out of lord-liveness fencing when set to false (a *bool so an unset
	// field defaults to on). By default every thrall self-terminates when it can no longer verify
	// its lord's KV lease (AE-031); on a shared external bus a KV hiccup then reaps the whole tree.
	// Set fencing = false for a thrall whose orphan is harmless (a stateless / read-only poller) so
	// a bus blip does not take it down - at the cost that it MAY outlive its lord. It does not
	// affect singleton fencing (a singleton still self-exits on losing its lock).
	Fencing *bool `toml:"fencing"`

	// EventLogUse declares what the event log is for, so the lord can turn the retention footgun
	// (a bound truncates a replayed log - there is no snapshot) from a warning into an invariant:
	// "rebuild" + a retention bound is a config error (fail-fast); "audit" + a bound is fine (no
	// warning). Unset keeps today's warning. Values: "" | "rebuild" | "audit".
	EventLogUse string `toml:"event_log_use"`
}

// fencingEnabled reports whether the thrall takes part in lord-liveness fencing (the default). An
// unset field (nil) means on; only an explicit fencing = false opts out.
func fencingEnabled(spec ThrallSpec) bool { return spec.Fencing == nil || *spec.Fencing }

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
	if err := m.loadEdgeSchemas(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return &m, nil
}

// loadEdgeSchemas resolves each edge route's optional schema path (relative to the manifest),
// reads it, and compile-checks it, so a missing or invalid schema fails the load rather than the
// first request or a spawned edge process. The content is inlined into the route for transport.
func (m *Manifest) loadEdgeSchemas(dir string) error {
	for i := range m.Edge.HTTP {
		spec := &m.Edge.HTTP[i]
		for key, route := range spec.Routes {
			if route.Schema == "" {
				continue
			}
			content, err := os.ReadFile(filepath.Join(dir, route.Schema))
			if err != nil {
				return fmt.Errorf("edge %q route %q: schema: %w", spec.Name, key, err)
			}
			if _, err := schema.Compile(content); err != nil {
				return fmt.Errorf("edge %q route %q: schema %q: %w", spec.Name, key, route.Schema, err)
			}
			route.schemaJSON = string(content)
			spec.Routes[key] = route // map values are copies; write the filled route back
		}
	}
	return nil
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
	// The [nats] section owns its own validity (mode enum, leaf constraints); the ether package
	// is the single place that knows it, so we delegate rather than duplicate the rules here.
	if err := m.Nats.Validate(); err != nil {
		return err
	}
	// A leaf spoke exports its data plane as aether.<app>.>, so the top-level app must be present -
	// caught here at load rather than later when the embedded server is built.
	if m.Nats.Leaf != nil && m.App == "" {
		return fmt.Errorf("nats.leaf requires a top-level app (the leaf exports aether.<app>.>)")
	}
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

		// event_log_use declares intent so a retention bound on a rebuild log is a config error,
		// not a silent-corruption warning nobody reads.
		switch t.EventLogUse {
		case "", "rebuild", "audit":
		default:
			return fmt.Errorf("thrall %q: event_log_use = %q is invalid (use \"rebuild\" or \"audit\")", t.Name, t.EventLogUse)
		}
		if t.EventLogUse != "" && !t.EventLog {
			return fmt.Errorf("thrall %q: event_log_use requires event_log = true", t.Name)
		}
		if t.EventLogUse == "rebuild" && (t.EventLogMaxMsgs > 0 || t.EventLogMaxAgeMs > 0) {
			return fmt.Errorf("thrall %q: event_log_use = \"rebuild\" with a retention bound (event_log_max_msgs / event_log_max_age_ms) truncates the rebuild - there is no snapshot; drop the bound, or set event_log_use = \"audit\" if the log is not a rebuild source", t.Name)
		}
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
