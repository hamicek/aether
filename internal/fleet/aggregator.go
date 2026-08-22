package fleet

import (
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/hamicek/aether/internal/wire"
	"github.com/nats-io/nats.go"
)

// defaultStaleMultiple is how many publish intervals may pass with no update before a node is
// considered stale (mirrors the liveness "misses" idea: a couple of missed beats, not one).
const defaultStaleMultiple = 3

// NodeView is one node's line in the fleet snapshot: its last health summary plus derived liveness.
type NodeView struct {
	Health
	Stale    bool  `json:"stale"`     // no update within staleMultiple publish intervals
	LastSeen int64 `json:"last_seen"` // unix millis of the last summary received
}

// Aggregator holds a live view of the fleet, keyed by (app, lord_id), assembled from the health
// summaries lords publish. It is transport-agnostic at its core (Ingest/Snapshot); Subscribe wires
// it to a NATS connection. Staleness is derived per node from that node's own published interval, so
// a slow-publishing node is not flagged as often as a fast one.
type Aggregator struct {
	staleMultiple int
	now           func() time.Time // injectable for tests

	mu    sync.Mutex
	nodes map[string]record
}

type record struct {
	health   Health
	lastSeen time.Time
}

// NewAggregator returns an empty aggregator with the default staleness multiple and wall clock.
func NewAggregator() *Aggregator {
	return &Aggregator{staleMultiple: defaultStaleMultiple, now: time.Now, nodes: map[string]record{}}
}

func key(app, lordID string) string { return app + "\x00" + lordID }

// Ingest records a health summary, stamping it with the current time as its last-seen.
func (a *Aggregator) Ingest(h Health) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nodes[key(h.App, h.LordID)] = record{health: h, lastSeen: a.now()}
}

// Snapshot returns the current fleet, sorted by (app, lord_id), with staleness computed now.
func (a *Aggregator) Snapshot() []NodeView {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	out := make([]NodeView, 0, len(a.nodes))
	for _, r := range a.nodes {
		out = append(out, NodeView{
			Health:   r.health,
			Stale:    a.isStale(r, now),
			LastSeen: r.lastSeen.UnixMilli(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].App != out[j].App {
			return out[i].App < out[j].App
		}
		return out[i].LordID < out[j].LordID
	})
	return out
}

// isStale reports whether a node has missed staleMultiple of its own publish intervals. A missing
// or nonsensical interval falls back to 5s so a bad summary cannot make a node look permanently live.
func (a *Aggregator) isStale(r record, now time.Time) bool {
	interval := time.Duration(r.health.IntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return now.Sub(r.lastSeen) > time.Duration(a.staleMultiple)*interval
}

// Subscribe wires the aggregator to a bus: it subscribes to every lord's health subject and ingests
// each summary. A malformed message is skipped, never fatal.
func (a *Aggregator) Subscribe(nc *nats.Conn) (*nats.Subscription, error) {
	return nc.Subscribe(wire.FleetHealthAll(), func(msg *nats.Msg) {
		var h Health
		if json.Unmarshal(msg.Data, &h) == nil {
			a.Ingest(h)
		}
	})
}
