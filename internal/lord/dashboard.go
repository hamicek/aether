package lord

// The observer dashboard is a read-only web view of the supervision tree, served on the lord's
// own HTTP server (the one that serves /metrics). It consumes signals the lord already holds -
// the children, their status and self-metrics, and the lifecycle event stream - so it needs no
// SDK change and works the same embedded or external. It deliberately does NOT chart metrics
// over time (that stays the domain of Prometheus/Grafana, which /metrics feeds); it shows the
// live tree, current values and the event stream. There are no control actions - it is a viewer.

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// dashboardTree is the read-only snapshot the dashboard renders: the app, its supervision
// strategy and the thralls the lord currently supervises.
type dashboardTree struct {
	App      string            `json:"app"`
	Strategy string            `json:"strategy"`
	NatsMode string            `json:"nats_mode"`
	LordID   string            `json:"lord_id"`
	Thralls  []dashboardThrall `json:"thralls"`
}

// dashboardThrall is one node in the tree: its topology (from the spec) plus its live status
// and self-metrics.
type dashboardThrall struct {
	Name     string               `json:"name"`
	Status   string               `json:"status"`
	Scope    string               `json:"scope"`
	Restart  string               `json:"restart"`
	Replicas int                  `json:"replicas"`
	Durable  bool                 `json:"durable"`
	EventLog bool                 `json:"event_log"`
	Dynamic  bool                 `json:"dynamic"`
	Live     bool                 `json:"live"`
	Metrics  thrallMetricSnapshot `json:"metrics"`
}

// treeSnapshot builds the read-only view from the lord's in-memory state: the manifest, the
// current children (under the children lock) and the metric snapshot. Defaults are filled so the
// UI never has to (an empty scope is "local", an empty restart is "permanent").
func (l *Lord) treeSnapshot() dashboardTree {
	metrics := l.metrics.snapshot()

	l.childrenMu.RLock()
	defer l.childrenMu.RUnlock()

	thralls := make([]dashboardThrall, 0, len(l.children))
	for _, ch := range l.children {
		scope := ch.spec.Scope
		if scope == "" {
			scope = "local"
		}
		restart := ch.spec.Restart
		if restart == "" {
			restart = "permanent"
		}
		m := metrics[ch.spec.Name] // zero value until the thrall reports
		thralls = append(thralls, dashboardThrall{
			Name:     ch.spec.Name,
			Status:   m.Status,
			Scope:    scope,
			Restart:  restart,
			Replicas: ch.spec.Replicas,
			Durable:  ch.spec.Durable,
			EventLog: ch.spec.EventLog,
			Dynamic:  ch.dynamic,
			Live:     ch.live.Load(),
			Metrics:  m,
		})
	}

	return dashboardTree{
		App:      l.manifest.App,
		Strategy: l.manifest.Strategy,
		NatsMode: l.manifest.Nats.Mode,
		LordID:   l.id,
		Thralls:  thralls,
	}
}

// treeHandler serves the supervision-tree snapshot as JSON for the dashboard's initial render
// and its refresh-on-event.
func (l *Lord) treeHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(l.treeSnapshot()); err != nil {
		l.log.Error("dashboard tree render failed", slog.Any("err", err))
	}
}
