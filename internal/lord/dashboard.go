package lord

// The observer dashboard is a read-only web view of the supervision tree, served on the lord's
// own HTTP server (the one that serves /metrics). It consumes signals the lord already holds -
// the children, their status and self-metrics, and the lifecycle event stream - so it needs no
// SDK change and works the same embedded or external. It deliberately does NOT chart metrics
// over time (that stays the domain of Prometheus/Grafana, which /metrics feeds); it shows the
// live tree, current values and the event stream. There are no control actions - it is a viewer.

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// dashboardHTML is the self-contained observer page (inline CSS/JS, no external assets), embedded
// into the binary so the lord can serve it in a closed/offline environment.
//
//go:embed dashboard.html
var dashboardHTML []byte

// dashboardPageHandler serves the embedded observer page at "/" and 404s any other path (the
// dashboard is a single page; /metrics, /api/tree and /events are the only other routes).
func (l *Lord) dashboardPageHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(dashboardHTML)
	}
}

// startEventForwarder subscribes to the lifecycle event stream and fans each event out to the
// dashboard clients over the SSE hub. The subscription is dropped when the lord's context ends.
// It runs only when the dashboard is enabled (l.sse != nil).
func (l *Lord) startEventForwarder() error {
	sub, err := l.ether.Conn().Subscribe(wire.Events, func(m *nats.Msg) {
		l.sse.broadcast(m.Data)
	})
	if err != nil {
		return fmt.Errorf("dashboard event stream: %w", err)
	}
	go func() {
		<-l.appCtx.Done()
		_ = sub.Unsubscribe()
	}()
	return nil
}

// sseHub fans lifecycle events out to the connected dashboard clients. Each client is a buffered
// channel; a client too slow to keep up has the message dropped rather than stalling the
// broadcast (a monitoring view tolerates a missed event, not a stalled lord).
type sseHub struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

func newSSEHub() *sseHub {
	return &sseHub{clients: map[chan []byte]struct{}{}}
}

// register adds a client and returns its channel. unregister must be called to release it.
func (h *sseHub) register() chan []byte {
	ch := make(chan []byte, 16)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *sseHub) unregister(ch chan []byte) {
	h.mu.Lock()
	if _, ok := h.clients[ch]; ok {
		delete(h.clients, ch)
		close(ch)
	}
	h.mu.Unlock()
}

// broadcast delivers a message to every client, non-blocking. register/unregister and broadcast
// share the lock, so a client is never sent to after it is closed.
func (h *sseHub) broadcast(msg []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default: // slow client - drop this event for it, keep the others flowing
		}
	}
}

// eventsHandler streams lifecycle events to one dashboard client over Server-Sent Events. It
// registers with the hub and forwards each event until the client disconnects.
func (l *Lord) eventsHandler(w http.ResponseWriter, r *http.Request) {
	if l.sse == nil {
		http.Error(w, "dashboard disabled", http.StatusServiceUnavailable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := l.sse.register()
	defer l.sse.unregister(ch)
	flusher.Flush() // flush the headers so the client's EventSource opens at once

	for {
		select {
		case <-r.Context().Done():
			return
		case <-l.appCtx.Done(): // lord shutting down: end the stream promptly, don't wait out Shutdown's grace
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			_, _ = w.Write([]byte("data: "))
			_, _ = w.Write(msg)
			_, _ = w.Write([]byte("\n\n"))
			flusher.Flush()
		}
	}
}

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
	// Metadata is the operator-declared deployment tags from the manifest (site, PLC, criticality).
	Metadata map[string]string `json:"metadata,omitempty"`
	// ExpectedVersion is the build the operator declared should run here (manifest); compared against
	// the reported Metrics.Describe.Version to surface a rollout mismatch.
	ExpectedVersion string `json:"expected_version,omitempty"`
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
			Name:            ch.spec.Name,
			Status:          m.Status,
			Scope:           scope,
			Restart:         restart,
			Replicas:        ch.spec.Replicas,
			Durable:         ch.spec.Durable,
			EventLog:        ch.spec.EventLog,
			Dynamic:         ch.dynamic,
			Live:            ch.live.Load(),
			Metrics:         m,
			Metadata:        ch.spec.Metadata,
			ExpectedVersion: ch.spec.ExpectedVersion,
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
