package lord

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/wire"
)

// newDashboardLord builds a Lord with a manifest and a couple of children (one static, one
// dynamic) plus recorded metrics - enough to exercise the dashboard read path without NATS.
func newDashboardLord() *Lord {
	l := newTestLord()
	l.id = "lord-1"
	l.manifest = &Manifest{App: "demo", Strategy: "one_for_one", Nats: ether.Config{Mode: "embedded"}}

	static := &child{spec: ThrallSpec{Name: "counter", Scope: "local", Restart: "permanent", Durable: true}}
	static.live.Store(true)
	dyn := &child{spec: ThrallSpec{Name: "worker", Scope: "singleton", Replicas: 3, EventLog: true}, dynamic: true}
	l.children = []*child{static, dyn}

	l.metrics.setStatus("counter", "ready")
	l.metrics.recordHeartbeat("counter", wire.HeartbeatMetrics{MailboxDepth: 2, ProcessedTotal: 10})
	l.metrics.setStatus("worker", "stale")
	l.metrics.incRestart("worker")
	return l
}

func TestTreeSnapshotReflectsChildren(t *testing.T) {
	tree := newDashboardLord().treeSnapshot()

	if tree.App != "demo" || tree.Strategy != "one_for_one" || tree.NatsMode != "embedded" || tree.LordID != "lord-1" {
		t.Fatalf("tree header wrong: %+v", tree)
	}
	if len(tree.Thralls) != 2 {
		t.Fatalf("thralls = %d, want 2", len(tree.Thralls))
	}

	byName := map[string]dashboardThrall{}
	for _, th := range tree.Thralls {
		byName[th.Name] = th
	}

	c := byName["counter"]
	if c.Status != "ready" || !c.Durable || c.Dynamic || !c.Live {
		t.Errorf("counter node wrong: %+v", c)
	}
	if c.Scope != "local" || c.Restart != "permanent" {
		t.Errorf("counter defaults wrong: scope=%q restart=%q", c.Scope, c.Restart)
	}
	if c.Metrics.MailboxDepth != 2 || c.Metrics.Processed != 10 {
		t.Errorf("counter metrics wrong: %+v", c.Metrics)
	}

	w := byName["worker"]
	if w.Status != "stale" || w.Scope != "singleton" || !w.EventLog || !w.Dynamic || w.Replicas != 3 {
		t.Errorf("worker node wrong: %+v", w)
	}
	if w.Metrics.Restarts != 1 {
		t.Errorf("worker restarts = %d, want 1", w.Metrics.Restarts)
	}
}

func TestTreeHandlerServesJSON(t *testing.T) {
	l := newDashboardLord()
	rec := httptest.NewRecorder()
	l.treeHandler(rec, httptest.NewRequest(http.MethodGet, "/api/tree", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("content-type = %q", ct)
	}
	var tree dashboardTree
	if err := json.Unmarshal(rec.Body.Bytes(), &tree); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if len(tree.Thralls) != 2 {
		t.Errorf("thralls in JSON = %d, want 2", len(tree.Thralls))
	}
}
