package lord

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/wire"
)

// Runtime metric names exposed on /metrics. Kept together so the exposition schema is
// visible in one place and mirrored by the docs.
const (
	metricUp             = "aether_up"
	metricThralls        = "aether_thralls"
	metricRestarts       = "aether_restarts_total"
	metricGaveUp         = "aether_gave_up_total"
	metricHeartbeatMiss  = "aether_heartbeat_misses_total"
	metricMailboxDepth   = "aether_mailbox_depth"
	metricMailboxLat     = "aether_mailbox_latency_ms"
	metricProcessed      = "aether_processed_total"
	metricDurableBacklog = "aether_durable_backlog"
)

// knownStatuses is the closed set of thrall statuses the lord assigns; the thralls gauge
// carries one series per status so a status that drops to zero still reports 0 (rather than
// vanishing, which a scraper would read as "no data").
var knownStatuses = []string{"starting", "ready", "down", "stale"}

// perThrallMetrics are the metrics carrying a {name=...} label. When a dynamic thrall is
// stopped its series in these are deleted, so a long-lived lord with spawn/stop churn does not
// accumulate frozen series (memory and scrape-cardinality growth).
var perThrallMetrics = []string{
	metricRestarts, metricGaveUp, metricHeartbeatMiss,
	metricMailboxDepth, metricMailboxLat, metricProcessed, metricDurableBacklog,
}

// lordMetrics owns the runtime metric registry and the derived per-status gauge. It tracks
// each thrall's last status so the gauge can be recomputed on every change.
type lordMetrics struct {
	reg *obs.Metrics

	mu     sync.Mutex
	status map[string]string // thrall name -> last status
}

func newLordMetrics() *lordMetrics {
	reg := obs.NewMetrics()
	reg.Gauge(metricUp, "1 while the lord is running")
	reg.Gauge(metricThralls, "thralls by current status")
	reg.Counter(metricRestarts, "thrall restarts")
	reg.Counter(metricGaveUp, "thralls the lord gave up restarting")
	reg.Counter(metricHeartbeatMiss, "detected heartbeat misses")
	reg.Gauge(metricMailboxDepth, "thrall mailbox depth (pending messages)")
	reg.Gauge(metricMailboxLat, "thrall mailbox handler latency in milliseconds")
	reg.Counter(metricProcessed, "messages processed by a thrall")
	reg.Gauge(metricDurableBacklog, "pending casts in a thrall's durable stream")

	reg.Set(metricUp, nil, 1)
	lm := &lordMetrics{reg: reg, status: map[string]string{}}
	lm.recompute()
	return lm
}

// setStatus records a thrall's new status and refreshes the per-status gauge.
func (lm *lordMetrics) setStatus(name, status string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.status[name] = status
	lm.recompute()
}

// forget drops a stopped thrall from the status tracking and deletes its per-name series, so
// nothing lingers in the exposition after the thrall is gone.
func (lm *lordMetrics) forget(name string) {
	lm.mu.Lock()
	delete(lm.status, name)
	lm.recompute()
	lm.mu.Unlock()

	labels := map[string]string{"name": name}
	for _, mname := range perThrallMetrics {
		lm.reg.Delete(mname, labels)
	}
}

// recompute sets the thralls gauge to the current per-status counts. Caller holds mu.
func (lm *lordMetrics) recompute() {
	counts := map[string]int{}
	for _, s := range lm.status {
		counts[s]++
	}
	for _, st := range knownStatuses {
		lm.reg.Set(metricThralls, map[string]string{"status": st}, float64(counts[st]))
	}
}

func (lm *lordMetrics) incRestart(name string) {
	lm.reg.Inc(metricRestarts, map[string]string{"name": name})
}

func (lm *lordMetrics) incGaveUp(name string) {
	lm.reg.Inc(metricGaveUp, map[string]string{"name": name})
}

func (lm *lordMetrics) incHeartbeatMiss(name string) {
	lm.reg.Inc(metricHeartbeatMiss, map[string]string{"name": name})
}

// recordHeartbeat folds a thrall's self-reported mailbox metrics into the registry. The
// processed total is cumulative and reported as an absolute value (it resets when the thrall
// restarts, like any process-local counter).
func (lm *lordMetrics) recordHeartbeat(name string, hm wire.HeartbeatMetrics) {
	labels := map[string]string{"name": name}
	lm.reg.Set(metricMailboxDepth, labels, float64(hm.MailboxDepth))
	lm.reg.Set(metricMailboxLat, labels, hm.MailboxLatencyMs)
	lm.reg.Set(metricProcessed, labels, float64(hm.ProcessedTotal))
}

// pollDurableBacklog periodically samples the pending-message count of each durable thrall's
// JetStream consumer and exposes it as a gauge. This is the accurate backlog (server-side
// num_pending), complementing the thrall's own best-effort mailbox depth.
func (l *Lord) pollDurableBacklog() {
	if l.backlogPollEvery <= 0 {
		return
	}
	t := time.NewTicker(l.backlogPollEvery)
	defer t.Stop()
	for {
		select {
		case <-l.appCtx.Done():
			return
		case <-t.C:
			l.pollDurableBacklogOnce()
		}
	}
}

// pollDurableBacklogOnce reads num_pending for every durable child's consumer. A consumer that
// does not exist yet (the thrall has not subscribed) is skipped, not reported as zero.
func (l *Lord) pollDurableBacklogOnce() {
	js, err := l.ether.Conn().JetStream()
	if err != nil {
		return
	}
	for _, ch := range l.durableChildren() {
		stream := wire.Stream(l.manifest.App, ch.spec.Name)
		ci, err := js.ConsumerInfo(stream, ch.spec.Name)
		if err != nil {
			continue // consumer not created yet, or stream gone - nothing to report
		}
		l.metrics.reg.Set(metricDurableBacklog, map[string]string{"name": ch.spec.Name}, float64(ci.NumPending))
	}
}

// durableChildren returns the current children with a durable mailbox.
func (l *Lord) durableChildren() []*child {
	l.childrenMu.RLock()
	defer l.childrenMu.RUnlock()
	out := make([]*child, 0, len(l.children))
	for _, ch := range l.children {
		if ch.spec.Durable {
			out = append(out, ch)
		}
	}
	return out
}

// metricsHandler builds the HTTP handler serving the Prometheus /metrics endpoint from the
// current registry. Extracted so it can be tested without binding a real port.
func (l *Lord) metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := l.metrics.reg.WriteText(w); err != nil {
			l.log.Error("metrics render failed", slog.Any("err", err))
		}
	})
	return mux
}

// startMetricsServer starts the Prometheus /metrics endpoint when an address is configured.
// The listener is bound synchronously so a misconfigured or occupied metrics_addr fails the
// lord's start loudly, rather than silently disabling metrics from a detached goroutine. The
// endpoint is the lord's own HTTP server, independent of the NATS mode, so it works the same
// embedded or external. It is shut down on Stop via stopMetricsServer.
func (l *Lord) startMetricsServer(addr string) error {
	if addr == "" {
		return nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics endpoint %q: %w", addr, err)
	}
	srv := &http.Server{Handler: l.metricsHandler(), ReadHeaderTimeout: 5 * time.Second}
	l.httpSrv = srv
	go func() {
		l.log.Info("metrics endpoint listening", slog.String("addr", addr), slog.String("path", "/metrics"))
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			l.log.Error("metrics endpoint failed", slog.Any("err", err))
		}
	}()
	return nil
}

func (l *Lord) stopMetricsServer() {
	if l.httpSrv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = l.httpSrv.Shutdown(ctx)
}
