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
	metricThrallRSS      = "aether_thrall_rss_bytes"
	metricThrallCPU      = "aether_thrall_cpu_percent"
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
	metricThrallRSS, metricThrallCPU,
}

// thrallRaw holds the latest raw metric values per thrall, kept alongside the Prometheus
// registry so the dashboard can read them back as structured data without parsing the text
// exposition. Guarded by lordMetrics.mu.
type thrallRaw struct {
	mailboxDepth   int
	mailboxLatMs   float64
	processed      uint64
	durableBacklog float64
	restarts       uint64
	gaveUp         uint64
	heartbeatMiss  uint64
	rssBytes       int64
	cpuPercent     float64
}

// thrallMetricSnapshot is the read-only per-thrall view the observer dashboard consumes.
type thrallMetricSnapshot struct {
	Status         string               `json:"status"`
	MailboxDepth   int                  `json:"mailbox_depth"`
	MailboxLatMs   float64              `json:"mailbox_latency_ms"`
	Processed      uint64               `json:"processed"`
	DurableBacklog float64              `json:"durable_backlog"`
	Restarts       uint64               `json:"restarts"`
	GaveUp         uint64               `json:"gave_up"`
	HeartbeatMiss  uint64               `json:"heartbeat_misses"`
	RSSBytes       int64                `json:"rss_bytes"`
	CPUPercent     float64              `json:"cpu_percent"`
	Describe       *wire.ThrallDescribe `json:"describe,omitempty"`
}

// lordMetrics owns the runtime metric registry and the derived per-status gauge. It tracks each
// thrall's last status (for the gauge) and its raw values (for the dashboard snapshot).
type lordMetrics struct {
	reg *obs.Metrics

	// labelsFor returns the label set for a per-thrall metric series - the name plus any allowlisted
	// metadata labels the lord projects. The lord sets it after construction; it is nil in metric-only
	// unit tests, where the bare name label is the whole set. It must be stable for a given name (the
	// same set is used to write and to delete a series), which holds: the allowlist and a thrall's
	// metadata are fixed for its lifetime.
	labelsFor func(name string) map[string]string

	mu       sync.Mutex
	status   map[string]string               // thrall name -> last status
	raw      map[string]*thrallRaw           // thrall name -> latest raw values
	describe map[string]*wire.ThrallDescribe // thrall name -> latest self-description
}

// labels returns the label set for a per-thrall metric series (see labelsFor).
func (lm *lordMetrics) labels(name string) map[string]string {
	if lm.labelsFor != nil {
		return lm.labelsFor(name)
	}
	return map[string]string{"name": name}
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
	reg.Gauge(metricThrallRSS, "thrall resident memory in bytes (summed over its process group)")
	reg.Gauge(metricThrallCPU, "thrall CPU usage in percent (summed over its process group)")

	reg.Set(metricUp, nil, 1)
	lm := &lordMetrics{reg: reg, status: map[string]string{}, raw: map[string]*thrallRaw{},
		describe: map[string]*wire.ThrallDescribe{}}
	lm.recompute()
	return lm
}

// rawFor returns the raw record for a thrall, creating it on first use. Caller holds mu.
func (lm *lordMetrics) rawFor(name string) *thrallRaw {
	r := lm.raw[name]
	if r == nil {
		r = &thrallRaw{}
		lm.raw[name] = r
	}
	return r
}

// snapshot returns the read-only per-thrall metric view for the dashboard, keyed by name.
func (lm *lordMetrics) snapshot() map[string]thrallMetricSnapshot {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	names := make(map[string]struct{}, len(lm.status)+len(lm.raw))
	for n := range lm.status {
		names[n] = struct{}{}
	}
	for n := range lm.raw {
		names[n] = struct{}{}
	}
	out := make(map[string]thrallMetricSnapshot, len(names))
	for n := range names {
		s := thrallMetricSnapshot{Status: lm.status[n]}
		if r := lm.raw[n]; r != nil {
			s.MailboxDepth = r.mailboxDepth
			s.MailboxLatMs = r.mailboxLatMs
			s.Processed = r.processed
			s.DurableBacklog = r.durableBacklog
			s.Restarts = r.restarts
			s.GaveUp = r.gaveUp
			s.HeartbeatMiss = r.heartbeatMiss
			s.RSSBytes = r.rssBytes
			s.CPUPercent = r.cpuPercent
		}
		s.Describe = lm.describe[n]
		out[n] = s
	}
	return out
}

// describeFor returns the last self-description reported by a thrall, or nil if it has reported
// none yet. The returned value is immutable (each heartbeat replaces the pointer), so the caller
// may read it without holding the lock.
func (lm *lordMetrics) describeFor(name string) *wire.ThrallDescribe {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.describe[name]
}

// setStatus records a thrall's new status and refreshes the per-status gauge.
func (lm *lordMetrics) setStatus(name, status string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.status[name] = status
	lm.recompute()
}

// forget drops a stopped thrall from the status tracking and deletes its per-name series, so
// nothing lingers in the exposition (or the dashboard snapshot) after the thrall is gone.
func (lm *lordMetrics) forget(name string) {
	lm.mu.Lock()
	delete(lm.status, name)
	delete(lm.raw, name)
	delete(lm.describe, name)
	lm.recompute()
	lm.mu.Unlock()

	labels := lm.labels(name)
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
	lm.mu.Lock()
	lm.rawFor(name).restarts++
	lm.mu.Unlock()
	lm.reg.Inc(metricRestarts, lm.labels(name))
}

func (lm *lordMetrics) incGaveUp(name string) {
	lm.mu.Lock()
	lm.rawFor(name).gaveUp++
	lm.mu.Unlock()
	lm.reg.Inc(metricGaveUp, lm.labels(name))
}

func (lm *lordMetrics) incHeartbeatMiss(name string) {
	lm.mu.Lock()
	lm.rawFor(name).heartbeatMiss++
	lm.mu.Unlock()
	lm.reg.Inc(metricHeartbeatMiss, lm.labels(name))
}

// recordHeartbeat folds a thrall's self-reported mailbox metrics into the registry and the raw
// snapshot. The processed total is cumulative and reported as an absolute value (it resets when
// the thrall restarts, like any process-local counter).
func (lm *lordMetrics) recordHeartbeat(name string, hm wire.HeartbeatMetrics) {
	lm.mu.Lock()
	r := lm.rawFor(name)
	r.mailboxDepth = hm.MailboxDepth
	r.mailboxLatMs = hm.MailboxLatencyMs
	r.processed = hm.ProcessedTotal
	// Keep the last self-description. A heartbeat that carries none (an older SDK) must not erase
	// what an earlier beat reported, so only a present describe replaces the stored one.
	if hm.Describe != nil {
		lm.describe[name] = hm.Describe
	}
	lm.mu.Unlock()

	labels := lm.labels(name)
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
		l.metrics.recordBacklog(ch.spec.Name, float64(ci.NumPending))
	}
}

// recordBacklog stores a durable thrall's pending-message count in both the registry and the raw
// snapshot.
func (lm *lordMetrics) recordBacklog(name string, pending float64) {
	lm.mu.Lock()
	lm.rawFor(name).durableBacklog = pending
	lm.mu.Unlock()
	lm.reg.Set(metricDurableBacklog, lm.labels(name), pending)
}

// recordProcStats stores a thrall's process-group resident memory (bytes) and CPU usage (percent)
// in both the registry and the raw snapshot.
func (lm *lordMetrics) recordProcStats(name string, rssBytes int64, cpuPercent float64) {
	lm.mu.Lock()
	r := lm.rawFor(name)
	r.rssBytes = rssBytes
	r.cpuPercent = cpuPercent
	lm.mu.Unlock()

	labels := lm.labels(name)
	lm.reg.Set(metricThrallRSS, labels, float64(rssBytes))
	lm.reg.Set(metricThrallCPU, labels, cpuPercent)
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

// metricsHandler builds the HTTP handler for the lord's observability server: always /metrics,
// plus the read-only dashboard routes (/, /api/tree, /events) when it is enabled. Extracted so
// it can be tested without binding a real port.
func (l *Lord) metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := l.metrics.reg.WriteText(w); err != nil {
			l.log.Error("metrics render failed", slog.Any("err", err))
		}
	})
	if l.manifest != nil && l.manifest.Observability.Dashboard {
		mux.HandleFunc("/api/tree", l.treeHandler)
		mux.HandleFunc("/events", l.eventsHandler)
		mux.Handle("/", l.dashboardPageHandler())
	}
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
		if l.manifest != nil && l.manifest.Observability.Dashboard {
			l.log.Info("observer dashboard listening", slog.String("addr", addr), slog.String("path", "/"))
		}
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
