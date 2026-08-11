// Package lord implements the supervisor: it reads the manifest, spawns thralls as
// OS processes, watches them and restarts them per strategy (one_for_one /
// one_for_all / rest_for_one). Restart decisions go through a SINGLE supervisor loop,
// so there are no races; stale exit events are recognized by the process generation.
package lord

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/registry"
	"github.com/hamicek/aether/internal/singleton"
	"github.com/hamicek/aether/internal/wire"
)

const (
	singletonRetry = 500 * time.Millisecond
	singletonRenew = 1 * time.Second

	// backlogPollInterval is how often the lord samples durable consumer backlogs.
	backlogPollInterval = 2 * time.Second
)

// exit reports the end of one process generation (sent by the watcher).
type exit struct {
	ch       *child
	gen      uint64
	abnormal bool
}

// Lord supervises a set of thralls under a single strategy (a supervision node).
type Lord struct {
	manifest *Manifest
	ether    *ether.Ether
	reg      *registry.Registry
	locks    *singleton.Manager
	children []*child
	id       string
	log      *slog.Logger
	metrics  *lordMetrics
	httpSrv  *http.Server

	exits chan exit // watchers -> supervisor loop; serializes restart decisions

	appCtx     context.Context
	procCtx    context.Context
	procCancel context.CancelFunc
	draining   atomic.Bool

	// childrenMu guards the children slice. Static children are appended once in New;
	// dynamic children (ctx.StartChild) are appended/removed at runtime from the control
	// callback goroutine, so every reader/writer of the slice takes this lock.
	childrenMu sync.RWMutex

	mu       sync.Mutex
	ready    map[string]bool
	stale    map[string]bool      // thralls currently flagged as heartbeat-missed
	lastSeen map[string]time.Time // last heartbeat per thrall (for miss detection)

	// Liveness status writes (ready/stale) are ordered by a version stamped under mu at the
	// moment the decision is made, then applied to the registry outside mu (registry I/O must
	// not run under mu). statusApplied records the last version written per thrall, so a write
	// carrying an older version - reordered by goroutine scheduling - is dropped and the
	// registry converges to the last decision instead of getting stuck on a stale status.
	statusSeq     uint64 // monotonic, incremented under mu
	statusMu      sync.Mutex
	statusApplied map[string]uint64

	// heartbeat miss-detection tuning (defaults from the constants; tests shorten them).
	hbMissAfter  time.Duration // no heartbeat for this long -> stale
	hbCheckEvery time.Duration // how often the reaper checks

	backlogPollEvery time.Duration // how often durable consumer backlogs are sampled
}

// New creates the root lord from a manifest.
func New(m *Manifest, eth *ether.Ether) (*Lord, error) {
	reg, err := registry.Open(eth.Conn())
	if err != nil {
		return nil, err
	}
	locks, err := singleton.Open(eth.Conn())
	if err != nil {
		return nil, err
	}
	procCtx, procCancel := context.WithCancel(context.Background())
	host, _ := os.Hostname()
	// Reaper timing derives from the manifest liveness config, the same interval injected into
	// thralls, so the reaper and thralls never drift. Clamp here too (not only in applyDefaults)
	// so New is robust to a manifest built directly, without LoadManifest.
	intervalMs := obs.ClampHeartbeatIntervalMs(m.Liveness.HeartbeatIntervalMs)
	misses := m.Liveness.StaleAfterMisses
	if misses <= 0 {
		misses = 3
	}
	hbInterval := time.Duration(intervalMs) * time.Millisecond
	l := &Lord{
		manifest:         m,
		ether:            eth,
		reg:              reg,
		locks:            locks,
		id:               fmt.Sprintf("%s-%d", host, os.Getpid()),
		log:              obs.NewLogger().With(slog.String("component", "lord"), slog.String("app", m.App)),
		metrics:          newLordMetrics(),
		exits:            make(chan exit, len(m.Thralls)+8),
		procCtx:          procCtx,
		procCancel:       procCancel,
		ready:            map[string]bool{},
		stale:            map[string]bool{},
		lastSeen:         map[string]time.Time{},
		statusApplied:    map[string]uint64{},
		hbMissAfter:      hbInterval * time.Duration(misses),
		hbCheckEvery:     hbInterval,
		backlogPollEvery: backlogPollInterval,
	}
	for _, spec := range m.Thralls {
		l.children = append(l.children, &child{
			spec:         spec,
			natsURL:      eth.URL(),
			app:          m.App,
			caPath:       m.Nats.TLS.CA,
			nkeySeed:     m.Nats.Auth.NkeySeed,
			hbIntervalMs: intervalMs,
		})
	}
	return l, nil
}

// Start provisions durable mailboxes, starts the supervisor loop, the heartbeat consumer and the thralls.
func (l *Lord) Start(ctx context.Context) error {
	l.appCtx = ctx

	if err := l.startMetricsServer(l.manifest.Observability.MetricsAddr); err != nil {
		return err
	}

	if err := l.provisionStreams(); err != nil {
		return err
	}

	if _, err := l.ether.Conn().Subscribe(wire.HeartbeatAll(), func(m *nats.Msg) {
		l.onHeartbeat(nameFromHB(m.Subject), m.Data)
	}); err != nil {
		return err
	}

	// Inbound control channel: thralls ask the lord to spawn/stop children at runtime.
	if _, err := l.ether.Conn().Subscribe(wire.LordCtl(), func(m *nats.Msg) {
		l.handleControl(m)
	}); err != nil {
		return err
	}

	go l.supervisorLoop()
	go l.reapHeartbeats()
	go l.pollDurableBacklog()

	for _, ch := range l.children {
		if ch.spec.Scope == "singleton" {
			go l.manageSingleton(ch) // its own lifecycle (distributed lock)
		} else if err := l.startChild(ch); err != nil {
			return err
		}
	}
	return nil
}

func (l *Lord) provisionStreams() error {
	for _, ch := range l.children {
		if err := l.provisionChildStreams(ch); err != nil {
			return err
		}
	}
	return nil
}

// provisionChildStreams provisions both the durable mailbox and the event log for one child
// (each opt-in and independent). Used at startup and when a child is spawned at runtime.
func (l *Lord) provisionChildStreams(ch *child) error {
	if err := l.provisionStream(ch); err != nil {
		return err
	}
	return l.provisionEventLog(ch)
}

// provisionEventLog idempotently creates the RETENTION stream backing a thrall's event log
// (opt-in via EventLog). Unlike the WorkQueue mailbox, a Limits stream keeps messages so init
// can replay them. Optional MaxMsgs/MaxAge bound the retention.
func (l *Lord) provisionEventLog(ch *child) error {
	if !ch.spec.EventLog {
		return nil
	}
	js, err := l.ether.Conn().JetStream()
	if err != nil {
		return err
	}
	stream := wire.EventLogStream(l.manifest.App, ch.spec.Name)
	subject := wire.EventLog(l.manifest.App, ch.spec.Name)
	if _, err := js.StreamInfo(stream); err == nil {
		return nil
	}
	cfg := &nats.StreamConfig{
		Name:      stream,
		Subjects:  []string{subject},
		Retention: nats.LimitsPolicy, // retain (replayable), unlike the WorkQueue mailbox
		Storage:   nats.FileStorage,
	}
	if ch.spec.EventLogMaxMsgs > 0 {
		cfg.MaxMsgs = ch.spec.EventLogMaxMsgs
	}
	if ch.spec.EventLogMaxAgeMs > 0 {
		cfg.MaxAge = time.Duration(ch.spec.EventLogMaxAgeMs) * time.Millisecond
	}
	if _, err := js.AddStream(cfg); err != nil {
		return err
	}
	l.log.Info("event log provisioned", slog.String("stream", stream), slog.String("subject", subject))
	return nil
}

// provisionStream idempotently creates the durable mailbox stream for one child. Used
// both at startup and when a durable child is spawned at runtime.
func (l *Lord) provisionStream(ch *child) error {
	if !ch.spec.Durable {
		return nil
	}
	js, err := l.ether.Conn().JetStream()
	if err != nil {
		return err
	}
	stream := wire.Stream(l.manifest.App, ch.spec.Name)
	subject := wire.Cast(l.manifest.App, ch.spec.Name)
	if _, err := js.StreamInfo(stream); err == nil {
		return nil
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:      stream,
		Subjects:  []string{subject},
		Retention: nats.WorkQueuePolicy,
		Storage:   nats.FileStorage,
	}); err != nil {
		return err
	}
	l.log.Info("durable mailbox provisioned", slog.String("stream", stream), slog.String("subject", subject))
	return nil
}

// startChild starts a local thrall and attaches a watcher that reports its exit.
func (l *Lord) startChild(ch *child) error {
	gen, err := ch.spawn(l.procCtx)
	if err != nil {
		return err
	}
	pid := ch.pid()
	l.setStatus(ch.spec.Name, pid, "starting")
	l.log.Info("starting thrall", slog.String("name", ch.spec.Name), slog.Int("pid", pid), slog.String("restart", ch.spec.Restart))
	l.emit("spawned", ch.spec.Name, pid)
	go l.watch(ch, gen)
	return nil
}

// watch waits for the process to end and only reports the exit - supervisorLoop decides on restart.
func (l *Lord) watch(ch *child, gen uint64) {
	abnormal := ch.wait()
	l.mu.Lock()
	l.ready[ch.spec.Name] = false
	l.mu.Unlock()
	l.exits <- exit{ch: ch, gen: gen, abnormal: abnormal}
}

// supervisorLoop is the SINGLE place where restart is decided -> no races.
func (l *Lord) supervisorLoop() {
	for ev := range l.exits {
		if l.stopping() {
			continue
		}
		// Stale event? If the process is already running in a newer generation
		// (some other strategy restarted it), we ignore this exit.
		if ev.gen != ev.ch.currentGen() {
			continue
		}
		// Retired on purpose (StopChild): the exit is expected, not a crash.
		if ev.ch.retired.Load() {
			continue
		}
		l.setStatus(ev.ch.spec.Name, 0, "down")
		l.logExit(ev.ch.spec.Name, ev.abnormal)
		l.emit("down", ev.ch.spec.Name, 0)
		l.handleCrash(ev.ch, ev.abnormal)
	}
}

// handleCrash selects, per strategy, which thralls to restart.
func (l *Lord) handleCrash(ch *child, abnormal bool) {
	action := decide(ch.spec.Restart, l.manifest.Strategy, abnormal)
	// A dynamic child is supervised one_for_one: it never pulls the manifest group
	// (and is itself excluded from localChildren/groupRest), so a group action on it
	// degrades to restarting just itself.
	if ch.dynamic && (action == RestartAll || action == RestartRest) {
		action = RestartOne
	}
	switch action {
	case DontRestart:
		l.log.Info("thrall not restarted", slog.String("name", ch.spec.Name), slog.String("policy", ch.spec.Restart))
	case RestartOne:
		l.restartOne(ch)
	case RestartAll:
		l.restartGroup("one_for_all", l.localChildren(), ch)
	case RestartRest:
		l.restartGroup("rest_for_one", l.groupRest(ch), ch)
	}
}

// restartOne restarts only the crashed thrall (with restart-intensity protection and backoff).
func (l *Lord) restartOne(ch *child) {
	if l.overIntensity(ch) {
		return
	}
	if l.backoff() {
		return
	}
	l.log.Warn("restarting thrall", slog.String("name", ch.spec.Name))
	l.metrics.incRestart(ch.spec.Name)
	l.emit("restarting", ch.spec.Name, 0)
	if err := l.startChild(ch); err != nil {
		l.log.Error("restart failed", slog.String("name", ch.spec.Name), slog.Any("err", err))
	}
}

// restartGroup stops the running siblings (in reverse order) and then restarts the
// whole group in order. It runs inside supervisorLoop, so the whole thing is atomic;
// the exit events of the stopped siblings are then discarded by generation.
func (l *Lord) restartGroup(strategy string, group []*child, crashed *child) {
	if l.overIntensity(crashed) {
		return
	}
	names := make([]string, len(group))
	for i, ch := range group {
		names[i] = ch.spec.Name
	}
	l.log.Warn("group restart", slog.String("strategy", strategy), slog.String("crashed", crashed.spec.Name), slog.String("group", strings.Join(names, " ")))
	l.metrics.incRestart(crashed.spec.Name)
	l.emit("group_restart", crashed.spec.Name, 0)

	nc := l.ether.Conn()
	// 1) stop the running siblings in reverse order (crashed is already dead)
	for i := len(group) - 1; i >= 0; i-- {
		ch := group[i]
		if ch == crashed || !ch.running() {
			continue
		}
		l.log.Info("stopping sibling", slog.String("strategy", strategy), slog.String("name", ch.spec.Name))
		ch.requestDrain(nc, defaultGrace) // blocks until done; its exit event will then be stale
	}

	if l.backoff() {
		return
	}

	// 2) restart the whole group in order
	for _, ch := range group {
		if err := l.startChild(ch); err != nil {
			l.log.Error("restart failed", slog.String("name", ch.spec.Name), slog.Any("err", err))
		}
	}
}

// overIntensity returns true (and gives up the restart) when the thrall exceeded the restart-intensity window.
func (l *Lord) overIntensity(ch *child) bool {
	window := time.Duration(l.manifest.RestartIntensity.WithinMs) * time.Millisecond
	if max := l.manifest.RestartIntensity.Max; max > 0 && window > 0 {
		if n := ch.restartsWithin(window); n > max {
			l.log.Error("restart-intensity exceeded - giving up", slog.String("name", ch.spec.Name), slog.Int("starts", n), slog.Duration("within", window))
			l.metrics.incGaveUp(ch.spec.Name)
			l.emit("gave_up", ch.spec.Name, 0)
			return true
		}
	}
	return false
}

// backoff waits restartBackoff; returns true if shutdown arrived in the meantime.
func (l *Lord) backoff() bool { return l.sleep(restartBackoff) }

// localChildren returns the static non-singleton thralls in manifest order. Singletons
// have their own lifecycle; dynamic children are supervised one_for_one and never
// participate in a manifest group strategy, so both are excluded here.
func (l *Lord) localChildren() []*child {
	l.childrenMu.RLock()
	defer l.childrenMu.RUnlock()
	out := make([]*child, 0, len(l.children))
	for _, ch := range l.children {
		if ch.spec.Scope != "singleton" && !ch.dynamic {
			out = append(out, ch)
		}
	}
	return out
}

// groupRest returns the crashed thrall and all local thralls started AFTER it (rest_for_one).
func (l *Lord) groupRest(crashed *child) []*child {
	locals := l.localChildren()
	for i, ch := range locals {
		if ch == crashed {
			return locals[i:]
		}
	}
	return []*child{crashed}
}

// manageSingleton drives a thrall with scope="singleton" via a distributed KV lock.
func (l *Lord) manageSingleton(ch *child) {
	name := ch.spec.Name
	waiting := false

	for {
		if l.stopping() {
			return
		}
		lock, ok, err := l.locks.TryAcquire(name, l.id)
		if err != nil {
			l.log.Error("singleton lock error", slog.String("name", name), slog.Any("err", err))
			if l.sleep(singletonRetry) {
				return
			}
			continue
		}
		if !ok {
			if !waiting {
				l.log.Info("singleton lock held by another lord - waiting", slog.String("name", name))
				waiting = true
			}
			if l.sleep(singletonRetry) {
				return
			}
			continue
		}
		waiting = false
		l.log.Info("singleton lock acquired - starting", slog.String("name", name), slog.String("lord", l.id))
		l.emit("lock_acquired", name, 0)

		renewStop := make(chan struct{})
		go l.renewLoop(ch, lock, renewStop)

		if _, err := ch.spawn(l.procCtx); err != nil {
			l.log.Error("singleton spawn failed", slog.String("name", name), slog.Any("err", err))
			close(renewStop)
			_ = lock.Release()
			if l.sleep(restartBackoff) {
				return
			}
			continue
		}
		pid := ch.pid()
		l.setStatus(name, pid, "starting")
		l.log.Info("starting thrall", slog.String("name", name), slog.Int("pid", pid), slog.String("scope", "singleton"))
		l.emit("spawned", name, pid)

		abnormal := ch.wait()

		close(renewStop)
		_ = lock.Release()
		l.mu.Lock()
		l.ready[name] = false
		l.mu.Unlock()
		l.logExit(name, abnormal)
		l.emit("lock_released", name, 0)

		if l.stopping() {
			return
		}
		if decide(ch.spec.Restart, l.manifest.Strategy, abnormal) == DontRestart {
			l.log.Info("singleton not restarted", slog.String("name", name), slog.String("policy", ch.spec.Restart))
			return
		}
		if l.sleep(restartBackoff) {
			return
		}
	}
}

func (l *Lord) renewLoop(ch *child, lock *singleton.Lock, stop <-chan struct{}) {
	t := time.NewTicker(singletonRenew)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if err := lock.Renew(); err != nil {
				l.log.Warn("singleton lock lost - terminating local instance", slog.String("name", ch.spec.Name), slog.Any("err", err))
				ch.kill()
				return
			}
		}
	}
}

func (l *Lord) onHeartbeat(name string, data []byte) {
	if name == "" {
		return
	}
	ch := l.childByName(name)
	if ch == nil {
		return
	}
	pid := ch.pid()
	if pid == 0 {
		return
	}
	l.recordHeartbeatMetrics(name, data)
	l.mu.Lock()
	// Announce "ready" on the first heartbeat and again on recovery from a stale outage.
	announce := !l.ready[name] || l.stale[name]
	l.ready[name] = true
	delete(l.stale, name)
	l.lastSeen[name] = time.Now()
	l.statusSeq++
	seq := l.statusSeq
	l.mu.Unlock()
	l.applyStatus(name, pid, "ready", seq)
	if announce {
		l.log.Info("thrall ready (on the bus)", slog.String("name", name), slog.Int("pid", pid))
		l.emit("ready", name, pid)
	}
}

// recordHeartbeatMetrics parses a thrall's self-reported metrics from the heartbeat payload
// and folds them into the registry. A heartbeat without a payload (older SDK, or none) is
// simply ignored - liveness still works from the heartbeat itself.
func (l *Lord) recordHeartbeatMetrics(name string, data []byte) {
	if len(data) == 0 {
		return
	}
	var e wire.Envelope
	if json.Unmarshal(data, &e) != nil || len(e.Payload) == 0 {
		return
	}
	var hm wire.HeartbeatMetrics
	if json.Unmarshal(e.Payload, &hm) != nil {
		return
	}
	l.metrics.recordHeartbeat(name, hm)
}

// reapHeartbeats periodically looks for ready thralls that stopped heart-beating (a hung
// process the OS watcher cannot see) and marks them stale.
func (l *Lord) reapHeartbeats() {
	if l.hbMissAfter <= 0 || l.hbCheckEvery <= 0 {
		return
	}
	t := time.NewTicker(l.hbCheckEvery)
	defer t.Stop()
	for {
		select {
		case <-l.appCtx.Done():
			return
		case now := <-t.C:
			l.checkHeartbeats(now)
		}
	}
}

// checkHeartbeats marks every ready thrall whose last heartbeat is older than hbMissAfter as
// stale (once per outage), emitting an event and counting the miss. Recovery back to ready is
// handled by onHeartbeat when a heartbeat resumes.
func (l *Lord) checkHeartbeats(now time.Time) {
	type staleMark struct {
		name string
		seq  uint64
	}
	l.mu.Lock()
	var newlyStale []staleMark
	for name, seen := range l.lastSeen {
		if l.ready[name] && !l.stale[name] && now.Sub(seen) > l.hbMissAfter {
			l.stale[name] = true
			l.ready[name] = false
			l.statusSeq++
			newlyStale = append(newlyStale, staleMark{name: name, seq: l.statusSeq})
		}
	}
	l.mu.Unlock()

	for _, m := range newlyStale {
		pid := 0
		if ch := l.childByName(m.name); ch != nil {
			pid = ch.pid()
		}
		l.applyStatus(m.name, pid, "stale", m.seq)
		l.metrics.incHeartbeatMiss(m.name)
		l.emit("stale", m.name, pid)
		l.log.Warn("thrall heartbeat missed - marking stale", slog.String("name", m.name))
	}
}

// Stop performs a graceful drain of all thralls.
func (l *Lord) Stop() {
	l.draining.Store(true)
	l.metrics.reg.Set(metricUp, nil, 0)
	l.stopMetricsServer()
	nc := l.ether.Conn()

	// Snapshot under the lock; drain both static and dynamic children.
	l.childrenMu.RLock()
	all := make([]*child, len(l.children))
	copy(all, l.children)
	l.childrenMu.RUnlock()

	var wg sync.WaitGroup
	for _, ch := range all {
		wg.Add(1)
		go func(ch *child) {
			defer wg.Done()
			ch.requestDrain(nc, defaultGrace)
		}(ch)
	}
	wg.Wait()

	l.procCancel()
}

// --- helpers ---

func (l *Lord) stopping() bool {
	return l.draining.Load() || (l.appCtx != nil && l.appCtx.Err() != nil)
}

func (l *Lord) sleep(d time.Duration) (stop bool) {
	select {
	case <-l.appCtx.Done():
		return true
	case <-time.After(d):
		return false
	}
}

func (l *Lord) setStatus(name string, pid int, status string) {
	if err := l.reg.Set(name, registry.Entry{
		PID: pid, Node: l.id, Status: status, UpdatedMs: time.Now().UnixMilli(),
	}); err != nil {
		l.log.Error("registry set failed", slog.String("name", name), slog.Any("err", err))
	}
	l.metrics.setStatus(name, status)
}

// applyStatus writes a liveness status (ready/stale) to the registry in decision order. The
// seq is stamped under l.mu when the ready/stale decision is made; a write whose seq is not
// newer than the last one applied for this thrall is dropped, so two concurrent decisions
// (a resuming heartbeat vs. the reaper) converge on the later one instead of leaving the
// registry stuck on the earlier. Registry I/O runs under statusMu, never under l.mu.
func (l *Lord) applyStatus(name string, pid int, status string, seq uint64) {
	l.statusMu.Lock()
	defer l.statusMu.Unlock()
	if seq <= l.statusApplied[name] {
		return
	}
	l.statusApplied[name] = seq
	l.setStatus(name, pid, status)
}

// logExit records a thrall's process exit at a level reflecting its nature: an abnormal
// exit (non-zero / signalled) is a warning, a clean exit is informational.
func (l *Lord) logExit(name string, abnormal bool) {
	if abnormal {
		l.log.Warn("thrall exited", slog.String("name", name), slog.Bool("abnormal", true))
		return
	}
	l.log.Info("thrall exited", slog.String("name", name), slog.Bool("abnormal", false))
}

func (l *Lord) emit(event, name string, pid int) {
	data, err := json.Marshal(map[string]any{
		"v": 1, "kind": "event", "event": event, "name": name, "pid": pid, "ts": time.Now().UnixMilli(),
	})
	if err != nil {
		return
	}
	_ = l.ether.Conn().Publish(wire.Events, data)
}

// forgetThrall drops all per-thrall observability state for a stopped thrall - the heartbeat
// maps and the per-name metric series - so a long-lived lord with dynamic spawn/stop churn does
// not accumulate stale entries.
func (l *Lord) forgetThrall(name string) {
	l.mu.Lock()
	delete(l.ready, name)
	delete(l.stale, name)
	delete(l.lastSeen, name)
	l.mu.Unlock()
	l.statusMu.Lock()
	delete(l.statusApplied, name)
	l.statusMu.Unlock()
	l.metrics.forget(name)
}

func (l *Lord) childByName(name string) *child {
	l.childrenMu.RLock()
	defer l.childrenMu.RUnlock()
	for _, ch := range l.children {
		if ch.spec.Name == name {
			return ch
		}
	}
	return nil
}

func nameFromHB(subject string) string {
	parts := strings.Split(subject, ".")
	if len(parts) == 4 {
		return parts[2]
	}
	return ""
}
