package lord

import (
	"encoding/json"
	"time"

	"github.com/hamicek/aether/internal/fleet"
	"github.com/hamicek/aether/internal/wire"
)

// fleetHealth builds this lord's curated health summary from the same in-memory state the observer
// dashboard renders (treeSnapshot), plus a timestamp and the publish cadence so a consumer can
// derive staleness. It is a curated summary of the runtime, never application data.
func (l *Lord) fleetHealth() fleet.Health {
	tree := l.treeSnapshot()
	thralls := make([]fleet.ThrallHealth, 0, len(tree.Thralls))
	for _, t := range tree.Thralls {
		h := fleet.ThrallHealth{
			Name:            t.Name,
			Status:          t.Status,
			Scope:           t.Scope,
			Restart:         t.Restart,
			Replicas:        t.Replicas,
			Durable:         t.Durable,
			EventLog:        t.EventLog,
			Dynamic:         t.Dynamic,
			Live:            t.Live,
			Restarts:        t.Metrics.Restarts,
			GaveUp:          t.Metrics.GaveUp,
			HeartbeatMisses: t.Metrics.HeartbeatMiss,
			MailboxDepth:    t.Metrics.MailboxDepth,
			Processed:       t.Metrics.Processed,
			RSSBytes:        t.Metrics.RSSBytes,
			CPUPercent:      t.Metrics.CPUPercent,
			Metadata:        t.Metadata,
		}
		if d := t.Metrics.Describe; d != nil {
			h.Version = d.Version
			h.CallOps = d.CallOps
			h.CastOps = d.CastOps
			h.LastError = d.LastError
			h.LastErrorMs = d.LastErrorMs
		}
		thralls = append(thralls, h)
	}
	return fleet.Health{
		App:        tree.App,
		LordID:     tree.LordID,
		Strategy:   tree.Strategy,
		NatsMode:   tree.NatsMode,
		TS:         time.Now().UnixMilli(),
		IntervalMs: l.fleetHealthEvery.Milliseconds(),
		Thralls:    thralls,
	}
}

// publishHealth periodically publishes the fleet health summary on aether._fleet.<app>.<lord_id>.
// It runs from Start only when opt-in is on. A marshal or publish error is skipped, never fatal, so
// it cannot disturb supervision. The subject stays outside aether._lord.> so raw supervision is not
// exported - only this curated summary is, and only where the operator exports aether._fleet.>.
func (l *Lord) publishHealth() {
	if l.fleetHealthEvery <= 0 {
		return
	}
	subject := wire.FleetHealth(l.manifest.App, l.id)
	// Publish once immediately so an aggregator already subscribed sees this lord within a heartbeat
	// of startup, rather than only after the first full interval. (This helps a long-running dashboard
	// on the same bus; across a leaf the first publish can still be missed until interest propagates,
	// so a freshly started `aether fleet` waits for a periodic tick.) Then tick at the configured cadence.
	l.publishHealthOnce(subject)
	t := time.NewTicker(l.fleetHealthEvery)
	defer t.Stop()
	for {
		select {
		case <-l.appCtx.Done():
			return
		case <-t.C:
			l.publishHealthOnce(subject)
		}
	}
}

// publishHealthOnce marshals and publishes a single fleet health summary. A marshal or publish error
// is skipped, never fatal, so it cannot disturb supervision.
func (l *Lord) publishHealthOnce(subject string) {
	data, err := json.Marshal(l.fleetHealth())
	if err != nil {
		return
	}
	_ = l.ether.Conn().Publish(subject, data)
}
