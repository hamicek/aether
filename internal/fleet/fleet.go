// Package fleet is the fleet-observability contract and aggregator: the curated health summary a
// lord publishes about itself (Health), and the consumer that assembles a live network-wide view
// from those summaries (Aggregator).
//
// This is a mechanism, not a domain concern: it describes the runtime itself (nodes, lords,
// thralls, their state), never application data. It is the network-wide sibling of the per-node
// observer dashboard - each lord publishes its own summary on aether._fleet.<app>.<lord_id>
// (wire.FleetHealth), and an aggregator subscribes to aether._fleet.> (wire.FleetHealthAll) to
// hold the whole fleet. The raw supervision channels (aether._lord.>) stay node-local and are
// never exported; this curated summary is what an operator deliberately exports to see the fleet.
//
// Health is a JSON wire contract: any language can consume the summary off the subject, so an
// application dashboard is not tied to Go.
package fleet

// Health is one lord's curated summary of its own state, published periodically. Identity travels
// here (App + LordID), not in the subject, so an aggregator keys off the payload and a lord id with
// dots (an FQDN hostname) needs no special handling.
type Health struct {
	App        string         `json:"app"`
	LordID     string         `json:"lord_id"`     // "<hostname>-<pid>" - the node/instance identifier
	Strategy   string         `json:"strategy"`    // supervision strategy of the app
	NatsMode   string         `json:"nats_mode"`   // embedded | external
	TS         int64          `json:"ts"`          // unix millis this summary was built
	IntervalMs int64          `json:"interval_ms"` // publish cadence, so a consumer can derive staleness
	Thralls    []ThrallHealth `json:"thralls"`
}

// ThrallHealth is one thrall's line in the summary: its topology plus its live status and the
// self-metrics the lord already tracks (mirrors the observer dashboard's per-thrall view).
type ThrallHealth struct {
	Name            string  `json:"name"`
	Status          string  `json:"status"` // starting | ready | down | stale
	Scope           string  `json:"scope"`
	Restart         string  `json:"restart"`
	Replicas        int     `json:"replicas"`
	Durable         bool    `json:"durable"`
	EventLog        bool    `json:"event_log"`
	Dynamic         bool    `json:"dynamic"`
	Live            bool    `json:"live"`
	Restarts        uint64  `json:"restarts"`
	GaveUp          uint64  `json:"gave_up"`
	HeartbeatMisses uint64  `json:"heartbeat_misses"`
	MailboxDepth    int     `json:"mailbox_depth"`
	Processed       uint64  `json:"processed"`
	RSSBytes        int64   `json:"rss_bytes"`
	CPUPercent      float64 `json:"cpu_percent"`

	// Self-description carried across the fleet: the thrall's self-declared version and the
	// operations it answers (from its heartbeat), the reason of its most recent failure, and the
	// deployment metadata the operator declared in the manifest. All optional.
	Version     string            `json:"version,omitempty"`
	CallOps     []string          `json:"call_ops,omitempty"`
	CastOps     []string          `json:"cast_ops,omitempty"`
	LastError   string            `json:"last_error,omitempty"`
	LastErrorMs int64             `json:"last_error_ms,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}
