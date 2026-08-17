package lord

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// TestEventLogProvisioned proves an opt-in event_log thrall gets a retention (Limits) stream,
// separate from the mailbox, with bounds propagated - and independent of Durable.
func TestEventLogProvisioned(t *testing.T) {
	eth := startEmbedded(t)
	m := &Manifest{
		App:      "es",
		Strategy: "one_for_one",
		Thralls: []ThrallSpec{
			{Name: "acct", Cmd: "true", Restart: "permanent", Scope: "local", EventLog: true, EventLogMaxMsgs: 1000},
		},
	}
	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("lord.New: %v", err)
	}
	if err := l.provisionStreams(); err != nil {
		t.Fatalf("provisionStreams: %v", err)
	}

	js, err := eth.Conn().JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	si, err := js.StreamInfo(wire.EventLogStream("es", "acct"))
	if err != nil {
		t.Fatalf("event log stream not provisioned: %v", err)
	}
	if si.Config.Retention != nats.LimitsPolicy {
		t.Errorf("retention = %v, want Limits (replayable)", si.Config.Retention)
	}
	if si.Config.MaxMsgs != 1000 {
		t.Errorf("MaxMsgs = %d, want 1000", si.Config.MaxMsgs)
	}
	// No manifest window -> the default is applied explicitly (not left to the server).
	wantDedup := time.Duration(wire.DefaultEventLogDedupWindowMs) * time.Millisecond
	if si.Config.Duplicates != wantDedup {
		t.Errorf("Duplicates = %v, want default %v", si.Config.Duplicates, wantDedup)
	}
	if len(si.Config.Subjects) != 1 || si.Config.Subjects[0] != wire.EventLog("es", "acct") {
		t.Errorf("subjects = %v, want [%s]", si.Config.Subjects, wire.EventLog("es", "acct"))
	}
	// A non-durable event_log thrall must NOT get a mailbox stream.
	if _, err := js.StreamInfo(wire.Stream("es", "acct")); err == nil {
		t.Error("mailbox stream should not exist for a non-durable thrall")
	}
}

// TestEventLogDedupWindowConfigured proves the manifest's event_log_dedup_window_ms overrides
// the default duplicate window on the provisioned stream.
func TestEventLogDedupWindowConfigured(t *testing.T) {
	eth := startEmbedded(t)
	m := &Manifest{
		App:      "es3",
		Strategy: "one_for_one",
		Thralls: []ThrallSpec{
			{Name: "acct", Cmd: "true", Restart: "permanent", Scope: "local", EventLog: true, EventLogDedupWindowMs: 600_000},
		},
	}
	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("lord.New: %v", err)
	}
	if err := l.provisionStreams(); err != nil {
		t.Fatalf("provisionStreams: %v", err)
	}
	js, err := eth.Conn().JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	si, err := js.StreamInfo(wire.EventLogStream("es3", "acct"))
	if err != nil {
		t.Fatalf("event log stream not provisioned: %v", err)
	}
	if want := 600_000 * time.Millisecond; si.Config.Duplicates != want {
		t.Errorf("Duplicates = %v, want configured %v", si.Config.Duplicates, want)
	}
}

func TestNoEventLogWithoutOptIn(t *testing.T) {
	eth := startEmbedded(t)
	m := &Manifest{
		App:      "es2",
		Strategy: "one_for_one",
		Thralls:  []ThrallSpec{{Name: "plain", Cmd: "true", Restart: "permanent", Scope: "local"}},
	}
	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("lord.New: %v", err)
	}
	if err := l.provisionStreams(); err != nil {
		t.Fatalf("provisionStreams: %v", err)
	}
	js, err := eth.Conn().JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if _, err := js.StreamInfo(wire.EventLogStream("es2", "plain")); err == nil {
		t.Error("event log stream should not exist without event_log opt-in")
	}
}
