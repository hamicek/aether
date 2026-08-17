package lord

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/obs"
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

// TestEventLogDedupWindowClampedToMaxAge proves a max_age shorter than the dedup window does not
// crash provisioning (JetStream rejects a duplicate window larger than MaxAge): the window is
// clamped to MaxAge. A max_age at/above the window leaves the window unchanged.
func TestEventLogDedupWindowClampedToMaxAge(t *testing.T) {
	t.Run("short max_age clamps the default window", func(t *testing.T) {
		eth := startEmbedded(t)
		m := &Manifest{App: "esc", Strategy: "one_for_one", Thralls: []ThrallSpec{
			{Name: "acct", Cmd: "true", Restart: "permanent", Scope: "local", EventLog: true, EventLogMaxAgeMs: 30_000},
		}}
		l, err := New(m, eth)
		if err != nil {
			t.Fatalf("lord.New: %v", err)
		}
		if err := l.provisionStreams(); err != nil {
			t.Fatalf("provisionStreams must not fail on a short max_age: %v", err)
		}
		js, _ := eth.Conn().JetStream()
		si, err := js.StreamInfo(wire.EventLogStream("esc", "acct"))
		if err != nil {
			t.Fatalf("event log stream not provisioned: %v", err)
		}
		if want := 30_000 * time.Millisecond; si.Config.Duplicates != want {
			t.Errorf("Duplicates = %v, want clamped to max_age %v", si.Config.Duplicates, want)
		}
		if si.Config.Duplicates > si.Config.MaxAge {
			t.Errorf("Duplicates %v > MaxAge %v (JetStream would reject this)", si.Config.Duplicates, si.Config.MaxAge)
		}
	})

	t.Run("explicit window above max_age is clamped too", func(t *testing.T) {
		eth := startEmbedded(t)
		m := &Manifest{App: "esc2", Strategy: "one_for_one", Thralls: []ThrallSpec{
			{Name: "acct", Cmd: "true", Restart: "permanent", Scope: "local", EventLog: true, EventLogDedupWindowMs: 600_000, EventLogMaxAgeMs: 60_000},
		}}
		l, err := New(m, eth)
		if err != nil {
			t.Fatalf("lord.New: %v", err)
		}
		if err := l.provisionStreams(); err != nil {
			t.Fatalf("provisionStreams: %v", err)
		}
		js, _ := eth.Conn().JetStream()
		si, err := js.StreamInfo(wire.EventLogStream("esc2", "acct"))
		if err != nil {
			t.Fatalf("event log stream not provisioned: %v", err)
		}
		if want := 60_000 * time.Millisecond; si.Config.Duplicates != want {
			t.Errorf("Duplicates = %v, want clamped to max_age %v", si.Config.Duplicates, want)
		}
	})

	t.Run("max_age at or above the window leaves it unchanged", func(t *testing.T) {
		eth := startEmbedded(t)
		m := &Manifest{App: "esc3", Strategy: "one_for_one", Thralls: []ThrallSpec{
			{Name: "acct", Cmd: "true", Restart: "permanent", Scope: "local", EventLog: true, EventLogDedupWindowMs: 60_000, EventLogMaxAgeMs: 600_000},
		}}
		l, err := New(m, eth)
		if err != nil {
			t.Fatalf("lord.New: %v", err)
		}
		if err := l.provisionStreams(); err != nil {
			t.Fatalf("provisionStreams: %v", err)
		}
		js, _ := eth.Conn().JetStream()
		si, err := js.StreamInfo(wire.EventLogStream("esc3", "acct"))
		if err != nil {
			t.Fatalf("event log stream not provisioned: %v", err)
		}
		if want := 60_000 * time.Millisecond; si.Config.Duplicates != want {
			t.Errorf("Duplicates = %v, want unchanged %v", si.Config.Duplicates, want)
		}
	})
}

// TestEventLogBoundedRetentionWarns proves the lord warns at provisioning when an event_log
// thrall has a retention bound (a truncated log silently breaks Rebuild, as there is no
// snapshot), and stays silent when the log is unbounded or event_log is off.
func TestEventLogBoundedRetentionWarns(t *testing.T) {
	const warnMarker = "bounded retention"
	cases := []struct {
		name     string
		spec     ThrallSpec
		wantWarn bool
	}{
		{"max_msgs bound warns", ThrallSpec{Name: "a", Cmd: "true", Restart: "permanent", Scope: "local", EventLog: true, EventLogMaxMsgs: 1000}, true},
		// max_age must exceed the dedup window (default 2 min) or AddStream rejects the stream; see
		// the inbox follow-up on clamping the dedup window to max_age.
		{"max_age bound warns", ThrallSpec{Name: "a", Cmd: "true", Restart: "permanent", Scope: "local", EventLog: true, EventLogMaxAgeMs: 300_000}, true},
		{"unbounded event log is silent", ThrallSpec{Name: "a", Cmd: "true", Restart: "permanent", Scope: "local", EventLog: true}, false},
		{"no event log is silent", ThrallSpec{Name: "a", Cmd: "true", Restart: "permanent", Scope: "local", EventLogMaxMsgs: 1000}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eth := startEmbedded(t)
			m := &Manifest{App: "es", Strategy: "one_for_one", Thralls: []ThrallSpec{tc.spec}}
			l, err := New(m, eth)
			if err != nil {
				t.Fatalf("lord.New: %v", err)
			}
			var buf bytes.Buffer
			l.log = obs.NewWithWriter(&buf)
			if err := l.provisionStreams(); err != nil {
				t.Fatalf("provisionStreams: %v", err)
			}
			got := strings.Contains(buf.String(), warnMarker)
			if got != tc.wantWarn {
				t.Errorf("warn present = %v, want %v; log:\n%s", got, tc.wantWarn, buf.String())
			}
			if tc.wantWarn && !strings.Contains(buf.String(), "level=WARN") {
				t.Errorf("expected a WARN-level line; log:\n%s", buf.String())
			}
		})
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
