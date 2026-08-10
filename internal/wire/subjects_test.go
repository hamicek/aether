package wire

import "testing"

// TestSubjects pins the subject convention. The same (app, name) -> subject table is
// mirrored in the TS and Python parity tests; any divergence in one language surfaces there.
func TestSubjects(t *testing.T) {
	const app, name = "demo", "counter"
	cases := []struct {
		got, want string
	}{
		{Call(app, name), "aether.demo.counter.call"},
		{Cast(app, name), "aether.demo.counter.cast"},
		{Info(app, name), "aether.demo.counter.info"},
		{Data(app, name), "aether.demo.counter.*"},
		{Stream(app, name), "aether_demo_counter"},
		{EventLog(app, name), "aether.demo.counter.evt"},
		{EventLogStream(app, name), "aether_demo_counter_evt"},
		{Ctl(name), "aether._lord.counter.ctl"},
		{Heartbeat(name), "aether._lord.counter.hb"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("subject: got %q, want %q", c.got, c.want)
		}
	}
	if HeartbeatAll() != "aether._lord.*.hb" {
		t.Errorf("HeartbeatAll: got %q, want %q", HeartbeatAll(), "aether._lord.*.hb")
	}
	if LordCtl() != "aether._lord.ctl" {
		t.Errorf("LordCtl: got %q, want %q", LordCtl(), "aether._lord.ctl")
	}
	if Events != "aether._lord.events" {
		t.Errorf("Events: got %q, want %q", Events, "aether._lord.events")
	}
}
