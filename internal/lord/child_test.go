package lord

import (
	"strings"
	"testing"

	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/singleton"
)

func envValue(env []string, key string) (string, bool) {
	for _, e := range env {
		if k, v, ok := strings.Cut(e, "="); ok && k == key {
			return v, true
		}
	}
	return "", false
}

func TestChildEnvBase(t *testing.T) {
	c := &child{
		spec:    ThrallSpec{Name: "worker", Cmd: "run"},
		natsURL: "nats://127.0.0.1:4222",
		app:     "counter",
	}
	env := c.env()
	for k, want := range map[string]string{
		"AETHER_NATS_URL": "nats://127.0.0.1:4222",
		"AETHER_APP":      "counter",
		"AETHER_NAME":     "worker",
	} {
		if got, ok := envValue(env, k); !ok || got != want {
			t.Errorf("env[%q] = %q (present=%v), want %q", k, got, ok, want)
		}
	}
	// A non-durable child must not carry the durable flag.
	if _, ok := envValue(env, "AETHER_DURABLE"); ok {
		t.Error("non-durable child should not set AETHER_DURABLE")
	}
}

func TestChildEnvInheritsLogConfig(t *testing.T) {
	t.Setenv(obs.EnvLogLevel, "debug")
	t.Setenv(obs.EnvLogFormat, "json")

	c := &child{spec: ThrallSpec{Name: "w", Cmd: "run"}, app: "a"}
	env := c.env()

	if got, ok := envValue(env, obs.EnvLogLevel); !ok || got != "debug" {
		t.Errorf("thrall did not inherit %s: got %q (present=%v)", obs.EnvLogLevel, got, ok)
	}
	if got, ok := envValue(env, obs.EnvLogFormat); !ok || got != "json" {
		t.Errorf("thrall did not inherit %s: got %q (present=%v)", obs.EnvLogFormat, got, ok)
	}
}

func TestChildEnvSingletonFencing(t *testing.T) {
	c := &child{
		spec:           ThrallSpec{Name: "single", Cmd: "run"},
		app:            "a",
		singletonKey:   "single",
		singletonEpoch: 42,
	}
	env := c.env()
	for k, want := range map[string]string{
		"AETHER_SINGLETON_BUCKET": singleton.Bucket,
		"AETHER_SINGLETON_KEY":    "single",
		"AETHER_SINGLETON_EPOCH":  "42",
	} {
		if got, ok := envValue(env, k); !ok || got != want {
			t.Errorf("env[%q] = %q (present=%v), want %q", k, got, ok, want)
		}
	}
}

func TestChildEnvNonSingletonHasNoFencing(t *testing.T) {
	c := &child{spec: ThrallSpec{Name: "worker", Cmd: "run"}, app: "a"}
	env := c.env()
	for _, k := range []string{"AETHER_SINGLETON_BUCKET", "AETHER_SINGLETON_KEY", "AETHER_SINGLETON_EPOCH"} {
		if _, ok := envValue(env, k); ok {
			t.Errorf("a non-singleton child must not carry %s", k)
		}
	}
}

func boolPtr(b bool) *bool { return &b }

// TestChildEnvLordFencingDefault: a thrall without an explicit fencing field carries the
// lord-liveness token (fencing is on by default).
func TestChildEnvLordFencingDefault(t *testing.T) {
	c := &child{spec: ThrallSpec{Name: "w", Cmd: "run"}, app: "a", lordKey: "a", lordEpoch: 7}
	if got, ok := envValue(c.env(), "AETHER_LORD_EPOCH"); !ok || got != "7" {
		t.Errorf("a default thrall must carry lord-liveness fencing: AETHER_LORD_EPOCH=%q present=%v", got, ok)
	}
}

// TestChildEnvFencingOptOut: fencing = false withholds the lord-liveness token (so the SDK's
// fencing loop is a no-op and the thrall does not self-exit on a lost lease), but singleton
// fencing is a separate mechanism and stays.
func TestChildEnvFencingOptOut(t *testing.T) {
	c := &child{
		spec:           ThrallSpec{Name: "poller", Cmd: "run", Fencing: boolPtr(false)},
		app:            "a",
		lordKey:        "a",
		lordEpoch:      7,
		singletonKey:   "poller",
		singletonEpoch: 42,
	}
	env := c.env()
	for _, k := range []string{"AETHER_LORD_BUCKET", "AETHER_LORD_KEY", "AETHER_LORD_EPOCH"} {
		if _, ok := envValue(env, k); ok {
			t.Errorf("fencing = false must withhold %s", k)
		}
	}
	if _, ok := envValue(env, "AETHER_SINGLETON_EPOCH"); !ok {
		t.Error("fencing = false must not disable singleton fencing (a separate mechanism)")
	}
}

// TestChildEnvFencingExplicitTrue: fencing = true is the same as the default (token present).
func TestChildEnvFencingExplicitTrue(t *testing.T) {
	c := &child{spec: ThrallSpec{Name: "w", Cmd: "run", Fencing: boolPtr(true)}, app: "a", lordKey: "a", lordEpoch: 7}
	if _, ok := envValue(c.env(), "AETHER_LORD_EPOCH"); !ok {
		t.Error("fencing = true must carry lord-liveness fencing")
	}
}
