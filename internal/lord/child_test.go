package lord

import (
	"strings"
	"testing"

	"github.com/hamicek/aether/internal/obs"
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
