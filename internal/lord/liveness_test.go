package lord

import (
	"testing"
	"time"

	"github.com/hamicek/aether/internal/obs"
)

func TestLivenessDefaults(t *testing.T) {
	m := &Manifest{}
	m.applyDefaults()
	if m.Liveness.HeartbeatIntervalMs != 2000 {
		t.Errorf("default interval = %d, want 2000", m.Liveness.HeartbeatIntervalMs)
	}
	if m.Liveness.StaleAfterMisses != 3 {
		t.Errorf("default misses = %d, want 3", m.Liveness.StaleAfterMisses)
	}
}

func TestLivenessClamp(t *testing.T) {
	m := &Manifest{Liveness: Liveness{HeartbeatIntervalMs: 10, StaleAfterMisses: 0}}
	m.applyDefaults()
	if m.Liveness.HeartbeatIntervalMs != 100 { // floored to the minimum
		t.Errorf("clamped interval = %d, want 100", m.Liveness.HeartbeatIntervalMs)
	}
	if m.Liveness.StaleAfterMisses != 3 { // non-positive -> default
		t.Errorf("misses = %d, want 3", m.Liveness.StaleAfterMisses)
	}
}

// TestReaperTimingFromConfig proves the lord derives its reaper threshold from the manifest
// liveness config (the same interval it injects into thralls), so they cannot drift.
func TestReaperTimingFromConfig(t *testing.T) {
	eth := startEmbedded(t)
	m := &Manifest{App: "lv", Strategy: "one_for_one", Liveness: Liveness{HeartbeatIntervalMs: 500, StaleAfterMisses: 2}}
	m.applyDefaults()
	l, err := New(m, eth)
	if err != nil {
		t.Fatalf("lord.New: %v", err)
	}
	if l.hbCheckEvery != 500*time.Millisecond {
		t.Errorf("hbCheckEvery = %s, want 500ms", l.hbCheckEvery)
	}
	if l.hbMissAfter != time.Second { // 500ms * 2 misses
		t.Errorf("hbMissAfter = %s, want 1s", l.hbMissAfter)
	}
}

// TestChildEnvInjectsHeartbeatInterval proves the configured interval reaches the thrall via env,
// so the SDK heartbeat ticker beats at the same rate the reaper expects.
func TestChildEnvInjectsHeartbeatInterval(t *testing.T) {
	c := &child{spec: ThrallSpec{Name: "w", Cmd: "run"}, app: "a", hbIntervalMs: 500}
	if got, ok := envValue(c.env(), obs.EnvHeartbeatIntervalMs); !ok || got != "500" {
		t.Errorf("env[%s] = %q (present=%v), want 500", obs.EnvHeartbeatIntervalMs, got, ok)
	}
}
