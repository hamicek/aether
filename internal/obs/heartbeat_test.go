package obs

import (
	"testing"
	"time"
)

func TestClampHeartbeatIntervalMs(t *testing.T) {
	cases := map[int]int{
		0:    DefaultHeartbeatIntervalMs, // unset -> default
		-5:   DefaultHeartbeatIntervalMs, // negative -> default
		50:   MinHeartbeatIntervalMs,     // too small -> floor
		100:  100,                        // at the floor
		500:  500,                        // passthrough
		2000: 2000,
	}
	for in, want := range cases {
		if got := ClampHeartbeatIntervalMs(in); got != want {
			t.Errorf("ClampHeartbeatIntervalMs(%d) = %d, want %d", in, got, want)
		}
	}
}

func TestHeartbeatIntervalFromEnv(t *testing.T) {
	cases := map[string]time.Duration{
		"500": 500 * time.Millisecond,
		"":    DefaultHeartbeatIntervalMs * time.Millisecond, // unset -> default
		"xx":  DefaultHeartbeatIntervalMs * time.Millisecond, // invalid -> default
		"10":  MinHeartbeatIntervalMs * time.Millisecond,     // too small -> floor
	}
	for in, want := range cases {
		t.Setenv(EnvHeartbeatIntervalMs, in)
		if got := HeartbeatInterval(); got != want {
			t.Errorf("HeartbeatInterval() with %q = %s, want %s", in, got, want)
		}
	}
}
