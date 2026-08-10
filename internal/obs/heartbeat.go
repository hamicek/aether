package obs

import (
	"os"
	"strconv"
	"time"
)

// EnvHeartbeatIntervalMs is the environment variable the lord injects into a thrall so its SDK
// heartbeats at the manifest-configured interval; the lord derives its reaper threshold from the
// same value, so the two never drift.
const EnvHeartbeatIntervalMs = "AETHER_HEARTBEAT_INTERVAL_MS"

const (
	// DefaultHeartbeatIntervalMs is the heartbeat interval when unset (the historical 2s).
	DefaultHeartbeatIntervalMs = 2000
	// MinHeartbeatIntervalMs floors the interval so a typo cannot flood the bus / peg the CPU.
	MinHeartbeatIntervalMs = 100
)

// ClampHeartbeatIntervalMs normalizes a configured interval in milliseconds: a non-positive
// value falls back to the default, and a too-small value is raised to the minimum floor.
func ClampHeartbeatIntervalMs(ms int) int {
	if ms <= 0 {
		return DefaultHeartbeatIntervalMs
	}
	if ms < MinHeartbeatIntervalMs {
		return MinHeartbeatIntervalMs
	}
	return ms
}

// HeartbeatInterval resolves the SDK heartbeat interval from the environment (empty or invalid
// -> default, clamped to the minimum). Used by the Go SDK; the TS/Python SDKs mirror it.
func HeartbeatInterval() time.Duration {
	ms, err := strconv.Atoi(os.Getenv(EnvHeartbeatIntervalMs))
	if err != nil {
		ms = 0
	}
	return time.Duration(ClampHeartbeatIntervalMs(ms)) * time.Millisecond
}
