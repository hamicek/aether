// Heartbeat interval resolution, mirroring internal/obs: the lord injects
// AETHER_HEARTBEAT_INTERVAL_MS and the SDK beats at it, so the lord's reaper threshold (derived
// from the same value) and the thralls never drift. An empty/invalid value falls back to the
// default; a too-small value is clamped to a floor.

export const DEFAULT_HEARTBEAT_INTERVAL_MS = 2000;
export const MIN_HEARTBEAT_INTERVAL_MS = 100;

export function clampHeartbeatIntervalMs(ms: number): number {
  if (!Number.isFinite(ms) || ms <= 0) return DEFAULT_HEARTBEAT_INTERVAL_MS;
  if (ms < MIN_HEARTBEAT_INTERVAL_MS) return MIN_HEARTBEAT_INTERVAL_MS;
  return ms;
}

export function heartbeatIntervalMs(): number {
  const raw = process.env.AETHER_HEARTBEAT_INTERVAL_MS;
  return clampHeartbeatIntervalMs(raw ? parseInt(raw, 10) : NaN);
}
