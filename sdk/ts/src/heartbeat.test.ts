import { test, expect, afterEach } from "bun:test";
import { clampHeartbeatIntervalMs, heartbeatIntervalMs, DEFAULT_HEARTBEAT_INTERVAL_MS, MIN_HEARTBEAT_INTERVAL_MS } from "./heartbeat";

afterEach(() => {
  delete process.env.AETHER_HEARTBEAT_INTERVAL_MS;
});

test("clamp: non-positive and NaN fall back to default; too-small floors; else passthrough", () => {
  expect(clampHeartbeatIntervalMs(0)).toBe(DEFAULT_HEARTBEAT_INTERVAL_MS);
  expect(clampHeartbeatIntervalMs(-5)).toBe(DEFAULT_HEARTBEAT_INTERVAL_MS);
  expect(clampHeartbeatIntervalMs(NaN)).toBe(DEFAULT_HEARTBEAT_INTERVAL_MS);
  expect(clampHeartbeatIntervalMs(50)).toBe(MIN_HEARTBEAT_INTERVAL_MS);
  expect(clampHeartbeatIntervalMs(500)).toBe(500);
});

test("heartbeatIntervalMs reads the env, clamps, defaults", () => {
  process.env.AETHER_HEARTBEAT_INTERVAL_MS = "500";
  expect(heartbeatIntervalMs()).toBe(500);

  process.env.AETHER_HEARTBEAT_INTERVAL_MS = "10";
  expect(heartbeatIntervalMs()).toBe(MIN_HEARTBEAT_INTERVAL_MS);

  process.env.AETHER_HEARTBEAT_INTERVAL_MS = "nonsense";
  expect(heartbeatIntervalMs()).toBe(DEFAULT_HEARTBEAT_INTERVAL_MS);

  delete process.env.AETHER_HEARTBEAT_INTERVAL_MS;
  expect(heartbeatIntervalMs()).toBe(DEFAULT_HEARTBEAT_INTERVAL_MS);
});
