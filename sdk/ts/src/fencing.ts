// Thrall-side singleton fencing for the TS SDK, mirroring the Go SDK. A scope="singleton"
// thrall verifies, independently of the lord, that it still holds its distributed KV lock:
// the lord injects the lock's bucket/key and a fencing epoch (AETHER_SINGLETON_*), and the
// thrall periodically reads the key and checks the epoch still matches. A confirmed loss
// (epoch superseded or key gone) terminates it at once; an unverifiable lock (bus unreachable)
// terminates it once the lease elapses, bounding any two-instance overlap to the lock TTL.

import { type NatsConnection } from "nats";
import { type Logger } from "./log";

// Lease/interval mirror internal/singleton.TTL (3s) and the Go SDK. The interval (a third of
// the lease) is the verification cadence; the lease is the grace after which an unverifiable
// lock is presumed lost.
export const FENCE_LEASE_MS = 3000;
export const FENCE_INTERVAL_MS = 1000;

export interface FenceConfig {
  bucket: string;
  key: string;
  epoch: number;
}

// fenceConfigFromEnv reads the fencing token; null for a non-singleton thrall (no env).
export function fenceConfigFromEnv(): FenceConfig | null {
  const bucket = process.env.AETHER_SINGLETON_BUCKET;
  const key = process.env.AETHER_SINGLETON_KEY;
  const epochRaw = process.env.AETHER_SINGLETON_EPOCH;
  if (!bucket || !key || !epochRaw) return null;
  const epoch = parseInt(epochRaw, 10);
  if (!Number.isFinite(epoch) || epoch <= 0) return null;
  return { bucket, key, epoch };
}

export interface FenceOptions {
  leaseMs?: number;
  intervalMs?: number;
}

const decoder = new TextDecoder();

// startFencing runs the verification loop. It returns a stop handle; onLost is called at most
// once and is expected to terminate the process. The loop self-schedules (no overlapping
// verifies) so a slow read never piles up requests.
export async function startFencing(
  nc: NatsConnection,
  cfg: FenceConfig,
  log: Logger,
  onLost: (reason: string) => void,
  opts: FenceOptions = {},
): Promise<{ stop: () => void }> {
  const leaseMs = opts.leaseMs ?? FENCE_LEASE_MS;
  const intervalMs = opts.intervalMs ?? FENCE_INTERVAL_MS;
  const kv = await nc.jetstream().views.kv(cfg.bucket);

  let lastConfirmed = Date.now();
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const stop = () => {
    stopped = true;
    if (timer) clearTimeout(timer);
  };

  const fireLost = (reason: string) => {
    stop();
    onLost(reason);
  };

  const tick = async () => {
    try {
      const entry = await kv.get(cfg.key);
      if (entry === null) {
        fireLost("singleton lock lost (key gone)");
        return;
      }
      const rec = JSON.parse(decoder.decode(entry.value)) as { epoch?: number };
      if (rec.epoch !== cfg.epoch) {
        fireLost("singleton lock lost (epoch superseded)");
        return;
      }
      lastConfirmed = Date.now();
    } catch (err) {
      // Cannot reach the KV: fail safe only once the lease has fully elapsed.
      if (Date.now() - lastConfirmed > leaseMs) {
        fireLost(`lock unverifiable for over ${leaseMs}ms: ${String(err)}`);
        return;
      }
      log.warn("singleton fencing: verify failed, within lease", { err: String(err) });
    }
  };

  const schedule = () => {
    timer = setTimeout(run, intervalMs);
    timer.unref?.();
  };
  const run = async () => {
    if (stopped) return;
    await tick();
    if (!stopped) schedule();
  };
  schedule();

  return { stop };
}
