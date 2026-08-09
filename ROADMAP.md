# Roadmap

aether is an early, experimental runtime. The core (supervision, message passing, durable
mailbox, cluster-wide singletons) works and is covered by tests, but several deliberate gaps
remain before it is production-grade. They are listed here so the trade-offs are explicit.

## Supervision semantics

- **Thrall-level fencing for singletons.** A cluster-wide singleton is guarded by a distributed
  KV lock with a TTL, so a crashed lock holder fails over. What is not yet handled is the window
  where a lord itself dies but its thrall process is briefly orphaned before the lock expires.
  Full thrall-level fencing (the orphaned process refusing to act once its lord is gone) is open.
- **`temporary` restart policy inside group strategies.** `temporary` is honoured under
  `one_for_one`, but its interaction with `one_for_all` / `rest_for_one` group restarts is not
  yet fully specified.

## State and durability

- **Thrall state persistence.** Durability today covers the *mailbox* (casts survive a crash via
  JetStream), not the *state*. Like OTP, a restarted thrall runs a clean `init` and loses its
  in-memory state. Optional state snapshots/restore are future work.

## Observability and operations

- **Liveness beyond the embedded privilege.** Liveness is currently derived from heartbeats. Using
  NATS `$SYS` connection events as a supplement would give liveness signals even outside the
  embedded-server case (external clusters).
- **Monitoring / outage detection.** A first-class way to surface thrall and lord outages: uptime
  and last-seen in the registry, an optional metrics exporter, and alerting on `ready -> down`
  transitions or a supervisor giving up (`gave_up`). The building blocks already exist (heartbeats,
  the `aether._lord.events` lifecycle stream, the KV registry with status).

## Testing

- **Long-running soak and chaos testing.** For high-reliability use (for example SCADA-style
  always-on deployments), a soak suite that runs for hours, kept separate from CI (`soak` build tag)
  and run on demand via `scripts/soak.sh`. In place: sustained call/cast load with a p99 and no-trend
  bar, zero loss on durable mailboxes, leak detection (goroutine, heap and thrall RSS trend), chaos
  (`SIGKILL` of random thralls with a per-strategy recovery bar and lossless durable delivery through
  the kills), singleton failover across killed lord nodes (a failover bar and a one-live-instance
  fencing bar), graceful drain with no lost work, and a structured report that fails on any breach.

## Not on the roadmap (by design)

aether is intentionally *not* BEAM-scale. It targets tens of processes with real OS-process
isolation, not millions of lightweight ones. It also does not try to hide NATS behind the thrall:
a thrall has full access to JetStream, KV and its own subjects.
