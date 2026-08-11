# Roadmap

aether is an early, experimental runtime. The core (supervision, message passing, durable
mailbox, cluster-wide singletons) works and is covered by tests, but several deliberate gaps
remain before it is production-grade. They are listed here so the trade-offs are explicit.

## Supervision semantics

- **`temporary` restart policy inside group strategies.** `temporary` is honoured under
  `one_for_one`, but its interaction with `one_for_all` / `rest_for_one` group restarts is not
  yet fully specified.

## State and durability

**Delivered:** state that must survive a restart is made durable by **event-sourcing**
(`event_log = true` + `Append`/`Rebuild`) - the thrall replays its retention event log in `init`
to rebuild state, rather than snapshotting the in-memory image; see [DESIGN.md §13c](./DESIGN.md)
and `examples/eventsourced/`. The mailbox survives a restart when JetStream storage is persistent
(embedded `store_dir` or external NATS; [DESIGN.md §13](./DESIGN.md)).

Still open:

- **Snapshots / compaction.** The event log is bounded only by its configured retention; a state
  snapshot + replay-since (so rebuild does not read full history) is future work.

## Observability and operations

**Delivered** (see [Observability](./README.md#observability) and [DESIGN.md §13b](./DESIGN.md)):
structured logs across the lord and all SDKs, a Prometheus `/metrics` endpoint (thrall count by
status, restarts, gave-ups, heartbeat misses, mailbox depth/latency, processed, durable backlog),
heartbeat miss detection (a `stale` status), and `trace` correlation propagated across call/cast
hops for cross-process tracing.

Still open:

- **Liveness beyond the embedded privilege.** Liveness is currently derived from heartbeats. Using
  NATS `$SYS` connection events as a supplement would give liveness signals even outside the
  embedded-server case (external clusters).
- **Alerting and an OTLP bridge.** Alerting rules on `ready -> down` / `gave_up` / `stale`, and an
  OpenTelemetry (OTLP) exporter over the existing exporter-agnostic metric/trace model, for shops
  that push rather than scrape.

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
