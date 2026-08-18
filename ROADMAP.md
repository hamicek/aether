# Roadmap

aether is an early, experimental runtime. The core (supervision, message passing, durable
mailbox, cluster-wide singletons) works and is covered by tests, but several deliberate gaps
remain before it is production-grade. They are listed here so the trade-offs are explicit.

## Supervision semantics

**Delivered:** thrall-level fencing for singletons and lord-liveness fencing for every thrall -
a thrall self-terminates when it loses its KV lock or its lord's lease, so no thrall outlives its
lord even on an external SIGKILL; the fencing epoch is exposed as `ctx.SingletonEpoch` for
write-side fencing tokens (see [DESIGN.md §14](./DESIGN.md) and `examples/fencing-token/`). A
per-thrall `fencing = false` opts a harmless orphan (a stateless / read-only poller) out of
lord-liveness fencing, so a shared-bus hiccup does not reap the whole tree.

Still open:

- **`temporary` restart policy inside group strategies.** `temporary` is honoured under
  `one_for_one`, but its interaction with `one_for_all` / `rest_for_one` group restarts is not
  yet fully specified.
- **Single-lord enforcement.** "One lord per app" is assumed, not enforced at startup; a naive
  refusal conflicts with the singleton-failover model (two lords per app racing for a lock), so
  it needs a per-lord-instance lease rather than the per-app one - see [DESIGN.md §14](./DESIGN.md).
- **Dynamic-child fencing opt-out.** `fencing = false` applies to manifest thralls; a dynamic
  child (`StartChild`) is always fenced (the spawn API carries no opt-out yet).

## State and durability

**Delivered:** state that must survive a restart is made durable by **event-sourcing**
(`event_log = true` + `Append`/`Rebuild`) - the thrall replays its retention event log in `init`
to rebuild state, rather than snapshotting the in-memory image; see [DESIGN.md §13c](./DESIGN.md)
and `examples/eventsourced/`. A **command-key** dedup key on `Append` (via `Nats-Msg-Id`, keyed on
`ctx.MsgID`) makes a redelivered non-idempotent event safe, and the lord warns when `event_log` is
combined with a retention bound that would truncate a rebuild. The mailbox survives a restart when
JetStream storage is persistent (embedded `store_dir` or external NATS; [DESIGN.md §13](./DESIGN.md)).

Still open:

- **Snapshots / compaction.** The event log is bounded only by its configured retention; a state
  snapshot + replay-since (so rebuild does not read full history) is future work.
- **Stream-config reconcile.** Retention bounds and the dedup window are fixed at stream creation;
  changing them in the manifest on an already-provisioned stream is silently a no-op. Comparing the
  desired config against the existing stream and failing fast on an un-appliable diff would close
  that (and related) silent config drift.

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
