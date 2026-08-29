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
lord-liveness fencing, so a shared-bus hiccup does not reap the whole tree. A handler can trigger a
typed **let-it-crash** by returning/throwing `Escalate(reason)`: the SDK terminates the thrall with
an abnormal exit so the lord restarts it through `init` per policy (a call caller first gets a
distinguishable `"escalated"` reply), instead of a manual `panic`/`os.Exit` - see
[DESIGN.md §8](./DESIGN.md). **One lord per app is enforced at startup** (best-effort): a second lord
for an app that already has a live incumbent - detected by the incumbent's active lease renewal /
KV-revision progress, not identity - refuses to start rather than stomping the lease and crash-looping
the tree ([DESIGN.md §14](./DESIGN.md)).

Still open:

- **`temporary` restart policy inside group strategies.** `temporary` is honoured under
  `one_for_one`, but its interaction with `one_for_all` / `rest_for_one` group restarts is not
  yet fully specified.
- **Race-free single-lord start.** The enforcement above is best-effort: two lords starting for the
  same app inside the same ~1.5s window can still both come up before either sees the other. Closing
  that fully needs a per-lord-instance lease rather than the per-app one - see [DESIGN.md §14](./DESIGN.md).
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
An opt-in per-thrall **handler-level idempotence** (`Idempotent`) deduplicates the call/cast itself
by an idempotency key (a caller-supplied key, else the envelope id): a duplicate cast is skipped and
a duplicate call returns the first reply. It is in-memory (the thrall's lifetime), complementing the
command-key that keeps the event-log *write* single across a restart - see [DESIGN.md §13c](./DESIGN.md).

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
hops for cross-process tracing. Each thrall also reports a **self-description** on its heartbeat -
the operations it answers (derived from its handler maps, so nothing is declared), its self-declared
version, and its last error - which the lord folds into the registry and the fleet summary;
`aether describe <name>` prints the whole record, `aether ps` gains a version column, and a
`[[thrall]] metadata` block adds operator-owned deployment tags (site, PLC, criticality). An edge
route whose op the target thrall does not report is flagged once the thrall describes itself
(the manifest-load check already rejects an edge route whose target thrall is not declared).

Still open:

- **Liveness beyond the embedded privilege.** Liveness is currently derived from heartbeats. Using
  NATS `$SYS` connection events as a supplement would give liveness signals even outside the
  embedded-server case (external clusters).
- **Alerting and an OTLP bridge.** Alerting rules on `ready -> down` / `gave_up` / `stale`, and an
  OpenTelemetry (OTLP) exporter over the existing exporter-agnostic metric/trace model, for shops
  that push rather than scrape.

## Security

**Delivered** (see [Security](./README.md#security) and [DESIGN.md §11b / §14](./DESIGN.md)): the
embedded server can be exposed on the network with **server-side TLS and mandatory nkey auth**
(`[nats.security]`); a **least-privilege** tier gives the lord, thralls and operator their own nkey
identities with deny-based subject permissions that make `aether._lord.>` node-local *by permission*
on a networked bus (a thrall cannot drive supervision, an operator cannot forge control); and
**credentials rotate without downtime** - replace a cert/key (or nkey) file in place and send
`SIGHUP`, and the server reloads via `ReloadOptions` (a TLS rotation keeps live connections, an nkey
rotation lets the new key in and the old out). Client-side auth against an *external* bus
(`[nats.tls]` / `[nats.auth]`, including the operator CLI `--ca`/`--nkey`) was delivered earlier. The
**soak/chaos suite can run against the secured bus** (`AETHER_SOAK_SECURED=1`), with a
TLS-certificate rotation under sustained load, and holds the same no-loss / leak / recovery bars.

Still open:

- **Mutual TLS.** The server authenticates clients by nkey over server-side TLS; a
  client-certificate (mTLS) identity is a later opt-in.
- **Per-thrall identities.** A single shared thrall identity does not isolate one thrall from
  another (name-scoped channels stay open across thralls); the roles isolate the lord / thrall /
  operator boundaries. Per-thrall identities would be a separate step.
- **Deny-based, not an allow-list.** The role permissions allow everything and subtract the
  dangerous subjects (so JetStream and KV keep working); a stricter allow-list is not attempted.
- **nats-server reload-under-load race.** Reloading credentials while heavy traffic flows trips a
  data race *inside* nats-server v2.10.20 (its authorization reload racing message delivery),
  surfaced by the secured soak under the Go race detector - not aether code, and not hit in a normal
  (non-`-race`) run. Follow-up: upgrade nats-server and re-check on the latest release.

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
