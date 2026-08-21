# durable-perf - measured snapshot

Drain rate of one durable thrall over a preloaded JetStream backlog, embedded NATS
(FileStorage), Apple Silicon dev machine. Backlog N = 200000 casts, trivial `inc` handler.

| Consumer shape | Drain time | Throughput |
|---|---|---|
| `Fetch(1)` (pre-AE-065) | 7.62 s | **~26 000 casts/s** |
| `Fetch(128)` + AckWait/MaxAckPending (AE-065) | 0.55 s | **~367 000 casts/s** |

**~14x** on this machine. The `Fetch(1)` rate is flat across backlog sizes (it is
round-trip bound: one server round-trip per cast); batching amortizes that round-trip
across the batch, and the gain grows with N as fixed startup cost fades. At N = 20000 the
tuned build reads ~243k casts/s, held down by the 20 ms poll granularity, not the consumer.

## Reading these numbers honestly

- This is an **isolated drain** on one thrall with a trivial handler - it measures the
  consumer machinery, not application work. A real handler that does I/O per cast is bound
  by that work, not by fetch batching.
- The soak suite paced durable publishers at ~400 casts/s (`internal/lord/soak_test.go`).
  That was a **deliberately conservative pace** to keep the backlog near empty under a
  single-message consumer during a chaos run, not a hard ceiling - this harness shows the
  drain path had ~65x headroom over that pace even before batching, and far more after.
- Embedded NATS on a fast local disk is a best case. A remote cluster over the network is
  where the per-message round-trip of `Fetch(1)` hurts most, so the batching win is expected
  to be **larger**, not smaller, off-box.

## Reproduce

```bash
scripts/durable-perf.sh 200000                      # tuned build
# then set durableBatchSize = 1 in sdk/go/thrall/thrall.go and rerun:
scripts/durable-perf.sh 200000                      # pre-AE-065 shape
```
