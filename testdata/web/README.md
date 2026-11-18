# Web thrall test peers

A small web thrall in TypeScript (`peer.ts`) and Python (`peer.py`), run as a real process by the
SDK test suites (`sdk/ts/src/web.test.ts`, `sdk/python/web_test.py`) against a real `nats-server`.
Both answer the same routes, so the two suites assert the same behaviour:

| Route | Answer |
|---|---|
| `GET /fast` | `fast`, at once |
| `GET /slow` with query `ms=N` | `slow`, after N milliseconds |
| `GET /trace` | the request's trace id as the handler's `ctx` reports it, after 50 ms |
| `GET /hits` | how many requests the thrall has counted (the `handle` shape threads the count as its state; the `serve` shape keeps it in the shared value) |
| `GET /throw` | nothing: the handler throws (raises) at once |
| `GET /reject` | nothing: the handler fails after its first await (a rejected promise) |
| `GET /nothing` | nothing: the handler returns no response |
| `GET /refuse` | a refusal with code `not_for_you` |

The environment picks the shape under test, on top of the identity every thrall gets
(`AETHER_NATS_URL`, `AETHER_APP`, `AETHER_NAME`):

- `WEB_SHAPE` - `serve` (default) or `handle`
- `WEB_CONCURRENCY` - the `concurrency` of a `serve` thrall; unset leaves the SDK default
