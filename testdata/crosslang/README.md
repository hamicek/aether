# Cross-language peers

The same small thrall written against the TypeScript SDK (`peer.ts`) and the Python SDK
(`peer.py`). Tests start them as real processes on a real bus to prove that what one SDK puts on
the wire, another reads - in particular a refusal (error type `refused` with a stable `code`) and a
call dropped past its deadline (`deadline_exceeded`).

Both peers answer the same operations, so a test can treat them interchangeably:

| op | payload | reply |
|---|---|---|
| `balance` | - | the balance (starts at 3) |
| `withdraw` | `{amount}` | the new balance, or a refusal `insufficient_funds` |
| `broken` | - | an ordinary handler error (`disk on fire`) |
| `hold` | `{ms}` | `"released"` after holding the mailbox for `ms` |
| `write` | - | how many times `write` has run, including this one |
| `writes` | - | how many times `write` has run |
| `bulk` | `{bytes}` | a string of that many bytes (to produce a reply too large for the bus) |
| `relay` | `{app, target, op, payload, timeout_ms}` | what the peer saw when it made that call itself |

`relay` is how a test puts the peer on the calling side: its reply is `{ok: true, payload}` or
`{ok: false, typed, type, code, message, retryable, text}`, where `typed` says whether the SDK
surfaced its typed call error.

Used by `sdk/go/thrall/crosslang_test.go`, `sdk/ts/src/dispatch.test.ts` and
`sdk/python/dispatch_test.py`. They are started with the environment a lord injects
(`AETHER_NATS_URL`, `AETHER_APP`, `AETHER_NAME`).
