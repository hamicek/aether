# aether

[![CI](https://github.com/hamicek/aether/actions/workflows/ci.yml/badge.svg)](https://github.com/hamicek/aether/actions/workflows/ci.yml)

Polyglotní distribuovaný actor/OTP runtime nad NATS. **Lord** (supervizor) spawnuje
**thrally** (genservery) jako OS procesy a nechává je komunikovat v **éteru** (NATS).

Cíl: SDK, kterým se velice jednoduše tvoří thrally a pouští lord. Ne BEAM-scale
(miliony procesů), ale desítky procesů, které spolu spolehlivě komunikují - zato
v libovolném jazyce a se skutečnou izolací OS procesů.

Kompletní návrh: [DESIGN.md](./DESIGN.md). Licence: [MIT](./LICENSE). Přispívání:
[CONTRIBUTING.md](./CONTRIBUTING.md). Plán a vědomě odložené věci: [ROADMAP.md](./ROADMAP.md).

## Slovník

| pojem | význam | OTP protějšek |
|---|---|---|
| **éter** | sběrnice - embedded NATS nebo external cluster | message transport |
| **lord** | supervizor - spawnuje a hlídá thrally, restartuje dle strategie | Supervisor |
| **thrall** | genserver - stav + serializované zpracování zpráv | GenServer |

## Hotové featury

Vše níže je implementované a ověřené naostro (viz manifesty v `examples/counter/`).

| Oblast | Stav | Detail |
|---|---|---|
| **Spawn** | ✅ | Lord spouští thrally jako OS procesy, injektuje `AETHER_*` env, hlídá exit code |
| **Polyglot SDK** | ✅ | **TS/Bun**, **Python**, **Go** - stejný wire kontrakt, pro lorda k nerozeznání |
| **Komunikace** | ✅ | `call` (sync request/reply), `cast` (fire-and-forget), serializovaný mailbox |
| **Supervize** | ✅ | `one_for_one`, `one_for_all`, `rest_for_one` + restart-intensity okno + backoff |
| **Graceful drain** | ✅ | `ctl:drain` → thrall doprocesuje mailbox → `terminate` → eskalace SIGTERM/SIGKILL |
| **Observability** | ✅ | KV registry (`name → pid/status`), lifecycle stream, CLI `ps` / `events` |
| **Durable mailbox** | ✅ | `durable=true` → casty přežijí pád thralla (JetStream). TS + Python + Go |
| **External NATS** | ✅ | `mode="external"` je čistě config switch - stejný stack proti reálnému clusteru |
| **Singleton** | ✅ | `scope="singleton"` → distribuovaný KV-CAS zámek, jedna instance v clusteru + failover |

Restart policy per thrall: `permanent` / `transient` / `temporary`.

## Struktura

```
cmd/aether/           CLI: up | ps | events | cast | call
internal/
  ether/              embedded NATS / external připojení (mode switch)
  lord/               supervizor: manifest, supervisor-loop, restart strategie,
                      graceful drain, durable stream provisioning, singleton zámek
  registry/           JetStream KV registr (name -> pid/status)
  singleton/          distribuovaný zámek přes KV (Create/CAS + TTL failover)
  wire/               envelope + subject/stream konvence (Go strana, sdílená s SDK)
sdk/ts/               @hamicek/aether (Bun/TS): defThrall, start, call, cast
sdk/python/           aether.py: def_thrall, start, run
sdk/go/thrall/        thrall.Def[S], thrall.Start, thrall.Call/Cast
examples/counter/     counter (TS/Py/Go) + gateway + manifesty pro každý scénář
```

## Subject konvence

```
aether.<app>.<name>.call     # request/reply (call)
aether.<app>.<name>.cast     # fire-and-forget (cast); u durable zachytává JetStream stream
aether.<app>.<name>.info     # out-of-band (timery, notifikace)
aether._lord.<name>.ctl      # lord → thrall (drain / shutdown / ping)
aether._lord.<name>.hb       # thrall → lord (heartbeat)
aether._lord.events          # lifecycle stream (spawned/ready/down/restarting/…)
aether_<app>_<name>          # JetStream stream durable mailboxu (tečky → podtržítka)
```

## CLI

```bash
aether up -f <manifest>          # nahodí éter + lorda dle manifestu
aether ps [--url <nats>]         # tabulka stavu thrallů z KV registru
aether events [--url <nats>]     # živý lifecycle stream
aether cast <name> <op> [json]   # pošle cast na thrall
aether call <name> <op> [json]   # pošle call a vytiskne odpověď
```

`ps`/`events`/`cast`/`call` se v embedded režimu připojí přes `.aether-endpoint`
(zapisuje ho `aether up`); proti external clusteru přes `--url`.

## Thrall v TS (příklad)

```ts
import { defThrall, start } from "@hamicek/aether";

const counter = defThrall<number>({
  name: process.env.AETHER_NAME ?? "counter",
  init: () => 0,
  handleCall: { get: (_p, s) => [s, s] },        // [reply, newState]
  handleCast: { inc: (_p, s) => s + 1, dec: (_p, s) => s - 1 },
  terminate: (reason, s) => console.log(`counter down: ${reason}, last=${s}`),
});

await start(counter);
```

Stejný thrall existuje i v `counter_py.py` (Python) a `counter_go.go` (Go) - funkčně
identický. Durabilita je čistě věc manifestu (`durable = true`), ne kódu.

## Manifest (příklad)

```toml
app = "demo"
strategy = "one_for_one"                 # | one_for_all | rest_for_one
restart_intensity = { max = 3, within_ms = 5000 }

[nats]
mode = "embedded"                        # | external (+ url = "nats://…")

[[thrall]]
name = "counter"
cmd  = "bun run ./counter.ts"
restart = "permanent"                    # | transient | temporary
scope   = "local"                        # | singleton
durable = false                          # true → cast přes JetStream
```

## Quickstart

```bash
# 1) build runtime
go build -o bin/aether ./cmd/aether

# 2) v examples/counter
cd examples/counter
bun install
../../bin/aether up -f aether.toml
# gateway vypíše: gateway: counter = 3
```

Ukázkové manifesty v `examples/counter/`: `aether.toml` (polyglot TS/Py/Go),
`aether-durable.toml`, `aether-external.toml`, `aether-singleton.toml`,
`aether-one-for-all.toml`, `aether-rest-for-one.toml`.

## Vědomá TODO (neblokující)

- `$SYS` connection eventy jako doplněk heartbeatu (liveness i mimo embedded výsadu)
- plný thrall-level fencing u singletonů (osiřelý thrall po pádu lorda)
- `temporary` sémantika uvnitř skupinových strategií
- perzistence stavu thralla (teď durable = mailbox, ne stav; restart = čistý `init`)
```
