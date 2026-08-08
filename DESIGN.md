# aether - architektonický náčrt

> Polyglotní distribuovaný actor/OTP runtime, kde substrátem je **NATS** a „procesy" jsou skutečné **OS procesy**.
> Cíl: SDK, kterým se velice jednoduše tvoří **thrally** (genservery) a pouští **lord** (supervizor). Nikoli BEAM-scale (miliony procesů), ale **desítky** procesů, které spolu spolehlivě komunikují.

Status: návrh (design fáze), 2026-08-08.

## Slovník

Projekt má vlastní pojmosloví (metafora éteru z Necroscope univerza), pod kterým leží úplně střízlivá OTP sémantika:

| pojem | význam | OTP protějšek |
|---|---|---|
| **aether** | název projektu / runtime | - |
| **éter** (ether) | sběrnice - embedded NATS nebo external cluster | message transport |
| **lord** | supervizor - spawnuje a hlídá thrally, restartuje dle policy | Supervisor |
| **thrall** | genserver - stav + serializované zpracování zpráv | GenServer |

V dokumentu používám „lord/thrall/éter" pro naše komponenty a „GenServer/Supervisor" jen tam, kde odkazuju na OTP jako vzor.

---

## 1. Idea v jedné větě

Vezmi OTP sémantiku (GenServer / Supervisor / Registry) z projektu `noex`, ale místo in-process runtime ji polož na **message broker (NATS)** a nech „procesy" být **jazykově nezávislé OS procesy**. Lord je nad nimi jako správce OS procesů - trochu jinak než v Elixiru, ale s OTP sémantikou restartů a supervizních stromů.

Není to konkurent BEAMu. Je to **OTP sémantika položená na NATS**, s vědomým ohraničením na desítky procesů.

---

## 2. Mapování OTP → NATS

Velká část OTP konceptů má v NATS čistý protějšek - to je nosný důvod, proč návrh dává smysl:

| OTP koncept | NATS primitiv |
|---|---|
| `call` (sync request/reply) | NATS **request/reply** - vestavěný timeout, ephemeral `_INBOX` |
| `cast` (fire-and-forget) | core **publish** (ephemeral) |
| Trvanlivý mailbox / at-least-once | **JetStream** stream + durable consumer |
| Registry (pojmenované thrally) | subject konvence + **KV bucket** name → node/pid/status |
| Pool workerů (poolboy) | **queue group** - load-balance zdarma |
| Liveness / heartbeat | KV s TTL + heartbeat subject + `$SYS` connection events |
| Perzistence stavu | JetStream **KV / Object store** |
| Observer / dashboard | odposlech subjectů + `$SYS` events |

---

## 3. Runtime: Go + embedded NATS

Runtime = **jedna Go binárka**, která je **zároveň broker i lord**. NATS jde použít jako Go knihovnu (`server.NewServer(...)`, spuštěný in-process).

```
aether up  ==  éter (embedded NATS)  +  lord (čte aether.toml)  +  KV registry
```

**Proč Go:** statická binárka, cross-compile, prvotřídní process management (`os/exec`, signály, process groups), a embedded NATS bez externí závislosti.

**Co tím získáš (elegance mechanismu):**
1. Lord sedí *na* sběrnici, ne vedle ní → „chytrý lord" (viz §5) je levný.
2. Observability skoro zadarmo (každá zpráva teče brokerem, který je tvůj kód).
3. Jeden artefakt: `aether up` = éter + lord + registry. To je diferenciace proti dapru (žádný sidecar, žádný povinný k8s).

**Co Go+embedded NATS NEvyřeší automaticky** (jsou to rozhodnutí o sémantice, ne o jazyku):
- **Dual-liveness** - broker zná „PID žije" + „subscription aktivní" (2 ze 3 signálů zadarmo), ale „handler je opravdu responzivní" pořád potřebuje aplikační heartbeat.
- **Durabilita mailboxu** - core vs JetStream (viz §11).
- **Idempotence / perzistence stavu** - jazykově neutrální kontrakty.

Elegance je reálná, ale je to elegance *mechanismu*, ne *sémantiky*.

---

## 4. Topologie

```
┌─────────────────────────────────────────────────────────────┐
│  aether (Go binárka) = "runtime"                            │
│                                                             │
│   ┌──────────────┐         ┌───────────────────────────┐   │
│   │  Lord        │◄───────►│  Éter (embedded NATS)     │   │
│   │  (čte        │  in-proc │  (subjects, req/reply,    │   │
│   │  aether.toml)│          │   KV registry, JetStream) │   │
│   └──────┬───────┘         └─────────────▲─────────────┘   │
│          │ fork+exec                     │ NATS (JSON)      │
└──────────┼───────────────────────────────┼─────────────────┘
           │                               │
     ┌─────▼─────┐   ┌───────────┐   ┌─────▼──────┐
     │ counter   │   │ gateway   │   │ ingest ×3  │
     │ (Bun/TS)  │   │ (Bun/TS)  │   │ (queue grp)│
     │ thrall    │   │ thrall    │   │ thrall     │
     └───────────┘   └───────────┘   └────────────┘
```

Lord spouští thrally jako OS procesy a injektuje jim env `AETHER_NATS_URL`, `AETHER_APP`, `AETHER_NAME`. Thrall přes SDK naváže spojení, přihlásí se k subjectům, začne tepat heartbeat.

**Klíčový princip pro budoucí škálování: SDK nikdy nemluví s lordem přímo - jen s éterem (NATS).** Lord a runtime jsou pro thrall neviditelné. Díky tomu je přechod embedded → external (§10) změna konfigurace, ne přepis.

---

## 5. Lord: varianta A vs B

Jde o to, jak těsně lord mluví s thrally:

- **A) Hloupý (jen správce procesů).** Spustí proces, hlídá PID/exit code, restartuje podle policy. Nic víc neví. Thrall se sám připojí, sám registruje, sám tepe heartbeat. Analogie: `systemd`.
- **B) Chytrý (koordinuje běh přes éter).** Navíc aktivně mluví přes NATS: *graceful drain* („doprocesuj a skonči"), *health nad rámec „žije"*, *koordinovaný restart* u `one_for_all`. Analogie: dirigent.

**Rozhodnutí:** začni **A**, ale nech v protokolu díru pro **B**. Konkrétně heartbeat a graceful-drain zprávu měj v kontraktu od začátku (jsou levné a zásadní pro spolehlivost). Ryze hloupý lord bez drainu zahodí při restartu rozdělanou práci - a to je přesně bolest, kvůli které lidi sahají po OTP.

Restart policy: `permanent` (vždy) / `transient` (jen při abnormálním exitu) / `temporary` (nikdy).
Strategie: `one_for_one` / `one_for_all` / `rest_for_one`.
Restart intensity: `max` restartů `within_ms` → překročení eskaluje dle strategie.

---

## 6. Subject konvence

```
aether.<app>.<name>.call     # cíl pro call (request/reply)
aether.<app>.<name>.cast     # cíl pro cast (fire-and-forget)
aether.<app>.<name>.info     # out-of-band zprávy (timery, notifikace)
aether._lord.<name>.ctl      # lord → thrall (drain / shutdown / ping)
aether._lord.<name>.hb       # thrall → lord (heartbeat)
aether._lord.events          # lifecycle události (started/crashed/restarted) pro dashboard
```

Pool workerů = víc thrallů se stejným `name` v **queue group** na `...call`/`...cast` → NATS load-balancuje. Reply subject se neřeší ručně - NATS request/reply drží ephemeral `_INBOX.*` sám.

---

## 7. Wire envelope (JSON)

Jedna obálka pro vše, `kind` rozlišuje typ, `op` je „která handler funkce".

**Call (request):**
```json
{
  "v": 1,
  "id": "01J8XZ...ULID",
  "kind": "call",
  "from": "gateway",
  "to": "counter",
  "op": "get",
  "payload": {},
  "ts": 1730000000000
}
```

**Reply (na `_INBOX`, koreluje přes `id`):**
```json
{ "v": 1, "id": "01J8XZ...ULID", "kind": "reply", "status": "ok", "payload": 2 }
```

**Reply s chybou:**
```json
{
  "v": 1, "id": "01J8XZ...ULID", "kind": "reply", "status": "error",
  "error": { "type": "handler_error", "message": "key not found", "retryable": false }
}
```

**Cast (bez odpovědi):**
```json
{ "v": 1, "id": "01J...", "kind": "cast", "to": "counter", "op": "inc", "payload": {} }
```

**Heartbeat (thrall → lord):**
```json
{ "v": 1, "kind": "hb", "name": "counter", "pid": 12345, "state": "ready", "mailbox": 0, "ts": 1730000000000 }
```

**Control (lord → thrall):**
```json
{ "v": 1, "kind": "ctl", "op": "drain" }
```
`op` ∈ `drain` (doprocesuj a skonči) | `shutdown` (skonči teď) | `ping` (odpověz na health).

---

## 8. Counter thrall - TS/Bun SDK (home base)

Handler tvary drží `noex` sémantiku: `handleCall` vrací `[reply, newState]`, `handleCast` vrací `newState`. Dispatch přes op-keyed mapu.

```ts
// counter.ts
import { defThrall, start } from "@hamicek/aether";

const counter = defThrall<number>({
  name: "counter",

  init: () => 0,

  handleCall: {
    get: (_payload, state) => [state, state],          // [reply, newState]
  },

  handleCast: {
    inc: (_payload, state) => state + 1,               // newState
    dec: (_payload, state) => state - 1,
  },

  terminate: (reason, state) => console.log(`counter down: ${reason}, last=${state}`),
});

await start(counter);
// SDK: přečte AETHER_NATS_URL z env, připojí se, subscribe na
// aether.<app>.counter.{call,cast,info} + aether._lord.counter.ctl,
// nastartuje heartbeat na aether._lord.counter.hb,
// a KLÍČOVÉ: interně serializuje zpracování (1 zpráva v čase) → GenServer sémantika.
```

Klient (`gateway.ts`), který counter volá:

```ts
import { call, cast } from "@hamicek/aether";

await cast("counter", "inc");
await cast("counter", "inc");
const value = await call<number>("counter", "get", {}, { timeoutMs: 5000 }); // → 2
```

---

## 9. Manifest - aether.toml

Separace: **chování thralla = kód** (v SDK), **topologie stromu = deklarativní manifest**.

```toml
app = "demo"
strategy = "one_for_one"                 # one_for_one | one_for_all | rest_for_one
restart_intensity = { max = 3, within_ms = 5000 }

[nats]
mode = "embedded"                        # embedded | external
# url = "nats://node-a:4222,nats://node-b:4222"   # pro mode = "external"

[[thrall]]
name = "counter"
cmd  = "bun run ./counter.ts"
restart = "permanent"                    # permanent | transient | temporary
scope   = "local"                        # local | singleton  (viz §12)

[[thrall]]
name = "gateway"
cmd  = "bun run ./gateway.ts"
restart = "permanent"

[[thrall]]
name = "ingest"
cmd  = "bun run ./ingest.ts"
restart = "transient"
replicas = 3                             # → queue group, pool 3 workerů
```

Ponech si i programové API navrch (DynamicSupervisor, §12) pro „přidej thrall za běhu". Defaultní cesta = manifest.

---

## 9b. Lifecycle lorda (krok za krokem)

1. `aether up` → nahodí embedded NATS (nebo se připojí k external), založí KV bucket `aether_registry`.
2. Načte `aether.toml`. Pro každý thrall `fork+exec` s injektovaným env. Zapíše `name → {pid, node, status:starting}` do KV.
3. Thrall (SDK) se připojí, subscribe, začne heartbeat. Lord vidí naskočení subscription → status `ready`, emituje `started` na `aether._lord.events`.
4. **Watch smyčka** hlídá tři signály: (a) PID exit code, (b) aktivní subscription, (c) čerstvost heartbeatu. Abnormální pád → restart dle policy + backoff, hlídá `restart_intensity` (překročení → eskalace dle `strategy`).
5. **Graceful shutdown** (`aether down` / restart): `ctl:drain` → grace period → `SIGTERM` → fallback `SIGKILL`. Thrall po drainu doprocesuje rozdělanou zprávu, zavolá `terminate`, odpojí se.

---

## 9c. Tok jednoho `call` end-to-end

```
gateway.call("counter","get")
   │  publish → aether.demo.counter.call   (reply-to: _INBOX.abc)
   ▼
[éter / embedded NATS] ── doručí ──► counter (thrall)
                                       │  zařadí do interní fronty (serializace)
                                       │  handleCall.get(payload, state=2) → [2, 2]
                                       ▼
                              publish reply → _INBOX.abc  { status:"ok", payload:2 }
   ▲
gateway ◄── NATS request vrátí 2 (nebo TimeoutError po timeoutMs)
```

---

## 10. Přechod embedded → připojený uzel (external cluster)

Cíl do budoucna: neběžet jen embedded, ale i proti připojenému NATS clusteru. Návrh to umožňuje jako **config switch**, protože SDK mluví jen s éterem:

```toml
[nats]
mode = "external"
url  = "nats://node-a:4222,nats://node-b:4222,nats://node-c:4222"
```

Thrally se nezmění ani o řádek. Dvě věci se ale posunou - počítat s nimi od začátku:

- **Lord přijde o „sedadlo brokera".** V external režimu je jen dalším NATS klientem. → Observability **nestav na embedded výsadě, ale na NATS `$SYS` / system account** (connect/disconnect/subscription events). Postaveno na `$SYS` funguje stejně embedded i external.
- **Lord běží na každém hostu** a spouští svoje *lokální* thrally (drží se pravidla „lord = lokální process manager"). To zavádí otázku singletonů - viz §12.

Runtime má tedy dva režimy: `embedded` (default, pohodlí „stáhni a spusť") a `external` (produkce, JetStream HA, škála).

---

## 11. Princip: NATS před thrallem NESCHOVÁVEJ

Thrall vrstva (envelope, call/cast, serializace mailboxu) je **pohodlí navrch, ne vězení.** SDK předá thrallu živé NATS spojení v kontextu, ať si sáhne na cokoli - JetStream, KV, Object store, vlastní subjecty:

```ts
const ingest = defThrall<State>({
  name: "ingest",
  init: async (ctx) => {
    const kv = await ctx.nats.jetstream().views.kv("cache");
    const js = ctx.nats.jetstream();
    return { kv, js, seen: 0 };
  },
  handleCast: {
    event: async (payload, state) => {
      await state.kv.put(payload.key, JSON.stringify(payload.value)); // durable
      await state.js.publish("audit.events", encode(payload));        // JetStream
      return { ...state, seen: state.seen + 1 };
    },
  },
});
```

Pravidlo palce: **thrall řeší „stav + serializované zpracování zpráv adresovaných mně". Všechno ostatní z NATS je thrallu volně k dispozici přes `ctx.nats`.** Fasáda, která NATS uzamkne, by byla chyba - vzala by přesně ty spolehlivé věci, kvůli kterým do NATS jdeme.

---

## 12. Katalog stavebních bloků (kromě thralla)

**Skoro zadarmo (tenký wrapper nad NATS):**
- **Registry** → NATS **KV** `name → {node, pid, status}`, s TTL i pro liveness. Cluster-wide bez další práce.
- **EventBus / PubSub** → nativní pub/sub, SDK jen osladí API.
- **Observer / Dashboard** → `$SYS` + `aether._lord.events`. Strom, stavy, traffic.

**Vlastní bloky (v pořadí priorit):**
1. **DynamicSupervisor** - spouštění thrallů za běhu (`ctx.startChild({...})`), ne jen z manifestu. Potřeba pro „spawn worker na request".
2. **Task / work queue** → cast + **JetStream work-queue stream** + queue group. Trvanlivá fronta úloh, at-least-once, pool workerů, retry. Věc, kterou in-process `noex` neuměl dobře - silný tahák NATS.
3. **gen_statem (konečný automat)** - varianta thralla s explicitními stavy a přechody (`new → paid → shipped`). Levná nadstavba, pro reálné workflow k nezaplacení.
4. **RateLimiter / Circuit breaker** - přeneseš z `noex`, sdílený stav v KV.

**Povinné v okamžiku multi-node:**
- **Singleton / global thrall** (ekvivalent Erlang `:global`). V single-node triviální; v clusteru běží lord na víc hostech a dva můžou nastartovat stejný `counter` → dvě instance stejného stavu = tichá katastrofa. Řešení: **distribuovaný zámek přes NATS KV s CAS (compare-and-swap na revizi)** - kdo získá klíč `singleton/<name>`, ten smí spustit; ostatní čekají a převezmou při výpadku (failover). Proto má manifest `scope = "local" | "singleton"` už teď - připravený háček, i když se implementuje až s external režimem.

---

## 13. Vědomě odloženo (díry v návrhu)

- **JetStream durable mailbox** - zatím vše core NATS = ephemeral (zpráva se při restartu ztratí, jako OTP mailbox). Rozhodnutí per-thrall flag (`call` = core request/reply, `cast` = volitelně durable) na později. Pozor: JetStream = at-least-once → **handlery musí být idempotentní**.
- **Perzistence stavu** - zatím stav jen v paměti procesu, mizí při restartu (stejně jako OTP; restart = čistý `init`).
- **Singleton / distribuovaný zámek** - háček (`scope`) připraven, implementace s external režimem.

---

## 14. Souhrn rozhodnutí

| Oblast | Rozhodnutí |
|---|---|
| Runtime | Go binárka, embedded NATS (default), external cluster (později) |
| Wire formát | JSON envelope, `kind` + `op` dispatch |
| SDK home base | TS/Bun (`@hamicek/aether`); dále Python, Go |
| Topologie | deklarativní `aether.toml`; chování v kódu |
| Lord | varianta A (hloupý) + heartbeat/drain kontrakt připravený pro B |
| Mailbox | core NATS ephemeral teď; JetStream durable později |
| Observability | `$SYS` events (funguje embedded i external) |
| NATS featury | thrallům NEschovávat - `ctx.nats` volně k dispozici |
| Multi-node | lord lokální; singletony přes KV CAS zámek |
