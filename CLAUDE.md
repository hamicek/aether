# aether

Polyglotní distribuovaný actor/OTP runtime nad NATS. **Lord** (supervizor) spouští
**thrally** (genservery) jako OS procesy, komunikace přes NATS (JSON envelope, call/cast).
Ne BEAM-scale, ale desítky procesů v libovolném jazyce se skutečnou izolací OS procesů.

**Slovník:** aether (projekt) / éter (sběrnice) / lord (supervizor) / thrall (genserver).

## Kde začít

- **DESIGN.md** - plná architektura, rozhodnutí a vědomé tradeoffy (čti první)
- **README.md** - přehled hotových featur, CLI, struktura repa
- **examples/counter/** - ukázky, jeden manifest per scénář

## Toolchain (tento stroj)

- **Go není na PATH** - spouštěj přes `mise exec go@latest -- go ...` a nastav `GOTOOLCHAIN=local`
- **Bun** - `bun install` v rootu (workspace); thrally běží `bun run ./counter.ts`
- **Python** - venv v `examples/counter/.venv` (`uv venv` + `uv pip install --python .venv/bin/python nats-py`), SDK přes `PYTHONPATH=../../sdk/python`

## Build & run

```bash
export GOTOOLCHAIN=local
mise exec go@latest -- go build -o bin/aether ./cmd/aether
mise exec go@latest -- go build -o bin/counter-go ./examples/counter

cd examples/counter
../../bin/aether up -f aether.toml        # polyglot demo (gateway vypíše counter=3 x3)
```

CLI: `aether up | ps | events | cast | call`. V embedded režimu se `ps`/`events`/`cast`/`call`
připojí přes `.aether-endpoint` (píše ho `up`); proti external clusteru přes `--url`.

Manifesty v `examples/counter/`: `aether.toml` (polyglot), `aether-durable*.toml`,
`aether-external*.toml`, `aether-singleton.toml`, `aether-one-for-all.toml`, `aether-rest-for-one.toml`.

## External NATS pro dev

Standalone server na přiděleném portu **7390** (blok 7390-7394). Binárka:
`mise exec go@latest -- go install github.com/nats-io/nats-server/v2@v2.10.20`
(nainstaluje se do mise go bin), spuštění `nats-server -a 127.0.0.1 -p 7390 -js -sd <dir>`.

## Konvence

- **Commity:** žádné dlouhé pomlčky (em dash U+2014, en dash U+2013) - používej obyčejný
  `-`, jinak commit zablokuje git hook. Lidský styl, žádná zmínka o AI (viz globální `~/.claude/CLAUDE.md`).
- **Workflow:** wf v2, prefix **AE**. Vstup: `wf resume`, `wf status aether`, `wf next aether <ID>`.
  Nový úkol: `wf create aether "..."`.
- **Wire kontrakt** má jediný zdroj pravdy v `internal/wire` (Go); TS a Python SDK ho zrcadlí.
  Při změně envelope/subjectů uprav všechny tři strany.

## Zbývá (vědomé TODO, neblokující)

- `$SYS` connection eventy jako doplněk heartbeatu (liveness i mimo embedded výsadu)
- `temporary` sémantika uvnitř skupinových strategií
- perzistence stavu thralla (durable = mailbox, ne stav; restart = čistý `init`)
