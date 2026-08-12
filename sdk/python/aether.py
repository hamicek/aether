"""aether - the Python SDK for thralls (genservers) in the ether (NATS).

Mirrors the TS and Go SDKs: the same JSON envelope, the same subjects and the same
GenServer semantics. A non-durable thrall reads call/cast/info from a single wildcard
subscription; a durable thrall (AETHER_DURABLE=1) reads call/info over core, but cast
from a durable JetStream consumer with ack (survives a crash). State is protected by a
single asyncio lock = a serialized mailbox.
"""

from __future__ import annotations

import asyncio
import json
import os
import ssl
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import nats
from nats.js.errors import KeyNotFoundError


# --- structured logging (mirrors internal/obs and the TS SDK log.ts) ---
_LOG_LEVELS = {"debug": 10, "info": 20, "warn": 30, "warning": 30, "error": 40}


def _level_from_env() -> int:
    """Resolve AETHER_LOG_LEVEL; empty or unknown falls back to info, so a typo never
    silences the runtime."""
    return _LOG_LEVELS.get(os.environ.get("AETHER_LOG_LEVEL", "").strip().lower(), 20)


def _format_from_env() -> str:
    return "json" if os.environ.get("AETHER_LOG_FORMAT", "").strip().lower() == "json" else "text"


def _render(fmt: str, level: str, msg: str, fields: dict) -> str:
    ts = datetime.now(timezone.utc).isoformat()
    if fmt == "json":
        return json.dumps({"time": ts, "level": level, "msg": msg, **fields})
    pairs = " ".join(f"{k}={v if isinstance(v, str) else json.dumps(v)}" for k, v in fields.items())
    return f"{ts} {level} {msg}" + (f" {pairs}" if pairs else "")


class Logger:
    """A minimal structured logger with level filtering and JSON/text rendering. The level
    and format come from the AETHER_LOG_* env the lord injects; base fields (app, name) are
    merged into every record so lord and thrall logs stay tellable apart on a shared stream."""

    def __init__(self, base=None, level=None, fmt=None, write=None):
        self._base = dict(base or {})
        self._level = level if level is not None else _level_from_env()
        self._fmt = fmt or _format_from_env()
        self._write = write or (lambda line: print(line, file=sys.stderr))

    def _emit(self, level_name: str, at: int, msg: str, fields: dict) -> None:
        if at < self._level:
            return
        self._write(_render(self._fmt, level_name, msg, {**self._base, **fields}))

    def debug(self, msg: str, **fields) -> None:
        self._emit("DEBUG", 10, msg, fields)

    def info(self, msg: str, **fields) -> None:
        self._emit("INFO", 20, msg, fields)

    def warn(self, msg: str, **fields) -> None:
        self._emit("WARN", 30, msg, fields)

    def error(self, msg: str, **fields) -> None:
        self._emit("ERROR", 40, msg, fields)

    def with_(self, **fields) -> "Logger":
        return Logger({**self._base, **fields}, self._level, self._fmt, self._write)


def new_logger(**base) -> Logger:
    return Logger(base)


class _MailboxStats:
    """Thrall self-metrics reported on each heartbeat (mirrors the Go SDK mailboxStats):
    depth = messages currently held, last_ms = duration of the most recent handler,
    processed = cumulative count. begin/end bracket every handled message."""

    def __init__(self):
        self.depth = 0
        self.processed = 0
        self.last_ms = 0.0

    def begin(self) -> float:
        self.depth += 1
        return time.perf_counter()

    def end(self, start: float) -> None:
        self.last_ms = (time.perf_counter() - start) * 1000.0
        self.processed += 1
        self.depth -= 1

    def snapshot(self) -> dict:
        return {
            "mailbox_depth": self.depth,
            "mailbox_latency_ms": self.last_ms,
            "processed_total": self.processed,
        }


def _heartbeat_interval() -> float:
    """Seconds between heartbeats, from AETHER_HEARTBEAT_INTERVAL_MS (mirrors internal/obs): the
    lord injects it and derives its reaper threshold from the same value. Empty/invalid -> default
    2.0s; too-small -> floored to 0.1s."""
    try:
        ms = int(os.environ.get("AETHER_HEARTBEAT_INTERVAL_MS", ""))
    except (TypeError, ValueError):
        ms = 0
    if ms <= 0:
        ms = 2000
    elif ms < 100:
        ms = 100
    return ms / 1000.0


# --- singleton fencing (mirrors the Go/TS SDKs) ---
# Lease/interval mirror internal/singleton.TTL (3s): the interval is the verification cadence,
# the lease the grace after which an unverifiable lock is presumed lost.
_FENCE_LEASE_MS = 3000
_FENCE_INTERVAL_MS = 1000


def _fence_config_from_env() -> Optional[dict]:
    """Read the singleton fencing token the lord injects (AETHER_SINGLETON_*); None otherwise."""
    return _fence_config_from("AETHER_SINGLETON_BUCKET", "AETHER_SINGLETON_KEY", "AETHER_SINGLETON_EPOCH")


def _lord_fence_config_from_env() -> Optional[dict]:
    """Read the lord-liveness token (AETHER_LORD_*), injected into every thrall the lord spawns;
    None for a thrall started outside a lord."""
    return _fence_config_from("AETHER_LORD_BUCKET", "AETHER_LORD_KEY", "AETHER_LORD_EPOCH")


def _fence_config_from(bucket_env: str, key_env: str, epoch_env: str) -> Optional[dict]:
    bucket = os.environ.get(bucket_env)
    key = os.environ.get(key_env)
    epoch_raw = os.environ.get(epoch_env)
    if not bucket or not key or not epoch_raw:
        return None
    try:
        epoch = int(epoch_raw)
    except ValueError:
        return None
    if epoch <= 0:
        return None
    return {"bucket": bucket, "key": key, "epoch": epoch}


async def _fencing(
    label: str,
    kv: Any,
    cfg: dict,
    log: Any,
    stop: asyncio.Event,
    on_lost: Callable[[str], None],
    lease_ms: int = _FENCE_LEASE_MS,
    interval_ms: int = _FENCE_INTERVAL_MS,
) -> None:
    """Run a self-fencing loop independent of the lord: on each tick verify the epoch in the KV.
    On a confirmed loss (epoch superseded or key gone) it calls on_lost at once; when the KV
    cannot be reached it calls on_lost once the lease elapses without a confirmation, bounding
    the window in which the fenced condition may already have failed. on_lost is expected to
    terminate the process. label prefixes the log lines."""
    interval = interval_ms / 1000.0
    lease = lease_ms / 1000.0
    last_confirmed = time.monotonic()
    while not stop.is_set():
        try:
            entry = await kv.get(cfg["key"])
            rec = json.loads(bytes(entry.value))
            if rec.get("epoch") != cfg["epoch"]:
                on_lost(f"{label} lost (epoch superseded)")
                return
            last_confirmed = time.monotonic()
        except KeyNotFoundError:
            on_lost(f"{label} lost (key gone)")
            return
        except Exception as ex:  # noqa: BLE001  - cannot reach the KV: fail safe after the lease
            if time.monotonic() - last_confirmed > lease:
                on_lost(f"{label} unverifiable for over {lease_ms}ms: {ex}")
                return
            log.warn(f"{label}: verify failed, within lease", err=str(ex))
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def _start_fencing_if_singleton(
    nc: Any, name: str, log: Any, stop: asyncio.Event
) -> Optional[asyncio.Task]:
    """Start the singleton fencing loop when the thrall is a singleton; None otherwise. Shared by
    start and start_fsm. Opens the lock bucket up front (a failure propagates, mirroring the Go
    SDK); on a lock loss it hard-exits the process (os._exit), matching the Go/TS SDKs."""
    cfg = _fence_config_from_env()
    if cfg is None:
        return None
    kv = await nc.jetstream().key_value(cfg["bucket"])
    return asyncio.create_task(_fencing("singleton fencing", kv, cfg, log, stop,
                                        _exit_on_lost("singleton fencing", name, log)))


async def _start_lord_liveness_fencing(
    nc: Any, name: str, log: Any, stop: asyncio.Event
) -> Optional[asyncio.Task]:
    """Start the lord-liveness fencing loop for EVERY thrall the lord spawned (AETHER_LORD_*);
    None for a thrall started outside a lord. Unlike singleton fencing it is not conditional on
    scope: any thrall self-terminates when its lord is gone or was replaced, closing the "no
    thrall survives its lord" invariant for a lord crash."""
    cfg = _lord_fence_config_from_env()
    if cfg is None:
        return None
    kv = await nc.jetstream().key_value(cfg["bucket"])
    return asyncio.create_task(_fencing("lord-liveness fencing", kv, cfg, log, stop,
                                        _exit_on_lost("lord-liveness fencing", name, log)))


def _exit_on_lost(label: str, name: str, log: Any) -> Callable[[str], None]:
    """Production on_lost: log the loss and hard-exit. Tests inject their own on_lost so the
    shared fencing loop stays verifiable without exiting."""
    def on_lost(reason: str) -> None:
        log.error(f"{label}: self-terminating", name=name, reason=reason)
        os._exit(1)

    return on_lost


def _connect_kwargs(
    name: str, ca: Optional[str] = None, nkey_seed: Optional[str] = None
) -> dict:
    """Build the kwargs for nats.connect: the thrall name plus, when the bus is
    secured, a TLS context (server verification) and the nkey seed path. Absent
    fields leave the connection unsecured, exactly as before."""
    kwargs: dict[str, Any] = {"name": name}
    if ca:
        kwargs["tls"] = ssl.create_default_context(cafile=ca)
    if nkey_seed:
        kwargs["nkeys_seed"] = nkey_seed
    return kwargs
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy


# --- subjects (must match the Go and TS sides) ---
def _sub_data(app: str, name: str) -> str:
    return f"aether.{app}.{name}.*"


def _sub_call(app: str, name: str) -> str:
    return f"aether.{app}.{name}.call"


def _sub_cast(app: str, name: str) -> str:
    return f"aether.{app}.{name}.cast"


def _sub_info(app: str, name: str) -> str:
    return f"aether.{app}.{name}.info"


def _sub_ctl(name: str) -> str:
    return f"aether._lord.{name}.ctl"


def _sub_hb(name: str) -> str:
    return f"aether._lord.{name}.hb"


def _sub_lord_ctl() -> str:
    # The lord's inbound control channel (thrall -> lord), request/reply, for runtime
    # spawn/stop. Unlike _sub_ctl, which is lord -> thrall.
    return "aether._lord.ctl"


def _stream(app: str, name: str) -> str:
    return f"aether_{app}_{name}"


def _sub_evt(app: str, name: str) -> str:
    # Event-sourcing append subject (opt-in), captured by a retention stream so it can be
    # replayed in init - unlike the WorkQueue mailbox.
    return f"aether.{app}.{name}.evt"


def _stream_evt(app: str, name: str) -> str:
    return f"aether_{app}_{name}_evt"


def _encode(e: dict) -> bytes:
    return json.dumps(e).encode()


def _decode(data: bytes) -> dict:
    return json.loads(data)


_id_seq = 0


def _next_id() -> str:
    # A simple envelope id (wire-shape parity with Go/TS). nats-py pairs the reply via
    # its own inbox, so this id is informational, not the correlation key.
    global _id_seq
    _id_seq += 1
    return f"{int(time.time() * 1000):x}-{_id_seq:x}"


_trace_seq = 0


def _new_trace() -> str:
    """Mint a fresh correlation id for an edge (a message that starts a new operation)."""
    global _trace_seq
    _trace_seq += 1
    return f"t-{int(time.time() * 1000):x}-{_trace_seq:x}"


def _or_new_trace(trace: str) -> str:
    """Return the given trace, or a fresh one when it is empty."""
    return trace if trace else _new_trace()


# Handler shapes hold the GenServer semantics:
#   handle_call: (payload, state, ctx) -> (reply, new_state)
#   handle_cast: (payload, state, ctx) -> new_state
CallHandler = Callable[[Any, Any, "Ctx"], Any]
CastHandler = Callable[[Any, Any, "Ctx"], Any]


@dataclass
class SpawnSpec:
    # Request to spawn a child at runtime. Mirrors internal/wire.SpawnSpec: the subset of
    # a manifest thrall relevant to a dynamic child (always local scope, single instance).
    name: str
    cmd: str
    restart: str = ""  # permanent | transient | temporary (default permanent)
    durable: bool = False  # true -> casts go through JetStream
    event_log: bool = False  # true -> provision an event-sourcing log (append/rebuild)

    def _payload(self) -> dict:
        # Omit empty optional fields for wire parity with the Go/TS json (omitempty).
        p: dict = {"name": self.name, "cmd": self.cmd}
        if self.restart:
            p["restart"] = self.restart
        if self.durable:
            p["durable"] = self.durable
        if self.event_log:
            p["event_log"] = self.event_log
        return p


async def _lord_control(nc: Any, op: str, payload: Any, timeout: float) -> Any:
    # Send a spawn/stop request on the lord's control channel; return the reply payload,
    # or raise RuntimeError with the lord's refusal.
    req = {"v": 1, "id": _next_id(), "kind": "ctl", "op": op, "payload": payload,
           "ts": int(time.time() * 1000)}
    msg = await nc.request(_sub_lord_ctl(), _encode(req), timeout=timeout)
    reply = _decode(msg.data)
    if reply.get("status") == "error":
        err = reply.get("error") or {}
        raise RuntimeError(f"{err.get('type')}: {err.get('message')}")
    return reply.get("payload")


@dataclass
class Ctx:
    # WE DO NOT HIDE NATS BEHIND THE THRALL - full access to JetStream, KV, its own subjects.
    nats: Any
    name: str
    app: str
    # Structured logger pre-tagged with app and name, configured from the logging env the
    # lord injected - handlers should log through it.
    log: Any = None
    # trace is the correlation id of the message currently being handled; ctx.call/ctx.cast
    # propagate it to downstream messages so one operation can be followed across processes.
    trace: str = ""

    async def call(self, target: str, op: str, payload: Any = None, timeout: float = 5.0) -> Any:
        """Trace-propagating request/reply to another thrall (GenServer.call): the downstream
        message carries the trace of the message currently being handled."""
        req = {"v": 1, "id": _next_id(), "trace": _or_new_trace(self.trace), "kind": "call",
               "to": target, "op": op, "payload": payload if payload is not None else {},
               "ts": int(time.time() * 1000)}
        msg = await self.nats.request(_sub_call(self.app, target), _encode(req), timeout=timeout)
        reply = _decode(msg.data)
        if reply.get("status") == "error":
            err = reply.get("error") or {}
            raise RuntimeError(f"{err.get('type')}: {err.get('message')}")
        return reply.get("payload")

    async def cast(self, target: str, op: str, payload: Any = None) -> None:
        """Trace-propagating fire-and-forget to another thrall (GenServer.cast)."""
        e = {"v": 1, "id": _next_id(), "trace": _or_new_trace(self.trace), "kind": "cast",
             "to": target, "op": op, "payload": payload if payload is not None else {},
             "ts": int(time.time() * 1000)}
        await self.nats.publish(_sub_cast(self.app, target), _encode(e))

    async def append(self, event: Any) -> None:
        """Persist a domain event to this thrall's event log (a JetStream publish that waits for
        the stream ack). Requires the thrall to have opted into an event log (event_log = true).
        rebuild() replays it in init. Mirrors the Go SDK ctx.Append."""
        js = self.nats.jetstream()
        await js.publish(_sub_evt(self.app, self.name), _encode(event))

    async def start_child(self, spec: SpawnSpec, timeout: float = 5.0) -> str:
        # Ask the lord to spawn a new thrall at runtime - a child not in the manifest.
        # The lord supervises it one_for_one, outside any group strategy. Returns its name.
        reply = await _lord_control(self.nats, "spawn", spec._payload(), timeout)
        return reply["name"]

    async def stop_child(self, name: str, timeout: float = 5.0) -> None:
        # Ask the lord to drain and stop a dynamic child started via start_child.
        await _lord_control(self.nats, "stop", {"name": name}, timeout)


@dataclass
class ThrallDef:
    name: str
    init: Callable[["Ctx"], Any]
    handle_call: dict[str, CallHandler] = field(default_factory=dict)
    handle_cast: dict[str, CastHandler] = field(default_factory=dict)
    terminate: Optional[Callable[[str, Any], None]] = None


def def_thrall(name, init, handle_call=None, handle_cast=None, terminate=None) -> ThrallDef:
    return ThrallDef(name, init, handle_call or {}, handle_cast or {}, terminate)


async def _maybe(v):
    return await v if asyncio.iscoroutine(v) else v


def _ok_reply(req: dict, payload: Any) -> dict:
    return {"v": 1, "id": req.get("id"), "kind": "reply", "status": "ok", "payload": payload}


def _err_reply(req: dict, type_: str, message: str) -> dict:
    return {
        "v": 1,
        "id": req.get("id"),
        "kind": "reply",
        "status": "error",
        "error": {"type": type_, "message": message, "retryable": False},
    }


async def start(defn: ThrallDef) -> None:
    """Connect the thrall to the ether and run its lifecycle."""
    url = os.environ.get("AETHER_NATS_URL")
    app = os.environ.get("AETHER_APP")
    env_name = os.environ.get("AETHER_NAME", "")
    if not url or not app:
        raise RuntimeError("missing AETHER_NATS_URL / AETHER_APP - a thrall is started via `aether up`")
    name = defn.name or env_name
    if defn.init is None:
        raise RuntimeError(f"thrall {name}: init is required")
    durable = os.environ.get("AETHER_DURABLE") == "1"
    ca = os.environ.get("AETHER_NATS_CA")
    nkey_seed = os.environ.get("AETHER_NATS_NKEY_SEED")

    nc = await nats.connect(url, **_connect_kwargs(name, ca, nkey_seed))
    ctx = Ctx(nats=nc, name=name, app=app, log=new_logger(component="thrall", app=app, name=name))
    state = await _maybe(defn.init(ctx))

    stop = asyncio.Event()
    lock = asyncio.Lock()  # serialized mailbox: 1 handler changes state at a time
    stats = _MailboxStats()

    async def process_call(e: dict, msg) -> None:
        nonlocal state
        start = stats.begin()
        try:
            async with lock:
                ctx.trace = _or_new_trace(e.get("trace") or "")
                ctx.log.debug("handling call", op=e.get("op"), trace=ctx.trace)
                handler = defn.handle_call.get(e.get("op"))
                if handler is None:
                    await msg.respond(_encode(_err_reply(e, "unknown_op", f"unknown call op: {e.get('op')}")))
                    return
                try:
                    reply, state = await _maybe(handler(e.get("payload"), state, ctx))
                    await msg.respond(_encode(_ok_reply(e, reply)))
                except Exception as ex:  # noqa: BLE001
                    await msg.respond(_encode(_err_reply(e, "handler_error", str(ex))))
        finally:
            stats.end(start)

    async def process_cast(e: dict) -> None:
        nonlocal state
        start = stats.begin()
        try:
            async with lock:
                ctx.trace = _or_new_trace(e.get("trace") or "")
                ctx.log.debug("handling cast", op=e.get("op"), trace=ctx.trace)
                handler = defn.handle_cast.get(e.get("op"))
                if handler is not None:
                    try:
                        state = await _maybe(handler(e.get("payload"), state, ctx))
                    except Exception as ex:  # noqa: BLE001
                        ctx.log.error("cast handler failed", op=e.get("op"), err=str(ex))
        finally:
            stats.end(start)

    tasks: list[asyncio.Task] = []

    if durable:
        # Durable: call/info over core (synchronous), cast via a durable JetStream consumer.
        call_sub = await nc.subscribe(_sub_call(app, name))
        info_sub = await nc.subscribe(_sub_info(app, name))

        async def call_loop() -> None:
            async for msg in call_sub.messages:
                await process_call(_decode(msg.data), msg)

        async def info_loop() -> None:
            async for _msg in info_sub.messages:
                pass  # TODO handle_info

        async def cast_pull_loop() -> None:
            js = nc.jetstream()
            psub = await js.pull_subscribe(
                _sub_cast(app, name),
                durable=name,
                stream=_stream(app, name),
                config=ConsumerConfig(deliver_policy=DeliverPolicy.ALL),
            )
            while not stop.is_set():
                try:
                    msgs = await psub.fetch(1, timeout=1)
                except Exception:  # noqa: BLE001  (timeout / no messages)
                    continue
                for m in msgs:
                    await process_cast(_decode(m.data))  # process ...
                    await m.ack()  #                       ... and only then ack

        tasks += [asyncio.create_task(c()) for c in (call_loop, info_loop, cast_pull_loop)]
    else:
        # Non-durable: a single wildcard subscription (call/cast/info) -> FIFO.
        data_sub = await nc.subscribe(_sub_data(app, name))

        async def data_loop() -> None:
            async for msg in data_sub.messages:
                verb = msg.subject.rsplit(".", 1)[1]
                e = _decode(msg.data)
                if verb == "call":
                    await process_call(e, msg)
                elif verb == "cast":
                    await process_cast(e)

        tasks.append(asyncio.create_task(data_loop()))

    # ctl: controlled shutdown from the lord (drain / shutdown)
    ctl_sub = await nc.subscribe(_sub_ctl(name))

    async def ctl_loop() -> None:
        async for msg in ctl_sub.messages:
            e = _decode(msg.data)
            if e.get("op") in ("drain", "shutdown"):
                if defn.terminate:
                    defn.terminate(e.get("op"), state)
                stop.set()
                return

    async def heartbeat() -> None:
        interval = _heartbeat_interval()  # lord-configured interval (default 2.0s)
        while not stop.is_set():
            hb = {"v": 1, "kind": "hb", "to": name, "payload": stats.snapshot(), "ts": int(time.time() * 1000)}
            await nc.publish(_sub_hb(name), _encode(hb))
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    tasks += [asyncio.create_task(c()) for c in (ctl_loop, heartbeat)]

    fence_task = await _start_fencing_if_singleton(nc, name, ctx.log, stop)
    if fence_task is not None:
        tasks.append(fence_task)
    lord_fence_task = await _start_lord_liveness_fencing(nc, name, ctx.log, stop)
    if lord_fence_task is not None:
        tasks.append(lord_fence_task)

    await stop.wait()
    for t in tasks:
        t.cancel()
    await nc.drain()


def run(defn: ThrallDef) -> None:
    """Convenient entry point: asyncio.run(start(defn))."""
    asyncio.run(start(defn))


# --- FSM behaviour (mirrors the Go StartFSM and TS startFSM; an OTP gen_statem analogue) ---
# A finite state machine: always in exactly one named state, dispatching an incoming message to
# the current state's reaction for that op. Events on the wire are ordinary call/cast (the
# envelope is unchanged), so FSM thralls interoperate with GenServer callers.

_FSM_STATE_OP = "_state"  # reserved call op the machine answers with its current state


@dataclass
class Event:
    op: str
    payload: Any
    kind: str  # call | cast | timeout


@dataclass
class StateTimeout:
    after: float  # seconds (asyncio-native; the Go/TS SDKs use their own time units)
    op: str


@dataclass
class Outcome:
    data: Any
    next: Optional[str] = None
    reply: Any = None
    timeout: Optional[StateTimeout] = None


@dataclass
class Reaction:
    fn: Callable  # (Event, data, ctx) -> Outcome (sync or async)
    guard: Optional[Callable] = None  # (data, Event) -> bool; absent = always


@dataclass
class State:
    on: dict  # op -> Reaction
    timeout: Optional[StateTimeout] = None


@dataclass
class FSM:
    name: str
    initial: str
    init: Callable  # (ctx) -> data
    states: dict  # state -> State
    terminate: Optional[Callable] = None  # (reason, state, data)


def def_fsm(name, initial, init, states, terminate=None) -> FSM:
    return FSM(name, initial, init, states, terminate)


class _Machine:
    """Serialized state-machine core, independent of NATS (so it is unit-testable). All state
    mutation happens under one asyncio lock, so events never interleave; a state timeout is
    delivered as an event into the same lock via a generation-guarded timer."""

    def __init__(self, defn: FSM, ctx: "Ctx", data: Any, log: Any):
        self.defn = defn
        self.ctx = ctx
        self.log = log
        self.cur = defn.initial
        self.data = data
        self._lock = asyncio.Lock()
        self._stats = _MailboxStats()
        self._timeout_gen = 0
        self._timer = None

    def state(self) -> str:
        return self.cur

    def snapshot(self) -> dict:
        return self._stats.snapshot()

    async def send(self, ev: Event, req=None, respond=None, gen=None) -> None:
        start = self._stats.begin()
        try:
            async with self._lock:
                if gen is not None and gen != self._timeout_gen:
                    return  # superseded timeout
                await self._dispatch(ev, req, respond)
        finally:
            self._stats.end(start)

    async def _dispatch(self, ev: Event, req, respond) -> None:
        self.ctx.trace = _or_new_trace("" if ev.kind == "timeout" else ((req or {}).get("trace") or ""))
        self.log.debug("fsm event", state=self.cur, op=ev.op, kind=ev.kind, trace=self.ctx.trace)

        if ev.kind == "call" and ev.op == _FSM_STATE_OP:
            if respond is not None and req is not None:
                respond(_ok_reply(req, {"state": self.cur}))
            return

        st = self.defn.states.get(self.cur)
        r = st.on.get(ev.op) if st else None
        if r is None:
            self._unhandled(ev, req, respond, "no_transition", f"no transition for op {ev.op} in state {self.cur}")
            return
        if r.guard is not None and not r.guard(self.data, ev):
            self._unhandled(ev, req, respond, "guard_rejected", f"guard rejected op {ev.op} in state {self.cur}")
            return

        try:
            out = await _maybe(r.fn(ev, self.data, self.ctx))
        except Exception as ex:  # noqa: BLE001
            self.log.error("fsm handler failed", state=self.cur, op=ev.op, err=str(ex))
            if respond is not None and req is not None:
                respond(_err_reply(req, "handler_error", str(ex)))
            return

        self.data = out.data
        if respond is not None and req is not None:
            respond(_ok_reply(req, out.reply))
        if out.next and out.next != self.cur:
            self._enter(out.next, out.timeout)
        elif out.timeout is not None:
            self._arm(out.timeout)

    def _unhandled(self, ev, req, respond, typ, message):
        self.log.warn("fsm unhandled event", state=self.cur, op=ev.op, kind=ev.kind, reason=typ)
        if respond is not None and req is not None:
            respond(_err_reply(req, typ, message))

    def _enter(self, nxt, override):
        frm = self.cur
        if nxt not in self.defn.states:
            self.log.warn("fsm transition to unknown state", **{"from": frm, "to": nxt})
        self.cur = nxt
        self.log.info("fsm transition", **{"from": frm, "to": nxt})
        to = override if override is not None else self.defn.states.get(nxt, State(on={})).timeout
        self._arm(to)

    def _arm(self, to):
        self._timeout_gen += 1
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if to is None:
            return
        gen = self._timeout_gen
        op = to.op
        loop = asyncio.get_running_loop()
        self._timer = loop.call_later(
            to.after,
            lambda: asyncio.create_task(self.send(Event(op=op, payload={}, kind="timeout"), None, None, gen)),
        )

    def arm_initial(self):
        """Arm the initial state's timeout. Called once from within the running loop."""
        self._arm(self.defn.states.get(self.cur, State(on={})).timeout)

    def stop(self):
        self._timeout_gen += 1
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None


async def start_fsm(defn: FSM) -> None:
    """Connect a state-machine thrall to the ether and run its lifecycle."""
    url = os.environ.get("AETHER_NATS_URL")
    app = os.environ.get("AETHER_APP")
    env_name = os.environ.get("AETHER_NAME", "")
    if not url or not app:
        raise RuntimeError("missing AETHER_NATS_URL / AETHER_APP - a thrall is started via `aether up`")
    name = defn.name or env_name
    if not defn.initial:
        raise RuntimeError(f"fsm {name}: initial state is required")
    if defn.init is None:
        raise RuntimeError(f"fsm {name}: init is required")
    if defn.initial not in defn.states:
        raise RuntimeError(f"fsm {name}: initial state {defn.initial} is not in states")
    durable = os.environ.get("AETHER_DURABLE") == "1"
    ca = os.environ.get("AETHER_NATS_CA")
    nkey_seed = os.environ.get("AETHER_NATS_NKEY_SEED")

    nc = await nats.connect(url, **_connect_kwargs(name, ca, nkey_seed))
    ctx = Ctx(nats=nc, name=name, app=app, log=new_logger(component="thrall", app=app, name=name))
    data = await _maybe(defn.init(ctx))
    m = _Machine(defn, ctx, data, ctx.log)
    m.arm_initial()

    stop = asyncio.Event()

    async def on_call(e: dict, msg) -> None:
        box: dict = {}
        await m.send(Event(op=e.get("op"), payload=e.get("payload"), kind="call"), e, lambda reply: box.__setitem__("r", reply))
        if "r" in box:
            await msg.respond(_encode(box["r"]))

    async def on_cast(e: dict) -> None:
        await m.send(Event(op=e.get("op"), payload=e.get("payload"), kind="cast"), e, None)

    tasks: list[asyncio.Task] = []

    if durable:
        call_sub = await nc.subscribe(_sub_call(app, name))
        info_sub = await nc.subscribe(_sub_info(app, name))

        async def call_loop() -> None:
            async for msg in call_sub.messages:
                await on_call(_decode(msg.data), msg)

        async def info_loop() -> None:
            async for _msg in info_sub.messages:
                pass  # info is out-of-band; not an FSM event yet

        async def cast_pull_loop() -> None:
            js = nc.jetstream()
            psub = await js.pull_subscribe(
                _sub_cast(app, name),
                durable=name,
                stream=_stream(app, name),
                config=ConsumerConfig(deliver_policy=DeliverPolicy.ALL),
            )
            while not stop.is_set():
                try:
                    msgs = await psub.fetch(1, timeout=1)
                except Exception:  # noqa: BLE001
                    continue
                for msg in msgs:
                    await on_cast(_decode(msg.data))
                    await msg.ack()

        tasks += [asyncio.create_task(c()) for c in (call_loop, info_loop, cast_pull_loop)]
    else:
        data_sub = await nc.subscribe(_sub_data(app, name))

        async def data_loop() -> None:
            async for msg in data_sub.messages:
                verb = msg.subject.rsplit(".", 1)[1]
                e = _decode(msg.data)
                if verb == "call":
                    await on_call(e, msg)
                elif verb == "cast":
                    await on_cast(e)

        tasks.append(asyncio.create_task(data_loop()))

    ctl_sub = await nc.subscribe(_sub_ctl(name))

    async def ctl_loop() -> None:
        async for msg in ctl_sub.messages:
            e = _decode(msg.data)
            if e.get("op") in ("drain", "shutdown"):
                m.stop()
                if defn.terminate:
                    defn.terminate(e.get("op"), m.state(), m.data)
                stop.set()
                return

    async def heartbeat() -> None:
        interval = _heartbeat_interval()  # lord-configured interval (default 2.0s)
        while not stop.is_set():
            hb = {"v": 1, "kind": "hb", "to": name, "payload": m.snapshot(), "ts": int(time.time() * 1000)}
            await nc.publish(_sub_hb(name), _encode(hb))
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    tasks += [asyncio.create_task(c()) for c in (ctl_loop, heartbeat)]

    fence_task = await _start_fencing_if_singleton(nc, name, ctx.log, stop)
    if fence_task is not None:
        tasks.append(fence_task)
    lord_fence_task = await _start_lord_liveness_fencing(nc, name, ctx.log, stop)
    if lord_fence_task is not None:
        tasks.append(lord_fence_task)

    await stop.wait()
    for t in tasks:
        t.cancel()
    await nc.drain()


def run_fsm(defn: FSM) -> None:
    """Convenient entry point: asyncio.run(start_fsm(defn))."""
    asyncio.run(start_fsm(defn))


# --- event-sourced rebuild (mirrors the Go/TS SDKs) ---
async def rebuild(ctx: "Ctx", initial: Any, fold: Callable) -> Any:
    """Reconstruct state by replaying the event log in order from the beginning. Call it from
    init: reads every persisted event (DeliverAll) into fold, starting from `initial`, and
    returns the reconstructed state. An empty log yields `initial`. The fold must be idempotent
    (the log is at-least-once). fold(event, state) may be sync or async."""
    js = ctx.nats.jetstream()
    stream = _stream_evt(ctx.app, ctx.name)
    try:
        info = await js.stream_info(stream)
    except Exception as ex:  # noqa: BLE001
        raise RuntimeError(f"event log stream {stream} (is event_log enabled?): {ex}")
    last = info.state.last_seq
    # Nothing to replay: an empty log, or one whose retention has purged every message.
    if last == 0 or info.state.messages == 0:
        return initial

    psub = await js.pull_subscribe(
        _sub_evt(ctx.app, ctx.name),
        durable=None,  # ephemeral: a one-shot replay consumer
        stream=stream,
        config=ConsumerConfig(deliver_policy=DeliverPolicy.ALL, ack_policy=AckPolicy.NONE),
    )
    try:
        state = initial
        seq = 0
        while seq < last:
            try:
                msgs = await psub.fetch(256, timeout=5)
            except Exception:  # noqa: BLE001  (timeout / no more)
                break
            for msg in msgs:
                state = await _maybe(fold(json.loads(msg.data), state))
                seq = msg.metadata.sequence.stream
        return state
    finally:
        await psub.unsubscribe()  # ephemeral consumer must be cleaned up (no self-clean)
