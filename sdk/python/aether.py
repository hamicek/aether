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
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import nats


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
from nats.js.api import ConsumerConfig, DeliverPolicy


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

    def _payload(self) -> dict:
        # Omit empty optional fields for wire parity with the Go/TS json (omitempty).
        p: dict = {"name": self.name, "cmd": self.cmd}
        if self.restart:
            p["restart"] = self.restart
        if self.durable:
            p["durable"] = self.durable
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
    durable = os.environ.get("AETHER_DURABLE") == "1"
    ca = os.environ.get("AETHER_NATS_CA")
    nkey_seed = os.environ.get("AETHER_NATS_NKEY_SEED")

    nc = await nats.connect(url, **_connect_kwargs(name, ca, nkey_seed))
    ctx = Ctx(nats=nc, name=name, app=app)
    state = await _maybe(defn.init(ctx))

    stop = asyncio.Event()
    lock = asyncio.Lock()  # serialized mailbox: 1 handler changes state at a time

    async def process_call(e: dict, msg) -> None:
        nonlocal state
        async with lock:
            handler = defn.handle_call.get(e.get("op"))
            if handler is None:
                await msg.respond(_encode(_err_reply(e, "unknown_op", f"unknown call op: {e.get('op')}")))
                return
            try:
                reply, state = await _maybe(handler(e.get("payload"), state, ctx))
                await msg.respond(_encode(_ok_reply(e, reply)))
            except Exception as ex:  # noqa: BLE001
                await msg.respond(_encode(_err_reply(e, "handler_error", str(ex))))

    async def process_cast(e: dict) -> None:
        nonlocal state
        async with lock:
            handler = defn.handle_cast.get(e.get("op"))
            if handler is not None:
                try:
                    state = await _maybe(handler(e.get("payload"), state, ctx))
                except Exception as ex:  # noqa: BLE001
                    print(f"[{name}] cast {e.get('op')} failed: {ex}")

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
        while not stop.is_set():
            hb = {"v": 1, "kind": "hb", "to": name, "ts": int(time.time() * 1000)}
            await nc.publish(_sub_hb(name), _encode(hb))
            try:
                await asyncio.wait_for(stop.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass

    tasks += [asyncio.create_task(c()) for c in (ctl_loop, heartbeat)]

    await stop.wait()
    for t in tasks:
        t.cancel()
    await nc.drain()


def run(defn: ThrallDef) -> None:
    """Convenient entry point: asyncio.run(start(defn))."""
    asyncio.run(start(defn))
