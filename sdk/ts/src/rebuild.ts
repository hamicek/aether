// Event-sourced rebuild for the TS SDK, mirroring the Go SDK: a thrall appends domain events to
// its retention event log and, in init, rebuilds state by replaying that log through a fold
// ("log is truth, state is a projection"). The event log is a separate retention stream
// (provisioned opt-in by the lord), independent of the WorkQueue durable mailbox. At-least-once
// + replay -> the fold must be idempotent.

import { type NatsConnection, JSONCodec, AckPolicy, DeliverPolicy } from "nats";
import { subjects } from "./subjects";
import type { Ctx } from "./thrall";

const jc = JSONCodec();

// appendEvent persists a domain event to a thrall's event log (a JetStream publish that waits
// for the stream ack, so it is durable). Wired onto ctx.append by start()/startFSM().
export async function appendEvent(nc: NatsConnection, app: string, name: string, event: unknown): Promise<void> {
  const js = nc.jetstream();
  await js.publish(subjects.eventLog(app, name), jc.encode(event));
}

// rebuild reconstructs state by replaying the event log in order from the beginning. Call it
// from init: it reads every persisted event (DeliverAll) into fold, starting from `initial`,
// and returns the reconstructed state. An empty log yields `initial`. The fold must be
// idempotent (the log is at-least-once).
export async function rebuild<S>(
  ctx: Ctx,
  initial: S,
  fold: (event: unknown, state: S) => Promise<S> | S,
): Promise<S> {
  const nc = ctx.nats;
  const stream = subjects.eventLogStream(ctx.app, ctx.name);
  const jsm = await nc.jetstreamManager();
  let last = 0;
  try {
    const info = await jsm.streams.info(stream);
    last = info.state.last_seq;
  } catch (err) {
    throw new Error(`event log stream ${stream} (is event_log enabled?): ${String(err)}`);
  }
  if (last === 0) return initial;

  // Ephemeral consumer over the whole log; read up to the last sequence captured above.
  const ci = await jsm.consumers.add(stream, {
    ack_policy: AckPolicy.None,
    deliver_policy: DeliverPolicy.All,
    inactive_threshold: 30_000_000_000, // 30s in ns; the ephemeral consumer self-cleans
  });
  const js = nc.jetstream();
  const consumer = await js.consumers.get(stream, ci.name);

  let state = initial;
  const iter = await consumer.fetch({ max_messages: Number(last), expires: 5000 });
  for await (const m of iter) {
    state = await fold(jc.decode(m.data), state);
    if (m.seq >= last) break;
  }
  return state;
}
