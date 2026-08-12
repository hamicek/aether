// Event-manager (gen_event) thrall in TypeScript: a single event fanned out to several handlers.
//
// `alarms` is an event manager with two handlers that BOTH see every event, in registration
// order, on one serialized mailbox - what raw NATS fan-out (independent subscribers) would not
// give you: co-located, ordered handlers that each keep their own state.
//
//   - `audit`  counts every alarm and logs the running total (its own state)
//   - `pager`  reacts only to a high-severity temperature, logging a would-page line
//
// Events are ordinary casts, so anything can raise one:  aether cast alarms temp_high '{"celsius":91}'
import { defEvent, startEvent } from "@hamicek/aether";

const alarms = defEvent({
  name: process.env.AETHER_NAME ?? "alarms",
  handlers: {
    // audit keeps a running count in its own state - independent of the pager handler.
    audit: {
      init: () => ({ count: 0 }),
      handleEvent: (ev, state: { count: number }, ctx) => {
        const count = state.count + 1;
        ctx.log.info("alarm audited", { op: ev.op, total: count });
        return { count };
      },
    },
    // pager reacts only to a hot temperature; other events pass through it untouched.
    pager: {
      init: () => ({}),
      handleEvent: (ev, state, ctx) => {
        const payload = (ev.payload ?? {}) as { celsius?: number };
        if (ev.op === "temp_high" && (payload.celsius ?? 0) >= 80) {
          ctx.log.warn("would page on-call", { celsius: payload.celsius });
        }
        return state;
      },
    },
  },
});

await startEvent(alarms);
