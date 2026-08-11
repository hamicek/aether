// Tracing demo in TypeScript - functionally identical to main.go and trace.py.
//
// Two thralls: an "api" thrall relays an incoming request to a "db" thrall via ctx.cast, which
// propagates the correlation trace. Both log the same trace for one logical operation, so the
// path can be reconstructed from the logs. One file plays both roles, selected by AETHER_NAME
// (injected by the lord). Run with AETHER_LOG_LEVEL=debug to see the shared trace.
import { defThrall, start, type Ctx } from "@hamicek/aether";

// runApi receives a "request" cast (the edge, from `aether cast`) and relays it to "db".
// ctx.cast carries the trace of the incoming message, so the whole path shares one id.
function runApi(): Promise<void> {
  return start(
    defThrall<number>({
      name: "api",
      init: () => 0,
      handleCast: {
        request: (payload, state, ctx: Ctx) => {
          ctx.log.info("api received request, relaying to db", { trace: ctx.trace });
          ctx.cast("db", "store", payload);
          return state;
        },
      },
    }),
  );
}

// runDb is the downstream thrall; it logs the trace it received, which must match the one the
// api thrall logged for the same request.
function runDb(): Promise<void> {
  return start(
    defThrall<number>({
      name: "db",
      init: () => 0,
      handleCast: {
        store: (payload, state, ctx: Ctx) => {
          ctx.log.info("db stored value", { trace: ctx.trace, payload });
          return state;
        },
      },
    }),
  );
}

const role = process.env.AETHER_NAME;
if (role === "api") await runApi();
else if (role === "db") await runDb();
else throw new Error(`unknown thrall ${role}`);
