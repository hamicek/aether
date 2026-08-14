// A live-dashboard edge (model B) in TypeScript - the parity of examples/webserver-custom/main.go. It
// holds an HTTP server and does a per-request Authorization check (which the declarative ingress cannot
// express) before calling the counter thrall over the ether. Runs via startEdge, so it is supervised
// like any thrall (heartbeat, restart, drain, fencing) - and coexists with Go thralls under one lord.
import { startEdge } from "@hamicek/aether";
import * as http from "node:http";

const addr = 7393;

// siteFromToken is the demo authorization: a bearer token maps to a site. A real edge would verify a JWT.
function authorized(req: http.IncomingMessage): boolean {
  const auth = req.headers["authorization"];
  return auth === "Bearer secret";
}

let server: http.Server;

await startEdge({
  // init builds the server (with access to ctx for the handlers) before run serves it.
  init: (ctx) => {
    server = http.createServer(async (req, res) => {
      if (req.method !== "GET" || req.url !== "/value") {
        res.writeHead(404);
        res.end("no route");
        return;
      }
      // This is the reason it is model B, not A: a per-request auth check.
      if (!authorized(req)) {
        res.writeHead(401);
        res.end("unauthorized");
        return;
      }
      try {
        const value = await ctx.call<number>("counter", "get", {});
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(value));
      } catch (e) {
        ctx.log.error("call counter failed", { err: String(e) });
        res.writeHead(502);
        res.end("bad gateway");
      }
    });
    ctx.log.info("custom edge (ts) listening", { addr });
  },

  // run owns the socket: it serves until drain (stop), then closes the server itself. In TS the run-loop
  // awaits `stop` rather than blocking (as Go's ListenAndServe does), so no separate stop hook is needed.
  run: (ctx, stop) =>
    new Promise<void>((resolve, reject) => {
      server.on("error", reject); // e.g. EADDRINUSE -> abnormal exit -> lord restarts
      server.listen(addr);
      void stop.then(() => server.close(() => resolve()));
    }),
});
