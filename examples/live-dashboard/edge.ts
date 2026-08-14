// A live-dashboard edge in TypeScript - the parity of examples/live-dashboard/edge/main.go. It holds
// browser SSE connections and pushes each client only the events of the site it is authorized for. The
// plumbing (SSE, per-connection scoped subscribe, backpressure, drain) is SSEStream; this file's job is
// only authorization - map the request to a site, then hand the connection to the stream.
import { startEdge, SSEStream, subjects } from "@hamicek/aether";
import * as http from "node:http";

const addr = 7392;

// siteFromToken is the demo authorization: a bearer token (query ?token= or Authorization header) maps to
// a site. A real edge would verify a JWT and take the site from its claims.
function siteFromToken(req: http.IncomingMessage): string | null {
  const url = new URL(req.url ?? "/", "http://localhost");
  let tok = url.searchParams.get("token") ?? "";
  if (!tok) tok = (req.headers["authorization"] ?? "").replace(/^Bearer /, "");
  if (tok === "tok-site-1") return "site-1";
  if (tok === "tok-site-2") return "site-2";
  return null;
}

let server: http.Server;
let stream: SSEStream;

await startEdge({
  init: (ctx) => {
    stream = new SSEStream(ctx);
    server = http.createServer(async (req, res) => {
      const url = new URL(req.url ?? "/", "http://localhost");
      if (url.pathname !== "/events") {
        res.writeHead(404);
        res.end("no route");
        return;
      }
      const site = siteFromToken(req);
      if (!site) {
        res.writeHead(401);
        res.end("unauthorized");
        return;
      }
      // The client sees ONLY its site's event subject - NATS never delivers anything else.
      await stream.serveClient(req, res, subjects.eventLog(ctx.app, site)); // aether.<app>.<site>.evt
    });
    ctx.log.info("live-dashboard edge (ts) listening", { addr });
  },

  // On drain: end the live SSE connections first (stream.close), then close the server.
  run: (ctx, stop) =>
    new Promise<void>((resolve, reject) => {
      server.on("error", reject);
      server.listen(addr);
      void stop.then(() => {
        stream.close();
        server.close(() => resolve());
      });
    }),
});
