// SSEStream is the live-push counterpart to the edge, mirroring the Go SDK's SSEStream: instead of
// pulling from the ether it pushes the ether OUT to browsers over Server-Sent Events. An edge thrall
// (via startEdge) holds an HTTP server; on a stream endpoint the application authorizes the request and
// derives a subject scope, then hands the connection to serveClient, which holds it open and forwards
// only the events within that scope.
//
// Authorization is deliberately the application's job (verify a token -> subject scope). SSEStream
// supplies the plumbing: the SSE wire format, a per-connection NATS subscription (so NATS never delivers
// a client an out-of-scope event), backpressure by dropping for a slow client, and a drain that closes
// live connections so the HTTP server can shut down.

import type { IncomingMessage, ServerResponse } from "node:http";
import type { NatsConnection, Subscription } from "nats";
import type { Ctx } from "./thrall";

// sseKeepAliveMs: how often an SSE comment (":\n\n") is sent to keep the connection alive through proxies.
const sseKeepAliveMs = 20_000;

// sseWriteBufferLimit bounds the outbound write buffer: when a slow client lets node's buffer grow past
// this, further events are dropped (a live view drops a stale event rather than stalling). This is the
// byte-threshold analogue of the Go SDK's fixed 16-event per-connection buffer.
const sseWriteBufferLimit = 1 << 16; // 64 KiB

// SSEStream forwards ether events to browser clients over SSE. One instance is shared by an edge's
// handlers; each serveClient call is one browser connection with its own scoped subscription.
export class SSEStream {
  private readonly nc: NatsConnection;
  private closed = false;
  private readonly done: Promise<void>;
  private doneResolve!: () => void;

  constructor(ctx: Ctx) {
    this.nc = ctx.nats;
    this.done = new Promise<void>((resolve) => {
      this.doneResolve = resolve;
    });
  }

  // close ends every live serveClient connection. Call it on drain BEFORE closing the HTTP server, which
  // would otherwise wait for the (long-lived) SSE handlers.
  close(): void {
    if (!this.closed) {
      this.closed = true;
      this.doneResolve();
    }
  }

  // serveClient holds one browser's SSE connection open, forwarding events from the given subjects until
  // the client disconnects or close() is called. It must be called AFTER the application has authorized
  // the request and mapped it to subjects - serveClient itself does no authorization. It resolves when
  // the connection ends.
  async serveClient(_req: IncomingMessage, res: ServerResponse, ...subjects: string[]): Promise<void> {
    if (subjects.length === 0) {
      res.writeHead(403);
      res.end("no subject scope");
      return;
    }
    // Defense in depth for a security primitive: the scope must be exact subjects. A wildcard (from an
    // application bug that let a client-controlled segment into the subject) would silently widen the scope.
    for (const subj of subjects) {
      if (subj.includes("*") || subj.includes(">")) {
        res.writeHead(400);
        res.end("invalid subject scope");
        return;
      }
    }

    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no", // tell nginx & co not to buffer the stream
    });

    const decoder = new TextDecoder();
    const writeFrame = (data: Uint8Array): void => {
      // Backpressure: if the client is not keeping up, drop this event rather than buffering unboundedly.
      if (res.writableLength > sseWriteBufferLimit) return;
      res.write(`data: ${decoder.decode(data)}\n\n`);
    };

    // Per-connection subscriptions, one per exact subject; NATS never delivers anything outside the scope.
    const subs: Subscription[] = subjects.map((subj) =>
      this.nc.subscribe(subj, {
        callback: (err, msg) => {
          if (!err) writeFrame(msg.data);
        },
      }),
    );

    const keepAlive = setInterval(() => res.write(":\n\n"), sseKeepAliveMs);
    (keepAlive as { unref?: () => void }).unref?.();

    await new Promise<void>((resolve) => {
      let cleaned = false;
      const cleanup = (): void => {
        if (cleaned) return;
        cleaned = true;
        clearInterval(keepAlive);
        for (const sub of subs) sub.unsubscribe();
        resolve();
      };
      _req.on("close", cleanup); // the browser disconnected
      void this.done.then(() => {
        // draining: end the response and tear down
        res.end();
        cleanup();
      });
    });
  }
}
