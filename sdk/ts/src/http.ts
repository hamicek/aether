// HTTP wire contract, mirroring internal/wire/http.go (the Go source of truth). See AE-091.
//
// A web thrall is an ordinary thrall whose single operation is a whole HTTP request/response
// (op http_request); it never owns a socket. A separate ingress translates HTTP <-> envelope and
// calls it over NATS. The body is a base64 string (the Go source carries it as []byte, which
// marshals to base64), so the contract is binary-safe and identical across languages. Headers
// mirror http.Header (a key maps to one or more values, preserving a repeated Set-Cookie). Query
// is the raw query string without the leading '?'.
//
// host, scheme and client_ip are the facts only the ingress can vouch for: the host the request was
// addressed to (as the client sent it, port included when there was one), "http" or "https" as the
// client reached the site, and the client's IP address (no port). The ingress derives them from its
// own connection and from the reverse proxies it is configured to trust, so read these rather than
// X-Forwarded-* headers. An ingress that predates them leaves them out; a missing value means "not
// stated", never a trusted default - do not rate-limit or authorize on a missing client_ip.

import { SERIALIZED, defThrall, start, withDispatch, type CallHandler, type Ctx, type ThrallDef } from "./thrall";

export const OP_HTTP_REQUEST = "http_request";

export interface HTTPRequest {
  method: string;
  path: string;
  query?: string;
  host?: string;
  scheme?: string;
  client_ip?: string;
  headers?: Record<string, string[]>;
  body?: string; // base64
}

export interface HTTPResponse {
  status: number;
  headers?: Record<string, string[]>;
  body?: string; // base64
}

// header builds a single-value header map, the common case when constructing a response. Keys are
// used verbatim, so pass the canonical MIME casing (e.g. "Content-Type").
export function header(key: string, value: string): Record<string, string[]> {
  return { [key]: [value] };
}

// textResponse / htmlResponse / jsonResponse build a response with the matching content type. The
// text is UTF-8 encoded then base64'd, so the body matches the Go []byte body on the wire.
export function textResponse(status: number, text: string): HTTPResponse {
  return { status, headers: header("Content-Type", "text/plain; charset=utf-8"), body: encodeBody(text) };
}

export function htmlResponse(status: number, html: string): HTTPResponse {
  return { status, headers: header("Content-Type", "text/html; charset=utf-8"), body: encodeBody(html) };
}

export function jsonResponse(status: number, value: unknown): HTTPResponse {
  return { status, headers: header("Content-Type", "application/json"), body: encodeBody(JSON.stringify(value)) };
}

// encodeBody base64-encodes a UTF-8 string for the body field. bodyBytes / bodyText decode a
// request or response body back - a handler reading an inbound body calls these, since the body
// stays base64 on the wire (unlike the Go SDK, where []byte decodes automatically).
export function encodeBody(text: string): string {
  return Buffer.from(text, "utf-8").toString("base64");
}

export function bodyBytes(msg: { body?: string }): Uint8Array {
  if (!msg.body) return new Uint8Array(0);
  return new Uint8Array(Buffer.from(msg.body, "base64"));
}

export function bodyText(msg: { body?: string }): string {
  if (!msg.body) return "";
  return Buffer.from(msg.body, "base64").toString("utf-8");
}

// A web thrall is written in one of two shapes, and the shape decides how its requests run:
//
//   - handle threads a state through the requests, GenServer style: each request gets the state
//     the previous one returned. That only means something one request at a time, so a handle
//     thrall is always serialized by the mailbox - a slow request holds every other one, and an
//     async handler does not interleave with the next request either.
//   - serve has no next state, and up to WebDef.concurrency of its requests run at the same time.
//
// WebHandler is the handle shape: given the request and the current state, return the response and
// the next state. It mirrors the call shape, so a web thrall can hold state across requests,
// serialized by the mailbox.
export type WebHandler<S> = (
  req: HTTPRequest,
  state: S,
  ctx: Ctx,
) => Promise<[HTTPResponse, S]> | [HTTPResponse, S];

// WebServer is the serve shape: given the request and the value init returned, return the
// response. Requests are served concurrently - interleaved at their awaits on the one event loop -
// so the mailbox no longer stands between them:
//
//   - shared is the one value init built, handed to every request. The runtime never replaces it
//     (there is no "next state" to return), so what it holds after init is what every request
//     sees. Treat it as read-only, or keep every change to it within one synchronous stretch: two
//     requests cannot interleave between awaits, but a read, an await and then a write based on
//     that read is a lost update waiting to happen.
//   - State that must change one request at a time belongs in an ordinary thrall that the handler
//     calls: its mailbox is the lock, and that is what a GenServer is for.
//   - ctx is this request's own (its trace and msgId are the request's); do not keep it or share
//     it with another request.
//   - Requests are independent: nothing orders one against another, not even two from one client.
export type WebServer<S> = (req: HTTPRequest, shared: S, ctx: Ctx) => Promise<HTTPResponse> | HTTPResponse;

// DEFAULT_WEB_CONCURRENCY is how many requests a serve web thrall handles at once when
// WebDef.concurrency is left out. A request mostly waits on calls to other thralls, so the number
// bounds how many of those are in flight, not CPU: high enough that a few slow requests do not
// hold up the rest of a site, low enough that a stalled dependency cannot be flooded. The same
// default as the Go and Python SDKs.
export const DEFAULT_WEB_CONCURRENCY = 32;

// WebDef defines a web thrall. Exactly one of handle and serve is required.
export interface WebDef<S> {
  name?: string;
  init?: (ctx: Ctx) => Promise<S> | S;
  // handle serves requests one at a time, threading the state (see WebHandler).
  handle?: WebHandler<S>;
  // serve serves requests concurrently over the value init returned (see WebServer).
  serve?: WebServer<S>;
  // concurrency is the most requests a serve thrall handles at the same time; further requests
  // wait for a free slot and are dropped unexecuted if their caller's deadline passes first.
  // Left out (or 0) means DEFAULT_WEB_CONCURRENCY. 1 serializes the requests through the mailbox,
  // exactly as handle does. It applies to serve only: a handle thrall is serialized by definition,
  // so setting it there is a mistake defWeb refuses rather than ignores.
  concurrency?: number;
  version?: string;
}

// webConcurrency validates the definition and returns how many requests it serves at once.
export function webConcurrency<S>(def: WebDef<S>): number {
  const who = `web thrall ${def.name ?? ""}`;
  const hasHandle = typeof def.handle === "function";
  const hasServe = typeof def.serve === "function";
  const asked = def.concurrency ?? 0;
  if (!hasHandle && !hasServe) throw new Error(`${who}: one of handle and serve is required`);
  if (hasHandle && hasServe) throw new Error(`${who}: handle and serve are two ways to write the same thrall - set one`);
  if (!Number.isInteger(asked) || asked < 0) throw new Error(`${who}: concurrency must be a non-negative integer, got ${def.concurrency}`);
  if (hasHandle && asked > 1) {
    throw new Error(`${who}: concurrency ${asked} needs serve - handle threads its state from one request to the next, which only works one request at a time`);
  }
  if (hasHandle) return SERIALIZED;
  return asked === 0 ? DEFAULT_WEB_CONCURRENCY : asked;
}

// defWeb builds a ThrallDef that answers http_request with the given handler; the definition
// carries how many requests it serves at once, so start(defWeb(def)) and startWeb(def) are the
// same thing. Exposed separately from startWeb so the definition can be inspected or tested
// without a connection.
export function defWeb<S>(def: WebDef<S>): ThrallDef<S> {
  const concurrency = webConcurrency(def);
  const init = def.init ?? (() => undefined as unknown as S);
  // The envelope payload is already parsed; the http_request contract is its shape. A serve
  // handler has no next state: the shared value is handed back as it came, whether its requests
  // run through the pool or (at concurrency 1) through the mailbox.
  const handler: CallHandler<S> = def.serve
    ? async (payload, shared, ctx) => [asResponse(await def.serve!(payload as HTTPRequest, shared, ctx)), shared]
    : async (payload, state, ctx) => {
        const [resp, next] = await def.handle!(payload as HTTPRequest, state, ctx);
        return [asResponse(resp), next];
      };
  const thrall = defThrall<S>({
    name: def.name ?? "",
    init,
    version: def.version,
    handleCall: { [OP_HTTP_REQUEST]: handler },
  });
  return withDispatch(thrall, { concurrency, failureReply: internalServerError });
}

// A failing handler costs one request, in either shape. A web thrall answers its own failures as
// HTTP, the way it answers its own "no": a handler that throws (or whose promise rejects, or that
// returns something that is not a response) is answered with a 500, the error is logged with the
// request's trace and its stack, counted (panics_total on the heartbeat, aether_panics_total on
// the lord) and becomes the thrall's last error - and the thrall keeps serving. With handle the
// state stays what it was before the request. refuse() and escalate() keep their meaning: an
// error reply of that type, and for escalate the crash it asks for. Only the handler's own promise
// is covered: a promise the handler started and did not await fails outside the request, and an
// unhandled rejection still ends the process, as it does for any thrall.

// internalServerError is that 500: bare, saying nothing about the inside of the node. The cause is
// in the thrall's log under the request's trace, which the ingress returns as X-Aether-Trace.
function internalServerError(): HTTPResponse {
  return textResponse(500, "Internal Server Error");
}

// asResponse insists that a handler returned a response. Without it a handler that forgot its
// return would send an empty reply, which the ingress can only report as an invalid upstream.
function asResponse(resp: unknown): HTTPResponse {
  if (typeof resp !== "object" || resp === null || typeof (resp as HTTPResponse).status !== "number") {
    throw new TypeError("the web handler did not return an HTTPResponse");
  }
  return resp as HTTPResponse;
}

// startWeb connects a web thrall to the ether and runs its lifecycle - a thin wrapper over start.
export async function startWeb<S>(def: WebDef<S>): Promise<void> {
  return start(defWeb(def));
}
