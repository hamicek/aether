// HTTP wire contract, mirroring internal/wire/http.go (the Go source of truth). See AE-091.
//
// A web thrall is an ordinary genserver whose single operation is a whole HTTP request/response
// (op http_request); it never owns a socket. A separate ingress translates HTTP <-> envelope and
// calls it over NATS. The body is a base64 string (the Go source carries it as []byte, which
// marshals to base64), so the contract is binary-safe and identical across languages. Headers
// mirror http.Header (a key maps to one or more values, preserving a repeated Set-Cookie). Query
// is the raw query string without the leading '?'.

import { defThrall, start, type Ctx, type ThrallDef } from "./thrall";

export const OP_HTTP_REQUEST = "http_request";

export interface HTTPRequest {
  method: string;
  path: string;
  query?: string;
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

// WebHandler is the one function a web thrall implements: given the request and the current state,
// return the response and the next state. It mirrors the call shape, so a web thrall can hold state
// across requests, serialized by the mailbox.
export type WebHandler<S> = (
  req: HTTPRequest,
  state: S,
  ctx: Ctx,
) => Promise<[HTTPResponse, S]> | [HTTPResponse, S];

export interface WebDef<S> {
  name?: string;
  init?: (ctx: Ctx) => Promise<S> | S;
  handle: WebHandler<S>;
  version?: string;
}

// defWeb builds a ThrallDef that answers http_request with the given handler. Exposed separately
// from startWeb so the definition can be inspected or tested without a connection.
export function defWeb<S>(def: WebDef<S>): ThrallDef<S> {
  if (typeof def.handle !== "function") {
    throw new Error(`web thrall ${def.name ?? ""}: handle is required`);
  }
  const init = def.init ?? (() => undefined as unknown as S);
  return defThrall<S>({
    name: def.name ?? "",
    init,
    version: def.version,
    handleCall: {
      [OP_HTTP_REQUEST]: async (payload, state, ctx) => {
        // The envelope payload is already parsed; the http_request contract is its shape.
        const [resp, next] = await def.handle(payload as HTTPRequest, state, ctx);
        return [resp, next];
      },
    },
  });
}

// startWeb connects a web thrall to the ether and runs its lifecycle - a thin wrapper over start.
export async function startWeb<S>(def: WebDef<S>): Promise<void> {
  return start(defWeb(def));
}
