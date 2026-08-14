import { test, expect } from "bun:test";
import { SSEStream } from "./stream";

// Live delivery / scope isolation is proven by a real run in the TS live-dashboard example (there is no
// embedded-NATS harness in the TS SDK). Here we cover the pure authorization guards, which need no NATS.

// fakeRes is a minimal ServerResponse stand-in capturing the status and body.
function fakeRes() {
  return {
    statusCode: 0,
    body: "",
    writableLength: 0,
    writeHead(code: number) {
      this.statusCode = code;
    },
    end(s?: string) {
      this.body = s ?? "";
    },
    write() {
      return true;
    },
  };
}

const stubStream = () => new SSEStream({ nats: {} } as any);

test("serveClient rejects an empty scope with 403", async () => {
  const res = fakeRes();
  await stubStream().serveClient({} as any, res as any);
  expect(res.statusCode).toBe(403);
});

test("serveClient rejects a wildcard scope with 400", async () => {
  for (const subj of ["test.>", "test.*.evt"]) {
    const res = fakeRes();
    await stubStream().serveClient({} as any, res as any, subj);
    expect(res.statusCode).toBe(400);
  }
});
