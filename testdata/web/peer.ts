// The web thrall test peer, TypeScript flavour. See README.md in this directory.
import { startWeb, textResponse, type Ctx, type HTTPRequest, type HTTPResponse } from "../../sdk/ts/src/index";

interface Counter {
  hits: number;
}

const queryParam = (req: HTTPRequest, key: string): string | null => new URLSearchParams(req.query ?? "").get(key);

// respond is the routing both shapes share; count says what /hits reports for this request.
async function respond(req: HTTPRequest, ctx: Ctx, count: number): Promise<HTTPResponse> {
  switch (req.path) {
    case "/fast":
      return textResponse(200, "fast");
    case "/slow":
      await Bun.sleep(Number(queryParam(req, "ms") ?? "0"));
      return textResponse(200, "slow");
    case "/trace":
      await Bun.sleep(50);
      return textResponse(200, ctx.trace);
    case "/hits":
      return textResponse(200, String(count));
    default:
      return textResponse(404, "not found");
  }
}

const concurrency = process.env.WEB_CONCURRENCY ? Number(process.env.WEB_CONCURRENCY) : undefined;

if (process.env.WEB_SHAPE === "handle") {
  await startWeb<Counter>({
    init: () => ({ hits: 0 }),
    handle: async (req, state, ctx) => {
      const next = { hits: state.hits + 1 };
      return [await respond(req, ctx, next.hits), next];
    },
  });
} else {
  await startWeb<Counter>({
    init: () => ({ hits: 0 }),
    concurrency,
    // The shared value is changed in one synchronous step, before the first await, which is what
    // keeps a serve handler's shared state consistent on the single event loop.
    serve: (req, shared, ctx) => respond(req, ctx, ++shared.hits),
  });
}
