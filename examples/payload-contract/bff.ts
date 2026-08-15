import { defThrall, start, decode, ValidationError } from "@hamicek/aether";
import measurementSchema from "./schemas/measurement.schema.json";

// The BFF is the trust boundary: measurements arrive from the driver as untyped `unknown`
// payloads, and the BFF validates each one against the shared schema before it counts. This
// is the payload contract in use - the same measurement.schema.json the producer is built
// against. See PAYLOAD-CONTRACT.md (AE-042).

// Hand-written for the PoC; the codegen follow-up (AE-042 §8, bundle 2) generates this.
interface Measurement {
  siteId: string;
  metric: "voltage" | "current" | "temperature";
  value: number;
  unit?: string;
  ts: number;
}

const bff = defThrall<number>({
  name: "bff",

  init: () => 0, // count of accepted measurements

  handleCall: {
    accepted: (_payload, state) => [state, state],
  },

  handleCast: {
    // ingest is the boundary: decode() validates + types the payload in one step. A malformed
    // measurement is rejected here with a clear reason and never pollutes downstream state.
    ingest: (payload, state) => {
      try {
        const m = decode<Measurement>(measurementSchema, payload);
        console.log(`bff: accepted ${m.metric}=${m.value}${m.unit ?? ""} from ${m.siteId}`);
        return state + 1;
      } catch (e) {
        if (e instanceof ValidationError) {
          const why = e.problems.map((p) => `${p.path || "(root)"}: ${p.message}`).join("; ");
          console.log(`bff: rejected a malformed measurement - ${why}`);
          return state; // boundary rejected it; do not count
        }
        throw e;
      }
    },
  },
});

await start(bff);
