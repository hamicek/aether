import type { Envelope } from "./envelope";

// dedupKey is the idempotency key an idempotent thrall deduplicates on: the caller-supplied idem
// when set, else the per-send envelope id. Keying on id never conflates two distinct sends (each
// mints a fresh id) but still catches an exact redelivery of the same message.
export function dedupKey(e: Envelope): string {
  return e.idem && e.idem.length > 0 ? e.idem : (e.id ?? "");
}

export const DEFAULT_IDEMPOTENCY_MAX = 1024;

// DedupCache is a bounded, generational (FIFO) cache of processed idempotency keys and their
// cached call replies. Two generations bound memory at ~2*max with O(1) inserts and no LRU list:
// when the current generation fills it becomes the previous one and a fresh current starts; a
// lookup checks both. A cast stores `undefined` (presence alone means "already processed").
export class DedupCache {
  private readonly max: number;
  private current = new Map<string, unknown>();
  private previous = new Map<string, unknown>();

  constructor(max: number) {
    this.max = max > 0 ? max : DEFAULT_IDEMPOTENCY_MAX;
  }

  // get returns [cachedReply, seen]; reply is undefined for a cast or a miss.
  get(key: string): [unknown, boolean] {
    if (this.current.has(key)) return [this.current.get(key), true];
    if (this.previous.has(key)) return [this.previous.get(key), true];
    return [undefined, false];
  }

  // put records a processed key with its cached reply (undefined for a cast), rotating
  // generations when the current one is full.
  put(key: string, reply: unknown): void {
    if (this.current.size >= this.max) {
      this.previous = this.current;
      this.current = new Map();
    }
    this.current.set(key, reply);
  }
}
