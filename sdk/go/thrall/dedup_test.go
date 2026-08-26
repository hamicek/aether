package thrall

import (
	"testing"

	"github.com/hamicek/aether/internal/wire"
)

// The dedup key prefers a caller-supplied Idem and falls back to the per-send envelope ID, so a
// keyed retry dedups while an unkeyed send still only matches an exact redelivery of itself.
func TestDedupKeyPrefersIdemOverID(t *testing.T) {
	if got := dedupKey(wire.Envelope{ID: "id-1", Idem: "key-1"}); got != "key-1" {
		t.Fatalf("dedupKey with Idem = %q, want %q", got, "key-1")
	}
	if got := dedupKey(wire.Envelope{ID: "id-1"}); got != "id-1" {
		t.Fatalf("dedupKey without Idem = %q, want the envelope ID %q", got, "id-1")
	}
	if got := dedupKey(wire.Envelope{}); got != "" {
		t.Fatalf("dedupKey with neither Idem nor ID = %q, want empty (dispatch then skips dedup)", got)
	}
}

// WithIdempotencyKey is the caller-side option that stamps the key onto the outgoing envelope.
func TestWithIdempotencyKeySetsOpt(t *testing.T) {
	if got := applySendOpts([]SendOption{WithIdempotencyKey("withdraw-42")}).idem; got != "withdraw-42" {
		t.Fatalf("idem = %q, want %q", got, "withdraw-42")
	}
	if got := applySendOpts(nil).idem; got != "" {
		t.Fatalf("idem with no options = %q, want empty", got)
	}
}

// A cast records presence (nil reply); a call records its reply bytes. get returns both.
func TestDedupCacheStoresReply(t *testing.T) {
	c := newDedupCache(8)

	c.put("cast-key", nil)
	if reply, seen := c.get("cast-key"); !seen || reply != nil {
		t.Fatalf("cast entry: got (%v, %v), want (nil, true)", reply, seen)
	}

	c.put("call-key", []byte(`{"value":42}`))
	if reply, seen := c.get("call-key"); !seen || string(reply) != `{"value":42}` {
		t.Fatalf("call entry: got (%s, %v), want the cached reply", reply, seen)
	}

	if _, seen := c.get("never"); seen {
		t.Fatal("an unseen key must not report as processed")
	}
}

// The cache is bounded: with max=2 the two generations hold at most ~2*max keys, so the oldest
// keys are evicted as new ones arrive - memory does not grow without limit.
func TestDedupCacheEvictsOldest(t *testing.T) {
	c := newDedupCache(2)

	// Fill and overflow: keys 0,1 land in the current generation; key 2 rotates them into
	// previous and starts a fresh current; keys are still reachable across both generations.
	for i, k := range []string{"k0", "k1", "k2"} {
		c.put(k, nil)
		if _, seen := c.get(k); !seen {
			t.Fatalf("just-put key %q (i=%d) not found", k, i)
		}
	}
	if _, seen := c.get("k0"); !seen {
		t.Fatal("k0 should still be reachable in the previous generation")
	}

	// Two more rotations push k0/k1 out entirely.
	for _, k := range []string{"k3", "k4"} {
		c.put(k, nil)
	}
	if _, seen := c.get("k0"); seen {
		t.Fatal("k0 should have been evicted after two generation rotations")
	}
	if _, seen := c.get("k4"); !seen {
		t.Fatal("the most recent key k4 must still be present")
	}
}

// A zero (or negative) max falls back to the default bound rather than a zero-size cache.
func TestDedupCacheDefaultMax(t *testing.T) {
	if c := newDedupCache(0); c.max != defaultIdempotencyMax {
		t.Fatalf("max with 0 = %d, want the default %d", c.max, defaultIdempotencyMax)
	}
}
