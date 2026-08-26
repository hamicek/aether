package thrall

import "github.com/hamicek/aether/internal/wire"

// SendOption customizes an outgoing call/cast, e.g. an idempotency key.
type SendOption func(*sendOpts)

type sendOpts struct {
	idem string
}

// WithIdempotencyKey attaches a stable idempotency key to a call/cast. On an idempotent thrall a
// retry carrying the same key is not re-processed: a duplicate cast is skipped and a duplicate
// call returns the first reply. See AE-077.
func WithIdempotencyKey(key string) SendOption {
	return func(o *sendOpts) { o.idem = key }
}

func applySendOpts(opts []SendOption) sendOpts {
	var o sendOpts
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// dedupKey is the idempotency key an idempotent thrall deduplicates on: the caller-supplied Idem
// when set, else the per-send envelope ID. Keying on ID never conflates two distinct sends (each
// mints a fresh ID) but still catches an exact redelivery of the same message.
func dedupKey(e wire.Envelope) string {
	if e.Idem != "" {
		return e.Idem
	}
	return e.ID
}

// defaultIdempotencyMax bounds the dedup cache when a thrall enables idempotence without a size.
const defaultIdempotencyMax = 1024

// dedupCache is a bounded, generational (FIFO) cache of processed idempotency keys and their
// cached call replies. Two generations bound memory at ~2*max with O(1) inserts and no LRU list:
// when the current generation fills it becomes the previous one and a fresh current starts; a
// lookup checks both. A cast stores a nil reply (presence alone means "already processed").
// Not safe for concurrent use - the caller (the serialized mailbox) holds its lock.
type dedupCache struct {
	max      int
	current  map[string][]byte
	previous map[string][]byte
}

func newDedupCache(max int) *dedupCache {
	if max <= 0 {
		max = defaultIdempotencyMax
	}
	return &dedupCache{max: max, current: make(map[string][]byte), previous: map[string][]byte{}}
}

// get returns the cached call reply (nil for a cast) and whether the key was already processed.
func (c *dedupCache) get(key string) (reply []byte, seen bool) {
	if r, ok := c.current[key]; ok {
		return r, true
	}
	if r, ok := c.previous[key]; ok {
		return r, true
	}
	return nil, false
}

// put records a processed key with its cached reply (nil for a cast), rotating generations when
// the current one is full.
func (c *dedupCache) put(key string, reply []byte) {
	if len(c.current) >= c.max {
		c.previous = c.current
		c.current = make(map[string][]byte, c.max)
	}
	c.current[key] = reply
}
