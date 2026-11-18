package thrall

import (
	"sync"
	"time"

	"github.com/hamicek/aether/internal/wire"
)

// callPool is what a concurrent web thrall has in place of the serialized mailbox for its calls:
// it runs up to limit handlers at once and makes the rest wait their turn.
//
// Waiting is bounded by the caller, not by a queue length. Every call the pool is given waits on
// its own goroutine for a free slot; if its deadline passes first, it is dropped right then -
// unexecuted, accounted and answered exactly as the deadline gate drops a call - so calls nobody
// waits for any more do not pile up behind slow handlers. A call that carries no deadline waits
// as long as it takes, as it always has. Slots are handed out in arrival order in practice, but a
// concurrent thrall makes no ordering promise between requests.
type callPool struct {
	slots   chan struct{}  // one token per running handler
	pending sync.WaitGroup // every call submitted and not yet finished or dropped
	gate    deadlineGate
	stats   *mailboxStats
}

func newCallPool(limit int, gate deadlineGate, stats *mailboxStats) *callPool {
	return &callPool{slots: make(chan struct{}, limit), gate: gate, stats: stats}
}

// submit takes one received call and returns at once. handle runs on the call's own goroutine
// when a slot is free and the call's caller is still waiting; otherwise the call is dropped and
// respond carries the deadline_exceeded reply.
func (p *callPool) submit(e wire.Envelope, respond func(wire.Envelope), handle func()) {
	received := p.stats.begin()
	p.pending.Add(1)
	go func() {
		defer p.pending.Done()
		if overdue, expired := p.acquire(e); expired {
			p.gate.expire(e, overdue, respond)
			return
		}
		defer p.release()
		// The slot may have come free at the very moment the deadline passed; the gate decides,
		// as it does for the mailbox, at the last moment before the handler would run.
		if p.gate.drop(e, respond) {
			return
		}
		p.stats.started()
		defer p.stats.end(received)
		handle()
	}()
}

// acquire waits for a free slot. It reports expired - with how long past its deadline the call is -
// when the call's deadline (plus the skew tolerance) passes first; the slot is then not taken.
func (p *callPool) acquire(e wire.Envelope) (overdue time.Duration, expired bool) {
	wait, bounded := p.gate.expiresIn(e, time.Now())
	if !bounded {
		p.slots <- struct{}{}
		return 0, false
	}
	// One past the tolerance, so that a call dropped here is expired by the gate's own rule
	// (strictly later than deadline + skew).
	timer := time.NewTimer(wait + time.Millisecond)
	defer timer.Stop()
	select {
	case p.slots <- struct{}{}:
		return 0, false
	case <-timer.C:
		overdue, _ = e.Expired(time.Now(), p.gate.skew)
		return overdue, true
	}
}

func (p *callPool) release() { <-p.slots }

// wait blocks until every submitted call has finished or been dropped. Used by drain, after the
// thrall has stopped reading new messages.
func (p *callPool) wait() { p.pending.Wait() }
