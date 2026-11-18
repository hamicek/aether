package thrall

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/hamicek/aether/internal/wire"
)

// deadlineSkew is the clock-skew tolerance this thrall applies when judging a call's deadline:
// wire.DefaultDeadlineSkewMs unless AETHER_DEADLINE_SKEW_MS says otherwise. A value that is not a
// non-negative integer is ignored rather than trusted.
func deadlineSkew() time.Duration {
	if v := os.Getenv(wire.EnvDeadlineSkewMs); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms >= 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return wire.DefaultDeadlineSkewMs * time.Millisecond
}

// deadlineGate stands in front of a thrall's call handlers and turns away calls whose caller has
// already stopped waiting. A call can sit in the mailbox behind a slow handler for longer than its
// caller's timeout; executing it then does work nobody receives the result of, and when the caller
// retries, a write happens twice. The gate is consulted once the mailbox lock is held, i.e. at the
// last moment before the handler would run. A concurrent web thrall consults it the same way once
// a call gets its slot, and additionally drops a call the moment its deadline passes while it is
// still waiting for one (see callPool).
//
// It narrows the window, it does not close it: a handler that starts just before the deadline can
// still finish after it. A caller that must not double-write sends an idempotency key as well.
type deadlineGate struct {
	skew  time.Duration
	stats *mailboxStats
	log   *slog.Logger
}

func newDeadlineGate(stats *mailboxStats, log *slog.Logger) deadlineGate {
	return deadlineGate{skew: deadlineSkew(), stats: stats, log: log}
}

// drop reports whether the call has expired, and if it has, turns it away (see expire).
func (g deadlineGate) drop(e wire.Envelope, respond func(wire.Envelope)) bool {
	overdue, expired := e.Expired(time.Now(), g.skew)
	if !expired {
		return false
	}
	g.expire(e, overdue, respond)
	return true
}

// expire turns away a call whose deadline has passed: it is accounted as dropped (not as
// processed), logged, and answered with ErrDeadlineExceeded - the caller is normally gone, but one
// whose clock disagrees with ours is still waiting and should learn at once that nothing ran.
func (g deadlineGate) expire(e wire.Envelope, overdue time.Duration, respond func(wire.Envelope)) {
	g.stats.drop()
	g.log.Warn("dropping expired call - its caller stopped waiting", slog.String("op", e.Op),
		slog.String("trace", e.Trace), slog.Int64("overdue_ms", overdue.Milliseconds()))
	respond(ErrorReply(e, wire.WireError{
		Type:      wire.ErrDeadlineExceeded,
		Message:   fmt.Sprintf("call expired %d ms before it could be handled", overdue.Milliseconds()),
		Retryable: true,
	}))
}

// expiresIn is how long from now the gate would start dropping the call: its deadline plus the
// skew tolerance. ok is false for a call without a deadline, which never expires.
func (g deadlineGate) expiresIn(e wire.Envelope, now time.Time) (wait time.Duration, ok bool) {
	if e.Deadline <= 0 {
		return 0, false
	}
	return time.UnixMilli(e.Deadline).Add(g.skew).Sub(now), true
}
