package thrall

import (
	"fmt"
	"log/slog"
	"runtime/debug"
)

// A panic in a handler is a bug, not an outcome the handler chose - and what it should cost
// depends on what the thrall is:
//
//   - An ordinary thrall (Start, StartFSM) still crashes, on purpose. A GenServer's state is
//     whatever the handlers made of it, and a handler that blew up half way may have left it
//     half changed (a map mutated in place, a call to another thrall already made). Carrying on
//     with it is the one thing "let it crash" exists to avoid: the process exits abnormally, the
//     lord restarts it through Init, the state is rebuilt clean. What changes is how it dies: the
//     caller of the call that panicked gets an "escalated" error reply at once instead of waiting
//     out its timeout, the panic is logged with its trace and stack through the thrall's logger,
//     and (for Start; a state machine reports no last error) the reason reaches the lord as the
//     thrall's last error. A durable cast that panics is
//     not acknowledged - unlike an Escalate, where the handler decided about that message, a panic
//     says nothing about it, so it stays at-least-once and is redelivered.
//   - A web thrall (StartWeb) survives. Its requests are independent, and a gateway that dies
//     takes every user's request down with the one that hit the bug. The request that panicked is
//     answered with a 500, the panic is logged and counted, and the thrall keeps serving.
//
// An event manager already isolates a panicking handler from the other handlers (see event.go).

// exitPanic is the exit status of a thrall that crashed on a panic: the status the Go runtime
// itself uses for an unrecovered one, so the lord and an operator see the exit they always did.
const exitPanic = 2

// handlerPanic is a panic caught at the edge of a handler: the value it was raised with and the
// stack of the goroutine at that moment.
type handlerPanic struct {
	value any
	stack []byte
}

// reason is the one-line form used for the last error and the reply to the caller.
func (p *handlerPanic) reason() string { return fmt.Sprintf("panic: %v", p.value) }

// guard runs a handler invocation and returns the panic it raised, or nil when it returned.
func guard(invoke func()) (caught *handlerPanic) {
	defer func() {
		if value := recover(); value != nil {
			// debug.Stack inside the deferred call still shows the panicking frames.
			caught = &handlerPanic{value: value, stack: debug.Stack()}
		}
	}()
	invoke()
	return nil
}

// logPanic records a handler panic with everything needed to find it: the operation, the trace
// of the message that triggered it and the stack.
func logPanic(log *slog.Logger, msg, op, trace string, p *handlerPanic) {
	log.Error(msg, slog.String("op", op), slog.String("trace", trace),
		slog.String("panic", fmt.Sprint(p.value)), slog.String("stack", string(p.stack)))
}
