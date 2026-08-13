package thrall

// SSEStream is the live-push counterpart to the edge: instead of pulling from the ether it pushes the
// ether OUT to browsers over Server-Sent Events. An edge thrall (via StartEdge) holds an HTTP server;
// on a stream endpoint the application authorizes the request and derives a subject scope, then hands
// the connection to ServeClient, which holds it open and forwards only the events within that scope.
//
// Authorization is deliberately the application's job (verify a token -> subject scope), because auth is
// policy, not mechanism. SSEStream supplies the plumbing: the SSE wire format, a per-connection NATS
// subscription (so NATS never delivers a client an out-of-scope event), backpressure by dropping for a
// slow client, and a drain that closes live connections so the HTTP server can shut down.

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// sseKeepAlive is how often an SSE comment (":\n\n") is sent to keep the connection alive through
// proxies during quiet periods; it is ignored by the EventSource client.
const sseKeepAlive = 20 * time.Second

// sseWriteTimeout bounds a single frame write, so a stuck client (one that stops reading) cannot block
// the writer loop forever and thus survive a drain. It is applied per write, not as a server-wide
// WriteTimeout (which would cut off the long-lived stream).
const sseWriteTimeout = 10 * time.Second

// sseClientBuffer bounds the per-connection queue. A client that cannot keep up drops events (a live
// view tolerates a missed event, not a stalled stream) rather than blocking the NATS callback.
const sseClientBuffer = 16

// SSEStream forwards ether events to browser clients over SSE. One instance is shared by an edge's
// handlers; each ServeClient call is one browser connection with its own scoped subscription.
type SSEStream struct {
	nc   *nats.Conn
	done chan struct{}
	once sync.Once
}

// NewSSEStream builds a stream bound to the thrall's NATS connection.
func NewSSEStream(ctx *Ctx) *SSEStream {
	return &SSEStream{nc: ctx.NATS, done: make(chan struct{})}
}

// Close ends every live ServeClient connection. Call it on drain BEFORE http.Server.Shutdown, which
// would otherwise block waiting for the (long-lived) SSE handlers to return.
func (s *SSEStream) Close() {
	s.once.Do(func() { close(s.done) })
}

// ServeClient holds one browser's SSE connection open, forwarding events from the given subjects until
// the client disconnects or Close is called. It must be called AFTER the application has authorized the
// request and mapped it to subjects - ServeClient itself does no authorization. It blocks for the life
// of the connection; a nil return is a normal end (client gone or drain).
func (s *SSEStream) ServeClient(w http.ResponseWriter, r *http.Request, subjects ...string) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return fmt.Errorf("sse: response writer is not a flusher")
	}
	if len(subjects) == 0 {
		http.Error(w, "no subject scope", http.StatusForbidden)
		return fmt.Errorf("sse: empty subject scope")
	}
	// Defense in depth for a security primitive: the scope must be exact subjects. A wildcard (from an
	// application bug that let a client-controlled segment into the subject) would silently widen the scope.
	for _, subj := range subjects {
		if strings.ContainsAny(subj, "*>") {
			http.Error(w, "invalid subject scope", http.StatusBadRequest)
			return fmt.Errorf("sse: subject %q contains a wildcard - scope must be exact subjects", subj)
		}
	}

	// A per-connection buffered channel; the subscription callbacks feed it, the writer loop drains it.
	// It is never closed (the connection owns it), so a callback racing Unsubscribe just does a safe send.
	ch := make(chan []byte, sseClientBuffer)
	subs := make([]*nats.Subscription, 0, len(subjects))
	for _, subj := range subjects {
		sub, err := s.nc.Subscribe(subj, func(m *nats.Msg) {
			select {
			case ch <- m.Data:
			default: // slow client - drop this event for it, keep the connection healthy
			}
		})
		if err != nil {
			for _, prev := range subs {
				_ = prev.Unsubscribe()
			}
			http.Error(w, "subscribe failed", http.StatusInternalServerError)
			return fmt.Errorf("sse: subscribe %q: %w", subj, err)
		}
		subs = append(subs, sub)
	}
	defer func() {
		for _, sub := range subs {
			_ = sub.Unsubscribe()
		}
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // tell nginx & co not to buffer the stream
	flusher.Flush()                           // send the headers so the browser's EventSource opens immediately

	// writeFrame writes one SSE frame under a per-write deadline, so a stuck client cannot block the loop
	// (and thus survive a drain). All chunk writes and the flush are checked; any error ends the connection.
	rc := http.NewResponseController(w)
	writeFrame := func(chunks ...[]byte) error {
		_ = rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout)) // best-effort; ignore if unsupported
		for _, c := range chunks {
			if _, err := w.Write(c); err != nil {
				return err
			}
		}
		return rc.Flush()
	}

	keepAlive := time.NewTicker(sseKeepAlive)
	defer keepAlive.Stop()

	for {
		select {
		case <-r.Context().Done(): // the browser disconnected
			return nil
		case <-s.done: // the edge is draining
			return nil
		case <-keepAlive.C:
			if writeFrame([]byte(":\n\n")) != nil {
				return nil
			}
		case msg := <-ch:
			// SSE frame: "data: <payload>\n\n". The payload is the raw event JSON from the ether.
			if writeFrame([]byte("data: "), msg, []byte("\n\n")) != nil {
				return nil
			}
		}
	}
}
