package ingress

import (
	"log/slog"
	"net/http"
	"time"
)

// exchange is what the ingress knows about one request by the time it is answered. It exists for
// the log: the access line and the failure line are written from it.
type exchange struct {
	start    time.Time
	trace    string
	facts    requestFacts
	target   string // the web thrall the host resolved to; empty when it did not get that far
	status   int
	bytesIn  int // request body
	bytesOut int // response body
}

// logAccess writes the access line: one per request, whoever produced the response. It is the
// record of what the site was asked and how it answered, so it carries what identifies the request
// (method, host, path, client, trace) and what happened (status, duration, sizes, thrall) - and
// deliberately nothing that may hold a secret: no query string (tokens and reset links travel
// there), no headers (cookies, authorization) and no bodies.
func (h *handler) logAccess(r *http.Request, ex exchange) {
	h.log.Info("http access",
		slog.String("method", r.Method),
		slog.String("host", r.Host),
		slog.String("path", r.URL.Path),
		slog.Int("status", ex.status),
		slog.Duration("duration", time.Since(ex.start)),
		slog.String("trace", ex.trace),
		slog.String("client_ip", ex.facts.clientIP),
		slog.String("thrall", ex.target),
		slog.Int("bytes_in", ex.bytesIn),
		slog.Int("bytes_out", ex.bytesOut),
	)
}

// logFailure writes the line that explains a response the ingress produced itself: which request
// (the same trace as its access line), the cause as a stable token and the underlying error. What
// goes wrong inside the node (5xx) is a warning; a request the ingress turns away for what it is
// (an unknown host, a body too large, a client that stalled) is routine and logged as information.
func (h *handler) logFailure(r *http.Request, ex exchange, f failure) {
	level := slog.LevelInfo
	if f.status >= http.StatusInternalServerError {
		level = slog.LevelWarn
	}
	attrs := []slog.Attr{
		slog.Int("status", f.status),
		slog.String("cause", f.cause),
		slog.String("method", r.Method),
		slog.String("host", r.Host),
		slog.String("path", r.URL.Path),
		slog.String("trace", ex.trace),
		slog.String("client_ip", ex.facts.clientIP),
	}
	if ex.target != "" {
		attrs = append(attrs, slog.String("thrall", ex.target))
	}
	if f.err != nil {
		attrs = append(attrs, slog.Any("err", f.err))
	}
	h.log.LogAttrs(r.Context(), level, "ingress request failed", attrs...)
}
