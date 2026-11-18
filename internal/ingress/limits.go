package ingress

import (
	"errors"
	"io"
	"net"
	"net/http"
	"time"
)

// Default limits of the ingress. They are chosen for a public site behind a reverse proxy: generous
// for a browser on a slow link, tight enough that a client which stalls, floods or opens thousands
// of connections costs a bounded amount of memory and no thrall time.
const (
	// DefaultTimeout bounds how long the ingress waits for a web thrall's reply before it answers
	// 504. It is also the deadline the call carries, so the thrall stops caring at the same moment.
	DefaultTimeout = 5 * time.Second
	// DefaultReadHeaderTimeout bounds how long a client may take to send the request line and headers.
	DefaultReadHeaderTimeout = 5 * time.Second
	// DefaultReadTimeout bounds the whole request, headers and body; a body that has not arrived by
	// then is answered 408.
	DefaultReadTimeout = 30 * time.Second
	// DefaultWriteTimeout bounds how long writing the response may take once the ingress has it.
	DefaultWriteTimeout = 30 * time.Second
	// DefaultIdleTimeout is how long a keep-alive connection may sit between requests.
	DefaultIdleTimeout = 2 * time.Minute
	// DefaultMaxConns caps the connections the ingress serves at once. A request is buffered whole
	// (it becomes one bus message), so this is what bounds the ingress's memory.
	DefaultMaxConns = 256
	// DefaultMaxHeaderBytes caps the request line and headers of one request.
	DefaultMaxHeaderBytes = 64 << 10
)

// natsDefaultMaxPayload is the largest message a NATS server accepts unless configured otherwise.
// It sizes the body limit of a handler that was not given one (Run always derives the limit from
// the live connection instead).
const natsDefaultMaxPayload = 1 << 20

// envelopeReserve is the room kept in a bus message for everything of an http_request call that is
// neither headers nor body: the envelope's own fields, the request facts and JSON punctuation.
const envelopeReserve = 8 << 10

// BodyLimit is the largest request body that still fits one bus message, given the connection's
// max payload and the cap on the request's header section. The body travels as base64 inside the
// JSON payload (four bytes on the wire for every three of the body), next to the headers and inside
// the envelope, so the limit is well below the max payload: about 714 KiB on a default 1 MiB bus.
// It returns 0 when the max payload leaves no room for a body at all.
//
// The header reserve assumes headers take about as many bytes in JSON as on the HTTP wire. That is
// an estimate (a header full of characters JSON has to escape grows), so the limit keeps almost
// every request that passes it deliverable, not all of them: the ingress still answers 413 when the
// finished message turns out too large for the bus.
func BodyLimit(maxPayload int64, maxHeaderBytes int) int64 {
	room := maxPayload - int64(maxHeaderBytes) - envelopeReserve
	if room <= 0 {
		return 0
	}
	return room / 4 * 3
}

// readBody reads the whole request body, which has to fit limit: the request becomes one bus
// message, so there is no forwarding a part of it. A body that is too large is refused with 413 -
// straight from Content-Length when the client states it, without reading a byte - and never
// truncated. A body that does not arrive within the server's read timeout is a 408.
func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, *failure) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, nil
	}
	if r.ContentLength > limit {
		return nil, &failure{status: http.StatusRequestEntityTooLarge, cause: "body_too_large",
			err: &bodyTooLargeError{limit: limit, declared: r.ContentLength}}
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err == nil {
		return body, nil
	}
	var tooLarge *http.MaxBytesError
	var netErr net.Error
	switch {
	case errors.As(err, &tooLarge):
		return nil, &failure{status: http.StatusRequestEntityTooLarge, cause: "body_too_large",
			err: &bodyTooLargeError{limit: limit}}
	case errors.As(err, &netErr) && netErr.Timeout():
		return nil, &failure{status: http.StatusRequestTimeout, cause: "body_timeout", err: err}
	default:
		// The client went away or sent a malformed body.
		return nil, &failure{status: http.StatusBadRequest, cause: "bad_request", err: err}
	}
}
