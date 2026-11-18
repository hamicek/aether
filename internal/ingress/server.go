package ingress

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// ServerOptions are the socket-level limits of the ingress's HTTP server. Zero values fall back to
// the Default* constants.
type ServerOptions struct {
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
	Log               *slog.Logger
}

// NewServer builds the HTTP server the ingress serves its handler on, with every phase of a
// connection's life bounded: reading the headers, reading the whole request, and sitting idle
// between requests. Writing the response is bounded by the handler (Options.WriteTimeout), which
// starts that clock when it has a response to write rather than when the request arrived - so the
// write limit does not have to be added up with the time a thrall is allowed to take.
func NewServer(handler http.Handler, opts ServerOptions) *http.Server {
	if opts.ReadHeaderTimeout <= 0 {
		opts.ReadHeaderTimeout = DefaultReadHeaderTimeout
	}
	if opts.ReadTimeout <= 0 {
		opts.ReadTimeout = DefaultReadTimeout
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = DefaultIdleTimeout
	}
	if opts.MaxHeaderBytes <= 0 {
		opts.MaxHeaderBytes = DefaultMaxHeaderBytes
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: opts.ReadHeaderTimeout,
		ReadTimeout:       opts.ReadTimeout,
		IdleTimeout:       opts.IdleTimeout,
		MaxHeaderBytes:    opts.MaxHeaderBytes,
		// What net/http reports on its own (a malformed request, a TLS handshake sent to a plain
		// port) goes to the same structured log as everything else instead of bare stderr lines.
		ErrorLog: slog.NewLogLogger(opts.Log.Handler(), slog.LevelWarn),
	}
}

// Listen binds addr and caps the connections served at once at maxConns. Past the cap the ingress
// stops accepting: further connections wait in the kernel's accept queue until one of the served
// ones closes, so overload turns into waiting (and eventually the client's or the proxy's timeout)
// rather than into unbounded goroutines and buffered requests.
func Listen(addr string, maxConns int) (net.Listener, error) {
	if maxConns <= 0 {
		return nil, fmt.Errorf("ingress: connection cap %d must be at least 1", maxConns)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("ingress: listen on %q: %w", addr, err)
	}
	return &limitListener{Listener: ln, slots: make(chan struct{}, maxConns), closed: make(chan struct{})}, nil
}

// limitListener hands out at most cap(slots) connections at a time. A slot is taken before a
// connection is accepted and given back when that connection is closed.
type limitListener struct {
	net.Listener
	slots     chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func (l *limitListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.closed:
		return nil, net.ErrClosed
	}
	conn, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	return &limitConn{Conn: conn, release: func() { <-l.slots }}, nil
}

// Close stops accepting and wakes an Accept that is waiting for a free slot.
func (l *limitListener) Close() error {
	err := l.Listener.Close()
	l.closeOnce.Do(func() { close(l.closed) })
	return err
}

// limitConn gives its slot back exactly once, however many times it is closed.
type limitConn struct {
	net.Conn
	releaseOnce sync.Once
	release     func()
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.releaseOnce.Do(c.release)
	return err
}
