package ingress_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"

	"github.com/hamicek/aether/internal/ingress"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// countingThrall starts a real web thrall that answers every request with the size of the body it
// received, and returns the count of requests that reached it.
func countingThrall(t *testing.T, srv *natsserver.Server, name string) *atomic.Int32 {
	t.Helper()
	var handled atomic.Int32
	startWebThrall(t, srv, name, func(req thrall.HTTPRequest, s struct{}, ctx *thrall.Ctx) (thrall.HTTPResponse, struct{}, error) {
		handled.Add(1)
		return thrall.TextResponse(http.StatusOK, strconv.Itoa(len(req.Body))), s, nil
	})
	return &handled
}

// TestOversizedRequestIs413AndNeverForwarded proves the body limit is what one bus message really
// carries: a body of exactly the limit arrives whole at a real web thrall, and anything larger is
// answered 413 without the thrall hearing of it - whether the client declared the size or not, and
// also when it is the headers that push the message over.
func TestOversizedRequestIs413AndNeverForwarded(t *testing.T) {
	srv := embeddedNATS(t)
	nc := connect(t, srv)
	handled := countingThrall(t, srv, "blog")

	limit := ingress.BodyLimit(nc.MaxPayload(), ingress.DefaultMaxHeaderBytes)
	url := front(t, nc, ingress.StaticRoutes{"blog.test": "blog"}, ingress.Options{Timeout: 5 * time.Second, MaxBodyBytes: limit})

	atLimit := do(t, http.MethodPost, url+"/upload", "blog.test", bytes.NewReader(make([]byte, limit)))
	if atLimit.status != http.StatusOK || atLimit.body != strconv.FormatInt(limit, 10) {
		t.Fatalf("a body of exactly the limit (%d): status %d, thrall saw %q bytes", limit, atLimit.status, atLimit.body)
	}

	declared := do(t, http.MethodPost, url+"/upload", "blog.test", bytes.NewReader(make([]byte, limit+1)))
	if declared.status != http.StatusRequestEntityTooLarge {
		t.Errorf("one byte over, size declared: status = %d, want 413", declared.status)
	}
	// A reader the client cannot size is sent chunked, with no Content-Length to judge it by.
	undeclared := do(t, http.MethodPost, url+"/upload", "blog.test", io.LimitReader(zeroes{}, limit+1))
	if undeclared.status != http.StatusRequestEntityTooLarge {
		t.Errorf("one byte over, size not declared: status = %d, want 413", undeclared.status)
	}
	// A body within the limit under headers far beyond what the limit reserves for them: the message
	// as a whole does not fit, which is the same answer.
	heavy := do(t, http.MethodPost, url+"/upload", "blog.test", bytes.NewReader(make([]byte, limit)),
		"X-Padding: "+strings.Repeat("p", 200<<10))
	if heavy.status != http.StatusRequestEntityTooLarge {
		t.Errorf("a request too large because of its headers: status = %d, want 413", heavy.status)
	}

	if got := handled.Load(); got != 1 {
		t.Errorf("the thrall handled %d requests, want only the one that fit", got)
	}
}

// TestSlowClientsAreCutOff drives the three ways a client can hold a connection without sending a
// request the ingress could forward: trickling the headers, stalling in the body, and sitting idle
// after a response. Each ends within its limit, and none of the unfinished requests reaches a thrall.
func TestSlowClientsAreCutOff(t *testing.T) {
	srv := embeddedNATS(t)
	nc := connect(t, srv)
	handled := countingThrall(t, srv, "blog")

	handler := ingress.NewHandler(nc, ingress.StaticRoutes{"blog.test": "blog"}, ingress.Options{App: testApp})
	addr := serve(t, handler, ingress.ServerOptions{
		ReadHeaderTimeout: 200 * time.Millisecond, ReadTimeout: 400 * time.Millisecond, IdleTimeout: 300 * time.Millisecond,
	}, 16)
	// patience is how long a test waits for the server to act; every limit above is well inside it.
	const patience = 3 * time.Second
	dial := func() net.Conn {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { conn.Close() })
		_ = conn.SetDeadline(time.Now().Add(patience))
		return conn
	}

	t.Run("headers that never finish", func(t *testing.T) {
		conn := dial()
		fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: blog.test\r\n")
		if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
			t.Errorf("the server must close a connection whose headers stall, read returned %v", err)
		}
	})

	t.Run("a body that stalls", func(t *testing.T) {
		conn := dial()
		fmt.Fprint(conn, "POST /upload HTTP/1.1\r\nHost: blog.test\r\nContent-Length: 100\r\n\r\nhello")
		res, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("no answer to a stalled body: %v", err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusRequestTimeout {
			t.Errorf("a stalled body: status = %d, want 408", res.StatusCode)
		}
	})

	t.Run("an idle keep-alive connection", func(t *testing.T) {
		conn := dial()
		fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: blog.test\r\n\r\n")
		reader := bufio.NewReader(conn)
		res, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatalf("a complete request: %v", err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("a complete request: status = %d, want 200", res.StatusCode)
		}
		if _, err := reader.ReadByte(); err != io.EOF {
			t.Errorf("the server must close an idle connection, read returned %v", err)
		}
	})

	if got := handled.Load(); got != 1 {
		t.Errorf("the thrall handled %d requests, want only the complete one", got)
	}
}

// TestConnectionCapMakesFurtherClientsWait proves the cap on concurrent connections end to end: with
// the single slot held by an open keep-alive connection, a second client is not served until the
// first one goes away - and then it is.
func TestConnectionCapMakesFurtherClientsWait(t *testing.T) {
	srv := embeddedNATS(t)
	nc := connect(t, srv)
	startWebThrall(t, srv, "blog", func(req thrall.HTTPRequest, s struct{}, ctx *thrall.Ctx) (thrall.HTTPResponse, struct{}, error) {
		return thrall.TextResponse(http.StatusOK, "served"), s, nil
	})
	handler := ingress.NewHandler(nc, ingress.StaticRoutes{"blog.test": "blog"}, ingress.Options{App: testApp})
	addr := serve(t, handler, ingress.ServerOptions{IdleTimeout: time.Minute}, 1)

	holder, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer holder.Close()
	fmt.Fprint(holder, "GET / HTTP/1.1\r\nHost: blog.test\r\n\r\n")
	res, err := http.ReadResponse(bufio.NewReader(holder), nil)
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("the first client must be served: %v", err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	second := make(chan response, 1)
	go func() { second <- get(t, "http://"+addr+"/", "blog.test") }()
	select {
	case res := <-second:
		t.Fatalf("a second client was served (status %d) while the only slot was taken", res.status)
	case <-time.After(300 * time.Millisecond):
	}
	holder.Close()
	select {
	case res := <-second:
		if res.status != http.StatusOK || res.body != "served" {
			t.Errorf("the waiting client: status %d, body %q; want it served once a slot is free", res.status, res.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting client was not served after the slot was freed")
	}
}

// zeroes is an endless stream of zero bytes.
type zeroes struct{}

func (zeroes) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
