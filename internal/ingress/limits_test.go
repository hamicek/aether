package ingress

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/wire"
)

// TestBodyLimitFitsOneBusMessage proves the derived limit against the real thing it stands for: a
// request with a body of exactly the limit and a full header section, wrapped in a call envelope,
// still fits the bus's max payload.
func TestBodyLimitFitsOneBusMessage(t *testing.T) {
	const maxPayload = 1 << 20
	limit := BodyLimit(maxPayload, DefaultMaxHeaderBytes)
	if limit < 700<<10 || limit > 768<<10 {
		t.Fatalf("limit on a 1 MiB bus = %d bytes, want roughly 714 KiB", limit)
	}

	headers := map[string][]string{"Cookie": {strings.Repeat("c", DefaultMaxHeaderBytes-1024)}}
	payload, err := json.Marshal(wire.HTTPRequest{
		Method: "POST", Path: "/upload", Query: strings.Repeat("q", 512),
		Host: "shop.example", Scheme: "https", ClientIP: "2001:db8::1",
		Headers: headers, Body: bytes.Repeat([]byte{0xff}, int(limit)),
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := json.Marshal(wire.Envelope{
		V: 1, ID: "_INBOX.0123456789abcdefghijkl", Trace: "_INBOX.0123456789abcdefghijkl", Kind: wire.KindCall,
		To: "a-thrall-with-a-long-name", Op: wire.OpHTTPRequest, Payload: payload,
		TS: time.Now().UnixMilli(), Deadline: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(envelope) > maxPayload {
		t.Errorf("a request at the limit takes %d bytes on the bus, more than the %d it carries", len(envelope), maxPayload)
	}
}

func TestBodyLimitOnASmallBus(t *testing.T) {
	if got := BodyLimit(int64(DefaultMaxHeaderBytes), DefaultMaxHeaderBytes); got != 0 {
		t.Errorf("a bus with no room beside the headers must give 0, got %d", got)
	}
	if got := BodyLimit(8<<20, DefaultMaxHeaderBytes); got <= BodyLimit(1<<20, DefaultMaxHeaderBytes) {
		t.Errorf("a larger bus must carry a larger body, got %d", got)
	}
}

func TestBodyLimitForConfig(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := Config{MaxHeaderBytes: DefaultMaxHeaderBytes}
	carried := BodyLimit(1<<20, DefaultMaxHeaderBytes)

	if got, err := bodyLimitFor(1<<20, cfg, log); err != nil || got != carried {
		t.Errorf("no flag: got %d, %v; want what the bus carries (%d)", got, err, carried)
	}
	cfg.MaxBodyBytes = 1024
	if got, err := bodyLimitFor(1<<20, cfg, log); err != nil || got != 1024 {
		t.Errorf("a lower flag must be honoured: got %d, %v", got, err)
	}
	cfg.MaxBodyBytes = 10 << 20
	if got, err := bodyLimitFor(1<<20, cfg, log); err != nil || got != carried {
		t.Errorf("a flag above what the bus carries must be lowered to it: got %d, %v", got, err)
	}
	if _, err := bodyLimitFor(4096, cfg, log); err == nil {
		t.Error("a bus too small to carry any body must fail startup")
	}
}

func TestReadBody(t *testing.T) {
	post := func(body io.Reader) *http.Request { return httptest.NewRequest(http.MethodPost, "/", body) }

	body, failed := readBody(httptest.NewRecorder(), post(strings.NewReader("0123456789")), 10)
	if failed != nil || string(body) != "0123456789" {
		t.Errorf("a body of exactly the limit: %q, %+v", body, failed)
	}

	// Content-Length over the limit: refused without reading.
	unread := &countingReader{r: strings.NewReader(strings.Repeat("x", 11))}
	req := post(unread)
	req.ContentLength = 11
	if _, failed := readBody(httptest.NewRecorder(), req, 10); failed == nil || failed.status != http.StatusRequestEntityTooLarge {
		t.Errorf("declared oversize: %+v, want 413", failed)
	}
	if unread.n != 0 {
		t.Errorf("an oversized body was read (%d bytes) before being refused", unread.n)
	}

	// No Content-Length (chunked): the limit is found while reading, and it is still a 413.
	chunked := post(strings.NewReader(strings.Repeat("x", 11)))
	chunked.ContentLength = -1
	if _, failed := readBody(httptest.NewRecorder(), chunked, 10); failed == nil || failed.status != http.StatusRequestEntityTooLarge {
		t.Errorf("undeclared oversize: %+v, want 413", failed)
	}

	stalled := post(errReader{err: timeoutError{}})
	stalled.ContentLength = -1
	if _, failed := readBody(httptest.NewRecorder(), stalled, 10); failed == nil || failed.status != http.StatusRequestTimeout {
		t.Errorf("a body that timed out: %+v, want 408", failed)
	}
	aborted := post(errReader{err: errors.New("connection reset")})
	aborted.ContentLength = -1
	if _, failed := readBody(httptest.NewRecorder(), aborted, 10); failed == nil || failed.status != http.StatusBadRequest {
		t.Errorf("an aborted body: %+v, want 400", failed)
	}

	if body, failed := readBody(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), 10); failed != nil || body != nil {
		t.Errorf("no body: %q, %+v", body, failed)
	}
}

// deadlineRecorder is a response writer that takes a write deadline, like a real connection's.
type deadlineRecorder struct {
	*httptest.ResponseRecorder
	deadline time.Time
}

func (d *deadlineRecorder) SetWriteDeadline(t time.Time) error {
	d.deadline = t
	return nil
}

// TestResponsesAreWrittenUnderTheWriteTimeout proves the write clock starts when the ingress has a
// response - for the ones it produces itself as well.
func TestResponsesAreWrittenUnderTheWriteTimeout(t *testing.T) {
	const writeTimeout = 7 * time.Second
	h := NewHandler(nil, StaticRoutes{}, Options{App: "web", WriteTimeout: writeTimeout})
	rec := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	before := time.Now()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	after := time.Now()
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want the default-deny 404", rec.Code)
	}
	if rec.deadline.Before(before.Add(writeTimeout)) || rec.deadline.After(after.Add(writeTimeout)) {
		t.Errorf("write deadline %v is not now + the write timeout", rec.deadline)
	}
}

func TestListenCapsConnections(t *testing.T) {
	if _, err := Listen("127.0.0.1:0", 0); err == nil {
		t.Error("a cap of 0 must be rejected")
	}
	ln, err := Listen("127.0.0.1:0", 1)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				close(accepted)
				return
			}
			accepted <- conn
		}
	}()
	dial := func() net.Conn {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}

	dial()
	first := <-accepted
	dial() // connects (the kernel queues it) but must not be handed out while the first is open
	select {
	case <-accepted:
		t.Fatal("a second connection was accepted past the cap of 1")
	case <-time.After(150 * time.Millisecond):
	}
	first.Close()
	first.Close() // a double close must not free two slots
	select {
	case second := <-accepted:
		defer second.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("closing the first connection did not let the second in")
	}
	dial()
	select {
	case <-accepted:
		t.Fatal("a double close freed a slot it did not hold")
	case <-time.After(150 * time.Millisecond):
	}

	// Closing the listener wakes the Accept that is waiting for a slot.
	ln.Close()
	select {
	case _, open := <-accepted:
		if open {
			t.Fatal("a connection was accepted after the listener closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not return when the listener closed")
	}
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// timeoutError is what a read past the connection's deadline returns.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
