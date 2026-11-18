package ingress

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

func TestNormalizeHost(t *testing.T) {
	cases := map[string]string{
		"Example.com:8080": "example.com",
		"blog.test":        "blog.test",
		"LOCALHOST:7391":   "localhost",
		"[::1]:80":         "[::1]",
		"BLOG.Example":     "blog.example",
	}
	for in, want := range cases {
		if got := normalizeHost(in); got != want {
			t.Errorf("normalizeHost(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestStaticRoutesResolve(t *testing.T) {
	routes := StaticRoutes{"blog.test": "blog"}
	if thrall, ok := routes.Resolve("Blog.Test:9000"); !ok || thrall != "blog" {
		t.Errorf("Resolve normalized host = %q, %v; want blog, true", thrall, ok)
	}
	if _, ok := routes.Resolve("nope.test"); ok {
		t.Error("Resolve of an unknown host must deny (default-deny)")
	}
	if _, ok := (StaticRoutes{}).Resolve("anything"); ok {
		t.Error("an empty table must deny every host")
	}
}

func TestLoadStaticRoutes(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "routes.json")
	if err := os.WriteFile(good, []byte(`{"Blog.Example:80":"blog","shop.example":"shop"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	routes, err := LoadStaticRoutes(good)
	if err != nil {
		t.Fatalf("LoadStaticRoutes: %v", err)
	}
	if thrall, ok := routes.Resolve("blog.example"); !ok || thrall != "blog" {
		t.Errorf("normalized key lookup = %q, %v; want blog, true", thrall, ok)
	}

	bad := filepath.Join(dir, "bad.json")
	os.WriteFile(bad, []byte(`{"host":""}`), 0o644)
	if _, err := LoadStaticRoutes(bad); err == nil {
		t.Error("an empty thrall must be rejected")
	}
	malformed := filepath.Join(dir, "malformed.json")
	os.WriteFile(malformed, []byte(`not json`), 0o644)
	if _, err := LoadStaticRoutes(malformed); err == nil {
		t.Error("malformed JSON must be rejected")
	}
	if _, err := LoadStaticRoutes(filepath.Join(dir, "missing.json")); err == nil {
		t.Error("a missing file must be rejected")
	}
}

func TestRequestToWire(t *testing.T) {
	body := []byte{0x00, 0x01, 0xff, 'h', 'i'}
	r := httptest.NewRequest(http.MethodPost, "/upload?q=cats&q=dogs", bytes.NewReader(body))
	r.Header.Add("X-Tag", "a")
	r.Header.Add("X-Tag", "b")

	req, err := requestToWire(r, defaultMaxBodyBytes)
	if err != nil {
		t.Fatalf("requestToWire: %v", err)
	}
	if req.Method != "POST" || req.Path != "/upload" || req.Query != "q=cats&q=dogs" {
		t.Errorf("method/path/query wrong: %+v", req)
	}
	if got := req.Headers["X-Tag"]; len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("repeated header lost: %v", got)
	}
	if !bytes.Equal(req.Body, body) {
		t.Errorf("binary body changed: %v", req.Body)
	}
}

func TestRequestToWireBodyCap(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(make([]byte, 100)))
	req, err := requestToWire(r, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(req.Body) != 10 {
		t.Errorf("body not capped: got %d bytes, want 10", len(req.Body))
	}
}

func TestWriteResponse(t *testing.T) {
	rr := httptest.NewRecorder()
	writeResponse(rr, wire.HTTPResponse{
		Status:  201,
		Headers: map[string][]string{"Set-Cookie": {"a=1", "b=2"}, TraceHeader: {"forged"}},
		Body:    []byte("done"),
	}, "trace-1")
	res := rr.Result()
	if res.StatusCode != 201 {
		t.Errorf("status = %d, want 201", res.StatusCode)
	}
	if cookies := res.Header["Set-Cookie"]; len(cookies) != 2 {
		t.Errorf("repeated Set-Cookie lost: %v", cookies)
	}
	if got := res.Header.Values(TraceHeader); len(got) != 1 || got[0] != "trace-1" {
		t.Errorf("trace header = %v, want only the ingress's own trace", got)
	}
	if rr.Body.String() != "done" {
		t.Errorf("body = %q", rr.Body.String())
	}
}

func TestFailureForCall(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		cause  string
	}{
		{"no responders", nats.ErrNoResponders, http.StatusServiceUnavailable, "thrall_unavailable"},
		{"reply timeout", nats.ErrTimeout, http.StatusGatewayTimeout, "thrall_timeout"},
		{"dropped past its deadline", &thrall.CallError{Type: thrall.ErrDeadlineExceeded, Retryable: true},
			http.StatusGatewayTimeout, "thrall_timeout"},
		{"wrapped deadline drop", fmt.Errorf("call: %w", &thrall.CallError{Type: thrall.ErrDeadlineExceeded}),
			http.StatusGatewayTimeout, "thrall_timeout"},
		{"handler error", &thrall.CallError{Type: thrall.ErrHandler, Message: "boom"}, http.StatusBadGateway, "thrall_error"},
		{"refusal", &thrall.CallError{Type: thrall.ErrRefused, Code: "nope"}, http.StatusBadGateway, "thrall_error"},
		{"undecodable reply", errors.New("decode reply"), http.StatusBadGateway, "invalid_reply"},
	}
	for _, c := range cases {
		f := failureForCall(c.err)
		if f.status != c.status || f.cause != c.cause {
			t.Errorf("%s: got %d/%s, want %d/%s", c.name, f.status, f.cause, c.status, c.cause)
		}
		if f.err == nil {
			t.Errorf("%s: the failure must keep the error for the log", c.name)
		}
	}
}

func TestForwardableStatus(t *testing.T) {
	for status, want := range map[int]bool{0: false, 42: false, 100: false, 199: false, 200: true, 404: true, 599: true, 600: false, 1000: false} {
		if got := forwardableStatus(status); got != want {
			t.Errorf("forwardableStatus(%d) = %v, want %v", status, got, want)
		}
	}
}

func TestHandlerDefaultDeny(t *testing.T) {
	// An unknown host is denied before the handler touches NATS, so a nil connection is safe here.
	h := NewHandler(nil, StaticRoutes{"blog.test": "blog"}, Options{App: "web"})
	rr := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Host = "evil.test"
	h.ServeHTTP(rr, r)
	if rr.Result().StatusCode != http.StatusNotFound {
		t.Errorf("unknown host -> %d, want 404", rr.Result().StatusCode)
	}
	if !strings.Contains(rr.Body.String(), "Not Found") {
		t.Errorf("unexpected body: %q", rr.Body.String())
	}
	if rr.Result().Header.Get(TraceHeader) == "" {
		t.Error("a response the ingress produces itself must carry the trace header too")
	}
}
