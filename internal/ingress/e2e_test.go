package ingress_test

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/ingress"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// TestEndToEnd proves the whole path: an HTTP request enters the ingress, is forwarded over NATS as
// an http_request call to a REAL web thrall (thrall.StartWeb, the AE-091 helper), and the web
// thrall's HTML reply comes back out as the HTTP response. An unknown host is denied (default-deny).
func TestEndToEnd(t *testing.T) {
	srv := embeddedNATS(t)
	const html = "<h1>hello from the blog</h1>"
	startWebThrall(t, srv, "blog", func(req thrall.HTTPRequest, s struct{}, ctx *thrall.Ctx) (thrall.HTTPResponse, struct{}, error) {
		return thrall.HTMLResponse(http.StatusOK, html+" path="+req.Path), s, nil
	})
	url := front(t, connect(t, srv), ingress.StaticRoutes{"blog.test": "blog"}, ingress.Options{Timeout: 2 * time.Second})

	res := get(t, url+"/hello", "blog.test")
	if res.status != http.StatusOK {
		t.Fatalf("allowed host: status = %d, body = %q", res.status, res.body)
	}
	if res.body != html+" path=/hello" {
		t.Errorf("allowed host: body = %q, want the thrall's HTML", res.body)
	}

	// Default-deny: a host with no route never reaches the ether.
	if denied := get(t, url+"/", "evil.test"); denied.status != http.StatusNotFound {
		t.Errorf("unknown host: status = %d, want 404", denied.status)
	}
}

// TestCallCarriesTraceAndDeadline proves the ingress calls the web thrall the way the SDK client
// does: the handler sees the request's trace as ctx.Trace (so its own calls continue it), the same
// trace is handed back to the browser, and the envelope states when the ingress stops waiting.
func TestCallCarriesTraceAndDeadline(t *testing.T) {
	srv := embeddedNATS(t)
	nc := connect(t, srv)
	startWebThrall(t, srv, "blog", func(req thrall.HTTPRequest, s struct{}, ctx *thrall.Ctx) (thrall.HTTPResponse, struct{}, error) {
		return thrall.TextResponse(http.StatusOK, ctx.Trace), s, nil
	})
	const timeout = 2 * time.Second
	stub := startStubThrall(t, nc, "probe", func(req wire.Envelope) wire.Envelope {
		return thrall.OKReply(req, thrall.TextResponse(http.StatusOK, "ok"))
	})
	url := front(t, nc, ingress.StaticRoutes{"blog.test": "blog", "probe.test": "probe"}, ingress.Options{Timeout: timeout})

	res := get(t, url+"/", "blog.test")
	trace := res.header.Get(ingress.TraceHeader)
	if res.status != http.StatusOK || trace == "" {
		t.Fatalf("status = %d, trace header = %q; want 200 and a trace", res.status, trace)
	}
	if res.body != trace {
		t.Errorf("the handler saw trace %q, the browser was told %q", res.body, trace)
	}
	if again := get(t, url+"/", "blog.test"); again.header.Get(ingress.TraceHeader) == trace {
		t.Error("two requests must not share a trace")
	}

	before := time.Now()
	probed := get(t, url+"/", "probe.test")
	after := time.Now()
	envs := stub.envelopes()
	if len(envs) != 1 {
		t.Fatalf("the stub saw %d envelopes, want 1", len(envs))
	}
	env := envs[0]
	if env.Kind != wire.KindCall || env.Op != wire.OpHTTPRequest || env.To != "probe" {
		t.Errorf("envelope kind/op/to = %q/%q/%q", env.Kind, env.Op, env.To)
	}
	if env.Trace == "" || env.Trace != probed.header.Get(ingress.TraceHeader) {
		t.Errorf("envelope trace %q, response header %q", env.Trace, probed.header.Get(ingress.TraceHeader))
	}
	earliest, latest := before.Add(timeout).UnixMilli(), after.Add(timeout).UnixMilli()
	if env.Deadline < earliest || env.Deadline > latest {
		t.Errorf("envelope deadline %d is not now + the ingress timeout (%d..%d)", env.Deadline, earliest, latest)
	}
}

// TestSlowThrallIs504AndQueuedCallIsNotExecuted stages the outage the deadline exists for: a web
// thrall stuck on one request while another queues behind it. Both browsers get 504 when the
// ingress stops waiting - and the queued request, which the thrall only reaches after its caller
// has gone, is dropped instead of being executed for nobody.
func TestSlowThrallIs504AndQueuedCallIsNotExecuted(t *testing.T) {
	srv := embeddedNATS(t)
	var handled atomic.Int32
	release := make(chan struct{})
	startWebThrall(t, srv, "blog", func(req thrall.HTTPRequest, s struct{}, ctx *thrall.Ctx) (thrall.HTTPResponse, struct{}, error) {
		handled.Add(1)
		if req.Path == "/slow" {
			<-release
		}
		return thrall.TextResponse(http.StatusOK, "done"), s, nil
	})
	url := front(t, connect(t, srv), ingress.StaticRoutes{"blog.test": "blog"}, ingress.Options{Timeout: 300 * time.Millisecond})

	statuses := make(chan int, 2)
	go func() { statuses <- get(t, url+"/slow", "blog.test").status }()
	// Let the slow request reach the handler first, so the second one queues behind it.
	for deadline := time.Now().Add(5 * time.Second); handled.Load() == 0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the slow request never reached the handler")
		}
	}
	go func() { statuses <- get(t, url+"/queued", "blog.test").status }()
	for i := 0; i < 2; i++ {
		if status := <-statuses; status != http.StatusGatewayTimeout {
			t.Errorf("request %d: status = %d, want 504", i, status)
		}
	}

	// Unblock the thrall well after the queued call's deadline (plus the SDK's clock tolerance), then
	// make it work through its mailbox: a fresh request is answered, the queued one never ran.
	time.Sleep(wire.DefaultDeadlineSkewMs*time.Millisecond + 100*time.Millisecond)
	close(release)
	if res := get(t, url+"/fresh", "blog.test"); res.status != http.StatusOK {
		t.Fatalf("after the stall: status = %d, want 200", res.status)
	}
	if got := handled.Load(); got != 2 {
		t.Errorf("the handler ran %d times, want 2 (slow + fresh): the queued call must be dropped past its deadline", got)
	}
}

// TestErrorRepliesMapToGatewayStatuses covers the replies a call can end with other than a
// response: a call the thrall dropped past its deadline is a 504 like the ingress's own timeout,
// a failed handler is a 502, a reply that is not a usable http_response is a 502, and a thrall
// that is not running is a 503.
func TestErrorRepliesMapToGatewayStatuses(t *testing.T) {
	srv := embeddedNATS(t)
	nc := connect(t, srv)
	startStubThrall(t, nc, "expired", func(req wire.Envelope) wire.Envelope {
		return thrall.ErrorReply(req, wire.WireError{Type: wire.ErrDeadlineExceeded, Message: "dropped", Retryable: true})
	})
	startStubThrall(t, nc, "broken", func(req wire.Envelope) wire.Envelope {
		return thrall.ErrorReply(req, wire.WireError{Type: wire.ErrHandler, Message: "boom"})
	})
	startStubThrall(t, nc, "statusless", func(req wire.Envelope) wire.Envelope {
		return thrall.OKReply(req, map[string]any{"headers": map[string][]string{}})
	})
	startStubThrall(t, nc, "wildstatus", func(req wire.Envelope) wire.Envelope {
		return thrall.OKReply(req, wire.HTTPResponse{Status: 42})
	})
	routes := ingress.StaticRoutes{
		"expired.test": "expired", "broken.test": "broken", "statusless.test": "statusless",
		"wildstatus.test": "wildstatus", "down.test": "nobody",
	}
	url := front(t, nc, routes, ingress.Options{Timeout: time.Second})

	for host, want := range map[string]int{
		"expired.test":    http.StatusGatewayTimeout,
		"broken.test":     http.StatusBadGateway,
		"statusless.test": http.StatusBadGateway,
		"wildstatus.test": http.StatusBadGateway,
		"down.test":       http.StatusServiceUnavailable,
	} {
		res := get(t, url+"/", host)
		if res.status != want {
			t.Errorf("%s: status = %d, want %d", host, res.status, want)
		}
		if res.header.Get(ingress.TraceHeader) == "" {
			t.Errorf("%s: an ingress-produced %d must carry the trace header", host, res.status)
		}
	}
}
