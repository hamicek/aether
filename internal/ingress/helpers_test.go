package ingress_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ingress"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// testApp is the ether app every thrall of these tests lives in.
const testApp = "web"

func embeddedNATS(t *testing.T) *natsserver.Server {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  filepath.Join(t.TempDir(), "js"),
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatalf("NATS did not start in time")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	return srv
}

func connect(t *testing.T, srv *natsserver.Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// startWebThrall runs a REAL web thrall (thrall.StartWeb, the SDK helper an application uses) in
// this process and returns once it answers calls. The thrall is drained when the test ends, so
// tests do not leave thralls behind reconnecting to a server that is gone. Thralls take their
// identity from the environment, so a test using this cannot run in parallel with another.
func startWebThrall(t *testing.T, srv *natsserver.Server, name string, handle thrall.WebHandler[struct{}]) {
	t.Helper()
	t.Setenv("AETHER_NATS_URL", srv.ClientURL())
	t.Setenv("AETHER_APP", testApp)
	t.Setenv("AETHER_NAME", name)
	exited := make(chan error, 1)
	go func() { exited <- thrall.StartWeb(thrall.WebDef[struct{}]{Handle: handle}) }()

	nc := connect(t, srv)
	t.Cleanup(func() {
		drain, _ := json.Marshal(wire.Envelope{V: 1, Kind: wire.KindCast, Op: "drain"})
		_ = nc.Publish(wire.Ctl(name), drain)
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Errorf("web thrall %q did not stop on drain", name)
		}
	})

	// The thrall subscribes asynchronously; an unknown op is answered (with an error) as soon as it
	// does, and gets "no responders" before that.
	probe := thrall.NewClient(nc, testApp)
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := probe.Call(name, "ready?", nil, time.Second)
		if _, answered := err.(*thrall.CallError); answered {
			return
		}
		select {
		case err := <-exited:
			t.Fatalf("web thrall %q exited early: %v", name, err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("web thrall %q did not come up: %v", name, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// stubThrall answers the call subject of a thrall by hand, for the replies a real thrall produces
// only in situations a test cannot stage reliably (a call dropped past its deadline) or must not
// produce at all (a malformed reply). It records every envelope it receives.
type stubThrall struct {
	mu   sync.Mutex
	seen []wire.Envelope
}

func startStubThrall(t *testing.T, nc *nats.Conn, name string, answer func(req wire.Envelope) wire.Envelope) *stubThrall {
	t.Helper()
	stub := &stubThrall{}
	sub, err := nc.Subscribe(wire.Call(testApp, name), func(msg *nats.Msg) {
		var req wire.Envelope
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Errorf("stub thrall %q: bad envelope: %v", name, err)
			return
		}
		stub.mu.Lock()
		stub.seen = append(stub.seen, req)
		stub.mu.Unlock()
		data, _ := json.Marshal(answer(req))
		_ = msg.Respond(data)
	})
	if err != nil {
		t.Fatalf("stub thrall %q: subscribe: %v", name, err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return stub
}

func (s *stubThrall) envelopes() []wire.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]wire.Envelope(nil), s.seen...)
}

// front serves an ingress handler on a real local socket and returns its base URL.
func front(t *testing.T, nc *nats.Conn, routes ingress.RouteProvider, opts ingress.Options) string {
	t.Helper()
	if opts.App == "" {
		opts.App = testApp
	}
	srv := httptest.NewServer(ingress.NewHandler(nc, routes, opts))
	t.Cleanup(srv.Close)
	return srv.URL
}

// serve runs an ingress handler the way the program does - NewServer over a Listen listener - so a
// test exercises the real socket limits, and returns the address it listens on.
func serve(t *testing.T, handler http.Handler, opts ingress.ServerOptions, maxConns int) string {
	t.Helper()
	ln, err := ingress.Listen("127.0.0.1:0", maxConns)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := ingress.NewServer(handler, opts)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

// lockedBuffer collects log output written from the server's goroutines while a test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// response is what a test needs from an HTTP exchange.
type response struct {
	status int
	body   string
	header http.Header
}

// do sends one request with the given Host header and optional extra headers ("Name: value"). It is
// safe to call from a goroutine other than the test's.
func do(t *testing.T, method, url, host string, body io.Reader, headers ...string) response {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	for _, h := range headers {
		name, value, ok := strings.Cut(h, ": ")
		if !ok {
			t.Fatalf("malformed test header %q", h)
		}
		req.Header.Add(name, value)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		// Errorf, not Fatalf: tests send requests from goroutines of their own, where FailNow is not
		// allowed. The zero response fails whatever the caller asserts next.
		t.Errorf("%s %s (Host %s): %v", method, url, host, err)
		return response{}
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return response{status: res.StatusCode, body: string(b), header: res.Header}
}

func get(t *testing.T, url, host string) response {
	t.Helper()
	return do(t, http.MethodGet, url, host, nil)
}
