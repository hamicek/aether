package ingress_test

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/ingress"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// freeAddr returns a loopback address nothing listens on at the moment.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// TestRunIsTheWholeProgram runs the ingress the way its binaries do - ingress.Run with a command
// line, identity from the environment like any thrall under a lord - against a real web thrall, and
// then drains it the way the lord does. It covers what no handler-level test reaches: the flags
// arriving at the server (the body limit derived from the live connection, the socket bound in
// Init) and the edge lifecycle ending cleanly on a drain.
func TestRunIsTheWholeProgram(t *testing.T) {
	srv := embeddedNATS(t)
	startWebThrall(t, srv, "blog", func(req thrall.HTTPRequest, s struct{}, ctx *thrall.Ctx) (thrall.HTTPResponse, struct{}, error) {
		return thrall.TextResponse(http.StatusOK, req.Scheme+"://"+req.Host+req.Path+" from "+req.ClientIP), s, nil
	})
	routes := filepath.Join(t.TempDir(), "routes.json")
	if err := os.WriteFile(routes, []byte(`{"blog.test":"blog"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	addr := freeAddr(t)

	t.Setenv("AETHER_NAME", "ingress") // AETHER_NATS_URL and AETHER_APP are the web thrall's, the same node
	stopped := make(chan error, 1)
	go func() { stopped <- ingress.Run([]string{"-addr", addr, "-routes", routes, "-timeout", "2s"}) }()

	// The ingress binds asynchronously: wait until it accepts connections.
	url := "http://" + addr
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		select {
		case err := <-stopped:
			t.Fatalf("the ingress exited during start-up: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the ingress did not start listening on %s: %v", addr, err)
		}
	}

	// The test client is on loopback, which the program trusts as its reverse proxy by default.
	res := do(t, http.MethodGet, url+"/hello", "blog.test", nil, "X-Forwarded-For: 203.0.113.7", "X-Forwarded-Proto: https")
	if res.status != http.StatusOK || res.body != "https://blog.test/hello from 203.0.113.7" {
		t.Errorf("through the program: status %d, body %q", res.status, res.body)
	}
	if res.header.Get(ingress.TraceHeader) == "" {
		t.Error("the response must carry the trace header")
	}
	if denied := get(t, url+"/", "evil.test"); denied.status != http.StatusNotFound {
		t.Errorf("unknown host: status = %d, want 404", denied.status)
	}
	// The body limit is the one derived from this bus, not a number of its own.
	limit := ingress.BodyLimit(connect(t, srv).MaxPayload(), ingress.DefaultMaxHeaderBytes)
	if over := do(t, http.MethodPost, url+"/", "blog.test", io.LimitReader(zeroes{}, limit+1)); over.status != http.StatusRequestEntityTooLarge {
		t.Errorf("a body one byte over what the bus carries: status = %d, want 413", over.status)
	}

	// A drain from the lord ends the program cleanly and releases the socket.
	drain, _ := json.Marshal(wire.Envelope{V: 1, Kind: wire.KindCtl, Op: "drain"})
	if err := connect(t, srv).Publish(wire.Ctl("ingress"), drain); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("Run after a drain = %v, want a clean stop", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the ingress did not stop on drain")
	}
	if conn, err := net.Dial("tcp", addr); err == nil {
		conn.Close()
		t.Error("the socket is still open after the drain")
	}
}

// TestRunFailsStartupOnABadCommandLineOrRouteTable: a misconfigured ingress must not come up and
// deny everything; it must fail where the lord and the operator can see it.
func TestRunFailsStartupOnABadCommandLineOrRouteTable(t *testing.T) {
	srv := embeddedNATS(t)
	t.Setenv("AETHER_NATS_URL", srv.ClientURL())
	t.Setenv("AETHER_APP", testApp)
	t.Setenv("AETHER_NAME", "ingress")

	if err := ingress.Run([]string{"-max-conns", "0"}); err == nil {
		t.Error("a command line without a connection cap must be rejected")
	}
	if err := ingress.Run([]string{"-addr", freeAddr(t), "-routes", filepath.Join(t.TempDir(), "missing.json")}); err == nil {
		t.Error("a missing route table must fail startup")
	}
	if err := ingress.Run([]string{"-addr", freeAddr(t), "-registry", "-registry-scope", "intranet"}); err == nil {
		t.Error("an invalid registry scope must fail startup")
	}
	if err := ingress.Run([]string{"-h"}); err != nil {
		t.Errorf("-h = %v, want usage and a clean return", err)
	}
}
