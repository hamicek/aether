package ingress

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// fakeClock is a manually advanced clock for exercising TTL expiry without sleeping.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// countingResolver returns a resolveFunc with a controllable result and a call counter, so a test can
// assert exactly when the cache does and does not reach the registry.
func countingResolver(thrall string, found bool, err error) (resolveFunc, *int, *[]string) {
	var calls int
	var hosts []string
	fn := func(host string) (string, bool, error) {
		calls++
		hosts = append(hosts, host)
		return thrall, found, err
	}
	return fn, &calls, &hosts
}

func TestRegistryCacheHitAndHostNormalization(t *testing.T) {
	resolve, calls, hosts := countingResolver("blog", true, nil)
	r := newRegistryRoutes(resolve, RegistryOptions{TTL: time.Minute})

	// A host with mixed case and a port must normalize before the lookup, and the second lookup of
	// the same host must be served from the cache (no second resolver call).
	for i := 0; i < 2; i++ {
		thrall, ok := r.Resolve("Blog.Test:9000")
		if !ok || thrall != "blog" {
			t.Fatalf("Resolve #%d = %q, %v; want blog, true", i, thrall, ok)
		}
	}
	if *calls != 1 {
		t.Errorf("resolver called %d times, want 1 (second lookup should hit the cache)", *calls)
	}
	if len(*hosts) != 1 || (*hosts)[0] != "blog.test" {
		t.Errorf("resolver saw hosts %v, want one normalized [blog.test]", *hosts)
	}
}

func TestRegistryNegativeResultCached(t *testing.T) {
	resolve, calls, _ := countingResolver("", false, nil)
	r := newRegistryRoutes(resolve, RegistryOptions{TTL: time.Minute})

	for i := 0; i < 3; i++ {
		if _, ok := r.Resolve("unknown.test"); ok {
			t.Fatalf("Resolve #%d admitted an unknown host", i)
		}
	}
	if *calls != 1 {
		t.Errorf("resolver called %d times, want 1 (a known-unroutable host must be cached too)", *calls)
	}
}

func TestRegistryTransportErrorNotCached(t *testing.T) {
	resolve, calls, _ := countingResolver("blog", true, errors.New("no responders"))
	r := newRegistryRoutes(resolve, RegistryOptions{TTL: time.Minute})

	// A transport failure denies the request (default-deny) ...
	if _, ok := r.Resolve("blog.test"); ok {
		t.Error("a transport error must deny the request")
	}
	// ... but must not be cached: the next request retries instead of latching the host out.
	if _, ok := r.Resolve("blog.test"); ok {
		t.Error("a transport error must deny the request")
	}
	if *calls != 2 {
		t.Errorf("resolver called %d times, want 2 (a transport error must not be cached)", *calls)
	}
}

func TestRegistryTTLExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	resolve, calls, _ := countingResolver("blog", true, nil)
	r := newRegistryRoutes(resolve, RegistryOptions{TTL: 5 * time.Second, Now: clock.Now})

	r.Resolve("blog.test")
	clock.advance(4 * time.Second) // still within the TTL
	r.Resolve("blog.test")
	if *calls != 1 {
		t.Fatalf("resolver called %d times before expiry, want 1", *calls)
	}
	clock.advance(2 * time.Second) // now past the 5s TTL
	r.Resolve("blog.test")
	if *calls != 2 {
		t.Errorf("resolver called %d times after expiry, want 2 (the entry must be refreshed)", *calls)
	}
}

// TestNatsResolverContractRoundTrip proves the NATS wiring against the site-registry resolve contract:
// a resolve request carries op/kind/scope/host, and a found reply maps to the target thrall while a
// not-found reply denies. A stub subscriber plays the registry so the test is self-contained.
func TestNatsResolverContractRoundTrip(t *testing.T) {
	srv := embeddedServer(t)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	const subject = "aether.ae.site-registry.call"
	var gotReq resolveRequest
	var gotOp, gotKind, gotTo string
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var env wire.Envelope
		if err := json.Unmarshal(msg.Data, &env); err != nil {
			t.Errorf("stub: bad envelope: %v", err)
			return
		}
		gotOp, gotKind, gotTo = env.Op, env.Kind, env.To
		_ = json.Unmarshal(env.Payload, &gotReq)

		// The stub's tiny table: blog.test is public, everything else is unknown.
		reply := resolveReply{Found: false}
		if gotReq.Host == "blog.test" {
			reply = resolveReply{Found: true, Thrall: "blog", Visibility: "public"}
		}
		_ = msg.Respond(mustMarshal(wire.Envelope{
			V: 1, ID: env.ID, Kind: wire.KindReply, Status: "ok", Payload: mustMarshal(reply),
		}))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	resolve := natsResolver(nc, subject, scopeVPN, 2*time.Second)

	thrall, found, err := resolve("blog.test")
	if err != nil {
		t.Fatalf("resolve known host: %v", err)
	}
	if !found || thrall != "blog" {
		t.Errorf("resolve known host = %q, %v; want blog, true", thrall, found)
	}
	// The request must match the contract: a resolve call carrying the ingress's scope.
	if gotOp != opResolve || gotKind != wire.KindCall {
		t.Errorf("request op/kind = %q/%q, want %q/%q", gotOp, gotKind, opResolve, wire.KindCall)
	}
	if gotReq.Scope != scopeVPN {
		t.Errorf("request scope = %q, want %q", gotReq.Scope, scopeVPN)
	}
	if gotTo != registryName {
		t.Errorf("request To = %q, want %q", gotTo, registryName)
	}

	if _, found, err := resolve("nope.test"); err != nil || found {
		t.Errorf("resolve unknown host = %v, %v; want deny, no error", found, err)
	}
}

// TestNatsResolverErrorReplyIsNotADeny proves an error reply from the registry is surfaced as a
// transport/protocol error (so it is not cached), not silently turned into an authoritative deny.
func TestNatsResolverErrorReplyIsNotADeny(t *testing.T) {
	srv := embeddedServer(t)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	const subject = "aether.ae.site-registry.call"
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var env wire.Envelope
		_ = json.Unmarshal(msg.Data, &env)
		_ = msg.Respond(mustMarshal(wire.Envelope{
			V: 1, ID: env.ID, Kind: wire.KindReply, Status: "error",
			Error: &wire.WireError{Type: "handler_error", Message: "boom"},
		}))
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	resolve := natsResolver(nc, subject, scopePublic, time.Second)
	if _, _, err := resolve("blog.test"); err == nil {
		t.Error("an error reply must be returned as an error, not a silent deny")
	}
}

// TestNewRegistryRoutesRejectsBadScope proves the startup fail-fast: a misconfigured scope is caught
// when the provider is built, not by silently denying every request.
func TestNewRegistryRoutesRejectsBadScope(t *testing.T) {
	if _, err := NewRegistryRoutes(nil, RegistryOptions{Scope: "intranet"}); err == nil {
		t.Error("an unknown scope must be rejected at construction")
	}
	if _, err := NewRegistryRoutes(nil, RegistryOptions{Scope: ""}); err != nil {
		t.Errorf("an empty scope must default to public, got error: %v", err)
	}
}

func embeddedServer(t *testing.T) *natsserver.Server {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host:     "127.0.0.1",
		Port:     -1,
		StoreDir: filepath.Join(t.TempDir(), "js"),
		NoSigs:   true,
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
