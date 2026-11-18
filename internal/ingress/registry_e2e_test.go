package ingress_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ingress"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// stubRegistry is a self-contained stand-in for the commons site-registry thrall: an in-memory
// host -> (thrall, visibility) table that answers the resolve contract over NATS, including the
// public/vpn scope filter and default-deny. It lets the e2e prove the whole dynamic-routing arc
// without depending on an ae-commons checkout. It talks only the wire contract, like the real thrall.
type stubRegistry struct {
	mu    sync.Mutex
	sites map[string]stubSite
}

type stubSite struct {
	thrall     string
	visibility string // "public" | "vpn"
}

// stubResolveRequest / stubResolveReply mirror the site-registry resolve contract (contract.md) on
// the test's side of the wire, independently of the provider's own mirror, so the e2e checks the two
// ends actually agree on the envelope rather than sharing one struct.
type stubResolveRequest struct {
	Host  string `json:"host"`
	Scope string `json:"scope,omitempty"`
}

type stubResolveReply struct {
	Found      bool   `json:"found"`
	Thrall     string `json:"thrall,omitempty"`
	Visibility string `json:"visibility,omitempty"`
}

func (s *stubRegistry) register(host, thrall, visibility string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sites[host] = stubSite{thrall: thrall, visibility: visibility}
}

// resolve applies the contract's scope filter: a public query admits only public sites, a vpn query
// admits both. An unknown host or a visibility not admitted at the scope is default-denied.
func (s *stubRegistry) resolve(req stubResolveRequest) stubResolveReply {
	s.mu.Lock()
	defer s.mu.Unlock()
	site, ok := s.sites[req.Host]
	if !ok {
		return stubResolveReply{Found: false}
	}
	scope := req.Scope
	if scope == "" {
		scope = "public"
	}
	admitted := site.visibility == "public" || scope == "vpn"
	if !admitted {
		return stubResolveReply{Found: false}
	}
	return stubResolveReply{Found: true, Thrall: site.thrall, Visibility: site.visibility}
}

// serve subscribes the stub to the resolve call subject and answers each request with a reply
// envelope shaped exactly like a real thrall's (status "ok", payload = reply). It returns the
// subscription so the caller can tear it down.
func (s *stubRegistry) serve(t *testing.T, nc *nats.Conn, subject string) *nats.Subscription {
	t.Helper()
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var env wire.Envelope
		if err := json.Unmarshal(msg.Data, &env); err != nil {
			t.Errorf("stub registry: bad envelope: %v", err)
			return
		}
		var req stubResolveRequest
		if err := json.Unmarshal(env.Payload, &req); err != nil {
			t.Errorf("stub registry: bad resolve payload: %v", err)
			return
		}
		replyPayload, _ := json.Marshal(s.resolve(req))
		reply := wire.Envelope{V: 1, ID: env.ID, Kind: wire.KindReply, Status: "ok", Payload: replyPayload}
		data, _ := json.Marshal(reply)
		_ = msg.Respond(data)
	})
	if err != nil {
		t.Fatalf("stub registry subscribe: %v", err)
	}
	return sub
}

// TestEndToEndRegistry proves the dynamic-routing arc: an HTTP request enters the ingress, the
// registry RouteProvider resolves its Host against the (stub) site-registry over NATS, and the real
// web thrall's HTML comes back out. It covers a seeded public host, a host REGISTERED AT RUNTIME
// (dynamics - no ingress restart), an unknown host (default-deny), and a vpn-only host on a public
// ingress (the scope filter lives in the registry).
func TestEndToEndRegistry(t *testing.T) {
	srv := embeddedNATS(t)

	// A real web thrall (the AE-091 helper) serving a tiny HTML page; every admitted host routes to it.
	const html = "<h1>hello from the web thrall</h1>"
	t.Setenv("AETHER_NATS_URL", srv.ClientURL())
	t.Setenv("AETHER_APP", "web")
	t.Setenv("AETHER_NAME", "blog")
	webErr := make(chan error, 1)
	go func() {
		webErr <- thrall.StartWeb(thrall.WebDef[struct{}]{
			Handle: func(req thrall.HTTPRequest, s struct{}, ctx *thrall.Ctx) (thrall.HTTPResponse, struct{}, error) {
				return thrall.HTMLResponse(http.StatusOK, html+" path="+req.Path), s, nil
			},
		})
	}()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	// The stub registry, seeded with one public site and one vpn-only site.
	const registrySubject = "aether.ae.site-registry.call"
	reg := &stubRegistry{sites: map[string]stubSite{
		"blog.test":    {thrall: "blog", visibility: "public"},
		"vpnonly.test": {thrall: "blog", visibility: "vpn"},
	}}
	sub := reg.serve(t, nc, registrySubject)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	// A PUBLIC ingress backed by the registry. A short TTL keeps the cache from masking a runtime
	// change in this test, though the newly-registered host below is never queried before it exists.
	provider, err := ingress.NewRegistryRoutes(nc, ingress.RegistryOptions{
		Subject: registrySubject,
		Scope:   "public",
		TTL:     200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new registry provider: %v", err)
	}
	handler := ingress.NewHandler(nc, provider, ingress.Options{App: "web", Timeout: 2 * time.Second})
	front := httptest.NewServer(handler)
	t.Cleanup(front.Close)

	// Wait for the web thrall to subscribe: retry until it is no longer "no responders".
	body, status := getWithHost(t, front.URL+"/hello", "blog.test")
	deadline := time.Now().Add(5 * time.Second)
	for status == http.StatusServiceUnavailable && time.Now().Before(deadline) {
		select {
		case err := <-webErr:
			t.Fatalf("web thrall exited early: %v", err)
		default:
		}
		time.Sleep(50 * time.Millisecond)
		body, status = getWithHost(t, front.URL+"/hello", "blog.test")
	}

	// 1) A seeded public host routes end to end.
	if status != http.StatusOK {
		t.Fatalf("seeded host: status = %d, body = %q", status, body)
	}
	if body != html+" path=/hello" {
		t.Errorf("seeded host: body = %q, want the thrall's HTML", body)
	}

	// 2) Dynamics: a host that is denied before registration becomes routable after it, with no
	// ingress restart. Registration must also win over the cached negative, so poll until the short
	// TTL lets the change through - that proves the route table is live, cache included.
	if _, denied := getWithHost(t, front.URL+"/", "shop.test"); denied != http.StatusNotFound {
		t.Fatalf("precondition: shop.test must be unknown before registration, got %d", denied)
	}
	reg.register("shop.test", "blog", "public")
	body, status = getWithHost(t, front.URL+"/catalog", "shop.test")
	deadline = time.Now().Add(3 * time.Second)
	for status == http.StatusNotFound && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		body, status = getWithHost(t, front.URL+"/catalog", "shop.test")
	}
	if status != http.StatusOK {
		t.Errorf("newly registered host: status = %d, body = %q, want 200", status, body)
	} else if body != html+" path=/catalog" {
		t.Errorf("newly registered host: body = %q, want the thrall's HTML", body)
	}

	// 3) Default-deny: an unknown host never reaches the ether.
	if _, denied := getWithHost(t, front.URL+"/", "evil.test"); denied != http.StatusNotFound {
		t.Errorf("unknown host: status = %d, want 404", denied)
	}

	// 4) Scope filter: a vpn-only site is not routable from a PUBLIC ingress (the registry denies it).
	if _, denied := getWithHost(t, front.URL+"/", "vpnonly.test"); denied != http.StatusNotFound {
		t.Errorf("vpn-only host on public ingress: status = %d, want 404 (scope filter)", denied)
	}
}
