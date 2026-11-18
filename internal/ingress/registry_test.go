package ingress

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
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

// fakeRegistry is a scriptable authoritative resolver: a table of sites, a switch that takes the
// registry down, and a record of the hosts it was asked for - so a test can assert exactly when the
// cache does and does not reach the registry.
type fakeRegistry struct {
	sites map[string]string
	down  bool
	asked []string
	// whileAsked, when set, runs in the middle of a resolve - where a concurrent request would be.
	whileAsked func()
}

func (f *fakeRegistry) resolve(host string) (string, bool, error) {
	f.asked = append(f.asked, host)
	if f.whileAsked != nil {
		f.whileAsked()
	}
	if f.down {
		return "", false, errors.New("no responders")
	}
	thrall, found := f.sites[host]
	return thrall, found, nil
}

// registryUnderTest builds the provider over a fake registry and a fake clock.
func registryUnderTest(opts RegistryOptions) (*RegistryRoutes, *fakeRegistry, *fakeClock) {
	reg := &fakeRegistry{sites: map[string]string{"blog.test": "blog"}}
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	opts.Now = clock.Now
	return newRegistryRoutes(reg.resolve, opts), reg, clock
}

func TestRegistryCacheHitAndHostNormalization(t *testing.T) {
	r, reg, _ := registryUnderTest(RegistryOptions{TTL: time.Minute})

	// A host with mixed case and a port must normalize before the lookup, and the second lookup of
	// the same host must be served from the cache (no second resolver call).
	for i := 0; i < 2; i++ {
		thrall, ok, err := r.Resolve("Blog.Test:9000")
		if err != nil || !ok || thrall != "blog" {
			t.Fatalf("Resolve #%d = %q, %v, %v; want blog, true", i, thrall, ok, err)
		}
	}
	if len(reg.asked) != 1 || reg.asked[0] != "blog.test" {
		t.Errorf("the registry was asked for %v, want one normalized [blog.test]", reg.asked)
	}
}

func TestRegistryNegativeResultCached(t *testing.T) {
	r, reg, _ := registryUnderTest(RegistryOptions{TTL: time.Minute})

	for i := 0; i < 3; i++ {
		if _, ok, err := r.Resolve("unknown.test"); ok || err != nil {
			t.Fatalf("Resolve #%d = %v, %v; want an authoritative deny", i, ok, err)
		}
	}
	if len(reg.asked) != 1 {
		t.Errorf("the registry was asked %d times, want 1 (a known-unroutable host must be cached too)", len(reg.asked))
	}
}

func TestRegistryTTLExpiry(t *testing.T) {
	r, reg, clock := registryUnderTest(RegistryOptions{TTL: 5 * time.Second})

	r.Resolve("blog.test")
	clock.advance(4 * time.Second) // still within the TTL
	r.Resolve("blog.test")
	if len(reg.asked) != 1 {
		t.Fatalf("the registry was asked %d times before expiry, want 1", len(reg.asked))
	}
	clock.advance(2 * time.Second) // now past the 5s TTL
	r.Resolve("blog.test")
	if len(reg.asked) != 2 {
		t.Errorf("the registry was asked %d times after expiry, want 2 (the entry must be refreshed)", len(reg.asked))
	}
}

// TestRegistryOutageIsAnErrorNotADeny: with nothing known about a host, an unreachable registry is
// reported as unavailable - the caller must be able to tell it from "no such site" - and is not
// cached as an answer.
func TestRegistryOutageIsAnErrorNotADeny(t *testing.T) {
	r, reg, clock := registryUnderTest(RegistryOptions{TTL: time.Minute, RetryAfter: time.Second})
	reg.down = true

	if _, found, err := r.Resolve("blog.test"); !errors.Is(err, ErrRegistryUnavailable) || found {
		t.Fatalf("outage, nothing known = %v, %v; want ErrRegistryUnavailable", found, err)
	}

	// The registry comes back: the host resolves on the next ask, nothing was latched.
	reg.down = false
	clock.advance(2 * time.Second)
	if thrall, found, err := r.Resolve("blog.test"); err != nil || !found || thrall != "blog" {
		t.Errorf("after the outage = %q, %v, %v; want blog", thrall, found, err)
	}
}

// TestRegistryIsLeftAloneAfterAFailure: during an outage the registry is asked once per RetryAfter,
// not by every request - otherwise each request would wait for the resolve to time out again.
func TestRegistryIsLeftAloneAfterAFailure(t *testing.T) {
	r, reg, clock := registryUnderTest(RegistryOptions{TTL: time.Minute, RetryAfter: time.Second})
	reg.down = true

	for i := 0; i < 5; i++ {
		if _, _, err := r.Resolve(fmt.Sprintf("host-%d.test", i)); !errors.Is(err, ErrRegistryUnavailable) {
			t.Fatalf("request %d during the outage: %v, want ErrRegistryUnavailable", i, err)
		}
	}
	if len(reg.asked) != 1 {
		t.Fatalf("the registry was asked %d times within one retry interval, want 1", len(reg.asked))
	}
	clock.advance(time.Second)
	r.Resolve("host-0.test")
	if len(reg.asked) != 2 {
		t.Errorf("the registry was asked %d times after the retry interval, want 2", len(reg.asked))
	}
}

// TestRegistryOutageProbeDoesNotHoldUpOtherRequests: once the registry is known to be down, the
// request that asks again may wait for the whole resolve timeout. Requests arriving meanwhile must
// be answered from what is known rather than each starting a wait of its own.
func TestRegistryOutageProbeDoesNotHoldUpOtherRequests(t *testing.T) {
	r, reg, clock := registryUnderTest(RegistryOptions{TTL: 5 * time.Second, StaleFor: time.Minute, RetryAfter: time.Second})
	r.Resolve("blog.test")
	reg.down = true
	clock.advance(6 * time.Second)
	r.Resolve("blog.test") // the failure that starts the outage
	clock.advance(time.Second)

	// The next ask is in flight; two more requests arrive while it is.
	asksBefore := len(reg.asked)
	reg.whileAsked = func() {
		reg.whileAsked = nil
		if thrall, found, err := r.Resolve("blog.test"); err != nil || !found || thrall != "blog" {
			t.Errorf("a request during the ask = %q, %v, %v; want the last known route at once", thrall, found, err)
		}
		if _, _, err := r.Resolve("never-seen.test"); !errors.Is(err, ErrRegistryUnavailable) {
			t.Errorf("an unknown host during the ask: %v, want ErrRegistryUnavailable at once", err)
		}
	}
	r.Resolve("blog.test")
	if got := len(reg.asked) - asksBefore; got != 1 {
		t.Errorf("the registry was asked %d times, want 1: requests during an ask must not start their own", got)
	}
}

// TestRegistryServesLastKnownRoutesDuringAnOutage walks an outage end to end: answers known before it
// keep being served past their TTL - a routable host stays routable, a denied one stays denied -
// until they are older than TTL + StaleFor, when the provider gives up on them; and the first
// successful ask afterwards restores normal service.
func TestRegistryServesLastKnownRoutesDuringAnOutage(t *testing.T) {
	const ttl, stale = 5 * time.Second, time.Minute
	r, reg, clock := registryUnderTest(RegistryOptions{TTL: ttl, StaleFor: stale, RetryAfter: time.Second})

	r.Resolve("blog.test")
	r.Resolve("scanner.test")

	reg.down = true
	clock.advance(ttl + time.Second) // both answers have expired
	if thrall, found, err := r.Resolve("blog.test"); err != nil || !found || thrall != "blog" {
		t.Fatalf("known host during the outage = %q, %v, %v; want the last known route", thrall, found, err)
	}
	if _, found, err := r.Resolve("scanner.test"); err != nil || found {
		t.Errorf("denied host during the outage = %v, %v; want the last known deny", found, err)
	}
	if _, _, err := r.Resolve("never-seen.test"); !errors.Is(err, ErrRegistryUnavailable) {
		t.Errorf("unknown host during the outage: %v, want ErrRegistryUnavailable", err)
	}

	// Still down just inside the window: still served.
	clock.advance(stale - 2*time.Second)
	if _, found, err := r.Resolve("blog.test"); err != nil || !found {
		t.Errorf("just inside the stale window = %v, %v; want the last known route", found, err)
	}
	// Past the window the answer is too old to vouch for.
	clock.advance(2 * time.Second)
	if _, _, err := r.Resolve("blog.test"); !errors.Is(err, ErrRegistryUnavailable) {
		t.Errorf("past the stale window: %v, want ErrRegistryUnavailable", err)
	}

	// Recovery: the next ask goes through and the answer is fresh again.
	reg.down = false
	clock.advance(time.Second)
	if thrall, found, err := r.Resolve("blog.test"); err != nil || !found || thrall != "blog" {
		t.Fatalf("after recovery = %q, %v, %v; want blog", thrall, found, err)
	}
	asked := len(reg.asked)
	r.Resolve("blog.test")
	if len(reg.asked) != asked {
		t.Error("after recovery the answer must be cached again")
	}
}

// TestRegistryStaleServingIsOptIn: without StaleFor an expired answer is not served.
func TestRegistryStaleServingIsOptIn(t *testing.T) {
	r, reg, clock := registryUnderTest(RegistryOptions{TTL: 5 * time.Second})
	r.Resolve("blog.test")
	reg.down = true
	clock.advance(6 * time.Second)
	if _, _, err := r.Resolve("blog.test"); !errors.Is(err, ErrRegistryUnavailable) {
		t.Errorf("an expired answer with no stale window: %v, want ErrRegistryUnavailable", err)
	}
}

// TestRegistryPicksUpAChangedAnswer: a host that the registry starts or stops serving changes sides
// once its cached answer expires, and is not answered both ways afterwards.
func TestRegistryPicksUpAChangedAnswer(t *testing.T) {
	r, reg, clock := registryUnderTest(RegistryOptions{TTL: 5 * time.Second, StaleFor: time.Minute})
	if _, found, _ := r.Resolve("shop.test"); found {
		t.Fatal("precondition: shop.test is not served yet")
	}
	reg.sites["shop.test"] = "shop"
	clock.advance(6 * time.Second)
	if thrall, found, err := r.Resolve("shop.test"); err != nil || !found || thrall != "shop" {
		t.Fatalf("a newly published host = %q, %v, %v; want shop", thrall, found, err)
	}
	delete(reg.sites, "shop.test")
	clock.advance(6 * time.Second)
	if _, found, err := r.Resolve("shop.test"); err != nil || found {
		t.Fatalf("a retired host = %v, %v; want a deny", found, err)
	}
	if len(r.routable.entries) != 0 || len(r.denied.entries) != 1 {
		t.Errorf("a retired host must leave the routable cache: routable %d, denied %d", len(r.routable.entries), len(r.denied.entries))
	}
}

// TestRegistryCacheIsBounded: a flood of random hosts cannot grow the cache past its cap, and
// because denied hosts are kept apart from routable ones it cannot push a real site out either.
func TestRegistryCacheIsBounded(t *testing.T) {
	const size = 8
	r, reg, _ := registryUnderTest(RegistryOptions{TTL: time.Minute, CacheSize: size})
	r.Resolve("blog.test")

	for i := 0; i < 500; i++ {
		r.Resolve(fmt.Sprintf("scan-%d.test", i))
	}
	if got := len(r.denied.entries); got != size {
		t.Errorf("denied cache holds %d entries after a flood, want the cap of %d", got, size)
	}
	asked := len(reg.asked)
	if thrall, found, err := r.Resolve("blog.test"); err != nil || !found || thrall != "blog" {
		t.Fatalf("the real site after the flood = %q, %v, %v", thrall, found, err)
	}
	if len(reg.asked) != asked {
		t.Error("the flood evicted a real site from the cache")
	}

	// The routable side is capped the same way.
	for i := 0; i < 50; i++ {
		host := fmt.Sprintf("site-%d.test", i)
		reg.sites[host] = "blog"
		r.Resolve(host)
	}
	if got := len(r.routable.entries); got != size {
		t.Errorf("routable cache holds %d entries, want the cap of %d", got, size)
	}
}

func TestParseCallSubject(t *testing.T) {
	app, name, err := parseCallSubject(DefaultRegistrySubject)
	if err != nil || app != "ae" || name != "site-registry" {
		t.Errorf("default subject = %q, %q, %v; want ae, site-registry", app, name, err)
	}
	for _, bad := range []string{"", "aether.ae.site-registry", "aether.ae.site-registry.cast", "nats.ae.registry.call",
		"aether..registry.call", "aether.ae..call", "aether.ae.a.b.call"} {
		if _, _, err := parseCallSubject(bad); err == nil {
			t.Errorf("subject %q must be rejected", bad)
		}
	}
}

// stubRegistryThrall answers the resolve subject by hand with the given reply builder and records
// the envelope and request it last saw.
func stubRegistryThrall(t *testing.T, nc *nats.Conn, subject string, answer func(req wire.Envelope, rr resolveRequest) wire.Envelope) (lastEnv *wire.Envelope, lastReq *resolveRequest) {
	t.Helper()
	lastEnv, lastReq = &wire.Envelope{}, &resolveRequest{}
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var env wire.Envelope
		if err := json.Unmarshal(msg.Data, &env); err != nil {
			t.Errorf("stub: bad envelope: %v", err)
			return
		}
		var rr resolveRequest
		_ = json.Unmarshal(env.Payload, &rr)
		*lastEnv, *lastReq = env, rr
		data, _ := json.Marshal(answer(env, rr))
		_ = msg.Respond(data)
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return lastEnv, lastReq
}

// TestNatsResolverContractRoundTrip proves the NATS wiring against the site-registry resolve contract:
// a resolve request carries op/kind/scope/host (and, coming from the SDK client, a deadline), and a
// found reply maps to the target thrall while a not-found reply denies. A stub subscriber plays the
// registry so the test is self-contained.
func TestNatsResolverContractRoundTrip(t *testing.T) {
	srv := embeddedServer(t)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	gotEnv, gotReq := stubRegistryThrall(t, nc, DefaultRegistrySubject, func(req wire.Envelope, rr resolveRequest) wire.Envelope {
		// The stub's tiny table: blog.test is public, everything else is unknown.
		if rr.Host == "blog.test" {
			return thrall.OKReply(req, resolveReply{Found: true, Thrall: "blog", Visibility: "public"})
		}
		return thrall.OKReply(req, resolveReply{Found: false})
	})

	resolve := natsResolver(thrall.NewClient(nc, "ae"), "site-registry", scopeVPN, 2*time.Second)

	target, found, err := resolve("blog.test")
	if err != nil {
		t.Fatalf("resolve known host: %v", err)
	}
	if !found || target != "blog" {
		t.Errorf("resolve known host = %q, %v; want blog, true", target, found)
	}
	// The request must match the contract: a resolve call carrying the ingress's scope.
	if gotEnv.Op != opResolve || gotEnv.Kind != wire.KindCall || gotEnv.To != "site-registry" {
		t.Errorf("request op/kind/to = %q/%q/%q, want %q/%q/site-registry", gotEnv.Op, gotEnv.Kind, gotEnv.To, opResolve, wire.KindCall)
	}
	if gotReq.Scope != scopeVPN || gotReq.Host != "blog.test" {
		t.Errorf("request = %+v, want host blog.test at scope %q", *gotReq, scopeVPN)
	}
	if gotEnv.Deadline == 0 || gotEnv.Trace == "" {
		t.Errorf("a resolve call must carry a deadline and a trace, got deadline=%d trace=%q", gotEnv.Deadline, gotEnv.Trace)
	}

	if _, found, err := resolve("nope.test"); err != nil || found {
		t.Errorf("resolve unknown host = %v, %v; want deny, no error", found, err)
	}
}

// TestNatsResolverFailuresAreNotDenies proves that everything short of a usable answer is surfaced
// as an error (so it is neither cached nor read as "no such site"): an error reply, a reply that
// claims a route without naming a thrall, and a registry that is not there.
func TestNatsResolverFailuresAreNotDenies(t *testing.T) {
	srv := embeddedServer(t)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	stubRegistryThrall(t, nc, "aether.ae.failing.call", func(req wire.Envelope, _ resolveRequest) wire.Envelope {
		return thrall.ErrorReply(req, wire.WireError{Type: wire.ErrHandler, Message: "boom"})
	})
	stubRegistryThrall(t, nc, "aether.ae.vague.call", func(req wire.Envelope, _ resolveRequest) wire.Envelope {
		return thrall.OKReply(req, resolveReply{Found: true})
	})
	client := thrall.NewClient(nc, "ae")
	for _, name := range []string{"failing", "vague", "absent"} {
		if _, found, err := natsResolver(client, name, scopePublic, time.Second)("blog.test"); err == nil || found {
			t.Errorf("registry %q = %v, %v; want an error, not an answer", name, found, err)
		}
	}
}

// TestNewRegistryRoutesFailsFastOnMisconfiguration proves the startup fail-fast: a bad scope or
// subject is caught when the provider is built, not by silently denying every request.
func TestNewRegistryRoutesFailsFastOnMisconfiguration(t *testing.T) {
	if _, err := NewRegistryRoutes(nil, RegistryOptions{Scope: "intranet"}); err == nil {
		t.Error("an unknown scope must be rejected at construction")
	}
	if _, err := NewRegistryRoutes(nil, RegistryOptions{Subject: "registry.resolve"}); err == nil {
		t.Error("a subject that is not a thrall call subject must be rejected at construction")
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
