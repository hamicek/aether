package ingress

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// This file is the dynamic counterpart of routes.go: a RouteProvider that sources the Host -> thrall
// table from the commons site-registry thrall at runtime, instead of a fixed routes.json. Sites can
// be published and retired while the ingress runs - the ingress itself stays the same dumb forwarder.
//
// Reuse is over the site-registry WIRE CONTRACT, not a code import: ae-commons is a private repo, so
// the small resolve request/reply types below MIRROR its contract.md (contract version 1.0.0) rather
// than importing it. The contract is the source of truth; keep these in sync with it.

// DefaultRegistrySubject is the site-registry resolve call subject for the family's default
// deployment (app "ae", name "site-registry"): aether.ae.site-registry.call. A product running the
// registry in another app overrides it.
const DefaultRegistrySubject = "aether.ae.site-registry.call"

// defaultRegistryTimeout bounds how long a single resolve call waits for the registry before the
// lookup is treated as a transport failure (which denies this request but is not cached).
const defaultRegistryTimeout = 2 * time.Second

// defaultRegistryTTL is the cache lifetime of a resolve result when none is configured. resolve is a
// call per request, so without caching every HTTP request would add a NATS round-trip; the TTL trades
// a little staleness for that latency.
const defaultRegistryTTL = 5 * time.Second

// opResolve is the site-registry op the ingress calls per request (contract.md "resolve").
const opResolve = "resolve"

// registryName is the site-registry thrall's aether name, used only to fill the envelope's To field
// for traceability; the registry subscribes by subject, so routing does not depend on it.
const registryName = "site-registry"

// scope is the reach of the asking ingress, carried in a resolve request: "public" admits only public
// sites, "vpn" admits both. It mirrors site-registry contract.md; the visibility filter itself lives
// in the registry, so the ingress only ever routes what it is handed (default-deny).
type scope string

const (
	scopePublic scope = "public"
	scopeVPN    scope = "vpn"
)

func (s scope) valid() bool { return s == scopePublic || s == scopeVPN }

// resolveRequest is the payload of the resolve call. Mirrors site-registry contract.md: a Host and the
// asking ingress's Scope (omitempty, registry defaults it to public).
type resolveRequest struct {
	Host  string `json:"host"`
	Scope scope  `json:"scope,omitempty"`
}

// resolveReply is the payload of the resolve reply. Mirrors site-registry contract.md: Found is false
// for an unknown host and for a host not admitted at the requested scope (default-deny); Thrall and
// Visibility are set only when Found is true.
type resolveReply struct {
	Found      bool   `json:"found"`
	Thrall     string `json:"thrall,omitempty"`
	Visibility string `json:"visibility,omitempty"`
}

// resolveFunc is the authoritative lookup behind the cache: it asks the registry and returns the
// target thrall and whether the host is routable at the provider's scope. A non-nil error is a
// transport/protocol failure (the registry could not be reached or replied with garbage), which is
// distinct from an authoritative found=false and is deliberately NOT cached by RegistryRoutes.
type resolveFunc func(host string) (thrall string, found bool, err error)

// RegistryRoutes is a RouteProvider backed by the site-registry thrall. It caches authoritative
// resolve results for a TTL so a hot host costs one NATS round-trip per TTL, not one per request, and
// it is safe for the concurrent HTTP handler. A transport failure denies the single request that hit
// it but is never cached, so a registry outage cannot latch a host into a stale "not found".
type RegistryRoutes struct {
	resolve resolveFunc
	ttl     time.Duration
	now     func() time.Time
	log     *slog.Logger

	mu    sync.Mutex
	cache map[string]cacheEntry
}

// cacheEntry is a cached authoritative resolve result and when it expires. found=false (a known
// unroutable host) is cached too, so a scan of unknown hosts does not hammer the registry.
type cacheEntry struct {
	thrall  string
	found   bool
	expires time.Time
}

// RegistryOptions configure a RegistryRoutes. Zero values fall back to sane defaults. Scope is a
// plain string ("public" | "vpn", default public) so the main package can set it straight from a
// flag; it is validated in NewRegistryRoutes. Now is an injection seam for tests (TTL expiry without
// sleeping) and defaults to time.Now.
type RegistryOptions struct {
	Subject string
	Scope   string
	TTL     time.Duration
	Timeout time.Duration
	Log     *slog.Logger
	Now     func() time.Time
}

// NewRegistryRoutes builds a site-registry-backed RouteProvider over a NATS connection. It fails fast
// on an invalid scope so a misconfigured ingress is caught at startup rather than silently denying
// (or over-admitting) every request, mirroring LoadStaticRoutes's startup validation.
func NewRegistryRoutes(nc *nats.Conn, opts RegistryOptions) (*RegistryRoutes, error) {
	sc := scope(opts.Scope)
	if sc == "" {
		sc = scopePublic
	}
	if !sc.valid() {
		return nil, fmt.Errorf("ingress: registry scope %q is not %q or %q", opts.Scope, scopePublic, scopeVPN)
	}
	if opts.Subject == "" {
		opts.Subject = DefaultRegistrySubject
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultRegistryTimeout
	}
	return newRegistryRoutes(natsResolver(nc, opts.Subject, sc, opts.Timeout), opts), nil
}

// newRegistryRoutes wraps an authoritative resolver in the caching RouteProvider, applying the
// cache-side defaults (TTL, clock, log). It is the shared core of NewRegistryRoutes and the seam a
// test uses to drive the cache with a fake resolver instead of a live NATS connection.
func newRegistryRoutes(resolve resolveFunc, opts RegistryOptions) *RegistryRoutes {
	if opts.TTL <= 0 {
		opts.TTL = defaultRegistryTTL
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &RegistryRoutes{
		resolve: resolve,
		ttl:     opts.TTL,
		now:     opts.Now,
		log:     opts.Log,
		cache:   make(map[string]cacheEntry),
	}
}

// Resolve implements RouteProvider. It serves a fresh cache hit, otherwise asks the registry. A
// transport failure is logged and denied for this request only (not cached); an authoritative reply
// (found true or false) is cached for the TTL.
func (r *RegistryRoutes) Resolve(host string) (string, bool) {
	key := normalizeHost(host)

	now := r.now()
	r.mu.Lock()
	if e, ok := r.cache[key]; ok && now.Before(e.expires) {
		r.mu.Unlock()
		return e.thrall, e.found
	}
	r.mu.Unlock()

	thrall, found, err := r.resolve(key)
	if err != nil {
		// An outage is not an authoritative "not found": deny this request to stay default-deny, but
		// do not cache it, so the next request retries instead of latching the host out for the TTL.
		r.log.Warn("ingress registry resolve failed", slog.String("host", key), slog.Any("err", err))
		return "", false
	}

	r.mu.Lock()
	r.cache[key] = cacheEntry{thrall: thrall, found: found, expires: r.now().Add(r.ttl)}
	r.mu.Unlock()
	return thrall, found
}

// natsResolver builds the authoritative lookup: one resolve request/reply round-trip to the registry.
// It returns an error for any transport or protocol failure (no responders, timeout, an error reply,
// or an unparseable reply) so RegistryRoutes can tell an outage apart from an authoritative deny.
func natsResolver(nc *nats.Conn, subject string, s scope, timeout time.Duration) resolveFunc {
	return func(host string) (string, bool, error) {
		env := wire.Envelope{
			V: 1, ID: nats.NewInbox(), Trace: nats.NewInbox(),
			Kind: wire.KindCall, To: registryName, Op: opResolve,
			Payload: mustMarshal(resolveRequest{Host: host, Scope: s}), TS: time.Now().UnixMilli(),
		}
		msg, err := nc.Request(subject, mustMarshal(env), timeout)
		if err != nil {
			return "", false, fmt.Errorf("resolve call: %w", err)
		}

		var reply wire.Envelope
		if err := json.Unmarshal(msg.Data, &reply); err != nil {
			return "", false, fmt.Errorf("resolve reply envelope: %w", err)
		}
		if reply.Status == "error" {
			return "", false, fmt.Errorf("resolve reply error: %s", wireErrorMessage(reply.Error))
		}

		var rr resolveReply
		if err := json.Unmarshal(reply.Payload, &rr); err != nil {
			return "", false, fmt.Errorf("resolve reply payload: %w", err)
		}
		return rr.Thrall, rr.Found, nil
	}
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// wireErrorMessage renders a reply's structured error for a log/error string, tolerating a nil error
// field (a reply marked status=error but carrying no detail).
func wireErrorMessage(e *wire.WireError) string {
	if e == nil {
		return "unknown error"
	}
	return strings.TrimSpace(e.Type + ": " + e.Message)
}
