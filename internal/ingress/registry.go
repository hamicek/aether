package ingress

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/sdk/go/thrall"
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

// Defaults of the registry provider.
const (
	// DefaultRegistryTTL is how long a resolve result is used before the registry is asked again.
	// resolve is a call per request, so without caching every HTTP request would add a NATS
	// round-trip; the TTL trades a little staleness for that latency.
	DefaultRegistryTTL = 5 * time.Second
	// DefaultRegistryStale is how long past its TTL a result keeps being served while the registry
	// cannot be reached. It covers a registry restart or redeploy; a site retired in the registry
	// stays reachable for at most this long if the registry goes down right after.
	DefaultRegistryStale = 5 * time.Minute
	// DefaultRegistryCacheSize caps the cached answers of each kind (routable hosts, denied hosts).
	DefaultRegistryCacheSize = 4096

	// defaultRegistryTimeout bounds how long a single resolve call waits for the registry.
	defaultRegistryTimeout = 2 * time.Second
	// defaultRegistryRetryAfter is how long the registry is left alone after a failed resolve.
	// Without it every request during an outage would wait out the resolve timeout before being
	// answered from the last known routes.
	defaultRegistryRetryAfter = time.Second
)

// ErrRegistryUnavailable is the error Resolve returns when the registry cannot be reached and there
// is no last known answer recent enough to serve instead.
var ErrRegistryUnavailable = errors.New("site-registry unavailable")

// opResolve is the site-registry op the ingress calls per request (contract.md "resolve").
const opResolve = "resolve"

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
// target thrall and whether the host is routable at the provider's scope. A non-nil error means the
// registry could not be asked or did not give a usable answer, which is distinct from an
// authoritative found=false and is never cached by RegistryRoutes.
type resolveFunc func(host string) (thrall string, found bool, err error)

// RegistryRoutes is a RouteProvider backed by the site-registry thrall, safe for the concurrent HTTP
// handler. It keeps the registry's answers so that:
//
//   - a hot host costs one NATS round-trip per TTL, not one per request;
//   - a registry outage does not take the sites down with it: the last known answer of a host is
//     served for a bounded time past its TTL (serve-stale), and only a host with no such answer
//     gets an error - which the ingress reports as 503, never as "not found";
//   - during an outage the registry is asked again only every so often, not by every request;
//   - the memory is bounded: routable and denied hosts are kept apart, each under a cap, so a scan
//     of random Host values can neither grow the cache without limit nor push real sites out of it.
type RegistryRoutes struct {
	resolve    resolveFunc
	ttl        time.Duration
	staleFor   time.Duration
	retryAfter time.Duration
	now        func() time.Time
	log        *slog.Logger

	mu        sync.Mutex
	routable  boundedCache // hosts the registry admits
	denied    boundedCache // hosts the registry does not serve at this scope
	downSince time.Time    // when the current outage was first seen; zero while the registry answers
	nextAsk   time.Time    // during an outage: the registry is not asked before this
}

// RegistryOptions configure a RegistryRoutes. Zero values fall back to the defaults, except
// StaleFor, where zero means what it says: no serving of expired answers. Scope is a plain string
// ("public" | "vpn", default public) so it can come straight from a flag; it is validated in
// NewRegistryRoutes. Now is an injection seam for tests (TTL expiry without sleeping) and defaults
// to time.Now.
type RegistryOptions struct {
	Subject string
	Scope   string
	// TTL is how long an answer is used before the registry is asked again.
	TTL time.Duration
	// StaleFor is how long past its TTL an answer is still served while the registry is unreachable.
	StaleFor time.Duration
	// RetryAfter is how long the registry is left alone after a failed resolve.
	RetryAfter time.Duration
	// CacheSize caps the cached routable hosts and, separately, the cached denied hosts.
	CacheSize int
	Timeout   time.Duration
	Log       *slog.Logger
	Now       func() time.Time
}

// NewRegistryRoutes builds a site-registry-backed RouteProvider over a NATS connection. It fails fast
// on an invalid scope or subject so a misconfigured ingress is caught at startup rather than
// silently denying (or over-admitting) every request, mirroring LoadStaticRoutes's startup validation.
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
	app, name, err := parseCallSubject(opts.Subject)
	if err != nil {
		return nil, err
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultRegistryTimeout
	}
	return newRegistryRoutes(natsResolver(thrall.NewClient(nc, app), name, sc, opts.Timeout), opts), nil
}

// newRegistryRoutes wraps an authoritative resolver in the caching RouteProvider, applying the
// cache-side defaults. It is the shared core of NewRegistryRoutes and the seam a test uses to drive
// the cache with a fake resolver instead of a live NATS connection.
func newRegistryRoutes(resolve resolveFunc, opts RegistryOptions) *RegistryRoutes {
	if opts.TTL <= 0 {
		opts.TTL = DefaultRegistryTTL
	}
	if opts.RetryAfter <= 0 {
		opts.RetryAfter = defaultRegistryRetryAfter
	}
	if opts.CacheSize <= 0 {
		opts.CacheSize = DefaultRegistryCacheSize
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &RegistryRoutes{
		resolve:    resolve,
		ttl:        opts.TTL,
		staleFor:   max(opts.StaleFor, 0),
		retryAfter: opts.RetryAfter,
		now:        opts.Now,
		log:        opts.Log,
		routable:   newBoundedCache(opts.CacheSize),
		denied:     newBoundedCache(opts.CacheSize),
	}
}

// Resolve implements RouteProvider. A fresh cached answer is served as is. Otherwise the registry is
// asked: its answer (found or not) is cached for the TTL; when it cannot be reached, the last known
// answer is served while it is not older than TTL + StaleFor, and beyond that - or for a host never
// resolved - the error is ErrRegistryUnavailable.
func (r *RegistryRoutes) Resolve(host string) (string, bool, error) {
	key := normalizeHost(host)

	r.mu.Lock()
	now := r.now()
	known, cached := r.lookup(key, now)
	if cached && now.Sub(known.fetched) < r.ttl {
		r.mu.Unlock()
		return known.thrall, known.found, nil
	}
	if now.Before(r.nextAsk) {
		// The registry failed a moment ago; do not make this request wait for it to fail again.
		since := r.downSince
		r.mu.Unlock()
		return lastKnown(known, cached, fmt.Errorf("%w (unreachable since %s)", ErrRegistryUnavailable, since.Format(time.RFC3339)))
	}
	r.mu.Unlock()

	thrall, found, err := r.resolve(key)

	r.mu.Lock()
	defer r.mu.Unlock()
	now = r.now()
	if err != nil {
		r.noteOutage(now, err)
		return lastKnown(known, cached, fmt.Errorf("%w: %v", ErrRegistryUnavailable, err))
	}
	r.noteRecovery(now)
	r.store(key, cacheEntry{thrall: thrall, found: found, fetched: now})
	return thrall, found, nil
}

// lastKnown answers from the last known entry when there is one, else with the given error.
func lastKnown(known cacheEntry, cached bool, unavailable error) (string, bool, error) {
	if cached {
		return known.thrall, known.found, nil
	}
	return "", false, unavailable
}

// lookup returns the cached answer for key if it may still be served at all - fresh, or expired but
// within the serve-stale window. An answer older than that is forgotten. The caller holds mu.
func (r *RegistryRoutes) lookup(key string, now time.Time) (cacheEntry, bool) {
	for _, cache := range []*boundedCache{&r.routable, &r.denied} {
		entry, ok := cache.entries[key]
		if !ok {
			continue
		}
		if now.Sub(entry.fetched) < r.ttl+r.staleFor {
			return entry, true
		}
		delete(cache.entries, key)
	}
	return cacheEntry{}, false
}

// store files an authoritative answer under the cache of its kind and removes the host from the
// other one, so a host that changed sides is not answered both ways. The caller holds mu.
func (r *RegistryRoutes) store(key string, entry cacheEntry) {
	if entry.found {
		r.routable.put(key, entry)
		delete(r.denied.entries, key)
		return
	}
	r.denied.put(key, entry)
	delete(r.routable.entries, key)
}

// noteOutage records a failed resolve: the registry is left alone for RetryAfter, and the start of
// an outage is logged once, not once per request. The caller holds mu.
func (r *RegistryRoutes) noteOutage(now time.Time, err error) {
	r.nextAsk = now.Add(r.retryAfter)
	if !r.downSince.IsZero() {
		return
	}
	r.downSince = now
	r.log.Error("ingress: site-registry unreachable, serving last known routes",
		slog.Duration("stale_for", r.staleFor), slog.Any("err", err))
}

// noteRecovery records a successful resolve, logging the end of an outage. The caller holds mu.
func (r *RegistryRoutes) noteRecovery(now time.Time) {
	if r.downSince.IsZero() {
		return
	}
	r.log.Info("ingress: site-registry reachable again", slog.Duration("outage", now.Sub(r.downSince)))
	r.downSince, r.nextAsk = time.Time{}, time.Time{}
}

// cacheEntry is an authoritative resolve result and when it was obtained. found=false (a host the
// registry does not serve) is cached too, so a scan of unknown hosts does not hammer the registry.
type cacheEntry struct {
	thrall  string
	found   bool
	fetched time.Time
}

// boundedCache is a host -> answer map that never holds more than max entries: adding to a full
// cache evicts an arbitrary entry. Arbitrary is enough here - an evicted answer only costs one more
// resolve - and it keeps the cost of a flood of new hosts constant per request.
type boundedCache struct {
	max     int
	entries map[string]cacheEntry
}

func newBoundedCache(max int) boundedCache {
	return boundedCache{max: max, entries: make(map[string]cacheEntry)}
}

func (c *boundedCache) put(key string, entry cacheEntry) {
	if _, present := c.entries[key]; !present && len(c.entries) >= c.max {
		for victim := range c.entries {
			delete(c.entries, victim)
			break
		}
	}
	c.entries[key] = entry
}

// natsResolver builds the authoritative lookup: one resolve call to the registry through the SDK
// client, so it carries a deadline like every call (a registry that only gets to it after the
// ingress has given up drops it). Any failure - no responders, a timeout, an error reply, an
// undecodable reply - is returned as an error so RegistryRoutes can tell an outage apart from an
// authoritative deny.
func natsResolver(client *thrall.Client, name string, s scope, timeout time.Duration) resolveFunc {
	return func(host string) (string, bool, error) {
		reply, err := thrall.CallInto[resolveReply](client, name, opResolve, resolveRequest{Host: host, Scope: s}, timeout)
		if err != nil {
			return "", false, fmt.Errorf("resolve call: %w", err)
		}
		if reply.Found && reply.Thrall == "" {
			return "", false, errors.New("resolve reply: found without a thrall")
		}
		return reply.Thrall, reply.Found, nil
	}
}

// parseCallSubject splits a thrall call subject, aether.<app>.<name>.call, into its app and name.
// The registry is addressed by subject on the command line (so a product can run it in another
// app), while the SDK client addresses a thrall by app and name.
func parseCallSubject(subject string) (app, name string, err error) {
	parts := strings.Split(subject, ".")
	if len(parts) != 4 || parts[0] != "aether" || parts[1] == "" || parts[2] == "" || parts[3] != "call" {
		return "", "", fmt.Errorf("ingress: registry subject %q is not a thrall call subject (aether.<app>.<name>.call)", subject)
	}
	return parts[1], parts[2], nil
}
