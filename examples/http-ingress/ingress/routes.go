// Package ingress is the translation core of the HTTP ingress: resolve an inbound request's Host to
// a web thrall, forward the whole request as an http_request call over NATS, and write the
// http_response back. It depends only on the wire contract, not on the lord or the manifest, so it
// stays a small, testable core; the HTTP server and the edge lifecycle live in the `main` package
// that drives it (built on thrall.StartEdge).
//
// This is the generic host->subject forwarder of the "web thrall is pure NATS" model (AE-091): the
// thrall is the web server and does its own path routing; the ingress knows only Host -> thrall. It
// is the complement of [[edge.http]] (route -> operation, internal/edge), not a replacement.
package ingress

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// RouteProvider maps an inbound Host to a target web thrall. It is the pluggable seam of the
// ingress: a static table today, a dynamic site-registry client (Wave 1 commons) later, behind the
// same interface. Resolve returns ok=false when the host is not routable, which the ingress treats
// as default-deny (404) - a host is reachable only if a provider explicitly allows it.
type RouteProvider interface {
	Resolve(host string) (thrall string, ok bool)
}

// StaticRoutes is a fixed Host -> thrall table, the simplest RouteProvider. Keys are normalized on
// lookup (port stripped, lowercased), so an entry "blog.example" matches a request with Host
// "Blog.Example:8080".
type StaticRoutes map[string]string

// Resolve implements RouteProvider. An empty table (or an unknown host) denies by default.
func (s StaticRoutes) Resolve(host string) (string, bool) {
	thrall, ok := s[normalizeHost(host)]
	return thrall, ok
}

// LoadStaticRoutes reads a JSON object of {host: thrall} from path and returns a normalized table.
// It fails on a missing file, malformed JSON, or an entry with an empty host or thrall, so a
// misconfigured route table is caught at startup rather than silently denying every request.
func LoadStaticRoutes(path string) (StaticRoutes, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ingress: read routes %q: %w", path, err)
	}
	var raw map[string]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("ingress: parse routes %q: %w", path, err)
	}
	routes := make(StaticRoutes, len(raw))
	for host, thrall := range raw {
		h, t := normalizeHost(host), strings.TrimSpace(thrall)
		if h == "" || t == "" {
			return nil, fmt.Errorf("ingress: route %q -> %q has an empty host or thrall", host, thrall)
		}
		routes[h] = t
	}
	return routes, nil
}

// normalizeHost strips an optional port and lowercases the host, so routing is case-insensitive and
// independent of the port the client connected on (the Host header carries both, e.g.
// "Example.com:8080").
func normalizeHost(host string) string {
	host = strings.TrimSpace(host)
	if i := strings.LastIndexByte(host, ':'); i >= 0 {
		// Only strip a trailing :port, not a bare IPv6 literal. A bracketed IPv6 host ("[::1]:80")
		// keeps its brackets; an unbracketed one has no port to strip.
		if !strings.Contains(host[i+1:], "]") {
			host = host[:i]
		}
	}
	return strings.ToLower(host)
}
