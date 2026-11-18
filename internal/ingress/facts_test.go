package ingress

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"reflect"
	"testing"
)

func mustTrust(t *testing.T, list string) TrustedProxies {
	t.Helper()
	trusted, err := ParseTrustedProxies(list)
	if err != nil {
		t.Fatalf("ParseTrustedProxies(%q): %v", list, err)
	}
	return trusted
}

// TestFacts walks the ways a request's origin can be stated or faked. The rule under test: the
// forwarding headers count only when the ingress's own peer is a trusted proxy, and then only the
// part of the chain the trusted proxies wrote.
func TestFacts(t *testing.T) {
	loopback := mustTrust(t, DefaultTrustedProxies)
	cases := []struct {
		name       string
		trusted    TrustedProxies
		remoteAddr string
		headers    map[string][]string
		tls        bool
		wantIP     string
		wantScheme string
	}{
		{name: "direct client, no forwarding headers",
			trusted: loopback, remoteAddr: "198.51.100.9:4711", wantIP: "198.51.100.9", wantScheme: "http"},
		{name: "direct client claiming to be someone else over https",
			trusted: loopback, remoteAddr: "198.51.100.9:4711",
			headers: map[string][]string{"X-Forwarded-For": {"203.0.113.7"}, "X-Forwarded-Proto": {"https"}},
			wantIP:  "198.51.100.9", wantScheme: "http"},
		{name: "trusted proxy vouching for its client",
			trusted: loopback, remoteAddr: "127.0.0.1:52000",
			headers: map[string][]string{"X-Forwarded-For": {"203.0.113.7"}, "X-Forwarded-Proto": {"https"}},
			wantIP:  "203.0.113.7", wantScheme: "https"},
		{name: "client-supplied entries left of what the proxy appended are ignored",
			trusted: loopback, remoteAddr: "127.0.0.1:52000",
			headers: map[string][]string{"X-Forwarded-For": {"10.0.0.1, 192.0.2.66, 203.0.113.7"}},
			wantIP:  "203.0.113.7", wantScheme: "http"},
		{name: "a chain of trusted proxies is walked back to the first outsider",
			trusted: mustTrust(t, "127.0.0.0/8,10.0.0.0/8"), remoteAddr: "127.0.0.1:52000",
			headers: map[string][]string{"X-Forwarded-For": {"192.0.2.66, 203.0.113.7, 10.0.0.5"}},
			wantIP:  "203.0.113.7", wantScheme: "http"},
		{name: "the chain split over several header lines",
			trusted: mustTrust(t, "127.0.0.0/8,10.0.0.0/8"), remoteAddr: "127.0.0.1:52000",
			headers: map[string][]string{"X-Forwarded-For": {"192.0.2.66", "203.0.113.7", "10.0.0.5"}},
			wantIP:  "203.0.113.7", wantScheme: "http"},
		{name: "a request from inside: every hop trusted, the origin is the leftmost",
			trusted: mustTrust(t, "127.0.0.0/8,10.0.0.0/8"), remoteAddr: "127.0.0.1:52000",
			headers: map[string][]string{"X-Forwarded-For": {"10.0.0.9, 10.0.0.5"}},
			wantIP:  "10.0.0.9", wantScheme: "http"},
		{name: "trusted proxy that sent no chain: the proxy itself is all that is known",
			trusted: loopback, remoteAddr: "127.0.0.1:52000", wantIP: "127.0.0.1", wantScheme: "http"},
		{name: "an entry that is not an address: fall back to the visible peer",
			trusted: loopback, remoteAddr: "127.0.0.1:52000",
			headers: map[string][]string{"X-Forwarded-For": {"203.0.113.7, unknown"}},
			wantIP:  "127.0.0.1", wantScheme: "http"},
		{name: "entries with a port and IPv6",
			trusted: loopback, remoteAddr: "[::1]:52000",
			headers: map[string][]string{"X-Forwarded-For": {"[2001:db8::7]:443"}, "X-Forwarded-Proto": {"HTTPS"}},
			wantIP:  "2001:db8::7", wantScheme: "https"},
		{name: "an IPv4-mapped peer is the IPv4 address it stands for",
			trusted: loopback, remoteAddr: "[::ffff:127.0.0.1]:52000",
			headers: map[string][]string{"X-Forwarded-For": {"::ffff:203.0.113.7"}},
			wantIP:  "203.0.113.7", wantScheme: "http"},
		{name: "a scheme that is not http or https is not believed",
			trusted: loopback, remoteAddr: "127.0.0.1:52000",
			headers: map[string][]string{"X-Forwarded-Proto": {"javascript"}},
			wantIP:  "127.0.0.1", wantScheme: "http"},
		{name: "several proxies each stating the scheme: the outermost counts",
			trusted: loopback, remoteAddr: "127.0.0.1:52000",
			headers: map[string][]string{"X-Forwarded-Proto": {"https, http"}},
			wantIP:  "127.0.0.1", wantScheme: "https"},
		{name: "trusting nobody: even loopback is just a client",
			trusted: nil, remoteAddr: "127.0.0.1:52000",
			headers: map[string][]string{"X-Forwarded-For": {"203.0.113.7"}, "X-Forwarded-Proto": {"https"}},
			wantIP:  "127.0.0.1", wantScheme: "http"},
		{name: "the ingress terminating TLS itself",
			trusted: loopback, remoteAddr: "198.51.100.9:4711", tls: true, wantIP: "198.51.100.9", wantScheme: "https"},
		{name: "a peer with no IP address (a unix socket)",
			trusted: loopback, remoteAddr: "@",
			headers: map[string][]string{"X-Forwarded-For": {"203.0.113.7"}},
			wantIP:  "", wantScheme: "http"},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodGet, "http://shop.example:8443/", nil)
		r.RemoteAddr = c.remoteAddr
		for name, values := range c.headers {
			r.Header[name] = values
		}
		if c.tls {
			r.TLS = &tls.ConnectionState{}
		}
		got := c.trusted.facts(r)
		if got.clientIP != c.wantIP || got.scheme != c.wantScheme {
			t.Errorf("%s: client %q over %q, want %q over %q", c.name, got.clientIP, got.scheme, c.wantIP, c.wantScheme)
		}
		if got.host != "shop.example:8443" {
			t.Errorf("%s: host = %q, want the Host the request was addressed to", c.name, got.host)
		}
	}
}

// TestForwardedHeaders: whatever a client wrote about its own origin is gone, the ingress's
// statement stands in its place, and everything else passes through untouched.
func TestForwardedHeaders(t *testing.T) {
	in := http.Header{
		"X-Forwarded-For":    {"6.6.6.6"},
		"X-Forwarded-Proto":  {"https"},
		"X-Forwarded-Host":   {"bank.example"},
		"X-Forwarded-Port":   {"443"},
		"X-Forwarded-Prefix": {"/admin"},
		"Forwarded":          {"for=6.6.6.6;proto=https"},
		"X-Real-Ip":          {"6.6.6.6"},
		"Cookie":             {"a=1", "b=2"},
		"X-Custom":           {"kept"},
	}
	out := forwardedHeaders(in, requestFacts{host: "shop.example", scheme: "http", clientIP: "198.51.100.9"})
	want := map[string][]string{
		"X-Forwarded-For":   {"198.51.100.9"},
		"X-Forwarded-Proto": {"http"},
		"X-Forwarded-Host":  {"shop.example"},
		"Cookie":            {"a=1", "b=2"},
		"X-Custom":          {"kept"},
	}
	if !reflect.DeepEqual(out, want) {
		t.Errorf("headers for the thrall = %v\nwant %v", out, want)
	}
	if in.Get("X-Forwarded-For") != "6.6.6.6" {
		t.Error("the request's own headers must not be modified")
	}

	// With no client address to state, none is invented.
	if got := forwardedHeaders(http.Header{}, requestFacts{host: "shop.example", scheme: "http"}); got["X-Forwarded-For"] != nil {
		t.Errorf("X-Forwarded-For = %v for an unknown client, want it absent", got["X-Forwarded-For"])
	}
}

func TestParseTrustedProxies(t *testing.T) {
	trusted := mustTrust(t, " 127.0.0.1 , 10.0.0.0/8,2001:db8::/32, ::ffff:192.0.2.1 ,")
	for addr, want := range map[string]bool{
		"127.0.0.1": true, "127.0.0.2": false, "10.200.3.4": true, "11.0.0.1": false,
		"2001:db8::1": true, "2001:db9::1": false, "192.0.2.1": true,
	} {
		if got := trusted.contains(netip.MustParseAddr(addr)); got != want {
			t.Errorf("trust in %s = %v, want %v", addr, got, want)
		}
	}
	if got := mustTrust(t, ""); len(got) != 0 {
		t.Errorf("an empty list = %v, want no trusted proxy", got)
	}
	for _, bad := range []string{"localhost", "10.0.0.0/33", "1.2.3"} {
		if _, err := ParseTrustedProxies(bad); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
}
