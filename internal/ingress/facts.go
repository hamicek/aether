package ingress

import (
	"fmt"
	"net/http"
	"net/netip"
	"strings"
)

// DefaultTrustedProxies is the set of peers whose forwarding headers the ingress believes unless
// told otherwise: loopback, which is where the reverse proxy in front of it runs when both share a
// host. A proxy that reaches the ingress from another address (a container bridge, another machine)
// has to be listed with -trusted-proxies.
const DefaultTrustedProxies = "127.0.0.0/8,::1/128"

// The forwarding headers a reverse proxy uses to tell an upstream what it saw.
const (
	headerForwardedFor   = "X-Forwarded-For"
	headerForwardedProto = "X-Forwarded-Proto"
	headerForwardedHost  = "X-Forwarded-Host"
)

// TrustedProxies is the set of peers allowed to vouch for a request: when the ingress's own peer is
// one of them, what that peer says in X-Forwarded-For and X-Forwarded-Proto is taken as fact. From
// anyone else those headers are just text a client typed.
type TrustedProxies []netip.Prefix

// ParseTrustedProxies reads a comma-separated list of IP addresses and CIDR prefixes. An empty list
// trusts nobody: every request is then described by the ingress's own connection alone.
func ParseTrustedProxies(list string) (TrustedProxies, error) {
	var trusted TrustedProxies
	for _, item := range strings.Split(list, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if prefix, err := netip.ParsePrefix(item); err == nil {
			trusted = append(trusted, prefix.Masked())
			continue
		}
		addr, err := netip.ParseAddr(item)
		if err != nil {
			return nil, fmt.Errorf("ingress: trusted proxy %q is neither an IP address nor a CIDR prefix", item)
		}
		addr = addr.Unmap()
		trusted = append(trusted, netip.PrefixFrom(addr, addr.BitLen()))
	}
	return trusted, nil
}

func (t TrustedProxies) contains(addr netip.Addr) bool {
	for _, prefix := range t {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// requestFacts are the facts about a request the ingress vouches for (see wire.HTTPRequest).
type requestFacts struct {
	host     string
	scheme   string
	clientIP string
}

// facts establishes who sent a request and how. The starting point is what the ingress sees itself:
// the peer of its connection and whether that connection is TLS. Only when that peer is a trusted
// proxy are its forwarding headers consulted - the client is then the nearest address in
// X-Forwarded-For that is not itself a trusted proxy, and the scheme is what X-Forwarded-Proto
// says. A client talking to the ingress directly cannot influence either.
func (t TrustedProxies) facts(r *http.Request) requestFacts {
	facts := requestFacts{host: r.Host, scheme: "http"}
	if r.TLS != nil {
		facts.scheme = "https"
	}
	peer, ok := peerAddr(r.RemoteAddr)
	if !ok {
		return facts // not an IP peer (a unix socket): no address to state, nobody to trust
	}
	facts.clientIP = peer.String()
	if !t.contains(peer) {
		return facts
	}
	if client, ok := t.forwardedClient(r.Header.Values(headerForwardedFor)); ok {
		facts.clientIP = client.String()
	}
	if scheme := forwardedScheme(r.Header.Get(headerForwardedProto)); scheme != "" {
		facts.scheme = scheme
	}
	return facts
}

// forwardedClient reads the client out of the X-Forwarded-For chain a trusted proxy handed over.
// Each proxy appends the address it received the request from, so the chain is walked from the
// right - the end the trusted proxies wrote - and the first address that is not a trusted proxy is
// the client. Everything left of it is what that client claimed and is ignored. When every address
// is a trusted proxy the request came from inside, and the leftmost is the origin. An entry that is
// not an address ends the walk with no answer: the caller keeps the peer it can see.
func (t TrustedProxies) forwardedClient(values []string) (netip.Addr, bool) {
	var chain []string
	for _, value := range values {
		chain = append(chain, strings.Split(value, ",")...)
	}
	var origin netip.Addr
	for i := len(chain) - 1; i >= 0; i-- {
		addr, ok := parseForwardedAddr(chain[i])
		if !ok {
			return netip.Addr{}, false
		}
		if !t.contains(addr) {
			return addr, true
		}
		origin = addr
	}
	return origin, origin.IsValid()
}

// parseForwardedAddr reads one X-Forwarded-For entry: an IP address, by some proxies with a port.
func parseForwardedAddr(entry string) (netip.Addr, bool) {
	entry = strings.TrimSpace(entry)
	if addr, err := netip.ParseAddr(entry); err == nil {
		return addr.Unmap(), true
	}
	if addrPort, err := netip.ParseAddrPort(entry); err == nil {
		return addrPort.Addr().Unmap(), true
	}
	return netip.Addr{}, false
}

// forwardedScheme reads X-Forwarded-Proto: the scheme the outermost proxy saw, which is the first
// element when several proxies each added theirs. Anything but http or https is ignored.
func forwardedScheme(value string) string {
	first, _, _ := strings.Cut(value, ",")
	switch scheme := strings.ToLower(strings.TrimSpace(first)); scheme {
	case "http", "https":
		return scheme
	default:
		return ""
	}
}

// peerAddr is the IP address of the connection's other end, from http.Request.RemoteAddr.
func peerAddr(remoteAddr string) (netip.Addr, bool) {
	addrPort, err := netip.ParseAddrPort(remoteAddr)
	if err != nil {
		return netip.Addr{}, false
	}
	return addrPort.Addr().Unmap(), true
}

// forwardedHeaders returns the request's headers as the web thrall should see them: without any
// forwarding header the client or a proxy sent, and with the ingress's own statement of the same
// facts in their place. A thrall that reads X-Forwarded-For, -Proto or -Host therefore gets what
// the ingress established, never what a client wrote; the dedicated request fields say the same.
// The request's own header map is left untouched.
func forwardedHeaders(in http.Header, facts requestFacts) map[string][]string {
	out := make(map[string][]string, len(in)+3)
	for name, values := range in {
		if isForwardingHeader(name) {
			continue
		}
		out[name] = values
	}
	if facts.clientIP != "" {
		out[headerForwardedFor] = []string{facts.clientIP}
	}
	out[headerForwardedProto] = []string{facts.scheme}
	if facts.host != "" {
		out[headerForwardedHost] = []string{facts.host}
	}
	return out
}

// isForwardingHeader reports whether a header claims something about the request's origin: the
// X-Forwarded-* family, the standard Forwarded header and X-Real-IP.
func isForwardingHeader(name string) bool {
	name = http.CanonicalHeaderKey(name)
	return strings.HasPrefix(name, "X-Forwarded-") || name == "Forwarded" || name == "X-Real-Ip"
}
