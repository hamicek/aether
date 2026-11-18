package ingress_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/ingress"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// seenByThrall is what the echo thrall reports back about the request it received.
type seenByThrall struct {
	Host     string              `json:"host"`
	Scheme   string              `json:"scheme"`
	ClientIP string              `json:"client_ip"`
	Headers  map[string][]string `json:"headers"`
}

// echoFacts asks the ingress at url and returns what the web thrall behind it was told.
func echoFacts(t *testing.T, url string, headers ...string) seenByThrall {
	t.Helper()
	res := do(t, http.MethodGet, url+"/", "shop.test", nil, headers...)
	if res.status != http.StatusOK {
		t.Fatalf("echo: status = %d, body %q", res.status, res.body)
	}
	var seen seenByThrall
	if err := json.Unmarshal([]byte(res.body), &seen); err != nil {
		t.Fatalf("echo: %v in %q", err, res.body)
	}
	return seen
}

// TestForwardingHeadersAreBelievedOnlyFromTrustedProxies sends the same forged headers through two
// ingresses that differ only in whom they trust. The test client connects over loopback, so to one
// ingress it is an arbitrary client and to the other it is the reverse proxy.
//
// A real web thrall reports what it was told. From an untrusted peer the forgery is dropped: the
// request fields state the real peer, and the X-Forwarded-* headers the thrall sees are the
// ingress's own. From a trusted peer the same headers are taken as the proxy's word.
func TestForwardingHeadersAreBelievedOnlyFromTrustedProxies(t *testing.T) {
	srv := embeddedNATS(t)
	nc := connect(t, srv)
	startWebThrall(t, srv, "shop", func(req thrall.HTTPRequest, s struct{}, ctx *thrall.Ctx) (thrall.HTTPResponse, struct{}, error) {
		resp, err := thrall.JSONResponse(http.StatusOK, seenByThrall{
			Host: req.Host, Scheme: req.Scheme, ClientIP: req.ClientIP, Headers: req.Headers,
		})
		return resp, s, err
	})
	routes := ingress.StaticRoutes{"shop.test": "shop"}
	loopback, err := ingress.ParseTrustedProxies(ingress.DefaultTrustedProxies)
	if err != nil {
		t.Fatal(err)
	}
	forged := []string{
		"X-Forwarded-For: 203.0.113.7",
		"X-Forwarded-Proto: https",
		"X-Forwarded-Host: bank.example",
		"Forwarded: for=203.0.113.7;proto=https;host=bank.example",
		"X-Real-Ip: 203.0.113.7",
		"X-Custom: kept",
	}

	t.Run("untrusted peer", func(t *testing.T) {
		url := front(t, nc, routes, ingress.Options{Timeout: 2 * time.Second}) // trusts nobody
		seen := echoFacts(t, url, forged...)
		if seen.ClientIP != "127.0.0.1" || seen.Scheme != "http" || seen.Host != "shop.test" {
			t.Errorf("facts = %s over %s to %s; want the real peer 127.0.0.1 over http to shop.test", seen.ClientIP, seen.Scheme, seen.Host)
		}
		assertHeader(t, seen.Headers, "X-Forwarded-For", "127.0.0.1")
		assertHeader(t, seen.Headers, "X-Forwarded-Proto", "http")
		assertHeader(t, seen.Headers, "X-Forwarded-Host", "shop.test")
		assertHeader(t, seen.Headers, "Forwarded")
		assertHeader(t, seen.Headers, "X-Real-Ip")
		assertHeader(t, seen.Headers, "X-Custom", "kept")
	})

	t.Run("trusted proxy", func(t *testing.T) {
		url := front(t, nc, routes, ingress.Options{Timeout: 2 * time.Second, TrustedProxies: loopback})
		seen := echoFacts(t, url, forged...)
		if seen.ClientIP != "203.0.113.7" || seen.Scheme != "https" {
			t.Errorf("facts = %s over %s; want the proxy's word, 203.0.113.7 over https", seen.ClientIP, seen.Scheme)
		}
		// The host is the one the request was routed by, whatever X-Forwarded-Host claims.
		if seen.Host != "shop.test" {
			t.Errorf("host = %q, want shop.test", seen.Host)
		}
		assertHeader(t, seen.Headers, "X-Forwarded-For", "203.0.113.7")
		assertHeader(t, seen.Headers, "X-Forwarded-Proto", "https")
		assertHeader(t, seen.Headers, "X-Forwarded-Host", "shop.test")
		assertHeader(t, seen.Headers, "Forwarded")
		assertHeader(t, seen.Headers, "X-Real-Ip")

		// Without forwarding headers the proxy itself is the only thing known.
		plain := echoFacts(t, url)
		if plain.ClientIP != "127.0.0.1" || plain.Scheme != "http" {
			t.Errorf("no forwarding headers: facts = %s over %s; want 127.0.0.1 over http", plain.ClientIP, plain.Scheme)
		}
	})
}

// assertHeader checks the values the thrall saw for a header; no wanted values means absent.
func assertHeader(t *testing.T, headers map[string][]string, name string, want ...string) {
	t.Helper()
	got := headers[name]
	if len(got) != len(want) {
		t.Errorf("header %s = %v, want %v", name, got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("header %s = %v, want %v", name, got, want)
			return
		}
	}
}
