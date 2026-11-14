package ingress_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/examples/http-ingress/ingress"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// TestEndToEnd proves the whole path: an HTTP request enters the ingress, is forwarded over NATS as
// an http_request call to a REAL web thrall (thrall.StartWeb, the AE-091 helper), and the web
// thrall's HTML reply comes back out as the HTTP response. An unknown host is denied (default-deny).
func TestEndToEnd(t *testing.T) {
	srv := embeddedNATS(t)

	// A real web thrall, started through the SDK helper, serving a tiny HTML page.
	const html = "<h1>hello from the blog</h1>"
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

	// The ingress connects on its own and routes blog.test -> the blog thrall.
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("ingress connect: %v", err)
	}
	t.Cleanup(nc.Close)
	handler := ingress.NewHandler(nc, ingress.StaticRoutes{"blog.test": "blog"},
		ingress.Options{App: "web", Timeout: 2 * time.Second})
	front := httptest.NewServer(handler)
	t.Cleanup(front.Close)

	// Wait for the web thrall to subscribe: retry the request until it is no longer "no responders".
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

	if status != http.StatusOK {
		t.Fatalf("allowed host: status = %d, body = %q", status, body)
	}
	if body != html+" path=/hello" {
		t.Errorf("allowed host: body = %q, want the thrall's HTML", body)
	}

	// Default-deny: a host with no route never reaches the ether.
	if _, denied := getWithHost(t, front.URL+"/", "evil.test"); denied != http.StatusNotFound {
		t.Errorf("unknown host: status = %d, want 404", denied)
	}
}

func getWithHost(t *testing.T, url, host string) (string, int) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request to %s (Host %s): %v", url, host, err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return string(b), res.StatusCode
}

func embeddedNATS(t *testing.T) *natsserver.Server {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  filepath.Join(t.TempDir(), "js"),
		NoSigs:    true,
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
