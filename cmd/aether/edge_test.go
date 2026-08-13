package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/edge"
	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/wire"
)

// TestEdgeHandlerBridge drives the HTTP->ether bridge against a real embedded NATS server with a
// fake thrall responder, covering call success, cast, an application error reply, an unknown route
// and a missing thrall (no responders).
func TestEdgeHandlerBridge(t *testing.T) {
	const app = "demo"

	eth, err := ether.Start(context.Background(), ether.Config{Mode: "embedded"})
	if err != nil {
		t.Fatalf("ether.Start: %v", err)
	}
	t.Cleanup(eth.Stop)

	nc, err := nats.Connect(eth.URL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)

	// Fake thrall "counter": replies to call ops, and records casts.
	gotCast := make(chan wire.Envelope, 1)
	if _, err := nc.Subscribe(wire.Call(app, "counter"), func(m *nats.Msg) {
		var req wire.Envelope
		_ = json.Unmarshal(m.Data, &req)
		reply := wire.Envelope{V: 1, Kind: wire.KindReply, Status: "ok"}
		switch req.Op {
		case "value":
			reply.Payload = json.RawMessage(`{"value":3}`)
		case "fail":
			reply.Status = "error"
			reply.Error = &wire.WireError{Type: "boom", Message: "handler failed"}
		}
		data, _ := json.Marshal(reply)
		_ = m.Respond(data)
	}); err != nil {
		t.Fatalf("subscribe call: %v", err)
	}
	if _, err := nc.Subscribe(wire.Cast(app, "counter"), func(m *nats.Msg) {
		var env wire.Envelope
		_ = json.Unmarshal(m.Data, &env)
		gotCast <- env
	}); err != nil {
		t.Fatalf("subscribe cast: %v", err)
	}
	_ = nc.Flush()

	spec := edge.Spec{
		Name: "api",
		Addr: ":0",
		Routes: map[string]edge.Route{
			"GET /value": {Thrall: "counter", Op: "value", Kind: "call"},
			"POST /inc":  {Thrall: "counter", Op: "inc", Kind: "cast"},
			"GET /fail":  {Thrall: "counter", Op: "fail", Kind: "call"},
			"GET /dead":  {Thrall: "ghost", Op: "x", Kind: "call"},
		},
	}
	srv := httptest.NewServer(edgeHandler(nc, app, edge.NewRouter(spec), time.Second, obs.NewLogger()))
	t.Cleanup(srv.Close)

	// call success -> 200 + reply payload
	t.Run("call returns reply body", func(t *testing.T) {
		status, body := get(t, srv.URL+"/value")
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
		if strings.TrimSpace(body) != `{"value":3}` {
			t.Fatalf("body = %q, want the thrall reply", body)
		}
	})

	// cast -> 202 and the message reaches the ether
	t.Run("cast is accepted and delivered", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/inc", strings.NewReader(`{}`))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", resp.StatusCode)
		}
		select {
		case env := <-gotCast:
			if env.Op != "inc" {
				t.Fatalf("cast op = %q, want inc", env.Op)
			}
		case <-time.After(time.Second):
			t.Fatal("cast not delivered to the ether")
		}
	})

	// application error reply -> 502
	t.Run("thrall error maps to 502", func(t *testing.T) {
		status, _ := get(t, srv.URL+"/fail")
		if status != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", status)
		}
	})

	// unknown route -> 404
	t.Run("unknown route maps to 404", func(t *testing.T) {
		status, _ := get(t, srv.URL+"/nope")
		if status != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", status)
		}
	})

	// non-JSON body -> 400 with a clear message
	t.Run("invalid JSON body maps to 400", func(t *testing.T) {
		resp, err := http.Post(srv.URL+"/inc", "text/plain", strings.NewReader("not json"))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "valid JSON") {
			t.Fatalf("body = %q, want a clear JSON error", body)
		}
	})

	// body over the limit -> 413
	t.Run("oversized body maps to 413", func(t *testing.T) {
		big := strings.NewReader(strings.Repeat("x", (1<<20)+1))
		resp, err := http.Post(srv.URL+"/inc", "application/json", big)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", resp.StatusCode)
		}
	})

	// a thrall error reply must not leak the internal detail to the caller
	t.Run("error reply does not leak thrall detail", func(t *testing.T) {
		status, body := get(t, srv.URL+"/fail")
		if status != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", status)
		}
		if strings.Contains(body, "handler failed") || strings.Contains(body, "boom") {
			t.Fatalf("response leaked thrall error detail: %q", body)
		}
	})

	// missing thrall (no responders) -> 503
	t.Run("no responders maps to 503", func(t *testing.T) {
		status, _ := get(t, srv.URL+"/dead")
		if status != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", status)
		}
	})
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}
