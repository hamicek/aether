package ingress_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/ingress"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

// logLines parses what a JSON slog handler wrote into one map per line.
func logLines(t *testing.T, logged *lockedBuffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(logged.String()), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line is not JSON: %v in %q", err, raw)
		}
		lines = append(lines, line)
	}
	return lines
}

// awaitAccessLine returns the log once the access line of the last request is in it. The line is
// written after the response, so a client can have the response in hand a moment before it.
func awaitAccessLine(t *testing.T, logged *lockedBuffer) []map[string]any {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		lines := logLines(t, logged)
		if len(withMessage(lines, "http access")) > 0 {
			return lines
		}
		if time.Now().After(deadline) {
			t.Fatalf("no access line was logged:\n%s", logged.String())
		}
	}
}

// withMessage keeps the lines with the given msg.
func withMessage(lines []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, line := range lines {
		if line["msg"] == msg {
			out = append(out, line)
		}
	}
	return out
}

// TestAccessLogSaysWhatHappenedAndKeepsSecretsOut: one line per request with the fields an operator
// searches by, and none of the places a secret travels in - the query string, the cookies, the
// authorization header, the bodies.
func TestAccessLogSaysWhatHappenedAndKeepsSecretsOut(t *testing.T) {
	srv := embeddedNATS(t)
	nc := connect(t, srv)
	startWebThrall(t, srv, "shop", func(req thrall.HTTPRequest, s struct{}, ctx *thrall.Ctx) (thrall.HTTPResponse, struct{}, error) {
		resp := thrall.TextResponse(http.StatusCreated, "response-body-secret")
		resp.Headers["Set-Cookie"] = []string{"session=set-cookie-secret"}
		return resp, s, nil
	})
	var logged lockedBuffer
	loopback, _ := ingress.ParseTrustedProxies(ingress.DefaultTrustedProxies)
	url := front(t, nc, ingress.StaticRoutes{"shop.test": "shop"}, ingress.Options{
		Timeout: 2 * time.Second, TrustedProxies: loopback, Log: slog.New(slog.NewJSONHandler(&logged, nil)),
	})

	res := do(t, http.MethodPost, url+"/reset/confirm?token=query-secret", "shop.test",
		bytes.NewReader([]byte("request-body-secret")),
		"Cookie: session=cookie-secret", "Authorization: Bearer header-secret", "X-Forwarded-For: 203.0.113.7")
	if res.status != http.StatusCreated {
		t.Fatalf("status = %d, want 201", res.status)
	}

	lines := awaitAccessLine(t, &logged)
	access := withMessage(lines, "http access")
	if len(access) != 1 || len(lines) != 1 {
		t.Fatalf("a forwarded request must log exactly one access line and nothing else, got %d lines:\n%s", len(lines), logged.String())
	}
	line := access[0]
	want := map[string]any{
		"method": "POST", "host": "shop.test", "path": "/reset/confirm", "status": float64(201),
		"trace": res.header.Get(ingress.TraceHeader), "client_ip": "203.0.113.7", "thrall": "shop",
		"bytes_in": float64(len("request-body-secret")), "bytes_out": float64(len("response-body-secret")),
	}
	for field, value := range want {
		if line[field] != value {
			t.Errorf("access line %s = %v, want %v", field, line[field], value)
		}
	}
	if duration, ok := line["duration"].(float64); !ok || duration <= 0 {
		t.Errorf("access line duration = %v, want a positive duration", line["duration"])
	}
	for _, secret := range []string{"query-secret", "token", "cookie-secret", "header-secret", "request-body-secret", "response-body-secret", "set-cookie-secret"} {
		if strings.Contains(logged.String(), secret) {
			t.Errorf("the log contains %q:\n%s", secret, logged.String())
		}
	}
}

// TestEveryIngressMadeFailureIsExplained drives each way the ingress answers a request itself and
// checks the log tells them apart: a failure line naming the cause (a warning when the node is at
// fault, routine information when the request is), followed by the access line of the same request.
func TestEveryIngressMadeFailureIsExplained(t *testing.T) {
	srv := embeddedNATS(t)
	nc := connect(t, srv)
	startStubThrall(t, nc, "broken", func(req wire.Envelope) wire.Envelope {
		return thrall.ErrorReply(req, wire.WireError{Type: wire.ErrHandler, Message: "disk on fire"})
	})
	startStubThrall(t, nc, "expired", func(req wire.Envelope) wire.Envelope {
		return thrall.ErrorReply(req, wire.WireError{Type: wire.ErrDeadlineExceeded, Message: "dropped", Retryable: true})
	})
	startStubThrall(t, nc, "bloated", func(req wire.Envelope) wire.Envelope {
		return thrall.ErrorReply(req, wire.WireError{Type: wire.ErrReplyTooLarge, Message: "reply of 2000000 bytes does not fit one bus message"})
	})
	startStubThrall(t, nc, "statusless", func(req wire.Envelope) wire.Envelope {
		return thrall.OKReply(req, map[string]any{})
	})
	sub, err := nc.SubscribeSync(wire.Call(testApp, "silent")) // subscribed, never answers
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	routes := ingress.StaticRoutes{
		"broken.test": "broken", "expired.test": "expired", "bloated.test": "bloated",
		"statusless.test": "statusless", "silent.test": "silent", "down.test": "nobody", "upload.test": "broken",
	}
	var logged lockedBuffer
	url := front(t, nc, routes, ingress.Options{
		Timeout: 200 * time.Millisecond, MaxBodyBytes: 8, Log: slog.New(slog.NewJSONHandler(&logged, nil)),
	})

	cases := []struct {
		host   string
		body   string
		status int
		cause  string
		level  string
		thrall any // nil when the request never resolved to one
		errHas string
	}{
		{host: "unknown.test", status: 404, cause: "no_route", level: "INFO"},
		{host: "upload.test", body: "more than eight bytes", status: 413, cause: "body_too_large", level: "INFO", thrall: "broken", errHas: "exceeds the limit of 8 bytes"},
		{host: "down.test", status: 503, cause: "thrall_unavailable", level: "WARN", thrall: "nobody", errHas: "no responders"},
		{host: "silent.test", status: 504, cause: "thrall_timeout", level: "WARN", thrall: "silent", errHas: "timeout"},
		{host: "expired.test", status: 504, cause: "thrall_timeout", level: "WARN", thrall: "expired", errHas: "deadline_exceeded"},
		{host: "broken.test", status: 502, cause: "thrall_error", level: "WARN", thrall: "broken", errHas: "disk on fire"},
		{host: "bloated.test", status: 502, cause: "response_too_large", level: "WARN", thrall: "bloated", errHas: "does not fit one bus message"},
		{host: "statusless.test", status: 502, cause: "invalid_reply", level: "WARN", thrall: "statusless", errHas: "status 0"},
	}
	for _, c := range cases {
		logged.Reset()
		res := do(t, http.MethodPost, url+"/p?q=1", c.host, strings.NewReader(c.body))
		if res.status != c.status {
			t.Errorf("%s: status = %d, want %d", c.host, res.status, c.status)
			continue
		}
		lines := awaitAccessLine(t, &logged)
		failures, access := withMessage(lines, "ingress request failed"), withMessage(lines, "http access")
		if len(failures) != 1 || len(access) != 1 {
			t.Errorf("%s: %d failure lines and %d access lines, want one of each:\n%s", c.host, len(failures), len(access), logged.String())
			continue
		}
		f := failures[0]
		if f["cause"] != c.cause || f["level"] != c.level || f["status"] != float64(c.status) || f["thrall"] != c.thrall {
			t.Errorf("%s: failure line cause=%v level=%v status=%v thrall=%v; want %s/%s/%d/%v",
				c.host, f["cause"], f["level"], f["status"], f["thrall"], c.cause, c.level, c.status, c.thrall)
		}
		if c.errHas != "" && !strings.Contains(asString(f["err"]), c.errHas) {
			t.Errorf("%s: failure line err = %v, want it to mention %q", c.host, f["err"], c.errHas)
		}
		trace := res.header.Get(ingress.TraceHeader)
		if trace == "" || f["trace"] != trace || access[0]["trace"] != trace {
			t.Errorf("%s: the response, its failure line and its access line must share one trace: %q / %v / %v",
				c.host, trace, f["trace"], access[0]["trace"])
		}
		if access[0]["status"] != float64(c.status) || access[0]["path"] != "/p" {
			t.Errorf("%s: access line status=%v path=%v", c.host, access[0]["status"], access[0]["path"])
		}
	}
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}
