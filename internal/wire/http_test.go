package wire

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const httpGoldenDir = "../../testdata/wire/http"

func httpGoldenPath(name string) string { return filepath.Join(httpGoldenDir, name+".json") }

// httpGoldenCases are the canonical HTTP payloads shared with the TS and Python parity tests. Go is
// the source of truth: `go test ./internal/wire -run TestHTTPGolden -update` regenerates the JSON
// fixtures that sdk/ts and sdk/python assert against. The cases use explicit literal values (not the
// response constructors) so the fixtures are byte-stable and independent of per-language JSON
// marshalling; the constructors are unit-tested separately in each SDK.
func httpGoldenCases() map[string]any {
	return map[string]any{
		"req_get": HTTPRequest{Method: "GET", Path: "/"},
		"req_post_binary": HTTPRequest{Method: "POST", Path: "/upload",
			Headers: map[string][]string{"Content-Type": {"application/octet-stream"}},
			Body:    []byte{0x00, 0x01, 0xff, 0x10}},
		"req_query_multi": HTTPRequest{Method: "GET", Path: "/search", Query: "q=cats&q=dogs&safe=1",
			Headers: map[string][]string{"X-Tag": {"a", "b"}, "Accept": {"text/html"}}},
		"resp_html": HTTPResponse{Status: 200,
			Headers: map[string][]string{"Content-Type": {"text/html; charset=utf-8"}},
			Body:    []byte("<h1>hi</h1>")},
		"resp_json": HTTPResponse{Status: 201,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    []byte(`{"ok":true}`)},
		"resp_cookies": HTTPResponse{Status: 200,
			Headers: map[string][]string{"Set-Cookie": {"a=1", "b=2"}},
			Body:    []byte("ok")},
		"resp_minimal": HTTPResponse{Status: 204},
	}
}

// TestHTTPGolden pins the canonical wire form of each HTTP payload. With -update it (re)writes the
// golden fixtures; otherwise it fails on any drift between the Go marshaller and the committed files.
func TestHTTPGolden(t *testing.T) {
	for name, payload := range httpGoldenCases() {
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("%s: marshal: %v", name, err)
		}
		path := httpGoldenPath(name)
		if *update {
			if err := os.MkdirAll(httpGoldenDir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		want, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: read golden (run with -update to generate): %v", name, err)
		}
		if got := string(data); got != strings.TrimSpace(string(want)) {
			t.Errorf("%s: http payload drift\n got: %s\nwant: %s", name, got, strings.TrimSpace(string(want)))
		}
	}
}

// TestHTTPRoundTrip proves the canonical form is a fixed point: unmarshalling a golden fixture back
// into its type and marshalling it again yields the exact same bytes.
func TestHTTPRoundTrip(t *testing.T) {
	reqs := map[string]bool{"req_get": true, "req_post_binary": true, "req_query_multi": true}
	for name := range httpGoldenCases() {
		raw, err := os.ReadFile(httpGoldenPath(name))
		if err != nil {
			t.Fatalf("%s: read golden: %v", name, err)
		}
		var out []byte
		if reqs[name] {
			var r HTTPRequest
			if err := json.Unmarshal(raw, &r); err != nil {
				t.Fatalf("%s: unmarshal: %v", name, err)
			}
			out, _ = json.Marshal(r)
		} else {
			var r HTTPResponse
			if err := json.Unmarshal(raw, &r); err != nil {
				t.Fatalf("%s: unmarshal: %v", name, err)
			}
			out, _ = json.Marshal(r)
		}
		if string(out) != strings.TrimSpace(string(raw)) {
			t.Errorf("%s: round-trip drift\n got: %s\nwant: %s", name, out, strings.TrimSpace(string(raw)))
		}
	}
}

// TestHTTPBinarySafe guards that an arbitrary byte body survives a JSON round-trip unchanged - the
// point of carrying the body as base64 rather than a string.
func TestHTTPBinarySafe(t *testing.T) {
	body := make([]byte, 256)
	for i := range body {
		body[i] = byte(i)
	}
	data, err := json.Marshal(HTTPResponse{Status: 200, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	var back HTTPResponse
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back.Body, body) {
		t.Errorf("binary body changed across round-trip: got %v", back.Body)
	}
}

// TestHTTPOmitEmpty guards that empty optional fields are absent from the wire form, and that Status
// is always present (a response always has one).
func TestHTTPOmitEmpty(t *testing.T) {
	req, _ := json.Marshal(HTTPRequest{Method: "GET", Path: "/"})
	if got := string(req); got != `{"method":"GET","path":"/"}` {
		t.Errorf("minimal request: got %s", got)
	}
	for _, field := range []string{"query", "headers", "body"} {
		if strings.Contains(string(req), `"`+field+`"`) {
			t.Errorf("minimal request unexpectedly contains %q: %s", field, req)
		}
	}
	resp, _ := json.Marshal(HTTPResponse{Status: 204})
	if got := string(resp); got != `{"status":204}` {
		t.Errorf("minimal response: got %s, want {\"status\":204}", got)
	}
}

// TestHTTPResponseConstructors covers the ergonomic response builders: the status, content type and
// body each constructor sets.
func TestHTTPResponseConstructors(t *testing.T) {
	text := TextResponse(200, "hello")
	if text.Status != 200 || string(text.Body) != "hello" || text.Headers["Content-Type"][0] != "text/plain; charset=utf-8" {
		t.Errorf("TextResponse: %+v", text)
	}
	html := HTMLResponse(404, "<p>gone</p>")
	if html.Status != 404 || string(html.Body) != "<p>gone</p>" || html.Headers["Content-Type"][0] != "text/html; charset=utf-8" {
		t.Errorf("HTMLResponse: %+v", html)
	}
	j, err := JSONResponse(201, map[string]int{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != 201 || string(j.Body) != `{"n":1}` || j.Headers["Content-Type"][0] != "application/json" {
		t.Errorf("JSONResponse: %+v", j)
	}
	if _, err := JSONResponse(200, make(chan int)); err == nil {
		t.Error("JSONResponse: expected error marshalling an unsupported value")
	}
}
