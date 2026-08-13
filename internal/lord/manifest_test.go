package lord

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeManifest writes body to a temp aether.toml and returns its path.
func writeManifest(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "aether.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return path
}

// TestLoadManifestEdge proves multiple [[edge.http]] servers parse, each with its own port and
// routes, and that a route without an explicit kind defaults to call.
func TestLoadManifestEdge(t *testing.T) {
	path := writeManifest(t, `
app = "demo"

[[thrall]]
name = "counter"
cmd  = "run counter"

[[edge.http]]
name = "api"
addr = ":8080"
route."GET /value"      = { thrall = "counter", op = "value" }
route."POST /increment" = { thrall = "counter", op = "increment", kind = "cast" }

[[edge.http]]
name = "admin"
addr = ":8081"
route."POST /reset" = { thrall = "counter", op = "reset", kind = "cast" }
`)

	m, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if len(m.Edge.HTTP) != 2 {
		t.Fatalf("edge servers = %d, want 2", len(m.Edge.HTTP))
	}

	api := m.Edge.HTTP[0]
	if api.Name != "api" || api.Addr != ":8080" {
		t.Errorf("api = %+v, want name=api addr=:8080", api)
	}
	if len(api.Routes) != 2 {
		t.Fatalf("api routes = %d, want 2", len(api.Routes))
	}
	if got := api.Routes["GET /value"]; got.Thrall != "counter" || got.Op != "value" || got.Kind != "call" {
		t.Errorf("GET /value = %+v, want counter/value/call (kind defaulted)", got)
	}
	if got := api.Routes["POST /increment"]; got.Kind != "cast" {
		t.Errorf("POST /increment kind = %q, want cast", got.Kind)
	}

	if m.Edge.HTTP[1].Name != "admin" || m.Edge.HTTP[1].Addr != ":8081" {
		t.Errorf("admin = %+v, want name=admin addr=:8081", m.Edge.HTTP[1])
	}
}

// TestManifestEdgeValidation covers the semantic checks that run after parsing.
func TestManifestEdgeValidation(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr string // substring the error must contain (empty = expect success)
	}{
		{
			name: "ok",
			body: `
[[edge.http]]
name = "api"
addr = ":8080"
route."GET /v" = { thrall = "c", op = "v" }
`,
		},
		{
			name: "duplicate name across thrall and edge",
			body: `
[[thrall]]
name = "api"
cmd  = "run"

[[edge.http]]
name = "api"
addr = ":8080"
route."GET /v" = { thrall = "c", op = "v" }
`,
			wantErr: "duplicate name",
		},
		{
			name: "empty addr",
			body: `
[[edge.http]]
name = "api"
route."GET /v" = { thrall = "c", op = "v" }
`,
			wantErr: "empty addr",
		},
		{
			name: "no routes",
			body: `
[[edge.http]]
name = "api"
addr = ":8080"
`,
			wantErr: "no routes",
		},
		{
			name: "missing thrall",
			body: `
[[edge.http]]
name = "api"
addr = ":8080"
route."GET /v" = { op = "v" }
`,
			wantErr: "thrall and op are required",
		},
		{
			name: "bad kind",
			body: `
[[edge.http]]
name = "api"
addr = ":8080"
route."GET /v" = { thrall = "c", op = "v", kind = "stream" }
`,
			wantErr: "kind must be call or cast",
		},
		{
			name: "route key without method",
			body: `
[[edge.http]]
name = "api"
addr = ":8080"
route."/v" = { thrall = "c", op = "v" }
`,
			wantErr: "METHOD /path",
		},
		{
			name: "unsupported method",
			body: `
[[edge.http]]
name = "api"
addr = ":8080"
route."TRACE /v" = { thrall = "c", op = "v" }
`,
			wantErr: "unsupported HTTP method",
		},
		{
			name: "path without leading slash",
			body: `
[[edge.http]]
name = "api"
addr = ":8080"
route."GET v" = { thrall = "c", op = "v" }
`,
			wantErr: "path must start",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadManifest(writeManifest(t, tc.body))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}
