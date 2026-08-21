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
			name: "duplicate addr across edge servers",
			body: `
[[edge.http]]
name = "api"
addr = ":8080"
route."GET /v" = { thrall = "c", op = "v" }

[[edge.http]]
name = "admin"
addr = ":8080"
route."GET /w" = { thrall = "c", op = "w" }
`,
			wantErr: "already used by",
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

// TestLoadManifestNatsLeaf proves the [nats.leaf] section parses into the ether config with all
// spoke-side fields, and that its absence leaves Leaf nil (a standalone embedded bus).
func TestLoadManifestNatsLeaf(t *testing.T) {
	m, err := LoadManifest(writeManifest(t, `
app = "demo"

[nats]
mode      = "embedded"
store_dir = "/var/lib/aether/siteA"

[nats.leaf]
remote   = "nats-leaf://hub.internal:7422"
site     = "SITE_A"
domain   = "sa"
user     = "leafA"
password = "leafA"
nkey     = "/etc/aether/siteA.nk"

[[thrall]]
name = "counter"
cmd  = "run counter"
`))
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	leaf := m.Nats.Leaf
	if leaf == nil {
		t.Fatalf("nats.leaf = nil, want parsed section")
	}
	if leaf.Remote != "nats-leaf://hub.internal:7422" || leaf.Site != "SITE_A" || leaf.Domain != "sa" {
		t.Errorf("leaf routing = %+v, want remote/SITE_A/sa", leaf)
	}
	if leaf.User != "leafA" || leaf.Password != "leafA" || leaf.Nkey != "/etc/aether/siteA.nk" {
		t.Errorf("leaf credentials = %+v, want leafA/leafA and the nkey path", leaf)
	}

	noLeaf, err := LoadManifest(writeManifest(t, `
app = "demo"

[[thrall]]
name = "counter"
cmd  = "run counter"
`))
	if err != nil {
		t.Fatalf("LoadManifest (no leaf): %v", err)
	}
	if noLeaf.Nats.Leaf != nil {
		t.Errorf("nats.leaf = %+v, want nil when the section is absent", noLeaf.Nats.Leaf)
	}
}

// TestManifestNatsValidation covers the [nats] semantic checks: a bad mode, and the leaf
// constraints (embedded-only, required remote/site/domain) that fail fast rather than silently
// falling back to an isolated embedded bus.
func TestManifestNatsValidation(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr string // substring the error must contain (empty = expect success)
	}{
		{
			name: "ok leaf",
			body: `
app = "demo"
[nats]
mode = "embedded"
[nats.leaf]
remote = "nats-leaf://hub:7422"
site   = "SITE_A"
domain = "sa"
`,
		},
		{
			name: "leaf without top-level app",
			body: `
[nats]
mode = "embedded"
[nats.leaf]
remote = "nats-leaf://hub:7422"
site   = "SITE_A"
domain = "sa"
`,
			wantErr: "requires a top-level app",
		},
		{
			name: "leaf site with space",
			body: `
app = "demo"
[nats]
mode = "embedded"
[nats.leaf]
remote = "nats-leaf://hub:7422"
site   = "SITE A"
domain = "sa"
`,
			wantErr: "must be a plain identifier",
		},
		{
			name: "leaf domain with brace",
			body: `
app = "demo"
[nats]
mode = "embedded"
[nats.leaf]
remote = "nats-leaf://hub:7422"
site   = "SITE_A"
domain = "s}a"
`,
			wantErr: "must be a plain identifier",
		},
		{
			name: "leaf site reserved SYS",
			body: `
app = "demo"
[nats]
mode = "embedded"
[nats.leaf]
remote = "nats-leaf://hub:7422"
site   = "SYS"
domain = "sa"
`,
			wantErr: "reserved",
		},
		{
			name: "leaf password without user",
			body: `
app = "demo"
[nats]
mode = "embedded"
[nats.leaf]
remote   = "nats-leaf://hub:7422"
site     = "SITE_A"
domain   = "sa"
password = "secret"
`,
			wantErr: "without nats.leaf.user",
		},
		{
			name: "unknown mode",
			body: `
[nats]
mode = "cluster"
`,
			wantErr: "mode must be",
		},
		{
			name: "leaf with external mode",
			body: `
[nats]
mode = "external"
url  = "nats://hub:7390"
[nats.leaf]
remote = "nats-leaf://hub:7422"
site   = "SITE_A"
domain = "sa"
`,
			wantErr: "requires mode = \"embedded\"",
		},
		{
			name: "leaf missing remote",
			body: `
[nats]
mode = "embedded"
[nats.leaf]
site   = "SITE_A"
domain = "sa"
`,
			wantErr: "nats.leaf.remote is required",
		},
		{
			name: "leaf missing site",
			body: `
[nats]
mode = "embedded"
[nats.leaf]
remote = "nats-leaf://hub:7422"
domain = "sa"
`,
			wantErr: "nats.leaf.site is required",
		},
		{
			name: "leaf missing domain",
			body: `
[nats]
mode = "embedded"
[nats.leaf]
remote = "nats-leaf://hub:7422"
site   = "SITE_A"
`,
			wantErr: "nats.leaf.domain is required",
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

// TestManifestEventLogUseValidation covers the retention-intent field (AE-063): "rebuild" with a
// retention bound is a fail-fast config error, "audit" makes a bound legitimate, an unset intent
// keeps today's behaviour, and the field is meaningless without event_log.
func TestManifestEventLogUseValidation(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr string // substring the error must contain (empty = expect success)
	}{
		{
			name: "rebuild without a bound is ok",
			body: `
[[thrall]]
name = "c"
cmd  = "run"
event_log = true
event_log_use = "rebuild"
`,
		},
		{
			name: "rebuild with max_msgs is rejected",
			body: `
[[thrall]]
name = "c"
cmd  = "run"
event_log = true
event_log_use = "rebuild"
event_log_max_msgs = 1000
`,
			wantErr: "truncates the rebuild",
		},
		{
			name: "rebuild with max_age is rejected",
			body: `
[[thrall]]
name = "c"
cmd  = "run"
event_log = true
event_log_use = "rebuild"
event_log_max_age_ms = 60000
`,
			wantErr: "truncates the rebuild",
		},
		{
			name: "audit with a bound is ok",
			body: `
[[thrall]]
name = "c"
cmd  = "run"
event_log = true
event_log_use = "audit"
event_log_max_msgs = 1000
`,
		},
		{
			name: "unset with a bound is ok at load (warns at runtime)",
			body: `
[[thrall]]
name = "c"
cmd  = "run"
event_log = true
event_log_max_msgs = 1000
`,
		},
		{
			name: "invalid value is rejected",
			body: `
[[thrall]]
name = "c"
cmd  = "run"
event_log = true
event_log_use = "replay"
`,
			wantErr: "is invalid",
		},
		{
			name: "use without event_log is rejected",
			body: `
[[thrall]]
name = "c"
cmd  = "run"
event_log_use = "audit"
`,
			wantErr: "requires event_log = true",
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

// writeFile writes body to name inside dir (helper for schema-alongside-manifest tests).
func writeFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

const measurementSchemaJSON = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["value"],
  "properties": { "value": { "type": "number" } }
}`

// TestLoadManifestEdgeSchemaInlined: an edge route's schema path is resolved relative to the
// manifest, its content read and compile-checked at load, and inlined into the runtime spec.
func TestLoadManifestEdgeSchemaInlined(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "measurement.schema.json", measurementSchemaJSON)
	path := writeFile(t, dir, "aether.toml", `
app = "demo"

[[thrall]]
name = "worker"
cmd  = "run worker"

[[edge.http]]
name = "api"
addr = ":8080"
route."POST /ingest" = { thrall = "worker", op = "ingest", kind = "cast", schema = "measurement.schema.json" }
`)

	m, err := LoadManifest(path)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if got := m.Edge.HTTP[0].Routes["POST /ingest"].schemaJSON; got == "" {
		t.Fatal("schema content was not inlined into the route")
	}
	// and it is carried into the runtime spec the edge process receives
	rt := edgeSpecToRuntime(m.Edge.HTTP[0])
	if rt.Routes["POST /ingest"].SchemaJSON == "" {
		t.Fatal("schema content was not carried into the runtime edge spec")
	}
	// a route without a schema stays empty
	if got := edgeSpecToRuntime(m.Edge.HTTP[0]).Routes["POST /ingest"].SchemaJSON; got == "" {
		t.Fatal("expected the ingest route to carry a schema")
	}
}

// TestLoadManifestEdgeSchemaFailFast: a missing or invalid schema fails the load, not the first request.
func TestLoadManifestEdgeSchemaFailFast(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		dir := t.TempDir()
		path := writeFile(t, dir, "aether.toml", `
app = "demo"
[[thrall]]
name = "worker"
cmd  = "run worker"
[[edge.http]]
name = "api"
addr = ":8080"
route."POST /x" = { thrall = "worker", op = "x", kind = "cast", schema = "nope.json" }
`)
		if _, err := LoadManifest(path); err == nil {
			t.Fatal("expected LoadManifest to fail for a missing schema file")
		}
	})
	t.Run("invalid schema", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "bad.json", `{"type": 123}`)
		path := writeFile(t, dir, "aether.toml", `
app = "demo"
[[thrall]]
name = "worker"
cmd  = "run worker"
[[edge.http]]
name = "api"
addr = ":8080"
route."POST /x" = { thrall = "worker", op = "x", kind = "cast", schema = "bad.json" }
`)
		if _, err := LoadManifest(path); err == nil {
			t.Fatal("expected LoadManifest to fail for an invalid schema")
		}
	})
}
