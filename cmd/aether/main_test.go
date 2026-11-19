package main

import (
	"bytes"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hamicek/aether/internal/fleet"
	"github.com/hamicek/aether/internal/natstest"
	"github.com/hamicek/aether/internal/wire"
)

// TestDial proves the CLI authenticates to a secured bus (server TLS + nkey) when given the
// credentials, and is refused without them (no silent fallback to unsecured).
func TestDial(t *testing.T) {
	sec := natstest.SecuredServer(t)

	t.Run("with credentials connects and round-trips", func(t *testing.T) {
		nc, err := dial(endpoint{URL: sec.URL, CA: sec.CAFile, NkeySeed: sec.SeedFile})
		if err != nil {
			t.Fatalf("dial with credentials: %v", err)
		}
		defer nc.Close()

		sub, err := nc.SubscribeSync("cli.ping")
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		_ = nc.Flush()
		if err := nc.Publish("cli.ping", []byte("hi")); err != nil {
			t.Fatalf("publish: %v", err)
		}
		_ = nc.Flush()
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("no round-trip: %v", err)
		}
		if string(msg.Data) != "hi" {
			t.Fatalf("round-trip payload = %q, want %q", msg.Data, "hi")
		}
	})

	t.Run("without credentials is refused", func(t *testing.T) {
		if nc, err := dial(endpoint{URL: sec.URL}); err == nil {
			nc.Close()
			t.Fatal("expected connect to be refused without credentials, but it succeeded")
		}
	})

	t.Run("CA without nkey is refused", func(t *testing.T) {
		if nc, err := dial(endpoint{URL: sec.URL, CA: sec.CAFile}); err == nil {
			nc.Close()
			t.Fatal("expected connect to be refused without an nkey, but it succeeded")
		}
	})
}

// TestResolveEndpointPrecedence checks each credential field layers flag > .aether-endpoint > env.
func TestResolveEndpointPrecedence(t *testing.T) {
	dir := t.TempDir()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	t.Setenv("AETHER_NATS_CA", "envCA")
	t.Setenv("AETHER_NATS_NKEY_SEED", "envSeed")

	// The endpoint file carries a URL (so resolveEndpoint does not fatal) and file-level creds.
	writeEndpoint(endpoint{URL: "nats://file:4222", App: "a", CA: "fileCA", NkeySeed: "fileSeed"})

	// A flag wins over the file and env.
	if ep := resolveEndpoint("", "", "flagCA", "flagSeed"); ep.CA != "flagCA" || ep.NkeySeed != "flagSeed" {
		t.Fatalf("flag should win: got CA=%q nkey=%q", ep.CA, ep.NkeySeed)
	}

	// No flag: the file wins over env.
	if ep := resolveEndpoint("", "", "", ""); ep.CA != "fileCA" || ep.NkeySeed != "fileSeed" {
		t.Fatalf("file should win over env: got CA=%q nkey=%q", ep.CA, ep.NkeySeed)
	}

	// No flag and the file omits creds: env fills in.
	writeEndpoint(endpoint{URL: "nats://file:4222", App: "a"})
	if ep := resolveEndpoint("", "", "", ""); ep.CA != "envCA" || ep.NkeySeed != "envSeed" {
		t.Fatalf("env should fill when the file omits creds: got CA=%q nkey=%q", ep.CA, ep.NkeySeed)
	}
}

// TestVersionMismatch covers the rollout-mismatch predicate used by ps/describe/fleet: a mismatch
// requires a declared expectation and a differing reported version (a reported-empty version
// against a set expectation counts); no expectation is never a mismatch.
func TestVersionMismatch(t *testing.T) {
	cases := []struct {
		reported, expected string
		want               bool
	}{
		{"1.4.0", "1.4.0", false}, // match
		{"1.3.0", "1.4.0", true},  // differs
		{"", "1.4.0", true},       // reports nothing, expected set
		{"1.4.0", "", false},      // no expectation declared
		{"", "", false},           // neither
	}
	for _, c := range cases {
		if got := versionMismatch(c.reported, c.expected); got != c.want {
			t.Errorf("versionMismatch(%q, %q) = %v, want %v", c.reported, c.expected, got, c.want)
		}
	}
}

// TestDescribeErrorReply: `aether call` shows a refusal with its code, any other error as
// "<type>: <message>", and does not crash on an error reply without an error object.
func TestDescribeErrorReply(t *testing.T) {
	cases := []struct {
		err  *wire.WireError
		want string
	}{
		{&wire.WireError{Type: wire.ErrRefused, Code: "insufficient_funds", Message: "the balance is 3"}, "refused (insufficient_funds): the balance is 3"},
		{&wire.WireError{Type: wire.ErrHandler, Message: "disk on fire"}, "handler_error: disk on fire"},
		{nil, "error reply without detail"},
	}
	for _, tc := range cases {
		if got := describeErrorReply(tc.err); got != tc.want {
			t.Errorf("describeErrorReply(%+v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

// fleetFixture builds two nodes: a live one with two thralls (one with a rollout mismatch and a
// last error) and a stale one with a single down thrall reporting no version. The order inside a
// node is deliberately not alphabetical, so the tests also pin printFleet's sorting.
func fleetFixture(now int64) []fleet.NodeView {
	return []fleet.NodeView{
		{
			Health: fleet.Health{
				App:        "demo",
				LordID:     "hub-1",
				IntervalMs: 5000,
				Thralls: []fleet.ThrallHealth{
					{Name: "zeta", Status: "ready", Version: "1.0.0", LastError: "boom"},
					{
						Name: "alpha", Status: "ready", Version: "1.3.0", ExpectedVersion: "1.4.0",
						CallOps: []string{"get"}, CastOps: []string{"dec", "inc"},
					},
				},
			},
			LastSeen: now,
		},
		{
			Health: fleet.Health{
				App:        "demo",
				LordID:     "site-2",
				IntervalMs: 5000,
				Thralls:    []fleet.ThrallHealth{{Name: "only", Status: "down"}},
			},
			Stale:    true,
			LastSeen: now,
		},
	}
}

// TestPrintFleetFormat pins the one-row-per-node layout: a header, a node line with ready/total,
// and one indented line per thrall with its status, version (dash when absent), rollout mismatch
// and a last-error marker.
func TestPrintFleetFormat(t *testing.T) {
	var buf bytes.Buffer
	printFleet(&buf, fleetFixture(time.Now().UnixMilli()))
	got := buf.String()

	for _, want := range []string{
		"APP",
		"LAST",
		"hub-1",
		"site-2",
		"2 (2 ready)",
		"1 (0 ready)",
		"STALE",
		"! version mismatch (want 1.4.0)",
		"! boom",
		"call(get) cast(dec, inc)",
		"1.0.0  ! boom  -", // a thrall declaring no ops reads as a dash, not a gap
	} {
		if !strings.Contains(got, want) {
			t.Errorf("fleet output missing %q\n---\n%s", want, got)
		}
	}

	// Thralls are sorted by name inside a node, so a shifted fixture order stays stable. Both names
	// must be present first: comparing -1 against a real index would prove nothing.
	ai, zi := strings.Index(got, "alpha"), strings.Index(got, "zeta")
	if ai < 0 || zi < 0 {
		t.Fatalf("expected both fixture thralls in the output (alpha=%d, zeta=%d)\n---\n%s", ai, zi, got)
	}
	if ai > zi {
		t.Errorf("thralls not sorted by name:\n%s", got)
	}

	// Header + one line per node + one line per thrall; no blank or duplicated lines.
	if n := len(strings.Split(strings.TrimRight(got, "\n"), "\n")); n != 6 {
		t.Errorf("got %d lines, want 6 (header + 2 nodes + 3 thralls)\n---\n%s", n, got)
	}

	// A thrall that reports no version reads as a dash, never as an empty column.
	for _, line := range strings.Split(got, "\n") {
		if strings.Contains(line, "only") && !strings.HasSuffix(strings.TrimRight(line, " "), "-") {
			t.Errorf("thrall without a version should end its line with a dash: %q", line)
		}
	}
}

// TestPrintFleetEmpty: with nothing on the bus the command says so explicitly instead of printing
// a bare header.
func TestPrintFleetEmpty(t *testing.T) {
	var buf bytes.Buffer
	printFleet(&buf, nil)
	got := buf.String()
	if !strings.Contains(got, "no lords publishing fleet health") {
		t.Errorf("empty fleet message missing:\n%s", got)
	}
	if strings.Contains(got, "APP") {
		t.Errorf("empty fleet should print no header:\n%s", got)
	}
}

// TestFormatVerbs pins the rendered shape of a thrall's declared ops: describe's names, the thrall's
// own order, the ", " separator, a dash when nothing is declared, whole-token summarising past
// maxShownOps, and a rune cap on very long names.
func TestFormatVerbs(t *testing.T) {
	cases := []struct {
		name string
		call []string
		cast []string
		want string
	}{
		{"none", nil, nil, "-"},
		{"empty slices", []string{}, []string{}, "-"},
		{"call only", []string{"get"}, nil, "call(get)"},
		{"cast only", nil, []string{"dec", "inc"}, "cast(dec, inc)"},
		{"both", []string{"get"}, []string{"dec", "inc"}, "call(get) cast(dec, inc)"},
		{"order preserved", []string{"b", "a"}, nil, "call(b, a)"},
		{"empty names skipped", []string{"", ""}, nil, "-"},
		{"mixed empty names", []string{"", "get"}, nil, "call(get)"},
	}
	for _, c := range cases {
		if got := formatVerbs(c.call, c.cast); got != c.want {
			t.Errorf("%s: formatVerbs(%v, %v) = %q, want %q", c.name, c.call, c.cast, got, c.want)
		}
	}

	// A long list is summarised on whole-token boundaries, so a long call list cannot swallow the
	// cast list behind a blind cut.
	ops := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		ops = append(ops, "op"+strconv.Itoa(i))
	}
	if got, want := formatVerbs(ops, []string{"zap"}), "call(op0, op1, op2, op3, +16) cast(zap)"; got != want {
		t.Errorf("long call list = %q, want %q", got, want)
	}

	// Very long single names still hit the rune cap.
	long := formatVerbs([]string{strings.Repeat("x", 200)}, nil)
	if n := len([]rune(long)); n > fleetVerbsLimit {
		t.Errorf("long verbs not capped: %d runes, want <= %d", n, fleetVerbsLimit)
	}
	if !strings.HasSuffix(long, "...") {
		t.Errorf("capped verbs should end with an ellipsis: %q", long)
	}
}

// TestSanitizePrintable: control characters never reach the terminal, and a replaced rune keeps the
// column width so a padded table stays aligned.
func TestSanitizePrintable(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", "plain"},
		{"zizkov", "zizkov"},
		{"a\x1b[31mred", "a?[31mred"},
		{"line\r\nbreak", "line??break"},
		{"nul\x00", "nul?"},
		{"del\x7f", "del?"},
	}
	for _, c := range cases {
		if got := sanitizePrintable(c.in); got != c.want {
			t.Errorf("sanitizePrintable(%q) = %q, want %q", c.in, got, c.want)
		}
	}

	in := "a\x1b[31m"
	if a, b := len([]rune(in)), len([]rune(sanitizePrintable(in))); a != b {
		t.Errorf("rune count changed: %d -> %d", a, b)
	}
}

// TestPrintFleetSanitizesRemoteStrings: thrall names, op names and error text arrive from remote
// thralls, so a control character there must not reach the operator's terminal.
func TestPrintFleetSanitizesRemoteStrings(t *testing.T) {
	nodes := []fleet.NodeView{{
		Health: fleet.Health{
			App: "demo", LordID: "hub-1", IntervalMs: 5000,
			Thralls: []fleet.ThrallHealth{{
				Name: "evil", Status: "ready", Version: "1.0.0",
				CallOps: []string{"\x1b[2J"}, LastError: "bad\rthing",
			}},
		},
		LastSeen: time.Now().UnixMilli(),
	}}

	var buf bytes.Buffer
	printFleet(&buf, nodes)
	got := buf.String()

	if strings.ContainsAny(got, "\x1b\r") {
		t.Errorf("control characters reached the output: %q", got)
	}
	if !strings.Contains(got, "call(?[2J)") {
		t.Errorf("control characters should be replaced by '?':\n%s", got)
	}
	if n := len(strings.Split(strings.TrimRight(got, "\n"), "\n")); n != 3 {
		t.Errorf("control characters changed the line count: got %d, want 3 (header + node + thrall)\n---\n%s", n, got)
	}
}
