package wire

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "regenerate golden fixtures in testdata/wire")

const goldenDir = "../../testdata/wire"

func goldenPath(name string) string { return filepath.Join(goldenDir, name+".json") }

// goldenCases are the canonical envelopes shared with the TS and Python parity tests.
// Go is the source of truth: `go test ./internal/wire -run TestEnvelopeGolden -update`
// regenerates the JSON fixtures that sdk/ts and sdk/python assert against. The set covers
// every Kind, the omitempty fields (minimal) and WireError (reply_error).
func goldenCases() map[string]Envelope {
	return map[string]Envelope{
		"call": {V: 1, ID: "c-1", Kind: KindCall, To: "counter", Op: "get",
			Payload: json.RawMessage(`{"n":1}`), TS: 1700000000000},
		"call_traced": {V: 1, ID: "c-9", Trace: "t-abc", Kind: KindCall, To: "counter", Op: "get",
			Payload: json.RawMessage(`{"n":1}`), TS: 1700000000004},
		"call_idem": {V: 1, ID: "c-10", Idem: "withdraw-42", Kind: KindCall, To: "account", Op: "withdraw",
			Payload: json.RawMessage(`{"amt":5}`), TS: 1700000000005},
		"cast": {V: 1, ID: "c-2", Kind: KindCast, To: "counter", Op: "inc",
			Payload: json.RawMessage(`{}`), TS: 1700000000001},
		"reply_ok": {V: 1, ID: "c-1", Kind: KindReply, Status: "ok",
			Payload: json.RawMessage(`{"value":42}`)},
		"reply_error": {V: 1, ID: "c-1", Kind: KindReply, Status: "error",
			Error: &WireError{Type: "unknown_op", Message: "unknown call op: nope", Retryable: false}},
		"hb": {V: 1, Kind: KindHB, To: "counter", TS: 1700000000002},
		"hb_metrics": {V: 1, Kind: KindHB, To: "counter", TS: 1700000000003,
			Payload: mustHeartbeatMetrics(HeartbeatMetrics{MailboxDepth: 2, MailboxLatencyMs: 1.5, ProcessedTotal: 10})},
		"hb_describe": {V: 1, Kind: KindHB, To: "counter", TS: 1700000000006,
			Payload: mustHeartbeatMetrics(HeartbeatMetrics{ProcessedTotal: 3, Describe: &ThrallDescribe{
				CallOps: []string{"get", "value"}, CastOps: []string{"inc", "reset"}, Version: "1.2.0",
				LastError: "handler_error: boom", LastErrorMs: 1700000000005}})},
		"ctl":     {V: 1, Kind: KindCtl, Op: "drain"},
		"minimal": {V: 1, Kind: KindCall},
		// A call carrying the instant its caller stops waiting (ts + a 5 s timeout).
		"call_deadline": {V: 1, ID: "c-11", Trace: "t-abc", Kind: KindCall, To: "account", Op: "withdraw",
			Payload: json.RawMessage(`{"amt":5}`), TS: 1700000000007, Deadline: 1700000005007},
		// A refusal: its own error type plus the refusing thrall's stable code.
		"reply_refused": {V: 1, ID: "c-11", Kind: KindReply, Status: StatusError,
			Error: &WireError{Type: ErrRefused, Code: "insufficient_funds", Message: "the balance is 3, the withdrawal 5"}},
		// The reply to a call dropped unexecuted because its deadline had passed.
		"reply_deadline_exceeded": {V: 1, ID: "c-11", Kind: KindReply, Status: StatusError,
			Error: &WireError{Type: ErrDeadlineExceeded, Message: "call expired 1200 ms before it could be handled", Retryable: true}},
		"hb_expired": {V: 1, Kind: KindHB, To: "account", TS: 1700000000008,
			Payload: mustHeartbeatMetrics(HeartbeatMetrics{MailboxDepth: 1, MailboxLatencyMs: 0.5, ProcessedTotal: 7, ExpiredTotal: 2})},
	}
}

// mustHeartbeatMetrics marshals HeartbeatMetrics into the canonical payload bytes. Go is the
// source of truth, so this fixes the byte order the TS and Python SDKs must reproduce.
func mustHeartbeatMetrics(hm HeartbeatMetrics) json.RawMessage {
	b, err := json.Marshal(hm)
	if err != nil {
		panic(err)
	}
	return b
}

// TestEnvelopeGolden pins the canonical wire form of each case. With -update it (re)writes
// the golden fixtures; otherwise it fails on any drift between the Go marshaller and the
// committed fixtures.
func TestEnvelopeGolden(t *testing.T) {
	for name, env := range goldenCases() {
		data, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("%s: marshal: %v", name, err)
		}
		path := goldenPath(name)
		if *update {
			if err := os.MkdirAll(goldenDir, 0o755); err != nil {
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
			t.Errorf("%s: envelope drift\n got: %s\nwant: %s", name, got, strings.TrimSpace(string(want)))
		}
	}
}

// TestHeartbeatDescribeOmitEmpty guards the backward-compatible shape of the heartbeat payload:
// a metrics-only heartbeat (older SDK, or one before a failure) must not carry a "describe" key,
// and a describe must drop its own empty fields so an idle thrall reports no last_error.
func TestHeartbeatDescribeOmitEmpty(t *testing.T) {
	metricsOnly, err := json.Marshal(HeartbeatMetrics{MailboxDepth: 1, ProcessedTotal: 5})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(metricsOnly), "describe") {
		t.Errorf("metrics-only heartbeat unexpectedly contains describe: %s", metricsOnly)
	}

	noError, err := json.Marshal(HeartbeatMetrics{Describe: &ThrallDescribe{CallOps: []string{"get"}, Version: "1.0.0"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"cast_ops", "last_error", "last_error_ms"} {
		if strings.Contains(string(noError), field) {
			t.Errorf("describe without %s unexpectedly contains it: %s", field, noError)
		}
	}
}

// TestEnvelopeRoundTrip proves the canonical form is a fixed point: unmarshalling a golden
// fixture and marshalling it back yields the exact same bytes.
func TestEnvelopeRoundTrip(t *testing.T) {
	for name := range goldenCases() {
		raw, err := os.ReadFile(goldenPath(name))
		if err != nil {
			t.Fatalf("%s: read golden: %v", name, err)
		}
		var env Envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatalf("%s: unmarshal: %v", name, err)
		}
		out, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("%s: marshal: %v", name, err)
		}
		if string(out) != strings.TrimSpace(string(raw)) {
			t.Errorf("%s: round-trip drift\n got: %s\nwant: %s", name, out, strings.TrimSpace(string(raw)))
		}
	}
}

// TestOmitEmpty guards that empty optional fields are absent from the wire form - a change
// that dropped an omitempty tag would bloat every message and break byte-level expectations.
func TestOmitEmpty(t *testing.T) {
	data, err := json.Marshal(Envelope{V: 1, Kind: KindCall})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != `{"v":1,"kind":"call"}` {
		t.Errorf("minimal envelope: got %s, want {\"v\":1,\"kind\":\"call\"}", got)
	}
	for _, field := range []string{"id", "trace", "from", "to", "op", "payload", "status", "error", "ts", "deadline"} {
		if strings.Contains(string(data), `"`+field+`"`) {
			t.Errorf("minimal envelope unexpectedly contains %q: %s", field, data)
		}
	}
}

// TestAdditiveFieldsKeepOldShapes guards the compatibility promise of the fields added after v1
// shipped: an error that is not a refusal carries no "code", and a heartbeat of a thrall that never
// dropped a call carries no "expired_total" - so a peer that predates them sees the bytes it always
// saw.
func TestAdditiveFieldsKeepOldShapes(t *testing.T) {
	plainError, err := json.Marshal(WireError{Type: ErrHandler, Message: "boom"})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(plainError); got != `{"type":"handler_error","message":"boom","retryable":false}` {
		t.Errorf("non-refusal error: got %s", got)
	}
	metrics, err := json.Marshal(HeartbeatMetrics{MailboxDepth: 1, ProcessedTotal: 5})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(metrics), "expired_total") {
		t.Errorf("heartbeat without a dropped call unexpectedly contains expired_total: %s", metrics)
	}
}

// TestOldShapesStillDecode proves the other direction: envelopes written before the fields existed
// decode into the zero value of the new fields, which every consumer treats as "absent".
func TestOldShapesStillDecode(t *testing.T) {
	var call Envelope
	if err := json.Unmarshal([]byte(`{"v":1,"id":"c-1","kind":"call","to":"counter","op":"get","payload":{},"ts":1700000000000}`), &call); err != nil {
		t.Fatal(err)
	}
	if call.Deadline != 0 {
		t.Errorf("deadline of an old call = %d, want 0", call.Deadline)
	}
	if _, expired := call.Expired(time.UnixMilli(1900000000000), 0); expired {
		t.Error("a call without a deadline must never expire")
	}

	var reply Envelope
	if err := json.Unmarshal([]byte(`{"v":1,"id":"c-1","kind":"reply","status":"error","error":{"type":"handler_error","message":"boom","retryable":false}}`), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Error == nil || reply.Error.Code != "" || reply.Error.Type != ErrHandler {
		t.Errorf("old error reply decoded as %+v", reply.Error)
	}
}

func TestDeadlineAfter(t *testing.T) {
	now := time.UnixMilli(1700000000000)
	if got := DeadlineAfter(now, 5*time.Second); got != 1700000005000 {
		t.Errorf("DeadlineAfter(5s) = %d, want 1700000005000", got)
	}
	for _, timeout := range []time.Duration{0, -time.Second} {
		if got := DeadlineAfter(now, timeout); got != 0 {
			t.Errorf("DeadlineAfter(%v) = %d, want 0 (no deadline)", timeout, got)
		}
	}
}

func TestExpired(t *testing.T) {
	const deadline = 1700000005000
	const skew = 250 * time.Millisecond
	e := Envelope{V: 1, Kind: KindCall, Deadline: deadline}
	cases := []struct {
		name        string
		nowMs       int64
		skew        time.Duration
		wantExpired bool
		wantOverdue time.Duration
	}{
		{"before the deadline", deadline - 1, skew, false, -time.Millisecond},
		{"at the deadline", deadline, skew, false, 0},
		{"within the tolerance", deadline + 250, skew, false, 250 * time.Millisecond},
		{"past the tolerance", deadline + 251, skew, true, 251 * time.Millisecond},
		{"exact judgement, at the deadline", deadline, 0, false, 0},
		{"exact judgement, one ms late", deadline + 1, 0, true, time.Millisecond},
	}
	for _, tc := range cases {
		overdue, expired := e.Expired(time.UnixMilli(tc.nowMs), tc.skew)
		if expired != tc.wantExpired || overdue != tc.wantOverdue {
			t.Errorf("%s: Expired = (%v, %v), want (%v, %v)", tc.name, overdue, expired, tc.wantOverdue, tc.wantExpired)
		}
	}
}
