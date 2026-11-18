//go:build !windows

package thrall

// Cross-language round trips: the same refusal and the same deadline drop, with every pairing of
// the Go, TypeScript and Python SDKs on the two sides of a call. The Go peer runs in this process;
// the TS and Python peers (testdata/crosslang) run as real processes on the same embedded ether.
// A peer is put on the calling side through its "relay" op, which makes the call itself and reports
// what its SDK surfaced.
//
// The test needs bun (with the workspace installed) and a python that can import nats; without
// them it is skipped, unless AETHER_CROSSLANG=require - which the cross-language CI job sets, so a
// missing tool there is a failure rather than a silently skipped proof.

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/wire"
)

const (
	crossLangApp     = "xl"
	crossLangTimeout = 5 * time.Second
)

// crossLangTools locates what the foreign peers need, or skips (fails, when required) the test.
func crossLangTools(t *testing.T) (bun string, python []string, peers string) {
	t.Helper()
	missing := func(what string) {
		if os.Getenv("AETHER_CROSSLANG") == "require" {
			t.Fatalf("cross-language test required but %s", what)
		}
		t.Skipf("cross-language test skipped: %s", what)
	}

	repo, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	peers = filepath.Join(repo, "testdata", "crosslang")

	bun, err = exec.LookPath("bun")
	if err != nil {
		missing("bun is not on PATH")
	}
	if _, err := os.Stat(filepath.Join(repo, "node_modules", "nats")); err != nil {
		if _, err := os.Stat(filepath.Join(repo, "sdk", "ts", "node_modules", "nats")); err != nil {
			missing("the TS workspace is not installed (run `bun install`)")
		}
	}

	python = pythonWithNATS()
	if python == nil {
		missing("no python that can import nats (pip install nats-py, or install uv)")
	}
	return bun, python, peers
}

// pythonWithNATS returns the command that runs a python able to import the nats client:
// AETHER_TEST_PYTHON when set, a python on PATH that has it, or uv providing it on the fly.
func pythonWithNATS() []string {
	if v := strings.Fields(os.Getenv("AETHER_TEST_PYTHON")); len(v) > 0 {
		return v
	}
	for _, bin := range []string{"python3", "python"} {
		if exec.Command(bin, "-c", "import nats").Run() == nil {
			return []string{bin}
		}
	}
	if _, err := exec.LookPath("uv"); err == nil {
		return []string{"uv", "run", "--quiet", "--with", "nats-py", "python"}
	}
	return nil
}

// syncBuffer collects a peer process's stderr for the failure message.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startForeignPeer runs a TS or Python peer as the lord would - identity through the environment -
// and returns once it answers on its call subject. It is drained on cleanup, and its process group
// is killed if it does not exit.
func startForeignPeer(t *testing.T, eth *ether.Ether, name string, command ...string) {
	t.Helper()
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Env = append(os.Environ(),
		"AETHER_NATS_URL="+eth.URL(), "AETHER_APP="+crossLangApp, "AETHER_NAME="+name, "AETHER_LOG_LEVEL=error")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // so a wrapper (uv) and its child die together
	stderr := &syncBuffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start peer %s: %v", name, err)
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	t.Cleanup(func() {
		drain, _ := json.Marshal(wire.Envelope{V: 1, Kind: wire.KindCtl, Op: "drain"})
		_ = eth.Conn().Publish(wire.Ctl(name), drain)
		_ = eth.Conn().Flush()
		select {
		case <-exited:
		case <-time.After(3 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-exited
		}
	})

	probe := NewClient(eth.Conn(), crossLangApp)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := probe.Call(name, "balance", nil, 300*time.Millisecond); err == nil {
			return
		}
		select {
		case <-exited:
			t.Fatalf("peer %s exited before it became ready:\n%s", name, stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer %s did not become ready:\n%s", name, stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

type peerState struct {
	Balance int
	Writes  int
}

// goPeerDef is the Go flavour of the cross-language peer: the same operations and the same words
// as testdata/crosslang/peer.ts and peer.py (it needs no relay - the test itself is the Go caller).
func goPeerDef() Def[peerState] {
	return Def[peerState]{
		Init: func(*Ctx) (peerState, error) { return peerState{Balance: 3}, nil },
		HandleCall: map[string]CallFn[peerState]{
			"balance": func(_ json.RawMessage, s peerState, _ *Ctx) (any, peerState, error) { return s.Balance, s, nil },
			"withdraw": func(p json.RawMessage, s peerState, _ *Ctx) (any, peerState, error) {
				var req withdrawRequest
				if err := json.Unmarshal(p, &req); err != nil {
					return nil, s, err
				}
				if req.Amount > s.Balance {
					return nil, s, Refusef("insufficient_funds", "the balance is %d, the withdrawal %d", s.Balance, req.Amount)
				}
				s.Balance -= req.Amount
				return s.Balance, s, nil
			},
			"broken": func(_ json.RawMessage, s peerState, _ *Ctx) (any, peerState, error) {
				return nil, s, errors.New("disk on fire")
			},
			"write": func(_ json.RawMessage, s peerState, _ *Ctx) (any, peerState, error) {
				s.Writes++
				return s.Writes, s, nil
			},
			"writes": func(_ json.RawMessage, s peerState, _ *Ctx) (any, peerState, error) { return s.Writes, s, nil },
		},
	}
}

// relayRequest asks a foreign peer to make a call itself; relayReply is what its SDK surfaced.
type relayRequest struct {
	App       string `json:"app"`
	Target    string `json:"target"`
	Op        string `json:"op"`
	Payload   any    `json:"payload,omitempty"`
	TimeoutMs int    `json:"timeout_ms,omitempty"`
}

type relayReply struct {
	OK        bool            `json:"ok"`
	Payload   json.RawMessage `json:"payload"`
	Typed     bool            `json:"typed"` // the SDK raised its typed call error
	Type      string          `json:"type"`
	Code      string          `json:"code"`
	Message   string          `json:"message"`
	Retryable bool            `json:"retryable"`
	Text      string          `json:"text"`
}

// callOutcome is one call as its caller's SDK saw it, whichever language the caller is.
type callOutcome struct {
	ok        bool
	typed     bool
	errType   string
	code      string
	message   string
	retryable bool
	text      string
}

// callFrom makes the call from the given caller: directly for Go, through the peer's relay
// otherwise.
func callFrom(t *testing.T, client *Client, caller, target, op string, payload any) callOutcome {
	t.Helper()
	if caller == "go" {
		_, err := client.Call(target, op, payload, crossLangTimeout)
		if err == nil {
			return callOutcome{ok: true}
		}
		out := callOutcome{text: err.Error()}
		var ce *CallError
		if errors.As(err, &ce) {
			out.typed, out.errType, out.code, out.message, out.retryable = true, ce.Type, ce.Code, ce.Message, ce.Retryable
		}
		return out
	}
	reply, err := CallInto[relayReply](client, caller, "relay",
		relayRequest{App: crossLangApp, Target: target, Op: op, Payload: payload, TimeoutMs: 3000}, crossLangTimeout)
	if err != nil {
		t.Fatalf("relay through %s to %s/%s: %v", caller, target, op, err)
	}
	return callOutcome{ok: reply.OK, typed: reply.Typed, errType: reply.Type, code: reply.Code,
		message: reply.Message, retryable: reply.Retryable, text: reply.Text}
}

func TestCrossLanguageRefusalAndDeadline(t *testing.T) {
	bun, python, peers := crossLangTools(t)
	eth := dispatchEther(t)
	nc := eth.Conn()
	client := NewClient(nc, crossLangApp)

	runThrall(t, eth, crossLangApp, "go", func() error { return Start(goPeerDef()) })
	startForeignPeer(t, eth, "ts", bun, filepath.Join(peers, "peer.ts"))
	startForeignPeer(t, eth, "py", append(python, filepath.Join(peers, "peer.py"))...)
	languages := []string{"go", "ts", "py"}
	// A foreign peer cannot be its own callee: while it handles the relay its mailbox is busy, so
	// the call it makes to itself would only be reached after timing out. TS-to-TS and
	// Python-to-Python are covered by each SDK's own end-to-end test instead.
	selfCall := func(caller, callee string) bool { return caller == callee && caller != "go" }

	// A refusal made by any SDK reads as a typed refusal in any SDK: same type, code and message,
	// and the same "<type>: <message>" text a plain error handler would see.
	for _, callee := range languages {
		for _, caller := range languages {
			if selfCall(caller, callee) {
				continue
			}
			t.Run("refusal "+caller+" calls "+callee, func(t *testing.T) {
				got := callFrom(t, client, caller, callee, "withdraw", withdrawRequest{Amount: 5})
				want := callOutcome{typed: true, errType: wire.ErrRefused, code: "insufficient_funds",
					message: "the balance is 3, the withdrawal 5", text: "refused: the balance is 3, the withdrawal 5"}
				if got != want {
					t.Fatalf("refusal as seen by %s:\n got %+v\nwant %+v", caller, got, want)
				}
			})
		}
	}

	// The control: an ordinary handler failure is a typed error in every SDK, but never a refusal.
	for _, callee := range languages {
		for _, caller := range languages {
			if selfCall(caller, callee) {
				continue
			}
			t.Run("failure "+caller+" calls "+callee, func(t *testing.T) {
				got := callFrom(t, client, caller, callee, "broken", nil)
				if got.ok || !got.typed || got.errType != wire.ErrHandler || got.code != "" {
					t.Fatalf("handler failure as seen by %s: %+v", caller, got)
				}
				if !strings.Contains(got.message, "disk on fire") {
					t.Fatalf("failure message = %q", got.message)
				}
			})
		}
	}

	// Refusals and failures left every peer's state alone.
	for _, peer := range languages {
		if got, err := CallInto[int](client, peer, "balance", nil, crossLangTimeout); err != nil || got != 3 {
			t.Fatalf("%s balance after the refusals = %d, %v; want 3", peer, got, err)
		}
	}

	// Every SDK's caller stamps the deadline the same way: its send time plus its timeout.
	seen := stubThrall(t, nc, crossLangApp, "probe", func(req Envelope) Envelope { return OKReply(req, "seen") })
	for _, caller := range languages {
		t.Run("deadline stamped by "+caller, func(t *testing.T) {
			var timeout int64 = crossLangTimeout.Milliseconds()
			if caller != "go" {
				timeout = 3000 // the relay's timeout_ms
			}
			if got := callFrom(t, client, caller, "probe", "ping", nil); !got.ok {
				t.Fatalf("probe call from %s failed: %+v", caller, got)
			}
			req := nextRequest(t, seen)
			if req.Deadline == 0 || req.Deadline != req.TS+timeout {
				t.Fatalf("%s stamped deadline %d with ts %d, want ts + %d", caller, req.Deadline, req.TS, timeout)
			}
			if req.Trace == "" {
				t.Fatalf("%s sent a call without a trace", caller)
			}
		})
	}

	// Every SDK's thrall drops a call that arrives past its deadline: it answers with the same
	// retryable error and does not run the handler. An envelope without a deadline still runs.
	for _, callee := range languages {
		t.Run("deadline honoured by "+callee, func(t *testing.T) {
			write := func(deadline int64) wire.Envelope {
				return rawCall(t, nc, crossLangApp, wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: callee,
					Op: "write", Payload: json.RawMessage(`{}`), TS: time.Now().UnixMilli(), Deadline: deadline})
			}
			expired := write(time.Now().Add(-5 * time.Second).UnixMilli())
			if expired.Status != wire.StatusError || expired.Error == nil ||
				expired.Error.Type != wire.ErrDeadlineExceeded || !expired.Error.Retryable {
				t.Fatalf("an expired call to %s = %+v (error %+v)", callee, expired, expired.Error)
			}
			if got, err := CallInto[int](client, callee, "writes", nil, crossLangTimeout); err != nil || got != 0 {
				t.Fatalf("%s ran an expired call: writes = %d, %v", callee, got, err)
			}
			if live := write(0); live.Status != wire.StatusOK {
				t.Fatalf("a call without a deadline to %s = %+v", callee, live.Error)
			}
			if got, err := CallInto[int](client, callee, "writes", nil, crossLangTimeout); err != nil || got != 1 {
				t.Fatalf("%s writes = %d, %v; want 1", callee, got, err)
			}
		})
	}
}
