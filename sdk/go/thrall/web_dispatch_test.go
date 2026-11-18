package thrall

// End-to-end tests of a web thrall's two dispatch modes: Serve (up to Concurrency requests at
// once) and Handle (serialized, threading its state). Like the other dispatch tests they run the
// real StartWeb against an embedded ether and call it with the production Client, so what is under
// test is the whole path from the subscription to the reply.

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/obs"
	"github.com/hamicek/aether/internal/wire"
)

// gate is the test's handle on requests parked inside a web handler: a request to /slow/<id>
// reports that it is running and then waits until the test lets it go.
type gate struct {
	mu      sync.Mutex
	entered map[string]chan struct{}
	release map[string]chan struct{}
}

func newGate() *gate {
	return &gate{entered: map[string]chan struct{}{}, release: map[string]chan struct{}{}}
}

func (g *gate) channels(id string) (entered, release chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.entered[id] == nil {
		g.entered[id], g.release[id] = make(chan struct{}), make(chan struct{})
	}
	return g.entered[id], g.release[id]
}

// park is called by the handler: it announces the request and blocks until let go.
func (g *gate) park(id string) {
	entered, release := g.channels(id)
	close(entered)
	<-release
}

// awaitEntered waits until the request is inside its handler.
func (g *gate) awaitEntered(t *testing.T, id string) {
	t.Helper()
	entered, _ := g.channels(id)
	select {
	case <-entered:
	case <-time.After(clientTimeout):
		t.Fatalf("request %s never reached its handler", id)
	}
}

// running reports whether the request has reached its handler yet.
func (g *gate) running(id string) bool {
	entered, _ := g.channels(id)
	select {
	case <-entered:
		return true
	default:
		return false
	}
}

func (g *gate) letGo(id string) {
	_, release := g.channels(id)
	close(release)
}

// site is the shared value of the Serve thralls under test: built once by Init and handed to
// every request. served is safe for concurrent use, which is what a Serve handler owes its state.
type site struct {
	gate   *gate
	served atomic.Int32
}

// serveSite answers /slow/<id> only once the test lets that request go, and everything else at
// once. The body names the request's trace, so a test can tell whose Ctx a request was given.
func serveSite(req HTTPRequest, s *site, ctx *Ctx) (HTTPResponse, error) {
	if id, slow := strings.CutPrefix(req.Path, "/slow/"); slow {
		s.gate.park(id)
	}
	s.served.Add(1)
	return TextResponse(200, req.Path+" trace="+ctx.Trace), nil
}

func serveDef(s *site, concurrency int) WebDef[*site] {
	return WebDef[*site]{
		Init:        func(*Ctx) (*site, error) { return s, nil },
		Serve:       serveSite,
		Concurrency: concurrency,
	}
}

// get sends one http_request for path in the background and returns the channel its outcome
// arrives on.
type outcome struct {
	resp HTTPResponse
	err  error
}

func get(client *Client, target, path string, timeout time.Duration, opts ...SendOption) <-chan outcome {
	done := make(chan outcome, 1)
	go func() {
		resp, err := CallInto[HTTPResponse](client, target, OpHTTPRequest, HTTPRequest{Method: "GET", Path: path}, timeout, opts...)
		done <- outcome{resp, err}
	}()
	return done
}

func awaitOutcome(t *testing.T, what string, done <-chan outcome) outcome {
	t.Helper()
	select {
	case o := <-done:
		return o
	case <-time.After(clientTimeout):
		t.Fatalf("%s: no answer", what)
		return outcome{}
	}
}

func mustOK(t *testing.T, what string, o outcome) HTTPResponse {
	t.Helper()
	if o.err != nil || o.resp.Status != 200 {
		t.Fatalf("%s: status %d, err %v", what, o.resp.Status, o.err)
	}
	return o.resp
}

// TestWebServeSlowRequestDoesNotBlockFastOne is the reason concurrent dispatch exists: while one
// request is stuck in its handler, another one on the same web thrall is answered.
func TestWebServeSlowRequestDoesNotBlockFastOne(t *testing.T) {
	eth := dispatchEther(t)
	s := &site{gate: newGate()}
	runThrall(t, eth, "ae", "web", func() error { return StartWeb(serveDef(s, 4)) })
	client := NewClient(eth.Conn(), "ae")

	slow := get(client, "web", "/slow/a", clientTimeout)
	s.gate.awaitEntered(t, "a")

	fast := mustOK(t, "the fast request", awaitOutcome(t, "the fast request", get(client, "web", "/fast", clientTimeout)))
	if !strings.HasPrefix(string(fast.Body), "/fast ") {
		t.Errorf("the fast request got someone else's answer: %q", fast.Body)
	}
	select {
	case o := <-slow:
		t.Fatalf("the slow request finished before it was let go: %+v", o)
	default:
	}

	s.gate.letGo("a")
	mustOK(t, "the slow request", awaitOutcome(t, "the slow request", slow))
	if got := s.served.Load(); got != 2 {
		t.Errorf("every request saw the same shared value: served = %d, want 2", got)
	}
}

// TestWebServeGivesEachRequestItsOwnContext: two requests that are in their handlers at the same
// time each read their own trace from ctx, not whichever was set last.
func TestWebServeGivesEachRequestItsOwnContext(t *testing.T) {
	eth := dispatchEther(t)
	s := &site{gate: newGate()}
	runThrall(t, eth, "ae", "web", func() error { return StartWeb(serveDef(s, 4)) })
	client := NewClient(eth.Conn(), "ae")

	first := get(client, "web", "/slow/a", clientTimeout, WithTrace("trace-a"))
	second := get(client, "web", "/slow/b", clientTimeout, WithTrace("trace-b"))
	s.gate.awaitEntered(t, "a")
	s.gate.awaitEntered(t, "b")
	s.gate.letGo("a")
	s.gate.letGo("b")

	if body := string(mustOK(t, "request a", awaitOutcome(t, "request a", first)).Body); body != "/slow/a trace=trace-a" {
		t.Errorf("request a = %q", body)
	}
	if body := string(mustOK(t, "request b", awaitOutcome(t, "request b", second)).Body); body != "/slow/b trace=trace-b" {
		t.Errorf("request b = %q", body)
	}
}

// TestWebSerializedModesServeOneRequestAtATime: Serve at Concurrency 1 and Handle are both the
// mailbox - a request waits until the one before it has returned, and Handle threads its state.
func TestWebSerializedModesServeOneRequestAtATime(t *testing.T) {
	type hits struct{ n int }
	g := newGate()
	defs := map[string]func() error{
		"serve at concurrency 1": func() error {
			return StartWeb(serveDef(&site{gate: g}, 1))
		},
		"handle": func() error {
			return StartWeb(WebDef[hits]{
				Handle: func(req HTTPRequest, s hits, _ *Ctx) (HTTPResponse, hits, error) {
					if id, slow := strings.CutPrefix(req.Path, "/slow/"); slow {
						g.park(id)
					}
					s.n++
					return TextResponse(200, strconv.Itoa(s.n)), s, nil
				},
			})
		},
	}
	for name, start := range defs {
		t.Run(name, func(t *testing.T) {
			eth := dispatchEther(t)
			awaitBeat := heartbeats(t, eth.Conn(), "web")
			runThrall(t, eth, "ae", "web", start)
			client := NewClient(eth.Conn(), "ae")
			id := strings.ReplaceAll(name, " ", "-")

			slow := get(client, "web", "/slow/"+id, clientTimeout)
			g.awaitEntered(t, id)
			fast := get(client, "web", "/fast", clientTimeout)
			select {
			case o := <-fast:
				t.Fatalf("the second request was answered while the first held the mailbox: %+v", o)
			case <-time.After(300 * time.Millisecond):
			}
			beat := awaitBeat("one handler running", func(m wire.HeartbeatMetrics) bool { return m.InFlight == 1 })
			if beat.Describe == nil || beat.Describe.Concurrency != 0 {
				t.Errorf("a serialized web thrall must not declare a concurrency: %+v", beat.Describe)
			}

			g.letGo(id)
			first := mustOK(t, "the first request", awaitOutcome(t, "the first request", slow))
			second := mustOK(t, "the second request", awaitOutcome(t, "the second request", fast))
			if name == "handle" && (string(first.Body) != "1" || string(second.Body) != "2") {
				t.Errorf("Handle must thread its state in arrival order: got %q then %q", first.Body, second.Body)
			}
		})
	}
}

// TestWebServeQueuedRequestExpiresByItsDeadline: with every slot busy a further request waits,
// and when its caller's deadline passes while it is still waiting it is dropped then - not run,
// and not kept until a slot comes free.
func TestWebServeQueuedRequestExpiresByItsDeadline(t *testing.T) {
	t.Setenv(wire.EnvDeadlineSkewMs, "0") // caller and thrall share this process's clock
	eth := dispatchEther(t)
	nc := eth.Conn()
	awaitBeat := heartbeats(t, nc, "web")
	s := &site{gate: newGate()}
	runThrall(t, eth, "ae", "web", func() error { return StartWeb(serveDef(s, 2)) })
	client := NewClient(nc, "ae")

	a, b := get(client, "web", "/slow/a", clientTimeout), get(client, "web", "/slow/b", clientTimeout)
	s.gate.awaitEntered(t, "a")
	s.gate.awaitEntered(t, "b")

	// A third request whose caller waits 200 ms. It is sent by hand so that this test, unlike a
	// real caller, is still listening when the thrall answers that it dropped the call.
	payload, _ := json.Marshal(HTTPRequest{Method: "GET", Path: "/slow/c"})
	sent := time.Now()
	reply := rawCall(t, nc, "ae", wire.Envelope{V: 1, ID: nats.NewInbox(), Trace: "t-queued", Kind: wire.KindCall,
		To: "web", Op: OpHTTPRequest, Payload: payload, TS: sent.UnixMilli(), Deadline: sent.Add(200 * time.Millisecond).UnixMilli()})
	waited := time.Since(sent)

	if reply.Error == nil || reply.Error.Type != wire.ErrDeadlineExceeded || !reply.Error.Retryable {
		t.Fatalf("the queued request = %+v (error %+v), want a retryable %s", reply.Status, reply.Error, wire.ErrDeadlineExceeded)
	}
	if waited < 150*time.Millisecond || waited > 2*time.Second {
		t.Errorf("the queued request was dropped after %v, want at its 200 ms deadline", waited)
	}
	if s.gate.running("c") {
		t.Error("the expired request reached its handler")
	}
	awaitBeat("the drop counted and the queue empty again", func(m wire.HeartbeatMetrics) bool {
		return m.ExpiredTotal == 1 && m.InFlight == 2 && m.MailboxDepth == 2
	})

	// A request sent with time to spare waits for a slot and runs once one is free.
	patient := get(client, "web", "/fast", clientTimeout)
	awaitBeat("the patient request waiting", func(m wire.HeartbeatMetrics) bool { return m.MailboxDepth == 3 && m.InFlight == 2 })
	s.gate.letGo("a")
	mustOK(t, "the patient request", awaitOutcome(t, "the patient request", patient))
	s.gate.letGo("b")
	mustOK(t, "request a", awaitOutcome(t, "request a", a))
	mustOK(t, "request b", awaitOutcome(t, "request b", b))
	if s.gate.running("c") {
		t.Error("the expired request ran after a slot came free")
	}
}

// TestWebServeReportsInFlightAndQueuedWork: the heartbeat tells running requests from waiting
// ones, and the self-description says how many may run at once.
func TestWebServeReportsInFlightAndQueuedWork(t *testing.T) {
	eth := dispatchEther(t)
	awaitBeat := heartbeats(t, eth.Conn(), "web")
	s := &site{gate: newGate()}
	runThrall(t, eth, "ae", "web", func() error { return StartWeb(serveDef(s, 2)) })
	client := NewClient(eth.Conn(), "ae")

	idle := awaitBeat("an idle beat", func(m wire.HeartbeatMetrics) bool { return m.Describe != nil && m.MailboxDepth == 0 })
	if idle.InFlight != 0 || idle.Describe.Concurrency != 2 {
		t.Errorf("idle: in_flight %d, concurrency %d, want 0 and 2", idle.InFlight, idle.Describe.Concurrency)
	}
	if len(idle.Describe.CallOps) != 1 || idle.Describe.CallOps[0] != OpHTTPRequest {
		t.Errorf("call ops = %v, want [%s]", idle.Describe.CallOps, OpHTTPRequest)
	}

	requests := []<-chan outcome{
		get(client, "web", "/slow/a", clientTimeout),
		get(client, "web", "/slow/b", clientTimeout),
	}
	s.gate.awaitEntered(t, "a")
	s.gate.awaitEntered(t, "b")
	requests = append(requests, get(client, "web", "/slow/c", clientTimeout))

	awaitBeat("two running and one waiting", func(m wire.HeartbeatMetrics) bool { return m.InFlight == 2 && m.MailboxDepth == 3 })
	if s.gate.running("c") {
		t.Error("a third request ran although both slots were taken")
	}

	for _, id := range []string{"a", "b", "c"} {
		s.gate.letGo(id)
	}
	for i, done := range requests {
		mustOK(t, "request "+strconv.Itoa(i), awaitOutcome(t, "request "+strconv.Itoa(i), done))
	}
	awaitBeat("idle again", func(m wire.HeartbeatMetrics) bool { return m.InFlight == 0 && m.MailboxDepth == 0 })
}

// startWebForDrain runs a web thrall without runThrall's cleanup, so the test itself decides when
// to drain it and can watch StartWeb return.
func startWebForDrain(t *testing.T, eth *ether.Ether, start func() error) <-chan error {
	t.Helper()
	t.Setenv("AETHER_NATS_URL", eth.URL())
	t.Setenv("AETHER_APP", "ae")
	t.Setenv("AETHER_NAME", "web")
	t.Setenv(obs.EnvHeartbeatIntervalMs, "100")
	stopped := make(chan error, 1)
	go func() { stopped <- start() }()

	probe := NewClient(eth.Conn(), "ae")
	deadline := time.Now().Add(clientTimeout)
	for {
		_, err := probe.Call("web", "_ready_probe", nil, 200*time.Millisecond)
		var ce *CallError
		if errors.As(err, &ce) {
			return stopped
		}
		if time.Now().After(deadline) {
			t.Fatalf("the web thrall did not become ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestWebDrainWaitsForInFlightRequests: a drain does not cut a request off. The thrall stops only
// after the requests it had already taken are answered - in both dispatch modes, and without a
// Terminate hook to wait on.
func TestWebDrainWaitsForInFlightRequests(t *testing.T) {
	g := newGate()
	defs := map[string]func() error{
		"serve": func() error { return StartWeb(serveDef(&site{gate: g}, 4)) },
		"handle": func() error {
			return StartWeb(WebDef[struct{}]{
				Handle: func(req HTTPRequest, s struct{}, _ *Ctx) (HTTPResponse, struct{}, error) {
					if id, slow := strings.CutPrefix(req.Path, "/slow/"); slow {
						g.park(id)
					}
					return TextResponse(200, "done"), s, nil
				},
			})
		},
	}
	for name, start := range defs {
		t.Run(name, func(t *testing.T) {
			eth := dispatchEther(t)
			stopped := startWebForDrain(t, eth, start)
			client := NewClient(eth.Conn(), "ae")

			inFlight := get(client, "web", "/slow/"+name, clientTimeout)
			g.awaitEntered(t, name)

			drain, _ := json.Marshal(wire.Envelope{V: 1, Kind: wire.KindCtl, Op: "drain"})
			if err := eth.Conn().Publish(wire.Ctl("web"), drain); err != nil {
				t.Fatalf("publish drain: %v", err)
			}
			select {
			case err := <-stopped:
				t.Fatalf("the thrall stopped with a request still in its handler (err %v)", err)
			case <-time.After(1200 * time.Millisecond): // well past the reader's 500 ms poll
			}

			g.letGo(name)
			mustOK(t, "the in-flight request", awaitOutcome(t, "the in-flight request", inFlight))
			select {
			case err := <-stopped:
				if err != nil {
					t.Errorf("drain ended with: %v", err)
				}
			case <-time.After(clientTimeout):
				t.Fatal("the thrall did not stop after its request finished")
			}
		})
	}
}

// TestWebDefShapes: a definition says which of the two shapes it is, and a combination that
// would run a state-threading handler concurrently is refused before anything connects.
func TestWebDefShapes(t *testing.T) {
	handle := func(HTTPRequest, int, *Ctx) (HTTPResponse, int, error) { return HTTPResponse{}, 0, nil }
	serve := func(HTTPRequest, int, *Ctx) (HTTPResponse, error) { return HTTPResponse{}, nil }

	valid := map[string]struct {
		def  WebDef[int]
		want int
	}{
		"handle":                {WebDef[int]{Handle: handle}, 1},
		"handle, concurrency 1": {WebDef[int]{Handle: handle, Concurrency: 1}, 1},
		"serve, default":        {WebDef[int]{Serve: serve}, DefaultWebConcurrency},
		"serve, concurrency 1":  {WebDef[int]{Serve: serve, Concurrency: 1}, 1},
		"serve, concurrency 8":  {WebDef[int]{Serve: serve, Concurrency: 8}, 8},
	}
	for name, tc := range valid {
		if got, err := tc.def.concurrency(); err != nil || got != tc.want {
			t.Errorf("%s: concurrency = %d, %v; want %d", name, got, err, tc.want)
		}
	}

	invalid := map[string]WebDef[int]{
		"neither handler":            {Name: "web"},
		"both handlers":              {Name: "web", Handle: handle, Serve: serve},
		"handle with concurrency":    {Name: "web", Handle: handle, Concurrency: 4},
		"serve, negative":            {Name: "web", Serve: serve, Concurrency: -1},
		"handle, negative":           {Name: "web", Handle: handle, Concurrency: -1},
		"both, even at concurrency1": {Name: "web", Handle: handle, Serve: serve, Concurrency: 1},
	}
	for name, def := range invalid {
		if err := StartWeb(def); err == nil {
			t.Errorf("%s: StartWeb accepted the definition", name)
		}
	}
}

// TestWebServeFnLeavesTheSharedValueAlone: adapted to the thrall's call shape, a Serve handler
// hands back the value it was given - there is no next state to thread, even through the mailbox.
func TestWebServeFnLeavesTheSharedValueAlone(t *testing.T) {
	shared := &site{}
	fn := webServeFn(func(req HTTPRequest, s *site, _ *Ctx) (HTTPResponse, error) {
		return TextResponse(200, req.Path), nil
	})
	payload, _ := json.Marshal(HTTPRequest{Method: "GET", Path: "/x"})
	reply, next, err := fn(payload, shared, &Ctx{})
	if err != nil || next != shared {
		t.Fatalf("webServeFn: next = %p (want %p), err %v", next, shared, err)
	}
	if resp, ok := reply.(HTTPResponse); !ok || string(resp.Body) != "/x" {
		t.Errorf("reply = %#v", reply)
	}
	if _, _, err := fn(json.RawMessage(`not json`), shared, &Ctx{}); err == nil {
		t.Error("a malformed payload must be a handler error")
	}
}

// TestIdleDrainIsImmediate: a thrall with nothing in hand stops as soon as it is told to. The
// readers are woken by the stop rather than left to time out their current read, which would add
// up to half a second to every node shutdown, group restart and StopChild.
func TestIdleDrainIsImmediate(t *testing.T) {
	defs := map[string]func() error{
		"genserver": func() error {
			return Start(Def[struct{}]{Init: func(*Ctx) (struct{}, error) { return struct{}{}, nil }})
		},
		"web, serialized": func() error {
			return StartWeb(WebDef[struct{}]{
				Handle: func(HTTPRequest, struct{}, *Ctx) (HTTPResponse, struct{}, error) {
					return TextResponse(200, "ok"), struct{}{}, nil
				},
			})
		},
		"web, concurrent": func() error { return StartWeb(serveDef(&site{gate: newGate()}, 4)) },
	}
	for name, start := range defs {
		t.Run(name, func(t *testing.T) {
			eth := dispatchEther(t)
			stopped := startWebForDrain(t, eth, start)
			time.Sleep(50 * time.Millisecond) // the readers are back in a blocking read

			drain, _ := json.Marshal(wire.Envelope{V: 1, Kind: wire.KindCtl, Op: "drain"})
			began := time.Now()
			if err := eth.Conn().Publish(wire.Ctl("web"), drain); err != nil {
				t.Fatalf("publish drain: %v", err)
			}
			select {
			case err := <-stopped:
				if err != nil {
					t.Errorf("drain ended with: %v", err)
				}
			case <-time.After(clientTimeout):
				t.Fatal("the thrall did not stop")
			}
			// Far below the 500 ms a read used to be waited out for, with room for a slow machine.
			if took := time.Since(began); took > 200*time.Millisecond {
				t.Errorf("an idle thrall took %v to drain, want it to stop at once", took)
			}
		})
	}
}

// TestWebServeFarFutureDeadlineWaitsForASlot: a deadline far beyond any real timeout must not be
// read as already passed. A request carrying one, arriving while every slot is taken, waits and
// is served when a slot comes free - whatever the size of the number.
func TestWebServeFarFutureDeadlineWaitsForASlot(t *testing.T) {
	eth := dispatchEther(t)
	nc := eth.Conn()
	s := &site{gate: newGate()}
	runThrall(t, eth, "ae", "web", func() error { return StartWeb(serveDef(s, 2)) })
	client := NewClient(nc, "ae")

	// Two requests take both slots; the far-future ones then have to wait.
	busy := []<-chan outcome{get(client, "web", "/slow/a", clientTimeout), get(client, "web", "/slow/b", clientTimeout)}
	s.gate.awaitEntered(t, "a")
	s.gate.awaitEntered(t, "b")

	payload, _ := json.Marshal(HTTPRequest{Method: "GET", Path: "/fast"})
	deadlines := map[string]int64{
		"a month away":            time.Now().Add(30 * 24 * time.Hour).UnixMilli(),
		"past what a timer holds": time.Now().UnixMilli() + (1 << 40),
		"the largest instant":     1<<63 - 1,
	}
	replies := make(chan wire.Envelope, len(deadlines))
	for name, deadline := range deadlines {
		e := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: "web", Op: OpHTTPRequest,
			Trace: name, Payload: payload, TS: time.Now().UnixMilli(), Deadline: deadline}
		go func() { replies <- rawCall(t, nc, "ae", e) }()
	}
	select {
	case reply := <-replies:
		t.Fatalf("a request with a far-future deadline was answered while both slots were taken: %+v (error %+v)", reply.Status, reply.Error)
	case <-time.After(300 * time.Millisecond):
	}

	s.gate.letGo("a")
	s.gate.letGo("b")
	for range deadlines {
		select {
		case reply := <-replies:
			if reply.Status != wire.StatusOK {
				t.Errorf("a request with a far-future deadline = %+v, want it served", reply.Error)
			}
		case <-time.After(clientTimeout):
			t.Fatal("a request with a far-future deadline was never served")
		}
	}
	for i, done := range busy {
		mustOK(t, "busy request "+strconv.Itoa(i), awaitOutcome(t, "busy request", done))
	}
}
