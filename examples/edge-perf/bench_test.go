//go:build edgeperf

package edgeperf

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/edge"
	"github.com/hamicek/aether/internal/ether"
	"github.com/hamicek/aether/internal/wire"
	"github.com/hamicek/aether/sdk/go/thrall"
)

const (
	benchApp    = "perf"
	backendName = "counter"
)

// --- backend thrall (the state behind the edge) ---

func counterDef() thrall.Def[int] {
	return thrall.Def[int]{
		Name: backendName,
		Init: func(_ *thrall.Ctx) (int, error) { return 0, nil },
		HandleCall: map[string]thrall.CallFn[int]{
			"get": func(_ json.RawMessage, s int, _ *thrall.Ctx) (any, int, error) { return s, s, nil },
		},
		HandleCast: map[string]thrall.CastFn[int]{
			"inc": func(_ json.RawMessage, s int, _ *thrall.Ctx) (int, error) { return s + 1, nil },
		},
	}
}

// --- harness: embedded NATS + an in-process backend thrall ---

type harness struct {
	eth *ether.Ether
	nc  *nats.Conn
}

func setup(t *testing.T) *harness {
	t.Helper()
	eth, err := ether.Start(context.Background(), ether.Config{Mode: "embedded"})
	if err != nil {
		t.Fatalf("ether start: %v", err)
	}
	// The in-process thrall reads its wiring from the environment, exactly as a spawned process would.
	os.Setenv("AETHER_NATS_URL", eth.URL())
	os.Setenv("AETHER_APP", benchApp)
	os.Setenv("AETHER_NAME", backendName)
	go func() { _ = thrall.Start(counterDef()) }()

	nc, err := nats.Connect(eth.URL())
	if err != nil {
		eth.Stop()
		t.Fatalf("client connect: %v", err)
	}
	h := &harness{eth: eth, nc: nc}

	// Wait until the backend answers a call - it is then subscribed and ready.
	deadline := time.Now().Add(5 * time.Second)
	for {
		req := wire.Envelope{V: 1, ID: nats.NewInbox(), Kind: wire.KindCall, To: backendName, Op: "get", Payload: json.RawMessage("{}")}
		data, _ := json.Marshal(req)
		if _, err := nc.Request(wire.Call(benchApp, backendName), data, 200*time.Millisecond); err == nil {
			break
		}
		if time.Now().After(deadline) {
			h.stop()
			t.Fatal("backend did not become ready")
		}
	}
	return h
}

func (h *harness) stop() {
	if h.nc != nil {
		h.nc.Close()
	}
	if h.eth != nil {
		h.eth.Stop()
	}
}

// benchEdgeHandler is the edge HTTP->ether translation, assembled from the same internal/edge
// primitives the production `aether _edge` handler uses (Router.Match, BuildEnvelope, StatusFor*), so
// the measured path is the real edge data path. The thin HTTP wiring mirrors the production handler.
func benchEdgeHandler(nc *nats.Conn, app string, router *edge.Router, timeout time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route, ok := router.Match(r.Method, r.URL.Path)
		if !ok {
			http.Error(w, "no route", http.StatusNotFound)
			return
		}
		body, _ := io.ReadAll(r.Body)
		env := edge.BuildEnvelope(route, body, r.URL.Query())
		data, _ := json.Marshal(env)
		if route.Kind == "cast" {
			_ = nc.Publish(wire.Cast(app, route.Thrall), data)
			_ = nc.Flush()
			w.WriteHeader(http.StatusAccepted)
			return
		}
		msg, err := nc.Request(wire.Call(app, route.Thrall), data, timeout)
		if err != nil {
			http.Error(w, http.StatusText(edge.StatusForError(err)), edge.StatusForError(err))
			return
		}
		var reply wire.Envelope
		_ = json.Unmarshal(msg.Data, &reply)
		w.WriteHeader(edge.StatusForReply(reply))
		if reply.Status != "error" && len(reply.Payload) > 0 {
			_, _ = w.Write(reply.Payload)
		}
	})
}

func ingressSpec() edge.Spec {
	return edge.Spec{
		Name: "api",
		Addr: ":0",
		Routes: map[string]edge.Route{
			"GET /value": {Thrall: backendName, Op: "get", Kind: "call"},
			"POST /inc":  {Thrall: backendName, Op: "inc", Kind: "cast"},
		},
	}
}

// --- measurement primitives ---

// pct returns the p-th percentile (0..1) of a sorted slice by nearest-rank index.
func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

type loadResult struct {
	achieved      float64 // req/s
	p50, p99, max time.Duration
	count, errors int
}

// measureClosedLoop runs `workers` goroutines that each send requests back-to-back for `dur`, recording
// per-request latency. Closed-loop (each worker waits for its request to finish) matches how a call
// blocks on the reply; throughput is then count/dur at that concurrency.
func measureClosedLoop(dur time.Duration, workers int, send func() error) loadResult {
	perWorker := make([][]time.Duration, workers) // no lock in the hot loop
	errs := make([]int, workers)
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	for wIdx := 0; wIdx < workers; wIdx++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			lats := make([]time.Duration, 0, 4096)
			for time.Now().Before(deadline) {
				t0 := time.Now()
				err := send()
				lats = append(lats, time.Since(t0))
				if err != nil {
					errs[w]++
				}
			}
			perWorker[w] = lats
		}(wIdx)
	}
	wg.Wait()

	var all []time.Duration
	totalErrs := 0
	for w := 0; w < workers; w++ {
		all = append(all, perWorker[w]...)
		totalErrs += errs[w]
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return loadResult{
		achieved: float64(len(all)) / dur.Seconds(),
		p50:      pct(all, 0.50),
		p99:      pct(all, 0.99),
		max:      pct(all, 1.0),
		count:    len(all),
		errors:   totalErrs,
	}
}

func newClient(workers int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        workers * 2,
			MaxIdleConnsPerHost: workers * 2, // avoid a connection-pool bottleneck skewing the numbers
			IdleConnTimeout:     30 * time.Second,
		},
	}
}

func logResult(t *testing.T, label string, r loadResult) {
	t.Logf("%-14s req/s=%-8.0f p50=%-9s p99=%-9s max=%-9s errs=%d", label, r.achieved, r.p50, r.p99, r.max, r.errors)
}

// --- the benchmark ---

const (
	benchDur     = 3 * time.Second
	benchWorkers = 16
)

func TestEdgePerf(t *testing.T) {
	h := setup(t)
	defer h.stop()

	edgeSrv := httptest.NewServer(benchEdgeHandler(h.nc, benchApp, edge.NewRouter(ingressSpec()), 2*time.Second))
	defer edgeSrv.Close()

	// Baseline: a bare net/http handler returning a constant, no ether at all. The delta to the edge
	// call is the cost of the aether layer (NATS round-trip + mailbox + envelope).
	baseSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("0"))
	}))
	defer baseSrv.Close()

	client := newClient(benchWorkers)
	get := func(url string, wantStatus int) func() error {
		return func() error {
			resp, err := client.Get(url)
			if err != nil {
				return err
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != wantStatus {
				return fmt.Errorf("status %d", resp.StatusCode)
			}
			return nil
		}
	}

	t.Logf("edge-perf: dur=%s workers=%d (numbers are indicative, machine-dependent)", benchDur, benchWorkers)
	logResult(t, "BASELINE", measureClosedLoop(benchDur, benchWorkers, get(baseSrv.URL, http.StatusOK)))
	logResult(t, "INGRESS-CALL", measureClosedLoop(benchDur, benchWorkers, get(edgeSrv.URL+"/value", http.StatusOK)))
	logResult(t, "INGRESS-CAST", measureClosedLoop(benchDur, benchWorkers, func() error {
		resp, err := client.Post(edgeSrv.URL+"/inc", "application/json", strings.NewReader("{}"))
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		return nil
	}))
}

// --- SSE push fan-out ---

type sseResult struct {
	clients, published int
	delivered          int
	fanout             float64 // delivered events/s across all clients
	p50, p99, max      time.Duration
	drops              int
}

// measureSSEFanout connects `clients` SSE clients to one edge stream, publishes `rate` events/s for
// `dur`, and measures how many event->client deliveries the edge sustains and the event->client latency.
// A drop is an event a client's bounded buffer shed under load (published*clients - delivered).
func measureSSEFanout(t *testing.T, nc *nats.Conn, clients, rate int, dur time.Duration) sseResult {
	t.Helper()
	const subject = "perf.sse.evt"
	stream := thrall.NewSSEStream(&thrall.Ctx{NATS: nc})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = stream.ServeClient(w, r, subject)
	}))
	defer ts.Close()

	clientCtx, cancelClients := context.WithCancel(context.Background())
	perClient := make([][]time.Duration, clients)
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(clientCtx, http.MethodGet, ts.URL, nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			lats := make([]time.Duration, 0, 4096)
			sc := bufio.NewScanner(resp.Body)
			for sc.Scan() {
				line := sc.Text()
				if !strings.HasPrefix(line, "data: ") {
					continue // keep-alive comment or blank line
				}
				now := time.Now().UnixNano()
				var ev struct {
					T int64 `json:"t"`
				}
				if json.Unmarshal([]byte(line[len("data: "):]), &ev) == nil {
					lats = append(lats, time.Duration(now-ev.T))
				}
			}
			perClient[idx] = lats
		}(c)
	}

	// Give the clients a moment to connect and subscribe before publishing.
	time.Sleep(300 * time.Millisecond)

	published := 0
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		<-ticker.C
		ev := fmt.Sprintf(`{"t":%d}`, time.Now().UnixNano())
		_ = nc.Publish(subject, []byte(ev))
		published++
	}
	ticker.Stop()
	_ = nc.Flush()

	time.Sleep(200 * time.Millisecond) // let in-flight deliveries land
	cancelClients()
	stream.Close()
	wg.Wait()

	var all []time.Duration
	for _, l := range perClient {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	delivered := len(all)
	return sseResult{
		clients:   clients,
		published: published,
		delivered: delivered,
		fanout:    float64(delivered) / dur.Seconds(),
		p50:       pct(all, 0.50),
		p99:       pct(all, 0.99),
		max:       pct(all, 1.0),
		drops:     published*clients - delivered,
	}
}

func TestEdgeSSEPerf(t *testing.T) {
	h := setup(t)
	defer h.stop()

	const rate = 2000 // events/s published to the stream
	t.Logf("edge-perf SSE: rate=%d/s dur=%s (fan-out = rate x clients if no drops)", rate, benchDur)
	for _, clients := range []int{1, 10, 50, 200} {
		r := measureSSEFanout(t, h.nc, clients, rate, benchDur)
		t.Logf("SSE-FANOUT clients=%-4d published=%-6d delivered=%-8d fanout=%-9.0f p50=%-9s p99=%-9s max=%-9s drops=%d",
			r.clients, r.published, r.delivered, r.fanout, r.p50, r.p99, r.max, r.drops)
	}
}
