package thrall

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// sseClient connects to an SSE endpoint and streams its lines onto a channel. The returned cancel
// closes the connection (so ServeClient returns and the test server can shut down).
func sseClient(t *testing.T, url string) (<-chan string, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("connect: %v", err)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		cancel()
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}
	lines := make(chan string, 32)
	go func() {
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	// The response headers arrive only after ServeClient has subscribed and flushed, so once Do has
	// returned the subscription is live and a subsequent publish is guaranteed to be seen.
	return lines, cancel
}

// waitForData reads until an SSE "data:" frame arrives, failing on timeout or a closed stream.
func waitForData(t *testing.T, lines <-chan string, d time.Duration) string {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case ln, ok := <-lines:
			if !ok {
				t.Fatal("stream closed before any data frame")
			}
			if strings.HasPrefix(ln, "data: ") {
				return ln
			}
		case <-deadline:
			t.Fatal("timed out waiting for a data frame")
		}
	}
}

func TestSSEStreamDeliversScopedEvents(t *testing.T) {
	nc, _ := embeddedNATS(t)
	stream := NewSSEStream(&Ctx{NATS: nc})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = stream.ServeClient(w, r, "test.site.1.evt")
	}))
	defer ts.Close()

	lines, cancel := sseClient(t, ts.URL)
	defer cancel()

	if err := nc.Publish("test.site.1.evt", []byte(`{"temp":21}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	_ = nc.Flush()

	if got := waitForData(t, lines, 2*time.Second); got != `data: {"temp":21}` {
		t.Fatalf("frame = %q, want the published event", got)
	}
}

func TestSSEStreamScopeIsolation(t *testing.T) {
	nc, _ := embeddedNATS(t)
	stream := NewSSEStream(&Ctx{NATS: nc})

	// The client is scoped to site 1 only.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = stream.ServeClient(w, r, "test.site.1.evt")
	}))
	defer ts.Close()

	lines, cancel := sseClient(t, ts.URL)
	defer cancel()

	// An event on site 2 must never reach a site-1 client (NATS does not deliver it - scope-safe).
	if err := nc.Publish("test.site.2.evt", []byte(`{"temp":99}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	_ = nc.Flush()

	deadline := time.After(700 * time.Millisecond)
	for {
		select {
		case ln, ok := <-lines:
			if !ok {
				return // stream closed, no leak
			}
			if strings.HasPrefix(ln, "data: ") {
				t.Fatalf("received an out-of-scope event: %q", ln)
			}
		case <-deadline:
			return // no data frame within the window - correct
		}
	}
}

func TestSSEStreamCloseEndsConnection(t *testing.T) {
	nc, _ := embeddedNATS(t)
	stream := NewSSEStream(&Ctx{NATS: nc})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = stream.ServeClient(w, r, "test.site.1.evt")
	}))
	defer ts.Close()

	lines, cancel := sseClient(t, ts.URL)
	defer cancel()

	stream.Close() // draining: every live connection must end

	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-lines:
			if !ok {
				return // connection closed by the server - correct
			}
		case <-deadline:
			t.Fatal("connection still open after Close()")
		}
	}
}

func TestServeClientRejectsEmptyScope(t *testing.T) {
	nc, _ := embeddedNATS(t)
	stream := NewSSEStream(&Ctx{NATS: nc})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	err := stream.ServeClient(rec, req) // no subjects
	if err == nil {
		t.Fatal("expected an error for an empty subject scope")
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}
