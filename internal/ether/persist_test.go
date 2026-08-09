package ether

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	persistStream  = "persist_test"
	persistSubject = "persist.cast"
)

// addDurableStream provisions a durable mailbox stream the same way the lord does:
// WorkQueue retention on file storage, so acked messages leave the stream and the
// backlog is a faithful server-side no-loss measure.
func addDurableStream(t *testing.T, js nats.JetStreamContext) {
	t.Helper()
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:      persistStream,
		Subjects:  []string{persistSubject},
		Retention: nats.WorkQueuePolicy,
		Storage:   nats.FileStorage,
	}); err != nil {
		t.Fatalf("add stream: %v", err)
	}
}

// streamMsgs returns how many messages remain in the durable stream - the same
// restart-proof, server-side measure the soak suite uses (StreamInfo().State.Msgs).
func streamMsgs(t *testing.T, js nats.JetStreamContext) int {
	t.Helper()
	si, err := js.StreamInfo(persistStream)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	return int(si.State.Msgs)
}

// TestEmbeddedPersistentStoreSurvivesRestart proves that a configured store_dir keeps
// the durable mailbox across a full embedded restart, and that every stored cast is
// delivered afterwards with no loss (backlog drains to zero, measured server-side).
func TestEmbeddedPersistentStoreSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	const n = 50

	// First run: provision the durable stream and enqueue n casts without consuming.
	eth1, err := Start(context.Background(), Config{Mode: "embedded", StoreDir: dir})
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	js1, err := eth1.Conn().JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	addDurableStream(t, js1)
	for i := 0; i < n; i++ {
		if _, err := js1.Publish(persistSubject, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if got := streamMsgs(t, js1); got != n {
		t.Fatalf("after publish: stream has %d msgs, want %d", got, n)
	}
	eth1.Stop()

	// The persistent store dir must outlive Stop.
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("store dir gone after Stop: %v", err)
	}

	// Second run against the same store dir: the mailbox must be recovered intact.
	eth2, err := Start(context.Background(), Config{Mode: "embedded", StoreDir: dir})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer eth2.Stop()
	js2, err := eth2.Conn().JetStream()
	if err != nil {
		t.Fatalf("jetstream after restart: %v", err)
	}
	if got := streamMsgs(t, js2); got != n {
		t.Fatalf("after restart: stream has %d msgs, want %d (mailbox not recovered)", got, n)
	}

	// Drain the recovered backlog: every stored cast is delivered and acked, so the
	// WorkQueue stream ends empty - proof of no loss across the restart.
	sub, err := js2.PullSubscribe(persistSubject, "worker")
	if err != nil {
		t.Fatalf("pull subscribe: %v", err)
	}
	for delivered := 0; delivered < n; {
		msgs, err := sub.Fetch(n-delivered, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Fatalf("fetch after %d delivered: %v", delivered, err)
		}
		for _, m := range msgs {
			if err := m.Ack(); err != nil {
				t.Fatalf("ack: %v", err)
			}
			delivered++
		}
	}
	waitStreamEmpty(t, js2)
}

// waitStreamEmpty polls until the stream backlog reaches zero. Acks are processed
// asynchronously, so a bounded retry avoids racing the server-side bookkeeping.
func waitStreamEmpty(t *testing.T, js nats.JetStreamContext) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if got := streamMsgs(t, js); got == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream did not drain to zero: %d left", streamMsgs(t, js))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestEmbeddedEphemeralDefaultRemovesStore proves the default (no store_dir) keeps
// today's behavior: a temp store dir that is removed on Stop.
func TestEmbeddedEphemeralDefaultRemovesStore(t *testing.T) {
	eth, err := Start(context.Background(), Config{Mode: "embedded"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	dir := eth.storeDir
	if dir == "" {
		t.Fatal("ephemeral start left an empty store dir")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("store dir missing while running: %v", err)
	}
	eth.Stop()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("ephemeral store dir survived Stop (err=%v), want removed", err)
	}
}
