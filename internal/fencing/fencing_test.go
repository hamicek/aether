package fencing

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("AETHER_TEST_EPOCH", "7")
	t.Setenv("AETHER_TEST_KEY", "app")
	cfg, ok := ConfigFromEnv("AETHER_TEST_EPOCH", "AETHER_TEST_KEY")
	if !ok || cfg.Epoch != 7 || cfg.Key != "app" {
		t.Fatalf("cfg = %+v (ok=%v), want {app 7}", cfg, ok)
	}

	// Missing / zero epoch -> not fenced.
	t.Setenv("AETHER_TEST_EPOCH", "0")
	if _, ok := ConfigFromEnv("AETHER_TEST_EPOCH", "AETHER_TEST_KEY"); ok {
		t.Fatal("zero epoch reported as fenced")
	}
	if _, ok := ConfigFromEnv("AETHER_MISSING_EPOCH", "AETHER_TEST_KEY"); ok {
		t.Fatal("missing epoch reported as fenced")
	}
}

func TestLoopConfirmedLossExitsImmediately(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)
	lost := make(chan string, 1)

	// verify returns false -> the epoch was superseded / key gone.
	go Loop("test", func() (bool, error) { return false, nil }, time.Millisecond, time.Hour, quietLog(), stop, func(r string) { lost <- r })

	select {
	case <-lost:
	case <-time.After(time.Second):
		t.Fatal("onLost not called on a confirmed loss")
	}
}

func TestLoopUnverifiableWaitsForLease(t *testing.T) {
	stop := make(chan struct{})
	defer close(stop)
	lost := make(chan string, 1)

	// verify always errors: onLost must fire only after the lease elapses, not on the first tick.
	go Loop("test", func() (bool, error) { return false, errors.New("kv down") }, time.Millisecond, 80*time.Millisecond, quietLog(), stop, func(r string) { lost <- r })

	select {
	case <-lost:
		t.Fatal("onLost fired before the lease elapsed")
	case <-time.After(30 * time.Millisecond):
	}
	select {
	case <-lost:
	case <-time.After(time.Second):
		t.Fatal("onLost not called after the lease elapsed")
	}
}

func TestLoopHealthyDoesNotExit(t *testing.T) {
	stop := make(chan struct{})
	lost := make(chan string, 1)

	go Loop("test", func() (bool, error) { return true, nil }, time.Millisecond, time.Hour, quietLog(), stop, func(r string) { lost <- r })

	select {
	case <-lost:
		t.Fatal("onLost called while healthy")
	case <-time.After(50 * time.Millisecond):
	}
	close(stop)
}
