package main

import (
	"strings"
	"testing"

	"github.com/hamicek/aether/internal/wire"
)

func TestConformsToOK(t *testing.T) {
	p := readFixture(t, "driver-control.protocol.yaml")
	dev, err := Project(p, "device")
	if err != nil {
		t.Fatal(err)
	}
	// The device receives (handles) write and health.
	d := wire.ThrallDescribe{CallOps: []string{"write", "health"}}
	if errs := ConformsTo(dev, d); len(errs) != 0 {
		t.Fatalf("expected conformance, got %v", errs)
	}
}

func TestConformsToReportsMissingHandler(t *testing.T) {
	p := readFixture(t, "driver-control.protocol.yaml")
	dev, err := Project(p, "device")
	if err != nil {
		t.Fatal(err)
	}
	d := wire.ThrallDescribe{CallOps: []string{"write"}} // health handler missing
	errs := ConformsTo(dev, d)
	if len(errs) != 1 || !strings.Contains(errs[0].Error(), "health") {
		t.Fatalf("want a single missing-health error, got %v", errs)
	}
}

func TestConformsToIgnoresSendsAndReplies(t *testing.T) {
	// The collector only sends write/health and receives replies, so it needs no handlers at all -
	// an empty describe still conforms. This pins the "received call/cast only" semantics.
	p := readFixture(t, "driver-control.protocol.yaml")
	col, err := Project(p, "collector")
	if err != nil {
		t.Fatal(err)
	}
	if errs := ConformsTo(col, wire.ThrallDescribe{}); len(errs) != 0 {
		t.Fatalf("collector needs no handlers, got %v", errs)
	}
}
