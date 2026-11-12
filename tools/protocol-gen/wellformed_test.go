package main

import (
	"strings"
	"testing"
)

func TestWellFormedFixtures(t *testing.T) {
	for _, f := range []string{"control-command.protocol.yaml", "driver-control.protocol.yaml"} {
		p := readFixture(t, f)
		for _, role := range p.Roles {
			a, err := Project(p, role)
			if err != nil {
				t.Fatalf("%s/%s project: %v", f, role, err)
			}
			if err := WellFormed(a); err != nil {
				t.Fatalf("%s/%s wellformed: %v", f, role, err)
			}
		}
	}
}

func TestProjectUndefinedGoto(t *testing.T) {
	y := "protocol: P\nroles: [a, b]\nstart: a\n" +
		"flow: [{rec: L, body: [{from: a, to: b, kind: call, op: x, reply: {choice: [{op: y, then: {goto: Nope}}]}}]}]\n"
	p, err := Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Project(p, "a"); err == nil || !strings.Contains(err.Error(), "undefined rec label") {
		t.Fatalf("want undefined rec label error, got %v", err)
	}
}

func TestWellFormedNeverTerminates(t *testing.T) {
	// A loop with no exit branch: every path goes back to L, end is never reached.
	y := "protocol: P\nroles: [a, b]\nstart: a\n" +
		"flow: [{rec: L, body: [{from: a, to: b, kind: call, op: x, reply: {choice: [{op: y, then: {goto: L}}]}}]}]\n"
	p, err := Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	a, err := Project(p, "a")
	if err != nil {
		t.Fatal(err)
	}
	if err := WellFormed(a); err == nil || !strings.Contains(err.Error(), "never terminate") {
		t.Fatalf("want never-terminate error, got %v", err)
	}
}
