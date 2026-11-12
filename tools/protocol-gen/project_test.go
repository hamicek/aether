package main

import (
	"testing"

	"github.com/hamicek/aether/sdk/go/session"
)

func TestProjectControlCommand(t *testing.T) {
	p := readFixture(t, "control-command.protocol.yaml")

	op, err := Project(p, "operator")
	if err != nil {
		t.Fatal(err)
	}
	start := op.States[op.Start]
	cmd := start.On["command"]
	if cmd.Dir != session.DirSend || cmd.Kind != "call" {
		t.Fatalf("operator start: want send call, got %+v", cmd)
	}
	reply := op.States[cmd.Next]
	if a := reply.On["authorized"]; a.Dir != session.DirRecv || a.Kind != "reply" {
		t.Fatalf("authorized: want recv reply, got %+v", a)
	}
	if rj := reply.On["rejected"]; rj.Dir != session.DirRecv || !op.States[rj.Next].Final {
		t.Fatalf("rejected should be recv and end, got %+v", rj)
	}
	// "confirmed" lives only in the sub-flow after "authorized" - it cannot precede it structurally.
	afterAuth := op.States[reply.On["authorized"].Next]
	exec := afterAuth.On["execute"]
	if exec.Dir != session.DirSend || exec.Kind != "call" {
		t.Fatalf("execute: want send call, got %+v", exec)
	}
	if _, bad := afterAuth.On["confirmed"]; bad {
		t.Fatalf("confirmed must not be legal before execute")
	}
	execReply := op.States[exec.Next]
	if c := execReply.On["confirmed"]; c.Dir != session.DirRecv || !op.States[c.Next].Final {
		t.Fatalf("confirmed should be recv and end, got %+v", c)
	}

	// The device is the dual: it receives the command and sends the replies.
	dev, err := Project(p, "device")
	if err != nil {
		t.Fatal(err)
	}
	dcmd := dev.States[dev.Start].On["command"]
	if dcmd.Dir != session.DirRecv || dcmd.Kind != "call" {
		t.Fatalf("device command: want recv call, got %+v", dcmd)
	}
	if da := dev.States[dcmd.Next].On["authorized"]; da.Dir != session.DirSend {
		t.Fatalf("device authorized: want send, got %+v", da)
	}
}

func TestProjectDriverLoopBackEdge(t *testing.T) {
	p := readFixture(t, "driver-control.protocol.yaml")
	col, err := Project(p, "collector")
	if err != nil {
		t.Fatal(err)
	}
	// The rec body opens with a choose, so the start state offers both commands.
	start := col.States[col.Start]
	write := start.On["write"]
	if write.Dir != session.DirSend || write.Kind != "call" {
		t.Fatalf("collector write: want send call, got %+v", write)
	}
	if _, ok := start.On["health"]; !ok {
		t.Fatalf("collector choose should also offer health, got %+v", start.On)
	}
	// write -> applied loops back to the start (goto Loop), fault ends.
	writeReply := col.States[write.Next]
	if ap := writeReply.On["applied"]; ap.Next != col.Start {
		t.Fatalf("applied should loop to start %s, got %s", col.Start, ap.Next)
	}
	if f := writeReply.On["fault"]; !col.States[f.Next].Final {
		t.Fatalf("fault should end, got %+v", f)
	}
	// health -> status also loops back.
	healthReply := col.States[start.On["health"].Next]
	if st := healthReply.On["status"]; st.Next != col.Start {
		t.Fatalf("status should loop to start %s, got %s", col.Start, st.Next)
	}
}
