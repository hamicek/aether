package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// readFixture loads a protocol from testdata.
func readFixture(t *testing.T, name string) *Protocol {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	p, err := Parse(data)
	if err != nil {
		t.Fatalf("parse fixture %s: %v", name, err)
	}
	return p
}

func TestParseValidFixtures(t *testing.T) {
	cmd := readFixture(t, "control-command.protocol.yaml")
	if cmd.Protocol != "ControlCommand" || cmd.Start != "operator" {
		t.Fatalf("control-command: unexpected header %+v", cmd)
	}
	if len(cmd.Flow) != 1 || cmd.Flow[0].Op != "command" || cmd.Flow[0].Reply == nil {
		t.Fatalf("control-command: expected a leading call with a reply, got %+v", cmd.Flow)
	}

	drv := readFixture(t, "driver-control.protocol.yaml")
	if drv.Protocol != "DriverControl" {
		t.Fatalf("driver: unexpected name %q", drv.Protocol)
	}
	if len(drv.Flow) != 1 || !drv.Flow[0].isRec() {
		t.Fatalf("driver: expected a leading rec block, got %+v", drv.Flow)
	}
	if !drv.Flow[0].Body[0].isChoose() {
		t.Fatalf("driver: expected the rec body to open with a choose, got %+v", drv.Flow[0].Body[0])
	}
}

func TestParseRejectsMalformed(t *testing.T) {
	cases := map[string]struct {
		yaml string
		want string // substring expected in the error
	}{
		"one role": {
			yaml: "protocol: P\nroles: [a]\nstart: a\nflow: [{from: a, to: a, kind: cast, op: x}]\n",
			want: "exactly two roles",
		},
		"start not a role": {
			yaml: "protocol: P\nroles: [a, b]\nstart: c\nflow: [{from: a, to: b, kind: cast, op: x}]\n",
			want: "start role",
		},
		"unknown from-role": {
			yaml: "protocol: P\nroles: [a, b]\nstart: a\nflow: [{from: z, to: b, kind: cast, op: x}]\n",
			want: "unknown from-role",
		},
		"self message": {
			yaml: "protocol: P\nroles: [a, b]\nstart: a\nflow: [{from: a, to: a, kind: cast, op: x}]\n",
			want: "from a role to itself",
		},
		"call without reply": {
			yaml: "protocol: P\nroles: [a, b]\nstart: a\nflow: [{from: a, to: b, kind: call, op: x}]\n",
			want: "no reply",
		},
		"cast with reply": {
			yaml: "protocol: P\nroles: [a, b]\nstart: a\nflow: [{from: a, to: b, kind: cast, op: x, reply: {choice: [{op: y, then: end}]}}]\n",
			want: "carries a reply",
		},
		"unknown kind": {
			yaml: "protocol: P\nroles: [a, b]\nstart: a\nflow: [{from: a, to: b, kind: shout, op: x}]\n",
			want: "unknown kind",
		},
		"empty choice": {
			yaml: "protocol: P\nroles: [a, b]\nstart: a\nflow: [{from: a, to: b, kind: call, op: x, reply: {choice: []}}]\n",
			want: "no reply",
		},
		"duplicate branch op": {
			yaml: "protocol: P\nroles: [a, b]\nstart: a\nflow: [{from: a, to: b, kind: call, op: x, reply: {choice: [{op: y, then: end}, {op: y, then: end}]}}]\n",
			want: "repeats branch op",
		},
		"then unknown keyword": {
			yaml: "protocol: P\nroles: [a, b]\nstart: a\nflow: [{from: a, to: b, kind: call, op: x, reply: {choice: [{op: y, then: stop}]}}]\n",
			want: "unknown keyword",
		},
		"empty rec body": {
			yaml: "protocol: P\nroles: [a, b]\nstart: a\nflow: [{rec: L, body: []}]\n",
			want: "empty body",
		},
		"empty step": {
			yaml: "protocol: P\nroles: [a, b]\nstart: a\nflow: [{}]\n",
			want: "neither a message",
		},
		"choose mixed roles": {
			yaml: "protocol: P\nroles: [a, b]\nstart: a\nflow: [{choose: [{from: a, to: b, kind: call, op: x, reply: {choice: [{op: r, then: end}]}}, {from: b, to: a, kind: call, op: y, reply: {choice: [{op: s, then: end}]}}]}]\n",
			want: "all be sent by one role",
		},
		"empty flow": {
			yaml: "protocol: P\nroles: [a, b]\nstart: a\nflow: []\n",
			want: "flow is empty",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("expected an error, got none")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.want)
			}
		})
	}
}
