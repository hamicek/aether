package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEmitMatchesGolden(t *testing.T) {
	fixtures := map[string][]string{
		"control-command.protocol.yaml": {"operator", "device"},
		"driver-control.protocol.yaml":  {"collector", "device"},
	}
	for f, roles := range fixtures {
		p := readFixture(t, f)
		base := strings.TrimSuffix(f, ".protocol.yaml")
		for _, role := range roles {
			a, err := Project(p, role)
			if err != nil {
				t.Fatalf("%s/%s project: %v", f, role, err)
			}
			got, err := EmitAutomaton(a)
			if err != nil {
				t.Fatalf("%s/%s emit: %v", f, role, err)
			}
			golden := filepath.Join("testdata", "golden", base+"."+role+".json")
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatalf("read golden %s: %v", golden, err)
			}
			if string(got) != string(want) {
				t.Fatalf("%s/%s: emit differs from golden %s\n--- got ---\n%s", f, role, golden, got)
			}
			if got2, _ := EmitAutomaton(a); string(got2) != string(got) {
				t.Fatalf("%s/%s: emit is not deterministic", f, role)
			}
		}
	}
}
