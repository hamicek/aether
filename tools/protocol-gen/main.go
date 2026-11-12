package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// protocol-gen <IN_DIR> [OUT_DIR] reads every *.protocol.yaml in IN_DIR, checks each (parse,
// well-formedness, duality) and writes the per-role automata to OUT_DIR/gen/automaton. A check
// failure exits non-zero, so a CI drift job doubles as the well-formedness gate. OUT_DIR defaults
// to IN_DIR; the drift check passes a temp dir and diffs it against the committed gen/.
func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Fprintln(os.Stderr, "usage: protocol-gen <IN_DIR> [OUT_DIR]")
		os.Exit(2)
	}
	inDir := os.Args[1]
	outDir := inDir
	if len(os.Args) == 3 {
		outDir = os.Args[2]
	}
	if err := run(inDir, outDir); err != nil {
		fmt.Fprintln(os.Stderr, "protocol-gen:", err)
		os.Exit(1)
	}
}

func run(inDir, outDir string) error {
	files, err := filepath.Glob(filepath.Join(inDir, "*.protocol.yaml"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no *.protocol.yaml found in %s", inDir)
	}
	sort.Strings(files)

	autoDir := filepath.Join(outDir, "gen", "automaton")
	if err := os.MkdirAll(autoDir, 0o755); err != nil {
		return err
	}
	for _, f := range files {
		if err := generate(f, autoDir); err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(f), err)
		}
	}
	return nil
}

func generate(file, autoDir string) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	p, err := Parse(data)
	if err != nil {
		return err
	}
	if err := CheckDuality(p); err != nil {
		return err
	}
	base := strings.TrimSuffix(filepath.Base(file), ".protocol.yaml")
	for _, role := range p.Roles {
		a, err := Project(p, role)
		if err != nil {
			return err
		}
		if err := WellFormed(a); err != nil {
			return err
		}
		out, err := EmitAutomaton(a)
		if err != nil {
			return err
		}
		dst := filepath.Join(autoDir, fmt.Sprintf("%s.%s.json", base, role))
		if err := os.WriteFile(dst, out, 0o644); err != nil {
			return err
		}
	}
	return nil
}
