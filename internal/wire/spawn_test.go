package wire

import (
	"encoding/json"
	"testing"
)

// TestSpawnSpecRoundTrip pins the runtime-spawn payload: it must survive a JSON
// round-trip carried inside a ctl envelope's Payload, since the SDK marshals it and the
// lord unmarshals it.
func TestSpawnSpecRoundTrip(t *testing.T) {
	in := SpawnSpec{Name: "worker-1", Cmd: "./bin/worker", Restart: "transient", Durable: true}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out SpawnSpec
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != in {
		t.Fatalf("round-trip: got %+v, want %+v", out, in)
	}
}

// TestSpawnSpecOmitsEmpty keeps the wire minimal: optional fields drop out when unset,
// so a default spawn (permanent, non-durable) carries only name and cmd.
func TestSpawnSpecOmitsEmpty(t *testing.T) {
	data, err := json.Marshal(SpawnSpec{Name: "w", Cmd: "./w"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(data)
	want := `{"name":"w","cmd":"./w"}`
	if got != want {
		t.Fatalf("minimal spawn: got %s, want %s", got, want)
	}
}

// TestSpawnOpsStable pins the op strings carried in the ctl envelope.
func TestSpawnOpsStable(t *testing.T) {
	if OpSpawn != "spawn" || OpStop != "stop" {
		t.Fatalf("ops: got %q/%q, want spawn/stop", OpSpawn, OpStop)
	}
}
