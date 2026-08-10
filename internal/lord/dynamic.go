package lord

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/internal/wire"
)

// handleControl serves the lord's inbound control channel (wire.LordCtl). It runs in a
// NATS callback goroutine; callbacks on a single subject are serialized, so spawn/stop
// requests are handled one at a time. Every reply is a wire reply envelope.
func (l *Lord) handleControl(m *nats.Msg) {
	var e wire.Envelope
	if json.Unmarshal(m.Data, &e) != nil {
		l.respondErr(m, e, "bad_envelope", "control message is not a valid envelope")
		return
	}
	switch e.Op {
	case wire.OpSpawn:
		var spec wire.SpawnSpec
		if json.Unmarshal(e.Payload, &spec) != nil {
			l.respondErr(m, e, "bad_payload", "spawn payload is not a valid SpawnSpec")
			return
		}
		name, err := l.spawnChild(spec)
		if err != nil {
			l.respondErr(m, e, "spawn_failed", err.Error())
			return
		}
		l.respondOK(m, e, wire.SpawnReply{Name: name})
	case wire.OpStop:
		var s wire.StopSpec
		if json.Unmarshal(e.Payload, &s) != nil {
			l.respondErr(m, e, "bad_payload", "stop payload is not a valid StopSpec")
			return
		}
		if err := l.stopChild(s.Name); err != nil {
			l.respondErr(m, e, "stop_failed", err.Error())
			return
		}
		l.respondOK(m, e, wire.SpawnReply{Name: s.Name})
	default:
		l.respondErr(m, e, "unknown_op", "unknown control op: "+e.Op)
	}
}

// spawnChild starts a thrall at runtime (not from the manifest) and puts it under full
// supervision. It is a local, one_for_one supervised child; it never joins a manifest
// group strategy. Returns the child's name once the process is started.
func (l *Lord) spawnChild(spec wire.SpawnSpec) (string, error) {
	if l.stopping() {
		return "", fmt.Errorf("lord is draining")
	}
	if spec.Name == "" || spec.Cmd == "" {
		return "", fmt.Errorf("name and cmd are required")
	}
	restart := spec.Restart
	if restart == "" {
		restart = "permanent"
	}
	ch := &child{
		spec: ThrallSpec{
			Name:     spec.Name,
			Cmd:      spec.Cmd,
			Restart:  restart,
			Scope:    "local",
			Durable:  spec.Durable,
			EventLog: spec.EventLog,
		},
		natsURL:      l.ether.URL(),
		app:          l.manifest.App,
		caPath:       l.manifest.Nats.TLS.CA,
		nkeySeed:     l.manifest.Nats.Auth.NkeySeed,
		hbIntervalMs: int(l.hbCheckEvery.Milliseconds()), // clamped interval the reaper uses
		dynamic:      true,
	}

	// Reserve the name and append atomically, so a second spawn with the same name loses.
	l.childrenMu.Lock()
	for _, c := range l.children {
		if c.spec.Name == spec.Name {
			l.childrenMu.Unlock()
			return "", fmt.Errorf("a child named %q already exists", spec.Name)
		}
	}
	l.children = append(l.children, ch)
	l.childrenMu.Unlock()

	if err := l.provisionChildStreams(ch); err != nil {
		l.removeChild(ch)
		return "", fmt.Errorf("provision streams: %w", err)
	}
	if err := l.startChild(ch); err != nil {
		l.removeChild(ch)
		return "", fmt.Errorf("start: %w", err)
	}
	l.log.Info("dynamically spawned thrall", slog.String("name", spec.Name))
	return spec.Name, nil
}

// stopChild drains and removes a dynamic child at runtime. It refuses static (manifest)
// children - those are owned by the manifest and drained on Stop. Marking retired first
// makes the supervisor loop treat the coming exit as expected, not a crash.
func (l *Lord) stopChild(name string) error {
	l.childrenMu.Lock()
	var target *child
	idx := -1
	for i, c := range l.children {
		if c.spec.Name == name {
			target, idx = c, i
			break
		}
	}
	if target == nil {
		l.childrenMu.Unlock()
		return fmt.Errorf("no child named %q", name)
	}
	if !target.dynamic {
		l.childrenMu.Unlock()
		return fmt.Errorf("%q is a static child (managed by the manifest)", name)
	}
	target.retired.Store(true)
	l.children = append(l.children[:idx], l.children[idx+1:]...)
	l.childrenMu.Unlock()

	target.requestDrain(l.ether.Conn(), defaultGrace)
	l.setStatus(name, 0, "down")
	l.emit("stopped", name, 0)
	l.forgetThrall(name)
	l.log.Info("dynamically stopped thrall", slog.String("name", name))
	return nil
}

// removeChild drops a child from the slice (rollback path when a spawn fails after the
// name was reserved).
func (l *Lord) removeChild(ch *child) {
	l.childrenMu.Lock()
	defer l.childrenMu.Unlock()
	for i, c := range l.children {
		if c == ch {
			l.children = append(l.children[:i], l.children[i+1:]...)
			return
		}
	}
}

func (l *Lord) respondOK(m *nats.Msg, req wire.Envelope, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		l.respondErr(m, req, "marshal_failed", err.Error())
		return
	}
	_ = m.Respond(mustJSON(wire.Envelope{V: 1, ID: req.ID, Kind: wire.KindReply, Status: "ok", Payload: data}))
}

func (l *Lord) respondErr(m *nats.Msg, req wire.Envelope, typ, message string) {
	_ = m.Respond(mustJSON(wire.Envelope{
		V: 1, ID: req.ID, Kind: wire.KindReply, Status: "error",
		Error: &wire.WireError{Type: typ, Message: message},
	}))
}

// mustJSON marshals a reply envelope; a reply envelope is always valid JSON.
func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
