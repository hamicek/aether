// Package registry holds a name -> state map in a NATS KV bucket ("aether_registry").
// Deliberately KV, not an in-memory map - it works the same in embedded and external
// mode and in a cluster it is visible across nodes (even for `aether ps` from another process).
package registry

import (
	"encoding/json"
	"errors"

	"github.com/nats-io/nats.go"
)

// Bucket is the name of the registry KV bucket.
const Bucket = "aether_registry"

// Entry = a record for a single thrall. Beyond liveness (pid/node/status), it carries the thrall's
// self-description: the operations it answers and its self-declared version (from the heartbeat),
// the reason of its most recent failure, and the deployment metadata the operator declared in the
// manifest. The descriptive fields are optional so a thrall that reports nothing keeps a minimal
// record and the KV JSON stays readable.
type Entry struct {
	PID       int    `json:"pid"`
	Node      string `json:"node"`
	Status    string `json:"status"` // starting | ready | down | stale
	UpdatedMs int64  `json:"updated_ms"`

	Version         string            `json:"version,omitempty"`
	ExpectedVersion string            `json:"expected_version,omitempty"`
	CallOps         []string          `json:"call_ops,omitempty"`
	CastOps         []string          `json:"cast_ops,omitempty"`
	LastError       string            `json:"last_error,omitempty"`
	LastErrorMs     int64             `json:"last_error_ms,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// NamedEntry = an Entry together with its key (the thrall's name) - for listing.
type NamedEntry struct {
	Name string
	Entry
}

// Registry wraps the registry KV bucket.
type Registry struct {
	kv nats.KeyValue
}

// Open creates/opens the registry KV bucket over the given NATS connection.
func Open(nc *nats.Conn) (*Registry, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	kv, err := js.KeyValue(Bucket)
	if err != nil {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: Bucket})
		if err != nil {
			return nil, err
		}
	}
	return &Registry{kv: kv}, nil
}

// Set writes/updates a thrall's state.
func (r *Registry) Set(name string, e Entry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = r.kv.Put(name, data)
	return err
}

// Get reads a thrall's state; ok=false if the key does not exist.
func (r *Registry) Get(name string) (Entry, bool, error) {
	kve, err := r.kv.Get(name)
	if errors.Is(err, nats.ErrKeyNotFound) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, err
	}
	var e Entry
	if err := json.Unmarshal(kve.Value(), &e); err != nil {
		return Entry{}, false, err
	}
	return e, true, nil
}

// List returns all registry records.
func (r *Registry) List() ([]NamedEntry, error) {
	keys, err := r.kv.Keys()
	if errors.Is(err, nats.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]NamedEntry, 0, len(keys))
	for _, k := range keys {
		e, ok, err := r.Get(k)
		if err != nil || !ok {
			continue
		}
		out = append(out, NamedEntry{Name: k, Entry: e})
	}
	return out, nil
}
