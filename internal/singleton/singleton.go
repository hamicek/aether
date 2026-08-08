// Package singleton implements a distributed lock over NATS KV - so that a thrall
// with scope="singleton" runs only once across the whole cluster, even when several
// lords run against it (multi-node). The lock rests on two primitives:
//
//   - kv.Create  = atomic "create only if it does not exist" (CAS on non-existence) -> acquire
//   - bucket TTL = when the holder stops renewing (crashed), the key expires -> failover
package singleton

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	Bucket = "aether_singletons"
	ttl    = 3 * time.Second // the key expires when the holder does not renew it -> failover
)

// Manager opens the KV bucket of locks.
type Manager struct {
	kv nats.KeyValue
}

// Lock is a held lock for a single name; it keeps the revision for CAS renewal.
type Lock struct {
	kv     nats.KeyValue
	key    string
	holder string
	rev    uint64
}

type record struct {
	Holder string `json:"holder"`
	TS     int64  `json:"ts"`
}

// Open opens/creates the bucket of locks (with TTL).
func Open(nc *nats.Conn) (*Manager, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	kv, err := js.KeyValue(Bucket)
	if err != nil {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: Bucket, TTL: ttl, History: 1})
		if err != nil {
			return nil, err
		}
	}
	return &Manager{kv: kv}, nil
}

// TryAcquire attempts to acquire the lock atomically. ok=false means another lord holds it.
func (m *Manager) TryAcquire(key, holder string) (*Lock, bool, error) {
	val, _ := json.Marshal(record{Holder: holder, TS: time.Now().UnixMilli()})
	rev, err := m.kv.Create(key, val)
	if errors.Is(err, nats.ErrKeyExists) {
		return nil, false, nil // held by someone else
	}
	if err != nil {
		return nil, false, err
	}
	return &Lock{kv: m.kv, key: key, holder: holder, rev: rev}, true, nil
}

// Renew renews the lock (CAS on the current revision) and resets the TTL. An error = the lock was lost.
func (l *Lock) Renew() error {
	val, _ := json.Marshal(record{Holder: l.holder, TS: time.Now().UnixMilli()})
	rev, err := l.kv.Update(l.key, val, l.rev)
	if err != nil {
		return err
	}
	l.rev = rev
	return nil
}

// Release releases the lock (immediately, without waiting for the TTL).
func (l *Lock) Release() error {
	return l.kv.Purge(l.key)
}
