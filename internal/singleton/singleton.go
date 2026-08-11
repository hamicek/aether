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
	epoch  uint64
}

// record is the value stored under the lock key. Epoch is the fencing token: it is set
// once at acquisition (to the create revision - unique and monotonic per key across the
// bucket history) and preserved verbatim across renewals. A later acquisition, even by
// the same holder after the key expired, gets a new, higher epoch, so a thrall can tell
// its ownership generation apart from any successor's.
type record struct {
	Holder string `json:"holder"`
	TS     int64  `json:"ts"`
	Epoch  uint64 `json:"epoch"`
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
// The create revision doubles as the fencing epoch handed out to the thrall.
func (m *Manager) TryAcquire(key, holder string) (*Lock, bool, error) {
	rev, err := m.kv.Create(key, mustRecord(record{Holder: holder, TS: time.Now().UnixMilli()}))
	if errors.Is(err, nats.ErrKeyExists) {
		return nil, false, nil // held by someone else
	}
	if err != nil {
		return nil, false, err
	}
	// Stamp the epoch into the record so readers (the thrall) see it: a follow-up update on
	// the just-created key, still under our revision, cannot race a competing acquire (the
	// key exists, so their Create fails).
	stamped := record{Holder: holder, TS: time.Now().UnixMilli(), Epoch: rev}
	newRev, err := m.kv.Update(key, mustRecord(stamped), rev)
	if err != nil {
		_ = m.kv.Purge(key) // do not leave an epoch-less record behind
		return nil, false, err
	}
	return &Lock{kv: m.kv, key: key, holder: holder, rev: newRev, epoch: rev}, true, nil
}

// Epoch returns the fencing token of this held lock. It is injected into the singleton
// thrall so the thrall can verify, independently of the lord, that it still holds the lock.
func (l *Lock) Epoch() uint64 { return l.epoch }

// Renew renews the lock (CAS on the current revision) and resets the TTL. An error = the lock was lost.
func (l *Lock) Renew() error {
	val := mustRecord(record{Holder: l.holder, TS: time.Now().UnixMilli(), Epoch: l.epoch})
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

// Verify reports whether the lock key still carries the given fencing epoch. ok=false with
// a nil error means the epoch was superseded or the key is gone (lock lost); a non-nil error
// means the KV could not be read (the caller cannot conclude either way).
func (m *Manager) Verify(key string, epoch uint64) (ok bool, err error) {
	entry, err := m.kv.Get(key)
	if errors.Is(err, nats.ErrKeyNotFound) {
		return false, nil // expired or released -> lock lost
	}
	if err != nil {
		return false, err
	}
	var r record
	if err := json.Unmarshal(entry.Value(), &r); err != nil {
		return false, err
	}
	return r.Epoch == epoch, nil
}

func mustRecord(r record) []byte {
	val, err := json.Marshal(r)
	if err != nil {
		panic(err) // a fixed struct of scalars cannot fail to marshal
	}
	return val
}
