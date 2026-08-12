// Package lordlease implements a lord-liveness lease over NATS KV, so that every thrall can
// tell - independently of any direct signal - whether the lord that spawned it is still
// alive, and self-terminate if it is not. It is the crash-case complement to AE-013's
// process-group kill (which only fires on a graceful shutdown, when the lord actively kills
// its children) and the generalization of the AE-027 singleton fencing to all thralls.
//
// The lord establishes one lease per app and renews it periodically; the bucket TTL expires
// the key once the lord stops renewing (a crash). Each thrall carries the lord's epoch and
// verifies it: a mismatch means the lord was replaced (a fast restart), a missing key means
// the lord is gone. There is a single writer per key (one lord per app), so unlike the
// singleton lock no CAS acquire is needed - a plain Put whose revision doubles as the epoch.
package lordlease

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	Bucket = "aether_lords"
	// TTL is the lease's time-to-live: the key expires when the lord stops renewing it (a
	// crash). Thralls derive their fencing lease window from the same value, so a KV hiccup
	// is tolerated for up to TTL before a thrall concludes the lord is gone.
	TTL = 3 * time.Second
)

// Manager opens the KV bucket of lord-liveness leases.
type Manager struct {
	kv nats.KeyValue
}

// Lease is a lord's established liveness lease for one app; it keeps the epoch to republish.
type Lease struct {
	kv     nats.KeyValue
	key    string
	holder string
	epoch  uint64
}

// record is the value stored under the lease key. Epoch is the fencing token: it is set once
// at Establish (to the write revision - monotonic per key across the bucket history) and
// preserved verbatim across renewals. A later lord (a restart) always gets a higher epoch
// than any orphan of the previous lord, so a thrall can tell its lord apart from a successor.
type record struct {
	Holder string `json:"holder"`
	TS     int64  `json:"ts"`
	Epoch  uint64 `json:"epoch"`
}

// Open opens/creates the bucket of lord-liveness leases (with TTL).
func Open(nc *nats.Conn) (*Manager, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	kv, err := js.KeyValue(Bucket)
	if err != nil {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: Bucket, TTL: TTL, History: 1})
		if err != nil {
			return nil, err
		}
	}
	return &Manager{kv: kv}, nil
}

// Establish publishes the lord's liveness lease for key (the app name). The first write's
// revision becomes the epoch: monotonic per key across the bucket history, so a later lord
// always outranks any orphan of the previous one. There is one writer per key (one lord per
// app), so a plain Put is enough - no acquire/CAS.
func (m *Manager) Establish(key, holder string) (*Lease, error) {
	rev, err := m.kv.Put(key, mustRecord(record{Holder: holder, TS: time.Now().UnixMilli()}))
	if err != nil {
		return nil, err
	}
	// Stamp the epoch into the record so readers (thralls) see it.
	if _, err := m.kv.Put(key, mustRecord(record{Holder: holder, TS: time.Now().UnixMilli(), Epoch: rev})); err != nil {
		return nil, err
	}
	return &Lease{kv: m.kv, key: key, holder: holder, epoch: rev}, nil
}

// Epoch returns the fencing token of this lease, injected into every thrall the lord spawns.
func (l *Lease) Epoch() uint64 { return l.epoch }

// Renew republishes the lease (keeping the epoch) and resets the TTL. Unlike the singleton
// lock this is a plain Put, not a CAS: there is a single writer per key, so there is no
// competing renewal to guard against.
func (l *Lease) Renew() error {
	_, err := l.kv.Put(l.key, mustRecord(record{Holder: l.holder, TS: time.Now().UnixMilli(), Epoch: l.epoch}))
	return err
}

// Verify reports whether the lease key still carries the given epoch. ok=false with a nil
// error means the epoch was superseded (a new lord) or the key is gone (the lord is dead); a
// non-nil error means the KV could not be read (the caller cannot conclude either way, and
// should tolerate it until the lease window elapses).
func (m *Manager) Verify(key string, epoch uint64) (ok bool, err error) {
	entry, err := m.kv.Get(key)
	if errors.Is(err, nats.ErrKeyNotFound) {
		return false, nil // expired -> lord gone
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
