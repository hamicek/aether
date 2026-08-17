// Command writer demonstrates the write-side fencing token pattern: a singleton thrall stamps
// its fencing epoch (ctx.SingletonEpoch) on every write to a resource, and the resource rejects
// a write carrying a lower epoch than it has already seen. Singleton fencing alone only bounds
// LIVENESS overlap (see DESIGN 14); this is how you get strict single-writer against a resource.
//
// The "resource" here is a NATS KV bucket holding {value, epoch}; a real one would be a PLC, a
// driver, or a DB enforcing the same monotonic-epoch check.
//
//	aether call writer write       '{"value": "A"}'   # accepted, stored with the live epoch
//	aether call writer write-stale  '{"value": "B"}'  # a simulated old instance -> fenced (rejected)
//	aether call writer read                            # -> {"value":"A","epoch":N}
package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"

	"github.com/hamicek/aether/sdk/go/thrall"
)

const resourceBucket = "resource"
const resourceKey = "reading"

// stored is the resource record: a value plus the epoch of the writer that last wrote it.
type stored struct {
	Value string `json:"value"`
	Epoch uint64 `json:"epoch"`
}

// writeFenced is the resource-side guard: it accepts the write only if epoch >= the epoch already
// stored (monotonic), so a stale writer (a lower epoch) is rejected. Returns whether it stored.
func writeFenced(kv nats.KeyValue, value string, epoch uint64) (bool, error) {
	if cur, err := kv.Get(resourceKey); err == nil {
		var prev stored
		if err := json.Unmarshal(cur.Value(), &prev); err == nil && epoch < prev.Epoch {
			return false, nil // fenced: a newer epoch has already written here
		}
	}
	rec, _ := json.Marshal(stored{Value: value, Epoch: epoch})
	if _, err := kv.Put(resourceKey, rec); err != nil {
		return false, err
	}
	return true, nil
}

func main() {
	var kv nats.KeyValue
	def := thrall.Def[struct{}]{
		Name: "writer",
		Init: func(ctx *thrall.Ctx) (struct{}, error) {
			js, err := ctx.NATS.JetStream()
			if err != nil {
				return struct{}{}, err
			}
			// The resource stub: a KV bucket standing in for an external resource.
			kv, err = js.CreateKeyValue(&nats.KeyValueConfig{Bucket: resourceBucket})
			if err != nil {
				return struct{}{}, err
			}
			ctx.Log.Info("writer ready", "singleton_epoch", ctx.SingletonEpoch)
			return struct{}{}, nil
		},
		HandleCall: map[string]thrall.CallFn[struct{}]{
			// write stamps the live epoch (the honest, correct write).
			"write": func(payload json.RawMessage, s struct{}, ctx *thrall.Ctx) (any, struct{}, error) {
				var in struct{ Value string `json:"value"` }
				_ = json.Unmarshal(payload, &in)
				ok, err := writeFenced(kv, in.Value, ctx.SingletonEpoch)
				return map[string]any{"stored": ok, "epoch": ctx.SingletonEpoch}, s, err
			},
			// write-stale simulates what a fenced-out old instance would do: write with an older
			// epoch. The resource rejects it - the point of the pattern.
			"write-stale": func(payload json.RawMessage, s struct{}, ctx *thrall.Ctx) (any, struct{}, error) {
				var in struct{ Value string `json:"value"` }
				_ = json.Unmarshal(payload, &in)
				stale := ctx.SingletonEpoch - 1
				ok, err := writeFenced(kv, in.Value, stale)
				return map[string]any{"stored": ok, "epoch": stale}, s, err
			},
			"read": func(_ json.RawMessage, s struct{}, _ *thrall.Ctx) (any, struct{}, error) {
				entry, err := kv.Get(resourceKey)
				if err != nil {
					return map[string]any{"value": nil}, s, nil
				}
				var rec stored
				_ = json.Unmarshal(entry.Value(), &rec)
				return rec, s, nil
			},
		},
	}
	if err := thrall.Start(def); err != nil {
		log.Fatal(fmt.Errorf("writer: %w", err))
	}
}
