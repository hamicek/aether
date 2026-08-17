// Command account is a domain-neutral demo of event-sourced rebuild: a bank account whose
// balance survives a restart by replaying its event log, without any in-memory snapshot.
//
// In init it rebuilds the balance from the event log; each deposit/withdraw appends an event
// and updates the balance. With a persistent JetStream (store_dir here), the balance is
// reconstructed after `aether up` is stopped and started again.
//
//	aether cast account deposit  '{"delta": 100}'
//	aether cast account withdraw '{"delta": 30}'
//	aether call account balance                     # -> 70   (and still 70 after a restart)
package main

import (
	"encoding/json"
	"log"

	"github.com/hamicek/aether/sdk/go/thrall"
)

type account struct {
	Balance int
}

type delta struct {
	Delta int `json:"delta"`
}

// apply folds one event (a signed delta) into the balance. Used both to rebuild in init and to
// update live; keeping it in one place keeps replay and live handling consistent.
func apply(payload json.RawMessage, acc account) (account, error) {
	var d delta
	if err := json.Unmarshal(payload, &d); err != nil {
		return acc, err
	}
	acc.Balance += d.Delta
	return acc, nil
}

func main() {
	def := thrall.Def[account]{
		Name: "account",
		Init: func(ctx *thrall.Ctx) (account, error) {
			// Rebuild the balance by replaying the event log ("log is truth, state is a projection").
			acc, err := thrall.Rebuild(ctx, account{}, apply)
			if err != nil {
				return acc, err
			}
			ctx.Log.Info("rebuilt from event log", "balance", acc.Balance)
			return acc, nil
		},
		HandleCast: map[string]thrall.CastFn[account]{
			"deposit":  move(+1),
			"withdraw": move(-1),
		},
		HandleCall: map[string]thrall.CallFn[account]{
			"balance": func(_ json.RawMessage, acc account, _ *thrall.Ctx) (any, account, error) {
				return acc.Balance, acc, nil
			},
		},
	}
	if err := thrall.Start(def); err != nil {
		log.Fatal(err)
	}
}

// move builds a cast handler that appends a signed event and updates the balance. sign is +1 for
// deposit, -1 for withdraw.
func move(sign int) thrall.CastFn[account] {
	return func(payload json.RawMessage, acc account, ctx *thrall.Ctx) (account, error) {
		var d delta
		if err := json.Unmarshal(payload, &d); err != nil {
			return acc, err
		}
		event := delta{Delta: sign * d.Delta}
		// Command-key: key the append on the message id so a redelivered cast (same envelope)
		// does not double-count. A non-idempotent event like a signed delta needs this - the
		// fold alone cannot tell a replayed event from a genuine second one.
		if err := ctx.Append(event, thrall.DedupKey(ctx.MsgID)); err != nil { // persist first (the log is the truth)
			return acc, err
		}
		acc.Balance += event.Delta
		ctx.Log.Info("balance changed", "delta", event.Delta, "balance", acc.Balance)
		return acc, nil
	}
}
