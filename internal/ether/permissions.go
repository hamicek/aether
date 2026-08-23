package ether

import (
	natsserver "github.com/nats-io/nats-server/v2/server"

	"github.com/hamicek/aether/internal/lordlease"
	"github.com/hamicek/aether/internal/registry"
	"github.com/hamicek/aether/internal/singleton"
	"github.com/hamicek/aether/internal/wire"
)

// rolePermissions returns the NATS subject permissions for a least-privilege role, or nil for the
// lord (which legitimately needs full rights).
//
// The model is deliberately deny-based: allow everything, then subtract the few subjects a role must
// never use. An allow-list would have to enumerate every $JS.> and $KV.> subject the durable
// mailbox, registry and fencing depend on, and would silently break them the moment one was missed;
// the dangerous set is small and specific (driving supervision, forging fencing), so denying it is
// both robust and to the point.
//
// Boundary this enforces: lord vs thrall vs operator. It does NOT isolate one thrall from another -
// a single shared thrall identity cannot be scoped per thrall name, so name-scoped channels
// (aether._lord.<name>.hb/ctl) stay open across thralls. Per-thrall isolation would need per-thrall
// identities, a separate future step.
func rolePermissions(role Role) *natsserver.Permissions {
	// KV write subjects for buckets a client must not forge. KV reads (direct get) go through
	// $JS.API and stay allowed; only the write path publishes to $KV.<bucket>.>.
	fencingKVWrites := []string{
		"$KV." + lordlease.Bucket + ".>",
		"$KV." + singleton.Bucket + ".>",
	}
	// The per-thrall supervision channels (4 tokens: aether._lord.<name>.ctl / .hb). The 3-token
	// LordCtl (wire.LordCtl, aether._lord.ctl) is the thrall->lord inbound channel, handled separately.
	const perThrallCtl = "aether._lord.*.ctl"
	const perThrallHb = "aether._lord.*.hb"

	switch role {
	case RoleThrall:
		// A thrall runs its data plane, beats its own heartbeat and may drive dynamic children via
		// LordCtl, but must not command sibling thralls, forge lifecycle events, or write the fencing
		// leases it only reads.
		deny := append([]string{perThrallCtl, wire.Events}, fencingKVWrites...)
		return &natsserver.Permissions{
			Publish:   &natsserver.SubjectPermission{Allow: []string{">"}, Deny: deny},
			Subscribe: &natsserver.SubjectPermission{Allow: []string{">"}},
		}
	case RoleOperator:
		// An operator calls/casts and observes, but must not drive supervision (LordCtl, per-thrall
		// ctl), pose as a thrall (heartbeats), forge events, or write any runtime KV.
		deny := append([]string{
			wire.LordCtl(), perThrallCtl, perThrallHb, wire.Events,
			"$KV." + registry.Bucket + ".>",
		}, fencingKVWrites...)
		return &natsserver.Permissions{
			Publish:   &natsserver.SubjectPermission{Allow: []string{">"}, Deny: deny},
			Subscribe: &natsserver.SubjectPermission{Allow: []string{">"}},
		}
	default: // RoleLord: full rights.
		return nil
	}
}
