package ether

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

// Role identifies which of the three least-privilege identities a client authenticates as. In the
// simple tier all three collapse onto the one shared seed; in the least-privilege tier each has its
// own seed and a role-scoped permission set on the server.
type Role string

const (
	RoleLord     Role = "lord"     // the supervisor: full rights, incl. aether._lord.>
	RoleThrall   Role = "thrall"   // a worker: its data plane, but not the broad supervision control
	RoleOperator Role = "operator" // the CLI / dashboard: call/cast and observe, but not control
)

// securedServerOptions builds the embedded server options for a network-exposed, secured bus: it
// binds Listen, presents the configured server certificate (server-side TLS), and admits the
// authorized nkey identities. JetStream stays on (durable mailbox / KV registry), exactly as on the
// loopback default. In the simple tier there is one full-rights identity; in the least-privilege
// tier there are three, each with a role-scoped permission set (see rolePermissions). mTLS is a
// later opt-in.
func securedServerOptions(sec *Security, storeDir string) (*natsserver.Options, error) {
	host, portStr, err := net.SplitHostPort(sec.Listen)
	if err != nil {
		return nil, fmt.Errorf("nats.security.listen %q: %w", sec.Listen, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("nats.security.listen %q: port must be numeric: %w", sec.Listen, err)
	}

	cert, err := tls.LoadX509KeyPair(sec.TLSCert, sec.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("nats.security: load server cert/key: %w", err)
	}

	users, err := securedNkeyUsers(sec)
	if err != nil {
		return nil, err
	}

	return &natsserver.Options{
		Host:      host,
		Port:      port,
		JetStream: true,
		StoreDir:  storeDir,
		NoSigs:    true,
		Nkeys:     users,
		TLS:       true,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	}, nil
}

// securedNkeyUsers builds the authorized nkey users: one full-rights user in the simple tier, or one
// role-scoped user per role (lord / thrall / operator) in the least-privilege tier.
func securedNkeyUsers(sec *Security) ([]*natsserver.NkeyUser, error) {
	if !sec.roleMode() {
		pub, err := authorizedNkey(sec.NkeySeed)
		if err != nil {
			return nil, err
		}
		return []*natsserver.NkeyUser{{Nkey: pub}}, nil
	}
	var users []*natsserver.NkeyUser
	for _, r := range []Role{RoleLord, RoleThrall, RoleOperator} {
		pub, err := authorizedNkey(sec.seedFor(r))
		if err != nil {
			return nil, err
		}
		users = append(users, &natsserver.NkeyUser{Nkey: pub, Permissions: rolePermissions(r)})
	}
	return users, nil
}

// dialableURL rewrites a wildcard bind host to loopback so a local process can actually dial
// it. A server bound to 0.0.0.0 (or ::, or an unspecified host) reports that address in its
// client URL, but 0.0.0.0 is a bind-all, not a destination; the lord and its thralls run on the
// same host and reach it over 127.0.0.1. A concrete bind host is left untouched (clients dial it
// as configured, and the server certificate is expected to cover it).
func dialableURL(clientURL string) string {
	u, err := url.Parse(clientURL)
	if err != nil {
		return clientURL
	}
	switch u.Hostname() {
	case "0.0.0.0", "::", "":
		u.Host = net.JoinHostPort("127.0.0.1", u.Port())
		return u.String()
	default:
		return clientURL
	}
}

// authorizedNkey reads an nkey seed file and returns the public key of the identity it
// authorizes. The seed itself stays on disk; only its public half goes into the server
// config. Surrounding whitespace/newlines in the file are tolerated.
func authorizedNkey(seedPath string) (string, error) {
	seed, err := os.ReadFile(seedPath)
	if err != nil {
		return "", fmt.Errorf("nats.security.nkey_seed %q: %w", seedPath, err)
	}
	kp, err := nkeys.FromSeed(bytes.TrimSpace(seed))
	if err != nil {
		return "", fmt.Errorf("nats.security.nkey_seed %q: %w", seedPath, err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return "", fmt.Errorf("nats.security.nkey_seed %q: derive public key: %w", seedPath, err)
	}
	return pub, nil
}
