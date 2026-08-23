package ether

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strconv"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

// securedServerOptions builds the embedded server options for a network-exposed, secured bus:
// it binds Listen, presents the configured server certificate (server-side TLS), and admits only
// the single nkey identity derived from NkeySeed. JetStream stays on (durable mailbox / KV
// registry), exactly as on the loopback default. The three-role split and mTLS are later
// increments; here one identity is shared by the lord, thralls and the operator CLI.
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

	pub, err := authorizedNkey(sec.NkeySeed)
	if err != nil {
		return nil, err
	}

	return &natsserver.Options{
		Host:      host,
		Port:      port,
		JetStream: true,
		StoreDir:  storeDir,
		NoSigs:    true,
		Nkeys:     []*natsserver.NkeyUser{{Nkey: pub}},
		TLS:       true,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	}, nil
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
