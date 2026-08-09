// Package natstest spins up an embedded NATS server secured with nkey auth and
// server TLS, for tests that need to prove aether authenticates to a secured bus.
// It generates an ephemeral nkey and a self-signed certificate into the test's temp
// dir and returns the paths a client needs (CA and nkey seed). It is test-only
// support code, imported only from _test.go files.
package natstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

// Secured holds the connection details for a secured embedded server.
type Secured struct {
	URL      string // bus address (the server requires TLS + nkey to connect)
	CAFile   string // PEM CA the client verifies the server against
	SeedFile string // nkey seed file the client authenticates with
}

// SecuredServer starts an embedded NATS server that requires nkey auth over server
// TLS, with JetStream enabled (durable mailbox / KV registry). The server and its
// storage are torn down via t.Cleanup. The returned CA and seed files are what a
// client must present; connecting without them is rejected.
func SecuredServer(t testing.TB) Secured {
	t.Helper()

	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("create nkey user: %v", err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatalf("nkey public key: %v", err)
	}
	seed, err := kp.Seed()
	if err != nil {
		t.Fatalf("nkey seed: %v", err)
	}

	dir := t.TempDir()
	seedFile := filepath.Join(dir, "user.nk")
	if err := os.WriteFile(seedFile, seed, 0o600); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	caFile, tlsCert := selfSignedCert(t, dir)

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  filepath.Join(dir, "js"),
		NoSigs:    true,
		Nkeys:     []*natsserver.NkeyUser{{Nkey: pub}},
		TLS:       true,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{tlsCert}, MinVersion: tls.VersionTLS12},
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new secured server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatalf("secured NATS did not start in time")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	return Secured{URL: srv.ClientURL(), CAFile: caFile, SeedFile: seedFile}
}

// selfSignedCert writes a self-signed certificate (valid for 127.0.0.1 / localhost)
// to dir and returns the CA file path and the parsed cert/key for the server. The
// self-signed cert doubles as the CA the client trusts.
func selfSignedCert(t testing.TB, dir string) (caFile string, cert tls.Certificate) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "aether-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	caFile = filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, certPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	cert, err = tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("build tls cert: %v", err)
	}
	return caFile, cert
}
