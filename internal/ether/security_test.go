package ether

import "testing"

// fullSecurity is a fully specified [nats.security] block. Paths need not exist for
// validation (it checks presence and the listen format, not the files).
func fullSecurity() *Security {
	return &Security{
		Listen:   "0.0.0.0:4222",
		TLSCert:  "server.pem",
		TLSKey:   "server-key.pem",
		CA:       "ca.pem",
		NkeySeed: "user.nk",
	}
}

func TestClientCredentials(t *testing.T) {
	cases := []struct {
		name             string
		cfg              Config
		wantCA, wantSeed string
	}{
		{
			"external uses client-side tls/auth",
			Config{Mode: "external", TLS: TLS{CA: "ext-ca.pem"}, Auth: Auth{NkeySeed: "ext.nk"}},
			"ext-ca.pem", "ext.nk",
		},
		{
			"secured embedded uses its own security fields",
			Config{Mode: "embedded", Security: &Security{CA: "sec-ca.pem", NkeySeed: "sec.nk"}},
			"sec-ca.pem", "sec.nk",
		},
		{
			"security takes precedence over stray client-side fields",
			Config{Mode: "embedded", TLS: TLS{CA: "ext-ca.pem"}, Security: &Security{CA: "sec-ca.pem", NkeySeed: "sec.nk"}},
			"sec-ca.pem", "sec.nk",
		},
		{"unsecured embedded returns empty", Config{Mode: "embedded"}, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ca, seed := tc.cfg.ClientCredentials()
			if ca != tc.wantCA || seed != tc.wantSeed {
				t.Fatalf("got (%q,%q), want (%q,%q)", ca, seed, tc.wantCA, tc.wantSeed)
			}
		})
	}
}

func TestSecurityValidate(t *testing.T) {
	valid := func() *Security { return fullSecurity() }
	without := func(mut func(*Security)) *Security { s := fullSecurity(); mut(s); return s }

	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"valid embedded security", Config{Mode: "embedded", Security: valid()}, false},
		{"nil security is plain embedded", Config{Mode: "embedded"}, false},
		{"missing listen", Config{Mode: "embedded", Security: without(func(s *Security) { s.Listen = "" })}, true},
		{"listen without port", Config{Mode: "embedded", Security: without(func(s *Security) { s.Listen = "0.0.0.0" })}, true},
		{"cert without key", Config{Mode: "embedded", Security: without(func(s *Security) { s.TLSKey = "" })}, true},
		{"key without cert", Config{Mode: "embedded", Security: without(func(s *Security) { s.TLSCert = "" })}, true},
		{"missing ca", Config{Mode: "embedded", Security: without(func(s *Security) { s.CA = "" })}, true},
		{"missing nkey seed", Config{Mode: "embedded", Security: without(func(s *Security) { s.NkeySeed = "" })}, true},
		{"security in external mode", Config{Mode: "external", URL: "nats://x:4222", Security: valid()}, true},
		{"security with leaf", Config{
			Mode:     "embedded",
			Security: valid(),
			Leaf:     &Leaf{Remote: "nats-leaf://hub:7422", Site: "SITE_A", Domain: "sa"},
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr && err == nil {
				t.Fatalf("expected validation error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
