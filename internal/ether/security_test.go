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
	roleCfg := Config{Mode: "embedded", Security: &Security{
		CA: "sec-ca.pem", LordNkey: "lord.nk", ThrallNkey: "thrall.nk", OperatorNkey: "operator.nk",
	}}
	cases := []struct {
		name             string
		cfg              Config
		role             Role
		wantCA, wantSeed string
	}{
		{"external ignores role", Config{Mode: "external", TLS: TLS{CA: "ext-ca.pem"}, Auth: Auth{NkeySeed: "ext.nk"}}, RoleThrall, "ext-ca.pem", "ext.nk"},
		{"simple tier: any role gets the shared seed", Config{Mode: "embedded", Security: &Security{CA: "sec-ca.pem", NkeySeed: "sec.nk"}}, RoleThrall, "sec-ca.pem", "sec.nk"},
		{"security takes precedence over stray client-side fields", Config{Mode: "embedded", TLS: TLS{CA: "ext-ca.pem"}, Security: &Security{CA: "sec-ca.pem", NkeySeed: "sec.nk"}}, RoleLord, "sec-ca.pem", "sec.nk"},
		{"unsecured embedded returns empty", Config{Mode: "embedded"}, RoleLord, "", ""},
		{"role tier: lord gets the lord seed", roleCfg, RoleLord, "sec-ca.pem", "lord.nk"},
		{"role tier: thrall gets the thrall seed", roleCfg, RoleThrall, "sec-ca.pem", "thrall.nk"},
		{"role tier: operator gets the operator seed", roleCfg, RoleOperator, "sec-ca.pem", "operator.nk"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ca, seed := tc.cfg.ClientCredentials(tc.role)
			if ca != tc.wantCA || seed != tc.wantSeed {
				t.Fatalf("got (%q,%q), want (%q,%q)", ca, seed, tc.wantCA, tc.wantSeed)
			}
		})
	}
}

func TestSecurityValidate(t *testing.T) {
	valid := func() *Security { return fullSecurity() }
	without := func(mut func(*Security)) *Security { s := fullSecurity(); mut(s); return s }
	withRoles := func(mut func(*Security)) *Security {
		s := &Security{
			Listen: "0.0.0.0:4222", TLSCert: "server.pem", TLSKey: "server-key.pem", CA: "ca.pem",
			LordNkey: "lord.nk", ThrallNkey: "thrall.nk", OperatorNkey: "operator.nk",
		}
		mut(s)
		return s
	}

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
		{"valid per-role trio", Config{Mode: "embedded", Security: withRoles(func(*Security) {})}, false},
		{"nkey_seed and per-role are mutually exclusive", Config{Mode: "embedded", Security: withRoles(func(s *Security) { s.NkeySeed = "user.nk" })}, true},
		{"partial per-role trio (missing operator)", Config{Mode: "embedded", Security: withRoles(func(s *Security) { s.OperatorNkey = "" })}, true},
		{"single per-role seed only", Config{Mode: "embedded", Security: without(func(s *Security) { s.NkeySeed = ""; s.ThrallNkey = "thrall.nk" })}, true},
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

func TestSecuritySeedFor(t *testing.T) {
	simple := &Security{NkeySeed: "shared.nk"}
	if simple.roleMode() {
		t.Errorf("simple tier should not be roleMode")
	}
	for _, r := range []Role{RoleLord, RoleThrall, RoleOperator} {
		if got := simple.seedFor(r); got != "shared.nk" {
			t.Errorf("simple seedFor(%s) = %q, want shared.nk", r, got)
		}
	}

	roles := &Security{LordNkey: "l.nk", ThrallNkey: "t.nk", OperatorNkey: "o.nk"}
	if !roles.roleMode() {
		t.Errorf("per-role tier should be roleMode")
	}
	for r, want := range map[Role]string{RoleLord: "l.nk", RoleThrall: "t.nk", RoleOperator: "o.nk"} {
		if got := roles.seedFor(r); got != want {
			t.Errorf("seedFor(%s) = %q, want %q", r, got, want)
		}
	}
}
