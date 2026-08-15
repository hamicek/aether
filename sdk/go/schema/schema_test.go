package schema

import (
	"strings"
	"testing"
)

const measurementSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["siteId", "metric", "value", "ts"],
  "properties": {
    "siteId": { "type": "string", "minLength": 1 },
    "metric": { "type": "string", "enum": ["voltage", "current", "temperature"] },
    "value":  { "type": "number" },
    "unit":   { "type": "string" },
    "ts":     { "type": "integer" }
  }
}`

type measurement struct {
	SiteID string  `json:"siteId"`
	Metric string  `json:"metric"`
	Value  float64 `json:"value"`
	Unit   string  `json:"unit,omitempty"`
	TS     int64   `json:"ts"`
}

func TestValidateAcceptsValidPayload(t *testing.T) {
	body := []byte(`{"siteId":"s-1","metric":"voltage","value":231.4,"ts":1700000000000}`)
	if err := Validate([]byte(measurementSchema), body); err != nil {
		t.Fatalf("valid payload rejected: %v", err)
	}
}

func TestValidateReportsFieldPath(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		wantIn  string // substring expected in the offending path
	}{
		{"missing required", `{"siteId":"s-1","metric":"voltage","value":1}`, ""},          // root: required ts
		{"wrong type", `{"siteId":"s-1","metric":"voltage","value":"hot","ts":1}`, "/value"}, // value must be number
		{"out of enum", `{"siteId":"s-1","metric":"pressure","value":1,"ts":1}`, "/metric"},  // metric not in enum
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate([]byte(measurementSchema), []byte(tc.payload))
			if err == nil {
				t.Fatal("invalid payload accepted")
			}
			ve, ok := err.(*ValidationError)
			if !ok {
				t.Fatalf("error is %T, want *ValidationError", err)
			}
			if len(ve.Problems) == 0 {
				t.Fatal("ValidationError carries no problems")
			}
			found := false
			for _, p := range ve.Problems {
				if strings.Contains(p.Path, tc.wantIn) {
					found = true
				}
			}
			if !found {
				t.Fatalf("no problem path contains %q; got %+v", tc.wantIn, ve.Problems)
			}
		})
	}
}

func TestDecodeReturnsTypedValue(t *testing.T) {
	body := []byte(`{"siteId":"s-1","metric":"current","value":12.5,"unit":"A","ts":1700000000000}`)
	m, err := Decode[measurement]([]byte(measurementSchema), body)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if m.SiteID != "s-1" || m.Metric != "current" || m.Value != 12.5 || m.Unit != "A" || m.TS != 1700000000000 {
		t.Fatalf("decoded value wrong: %+v", m)
	}
}

func TestDecodeRejectsInvalidBeforeUnmarshal(t *testing.T) {
	body := []byte(`{"siteId":"","metric":"voltage","value":1,"ts":1}`) // siteId minLength 1
	if _, err := Decode[measurement]([]byte(measurementSchema), body); err == nil {
		t.Fatal("decode accepted a payload that violates the schema")
	}
}

func TestCompileReusableSchema(t *testing.T) {
	s, err := Compile([]byte(measurementSchema))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	// A compiled schema is reusable across payloads without recompiling.
	if err := s.Validate([]byte(`{"siteId":"s-1","metric":"voltage","value":1,"ts":1}`)); err != nil {
		t.Fatalf("valid payload rejected by compiled schema: %v", err)
	}
	if err := s.Validate([]byte(`{"siteId":"s-1","metric":"voltage","ts":1}`)); err == nil {
		t.Fatal("compiled schema accepted a payload missing a required field")
	}
}

func TestBadSchemaSurfacesError(t *testing.T) {
	if _, err := Compile([]byte(`{"type": 123}`)); err == nil {
		t.Fatal("a malformed schema compiled without error")
	}
}
