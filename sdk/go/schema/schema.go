// Package schema is an opt-in payload-contract helper for the Go SDK: validate a JSON
// payload against a JSON Schema at an application boundary, and decode it into a native
// type in one step.
//
// It is deliberately outside the transport: the runtime and internal/wire stay untyped,
// the payload stays json.RawMessage on the wire, and nothing here is wired into call/cast.
// The application calls Validate/Decode explicitly, at the boundary it owns (an HTTP
// ingress, a driver->BFF hand-off, a frontend command). See PAYLOAD-CONTRACT.md (AE-042).
package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Schema is a compiled JSON Schema ready to validate payloads. It is safe for concurrent
// use; reuse one across messages rather than recompiling per message.
type Schema struct {
	compiled *jsonschema.Schema
}

// Compile parses and compiles a JSON Schema document (draft 2020-12).
func Compile(document []byte) (*Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(document))
	if err != nil {
		return nil, fmt.Errorf("schema: parse document: %w", err)
	}
	c := jsonschema.NewCompiler()
	const loc = "mem://schema"
	if err := c.AddResource(loc, doc); err != nil {
		return nil, fmt.Errorf("schema: add resource: %w", err)
	}
	compiled, err := c.Compile(loc)
	if err != nil {
		return nil, fmt.Errorf("schema: compile: %w", err)
	}
	return &Schema{compiled: compiled}, nil
}

// schemaCache memoizes compiled schemas by document content, so the convenience
// Validate/Decode functions parse a given schema only once across all messages.
var schemaCache sync.Map // string(document) -> cacheEntry

type cacheEntry struct {
	schema *Schema
	err    error
}

func cached(document []byte) (*Schema, error) {
	key := string(document)
	if v, ok := schemaCache.Load(key); ok {
		e := v.(cacheEntry)
		return e.schema, e.err
	}
	s, err := Compile(document)
	schemaCache.Store(key, cacheEntry{schema: s, err: err})
	return s, err
}

// Validate checks payload against schema. The schema is compiled once and cached by
// content, so repeated calls with the same schema do not re-parse it. On a schema
// violation it returns a *ValidationError listing each offending field path.
func Validate(document, payload []byte) error {
	s, err := cached(document)
	if err != nil {
		return err
	}
	return s.Validate(payload)
}

// Validate checks payload against the compiled schema.
func (s *Schema) Validate(payload []byte) error {
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("schema: parse payload: %w", err)
	}
	if err := s.compiled.Validate(inst); err != nil {
		var ve *jsonschema.ValidationError
		if errors.As(err, &ve) {
			return &ValidationError{Problems: flatten(ve)}
		}
		return err
	}
	return nil
}

// Decode validates payload against schema and, on success, unmarshals it into T. It is
// the ergonomic boundary call: one step yields a trusted, typed value.
func Decode[T any](document, payload []byte) (T, error) {
	s, err := cached(document)
	if err != nil {
		var zero T
		return zero, err
	}
	return DecodeWith[T](s, payload)
}

// DecodeWith is Decode against a pre-compiled schema, for hot paths that reuse a Schema.
func DecodeWith[T any](s *Schema, payload []byte) (T, error) {
	var out T
	if err := s.Validate(payload); err != nil {
		return out, err
	}
	if err := json.Unmarshal(payload, &out); err != nil {
		return out, fmt.Errorf("schema: decode: %w", err)
	}
	return out, nil
}

// Problem is one schema violation: the JSON Pointer path to the offending value (empty
// for the document root) and a human-readable message.
type Problem struct {
	Path    string
	Message string
}

// ValidationError reports one or more schema violations in a payload.
type ValidationError struct {
	Problems []Problem
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Problems))
	for i, p := range e.Problems {
		path := p.Path
		if path == "" {
			path = "(root)"
		}
		parts[i] = path + ": " + p.Message
	}
	return "payload does not match schema: " + strings.Join(parts, "; ")
}

// flatten walks the basic output tree and collects the leaf errors, each carrying the
// instance location of the offending value.
func flatten(ve *jsonschema.ValidationError) []Problem {
	var problems []Problem
	var walk func(u *jsonschema.OutputUnit)
	walk = func(u *jsonschema.OutputUnit) {
		if u.Error != nil {
			problems = append(problems, Problem{Path: u.InstanceLocation, Message: u.Error.String()})
		}
		for i := range u.Errors {
			walk(&u.Errors[i])
		}
	}
	walk(ve.BasicOutput())
	return problems
}
