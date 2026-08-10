package obs

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestLevelFromEnv(t *testing.T) {
	cases := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"info":    slog.LevelInfo,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"":        slog.LevelInfo, // default
		"nonsense": slog.LevelInfo, // unknown -> default, never silent
		"WARN":    slog.LevelWarn, // case-insensitive
	}
	for in, want := range cases {
		t.Setenv(EnvLogLevel, in)
		if got := LevelFromEnv(); got != want {
			t.Errorf("LevelFromEnv(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestJSONHandlerCarriesAttributes(t *testing.T) {
	t.Setenv(EnvLogFormat, "json")
	t.Setenv(EnvLogLevel, "info")

	var buf bytes.Buffer
	log := NewWithWriter(&buf).With(slog.String("component", "lord"), slog.String("app", "counter"))
	log.Info("thrall ready", slog.String("name", "worker"), slog.Int("pid", 42))

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not JSON: %v (%q)", err, buf.String())
	}
	for k, want := range map[string]any{
		"level": "INFO", "msg": "thrall ready", "component": "lord", "app": "counter", "name": "worker",
	} {
		if got, ok := rec[k]; !ok || got != want {
			t.Errorf("record[%q] = %v (present=%v), want %v", k, got, ok, want)
		}
	}
}

func TestLevelFiltersBelowThreshold(t *testing.T) {
	t.Setenv(EnvLogFormat, "json")
	t.Setenv(EnvLogLevel, "warn")

	var buf bytes.Buffer
	log := NewWithWriter(&buf)
	log.Info("should be dropped")
	log.Warn("should pass")

	out := buf.String()
	if strings.Contains(out, "should be dropped") {
		t.Errorf("info leaked under warn threshold: %q", out)
	}
	if !strings.Contains(out, "should pass") {
		t.Errorf("warn was filtered out: %q", out)
	}
}

func TestTextFormatIsDefault(t *testing.T) {
	t.Setenv(EnvLogFormat, "")
	var buf bytes.Buffer
	NewWithWriter(&buf).Info("hello", slog.String("name", "w"))
	out := buf.String()
	if strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Errorf("default format should be text, got JSON: %q", out)
	}
	if !strings.Contains(out, "name=w") {
		t.Errorf("text output missing attribute: %q", out)
	}
}
