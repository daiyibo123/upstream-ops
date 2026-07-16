package config

import (
	"path/filepath"
	"testing"
)

func TestDefaultUpstreamUserAgentMatchesCurrentCodexCLI(t *testing.T) {
	const want = "codex_cli_rs/0.144.1 (Ubuntu 22.4.0; x86_64) xterm-256color"
	if DefaultUpstreamUserAgent != want {
		t.Fatalf("default user agent = %q, want %q", DefaultUpstreamUserAgent, want)
	}
}

func TestLoadAppliesUpstreamDefaults(t *testing.T) {
	cfg, err := LoadFile(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.Upstream.TimeoutSeconds != DefaultUpstreamTimeoutSeconds {
		t.Fatalf("timeout seconds = %d", cfg.Upstream.TimeoutSeconds)
	}
	if cfg.Upstream.UserAgent != DefaultUpstreamUserAgent {
		t.Fatalf("user agent = %q", cfg.Upstream.UserAgent)
	}
}

func TestUpstreamConfigWithDefaultsKeepsCustomUserAgent(t *testing.T) {
	cfg := UpstreamConfig{
		TimeoutSeconds: 0,
		UserAgent:      "custom-agent",
	}.WithDefaults()
	if cfg.TimeoutSeconds != DefaultUpstreamTimeoutSeconds {
		t.Fatalf("timeout seconds = %d", cfg.TimeoutSeconds)
	}
	if cfg.UserAgent != "custom-agent" {
		t.Fatalf("user agent = %q", cfg.UserAgent)
	}
}

func TestUpstreamConfigWithDefaultsMigratesLegacyDefaultUserAgent(t *testing.T) {
	cfg := UpstreamConfig{UserAgent: " upstream-ops/0.1 "}.WithDefaults()
	if cfg.UserAgent != DefaultUpstreamUserAgent {
		t.Fatalf("migrated user agent = %q", cfg.UserAgent)
	}
}
