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
	if cfg.Upstream.StreamFirstEventTimeoutSeconds != DefaultStreamFirstEventTimeoutSeconds {
		t.Fatalf("stream first event timeout = %d", cfg.Upstream.StreamFirstEventTimeoutSeconds)
	}
	if cfg.Upstream.HealthProbeTimeoutSeconds != DefaultHealthProbeTimeoutSeconds {
		t.Fatalf("health probe timeout = %d", cfg.Upstream.HealthProbeTimeoutSeconds)
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

func TestUpstreamConfigWithDefaultsFillsTimeoutBudgets(t *testing.T) {
	cfg := UpstreamConfig{}.WithDefaults()
	if cfg.StreamFirstEventTimeoutSeconds != DefaultStreamFirstEventTimeoutSeconds {
		t.Fatalf("stream first event timeout = %d, want %d", cfg.StreamFirstEventTimeoutSeconds, DefaultStreamFirstEventTimeoutSeconds)
	}
	if cfg.HealthProbeTimeoutSeconds != DefaultHealthProbeTimeoutSeconds {
		t.Fatalf("health probe timeout = %d, want %d", cfg.HealthProbeTimeoutSeconds, DefaultHealthProbeTimeoutSeconds)
	}
	// 自定义值必须被保留，不能被默认值覆盖。
	custom := UpstreamConfig{StreamFirstEventTimeoutSeconds: 120, HealthProbeTimeoutSeconds: 45}.WithDefaults()
	if custom.StreamFirstEventTimeoutSeconds != 120 || custom.HealthProbeTimeoutSeconds != 45 {
		t.Fatalf("custom timeout budgets not preserved: %#v", custom)
	}
}

func TestUpstreamConfigWithDefaultsMigratesLegacyDefaultUserAgent(t *testing.T) {
	cfg := UpstreamConfig{UserAgent: " upstream-ops/0.1 "}.WithDefaults()
	if cfg.UserAgent != DefaultUpstreamUserAgent {
		t.Fatalf("migrated user agent = %q", cfg.UserAgent)
	}
}

func TestRouteAffinityWithDefaultsClampsPromoteRatio(t *testing.T) {
	// 未设置（0）时兜底到默认阈值。
	if got := (RouteAffinityConfig{}).WithDefaults().PromoteMinSavingsRatio; got != DefaultRouteAffinityPromoteMinSavingsRatio {
		t.Fatalf("zero promote ratio = %v, want default %v", got, DefaultRouteAffinityPromoteMinSavingsRatio)
	}
	// 越界（>=1）时同样兜底，避免不可达阈值锁死切换。
	if got := (RouteAffinityConfig{PromoteMinSavingsRatio: 1.5}).WithDefaults().PromoteMinSavingsRatio; got != DefaultRouteAffinityPromoteMinSavingsRatio {
		t.Fatalf("out-of-range promote ratio = %v, want default %v", got, DefaultRouteAffinityPromoteMinSavingsRatio)
	}
	// 合法自定义值必须保留。
	if got := (RouteAffinityConfig{PromoteMinSavingsRatio: 0.5}).WithDefaults().PromoteMinSavingsRatio; got != 0.5 {
		t.Fatalf("custom promote ratio not preserved: %v", got)
	}
}

func TestLoadAppliesRouteAffinityDefaults(t *testing.T) {
	cfg, err := LoadFile(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if !cfg.App.RouteAffinity.Enabled {
		t.Fatal("route affinity should default to enabled")
	}
	if cfg.App.RouteAffinity.PromoteMinSavingsRatio != DefaultRouteAffinityPromoteMinSavingsRatio {
		t.Fatalf("promote ratio = %v, want default %v", cfg.App.RouteAffinity.PromoteMinSavingsRatio, DefaultRouteAffinityPromoteMinSavingsRatio)
	}
}
