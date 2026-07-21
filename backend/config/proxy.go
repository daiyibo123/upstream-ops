package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

const (
	ProxyTargetChatGPTPool       = "pool:chatgpt"
	ProxyTargetGrokPool          = "pool:grok"
	ProxyTargetGPTPoolChannel    = "fixed-channel:gpt"
	ProxyTargetGrokPoolChannel   = "fixed-channel:grok"
)

func (p ProxyConfig) URL() (string, error) {
	protocol := strings.ToLower(strings.TrimSpace(p.Protocol))
	if protocol == "" {
		protocol = "http"
	}
	switch protocol {
	case "http", "https", "socks5":
	default:
		return "", fmt.Errorf("unsupported proxy protocol: %s", p.Protocol)
	}

	host := strings.TrimSpace(p.Host)
	if host == "" {
		return "", errors.New("proxy host is required")
	}
	if p.Port <= 0 {
		return "", errors.New("proxy port is required")
	}

	u := url.URL{
		Scheme: protocol,
		Host:   net.JoinHostPort(host, strconv.Itoa(p.Port)),
	}
	username := strings.TrimSpace(p.Username)
	if username != "" || p.Password != "" {
		u.User = url.UserPassword(username, p.Password)
	}
	return u.String(), nil
}

func (p ProxyConfig) ActiveURL() (string, error) {
	if !p.Enabled {
		return "", nil
	}
	return p.URL()
}

// AppliesTo implements the persisted selection semantics: disabled means
// direct; enabled with no selected target means global proxy; otherwise any
// matching stable target enables the proxy for that request.
func (p ProxyConfig) AppliesTo(targets ...string) bool {
	if !p.Enabled {
		return false
	}
	if len(p.SelectedTargets) == 0 {
		return true
	}
	wanted := make(map[string]struct{}, len(p.SelectedTargets))
	for _, target := range p.SelectedTargets {
		if target = strings.ToLower(strings.TrimSpace(target)); target != "" {
			wanted[target] = struct{}{}
		}
	}
	for _, target := range targets {
		if _, ok := wanted[strings.ToLower(strings.TrimSpace(target))]; ok {
			return true
		}
	}
	return false
}

func ProxyChannelTarget(id uint) string {
	return "channel:" + strconv.FormatUint(uint64(id), 10)
}

func CleanProxyTargets(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
