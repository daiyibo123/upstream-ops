package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestProxyTargetsForGroupCoversPoolAndFamilyScopes(t *testing.T) {
	cases := []struct {
		name   string
		group  storage.UpstreamGroupKey
		wanted string
	}{
		{name: "gpt channel", group: storage.UpstreamGroupKey{ClientFormat: "openai"}, wanted: config.ProxyTargetGPTPoolChannel},
		{name: "grok channel", group: storage.UpstreamGroupKey{ClientFormat: "grok"}, wanted: config.ProxyTargetGrokPoolChannel},
		{name: "legacy grok any", group: storage.UpstreamGroupKey{ClientFormat: "any", ChannelName: "xai relay"}, wanted: config.ProxyTargetGrokPoolChannel},
		{name: "chatgpt pool", group: storage.UpstreamGroupKey{ChannelType: storage.ChannelTypeChatGPTPool}, wanted: config.ProxyTargetChatGPTPool},
		{name: "grok pool", group: storage.UpstreamGroupKey{ChannelType: storage.ChannelTypeGrokPool}, wanted: config.ProxyTargetGrokPool},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			found := false
			for _, target := range proxyTargetsForGroup(&tc.group) {
				if target == tc.wanted {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("targets=%v, missing %q", proxyTargetsForGroup(&tc.group), tc.wanted)
			}
		})
	}
}

func TestProxyTargetsKeepOAuthPoolsSeparateFromChannelFamilies(t *testing.T) {
	chatPool := proxyTargetsForGroup(&storage.UpstreamGroupKey{ChannelType: storage.ChannelTypeChatGPTPool})
	grokPool := proxyTargetsForGroup(&storage.UpstreamGroupKey{ChannelType: storage.ChannelTypeGrokPool})
	if len(chatPool) != 1 || chatPool[0] != config.ProxyTargetChatGPTPool {
		t.Fatalf("chatgpt pool targets=%v", chatPool)
	}
	if len(grokPool) != 1 || grokPool[0] != config.ProxyTargetGrokPool {
		t.Fatalf("grok pool targets=%v", grokPool)
	}
	svc := &Service{}
	svc.UpdateProxyConfig(config.ProxyConfig{Enabled: true, Protocol: "http", Host: "127.0.0.1", Port: 18080, SelectedTargets: []string{config.ProxyTargetGPTPoolChannel}})
	client, err := svc.httpClientFor(context.Background(), &storage.Channel{ID: 1, Type: storage.ChannelTypeChatGPTPool}, &storage.UpstreamGroupKey{ChannelType: storage.ChannelTypeChatGPTPool})
	if err != nil {
		t.Fatalf("fixed pool client: %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, "https://example.com/v1/models", nil)
	proxyURL, err := client.Transport.(*http.Transport).Proxy(request)
	if err != nil || proxyURL != nil {
		t.Fatalf("fixed OAuth pool unexpectedly used channel-family proxy: url=%v err=%v", proxyURL, err)
	}
}

func TestHTTPClientForUsesFamilyScopeAndCanDisableProxy(t *testing.T) {
	svc := &Service{}
	channel := &storage.Channel{ID: 11, Type: storage.ChannelTypeSub2API}
	gpt := &storage.UpstreamGroupKey{ClientFormat: "openai"}
	svc.UpdateProxyConfig(config.ProxyConfig{Enabled: true, Protocol: "http", Host: "127.0.0.1", Port: 18080, SelectedTargets: []string{config.ProxyTargetGPTPoolChannel}})
	client, err := svc.httpClientFor(context.Background(), channel, gpt)
	if err != nil {
		t.Fatalf("httpClientFor: %v", err)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type=%T", client.Transport)
	}
	request := httptest.NewRequest(http.MethodGet, "https://example.com/v1/models", nil)
	proxyURL, err := transport.Proxy(request)
	if err != nil || proxyURL == nil || proxyURL.Host != "127.0.0.1:18080" {
		t.Fatalf("proxy=%v err=%v", proxyURL, err)
	}

	svc.UpdateProxyConfig(config.ProxyConfig{Enabled: false})
	client, err = svc.httpClientFor(context.Background(), channel, gpt)
	if err != nil {
		t.Fatalf("httpClientFor disabled: %v", err)
	}
	transport = client.Transport.(*http.Transport)
	proxyURL, err = transport.Proxy(request)
	if err != nil || proxyURL != nil {
		t.Fatalf("disabled proxy=%v err=%v", proxyURL, err)
	}
}
