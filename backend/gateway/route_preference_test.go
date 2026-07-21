package gateway

import (
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestGatewayRoutePreferenceStablePartitionsCandidates(t *testing.T) {
	svc := &Service{}
	ordered := []storage.UpstreamGroupKey{
		{ID: 1, ChannelType: storage.ChannelTypeNewAPI, Ratio: 0.1, Status: "alive"},
		{ID: 2, ChannelType: storage.ChannelTypeChatGPTPool, Ratio: 0.1, Status: "alive"},
		{ID: 3, ChannelType: storage.ChannelTypeSub2API, Ratio: 0.2, Status: "alive"},
		{ID: 4, ChannelType: storage.ChannelTypeGrokPool, Ratio: 0.2, Status: "alive"},
	}

	poolFirst := svc.applyGatewayRoutePreference(ordered, &storage.GatewayKey{RoutePreference: gatewayRoutePoolFirst}, "")
	assertCandidateIDs(t, poolFirst, []uint{2, 1, 4, 3})

	upstreamFirst := svc.applyGatewayRoutePreference(ordered, &storage.GatewayKey{RoutePreference: gatewayRouteUpstreamFirst}, "")
	assertCandidateIDs(t, upstreamFirst, []uint{1, 2, 3, 4})

	ratioFirst := svc.applyGatewayRoutePreference(ordered, &storage.GatewayKey{RoutePreference: gatewayRouteRatioFirst}, "")
	assertCandidateIDs(t, ratioFirst, []uint{1, 2, 3, 4})
}

func TestGatewayRoutePreferenceNeverOverridesLowerRatio(t *testing.T) {
	svc := &Service{}
	ordered := []storage.UpstreamGroupKey{
		{ID: 1, ChannelType: storage.ChannelTypeNewAPI, Ratio: 0.05, Status: "alive"},
		{ID: 2, ChannelType: storage.ChannelTypeChatGPTPool, Ratio: 0.2, Status: "alive"},
	}
	got := svc.applyGatewayRoutePreference(ordered, &storage.GatewayKey{RoutePreference: gatewayRoutePoolFirst}, "")
	assertCandidateIDs(t, got, []uint{1, 2})
}

func TestNormalizeGatewayRoutePreferenceDefaultsSafely(t *testing.T) {
	for _, value := range []string{"", "unknown", "POOL-FIRST"} {
		if got := normalizeGatewayRoutePreference(value); got != gatewayRouteRatioFirst {
			t.Fatalf("normalize %q = %q, want %q", value, got, gatewayRouteRatioFirst)
		}
	}
	if got := normalizeGatewayRoutePreference(" pool_first "); got != gatewayRoutePoolFirst {
		t.Fatalf("pool preference = %q", got)
	}
	if got := normalizeGatewayRoutePreference("upstream_first"); got != gatewayRouteUpstreamFirst {
		t.Fatalf("upstream preference = %q", got)
	}
}

func assertCandidateIDs(t *testing.T, candidates []storage.UpstreamGroupKey, expected []uint) {
	t.Helper()
	if len(candidates) != len(expected) {
		t.Fatalf("candidate count=%d want=%d", len(candidates), len(expected))
	}
	for i, id := range expected {
		if candidates[i].ID != id {
			t.Fatalf("candidate[%d]=%d want=%d", i, candidates[i].ID, id)
		}
	}
}
