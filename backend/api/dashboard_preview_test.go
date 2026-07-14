package api

import "testing"

func TestDashboardPublicDispatchPreviewKeepsOneCheapestGroupPerWebsite(t *testing.T) {
	groups := []dashboardGatewayGroup{
		{ID: 11, ChannelID: 1, SiteDomain: "a.example.com", ClientFormat: "openai", Ratio: 0.05, Enabled: true, Status: "alive"},
		{ID: 12, ChannelID: 1, SiteDomain: "a.example.com", ClientFormat: "openai", Ratio: 0.01, Enabled: true, Status: "alive"},
		{ID: 13, ChannelID: 6, SiteDomain: "a.example.com", ClientFormat: "openai", Ratio: 0.02, Enabled: true, Status: "alive"},
		{ID: 21, ChannelID: 2, SiteDomain: "b.example.com", ClientFormat: "openai", Ratio: 0.02, Enabled: true, Status: "unknown"},
		{ID: 22, ChannelID: 2, SiteDomain: "b.example.com", ClientFormat: "openai", Ratio: 0.03, Enabled: true, Status: "alive"},
		{ID: 31, ChannelID: 3, SiteDomain: "c.example.com", ClientFormat: "openai", Ratio: 0.04, Enabled: true, Status: "alive"},
		{ID: 41, ChannelID: 4, SiteDomain: "d.example.com", ClientFormat: "openai", Ratio: 0.06, Enabled: true, Status: "alive"},
		{ID: 42, ChannelID: 7, SiteDomain: "e.example.com", ClientFormat: "openai", Ratio: 0.07, Enabled: true, Status: "alive"},
		{ID: 43, ChannelID: 8, SiteDomain: "f.example.com", ClientFormat: "openai", Ratio: 0.08, Enabled: true, Status: "alive"},
		{ID: 51, ChannelID: 5, SiteDomain: "claude.example.com", ClientFormat: "claude", Ratio: 0.001, Enabled: true, Status: "alive"},
		{ID: 61, ChannelID: 9, SiteDomain: "dead.example.com", ClientFormat: "openai", Ratio: 0.001, Enabled: true, Status: "dead"},
	}

	got := dashboardPublicDispatchPreview(groups)
	if len(got) != 5 {
		t.Fatalf("preview length = %d, want 5", len(got))
	}
	wantIDs := []uint{12, 21, 31, 41, 42}
	for i, want := range wantIDs {
		if got[i].ID != want {
			t.Fatalf("preview[%d].ID = %d, want %d", i, got[i].ID, want)
		}
	}
}
