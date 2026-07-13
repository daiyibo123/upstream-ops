package notify

import "testing"

func TestRateChangeMatchesOpenAILowRatioPolicy(t *testing.T) {
	tests := []struct {
		name string
		rc   RateChange
		want bool
	}{
		{
			name: "openai below 0.05 decrease",
			rc:   RateChange{GroupName: "openai-low", OldRatio: 0.049, NewRatio: 0.03},
			want: true,
		},
		{
			name: "openai below 0.05 increase",
			rc:   RateChange{GroupName: "gpt-4o-mini", OldRatio: 0.03, NewRatio: 0.049},
			want: true,
		},
		{
			name: "openai 0.05 to 0.1 decrease",
			rc:   RateChange{GroupName: "gpt-4o", OldRatio: 0.12, NewRatio: 0.08},
			want: true,
		},
		{
			name: "openai 0.05 to 0.1 increase skipped",
			rc:   RateChange{GroupName: "gpt-4o", OldRatio: 0.06, NewRatio: 0.08},
			want: false,
		},
		{
			name: "openai above 0.1 decrease skipped",
			rc:   RateChange{GroupName: "gpt-4o", OldRatio: 0.2, NewRatio: 0.12},
			want: false,
		},
		{
			name: "claude below 0.05 skipped",
			rc:   RateChange{GroupName: "claude-sonnet", OldRatio: 0.03, NewRatio: 0.02},
			want: false,
		},
		{
			name: "grok description skipped",
			rc:   RateChange{GroupName: "fast-low", Description: "xAI Grok", OldRatio: 0.03, NewRatio: 0.02},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rc.MatchesOpenAILowRatioPolicy(); got != tt.want {
				t.Fatalf("MatchesOpenAILowRatioPolicy() = %v, want %v", got, tt.want)
			}
		})
	}
}
