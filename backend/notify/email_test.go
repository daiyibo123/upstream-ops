package notify

import (
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestEmailContentUsesMobileFriendlyStackedRows(t *testing.T) {
	html := buildEmailHTML("AI Gateway", "倍率变化提醒", "渠道：https://mdkj.lol/\n分组倍率：ChatGPT-K12 由 0.015 下调至 0.01\n变化时间：2026-07-15 18:00", Message{
		Event:     storage.EventRateChanged,
		ChannelID: 7,
		ModelName: "ChatGPT-K12",
	})
	if strings.Contains(html, "<tr><td") {
		t.Fatalf("email detail rows should be stacked blocks, not a narrow two-column table:\n%s", html)
	}
	for _, want := range []string{"overflow-wrap:anywhere", "word-break:break-word", "ChatGPT-K12", "https://mdkj.lol/"} {
		if !strings.Contains(html, want) {
			t.Fatalf("email html missing %q:\n%s", want, html)
		}
	}
}
