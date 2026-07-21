package sanitize

import (
	"strings"
	"testing"
)

func TestRedactTextRecursiveJSON(t *testing.T) {
	raw := `{"error":{"authorization":"Bearer top-secret","details":[{"access_token":"token-value"},{"message":"safe"}]},"cookie":"sso=private"}`
	got := RedactText(raw)
	for _, secret := range []string{"top-secret", "token-value", "sso=private"} {
		if strings.Contains(got, secret) {
			t.Fatalf("redacted JSON still contains %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, `"message":"safe"`) {
		t.Fatalf("non-sensitive error detail was removed: %s", got)
	}
}

func TestRedactTextHeadersAndProxyCredentials(t *testing.T) {
	raw := "Authorization: Bearer abc.def\nCookie: sso=secret; sso-rw=secret2\nproxy http://user:pass@127.0.0.1:8080 failed"
	got := RedactText(raw)
	for _, secret := range []string{"abc.def", "sso=secret", "secret2", "user", "pass"} {
		if strings.Contains(got, secret) {
			t.Fatalf("redacted text still contains %q: %s", secret, got)
		}
	}
	if !strings.Contains(got, "127.0.0.1:8080") {
		t.Fatalf("safe proxy host should remain diagnosable: %s", got)
	}
}

func TestTruncateUsesRunes(t *testing.T) {
	if got := Truncate("账号错误详情", 5); got != "账号..." {
		t.Fatalf("unexpected UTF-8 truncation: %q", got)
	}
}

func TestMaskIdentifier(t *testing.T) {
	if got := MaskIdentifier("someone@example.com"); got != "s***@example.com" {
		t.Fatalf("unexpected email mask: %q", got)
	}
	if got := MaskIdentifier("account-12345"); got != "acc***45" {
		t.Fatalf("unexpected identifier mask: %q", got)
	}
}

func TestSafeProxyURLDropsCredentialsAndQuery(t *testing.T) {
	got := SafeProxyURL("http://user:pass@proxy.example:8080/path?token=secret#fragment")
	if got != "http://proxy.example:8080/path" {
		t.Fatalf("unexpected safe proxy URL: %q", got)
	}
}
