package storage

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	appcrypto "github.com/bejix/upstream-ops/backend/crypto"
)

func newOAuthTestRepository(t *testing.T) (*OAuthAccounts, *appcrypto.Cipher) {
	t.Helper()
	db := openTestDB(t)
	if err := AutoMigrateOAuthAccounts(db); err != nil {
		t.Fatalf("migrate OAuth accounts: %v", err)
	}
	cipher, err := appcrypto.NewCipher("oauth-account-test-secret")
	if err != nil {
		t.Fatalf("create cipher: %v", err)
	}
	return NewOAuthAccounts(db, cipher), cipher
}

func testJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	return testJWTWithHeader(t, map[string]any{"alg": "RS256", "typ": "JWT"}, claims, "signature")
}

func testJWTWithHeader(t *testing.T, header, claims map[string]any, signature string) string {
	t.Helper()
	encode := func(value any) string {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal JWT: %v", err)
		}
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	return encode(header) + "." + encode(claims) + "." + signature
}

func futureRFC3339() string { return time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339) }

func TestOAuthImportSub2APIChatGPTMapping(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	idToken := testJWT(t, map[string]any{
		"exp": time.Now().Add(time.Hour).Unix(), "sub": "subject-1", "email": "User@Example.COM",
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": "acct-1", "chatgpt_user_id": "user-1",
			"chatgpt_plan_type": "plus", "organization_id": "org-1",
		},
	})
	raw := []byte(fmt.Sprintf(`{
		"exported_at":"2026-07-20T12:00:00Z",
		"proxies":[],
		"accounts":[{
			"name":"Primary",
			"token":"generic-token-must-not-win",
			"credentials":{
				"access_token":"access-one","refresh_token":"refresh-one","id_token":%q,
				"client_id":"client-one","token_type":"Bearer","expires_in":3600,
				"expires_at":%q,"base_url":"https://chatgpt.com","auth_kind":"oauth"
			}
		}]
	}`, idToken, futureRFC3339()))

	result, err := repository.ImportJSON(OAuthPoolChatGPT, raw)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if result.Created != 1 || result.Failed != 0 || result.Items[0].SourceFormat != "sub2api" {
		t.Fatalf("unexpected result: %#v", result)
	}
	account, err := repository.Find(OAuthPoolChatGPT, result.Items[0].AccountID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if account.ExternalID != "acct-1" || account.Email != "user@example.com" || account.DisplayName != "Primary" || account.WeakIdentity {
		t.Fatalf("unexpected account: %#v", account)
	}
	credentials, err := repository.Credentials(OAuthPoolChatGPT, account.ID)
	if err != nil {
		t.Fatalf("credentials: %v", err)
	}
	if credentials.AccessToken != "access-one" || credentials.RefreshToken != "refresh-one" || credentials.IDToken != idToken ||
		credentials.ClientID != "client-one" || credentials.TokenType != "Bearer" || credentials.ExpiresIn != 3600 ||
		credentials.LastRefresh != "2026-07-20T12:00:00Z" || credentials.AccountID != "acct-1" ||
		credentials.Subject != "subject-1" || credentials.UserID != "user-1" || credentials.Organization != "org-1" ||
		credentials.PlanType != "plus" || credentials.BaseURL != "https://chatgpt.com" || credentials.AuthKind != "oauth" {
		t.Fatalf("unexpected credentials: %#v", credentials)
	}
}

func TestOAuthImportCLIAndCPADirectFormats(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	usingAPI := true
	_ = usingAPI
	cliJWT := testJWT(t, map[string]any{
		"exp": time.Now().Add(time.Hour).Unix(), "sub": "grok-sub", "user_id": "grok-user", "team_id": "grok-team",
	})
	cliRaw := []byte(fmt.Sprintf(`{
		"client":"CLIProxyAPI","type":"xai","access_token":"grok-access","id_token":%q,
		"client_id":"grok-cli","base_url":"https://api.x.ai/v1","token_endpoint":"https://auth.x.ai/oauth/token",
		"auth_kind":"oauth","using_api":true,"expired":%q
	}`, cliJWT, futureRFC3339()))
	cliResult, err := repository.ImportJSON(OAuthPoolGrok, cliRaw)
	if err != nil || cliResult.Created != 1 {
		t.Fatalf("CLI import result=%#v err=%v", cliResult, err)
	}
	cliCredentials, err := repository.Credentials(OAuthPoolGrok, cliResult.Items[0].AccountID)
	if err != nil {
		t.Fatalf("CLI credentials: %v", err)
	}
	if cliResult.Items[0].SourceFormat != "cliproxyapi" || cliCredentials.Subject != "grok-sub" ||
		cliCredentials.UserID != "grok-user" || cliCredentials.TeamID != "grok-team" ||
		cliCredentials.BaseURL != "https://api.x.ai/v1" || cliCredentials.TokenEndpoint != "https://auth.x.ai/oauth/token" ||
		cliCredentials.AuthKind != "oauth" || cliCredentials.UsingAPI == nil || !*cliCredentials.UsingAPI {
		t.Fatalf("unexpected CLI mapping: result=%#v credentials=%#v", cliResult, cliCredentials)
	}

	cpaRaw := []byte(`{"source":"CPA","type":"codex","email":"cpa@example.com","accessToken":"cpa-access","refreshToken":"cpa-refresh","accountId":"cpa-account"}`)
	cpaResult, err := repository.ImportJSON(OAuthPoolChatGPT, cpaRaw)
	if err != nil || cpaResult.Created != 1 || cpaResult.Items[0].SourceFormat != "cpa" {
		t.Fatalf("CPA import result=%#v err=%v", cpaResult, err)
	}
	cpaCredentials, err := repository.Credentials(OAuthPoolChatGPT, cpaResult.Items[0].AccountID)
	if err != nil || cpaCredentials.AccessToken != "cpa-access" || cpaCredentials.RefreshToken != "cpa-refresh" {
		t.Fatalf("unexpected CPA credentials=%#v err=%v", cpaCredentials, err)
	}
}

func TestOAuthImportRealSub2APIEnvelopeSupportsExtraAndPartialSuccess(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	unsignedIDToken := testJWTWithHeader(t, map[string]any{"alg": "none", "typ": "JWT"}, map[string]any{
		"exp": time.Now().Add(time.Hour).Unix(), "sub": "sub2api-subject", "email": "token@example.com",
		"https://api.openai.com/auth": map[string]any{"chatgpt_account_id": "token-account"},
	}, "")
	raw := []byte(fmt.Sprintf(`{
		"type":"sub2api-data",
		"data":{
			"exported_at":"2026-07-20T12:00:00Z",
			"proxies":[],
			"accounts":[
				{
					"name":"Sub2API OpenAI","platform":"openai","type":"oauth",
					"credentials":{"access_token":"sub2api-access","refresh_token":"sub2api-refresh","id_token":%q},
					"extra":{"chatgpt_account_id":"extra-account","chatgpt_user_id":"extra-user","plan_type":"plus","email":"extra@example.com"}
				},
				{"name":"Unsupported provider","platform":"anthropic","type":"oauth","credentials":{"access_token":"must-not-leak"}}
			]
		}
	}`, unsignedIDToken))

	result, err := repository.ImportJSON(OAuthPoolChatGPT, raw)
	if err != nil {
		t.Fatalf("import sub2api envelope: %v", err)
	}
	if result.Total != 2 || result.Created != 1 || result.Failed != 1 || result.Items[0].SourceFormat != "sub2api" {
		t.Fatalf("unexpected sub2api result: %#v", result)
	}
	if strings.Contains(strings.Join([]string{result.Items[1].Reason, result.Failures[0].Reason}, " "), "must-not-leak") {
		t.Fatalf("import error leaked credential: %#v", result)
	}
	account, err := repository.Find(OAuthPoolChatGPT, result.Items[0].AccountID)
	if err != nil {
		t.Fatalf("find imported sub2api account: %v", err)
	}
	if account.ExternalID != "extra-account" || account.Email != "extra@example.com" || account.DisplayName != "Sub2API OpenAI" {
		t.Fatalf("sub2api metadata was not preserved: %#v", account)
	}
	credentials, err := repository.Credentials(OAuthPoolChatGPT, account.ID)
	if err != nil {
		t.Fatalf("read imported sub2api credentials: %v", err)
	}
	if credentials.Subject != "sub2api-subject" || credentials.UserID != "extra-user" || credentials.PlanType != "plus" || credentials.LastRefresh != "2026-07-20T12:00:00Z" {
		t.Fatalf("sub2api credentials were not normalized: %#v", credentials)
	}
}

func TestOAuthImportCPAAndCLIProxyNestedRealWorldShapes(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	cpaIDToken := testJWTWithHeader(t, map[string]any{"alg": "none", "typ": "JWT"}, map[string]any{
		"exp": time.Now().Add(time.Hour).Unix(), "sub": "cpa-subject",
	}, "")
	cpaRaw := []byte(fmt.Sprintf(`{"data":{"source":"CPA","type":"codex","session":{"tokens":{"accessToken":"cpa-access","refreshToken":"cpa-refresh","idToken":%q}},"profile":{"user":{"email":"cpa@example.com","id":"cpa-user"},"account":{"id":"cpa-account","planType":"pro"}}}}`, cpaIDToken))
	cpa, err := repository.ImportJSON(OAuthPoolChatGPT, cpaRaw)
	if err != nil || cpa.Created != 1 || cpa.Items[0].SourceFormat != "cpa" {
		t.Fatalf("import CPA nested object: result=%#v err=%v", cpa, err)
	}
	cpaAccount, err := repository.Find(OAuthPoolChatGPT, cpa.Items[0].AccountID)
	if err != nil {
		t.Fatalf("find CPA account: %v", err)
	}
	if cpaAccount.ExternalID != "cpa-account" || cpaAccount.Email != "cpa@example.com" {
		t.Fatalf("CPA identity was not normalized: %#v", cpaAccount)
	}
	cpaCredentials, err := repository.Credentials(OAuthPoolChatGPT, cpaAccount.ID)
	if err != nil || cpaCredentials.UserID != "cpa-user" || cpaCredentials.PlanType != "pro" || cpaCredentials.Subject != "cpa-subject" {
		t.Fatalf("CPA credentials were not normalized: %#v err=%v", cpaCredentials, err)
	}

	grokIDToken := testJWTWithHeader(t, map[string]any{"alg": "none", "typ": "JWT"}, map[string]any{
		"exp": time.Now().Add(time.Hour).Unix(), "sub": "grok-subject", "email": "grok@example.com", "user_id": "grok-user",
	}, "")
	grokRaw := []byte(fmt.Sprintf(`[
		{"client":"CLIProxyAPI","type":"xai","access_token":"grok-access","refresh_token":"grok-refresh","id_token":%q,"base_url":"https://cli-chat-proxy.grok.com/v1","token_endpoint":"https://auth.x.ai/oauth2/token","auth_kind":"oauth"}
	]`, grokIDToken))
	grok, err := repository.ImportJSON(OAuthPoolGrok, grokRaw)
	if err != nil || grok.Created != 1 || grok.Items[0].SourceFormat != "cliproxyapi" {
		t.Fatalf("import CLIProxyAPI Grok array: result=%#v err=%v", grok, err)
	}
	grokCredentials, err := repository.Credentials(OAuthPoolGrok, grok.Items[0].AccountID)
	if err != nil || grokCredentials.Subject != "grok-subject" || grokCredentials.UserID != "grok-user" {
		t.Fatalf("CLIProxyAPI Grok credentials were not normalized: %#v err=%v", grokCredentials, err)
	}
	grokAccount, err := repository.Find(OAuthPoolGrok, grok.Items[0].AccountID)
	if err != nil || grokAccount.Email != "grok@example.com" || grokAccount.ExternalID != "grok-subject" {
		t.Fatalf("CLIProxyAPI Grok identity was not normalized: %#v err=%v", grokAccount, err)
	}
}

func TestOAuthImportGrokSSOCookieNormalization(t *testing.T) {
	for _, test := range []struct {
		name   string
		cookie string
		want   string
	}{
		{"object", `{"z":"last","sso":"sso-secret","a":"first"}`, "a=first; sso=sso-secret; z=last"},
		{"array", `[{"name":"sso-rw","value":"rw-secret"},{"name":"cf_clearance","value":"clear"}]`, "cf_clearance=clear; sso-rw=rw-secret"},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository, _ := newOAuthTestRepository(t)
			raw := []byte(fmt.Sprintf(`{"type":"grok","source":"SSO","cookies":%s}`, test.cookie))
			result, err := repository.ImportJSON(OAuthPoolGrok, raw)
			if err != nil || result.Created != 1 || result.Items[0].SourceFormat != "grok_sso" {
				t.Fatalf("result=%#v err=%v", result, err)
			}
			credentials, err := repository.Credentials(OAuthPoolGrok, result.Items[0].AccountID)
			if err != nil || credentials.Cookie != test.want || credentials.SSOToken == "" {
				t.Fatalf("credentials=%#v err=%v", credentials, err)
			}
		})
	}
	repository, _ := newOAuthTestRepository(t)
	result, err := repository.ImportJSON(OAuthPoolGrok, []byte(`{"type":"grok","sso":"direct-secret","cf_clearance":"clearance"}`))
	if err != nil || result.Created != 1 {
		t.Fatalf("direct SSO result=%#v err=%v", result, err)
	}
	credentials, err := repository.Credentials(OAuthPoolGrok, result.Items[0].AccountID)
	if err != nil || credentials.SSOToken != "direct-secret" || credentials.Cookie != "cf_clearance=clearance; sso=direct-secret" {
		t.Fatalf("direct SSO credentials=%#v err=%v", credentials, err)
	}
}

func TestOAuthImportGrok2APICurrentJSONShapes(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	web, err := repository.ImportJSON(OAuthPoolGrok, []byte(`{
		"provider":"grok_web",
		"accounts":[{"name":"primary","sso_token":"web-sso","tier":"super","cloudflare_cookies":"cf_clearance=abc; sso=drop"}]
	}`))
	if err != nil || web.Created != 1 || web.Items[0].SourceFormat != "grok2api_sso" {
		t.Fatalf("import grok2api web: result=%#v err=%v", web, err)
	}
	webCredentials, err := repository.Credentials(OAuthPoolGrok, web.Items[0].AccountID)
	if err != nil || webCredentials.SSOToken != "web-sso" || webCredentials.AccessToken != "" || webCredentials.PlanType != "super" || !strings.Contains(webCredentials.Cookie, "cf_clearance=abc") {
		t.Fatalf("grok2api web credentials were not normalized: %#v err=%v", webCredentials, err)
	}

	console, err := repository.ImportJSON(OAuthPoolGrok, []byte(`{
		"provider":"grok_console",
		"accounts":[{"name":"console","token":"console-sso","cloudflare_cookies":"cf_clearance=console"}]
	}`))
	if err != nil || console.Created != 1 || console.Items[0].SourceFormat != "grok2api_console" {
		t.Fatalf("import grok2api console: result=%#v err=%v", console, err)
	}
	consoleCredentials, err := repository.Credentials(OAuthPoolGrok, console.Items[0].AccountID)
	if err != nil || consoleCredentials.SSOToken != "console-sso" || consoleCredentials.AccessToken != "" || !strings.Contains(consoleCredentials.Cookie, "cf_clearance=console") {
		t.Fatalf("grok2api console credentials were not normalized: %#v err=%v", consoleCredentials, err)
	}

	build, err := repository.ImportJSON(OAuthPoolGrok, []byte(`{
		"accounts":[{"provider":"grok_build","name":"build","token":"build-access","refresh_token":"build-refresh","user_id":"build-user"}]
	}`))
	if err != nil || build.Created != 1 || build.Items[0].SourceFormat != "grok2api_build" {
		t.Fatalf("import grok2api build: result=%#v err=%v", build, err)
	}
	buildCredentials, err := repository.Credentials(OAuthPoolGrok, build.Items[0].AccountID)
	if err != nil || buildCredentials.AccessToken != "build-access" || buildCredentials.RefreshToken != "build-refresh" || buildCredentials.SSOToken != "" {
		t.Fatalf("grok2api build credentials were not normalized: %#v err=%v", buildCredentials, err)
	}
}

func TestOAuthImportPartialSuccess(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	raw := []byte(`[
		{"type":"codex","account_id":"good","access_token":"good-token"},
		{"type":"codex","account_id":"missing-token"},
		"not-an-object"
	]`)
	result, err := repository.ImportJSON(OAuthPoolChatGPT, raw)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if result.Total != 3 || result.Created != 1 || result.Succeeded != 1 || result.Failed != 2 || len(result.Failures) != 2 {
		t.Fatalf("unexpected partial result: %#v", result)
	}
}

func TestOAuthImportReplacesSameIdentityAndResetsHealth(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	first, err := repository.ImportJSON(OAuthPoolChatGPT, []byte(`{"type":"codex","account_id":"stable-account","access_token":"old-token"}`))
	if err != nil || first.Created != 1 {
		t.Fatalf("first import=%#v err=%v", first, err)
	}
	id := first.Items[0].AccountID
	future := time.Now().Add(time.Hour)
	if err := repository.db.Model(&OAuthAccount{}).Where("id = ?", id).Updates(map[string]any{
		"enabled": false, "status": OAuthStatusAlive, "in_rotation": true, "consecutive_fails": 3,
		"disabled_until": future, "last_error": "old error", "last_checked_at": time.Now(),
	}).Error; err != nil {
		t.Fatalf("seed state: %v", err)
	}
	second, err := repository.ImportJSON(OAuthPoolChatGPT, []byte(`{"type":"codex","account_id":"stable-account","access_token":"new-token"}`))
	if err != nil || second.Updated != 1 || second.Created != 0 || second.Items[0].Status != "updated" {
		t.Fatalf("second import=%#v err=%v", second, err)
	}
	var count int64
	if err := repository.db.Model(&OAuthAccount{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	var account OAuthAccount
	if err := repository.db.First(&account, id).Error; err != nil {
		t.Fatalf("find row: %v", err)
	}
	if account.Enabled || account.Status != OAuthStatusUnchecked || account.InRotation || account.ConsecutiveFails != 0 ||
		account.DisabledUntil != nil || account.LastCheckedAt != nil || account.LastError != "" {
		t.Fatalf("health state was not reset while enabled was preserved: %#v", account)
	}
	credentials, err := repository.Credentials(OAuthPoolChatGPT, id)
	if err != nil || credentials.AccessToken != "new-token" {
		t.Fatalf("credentials=%#v err=%v", credentials, err)
	}
}

func TestOAuthImportStrongAndWeakIdentityRules(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	strong := []byte(`[
		{"type":"codex","account_id":"account-a","email":"same@example.com","access_token":"a"},
		{"type":"codex","account_id":"account-b","email":"same@example.com","access_token":"b"}
	]`)
	result, err := repository.ImportJSON(OAuthPoolChatGPT, strong)
	if err != nil || result.Created != 2 {
		t.Fatalf("strong identities result=%#v err=%v", result, err)
	}

	weakFirst, err := repository.ImportJSON(OAuthPoolGrok, []byte(fmt.Sprintf(`{"type":"xai","client_id":"stable-client","base_url":"https://api.x.ai/v1","access_token":"old","expired":%q}`, futureRFC3339())))
	if err != nil || weakFirst.Created != 1 || !weakFirst.Items[0].WeakIdentity {
		t.Fatalf("weak first=%#v err=%v", weakFirst, err)
	}
	weakSecond, err := repository.ImportJSON(OAuthPoolGrok, []byte(fmt.Sprintf(`{"type":"xai","client_id":"stable-client","base_url":"https://api.x.ai/v1","access_token":"new","expired":%q}`, time.Now().Add(48*time.Hour).UTC().Format(time.RFC3339))))
	if err != nil || weakSecond.Updated != 1 || weakSecond.Items[0].AccountID != weakFirst.Items[0].AccountID {
		t.Fatalf("weak second=%#v err=%v", weakSecond, err)
	}
	credentialOnly, err := repository.ImportJSON(OAuthPoolGrok, []byte(`[
		{"type":"grok","sso":"credential-one"},
		{"type":"grok","sso":"credential-two"}
	]`))
	if err != nil || credentialOnly.Created != 2 || credentialOnly.Items[0].AccountID == credentialOnly.Items[1].AccountID {
		t.Fatalf("credential-only identities collided: result=%#v err=%v", credentialOnly, err)
	}
}

func TestOAuthImportRejectsUnsafeCredentials(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	expiredJWT := testJWT(t, map[string]any{"exp": time.Now().Add(-time.Hour).Unix()})
	tests := []struct {
		name string
		pool OAuthPool
		raw  string
	}{
		{"expired explicit", OAuthPoolChatGPT, fmt.Sprintf(`{"type":"codex","account_id":"a","access_token":"x","expired":%q}`, time.Now().Add(-time.Hour).UTC().Format(time.RFC3339))},
		{"expired JWT", OAuthPoolChatGPT, fmt.Sprintf(`{"type":"codex","account_id":"a","access_token":"x","id_token":%q}`, expiredJWT)},
		{"hostile token endpoint", OAuthPoolGrok, `{"type":"xai","access_token":"x","token_endpoint":"https://evil.example/token"}`},
		{"cross pool", OAuthPoolChatGPT, `{"type":"xai","access_token":"x"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := repository.ImportJSON(test.pool, []byte(test.raw))
			if err != nil {
				t.Fatalf("top-level import should preserve partial-success contract: %v", err)
			}
			if result.Failed != 1 || result.Succeeded != 0 || result.Items[0].Reason == "" {
				t.Fatalf("unexpected rejection result: %#v", result)
			}
		})
	}
}

// id_token 只用于提取元数据、从不验签，所以它的 alg=none / 空签名这类"验签相关"问题
// 绝不能让导入失败——真实世界的 Codex / sub2api 导出经常带 alg=none 的 id_token。
// 只要 payload 结构正常就仍可用于元数据提取；否则降级为忽略该 token，
// 但 access_token 仍作为凭据可用。
func TestOAuthImportIgnoresUnverifiableIDTokenButStillImports(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	algNone := testJWTWithHeader(t, map[string]any{"alg": "none"}, map[string]any{"exp": time.Now().Add(time.Hour).Unix()}, "signature")
	emptySignature := testJWTWithHeader(t, map[string]any{"alg": "RS256"}, map[string]any{"exp": time.Now().Add(time.Hour).Unix()}, "")
	tests := []struct {
		name string
		raw  string
	}{
		{"alg none", fmt.Sprintf(`{"type":"codex","account_id":"a","access_token":"x","id_token":%q}`, algNone)},
		{"empty signature", fmt.Sprintf(`{"type":"codex","account_id":"b","access_token":"y","id_token":%q}`, emptySignature)},
		{"garbage id_token", `{"type":"codex","account_id":"c","access_token":"z","id_token":"not-a-jwt"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := repository.ImportJSON(OAuthPoolChatGPT, []byte(test.raw))
			if err != nil {
				t.Fatalf("import error: %v", err)
			}
			if result.Succeeded != 1 || result.Failed != 0 {
				t.Fatalf("unverifiable id_token must not block import: %#v", result)
			}
		})
	}
}

func TestOAuthRepositoryDoesNotExposeCredentials(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	secret := "very-secret-access-token"
	result, err := repository.ImportJSON(OAuthPoolChatGPT, []byte(fmt.Sprintf(`{"type":"codex","account_id":"secret-account","access_token":%q,"cookie":"session=cookie-secret"}`, secret)))
	if err != nil || result.Created != 1 {
		t.Fatalf("import=%#v err=%v", result, err)
	}
	accounts, _, err := repository.List(OAuthPoolChatGPT, 1, 10, "", "")
	if err != nil || len(accounts) != 1 {
		t.Fatalf("list=%#v err=%v", accounts, err)
	}
	encoded, _ := json.Marshal(accounts[0])
	if strings.Contains(string(encoded), secret) || strings.Contains(string(encoded), "cookie-secret") || accounts[0].CredentialCipher != "" {
		t.Fatalf("list leaked credentials: %s %#v", encoded, accounts[0])
	}
	redacted := RedactSensitiveText(`Authorization: Bearer abc.def.ghi password=hunter2 proxy=http://user:pass@127.0.0.1:8080`)
	if strings.Contains(redacted, "abc.def.ghi") || strings.Contains(redacted, "hunter2") || strings.Contains(redacted, "user:pass") {
		t.Fatalf("redaction leaked a secret: %s", redacted)
	}
	credentials, _ := repository.Credentials(OAuthPoolChatGPT, result.Items[0].AccountID)
	redacted = RedactOAuthCredentialValues("upstream echoed "+secret+" and "+credentials.Cookie, credentials)
	if strings.Contains(redacted, secret) || strings.Contains(redacted, "cookie-secret") {
		t.Fatalf("credential-value redaction leaked a secret: %s", redacted)
	}
	redacted = RedactSensitiveText("Cookie: a=one; b=two\nnext=safe")
	if strings.Contains(redacted, "one") || strings.Contains(redacted, "two") || !strings.Contains(redacted, "next=safe") {
		t.Fatalf("cookie-line redaction failed: %s", redacted)
	}
}

func TestOAuthRepositoryPagingStatsHealthAndDelete(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	for i := 0; i < 12; i++ {
		raw := []byte(fmt.Sprintf(`{"type":"codex","account_id":"account-%02d","email":"user-%02d@example.com","access_token":"token-%02d"}`, i, i, i))
		result, err := repository.ImportJSON(OAuthPoolChatGPT, raw)
		if err != nil || result.Created != 1 {
			t.Fatalf("import %d result=%#v err=%v", i, result, err)
		}
	}
	accounts, total, err := repository.List(OAuthPoolChatGPT, 2, 10, OAuthStatusUnchecked, "user-")
	if err != nil || total != 12 || len(accounts) != 2 {
		t.Fatalf("page total=%d len=%d err=%v", total, len(accounts), err)
	}
	allIDs, err := repository.ListIDs(OAuthPoolChatGPT)
	if err != nil || len(allIDs) != 12 {
		t.Fatalf("ids=%v err=%v", allIDs, err)
	}
	now := time.Now().UTC()
	if err := repository.ApplyHealthResult(OAuthPoolChatGPT, allIDs[0], OAuthHealthResult{Status: OAuthStatusAlive, Schedulable: true}, now); err != nil {
		t.Fatalf("alive health result: %v", err)
	}
	future := now.Add(time.Hour)
	if err := repository.ApplyHealthResult(OAuthPoolChatGPT, allIDs[1], OAuthHealthResult{Status: OAuthStatusRateLimited, DisabledUntil: &future, Error: "Bearer hidden-secret"}, now); err != nil {
		t.Fatalf("rate-limit health result: %v", err)
	}
	if err := repository.ApplyHealthResult(OAuthPoolChatGPT, allIDs[2], OAuthHealthResult{Status: OAuthStatusDead, Error: "dead"}, now); err != nil {
		t.Fatalf("dead health result: %v", err)
	}
	if err := repository.ApplyHealthResult(OAuthPoolChatGPT, allIDs[3], OAuthHealthResult{Status: OAuthStatusCooling, DisabledUntil: &future}, now); err != nil {
		t.Fatalf("cooling health result: %v", err)
	}
	stats, err := repository.Stats(OAuthPoolChatGPT, now)
	if err != nil || stats.Total != 12 || stats.Available != 1 || stats.RateLimited != 1 || stats.Dead != 1 || stats.Cooling != 1 || stats.Unchecked != 8 {
		t.Fatalf("stats=%#v err=%v", stats, err)
	}
	schedulable, err := repository.ListSchedulable(OAuthPoolChatGPT, now, 0)
	if err != nil || len(schedulable) != 3 || schedulable[0].ID != allIDs[0] || schedulable[1].ID != allIDs[1] || schedulable[2].ID != allIDs[3] {
		t.Fatalf("schedulable=%#v err=%v", schedulable, err)
	}
	currentlyAvailable := 0
	for _, account := range schedulable {
		if account.CurrentlySchedulable(now) {
			currentlyAvailable++
		}
	}
	if currentlyAvailable != 1 {
		t.Fatalf("recovery snapshot has %d currently available accounts, want 1", currentlyAvailable)
	}
	batch, err := repository.BatchDelete(OAuthPoolChatGPT, []uint{allIDs[1], allIDs[2], 999999})
	if err != nil || batch.Succeeded != 2 || batch.Failed != 1 || batch.Failures[999999] == "" {
		t.Fatalf("batch=%#v err=%v", batch, err)
	}
	if err := repository.Delete(OAuthPoolChatGPT, allIDs[0]); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := repository.Find(OAuthPoolChatGPT, allIDs[0]); err == nil {
		t.Fatal("deleted account is still readable")
	}
}

func TestOAuthCurrentlySchedulable(t *testing.T) {
	now := time.Now()
	past, future := now.Add(-time.Minute), now.Add(time.Minute)
	base := OAuthAccount{Enabled: true, InRotation: true, Status: OAuthStatusAlive, CredentialCipher: "cipher"}
	if !base.CurrentlySchedulable(now) {
		t.Fatal("alive account should be schedulable")
	}
	for name, mutate := range map[string]func(*OAuthAccount){
		"dead":            func(account *OAuthAccount) { account.Status = OAuthStatusDead },
		"rate limited":    func(account *OAuthAccount) { account.Status = OAuthStatusRateLimited },
		"cooling":         func(account *OAuthAccount) { account.Status = OAuthStatusCooling },
		"disabled":        func(account *OAuthAccount) { account.Enabled = false },
		"not rotated":     func(account *OAuthAccount) { account.InRotation = false },
		"future disabled": func(account *OAuthAccount) { account.DisabledUntil = &future },
		"missing cipher":  func(account *OAuthAccount) { account.CredentialCipher = "" },
	} {
		t.Run(name, func(t *testing.T) {
			account := base
			mutate(&account)
			if account.CurrentlySchedulable(now) {
				t.Fatalf("%s account should not be schedulable", name)
			}
		})
	}
	base.DisabledUntil = &past
	if !base.CurrentlySchedulable(now) {
		t.Fatal("expired disabled_until should be schedulable")
	}
}

func TestOAuthRuntimeFailureThresholdRecoveryAndQuota(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	result, err := repository.ImportJSON(OAuthPoolGrok, []byte(`{"type":"xai","user_id":"runtime-user","access_token":"runtime-token"}`))
	if err != nil || result.Created != 1 {
		t.Fatalf("import=%#v err=%v", result, err)
	}
	id := result.Items[0].AccountID
	if err := repository.RecordRuntimeSuccess(OAuthPoolGrok, id, time.Now()); err != nil {
		t.Fatalf("initial success: %v", err)
	}
	future := time.Now().Add(time.Minute)
	for i := 1; i <= 2; i++ {
		if err := repository.RecordRuntimeFailure(OAuthPoolGrok, id, OAuthStatusCooling, "Bearer runtime-token", &future, 3); err != nil {
			t.Fatalf("temporary failure %d: %v", i, err)
		}
		var account OAuthAccount
		if err := repository.db.First(&account, id).Error; err != nil {
			t.Fatalf("read failure %d: %v", i, err)
		}
		if account.Status != OAuthStatusAlive || !account.InRotation || account.ConsecutiveFails != i || strings.Contains(account.LastError, "runtime-token") {
			t.Fatalf("unexpected pre-threshold state: %#v", account)
		}
	}
	if err := repository.RecordRuntimeFailure(OAuthPoolGrok, id, OAuthStatusCooling, "temporary", &future, 3); err != nil {
		t.Fatalf("threshold failure: %v", err)
	}
	var cooling OAuthAccount
	if err := repository.db.First(&cooling, id).Error; err != nil {
		t.Fatalf("read cooling: %v", err)
	}
	if cooling.Status != OAuthStatusCooling || cooling.InRotation || cooling.ConsecutiveFails != 3 || cooling.DisabledUntil == nil {
		t.Fatalf("unexpected cooling state: %#v", cooling)
	}
	if err := repository.RecordRuntimeSuccess(OAuthPoolGrok, id, time.Now()); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	used, limit := 2.0, 10.0
	reset := time.Now().Add(time.Hour).UTC()
	if err := repository.UpdateQuota(OAuthPoolGrok, id, &used, &limit, "requests", &reset); err != nil {
		t.Fatalf("update quota: %v", err)
	}
	account, err := repository.Find(OAuthPoolGrok, id)
	if err != nil || account.Status != OAuthStatusAlive || !account.InRotation || account.ConsecutiveFails != 0 ||
		account.QuotaUsed == nil || *account.QuotaUsed != used || account.QuotaLimit == nil || *account.QuotaLimit != limit || account.QuotaUnit != "requests" {
		t.Fatalf("recovered account=%#v err=%v", account, err)
	}
}

func TestOAuthRuntimeFailureConcurrentKeepsStrongestStatusAndLongestCooldown(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	result, err := repository.ImportJSON(OAuthPoolGrok, []byte(`{"type":"xai","user_id":"runtime-race","access_token":"runtime-token"}`))
	if err != nil || result.Created != 1 {
		t.Fatalf("import=%#v err=%v", result, err)
	}
	id := result.Items[0].AccountID
	if err := repository.RecordRuntimeSuccess(OAuthPoolGrok, id, time.Now()); err != nil {
		t.Fatalf("initial success: %v", err)
	}

	type failure struct {
		status OAuthAccountStatus
		until  time.Time
	}
	now := time.Now().UTC()
	failures := []failure{
		{status: OAuthStatusCooling, until: now.Add(30 * time.Minute)},
		{status: OAuthStatusRateLimited, until: now.Add(2 * time.Hour)},
		{status: OAuthStatusDead, until: now.Add(time.Hour)},
	}
	start := make(chan struct{})
	errorsSeen := make(chan error, len(failures))
	var workers sync.WaitGroup
	for _, item := range failures {
		item := item
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			errorsSeen <- repository.RecordRuntimeFailure(OAuthPoolGrok, id, item.status, string(item.status), &item.until, 1)
		}()
	}
	close(start)
	workers.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("concurrent runtime failure: %v", err)
		}
	}

	account, err := repository.Find(OAuthPoolGrok, id)
	if err != nil {
		t.Fatalf("reload account: %v", err)
	}
	if account.Status != OAuthStatusDead || account.InRotation || account.ConsecutiveFails != len(failures) {
		t.Fatalf("concurrent account state lost: %#v", account)
	}
	if account.DisabledUntil == nil || account.DisabledUntil.Before(now.Add(110*time.Minute)) {
		t.Fatalf("longest account cooldown lost: %v", account.DisabledUntil)
	}
	shorter := now.Add(5 * time.Minute)
	if err := repository.RecordRuntimeFailure(OAuthPoolGrok, id, OAuthStatusCooling, "later network error", &shorter, 1); err != nil {
		t.Fatalf("record later transient failure: %v", err)
	}
	after, err := repository.Find(OAuthPoolGrok, id)
	if err != nil {
		t.Fatalf("reload account after transient failure: %v", err)
	}
	if after.Status != OAuthStatusDead || after.DisabledUntil == nil || !after.DisabledUntil.Equal(*account.DisabledUntil) {
		t.Fatalf("account failure state was downgraded: before=%#v after=%#v", account, after)
	}
}

func TestOAuthRuntimeFailureNeverShortensRateLimitCooldown(t *testing.T) {
	repository, _ := newOAuthTestRepository(t)
	result, err := repository.ImportJSON(OAuthPoolGrok, []byte(`{"type":"xai","user_id":"runtime-rate-limit","access_token":"runtime-token"}`))
	if err != nil || result.Created != 1 {
		t.Fatalf("import=%#v err=%v", result, err)
	}
	id := result.Items[0].AccountID
	if err := repository.RecordRuntimeSuccess(OAuthPoolGrok, id, time.Now()); err != nil {
		t.Fatalf("initial success: %v", err)
	}

	longer := time.Now().UTC().Add(2 * time.Hour)
	if err := repository.RecordRuntimeFailure(OAuthPoolGrok, id, OAuthStatusRateLimited, "rate limited", &longer, 3); err != nil {
		t.Fatalf("record rate limit: %v", err)
	}
	shorter := time.Now().UTC().Add(5 * time.Minute)
	if err := repository.RecordRuntimeFailure(OAuthPoolGrok, id, OAuthStatusCooling, "later network error", &shorter, 1); err != nil {
		t.Fatalf("record later transient failure: %v", err)
	}

	account, err := repository.Find(OAuthPoolGrok, id)
	if err != nil {
		t.Fatalf("reload account: %v", err)
	}
	if account.Status != OAuthStatusRateLimited || account.DisabledUntil == nil || !account.DisabledUntil.Equal(longer) {
		t.Fatalf("rate-limit state was downgraded: %#v", account)
	}
}
