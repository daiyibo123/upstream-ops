package oauthadmin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	appcrypto "github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/oauthpool"
	"github.com/bejix/upstream-ops/backend/storage"
	sqliteDriver "github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestCheckOneRequiresThreeTransientFailuresBeforeCooling(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary upstream outage", http.StatusServiceUnavailable)
	}))
	defer upstream.Close()
	db, err := gorm.Open(sqliteDriver.Open(filepath.Join(t.TempDir(), "oauth-admin.db")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if sqlDB, sqlErr := db.DB(); sqlErr == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}
	if err := storage.AutoMigrateOAuthAccounts(db); err != nil {
		t.Fatal(err)
	}
	cipher, err := appcrypto.NewCipher("oauth-admin-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	repository := storage.NewOAuthAccounts(db, cipher)
	result, err := repository.ImportJSON(storage.OAuthPoolChatGPT, []byte(`{"type":"codex","account_id":"account-a","access_token":"token-a"}`))
	if err != nil || result.Succeeded != 1 {
		t.Fatalf("import=%#v err=%v", result, err)
	}
	id := result.Items[0].AccountID
	if err := repository.RecordRuntimeSuccess(storage.OAuthPoolChatGPT, id, time.Now()); err != nil {
		t.Fatal(err)
	}
	pool := oauthpool.NewService(repository, oauthpool.WithEndpoints(oauthpool.Endpoints{ChatGPTCodex: upstream.URL}))
	service := New(repository, pool)
	for attempt := 1; attempt <= 3; attempt++ {
		account, _, err := service.CheckOne(context.Background(), storage.OAuthPoolChatGPT, id)
		if err != nil {
			t.Fatalf("check %d: %v", attempt, err)
		}
		if attempt < 3 {
			if account.Status != storage.OAuthStatusAlive || !account.InRotation || account.ConsecutiveFails != attempt {
				t.Fatalf("attempt %d opened breaker too early: %#v", attempt, account)
			}
		} else if account.Status != storage.OAuthStatusCooling || account.InRotation || account.DisabledUntil == nil || account.ConsecutiveFails != 3 {
			t.Fatalf("third failure did not open breaker: %#v", account)
		}
	}
}
