package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	appcrypto "github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/oauthadmin"
	"github.com/bejix/upstream-ops/backend/oauthpool"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
	sqliteDriver "github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestOAuthAccountAPIImportListStatsAndDelete(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"used":1,"limit":10}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n"))
	}))
	defer upstream.Close()

	db, err := gorm.Open(sqliteDriver.Open(filepath.Join(t.TempDir(), "oauth-api.db")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if sqlDB, sqlErr := db.DB(); sqlErr == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}
	if err := storage.AutoMigrateOAuthAccounts(db); err != nil {
		t.Fatal(err)
	}
	cipher, err := appcrypto.NewCipher("oauth-api-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	repository := storage.NewOAuthAccounts(db, cipher)
	pool := oauthpool.NewService(repository, oauthpool.WithEndpoints(oauthpool.Endpoints{ChatGPTCodex: upstream.URL}))
	admin := oauthadmin.New(repository, pool)

	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerOAuthAccounts(router.Group("/api"), &Deps{OAuthAdmin: admin})

	importBody := `{"type":"codex","account_id":"acct-1","email":"one@example.com","access_token":"access-one"}`
	response := performOAuthRequest(router, http.MethodPost, "/api/oauth-accounts/chatgpt/import", importBody)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"success":1`) {
		t.Fatalf("import status=%d body=%s", response.Code, response.Body.String())
	}

	var importedID uint
	ready := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ids, listErr := repository.ListIDs(storage.OAuthPoolChatGPT)
		if listErr == nil && len(ids) == 1 {
			importedID = ids[0]
			account, findErr := repository.Find(storage.OAuthPoolChatGPT, importedID)
			if findErr == nil && account.Status == storage.OAuthStatusAlive && account.InRotation {
				ready = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if importedID == 0 || !ready {
		t.Fatal("imported account was not persisted and activated")
	}

	response = performOAuthRequest(router, http.MethodGet, "/api/oauth-accounts/chatgpt?page=1&page_size=10&status=alive", "")
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}
	var listPayload struct {
		Data struct {
			Items []map[string]any `json:"items"`
			Total int              `json:"total"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &listPayload); err != nil {
		t.Fatal(err)
	}
	if listPayload.Data.Total != 1 || len(listPayload.Data.Items) != 1 || listPayload.Data.Items[0]["masked_identifier"] != "o***@example.com" {
		t.Fatalf("unexpected account page: %s", response.Body.String())
	}
	if _, exists := listPayload.Data.Items[0]["credential_cipher"]; exists {
		t.Fatal("account list exposed encrypted credentials")
	}

	response = performOAuthRequest(router, http.MethodGet, "/api/oauth-accounts/chatgpt/stats", "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"schedulable":1`) {
		t.Fatalf("stats status=%d body=%s", response.Code, response.Body.String())
	}

	response = performOAuthRequest(router, http.MethodDelete, "/api/oauth-accounts/chatgpt/"+strconv.FormatUint(uint64(importedID), 10), "")
	if response.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", response.Code, response.Body.String())
	}
	if ids, _ := repository.ListIDs(storage.OAuthPoolChatGPT); len(ids) != 0 {
		t.Fatalf("deleted account remains: %v", ids)
	}
}

func performOAuthRequest(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
