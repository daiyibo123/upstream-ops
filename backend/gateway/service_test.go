package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	appcrypto "github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/storage"
)

type gatewayProxyTestEnv struct {
	svc        *Service
	channels   *storage.Channels
	groupKeys  *storage.UpstreamGroupKeys
	affinities *storage.GatewayAffinities
	cipher     *appcrypto.Cipher
	localKey   *GatewayKeyOutput
}

func newGatewayProxyTestEnv(t *testing.T, secret string) *gatewayProxyTestEnv {
	t.Helper()
	db, err := storage.Open(storage.DBConfig{
		Driver: storage.DBDriverSQLite,
		Path:   filepath.Join(t.TempDir(), "gateway.db"),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := storage.AutoMigrate(db); err != nil {
		t.Fatalf("migrate db: %v", err)
	}
	if sqlDB, err := db.DB(); err == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}
	cipher, err := appcrypto.NewCipher(secret)
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channels := storage.NewChannels(db)
	gatewayKeys := storage.NewGatewayKeys(db)
	affinities := storage.NewGatewayAffinities(db)
	groupKeys := storage.NewUpstreamGroupKeys(db)
	svc := NewService(channels, gatewayKeys, affinities, groupKeys, cipher, nil, nil)
	localKey, err := svc.CreateGatewayKey(CreateGatewayKeyInput{Name: "test"})
	if err != nil {
		t.Fatalf("create gateway key: %v", err)
	}
	return &gatewayProxyTestEnv{
		svc:        svc,
		channels:   channels,
		groupKeys:  groupKeys,
		affinities: affinities,
		cipher:     cipher,
		localKey:   localKey,
	}
}

func enableTestRectifier(svc *Service) {
	svc.UpdateUpstreamConfig(config.UpstreamConfig{
		RequestRectifier: config.RequestRectifierConfig{
			Enabled:                  true,
			ThinkingSignature:        true,
			ThinkingBudget:           true,
			UnsupportedImageFallback: true,
			HeuristicTextOnlyModels:  false,
		},
	})
}

func TestProxyFailsOverAndSkipsTemporarilyDisabledGroup(t *testing.T) {
	var deadHits int
	deadUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadHits++
		http.Error(w, "dead key", http.StatusUnauthorized)
	}))
	defer deadUpstream.Close()

	var liveHits int
	liveUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		liveHits++
		if got := r.Header.Get("Authorization"); got != "Bearer sk-live" {
			t.Fatalf("authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"ok"}]}`))
	}))
	defer liveUpstream.Close()

	db, err := storage.Open(storage.DBConfig{
		Driver: storage.DBDriverSQLite,
		Path:   filepath.Join(t.TempDir(), "gateway.db"),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := storage.AutoMigrate(db); err != nil {
		t.Fatalf("migrate db: %v", err)
	}
	if sqlDB, err := db.DB(); err == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}

	cipher, err := appcrypto.NewCipher(strings.Repeat("a", 32))
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channels := storage.NewChannels(db)
	gatewayKeys := storage.NewGatewayKeys(db)
	affinities := storage.NewGatewayAffinities(db)
	groupKeys := storage.NewUpstreamGroupKeys(db)
	svc := NewService(channels, gatewayKeys, affinities, groupKeys, cipher, nil, nil)

	localKey, err := svc.CreateGatewayKey(CreateGatewayKeyInput{Name: "test"})
	if err != nil {
		t.Fatalf("create gateway key: %v", err)
	}

	deadChannel := &storage.Channel{Name: "dead", Type: storage.ChannelTypeSub2API, SiteURL: deadUpstream.URL, MonitorEnabled: true}
	liveChannel := &storage.Channel{Name: "live", Type: storage.ChannelTypeSub2API, SiteURL: liveUpstream.URL, MonitorEnabled: true}
	if err := channels.Create(deadChannel); err != nil {
		t.Fatalf("create dead channel: %v", err)
	}
	if err := channels.Create(liveChannel); err != nil {
		t.Fatalf("create live channel: %v", err)
	}
	deadCipher, err := cipher.Encrypt("sk-dead")
	if err != nil {
		t.Fatalf("encrypt dead key: %v", err)
	}
	liveCipher, err := cipher.Encrypt("sk-live")
	if err != nil {
		t.Fatalf("encrypt live key: %v", err)
	}
	if err := groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: deadChannel.ID, ChannelName: "dead", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "dead", GroupName: "dead", Ratio: 0.1, KeyCipher: deadCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert dead group key: %v", err)
	}
	if err := groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: liveChannel.ID, ChannelName: "live", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "live", GroupName: "live", Ratio: 0.2, KeyCipher: liveCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert live group key: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+localKey.Key)
	rec := httptest.NewRecorder()
	if err := svc.Proxy(rec, req, "/v1/models"); err != nil {
		t.Fatalf("proxy first request: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("first status = %d body=%s", rec.Code, rec.Body.String())
	}
	if deadHits != 1 || liveHits != 1 {
		t.Fatalf("hits after first request: dead=%d live=%d", deadHits, liveHits)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+localKey.Key)
	rec = httptest.NewRecorder()
	if err := svc.Proxy(rec, req, "/v1/models"); err != nil {
		t.Fatalf("proxy second request: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("second status = %d body=%s", rec.Code, rec.Body.String())
	}
	if deadHits != 1 || liveHits != 2 {
		t.Fatalf("temporarily disabled group was not skipped: dead=%d live=%d", deadHits, liveHits)
	}

	deadGroup, err := groupKeys.FindByChannelGroup(deadChannel.ID, "dead")
	if err != nil {
		t.Fatalf("load dead group key: %v", err)
	}
	if deadGroup.Status != "dead" || deadGroup.DisabledUntil == nil || deadGroup.FailureCount == 0 {
		t.Fatalf("dead group was not marked unhealthy: %#v", deadGroup)
	}
}

func TestProxyConvertsChatCompletionToResponses(t *testing.T) {
	var upstreamPath string
	var upstreamBody map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&upstreamBody); err != nil {
			t.Fatalf("decode upstream body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_1","model":"gpt-test","output_text":"pong","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}`))
	}))
	defer upstream.Close()

	db, err := storage.Open(storage.DBConfig{
		Driver: storage.DBDriverSQLite,
		Path:   filepath.Join(t.TempDir(), "gateway.db"),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := storage.AutoMigrate(db); err != nil {
		t.Fatalf("migrate db: %v", err)
	}
	if sqlDB, err := db.DB(); err == nil {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}
	cipher, err := appcrypto.NewCipher(strings.Repeat("b", 32))
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channels := storage.NewChannels(db)
	gatewayKeys := storage.NewGatewayKeys(db)
	affinities := storage.NewGatewayAffinities(db)
	groupKeys := storage.NewUpstreamGroupKeys(db)
	svc := NewService(channels, gatewayKeys, affinities, groupKeys, cipher, nil, nil)
	localKey, err := svc.CreateGatewayKey(CreateGatewayKeyInput{Name: "chat"})
	if err != nil {
		t.Fatalf("create gateway key: %v", err)
	}
	channel := &storage.Channel{Name: "upstream", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt upstream key: %v", err)
	}
	if err := groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "upstream", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-test","messages":[{"role":"user","content":"ping"}],"max_tokens":16}`))
	req.Header.Set("Authorization", "Bearer "+localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := svc.Proxy(rec, req, "/v1/chat/completions"); err != nil {
		t.Fatalf("proxy chat request: %v", err)
	}
	if upstreamPath != "/v1/responses" {
		t.Fatalf("upstream path = %q", upstreamPath)
	}
	if _, ok := upstreamBody["messages"]; ok {
		t.Fatalf("upstream body still has messages: %#v", upstreamBody)
	}
	if _, ok := upstreamBody["input"]; !ok {
		t.Fatalf("upstream body missing input: %#v", upstreamBody)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "chat.completion") {
		t.Fatalf("response status=%d body=%s", rec.Code, rec.Body.String())
	}
	updated, err := gatewayKeys.FindByID(localKey.ID)
	if err != nil {
		t.Fatalf("load gateway key: %v", err)
	}
	if updated.TotalTokens != 5 || updated.TodayTokens != 5 {
		t.Fatalf("usage not recorded: %#v", updated)
	}
}

func TestChatToResponsesPreservesImageURLBlocks(t *testing.T) {
	body := []byte(`{"model":"gpt-test","messages":[{"role":"user","content":[{"type":"text","text":"look"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]}]}`)
	converted, _, err := chatToResponsesBody(body)
	if err != nil {
		t.Fatalf("convert chat: %v", err)
	}
	encoded := string(converted)
	if !strings.Contains(encoded, "input_text") || !strings.Contains(encoded, "input_image") || !strings.Contains(encoded, "data:image/png;base64,AAAA") {
		t.Fatalf("chat image was not preserved as responses input: %s", encoded)
	}
}

func TestProxyPrefersAffinityForStatefulResponsesRequest(t *testing.T) {
	var cheapHits int
	cheapUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cheapHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_cheap","model":"gpt-test","output_text":"cheap","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
	}))
	defer cheapUpstream.Close()

	var stickyHits int
	stickyUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stickyHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_sticky_next","model":"gpt-test","output_text":"sticky","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
	}))
	defer stickyUpstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("f", 32))
	cheapChannel := &storage.Channel{Name: "cheap", Type: storage.ChannelTypeSub2API, SiteURL: cheapUpstream.URL, MonitorEnabled: true}
	stickyChannel := &storage.Channel{Name: "sticky", Type: storage.ChannelTypeSub2API, SiteURL: stickyUpstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(cheapChannel); err != nil {
		t.Fatalf("create cheap channel: %v", err)
	}
	if err := env.channels.Create(stickyChannel); err != nil {
		t.Fatalf("create sticky channel: %v", err)
	}
	cheapCipher, err := env.cipher.Encrypt("sk-cheap")
	if err != nil {
		t.Fatalf("encrypt cheap key: %v", err)
	}
	stickyCipher, err := env.cipher.Encrypt("sk-sticky")
	if err != nil {
		t.Fatalf("encrypt sticky key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: cheapChannel.ID, ChannelName: "cheap", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "cheap", GroupName: "cheap", Ratio: 0.1, KeyCipher: cheapCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert cheap group key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: stickyChannel.ID, ChannelName: "sticky", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "sticky", GroupName: "sticky", Ratio: 0.9, KeyCipher: stickyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert sticky group key: %v", err)
	}
	stickyGroup, err := env.groupKeys.FindByChannelGroup(stickyChannel.ID, "sticky")
	if err != nil {
		t.Fatalf("load sticky group: %v", err)
	}
	if err := env.affinities.Upsert(HashKey("response:resp_previous"), stickyGroup.ID, time.Now().Add(time.Hour), time.Now()); err != nil {
		t.Fatalf("upsert affinity: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","previous_response_id":"resp_previous","input":"continue"}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "sticky") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if stickyHits != 1 || cheapHits != 0 {
		t.Fatalf("affinity was not preferred: sticky=%d cheap=%d", stickyHits, cheapHits)
	}
}

func TestProxyFailsOverOnHTTP200ErrorPayload(t *testing.T) {
	var deadHits int
	deadUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":{"message":"dead key"}}`))
	}))
	defer deadUpstream.Close()

	var liveHits int
	liveUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		liveHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_live","model":"gpt-test","output_text":"pong","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}`))
	}))
	defer liveUpstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("c", 32))
	deadChannel := &storage.Channel{Name: "dead-json", Type: storage.ChannelTypeSub2API, SiteURL: deadUpstream.URL, MonitorEnabled: true}
	liveChannel := &storage.Channel{Name: "live-json", Type: storage.ChannelTypeSub2API, SiteURL: liveUpstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(deadChannel); err != nil {
		t.Fatalf("create dead channel: %v", err)
	}
	if err := env.channels.Create(liveChannel); err != nil {
		t.Fatalf("create live channel: %v", err)
	}
	deadCipher, err := env.cipher.Encrypt("sk-dead")
	if err != nil {
		t.Fatalf("encrypt dead key: %v", err)
	}
	liveCipher, err := env.cipher.Encrypt("sk-live")
	if err != nil {
		t.Fatalf("encrypt live key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: deadChannel.ID, ChannelName: "dead-json", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "dead", GroupName: "dead", Ratio: 0.1, KeyCipher: deadCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert dead group key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: liveChannel.ID, ChannelName: "live-json", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "live", GroupName: "live", Ratio: 0.2, KeyCipher: liveCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert live group key: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping"}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "resp_live") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if deadHits != 1 || liveHits != 1 {
		t.Fatalf("hits: dead=%d live=%d", deadHits, liveHits)
	}
	deadGroup, err := env.groupKeys.FindByChannelGroup(deadChannel.ID, "dead")
	if err != nil {
		t.Fatalf("load dead group key: %v", err)
	}
	if deadGroup.Status != "dead" || deadGroup.DisabledUntil == nil {
		t.Fatalf("dead group was not disabled: %#v", deadGroup)
	}
}

func TestProxyFailsOverOnUnsupportedModelBadRequest(t *testing.T) {
	var badHits int
	badUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badHits++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"model gpt-test is unsupported by this channel"}}`))
	}))
	defer badUpstream.Close()

	var liveHits int
	liveUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		liveHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_live_model","model":"gpt-test","output_text":"ok","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
	}))
	defer liveUpstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("g", 32))
	badChannel := &storage.Channel{Name: "bad-model", Type: storage.ChannelTypeSub2API, SiteURL: badUpstream.URL, MonitorEnabled: true}
	liveChannel := &storage.Channel{Name: "live-model", Type: storage.ChannelTypeSub2API, SiteURL: liveUpstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(badChannel); err != nil {
		t.Fatalf("create bad channel: %v", err)
	}
	if err := env.channels.Create(liveChannel); err != nil {
		t.Fatalf("create live channel: %v", err)
	}
	badCipher, err := env.cipher.Encrypt("sk-bad")
	if err != nil {
		t.Fatalf("encrypt bad key: %v", err)
	}
	liveCipher, err := env.cipher.Encrypt("sk-live")
	if err != nil {
		t.Fatalf("encrypt live key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: badChannel.ID, ChannelName: "bad-model", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "bad", GroupName: "bad", Ratio: 0.1, KeyCipher: badCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert bad group key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: liveChannel.ID, ChannelName: "live-model", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "live", GroupName: "live", Ratio: 0.2, KeyCipher: liveCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert live group key: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping"}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "resp_live_model") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if badHits != 1 || liveHits != 1 {
		t.Fatalf("unsupported model did not fail over: bad=%d live=%d", badHits, liveHits)
	}
}

func TestProxyConcurrentRequestsFailOverAndRecordUsage(t *testing.T) {
	var deadHits int64
	deadUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"dead key"}}`))
		atomic.AddInt64(&deadHits, 1)
	}))
	defer deadUpstream.Close()

	liveUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_live_concurrent","model":"gpt-test","output_text":"ok","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
	}))
	defer liveUpstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("h", 32))
	deadChannel := &storage.Channel{Name: "dead-concurrent", Type: storage.ChannelTypeSub2API, SiteURL: deadUpstream.URL, MonitorEnabled: true}
	liveChannel := &storage.Channel{Name: "live-concurrent", Type: storage.ChannelTypeSub2API, SiteURL: liveUpstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(deadChannel); err != nil {
		t.Fatalf("create dead channel: %v", err)
	}
	if err := env.channels.Create(liveChannel); err != nil {
		t.Fatalf("create live channel: %v", err)
	}
	deadCipher, err := env.cipher.Encrypt("sk-dead")
	if err != nil {
		t.Fatalf("encrypt dead key: %v", err)
	}
	liveCipher, err := env.cipher.Encrypt("sk-live")
	if err != nil {
		t.Fatalf("encrypt live key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: deadChannel.ID, ChannelName: "dead-concurrent", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "dead", GroupName: "dead", Ratio: 0.1, KeyCipher: deadCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert dead group key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: liveChannel.ID, ChannelName: "live-concurrent", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "live", GroupName: "live", Ratio: 0.2, KeyCipher: liveCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert live group key: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan string, 15)
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping"}`))
			req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
				errs <- err.Error()
				return
			}
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "resp_live_concurrent") {
				errs <- rec.Body.String()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent request failed: %s", err)
	}
	updated, err := env.svc.gateway.FindByID(env.localKey.ID)
	if err != nil {
		t.Fatalf("load gateway key: %v", err)
	}
	if updated.TotalTokens != 30 || updated.TodayTokens != 30 {
		t.Fatalf("usage not atomically recorded: %#v", updated)
	}
	if got := atomic.LoadInt64(&deadHits); got <= 0 || got > 15 {
		t.Fatalf("dead upstream hit count out of range: %d", got)
	}
}

func TestOrderCandidatesUsesRuntimeLatencyWithinSamePrice(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("i", 32))
	env.svc.recordRuntimeSuccess(1, 200*time.Millisecond)
	env.svc.recordRuntimeSuccess(2, 20*time.Millisecond)
	candidates := []storage.UpstreamGroupKey{
		{ID: 1, Status: "alive", Ratio: 1},
		{ID: 2, Status: "alive", Ratio: 1},
		{ID: 3, Status: "alive", Ratio: 0.5},
	}
	ordered := env.svc.orderCandidatesWithRuntime(candidates)
	if ordered[0].ID != 3 {
		t.Fatalf("cheapest candidate should still win: %#v", ordered)
	}
	if ordered[1].ID != 2 || ordered[2].ID != 1 {
		t.Fatalf("same-price candidates should prefer lower runtime latency: %#v", ordered)
	}
}

func TestOrderCandidatesRespectsManualPriorityBeforeRatio(t *testing.T) {
	candidates := []storage.UpstreamGroupKey{
		{ID: 1, Status: "alive", Ratio: 0.01, Priority: 0},
		{ID: 2, Status: "alive", Ratio: 1, Priority: 20},
		{ID: 3, Status: "alive", Ratio: 0.05, Priority: 10},
	}
	ordered := orderCandidates(candidates)
	if ordered[0].ID != 2 || ordered[1].ID != 3 || ordered[2].ID != 1 {
		t.Fatalf("manual priority should win before ratio: %#v", ordered)
	}
}

func TestOrderCandidatesPrefersCharityBeforePaid(t *testing.T) {
	// 公益渠道即便倍率更高，也应排在付费渠道前面。
	candidates := []storage.UpstreamGroupKey{
		{ID: 1, Status: "alive", Ratio: 0.01, Charity: false},        // 便宜的付费
		{ID: 2, Status: "alive", Ratio: 0.5, Charity: true},          // 贵一点的公益
		{ID: 3, Status: "alive", Ratio: 0.2, Charity: true},          // 更便宜的公益
	}
	ordered := orderCandidates(candidates)
	// 公益先行：ID3(0.2公益) → ID2(0.5公益) → ID1(付费)
	if ordered[0].ID != 3 || ordered[1].ID != 2 || ordered[2].ID != 1 {
		t.Fatalf("charity should be scheduled before paid: %#v", ordered)
	}
}

func TestHealthProbeRequiresGenerationSuccess(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/models":
			_, _ = w.Write([]byte(`{"data":[{"id":"gpt-test"}]}`))
		case "/v1/responses":
			_, _ = w.Write([]byte(`{"error":{"message":"generation blocked"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("l", 32))
	channel := &storage.Channel{Name: "blocked-generation", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "blocked-generation", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "unknown",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}
	group, err := env.groupKeys.FindByChannelGroup(channel.ID, "default")
	if err != nil {
		t.Fatalf("load group: %v", err)
	}
	status, _, _, err := env.svc.healthProbeCandidate(context.Background(), group)
	if err == nil {
		t.Fatalf("expected health probe to fail when generation endpoint returns error")
	}
	if status != http.StatusOK || !strings.Contains(err.Error(), "generation blocked") {
		t.Fatalf("status=%d err=%v", status, err)
	}
}

func TestHealthProbeUsesStreamingOpenAIRequest(t *testing.T) {
	var seen map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"id":"gpt-test"}]}`))
		case "/v1/responses":
			if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
				t.Errorf("decode response probe body: %v", err)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: response.output_text.delta\n"))
			_, _ = w.Write([]byte(`data: {"type":"response.output_text.delta","delta":"ok"}` + "\n\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("m", 32))
	channel := &storage.Channel{Name: "stream-openai", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "stream-openai", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "unknown",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}
	group, err := env.groupKeys.FindByChannelGroup(channel.ID, "default")
	if err != nil {
		t.Fatalf("load group: %v", err)
	}
	status, _, _, err := env.svc.healthProbeCandidate(context.Background(), group)
	if err != nil || status != http.StatusOK {
		t.Fatalf("status=%d err=%v", status, err)
	}
	if seen["stream"] != true || seen["max_output_tokens"] != float64(1) {
		t.Fatalf("probe body = %#v", seen)
	}
}

func TestHealthProbeUsesStreamingClaudeRequest(t *testing.T) {
	var seen map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			http.NotFound(w, r)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Errorf("decode claude probe body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message_start\n"))
		_, _ = w.Write([]byte(`data: {"type":"message_start","message":{"id":"msg_1","role":"assistant"}}` + "\n\n"))
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("n", 32))
	channel := &storage.Channel{Name: "stream-claude", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "stream-claude", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "unknown", ClientFormat: "claude",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}
	group, err := env.groupKeys.FindByChannelGroup(channel.ID, "default")
	if err != nil {
		t.Fatalf("load group: %v", err)
	}
	status, _, _, err := env.svc.healthProbeCandidate(context.Background(), group)
	if err != nil || status != http.StatusOK {
		t.Fatalf("status=%d err=%v", status, err)
	}
	if seen["stream"] != true || seen["max_tokens"] != float64(1) {
		t.Fatalf("probe body = %#v", seen)
	}
}

func TestTestAllGroupKeysStartsAllEnabledGroupsConcurrently(t *testing.T) {
	var inFlight int64
	var maxInFlight int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt64(&inFlight, 1)
		defer atomic.AddInt64(&inFlight, -1)
		for {
			previous := atomic.LoadInt64(&maxInFlight)
			if current <= previous || atomic.CompareAndSwapInt64(&maxInFlight, previous, current) {
				break
			}
		}
		time.Sleep(120 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/models":
			_, _ = w.Write([]byte(`{"data":[{"id":"gpt-test"}]}`))
		case "/v1/responses":
			_, _ = w.Write([]byte(`{"id":"resp_probe","output_text":"ok"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("p", 32))
	channel := &storage.Channel{Name: "parallel-health", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	for i := 0; i < 7; i++ {
		ref := "group-" + strconv.Itoa(i)
		if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
			ChannelID: channel.ID, ChannelName: channel.Name, ChannelType: storage.ChannelTypeSub2API,
			GroupRef: ref, GroupName: ref, Ratio: 1, KeyCipher: keyCipher, Status: "unknown",
		}); err != nil {
			t.Fatalf("insert group %d: %v", i, err)
		}
	}

	result, err := env.svc.TestAllGroupKeys(context.Background())
	if err != nil {
		t.Fatalf("test all groups: %v", err)
	}
	if result.Alive != 7 {
		t.Fatalf("alive = %d, want 7; result=%#v", result.Alive, result)
	}
	if got := atomic.LoadInt64(&maxInFlight); got < 7 {
		t.Fatalf("maximum concurrent upstream requests = %d, want all 7 groups to start together", got)
	}
}

func TestProxySkipsCandidateAtConcurrencyLimit(t *testing.T) {
	var limitedHits int64
	limitedUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&limitedHits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_limited","model":"gpt-test","output_text":"limited","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
	}))
	defer limitedUpstream.Close()

	var fallbackHits int64
	fallbackUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&fallbackHits, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_fallback","model":"gpt-test","output_text":"fallback","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
	}))
	defer fallbackUpstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("j", 32))
	limitedChannel := &storage.Channel{Name: "limited", Type: storage.ChannelTypeSub2API, SiteURL: limitedUpstream.URL, MonitorEnabled: true}
	fallbackChannel := &storage.Channel{Name: "fallback", Type: storage.ChannelTypeSub2API, SiteURL: fallbackUpstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(limitedChannel); err != nil {
		t.Fatalf("create limited channel: %v", err)
	}
	if err := env.channels.Create(fallbackChannel); err != nil {
		t.Fatalf("create fallback channel: %v", err)
	}
	limitedCipher, err := env.cipher.Encrypt("sk-limited")
	if err != nil {
		t.Fatalf("encrypt limited key: %v", err)
	}
	fallbackCipher, err := env.cipher.Encrypt("sk-fallback")
	if err != nil {
		t.Fatalf("encrypt fallback key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: limitedChannel.ID, ChannelName: "limited", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "limited", GroupName: "limited", Ratio: 0.1, KeyCipher: limitedCipher, Status: "alive", ConcurrencyLimit: 1,
	}); err != nil {
		t.Fatalf("insert limited group key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: fallbackChannel.ID, ChannelName: "fallback", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "fallback", GroupName: "fallback", Ratio: 0.2, KeyCipher: fallbackCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert fallback group key: %v", err)
	}
	limitedGroup, err := env.groupKeys.FindByChannelGroup(limitedChannel.ID, "limited")
	if err != nil {
		t.Fatalf("load limited group: %v", err)
	}
	release, ok := env.svc.tryAcquireCandidate(limitedGroup.ID, 1)
	if !ok {
		t.Fatalf("pre-acquire limited candidate")
	}
	defer release()

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping"}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "resp_fallback") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if atomic.LoadInt64(&limitedHits) != 0 || atomic.LoadInt64(&fallbackHits) != 1 {
		t.Fatalf("unexpected hits: limited=%d fallback=%d", atomic.LoadInt64(&limitedHits), atomic.LoadInt64(&fallbackHits))
	}
}

func TestUpsertPreservesExistingConcurrencyLimit(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("k", 32))
	channel := &storage.Channel{Name: "preserve-limit", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com", MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-test")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "preserve-limit", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive", ConcurrencyLimit: 3,
	}); err != nil {
		t.Fatalf("insert group: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "preserve-limit", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default-renamed", Ratio: 0.5, Status: "unknown",
	}); err != nil {
		t.Fatalf("update group: %v", err)
	}
	updated, err := env.groupKeys.FindByChannelGroup(channel.ID, "default")
	if err != nil {
		t.Fatalf("load group: %v", err)
	}
	if updated.ConcurrencyLimit != 3 || updated.Ratio != 0.5 || updated.GroupName != "default-renamed" {
		t.Fatalf("upsert did not preserve concurrency limit while updating metadata: %#v", updated)
	}
}

func TestProxyStreamFailsOverOnSSEErrorBeforeWriting(t *testing.T) {
	var deadHits int
	deadUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadHits++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: error\n" +
			"data: {\"error\":{\"message\":\"dead stream\"}}\n\n"))
	}))
	defer deadUpstream.Close()

	var liveHits int
	liveUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		liveHits++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\n" +
			"data: {\"type\":\"response.output_text.delta\",\"delta\":\"pong\",\"response_id\":\"resp_live\"}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_live\",\"model\":\"gpt-test\",\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}\n\n"))
	}))
	defer liveUpstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("d", 32))
	deadChannel := &storage.Channel{Name: "dead-stream", Type: storage.ChannelTypeSub2API, SiteURL: deadUpstream.URL, MonitorEnabled: true}
	liveChannel := &storage.Channel{Name: "live-stream", Type: storage.ChannelTypeSub2API, SiteURL: liveUpstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(deadChannel); err != nil {
		t.Fatalf("create dead channel: %v", err)
	}
	if err := env.channels.Create(liveChannel); err != nil {
		t.Fatalf("create live channel: %v", err)
	}
	deadCipher, err := env.cipher.Encrypt("sk-dead")
	if err != nil {
		t.Fatalf("encrypt dead key: %v", err)
	}
	liveCipher, err := env.cipher.Encrypt("sk-live")
	if err != nil {
		t.Fatalf("encrypt live key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: deadChannel.ID, ChannelName: "dead-stream", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "dead", GroupName: "dead", Ratio: 0.1, KeyCipher: deadCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert dead group key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: liveChannel.ID, ChannelName: "live-stream", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "live", GroupName: "live", Ratio: 0.2, KeyCipher: liveCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert live group key: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-test","stream":true,"messages":[{"role":"user","content":"ping"}]}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/chat/completions"); err != nil {
		t.Fatalf("proxy stream request: %v", err)
	}
	out := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(out, "pong") || strings.Contains(out, "dead stream") {
		t.Fatalf("status=%d body=%s", rec.Code, out)
	}
	if deadHits != 1 || liveHits != 1 {
		t.Fatalf("hits: dead=%d live=%d", deadHits, liveHits)
	}
}

func TestProxyRetriesSameCandidateWithImageFallback(t *testing.T) {
	var hits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if hits == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"model does not support image input"}}`))
			return
		}
		if !strings.Contains(string(body), "[Unsupported Image]") {
			t.Fatalf("fallback body missing unsupported image marker: %s", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_fixed","model":"gpt-test","output_text":"ok","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("e", 32))
	enableTestRectifier(env.svc)
	channel := &storage.Channel{Name: "image-fallback", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "image-fallback", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}

	reqBody := `{"model":"text-only-test","input":[{"role":"user","content":[{"type":"input_text","text":"describe"},{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "resp_fixed") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if hits != 2 {
		t.Fatalf("same candidate was not retried once: hits=%d", hits)
	}
}

// TestProxyFallsBackToChatWhenUpstreamLacksResponses 覆盖"不开路由直连"的关键场景：
func TestProxySynthesizesCompletedWhenUpstreamDropsStreamMidway(t *testing.T) {
	// 上游发了 delta 后直接把连接断掉（不发 response.completed），模拟真实的中途断流。
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			_, _ = w.Write([]byte("event: response.output_text.delta\n" +
				"data: {\"type\":\"response.output_text.delta\",\"delta\":\"pong\",\"response_id\":\"resp_mid\",\"model\":\"gpt-test\"}\n\n"))
			f.Flush()
		}
		// 用 hijack 直接断开，制造 EOF 之外的中途断流。
		hj, ok := w.(http.Hijacker)
		if ok {
			conn, _, err := hj.Hijack()
			if err == nil {
				_ = conn.Close()
			}
		}
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("m", 32))
	channel := &storage.Channel{Name: "mid-drop", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "mid-drop", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	out := rec.Body.String()
	if !strings.Contains(out, "pong") {
		t.Fatalf("missing streamed delta: %s", out)
	}
	if !strings.Contains(out, "response.completed") {
		t.Fatalf("gateway must synthesize response.completed even when upstream drops mid-stream: %s", out)
	}
}

// TestProxyFallsBackToChatWhenUpstreamLacksResponses 覆盖"不开路由直连"的关键场景：
// Codex 直连网关发原生 /v1/responses，但上游只支持 /v1/chat/completions（返回 404）。
// 网关应在同一候选上自动降级到 chat 再打一次并成功，而不是把断流错误抛给客户端。
func TestProxyFallsBackToChatWhenUpstreamLacksResponses(t *testing.T) {
	var responsesHits, chatHits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/responses") {
			responsesHits++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("404 page not found"))
			return
		}
		if strings.Contains(r.URL.Path, "/chat/completions") {
			chatHits++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"chatcmpl_ok","model":"gpt-test","choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
			return
		}
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("c", 32))
	channel := &storage.Channel{Name: "chat-only", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "chat-only", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping"}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "pong") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if responsesHits != 1 || chatHits != 1 {
		t.Fatalf("expected responses->chat fallback on same candidate: responses=%d chat=%d", responsesHits, chatHits)
	}
}

// TestProxyStreamFallsBackToChatWhenUpstreamLacksResponses 覆盖 Codex 直连的真实场景：
// 流式 /v1/responses 打到只支持 chat 的上游，网关降级到 chat 流并把 chat chunk 转回
// responses SSE 事件，客户端最终能收到 response.completed。
func TestProxyStreamFallsBackToChatWhenUpstreamLacksResponses(t *testing.T) {
	var responsesHits, chatHits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/responses") {
			responsesHits++
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("404 page not found"))
			return
		}
		if strings.Contains(r.URL.Path, "/chat/completions") {
			chatHits++
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("data: {\"id\":\"chatcmpl_s\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n" +
				"data: {\"id\":\"chatcmpl_s\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"pong\"}}]}\n\n" +
				"data: {\"id\":\"chatcmpl_s\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n" +
				"data: [DONE]\n\n"))
			return
		}
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("s", 32))
	channel := &storage.Channel{Name: "chat-only-stream", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "chat-only-stream", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	out := rec.Body.String()
	if responsesHits != 1 || chatHits != 1 {
		t.Fatalf("expected responses->chat stream fallback: responses=%d chat=%d", responsesHits, chatHits)
	}
	if !strings.Contains(out, "response.completed") || !strings.Contains(out, "pong") || !strings.Contains(out, "[DONE]") {
		t.Fatalf("stream fallback output malformed: %s", out)
	}
}

func TestSynthesizesResponseCompletedWhenUpstreamOmitsIt(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// 上游发了 delta，但故意不发 response.completed 就结束流（模拟不规范上游）。
		_, _ = w.Write([]byte("event: response.output_text.delta\n" +
			"data: {\"type\":\"response.output_text.delta\",\"delta\":\"pong\",\"response_id\":\"resp_x\",\"model\":\"gpt-test\"}\n\n"))
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("y", 32))
	channel := &storage.Channel{Name: "no-completed", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "no-completed", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	out := rec.Body.String()
	if !strings.Contains(out, "pong") {
		t.Fatalf("missing delta content: %s", out)
	}
	if !strings.Contains(out, "response.completed") {
		t.Fatalf("gateway should synthesize response.completed when upstream omits it: %s", out)
	}
}

func TestNormalizeThinkingBudgetUsesResponsesFieldsForConvertedRequests(t *testing.T) {
	body, changed := normalizeThinkingBudget([]byte(`{"model":"gpt-test","input":"hi","max_tokens":1024}`), "responses")
	if !changed {
		t.Fatalf("expected thinking budget normalization")
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("decode normalized body: %v", err)
	}
	if _, ok := raw["thinking"]; ok {
		t.Fatalf("responses request should not gain anthropic thinking field: %#v", raw)
	}
	if _, ok := raw["max_tokens"]; ok {
		t.Fatalf("responses request should not keep max_tokens: %#v", raw)
	}
	if intField(raw, "max_output_tokens") != 64000 {
		t.Fatalf("max_output_tokens not raised: %#v", raw)
	}
	reasoning, _ := raw["reasoning"].(map[string]any)
	if reasoning["effort"] != "high" {
		t.Fatalf("reasoning not normalized: %#v", raw)
	}
}

func TestNormalizeThinkingBudgetKeepsAnthropicFieldsForRawRequests(t *testing.T) {
	body, changed := normalizeThinkingBudget([]byte(`{"model":"claude-test","messages":[],"max_tokens":1024}`), "raw")
	if !changed {
		t.Fatalf("expected thinking budget normalization")
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("decode normalized body: %v", err)
	}
	thinking, _ := raw["thinking"].(map[string]any)
	if thinking["type"] != "enabled" || intField(thinking, "budget_tokens") != 32000 {
		t.Fatalf("thinking not normalized: %#v", raw)
	}
	if intField(raw, "max_tokens") != 64000 {
		t.Fatalf("max_tokens not raised: %#v", raw)
	}
}

func TestClaudeToResponsesPreservesImagesAndReasoning(t *testing.T) {
	body := []byte(`{"model":"claude-test","max_tokens":1024,"thinking":{"type":"enabled","budget_tokens":32000},"messages":[{"role":"user","content":[{"type":"text","text":"look"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}]}]}`)
	converted, stream, err := claudeToResponsesBody(body)
	if err != nil {
		t.Fatalf("convert claude: %v", err)
	}
	if stream {
		t.Fatalf("stream = true")
	}
	var raw map[string]any
	if err := json.Unmarshal(converted, &raw); err != nil {
		t.Fatalf("decode converted: %v", err)
	}
	if _, ok := raw["reasoning"].(map[string]any); !ok {
		t.Fatalf("missing reasoning: %#v", raw)
	}
	encoded := string(converted)
	if !strings.Contains(encoded, "input_image") || !strings.Contains(encoded, "data:image/png;base64,AAAA") {
		t.Fatalf("image was not preserved: %s", encoded)
	}
}

func TestOrderCandidatesPrefersHealthyCheapStable(t *testing.T) {
	candidates := []storage.UpstreamGroupKey{
		{ID: 1, Status: "alive", Ratio: 1, TotalTokens: 100},
		{ID: 2, Status: "alive", Ratio: 1, TotalTokens: 10},
		{ID: 3, Status: "dead", Ratio: 0.1, TotalTokens: 0},
		{ID: 4, Status: "alive", Ratio: 2, TotalTokens: 0},
	}
	ordered := orderCandidates(candidates)
	if ordered[0].ID != 1 {
		t.Fatalf("first candidate = %#v", ordered[0])
	}
	if ordered[len(ordered)-1].ID != 3 {
		t.Fatalf("dead candidate should be last: %#v", ordered)
	}
}

func TestExtractStreamUsageFromResponsesSSE(t *testing.T) {
	body := []byte("event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":7,\"output_tokens\":5,\"total_tokens\":12}}}\n\n" +
		"data: [DONE]\n")
	usage := extractStreamUsage(body)
	if usage.Prompt != 7 || usage.Completion != 5 || usage.Total != 12 {
		t.Fatalf("usage = %#v", usage)
	}
}

func TestStreamRawSSEPreservesChatDone(t *testing.T) {
	body := strings.NewReader("data: {\"id\":\"chatcmpl_1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"pong\"}}]}\n\n" +
		"data: [DONE]\n\n")
	rec := httptest.NewRecorder()
	usage, err := streamRawSSE(rec, nil, newSSEStreamReader(body), "raw")
	if err != nil {
		t.Fatalf("stream raw sse: %v", err)
	}
	out := rec.Body.String()
	if !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("raw chat stream should preserve [DONE]: %s", out)
	}
	if strings.Contains(out, "response.completed") {
		t.Fatalf("raw chat stream should not synthesize responses events: %s", out)
	}
	if usage.SoftFailure != "" {
		t.Fatalf("raw chat stream should not mark soft failure: %#v", usage)
	}
}

func TestStreamResponsesAsChat(t *testing.T) {
	body := strings.NewReader("event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"pong\",\"response_id\":\"resp_1\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\",\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}\n\n")
	rec := httptest.NewRecorder()
	usage, err := streamResponsesAsChat(rec, body)
	if err != nil {
		t.Fatalf("stream chat: %v", err)
	}
	out := rec.Body.String()
	if !strings.Contains(out, "chat.completion.chunk") || !strings.Contains(out, "pong") || !strings.Contains(out, "[DONE]") {
		t.Fatalf("unexpected chat stream: %s", out)
	}
	if usage.Total != 5 {
		t.Fatalf("usage = %#v", usage)
	}
}
