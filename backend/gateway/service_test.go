package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
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
	"github.com/bejix/upstream-ops/backend/connector"
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
	if deadGroup.Status != "auth_failed" || deadGroup.Enabled || deadGroup.DisabledUntil != nil || deadGroup.FailureCount == 0 {
		t.Fatalf("unauthorized upstream key was not disabled as auth_failed: %#v", deadGroup)
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
	if math.Abs(updated.TotalCost-0.000075) > 0.000000001 || math.Abs(updated.TodayCost-0.000075) > 0.000000001 {
		t.Fatalf("cost = today %.8f total %.8f, want 0.000075", updated.TodayCost, updated.TotalCost)
	}
}

func TestGatewayKeyBalanceLimitDisablesKey(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_balance","model":"gpt-test","output_text":"ok","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("q", 32))
	balanceLimit := 1.0
	if _, err := env.svc.UpdateGatewayKey(env.localKey.ID, UpdateGatewayKeyInput{
		BalanceLimit: &balanceLimit,
	}); err != nil {
		t.Fatalf("update gateway key balance: %v", err)
	}

	channel := &storage.Channel{Name: "balance", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-balance")
	if err != nil {
		t.Fatalf("encrypt upstream key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "balance", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, InputPricePerMillion: 500000, OutputPricePerMillion: 500000, KeyCipher: keyCipher, Status: "alive",
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
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}

	updated, err := env.svc.gateway.FindByID(env.localKey.ID)
	if err != nil {
		t.Fatalf("load gateway key: %v", err)
	}
	if updated.Enabled {
		t.Fatalf("key should be disabled after balance exhausted: %#v", updated)
	}
	if updated.TodayTokens != 2 || updated.TotalTokens != 2 {
		t.Fatalf("tokens = today %d total %d, want 2/2", updated.TodayTokens, updated.TotalTokens)
	}
	if updated.TodayCost != 1 || updated.TotalCost != 1 {
		t.Fatalf("cost = today %.4f total %.4f, want 1/1", updated.TodayCost, updated.TotalCost)
	}
	if _, err := env.svc.Authenticate(env.localKey.Key, "127.0.0.1"); err == nil {
		t.Fatal("exhausted key should not authenticate")
	}
}

func TestGatewayUsageCostUsesGroupPricesAndRatio(t *testing.T) {
	got := gatewayUsageCost(usageTokens{Prompt: 3, Completion: 2, Total: 5}, &storage.UpstreamGroupKey{
		Ratio:                 2,
		InputPricePerMillion:  5,
		OutputPricePerMillion: 30,
	})
	if math.Abs(got-0.00015) > 0.000000001 {
		t.Fatalf("cost = %.8f, want 0.00015", got)
	}
}

func TestGatewayKeyConcurrencyLimitQueuesRequests(t *testing.T) {
	var active int64
	var maxActive int64
	hits := make(chan int64, 2)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseFirst) })
	}
	defer release()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := atomic.AddInt64(&active, 1)
		for {
			observed := atomic.LoadInt64(&maxActive)
			if current <= observed || atomic.CompareAndSwapInt64(&maxActive, observed, current) {
				break
			}
		}
		hits <- current
		if current == 1 {
			<-releaseFirst
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_queue","model":"gpt-test","output_text":"ok","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
		atomic.AddInt64(&active, -1)
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("r", 32))
	concurrencyLimit := 1
	if _, err := env.svc.UpdateGatewayKey(env.localKey.ID, UpdateGatewayKeyInput{
		ConcurrencyLimit: &concurrencyLimit,
	}); err != nil {
		t.Fatalf("update gateway key concurrency: %v", err)
	}

	channel := &storage.Channel{Name: "queue", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-queue")
	if err != nil {
		t.Fatalf("encrypt upstream key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "queue", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}

	doRequest := func() error {
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping"}`))
		req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
			return err
		}
		if rec.Code != http.StatusOK {
			return fmt.Errorf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		return nil
	}

	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() { firstDone <- doRequest() }()

	select {
	case <-hits:
	case err := <-firstDone:
		release()
		t.Fatalf("first request finished before hitting upstream: %v", err)
	case <-time.After(time.Second):
		release()
		t.Fatal("first request did not reach upstream")
	}

	go func() { secondDone <- doRequest() }()
	select {
	case hit := <-hits:
		release()
		t.Fatalf("second request reached upstream before first released; active=%d", hit)
	case err := <-secondDone:
		release()
		t.Fatalf("second request finished instead of queueing: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	release()
	for i := 0; i < 2; i++ {
		select {
		case err := <-firstDone:
			if err != nil {
				t.Fatalf("first request: %v", err)
			}
		case err := <-secondDone:
			if err != nil {
				t.Fatalf("second request: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("queued requests did not finish")
		}
	}
	if got := atomic.LoadInt64(&maxActive); got != 1 {
		t.Fatalf("max active upstream requests = %d, want 1", got)
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
	if deadGroup.Status != "upstream_error" || deadGroup.DisabledUntil != nil || !deadGroup.Enabled {
		t.Fatalf("transient upstream error should be recorded without immediate cooldown/disable: %#v", deadGroup)
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

func TestProxyFailsOverOnOpenAIModelAccessBadRequest(t *testing.T) {
	var badHits int
	badUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badHits++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"The model gpt-5.6 does not exist or you do not have access to it.","type":"invalid_request_error","param":"model","code":"model_not_found"}}`))
	}))
	defer badUpstream.Close()

	var liveHits int
	liveUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		liveHits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_live_56","model":"gpt-5.6","output_text":"ok","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
	}))
	defer liveUpstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("5", 32))
	badChannel := &storage.Channel{Name: "bad-5.6", Type: storage.ChannelTypeSub2API, SiteURL: badUpstream.URL, MonitorEnabled: true}
	liveChannel := &storage.Channel{Name: "live-5.6", Type: storage.ChannelTypeSub2API, SiteURL: liveUpstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(badChannel); err != nil {
		t.Fatalf("create bad channel: %v", err)
	}
	if err := env.channels.Create(liveChannel); err != nil {
		t.Fatalf("create live channel: %v", err)
	}
	badCipher, err := env.cipher.Encrypt("sk-bad-56")
	if err != nil {
		t.Fatalf("encrypt bad key: %v", err)
	}
	liveCipher, err := env.cipher.Encrypt("sk-live-56")
	if err != nil {
		t.Fatalf("encrypt live key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: badChannel.ID, ChannelName: "bad-5.6", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "bad", GroupName: "bad", Ratio: 0.1, KeyCipher: badCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert bad group key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: liveChannel.ID, ChannelName: "live-5.6", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "live", GroupName: "live", Ratio: 0.2, KeyCipher: liveCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert live group key: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6","input":"ping"}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "resp_live_56") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if badHits != 1 || liveHits != 1 {
		t.Fatalf("model access error did not fail over: bad=%d live=%d", badHits, liveHits)
	}
}

func TestShouldRetryUpstreamStatusTreatsModelAccessAsRetryable(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{
			name:   "openai invalid request model access",
			status: http.StatusBadRequest,
			body:   `{"error":{"message":"The model gpt-5.6 does not exist or you do not have access to it.","type":"invalid_request_error","param":"model","code":"model_not_found"}}`,
		},
		{
			name:   "unprocessable unsupported model",
			status: http.StatusUnprocessableEntity,
			body:   `{"error":{"message":"model gpt-5.6 is not enabled for this channel","type":"invalid_request_error"}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !shouldRetryUpstreamStatus(tc.status, tc.body) {
				t.Fatalf("status %d body %s should fail over", tc.status, tc.body)
			}
		})
	}

	if shouldRetryUpstreamStatus(http.StatusBadRequest, `{"error":{"message":"missing required field: model","type":"invalid_request_error"}}`) {
		t.Fatal("plain client request validation error should not fail over")
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
		{ID: 1, Status: "alive", Ratio: 0.01, Charity: false}, // 便宜的付费
		{ID: 2, Status: "alive", Ratio: 0.5, Charity: true},   // 贵一点的公益
		{ID: 3, Status: "alive", Ratio: 0.2, Charity: true},   // 更便宜的公益
	}
	ordered := orderCandidates(candidates)
	// 公益先行：ID3(0.2公益) → ID2(0.5公益) → ID1(付费)
	if ordered[0].ID != 3 || ordered[1].ID != 2 || ordered[2].ID != 1 {
		t.Fatalf("charity should be scheduled before paid: %#v", ordered)
	}
}

func TestOrderCandidatesPrefersUnknownCharityBeforeAlivePaid(t *testing.T) {
	candidates := []storage.UpstreamGroupKey{
		{ID: 1, Status: "alive", Ratio: 0.01, Charity: false},
		{ID: 2, Status: "unknown", Ratio: 1, Charity: true},
	}
	ordered := orderCandidates(candidates)
	if ordered[0].ID != 2 || ordered[1].ID != 1 {
		t.Fatalf("unknown charity should be tried before alive paid: %#v", ordered)
	}
}

func TestOrderCandidatesFallsBackToPaidWhenCharityUnusable(t *testing.T) {
	candidates := []storage.UpstreamGroupKey{
		{ID: 1, Status: "alive", Ratio: 0.01, Charity: false},
		{ID: 2, Status: "rate_limited", Ratio: 1, Charity: true},
		{ID: 3, Status: "dead", Ratio: 0.5, Charity: true},
	}
	ordered := orderCandidates(candidates)
	if ordered[0].ID != 1 {
		t.Fatalf("unusable charity should not block paid fallback: %#v", ordered)
	}
}

func TestSoftAffinityDoesNotPromotePaidOverCharity(t *testing.T) {
	paid := storage.UpstreamGroupKey{ID: 1, Status: "alive", Ratio: 0.01, Charity: false}
	charity := storage.UpstreamGroupKey{ID: 2, Status: "unknown", Ratio: 1, Charity: true}
	if !affinityWouldPromoteCostlier(paid, charity) {
		t.Fatal("soft affinity should not promote paid candidate over schedulable charity")
	}
	if affinityWouldPromoteCostlier(charity, paid) {
		t.Fatal("soft affinity may promote schedulable charity over paid candidate")
	}
}

func TestOrderCandidatesPrefersSameGroupHealthyKeyBeforeBackupGroup(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("e", 32))
	candidates := []storage.UpstreamGroupKey{
		{ID: 1, GroupName: "primary", ClientFormat: "openai", RequestMode: "responses", Status: "alive", Ratio: 0.01},
		{ID: 2, GroupName: "backup", ClientFormat: "openai", RequestMode: "responses", Status: "alive", Ratio: 0.02},
		{ID: 3, GroupName: "primary", ClientFormat: "openai", RequestMode: "responses", Status: "alive", Ratio: 0.9},
	}
	ordered := env.svc.orderCandidatesForRequest(candidates, normalizedRequest{})
	if len(ordered) != 3 || ordered[0].ID != 1 || ordered[1].ID != 3 || ordered[2].ID != 2 {
		t.Fatalf("ordered candidates = %#v, want primary key, same-group key, then backup group", ordered)
	}
}

func TestOrderCandidatesUsesBackupGroupWhenSameGroupKeyUnhealthy(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("f", 32))
	candidates := []storage.UpstreamGroupKey{
		{ID: 1, GroupName: "primary", ClientFormat: "openai", RequestMode: "responses", Status: "alive", Ratio: 0.01},
		{ID: 2, GroupName: "backup", ClientFormat: "openai", RequestMode: "responses", Status: "alive", Ratio: 0.02},
		{ID: 3, GroupName: "primary", ClientFormat: "openai", RequestMode: "responses", Status: "rate_limited", Ratio: 0.9},
	}
	ordered := env.svc.orderCandidatesForRequest(candidates, normalizedRequest{})
	if len(ordered) != 3 || ordered[0].ID != 1 || ordered[1].ID != 2 {
		t.Fatalf("ordered candidates = %#v, want backup group before unhealthy same-group key", ordered)
	}
}

func TestApplyUpstreamAuthHeadersMatchesChannelFormat(t *testing.T) {
	openAIHeader := http.Header{}
	applyUpstreamAuthHeaders(openAIHeader, &storage.UpstreamGroupKey{ClientFormat: "openai"}, " sk-openai ")
	if got := openAIHeader.Get("Authorization"); got != "Bearer sk-openai" {
		t.Fatalf("openai authorization = %q", got)
	}
	if got := openAIHeader.Get("X-Api-Key"); got != "" {
		t.Fatalf("openai x-api-key should be empty, got %q", got)
	}

	claudeHeader := http.Header{"Authorization": []string{"Bearer old"}}
	applyUpstreamAuthHeaders(claudeHeader, &storage.UpstreamGroupKey{ClientFormat: "claude"}, " sk-claude ")
	if got := claudeHeader.Get("Authorization"); got != "" {
		t.Fatalf("claude authorization should be removed, got %q", got)
	}
	if got := claudeHeader.Get("X-Api-Key"); got != "sk-claude" {
		t.Fatalf("claude x-api-key = %q", got)
	}
	if got := claudeHeader.Get("Anthropic-Version"); got == "" {
		t.Fatal("claude anthropic-version should be set")
	}
}

func TestProxyFailurePolicyRequiresThreeTransientFailures(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("b", 32))
	channel := &storage.Channel{Name: "transient", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.invalid", MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-transient")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: channel.Name, ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "transient", GroupName: "transient", Ratio: 0.1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}
	group, err := env.groupKeys.FindByChannelGroup(channel.ID, "transient")
	if err != nil {
		t.Fatalf("load group key: %v", err)
	}
	for i := 1; i <= 2; i++ {
		env.svc.markProxyFailure(group.ID, "upstream returned HTTP 503: temporary upstream failure")
		stored, err := env.groupKeys.FindByID(group.ID)
		if err != nil {
			t.Fatalf("load group after failure %d: %v", i, err)
		}
		if stored.FailureCount != i || stored.DisabledUntil != nil || !stored.Enabled || stored.Status != "server_error" {
			t.Fatalf("failure %d should only be recorded, got %#v", i, stored)
		}
	}
	env.svc.markProxyFailure(group.ID, "upstream returned HTTP 503: temporary upstream failure")
	stored, err := env.groupKeys.FindByID(group.ID)
	if err != nil {
		t.Fatalf("load group after third failure: %v", err)
	}
	if stored.FailureCount != 3 || stored.DisabledUntil == nil || !stored.Enabled || stored.Status != "server_error" {
		t.Fatalf("third transient failure should short-circuit with cooldown but not disable key: %#v", stored)
	}
}

func TestProxyFailurePolicyUsesRetryAfterForRateLimit(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("c", 32))
	channel := &storage.Channel{Name: "rate-limit", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.invalid", MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-rate")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: channel.Name, ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "rate", GroupName: "rate", Ratio: 0.1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}
	group, err := env.groupKeys.FindByChannelGroup(channel.ID, "rate")
	if err != nil {
		t.Fatalf("load group key: %v", err)
	}
	before := time.Now()
	env.svc.markProxyFailure(group.ID, "upstream returned HTTP 429 (retry-after: 120): too many requests")
	stored, err := env.groupKeys.FindByID(group.ID)
	if err != nil {
		t.Fatalf("load group: %v", err)
	}
	if stored.Status != "rate_limited" || stored.DisabledUntil == nil || !stored.Enabled {
		t.Fatalf("rate limit should set cooldown without disabling key: %#v", stored)
	}
	if delay := stored.DisabledUntil.Sub(before); delay < 110*time.Second || delay > 130*time.Second {
		t.Fatalf("retry-after cooldown = %s, want about 120s", delay)
	}
}

func TestRetryAfterDurationFromCodexRateLimitPayload(t *testing.T) {
	got, ok := retryAfterDurationFromText(`upstream returned non-generation payload: {"type":"codex_rate_limits","rate_limits":{"reset_after_seconds":2920}}`, time.Now())
	if !ok || got != 2920*time.Second {
		t.Fatalf("retry-after duration = %s ok=%v, want 2920s", got, ok)
	}
}

func TestProxyFailurePolicyDisablesOnlyInvalidOrZeroBalanceKey(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("d", 32))
	channel := &storage.Channel{Name: "permanent", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.invalid", MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-permanent")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	for _, ref := range []string{"bad", "good"} {
		if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
			ChannelID: channel.ID, ChannelName: channel.Name, ChannelType: storage.ChannelTypeSub2API,
			GroupRef: ref, GroupName: ref, Ratio: 0.1, KeyCipher: keyCipher, Status: "alive",
		}); err != nil {
			t.Fatalf("insert group key %s: %v", ref, err)
		}
	}
	bad, err := env.groupKeys.FindByChannelGroup(channel.ID, "bad")
	if err != nil {
		t.Fatalf("load bad group key: %v", err)
	}
	good, err := env.groupKeys.FindByChannelGroup(channel.ID, "good")
	if err != nil {
		t.Fatalf("load good group key: %v", err)
	}
	env.svc.markProxyFailure(bad.ID, `upstream returned HTTP 402: {"error":{"message":"insufficient balance"}}`)
	bad, err = env.groupKeys.FindByID(bad.ID)
	if err != nil {
		t.Fatalf("reload bad group key: %v", err)
	}
	good, err = env.groupKeys.FindByID(good.ID)
	if err != nil {
		t.Fatalf("reload good group key: %v", err)
	}
	if bad.Enabled || bad.Status != "zero_balance" {
		t.Fatalf("zero-balance key should be disabled with zero_balance status: %#v", bad)
	}
	if !good.Enabled || good.Status != "alive" {
		t.Fatalf("other group key should not be affected: %#v", good)
	}
}

func TestHardAffinityPromotesPromptCacheAndConversation(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("a", 32))
	cheap := storage.UpstreamGroupKey{ID: 1, Status: "alive", Ratio: 0.01, Priority: 10}
	sticky := storage.UpstreamGroupKey{ID: 2, Status: "alive", Ratio: 0.9, Priority: 1}
	for _, key := range []string{"prompt-cache:codex-session-1", "conversation:conv-1", "response:resp-1"} {
		if err := env.affinities.Upsert(HashKey(key), sticky.ID, time.Now().Add(time.Hour), time.Now()); err != nil {
			t.Fatalf("upsert affinity %q: %v", key, err)
		}
		ordered := env.svc.orderCandidatesForRequest([]storage.UpstreamGroupKey{cheap, sticky}, normalizedRequest{AffinityKey: key})
		if len(ordered) == 0 || ordered[0].ID != sticky.ID {
			t.Fatalf("hard affinity %q did not promote sticky candidate: %#v", key, ordered)
		}
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

func TestHealthProbeUsesNativeStreamingOpenAIResponsesRequest(t *testing.T) {
	var seen map[string]any
	var chatHits, responsesHits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"id":"gpt-test"}]}`))
		case "/v1/chat/completions":
			chatHits++
			http.NotFound(w, r)
		case "/v1/responses":
			responsesHits++
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
	if chatHits != 0 || responsesHits != 1 {
		t.Fatalf("health probe must use native responses directly, chat=%d responses=%d", chatHits, responsesHits)
	}
	if seen["stream"] != true || seen["max_output_tokens"] != float64(1) {
		t.Fatalf("probe body = %#v", seen)
	}
	if seen["model"] != "gpt-5.5" {
		t.Fatalf("probe model = %#v, want gpt-5.5", seen["model"])
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

func TestTestAllGroupKeysRunsAllEnabledGroupsInBatches(t *testing.T) {
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
	// More than one batch. Queued groups must not inherit timeout from earlier
	// batches, and each batch should run with controlled parallelism.
	for i := 0; i < 35; i++ {
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
	if result.Alive != 35 {
		t.Fatalf("alive = %d, want 35; result=%#v", result.Alive, result)
	}
	if result.BatchSize != 30 || result.Batches != 2 {
		t.Fatalf("batch metadata = size %d batches %d, want 30/2", result.BatchSize, result.Batches)
	}
	if got := atomic.LoadInt64(&maxInFlight); got > 30 {
		t.Fatalf("maximum concurrent upstream requests = %d, want at most 30", got)
	}
	if got := atomic.LoadInt64(&maxInFlight); got < 30 {
		t.Fatalf("maximum concurrent upstream requests = %d, want first batch to fill 30 slots", got)
	}
	for _, item := range result.Items {
		if item.Status != "alive" {
			t.Fatalf("group %d status = %s: %s", item.ID, item.Status, item.Error)
		}
		if item.Batch < 1 || item.Batch > 2 {
			t.Fatalf("group %d batch = %d, want 1..2", item.ID, item.Batch)
		}
	}
}

func TestTestGroupKeysHonorsSelectedGroupIDs(t *testing.T) {
	hits := map[string]int{}
	var hitsMu sync.Mutex
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/models":
			_, _ = w.Write([]byte(`{"data":[{"id":"gpt-test"}]}`))
		case "/v1/responses":
			hitsMu.Lock()
			hits[r.Header.Get("X-UpstreamOps-Group")]++
			hitsMu.Unlock()
			_, _ = w.Write([]byte(`{"id":"resp_probe","output_text":"ok"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("s", 32))
	channel := &storage.Channel{Name: "selected-health", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	var ids []uint
	for _, ref := range []string{"group-a", "group-b", "group-c"} {
		key := &storage.UpstreamGroupKey{
			ChannelID: channel.ID, ChannelName: channel.Name, ChannelType: storage.ChannelTypeSub2API,
			GroupRef: ref, GroupName: ref, Ratio: 1, KeyCipher: keyCipher, Status: "unknown",
		}
		if err := env.groupKeys.Upsert(key); err != nil {
			t.Fatalf("insert group %s: %v", ref, err)
		}
		ids = append(ids, key.ID)
	}

	result, err := env.svc.TestGroupKeys(context.Background(), HealthTestOptions{BatchSize: 10, GroupIDs: []uint{ids[0], ids[2]}})
	if err != nil {
		t.Fatalf("test selected groups: %v", err)
	}
	if result.Total != 2 || result.Checked != 2 || result.Alive != 2 || len(result.Items) != 2 {
		t.Fatalf("result = %#v, want two selected alive items", result)
	}
	hitsMu.Lock()
	defer hitsMu.Unlock()
	if hits["group-a"] != 1 || hits["group-c"] != 1 || hits["group-b"] != 0 {
		t.Fatalf("unexpected response hits: %#v", hits)
	}
}

func TestTestGroupKeysClassifiesZeroBalanceSeparatelyFromDead(t *testing.T) {
	var responseHits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/models":
			_, _ = w.Write([]byte(`{"data":[{"id":"gpt-test"}]}`))
		case "/v1/responses":
			responseHits++
			w.WriteHeader(http.StatusPaymentRequired)
			_, _ = w.Write([]byte(`{"error":{"code":"insufficient_quota","message":"insufficient balance"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("z", 32))
	channel := &storage.Channel{Name: "zero-balance", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	key := &storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: channel.Name, ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "limited", GroupName: "limited", Ratio: 1, KeyCipher: keyCipher, Status: "unknown",
	}
	if err := env.groupKeys.Upsert(key); err != nil {
		t.Fatalf("insert group: %v", err)
	}

	result, err := env.svc.TestGroupKeys(context.Background(), HealthTestOptions{BatchSize: 10, GroupIDs: []uint{key.ID}})
	if err != nil {
		t.Fatalf("test zero balance group: %v", err)
	}
	if result.ZeroBalance != 1 || result.Dead != 0 || len(result.Items) != 1 || result.Items[0].Status != "zero_balance" {
		t.Fatalf("result = %#v, want zero_balance separate from dead", result)
	}
	if responseHits != 1 {
		t.Fatalf("response hits = %d, want no retry for zero balance", responseHits)
	}
	stored, err := env.groupKeys.FindByID(key.ID)
	if err != nil {
		t.Fatalf("load group: %v", err)
	}
	if stored.Status != "zero_balance" {
		t.Fatalf("stored status = %q, want zero_balance", stored.Status)
	}
}

func TestTestGroupKeysClassifiesCommonProbeFailures(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
		body       string
		wantStatus string
	}{
		{
			name:       "codex rate limits payload",
			statusCode: http.StatusOK,
			body:       `{"allowed":false,"limit_reached":true,"type":"codex_rate_limits","plan_type":"k12","rate_limits":{"used_percent":100,"window_minutes":300,"reset_after_seconds":2920}}`,
			wantStatus: "rate_limited",
		},
		{
			name:       "forbidden html",
			statusCode: http.StatusForbidden,
			body:       `<html><head><title>403 Forbidden</title></head><body><center><h1>403 Forbidden</h1></center><hr><center>nginx</center></body></html>`,
			wantStatus: "forbidden",
		},
		{
			name:       "non generation payload",
			statusCode: http.StatusOK,
			body:       `{"ok":true,"message":"pong"}`,
			wantStatus: "non_generation",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var responseHits int
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/models":
					_, _ = w.Write([]byte(`{"data":[{"id":"gpt-test"}]}`))
				case "/v1/responses":
					responseHits++
					w.WriteHeader(tc.statusCode)
					_, _ = w.Write([]byte(tc.body))
				default:
					http.NotFound(w, r)
				}
			}))
			defer upstream.Close()

			env := newGatewayProxyTestEnv(t, strings.Repeat("c", 32))
			channel := &storage.Channel{Name: "classified-" + tc.wantStatus, Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
			if err := env.channels.Create(channel); err != nil {
				t.Fatalf("create channel: %v", err)
			}
			keyCipher, err := env.cipher.Encrypt("sk-upstream")
			if err != nil {
				t.Fatalf("encrypt key: %v", err)
			}
			key := &storage.UpstreamGroupKey{
				ChannelID: channel.ID, ChannelName: channel.Name, ChannelType: storage.ChannelTypeSub2API,
				GroupRef: tc.wantStatus, GroupName: tc.wantStatus, Ratio: 1, KeyCipher: keyCipher, Status: "unknown",
			}
			if err := env.groupKeys.Upsert(key); err != nil {
				t.Fatalf("insert group: %v", err)
			}

			result, err := env.svc.TestGroupKeys(context.Background(), HealthTestOptions{BatchSize: 10, GroupIDs: []uint{key.ID}})
			if err != nil {
				t.Fatalf("test classified group: %v", err)
			}
			if len(result.Items) != 1 || result.Items[0].Status != tc.wantStatus || result.Items[0].ErrorType != tc.wantStatus {
				t.Fatalf("result item = %#v, want status/error_type %s", result.Items, tc.wantStatus)
			}
			if result.Dead != 0 {
				t.Fatalf("dead = %d, want classified failure not counted as dead; result=%#v", result.Dead, result)
			}
			if responseHits != 1 {
				t.Fatalf("response hits = %d, want no retry for classified failure", responseHits)
			}
		})
	}
}

func TestJoinUpstreamURLNormalizesDirectBaseURL(t *testing.T) {
	got, err := joinUpstreamURL("https://relay.example.com/v1", "/v1/responses?stream=1")
	if err != nil {
		t.Fatalf("join upstream URL: %v", err)
	}
	if got != "https://relay.example.com/v1/responses?stream=1" {
		t.Fatalf("joined URL = %q", got)
	}

	got, err = joinUpstreamURL("https://relay.example.com/api/v1/", "/v1/chat/completions")
	if err != nil {
		t.Fatalf("join upstream URL with nested v1: %v", err)
	}
	if got != "https://relay.example.com/api/v1/chat/completions" {
		t.Fatalf("joined nested URL = %q", got)
	}

	if _, err := joinUpstreamURL("relay.example.com", "/v1/responses"); err == nil {
		t.Fatal("expected missing scheme to be rejected")
	}
}

func TestNormalizeManualAPIBaseURLTrimsEndpointAndRejectsAdminURL(t *testing.T) {
	got, err := normalizeManualAPIBaseURL(` "https://relay.example.com/v1/responses?foo=bar" `)
	if err != nil {
		t.Fatalf("normalize manual base URL: %v", err)
	}
	if got != "https://relay.example.com/v1" {
		t.Fatalf("normalized URL = %q, want https://relay.example.com/v1", got)
	}
	if _, err := normalizeManualAPIBaseURL("https://relay.example.com/admin"); err == nil {
		t.Fatal("expected admin URL to be rejected")
	}
}

func TestSanitizeManualSecretTrimsWrappingQuotes(t *testing.T) {
	if got := sanitizeManualSecret(` "sk-test" `); got != "sk-test" {
		t.Fatalf("secret = %q, want sk-test", got)
	}
}

func TestGrokFormatIsIsolatedAndUsesXAIHeaders(t *testing.T) {
	candidates := []storage.UpstreamGroupKey{
		{ID: 1, ClientFormat: "openai"},
		{ID: 2, ClientFormat: "grok"},
		{ID: 3, ClientFormat: "claude"},
	}
	filtered := filterCandidatesForClientFormat("grok", "responses", candidates)
	if len(filtered) != 1 || filtered[0].ID != 2 {
		t.Fatalf("Grok key candidates = %#v, want only Grok", filtered)
	}
	if err := validateClientFormat("grok", "claude"); err == nil {
		t.Fatal("Grok key must reject Claude Messages requests")
	}
	if got := defaultHealthProbeModel("grok"); got != "grok-3-mini" {
		t.Fatalf("Grok fallback probe model = %q", got)
	}

	header := http.Header{}
	applyUpstreamAuthHeaders(header, &storage.UpstreamGroupKey{GroupName: "grok", ClientFormat: "grok"}, "xai-key")
	if got := header.Get("Authorization"); got != "Bearer xai-key" {
		t.Fatalf("Authorization = %q", got)
	}
	if got := header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := header.Get("Accept"); got != "application/json, text/event-stream" {
		t.Fatalf("Accept = %q", got)
	}
	if got := header.Get("User-Agent"); got != "upstream-ops-grok/1.0" {
		t.Fatalf("User-Agent = %q", got)
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

type failWriteResponseWriter struct {
	header http.Header
	status int
}

func (w *failWriteResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *failWriteResponseWriter) WriteHeader(status int) {
	w.status = status
}

func (w *failWriteResponseWriter) Write(_ []byte) (int, error) {
	return 0, fmt.Errorf("404 page not found")
}

func assertResponsesStreamTerminalOnce(t *testing.T, out, want string) {
	t.Helper()
	counts := map[string]int{
		"response.completed": strings.Count(out, "event: response.completed\n"),
		"response.failed":    strings.Count(out, "event: response.failed\n"),
		"response.cancelled": strings.Count(out, "event: response.cancelled\n"),
	}
	total := 0
	for _, count := range counts {
		total += count
	}
	if total != 1 || counts[want] != 1 {
		t.Fatalf("responses stream terminal counts=%v want exactly one %s; stream:\n%s", counts, want, out)
	}
	if strings.Count(out, "data: [DONE]\n\n") != 1 {
		t.Fatalf("responses stream must contain exactly one [DONE]; stream:\n%s", out)
	}
}

func TestAttemptStreamDoesNotFallbackAfterWriterStarted(t *testing.T) {
	var hits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\n" +
			"data: {\"type\":\"response.output_text.delta\",\"delta\":\"pong\",\"response_id\":\"resp_started\"}\n\n"))
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("s", 32))
	channel := &storage.Channel{Name: "started-stream", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-started")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	candidate := &storage.UpstreamGroupKey{
		ID:          1,
		ChannelID:   channel.ID,
		ChannelName: "started-stream",
		ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "started", GroupName: "started", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}
	normalized := normalizedRequest{
		Method:       http.MethodPost,
		Path:         "/v1/responses",
		Body:         []byte(`{"model":"gpt-test","input":"ping","stream":true}`),
		Header:       http.Header{"Content-Type": []string{"application/json"}},
		ResponseMode: "claude",
		Stream:       true,
		AltPath:      "/v1/responses",
		AltBody:      []byte(`{"model":"gpt-test","input":"fallback","stream":true}`),
		AltMode:      "responses",
		AltStream:    true,
	}
	outcome := env.svc.attemptStream(context.Background(), &storage.GatewayKey{ID: env.localKey.ID}, normalized, candidate, &failWriteResponseWriter{})
	if outcome.kind != candFatal {
		t.Fatalf("outcome = %#v, want fatal", outcome)
	}
	if hits != 1 {
		t.Fatalf("stream fallback after writer started hit upstream %d times, want 1", hits)
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

func TestProxySynthesizesFailedWhenUpstreamDropsStreamMidway(t *testing.T) {
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
	if !strings.Contains(out, "response.failed") || !strings.Contains(out, "upstream stream ended before response.completed") || !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("gateway must synthesize response.failed when upstream drops mid-stream: %s", out)
	}
}

func TestProxyDoesNotDowngradeResponsesToChatWhenUpstreamLacksResponses(t *testing.T) {
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
	err = env.svc.Proxy(rec, req, "/v1/responses")
	if err == nil {
		t.Fatalf("proxy unexpectedly succeeded with body=%s", rec.Body.String())
	}
	gwErr, ok := err.(*GatewayError)
	if !ok || gwErr.Status != http.StatusServiceUnavailable {
		t.Fatalf("error = %#v, want 503 GatewayError", err)
	}
	if responsesHits != 1 || chatHits != 0 {
		t.Fatalf("native responses must not downgrade to chat: responses=%d chat=%d", responsesHits, chatHits)
	}
}

func TestProxyConvertsChatCompletionFromResponsesEndpoint(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chatcmpl_direct","object":"chat.completion","model":"gpt-test","choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("n", 32))
	channel := &storage.Channel{Name: "chat-json-on-responses", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "chat-json-on-responses", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
		RequestMode: "responses",
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
	out := rec.Body.String()
	if strings.Contains(out, "chat.completion") {
		t.Fatalf("chat completion must be converted for responses clients: %s", out)
	}
	if !strings.Contains(out, `"object":"response"`) || !strings.Contains(out, `"output_text":"pong"`) {
		t.Fatalf("responses object malformed: %s", out)
	}
}

// 显式标记为 Chat 的候选才会使用 Chat→Responses 桥接；原生 Responses 候选不再隐藏降级。
func TestProxyStreamUsesExplicitChatBridgeForResponsesClients(t *testing.T) {
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
		RequestMode: "chat",
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
	if responsesHits != 0 || chatHits != 1 {
		t.Fatalf("expected direct chat bridge for responses stream: responses=%d chat=%d", responsesHits, chatHits)
	}
	if !strings.Contains(out, "response.completed") || !strings.Contains(out, "pong") || !strings.Contains(out, "[DONE]") {
		t.Fatalf("stream fallback output malformed: %s", out)
	}
}

func TestResponsesStreamConvertsChatChunksFromNativeResponsesEndpoint(t *testing.T) {
	var responsesHits, chatHits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/chat/completions" {
			chatHits++
			http.NotFound(w, r)
			return
		}
		if r.URL.Path != "/v1/responses" {
			http.NotFound(w, r)
			return
		}
		responsesHits++
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"id\":\"chatcmpl_direct\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n" +
			"data: {\"id\":\"chatcmpl_direct\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"pong\"}}]}\n\n" +
			"data: {\"id\":\"chatcmpl_direct\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n" +
			"data: [DONE]\n\n"))
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("d", 32))
	channel := &storage.Channel{Name: "chat-on-responses", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "chat-on-responses", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
		RequestMode: "responses",
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
	if chatHits != 0 || responsesHits != 1 {
		t.Fatalf("native responses endpoint must be tried directly, chat=%d responses=%d", chatHits, responsesHits)
	}
	if strings.Contains(out, "chat.completion.chunk") {
		t.Fatalf("chat chunks must not be passed to Codex responses stream: %s", out)
	}
	if !strings.Contains(out, "response.created") || !strings.Contains(out, "response.output_text.delta") || !strings.Contains(out, "response.completed") || !strings.Contains(out, "pong") || !strings.Contains(out, "[DONE]") {
		t.Fatalf("responses stream malformed: %s", out)
	}
}

func TestResponsesStreamUsesExplicitChatBridgeWithoutCodexHeaders(t *testing.T) {
	var responsesHits, chatHits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			responsesHits++
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("native responses should not be used first for Codex"))
		case "/v1/chat/completions":
			chatHits++
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("data: {\"id\":\"chatcmpl_codex\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n" +
				"data: {\"id\":\"chatcmpl_codex\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"pong\"}}]}\n\n" +
				"data: {\"id\":\"chatcmpl_codex\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\n\n" +
				"data: [DONE]\n\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("c", 32))
	channel := &storage.Channel{Name: "codex-chat-bridge", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "codex-chat-bridge", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
		RequestMode: "chat",
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
	if responsesHits != 0 || chatHits != 1 {
		t.Fatalf("hits responses=%d chat=%d, want direct chat bridge", responsesHits, chatHits)
	}
	out := rec.Body.String()
	if !strings.Contains(out, "response.output_item.added") || !strings.Contains(out, "response.output_text.done") || !strings.Contains(out, "response.completed") || !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("responses bridge missing lifecycle events: %s", out)
	}
	if got := rec.Header().Get("X-Accel-Buffering"); got != "no" {
		t.Fatalf("X-Accel-Buffering = %q, want no", got)
	}
}

func TestResponsesStreamChoosesChatBridgeByProtocolNotHeaders(t *testing.T) {
	req := normalizedRequest{
		Path:         "/v1/responses",
		Body:         []byte(`{"model":"gpt-test","input":"ping","stream":true}`),
		ResponseMode: "responses",
		Stream:       true,
		Header:       make(http.Header),
		AltPath:      "/v1/chat/completions",
		AltBody:      []byte(`{"model":"gpt-test","messages":[{"role":"user","content":"ping"}],"stream":true}`),
		AltMode:      "responses_from_chat",
		AltStream:    true,
	}
	got := requestForCandidate(req, &storage.UpstreamGroupKey{ClientFormat: "openai", RequestMode: "responses"})
	if got.Path != "/v1/responses" || got.ResponseMode != "responses" || !got.Stream {
		t.Fatalf("responses mode should preserve native responses, got path=%q mode=%q stream=%v", got.Path, got.ResponseMode, got.Stream)
	}
	chat := requestForCandidate(req, &storage.UpstreamGroupKey{ClientFormat: "openai", RequestMode: "chat"})
	if chat.Path != "/v1/chat/completions" || chat.ResponseMode != "responses_from_chat" || !chat.Stream {
		t.Fatalf("explicit chat mode should choose chat bridge, got path=%q mode=%q stream=%v", chat.Path, chat.ResponseMode, chat.Stream)
	}
}

func TestNormalizeProxyRequestTreatsTrailingSlashResponsesAsNative(t *testing.T) {
	body := []byte(`{"model":"gpt-test","input":"ping","stream":true}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses/", strings.NewReader(string(body)))
	normalized, err := normalizeProxyRequest(req, "/v1/responses/?stream=1", body)
	if err != nil {
		t.Fatalf("normalize request: %v", err)
	}
	if normalized.Path != "/v1/responses?stream=1" || normalized.ResponseMode != "responses" || !normalized.Stream {
		t.Fatalf("normalized path=%q mode=%q stream=%v", normalized.Path, normalized.ResponseMode, normalized.Stream)
	}
}

func TestProxyResponsesStreamHintsReturnSSEFailureWithoutBodyStream(t *testing.T) {
	cases := []struct {
		name   string
		target string
		path   string
		accept string
	}{
		{name: "query stream flag", target: "/v1/responses?stream=1", path: "/v1/responses?stream=1"},
		{name: "accept event stream", target: "/v1/responses", path: "/v1/responses", accept: "text/event-stream"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newGatewayProxyTestEnv(t, strings.Repeat("h", 32))
			req := httptest.NewRequest(http.MethodPost, tc.target, strings.NewReader(`{"model":"gpt-test","input":"ping"}`))
			req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
			req.Header.Set("Content-Type", "application/json")
			if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			rec := httptest.NewRecorder()
			if err := env.svc.Proxy(rec, req, tc.path); err != nil {
				t.Fatalf("proxy request should write responses failure stream, got err: %v", err)
			}
			out := rec.Body.String()
			if rec.Code != http.StatusOK || !strings.Contains(out, "event: response.failed") || !strings.Contains(out, "data: [DONE]") {
				t.Fatalf("stream hint should return responses SSE terminal, status=%d body=%s", rec.Code, out)
			}
			assertResponsesStreamTerminalOnce(t, out, "response.failed")
		})
	}
}

func TestSynthesizesResponseFailedWhenUpstreamOmitsCompleted(t *testing.T) {
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
	if !strings.Contains(out, "response.failed") || !strings.Contains(out, "upstream stream ended before response.completed") || !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("gateway should synthesize response.failed when upstream omits completed: %s", out)
	}
	assertResponsesStreamTerminalOnce(t, out, "response.failed")
}

func TestProxyResponsesStreamNoUpstreamsReturnsFailedTerminal(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("u", 32))
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request should write responses failure stream, got err: %v", err)
	}
	out := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(rec.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status=%d content-type=%q body=%s", rec.Code, rec.Header().Get("Content-Type"), out)
	}
	if rec.Header().Get("X-Accel-Buffering") != "no" {
		t.Fatalf("X-Accel-Buffering = %q, want no", rec.Header().Get("X-Accel-Buffering"))
	}
	if !strings.Contains(out, "response.failed") || !strings.Contains(out, "当前没有可用上游") || !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("missing friendly terminal failure stream: %s", out)
	}
	assertResponsesStreamTerminalOnce(t, out, "response.failed")
}

func TestProxyResponsesStreamAuthErrorReturnsFailedTerminal(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("a", 32))
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping","stream":true}`))
	req.Header.Set("Authorization", "Bearer sk-bad")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request should write responses auth failure stream, got err: %v", err)
	}
	out := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(out, "response.failed") || !strings.Contains(out, "网关密钥无效") {
		t.Fatalf("auth failure stream malformed: status=%d body=%s", rec.Code, out)
	}
	assertResponsesStreamTerminalOnce(t, out, "response.failed")
}

func TestProxyPublicGatewayQuotaExceededReturnsCodexTextStream(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("p", 32))
	dailyLimit := int64(1)
	if _, err := env.svc.UpdateGatewayKey(env.localKey.ID, UpdateGatewayKeyInput{DailyLimit: &dailyLimit}); err != nil {
		t.Fatalf("set daily limit: %v", err)
	}
	if _, err := env.svc.ConfigurePublicGatewayKey(ConfigurePublicGatewayKeyInput{GatewayKeyID: env.localKey.ID, Enabled: true, Name: "公益 Key"}); err != nil {
		t.Fatalf("configure public key: %v", err)
	}
	if err := env.svc.gateway.AddUsage(env.localKey.ID, 0, 0, 1, 0, 0, time.Now()); err != nil {
		t.Fatalf("seed usage: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request should write public quota text stream, got err: %v", err)
	}
	out := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(rec.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status=%d content-type=%q body=%s", rec.Code, rec.Header().Get("Content-Type"), out)
	}
	if !strings.Contains(out, publicGatewayQuotaExhaustedMessage) || !strings.Contains(out, "response.output_text.delta") {
		t.Fatalf("missing public quota assistant text stream: %s", out)
	}
	if strings.Contains(out, "response.failed") {
		t.Fatalf("public quota stream must be rendered as assistant text, not failed: %s", out)
	}
	assertResponsesStreamTerminalOnce(t, out, "response.completed")
}

func TestProxyPublicGatewayExpiredReturnsCodexTextStream(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("x", 32))
	expiresAt := time.Now().Add(-time.Hour)
	if _, err := env.svc.UpdateGatewayKey(env.localKey.ID, UpdateGatewayKeyInput{ExpiresAt: &expiresAt}); err != nil {
		t.Fatalf("set expiry: %v", err)
	}
	if _, err := env.svc.ConfigurePublicGatewayKey(ConfigurePublicGatewayKeyInput{GatewayKeyID: env.localKey.ID, Enabled: true, Name: "公益 Key"}); err != nil {
		t.Fatalf("configure public key: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request should write public expired text stream, got err: %v", err)
	}
	out := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(out, publicGatewayExpiredMessage) || !strings.Contains(out, "response.output_text.delta") {
		t.Fatalf("missing public expired assistant text stream: status=%d body=%s", rec.Code, out)
	}
	if strings.Contains(out, "response.failed") {
		t.Fatalf("public expired stream must be rendered as assistant text, not failed: %s", out)
	}
	assertResponsesStreamTerminalOnce(t, out, "response.completed")
}

func TestProxyPublicGatewayBalanceExhaustedAfterDisableReturnsCodexTextStream(t *testing.T) {
	env := newGatewayProxyTestEnv(t, strings.Repeat("b", 32))
	balanceLimit := 0.01
	if _, err := env.svc.UpdateGatewayKey(env.localKey.ID, UpdateGatewayKeyInput{BalanceLimit: &balanceLimit}); err != nil {
		t.Fatalf("set balance limit: %v", err)
	}
	if _, err := env.svc.ConfigurePublicGatewayKey(ConfigurePublicGatewayKeyInput{GatewayKeyID: env.localKey.ID, Enabled: true, Name: "公益 Key"}); err != nil {
		t.Fatalf("configure public key: %v", err)
	}
	if err := env.svc.gateway.AddUsage(env.localKey.ID, 0, 0, 1, 0, 0.01, time.Now()); err != nil {
		t.Fatalf("seed exhausted balance: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request should write public exhausted-balance text stream, got err: %v", err)
	}
	out := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(out, publicGatewayQuotaExhaustedMessage) {
		t.Fatalf("missing public balance quota assistant text stream: status=%d body=%s", rec.Code, out)
	}
	if strings.Contains(out, "response.failed") || strings.Contains(out, "网关密钥无效") {
		t.Fatalf("disabled public balance key must not fall back to invalid-key failure: %s", out)
	}
	assertResponsesStreamTerminalOnce(t, out, "response.completed")
}

func TestProxyResponsesStreamAllUpstreamsFailBeforeWriteReturnsFailedTerminal(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":{"message":"upstream temporarily down"}}`))
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("f", 32))
	channel := &storage.Channel{Name: "all-fail-stream", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt upstream key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "all-fail-stream", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request should write responses failure stream, got err: %v", err)
	}
	out := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(out, "response.failed") || !strings.Contains(out, "当前没有可用上游") {
		t.Fatalf("all-upstreams failure stream malformed: status=%d body=%s", rec.Code, out)
	}
	assertResponsesStreamTerminalOnce(t, out, "response.failed")
}

func TestProxyResponsesStreamFatalUpstreamErrorBeforeWriteReturnsFailedTerminal(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"missing required field: model","type":"invalid_request_error"}}`))
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("r", 32))
	channel := &storage.Channel{Name: "fatal-stream", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt upstream key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "fatal-stream", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping","stream":true}`))
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request should write responses failure stream, got err: %v", err)
	}
	out := rec.Body.String()
	if rec.Code != http.StatusOK || strings.Contains(out, "invalid_request_error") || !strings.Contains(out, "response.failed") {
		t.Fatalf("fatal upstream stream error malformed: status=%d body=%s", rec.Code, out)
	}
	assertResponsesStreamTerminalOnce(t, out, "response.failed")
}

func TestProxyResponsesStreamCancelledBeforeWriteReturnsCancelledTerminal(t *testing.T) {
	var upstreamHits int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&upstreamHits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	env := newGatewayProxyTestEnv(t, strings.Repeat("c", 32))
	channel := &storage.Channel{Name: "cancel-stream", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, MonitorEnabled: true}
	if err := env.channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := env.cipher.Encrypt("sk-upstream")
	if err != nil {
		t.Fatalf("encrypt upstream key: %v", err)
	}
	if err := env.groupKeys.Upsert(&storage.UpstreamGroupKey{
		ChannelID: channel.ID, ChannelName: "cancel-stream", ChannelType: storage.ChannelTypeSub2API,
		GroupRef: "default", GroupName: "default", Ratio: 1, KeyCipher: keyCipher, Status: "alive",
	}); err != nil {
		t.Fatalf("insert group key: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","input":"ping","stream":true}`)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+env.localKey.Key)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	if err := env.svc.Proxy(rec, req, "/v1/responses"); err != nil {
		t.Fatalf("proxy request should write responses cancelled stream, got err: %v", err)
	}
	out := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(out, "response.cancelled") || !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("cancelled stream malformed: status=%d body=%s", rec.Code, out)
	}
	assertResponsesStreamTerminalOnce(t, out, "response.cancelled")
	if got := atomic.LoadInt64(&upstreamHits); got != 0 {
		t.Fatalf("upstream hits = %d, want 0 for pre-write cancellation", got)
	}
}

func TestResponsesToChatRequestBodyConvertsCodexTools(t *testing.T) {
	body := []byte(`{
		"model":"gpt-test",
		"input":"ping",
		"stream":true,
		"tools":[
			{"type":"custom","name":"exec","description":"Run command","input_schema":{"type":"object","properties":{"cmd":{"type":"string"}}}},
			{"type":"namespace","name":"mcp.fs","tools":[{"type":"function","name":"read","description":"Read file","parameters":{"type":"object","properties":{"path":{"type":"string"}}}}]},
			{"type":"tool_search"}
		],
		"tool_choice":{"type":"custom","name":"exec"}
	}`)
	converted, stream, err := responsesToChatRequestBody(body)
	if err != nil {
		t.Fatalf("convert responses to chat: %v", err)
	}
	if !stream {
		t.Fatalf("stream flag was not preserved")
	}
	var raw map[string]any
	if err := json.Unmarshal(converted, &raw); err != nil {
		t.Fatalf("decode converted body: %v", err)
	}
	tools, _ := raw["tools"].([]any)
	if len(tools) != 3 {
		t.Fatalf("tools len = %d, body=%s", len(tools), converted)
	}
	gotNames := map[string]bool{}
	for _, item := range tools {
		tool, _ := item.(map[string]any)
		if tool["type"] != "function" {
			t.Fatalf("tool was not converted to chat function tool: %#v", tool)
		}
		fn, _ := tool["function"].(map[string]any)
		gotNames[stringValue(fn["name"])] = true
		if fn["parameters"] == nil {
			t.Fatalf("tool parameters missing: %#v", fn)
		}
	}
	for _, name := range []string{"exec", "mcp__fs__read", "tool_search"} {
		if !gotNames[name] {
			t.Fatalf("missing converted tool %q in %v; body=%s", name, gotNames, converted)
		}
	}
	choice, _ := raw["tool_choice"].(map[string]any)
	fn, _ := choice["function"].(map[string]any)
	if stringValue(fn["name"]) != "exec" {
		t.Fatalf("tool_choice not converted: %#v", raw["tool_choice"])
	}
}

func TestStreamChatAsResponsesEventsConvertsToolCalls(t *testing.T) {
	body := strings.NewReader(
		"data: {\"id\":\"chatcmpl_tool\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_exec\",\"type\":\"function\",\"function\":{\"name\":\"exec\",\"arguments\":\"{\\\"cmd\\\"\"}}]}}]}\n\n" +
			"data: {\"id\":\"chatcmpl_tool\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\":\\\"ls\\\"}\"}}]}}]}\n\n" +
			"data: [DONE]\n\n")
	rec := httptest.NewRecorder()
	if _, err := streamChatAsResponsesEvents(rec, nil, newSSEStreamReader(body)); err != nil {
		t.Fatalf("stream chat as responses: %v", err)
	}
	out := rec.Body.String()
	for _, want := range []string{
		"response.output_item.added",
		"response.function_call_arguments.delta",
		"response.function_call_arguments.done",
		"response.output_item.done",
		"response.completed",
		"data: [DONE]",
		"exec",
		"{\\\"cmd\\\":\\\"ls\\\"}",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in stream:\n%s", want, out)
		}
	}
}

func TestStreamChatAsResponsesEventsReturnsOnFinishReasonWithoutDone(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	rec := httptest.NewRecorder()
	done := make(chan error, 1)
	go func() {
		_, err := streamChatAsResponsesEvents(rec, nil, newSSEStreamReader(pr))
		done <- err
	}()
	_, err := io.WriteString(pw, "data: {\"id\":\"chatcmpl_1\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-test\",\"choices\":[{\"delta\":{\"content\":\"pong\"},\"finish_reason\":\"stop\"}]}\n\n")
	if err != nil {
		t.Fatalf("write pipe: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stream chat as responses: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		_ = pr.Close()
		t.Fatal("chat bridge did not return after finish_reason without upstream [DONE]")
	}
	out := rec.Body.String()
	if !strings.Contains(out, "response.completed") || !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("chat bridge did not complete responses stream: %s", out)
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

func TestOrderCandidatesPrefersLowerRatioForSchedulableGroups(t *testing.T) {
	candidates := []storage.UpstreamGroupKey{
		{ID: 10, Status: "alive", Ratio: 0.2},
		{ID: 11, Status: "alive", Ratio: 0.05},
		{ID: 12, Status: "unknown", Ratio: 0.01},
		{ID: 13, Status: "dead", Ratio: 0.001},
	}
	ordered := orderCandidates(candidates)
	if ordered[0].ID != 11 {
		t.Fatalf("first candidate = %#v, want lowest-ratio alive group", ordered[0])
	}
	if ordered[1].ID != 10 {
		t.Fatalf("second candidate = %#v, want other alive group before unknown/dead", ordered[1])
	}
}

func TestBlockedBootstrapKeyKeywordChecksGroupDescription(t *testing.T) {
	if keyword, blocked := blockedBootstrapKeyKeyword(
		storage.Channel{Name: "regular"},
		connector.APIKeyGroup{Name: "gpt", Description: "no img relay"},
	); !blocked || keyword != "img" {
		t.Fatalf("blocked = %v keyword = %q, want img description hit", blocked, keyword)
	}
}

func TestBlockedBootstrapKeyKeywordChecksIM2(t *testing.T) {
	if keyword, blocked := blockedBootstrapKeyKeyword(
		storage.Channel{Name: "regular"},
		connector.APIKeyGroup{Name: "im2-production"},
	); !blocked || keyword != "im2" {
		t.Fatalf("blocked = %v keyword = %q, want im2 group-name hit", blocked, keyword)
	}
}

func TestInferGroupClientFormatRecognizesClaudeAliases(t *testing.T) {
	for _, name := range []string{"cc relay", "cs relay", "kiro", "max"} {
		if got := inferGroupClientFormat(name, ""); got != "claude" {
			t.Fatalf("format for %q = %q, want claude", name, got)
		}
	}
}

func TestFilterOpenAIHealthGroupsSkipsClaudeAndGrok(t *testing.T) {
	groups := []storage.UpstreamGroupKey{
		{ID: 1, ClientFormat: "openai", RequestMode: "responses"},
		{ID: 2, ClientFormat: "claude"},
		{ID: 3, ClientFormat: "grok"},
		{ID: 4, ClientFormat: "openai", RequestMode: "chat"},
	}
	filtered := filterOpenAIHealthGroups(groups)
	if len(filtered) != 1 || filtered[0].ID != 1 {
		t.Fatalf("filtered groups = %#v, want only OpenAI", filtered)
	}
}

func TestAffinityLookupKeyPrefersPromptCacheKey(t *testing.T) {
	got := affinityLookupKey([]byte(`{"model":"gpt-5.5","prompt_cache_key":"codex-session-1","input":"next turn"}`))
	if got != "prompt-cache:codex-session-1" {
		t.Fatalf("affinity key = %q, want prompt-cache key", got)
	}
}

func TestExtractStreamUsageFromResponsesSSE(t *testing.T) {
	body := []byte("event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-5.6-codex\",\"usage\":{\"input_tokens\":7,\"output_tokens\":5,\"total_tokens\":12,\"input_tokens_details\":{\"cached_tokens\":3}}}}\n\n" +
		"data: [DONE]\n")
	usage := extractStreamUsage(body)
	if usage.Prompt != 7 || usage.Completion != 5 || usage.Total != 12 || usage.Cached != 3 {
		t.Fatalf("usage = %#v", usage)
	}
	if usage.Model != "gpt-5.6-codex" {
		t.Fatalf("model = %q, want gpt-5.6-codex", usage.Model)
	}
}

func TestUsageLogModelPrefersOriginalRequestModel(t *testing.T) {
	req := normalizedRequest{
		RequestModel: "gpt-5.6-codex",
		Body:         []byte(`{"model":"gpt-5.6"}`),
	}
	usage := usageTokens{Model: "gpt-5.6-thinking"}
	if got := usageLogModel(req, usage); got != "gpt-5.6-codex" {
		t.Fatalf("usage log model = %q, want original request model", got)
	}
}

func TestUsageLogModelFallsBackToResponseModel(t *testing.T) {
	req := normalizedRequest{Body: []byte(`{"input":"hi"}`)}
	usage := usageTokens{Model: "gpt-5.6"}
	if got := usageLogModel(req, usage); got != "gpt-5.6" {
		t.Fatalf("usage log model = %q, want response model", got)
	}
}

func TestExtractUsagePreservesResponseModel(t *testing.T) {
	body := []byte(`{"id":"resp_1","model":"gpt-5.6-codex","usage":{"input_tokens":9,"output_tokens":4,"total_tokens":13}}`)
	usage := extractUsage(body)
	if usage.Model != "gpt-5.6-codex" {
		t.Fatalf("model = %q, want gpt-5.6-codex", usage.Model)
	}
	if usage.ResponseID != "resp_1" || usage.Total != 13 {
		t.Fatalf("usage = %#v", usage)
	}
}

func TestUsageFromMapExtractsCachedTokens(t *testing.T) {
	cases := []struct {
		name string
		raw  map[string]any
		want int64
	}{
		{
			name: "openai prompt details",
			raw: map[string]any{
				"prompt_tokens": 10,
				"prompt_tokens_details": map[string]any{
					"cached_tokens": 4,
				},
			},
			want: 4,
		},
		{
			name: "responses input details",
			raw: map[string]any{
				"input_tokens": 12,
				"input_tokens_details": map[string]any{
					"cached_tokens": 5,
				},
			},
			want: 5,
		},
		{
			name: "claude cache read",
			raw: map[string]any{
				"input_tokens":            20,
				"cache_read_input_tokens": 7,
			},
			want: 7,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			usage := usageFromMap(tc.raw)
			if usage.Cached != tc.want {
				t.Fatalf("cached tokens = %d, want %d", usage.Cached, tc.want)
			}
		})
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

func TestStreamRawSSENormalizesDataOnlyResponsesEventsForCodex(t *testing.T) {
	body := strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_data_only\",\"model\":\"gpt-test\",\"delta\":\"pong\"}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_data_only\",\"model\":\"gpt-test\",\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}\n\n")
	rec := httptest.NewRecorder()
	usage, err := streamRawSSE(rec, nil, newSSEStreamReader(body), "responses")
	if err != nil {
		t.Fatalf("stream raw responses sse: %v", err)
	}
	out := rec.Body.String()
	if !strings.Contains(out, "event: response.output_text.delta") || !strings.Contains(out, "event: response.completed") || !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("responses data-only events were not normalized for Codex: %s", out)
	}
	if usage.Total != 5 || usage.ResponseID != "resp_data_only" {
		t.Fatalf("usage = %#v", usage)
	}
}

func TestStreamRawSSEConvertsResponseDoneToCompletedForCodex(t *testing.T) {
	body := strings.NewReader("event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_done_alias\",\"model\":\"gpt-test\",\"delta\":\"pong\"}\n\n" +
		"event: response.done\n" +
		"data: {\"type\":\"response.done\",\"response\":{\"id\":\"resp_done_alias\",\"model\":\"gpt-test\",\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}\n\n")
	rec := httptest.NewRecorder()
	usage, err := streamRawSSE(rec, nil, newSSEStreamReader(body), "responses")
	if err != nil {
		t.Fatalf("stream raw responses sse: %v", err)
	}
	out := rec.Body.String()
	if strings.Contains(out, "response.done") {
		t.Fatalf("response.done must be normalized before reaching Codex: %s", out)
	}
	if !strings.Contains(out, "event: response.completed") || !strings.Contains(out, "\"type\":\"response.completed\"") || !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("response.done was not converted to a completed terminal: %s", out)
	}
	if strings.Contains(out, "event: response.failed") {
		t.Fatalf("response.done must not be treated as stream failure: %s", out)
	}
	assertResponsesStreamTerminalOnce(t, out, "response.completed")
	if usage.Total != 5 || usage.ResponseID != "resp_done_alias" {
		t.Fatalf("usage = %#v", usage)
	}
}

func TestStreamRawSSEReturnsAfterResponsesCompletedWithoutUpstreamDone(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	rec := httptest.NewRecorder()
	done := make(chan error, 1)
	go func() {
		_, err := streamRawSSE(rec, nil, newSSEStreamReader(pr), "responses")
		done <- err
	}()
	_, err := io.WriteString(pw, "event: response.output_text.delta\n"+
		"data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_done_now\",\"model\":\"gpt-test\",\"delta\":\"pong\"}\n\n"+
		"event: response.completed\n"+
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_done_now\",\"model\":\"gpt-test\",\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}\n\n")
	if err != nil {
		t.Fatalf("write pipe: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stream raw responses sse: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		_ = pr.Close()
		t.Fatal("stream did not return after response.completed without upstream [DONE]")
	}
	out := rec.Body.String()
	completedAt := strings.Index(out, "event: response.completed")
	doneAt := strings.Index(out, "data: [DONE]")
	if completedAt < 0 || doneAt < 0 || completedAt > doneAt {
		t.Fatalf("response.completed must be emitted before [DONE]: %s", out)
	}
}

func TestStreamRawSSESynthesizesResponsesFailedBeforePrematureUpstreamDone(t *testing.T) {
	body := strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_done_first\",\"model\":\"gpt-test\",\"delta\":\"pong\"}\n\n" +
		"data: [DONE]\n\n")
	rec := httptest.NewRecorder()
	_, err := streamRawSSE(rec, nil, newSSEStreamReader(body), "responses")
	if err != nil {
		t.Fatalf("stream raw responses sse: %v", err)
	}
	out := rec.Body.String()
	completedAt := strings.Index(out, "event: response.failed")
	doneAt := strings.Index(out, "data: [DONE]")
	if completedAt < 0 || doneAt < 0 || completedAt > doneAt {
		t.Fatalf("response.failed must be emitted before [DONE]: %s", out)
	}
}

func TestStreamRawSSESyntheticEventsReuseResponseID(t *testing.T) {
	body := strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"model\":\"gpt-test\",\"delta\":\"pong\"}\n\n" +
		"data: [DONE]\n\n")
	rec := httptest.NewRecorder()
	_, err := streamRawSSE(rec, nil, newSSEStreamReader(body), "responses")
	if err != nil {
		t.Fatalf("stream raw responses sse: %v", err)
	}
	var createdID, failedID string
	if err := readSSE(strings.NewReader(rec.Body.String()), func(event, data string) error {
		if strings.TrimSpace(data) == "[DONE]" {
			return nil
		}
		var raw map[string]any
		if err := json.Unmarshal([]byte(data), &raw); err != nil {
			return err
		}
		response, _ := raw["response"].(map[string]any)
		switch event {
		case "response.created":
			createdID = stringValue(response["id"])
		case "response.failed":
			failedID = stringValue(response["id"])
		}
		return nil
	}); err != nil {
		t.Fatalf("parse output sse: %v", err)
	}
	if createdID == "" || failedID == "" || createdID != failedID {
		t.Fatalf("synthetic response IDs mismatch: created=%q failed=%q stream=%s", createdID, failedID, rec.Body.String())
	}
}

func TestStreamRawSSEHandlesSplitJSONAndFailsBeforePrematureDone(t *testing.T) {
	body := strings.NewReader("data: {\"type\":\"response.output_text.delta\",\n" +
		"data: \"response_id\":\"resp_split\",\"model\":\"gpt-test\",\"delta\":\"pong\"}\n\n" +
		"data: [DONE]\n\n")
	rec := httptest.NewRecorder()
	_, err := streamRawSSE(rec, nil, newSSEStreamReader(body), "responses")
	if err != nil {
		t.Fatalf("stream raw responses sse: %v", err)
	}
	out := rec.Body.String()
	if !strings.Contains(out, "response.output_text.delta") || !strings.Contains(out, "pong") {
		t.Fatalf("split JSON delta was not forwarded: %s", out)
	}
	failedAt := strings.Index(out, "event: response.failed")
	doneAt := strings.Index(out, "data: [DONE]")
	if failedAt < 0 || doneAt < 0 || failedAt > doneAt {
		t.Fatalf("split JSON stream must fail before [DONE] when completed is missing: %s", out)
	}
	if strings.Contains(out, "event: response.completed") {
		t.Fatalf("premature upstream [DONE] must not be converted into response.completed: %s", out)
	}
}

func TestStreamRawSSEEmitsSingleFailureTerminal(t *testing.T) {
	body := strings.NewReader("event: response.failed\n" +
		"data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_failed_once\",\"model\":\"gpt-test\",\"error\":{\"message\":\"upstream broke\"}}}\n\n" +
		"data: [DONE]\n\n")
	rec := httptest.NewRecorder()
	_, err := streamRawSSE(rec, nil, newSSEStreamReader(body), "responses")
	if err != nil {
		t.Fatalf("stream raw responses sse: %v", err)
	}
	out := rec.Body.String()
	if count := strings.Count(out, "event: response.failed"); count != 1 {
		t.Fatalf("response.failed count = %d, want 1; stream=%s", count, out)
	}
	if count := strings.Count(out, "data: [DONE]"); count != 1 {
		t.Fatalf("[DONE] count = %d, want 1; stream=%s", count, out)
	}
}

func TestStreamRawSSEConvertsIncompleteToFailedTerminal(t *testing.T) {
	body := strings.NewReader("event: response.incomplete\n" +
		"data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_incomplete\",\"model\":\"gpt-test\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n" +
		"data: [DONE]\n\n")
	rec := httptest.NewRecorder()
	_, err := streamRawSSE(rec, nil, newSSEStreamReader(body), "responses")
	if err != nil {
		t.Fatalf("stream raw responses sse: %v", err)
	}
	out := rec.Body.String()
	if strings.Contains(out, "event: response.incomplete") {
		t.Fatalf("response.incomplete must not be exposed as terminal: %s", out)
	}
	assertResponsesStreamTerminalOnce(t, out, "response.failed")
}

func TestReadSSEEventsSendsHeartbeatWhileWaitingForNextEvent(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	reader := newSSEStreamReader(pr)
	reader.closer = pr
	reader.idleTimeout = 250 * time.Millisecond
	reader.heartbeatInterval = 25 * time.Millisecond
	var beats int64
	reader.heartbeat = func() error {
		atomic.AddInt64(&beats, 1)
		return nil
	}
	err := readSSEEvents(nil, reader, func(_, _ string) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "next event") {
		t.Fatalf("readSSEEvents err = %v, want next-event timeout", err)
	}
	if atomic.LoadInt64(&beats) == 0 {
		t.Fatal("expected at least one heartbeat while waiting for upstream event")
	}
}

func TestStreamNonSSEAsResponsesEventsWrapsChatJSON(t *testing.T) {
	body := []byte(`{"id":"chatcmpl_1","object":"chat.completion","model":"gpt-test","choices":[{"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`)
	rec := httptest.NewRecorder()
	usage, err := streamNonSSEAsResponsesEvents(rec, http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, body, &storage.UpstreamGroupKey{
		ChannelName: "test-channel",
		GroupName:   "test-group",
		Ratio:       0.01,
	}, "responses_from_chat")
	if err != nil {
		t.Fatalf("stream non-sse responses: %v", err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("content-type = %q", got)
	}
	out := rec.Body.String()
	if !strings.Contains(out, "event: response.output_item.added") || !strings.Contains(out, "event: response.output_text.done") || !strings.Contains(out, "event: response.completed") || !strings.Contains(out, "pong") || !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("unexpected wrapped stream: %s", out)
	}
	if usage.Prompt != 3 || usage.Completion != 2 || usage.Total != 5 {
		t.Fatalf("usage = %#v", usage)
	}
}

func TestSSEReaderDispatchesDataLineWithoutBlankSeparator(t *testing.T) {
	reader := newSSEStreamReader(strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"delta\":\"pong\"}\ndata: [DONE]\n"))
	ev, err := reader.Next()
	if err != nil {
		t.Fatalf("first event: %v", err)
	}
	if !strings.Contains(ev.Data, "pong") {
		t.Fatalf("first event = %#v", ev)
	}
	ev, err = reader.Next()
	if err != nil {
		t.Fatalf("done event: %v", err)
	}
	if strings.TrimSpace(ev.Data) != "[DONE]" {
		t.Fatalf("done event = %#v", ev)
	}
}

func TestStreamEventReadyAcceptsResponsesLifecycleEvents(t *testing.T) {
	if !streamEventReady(sseEvent{
		Event: "response.created",
		Data:  `{"type":"response.created","response":{"id":"resp_1","status":"in_progress"}}`,
	}) {
		t.Fatal("response.created should start forwarding immediately")
	}
	if !streamEventReady(sseEvent{
		Data: `{"id":"chatcmpl_1","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"}}]}`,
	}) {
		t.Fatal("chat completion chunk should start forwarding immediately")
	}
}

func TestProxyTransportAllowsSlowStreamingHeaders(t *testing.T) {
	transport := buildProxyTransport("")
	if transport.ResponseHeaderTimeout != streamFirstEventTimeout {
		t.Fatalf("response header timeout = %s, want %s", transport.ResponseHeaderTimeout, streamFirstEventTimeout)
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

func TestStreamResponsesAsChatReturnsAfterCompletedWithoutUpstreamDone(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	rec := httptest.NewRecorder()
	done := make(chan error, 1)
	go func() {
		_, err := streamResponsesAsChat(rec, pr)
		done <- err
	}()
	_, err := io.WriteString(pw, "event: response.output_text.delta\n"+
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"pong\",\"response_id\":\"resp_chat_done\"}\n\n"+
		"event: response.completed\n"+
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_chat_done\",\"model\":\"gpt-test\",\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}\n\n")
	if err != nil {
		t.Fatalf("write pipe: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stream responses as chat: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		_ = pr.Close()
		t.Fatal("chat stream did not return after response.completed without upstream [DONE]")
	}
	if out := rec.Body.String(); !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("chat stream missing [DONE]: %s", out)
	}
}
