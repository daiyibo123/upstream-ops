package notify

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestWecomSendFailsOnRobotErrCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errcode":93000,"errmsg":"invalid webhook"}`))
	}))
	defer srv.Close()

	n, err := newWecom(`{"webhook_url":"` + srv.URL + `"}`)
	if err != nil {
		t.Fatalf("new wecom: %v", err)
	}
	err = n.Send(context.Background(), Message{Subject: "测试", Body: "正文"})
	if err == nil || !strings.Contains(err.Error(), "invalid webhook") {
		t.Fatalf("Send error = %v, want invalid webhook", err)
	}
}

func TestWecomSendSucceedsOnRobotOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errcode":0,"errmsg":"ok"}`))
	}))
	defer srv.Close()

	n, err := newWecom(`{"webhook_url":"` + srv.URL + `"}`)
	if err != nil {
		t.Fatalf("new wecom: %v", err)
	}
	if err := n.Send(context.Background(), Message{Subject: "测试", Body: "正文"}); err != nil {
		t.Fatalf("Send error = %v, want nil", err)
	}
}

func TestFeishuSendFailsOnCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":9499,"msg":"sign error"}`))
	}))
	defer srv.Close()

	n, err := newFeishu(`{"webhook_url":"` + srv.URL + `"}`)
	if err != nil {
		t.Fatalf("new feishu: %v", err)
	}
	err = n.Send(context.Background(), Message{Subject: "测试", Body: "正文"})
	if err == nil || !strings.Contains(err.Error(), "sign error") {
		t.Fatalf("Send error = %v, want sign error", err)
	}
}

func TestFeishuSendFailsOnStatusCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"StatusCode":19024,"StatusMessage":"bad timestamp"}`))
	}))
	defer srv.Close()

	n, err := newFeishu(`{"webhook_url":"` + srv.URL + `"}`)
	if err != nil {
		t.Fatalf("new feishu: %v", err)
	}
	err = n.Send(context.Background(), Message{Subject: "测试", Body: "正文", Event: storage.EventMonitorFailed})
	if err == nil || !strings.Contains(err.Error(), "bad timestamp") {
		t.Fatalf("Send error = %v, want bad timestamp", err)
	}
}
