package notify

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/go-resty/resty/v2"
)

func init() {
	Register(storage.NotifyWecom, func(raw string) (Notifier, error) { return newWecom(raw) })
}

type wecomConfig struct {
	WebhookURL string `json:"webhook_url"`
}

type wecom struct {
	cfg  wecomConfig
	http *resty.Client
}

func newWecom(raw string) (*wecom, error) {
	var cfg wecomConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, err
	}
	cfg.WebhookURL = strings.TrimSpace(cfg.WebhookURL)
	if cfg.WebhookURL == "" {
		return nil, errors.New("wecom webhook_url is required")
	}
	return &wecom{cfg: cfg, http: newNotifyHTTPClient()}, nil
}

func (w *wecom) Type() storage.NotificationChannelType { return storage.NotifyWecom }

func (w *wecom) SetProxy(proxyURL string) {
	if proxyURL != "" {
		w.http.SetProxy(proxyURL)
	}
}

func (w *wecom) Send(ctx context.Context, msg Message) error {
	content := truncateUTF8Bytes(messageText(msg), 3900)
	resp, err := w.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(map[string]any{
			"msgtype": "markdown",
			"markdown": map[string]string{
				"content": content,
			},
		}).
		Post(w.cfg.WebhookURL)
	if err != nil {
		return err
	}
	if err := checkRobotResponse("wecom", resp, []string{"errcode"}, []string{"errmsg"}); err != nil {
		return err
	}
	return nil
}
