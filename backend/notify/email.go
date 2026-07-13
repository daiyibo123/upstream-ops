package notify

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"mime"
	"net"
	"net/smtp"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

func init() {
	Register(storage.NotifyEmail, func(raw string) (Notifier, error) { return newEmail(raw) })
}

type emailConfig struct {
	Host     string   `json:"host"`     // smtp.example.com
	Port     int      `json:"port"`     // 465 / 587
	Username string   `json:"username"` // SMTP 用户名
	Password string   `json:"password"` // SMTP 密码 / 授权码
	From     string   `json:"from"`     // 发件人（可与 Username 不同）
	To       []string `json:"to"`       // 收件人列表
	UseTLS   bool     `json:"use_tls"`  // 是否使用隐式 TLS（一般 465 端口）
}

type email struct{ cfg emailConfig }

func newEmail(raw string) (*email, error) {
	var cfg emailConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, err
	}
	cfg.Host = strings.TrimSpace(cfg.Host)
	cfg.Username = strings.TrimSpace(cfg.Username)
	cfg.From = strings.TrimSpace(cfg.From)
	to := cfg.To[:0]
	for _, rcpt := range cfg.To {
		rcpt = strings.TrimSpace(rcpt)
		if rcpt != "" {
			to = append(to, rcpt)
		}
	}
	cfg.To = to
	if cfg.Host == "" || cfg.Port == 0 || cfg.From == "" || len(cfg.To) == 0 {
		return nil, errors.New("email config requires host/port/from/to")
	}
	return &email{cfg: cfg}, nil
}

func (e *email) Type() storage.NotificationChannelType { return storage.NotifyEmail }

func (e *email) Send(ctx context.Context, msg Message) error {
	addr := fmt.Sprintf("%s:%d", e.cfg.Host, e.cfg.Port)
	var auth smtp.Auth
	if e.cfg.Username != "" || e.cfg.Password != "" {
		auth = smtp.PlainAuth("", e.cfg.Username, e.cfg.Password, e.cfg.Host)
	}

	body := buildEmailBody(e.cfg.From, e.cfg.To, msg)

	// 简单 deadline，避免完全阻塞调度。
	done := make(chan error, 1)
	go func() {
		if e.cfg.Port == 465 {
			done <- sendTLS(addr, e.cfg.Host, auth, e.cfg.From, e.cfg.To, []byte(body))
			return
		}
		done <- sendSMTP(addr, e.cfg.Host, auth, e.cfg.From, e.cfg.To, []byte(body), e.cfg.UseTLS)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(45 * time.Second):
		return errors.New("smtp send timeout")
	}
}

func buildEmailBody(from string, to []string, msg Message) string {
	subject := sanitizeMailHeader(msg.Subject)
	plainBody := strings.TrimSpace(msg.Body)
	if plainBody == "" {
		plainBody = subject
	}
	boundary := fmt.Sprintf("upstreamops-%d", time.Now().UnixNano())
	headers := []string{
		"From: " + sanitizeMailHeader(from),
		"To: " + sanitizeMailHeader(strings.Join(to, ", ")),
		"Subject: " + mime.QEncoding.Encode("UTF-8", subject),
		"Date: " + time.Now().Format(time.RFC1123Z),
		"MIME-Version: 1.0",
		fmt.Sprintf(`Content-Type: multipart/alternative; boundary="%s"`, boundary),
		"X-Mailer: UpstreamOps",
	}
	parts := []string{
		strings.Join(headers, "\r\n"),
		"",
		"--" + boundary,
		"Content-Type: text/plain; charset=UTF-8",
		"Content-Transfer-Encoding: 8bit",
		"",
		plainBody,
		"--" + boundary,
		"Content-Type: text/html; charset=UTF-8",
		"Content-Transfer-Encoding: 8bit",
		"",
		buildEmailHTML(subject, plainBody, msg),
		"--" + boundary + "--",
		"",
	}
	return strings.Join(parts, "\r\n")
}

func sanitizeMailHeader(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return strings.TrimSpace(value)
}

func buildEmailHTML(subject, body string, msg Message) string {
	event := emailEventLabel(msg.Event)
	when := time.Now().Format("2006-01-02 15:04:05")
	details := emailDetailRows(body)
	pre := html.EscapeString(body)
	pre = strings.ReplaceAll(pre, "\n", "<br>")
	channel := ""
	if msg.ChannelID > 0 {
		channel = fmt.Sprintf(`<span style="display:inline-block;margin-left:8px;color:#64748b;">上游 #%d</span>`, msg.ChannelID)
	}
	model := ""
	if strings.TrimSpace(msg.ModelName) != "" {
		model = fmt.Sprintf(`<span style="display:inline-block;margin-left:8px;color:#64748b;">%s</span>`, html.EscapeString(msg.ModelName))
	}
	return fmt.Sprintf(`<!doctype html>
<html>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,'PingFang SC','Microsoft YaHei',sans-serif;color:#0f172a;">
  <div style="display:none;max-height:0;overflow:hidden;color:transparent;">%s</div>
  <table role="presentation" width="100%%" cellspacing="0" cellpadding="0" style="background:#f1f5f9;padding:28px 12px;">
    <tr>
      <td align="center">
        <table role="presentation" width="100%%" cellspacing="0" cellpadding="0" style="max-width:640px;overflow:hidden;border-radius:18px;background:#ffffff;box-shadow:0 18px 50px rgba(15,23,42,.10);">
          <tr>
            <td style="padding:24px 28px;background:linear-gradient(135deg,#0891b2,#2563eb 56%%,#10b981);color:#ffffff;">
              <div style="font-size:13px;letter-spacing:.08em;text-transform:uppercase;opacity:.86;">UpstreamOps</div>
              <h1 style="margin:10px 0 0;font-size:22px;line-height:1.35;font-weight:750;">%s</h1>
              <div style="margin-top:12px;font-size:13px;opacity:.92;">
                <span style="display:inline-block;border-radius:999px;background:rgba(255,255,255,.18);padding:5px 10px;">%s</span>
                %s%s
              </div>
            </td>
          </tr>
          <tr>
            <td style="padding:24px 28px 8px;">
              <div style="margin-bottom:16px;color:#64748b;font-size:13px;">发送时间：%s</div>
              %s
              <div style="margin-top:18px;border:1px solid #e2e8f0;border-radius:14px;background:#f8fafc;padding:16px;color:#334155;font-size:14px;line-height:1.75;">%s</div>
            </td>
          </tr>
          <tr>
            <td style="padding:18px 28px 24px;color:#94a3b8;font-size:12px;border-top:1px solid #eef2f7;">
              这封邮件由 UpstreamOps 自动发送。你可以在系统设置的通知渠道和通知策略里调整接收范围。
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>`, html.EscapeString(subject), html.EscapeString(subject), html.EscapeString(event), channel, model, when, details, pre)
}

func emailDetailRows(body string) string {
	lines := strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n")
	rows := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		idx := strings.Index(line, "：")
		sepLen := len("：")
		if idx < 0 {
			idx = strings.Index(line, ":")
			sepLen = 1
		}
		if idx <= 0 || idx >= len(line)-1 {
			continue
		}
		label := strings.TrimSpace(line[:idx])
		value := strings.TrimSpace(line[idx+sepLen:])
		rows = append(rows, fmt.Sprintf(
			`<tr><td style="padding:10px 12px;color:#64748b;font-size:13px;border-bottom:1px solid #eef2f7;width:120px;">%s</td><td style="padding:10px 12px;color:#0f172a;font-size:13px;border-bottom:1px solid #eef2f7;font-weight:600;">%s</td></tr>`,
			html.EscapeString(label),
			html.EscapeString(value),
		))
	}
	if len(rows) == 0 {
		return ""
	}
	return `<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border:1px solid #e2e8f0;border-radius:14px;border-collapse:separate;border-spacing:0;overflow:hidden;">` + strings.Join(rows, "") + `</table>`
}

func emailEventLabel(event storage.NotificationEvent) string {
	switch event {
	case storage.EventBalanceLow:
		return "余额告警"
	case storage.EventRateChanged:
		return "倍率变化"
	case storage.EventRateStructureChanged:
		return "分组变动"
	case storage.EventRateAdded:
		return "分组新增"
	case storage.EventRateRemoved:
		return "分组删除"
	case storage.EventLoginFailed:
		return "登录异常"
	case storage.EventMonitorFailed:
		return "监控异常"
	case storage.EventAnnouncement:
		return "上游公告"
	case storage.EventSubscriptionDailyLow, storage.EventSubscriptionWeeklyLow, storage.EventSubscriptionMonthlyLow, storage.EventSubscriptionExpiring:
		return "订阅告警"
	default:
		return "系统通知"
	}
}

// sendTLS 通过 SMTPS（隐式 TLS，常见于 465）发送邮件。
func sendTLS(addr, host string, auth smtp.Auth, from string, to []string, body []byte) error {
	tlsConfig := &tls.Config{ServerName: host}
	dialer := &net.Dialer{Timeout: 15 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", addr, tlsConfig)
	if err != nil {
		return fmt.Errorf("smtp tls dial: %w", err)
	}
	defer conn.Close()

	client, err := smtp.NewClient(conn, host)
	if err != nil {
		return fmt.Errorf("smtp new client: %w", err)
	}
	defer client.Quit()

	return sendWithSMTPClient(client, auth, from, to, body)
}

// sendSMTP 通过普通 SMTP 发送；服务端支持 STARTTLS 时自动升级。
// requireStartTLS=true 时如果服务端不支持 STARTTLS 会直接报错，用于兼容用户把
// 587 端口理解成“开启 TLS”的常见配置方式。
func sendSMTP(addr, host string, auth smtp.Auth, from string, to []string, body []byte, requireStartTLS bool) error {
	dialer := &net.Dialer{Timeout: 15 * time.Second}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("smtp dial: %w", err)
	}
	defer conn.Close()

	client, err := smtp.NewClient(conn, host)
	if err != nil {
		return fmt.Errorf("smtp new client: %w", err)
	}
	defer client.Quit()

	if ok, _ := client.Extension("STARTTLS"); ok {
		if err := client.StartTLS(&tls.Config{ServerName: host}); err != nil {
			return fmt.Errorf("smtp starttls: %w", err)
		}
	} else if requireStartTLS {
		return errors.New("smtp server does not support STARTTLS")
	}

	return sendWithSMTPClient(client, auth, from, to, body)
}

func sendWithSMTPClient(client *smtp.Client, auth smtp.Auth, from string, to []string, body []byte) error {
	if auth != nil {
		if err := client.Auth(auth); err != nil {
			return fmt.Errorf("smtp auth: %w", err)
		}
	}
	if err := client.Mail(from); err != nil {
		return fmt.Errorf("smtp mail: %w", err)
	}
	for _, rcpt := range to {
		if err := client.Rcpt(rcpt); err != nil {
			return fmt.Errorf("smtp rcpt %s: %w", rcpt, err)
		}
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp data: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("smtp write: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("smtp close: %w", err)
	}
	return nil
}
