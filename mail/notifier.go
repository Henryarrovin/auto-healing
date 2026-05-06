package mail

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/smtp"
	"strings"
	"time"
)

// Gmail:        Host=smtp.gmail.com       Port=587  (App Password required)
// Outlook/O365: Host=smtp.office365.com   Port=587
// Custom relay: Host=mail.yourcompany.com Port=587 or 465
// Local relay:  Host=localhost            Port=25   (no auth needed)
type Config struct {
	Host     string // SMTP server hostname
	Port     string // 25 (relay), 465 (implicit TLS), 587 (STARTTLS)
	Username string // leave empty for unauthenticated relays
	Password string
	From     string // envelope From address
	To       string // recipient — comma-separate multiple addresses
}

type Message struct {
	Resource   string
	Reason     string
	Action     string
	Status     string
	Diagnosis  string
	Namespace  string
	OccurredAt string
}

type Notifier struct {
	cfg Config
}

func NewNotifier(cfg Config) *Notifier {
	return &Notifier{cfg: cfg}
}

// Send builds an HTML email and delivers it.
// Port 465  → implicit TLS (SMTPS).
// Port 587 / anything else → STARTTLS upgrade.
// No credentials → unauthenticated relay (port 25 use-case).
func (n *Notifier) Send(msg Message) {
	if err := n.send(msg); err != nil {
		log.Printf("[mail] send failed: %v", err)
	}
}

func (n *Notifier) send(msg Message) error {
	body := n.buildBody(msg)
	recipients := splitAddresses(n.cfg.To)

	if n.cfg.Port == "465" {
		return n.sendImplicitTLS(body, recipients)
	}
	return n.sendSTARTTLS(body, recipients)
}

// dials port 465 — TLS from the very first byte.
func (n *Notifier) sendImplicitTLS(body []byte, recipients []string) error {
	tlsCfg := &tls.Config{ServerName: n.cfg.Host}
	conn, err := tls.Dial("tcp", net.JoinHostPort(n.cfg.Host, n.cfg.Port), tlsCfg)
	if err != nil {
		return fmt.Errorf("tls dial %s:%s: %w", n.cfg.Host, n.cfg.Port, err)
	}
	defer conn.Close()

	client, err := smtp.NewClient(conn, n.cfg.Host)
	if err != nil {
		return fmt.Errorf("smtp client: %w", err)
	}
	defer client.Quit()

	if n.cfg.Username != "" {
		auth := smtp.PlainAuth("", n.cfg.Username, n.cfg.Password, n.cfg.Host)
		if err = client.Auth(auth); err != nil {
			return fmt.Errorf("smtp auth: %w", err)
		}
	}
	return smtpDeliver(client, n.cfg.From, recipients, body)
}

// connects plain then upgrades (port 587 / 25).
func (n *Notifier) sendSTARTTLS(body []byte, recipients []string) error {
	addr := net.JoinHostPort(n.cfg.Host, n.cfg.Port)
	var auth smtp.Auth
	if n.cfg.Username != "" {
		auth = smtp.PlainAuth("", n.cfg.Username, n.cfg.Password, n.cfg.Host)
	}
	return smtp.SendMail(addr, auth, n.cfg.From, recipients, body)
}

func smtpDeliver(client *smtp.Client, from string, to []string, body []byte) error {
	if err := client.Mail(from); err != nil {
		return fmt.Errorf("MAIL FROM: %w", err)
	}
	for _, rcpt := range to {
		if err := client.Rcpt(rcpt); err != nil {
			return fmt.Errorf("RCPT TO %s: %w", rcpt, err)
		}
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("DATA: %w", err)
	}
	defer w.Close()
	_, err = w.Write(body)
	return err
}

func (n *Notifier) buildBody(msg Message) []byte {
	statusClass := "healed"
	if strings.Contains(msg.Status, "failed") || strings.HasPrefix(msg.Status, "⚠") {
		statusClass = "failed"
	}

	subject := fmt.Sprintf("[auto-healer] %s — %s", msg.Resource, msg.Status)

	html := fmt.Sprintf(`<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
body{font-family:Arial,sans-serif;background:#f4f4f4;margin:0;padding:20px}
.card{background:#fff;border-radius:8px;padding:28px 32px;max-width:620px;margin:0 auto;border:1px solid #e0e0e0}
.top{margin-bottom:18px}
h2{margin:0;font-size:19px;color:#1a1a1a;display:inline}
.badge{display:inline-block;padding:3px 11px;border-radius:20px;font-size:12px;font-weight:600;margin-left:10px;vertical-align:middle}
.healed{background:#d4edda;color:#155724}
.failed{background:#f8d7da;color:#721c24}
.meta{font-size:12px;color:#999;margin:4px 0 0}
table{width:100%%;border-collapse:collapse;margin:0 0 20px}
td{padding:9px 12px;border-bottom:1px solid #f0f0f0;font-size:14px;color:#333;vertical-align:top}
td:first-child{font-weight:600;color:#666;width:36%%;white-space:nowrap}
.diagnosis{background:#f5f8ff;border-left:4px solid #4a90e2;padding:14px 16px;border-radius:0 6px 6px 0;font-size:14px;line-height:1.75;color:#333;white-space:pre-wrap}
.diag-title{font-size:12px;font-weight:700;color:#555;margin-bottom:8px;display:block;text-transform:uppercase;letter-spacing:.04em}
.footer{margin-top:20px;font-size:11px;color:#bbb;text-align:center}
</style></head>
<body><div class="card">
  <div class="top">
    <h2>🩺 Auto-healer triggered</h2>
    <span class="badge %s">%s</span>
    <p class="meta">%s &middot; namespace <strong>%s</strong></p>
  </div>
  <table>
    <tr><td>Resource</td><td><code>%s</code></td></tr>
    <tr><td>Trigger</td><td>%s</td></tr>
    <tr><td>Action taken</td><td>%s</td></tr>
    <tr><td>Occurred at</td><td>%s</td></tr>
  </table>
  <div class="diagnosis">
    <span class="diag-title">🤖 qwen2.5:1.5b — AI diagnosis</span>%s
  </div>
  <div class="footer">auto-healer &middot; %s &middot; namespace %s</div>
</div></body></html>`,
		statusClass, msg.Status,
		msg.OccurredAt, msg.Namespace,
		msg.Resource,
		msg.Reason,
		msg.Action,
		msg.OccurredAt,
		msg.Diagnosis,
		time.Now().Format("2006-01-02"), msg.Namespace,
	)

	headers := fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=\"UTF-8\"\r\n\r\n",
		n.cfg.From, n.cfg.To, subject,
	)
	return []byte(headers + html)
}

func splitAddresses(s string) []string {
	var out []string
	for _, addr := range strings.Split(s, ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			out = append(out, addr)
		}
	}
	return out
}
