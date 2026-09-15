package opsnotify

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestSendUsesConfiguredChatAndPrefix(t *testing.T) {
	t.Setenv("TELEGRAM_BOT_TOKEN", "test-token")
	t.Setenv("TELEGRAM_CHAT_ID", "123")
	t.Setenv("TELEGRAM_ALERT_PREFIX", "[DEV]")
	previous := client
	t.Cleanup(func() { client = previous })
	client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost || req.URL.String() != "https://api.telegram.org/bottest-token/sendMessage" {
			t.Fatalf("unexpected Telegram request: %s %s", req.Method, req.URL)
		}
		if err := req.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if req.Form.Get("chat_id") != "123" || req.Form.Get("text") != "[DEV] hello" {
			t.Fatalf("unexpected Telegram form: %v", req.Form)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
	})}
	if err := Send("hello"); err != nil {
		t.Fatal(err)
	}
}

func TestSendUnprefixedKeepsCopyReadyText(t *testing.T) {
	t.Setenv("TELEGRAM_BOT_TOKEN", "test-token")
	t.Setenv("TELEGRAM_CHAT_ID", "123")
	t.Setenv("TELEGRAM_ALERT_PREFIX", "[PROD]")
	previous := client
	t.Cleanup(func() { client = previous })
	client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if err := req.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if req.Form.Get("text") != "Halo Kak Nindya" {
			t.Fatalf("copy-ready text must not have an environment prefix: %q", req.Form.Get("text"))
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
	})}
	if err := SendUnprefixed("Halo Kak Nindya"); err != nil {
		t.Fatal(err)
	}
}

func TestSendWithButtonsKeepsCallbackDataOutOfVisibleText(t *testing.T) {
	t.Setenv("TELEGRAM_BOT_TOKEN", "test-token")
	t.Setenv("TELEGRAM_CHAT_ID", "123")
	previous := client
	t.Cleanup(func() { client = previous })
	client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if err := req.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(req.Form.Get("text"), "internal-key") {
			t.Fatal("callback data leaked into visible message")
		}
		markup := req.Form.Get("reply_markup")
		if !strings.Contains(markup, `"text":"Verify"`) || !strings.Contains(markup, `"callback_data":"internal-key"`) {
			t.Fatalf("unexpected reply markup: %s", markup)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
	})}
	if err := SendWithButtons("customer phone", [][]Button{{{Text: "Verify", Data: "internal-key"}}}); err != nil {
		t.Fatal(err)
	}
}
