package opsnotify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

var client = &http.Client{Timeout: 15 * time.Second}

// Send posts an operations-only alert. Missing configuration is a no-op so
// Telegram never becomes a dependency of signup, verification, or checkout.
func Send(message string) error {
	return send(message, nil)
}

type Button struct {
	Text string `json:"text"`
	Data string `json:"callback_data"`
}

// SendWithButtons sends a message with inline action buttons. Callback data is
// never rendered to the operator, so internal routing keys stay out of chat.
func SendWithButtons(message string, rows [][]Button) error {
	return send(message, rows)
}

func SendWithButtonsAsync(message string, rows [][]Button) {
	go func() {
		if err := SendWithButtons(message, rows); err != nil {
			log.Printf("ops Telegram alert failed: %v", err)
		}
	}()
}

func send(message string, rows [][]Button) error {
	token := setting("TELEGRAM_BOT_TOKEN")
	chatID := setting("TELEGRAM_CHAT_ID")
	if token == "" || chatID == "" {
		return nil
	}
	if prefix := setting("TELEGRAM_ALERT_PREFIX"); prefix != "" {
		message = prefix + " " + message
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	form := url.Values{"chat_id": {chatID}, "text": {message}, "disable_web_page_preview": {"true"}}
	if len(rows) > 0 {
		markup, err := json.Marshal(map[string]interface{}{"inline_keyboard": rows})
		if err != nil {
			return err
		}
		form.Set("reply_markup", string(markup))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://api.telegram.org/bot"+token+"/sendMessage", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		// net/http errors include the request URL, which embeds the bot token.
		if urlErr, ok := err.(*url.Error); ok {
			return fmt.Errorf("telegram request failed: %v", urlErr.Err)
		}
		return errors.New("telegram request failed")
	}
	defer resp.Body.Close()
	var result struct {
		OK bool `json:"ok"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK || !result.OK {
		return errors.New(fmt.Sprintf("telegram send failed: HTTP %d", resp.StatusCode))
	}
	return nil
}

func SendAsync(message string) {
	go func() {
		configured := setting("TELEGRAM_BOT_TOKEN") != "" && setting("TELEGRAM_CHAT_ID") != ""
		if err := Send(message); err != nil {
			log.Printf("ops Telegram alert failed: %v", err)
		} else if configured {
			log.Print("ops Telegram alert accepted")
		}
	}()
}

func setting(key string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return strings.TrimSpace(viper.GetString(key))
}
