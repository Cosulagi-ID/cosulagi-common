package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// LogConfig holds the configuration for logging
type LogConfig struct {
	Environment  string
	ServiceName  string
	Version      string
	DiscordURL   string
	MinLogLevel  zerolog.Level
	BatchSize    int
	BatchTimeout time.Duration
}

// DiscordHook implements zerolog.Hook interface with batching and rate limiting
type DiscordHook struct {
	config    LogConfig
	messages  chan *DiscordMessage
	batch     []*DiscordMessage
	mu        sync.Mutex
	client    *http.Client
	rateLimit *RateLimiter
}

// DiscordMessage represents a message to be sent to Discord
type DiscordMessage struct {
	Level     zerolog.Level
	Message   string
	Time      time.Time
	Fields    map[string]interface{}
	Caller    string
	StackInfo string
}

// RateLimiter implements a simple token bucket rate limiter
type RateLimiter struct {
	tokens     float64
	rate       float64
	burst      float64
	lastUpdate time.Time
	mu         sync.Mutex
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(rate, burst float64) *RateLimiter {
	return &RateLimiter{
		tokens:     burst,
		rate:       rate,
		burst:      burst,
		lastUpdate: time.Now(),
	}
}

// Allow checks if an action should be allowed under the rate limit
func (r *RateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(r.lastUpdate).Seconds()
	r.tokens = min(r.burst, r.tokens+elapsed*r.rate)
	r.lastUpdate = now

	if r.tokens >= 1 {
		r.tokens--
		return true
	}
	return false
}

// NewDiscordHook creates a new Discord hook with the given configuration
func NewDiscordHook(config LogConfig) (*DiscordHook, error) {
	if config.BatchSize == 0 {
		config.BatchSize = 10
	}
	if config.BatchTimeout == 0 {
		config.BatchTimeout = 5 * time.Second
	}

	hook := &DiscordHook{
		config:    config,
		messages:  make(chan *DiscordMessage, 1000),
		batch:     make([]*DiscordMessage, 0, config.BatchSize),
		client:    &http.Client{Timeout: 10 * time.Second},
		rateLimit: NewRateLimiter(1, 5), // 1 request per second, burst of 5
	}

	// Start the background worker
	go hook.worker()

	return hook, nil
}

// Run implements zerolog.Hook
func (h *DiscordHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if level < h.config.MinLogLevel {
		return
	}

	// Capture caller information
	_, file, line, ok := runtime.Caller(2)
	caller := "unknown"
	if ok {
		caller = fmt.Sprintf("%s:%d", file, line)
	}

	// Capture stack trace for errors and above
	var stack string
	if level <= zerolog.ErrorLevel {
		buf := make([]byte, 4096)
		n := runtime.Stack(buf, false)
		stack = string(buf[:n])
	}

	// Create message
	message := &DiscordMessage{
		Level:     level,
		Message:   msg,
		Time:      time.Now(),
		Fields:    make(map[string]interface{}),
		Caller:    caller,
		StackInfo: stack,
	}

	// Don't block if channel is full
	select {
	case h.messages <- message:
	default:
		fmt.Printf("Discord hook message channel full, dropping message\n")
	}
}

func (h *DiscordHook) worker() {
	ticker := time.NewTicker(h.config.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case msg := <-h.messages:
			h.batch = append(h.batch, msg)
			if len(h.batch) >= h.config.BatchSize {
				h.sendBatch()
			}
		case <-ticker.C:
			if len(h.batch) > 0 {
				h.sendBatch()
			}
		}
	}
}

func (h *DiscordHook) sendBatch() {
	h.mu.Lock()
	batch := h.batch
	h.batch = make([]*DiscordMessage, 0, h.config.BatchSize)
	h.mu.Unlock()

	if !h.rateLimit.Allow() {
		fmt.Printf("Rate limit exceeded, retrying later\n")
		// Re-queue messages
		for _, msg := range batch {
			h.messages <- msg
		}
		return
	}

	embeds := make([]map[string]interface{}, 0, len(batch))
	for _, msg := range batch {
		embed := map[string]interface{}{
			"title":       fmt.Sprintf("[%s] %s Log Event", h.config.Environment, h.config.ServiceName),
			"description": msg.Message,
			"color":       getLevelColor(msg.Level),
			"fields": []map[string]interface{}{
				{
					"name":   "Level",
					"value":  msg.Level.String(),
					"inline": true,
				},
				{
					"name":   "Timestamp",
					"value":  msg.Time.Format(time.RFC3339),
					"inline": true,
				},
				{
					"name":   "Caller",
					"value":  msg.Caller,
					"inline": false,
				},
			},
			"footer": map[string]interface{}{
				"text": fmt.Sprintf("Version: %s", h.config.Version),
			},
		}

		if msg.StackInfo != "" {
			embed["fields"] = append(embed["fields"].([]map[string]interface{}), map[string]interface{}{
				"name":   "Stack Trace",
				"value":  fmt.Sprintf("```\n%s\n```", msg.StackInfo),
				"inline": false,
			})
		}

		embeds = append(embeds, embed)
	}

	payload := map[string]interface{}{
		"embeds": embeds,
	}

	go h.sendToDiscord(payload)
}

func (h *DiscordHook) sendToDiscord(payload map[string]interface{}) {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("Error marshaling Discord payload: %v\n", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", h.config.DiscordURL, bytes.NewBuffer(jsonBytes))
	if err != nil {
		fmt.Printf("Error creating request: %v\n", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		fmt.Printf("Error sending to Discord: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		fmt.Printf("Discord API returned non-2xx status: %d\n", resp.StatusCode)
	}
}

func getLevelColor(level zerolog.Level) int {
	switch level {
	case zerolog.DebugLevel:
		return 0x7289DA
	case zerolog.InfoLevel:
		return 0x3498DB
	case zerolog.WarnLevel:
		return 0xF1C40F
	case zerolog.ErrorLevel:
		return 0xE74C3C
	case zerolog.FatalLevel:
		return 0x992D22
	default:
		return 0x95A5A6
	}
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
