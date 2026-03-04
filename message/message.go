package message

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/AsidStorm/go-amqp-reconnect/rabbitmq"
	"github.com/spf13/viper"
)

var Conn *rabbitmq.Connection
var Channel *rabbitmq.Channel

func Init() error {
	rabbitmq.Debug = true
	conn, err := rabbitmq.Dial(viper.GetString("RABBITMQ_URL"))
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	fmt.Println("Connected to RabbitMQ")
	Conn = conn
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}
	Channel = ch
	return nil
}

// IsConnected returns true if the RabbitMQ connection is alive.
func IsConnected() bool {
	return Conn != nil && !Conn.IsClosed()
}

func GetChannel() (*rabbitmq.Channel, error) {
	if Channel == nil {
		return nil, errors.New("channel is nil, connection not initialized")
	}
	if Channel.IsClosed() {
		return nil, errors.New("channel is closed")
	}
	return Channel, nil
}

// IsConnectionError returns true if the error indicates an AMQP connection/channel problem.
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	patterns := []string{
		"connection is not open",
		"channel/connection is not open",
		"channel is not open",
		"closed",
		"connection closed",
		"channel closed",
		"transport error",
		"i/o timeout",
		"connection refused",
		"broken pipe",
		"connection reset",
		"504",
	}
	for _, p := range patterns {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}

func GenerateRandomString(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
