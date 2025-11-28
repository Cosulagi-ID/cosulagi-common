package message

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/AsidStorm/go-amqp-reconnect/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"
)

var Conn *amqp.Connection
var Channel *amqp.Channel

func Init() error {
	conn, err := amqp.Dial(viper.GetString("RABBITMQ_URL"))
	rabbitmq.Debug = true
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

func GetChannel() (*amqp.Channel, error) {
	if Channel == nil {
		return nil, errors.New("channel is nil, connection not initialized")
	}
	return Channel, nil
}

func GenerateRandomString(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
