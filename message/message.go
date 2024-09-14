package message

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/AsidStorm/go-amqp-reconnect/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"
)

var Conn *amqp.Connection
var Channel *amqp.Channel
var QueueRespondRPC *amqp.Queue

func Init() {
	conn, err := amqp.Dial(viper.GetString("RABBITMQ_URL"))
	rabbitmq.Debug = true
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println("Connected to RabbitMQ")
	Conn = conn
	ch, err := conn.Channel()
	if err != nil {
		fmt.Println(err.Error())
	}
	Channel = ch
	q, err := ch.QueueDeclare("", false, true, false, false, amqp.Table{
		"x-message-ttl": 60000,
		"x-expires":     60000,
	})
	if err != nil {
		fmt.Println(err.Error())
	}
	QueueRespondRPC = &q
}

func GetChannel() (*amqp.Channel, error) {
	return Channel, nil
}

func GenerateRandomString(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
