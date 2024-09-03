package message

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/AsidStorm/go-amqp-reconnect/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"
)

var Conn *rabbitmq.Connection
var Channel *rabbitmq.Channel
var QueueRespondRPC *amqp.Queue
var Msgs <-chan amqp.Delivery

func Init() {
	conn, err := rabbitmq.Dial(viper.GetString("RABBITMQ_URL"))
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
	q, err := ch.QueueDeclare("rpc_queue", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err.Error())
	}
	QueueRespondRPC = &q
	ms, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	Msgs = ms
}

func GetChannel() (*rabbitmq.Channel, error) {
	return Channel, nil
}

func GenerateRandomString(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
