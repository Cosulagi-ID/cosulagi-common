package message

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"
)

var Conn *amqp.Connection

func Init() {
	conn, err := amqp.Dial(viper.GetString("RABBITMQ_URL"))
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println("Connected to RabbitMQ")
	Conn = conn
}

func GetChannel() (*amqp.Channel, error) {
	ch, err := Conn.Channel()
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	return ch, nil
}

func GenerateRandomString(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
