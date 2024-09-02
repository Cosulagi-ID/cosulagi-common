package message

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/AsidStorm/go-amqp-reconnect/rabbitmq"
	"github.com/spf13/viper"
)

var Conn *rabbitmq.Connection
var Channel *rabbitmq.Channel

func Init() {
	conn, err := rabbitmq.Dial(viper.GetString("RABBITMQ_URL"))
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
