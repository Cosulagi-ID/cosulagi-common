package rpc

import (
	"encoding/json"
	"fmt"
	"github.com/AsidStorm/go-amqp-reconnect/rabbitmq"
	"github.com/Cosulagi-ID/cosulagi-common/message"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

var rpcFunctions = make(map[string]func(params ...interface{}) (interface{}, error))

type RPCRequestParams struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

type RPCRequest struct {
	Name       string             `json:"name"`
	Parameters []RPCRequestParams `json:"parameters"`
}

func GetRPCProp() (*rabbitmq.Channel, <-chan amqp.Delivery, error) {
	ch, err := message.GetChannel()
	q, err := ch.QueueDeclare("rpc_queue", false, false, false, false, nil)
	err = ch.Qos(1, 0, false)
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)

	return ch, msgs, err
}

func RegisterRPCFunction(name string, f func(params ...interface{}) (interface{}, error)) {
	rpcFunctions[name] = f
}

func CallRPC(name string, dst interface{}, params ...interface{}) error {
	ch, err := message.Conn.Channel()
	defer ch.Close()
	corrID, err := message.GenerateRandomString(32)
	paramsList := make([]RPCRequestParams, 0)
	for _, param := range params {
		paramsList = append(paramsList, RPCRequestParams{
			Name:  "",
			Value: param,
		})
	}

	request := RPCRequest{
		Name:       name,
		Parameters: paramsList,
	}

	jsonRequest, err := json.Marshal(request)

	err = ch.Publish("", "rpc_queue", false, false, rabbitmq.Publishing{
		ContentType:   "application/json",
		CorrelationId: corrID,
		ReplyTo:       message.QueueRespondRPC.Name,
		Body:          jsonRequest,
		Timestamp:     time.Now(),
	})

	if err != nil {
		fmt.Println("error calling rpc", err)
		return err
	}

	for d := range message.Msgs {
		if d.ContentType == "text/plain" {
			err = fmt.Errorf(string(d.Body))
			d.Reject(false)
		} else {
			json.Unmarshal(d.Body, dst)
			d.Ack(false)
		}
	}

	return err
}

func RPCServer() {
	ch, msgs, err := GetRPCProp()

	if err != nil {
		fmt.Println(err)
	}

	for d := range msgs {
		//parse body to RPCRequest
		var rpcRequest RPCRequest
		_ = json.Unmarshal(d.Body, &rpcRequest)

		//get function by name
		f, ok := rpcFunctions[rpcRequest.Name]
		if !ok {

			//check if it's 1 hour old
			if time.Now().Sub(d.Timestamp) > time.Hour {
				_ = d.Reject(false) //if it's 1 hour old, reject the message, it's probably a dead message
				continue
			}

			_ = d.Reject(true) //we don't have function with that name, reject the message and give it back to the queue
			continue
		}

		//get parameters
		params := make([]interface{}, 0)
		for _, param := range rpcRequest.Parameters {
			params = append(params, param.Value)
		}

		//call function
		result, err := f(params...)

		if err != nil {
			err = ch.Publish("", d.ReplyTo, false, false, rabbitmq.Publishing{
				ContentType:   "text/plain",
				CorrelationId: d.CorrelationId,
				Body:          []byte(err.Error()),
			})

			err = d.Reject(false)

			if err != nil {
				fmt.Println(err)
			}

			continue
		}

		parseResult, _ := json.Marshal(result)
		//send result
		err = ch.Publish("", d.ReplyTo, false, false, rabbitmq.Publishing{
			ContentType:   "application/json",
			CorrelationId: d.CorrelationId,
			Body:          parseResult,
		})

		err = d.Ack(false)

		if err != nil {
			fmt.Println(err)
		}
	}
}
