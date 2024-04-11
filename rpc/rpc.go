package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Cosulagi-ID/cosulagi-common/message"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

var rpcFunctions = make(map[string]func(params ...interface{}) interface{})

type RPCRequestParams struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

type RPCRequest struct {
	Name       string             `json:"name"`
	Parameters []RPCRequestParams `json:"parameters"`
}

func GetRPCProp() (*amqp.Channel, <-chan amqp.Delivery) {
	ch, _ := message.GetChannel()
	q, _ := ch.QueueDeclare("rpc_queue", false, false, false, false, nil)
	_ = ch.Qos(1, 0, false)
	msgs, _ := ch.Consume(q.Name, "", false, false, false, false, nil)
	return ch, msgs
}

func RegisterRPCFunction(name string, f func(params ...interface{}) interface{}) {
	rpcFunctions[name] = f
}

func CallRPC(request RPCRequest, dst interface{}) {
	ch, _ := message.GetChannel()
	q, _ := ch.QueueDeclare("", false, false, true, false, nil)
	msgs, _ := ch.Consume(q.Name, "", true, false, false, false, nil)
	corrID, _ := message.GenerateRandomString(32)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jsonRequest, _ := json.Marshal(request)

	err := ch.PublishWithContext(ctx, "", "rpc_queue", false, false, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: corrID,
		ReplyTo:       q.Name,
		Body:          jsonRequest,
	})

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	for d := range msgs {
		if corrID == d.CorrelationId {
			_ = json.Unmarshal(d.Body, dst)
			break
		}
	}
}

func RPCServer() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, msgs := GetRPCProp()

	for d := range msgs {
		//parse body to RPCRequest
		var rpcRequest RPCRequest
		_ = json.Unmarshal(d.Body, &rpcRequest)

		//get function by name
		f, ok := rpcFunctions[rpcRequest.Name]
		if !ok {
			_ = d.Reject(false)
			continue
		}

		//get parameters
		params := make([]interface{}, 0)
		for _, param := range rpcRequest.Parameters {
			params = append(params, param.Value)
		}

		//call function
		result := f(params...)

		parseResult, _ := json.Marshal(result)
		//send result
		_ = ch.PublishWithContext(ctx, "", d.ReplyTo, false, false, amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: d.CorrelationId,
			Body:          parseResult,
		})

		_ = d.Ack(false)
	}
}
