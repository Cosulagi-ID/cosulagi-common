package rpc

import (
	"encoding/json"
	"fmt"
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
	Timeout    *time.Duration     `json:"timeout"`
}

func GetRPCProp() (*amqp.Channel, <-chan amqp.Delivery, error) {
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

	corrID, _ := message.GenerateRandomString(32)
	queueResp, err := ch.QueueDeclare(corrID, false, true, true, false, nil)
	if err != nil {
		return err
	}

	//defer ch.Close()
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

	channelPublish, _ := message.Conn.Channel()
	defer channelPublish.Close()

	err = channelPublish.Publish("", "rpc_queue", false, false, amqp.Publishing{
		ContentType:   "application/json",
		ReplyTo:       queueResp.Name,
		Body:          jsonRequest,
		Timestamp:     time.Now(),
		CorrelationId: corrID,
	})

	if err != nil {
		fmt.Println("error calling rpc", err)
		return err
	}

	ms, err := ch.Consume(queueResp.Name, corrID, true, false, false, false, nil)

	for d := range ms {
		if d.ContentType != "application/json" {
			err = fmt.Errorf(string(d.Body))
			d.Nack(false, false)
		}
		if d.CorrelationId == corrID {
			err = json.Unmarshal(d.Body, dst)
			d.Ack(false)
			ch.Cancel(corrID, true)
			ch.QueueDelete(queueResp.Name, true, true, true)
			break
		}
		d.Nack(false, true)
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

			timeout := rpcRequest.Timeout
			if timeout == nil {
				timeout = new(time.Duration)
				*timeout = 5 * time.Second
			}

			if time.Now().Sub(d.Timestamp) > *timeout {
				err = ch.Publish("", d.ReplyTo, false, false, amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte("Timeout"),
				})
				_ = d.Reject(false)
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
			err = ch.Publish("", d.ReplyTo, false, false, amqp.Publishing{
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
		err = ch.Publish("", d.ReplyTo, false, false, amqp.Publishing{
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
