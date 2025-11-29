package rpc

import (
	"encoding/json"
	"fmt"
	"strings"
	"github.com/Cosulagi-ID/cosulagi-common/message"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

var rpcFunctions = make(map[string]func(params ...interface{}) (interface{}, error))

// isChannelClosed checks if the channel is closed or invalid
func isChannelClosed(ch *amqp.Channel) bool {
	if ch == nil {
		return true
	}
	// Check if channel is closed by trying to get its state
	// If channel is closed, IsClosed() will return true
	return ch.IsClosed()
}

// safeAck safely acknowledges a message, handling errors gracefully
func safeAck(d amqp.Delivery) error {
	if d.Acknowledger == nil {
		return fmt.Errorf("delivery acknowledger is nil")
	}
	err := d.Ack(false)
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		// Ignore "unknown delivery tag" errors as the message may have already been acked
		if strings.Contains(errMsg, "unknown delivery tag") {
			return nil // Message already processed, ignore
		}
		// Ignore errors if channel is closed
		if strings.Contains(errMsg, "channel/connection is not open") ||
			strings.Contains(errMsg, "channel is not open") ||
			strings.Contains(errMsg, "504") {
			return nil // Channel closed, message will be redelivered
		}
	}
	return err
}

// safeReject safely rejects a message, handling errors gracefully
func safeReject(d amqp.Delivery, requeue bool) error {
	if d.Acknowledger == nil {
		return fmt.Errorf("delivery acknowledger is nil")
	}
	err := d.Reject(requeue)
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		// Ignore "unknown delivery tag" errors as the message may have already been processed
		if strings.Contains(errMsg, "unknown delivery tag") {
			return nil // Message already processed, ignore
		}
		// Ignore errors if channel is closed
		if strings.Contains(errMsg, "channel/connection is not open") ||
			strings.Contains(errMsg, "channel is not open") ||
			strings.Contains(errMsg, "504") {
			return nil // Channel closed, message will be redelivered
		}
	}
	return err
}

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
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get channel: %w", err)
	}
	if ch == nil {
		return nil, nil, fmt.Errorf("channel is nil")
	}
	
	q, err := ch.QueueDeclare("rpc_queue", false, false, false, false, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to declare queue: %w", err)
	}
	
	err = ch.Qos(1, 0, false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to set Qos: %w", err)
	}
	
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to consume queue: %w", err)
	}

	return ch, msgs, nil
}

func RegisterRPCFunction(name string, f func(params ...interface{}) (interface{}, error)) {
	rpcFunctions[name] = f
}

func CallRPC(name string, dst interface{}, params ...interface{}) error {
	if message.Conn == nil {
		return fmt.Errorf("RabbitMQ connection is nil, connection not initialized")
	}
	
	ch, err := message.Conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	defer ch.Close()

	corrID, err := message.GenerateRandomString(32)
	if err != nil {
		return fmt.Errorf("failed to generate correlation ID: %w", err)
	}
	
	queueResp, err := ch.QueueDeclare(corrID, false, true, true, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare response queue: %w", err)
	}

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
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	channelPublish, err := message.Conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create publish channel: %w", err)
	}
	defer channelPublish.Close()

	err = channelPublish.Publish("", "rpc_queue", false, false, amqp.Publishing{
		ContentType:   "application/json",
		ReplyTo:       queueResp.Name,
		Body:          jsonRequest,
		Timestamp:     time.Now(),
		CorrelationId: corrID,
	})

	if err != nil {
		return fmt.Errorf("failed to publish RPC request: %w", err)
	}

	ms, err := ch.Consume(queueResp.Name, corrID, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to consume response queue: %w", err)
	}

	for d := range ms {
		if d.CorrelationId == corrID {
			// Found matching correlation ID
			if d.ContentType != "application/json" {
				err = fmt.Errorf(string(d.Body))
				// Don't nack here since we're using auto-ack (true in Consume)
				ch.Cancel(corrID, true)
				ch.QueueDelete(queueResp.Name, true, true, true)
				return err
			}
			err = json.Unmarshal(d.Body, dst)
			// Don't ack here since we're using auto-ack (true in Consume)
			ch.Cancel(corrID, true)
			ch.QueueDelete(queueResp.Name, true, true, true)
			return err
		}
		// Message doesn't match correlation ID, ignore it
		// Don't nack since we're using auto-ack (true in Consume)
	}
	return fmt.Errorf("no response received")
}

func RPCServer() error {
	ch, msgs, err := GetRPCProp()
	if err != nil {
		return fmt.Errorf("failed to get RPC properties: %w", err)
	}
	if ch == nil {
		return fmt.Errorf("channel is nil")
	}
	if msgs == nil {
		return fmt.Errorf("message channel is nil")
	}

	for d := range msgs {
		// Check if channel is still valid before processing
		if isChannelClosed(ch) {
			fmt.Println("Channel closed, breaking message loop")
			break
		}

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
				// Try to publish response, but don't fail if channel is closed
				publishErr := ch.Publish("", d.ReplyTo, false, false, amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte("Timeout"),
				})
				if publishErr != nil {
					fmt.Printf("Error publishing timeout response: %v\n", publishErr)
				}
				// Safely reject the message
				if rejectErr := safeReject(d, false); rejectErr != nil {
					fmt.Printf("Error rejecting timeout message: %v\n", rejectErr)
				}
				continue
			}

			// Safely reject and requeue - we don't have function with that name
			if rejectErr := safeReject(d, true); rejectErr != nil {
				fmt.Printf("Error rejecting unknown function message: %v\n", rejectErr)
			}
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
			// Try to publish error response
			publishErr := ch.Publish("", d.ReplyTo, false, false, amqp.Publishing{
				ContentType:   "text/plain",
				CorrelationId: d.CorrelationId,
				Body:          []byte(err.Error()),
			})
			if publishErr != nil {
				fmt.Printf("Error publishing error response: %v\n", publishErr)
			}

			// Safely reject the message without requeue
			if rejectErr := safeReject(d, false); rejectErr != nil {
				fmt.Printf("Error rejecting error message: %v\n", rejectErr)
			}

			continue
		}

		parseResult, _ := json.Marshal(result)
		//send result
		publishErr := ch.Publish("", d.ReplyTo, false, false, amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: d.CorrelationId,
			Body:          parseResult,
		})
		if publishErr != nil {
			fmt.Printf("Error publishing result: %v\n", publishErr)
			// If publish failed, reject the message to requeue it
			if rejectErr := safeReject(d, true); rejectErr != nil {
				fmt.Printf("Error rejecting after publish failure: %v\n", rejectErr)
			}
			continue
		}

		// Safely acknowledge the message
		if ackErr := safeAck(d); ackErr != nil {
			fmt.Printf("Error acknowledging message: %v\n", ackErr)
		}
	}
	
	return nil
}
