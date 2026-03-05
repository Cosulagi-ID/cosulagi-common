package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/AsidStorm/go-amqp-reconnect/rabbitmq"
	"github.com/Cosulagi-ID/cosulagi-common/circuitbreaker"
	"github.com/Cosulagi-ID/cosulagi-common/message"
	amqp "github.com/rabbitmq/amqp091-go"
)

// rpcCircuitBreaker is the global circuit breaker for all RPC calls.
// Opens after 5 consecutive connection failures and attempts recovery after 30s.
var rpcCircuitBreaker = circuitbreaker.New("rpc", circuitbreaker.Options{
	FailureThreshold: 5,
	SuccessThreshold: 2,
	Timeout:          30 * time.Second,
})

var rpcFunctions = make(map[string]func(params ...interface{}) (interface{}, error))

// isChannelClosed checks if the channel is closed or invalid
func isChannelClosed(ch *rabbitmq.Channel) bool {
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

func GetRPCProp() (*rabbitmq.Channel, <-chan amqp.Delivery, error) {
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

// BusinessError represents an error returned by the remote RPC function (e.g., "record not found")
// instead of a system-level failure (e.g., connection lost, timeout).
type BusinessError struct {
	Message string
}

func (e *BusinessError) Error() string {
	return e.Message
}

// CallRPC calls a registered RPC function by name with optional parameters.
// It uses a circuit breaker to fail-fast when the broker is repeatedly unavailable,
// retries up to 3 times with exponential backoff on connection errors,
// and enforces a 10-second timeout per attempt to prevent indefinite hangs.
func CallRPC(name string, dst interface{}, params ...interface{}) error {
	const (
		maxRetries   = 3
		rpcTimeout   = 10 * time.Second
		initialDelay = 500 * time.Millisecond
	)

	start := time.Now()

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			delay := initialDelay * time.Duration(1<<uint(attempt-1)) // 500ms, 1s, 2s
			time.Sleep(delay)
		}

		// Wrap each attempt with the circuit breaker.
		// If CB is open, this returns ErrCircuitOpen immediately.
		var businessErr error
		attemptErr := rpcCircuitBreaker.Do(func() error {
			err := callRPCOnce(name, dst, rpcTimeout, params...)
			if err != nil {
				// If it's a BusinessError, we capture it and return nil to CB
				// so it doesn't count as a system failure.
				if bErr, ok := err.(*BusinessError); ok {
					businessErr = bErr
					return nil
				}
			}
			return err
		})

		// If we captured a business error, return it immediately
		if businessErr != nil {
			return businessErr
		}

		if attemptErr == nil {
			if attempt > 0 {
				fmt.Printf("[RPC] %q succeeded on attempt %d/%d (%.0fms)\n",
					name, attempt+1, maxRetries, float64(time.Since(start).Milliseconds()))
			}
			return nil
		}

		lastErr = attemptErr

		// If circuit breaker is open, don't retry — fail fast
		if _, isOpen := attemptErr.(*circuitbreaker.ErrCircuitOpen); isOpen {
			fmt.Printf("[RPC] %q rejected by circuit breaker (open state)\n", name)
			return lastErr
		}

		// Only retry on connection errors, not business logic errors
		if !message.IsConnectionError(lastErr) {
			return lastErr
		}
		fmt.Printf("[RPC] attempt %d/%d failed for %q (connection error, %.0fms): %v\n",
			attempt+1, maxRetries, name, float64(time.Since(start).Milliseconds()), lastErr)
	}

	fmt.Printf("[RPC] %q exhausted all %d retries (%.0fms total): %v\n",
		name, maxRetries, float64(time.Since(start).Milliseconds()), lastErr)
	return fmt.Errorf("RPC %q failed after %d attempts: %w", name, maxRetries, lastErr)
}

// GetCircuitBreakerStats returns the current stats of the global RPC circuit breaker.
// Useful for health check endpoints and monitoring dashboards.
func GetCircuitBreakerStats() map[string]interface{} {
	return rpcCircuitBreaker.Stats()
}

// ResetCircuitBreaker manually resets the circuit breaker to closed state.
// Use this for emergency recovery or admin-triggered resets.
func ResetCircuitBreaker() {
	rpcCircuitBreaker.Reset()
}

// callRPCOnce performs a single RPC call with the given timeout.
func callRPCOnce(name string, dst interface{}, timeout time.Duration, params ...interface{}) error {
	if !message.IsConnected() {
		return fmt.Errorf("RabbitMQ connection is not available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

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

	err = channelPublish.Publish("", "rpc_queue", false, false, rabbitmq.Publishing{
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

	// Wait for response or timeout
	for {
		select {
		case <-ctx.Done():
			ch.Cancel(corrID, true)
			ch.QueueDelete(queueResp.Name, true, true, true)
			return fmt.Errorf("RPC %q timed out after %s", name, timeout)
		case d, ok := <-ms:
			if !ok {
				return fmt.Errorf("RPC response channel closed")
			}
			if d.CorrelationId != corrID {
				continue
			}
			ch.Cancel(corrID, true)
			ch.QueueDelete(queueResp.Name, true, true, true)
			if d.ContentType != "application/json" {
				return &BusinessError{Message: string(d.Body)}
			}
			return json.Unmarshal(d.Body, dst)
		}
	}
}

func RPCServer() error {
	for {
		ch, msgs, err := GetRPCProp()
		if err != nil {
			fmt.Printf("RPC setup failed, retrying in 5s: %v\n", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if ch == nil {
			fmt.Println("channel is nil, retrying in 5s")
			time.Sleep(5 * time.Second)
			continue
		}
		if msgs == nil {
			fmt.Println("message channel is nil, retrying in 5s")
			time.Sleep(5 * time.Second)
			continue
		}

		fmt.Println("RPC Server started and waiting for messages...")

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

				if time.Since(d.Timestamp) > *timeout {
					// Try to publish response, but don't fail if channel is closed
					publishErr := ch.Publish("", d.ReplyTo, false, false, rabbitmq.Publishing{
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
				publishErr := ch.Publish("", d.ReplyTo, false, false, rabbitmq.Publishing{
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
			publishErr := ch.Publish("", d.ReplyTo, false, false, rabbitmq.Publishing{
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

		fmt.Println("RPC Server message loop ended, reconnecting in 2s...")
		time.Sleep(2 * time.Second)
	}
}
