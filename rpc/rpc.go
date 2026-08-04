package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/AsidStorm/go-amqp-reconnect/rabbitmq"
	"github.com/Cosulagi-ID/cosulagi-common/circuitbreaker"
	"github.com/Cosulagi-ID/cosulagi-common/message"
	amqp "github.com/rabbitmq/amqp091-go"
)

// rpcQueueName is the shared RPC request queue every service consumes from and
// publishes to. It defaults to "rpc_queue" (backward-compatible) but can be
// overridden per environment via RPC_QUEUE. Dev and prod are already isolated by
// RabbitMQ vhost; this is belt-and-suspenders so a mis-set vhost can't silently
// let dev and prod share one queue and cross-answer each other's RPCs (FINDING-4).
// Server (consumer) and client (publisher) both read this, so the two stay in sync
// within a process, and a per-env RPC_QUEUE keeps environments apart.
func rpcQueueName() string {
	if q := strings.TrimSpace(os.Getenv("RPC_QUEUE")); q != "" {
		return q
	}
	return "rpc_queue"
}

// queueForRPC returns the queue a request for the given RPC function should be
// published to. Function names follow the "<Prefix>_<Func>" convention where
// the prefix identifies the owning service domain (Product_, Store_, User_, ...),
// so requests are routed to "<base>.<prefix>" and only the service that
// registered that prefix consumes them.
//
// Historically every service consumed one shared queue with QoS 1 and rejected
// + requeued messages whose function it didn't know. With ~10 consumers a
// request round-robined through wrong services until it happened to reach the
// right one, adding seconds of latency per call (observed 1-4.5s on dev) and
// getting killed with a "Timeout" reply after 5s of bouncing. Prefix routing
// eliminates the bouncing entirely.
//
// Names without a "_" fall back to the shared base queue.
func queueForRPC(name string) string {
	base := rpcQueueName()
	if i := strings.Index(name, "_"); i > 0 {
		return base + "." + strings.ToLower(name[:i])
	}
	return base
}

// registeredQueues returns the set of queues this service must consume:
// one per distinct function-name prefix it registered, plus the legacy shared
// base queue so publishers running older code (which publish everything to the
// base queue) still get answered during mixed-version rollouts.
func registeredQueues() []string {
	seen := map[string]bool{rpcQueueName(): true}
	queues := []string{rpcQueueName()}
	for name := range rpcFunctions {
		q := queueForRPC(name)
		if !seen[q] {
			seen[q] = true
			queues = append(queues, q)
		}
	}
	return queues
}

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
	// Caller identifies which service sent this request, e.g. "user", "auth",
	// "product" — populated automatically by CallRPC (see callerServiceName),
	// never by hand at the call site.
	//
	// It is OPTIONAL on the wire by design, so a mixed-version rollout across
	// 12 independently-deployed services can never break:
	//   - a NEW consumer decoding a request from an OLD producer: the field
	//     is simply absent from the JSON, Caller decodes to its zero value
	//     "". Every reader of this field must treat "" as "unknown caller",
	//     not as an error.
	//   - an OLD consumer (compiled before this field existed) decoding a
	//     request from a NEW producer: encoding/json silently ignores JSON
	//     keys it doesn't have a struct field for. The old RPCRequest struct
	//     decodes fine and the extra "caller" key is dropped on the floor.
	// `omitempty` also keeps old-producer traffic byte-for-byte the same as
	// today when Caller is unset, which it never should be for services built
	// against this change, but keeps behavior boring if it ever is.
	Caller string `json:"caller,omitempty"`
}

// callerName is resolved once per process and reused for every outgoing
// RPCRequest.
var (
	callerNameOnce sync.Once
	callerName     string
)

// callerServiceName identifies the calling service without any new
// configuration. Every Cosulagi service's go.mod module path already follows
// "github.com/Cosulagi-ID/cosulagi-<service>" (cosulagi-user, cosulagi-auth,
// cosulagi-product, ...), and Go's runtime/debug.ReadBuildInfo() exposes that
// path at runtime for free, straight from the compiled binary — no service
// has to set an env var or a config key for this to work.
//
// If build info isn't available (e.g. run via `go run`, or a build mode that
// strips it) or the module doesn't follow the "cosulagi-<service>" naming
// convention, this returns "", which CallRPC then sends as an absent Caller —
// exactly the same wire shape as an old producer, so nothing breaks.
func callerServiceName() string {
	callerNameOnce.Do(func() {
		bi, ok := debug.ReadBuildInfo()
		if !ok || bi.Main.Path == "" {
			return
		}
		const orgPrefix = "github.com/Cosulagi-ID/cosulagi-"
		if strings.HasPrefix(bi.Main.Path, orgPrefix) {
			callerName = strings.TrimPrefix(bi.Main.Path, orgPrefix)
			return
		}
		callerName = bi.Main.Path
	})
	return callerName
}

func GetRPCProp(queueName string) (*rabbitmq.Channel, <-chan amqp.Delivery, error) {
	// Dedicated channel per consumer loop: QoS is per-channel, and sharing the
	// global channel across multiple queue consumers would serialize them.
	if !message.IsConnected() {
		return nil, nil, fmt.Errorf("RabbitMQ connection is not available")
	}
	ch, err := message.Conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get channel: %w", err)
	}
	if ch == nil {
		return nil, nil, fmt.Errorf("channel is nil")
	}

	q, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
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
		Caller:     callerServiceName(),
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

	// Route to the owning service's queue (see queueForRPC). Declare it so the
	// request isn't dropped when the target service hasn't started (yet) —
	// it will be picked up as soon as the service consumes, or answered with
	// "Timeout" by the server-side staleness check if it sat too long.
	targetQueue := queueForRPC(name)
	if _, err := channelPublish.QueueDeclare(targetQueue, false, false, false, false, nil); err != nil {
		return fmt.Errorf("failed to declare target queue: %w", err)
	}

	err = channelPublish.Publish("", targetQueue, false, false, rabbitmq.Publishing{
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
			// Fire-and-forget calls pass dst=nil (they only want the side effect and
			// the success/BusinessError signal, not the payload). json.Unmarshal into a
			// nil dst always fails with "json: Unmarshal(nil)", which callers that DO
			// check the error (e.g. addRentalCalendarBlock) misread as an RPC failure —
			// even though the remote side executed successfully. Treat a JSON response
			// with no destination as success (FINDING-10).
			if dst == nil {
				return nil
			}
			return json.Unmarshal(d.Body, dst)
		}
	}
}

// RPCServer consumes every queue this service is responsible for: its
// prefix-routed queues (derived from the registered function names) plus the
// legacy shared queue for backward compatibility with older publishers.
// Call it AFTER all RegisterRPCFunction calls. Blocks forever.
func RPCServer() error {
	queues := registeredQueues()
	for _, q := range queues[1:] {
		go serveQueue(q)
	}
	serveQueue(queues[0])
	return nil
}

// callHandlerSafely invokes an RPC handler and turns a panic into an ordinary
// error instead of letting it unwind.
//
// Handlers index their arguments positionally (params[0], params[1], ...) and
// type-assert them, so a request carrying too few parameters — or a parameter of
// the wrong type — panics. Before this guard that panic escaped serveQueue and
// terminated the whole process: a single malformed message on the bus was enough
// to kill a service, and for `auth` that means nobody on the platform can log in.
//
// The caller still gets an error response and the message is rejected WITHOUT
// requeue by the existing error branch, so a poison message cannot loop forever.
// The returned text is deliberately generic; the panic value and stack go to the
// service log, not back over the bus.
// seenCallerFunctionPairs tracks which (caller, function) edges have already
// been logged, so logCallerOnce prints one line per distinct edge instead of
// one line per RPC call. RPC volume is orders of magnitude higher than the
// number of distinct edges, so logging every call would drown the logs.
var seenCallerFunctionPairs sync.Map

// logCallerOnce is step 3 of the RPC caller-identity work: pure observation,
// no enforcement. It logs the first time a given caller is seen invoking a
// given function, so that over time the logs accumulate a real map of who
// calls what across the 12 services — a map that does not exist today and
// that any future enforcement (see RestrictCallers) needs before it can be
// turned on safely.
//
// A missing caller (old producer, still valid mid-rollout) is logged as
// "unknown" rather than skipped, so the rollout gap itself is visible in the
// logs instead of hidden.
func logCallerOnce(function, caller string) {
	if caller == "" {
		caller = "unknown"
	}
	key := caller + "->" + function
	if _, alreadySeen := seenCallerFunctionPairs.LoadOrStore(key, struct{}{}); alreadySeen {
		return
	}
	fmt.Printf("[RPC] first-seen caller=%q function=%q\n", caller, function)
}

// callerAllowlists maps an RPC function name to the set of caller services
// permitted to invoke it. Empty (the default, and the state this ships in)
// means no restriction on that function. Populated only via RestrictCallers,
// which no handler in any service calls yet.
var (
	callerAllowlists   = make(map[string]map[string]bool)
	callerAllowlistsMu sync.RWMutex
)

// RestrictCallers is the enforcement primitive for step 4: it is provided but
// left OFF, because nobody currently knows the real caller map (that's what
// step 3's logging builds over time) and turning restriction on blind would
// risk breaking a legitimate caller nobody remembered.
//
// It returns fn unchanged, so it drops straight into the existing
// registration call:
//
//	rpc.RegisterRPCFunction("User_AdminDeleteAccount",
//	    rpc.RestrictCallers("User_AdminDeleteAccount", []string{"auth"}, controller.AdminDeleteAccount))
//
// The restriction is enforced by serveQueue at dispatch time, BEFORE the
// handler runs, by looking up rpcRequest.Name in callerAllowlists — the
// handler's own func(params ...interface{}) (interface{}, error) signature
// never has to change, so wrapping a handler with this does not touch any of
// the twelve services' existing RPC functions.
//
// A request with no Caller (old producer, or any request sent before a
// producer picks up this change) is ALWAYS allowed regardless of the
// allowlist: a partial rollout, where some producers haven't been rebuilt
// against this yet, must never start rejecting calls that used to work.
//
// Not called by any handler in this change, so callerAllowlists stays empty
// and every RPC function remains unrestricted exactly as it is today.
func RestrictCallers(name string, allowed []string, fn func(params ...interface{}) (interface{}, error)) func(params ...interface{}) (interface{}, error) {
	set := make(map[string]bool, len(allowed))
	for _, c := range allowed {
		set[c] = true
	}
	callerAllowlistsMu.Lock()
	callerAllowlists[name] = set
	callerAllowlistsMu.Unlock()
	return fn
}

// callerAllowed reports whether caller may invoke the RPC function name. No
// allowlist registered for name (the default) means unrestricted. A missing
// caller is always allowed, see RestrictCallers.
func callerAllowed(name, caller string) bool {
	if caller == "" {
		return true
	}
	callerAllowlistsMu.RLock()
	set, restricted := callerAllowlists[name]
	callerAllowlistsMu.RUnlock()
	if !restricted {
		return true
	}
	return set[caller]
}

func callHandlerSafely(
	name string,
	f func(params ...interface{}) (interface{}, error),
	params []interface{},
) (result interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("PANIC in RPC handler %q: %v\n%s\n", name, r, debug.Stack())
			result = nil
			err = fmt.Errorf("internal error handling %q", name)
		}
	}()
	return f(params...)
}

func serveQueue(queueName string) {
	for {
		ch, msgs, err := GetRPCProp(queueName)
		if err != nil {
			fmt.Printf("RPC setup failed for queue %q, retrying in 5s: %v\n", queueName, err)
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

		fmt.Printf("RPC Server started and waiting for messages on %q...\n", queueName)

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

			// Observation only (step 3): build the real caller map before any
			// enforcement is ever turned on. Does not affect dispatch.
			logCallerOnce(rpcRequest.Name, rpcRequest.Caller)

			// Enforcement primitive (step 4), OFF today: callerAllowlists is
			// only ever populated by RestrictCallers, which nothing calls yet,
			// so this is always true and this branch never fires.
			if !callerAllowed(rpcRequest.Name, rpcRequest.Caller) {
				publishErr := ch.Publish("", d.ReplyTo, false, false, rabbitmq.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(fmt.Sprintf("caller %q is not allowed to call %q", rpcRequest.Caller, rpcRequest.Name)),
				})
				if publishErr != nil {
					fmt.Printf("Error publishing caller-restricted response: %v\n", publishErr)
				}
				if rejectErr := safeReject(d, false); rejectErr != nil {
					fmt.Printf("Error rejecting caller-restricted message: %v\n", rejectErr)
				}
				continue
			}

			//get parameters
			params := make([]interface{}, 0)
			for _, param := range rpcRequest.Parameters {
				params = append(params, param.Value)
			}

			//call function
			result, err := callHandlerSafely(rpcRequest.Name, f, params)

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

		ch.Close()
		fmt.Printf("RPC Server message loop for %q ended, reconnecting in 2s...\n", queueName)
		time.Sleep(2 * time.Second)
	}
}
