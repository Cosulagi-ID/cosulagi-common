// Package circuitbreaker implements a simple thread-safe circuit breaker pattern.
//
// The circuit breaker has three states:
//   - Closed: Normal operation, requests pass through.
//   - Open: Failure threshold exceeded, requests are rejected immediately.
//   - Half-Open: Probe period after Open; one request is allowed through to test recovery.
//
// Usage:
//
//	cb := circuitbreaker.New("my-rpc", circuitbreaker.Options{
//	    FailureThreshold: 5,
//	    SuccessThreshold: 2,
//	    Timeout:          30 * time.Second,
//	})
//
//	err := cb.Do(func() error {
//	    return doSomething()
//	})
package circuitbreaker

import (
	"fmt"
	"sync"
	"time"
)

// State represents the current state of the circuit breaker.
type State int

const (
	// StateClosed allows all requests through.
	StateClosed State = iota
	// StateOpen rejects all requests immediately.
	StateOpen
	// StateHalfOpen allows a single probe request through.
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ErrCircuitOpen is returned when the circuit breaker is open and rejecting requests.
type ErrCircuitOpen struct {
	Name    string
	Timeout time.Duration
}

func (e *ErrCircuitOpen) Error() string {
	return fmt.Sprintf("circuit breaker %q is open (will retry in ~%.0fs)", e.Name, e.Timeout.Seconds())
}

// Options configures the circuit breaker behavior.
type Options struct {
	// FailureThreshold is the number of consecutive failures that trip the circuit.
	// Defaults to 5.
	FailureThreshold int
	// SuccessThreshold is the number of consecutive successes in HalfOpen state required to close the circuit.
	// Defaults to 2.
	SuccessThreshold int
	// Timeout is how long the circuit stays Open before trying HalfOpen.
	// Defaults to 30 seconds.
	Timeout time.Duration
}

// CircuitBreaker is a thread-safe circuit breaker.
type CircuitBreaker struct {
	name             string
	mu               sync.Mutex
	state            State
	failures         int
	successes        int
	failureThreshold int
	successThreshold int
	timeout          time.Duration
	lastFailureTime  time.Time
}

// New creates a new CircuitBreaker with the given name and options.
func New(name string, opts Options) *CircuitBreaker {
	if opts.FailureThreshold <= 0 {
		opts.FailureThreshold = 5
	}
	if opts.SuccessThreshold <= 0 {
		opts.SuccessThreshold = 2
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	return &CircuitBreaker{
		name:             name,
		state:            StateClosed,
		failureThreshold: opts.FailureThreshold,
		successThreshold: opts.SuccessThreshold,
		timeout:          opts.Timeout,
	}
}

// State returns the current state of the circuit breaker.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.tickState()
	return cb.state
}

// tickState transitions Open → HalfOpen if the timeout has elapsed.
// Must be called with cb.mu held.
func (cb *CircuitBreaker) tickState() {
	if cb.state == StateOpen && time.Since(cb.lastFailureTime) >= cb.timeout {
		fmt.Printf("[CircuitBreaker] %q: open → half-open (timeout elapsed)\n", cb.name)
		cb.state = StateHalfOpen
		cb.successes = 0
	}
}

// Do executes fn if the circuit allows it, and records success/failure accordingly.
// Returns ErrCircuitOpen immediately if the circuit is open.
func (cb *CircuitBreaker) Do(fn func() error) error {
	cb.mu.Lock()
	cb.tickState()

	switch cb.state {
	case StateOpen:
		cb.mu.Unlock()
		return &ErrCircuitOpen{Name: cb.name, Timeout: cb.timeout}

	case StateHalfOpen:
		// Allow exactly one probe through but block concurrent probes
		cb.mu.Unlock()
		err := fn()
		cb.mu.Lock()
		if err != nil {
			// Probe failed — go back to Open
			fmt.Printf("[CircuitBreaker] %q: half-open probe FAILED, reopening\n", cb.name)
			cb.state = StateOpen
			cb.lastFailureTime = time.Now()
			cb.successes = 0
			cb.mu.Unlock()
			return err
		}
		cb.successes++
		if cb.successes >= cb.successThreshold {
			fmt.Printf("[CircuitBreaker] %q: half-open → closed (recovery confirmed)\n", cb.name)
			cb.state = StateClosed
			cb.failures = 0
			cb.successes = 0
		}
		cb.mu.Unlock()
		return nil

	default: // StateClosed
		cb.mu.Unlock()
		err := fn()
		cb.mu.Lock()
		if err != nil {
			cb.failures++
			cb.successes = 0
			if cb.failures >= cb.failureThreshold {
				fmt.Printf("[CircuitBreaker] %q: threshold reached (%d failures), opening circuit\n",
					cb.name, cb.failures)
				cb.state = StateOpen
				cb.lastFailureTime = time.Now()
			}
			cb.mu.Unlock()
			return err
		}
		// Success in closed state resets failure counter
		cb.failures = 0
		cb.mu.Unlock()
		return nil
	}
}

// Reset manually resets the circuit breaker to Closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.failures = 0
	cb.successes = 0
	fmt.Printf("[CircuitBreaker] %q: manually reset to closed\n", cb.name)
}

// Stats returns a snapshot of the circuit breaker's current counters.
func (cb *CircuitBreaker) Stats() map[string]interface{} {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.tickState()
	return map[string]interface{}{
		"name":              cb.name,
		"state":             cb.state.String(),
		"failures":          cb.failures,
		"successes":         cb.successes,
		"failure_threshold": cb.failureThreshold,
		"success_threshold": cb.successThreshold,
	}
}
