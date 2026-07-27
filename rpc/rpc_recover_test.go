package rpc

import (
	"errors"
	"strings"
	"testing"
)

// A panicking handler must come back as an error, not unwind and kill the
// process. The realistic trigger is a handler indexing params positionally when
// the caller sent fewer parameters than it expects.
func TestCallHandlerSafely_RecoversIndexOutOfRange(t *testing.T) {
	handler := func(params ...interface{}) (interface{}, error) {
		return params[2], nil // panics: only one param is supplied below
	}

	result, err := callHandlerSafely("Upload_Whatever", handler, []interface{}{"only-one"})

	if err == nil {
		t.Fatal("expected an error from a panicking handler, got nil")
	}
	if result != nil {
		t.Fatalf("expected nil result on panic, got %#v", result)
	}
	if !strings.Contains(err.Error(), "Upload_Whatever") {
		t.Fatalf("error should name the handler so logs are searchable, got %q", err)
	}
	// The panic value itself must not travel back over the bus.
	if strings.Contains(err.Error(), "index out of range") {
		t.Fatalf("panic detail leaked into the RPC response: %q", err)
	}
}

func TestCallHandlerSafely_RecoversNilMap(t *testing.T) {
	handler := func(params ...interface{}) (interface{}, error) {
		var m map[string]string
		m["boom"] = "x" // panics: assignment to entry in nil map
		return nil, nil
	}

	if _, err := callHandlerSafely("User_Whatever", handler, nil); err == nil {
		t.Fatal("expected an error from a panicking handler, got nil")
	}
}

// The guard must stay transparent for the normal path: real results and real
// errors have to pass through untouched.
func TestCallHandlerSafely_PassesThroughResultAndError(t *testing.T) {
	ok := func(params ...interface{}) (interface{}, error) {
		return "value", nil
	}
	result, err := callHandlerSafely("Ok", ok, []interface{}{1, 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "value" {
		t.Fatalf("result mangled: %#v", result)
	}

	sentinel := errors.New("business rule violated")
	failing := func(params ...interface{}) (interface{}, error) {
		return nil, sentinel
	}
	if _, err := callHandlerSafely("Failing", failing, nil); !errors.Is(err, sentinel) {
		t.Fatalf("handler error should pass through unchanged, got %v", err)
	}
}
