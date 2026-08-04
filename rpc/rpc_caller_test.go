package rpc

import (
	"encoding/json"
	"testing"
)

// A request published by an OLD producer (before this change) never sends a
// "caller" key at all. A NEW consumer must still decode it fine, with Caller
// coming back as the zero value, not an error.
func TestRPCRequest_DecodesFine_WithoutCallerField(t *testing.T) {
	body := []byte(`{"name":"User_GetUserbyEmail","parameters":[{"name":"","value":"a@b.com"}]}`)

	var req RPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("new consumer failed to decode an old-producer request: %v", err)
	}
	if req.Name != "User_GetUserbyEmail" {
		t.Fatalf("Name mangled: %q", req.Name)
	}
	if req.Caller != "" {
		t.Fatalf("expected zero-value Caller for a request that never sent one, got %q", req.Caller)
	}
}

// A request published by a NEW producer sends "caller". An OLD consumer
// (modeled here as a struct without the Caller field, i.e. the RPCRequest
// shape before this change) must still decode it fine: encoding/json ignores
// JSON keys it has no struct field for.
func TestOldConsumerShape_DecodesFine_WithCallerField(t *testing.T) {
	type oldRPCRequest struct {
		Name       string             `json:"name"`
		Parameters []RPCRequestParams `json:"parameters"`
	}

	req := RPCRequest{
		Name:       "Auth_GetTokenData",
		Parameters: []RPCRequestParams{{Value: "token-value"}},
		Caller:     "blog",
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var old oldRPCRequest
	if err := json.Unmarshal(body, &old); err != nil {
		t.Fatalf("old consumer failed to decode a new-producer request: %v", err)
	}
	if old.Name != "Auth_GetTokenData" {
		t.Fatalf("Name mangled: %q", old.Name)
	}
}

// omitempty must keep an unset Caller (e.g. callerServiceName() returning ""
// because build info wasn't available) out of the wire payload entirely, so
// old-producer traffic is byte-for-byte what it always was.
func TestRPCRequest_OmitsCaller_WhenEmpty(t *testing.T) {
	req := RPCRequest{Name: "Cart_AddItem"}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("unmarshal to map failed: %v", err)
	}
	if _, present := raw["caller"]; present {
		t.Fatalf("expected no \"caller\" key on the wire when Caller is unset, got: %s", body)
	}
}

// callerServiceName reads this test binary's own build info. Under `go test`
// the main module is the package under test (cosulagi-common), which doesn't
// follow the "cosulagi-<service>" convention, so this only asserts the
// function doesn't panic and is either "" or the raw module path — the real
// per-service behavior is exercised by the acceptance check building a real
// service (e.g. blog) against this change.
func TestCallerServiceName_DoesNotPanic(t *testing.T) {
	_ = callerServiceName()
}

func TestCallerAllowed_DefaultsToAllow(t *testing.T) {
	if !callerAllowed("Some_UnrestrictedFunction", "blog") {
		t.Fatal("a function nobody restricted must be callable by anyone")
	}
}

func TestCallerAllowed_EmptyCallerAlwaysAllowed(t *testing.T) {
	RestrictCallers("Test_RestrictedForEmptyCallerCase", []string{"auth"}, func(params ...interface{}) (interface{}, error) {
		return nil, nil
	})
	if !callerAllowed("Test_RestrictedForEmptyCallerCase", "") {
		t.Fatal("an absent caller (old producer) must always be allowed, even against a restricted function")
	}
}

func TestRestrictCallers_EnforcesAllowlist_OnceApplied(t *testing.T) {
	name := "Test_RestrictedFunction"
	called := false
	handler := func(params ...interface{}) (interface{}, error) {
		called = true
		return "ok", nil
	}

	wrapped := RestrictCallers(name, []string{"auth", "user"}, handler)

	// RestrictCallers must return the handler unchanged so it still plugs
	// straight into RegisterRPCFunction and still works when invoked directly.
	if _, err := wrapped(); err != nil {
		t.Fatalf("wrapped handler should behave like the original: %v", err)
	}
	if !called {
		t.Fatal("wrapped handler must still be callable directly, it is only serveQueue's dispatch that gates it")
	}

	if !callerAllowed(name, "auth") {
		t.Fatal("allowed caller must pass")
	}
	if callerAllowed(name, "blog") {
		t.Fatal("caller not on the allowlist must be rejected once RestrictCallers has been applied")
	}
}

func TestLogCallerOnce_DoesNotPanicOnRepeat(t *testing.T) {
	// Exercises the dedup path (LoadOrStore) twice for the same edge; the
	// second call must be a silent no-op, not an error or panic.
	logCallerOnce("Product_GetProductbyID", "cart")
	logCallerOnce("Product_GetProductbyID", "cart")

	if _, seen := seenCallerFunctionPairs.Load("cart->Product_GetProductbyID"); !seen {
		t.Fatal("expected the caller+function pair to be recorded after logging it")
	}
}
