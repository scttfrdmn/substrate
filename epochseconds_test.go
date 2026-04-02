package substrate_test

import (
	"encoding/json"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// TestEpochSeconds_MarshalUnmarshal verifies the full round-trip and helper methods.
func TestEpochSeconds_MarshalUnmarshal(t *testing.T) {
	now := time.Unix(1700000000, 500000000).UTC() // 2023-11-14T22:13:20.5Z
	e := substrate.EpochSeconds(now)

	// IsZero on non-zero value.
	if e.IsZero() {
		t.Error("IsZero() should be false for non-zero time")
	}

	// Time() round-trip.
	if !e.Time().Equal(now) {
		t.Errorf("Time() = %v, want %v", e.Time(), now)
	}

	// MarshalJSON produces float64.
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	// UnmarshalJSON from float64.
	var e2 substrate.EpochSeconds
	if err := json.Unmarshal(b, &e2); err != nil {
		t.Fatalf("UnmarshalJSON float64: %v", err)
	}
	if !e2.Time().Equal(e.Time()) {
		t.Errorf("round-trip: got %v, want %v", e2.Time(), e.Time())
	}

	// IsZero on zero value.
	var zero substrate.EpochSeconds
	if !zero.IsZero() {
		t.Error("IsZero() should be true for zero value")
	}

	// MarshalJSON for zero → null.
	nb, err := json.Marshal(zero)
	if err != nil {
		t.Fatalf("MarshalJSON zero: %v", err)
	}
	if string(nb) != "null" {
		t.Errorf("MarshalJSON zero = %s, want null", nb)
	}

	// UnmarshalJSON null → zero.
	var e3 substrate.EpochSeconds
	if err := json.Unmarshal([]byte("null"), &e3); err != nil {
		t.Fatalf("UnmarshalJSON null: %v", err)
	}
	if !e3.IsZero() {
		t.Error("UnmarshalJSON(null) should yield zero time")
	}

	// UnmarshalJSON RFC3339 string (legacy format).
	rfc := `"2023-11-14T22:13:20Z"`
	var e4 substrate.EpochSeconds
	if err := json.Unmarshal([]byte(rfc), &e4); err != nil {
		t.Fatalf("UnmarshalJSON RFC3339: %v", err)
	}
	want := time.Date(2023, 11, 14, 22, 13, 20, 0, time.UTC)
	if !e4.Time().Equal(want) {
		t.Errorf("RFC3339 parse: got %v, want %v", e4.Time(), want)
	}

	// UnmarshalJSON invalid → error.
	var e5 substrate.EpochSeconds
	if err := json.Unmarshal([]byte(`"not-a-date"`), &e5); err == nil {
		t.Error("expected error for invalid RFC3339 string")
	}

	// UnmarshalJSON unrecognized token → error.
	var e6 substrate.EpochSeconds
	if err := json.Unmarshal([]byte(`{}`), &e6); err == nil {
		t.Error("expected error for object token")
	}
}
