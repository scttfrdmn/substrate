package emulator_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// newOrganizationsServerForSeeds returns a server and the plugin sharing its
// state, so a test can post a seed over HTTP and then read what the plugin
// resolves from it.
func newOrganizationsServerForSeeds(t *testing.T) (*emulator.Server, *emulator.OrganizationsPlugin) {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false})
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.OrganizationsPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize organizations plugin: %v", err)
	}
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger)
	return srv, p
}

// orgSeedRequest posts or deletes a control-plane seed and returns the status.
func orgSeedRequest(t *testing.T, srv *emulator.Server, method, path string, body map[string]any) int {
	t.Helper()
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal seed: %v", err)
		}
		reader = bytes.NewReader(b)
	}
	r := httptest.NewRequest(method, path, reader)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result().StatusCode //nolint:bodyclose // httptest.Recorder result needs no close.
}

// TestOrganizations_SeedFeatureSet covers the round trip through the endpoint:
// seeding CONSOLIDATED_BILLING makes the organization one in which no service
// control policy can exist, and clearing restores the stored value.
func TestOrganizations_SeedFeatureSet(t *testing.T) {
	srv, p := newOrganizationsServerForSeeds(t)

	if got := orgSeedRequest(t, srv, http.MethodPost, "/v1/organizations/feature-set",
		map[string]any{"featureSet": "CONSOLIDATED_BILLING"}); got != http.StatusOK {
		t.Fatalf("seed feature set: expected 200, got %d", got)
	}
	featureSet, err := p.EffectiveFeatureSetForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("effective feature set: %v", err)
	}
	if featureSet != "CONSOLIDATED_BILLING" {
		t.Errorf("expected CONSOLIDATED_BILLING, got %q", featureSet)
	}

	if got := orgSeedRequest(t, srv, http.MethodDelete, "/v1/organizations/feature-set", nil); got != http.StatusOK {
		t.Fatalf("clear feature set: expected 200, got %d", got)
	}
	featureSet, err = p.EffectiveFeatureSetForTest(t.Context(), orgTestAccount)
	if err != nil {
		t.Fatalf("effective feature set after clear: %v", err)
	}
	if featureSet != "ALL" {
		t.Errorf("expected ALL after clearing the seed, got %q", featureSet)
	}
}

// TestOrganizations_SeedFeatureSet_Rejects checks a value outside the enum is
// refused rather than stored: a seeded "all" would leave the organization in a
// mode no branch handles, and every assertion downstream would be meaningless.
func TestOrganizations_SeedFeatureSet_Rejects(t *testing.T) {
	srv, _ := newOrganizationsServerForSeeds(t)

	for _, body := range []map[string]any{
		{"featureSet": "all"},
		{"featureSet": "BILLING"},
		{},
	} {
		if got := orgSeedRequest(t, srv, http.MethodPost, "/v1/organizations/feature-set", body); got != http.StatusBadRequest {
			t.Errorf("seed %v: expected 400, got %d", body, got)
		}
	}
}

// TestOrganizations_SeedCreateAccountFailure covers exact-name and wildcard
// resolution, and the clear paths.
func TestOrganizations_SeedCreateAccountFailure(t *testing.T) {
	srv, p := newOrganizationsServerForSeeds(t)
	ctx := t.Context()

	if _, seeded, err := p.ResolveSeededCreateFailureForTest(ctx, "dev"); err != nil || seeded {
		t.Fatalf("expected no seed to apply initially (seeded=%v, err=%v)", seeded, err)
	}

	if got := orgSeedRequest(t, srv, http.MethodPost, "/v1/organizations/create-account-failure",
		map[string]any{"accountName": "dev", "failureReason": "EMAIL_ALREADY_EXISTS"}); got != http.StatusOK {
		t.Fatalf("seed named failure: expected 200, got %d", got)
	}
	reason, seeded, err := p.ResolveSeededCreateFailureForTest(ctx, "dev")
	if err != nil || !seeded || reason != "EMAIL_ALREADY_EXISTS" {
		t.Fatalf("expected the named seed to apply, got (%q, %v, %v)", reason, seeded, err)
	}
	// A different name does not match a name-scoped seed.
	if _, seeded, err = p.ResolveSeededCreateFailureForTest(ctx, "prod"); err != nil || seeded {
		t.Errorf("expected the named seed not to apply to another name (seeded=%v, err=%v)", seeded, err)
	}

	// The wildcard applies to any name.
	if got := orgSeedRequest(t, srv, http.MethodPost, "/v1/organizations/create-account-failure",
		map[string]any{"accountName": "*", "failureReason": "INTERNAL_FAILURE"}); got != http.StatusOK {
		t.Fatalf("seed wildcard failure: expected 200, got %d", got)
	}
	reason, seeded, err = p.ResolveSeededCreateFailureForTest(ctx, "prod")
	if err != nil || !seeded || reason != "INTERNAL_FAILURE" {
		t.Fatalf("expected the wildcard seed to apply, got (%q, %v, %v)", reason, seeded, err)
	}
	// The exact name still wins over the wildcard.
	reason, _, err = p.ResolveSeededCreateFailureForTest(ctx, "dev")
	if err != nil || reason != "EMAIL_ALREADY_EXISTS" {
		t.Errorf("expected the exact name to win over the wildcard, got %q (err %v)", reason, err)
	}

	// A scoped delete removes only that seed.
	if got := orgSeedRequest(t, srv, http.MethodDelete,
		"/v1/organizations/create-account-failure?accountName=dev", nil); got != http.StatusOK {
		t.Fatalf("clear named seed: expected 200, got %d", got)
	}
	reason, _, err = p.ResolveSeededCreateFailureForTest(ctx, "dev")
	if err != nil || reason != "INTERNAL_FAILURE" {
		t.Errorf("expected the wildcard to remain, got %q (err %v)", reason, err)
	}

	// An unscoped delete removes everything.
	if got := orgSeedRequest(t, srv, http.MethodDelete,
		"/v1/organizations/create-account-failure", nil); got != http.StatusOK {
		t.Fatalf("clear all seeds: expected 200, got %d", got)
	}
	if _, seeded, err = p.ResolveSeededCreateFailureForTest(ctx, "dev"); err != nil || seeded {
		t.Errorf("expected every seed cleared (seeded=%v, err=%v)", seeded, err)
	}
}

// TestOrganizations_SeedCreateAccountFailure_RejectsUnknownReason is the reason
// the endpoint validates at all: a typo'd reason would seed a FAILED status
// carrying a value no SDK catch branch matches, so the caller's fallback path
// would go untested while the test still passed.
func TestOrganizations_SeedCreateAccountFailure_RejectsUnknownReason(t *testing.T) {
	srv, _ := newOrganizationsServerForSeeds(t)

	for _, body := range []map[string]any{
		{"accountName": "*", "failureReason": "EMAIL_EXISTS"},
		{"accountName": "*", "failureReason": "email_already_exists"},
		{"accountName": "*"},
	} {
		if got := orgSeedRequest(t, srv, http.MethodPost,
			"/v1/organizations/create-account-failure", body); got != http.StatusBadRequest {
			t.Errorf("seed %v: expected 400, got %d", body, got)
		}
	}
}
