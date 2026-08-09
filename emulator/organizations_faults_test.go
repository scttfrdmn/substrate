package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// errOrgState fails one kind of state operation for keys under a prefix once
// armed, leaving everything else working. Arming is deferred because setting the
// organization up touches the same keys: a store that fails from construction
// never gets an organization to read.
type errOrgState struct {
	inner    emulator.StateManager
	prefix   string
	err      error
	armed    bool
	onGet    bool
	onPut    bool
	onDelete bool
	onList   bool
}

func (m *errOrgState) fails(key string, op bool) bool {
	return m.armed && op && strings.HasPrefix(key, m.prefix)
}

func (m *errOrgState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.fails(key, m.onGet) {
		return nil, m.err
	}
	return m.inner.Get(ctx, namespace, key)
}

func (m *errOrgState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if m.fails(key, m.onPut) {
		return m.err
	}
	return m.inner.Put(ctx, namespace, key, value)
}

func (m *errOrgState) Delete(ctx context.Context, namespace, key string) error {
	if m.fails(key, m.onDelete) {
		return m.err
	}
	return m.inner.Delete(ctx, namespace, key)
}

func (m *errOrgState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if m.fails(prefix, m.onList) {
		return nil, m.err
	}
	return m.inner.List(ctx, namespace, prefix)
}

// corruptOrgState returns a value that is not valid JSON for keys under a prefix,
// which is how a record written by a different schema reads back. A consistent
// store cannot produce it, so it is the only way to reach the unmarshal branch
// that separates "the record is unreadable" from "there is no record".
type corruptOrgState struct {
	emulator.StateManager
	prefix string
	armed  bool
}

func (m *corruptOrgState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.armed && strings.HasPrefix(key, m.prefix) {
		return []byte("{not json"), nil
	}
	return m.StateManager.Get(ctx, namespace, key)
}

// newOrgFaultFixture returns a plugin over the supplied state manager.
func newOrgFaultFixture(t *testing.T, state emulator.StateManager) *emulator.OrganizationsPlugin {
	t.Helper()
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	return emulator.NewOrganizationsPluginForTest(state, tc)
}

// TestOrganizations_StateReadFailurePropagates asserts a store read failure is
// reported as a failure rather than as an absent entity. The two are opposite
// signals to a caller: "no such organization" is terminal and it should stop,
// while a store failure is transient and it should retry. Collapsing the latter
// into the former would send a consumer down a permanent-failure path over a
// blip, and would also make an auto-created organization appear to spring into
// existence a second time with a different ID.
func TestOrganizations_StateReadFailurePropagates(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{inner: inner, prefix: "org:", err: errors.New("store unavailable"), onGet: true}
	p := newOrgFaultFixture(t, state)
	ctx := t.Context()

	if _, err := p.EnsureOrganizationForTest(ctx, orgTestAccount); err != nil {
		t.Fatalf("setup: %v", err)
	}
	state.armed = true

	if _, err := p.EnsureOrganizationForTest(ctx, orgTestAccount); err == nil {
		t.Error("expected the read failure to propagate, got an organization")
	}
}

// TestOrganizations_RootReadFailurePropagates covers the same boundary for the
// root. A swallowed failure here is worse than elsewhere: loadRoot returning no
// root would have the caller mint a new one, which is exactly the unstable
// identity #577 was about.
func TestOrganizations_RootReadFailurePropagates(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{inner: inner, prefix: "root:", err: errors.New("store unavailable"), onGet: true}
	p := newOrgFaultFixture(t, state)
	ctx := t.Context()

	if _, err := p.LoadRootForTest(ctx, orgTestAccount); err != nil {
		t.Fatalf("setup: %v", err)
	}
	state.armed = true

	if _, err := p.LoadRootForTest(ctx, orgTestAccount); err == nil {
		t.Error("expected the root read failure to propagate")
	}
	if _, err := p.LoadStoredRootForTest(ctx, orgTestAccount); err == nil {
		t.Error("expected the stored-root read failure to propagate")
	}
	if _, err := p.SCPEnabledForTest(ctx, orgTestAccount); err == nil {
		t.Error("expected scpEnabled to report the read failure rather than 'not enabled'")
	}
}

// TestOrganizations_MissingRootIsAnError pins what happens when the organization
// record exists but its root does not. That pairing is unreachable through the
// API — ensureOrganization writes both — so it means the store lost a record,
// and answering with a freshly minted root would hand back an identity nothing
// else agrees with.
func TestOrganizations_MissingRootIsAnError(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	p := newOrgFaultFixture(t, state)
	ctx := t.Context()

	if _, err := p.EnsureOrganizationForTest(ctx, orgTestAccount); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := state.Delete(ctx, "organizations", "root:"+orgTestAccount); err != nil {
		t.Fatalf("delete root: %v", err)
	}

	if _, err := p.LoadRootForTest(ctx, orgTestAccount); err == nil {
		t.Error("expected an error for an organization with no root")
	}
	if _, err := p.LoadStoredRootForTest(ctx, orgTestAccount); err == nil {
		t.Error("expected an error from loadStoredRoot for an organization with no root")
	}
}

// TestOrganizations_StateWriteFailurePropagates asserts a failed write is not
// reported as a successful create. A CreateAccount that answers 200 while the
// account record never landed is the worst outcome available: the caller records
// an account ID that no subsequent call can find.
func TestOrganizations_StateWriteFailurePropagates(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{inner: inner, prefix: "account:", err: errors.New("store unavailable"), onPut: true}
	p := newOrgFaultFixture(t, state)
	ctx := t.Context()

	if _, err := p.EnsureOrganizationForTest(ctx, orgTestAccount); err != nil {
		t.Fatalf("setup: %v", err)
	}
	state.armed = true

	err := p.SaveAccountForTest(ctx, orgTestAccount, emulator.OrgAccount{ID: "111111111111", Name: "dev"})
	if err == nil {
		t.Error("expected the write failure to propagate")
	}
}

// TestOrganizations_IndexWriteFailurePropagates covers the index half of a save.
// The record and its index are two writes, and a caller told the save succeeded
// when only the record landed would find the entity through Describe but never
// through a List — the split that reads as a pagination bug.
func TestOrganizations_IndexWriteFailurePropagates(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{inner: inner, prefix: "policy_ids:", err: errors.New("store unavailable"), onPut: true}
	p := newOrgFaultFixture(t, state)
	ctx := t.Context()

	if _, err := p.EnsureOrganizationForTest(ctx, orgTestAccount); err != nil {
		t.Fatalf("setup: %v", err)
	}
	state.armed = true

	pol := emulator.OrgPolicy{
		PolicySummary: emulator.OrgPolicySummary{ID: "p-11112222", Name: "deny", Type: emulator.OrgPolicyTypeSCPForTest},
		Content:       `{"Version":"2012-10-17"}`,
	}
	if err := p.SavePolicyForTest(ctx, orgTestAccount, pol); err == nil {
		t.Error("expected the index write failure to propagate")
	}
}

// TestOrganizations_CorruptRecordIsAnError asserts an unreadable record is
// reported rather than treated as absent. Treating it as absent would silently
// resurrect a fresh entity over the top of one that still exists in the store.
func TestOrganizations_CorruptRecordIsAnError(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &corruptOrgState{StateManager: inner, prefix: "org:"}
	p := newOrgFaultFixture(t, state)
	ctx := t.Context()

	if _, err := p.EnsureOrganizationForTest(ctx, orgTestAccount); err != nil {
		t.Fatalf("setup: %v", err)
	}
	state.armed = true

	if _, err := p.EnsureOrganizationForTest(ctx, orgTestAccount); err == nil {
		t.Error("expected an unreadable organization record to be an error, not an absent one")
	}
}

// TestOrganizations_CorruptIndexIsAnError covers the same for an ID index, which
// every listing and every attachment lookup reads.
func TestOrganizations_CorruptIndexIsAnError(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &corruptOrgState{StateManager: inner, prefix: "attachments:"}
	p := newOrgFaultFixture(t, state)
	ctx := t.Context()

	root, err := p.LoadRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	state.armed = true

	if _, err := p.LoadAttachmentsForTest(ctx, root.ID); err == nil {
		t.Error("expected an unreadable attachment index to be an error")
	}
	if _, err := p.AttachPolicyToForTest(ctx, "p-11112222", root.ID); err == nil {
		t.Error("expected an attach over an unreadable index to fail rather than overwrite it")
	}
	if _, err := p.DetachPolicyFromForTest(ctx, emulator.OrgFullAWSAccessIDForTest, root.ID); err == nil {
		t.Error("expected a detach over an unreadable index to fail rather than overwrite it")
	}
}

// TestOrganizations_HierarchyReadFailurePropagates covers the placement index.
// ouDepth walks it, so a swallowed failure there would report a shallower depth
// than the tree really has and let a handler create a level past the limit.
func TestOrganizations_HierarchyReadFailurePropagates(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{inner: inner, prefix: "parent:", err: errors.New("store unavailable"), onGet: true}
	p := newOrgFaultFixture(t, state)
	ctx := t.Context()

	root, err := p.LoadRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	ouID := "ou-" + root.ID[2:] + "-11112222"
	if err := p.PlaceChildForTest(ctx, root.ID, ouID); err != nil {
		t.Fatalf("place OU: %v", err)
	}
	state.armed = true

	if _, err := p.LoadParentForTest(ctx, ouID); err == nil {
		t.Error("expected the parent read failure to propagate")
	}
	if _, err := p.OUDepthForTest(ctx, ouID); err == nil {
		t.Error("expected ouDepth to report the read failure rather than a shallower depth")
	}
	if err := p.PlaceChildForTest(ctx, root.ID, ouID); err == nil {
		t.Error("expected a placement that cannot read the current parent to fail")
	}
}

// TestOrganizations_TargetResolutionFailurePropagates covers resolveOrgTarget,
// which every attachment and every tag operation consults. A swallowed failure
// resolves to "" and the caller turns that into a not-found refusal, telling the
// consumer its resource does not exist when the store merely could not be read.
func TestOrganizations_TargetResolutionFailurePropagates(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{inner: inner, prefix: "ou:", err: errors.New("store unavailable"), onGet: true}
	p := newOrgFaultFixture(t, state)
	ctx := t.Context()

	root, err := p.LoadRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	state.armed = true

	if _, err := p.ResolveOrgTargetForTest(ctx, orgTestAccount, "ou-"+root.ID[2:]+"-11112222"); err == nil {
		t.Error("expected the OU read failure to propagate rather than resolve to 'no such target'")
	}
}

// TestOrganizations_SeedReadFailurePropagates covers the control-plane reads. A
// swallowed failure would silently fall back to the nominal path: the test would
// pass while exercising SUCCEEDED, which is precisely the path the seed exists
// to avoid.
func TestOrganizations_SeedReadFailurePropagates(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{inner: inner, prefix: "create-account-failure:", err: errors.New("store unavailable"), onGet: true, armed: true}
	p := newOrgFaultFixture(t, state)

	if _, _, err := p.ResolveSeededCreateFailureForTest(t.Context(), "dev"); err == nil {
		t.Error("expected the seed read failure to propagate rather than resolve to 'no seed'")
	}
}

// TestOrganizations_FeatureSetReadFailurePropagates covers the other seed read.
// Falling back to ALL on a failure would report policy types on a root that may
// not have them.
func TestOrganizations_FeatureSetReadFailurePropagates(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{inner: inner, prefix: "feature-set", err: errors.New("store unavailable"), onGet: true, armed: true}
	p := newOrgFaultFixture(t, state)

	if _, err := p.EffectiveFeatureSetForTest(t.Context(), orgTestAccount); err == nil {
		t.Error("expected the feature-set read failure to propagate rather than default to ALL")
	}
}

// TestOrganizations_StoreFailureIsInternalFailure pins the wire result: a store
// failure reaching an operation is a 500 InternalFailure, which an SDK retries,
// rather than a 400 the SDK treats as terminal.
func TestOrganizations_StoreFailureIsInternalFailure(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{inner: inner, prefix: "org:", err: errors.New("store unavailable"), onGet: true}

	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false})
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.OrganizationsPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)

	// Unarmed, the organization is created and read normally.
	warm := orgsRequest(t, ts, "DescribeOrganization", map[string]interface{}{})
	warm.Body.Close() //nolint:errcheck
	state.armed = true

	for _, op := range []string{"DescribeOrganization", "ListAccounts", "DescribeAccount", "ListRoots", "CreateAccount"} {
		body := map[string]interface{}{}
		switch op {
		case "DescribeAccount":
			body["AccountId"] = orgTestAccount
		case "CreateAccount":
			body["AccountName"] = "dev"
			body["Email"] = "dev@example.com"
		}
		resp := orgsRequest(t, ts, op, body)
		gotStatus := resp.StatusCode
		resp.Body.Close() //nolint:errcheck
		if gotStatus != http.StatusInternalServerError {
			t.Errorf("%s: expected 500 for a store failure, got %d", op, gotStatus)
		}
	}
}

// TestOrganizations_SeedEndpointStoreFailures asserts the seed endpoints report a
// store failure as a 500 rather than answering ok. A seed silently not stored
// makes the test that depends on it exercise the nominal path while passing.
func TestOrganizations_SeedEndpointStoreFailures(t *testing.T) {
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false})
	inner := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(0, false)

	state := &errOrgState{
		inner: inner, prefix: "", err: errors.New("store unavailable"),
		armed: true, onPut: true, onDelete: true, onList: true,
	}
	cfg := emulator.DefaultConfig()
	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger)

	cases := []struct {
		name   string
		method string
		path   string
		body   map[string]any
	}{
		{"seed feature set", http.MethodPost, "/v1/organizations/feature-set", map[string]any{"featureSet": "ALL"}},
		{"clear feature set", http.MethodDelete, "/v1/organizations/feature-set", nil},
		{
			"seed create failure", http.MethodPost, "/v1/organizations/create-account-failure",
			map[string]any{"accountName": "*", "failureReason": "INTERNAL_FAILURE"},
		},
		{"clear one create failure", http.MethodDelete, "/v1/organizations/create-account-failure?accountName=dev", nil},
		{"clear all create failures", http.MethodDelete, "/v1/organizations/create-account-failure", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := orgSeedRequest(t, srv, tc.method, tc.path, tc.body); got != http.StatusInternalServerError {
				t.Errorf("expected 500 for a store failure, got %d", got)
			}
		})
	}
}

// TestOrganizations_SeedEndpointRejectsUnparseableBody covers the decode branch
// on both seed endpoints, which is the one a hand-written curl reaches first.
func TestOrganizations_SeedEndpointRejectsUnparseableBody(t *testing.T) {
	srv, _ := newOrganizationsServerForSeeds(t)

	for _, path := range []string{"/v1/organizations/feature-set", "/v1/organizations/create-account-failure"} {
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"featureSet":`))
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		if got := w.Result().StatusCode; got != http.StatusBadRequest { //nolint:bodyclose // httptest.Recorder result needs no close.
			t.Errorf("%s: expected 400 for an unparseable body, got %d", path, got)
		}
	}
}

// TestOrganizations_ClearAllCreateFailuresDeletesEveryKey pins that the unscoped
// clear removes every seed rather than the first. A leftover wildcard seed would
// fail an unrelated later CreateAccount in the same run.
func TestOrganizations_ClearAllCreateFailuresDeletesEveryKey(t *testing.T) {
	srv, p := newOrganizationsServerForSeeds(t)
	ctx := t.Context()

	for _, name := range []string{"dev", "prod", "*"} {
		if got := orgSeedRequest(t, srv, http.MethodPost, "/v1/organizations/create-account-failure",
			map[string]any{"accountName": name, "failureReason": "INTERNAL_FAILURE"}); got != http.StatusOK {
			t.Fatalf("seed %s: expected 200, got %d", name, got)
		}
	}
	if got := orgSeedRequest(t, srv, http.MethodDelete, "/v1/organizations/create-account-failure", nil); got != http.StatusOK {
		t.Fatalf("clear all: expected 200, got %d", got)
	}
	for _, name := range []string{"dev", "prod", "other"} {
		if _, seeded, err := p.ResolveSeededCreateFailureForTest(ctx, name); err != nil || seeded {
			t.Errorf("%s: expected every seed cleared (seeded=%v, err=%v)", name, seeded, err)
		}
	}
}

// TestOrganizations_OmittedSeedNameIsTheWildcard pins the documented default: a
// seed posted with no accountName applies to every name. A caller who omitted it
// expecting a no-op would otherwise fail an unrelated CreateAccount later in the
// run and have nothing to trace it to.
func TestOrganizations_OmittedSeedNameIsTheWildcard(t *testing.T) {
	srv, p := newOrganizationsServerForSeeds(t)

	if got := orgSeedRequest(t, srv, http.MethodPost, "/v1/organizations/create-account-failure",
		map[string]any{"failureReason": "INVALID_EMAIL"}); got != http.StatusOK {
		t.Fatalf("seed with no accountName: expected 200, got %d", got)
	}
	reason, seeded, err := p.ResolveSeededCreateFailureForTest(t.Context(), "anything")
	if err != nil || !seeded || reason != "INVALID_EMAIL" {
		t.Errorf("expected the omitted name to seed the wildcard, got (%q, %v, %v)", reason, seeded, err)
	}
}

// TestOrganizations_StaleNextTokenIsRefused asserts a token from a different
// listing is refused rather than silently treated as "start over". Restarting is
// the worst answer available to a paging loop: it never terminates, and the
// caller sees the first page forever without an error to explain it.
func TestOrganizations_StaleNextTokenIsRefused(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	for _, op := range []string{"ListAccounts", "ListRoots"} {
		resp := orgsRequest(t, ts, op, map[string]interface{}{"NextToken": "bm90LWEtcmVhbC1pZA=="})
		gotStatus := resp.StatusCode
		resp.Body.Close() //nolint:errcheck
		if gotStatus != http.StatusBadRequest {
			t.Errorf("%s: expected 400 for a token naming no known ID, got %d", op, gotStatus)
		}
	}
}

// TestOrganizations_MalformedBodyPerOperation covers the decode guard on every
// operation the foundation claims, not just one. An operation that skipped the
// guard would decode into a zero-valued input and answer as though the caller had
// asked for the empty account, which is a wrong answer rather than an error.
func TestOrganizations_MalformedBodyPerOperation(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	for _, op := range []string{"ListAccounts", "DescribeAccount", "ListRoots", "CreateAccount"} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", newOrgBadBody())
		if err != nil {
			t.Fatalf("%s: build request: %v", op, err)
		}
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Organizations_20161128."+op)
		req.Host = "organizations.us-east-1.amazonaws.com"
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		gotStatus := resp.StatusCode
		resp.Body.Close() //nolint:errcheck
		if gotStatus != http.StatusBadRequest {
			t.Errorf("%s: expected 400 for an unparseable body, got %d", op, gotStatus)
		}
	}
}

// TestOrganizations_ListAccountsSkipsVanishedRecords asserts a listing tolerates
// an index entry whose record is gone rather than reporting a zero-valued account.
// An account with an empty Id in a listing is worse than a short page: a consumer
// iterating it would call DescribeAccount with "" and get a not-found it cannot
// explain.
func TestOrganizations_ListAccountsSkipsVanishedRecords(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false})
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.OrganizationsPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	registry.Register(p)
	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)

	warm := orgsRequest(t, ts, "ListAccounts", map[string]interface{}{})
	warm.Body.Close() //nolint:errcheck

	// The index still names the management account; its record does not exist.
	if err := state.Delete(t.Context(), "organizations", "account:"+orgTestAccount); err != nil {
		t.Fatalf("delete account record: %v", err)
	}

	resp := orgsRequest(t, ts, "ListAccounts", map[string]interface{}{})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out struct {
		Accounts []struct {
			ID string `json:"Id"`
		} `json:"Accounts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Accounts) != 0 {
		t.Errorf("expected the vanished record skipped rather than listed empty, got %+v", out.Accounts)
	}
}

// TestOrganizations_OUAndStatusIndexWriteFailures covers the remaining index
// halves, for the same reason the policy index is covered: a save reported
// successful with only the record written is invisible to every listing.
func TestOrganizations_OUAndStatusIndexWriteFailures(t *testing.T) {
	t.Run("OU index", func(t *testing.T) {
		inner := emulator.NewMemoryStateManager()
		state := &errOrgState{inner: inner, prefix: "ou_ids:", err: errors.New("store unavailable"), onPut: true, armed: true}
		p := newOrgFaultFixture(t, state)
		ou := emulator.OrgOrganizationalUnit{ID: "ou-abcd-11112222", Name: "prod"}
		if err := p.SaveOUForTest(t.Context(), orgTestAccount, ou); err == nil {
			t.Error("expected the OU index write failure to propagate")
		}
	})
	t.Run("create-account-status index", func(t *testing.T) {
		inner := emulator.NewMemoryStateManager()
		state := &errOrgState{inner: inner, prefix: "car_ids:", err: errors.New("store unavailable"), onPut: true, armed: true}
		p := newOrgFaultFixture(t, state)
		st := emulator.OrgCreateAccountStatus{ID: "car-11112222", AccountName: "dev", State: "IN_PROGRESS"}
		if err := p.SaveCreateAccountStatusForTest(t.Context(), orgTestAccount, st); err == nil {
			t.Error("expected the request index write failure to propagate")
		}
	})
	t.Run("account index", func(t *testing.T) {
		inner := emulator.NewMemoryStateManager()
		state := &errOrgState{inner: inner, prefix: "account_ids:", err: errors.New("store unavailable"), onPut: true, armed: true}
		p := newOrgFaultFixture(t, state)
		a := emulator.OrgAccount{ID: "111111111111", Name: "dev"}
		if err := p.SaveAccountForTest(t.Context(), orgTestAccount, a); err == nil {
			t.Error("expected the account index write failure to propagate")
		}
	})
}

// TestOrganizations_ReverseIndexFailurePropagates covers the second half of an
// attachment write. An attachment is two writes, one per direction, and reporting
// success when only the forward one landed leaves ListPoliciesForTarget and
// ListTargetsForPolicy contradicting each other — a state no sequence of API calls
// can produce, so nothing downstream is prepared for it.
func TestOrganizations_ReverseIndexFailurePropagates(t *testing.T) {
	root := ""
	setup := func(t *testing.T, onPut bool) *emulator.OrganizationsPlugin {
		t.Helper()
		inner := emulator.NewMemoryStateManager()
		state := &errOrgState{inner: inner, prefix: "policy_targets:", err: errors.New("store unavailable"), onPut: onPut, onGet: !onPut}
		p := newOrgFaultFixture(t, state)
		r, err := p.LoadRootForTest(t.Context(), orgTestAccount)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		root = r.ID
		state.armed = true
		return p
	}

	t.Run("attach", func(t *testing.T) {
		p := setup(t, true)
		if _, err := p.AttachPolicyToForTest(t.Context(), "p-11112222", root); err == nil {
			t.Error("expected the reverse-index write failure to propagate")
		}
	})
	t.Run("detach", func(t *testing.T) {
		p := setup(t, true)
		if _, err := p.DetachPolicyFromForTest(t.Context(), emulator.OrgFullAWSAccessIDForTest, root); err == nil {
			t.Error("expected the reverse-index write failure to propagate")
		}
	})
	t.Run("read", func(t *testing.T) {
		p := setup(t, false)
		if _, err := p.LoadPolicyTargetsForTest(t.Context(), emulator.OrgFullAWSAccessIDForTest); err == nil {
			t.Error("expected the reverse-index read failure to propagate")
		}
	})
}

// TestOrganizations_FullAWSAccessAttachFailurePropagates covers the guard that
// decides whether to attach at all. Reading it as "not enabled" on a failure would
// create an account with no SCP attached, which then makes the minimum-attachment
// rule unenforceable for that account.
func TestOrganizations_FullAWSAccessAttachFailurePropagates(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &errOrgState{inner: inner, prefix: "root:", err: errors.New("store unavailable"), onGet: true}
	p := newOrgFaultFixture(t, state)
	ctx := t.Context()

	if _, err := p.LoadRootForTest(ctx, orgTestAccount); err != nil {
		t.Fatalf("setup: %v", err)
	}
	state.armed = true

	if err := p.AttachFullAWSAccessForTest(ctx, orgTestAccount, "111111111111"); err == nil {
		t.Error("expected the enablement read failure to propagate rather than skip the attachment")
	}
}

// TestOrganizations_CorruptTagsAreAnError asserts unreadable tags are reported.
// Reading them as "no tags" would answer an authorization decision with an empty
// tag set, which fails open on a tag-gated policy.
func TestOrganizations_CorruptTagsAreAnError(t *testing.T) {
	inner := emulator.NewMemoryStateManager()
	state := &corruptOrgState{StateManager: inner, prefix: "tags:", armed: true}
	p := newOrgFaultFixture(t, state)

	if _, err := p.LoadTagsForTest(t.Context(), orgTestAccount); err == nil {
		t.Error("expected unreadable tags to be an error, not an empty tag set")
	}
}

// TestOrganizations_PartialAutoCreateIsRefused walks every write auto-creation
// performs and asserts a failure at each one is reported rather than swallowed.
// Auto-creation is six writes — the organization, the root, the root's
// FullAWSAccess attachment, the management account, its placement, and its own
// attachment — and a caller told the organization exists when only some landed
// would go on to reference a root or a management account that is not there. The
// prefixes are named individually so a reordering of the writes cannot quietly
// stop covering one.
func TestOrganizations_PartialAutoCreateIsRefused(t *testing.T) {
	for _, prefix := range []string{
		"org:", "root:", "attachments:", "policy_targets:",
		"account:", "account_ids:", "children:", "parent:",
	} {
		t.Run(prefix, func(t *testing.T) {
			inner := emulator.NewMemoryStateManager()
			state := &errOrgState{
				inner: inner, prefix: prefix, err: errors.New("store unavailable"),
				onPut: true, armed: true,
			}
			p := newOrgFaultFixture(t, state)
			if _, err := p.EnsureOrganizationForTest(t.Context(), orgTestAccount); err == nil {
				t.Errorf("expected a failed %s write to refuse auto-creation, got an organization", prefix)
			}
		})
	}
}

// TestOrganizations_RecordWriteFailuresPropagate covers the record half of each
// save, the counterpart to the index half. Reporting a save successful when the
// record never landed is the worse direction: the listing names an ID that no
// Describe can resolve.
func TestOrganizations_RecordWriteFailuresPropagate(t *testing.T) {
	newFixture := func(t *testing.T, prefix string) *emulator.OrganizationsPlugin {
		t.Helper()
		inner := emulator.NewMemoryStateManager()
		state := &errOrgState{
			inner: inner, prefix: prefix, err: errors.New("store unavailable"),
			onPut: true, armed: true,
		}
		return newOrgFaultFixture(t, state)
	}
	ctx := t.Context()

	t.Run("OU record", func(t *testing.T) {
		p := newFixture(t, "ou:")
		ou := emulator.OrgOrganizationalUnit{ID: "ou-abcd-11112222", Name: "prod"}
		if err := p.SaveOUForTest(ctx, orgTestAccount, ou); err == nil {
			t.Error("expected the OU record write failure to propagate")
		}
	})
	t.Run("policy record", func(t *testing.T) {
		p := newFixture(t, "policy:")
		pol := emulator.OrgPolicy{
			PolicySummary: emulator.OrgPolicySummary{ID: "p-11112222", Name: "deny", Type: emulator.OrgPolicyTypeSCPForTest},
		}
		if err := p.SavePolicyForTest(ctx, orgTestAccount, pol); err == nil {
			t.Error("expected the policy record write failure to propagate")
		}
	})
	t.Run("create-account-status record", func(t *testing.T) {
		p := newFixture(t, "car:")
		st := emulator.OrgCreateAccountStatus{ID: "car-11112222", AccountName: "dev", State: "IN_PROGRESS"}
		if err := p.SaveCreateAccountStatusForTest(ctx, orgTestAccount, st); err == nil {
			t.Error("expected the request record write failure to propagate")
		}
	})
	t.Run("tags", func(t *testing.T) {
		p := newFixture(t, "tags:")
		if err := p.SaveTagsForTest(ctx, orgTestAccount, []emulator.OrgTag{{Key: "Owner", Value: "x"}}); err == nil {
			t.Error("expected the tag write failure to propagate")
		}
	})
	t.Run("placement", func(t *testing.T) {
		p := newFixture(t, "children:")
		if err := p.PlaceChildForTest(ctx, "r-abcd", "ou-abcd-11112222"); err == nil {
			t.Error("expected the placement write failure to propagate")
		}
	})
}

// TestOrganizations_CorruptRecordsAreErrors asserts every record loader reports an
// unreadable record rather than reporting the entity absent. Reporting absent is
// the dangerous direction: the caller's next step is to create the entity again,
// on top of one that is still in the store.
func TestOrganizations_CorruptRecordsAreErrors(t *testing.T) {
	newFixture := func(t *testing.T, prefix string) *emulator.OrganizationsPlugin {
		t.Helper()
		inner := emulator.NewMemoryStateManager()
		return newOrgFaultFixture(t, &corruptOrgState{StateManager: inner, prefix: prefix, armed: true})
	}
	ctx := t.Context()

	t.Run("account", func(t *testing.T) {
		if _, err := newFixture(t, "account:").LoadAccountForTest(ctx, orgTestAccount); err == nil {
			t.Error("expected an unreadable account record to be an error")
		}
	})
	t.Run("policy", func(t *testing.T) {
		if _, err := newFixture(t, "policy:").LoadPolicyForTest(ctx, "p-11112222"); err == nil {
			t.Error("expected an unreadable policy record to be an error")
		}
	})
	t.Run("OU", func(t *testing.T) {
		if _, err := newFixture(t, "ou:").LoadOUForTest(ctx, "ou-abcd-11112222"); err == nil {
			t.Error("expected an unreadable OU record to be an error")
		}
	})
	t.Run("create-account-status", func(t *testing.T) {
		if _, err := newFixture(t, "car:").LoadCreateAccountStatusForTest(ctx, "car-11112222"); err == nil {
			t.Error("expected an unreadable request record to be an error")
		}
	})
	t.Run("parent", func(t *testing.T) {
		if _, err := newFixture(t, "parent:").LoadParentForTest(ctx, orgTestAccount); err == nil {
			t.Error("expected an unreadable placement record to be an error")
		}
	})
}

// TestOrganizations_ResolveTargetReadFailures covers the remaining lookups
// resolveOrgTarget performs. It answers one of four kinds or "no such entity", and
// every caller turns "" into a not-found refusal, so a swallowed failure anywhere
// in the chain tells the consumer its resource does not exist.
func TestOrganizations_ResolveTargetReadFailures(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		id     string
	}{
		{"root", "root:", ""},
		{"policy", "policy:", "p-11112222"},
		{"account", "account:", "111111111111"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inner := emulator.NewMemoryStateManager()
			state := &errOrgState{inner: inner, prefix: c.prefix, err: errors.New("store unavailable"), onGet: true}
			p := newOrgFaultFixture(t, state)
			root, err := p.LoadRootForTest(t.Context(), orgTestAccount)
			if err != nil {
				t.Fatalf("setup: %v", err)
			}
			id := c.id
			if id == "" {
				id = root.ID
			}
			state.armed = true
			if _, err := p.ResolveOrgTargetForTest(t.Context(), orgTestAccount, id); err == nil {
				t.Errorf("expected the %s read failure to propagate rather than resolve to 'no such target'", c.name)
			}
		})
	}
}

// TestOrganizations_InitializeDefaultsTheClock covers the branch taken when no
// time controller is supplied. Leaving p.tc nil there would panic on the first
// timestamped write rather than fall back, so the fallback is what keeps a plugin
// constructed outside the server usable.
func TestOrganizations_InitializeDefaultsTheClock(t *testing.T) {
	p := &emulator.OrganizationsPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:  emulator.NewMemoryStateManager(),
		Logger: emulator.NewDefaultLogger(0, false),
	}); err != nil {
		t.Fatalf("initialize with no time controller: %v", err)
	}
	// The management account carries a JoinedAt from the clock, so auto-creation
	// is what would panic on a nil controller.
	if _, err := p.EnsureOrganizationForTest(t.Context(), orgTestAccount); err != nil {
		t.Errorf("expected auto-creation to work on the default clock: %v", err)
	}
}

// TestOrganizations_EmptyBodyIsNotAnError pins that an operation with no body at
// all is treated as an empty request rather than a parse failure. The AWS CLI
// sends no body for a no-argument call like ListRoots, so refusing it would make
// every such call fail.
func TestOrganizations_EmptyBodyIsNotAnError(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	for _, op := range []string{"ListRoots", "ListAccounts", "DescribeOrganization"} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", http.NoBody)
		if err != nil {
			t.Fatalf("%s: build request: %v", op, err)
		}
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Organizations_20161128."+op)
		req.Host = "organizations.us-east-1.amazonaws.com"
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		gotStatus := resp.StatusCode
		resp.Body.Close() //nolint:errcheck
		if gotStatus != http.StatusOK {
			t.Errorf("%s: expected 200 for an empty body, got %d", op, gotStatus)
		}
	}
}

// TestOrganizations_OUDepthTerminatesOnACycle asserts the depth walk is bounded.
// A cycle in the placement index is unreachable through the API, but an unbounded
// walk over one would hang the server rather than fail a request, and a hang is
// the one failure a deterministic emulator cannot let a test observe.
func TestOrganizations_OUDepthTerminatesOnACycle(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	p := newOrgFaultFixture(t, state)
	ctx := t.Context()

	root, err := p.LoadRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	a := "ou-" + root.ID[2:] + "-aaaaaaaa"
	b := "ou-" + root.ID[2:] + "-bbbbbbbb"
	if err := p.PlaceChildForTest(ctx, a, b); err != nil {
		t.Fatalf("place b under a: %v", err)
	}
	// Closing the loop by hand; placeChild would not produce this.
	if err := p.PlaceChildForTest(ctx, b, a); err != nil {
		t.Fatalf("place a under b: %v", err)
	}

	depth, err := p.OUDepthForTest(ctx, a)
	if err != nil {
		t.Fatalf("ou depth over a cycle: %v", err)
	}
	if depth <= 0 {
		t.Errorf("expected a bounded positive depth, got %d", depth)
	}
}

// TestOrganizations_ShutdownIsANoOp pins that shutting the plugin down neither
// errors nor discards state, which is what the registry's shutdown sweep relies
// on when a run ends mid-journey.
func TestOrganizations_ShutdownIsANoOp(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	p := newOrgFaultFixture(t, state)
	ctx := t.Context()

	before, err := p.LoadRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load root: %v", err)
	}
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	after, err := p.LoadRootForTest(ctx, orgTestAccount)
	if err != nil {
		t.Fatalf("load root after shutdown: %v", err)
	}
	if after.ID != before.ID {
		t.Errorf("the root changed identity across shutdown: %q became %q", before.ID, after.ID)
	}
}
