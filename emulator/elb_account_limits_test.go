package emulator_test

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// elbExpectedAccountLimitNames is the limit set DescribeAccountLimits must report, in the
// order it must report it.
//
// It is written out here rather than read from the plugin so the test pins the set from the
// outside: the names come from the example output on the AWS CLI v2 reference page for the
// operation, because the v2 API_Limit page enumerates none of them (see
// elbDefaultAccountLimits for the full provenance). A name silently disappearing from the
// response is exactly the regression a consumer that keys on one would see, so the list is
// duplicated deliberately.
var elbExpectedAccountLimitNames = []string{
	"application-load-balancers",
	"certificates-per-application-load-balancer",
	"certificates-per-network-load-balancer",
	"condition-values-per-alb-rule",
	"condition-wildcards-per-alb-rule",
	"gateway-load-balancers",
	"gateway-load-balancers-per-vpc",
	"geneve-target-groups",
	"listeners-per-application-load-balancer",
	"listeners-per-network-load-balancer",
	"network-load-balancer-enis-per-vpc",
	"network-load-balancers",
	"rules-per-application-load-balancer",
	"target-groups",
	"target-groups-per-action-on-application-load-balancer",
	"target-groups-per-action-on-network-load-balancer",
	"target-groups-per-application-load-balancer",
	"target-id-registrations-per-application-load-balancer",
	"targets-per-application-load-balancer",
	"targets-per-availability-zone-per-gateway-load-balancer",
	"targets-per-availability-zone-per-network-load-balancer",
	"targets-per-network-load-balancer",
	"targets-per-target-group",
}

// elbAccountLimit is one decoded Limit member.
type elbAccountLimit struct {
	Max  string `xml:"Max"`
	Name string `xml:"Name"`
}

// elbDescribeAccountLimits makes a DescribeAccountLimits call and returns the raw response
// body alongside the decoded limits and NextMarker.
//
// The raw body is returned as well as the decoded struct because a decoded struct cannot
// answer two of the questions this operation has to get right: whether the result wrapper
// is named what botocore looks up, and whether NextMarker is absent or present-and-empty.
func elbDescribeAccountLimits(t *testing.T, ts *httptest.Server, extra map[string]string) (string, []elbAccountLimit, string) {
	t.Helper()
	params := map[string]string{"Action": "DescribeAccountLimits"}
	for k, v := range extra {
		params[k] = v
	}
	resp := elbRequest(t, ts.URL, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var decoded struct {
		Result struct {
			Limits     []elbAccountLimit `xml:"Limits>member"`
			NextMarker string            `xml:"NextMarker"`
		} `xml:"DescribeAccountLimitsResult"`
	}
	require.NoError(t, xml.Unmarshal(raw, &decoded))
	return string(raw), decoded.Result.Limits, decoded.Result.NextMarker
}

// elbSeedAccountLimit posts a limit seed and asserts it was accepted.
func elbSeedAccountLimit(t *testing.T, ts *httptest.Server, body string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/elb/account-limits", "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// elbClearAccountLimit deletes one seed by name, or all of them when name is empty.
func elbClearAccountLimit(t *testing.T, ts *httptest.Server, name string) {
	t.Helper()
	target := ts.URL + "/v1/elb/account-limits"
	if name != "" {
		target += "?" + url.Values{"name": {name}}.Encode()
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, target, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// elbLimitByName finds one limit in a decoded set.
func elbLimitByName(t *testing.T, limits []elbAccountLimit, name string) elbAccountLimit {
	t.Helper()
	for _, l := range limits {
		if l.Name == name {
			return l
		}
	}
	t.Fatalf("limit %q not reported; got %d limits", name, len(limits))
	return elbAccountLimit{}
}

// TestELB_DescribeAccountLimits_Dispatches is the regression for #885: the action was in
// substrate's authorization reference with an empty resource list while the plugin's action
// switch had no arm for it, so a caller could hold a permission substrate answered
// InvalidAction for.
func TestELB_DescribeAccountLimits_Dispatches(t *testing.T) {
	ts := newELBTestServer(t)
	raw, limits, nextMarker := elbDescribeAccountLimits(t, ts, nil)

	assert.NotContains(t, raw, "InvalidAction")
	assert.Len(t, limits, len(elbExpectedAccountLimitNames))
	assert.Empty(t, nextMarker, "the default page size is the documented maximum, so one page holds the set")
}

// TestELB_DescribeAccountLimits_RawWireShape asserts on the bytes rather than a decoded
// struct, for the two reasons a struct cannot answer.
//
// The result wrapper: ELBv2 speaks the Query protocol, where each output shape declares a
// resultWrapper and botocore looks it up by name, so a wrapper named anything else makes
// boto3 and the AWS CLI raise KeyError while a test reading the XML directly sees nothing
// wrong — the failure #748 fixed on six other ELB operations.
//
// The member set: Max is published as a **String**, and Max and Name are the only two
// members Limit declares. A decoded struct cannot distinguish a member that is absent from
// one that is present and empty, and it would silently accept an extra member substrate
// invented.
func TestELB_DescribeAccountLimits_RawWireShape(t *testing.T) {
	ts := newELBTestServer(t)
	raw, _, _ := elbDescribeAccountLimits(t, ts, nil)

	assert.Contains(t, raw, `<DescribeAccountLimitsResponse xmlns="`+elbV2Namespace+`">`)
	assert.Contains(t, raw, "<DescribeAccountLimitsResult>")
	assert.Contains(t, raw, "</DescribeAccountLimitsResult>")

	// One member, whole and in the model's declaration order, with no third member.
	assert.Contains(t, raw,
		"<member><Max>50</Max><Name>application-load-balancers</Name></member>")
	assert.Contains(t, raw, "<Limits>")

	// v2 documents NextMarker as null when the walk is exhausted, which on the wire is an
	// absent element — not the empty one classic (2012-06-01) documents. See #844.
	assert.NotContains(t, raw, "NextMarker")
}

// TestELB_DescribeAccountLimits_ReportsTheSourcedSet pins every name and its default Max.
//
// The Max values are the current defaults from the three Load Balancer quota user guides,
// except the three noted in elbDefaultAccountLimits that no guide row names cleanly.
// Substrate enforces none of them — no ELB operation counts a resource against a quota —
// so this test is pinning what is *reported*, which is the whole of what the operation
// promises.
func TestELB_DescribeAccountLimits_ReportsTheSourcedSet(t *testing.T) {
	ts := newELBTestServer(t)
	_, limits, _ := elbDescribeAccountLimits(t, ts, nil)

	gotNames := make([]string, 0, len(limits))
	for _, l := range limits {
		gotNames = append(gotNames, l.Name)
		assert.NotEmpty(t, l.Max, "limit %q reported an empty Max", l.Name)
	}
	assert.Equal(t, elbExpectedAccountLimitNames, gotNames,
		"the reported set and its order must be stable, since the Marker is an offset into it")

	for name, want := range map[string]string{
		"application-load-balancers":                            "50",
		"condition-wildcards-per-alb-rule":                      "6",
		"gateway-load-balancers":                                "100",
		"network-load-balancer-enis-per-vpc":                    "1200",
		"target-groups":                                         "3000",
		"target-groups-per-action-on-network-load-balancer":     "1",
		"target-id-registrations-per-application-load-balancer": "1000",
	} {
		assert.Equal(t, want, elbLimitByName(t, limits, name).Max, "Max for %q", name)
	}
}

// TestELB_DescribeAccountLimits_MarkerRoundTrip walks the whole set a page at a time and
// asserts each limit is returned exactly once.
//
// The Marker is a decimal offset into a fixed order, the cursor RDS and ElastiCache already
// answer with. An offset over an unordered list could skip or repeat an entry between pages,
// which a deterministic emulator cannot do.
func TestELB_DescribeAccountLimits_MarkerRoundTrip(t *testing.T) {
	ts := newELBTestServer(t)

	seen := make([]string, 0, len(elbExpectedAccountLimitNames))
	counts := make(map[string]int)
	marker := ""
	for pages := 0; ; pages++ {
		require.Less(t, pages, len(elbExpectedAccountLimitNames)+2, "the walk did not terminate")
		params := map[string]string{"PageSize": "5"}
		if marker != "" {
			params["Marker"] = marker
		}
		_, limits, next := elbDescribeAccountLimits(t, ts, params)
		require.LessOrEqual(t, len(limits), 5, "a page must not exceed the PageSize asked for")
		for _, l := range limits {
			seen = append(seen, l.Name)
			counts[l.Name]++
		}
		if next == "" {
			break
		}
		marker = next
	}

	assert.Equal(t, elbExpectedAccountLimitNames, seen, "a paged walk returns the set in order")
	for _, name := range elbExpectedAccountLimitNames {
		assert.Equal(t, 1, counts[name], "limit %q was returned %d times", name, counts[name])
	}
}

// TestELB_DescribeAccountLimits_PageSize covers the documented 1–400 range from the inside:
// every value the operation accepts, and the page each one answers.
//
// The outside of the range moved to `elb_page_size_test.go` when #1150 gave the plugin one
// rule. Until then a value substrate could not use fell back to the default — the behavior
// the Query family had before #913 — and the argument for it turned on
// DescribeAccountLimits publishing no operation-specific error, which #1064 answered: the
// Common Errors page its Errors section links as its own publishes `ValidationError`. So the
// four out-of-range cases are now refusals, and they are asserted there against **both**
// generations at once, because "the same code and status from both" is the property that was
// missing rather than "this operation refuses".
func TestELB_DescribeAccountLimits_PageSize(t *testing.T) {
	all := len(elbExpectedAccountLimitNames)
	tests := []struct {
		name           string
		pageSize       string
		wantLen        int
		wantNextMarker string
	}{
		{"absent returns the whole set", "", all, ""},
		{"the documented minimum", "1", 1, "1"},
		{"the documented minimum plus one", "2", 2, "2"},
		{"exactly the set size", "23", all, ""},
		{"the documented maximum", "400", all, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newELBTestServer(t)
			extra := map[string]string{}
			if tt.pageSize != "" {
				extra["PageSize"] = tt.pageSize
			}
			_, limits, next := elbDescribeAccountLimits(t, ts, extra)
			assert.Len(t, limits, tt.wantLen)
			assert.Equal(t, tt.wantNextMarker, next)
		})
	}
}

// TestELB_DescribeAccountLimits_MarkerEdges covers a Marker substrate cannot use and one
// past the end. Neither is an error the operation publishes a code for, so an unparseable
// Marker restarts the walk and an exhausted offset answers an empty last page — as RDS's
// and ElastiCache's cursors do.
func TestELB_DescribeAccountLimits_MarkerEdges(t *testing.T) {
	tests := []struct {
		name    string
		marker  string
		wantLen int
	}{
		{"unparseable restarts the walk", "not-an-offset", len(elbExpectedAccountLimitNames)},
		{"negative restarts the walk", "-3", len(elbExpectedAccountLimitNames)},
		{"past the end is an empty last page", "9999", 0},
		{"exactly the end is an empty last page", "23", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newELBTestServer(t)
			raw, limits, next := elbDescribeAccountLimits(t, ts, map[string]string{"Marker": tt.marker})
			assert.Len(t, limits, tt.wantLen)
			assert.Empty(t, next)
			// Even an empty page carries the wrapper botocore looks up.
			assert.Contains(t, raw, "<DescribeAccountLimitsResult>")
		})
	}
}

// TestELB_DescribeAccountLimits_SeedOneLimit seeds a low limit and reads it back over the
// wire, leaving the rest of the set at its default.
//
// This is the reason the limits are seedable rather than constants: substrate enforces no
// ELB quota, so a constant here would be a number a caller computing headroom could not
// act on. Seeding turns it into the value a consumer's "approaching my quota" branch reads,
// which is what the operation is polled for.
func TestELB_DescribeAccountLimits_SeedOneLimit(t *testing.T) {
	ts := newELBTestServer(t)
	elbSeedAccountLimit(t, ts, `{"name":"application-load-balancers","max":"1"}`)

	raw, limits, _ := elbDescribeAccountLimits(t, ts, nil)
	assert.Equal(t, "1", elbLimitByName(t, limits, "application-load-balancers").Max)
	assert.Equal(t, "50", elbLimitByName(t, limits, "network-load-balancers").Max,
		"a name-scoped seed must not move any other limit")
	assert.Len(t, limits, len(elbExpectedAccountLimitNames), "seeding does not change the set")
	assert.Contains(t, raw, "<member><Max>1</Max><Name>application-load-balancers</Name></member>")
}

// TestELB_DescribeAccountLimits_SeedWildcard covers the "*" seed, which lowers every limit
// at once — the "the whole account is at quota" case, in one call.
func TestELB_DescribeAccountLimits_SeedWildcard(t *testing.T) {
	ts := newELBTestServer(t)
	elbSeedAccountLimit(t, ts, `{"name":"*","max":"0"}`)

	_, limits, _ := elbDescribeAccountLimits(t, ts, nil)
	require.Len(t, limits, len(elbExpectedAccountLimitNames))
	for _, l := range limits {
		assert.Equal(t, "0", l.Max, "limit %q", l.Name)
	}
}

// TestELB_DescribeAccountLimits_SeedNameBeatsWildcard pins the resolution order the house
// seed pattern uses: the exact name first, the "*" wildcard second.
func TestELB_DescribeAccountLimits_SeedNameBeatsWildcard(t *testing.T) {
	ts := newELBTestServer(t)
	elbSeedAccountLimit(t, ts, `{"name":"*","max":"0"}`)
	elbSeedAccountLimit(t, ts, `{"name":"target-groups","max":"7"}`)

	_, limits, _ := elbDescribeAccountLimits(t, ts, nil)
	assert.Equal(t, "7", elbLimitByName(t, limits, "target-groups").Max)
	assert.Equal(t, "0", elbLimitByName(t, limits, "application-load-balancers").Max)
}

// TestELB_DescribeAccountLimits_SeedOmittedNameIsTheWildcard covers a seed carrying no
// name, which the key builder resolves to "*" as the EC2 Fleet shortfall seed's key builder
// does for a missing launch template.
func TestELB_DescribeAccountLimits_SeedOmittedNameIsTheWildcard(t *testing.T) {
	ts := newELBTestServer(t)
	elbSeedAccountLimit(t, ts, `{"max":"3"}`)

	_, limits, _ := elbDescribeAccountLimits(t, ts, nil)
	require.NotEmpty(t, limits)
	for _, l := range limits {
		assert.Equal(t, "3", l.Max, "limit %q", l.Name)
	}
}

// TestELB_DescribeAccountLimits_ClearSeed covers both DELETE forms: one seed by name, and
// every seed at once.
func TestELB_DescribeAccountLimits_ClearSeed(t *testing.T) {
	t.Run("by name", func(t *testing.T) {
		ts := newELBTestServer(t)
		elbSeedAccountLimit(t, ts, `{"name":"target-groups","max":"7"}`)
		elbSeedAccountLimit(t, ts, `{"name":"geneve-target-groups","max":"9"}`)
		elbClearAccountLimit(t, ts, "target-groups")

		_, limits, _ := elbDescribeAccountLimits(t, ts, nil)
		assert.Equal(t, "3000", elbLimitByName(t, limits, "target-groups").Max)
		assert.Equal(t, "9", elbLimitByName(t, limits, "geneve-target-groups").Max)
	})

	t.Run("all", func(t *testing.T) {
		ts := newELBTestServer(t)
		elbSeedAccountLimit(t, ts, `{"name":"*","max":"0"}`)
		elbSeedAccountLimit(t, ts, `{"name":"target-groups","max":"7"}`)
		elbClearAccountLimit(t, ts, "")

		_, limits, _ := elbDescribeAccountLimits(t, ts, nil)
		assert.Equal(t, "3000", elbLimitByName(t, limits, "target-groups").Max)
		assert.Equal(t, "50", elbLimitByName(t, limits, "application-load-balancers").Max)
	})
}

// TestELB_SeedAccountLimit_Rejects covers the seed endpoint's own refusals. A seed with no
// max is refused rather than stored, because an empty Max resolves to "no seed applies" and
// would silently do nothing.
func TestELB_SeedAccountLimit_Rejects(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"malformed JSON", `{"name":`},
		{"no max", `{"name":"target-groups"}`},
		{"empty max", `{"name":"target-groups","max":""}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newELBTestServer(t)
			resp, err := http.Post(ts.URL+"/v1/elb/account-limits", "application/json", strings.NewReader(tt.body))
			require.NoError(t, err)
			defer resp.Body.Close() //nolint:errcheck
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

			_, limits, _ := elbDescribeAccountLimits(t, ts, nil)
			assert.Equal(t, "3000", elbLimitByName(t, limits, "target-groups").Max,
				"a refused seed must not have been stored")
		})
	}
}

// elbAccountLimitsNamespace is the state namespace the seed lives in, duplicated here so
// elbLimitFaultState can scope its faults to it. Failing every namespace would break the
// server's own per-request bookkeeping and the request would then fail for a reason other
// than the one under test.
const elbAccountLimitsNamespace = "elb-limits-ctrl"

// elbLimitFaultState wraps a working StateManager and injects a store failure on one
// operation within the account-limit namespace, following sqsAlwaysFailState.
//
// corruptGet is the separate case of a store that answers successfully with bytes that are
// not the seed: state written by an older substrate, or by hand. That decodes to a JSON
// error, not a store error, and the two must not be conflated.
type elbLimitFaultState struct {
	emulator.StateManager
	getErr     error
	corruptGet bool
	putErr     error
	deleteErr  error
	listErr    error
}

func (m *elbLimitFaultState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if namespace == elbAccountLimitsNamespace {
		if m.getErr != nil {
			return nil, m.getErr
		}
		if m.corruptGet {
			return []byte("{not-json"), nil
		}
	}
	return m.StateManager.Get(ctx, namespace, key)
}

func (m *elbLimitFaultState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if namespace == elbAccountLimitsNamespace && m.putErr != nil {
		return m.putErr
	}
	return m.StateManager.Put(ctx, namespace, key, value)
}

func (m *elbLimitFaultState) Delete(ctx context.Context, namespace, key string) error {
	if namespace == elbAccountLimitsNamespace && m.deleteErr != nil {
		return m.deleteErr
	}
	return m.StateManager.Delete(ctx, namespace, key)
}

func (m *elbLimitFaultState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if namespace == elbAccountLimitsNamespace && m.listErr != nil {
		return nil, m.listErr
	}
	return m.StateManager.List(ctx, namespace, prefix)
}

// newELBTestServerWithState builds the same server newELBTestServer does over a caller's
// StateManager, so a store fault can be injected.
func newELBTestServerWithState(t *testing.T, state emulator.StateManager) *httptest.Server {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.ELBPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)
	return ts
}

// TestELB_DescribeAccountLimits_StateFailure asserts a store failure during the seed lookup
// propagates instead of being answered as a successful set of defaults.
//
// Silently falling back would be the worse failure: a harness that seeded "every limit is
// 0" to drive its at-quota branch would be handed 50 back, the assertion would pass
// vacuously, and the seed would look armed when it was not — the same failure mode #413
// filed against the SQS consistency seed. A 5xx is a signal the caller can retry; a
// wrong-but-plausible 200 is not.
func TestELB_DescribeAccountLimits_StateFailure(t *testing.T) {
	boom := errors.New("state store unavailable")

	tests := []struct {
		name  string
		state emulator.StateManager
	}{
		{
			name:  "seed lookup fails",
			state: &elbLimitFaultState{StateManager: emulator.NewMemoryStateManager(), getErr: boom},
		},
		{
			name:  "stored seed does not decode",
			state: &elbLimitFaultState{StateManager: emulator.NewMemoryStateManager(), corruptGet: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newELBTestServerWithState(t, tt.state)
			resp := elbRequest(t, ts.URL, map[string]string{"Action": "DescribeAccountLimits"})
			defer resp.Body.Close() //nolint:errcheck
			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.GreaterOrEqual(t, resp.StatusCode, http.StatusInternalServerError,
				"a store failure must not be answered as a successful set of defaults; body was %s", raw)
			assert.NotContains(t, string(raw), "<Limits>",
				"no limit may be reported when the seed that would override it could not be read")
		})
	}
}

// TestELB_AccountLimitsControlPlane_StateFailures covers the control plane's own store
// errors. A seed that fails to persist while answering 200 is worse than no seed: the
// harness proceeds believing the limit is armed and every later assertion passes against
// the defaults.
func TestELB_AccountLimitsControlPlane_StateFailures(t *testing.T) {
	boom := errors.New("state store unavailable")

	tests := []struct {
		name    string
		state   func() *elbLimitFaultState
		preseed bool
		req     func(t *testing.T, ts *httptest.Server) *http.Request
	}{
		{
			name: "seed put fails",
			state: func() *elbLimitFaultState {
				return &elbLimitFaultState{StateManager: emulator.NewMemoryStateManager(), putErr: boom}
			},
			req: func(t *testing.T, ts *httptest.Server) *http.Request {
				t.Helper()
				req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
					ts.URL+"/v1/elb/account-limits",
					strings.NewReader(`{"name":"target-groups","max":"1"}`))
				require.NoError(t, err)
				return req
			},
		},
		{
			name: "clear one delete fails",
			state: func() *elbLimitFaultState {
				return &elbLimitFaultState{StateManager: emulator.NewMemoryStateManager(), deleteErr: boom}
			},
			req: func(t *testing.T, ts *httptest.Server) *http.Request {
				t.Helper()
				req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete,
					ts.URL+"/v1/elb/account-limits?name=target-groups", nil)
				require.NoError(t, err)
				return req
			},
		},
		{
			name: "clear all list fails",
			state: func() *elbLimitFaultState {
				return &elbLimitFaultState{StateManager: emulator.NewMemoryStateManager(), listErr: boom}
			},
			req: func(t *testing.T, ts *httptest.Server) *http.Request {
				t.Helper()
				req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete,
					ts.URL+"/v1/elb/account-limits", nil)
				require.NoError(t, err)
				return req
			},
		},
		{
			// The sweep needs a seed present for List to return a key to delete. Only
			// Delete is faulted, so the seed can be written over the wire first and the
			// fault needs no mutation after the server is serving — which would be a race
			// the detector would flag.
			name: "clear all delete fails",
			state: func() *elbLimitFaultState {
				return &elbLimitFaultState{StateManager: emulator.NewMemoryStateManager(), deleteErr: boom}
			},
			preseed: true,
			req: func(t *testing.T, ts *httptest.Server) *http.Request {
				t.Helper()
				req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete,
					ts.URL+"/v1/elb/account-limits", nil)
				require.NoError(t, err)
				return req
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newELBTestServerWithState(t, tt.state())
			if tt.preseed {
				elbSeedAccountLimit(t, ts, `{"name":"target-groups","max":"1"}`)
			}

			resp, err := http.DefaultClient.Do(tt.req(t, ts))
			require.NoError(t, err)
			defer resp.Body.Close() //nolint:errcheck
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, http.StatusInternalServerError, resp.StatusCode, "body was %s", body)
		})
	}
}
