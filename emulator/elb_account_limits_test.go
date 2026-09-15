package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

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
	resp := elbRequest(t, ts, params)
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

	assert.Contains(t, raw, `<DescribeAccountLimitsResponse xmlns="`+
		`https://elasticloadbalancing.amazonaws.com/doc/2015-12-01/">`)
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

// TestELB_DescribeAccountLimits_PageSize covers the documented 1–400 range and what happens
// outside it.
//
// A value substrate cannot use falls back to the default, which is the documented maximum —
// the behavior RDS's and ElastiCache's MaxRecords and CloudWatch's already have. It is a
// choice, not the only convention in the tree: EC2's DescribeTags refuses a MaxResults
// outside 5–1000 with InvalidParameterValue. The deciding argument is that
// DescribeAccountLimits publishes **no operation-specific error** — its Errors section is
// Common Errors only on both generations' pages — so refusing would mean inventing a code
// AWS does not publish for it (#885).
func TestELB_DescribeAccountLimits_PageSize(t *testing.T) {
	all := len(elbExpectedAccountLimitNames)
	tests := []struct {
		name           string
		pageSize       string
		wantLen        int
		wantNextMarker string
	}{
		{"absent returns the whole set", "", all, ""},
		{"one", "1", 1, "1"},
		{"the documented minimum plus one", "2", 2, "2"},
		{"exactly the set size", "23", all, ""},
		{"the documented maximum", "400", all, ""},
		{"zero falls back to the default", "0", all, ""},
		{"negative falls back to the default", "-5", all, ""},
		{"above the documented maximum falls back to the default", "401", all, ""},
		{"non-numeric falls back to the default", "not-a-number", all, ""},
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
