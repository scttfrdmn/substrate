package emulator_test

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ListPolicies filters (#497).
//
// The operation parsed Scope and PathPrefix and applied neither, and it listed only policies
// created through CreatePolicy — so the 52 bundled AWS managed policies were invisible to
// every listing. `--scope AWS` returned whatever the caller happened to have created. The
// pairing that breaks is the one a reader assumes: an ARN GetPolicy resolves appeared in no
// listing, so a consumer discovering a policy rather than hardcoding its ARN had no testable
// path at all.

// listPoliciesXML decodes a ListPolicies response.
type listPoliciesXML struct {
	Policies []struct {
		PolicyName      string `xml:"PolicyName"`
		ARN             string `xml:"Arn"`
		Path            string `xml:"Path"`
		AttachmentCount int    `xml:"AttachmentCount"`
	} `xml:"ListPoliciesResult>Policies>member"`
	IsTruncated bool   `xml:"ListPoliciesResult>IsTruncated"`
	Marker      string `xml:"ListPoliciesResult>Marker"`
}

// arns returns the listed ARNs, in order.
func (l listPoliciesXML) arns() []string {
	out := make([]string, 0, len(l.Policies))
	for _, p := range l.Policies {
		out = append(out, p.ARN)
	}
	return out
}

// countFor returns the AttachmentCount reported for one ARN, and whether it was listed.
func (l listPoliciesXML) countFor(arn string) (int, bool) {
	for _, p := range l.Policies {
		if p.ARN == arn {
			return p.AttachmentCount, true
		}
	}
	return 0, false
}

// iamListPolicies posts a ListPolicies request, requires 200, and decodes it.
func iamListPolicies(t *testing.T, srv *emulator.Server, body map[string]any) listPoliciesXML {
	t.Helper()
	resp := iamRequest(t, srv, "ListPolicies", body)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", raw)

	var decoded listPoliciesXML
	require.NoError(t, xml.Unmarshal(raw, &decoded), "body: %s", raw)
	return decoded
}

// TestListPolicies_ScopeSelectsTheSource is the bug: Scope was parsed and never applied, and
// the bundled catalog was in no listing at all.
func TestListPolicies_ScopeSelectsTheSource(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	mine := iamCreatePolicyForVersions(t, srv, "mine",
		iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"))
	const bundled = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
	catalogSize := len(emulator.ListManagedPolicies())
	require.Positive(t, catalogSize, "the catalog is the premise of this test")

	// MaxItems is at its ceiling throughout, because the catalog alone exceeds the default
	// page of 100 once it grows — a test that paged by accident would assert on a slice.
	const maxItems = 1000

	t.Run("AWS lists the catalog and nothing created", func(t *testing.T) {
		t.Parallel()
		got := iamListPolicies(t, srv, map[string]any{"Scope": "AWS", "MaxItems": maxItems})
		assert.Len(t, got.Policies, catalogSize)
		assert.Contains(t, got.arns(), bundled)
		assert.NotContains(t, got.arns(), mine)
	})

	t.Run("Local lists what was created and none of the catalog", func(t *testing.T) {
		t.Parallel()
		got := iamListPolicies(t, srv, map[string]any{"Scope": "Local", "MaxItems": maxItems})
		assert.Contains(t, got.arns(), mine)
		assert.NotContains(t, got.arns(), bundled)
	})

	t.Run("All lists both", func(t *testing.T) {
		t.Parallel()
		got := iamListPolicies(t, srv, map[string]any{"Scope": "All", "MaxItems": maxItems})
		assert.Contains(t, got.arns(), mine)
		assert.Contains(t, got.arns(), bundled)
	})

	t.Run("an absent Scope defaults to All", func(t *testing.T) {
		t.Parallel()
		got := iamListPolicies(t, srv, map[string]any{"MaxItems": maxItems})
		assert.Contains(t, got.arns(), mine)
		assert.Contains(t, got.arns(), bundled)
	})
}

// TestListPolicies_EveryBundledARNIsResolvable closes the pairing #497 names: a policy in a
// listing can be fetched, and a policy GetPolicy resolves is in a listing.
//
// Asserted over the whole catalog rather than a sample, because the failure mode is one
// entry whose ARN was built differently — a path folded into PolicyName, say — and a
// sampled test would miss exactly that.
func TestListPolicies_EveryBundledARNIsResolvable(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	listed := iamListPolicies(t, srv, map[string]any{"Scope": "AWS", "MaxItems": 1000})
	require.Len(t, listed.Policies, len(emulator.ListManagedPolicies()))

	for _, p := range listed.Policies {
		resp := iamRequest(t, srv, "GetPolicy", map[string]any{"PolicyArn": p.ARN})
		require.Equal(t, http.StatusOK, resp.StatusCode, "GetPolicy must resolve %s", p.ARN)
		require.NoError(t, resp.Body.Close())

		// The ARN and the path have to agree, since PathPrefix filters on Path and a
		// consumer builds nothing from it but the ARN.
		if p.Path != "/" {
			assert.Contains(t, p.ARN, "policy"+p.Path+p.PolicyName,
				"a policy's ARN must carry its path")
		}
	}
}

// TestListPolicies_PathPrefixNarrows covers the query a consumer actually writes:
// --scope AWS --path-prefix /service-role/ then pick a name.
func TestListPolicies_PathPrefixNarrows(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	// The expectation is derived from the catalog rather than hardcoded at five, so adding a
	// service-role policy does not fail an unrelated test — but it is still asserted
	// non-empty, so a filter that dropped everything cannot pass.
	var want []string
	for _, mp := range emulator.ListManagedPolicies() {
		if mp.Path == "/service-role/" {
			want = append(want, mp.ARN)
		}
	}
	require.NotEmpty(t, want, "the catalog must carry service-role policies")

	got := iamListPolicies(t, srv, map[string]any{
		"Scope":      "AWS",
		"PathPrefix": "/service-role/",
		"MaxItems":   1000,
	})
	assert.ElementsMatch(t, want, got.arns())

	// And a root prefix still admits everything, because every path begins with "/".
	all := iamListPolicies(t, srv, map[string]any{"PathPrefix": "/", "MaxItems": 1000})
	assert.Len(t, all.Policies, len(emulator.ListManagedPolicies()))

	// A prefix nothing matches is an empty list, not an error.
	none := iamListPolicies(t, srv, map[string]any{"PathPrefix": "/nothing-here/", "MaxItems": 1000})
	assert.Empty(t, none.Policies)
	assert.False(t, none.IsTruncated)
}

// TestListPolicies_PathPrefixCombinesWithLocalScope pins the two filters composing, on a
// created policy under a path — the combination the issue asks for.
func TestListPolicies_PathPrefixCombinesWithLocalScope(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	resp := iamRequest(t, srv, "CreatePolicy", map[string]any{
		"PolicyName":     "deployer",
		"Path":           "/team/",
		"PolicyDocument": iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"),
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	iamCreatePolicyForVersions(t, srv, "rooted",
		iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"))

	got := iamListPolicies(t, srv, map[string]any{
		"Scope":      "Local",
		"PathPrefix": "/team/",
		"MaxItems":   1000,
	})
	require.Len(t, got.Policies, 1)
	assert.Equal(t, "deployer", got.Policies[0].PolicyName)
	assert.Equal(t, "/team/", got.Policies[0].Path)
}

// TestListPolicies_AttachmentCountComesFromState is why OnlyAttached can work at all.
//
// The catalog carries AttachmentCount 0 for every bundled policy and no attach operation
// increments a stored count, so a listing reading the stored field would report 0 forever
// and OnlyAttached=true could never return a managed policy.
func TestListPolicies_AttachmentCountComesFromState(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	const bundled = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
	catalog, ok := emulator.GetManagedPolicy(bundled)
	require.True(t, ok)
	require.Zero(t, catalog.AttachmentCount,
		"the catalog's stored count is zero — that is the premise")

	before, listed := iamListPolicies(t, srv,
		map[string]any{"Scope": "AWS", "MaxItems": 1000}).countFor(bundled)
	require.True(t, listed)
	assert.Zero(t, before)

	// Attached to three entity kinds, because the count is summed across three separate
	// state prefixes and a loop covering only users would still pass a one-entity test.
	iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "jill"})
	iamRequest(t, srv, "CreateGroup", map[string]any{"GroupName": "devs"})
	iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "runner"})
	for op, key := range map[string]string{
		"AttachUserPolicy":  "UserName",
		"AttachGroupPolicy": "GroupName",
		"AttachRolePolicy":  "RoleName",
	} {
		name := map[string]string{"UserName": "jill", "GroupName": "devs", "RoleName": "runner"}[key]
		resp := iamRequest(t, srv, op, map[string]any{key: name, "PolicyArn": bundled})
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s must succeed", op)
		require.NoError(t, resp.Body.Close())
	}

	after, ok := iamListPolicies(t, srv,
		map[string]any{"Scope": "AWS", "MaxItems": 1000}).countFor(bundled)
	require.True(t, ok)
	assert.Equal(t, 3, after, "a user, a group and a role are three attachments")

	// The catalog itself must not have been written through. ListManagedPolicies hands back
	// shared pointers, so a listing that set the count in place would leak it into GetPolicy
	// and into every later listing in the process.
	assert.Zero(t, catalog.AttachmentCount, "the shared catalog entry must not be mutated")

	// A detach is immediately visible, which a stored counter would have to be told about.
	detach := iamRequest(t, srv, "DetachUserPolicy", map[string]any{
		"UserName": "jill", "PolicyArn": bundled,
	})
	require.Equal(t, http.StatusOK, detach.StatusCode)
	require.NoError(t, detach.Body.Close())

	dropped, ok := iamListPolicies(t, srv,
		map[string]any{"Scope": "AWS", "MaxItems": 1000}).countFor(bundled)
	require.True(t, ok)
	assert.Equal(t, 2, dropped)
}

// TestListPolicies_OnlyAttachedNarrows covers the fourth acceptance criterion, against a
// bundled policy — the case a stored count could never satisfy.
func TestListPolicies_OnlyAttachedNarrows(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	const attached = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
	const unattached = "arn:aws:iam::aws:policy/AmazonS3FullAccess"

	iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "jill"})
	resp := iamRequest(t, srv, "AttachUserPolicy", map[string]any{
		"UserName": "jill", "PolicyArn": attached,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	created := iamCreatePolicyForVersions(t, srv, "never-attached",
		iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"))

	got := iamListPolicies(t, srv, map[string]any{"OnlyAttached": true, "MaxItems": 1000})
	assert.Equal(t, []string{attached}, got.arns())
	assert.NotContains(t, got.arns(), unattached)
	assert.NotContains(t, got.arns(), created)

	// OnlyAttached=false is the same as absent, per the reference.
	off := iamListPolicies(t, srv, map[string]any{"OnlyAttached": false, "MaxItems": 1000})
	assert.Contains(t, off.arns(), unattached)
}

// TestListPolicies_PaginationHoldsWithTheCatalog is the criterion the extra ~52 entries put
// at risk: the result set is now large enough to page by default, so a marker that was
// merely untested is now on every caller's path.
func TestListPolicies_PaginationHoldsWithTheCatalog(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	mine := iamCreatePolicyForVersions(t, srv, "mine",
		iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"))

	// Paged 10 at a time, following the marker to exhaustion, and the union must equal the
	// unpaged listing exactly — no repeat, no gap. A page size that does not divide the
	// total is deliberate, so the final short page is exercised.
	full := iamListPolicies(t, srv, map[string]any{"MaxItems": 1000})
	require.Greater(t, len(full.Policies), 10, "the catalog makes this multi-page")
	assert.False(t, full.IsTruncated)
	assert.Empty(t, full.Marker, "an untruncated response carries no Marker")

	var walked []string
	marker := ""
	for pages := 0; ; pages++ {
		require.Less(t, pages, 100, "pagination must terminate")
		body := map[string]any{"MaxItems": 10}
		if marker != "" {
			body["Marker"] = marker
		}
		page := iamListPolicies(t, srv, body)
		walked = append(walked, page.arns()...)
		if !page.IsTruncated {
			assert.Empty(t, page.Marker)
			break
		}
		require.NotEmpty(t, page.Marker, "a truncated page must carry a Marker")
		assert.Len(t, page.Policies, 10)
		marker = page.Marker
	}

	assert.Equal(t, full.arns(), walked, "the pages must reassemble the whole listing")
	assert.Contains(t, walked, mine)

	// Sorted by ARN, which is what makes a marker stable across calls: a marker naming an
	// ARN can only resume at one place if the order is total.
	sorted := append([]string(nil), full.arns()...)
	assert.True(t, sortedStrings(sorted), "the listing must be sorted by ARN")

	// A filtered listing pages over the filtered set, not the whole one.
	filtered := iamListPolicies(t, srv, map[string]any{
		"Scope":      "AWS",
		"PathPrefix": "/service-role/",
		"MaxItems":   2,
	})
	assert.LessOrEqual(t, len(filtered.Policies), 2)
}

// TestListPolicies_RefusesBadInput covers the enum and pattern arms.
func TestListPolicies_RefusesBadInput(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	tests := []struct {
		name string
		body map[string]any
	}{
		{
			// The model's policyScopeType is All/AWS/Local and nothing else.
			name: "an unknown Scope",
			body: map[string]any{"Scope": "Everything"},
		},
		{
			// Case matters: the enum is "AWS", not "aws".
			name: "a lowercased Scope",
			body: map[string]any{"Scope": "aws"},
		},
		{
			// policyPathType requires a leading and trailing slash, and dropping the
			// trailing one is the natural typo — AWS refuses it, so accepting it would let a
			// call pass here and fail against IAM.
			name: "a PathPrefix with no trailing slash",
			body: map[string]any{"PathPrefix": "/service-role"},
		},
		{
			name: "a PathPrefix with no leading slash",
			body: map[string]any{"PathPrefix": "service-role/"},
		},
		{
			name: "a PathPrefix with an illegal character",
			body: map[string]any{"PathPrefix": "/service role/"},
		},
		{
			name: "an unknown PolicyUsageFilter",
			body: map[string]any{"PolicyUsageFilter": "Whatever"},
		},
		{
			name: "MaxItems that is not a number",
			body: map[string]any{"MaxItems": "abc"},
		},
		{
			name: "OnlyAttached that is not a boolean",
			body: map[string]any{"OnlyAttached": "yes"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp := iamRequest(t, srv, "ListPolicies", tc.body)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, "ValidationError", result["__type"])
		})
	}
}

// TestListPolicies_AcceptsAValidPolicyUsageFilter pins the deliberate non-application of
// PolicyUsageFilter.
//
// The reference says PermissionsPolicy lists "permissions policies" and PermissionsBoundary
// lists "the policies used to set permissions boundaries"; it does not say which side an
// entirely-unused policy falls on, and in a fresh substrate every bundled policy is unused.
// Guessing that unattached means "not a permissions policy" would drop all 52 from a
// filtered listing — the same failure #497 reports, under a different parameter. So a valid
// value is accepted and narrows nothing, and docs/services.md says so.
func TestListPolicies_AcceptsAValidPolicyUsageFilter(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	for _, filter := range []string{"PermissionsPolicy", "PermissionsBoundary"} {
		t.Run(filter, func(t *testing.T) {
			t.Parallel()
			got := iamListPolicies(t, srv, map[string]any{
				"PolicyUsageFilter": filter,
				"MaxItems":          1000,
			})
			assert.Len(t, got.Policies, len(emulator.ListManagedPolicies()))
		})
	}
}

// TestListPolicies_IsAuthorized keeps the iam:ListPolicies grant meaningful.
func TestListPolicies_IsAuthorized(t *testing.T) {
	t.Parallel()
	srv := newTrustPolicyTestServer(t)
	accessKey := trustSetupCallerWithoutAssume(t, srv, "ungranted")

	resp := iamCallAs(t, srv, accessKey, "ListPolicies", map[string]any{"Scope": "AWS"})
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, emulator.IAMAccessDeniedCodeForTest, result["__type"])
}

// TestListPoliciesWire_OverTheQueryProtocol drives the filters the way a client sends them:
// every value arrives as a string, so Scope, PathPrefix, OnlyAttached and MaxItems all reach
// the handler through the encoding an SDK produces rather than the JSON a unit test builds.
func TestListPoliciesWire_OverTheQueryProtocol(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	resp := iamFormRequest(t, srv, "ListPolicies", map[string]string{
		"Scope":      "AWS",
		"PathPrefix": "/service-role/",
		"MaxItems":   "1000",
	})
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", raw)
	require.NotContains(t, string(raw), "InvalidAction")

	var decoded listPoliciesXML
	require.NoError(t, xml.Unmarshal(raw, &decoded))
	require.NotEmpty(t, decoded.Policies)
	for _, p := range decoded.Policies {
		assert.Equal(t, "/service-role/", p.Path)
	}

	// OnlyAttached arrives as the string "true", which is the shape #642 was about.
	attached := iamFormRequest(t, srv, "ListPolicies", map[string]string{
		"OnlyAttached": "true",
		"MaxItems":     "1000",
	})
	attachedRaw, err := io.ReadAll(attached.Body)
	require.NoError(t, err)
	require.NoError(t, attached.Body.Close())
	require.Equal(t, http.StatusOK, attached.StatusCode, "body: %s", attachedRaw)

	var none listPoliciesXML
	require.NoError(t, xml.Unmarshal(attachedRaw, &none))
	assert.Empty(t, none.Policies, "nothing is attached in a fresh server")
}

// iamFaultState is a StateManager that fails or corrupts specific reads, so the listing's
// error arms are reachable.
type iamFaultState struct {
	emulator.StateManager

	// listErrPrefix, when non-empty, makes List fail for that prefix.
	listErrPrefix string

	// getErrKeySubstr, when non-empty, makes Get fail for any key containing it.
	getErrKeySubstr string

	// corruptKeySubstr, when non-empty, makes Get return bytes that are not a policy.
	corruptKeySubstr string
}

func (s *iamFaultState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if s.listErrPrefix != "" && prefix == s.listErrPrefix {
		return nil, errors.New("state store unavailable")
	}
	return s.StateManager.List(ctx, namespace, prefix)
}

func (s *iamFaultState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if s.getErrKeySubstr != "" && strings.Contains(key, s.getErrKeySubstr) {
		return nil, errors.New("state store unavailable")
	}
	if s.corruptKeySubstr != "" && strings.Contains(key, s.corruptKeySubstr) {
		return []byte("this is not a policy"), nil
	}
	return s.StateManager.Get(ctx, namespace, key)
}

// TestListPolicies_StateFailureIsNotAnEmptyList pins the error arms, which are the ones with
// a wrong answer readily available.
//
// An empty list and a broken store are opposite signals: the first says "no policy matches,
// stop looking" and the second says "ask again". A listing that swallowed a store failure
// would hand a consumer the first when the truth is the second — and #497 is itself a report
// of a listing that returned nothing when it should have returned something.
func TestListPolicies_StateFailureIsNotAnEmptyList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string

		// arm applies the fault, and is called only after the setup writes have landed: a
		// store that fails from the first call never gets a policy created to trip over.
		arm func(*iamFaultState)
	}{
		{
			// The Local arm's own listing.
			name: "the policy listing fails",
			arm:  func(s *iamFaultState) { s.listErrPrefix = "policy:" },
		},
		{
			name: "a policy record cannot be read",
			arm:  func(s *iamFaultState) { s.getErrKeySubstr = "policy:arn:" },
		},
		{
			// The attachment counts walk their own prefixes, so a failure there is a
			// separate arm from the policy listing.
			name: "the attachment listing fails",
			arm:  func(s *iamFaultState) { s.listErrPrefix = "user_policies:" },
		},
		{
			name: "an attachment list cannot be read",
			arm:  func(s *iamFaultState) { s.getErrKeySubstr = "user_policies:" },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := &iamFaultState{StateManager: emulator.NewMemoryStateManager()}
			srv := newIAMTestServerWithState(t, state)

			// A policy and an attachment exist first, so the failing read is one the listing
			// actually reaches rather than a prefix with nothing under it.
			iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "jill"})
			iamRequest(t, srv, "CreatePolicy", map[string]any{
				"PolicyName":     "mine",
				"PolicyDocument": iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"),
			})
			iamRequest(t, srv, "AttachUserPolicy", map[string]any{
				"UserName":  "jill",
				"PolicyArn": "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
			})
			tc.arm(state)

			resp := iamRequest(t, srv, "ListPolicies", map[string]any{"MaxItems": 1000})
			assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
				"a store failure must surface as a server error, not as an empty listing")
			require.NoError(t, resp.Body.Close())
		})
	}
}

// TestListPolicies_ACorruptRecordDoesNotHideTheRest pins the one read failure that is
// deliberately *not* fatal.
//
// A record that does not decode is skipped, because one unreadable key should not make every
// policy unlistable — the bundled catalog does not live in state at all and has nothing to do
// with it. That is a different judgement from a store failure, where nothing can be trusted.
func TestListPolicies_ACorruptRecordDoesNotHideTheRest(t *testing.T) {
	t.Parallel()

	state := &iamFaultState{StateManager: emulator.NewMemoryStateManager()}
	srv := newIAMTestServerWithState(t, state)

	iamRequest(t, srv, "CreatePolicy", map[string]any{
		"PolicyName":     "mine",
		"PolicyDocument": iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"),
	})
	state.corruptKeySubstr = "policy:arn:"

	got := iamListPolicies(t, srv, map[string]any{"MaxItems": 1000})
	assert.Len(t, got.Policies, len(emulator.ListManagedPolicies()),
		"the catalog is still listed with a state record unreadable")
}

// sortedStrings reports whether s is in non-decreasing order.
func sortedStrings(s []string) bool {
	for i := 1; i < len(s); i++ {
		if strings.Compare(s[i-1], s[i]) > 0 {
			return false
		}
	}
	return true
}
