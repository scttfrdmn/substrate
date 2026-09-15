package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ListInstanceProfiles' three request parameters (#873).
//
// Every call here goes over the query protocol rather than the JSON one, for two reasons: it is
// the path a real SDK client takes, and iamValidateMaxItems reads presence from the decoded form
// values, so an absent MaxItems cannot be distinguished from MaxItems=0 on any other path.
//
// The profiles are created through CreateInstanceProfile rather than written into state, per
// #765: a helper that writes the record directly cannot show that Path survives the owning
// operation, which is the value PathPrefix filters on.

// iamProfileSpec is one instance profile to create before a listing.
type iamProfileSpec struct {
	name string
	path string
}

// iamSeedProfiles creates each spec over the wire, so Path is whatever CreateInstanceProfile
// stored rather than whatever a test wrote.
func iamSeedProfiles(t *testing.T, srv *emulator.Server, specs []iamProfileSpec) {
	t.Helper()
	for _, spec := range specs {
		params := map[string]string{"InstanceProfileName": spec.name}
		if spec.path != "" {
			params["Path"] = spec.path
		}
		resp := iamFormRequest(t, srv, "CreateInstanceProfile", params)
		require.Equal(t, http.StatusOK, resp.StatusCode, "create %s", spec.name)
		require.NoError(t, resp.Body.Close())
	}
}

// iamListProfiles runs one ListInstanceProfiles call and returns the decoded result.
func iamListProfiles(t *testing.T, srv *emulator.Server, params map[string]string) map[string]any {
	t.Helper()
	resp := iamFormRequest(t, srv, "ListInstanceProfiles", params)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	return result
}

// iamProfileNames returns the InstanceProfileName of every member in a decoded listing.
//
// A missing InstanceProfiles key would be a decoder change rather than an empty page — the
// operation always renders the wrapper — so its absence is asserted rather than tolerated.
func iamProfileNames(t *testing.T, result map[string]any) []string {
	t.Helper()
	raw, ok := result["InstanceProfiles"]
	require.True(t, ok, "the wrapper is always rendered, even for an empty page")
	members, ok := raw.([]any)
	require.True(t, ok, "InstanceProfiles decodes to a list")

	names := make([]string, 0, len(members))
	for _, m := range members {
		entry, isMap := m.(map[string]any)
		require.True(t, isMap)
		name, isStr := entry["InstanceProfileName"].(string)
		require.True(t, isStr, "every member names itself")
		names = append(names, name)
	}
	return names
}

// iamIsTruncated returns the listing's IsTruncated flag, asserting it is present.
//
// A listing that omitted it would leave a caller's paginator reading a missing key as false and
// stopping one page early, so its absence is a failure rather than a default.
func iamIsTruncated(t *testing.T, result map[string]any) bool {
	t.Helper()
	truncated, ok := result["IsTruncated"].(bool)
	require.True(t, ok, "every page reports IsTruncated")
	return truncated
}

// TestListInstanceProfiles_MaxItemsPagesAndTheMarkerRoundTrips walks a truncated listing to the
// end and asserts every profile is reported exactly once.
//
// The walk is the assertion, not the first page: a marker that did not resume where the previous
// page stopped would repeat or skip a profile while every individual response still looked
// well-formed.
func TestListInstanceProfiles_MaxItemsPagesAndTheMarkerRoundTrips(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamSeedProfiles(t, srv, []iamProfileSpec{
		{name: "profile-a"}, {name: "profile-b"}, {name: "profile-c"},
	})

	first := iamListProfiles(t, srv, map[string]string{"MaxItems": "1"})
	require.Len(t, iamProfileNames(t, first), 1, "MaxItems=1 is one profile, not three")
	assert.True(t, iamIsTruncated(t, first), "two profiles remain")
	marker, ok := first["Marker"].(string)
	require.True(t, ok, "a truncated page carries the Marker to resume from")
	require.NotEmpty(t, marker)

	seen := iamProfileNames(t, first)
	page := first
	for i := 0; iamIsTruncated(t, page); i++ {
		require.Less(t, i, 10, "the walk must terminate; a marker that never advances would not")
		next, isStr := page["Marker"].(string)
		require.True(t, isStr, "a truncated page always carries a Marker")
		page = iamListProfiles(t, srv, map[string]string{"MaxItems": "1", "Marker": next})
		seen = append(seen, iamProfileNames(t, page)...)
	}

	assert.Equal(t, []string{"profile-a", "profile-b", "profile-c"}, seen,
		"each profile exactly once, in the order the shared paginator sorts keys into")
	_, hasMarker := page["Marker"]
	assert.False(t, hasMarker, "the last page reports no Marker, so a caller knows to stop")
}

// TestListInstanceProfiles_RefusesAnOutOfRangeMaxItems asserts the guard #868 applied to the
// other paginated IAM operations now reaches this one too.
//
// #868 could not cover it: that fix range-checks a decoded MaxItems and there was nothing
// decoded here. ListUsers is asserted alongside so the two cannot drift apart on the bounds.
func TestListInstanceProfiles_RefusesAnOutOfRangeMaxItems(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamSeedProfiles(t, srv, []iamProfileSpec{{name: "profile-a"}})

	for _, operation := range []string{"ListInstanceProfiles", "ListUsers"} {
		for _, maxItems := range []string{"0", "-1", "1001"} {
			t.Run(operation+"/"+maxItems, func(t *testing.T) {
				t.Parallel()
				resp := iamFormRequest(t, srv, operation, map[string]string{"MaxItems": maxItems})
				require.Equal(t, http.StatusBadRequest, resp.StatusCode)
				var result map[string]any
				decodeIAMXML(t, resp, &result)
				assert.Equal(t, "ValidationError", result["__type"],
					"the operation publishes only ServiceFailure, so the code comes from CommonErrors")
				assert.Contains(t, result["message"], "MaxItems must be between 1 and 1000")
			})
		}
	}

	t.Run("the published bounds themselves are accepted", func(t *testing.T) {
		t.Parallel()
		for _, maxItems := range []string{"1", "1000"} {
			result := iamListProfiles(t, srv, map[string]string{"MaxItems": maxItems})
			assert.Len(t, iamProfileNames(t, result), 1)
		}
	})

	t.Run("present but empty is accepted", func(t *testing.T) {
		// By decision, not omission: a form body carrying "MaxItems=" expressed no limit, so it
		// takes the documented default. iam_max_items.go records the rule.
		t.Parallel()
		result := iamListProfiles(t, srv, map[string]string{"MaxItems": ""})
		assert.Len(t, iamProfileNames(t, result), 1)
	})
}

// TestListInstanceProfiles_AnAbsentMaxItemsTakesTheDocumentedDefault asserts the default of 100
// is applied and reported truthfully, rather than every profile being returned under
// IsTruncated=false.
//
// 101 profiles is the smallest count that tells the two apart: at 100 or fewer, a handler that
// ignored MaxItems entirely would give the same answer.
func TestListInstanceProfiles_AnAbsentMaxItemsTakesTheDocumentedDefault(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	specs := make([]iamProfileSpec, 0, 101)
	for i := range 101 {
		// Zero-padded so the sorted key order is the creation order, and the last page's single
		// profile is a named one rather than whichever name happened to sort last.
		specs = append(specs, iamProfileSpec{name: iamPaddedProfileName(i)})
	}
	iamSeedProfiles(t, srv, specs)

	first := iamListProfiles(t, srv, nil)
	assert.Len(t, iamProfileNames(t, first), 100, "the documented default is 100, not everything")
	assert.True(t, iamIsTruncated(t, first),
		"one profile remains, and a caller told otherwise would stop paging with it unseen")

	marker, ok := first["Marker"].(string)
	require.True(t, ok)
	last := iamListProfiles(t, srv, map[string]string{"Marker": marker})
	assert.Equal(t, []string{iamPaddedProfileName(100)}, iamProfileNames(t, last))
	assert.False(t, iamIsTruncated(t, last))
}

// iamPaddedProfileName names the i-th profile so lexicographic order matches numeric order.
func iamPaddedProfileName(i int) string {
	digits := []byte{byte('0' + i/100), byte('0' + (i/10)%10), byte('0' + i%10)}
	return "profile-" + string(digits)
}

// TestListInstanceProfiles_PathPrefixFiltersBeforeThePageIsCut is the criterion that fails
// against the filter-after-paginate order listUsers and listRoles use.
//
// Two profiles outside the prefix sort ahead of the one inside it, and MaxItems is 1. Filtering
// after the page is cut yields a page holding only the first non-matching key, which is then
// discarded — so the caller receives an empty page while IsTruncated says there is more, and has
// no way to tell that from an account with no profiles under that path.
func TestListInstanceProfiles_PathPrefixFiltersBeforeThePageIsCut(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamSeedProfiles(t, srv, []iamProfileSpec{
		{name: "aaa-one"},
		{name: "aaa-two"},
		{name: "zzz-target", path: "/service-role/"},
	})

	result := iamListProfiles(t, srv,
		map[string]string{"MaxItems": "1", "PathPrefix": "/service-role/"})

	assert.Equal(t, []string{"zzz-target"}, iamProfileNames(t, result),
		"the page is cut from the matches, not from every key in the account")
	assert.False(t, iamIsTruncated(t, result),
		"one match means the listing is complete; reporting truncation would send a caller "+
			"paging after a page that is already the whole answer")
	_, hasMarker := result["Marker"]
	assert.False(t, hasMarker)
}

// TestListInstanceProfiles_PathPrefixSelectsOnlyItsPath asserts the filter is a prefix match on
// the profile's stored Path, including a partial path segment and the documented default.
func TestListInstanceProfiles_PathPrefixSelectsOnlyItsPath(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamSeedProfiles(t, srv, []iamProfileSpec{
		{name: "root"},
		{name: "service", path: "/service-role/"},
		{name: "infra", path: "/team/infra/"},
		{name: "ops", path: "/team/ops/"},
	})

	cases := []struct {
		name       string
		pathPrefix string
		want       []string
	}{
		{"absent lists everything", "", []string{"infra", "ops", "root", "service"}},
		{"the documented default of / lists everything", "/", []string{"infra", "ops", "root", "service"}},
		{"one full path", "/service-role/", []string{"service"}},
		{"a parent path spans its children", "/team/", []string{"infra", "ops"}},
		// No trailing slash, which ListPolicies refuses and this operation must not: the two
		// publish different patterns, and a prefix match needs no trailing slash to be
		// meaningful.
		{"no trailing slash", "/team", []string{"infra", "ops"}},
		{"a path nothing is under", "/nobody/", nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			params := map[string]string{}
			if tc.pathPrefix != "" {
				params["PathPrefix"] = tc.pathPrefix
			}
			result := iamListProfiles(t, srv, params)
			assert.ElementsMatch(t, tc.want, iamProfileNames(t, result))
			assert.False(t, iamIsTruncated(t, result))
		})
	}
}

// TestListInstanceProfiles_RefusesAMalformedPathPrefix pins the decision to validate the
// parameter rather than leaving it advisory.
//
// A prefix without its leading slash matches no profile, and a silent empty result is
// indistinguishable from "nothing is under that path" — the accepted-and-ignored failure this
// issue is about, moved from the parameter to its value.
func TestListInstanceProfiles_RefusesAMalformedPathPrefix(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamSeedProfiles(t, srv, []iamProfileSpec{{name: "service", path: "/service-role/"}})

	cases := []struct {
		name        string
		pathPrefix  string
		wantMessage string
	}{
		{"no leading slash", "service-role/", "must begin with /"},
		{"a bare name", "service-role", "must begin with /"},
		// Space is U+0020, one below the published range's lower bound of U+0021.
		{"a space", "/service role/", "printable ASCII"},
		{"a non-ASCII character", "/service-rôle/", "printable ASCII"},
		{"longer than the published 512", "/" + strings.Repeat("x", 512), "between 1 and 512"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp := iamFormRequest(t, srv, "ListInstanceProfiles",
				map[string]string{"PathPrefix": tc.pathPrefix})
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, "ValidationError", result["__type"])
			assert.Contains(t, result["message"], tc.wantMessage)
		})
	}

	t.Run("512 characters is accepted", func(t *testing.T) {
		// The bound itself, so the refusal cannot be off by one in the direction that fails a
		// call IAM accepts.
		t.Parallel()
		result := iamListProfiles(t, srv,
			map[string]string{"PathPrefix": "/" + strings.Repeat("x", 511)})
		assert.Empty(t, iamProfileNames(t, result))
	})
}

// TestListInstanceProfiles_PathPrefixIsRefusedBeforeMaxItems asserts the two checks do not
// disagree about which of a doubly-invalid request to report.
//
// The order is the one every other IAM listing uses — request shape before anything else — and a
// caller that fixed the MaxItems it was told about only to be refused again for the prefix would
// have been sent round the loop twice.
func TestListInstanceProfiles_PathPrefixIsRefusedBeforeMaxItems(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	resp := iamFormRequest(t, srv, "ListInstanceProfiles",
		map[string]string{"PathPrefix": "service-role/", "MaxItems": "1001"})
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, "ValidationError", result["__type"])
	assert.Contains(t, result["message"], "pathPrefix")
}

// TestListInstanceProfiles_AProfileKeepsItsRolesUnderAFilteredPage asserts the filter narrows the
// set without narrowing the record: a paged, filtered listing still carries the roles the
// unpaged one does.
//
// Worth its own test because the page is now built from a map keyed by state key rather than by
// appending in scan order, so a mis-keyed lookup would render an empty member in place of the
// profile and still produce a well-formed response of the right length.
func TestListInstanceProfiles_AProfileKeepsItsRolesUnderAFilteredPage(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamAuthzOK(t, srv, "CreateRole", map[string]string{
		"RoleName": "deploy", "AssumeRolePolicyDocument": iamAuthzTestTrustDoc,
	})
	iamSeedProfiles(t, srv, []iamProfileSpec{
		{name: "other"},
		{name: "with-role", path: "/service-role/"},
	})
	iamAuthzOK(t, srv, "AddRoleToInstanceProfile", map[string]string{
		"InstanceProfileName": "with-role", "RoleName": "deploy",
	})

	result := iamListProfiles(t, srv,
		map[string]string{"PathPrefix": "/service-role/", "MaxItems": "1"})
	require.Equal(t, []string{"with-role"}, iamProfileNames(t, result))

	members, ok := result["InstanceProfiles"].([]any)
	require.True(t, ok)
	entry, ok := members[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "/service-role/", entry["Path"], "the filtered value is the reported one")
	roles, ok := entry["Roles"].([]any)
	require.True(t, ok, "the profile's role survives the filter and the page")
	require.Len(t, roles, 1)
	role, ok := roles[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "deploy", role["RoleName"])
}
