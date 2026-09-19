package emulator_test

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// GetDistributionConfig's shape, and the one input AWS answers that substrate refuses (#1091).
//
// API_GetDistributionConfig returns a DistributionConfig and nothing else. Substrate rendered
// `Id` and `ARN` inside it — both members of the enclosing Distribution type, published one
// level up — so a caller could read a distribution's identity out of a document that does not
// carry it and would find nothing there against real CloudFront (#1013).
//
// The assertions are on the raw document rather than on a decoded struct, because the members
// under test are the ones that must be **absent**: an unmarshal into a struct that omits them
// passes whether they are there or not.

// cloudfrontConfigServer starts a server holding one distribution created with the given
// DistributionConfig body, and returns the server and the distribution's ID.
func cloudfrontConfigServer(t *testing.T, createBody string) (*emulator.TestServer, string) {
	t.Helper()

	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
	status, body := cloudfrontRequest(t, ts, http.MethodPost, "/2020-05-31/distribution", "", createBody)
	require.Equal(t, http.StatusCreated, status, "CreateDistribution: %s", body)

	var created struct {
		XMLName xml.Name `xml:"Distribution"`
		ID      string   `xml:"Id"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &created), "decode CreateDistribution: %s", body)
	require.NotEmpty(t, created.ID, "CreateDistribution reports an Id")
	return ts, created.ID
}

// TestCloudFront_ADistributionConfigCarriesOnlyItsOwnMembers is the leak read back.
//
// Id and ARN must be absent from the configuration and present on the distribution, which is
// the whole content of the divergence: the two operations return two different published types
// and substrate answered the same fields for both.
func TestCloudFront_ADistributionConfigCarriesOnlyItsOwnMembers(t *testing.T) {
	ts, distID := cloudfrontConfigServer(t,
		`<DistributionConfig><Comment>a described distribution</Comment><Enabled>true</Enabled></DistributionConfig>`)

	status, config := cloudfrontRequest(t, ts, http.MethodGet,
		"/2020-05-31/distribution/"+distID+"/config", "", "")
	require.Equal(t, http.StatusOK, status, "GetDistributionConfig: %s", config)

	var decoded struct {
		XMLName xml.Name `xml:"DistributionConfig"`
		Comment string   `xml:"Comment"`
		Enabled bool     `xml:"Enabled"`
	}
	require.NoError(t, xml.Unmarshal([]byte(config), &decoded), "decode DistributionConfig: %s", config)
	assert.Equal(t, "a described distribution", decoded.Comment)
	assert.True(t, decoded.Enabled)

	// The two members that belong to Distribution, not to DistributionConfig.
	assert.NotContains(t, config, "<Id>", "Id is a Distribution member, published one level up")
	assert.NotContains(t, config, "<ARN>", "ARN is a Distribution member, published one level up")

	// The three Required: Yes members substrate records no value for. Asserted absent so the
	// recorded divergence is pinned: if a later change starts answering them, it answers them
	// deliberately rather than with an invented empty shape.
	for _, member := range []string{"CallerReference", "Origins", "DefaultCacheBehavior"} {
		assert.NotContains(t, config, "<"+member+">",
			"%s is Required: Yes and CreateDistribution records no value for it", member)
	}

	// The anchor: the sibling operation returns a Distribution, where both members are
	// published and must appear. (That document carries Comment and Enabled too, which are
	// DistributionConfig members rather than Distribution ones — a separate leak, in a
	// separate function, not this issue's.)
	status, dist := cloudfrontRequest(t, ts, http.MethodGet, "/2020-05-31/distribution/"+distID, "", "")
	require.Equal(t, http.StatusOK, status, "GetDistribution: %s", dist)
	assert.Contains(t, dist, "<Id>"+distID+"</Id>", "Distribution publishes Id as Required: Yes")
	assert.Contains(t, dist, "<ARN>", "Distribution publishes ARN as Required: Yes")
}

// TestCloudFront_AnEmptyCommentIsStillAnswered covers the omitempty that was dropped.
//
// Comment is Required: Yes on API_DistributionConfig and the Response Syntax renders it
// unconditionally, so a distribution created without one must answer an empty element rather
// than drop the member.
func TestCloudFront_AnEmptyCommentIsStillAnswered(t *testing.T) {
	ts, distID := cloudfrontConfigServer(t, `<DistributionConfig><Enabled>true</Enabled></DistributionConfig>`)

	status, config := cloudfrontRequest(t, ts, http.MethodGet,
		"/2020-05-31/distribution/"+distID+"/config", "", "")
	require.Equal(t, http.StatusOK, status, "GetDistributionConfig: %s", config)
	assert.Contains(t, config, "<Comment></Comment>",
		"a Required: Yes member is answered empty, not omitted")
}

// TestCloudFront_AnEmptyDistributionIDIsRefusedNotAnswered pins the deliberate divergence.
//
// API_GetDistributionConfig publishes, on its Id parameter: "The distribution's ID. If the ID
// is empty, an empty distribution configuration is returned." Substrate refuses instead, and
// the reason is that the page publishes no example of that empty configuration while
// DistributionConfig marks five members Required: Yes — answering it means inventing a shape.
// The refusal is the published code at the published status (NoSuchDistribution/404), so a
// caller is told something true about substrate; it is simply not what AWS says for this input.
//
// The code is also what proves the route resolved: an unrouted path answers
// UnknownOperationException, so NoSuchDistribution means the empty ID reached the handler.
func TestCloudFront_AnEmptyDistributionIDIsRefusedNotAnswered(t *testing.T) {
	ts, _ := cloudfrontConfigServer(t,
		`<DistributionConfig><Comment>present</Comment><Enabled>true</Enabled></DistributionConfig>`)

	for _, tc := range []struct {
		name      string
		method    string
		path      string
		body      string
		operation string
	}{
		{
			name:      "GetDistributionConfig",
			method:    http.MethodGet,
			path:      "/2020-05-31/distribution//config",
			operation: "GetDistributionConfig",
		},
		{
			name:      "UpdateDistribution",
			method:    http.MethodPut,
			path:      "/2020-05-31/distribution//config",
			body:      `<DistributionConfig><Comment>rewritten</Comment><Enabled>true</Enabled></DistributionConfig>`,
			operation: "UpdateDistribution",
		},
		{
			name:      "ListInvalidations",
			method:    http.MethodGet,
			path:      "/2020-05-31/distribution//invalidation",
			operation: "ListInvalidations",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := cloudfrontRequest(t, ts, tc.method, tc.path, "", tc.body)
			assert.Equal(t, http.StatusNotFound, status, "%s: %s", tc.operation, body)
			assert.Equal(t, "NoSuchDistribution", cloudfrontErrorCode(t, body),
				"an empty ID reaches %s rather than falling off the router", tc.operation)

			// The message named the ID until #1091, so an empty one trailed a bare colon.
			assert.NotContains(t, body, "does not exist.:")
			assert.False(t, strings.Contains(body, "<Message>Distribution not found: </Message>"),
				"the message no longer trails a colon with nothing after it")
		})
	}

	// A distribution ID that is merely wrong rather than empty is the same refusal, and is the
	// reason the two invalidation handlers changed: neither loaded the distribution.
	for _, tc := range []struct{ name, path string }{
		{name: "ListInvalidations", path: "/2020-05-31/distribution/E0000000000000/invalidation"},
		{name: "GetInvalidation", path: "/2020-05-31/distribution/E0000000000000/invalidation/I1234567890123"},
	} {
		t.Run(tc.name+" on an absent distribution", func(t *testing.T) {
			status, body := cloudfrontRequest(t, ts, http.MethodGet, tc.path, "", "")
			assert.Equal(t, http.StatusNotFound, status, "%s: %s", tc.name, body)
			assert.Equal(t, "NoSuchDistribution", cloudfrontErrorCode(t, body),
				"both codes are published and they name different absences")
		})
	}

	// And the shape that is not an empty ID: a trailing slash is trimmed before routing, so
	// GET /2020-05-31/distribution/ is ListDistributions and not a GetDistribution with no ID.
	// Recorded because the reverse is the natural reading of the arithmetic.
	status, body := cloudfrontRequest(t, ts, http.MethodGet, "/2020-05-31/distribution/", "", "")
	require.Equal(t, http.StatusOK, status, "ListDistributions: %s", body)
	assert.Contains(t, body, "<DistributionList", "a trailing slash lists, it does not load")
}
