package emulator_test

import (
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestCloudFrontWire_DistributionResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for CloudFrontDistribution (#756).
//
// CloudFrontDistribution declares AccountID under a wire-visible `json` tag, never under omitempty,
// and `ever_tagged,omitempty`, which only a tag write sets (#938). So the distribution is tagged and
// both members are read back before any response is walked, since an unset omitempty member is absent
// for free (#1304). CloudFront speaks REST-XML and every response is marshaled from an XML struct
// declared for its operation. Every operation that answers the distribution is driven; TagResource
// and UntagResource answer 204 with no body, and the origin access control operations answer a record
// of their own.
func TestCloudFrontWire_DistributionResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.CloudFrontPlugin{}
	ctx, state := wireSetup(t, p, "req-cloudfront-wire")
	call := func(method, path string, params map[string]string, body string) []byte {
		t.Helper()
		if params == nil {
			params = map[string]string{}
		}
		resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
			Service: "cloudfront", HTTPMethod: method, Path: path, Body: []byte(body),
			Headers: map[string]string{"Content-Type": "application/xml"}, Params: params,
		})
		require.NoError(t, err, "%s %s", method, path)
		require.Truef(t, resp.StatusCode >= 200 && resp.StatusCode < 300, "%s %s answered %d: %s", method, path, resp.StatusCode, resp.Body)
		return resp.Body
	}

	const config = `<DistributionConfig xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><CallerReference>wire-1</CallerReference><Comment>wire</Comment><Enabled>true</Enabled></DistributionConfig>`
	created := call(http.MethodPost, "/2020-05-31/distribution", nil, config)
	m := regexp.MustCompile(`<ARN>(arn:aws:cloudfront::\d+:distribution/([^<]+))</ARN>`).FindSubmatch(created)
	require.NotNil(t, m, "CreateDistribution must report an ARN: %s", created)
	arn, id := string(m[1]), string(m[2])

	tagging := func(op string) map[string]string { return map[string]string{"Operation": op, "Resource": arn} }
	call(http.MethodPost, "/2020-05-31/tagging", tagging("Tag"),
		`<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><Items><Tag><Key>team</Key><Value>wire</Value></Tag></Items></Tags>`)
	record := wireRequireHeld(t, state, "cloudfront", "cfdist:123456789012/"+id, "AccountID")
	require.JSONEq(t, "true", string(record["ever_tagged"]), "the distribution must persist ever_tagged before an absence assertion on it means anything")

	dist := "/2020-05-31/distribution/" + id
	invalidation := `<InvalidationBatch xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><Paths><Quantity>1</Quantity><Items><Path>/*</Path></Items></Paths><CallerReference>wire-inv</CallerReference></InvalidationBatch>`
	members := []string{"AccountID", "EverTagged", "ever_tagged"}
	for _, tc := range []struct {
		op     string
		body   func() []byte
		anchor string
	}{
		{"CreateDistribution", func() []byte { return created }, "<Id>" + id + "</Id>"},
		{"GetDistribution", func() []byte { return call(http.MethodGet, dist, nil, "") }, "<Id>" + id + "</Id>"},
		{"GetDistributionConfig", func() []byte { return call(http.MethodGet, dist+"/config", nil, "") }, "<Comment>wire</Comment>"},
		{"ListDistributions", func() []byte { return call(http.MethodGet, "/2020-05-31/distribution", nil, "") }, "<Id>" + id + "</Id>"},
		{"ListTagsForResource", func() []byte {
			return call(http.MethodGet, "/2020-05-31/tagging", map[string]string{"Resource": arn}, "")
		}, "<Key>team</Key>"},
		{"UpdateDistribution", func() []byte { return call(http.MethodPut, dist+"/config", nil, config) }, "<Id>" + id + "</Id>"},
		{"CreateInvalidation", func() []byte { return call(http.MethodPost, dist+"/invalidation", nil, invalidation) }, "<Status>Completed</Status>"},
		{"ListInvalidations", func() []byte { return call(http.MethodGet, dist+"/invalidation", nil, "") }, "<Id>"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.body()
			require.Containsf(t, string(body), tc.anchor, "presence anchor: %s has to render %s", tc.op, tc.anchor)
			wireAssertNoMemberXML(t, tc.op, body, members, "")
		})
	}
}
