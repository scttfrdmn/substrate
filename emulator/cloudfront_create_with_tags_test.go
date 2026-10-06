package emulator_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cfWire sends one CloudFront REST request through a full test server.
func cfWire(t *testing.T, ts *emulator.TestServer, method, pathAndQuery, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+pathAndQuery, bytes.NewReader([]byte(body)))
	require.NoError(t, err)
	req.Host = "cloudfront.amazonaws.com"
	req.Header.Set("Content-Type", "application/xml")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, out
}

// TestCloudFrontCreateDistributionWithTags_OverTheWire pins #1133 end to end: the `?WithTags` create
// keeps the two DistributionConfig members a mis-route once defaulted, Enabled false and a Comment,
// and its tag set is readable through both ListTagsForResource and GetResources (#765).
func TestCloudFrontCreateDistributionWithTags_OverTheWire(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	body := `<DistributionConfigWithTags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">` +
		`<DistributionConfig><CallerReference>wt-1</CallerReference><Comment>tagged and disabled</Comment><Enabled>false</Enabled></DistributionConfig>` +
		`<Tags><Items><Tag><Key>env</Key><Value>test</Value></Tag><Tag><Key>team</Key><Value>edge</Value></Tag></Items></Tags>` +
		`</DistributionConfigWithTags>`
	status, out := cfWire(t, ts, http.MethodPost, "/2020-05-31/distribution?WithTags", body)
	require.Equal(t, http.StatusCreated, status, "%s", out)

	var dist struct {
		ID  string `xml:"Id"`
		ARN string `xml:"ARN"`
	}
	require.NoError(t, xml.Unmarshal(out, &dist), "%s", out)

	// Read back through GetDistributionConfig: the create's own <Distribution> does not yet carry its
	// DistributionConfig, which is #1115's.
	status, out = cfWire(t, ts, http.MethodGet, "/2020-05-31/distribution/"+dist.ID+"/config", "")
	require.Equal(t, http.StatusOK, status, "%s", out)
	require.Contains(t, string(out), "<Comment>tagged and disabled</Comment>")
	require.Contains(t, string(out), "<Enabled>false</Enabled>")

	status, out = cfWire(t, ts, http.MethodGet, "/2020-05-31/tagging?Resource="+dist.ARN, "")
	require.Equal(t, http.StatusOK, status, "%s", out)
	var tags struct {
		Items []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"Items>Tag"`
	}
	require.NoError(t, xml.Unmarshal(out, &tags), "%s", out)
	got := map[string]string{}
	for _, tag := range tags.Items {
		got[tag.Key] = tag.Value
	}
	want := map[string]string{"env": "test", "team": "edge"}
	require.Equal(t, want, got, "ListTagsForResource")

	status, out = cicdCall(t, ts, "tagging.us-east-1.amazonaws.com", "ResourceGroupsTaggingAPI_20170126", "GetResources",
		map[string]any{"ResourceTypeFilters": []string{"cloudfront:distribution"}})
	require.Equal(t, http.StatusOK, status, "GetResources: %s", out)
	var scanned struct {
		List []struct {
			ARN  string `json:"ResourceARN"`
			Tags []struct {
				Key   string `json:"Key"`
				Value string `json:"Value"`
			} `json:"Tags"`
		} `json:"ResourceTagMappingList"`
	}
	require.NoError(t, json.Unmarshal(out, &scanned), "%s", out)
	var found map[string]string
	for _, m := range scanned.List {
		if m.ARN == dist.ARN {
			found = map[string]string{}
			for _, tag := range m.Tags {
				found[tag.Key] = tag.Value
			}
		}
	}
	require.Equal(t, want, found, "GetResources: %s", out)
}
