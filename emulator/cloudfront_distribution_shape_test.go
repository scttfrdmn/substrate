package emulator_test

import (
	"encoding/xml"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// One published Distribution, one member set (#1115). API_Distribution marks ARN,
// DistributionConfig, DomainName, Id, InProgressInvalidationBatches, LastModifiedTime and Status
// Required: Yes; Comment and Enabled are DistributionConfig members and belong only inside the nested
// configuration. These tests read the raw document, because the defect under test is members that
// must be absent — a decode into a struct that omits them passes whether they are there or not.

// cfXMLNode is a generic element tree, for asserting member sets.
type cfXMLNode struct {
	XMLName xml.Name
	Text    string      `xml:",chardata"`
	Nodes   []cfXMLNode `xml:",any"`
}

// cfChildNames returns the names of a document's top-level members, in order.
func cfChildNames(t *testing.T, body []byte) (root string, names []string) {
	t.Helper()
	var n cfXMLNode
	require.NoError(t, xml.Unmarshal(body, &n), "%s", body)
	for _, c := range n.Nodes {
		names = append(names, c.XMLName.Local)
	}
	return n.XMLName.Local, names
}

// cfNestedConfig returns the DistributionConfig element a Distribution document nests, verbatim.
var cfNestedConfig = regexp.MustCompile(`(?s)<DistributionConfig>.*</DistributionConfig>`)

func TestCloudFrontDistributionShape_CreateGetAndUpdateAnswerThePublishedMembers(t *testing.T) {
	t.Parallel()
	want := []string{"Id", "ARN", "Status", "LastModifiedTime", "InProgressInvalidationBatches", "DomainName", "DistributionConfig"}

	p, ctx := setupCloudFrontPlugin(t)
	created, err := p.HandleRequest(ctx, cfRequest(http.MethodPost, "/2020-05-31/distribution", nil, cfRMWConfig))
	require.NoError(t, err)
	id := regexp.MustCompile(`<Id>([^<]+)</Id>`).FindStringSubmatch(string(created.Body))[1]

	got, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, "/2020-05-31/distribution/"+id, nil, ""))
	require.NoError(t, err)

	config, etag := cfGetDistributionConfig(t, p, ctx, id)
	put := cfRequest(http.MethodPut, "/2020-05-31/distribution/"+id+"/config", nil,
		strings.Replace(config, "<Comment>rmw</Comment>", "<Comment>rmw-2</Comment>", 1))
	put.Headers["If-Match"] = etag
	updated, err := p.HandleRequest(ctx, put)
	require.NoError(t, err)
	afterUpdate, _ := cfGetDistributionConfig(t, p, ctx, id)

	for _, tc := range []struct {
		name   string
		resp   *emulator.AWSResponse
		status int
		config string // the GetDistributionConfig document the nested one must equal
	}{
		{"CreateDistribution", created, http.StatusCreated, config},
		{"GetDistribution", got, http.StatusOK, config},
		{"UpdateDistribution", updated, http.StatusOK, afterUpdate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.status, tc.resp.StatusCode)
			root, names := cfChildNames(t, tc.resp.Body)
			assert.Equal(t, "Distribution", root)
			assert.Equal(t, want, names, "exactly the seven Required: Yes members, no Comment or Enabled beside them")
			assert.NotEmpty(t, tc.resp.Headers["ETag"])

			nested := cfNestedConfig.FindString(string(tc.resp.Body))
			assert.Equal(t, tc.config, nested,
				"the nested configuration is exactly what GetDistributionConfig answers, so the two cannot diverge")
		})
	}
}

// TestCloudFrontDistributionShape_InProgressInvalidationBatchesCountsTheIndex: substrate completes an
// invalidation when it is created, so the count is 0 with invalidations present — answered from the
// index, not omitted.
func TestCloudFrontDistributionShape_InProgressInvalidationBatchesCountsTheIndex(t *testing.T) {
	t.Parallel()
	p, ctx := setupCloudFrontPlugin(t)
	id := createTestDistribution(t, p, ctx, "inv-count")
	_, err := p.HandleRequest(ctx, cfRequest(http.MethodPost, "/2020-05-31/distribution/"+id+"/invalidation", nil,
		`<InvalidationBatch><Paths><Quantity>1</Quantity><Items><Path>/*</Path></Items></Paths><CallerReference>i1</CallerReference></InvalidationBatch>`))
	require.NoError(t, err)

	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, "/2020-05-31/distribution/"+id, nil, ""))
	require.NoError(t, err)
	var dist struct {
		InProgress *int `xml:"InProgressInvalidationBatches"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &dist))
	require.NotNil(t, dist.InProgress, "InProgressInvalidationBatches is Required: Yes: %s", resp.Body)
	assert.Equal(t, 0, *dist.InProgress, "a Completed invalidation is not in progress")
}

// TestCloudFrontDistributionShape_ListDistributionsSummarizesTheStoredConfiguration: a summary
// copies its configuration members from the stored configuration, answers Comment even when empty
// (Required: Yes), and answers LastModifiedTime and ETag, which it omitted until #1115. A member the
// create never sent is absent rather than invented.
func TestCloudFrontDistributionShape_ListDistributionsSummarizesTheStoredConfiguration(t *testing.T) {
	t.Parallel()
	p, ctx := setupCloudFrontPlugin(t)
	_, err := p.HandleRequest(ctx, cfRequest(http.MethodPost, "/2020-05-31/distribution", nil, cfRMWConfig))
	require.NoError(t, err)
	_, err = p.HandleRequest(ctx, cfRequest(http.MethodPost, "/2020-05-31/distribution", nil,
		`<DistributionConfig><CallerReference>bare</CallerReference><Enabled>false</Enabled></DistributionConfig>`))
	require.NoError(t, err)

	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, "/2020-05-31/distribution", nil, ""))
	require.NoError(t, err)
	var list struct {
		Items []cfXMLNode `xml:"Items>DistributionSummary"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &list))
	require.Len(t, list.Items, 2)

	byComment := map[string][]string{}
	for _, item := range list.Items {
		var names []string
		comment := "<absent>"
		for _, c := range item.Nodes {
			names = append(names, c.XMLName.Local)
			if c.XMLName.Local == "Comment" {
				comment = c.Text
			}
		}
		byComment[comment] = names
	}

	assert.Equal(t, []string{
		"Id", "ARN", "ETag", "Status", "LastModifiedTime", "DomainName",
		"Aliases", "Origins", "DefaultCacheBehavior", "CacheBehaviors", "Comment", "PriceClass", "Enabled",
	}, byComment["rmw"], "the summary carries the configuration members the create sent, in the summary's order")
	assert.Equal(t, []string{
		"Id", "ARN", "ETag", "Status", "LastModifiedTime", "DomainName", "Aliases", "Comment", "Enabled",
	}, byComment[""], "an empty Comment is answered, and members never sent are absent")
}
