package emulator_test

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cfOACConfigXML is a valid OriginAccessControlConfig: the private-S3-origin case, which is what
// every one of these operations exists to serve.
const cfOACConfigXML = `<OriginAccessControlConfig>` +
	`<Description>substrate test control</Description>` +
	`<Name>substrate-oac</Name>` +
	`<OriginAccessControlOriginType>s3</OriginAccessControlOriginType>` +
	`<SigningBehavior>always</SigningBehavior>` +
	`<SigningProtocol>sigv4</SigningProtocol>` +
	`</OriginAccessControlConfig>`

// cfOACPath is the collection path the four operations are addressed on.
const cfOACPath = "/2020-05-31/origin-access-control"

// cfOACIDPattern is the shape a CloudFront identifier is published in: E and thirteen uppercase
// alphanumerics. Anchored, so a longer ID that merely starts right fails.
var cfOACIDPattern = regexp.MustCompile(`^E[A-Z0-9]{13}$`)

// cfOACDoc is the OriginAccessControl document the create and the get both answer.
type cfOACDoc struct {
	XMLName xml.Name `xml:"OriginAccessControl"`
	ID      string   `xml:"Id"`
	Config  struct {
		Description     string `xml:"Description"`
		Name            string `xml:"Name"`
		OriginType      string `xml:"OriginAccessControlOriginType"`
		SigningBehavior string `xml:"SigningBehavior"`
		SigningProtocol string `xml:"SigningProtocol"`
	} `xml:"OriginAccessControlConfig"`
}

// cfCreateOAC creates one origin access control and returns the decoded document and the response.
func cfCreateOAC(t *testing.T, p *emulator.CloudFrontPlugin, ctx *emulator.RequestContext,
	body string,
) (cfOACDoc, *emulator.AWSResponse) {
	t.Helper()
	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodPost, cfOACPath, nil, body))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "body=%s", resp.Body)

	var doc cfOACDoc
	require.NoError(t, xml.Unmarshal(resp.Body, &doc), "body=%s", resp.Body)
	return doc, resp
}

// TestCloudFrontOAC_CreateThenGetAnswersTheConfigItWasGiven is the round trip the whole family
// exists for: a control created here is readable by the ID the create handed back, with the config
// members it was given and the version it was given.
//
// The ETag is asserted on both responses because it is the value a caller has to carry from the
// create to the delete — a get that answered a *different* version would make a converging
// consumer's delete fail its precondition against a control nothing had changed.
func TestCloudFrontOAC_CreateThenGetAnswersTheConfigItWasGiven(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)

	created, createResp := cfCreateOAC(t, p, ctx, cfOACConfigXML)

	assert.Regexp(t, cfOACIDPattern, created.ID, "an OAC ID has a distribution ID's shape")
	assert.Equal(t, "substrate-oac", created.Config.Name)
	assert.Equal(t, "substrate test control", created.Config.Description)
	assert.Equal(t, "s3", created.Config.OriginType)
	assert.Equal(t, "always", created.Config.SigningBehavior)
	assert.Equal(t, "sigv4", created.Config.SigningProtocol)

	etag := createResp.Headers["ETag"]
	assert.Regexp(t, cfOACIDPattern, etag, "the create answers a version in the ETag header")
	assert.Equal(t, "https://cloudfront.amazonaws.com/2020-05-31/origin-access-control/"+created.ID,
		createResp.Headers["Location"], "and the Location of the resource it made")

	getResp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, cfOACPath+"/"+created.ID, nil, ""))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, getResp.StatusCode, "body=%s", getResp.Body)

	var fetched cfOACDoc
	require.NoError(t, xml.Unmarshal(getResp.Body, &fetched))
	assert.Equal(t, created, fetched, "the get answers the document the create answered")
	assert.Equal(t, etag, getResp.Headers["ETag"], "and the same version")
}

// TestCloudFrontOAC_AnUnusedAccountAnswersNoItemsElement pins the reference's own sentence: "If
// you're not using origin access controls for your AWS account, the ListOriginAccessControls
// operation doesn't return the Items element in the response."
//
// Asserted on the raw XML rather than on a decoded shape, because a decoder cannot tell an absent
// element from an empty one — which is exactly the distinction being claimed.
func TestCloudFrontOAC_AnUnusedAccountAnswersNoItemsElement(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)

	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, cfOACPath, nil, ""))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body := string(resp.Body)
	assert.Contains(t, body, "<OriginAccessControlList>")
	assert.Contains(t, body, "<Quantity>0</Quantity>")
	assert.NotContains(t, body, "<Items>", "an account using no OACs answers no Items element")
}

// TestCloudFrontOAC_ListReportsWhatWasCreated covers the other side: once a control exists, the
// list carries a summary for it — flattened, not a nested config.
func TestCloudFrontOAC_ListReportsWhatWasCreated(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)

	first, _ := cfCreateOAC(t, p, ctx, cfOACConfigXML)
	second, _ := cfCreateOAC(t, p, ctx, strings.Replace(cfOACConfigXML,
		"<Name>substrate-oac</Name>", "<Name>substrate-oac-2</Name>", 1))
	require.NotEqual(t, first.ID, second.ID, "two creates mint two IDs")

	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, cfOACPath, nil, ""))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var list struct {
		XMLName     xml.Name `xml:"OriginAccessControlList"`
		IsTruncated bool     `xml:"IsTruncated"`
		Quantity    int      `xml:"Quantity"`
		Items       []struct {
			ID              string `xml:"Id"`
			Name            string `xml:"Name"`
			Description     string `xml:"Description"`
			OriginType      string `xml:"OriginAccessControlOriginType"`
			SigningBehavior string `xml:"SigningBehavior"`
			SigningProtocol string `xml:"SigningProtocol"`
		} `xml:"Items>OriginAccessControlSummary"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &list), "body=%s", resp.Body)

	assert.False(t, list.IsTruncated, "the list is answered whole")
	assert.Equal(t, 2, list.Quantity)
	require.Len(t, list.Items, 2)

	byID := map[string]string{}
	for _, item := range list.Items {
		byID[item.ID] = item.Name
		assert.Equal(t, "s3", item.OriginType, "a summary carries the config's members")
		assert.Equal(t, "always", item.SigningBehavior)
		assert.Equal(t, "sigv4", item.SigningProtocol)
		assert.Equal(t, "substrate test control", item.Description)
	}
	assert.Equal(t, map[string]string{
		first.ID: "substrate-oac", second.ID: "substrate-oac-2",
	}, byID)
}

// TestCloudFrontOAC_AnInvalidConfigIsRefused walks the four Required: Yes members and the three
// published enums. Every refusal is InvalidArgument/400, which is the code the operation publishes
// for an invalid argument of any kind; the table exists to prove none of the seven is *accepted*,
// since a create that ignored SigningBehavior would answer 201 here and fail against AWS.
func TestCloudFrontOAC_AnInvalidConfigIsRefused(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "no Name",
			body: strings.Replace(cfOACConfigXML, "<Name>substrate-oac</Name>", "", 1),
		},
		{
			name: "no OriginAccessControlOriginType",
			body: strings.Replace(cfOACConfigXML,
				"<OriginAccessControlOriginType>s3</OriginAccessControlOriginType>", "", 1),
		},
		{
			name: "no SigningBehavior",
			body: strings.Replace(cfOACConfigXML,
				"<SigningBehavior>always</SigningBehavior>", "", 1),
		},
		{
			name: "no SigningProtocol",
			body: strings.Replace(cfOACConfigXML,
				"<SigningProtocol>sigv4</SigningProtocol>", "", 1),
		},
		{
			name: "an origin type outside the enum",
			body: strings.Replace(cfOACConfigXML,
				"<OriginAccessControlOriginType>s3</OriginAccessControlOriginType>",
				"<OriginAccessControlOriginType>S3</OriginAccessControlOriginType>", 1),
		},
		{
			name: "a signing behavior outside the enum",
			body: strings.Replace(cfOACConfigXML,
				"<SigningBehavior>always</SigningBehavior>",
				"<SigningBehavior>sometimes</SigningBehavior>", 1),
		},
		{
			name: "a signing protocol outside the enum",
			body: strings.Replace(cfOACConfigXML,
				"<SigningProtocol>sigv4</SigningProtocol>",
				"<SigningProtocol>sigv2</SigningProtocol>", 1),
		},
		{
			name: "a body of another shape entirely",
			body: `<DistributionConfig><Comment>wrong document</Comment></DistributionConfig>`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, ctx := setupCloudFrontPlugin(t)

			resp, err := p.HandleRequest(ctx, cfRequest(http.MethodPost, cfOACPath, nil, tc.body))
			assert.Nil(t, resp)

			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			assert.Equal(t, "InvalidArgument", awsErr.Code)
			assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)

			// And nothing was recorded: a refused create must not leave a control behind.
			listResp, listErr := p.HandleRequest(ctx, cfRequest(http.MethodGet, cfOACPath, nil, ""))
			require.NoError(t, listErr)
			assert.Contains(t, string(listResp.Body), "<Quantity>0</Quantity>")
		})
	}
}

// TestCloudFrontOAC_DeleteRequiresTheCurrentVersion is the precondition contract, and the reason
// the record stores an ETag at all.
//
// The three refusals are asserted separately because they are three different published codes for
// three different mistakes, and collapsing any two of them tells a caller something untrue: a
// missing If-Match reported as PreconditionFailed says a version was stale when none was sent, and
// a stale one reported as NoSuchOriginAccessControl says the control is gone when it is not.
func TestCloudFrontOAC_DeleteRequiresTheCurrentVersion(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)

	created, createResp := cfCreateOAC(t, p, ctx, cfOACConfigXML)
	etag := createResp.Headers["ETag"]
	path := cfOACPath + "/" + created.ID

	t.Run("an unknown id is NoSuchOriginAccessControl", func(t *testing.T) {
		req := cfRequest(http.MethodDelete, cfOACPath+"/ENOSUCHCONTROL", nil, "")
		req.Headers["If-Match"] = etag
		resp, err := p.HandleRequest(ctx, req)
		assert.Nil(t, resp)

		var awsErr *emulator.AWSError
		require.ErrorAs(t, err, &awsErr)
		assert.Equal(t, "NoSuchOriginAccessControl", awsErr.Code)
		assert.Equal(t, http.StatusNotFound, awsErr.HTTPStatus)
	})

	t.Run("no If-Match is InvalidIfMatchVersion", func(t *testing.T) {
		resp, err := p.HandleRequest(ctx, cfRequest(http.MethodDelete, path, nil, ""))
		assert.Nil(t, resp)

		var awsErr *emulator.AWSError
		require.ErrorAs(t, err, &awsErr)
		assert.Equal(t, "InvalidIfMatchVersion", awsErr.Code)
		assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
	})

	t.Run("a stale If-Match is PreconditionFailed", func(t *testing.T) {
		req := cfRequest(http.MethodDelete, path, nil, "")
		req.Headers["If-Match"] = "EOUTOFDATE0000"
		resp, err := p.HandleRequest(ctx, req)
		assert.Nil(t, resp)

		var awsErr *emulator.AWSError
		require.ErrorAs(t, err, &awsErr)
		assert.Equal(t, "PreconditionFailed", awsErr.Code)
		assert.Equal(t, http.StatusPreconditionFailed, awsErr.HTTPStatus)
	})

	t.Run("the current version deletes it", func(t *testing.T) {
		// Quoted, and folded: the header reader is case-insensitive and a quoted ETag is
		// tolerated, so a caller that re-quotes what substrate handed it still succeeds.
		req := cfRequest(http.MethodDelete, path, nil, "")
		req.Headers["if-match"] = `"` + etag + `"`
		resp, err := p.HandleRequest(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, resp.StatusCode)
		assert.Empty(t, resp.Body, "the documented response has an empty body")

		getResp, getErr := p.HandleRequest(ctx, cfRequest(http.MethodGet, path, nil, ""))
		assert.Nil(t, getResp)
		var awsErr *emulator.AWSError
		require.ErrorAs(t, getErr, &awsErr)
		assert.Equal(t, "NoSuchOriginAccessControl", awsErr.Code)

		listResp, listErr := p.HandleRequest(ctx, cfRequest(http.MethodGet, cfOACPath, nil, ""))
		require.NoError(t, listErr)
		assert.NotContains(t, string(listResp.Body), created.ID,
			"a deleted control leaves the account's index too")
	})
}

// TestCloudFrontOAC_ControlsAreScopedToTheCallingAccount pins that the record is read back under
// the account that created it: another account's get answers the absence, not the control.
func TestCloudFrontOAC_ControlsAreScopedToTheCallingAccount(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)
	created, _ := cfCreateOAC(t, p, ctx, cfOACConfigXML)

	other := &emulator.RequestContext{AccountID: "999988887777", Region: "us-east-1", RequestID: "req-2"}

	resp, err := p.HandleRequest(other, cfRequest(http.MethodGet, cfOACPath+"/"+created.ID, nil, ""))
	assert.Nil(t, resp)
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "NoSuchOriginAccessControl", awsErr.Code)

	listResp, listErr := p.HandleRequest(other, cfRequest(http.MethodGet, cfOACPath, nil, ""))
	require.NoError(t, listErr)
	assert.NotContains(t, string(listResp.Body), created.ID)
}

// TestCloudFrontOAC_AnUnpublishedMethodIsRefused pins that the path family does not become a
// catch-all: a PUT on a control — which would be UpdateOriginAccessControl, an operation substrate
// does not implement — is refused rather than resolving to one of the four.
func TestCloudFrontOAC_AnUnpublishedMethodIsRefused(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)
	created, _ := cfCreateOAC(t, p, ctx, cfOACConfigXML)

	resp, err := p.HandleRequest(ctx,
		cfRequest(http.MethodPut, cfOACPath+"/"+created.ID, nil, cfOACConfigXML))
	assert.Nil(t, resp)
	require.Error(t, err, "an unimplemented operation is refused, not silently routed")
}

// TestCloudFront_CreateDistributionWithTagsTagsTheDistributionItCreates is the assertion a
// consumer's teardown verification makes: the tags sent with the create are readable through
// ListTagsForResource afterwards.
//
// One call, not two — which is why it is worth a test. The operation is documented as requiring
// both the CreateDistribution and the TagResource permission, and a substrate that routed
// `?WithTags` to the plain create would answer the same 201 while storing no tags at all.
func TestCloudFront_CreateDistributionWithTagsTagsTheDistributionItCreates(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)

	body := `<DistributionConfigWithTags>` +
		cfDistributionConfigXML +
		`<Tags><Items>` +
		`<Tag><Key>env</Key><Value>test</Value></Tag>` +
		`<Tag><Key>owner</Key><Value>substrate</Value></Tag>` +
		`</Items></Tags>` +
		`</DistributionConfigWithTags>`

	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodPost, "/2020-05-31/distribution",
		map[string]string{"WithTags": "1"}, body))
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "body=%s", resp.Body)

	var dist struct {
		XMLName xml.Name `xml:"Distribution"`
		ID      string   `xml:"Id"`
		ARN     string   `xml:"ARN"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &dist))
	require.NotEmpty(t, dist.ID, "the tagged create answers the same Distribution document")

	tagsResp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, "/2020-05-31/tagging",
		map[string]string{"Resource": dist.ARN}, ""))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, tagsResp.StatusCode)

	var tags struct {
		XMLName xml.Name `xml:"Tags"`
		Items   []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"Items>Tag"`
	}
	require.NoError(t, xml.Unmarshal(tagsResp.Body, &tags), "body=%s", tagsResp.Body)

	got := map[string]string{}
	for _, tag := range tags.Items {
		got[tag.Key] = tag.Value
	}
	assert.Equal(t, map[string]string{"env": "test", "owner": "substrate"}, got)
}

// TestCloudFront_CreateDistributionWithTagsRefusesABodyItCannotRead is the other half of the same
// operation, and the one #883's argument applies to: a caller that asked for tags must not be
// answered 201 by a create that dropped them.
func TestCloudFront_CreateDistributionWithTagsRefusesABodyItCannotRead(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)

	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodPost, "/2020-05-31/distribution",
		map[string]string{"WithTags": "1"}, `<DistributionConfigWithTags><Tags>`))
	assert.Nil(t, resp)

	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "InvalidArgument", awsErr.Code)
	assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)

	listResp, listErr := p.HandleRequest(ctx,
		cfRequest(http.MethodGet, "/2020-05-31/distribution", nil, ""))
	require.NoError(t, listErr)
	assert.Contains(t, string(listResp.Body), "<Quantity>0</Quantity>",
		"a refused tagged create leaves no distribution behind")
}

// TestReplay_ACloudFrontCreateReplaysWithTheIdentifiersItMinted is #856's claim for CloudFront:
// the distribution ID, the invalidation ID and the origin access control's ID and ETag all come
// from the request's own mint, so a recorded stream replays byte-identically.
//
// The stream interlocks deliberately. The invalidation is created *inside* the recorded
// distribution and the delete names the recorded control's ETag, so a single re-minted value is
// not a cosmetic difference: the request naming it answers NoSuchDistribution or
// PreconditionFailed instead of succeeding.
func TestReplay_ACloudFrontCreateReplaysWithTheIdentifiersItMinted(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t,
		emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	require.True(t, ts.Store().RecordsStateHashes(), "precondition: state_hash_after is compared")

	// Frozen for the reason the EC2 and shared-mint replay tests freeze: a replay pins the clock
	// to the recorded event's timestamp, and a handler stamping a record off a live clock
	// diverges in a state hash for a reason that has nothing to do with an identifier.
	ts.FreezeTime()

	var created cfOACDoc
	createBody, createHeaders := cfReplayCall(t, ts, http.MethodPost, cfOACPath, cfOACConfigXML)
	require.NoError(t, xml.Unmarshal(createBody, &created))
	require.NotEmpty(t, created.ID)
	etag := createHeaders.Get("ETag")
	require.NotEmpty(t, etag, "the delete below needs the version the create handed out")

	cfReplayCall(t, ts, http.MethodGet, cfOACPath+"/"+created.ID, "")

	var dist struct {
		ID string `xml:"Id"`
	}
	distBody, _ := cfReplayCall(t, ts, http.MethodPost, "/2020-05-31/distribution",
		cfDistributionConfigXML)
	require.NoError(t, xml.Unmarshal(distBody, &dist))
	require.NotEmpty(t, dist.ID)

	// An invalidation inside the distribution the previous request minted.
	cfReplayCall(t, ts, http.MethodPost,
		"/2020-05-31/distribution/"+dist.ID+"/invalidation", "")

	// And a delete that names the version the create handed out.
	cfReplayDelete(t, ts, cfOACPath+"/"+created.ID, etag)

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).
		Replay(t.Context(), replayStreamID)
	require.NoError(t, err)

	assert.Positive(t, results.TotalEvents, "the stream has to contain the creates")
	assert.Equal(t, results.TotalEvents, results.SuccessEvents,
		"every recorded request is re-executed and answers")
	assert.Empty(t, results.Differences,
		"a replayed CloudFront create mints the identifiers its recording minted: %s",
		replayDifferenceSummary(results))
	assert.True(t, results.StateValid,
		"and the state it reaches is the recorded state: %v", results.StateErrors)
}

// TestCloudFrontOAC_AStoreFailureIsNotAPublishedRefusal covers the error paths every handler here
// carries, and the property they exist for: a state manager that cannot serve a read or a write is
// substrate's own failure, so it surfaces as a wrapped error — never as one of the operation's
// published codes.
//
// That distinction is the point. A caller told NoSuchOriginAccessControl by a broken store would
// conclude its control was deleted and move on; a caller told InvalidArgument would conclude its
// request was wrong and rewrite it. Both are wrong answers to "substrate could not read its state",
// and the assertion each case makes is that the error is not an [emulator.AWSError] at all.
func TestCloudFrontOAC_AStoreFailureIsNotAPublishedRefusal(t *testing.T) {
	// The index key is "cfoac_ids:…" and a record key is "cfoac:…", so faulting on "cfoac:"
	// reaches the records without disturbing the index a list has to read first.
	const recordKey, indexKey = "cfoac:", "cfoac_ids:"

	tests := []struct {
		name  string
		fault cfFaultStateManager
		call  func(t *testing.T, p *emulator.CloudFrontPlugin, ctx *emulator.RequestContext, oacID string) (*emulator.AWSResponse, error)
	}{
		{
			name:  "a create whose write fails",
			fault: cfFaultStateManager{failPut: recordKey},
			call: func(t *testing.T, p *emulator.CloudFrontPlugin, ctx *emulator.RequestContext, _ string) (*emulator.AWSResponse, error) {
				t.Helper()
				return p.HandleRequest(ctx, cfRequest(http.MethodPost, cfOACPath, nil, cfOACConfigXML))
			},
		},
		{
			name:  "a get whose read fails",
			fault: cfFaultStateManager{failGet: recordKey},
			call: func(t *testing.T, p *emulator.CloudFrontPlugin, ctx *emulator.RequestContext, oacID string) (*emulator.AWSResponse, error) {
				t.Helper()
				return p.HandleRequest(ctx, cfRequest(http.MethodGet, cfOACPath+"/"+oacID, nil, ""))
			},
		},
		{
			name:  "a get whose record does not decode",
			fault: cfFaultStateManager{corruptGet: recordKey},
			call: func(t *testing.T, p *emulator.CloudFrontPlugin, ctx *emulator.RequestContext, oacID string) (*emulator.AWSResponse, error) {
				t.Helper()
				return p.HandleRequest(ctx, cfRequest(http.MethodGet, cfOACPath+"/"+oacID, nil, ""))
			},
		},
		{
			name:  "a list whose index cannot be read",
			fault: cfFaultStateManager{failGet: indexKey},
			call: func(t *testing.T, p *emulator.CloudFrontPlugin, ctx *emulator.RequestContext, _ string) (*emulator.AWSResponse, error) {
				t.Helper()
				return p.HandleRequest(ctx, cfRequest(http.MethodGet, cfOACPath, nil, ""))
			},
		},
		{
			name:  "a delete whose removal fails",
			fault: cfFaultStateManager{failDelete: recordKey},
			call: func(t *testing.T, p *emulator.CloudFrontPlugin, ctx *emulator.RequestContext, oacID string) (*emulator.AWSResponse, error) {
				t.Helper()
				req := cfRequest(http.MethodDelete, cfOACPath+"/"+oacID, nil, "")
				req.Headers["If-Match"] = cfOACStoredETag(t, p, ctx, oacID)
				return p.HandleRequest(ctx, req)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := emulator.NewMemoryStateManager()
			healthy := cfOACPluginOn(t, state)
			ctx := &emulator.RequestContext{
				AccountID: "123456789012", Region: "us-east-1", RequestID: "req-1",
			}
			created, _ := cfCreateOAC(t, healthy, ctx, cfOACConfigXML)

			fault := tc.fault
			fault.inner = state
			resp, err := tc.call(t, cfOACPluginOn(t, &fault), ctx, created.ID)

			assert.Nil(t, resp)
			require.Error(t, err)
			var awsErr *emulator.AWSError
			assert.NotErrorAs(t, err, &awsErr,
				"a store failure is substrate's, not one of the operation's published codes")
		})
	}
}

// TestCloudFrontOAC_AnUnreadableRecordDoesNotHideTheRest is the one store fault the list answers
// *through* rather than failing on: one corrupt record must not make an account's other controls
// unfindable, which is the reading [listDistributions] already takes.
func TestCloudFrontOAC_AnUnreadableRecordDoesNotHideTheRest(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	healthy := cfOACPluginOn(t, state)
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-1"}
	created, _ := cfCreateOAC(t, healthy, ctx, cfOACConfigXML)

	broken := &cfFaultStateManager{inner: state, corruptGet: "cfoac:" + ctx.AccountID + "/" + created.ID}
	resp, err := cfOACPluginOn(t, broken).HandleRequest(ctx, cfRequest(http.MethodGet, cfOACPath, nil, ""))
	require.NoError(t, err, "the list answers despite the unreadable record")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "<Quantity>0</Quantity>",
		"and reports the records it could read, which here is none")
}

// cfOACStoredETag reads back a control's current version through its own get.
func cfOACStoredETag(t *testing.T, p *emulator.CloudFrontPlugin, ctx *emulator.RequestContext, oacID string) string {
	t.Helper()
	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, cfOACPath+"/"+oacID, nil, ""))
	require.NoError(t, err)
	return resp.Headers["ETag"]
}

// cfOACPluginOn builds a CloudFront plugin over the given state manager, so a test can serve one
// request from a healthy store and the next from a faulting one sharing the same records.
func cfOACPluginOn(t *testing.T, state emulator.StateManager) *emulator.CloudFrontPlugin {
	t.Helper()
	p := &emulator.CloudFrontPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Now())},
	}))
	return p
}

// cfFaultStateManager is a StateManager that faults on keys containing a given substring, so each
// handler's error path can be reached without reaching for an unexported field.
//
// Keyed by substring rather than armed by call count because these handlers touch two key kinds —
// a record and the account's index — and a counter would fault whichever the implementation happened
// to read first, which is the kind of test that fails when a handler is reordered rather than when
// its behavior changes.
type cfFaultStateManager struct {
	inner      emulator.StateManager
	failGet    string
	corruptGet string
	failPut    string
	failDelete string
}

func (m *cfFaultStateManager) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.failGet != "" && strings.Contains(key, m.failGet) {
		return nil, errCFFaultStore
	}
	if m.corruptGet != "" && strings.Contains(key, m.corruptGet) {
		return []byte("{not json"), nil
	}
	return m.inner.Get(ctx, namespace, key)
}

func (m *cfFaultStateManager) Put(ctx context.Context, namespace, key string, value []byte) error {
	if m.failPut != "" && strings.Contains(key, m.failPut) {
		return errCFFaultStore
	}
	return m.inner.Put(ctx, namespace, key, value)
}

func (m *cfFaultStateManager) Delete(ctx context.Context, namespace, key string) error {
	if m.failDelete != "" && strings.Contains(key, m.failDelete) {
		return errCFFaultStore
	}
	return m.inner.Delete(ctx, namespace, key)
}

func (m *cfFaultStateManager) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	return m.inner.List(ctx, namespace, prefix)
}

// errCFFaultStore is what a faulting store returns: an error carrying no AWS code, so a handler
// that mistook it for a published refusal would be visible in the assertions above.
var errCFFaultStore = errors.New("cloudfront test store fault")

// cfReplayCall issues one CloudFront REST/XML request against the test server and requires a 2xx,
// returning the body and the response headers.
func cfReplayCall(t *testing.T, ts *emulator.TestServer, method, path, body string,
) ([]byte, http.Header) {
	t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, reader)
	require.NoError(t, err)
	req.Host = "cloudfront.amazonaws.com"
	if body != "" {
		req.Header.Set("Content-Type", "application/xml")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Less(t, resp.StatusCode, 300, "%s %s: %s", method, path, out)
	return out, resp.Header
}

// cfReplayDelete issues a DELETE carrying the If-Match version and requires the documented 204.
func cfReplayDelete(t *testing.T, ts *emulator.TestServer, path, etag string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts.URL+path, nil)
	require.NoError(t, err)
	req.Host = "cloudfront.amazonaws.com"
	req.Header.Set("If-Match", etag)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "DELETE %s: %s", path, out)
}
