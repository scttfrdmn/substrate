package emulator_test

import (
	"encoding/xml"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CloudFront's CallerReference is the create's replay guard (#1116). API_DistributionConfig: "If
// CallerReference is a value that you already sent in a previous request to create a distribution,
// CloudFront returns a DistributionAlreadyExists error", and API_CreateDistribution publishes that
// code at 409. Until #1116 the member was never read, so a create without one was answered 201 and
// a replayed create minted a second distribution.

// cfCreate posts body to CreateDistribution (or, with withTags, CreateDistributionWithTags) and
// returns the status and either the body or "Code: Message".
func cfCreate(t *testing.T, p *emulator.CloudFrontPlugin, ctx *emulator.RequestContext, withTags bool, body string) (int, string) {
	t.Helper()
	var params map[string]string
	if withTags {
		params = map[string]string{"WithTags": "1"}
	}
	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodPost, "/2020-05-31/distribution", params, body))
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return awsErr.HTTPStatus, awsErr.Code + ": " + awsErr.Message
	}
	require.NoError(t, err)
	return resp.StatusCode, string(resp.Body)
}

// cfListedIDs answers the IDs ListDistributions reports for ctx's account.
func cfListedIDs(t *testing.T, p *emulator.CloudFrontPlugin, ctx *emulator.RequestContext) []string {
	t.Helper()
	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, "/2020-05-31/distribution", nil, ""))
	require.NoError(t, err)
	var list struct {
		Items []struct {
			ID string `xml:"Id"`
		} `xml:"Items>DistributionSummary"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &list))
	ids := make([]string, 0, len(list.Items))
	for _, item := range list.Items {
		ids = append(ids, item.ID)
	}
	return ids
}

// cfWithTags wraps a configuration in the CreateDistributionWithTags document.
func cfWithTags(config string) string {
	return `<DistributionConfigWithTags>` + config +
		`<Tags><Items><Tag><Key>k</Key><Value>v</Value></Tag></Items></Tags></DistributionConfigWithTags>`
}

func TestCloudFrontCallerReference_ACreateWithoutOneIsRefused(t *testing.T) {
	t.Parallel()
	noRef := `<DistributionConfig><Comment>c</Comment><Enabled>true</Enabled></DistributionConfig>`
	blankRef := `<DistributionConfig><CallerReference>  </CallerReference><Comment>c</Comment><Enabled>true</Enabled></DistributionConfig>`
	for _, tc := range []struct {
		name     string
		withTags bool
		body     string
	}{
		{"CreateDistribution, member absent", false, noRef},
		{"CreateDistribution, member blank", false, blankRef},
		{"CreateDistributionWithTags, member absent", true, cfWithTags(noRef)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx := setupCloudFrontPlugin(t)
			status, got := cfCreate(t, p, ctx, tc.withTags, tc.body)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidArgument: CallerReference is required", got)
			assert.Empty(t, cfListedIDs(t, p, ctx), "a refused create leaves nothing behind")
		})
	}
}

func TestCloudFrontCallerReference_AReusedOneIsDistributionAlreadyExists(t *testing.T) {
	t.Parallel()
	const first = `<DistributionConfig><CallerReference>dup</CallerReference><Comment>one</Comment><Enabled>true</Enabled></DistributionConfig>`
	for _, tc := range []struct {
		name     string
		withTags bool
		body     string
	}{
		// "regardless of the content of the DistributionConfig object": an identical retry is refused
		// as surely as a different configuration is — the page publishes no "returns the original".
		{"an identical configuration", false, first},
		{"a different configuration", false, `<DistributionConfig><CallerReference>dup</CallerReference><Comment>two</Comment><Enabled>false</Enabled></DistributionConfig>`},
		{"the tagged create", true, cfWithTags(first)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx := setupCloudFrontPlugin(t)
			status, body := cfCreate(t, p, ctx, false, first)
			require.Equal(t, http.StatusCreated, status, body)
			original := cfListedIDs(t, p, ctx)
			require.Len(t, original, 1)

			status, got := cfCreate(t, p, ctx, tc.withTags, tc.body)
			assert.Equal(t, http.StatusConflict, status)
			assert.Equal(t, "DistributionAlreadyExists: The caller reference you attempted to create "+
				"the distribution with is associated with another distribution.", got)
			assert.Equal(t, original, cfListedIDs(t, p, ctx), "the refused create minted no second distribution")
		})
	}
}

func TestCloudFrontCallerReference_ScopeAndRelease(t *testing.T) {
	t.Parallel()
	config := cfTestConfigXML("shared")

	t.Run("another account may use the same reference", func(t *testing.T) {
		t.Parallel()
		p, ctx := setupCloudFrontPlugin(t)
		status, body := cfCreate(t, p, ctx, false, config)
		require.Equal(t, http.StatusCreated, status, body)
		other := *ctx
		other.AccountID = "210987654321"
		status, body = cfCreate(t, p, &other, false, config)
		assert.Equal(t, http.StatusCreated, status, "a reference is the account's: %s", body)
	})

	// AWS publishes nothing on whether a deleted distribution frees its reference. Substrate's
	// reading is that it does (cloudfront_caller_reference.go), and this pins it.
	t.Run("a deleted distribution releases its reference", func(t *testing.T) {
		t.Parallel()
		p, ctx := setupCloudFrontPlugin(t)
		id := createTestDistribution(t, p, ctx, "shared")
		etag := cfDisableDistribution(t, p, ctx, id)
		del := cfRequest(http.MethodDelete, "/2020-05-31/distribution/"+id, nil, "")
		del.Headers["If-Match"] = etag
		_, err := p.HandleRequest(ctx, del)
		require.NoError(t, err)

		status, body := cfCreate(t, p, ctx, false, config)
		assert.Equal(t, http.StatusCreated, status, "the reference is free again: %s", body)
	})

	t.Run("a disabled distribution still holds it", func(t *testing.T) {
		t.Parallel()
		p, ctx := setupCloudFrontPlugin(t)
		id := createTestDistribution(t, p, ctx, "shared")
		cfDisableDistribution(t, p, ctx, id)
		status, _ := cfCreate(t, p, ctx, false, config)
		assert.Equal(t, http.StatusConflict, status, "only a delete releases the reference")
	})
}

func TestCloudFrontCallerReference_IsAnsweredByGetDistributionConfig(t *testing.T) {
	t.Parallel()
	p, ctx := setupCloudFrontPlugin(t)
	id := createTestDistribution(t, p, ctx, "answered-ref")
	config, _ := cfGetDistributionConfig(t, p, ctx, id)
	assert.Contains(t, config, "<CallerReference>answered-ref</CallerReference>")
}

// TestCFNCloudFrontDistribution_EachDeploymentSendsItsOwnCallerReference: the template has no
// property for the reference, so the deployer supplies one. Two stacks deploying the same logical ID
// must not collide on it, which they would if it were derived from the logical ID alone.
func TestCFNCloudFrontDistribution_EachDeploymentSendsItsOwnCallerReference(t *testing.T) {
	t.Parallel()
	d, registry := newFullTestDeployerWithRegistry(t)
	refPattern := regexp.MustCompile(`<CallerReference>([^<]+)</CallerReference>`)

	refs := map[string]string{}
	for _, stack := range []string{"cdn-a", "cdn-b"} {
		id := cfnDeployedDistributionID(t, d, stack)
		resp, err := registry.RouteRequest(&emulator.RequestContext{AccountID: cfnDottedPropsAccount, Region: cfnDottedPropsRegion}, &emulator.AWSRequest{
			Service: "cloudfront", Operation: http.MethodGet,
			Path:    "/2020-05-31/distribution/" + id + "/config",
			Headers: map[string]string{}, Params: map[string]string{},
		})
		require.NoError(t, err)
		m := refPattern.FindStringSubmatch(string(resp.Body))
		require.NotNil(t, m, "the deployed configuration carries a CallerReference: %s", resp.Body)
		assert.True(t, strings.HasPrefix(m[1], stack+"-Dist-"), "the reference names its stack and resource: %q", m[1])
		refs[stack] = m[1]
	}
	assert.NotEqual(t, refs["cdn-a"], refs["cdn-b"])
}
