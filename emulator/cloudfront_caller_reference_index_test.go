package emulator_test

import (
	"context"
	"encoding/json"
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

// The CallerReference index (#1116): one entry per reference, honored only when the distribution it
// names exists and still holds the reference, so a bad record or a stale entry never refuses a
// create; an update cannot claim a reference another distribution holds; and a CloudFormation
// deployment derives its reference without drawing from the deployment's mint.

// cfPluginWithState is setupCloudFrontPlugin with the state manager handed back, for tests that
// plant records a request cannot write.
func cfPluginWithState(t *testing.T) (*emulator.CloudFrontPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.CloudFrontPlugin{}
	require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
	}))
	return p, &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-1"}, state
}

const cfIndexAccount = "123456789012"

func TestCloudFrontCallerReferenceIndex_ABadRecordDoesNotRefuseCreates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		plant func(t *testing.T, state emulator.StateManager)
	}{
		{"an entry naming a distribution that is gone", func(t *testing.T, state emulator.StateManager) {
			require.NoError(t, state.Put(context.Background(), "cloudfront", "cfref:"+cfIndexAccount+"/held", []byte(`"EGONE00000000"`)))
		}},
		{"an entry that does not decode", func(t *testing.T, state emulator.StateManager) {
			require.NoError(t, state.Put(context.Background(), "cloudfront", "cfref:"+cfIndexAccount+"/held", []byte(`{`)))
		}},
		{"an entry naming a record that does not decode", func(t *testing.T, state emulator.StateManager) {
			require.NoError(t, state.Put(context.Background(), "cloudfront", "cfref:"+cfIndexAccount+"/held", []byte(`"EBAD000000000"`)))
			require.NoError(t, state.Put(context.Background(), "cloudfront", "cfdist:"+cfIndexAccount+"/EBAD000000000", []byte(`{`)))
		}},
		{"an entry naming a record whose configuration does not parse", func(t *testing.T, state emulator.StateManager) {
			require.NoError(t, state.Put(context.Background(), "cloudfront", "cfref:"+cfIndexAccount+"/held", []byte(`"EBAD000000000"`)))
			require.NoError(t, state.Put(context.Background(), "cloudfront", "cfdist:"+cfIndexAccount+"/EBAD000000000",
				[]byte(`{"Id":"EBAD000000000","AccountID":"`+cfIndexAccount+`","Config":"<DistributionConfig><Comment>"}`)))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx, state := cfPluginWithState(t)
			tc.plant(t, state)
			status, body := cfCreate(t, p, ctx, false, cfTestConfigXML("held"))
			assert.Equal(t, http.StatusCreated, status, "an entry the record does not back is free: %s", body)
			status, _ = cfCreate(t, p, ctx, false, cfTestConfigXML("held"))
			assert.Equal(t, http.StatusConflict, status, "and the create that took it holds it now")
		})
	}
}

func TestCloudFrontCallerReferenceIndex_ADeleteRemovesTheEntry(t *testing.T) {
	t.Parallel()
	p, ctx, state := cfPluginWithState(t)
	id := createTestDistribution(t, p, ctx, "gone-soon")
	entry, err := state.Get(context.Background(), "cloudfront", "cfref:"+cfIndexAccount+"/gone-soon")
	require.NoError(t, err)
	assert.JSONEq(t, `"`+id+`"`, string(entry), "the create records which distribution holds the reference")

	etag := cfDisableDistribution(t, p, ctx, id)
	del := cfRequest(http.MethodDelete, "/2020-05-31/distribution/"+id, nil, "")
	del.Headers["If-Match"] = etag
	_, err = p.HandleRequest(ctx, del)
	require.NoError(t, err)
	entry, err = state.Get(context.Background(), "cloudfront", "cfref:"+cfIndexAccount+"/gone-soon")
	require.NoError(t, err)
	assert.Nil(t, entry, "the delete removes the entry with the distribution")
}

// TestCloudFrontCallerReferenceIndex_AnUpdateCannotClaimAHeldReference: a record written before
// #1116 holds no reference, so an update may give it one — but not one another distribution holds.
// DistributionAlreadyExists is not on API_UpdateDistribution's Errors list; IllegalUpdate is.
func TestCloudFrontCallerReferenceIndex_AnUpdateCannotClaimAHeldReference(t *testing.T) {
	t.Parallel()
	p, ctx, state := cfPluginWithState(t)
	const legacy = "ELEGACY0000000" // E and thirteen, so its ID serves as its version (cfDistributionETag)
	record, err := json.Marshal(map[string]any{
		"Id": legacy, "ARN": "arn:aws:cloudfront::" + cfIndexAccount + ":distribution/" + legacy,
		"Status": "Deployed", "DomainName": legacy + ".cloudfront.net", "Comment": "legacy", "Enabled": true,
		"AccountID": cfIndexAccount,
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(context.Background(), "cloudfront", "cfdist:"+cfIndexAccount+"/"+legacy, record))
	require.NoError(t, state.Put(context.Background(), "cloudfront", "cfdist_ids:"+cfIndexAccount, []byte(`["`+legacy+`"]`)))
	createTestDistribution(t, p, ctx, "taken")

	update := func(ref string) (int, string) {
		config, etag := cfGetDistributionConfig(t, p, ctx, legacy)
		require.NotContains(t, config, "<CallerReference>", "the legacy record holds no reference")
		req := cfRequest(http.MethodPut, "/2020-05-31/distribution/"+legacy+"/config", nil,
			strings.Replace(config, "<Comment>", "<CallerReference>"+ref+"</CallerReference><Comment>", 1))
		req.Headers["If-Match"] = etag
		resp, err := p.HandleRequest(ctx, req)
		if awsErr, ok := err.(*emulator.AWSError); ok { //nolint:errorlint // HandleRequest returns *AWSError unwrapped.
			return awsErr.HTTPStatus, awsErr.Code + ": " + awsErr.Message
		}
		require.NoError(t, err)
		return resp.StatusCode, string(resp.Body)
	}

	status, got := update("taken")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "IllegalUpdate: The CallerReference is associated with another distribution.", got)

	status, got = update("adopted")
	require.Equal(t, http.StatusOK, status, got)
	status, _ = cfCreate(t, p, ctx, false, cfTestConfigXML("adopted"))
	assert.Equal(t, http.StatusConflict, status, "the adopted reference is held now")
}

func TestCloudFrontCreate_AnswersTheLocationHeader(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		withTags bool
		body     string
	}{
		{"CreateDistribution", false, cfTestConfigXML("loc")},
		{"CreateDistributionWithTags", true, cfWithTags(cfTestConfigXML("loc"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx := setupCloudFrontPlugin(t)
			var params map[string]string
			if tc.withTags {
				params = map[string]string{"WithTags": "1"}
			}
			resp, err := p.HandleRequest(ctx, cfRequest(http.MethodPost, "/2020-05-31/distribution", params, tc.body))
			require.NoError(t, err)
			id := regexp.MustCompile(`<Id>([^<]+)</Id>`).FindStringSubmatch(string(resp.Body))[1]
			assert.Equal(t, "https://cloudfront.amazonaws.com/2020-05-31/distribution/"+id, resp.Headers["Location"])
		})
	}
}

// TestCloudFrontList_ABadConfigurationFallsBack: a stored configuration that
// does not parse summarizes from the record's own Comment and Enabled rather than failing the list.
func TestCloudFrontList_ABadConfigurationFallsBack(t *testing.T) {
	t.Parallel()
	p, ctx, state := cfPluginWithState(t)
	good := createTestDistribution(t, p, ctx, "good")
	const bad = "EBADCONFIG000"
	require.NoError(t, state.Put(context.Background(), "cloudfront", "cfdist:"+cfIndexAccount+"/"+bad,
		[]byte(`{"Id":"`+bad+`","Comment":"from the record","Enabled":false,"AccountID":"`+cfIndexAccount+`","Config":"<DistributionConfig><Comment>"}`)))
	require.NoError(t, state.Put(context.Background(), "cloudfront", "cfdist_ids:"+cfIndexAccount, []byte(`["`+bad+`","`+good+`"]`)))

	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, "/2020-05-31/distribution", nil, ""))
	require.NoError(t, err)
	body := string(resp.Body)
	assert.Contains(t, body, "<Id>"+good+"</Id>", "the readable distribution is still listed")
	assert.Contains(t, body, "<Comment>from the record</Comment><Enabled>false</Enabled>")
}

// cfnDistributionRefs reads the CallerReference of every distribution in state.
func cfnDistributionRefs(t *testing.T, state emulator.StateManager) map[string]string {
	t.Helper()
	keys, err := state.List(context.Background(), "cloudfront", "cfdist:")
	require.NoError(t, err)
	refs := map[string]string{}
	for _, k := range keys {
		data, err := state.Get(context.Background(), "cloudfront", k)
		require.NoError(t, err)
		var dist struct {
			ID     string `json:"Id"`
			Config string `json:"Config"`
		}
		require.NoError(t, json.Unmarshal(data, &dist))
		m := regexp.MustCompile(`<CallerReference>([^<]+)</CallerReference>`).FindStringSubmatch(dist.Config)
		require.NotNil(t, m, "%s", dist.Config)
		refs[dist.ID] = m[1]
	}
	return refs
}

func cfnDistTemplate(comment string) string {
	return `{"Resources":{"Dist":{"Type":"AWS::CloudFront::Distribution","Properties":{` +
		`"DistributionConfig":{"Comment":"` + comment + `","Enabled":true}}}}}`
}

// TestCFNCloudFrontDistribution_TheReferenceIsDerivedNotDrawn: two independent deployers of the same
// stack derive the same reference — it consumes nothing from the deployment's mint — and a
// replacement derives a fresh one, so it can be created while its predecessor holds the old.
func TestCFNCloudFrontDistribution_TheReferenceIsDerivedNotDrawn(t *testing.T) {
	t.Parallel()
	var first []string
	for range 2 {
		d, state, _, _ := newSweepDeployer(t)
		created, err := d.Deploy(context.Background(), cfnDistTemplate("v1"), "cdn-derive", nil)
		require.NoError(t, err)
		require.Empty(t, resourceByLogicalID(t, created, "Dist").Error)
		var refs []string
		for _, ref := range cfnDistributionRefs(t, state) {
			refs = append(refs, ref)
		}
		require.Len(t, refs, 1)
		assert.Regexp(t, `^cdn-derive-Dist-[0-9a-f]{16}$`, refs[0])
		if first == nil {
			first = refs
			continue
		}
		assert.Equal(t, first, refs, "the same stack derives the same reference in any deployer")
	}

	d, state, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	created, err := d.Deploy(ctx, cfnDistTemplate("v1"), "cdn-replace", nil)
	require.NoError(t, err)
	old := resourceByLogicalID(t, created, "Dist")
	oldRef := cfnDistributionRefs(t, state)[old.PhysicalID]

	updated, err := d.UpdateStack(ctx, cfnDistTemplate("v2"), "cdn-replace", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	fresh := resourceByLogicalID(t, updated, "Dist")
	require.Empty(t, fresh.Error)
	require.NotEqual(t, old.PhysicalID, fresh.PhysicalID, "the distribution was re-created")
	freshRef := cfnDistributionRefs(t, state)[fresh.PhysicalID]
	assert.NotEqual(t, oldRef, freshRef, "a replacement derives a fresh reference")
}
