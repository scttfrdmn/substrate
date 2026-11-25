package emulator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A record set's identity: its name in Route 53's normalized form, its type and its SetIdentifier,
// including record sets stored without routing members; and the zone record's writers serialized.

func TestRoute53Identity_AnEscapedNameIsTheSameRecordSet(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	zoneID, _, _ := r53CreateZoneOn(t, ts, "esc.example.com", "esc")
	code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset",
		r53ChangeBody(r53A("CREATE", "*.esc.example.com", "", "192.0.2.1"), r53A("CREATE", "WWW.Esc.Example.COM", "", "192.0.2.2")))
	require.Equal(t, http.StatusOK, code, body)

	for _, name := range []string{`\052.esc.example.com`, `\052.ESC.example.com.`, `www.esc.example.com`, `\167ww.esc.example.com`} {
		code, body = r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset",
			r53ChangeBody(r53A("CREATE", name, "", "192.0.2.9")))
		assert.Equal(t, http.StatusBadRequest, code, "%s names an existing record set: %s", name, body)
		_, msg := r53ErrorOf(body)
		assert.Contains(t, msg, "but it already exists", name)
	}
	_, msg := r53ErrorOf(body)
	assert.Contains(t, msg, "[name='www.esc.example.com.'", "the message reports the normalized name")

	list := r53ListSets(t, ts, zoneID)
	assert.Contains(t, list, `<Name>\052.esc.example.com.</Name>`, "API actions return * as its escape code")
	assert.Contains(t, list, "<Name>www.esc.example.com.</Name>", "letters are reported lower-cased")

	code, body = r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset",
		r53ChangeBody(r53A("DELETE", `\052.esc.example.com.`, "", "192.0.2.1")))
	require.Equal(t, http.StatusOK, code, "the escaped spelling deletes the wildcard: %s", body)
	assert.NotContains(t, r53ListSets(t, ts, zoneID), `\052`)
}

// A weighted record set stored under its name and type alone, without routing members, is found by a
// CREATE and an UPSERT that name a SetIdentifier, so neither leaves two record sets of one name and type.
func TestRoute53Identity_ALegacyRecordSetMeetsCreateAndUpsertWithASetIdentifier(t *testing.T) {
	t.Parallel()
	weighted := "<SetIdentifier>blue</SetIdentifier><Weight>10</Weight>"
	batch := func(t *testing.T, p *emulator.Route53Plugin, ctx *emulator.RequestContext, change string) error {
		t.Helper()
		_, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodPost, "/2013-04-01/hostedzone/"+r53LegacyZoneID+"/rrset",
			`<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes>`+change+
				`</Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`))
		return err
	}

	state := emulator.NewMemoryStateManager()
	r53SeedLegacyZone(t, state, map[string][2]string{"A:w.example.com": {"w.example.com.", "A"}})
	p, ctx := route53DeleteSetup(t, state)
	err := batch(t, p, ctx, r53A("CREATE", "w.example.com.", weighted, "192.0.2.7"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "[name='w.example.com.', type='A', set-identifier='blue'] but it already exists")

	// With other values the stored record set may be a simple one, so the UPSERT does not take it over.
	err = batch(t, p, ctx, r53A("UPSERT", "w.example.com.", weighted, "192.0.2.7"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "set-identifier='blue'] but it already exists")
	require.Len(t, r53LegacyKeys(t, state), 1)

	// The stored values restated with the identifier move the record set.
	require.NoError(t, batch(t, p, ctx, r53A("UPSERT", "w.example.com.", weighted, r53LegacyValue)))
	keys := r53LegacyKeys(t, state)
	require.Len(t, keys, 1, "the UPSERT moved the record set rather than writing a second: %v", keys)
	assert.True(t, strings.HasPrefix(keys[0], "rrset:"+r53LegacyZoneID+":A:w.example.com|"), keys[0])
	raw, err := state.Get(context.Background(), "route53", keys[0])
	require.NoError(t, err)
	var stored map[string]any
	require.NoError(t, json.Unmarshal(raw, &stored))
	assert.Equal(t, "blue", stored["SetIdentifier"])
	index, err := state.Get(context.Background(), "route53", "rrset_keys:"+r53LegacyZoneID)
	require.NoError(t, err)
	assert.NotContains(t, string(index), `"A:w.example.com"`, "the old key left the index")
}

// The zone record is rewritten by change batches, tag writes and comment updates. Run together,
// every write survives.
func TestRoute53Identity_ConcurrentZoneWritesLoseNothing(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	zoneID, _, _ := r53CreateZoneOn(t, ts, "race.example.com", "race")
	const n = 8
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(3)
		go func() {
			defer wg.Done()
			code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset",
				r53ChangeBody(r53A("CREATE", fmt.Sprintf("r%d.race.example.com", i), "", "192.0.2.1")))
			assert.Equal(t, http.StatusOK, code, body)
		}()
		go func() {
			defer wg.Done()
			code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/tags/hostedzone/"+zoneID,
				fmt.Sprintf(`<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><AddTags><Tag><Key>k%d</Key><Value>v</Value></Tag></AddTags></ChangeTagsForResourceRequest>`, i))
			assert.Equal(t, http.StatusOK, code, body)
		}()
		go func() {
			defer wg.Done()
			code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID,
				`<UpdateHostedZoneCommentRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Comment>kept</Comment></UpdateHostedZoneCommentRequest>`)
			assert.Equal(t, http.StatusOK, code, body)
		}()
	}
	wg.Wait()

	code, body := r53Call(t, ts, http.MethodGet, "/2013-04-01/tags/hostedzone/"+zoneID, "")
	require.Equal(t, http.StatusOK, code, body)
	assert.Equal(t, n, strings.Count(body, "<Tag>"), "every tag write survived: %s", body)
	code, body = r53Call(t, ts, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, "")
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "<Comment>kept</Comment>")
	assert.Contains(t, body, fmt.Sprintf("<ResourceRecordSetCount>%d</ResourceRecordSetCount>", n+2))
	assert.Equal(t, n+2, strings.Count(r53ListSets(t, ts, zoneID), "<ResourceRecordSet>"))
}

// A retried CreateHealthCheck is compared with the settings it was first sent with, not with the
// health check as UpdateHealthCheck has since changed it; and a reference whose health check is gone
// creates one.
func TestRoute53Identity_HealthCheckIdempotencyUsesTheCreateSettings(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	p, ctx := route53DeleteSetup(t, state)
	create := func(body string) (string, *emulator.AWSError) {
		resp, awsErr, err := r53Direct(t, p, ctx, http.MethodPost, "/2013-04-01/healthcheck", body)
		require.NoError(t, err)
		if awsErr != nil {
			return "", awsErr
		}
		return r53HealthCheckIDRe.FindStringSubmatch(string(resp.Body))[1], nil
	}
	id, awsErr := create(r53HealthCheckBody("retry", "192.0.2.10"))
	require.Nil(t, awsErr)
	_, awsErr, err := r53Direct(t, p, ctx, http.MethodPost, "/2013-04-01/healthcheck/"+id,
		`<UpdateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><IPAddress>192.0.2.11</IPAddress></UpdateHealthCheckRequest>`)
	require.NoError(t, err)
	require.Nil(t, awsErr)

	again, awsErr := create(r53HealthCheckBody("retry", "192.0.2.10"))
	require.Nil(t, awsErr, "the create's own settings still match")
	assert.Equal(t, id, again)
	_, awsErr = create(r53HealthCheckBody("retry", "192.0.2.11"))
	require.NotNil(t, awsErr, "the updated settings are not the create's")
	assert.Equal(t, "HealthCheckAlreadyExists", awsErr.Code)

	require.NoError(t, state.Delete(context.Background(), "route53", "healthcheck:"+id))
	fresh, awsErr := create(r53HealthCheckBody("retry", "192.0.2.10"))
	require.Nil(t, awsErr, "a live reference whose health check is gone creates one")
	assert.NotEqual(t, id, fresh)
}

// A stack's health check gets a reference per create attempt, so a stack deleted and deployed again
// creates a new health check, two stacks get one each, and a health check already gone does not stop
// the stack delete.
func TestCFNRoute53_HealthCheckReferencesArePerStackAndAttempt(t *testing.T) {
	d := newV016FullDeployer(t)
	ctx := context.Background()
	tmpl := `{"Resources": {"Check": {"Type": "AWS::Route53::HealthCheck", "Properties": {
		"HealthCheckConfig": {"Type": "TCP", "IPAddress": "192.0.2.1", "Port": 22}}}}}`
	deploy := func(stack string) string {
		t.Helper()
		res, err := d.Deploy(ctx, tmpl, stack, nil)
		require.NoError(t, err)
		require.Equal(t, "CREATE_COMPLETE", res.Status, "%+v", res.Resources)
		return cfnRoute53ByLogical(res)["Check"].PhysicalID
	}
	first := deploy("r53-hc-a")
	other := deploy("r53-hc-b")
	assert.NotEqual(t, first, other, "two stacks get two health checks")
	require.NoError(t, d.DeleteStack(ctx, "r53-hc-a"))
	second := deploy("r53-hc-a")
	assert.NotEqual(t, first, second, "a redeploy after a delete creates a new health check")

	// A second sweep of stack b finds its health check already deleted by the first.
	for _, del := range d.DeleteStackResourcesForTest(ctx, "r53-hc-b") {
		require.Equal(t, "DELETE_COMPLETE", del.Status, "%+v", del)
	}
	for _, del := range d.DeleteStackResourcesForTest(ctx, "r53-hc-b") {
		assert.NotEqual(t, "DELETE_FAILED", del.Status, "an already-deleted health check: %+v", del)
	}
}

// A weighted record set a stack wrote is deleted by its SetIdentifier, so its zone is deleted after it.
func TestCFNRoute53_AWeightedRecordSetIsDeletedByItsSetIdentifier(t *testing.T) {
	d := newV016FullDeployer(t)
	ctx := context.Background()
	result, err := d.Deploy(ctx, `{
		"Resources": {
			"MyZone": {"Type": "AWS::Route53::HostedZone", "Properties": {"Name": "weighted.example.com"}},
			"Blue": {"Type": "AWS::Route53::RecordSet", "Properties": {
				"HostedZoneId": {"Ref": "MyZone"}, "Name": "app.weighted.example.com", "Type": "A", "TTL": "60",
				"SetIdentifier": "blue", "Weight": 90, "ResourceRecords": "192.0.2.1"}},
			"Green": {"Type": "AWS::Route53::RecordSet", "Properties": {
				"HostedZoneId": {"Ref": "MyZone"}, "Name": "app.weighted.example.com", "Type": "A", "TTL": "60",
				"SetIdentifier": "green", "Weight": 10, "ResourceRecords": "192.0.2.2"}}
		}
	}`, "r53-weighted", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", result.Status)
	for _, r := range result.Resources {
		require.Empty(t, r.Error, r.LogicalID)
	}
	byLogical := map[string]emulator.CFNResourceDeletion{}
	for _, del := range d.DeleteStackResourcesForTest(ctx, "r53-weighted") {
		byLogical[del.LogicalID] = del
	}
	for _, id := range []string{"Blue", "Green", "MyZone"} {
		assert.Equal(t, "DELETE_COMPLETE", byLogical[id].Status, "%s: %+v", id, byLogical[id])
	}
}
