package emulator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A change batch is validated whole against the zone's record sets before any change is made
// (#1586), and record sets are keyed by name, type and SetIdentifier.

const r53BatchZone = "batch.example.com"

// r53ListSets answers the record set listing of zoneID.
func r53ListSets(t *testing.T, ts *emulator.TestServer, zoneID string) string {
	t.Helper()
	code, body := r53Call(t, ts, http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", "")
	require.Equal(t, http.StatusOK, code, body)
	return body
}

func TestRoute53Batch_InvalidChangeBatchRefusesTheWholeBatch(t *testing.T) {
	t.Parallel()
	const (
		www = "www." + r53BatchZone
		api = "api." + r53BatchZone
	)
	for _, tc := range []struct {
		name    string
		changes []string
		want    string
	}{
		{"a CREATE of an existing record set",
			[]string{r53A("CREATE", www, "", "192.0.2.9")},
			"Tried to create resource record set [name='www.batch.example.com.', type='A'] but it already exists"},
		{"a DELETE of a missing record set",
			[]string{r53A("DELETE", api, "", "192.0.2.1")},
			"Tried to delete resource record set [name='api.batch.example.com.', type='A'] but it was not found"},
		{"a DELETE whose values differ",
			[]string{r53A("DELETE", www, "", "192.0.2.9")},
			"Tried to delete resource record set [name='www.batch.example.com.', type='A'] but the values provided do not match the current values"},
		{"the same record set deleted twice",
			[]string{r53A("DELETE", www, "", "192.0.2.1"), r53A("DELETE", www+".", "", "192.0.2.1")},
			"Tried to delete resource record set [name='www.batch.example.com.', type='A'] but it was not found"},
		{"every failure is reported, and a good change in the batch is not made",
			[]string{r53A("UPSERT", "new."+r53BatchZone, "", "192.0.2.3"), r53A("CREATE", www, "", "192.0.2.9"),
				r53A("DELETE", api, "", "192.0.2.1")},
			"Tried to create resource record set [name='www.batch.example.com.', type='A'] but it already exists, " +
				"Tried to delete resource record set [name='api.batch.example.com.', type='A'] but it was not found"},
		{"a CREATE of a weighted record set that exists names its set identifier",
			[]string{r53A("CREATE", www, "<SetIdentifier>blue</SetIdentifier><Weight>10</Weight>", "192.0.2.5")},
			"Tried to create resource record set [name='www.batch.example.com.', type='A', set-identifier='blue'] but it already exists"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			zoneID, _, _ := r53CreateZoneOn(t, ts, r53BatchZone, "batch")
			code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", r53ChangeBody(
				r53A("CREATE", www, "", "192.0.2.1"),
				r53A("CREATE", www, "<SetIdentifier>blue</SetIdentifier><Weight>10</Weight>", "192.0.2.5")))
			require.Equal(t, http.StatusOK, code, body)
			before := r53ListSets(t, ts, zoneID)

			code, body = r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", r53ChangeBody(tc.changes...))
			assert.Equal(t, http.StatusBadRequest, code)
			gotCode, msg := r53ErrorOf(body)
			assert.Equal(t, "InvalidChangeBatch", gotCode)
			assert.Equal(t, tc.want, msg)
			assert.Equal(t, before, r53ListSets(t, ts, zoneID), "a refused batch makes no change")
		})
	}
}

func TestRoute53Batch_WeightedRecordSetsOfOneNameAndTypeCoexist(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	zoneID, _, _ := r53CreateZoneOn(t, ts, r53BatchZone, "weighted")
	name := "w." + r53BatchZone
	blue := "<SetIdentifier>blue</SetIdentifier><Weight>10</Weight>"
	green := "<SetIdentifier>Green</SetIdentifier><Weight>0</Weight><HealthCheckId>hc-1</HealthCheckId>"
	code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", r53ChangeBody(
		r53A("CREATE", name, blue, "192.0.2.1"), r53A("CREATE", name, green, "192.0.2.2")))
	require.Equal(t, http.StatusOK, code, body)

	list := r53ListSets(t, ts, zoneID)
	assert.Contains(t, list, "<SetIdentifier>blue</SetIdentifier><Weight>10</Weight>")
	assert.Contains(t, list, "<SetIdentifier>Green</SetIdentifier><Weight>0</Weight>", "a weight of 0 is kept, and the identifier's case")
	assert.Contains(t, list, "<HealthCheckId>hc-1</HealthCheckId>")
	assert.Equal(t, 4, strings.Count(list, "<ResourceRecordSet>"), "the defaults and both weighted record sets")

	// A DELETE must carry the record set's routing members too.
	code, body = r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset",
		r53ChangeBody(r53A("DELETE", name, "<SetIdentifier>Green</SetIdentifier><Weight>0</Weight>", "192.0.2.2")))
	assert.Equal(t, http.StatusBadRequest, code, "the HealthCheckId is missing: %s", body)
	code, body = r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset",
		r53ChangeBody(r53A("DELETE", name, green, "192.0.2.2"), r53A("DELETE", name, blue, "192.0.2.1")))
	require.Equal(t, http.StatusOK, code, body)
	assert.Equal(t, 2, strings.Count(r53ListSets(t, ts, zoneID), "<ResourceRecordSet>"))
}

func TestRoute53Batch_AnUnreadableTTLIsInvalidInput(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	zoneID, _, _ := r53CreateZoneOn(t, ts, r53BatchZone, "ttl")
	for _, bad := range []string{
		strings.Replace(r53A("CREATE", "t."+r53BatchZone, "", "192.0.2.1"), "<TTL>60</TTL>", "<TTL>sixty</TTL>", 1),
		r53A("CREATE", "t."+r53BatchZone, "<SetIdentifier>a</SetIdentifier><Weight>heavy</Weight>", "192.0.2.1"),
	} {
		code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", r53ChangeBody(bad))
		assert.Equal(t, http.StatusBadRequest, code)
		gotCode, _ := r53ErrorOf(body)
		assert.Equal(t, "InvalidInput", gotCode, body)
	}
	assert.Equal(t, 2, strings.Count(r53ListSets(t, ts, zoneID), "<ResourceRecordSet>"), "nothing was written")
}

func TestRoute53Batch_AnAliasDeleteMatchesTheDNSNameLoosely(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	zoneID, _, _ := r53CreateZoneOn(t, ts, r53BatchZone, "alias")
	alias := func(action, dnsName string) string {
		return `<Change><Action>` + action + `</Action><ResourceRecordSet><Name>alias.` + r53BatchZone + `</Name><Type>A</Type>` +
			`<AliasTarget><HostedZoneId>Z2FDTNDATAQYW2</HostedZoneId><DNSName>` + dnsName +
			`</DNSName><EvaluateTargetHealth>false</EvaluateTargetHealth></AliasTarget></ResourceRecordSet></Change>`
	}
	code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", r53ChangeBody(alias("CREATE", "d111.cloudfront.net")))
	require.Equal(t, http.StatusOK, code, body)
	code, body = r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", r53ChangeBody(alias("DELETE", "D111.cloudfront.net.")))
	require.Equal(t, http.StatusOK, code, "a DNS name with its trailing dot and another case is the same target: %s", body)
}

// A weighted record set written before the parser kept SetIdentifier is stored under its name and type
// alone, without the RoutingRecorded flag. A DELETE that names its SetIdentifier still finds it.
func TestRoute53Batch_ALegacyWeightedRecordSetCanStillBeDeleted(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	r53SeedLegacyZone(t, state, map[string][2]string{"A:w.example.com": {"w.example.com.", "A"}})
	p, ctx := route53DeleteSetup(t, state)
	body := `<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes>` +
		r53A("DELETE", "w.example.com.", "<SetIdentifier>blue</SetIdentifier><Weight>10</Weight>", r53LegacyValue) +
		`</Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	_, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodPost, "/2013-04-01/hostedzone/"+r53LegacyZoneID+"/rrset", body))
	require.NoError(t, err)
	assert.Empty(t, r53LegacyKeys(t, state))

	// A record set written since carries the flag, so a DELETE naming a SetIdentifier it lacks is not
	// matched to it.
	route53ChangeWithValue(t, p, ctx, r53LegacyZoneID, "CREATE", "v.example.com.", "A", r53LegacyValue)
	body = strings.Replace(body, "w.example.com.", "v.example.com.", 1)
	_, err = p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodPost, "/2013-04-01/hostedzone/"+r53LegacyZoneID+"/rrset", body))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "but it was not found")

	raw, err := state.Get(context.Background(), "route53", "rrset:"+r53LegacyZoneID+":A:v.example.com")
	require.NoError(t, err)
	var stored map[string]any
	require.NoError(t, json.Unmarshal(raw, &stored))
	assert.Equal(t, true, stored["RoutingRecorded"])
}

// A stack delete sends the stored record set's own values, so a template whose values were filled in
// at deploy time, or a record set already removed out of band, still tears down.
func TestCFNRoute53_AStackDeleteSendsTheStoredValues(t *testing.T) {
	d := newV016FullDeployer(t)
	ctx := context.Background()
	result, err := d.Deploy(ctx, `{
		"Resources": {
			"MyZone": {"Type": "AWS::Route53::HostedZone", "Properties": {"Name": "values.example.com"}},
			"NoTTL": {"Type": "AWS::Route53::RecordSet", "Properties": {
				"HostedZoneId": {"Ref": "MyZone"}, "Name": "a.values.example.com", "Type": "A",
				"ResourceRecords": "192.0.2.2"}},
			"Gone": {"Type": "AWS::Route53::RecordSet", "Properties": {
				"HostedZoneId": {"Ref": "MyZone"}, "Name": "b.values.example.com", "Type": "A", "TTL": "60",
				"ResourceRecords": "192.0.2.3"}}
		}
	}`, "r53-values-stack", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", result.Status)
	zoneID := cfnRoute53ByLogical(result)["MyZone"].PhysicalID

	// A second stack upserts the same record set with another value, and its delete removes it, so the
	// first stack's record set is gone before its own delete runs.
	other, err := d.Deploy(ctx, `{"Resources": {"Same": {"Type": "AWS::Route53::RecordSet", "Properties": {
		"HostedZoneId": "`+zoneID+`", "Name": "b.values.example.com", "Type": "A", "TTL": "120",
		"ResourceRecords": "192.0.2.4"}}}}`, "r53-values-other", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", other.Status)
	for _, del := range d.DeleteStackResourcesForTest(ctx, "r53-values-other") {
		require.Equal(t, "DELETE_COMPLETE", del.Status, "%+v", del)
	}

	byLogical := map[string]emulator.CFNResourceDeletion{}
	for _, del := range d.DeleteStackResourcesForTest(ctx, "r53-values-stack") {
		byLogical[del.LogicalID] = del
	}
	for _, id := range []string{"NoTTL", "Gone", "MyZone"} {
		assert.Equal(t, "DELETE_COMPLETE", byLogical[id].Status, "%s: %+v", id, byLogical[id])
	}
}
