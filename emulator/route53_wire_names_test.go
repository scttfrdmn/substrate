package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Names in responses, name lengths in octets, refusals of an unknown Action and of an UPSERT that
// would replace a simple record set, and the record-set stack delete's handling of a vanished record.

func TestRoute53WireNames_ZoneNamesAreReportedNormalized(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone",
		`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>Mixed.EXAMPLE.com</Name><CallerReference>norm</CallerReference></CreateHostedZoneRequest>`)
	require.Equal(t, http.StatusCreated, code, body)
	assert.Contains(t, body, "<Name>mixed.example.com.</Name>", "CreateHostedZone")
	zoneID := r53ZoneIDRe.FindStringSubmatch(body)[1]
	_, _, _ = r53CreateZoneOn(t, ts, "Other.EXAMPLE.com", "norm-2")

	for _, tc := range []struct{ op, method, path, body string }{
		{"GetHostedZone", http.MethodGet, "/2013-04-01/hostedzone/" + zoneID, ""},
		{"ListHostedZones", http.MethodGet, "/2013-04-01/hostedzone", ""},
		{"ListHostedZonesByName", http.MethodGet, "/2013-04-01/hostedzonesbyname", ""},
		{"UpdateHostedZoneComment", http.MethodPost, "/2013-04-01/hostedzone/" + zoneID,
			`<UpdateHostedZoneCommentRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Comment>c</Comment></UpdateHostedZoneCommentRequest>`},
	} {
		code, body := r53Call(t, ts, tc.method, tc.path, tc.body)
		require.Equal(t, http.StatusOK, code, "%s: %s", tc.op, body)
		assert.Contains(t, body, "<Name>mixed.example.com.</Name>", tc.op)
		assert.NotContains(t, body, "EXAMPLE", tc.op)
	}
	code, body = r53Call(t, ts, http.MethodGet, "/2013-04-01/hostedzonesbyname?maxitems=1", "")
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "<NextDNSName>other.example.com.</NextDNSName>")

	// The zone's own apex is still its apex: its default SOA cannot be deleted away.
	code, body = r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", r53ChangeBody(
		`<Change><Action>UPSERT</Action><ResourceRecordSet><Name>MIXED.example.com</Name><Type>TXT</Type><TTL>60</TTL>`+
			`<ResourceRecords><ResourceRecord><Value>"v"</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change>`))
	require.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, r53ListSets(t, ts, zoneID), "<Name>mixed.example.com.</Name><Type>TXT</Type>")
}

func TestRoute53WireNames_AliasTargetsAreReportedNormalized(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	zoneID, _, _ := r53CreateZoneOn(t, ts, "alias.example.com", "alias-norm")
	code, body := r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", r53ChangeBody(
		`<Change><Action>CREATE</Action><ResourceRecordSet><Name>lb.alias.example.com</Name><Type>A</Type>`+
			`<AliasTarget><HostedZoneId>Z35SXDOTRQ7X7K</HostedZoneId><DNSName>My-LB-123.us-east-1.elb.amazonaws.com</DNSName>`+
			`<EvaluateTargetHealth>false</EvaluateTargetHealth></AliasTarget></ResourceRecordSet></Change>`))
	require.Equal(t, http.StatusOK, code, body)
	list := r53ListSets(t, ts, zoneID)
	assert.Contains(t, list, "<DNSName>my-lb-123.us-east-1.elb.amazonaws.com.</DNSName>")
	assert.Contains(t, list, "<Name>lb.alias.example.com.</Name><Type>A</Type><AliasTarget>",
		"an alias record set carries no ResourceRecords element, which a DELETE read back from it would send empty")

	// The record set as listed deletes: no TTL, no ResourceRecords.
	code, body = r53Call(t, ts, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", r53ChangeBody(
		`<Change><Action>DELETE</Action><ResourceRecordSet><Name>lb.alias.example.com.</Name><Type>A</Type>`+
			`<AliasTarget><HostedZoneId>Z35SXDOTRQ7X7K</HostedZoneId><DNSName>my-lb-123.us-east-1.elb.amazonaws.com.</DNSName>`+
			`<EvaluateTargetHealth>false</EvaluateTargetHealth></AliasTarget></ResourceRecordSet></Change>`))
	require.Equal(t, http.StatusOK, code, body)
}

func TestRoute53WireNames_NameLengthsCountOctets(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	_, _, _ = r53CreateZoneOn(t, ts, "len.example.com", "len")
	for _, tc := range []struct {
		name string
		ok   bool
	}{
		{`\052` + strings.Repeat("x", 62) + ".com", true},  // 63 octets
		{`\052` + strings.Repeat("x", 63) + ".com", false}, // 64 octets
		{strings.Repeat("a.", 127) + "b", true},            // 255 octets
		{strings.Repeat("a.", 127) + "bc", false},          // 256 octets
		{strings.Repeat(`\141.`, 127) + "bc", false},       // 256 octets, escaped
	} {
		code, body := r53Call(t, ts, http.MethodGet, "/2013-04-01/hostedzonesbyname?dnsname="+strings.ReplaceAll(tc.name, `\`, "%5C"), "")
		if tc.ok {
			assert.Equal(t, http.StatusOK, code, "%d bytes: %s", len(tc.name), body)
			continue
		}
		assert.Equal(t, http.StatusBadRequest, code, "%d bytes", len(tc.name))
		gotCode, _ := r53ErrorOf(body)
		assert.Equal(t, "InvalidDomainName", gotCode)
	}
}

func TestRoute53WireNames_AnUnknownActionIsInvalidInput(t *testing.T) {
	t.Parallel()
	p, ctx := route53DeleteSetup(t, emulator.NewMemoryStateManager())
	zoneID := route53DeleteCreateZone(t, p, ctx)
	_, awsErr, err := r53Direct(t, p, ctx, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset",
		r53ChangeBody(r53A("REPLACE", "x.example.com", "", "192.0.2.1")))
	require.NoError(t, err)
	require.NotNil(t, awsErr)
	assert.Equal(t, "InvalidInput", awsErr.Code)
	assert.Len(t, r53DefaultList(t, p, ctx, zoneID), 2, "nothing was written")
}

// State as v0.133 wrote it: a zone with its defaults and a simple record set stored without the
// RoutingRecorded flag. A weighted UPSERT of the same name and type with other values does not
// replace it.
func TestRoute53WireNames_AWeightedUpsertDoesNotReplaceAStoredSimpleRecordSet(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	p, ctx := route53DeleteSetup(t, state)
	zoneID := route53DeleteCreateZone(t, p, ctx)
	raw, err := json.Marshal(map[string]any{
		"Name": "www.example.com.", "Type": "A", "TTL": 300, "ZoneID": zoneID,
		"ResourceRecords": []map[string]string{{"Value": "192.0.2.1"}},
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(context.Background(), "route53", "rrset:"+zoneID+":A:www.example.com", raw))
	index, err := state.Get(context.Background(), "route53", "rrset_keys:"+zoneID)
	require.NoError(t, err)
	var keys []string
	require.NoError(t, json.Unmarshal(index, &keys))
	keys = append(keys, "A:www.example.com")
	data, err := json.Marshal(keys)
	require.NoError(t, err)
	require.NoError(t, state.Put(context.Background(), "route53", "rrset_keys:"+zoneID, data))

	_, awsErr, err := r53Direct(t, p, ctx, http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset",
		r53ChangeBody(r53A("UPSERT", "www.example.com", "<SetIdentifier>blue</SetIdentifier><Weight>10</Weight>", "192.0.2.9")))
	require.NoError(t, err)
	require.NotNil(t, awsErr)
	assert.Equal(t, "InvalidChangeBatch", awsErr.Code)
	got, err := state.Get(context.Background(), "route53", "rrset:"+zoneID+":A:www.example.com")
	require.NoError(t, err)
	assert.JSONEq(t, string(raw), string(got), "the simple record set is untouched")
}

func TestRoute53WireNames_ARefusalOfOnlyMissingRecordSetsReadsAsGone(t *testing.T) {
	t.Parallel()
	notFound := "Tried to delete resource record set [name='a.example.com.', type='A'] but it was not found"
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&emulator.AWSError{Code: "InvalidChangeBatch", Message: notFound}, true},
		{&emulator.AWSError{Code: "InvalidChangeBatch", Message: notFound + ", " + strings.Replace(notFound, "a.", "b.", 1)}, true},
		{&emulator.AWSError{Code: "InvalidChangeBatch", Message: notFound + ", Tried to delete resource record set [name='c.example.com.', type='A'] but the values provided do not match the current values"}, false},
		{&emulator.AWSError{Code: "InvalidInput", Message: notFound}, false},
		{fmt.Errorf("wrapped: %w", &emulator.AWSError{Code: "InvalidChangeBatch", Message: notFound}), true},
		{errors.New(notFound), false},
	} {
		assert.Equal(t, tc.want, emulator.R53OnlyNotFoundForTest(tc.err), "%v", tc.err)
	}
}

// A record set group naming one record set twice deletes it once.
func TestCFNRoute53_ARecordSetGroupNamingARecordSetTwiceDeletes(t *testing.T) {
	d := newV016FullDeployer(t)
	ctx := context.Background()
	result, err := d.Deploy(ctx, `{"Resources": {
		"MyZone": {"Type": "AWS::Route53::HostedZone", "Properties": {"Name": "twice.example.com"}},
		"Group": {"Type": "AWS::Route53::RecordSetGroup", "Properties": {"HostedZoneId": {"Ref": "MyZone"}, "RecordSets": [
			{"Name": "t.twice.example.com", "Type": "A", "TTL": "60", "ResourceRecords": "192.0.2.1"},
			{"Name": "T.twice.example.com.", "Type": "A", "TTL": "60", "ResourceRecords": "192.0.2.1"}]}}}}`, "r53-twice", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", result.Status)
	byLogical := map[string]emulator.CFNResourceDeletion{}
	for _, del := range d.DeleteStackResourcesForTest(ctx, "r53-twice") {
		byLogical[del.LogicalID] = del
	}
	assert.Equal(t, "DELETE_COMPLETE", byLogical["Group"].Status, "%+v", byLogical["Group"])
	assert.Equal(t, "DELETE_COMPLETE", byLogical["MyZone"].Status, "%+v", byLogical["MyZone"])
}
