package emulator_test

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CreateHostedZone stores the default SOA and NS record sets (#1537). API_CreateHostedZone: "Route 53
// automatically creates a default SOA record and four NS records for the zone", and both its example
// responses, the private-zone one included, report a ResourceRecordSetCount of 2.

type r53DefaultRRSet struct {
	Name   string   `xml:"Name"`
	Type   string   `xml:"Type"`
	TTL    int64    `xml:"TTL"`
	Values []string `xml:"ResourceRecords>ResourceRecord>Value"`
}

func r53DefaultList(t *testing.T, p *emulator.Route53Plugin, ctx *emulator.RequestContext, zoneID string) []r53DefaultRRSet {
	t.Helper()
	resp, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", ""))
	require.NoError(t, err)
	var out struct {
		Sets []r53DefaultRRSet `xml:"ResourceRecordSets>ResourceRecordSet"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &out))
	return out.Sets
}

func r53DefaultZone(t *testing.T, p *emulator.Route53Plugin, ctx *emulator.RequestContext, zoneID string) (count int64, nameServers []string) {
	t.Helper()
	resp, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, ""))
	require.NoError(t, err)
	var out struct {
		Count       int64    `xml:"HostedZone>ResourceRecordSetCount"`
		NameServers []string `xml:"DelegationSet>NameServers>NameServer"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &out))
	return out.Count, out.NameServers
}

func TestRoute53Defaults_CreateHostedZoneStoresTheApexSOAAndNS(t *testing.T) {
	t.Parallel()
	for _, private := range []bool{false, true} {
		name := "public"
		if private {
			name = "private"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p, ctx := route53DeleteSetup(t, emulator.NewMemoryStateManager())
			cfg := ""
			if private {
				cfg = `<HostedZoneConfig><PrivateZone>true</PrivateZone></HostedZoneConfig>`
			}
			resp, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodPost, "/2013-04-01/hostedzone",
				`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>example.com</Name><CallerReference>d-1</CallerReference>`+cfg+`</CreateHostedZoneRequest>`))
			require.NoError(t, err)
			var created struct {
				ID    string `xml:"HostedZone>Id"`
				Count int64  `xml:"HostedZone>ResourceRecordSetCount"`
			}
			require.NoError(t, xml.Unmarshal(resp.Body, &created))
			assert.Equal(t, int64(2), created.Count, "CreateHostedZone reports the two default record sets")
			zoneID := created.ID[len("/hostedzone/"):]

			count, servers := r53DefaultZone(t, p, ctx, zoneID)
			assert.Equal(t, int64(2), count)
			require.Len(t, servers, 4)

			sets := r53DefaultList(t, p, ctx, zoneID)
			require.Len(t, sets, 2, "%+v", sets)
			assert.Equal(t, r53DefaultRRSet{Name: "example.com.", Type: "NS", TTL: 172800, Values: servers}, sets[0],
				"the NS record set lists the delegation set's four name servers")
			assert.Equal(t, r53DefaultRRSet{Name: "example.com.", Type: "SOA", TTL: 900,
				Values: []string{servers[0] + " awsdns-hostmaster.amazon.com. 1 7200 900 1209600 86400"}}, sets[1])

			listResp, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodGet, "/2013-04-01/hostedzone", ""))
			require.NoError(t, err)
			assert.Contains(t, string(listResp.Body), "<ResourceRecordSetCount>2</ResourceRecordSetCount>")
		})
	}
}

func TestRoute53Defaults_ResourceRecordSetCountFollowsEveryChange(t *testing.T) {
	t.Parallel()
	p, ctx := route53DeleteSetup(t, emulator.NewMemoryStateManager())
	zoneID := route53DeleteCreateZone(t, p, ctx)
	steps := []struct {
		action, name, typ string
		want              int64
	}{
		{"CREATE", "www.example.com.", "A", 3},
		{"UPSERT", "api.example.com", "A", 4},
		// One record set per name and type, with or without the trailing dot: the apex NS is the default
		// one modified, not a third apex record set.
		{"UPSERT", "example.com", "NS", 4},
		{"DELETE", "www.example.com.", "A", 3},
		{"DELETE", "api.example.com.", "A", 2},
	}
	for _, s := range steps {
		route53DeleteChange(t, p, ctx, zoneID, s.action, s.name, s.typ)
		count, _ := r53DefaultZone(t, p, ctx, zoneID)
		assert.Equal(t, s.want, count, "after %s %s %s", s.action, s.typ, s.name)
		assert.Len(t, r53DefaultList(t, p, ctx, zoneID), int(s.want), "the listing agrees with the count")
	}
}

func TestRoute53Defaults_AChangeBatchCannotDeleteTheDefaults(t *testing.T) {
	t.Parallel()
	for _, typ := range []string{"SOA", "NS"} {
		t.Run(typ, func(t *testing.T) {
			t.Parallel()
			p, ctx := route53DeleteSetup(t, emulator.NewMemoryStateManager())
			zoneID := route53DeleteCreateZone(t, p, ctx)
			// A batch holding a valid CREATE and the refused DELETE makes neither change: the page calls a
			// change batch transactional.
			body := `<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes>` +
				`<Change><Action>CREATE</Action><ResourceRecordSet><Name>www.example.com</Name><Type>A</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>192.0.2.1</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change>` +
				`<Change><Action>DELETE</Action><ResourceRecordSet><Name>Example.com</Name><Type>` + typ + `</Type></ResourceRecordSet></Change>` +
				`</Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
			_, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body))
			var awsErr *emulator.AWSError
			require.Truef(t, errors.As(err, &awsErr), "deleting the default %s must be refused, got %v", typ, err)
			assert.Equal(t, "InvalidChangeBatch", awsErr.Code)
			assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
			assert.Len(t, r53DefaultList(t, p, ctx, zoneID), 2, "the refused batch made no change")
		})
	}
}

func TestRoute53Defaults_DeleteHostedZoneRemovesTheDefaults(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	p, ctx := route53DeleteSetup(t, state)
	zoneID := route53DeleteCreateZone(t, p, ctx)
	route53DeleteChange(t, p, ctx, zoneID, "UPSERT", "example.com.", "NS")
	require.Nil(t, route53DeleteZone(t, p, ctx, zoneID), "a zone holding only its defaults is deleted")

	keys, err := state.List(context.Background(), "route53", "rrset")
	require.NoError(t, err)
	assert.Empty(t, keys, "the default record sets and the record-set index go with the zone")
}

// A stack that writes the zone's apex NS modifies the default record set; deleting the stack's
// resource restores the default rather than sending a DELETE the zone refuses.
func TestRoute53Defaults_ACloudFormationApexNSRecordSetTearsDown(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	d := newCFNRoute53Deployer(t, state)
	ctx := context.Background()
	result, err := d.Deploy(ctx, `{
	"Resources": {
		"Zone": {"Type": "AWS::Route53::HostedZone", "Properties": {"Name": "apex.example.com"}},
		"ApexNS": {"Type": "AWS::Route53::RecordSet", "Properties": {
			"HostedZoneId": {"Ref": "Zone"}, "Name": "apex.example.com", "Type": "NS", "TTL": "60",
			"ResourceRecords": "ns-9.example.net."}},
		"Group": {"Type": "AWS::Route53::RecordSetGroup", "Properties": {
			"HostedZoneId": {"Ref": "Zone"},
			"RecordSets": [{"Name": "apex.example.com.", "Type": "SOA", "TTL": "60", "ResourceRecords": "ns-9.example.net. h.example.net. 2 1 1 1 1"}]}}
	}
}`, "r53-apex-ns", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", result.Status)
	for _, r := range result.Resources {
		require.Empty(t, r.Error, "%s", r.LogicalID)
	}
	keys, err := state.List(ctx, "route53", "rrset:")
	require.NoError(t, err)
	assert.Len(t, keys, 2, "the stack's apex NS and SOA replaced the defaults: %v", keys)

	deletions := d.DeleteStackResourcesForTest(ctx, "r53-apex-ns")
	for _, del := range deletions {
		assert.Equal(t, "DELETE_COMPLETE", del.Status, "%+v", del)
	}
	keys, err = state.List(ctx, "route53", "rrset")
	require.NoError(t, err)
	assert.Empty(t, keys, "the zone and its record sets are gone")
}

// Deleting a stack that only wrote the apex NS of a zone it does not own leaves the zone's default NS
// record set, with its default values, in place.
func TestRoute53Defaults_ACloudFormationApexNSDeleteRestoresTheDefault(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	d := newCFNRoute53Deployer(t, state)
	ctx := context.Background()
	zoneStack, err := d.Deploy(ctx, `{"Resources": {"Zone": {"Type": "AWS::Route53::HostedZone", "Properties": {"Name": "own.example.com"}}}}`,
		"r53-own-zone", nil)
	require.NoError(t, err)
	zoneID := cfnRoute53ByLogical(zoneStack)["Zone"].PhysicalID
	require.NotEmpty(t, zoneID)

	result, err := d.Deploy(ctx, `{"Resources": {"ApexNS": {"Type": "AWS::Route53::RecordSet", "Properties": {
		"HostedZoneId": "`+zoneID+`", "Name": "own.example.com", "Type": "NS", "TTL": "60", "ResourceRecords": "ns-9.example.net."}}}}`,
		"r53-apex-only", nil)
	require.NoError(t, err)
	require.Equal(t, "CREATE_COMPLETE", result.Status)

	for _, del := range d.DeleteStackResourcesForTest(ctx, "r53-apex-only") {
		assert.Equal(t, "DELETE_COMPLETE", del.Status, "%+v", del)
	}
	data, err := state.Get(ctx, "route53", "rrset:"+zoneID+":NS:own.example.com")
	require.NoError(t, err)
	require.NotNil(t, data, "the default NS record set stays")
	assert.Contains(t, string(data), "ns-1.awsdns-01.com.", "its default name servers are restored")
	assert.Contains(t, string(data), "172800")
	assert.NotContains(t, string(data), "ns-9.example.net.")
}
