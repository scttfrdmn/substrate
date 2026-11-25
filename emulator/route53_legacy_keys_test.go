package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// State written before #1537: record sets keyed by their Type and Name exactly as the request spelled
// them, and zones holding no default SOA or NS and no DefaultRecordSets flag. Both stay reachable.

const r53LegacyZoneID = "ZLEGACY0000001"

// r53LegacyValue is the one value every legacy record set holds; a DELETE must name it (#1586).
const r53LegacyValue = "192.0.2.1"

// route53ChangeWithValue sends one change for name and type with a TTL of 60 and the one value.
func route53ChangeWithValue(t *testing.T, p *emulator.Route53Plugin, ctx *emulator.RequestContext, zoneID, action, name, typ, value string) {
	t.Helper()
	body := `<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>` + action + `</Action>` +
		`<ResourceRecordSet><Name>` + name + `</Name><Type>` + typ + `</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>` + value +
		`</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	_, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body))
	require.NoError(t, err)
}

// r53DefaultSOAXML and r53DefaultNSXML are the default apex record sets' TTL and values, which a
// DELETE of either must carry.
const (
	r53DefaultSOAXML = `<TTL>900</TTL><ResourceRecords><ResourceRecord><Value>ns-1.awsdns-01.com. awsdns-hostmaster.amazon.com. 1 7200 900 1209600 86400</Value></ResourceRecord></ResourceRecords>`
	r53DefaultNSXML  = `<TTL>172800</TTL><ResourceRecords><ResourceRecord><Value>ns-1.awsdns-01.com.</Value></ResourceRecord>` +
		`<ResourceRecord><Value>ns-2.awsdns-02.net.</Value></ResourceRecord><ResourceRecord><Value>ns-3.awsdns-03.org.</Value></ResourceRecord>` +
		`<ResourceRecord><Value>ns-4.awsdns-04.co.uk.</Value></ResourceRecord></ResourceRecords>`
)

// r53SeedLegacyZone writes a zone and its record sets in the pre-#1537 form: no defaults, no flag, and
// raw keys.
func r53SeedLegacyZone(t *testing.T, state emulator.StateManager, sets map[string][2]string) {
	t.Helper()
	ctx := context.Background()
	zone := map[string]any{
		"Id": "/hostedzone/" + r53LegacyZoneID, "Name": "example.com.", "CallerReference": "legacy",
		"Config": map[string]any{"PrivateZone": false}, "ResourceRecordSetCount": 0, "AccountID": "123456789012",
	}
	put := func(key string, v any) {
		data, err := json.Marshal(v)
		require.NoError(t, err)
		require.NoError(t, state.Put(ctx, "route53", key, data))
	}
	put("hostedzone:"+r53LegacyZoneID, zone)
	put("hostedzone_ids:123456789012", []string{r53LegacyZoneID})
	var index []string
	for key, set := range sets {
		put("rrset:"+r53LegacyZoneID+":"+key, map[string]any{
			"Name": set[0], "Type": set[1], "TTL": 60, "ZoneID": r53LegacyZoneID,
			"ResourceRecords": []map[string]string{{"Value": r53LegacyValue}},
		})
		index = append(index, key)
	}
	put("rrset_keys:"+r53LegacyZoneID, index)
}

func r53LegacyKeys(t *testing.T, state emulator.StateManager) []string {
	t.Helper()
	keys, err := state.List(context.Background(), "route53", "rrset:"+r53LegacyZoneID+":")
	require.NoError(t, err)
	return keys
}

func TestRoute53Legacy_ARecordSetUnderAnOldKeyIsReplacedNotDuplicated(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	r53SeedLegacyZone(t, state, map[string][2]string{"A:WWW.Example.com.": {"WWW.Example.com.", "A"}})
	p, ctx := route53DeleteSetup(t, state)

	route53DeleteChange(t, p, ctx, r53LegacyZoneID, "UPSERT", "www.example.com", "A")
	assert.Equal(t, []string{"rrset:" + r53LegacyZoneID + ":A:www.example.com"}, r53LegacyKeys(t, state),
		"the UPSERT migrated the record set to its canonical key rather than writing a second")
	sets := r53DefaultList(t, p, ctx, r53LegacyZoneID)
	require.Len(t, sets, 1)
	count, _ := r53DefaultZone(t, p, ctx, r53LegacyZoneID)
	assert.Equal(t, int64(1), count)
}

func TestRoute53Legacy_ARecordSetUnderAnOldKeyCanBeDeletedAndItsZoneWithIt(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	r53SeedLegacyZone(t, state, map[string][2]string{
		"A:WWW.Example.com.": {"WWW.Example.com.", "A"},
		"txt:example.com":    {"example.com", "txt"},
	})
	p, ctx := route53DeleteSetup(t, state)
	require.NotNil(t, route53DeleteZone(t, p, ctx, r53LegacyZoneID), "the zone still holds record sets")

	route53ChangeWithValue(t, p, ctx, r53LegacyZoneID, "DELETE", "www.example.com.", "A", r53LegacyValue)
	route53ChangeWithValue(t, p, ctx, r53LegacyZoneID, "DELETE", "example.com.", "TXT", r53LegacyValue)
	assert.Empty(t, r53LegacyKeys(t, state), "a DELETE finds a record set under its old key")
	assert.Nil(t, route53DeleteZone(t, p, ctx, r53LegacyZoneID), "and the emptied zone is deleted")
}

// A zone stored before #1537 holds no defaults. It is treated as having none: its apex records are the
// caller's, deletable, and its count is what it holds. A lower-case apex type under an old key is
// still an apex SOA or NS for DeleteHostedZone.
func TestRoute53Legacy_AZoneWithoutDefaultsIsNotHeldToThem(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	r53SeedLegacyZone(t, state, map[string][2]string{"ns:example.com": {"example.com", "ns"}})
	p, ctx := route53DeleteSetup(t, state)

	route53DeleteChange(t, p, ctx, r53LegacyZoneID, "UPSERT", "api.example.com.", "A")
	count, _ := r53DefaultZone(t, p, ctx, r53LegacyZoneID)
	assert.Equal(t, int64(2), count, "the count is the record sets the zone holds, with no defaults added")
	require.NotNil(t, route53DeleteZone(t, p, ctx, r53LegacyZoneID), "api.example.com blocks the delete")

	route53DeleteChange(t, p, ctx, r53LegacyZoneID, "DELETE", "api.example.com.", "A")
	assert.Nil(t, route53DeleteZone(t, p, ctx, r53LegacyZoneID),
		"the lower-case apex NS under its old key does not block the delete")

	state2 := emulator.NewMemoryStateManager()
	r53SeedLegacyZone(t, state2, map[string][2]string{"NS:example.com.": {"example.com.", "NS"}})
	p2, ctx2 := route53DeleteSetup(t, state2)
	route53ChangeWithValue(t, p2, ctx2, r53LegacyZoneID, "DELETE", "example.com.", "NS", r53LegacyValue)
	assert.Empty(t, r53LegacyKeys(t, state2), "an apex NS the caller wrote to a zone without defaults is deletable")
}

func r53Batch(t *testing.T, p *emulator.Route53Plugin, ctx *emulator.RequestContext, zoneID string, changes ...[3]string) error {
	t.Helper()
	var b strings.Builder
	for _, c := range changes {
		// A DELETE of a default apex record set carries its values, which a DELETE must match (#1586).
		values := `<TTL>60</TTL><ResourceRecords><ResourceRecord><Value>ns-9.example.net. h.example.net. 2 1 1 1 1</Value></ResourceRecord></ResourceRecords>`
		switch {
		case c[0] == "DELETE" && c[2] == "SOA":
			values = r53DefaultSOAXML
		case c[0] == "DELETE" && c[2] == "NS":
			values = r53DefaultNSXML
		}
		b.WriteString(`<Change><Action>` + c[0] + `</Action><ResourceRecordSet><Name>` + c[1] + `</Name><Type>` + c[2] +
			`</Type>` + values + `</ResourceRecordSet></Change>`)
	}
	_, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset",
		`<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes>`+b.String()+`</Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`))
	return err
}

// The apex check reads the batch's end state: a zone must end with exactly one apex SOA and an apex
// NS, so a DELETE with its replacement in the same batch is accepted.
func TestRoute53Defaults_TheApexCheckReadsTheBatchsEndState(t *testing.T) {
	t.Parallel()
	const (
		soaMsg = "A HostedZone must contain exactly one SOA record."
		nsMsg  = "A HostedZone must contain at least one NS record for the zone itself."
		// A CREATE of the existing apex SOA is refused as any CREATE of an existing record set is,
		// before the end-state check runs (#1586).
		soaExists   = "Tried to create resource record set [name='example.com.', type='SOA'] but it already exists"
		soaNotFound = "Tried to delete resource record set [name='example.com.', type='SOA'] but it was not found"
	)
	for _, tc := range []struct {
		name    string
		changes [][3]string
		want    []string // messages the refusal carries; nil is accepted
	}{
		{"delete and create the SOA", [][3]string{{"DELETE", "example.com.", "SOA"}, {"CREATE", "example.com.", "SOA"}}, nil},
		{"delete and upsert the NS", [][3]string{{"DELETE", "example.com", "NS"}, {"UPSERT", "example.com", "NS"}}, nil},
		{"upsert the SOA", [][3]string{{"UPSERT", "example.com.", "SOA"}}, nil},
		{"delete the SOA", [][3]string{{"DELETE", "example.com.", "SOA"}}, []string{soaMsg}},
		{"delete the NS", [][3]string{{"DELETE", "example.com.", "NS"}}, []string{nsMsg}},
		{"delete both", [][3]string{{"DELETE", "example.com.", "NS"}, {"DELETE", "example.com.", "SOA"}}, []string{nsMsg, soaMsg}},
		{"create a second SOA", [][3]string{{"CREATE", "example.com.", "SOA"}}, []string{soaExists}},
		{"create, delete, delete", [][3]string{{"CREATE", "example.com.", "SOA"}, {"DELETE", "example.com.", "SOA"}, {"DELETE", "example.com.", "SOA"}},
			[]string{soaExists, soaNotFound}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx := route53DeleteSetup(t, emulator.NewMemoryStateManager())
			zoneID := route53DeleteCreateZone(t, p, ctx)
			err := r53Batch(t, p, ctx, zoneID, tc.changes...)
			if tc.want == nil {
				require.NoError(t, err)
				assert.Len(t, r53DefaultList(t, p, ctx, zoneID), 2, "the zone still holds one SOA and one NS")
				return
			}
			var awsErr *emulator.AWSError
			require.Truef(t, errors.As(err, &awsErr), "%v", err)
			assert.Equal(t, "InvalidChangeBatch", awsErr.Code)
			assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
			assert.Equal(t, strings.Join(tc.want, ", "), awsErr.Message)
		})
	}
}

// API_ResourceRecordSet publishes the Type values in upper case; a lower-case Type is InvalidInput.
func TestRoute53_ALowerCaseTypeIsInvalidInput(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	p, ctx := route53DeleteSetup(t, state)
	zoneID := route53DeleteCreateZone(t, p, ctx)
	for _, typ := range []string{"a", "Ns", "", "BOGUS"} {
		err := r53Batch(t, p, ctx, zoneID, [3]string{"UPSERT", "www.example.com.", typ})
		var awsErr *emulator.AWSError
		require.Truef(t, errors.As(err, &awsErr), "%q: %v", typ, err)
		assert.Equal(t, "InvalidInput", awsErr.Code, "%q", typ)
		assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
	}
	assert.Len(t, r53DefaultList(t, p, ctx, zoneID), 2, "nothing was written")
}
