package emulator_test

import (
	"bytes"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestRoute53Wire_HostedZoneResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for Route53HostedZone (#756).
//
// Route53HostedZone declares AccountID under a wire-visible `json` tag, not under omitempty, and it
// does not reach a body: Route 53 speaks REST-XML and every response is marshaled from an XML struct
// declared for its operation. All six routed operations are driven.
func TestRoute53Wire_HostedZoneResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.Route53Plugin{}
	ctx, state := wireSetup(t, p, "req-route53-wire")
	call := func(method, path, body string) []byte {
		t.Helper()
		resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
			Service: "route53", HTTPMethod: method, Path: path,
			Body: []byte(body), Headers: map[string]string{"Content-Type": "application/xml"}, Params: map[string]string{},
		})
		require.NoError(t, err, "%s %s", method, path)
		require.Truef(t, resp.StatusCode >= 200 && resp.StatusCode < 300, "%s %s answered %d: %s", method, path, resp.StatusCode, resp.Body)
		return resp.Body
	}

	created := call(http.MethodPost, "/2013-04-01/hostedzone", `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>wire.example.com</Name><CallerReference>wire-1</CallerReference></CreateHostedZoneRequest>`)
	m := regexp.MustCompile(`<Id>/hostedzone/([^<]+)</Id>`).FindSubmatch(created)
	require.NotNil(t, m, "CreateHostedZone must report a zone id: %s", created)
	zoneID := string(m[1])
	wireRequireHeld(t, state, "route53", "hostedzone:"+zoneID, "AccountID")

	change := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>CREATE</Action>` +
		`<ResourceRecordSet><Name>a.wire.example.com</Name><Type>A</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>192.0.2.1</Value></ResourceRecord></ResourceRecords></ResourceRecordSet>` +
		`</Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	zone := "/2013-04-01/hostedzone/" + zoneID
	for _, tc := range []struct {
		op     string
		body   []byte
		anchor string
	}{
		{"CreateHostedZone", created, zoneID},
		{"GetHostedZone", call(http.MethodGet, zone, ""), zoneID},
		{"ListHostedZones", call(http.MethodGet, "/2013-04-01/hostedzone", ""), zoneID},
		{"ChangeResourceRecordSets", call(http.MethodPost, zone+"/rrset", change), "<ChangeInfo>"},
		{"ListResourceRecordSets", call(http.MethodGet, zone+"/rrset", ""), "a.wire.example.com"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			require.Containsf(t, string(tc.body), tc.anchor, "presence anchor: %s has to render %s", tc.op, tc.anchor)
			wireAssertNoMemberXML(t, tc.op, tc.body, []string{"AccountID"}, "")
		})
	}
	// Last: it removes the zone. Its body is an XML ChangeInfo, walked like the rest.
	t.Run("DeleteHostedZone", func(t *testing.T) {
		// A zone holding a non-default record set cannot be deleted, so the record is removed first.
		call(http.MethodPost, zone+"/rrset", strings.Replace(change, "<Action>CREATE</Action>", "<Action>DELETE</Action>", 1))
		body := call(http.MethodDelete, zone, "")
		require.True(t, bytes.Contains(body, []byte("<ChangeInfo>")), "DeleteHostedZone: %s", body)
		wireAssertNoMemberXML(t, "DeleteHostedZone", body, []string{"AccountID"}, "")
	})
}
