package emulator_test

import (
	"errors"
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

// DeleteHostedZone refuses an absent zone and a zone that still holds record sets (#1416), and the
// two listings report a record they cannot read rather than leaving it out (#1417).

// route53DeleteSetup starts a Route 53 plugin over state, on a frozen clock.
func route53DeleteSetup(t *testing.T, state emulator.StateManager) (*emulator.Route53Plugin, *emulator.RequestContext) {
	t.Helper()
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	p := &emulator.Route53Plugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}))
	return p, &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1", RequestID: "req-r53-delete",
		IDs: emulator.NewIDMint("req-r53-delete"),
	}
}

// route53DeleteCreateZone creates example.com. and returns its ID.
func route53DeleteCreateZone(t *testing.T, p *emulator.Route53Plugin, ctx *emulator.RequestContext) string {
	t.Helper()
	resp, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodPost, "/2013-04-01/hostedzone", `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>example.com.</Name><CallerReference>delete-1</CallerReference></CreateHostedZoneRequest>`))
	require.NoError(t, err)
	m := regexp.MustCompile(`<Id>/hostedzone/([^<]+)</Id>`).FindSubmatch(resp.Body)
	require.NotNil(t, m, "CreateHostedZone must report a zone id: %s", resp.Body)
	return string(m[1])
}

// route53DeleteChange sends one change to the zone's record sets.
func route53DeleteChange(t *testing.T, p *emulator.Route53Plugin, ctx *emulator.RequestContext, zoneID, action, name, typ string) {
	t.Helper()
	body := `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>` + action + `</Action>` +
		`<ResourceRecordSet><Name>` + name + `</Name><Type>` + typ + `</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>ns-1.example.net.</Value></ResourceRecord></ResourceRecords></ResourceRecordSet>` +
		`</Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	_, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", body))
	require.NoError(t, err)
}

// route53DeleteZone sends DeleteHostedZone and returns the AWS error it was refused with, or nil.
func route53DeleteZone(t *testing.T, p *emulator.Route53Plugin, ctx *emulator.RequestContext, zoneID string) *emulator.AWSError {
	t.Helper()
	_, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, ""))
	if err == nil {
		return nil
	}
	var awsErr *emulator.AWSError
	require.Truef(t, errors.As(err, &awsErr), "DeleteHostedZone failed with a non-AWS error: %v", err)
	return awsErr
}

func TestRoute53Delete_AnAbsentZoneIsNoSuchHostedZone(t *testing.T) {
	t.Parallel()
	p, ctx := route53DeleteSetup(t, emulator.NewMemoryStateManager())

	awsErr := route53DeleteZone(t, p, ctx, "Z0NOSUCHZONE00")
	require.NotNil(t, awsErr, "an absent zone must be refused")
	assert.Equal(t, "NoSuchHostedZone", awsErr.Code)
	assert.Equal(t, http.StatusNotFound, awsErr.HTTPStatus)

	// A zone deleted once is absent the second time.
	zoneID := route53DeleteCreateZone(t, p, ctx)
	require.Nil(t, route53DeleteZone(t, p, ctx, zoneID))
	awsErr = route53DeleteZone(t, p, ctx, zoneID)
	require.NotNil(t, awsErr, "a deleted zone must be refused")
	assert.Equal(t, "NoSuchHostedZone", awsErr.Code)
}

func TestRoute53Delete_AZoneHoldingRecordSetsIsHostedZoneNotEmpty(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		records   [][2]string // name, type
		wantEmpty bool
	}{
		{"no record sets", nil, true},
		{"an A record", [][2]string{{"www.example.com.", "A"}}, false},
		{"a delegation NS below the apex", [][2]string{{"sub.example.com.", "NS"}}, false},
		{"the apex NS and SOA", [][2]string{{"example.com.", "NS"}, {"Example.com", "SOA"}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx := route53DeleteSetup(t, emulator.NewMemoryStateManager())
			zoneID := route53DeleteCreateZone(t, p, ctx)
			for _, rec := range tc.records {
				route53DeleteChange(t, p, ctx, zoneID, "UPSERT", rec[0], rec[1])
			}

			awsErr := route53DeleteZone(t, p, ctx, zoneID)
			if tc.wantEmpty {
				assert.Nil(t, awsErr, "a zone holding only its apex SOA and NS may be deleted")
				return
			}
			require.NotNil(t, awsErr, "a zone holding record sets must be refused")
			assert.Equal(t, "HostedZoneNotEmpty", awsErr.Code)
			assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)

			// The zone survives the refusal, and deleting the record sets lets it go.
			_, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, ""))
			require.NoError(t, err, "a refused delete leaves the zone in place")
			for _, rec := range tc.records {
				route53DeleteChange(t, p, ctx, zoneID, "DELETE", rec[0], rec[1])
			}
			assert.Nil(t, route53DeleteZone(t, p, ctx, zoneID))
		})
	}
}

func TestRoute53Lists_AnUnreadableRecordIsAnErrorNotAnOmission(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		fault func(m *cfFaultStateManager)
		path  func(zoneID string) string
	}{
		{"ListResourceRecordSets, a read fault", func(m *cfFaultStateManager) { m.failGet = "rrset:" },
			func(zoneID string) string { return "/2013-04-01/hostedzone/" + zoneID + "/rrset" }},
		{"ListResourceRecordSets, an undecodable record", func(m *cfFaultStateManager) { m.corruptGet = "rrset:" },
			func(zoneID string) string { return "/2013-04-01/hostedzone/" + zoneID + "/rrset" }},
		{"ListHostedZones, a read fault", func(m *cfFaultStateManager) { m.failGet = "hostedzone:" },
			func(string) string { return "/2013-04-01/hostedzone" }},
		{"ListHostedZones, an undecodable record", func(m *cfFaultStateManager) { m.corruptGet = "hostedzone:" },
			func(string) string { return "/2013-04-01/hostedzone" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p, ctx := route53DeleteSetup(t, fault)
			zoneID := route53DeleteCreateZone(t, p, ctx)
			route53DeleteChange(t, p, ctx, zoneID, "UPSERT", "www.example.com.", "A")

			tc.fault(fault)
			_, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodGet, tc.path(zoneID), ""))
			require.Error(t, err, "an unreadable record must fail the listing, not vanish from it")
			var awsErr *emulator.AWSError
			assert.False(t, errors.As(err, &awsErr), "a store fault is an internal error, not a published refusal: %v", err)
		})
	}
}

// TestRoute53Lists_AnIndexEntryWithNoRecordIsSkipped is the one case still skipped: the index outlived
// the record it names, which is not a fault in what was read.
func TestRoute53Lists_AnIndexEntryWithNoRecordIsSkipped(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	p, ctx := route53DeleteSetup(t, state)
	zoneID := route53DeleteCreateZone(t, p, ctx)
	route53DeleteChange(t, p, ctx, zoneID, "UPSERT", "www.example.com.", "A")
	route53DeleteChange(t, p, ctx, zoneID, "UPSERT", "api.example.com.", "A")

	keys, err := state.List(t.Context(), "route53", "rrset:"+zoneID+":A:")
	require.NoError(t, err)
	require.Len(t, keys, 2)
	require.NoError(t, state.Delete(t.Context(), "route53", keys[0]))

	resp, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", ""))
	require.NoError(t, err)
	assert.Equal(t, 3, strings.Count(string(resp.Body), "<ResourceRecordSet>"),
		"the record set whose record is gone is left out, and the other is listed with the default NS and SOA: %s", resp.Body)

	// The orphaned index entry does not block a delete once the surviving record set is removed.
	surviving := "www.example.com."
	if strings.Contains(keys[0], "www") {
		surviving = "api.example.com."
	}
	route53DeleteChange(t, p, ctx, zoneID, "DELETE", surviving, "A")
	assert.Nil(t, route53DeleteZone(t, p, ctx, zoneID))
}
