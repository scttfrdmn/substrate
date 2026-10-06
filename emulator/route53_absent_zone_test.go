package emulator_test

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ChangeResourceRecordSets and ListResourceRecordSets refuse a zone ID that names no hosted zone with
// NoSuchHostedZone (404), as both pages publish, and store nothing (#1410). A change batch for a
// missing zone used to be written and answered with a ChangeInfo, leaving records under a zone
// GetHostedZone said did not exist.

const route53AbsentZoneChange = `<?xml version="1.0" encoding="UTF-8"?>
<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>UPSERT</Action>` +
	`<ResourceRecordSet><Name>a.absent.example.com</Name><Type>A</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>192.0.2.1</Value></ResourceRecord></ResourceRecords></ResourceRecordSet>` +
	`</Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`

func route53AbsentZoneRequest(method, path, body string) *emulator.AWSRequest {
	return &emulator.AWSRequest{
		Service: "route53", HTTPMethod: method, Path: path,
		Body: []byte(body), Headers: map[string]string{"Content-Type": "application/xml"}, Params: map[string]string{},
	}
}

func TestRoute53AbsentZone_RecordSetOperationsAnswerNoSuchHostedZone(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// deleted creates a zone and deletes it first, so the ID once named a zone.
		deleted bool
		method  string
		body    string
	}{
		{"ChangeResourceRecordSets on an ID never issued", false, http.MethodPost, route53AbsentZoneChange},
		{"ListResourceRecordSets on an ID never issued", false, http.MethodGet, ""},
		{"ChangeResourceRecordSets on a deleted zone", true, http.MethodPost, route53AbsentZoneChange},
		{"ListResourceRecordSets on a deleted zone", true, http.MethodGet, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := &emulator.Route53Plugin{}
			ctx, state := wireSetup(t, p, "req-route53-absent")

			zoneID := "Z0NOSUCHZONE00"
			if tc.deleted {
				resp, err := p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodPost, "/2013-04-01/hostedzone", `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>absent.example.com</Name><CallerReference>absent-1</CallerReference></CreateHostedZoneRequest>`))
				require.NoError(t, err)
				m := regexp.MustCompile(`<Id>/hostedzone/([^<]+)</Id>`).FindSubmatch(resp.Body)
				require.NotNil(t, m, "CreateHostedZone must report a zone id: %s", resp.Body)
				zoneID = string(m[1])
				_, err = p.HandleRequest(ctx, route53AbsentZoneRequest(http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, ""))
				require.NoError(t, err)
			}

			_, err := p.HandleRequest(ctx, route53AbsentZoneRequest(tc.method, "/2013-04-01/hostedzone/"+zoneID+"/rrset", tc.body))
			var awsErr *emulator.AWSError
			require.Truef(t, errors.As(err, &awsErr), "%s must be refused, got %v", tc.name, err)
			assert.Equal(t, "NoSuchHostedZone", awsErr.Code, "%s", awsErr.Message)
			assert.Equal(t, http.StatusNotFound, awsErr.HTTPStatus)

			keys, err := state.List(context.Background(), "route53", "rrset")
			require.NoError(t, err)
			assert.Empty(t, keys, "a refused request stores no record set and no record-set index")
		})
	}
}

// A CloudFormation AWS::Route53::RecordSet naming a zone that does not exist fails its resource,
// where it used to report CREATE_COMPLETE over an orphan record set.
func TestRoute53AbsentZone_ACloudFormationRecordSetForAMissingZoneFails(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	d := newCFNRoute53Deployer(t, state)
	result, err := d.Deploy(context.Background(), `{
	"Resources": {
		"Record": {
			"Type": "AWS::Route53::RecordSet",
			"Properties": {"HostedZoneId": "Z0NOSUCHZONE00", "Name": "www.absent.example.com", "Type": "A", "TTL": "60", "ResourceRecords": ["192.0.2.1"]}
		}
	}
}`, "r53-absent-zone", nil)
	require.NoError(t, err)

	record := cfnRoute53ByLogical(result)["Record"]
	assert.Contains(t, record.Error, "NoSuchHostedZone", "the record set's refusal is reported on the resource")
	assert.NotEqual(t, "CREATE_COMPLETE", result.Status, "a refused record set fails its stack")
	keys, err := state.List(context.Background(), "route53", "rrset")
	require.NoError(t, err)
	assert.Empty(t, keys, "no orphan record set is written")
}
