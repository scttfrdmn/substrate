package emulator_test

import (
	"encoding/xml"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ec2RequestIDElement matches the requestId element every EC2 success document leads with since
// #1189.
var ec2RequestIDElement = regexp.MustCompile(`<requestId>[^<]*</requestId>`)

// ec2StripRequestID removes the requestId element from an EC2 body, for a test that compares two
// bodies over unchanged state: the element is each request's own ID, so two calls differ in it by
// design.
func ec2StripRequestID(body string) string {
	return ec2RequestIDElement.ReplaceAllString(body, "")
}

// TestEC2_SuccessResponsesCarryTheRequestID is #1189: every EC2 success document carries the
// published requestId element — "requestId: The ID of the request" — as its first child, as every
// sample response opens with it, and its value is the request ID the event log records for that
// request, not a freshly minted one.
func TestEC2_SuccessResponsesCarryTheRequestID(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	calls := []struct {
		action string
		params url.Values
		root   string
	}{
		{"DescribeVpcs", nil, "DescribeVpcsResponse"},
		{"CreateVpc", url.Values{"CidrBlock": {"10.90.0.0/16"}}, "CreateVpcResponse"},
		{"AllocateAddress", nil, "AllocateAddressResponse"},
		{"CreateKeyPair", url.Values{"KeyName": {"rid"}, "KeyType": {"ed25519"}}, "CreateKeyPairResponse"},
		{"DescribeInstances", nil, "DescribeInstancesResponse"},
	}
	var bodies []string
	for _, c := range calls {
		body := string(ec2ReplayCall(t, ts, c.action, c.params))
		bodies = append(bodies, body)
		open := `<` + c.root + ` xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"><requestId>`
		assert.Contains(t, body, open, "%s leads with requestId", c.action)
	}

	events, err := ts.Store().GetStream(t.Context(), replayStreamID)
	require.NoError(t, err)
	var recorded []string
	for _, e := range events {
		if e.Service == "ec2" {
			recorded = append(recorded, e.RequestID)
		}
	}
	require.Len(t, recorded, len(calls))
	for i, body := range bodies {
		var doc struct {
			RequestID string `xml:"requestId"`
		}
		require.NoError(t, xml.Unmarshal([]byte(body), &doc))
		require.NotEmpty(t, doc.RequestID)
		assert.Equal(t, recorded[i], doc.RequestID, "%s answers the request ID the event records", calls[i].action)
		assert.Equal(t, 1, strings.Count(body, "<requestId>"), "%s carries one requestId", calls[i].action)
	}
}

// TestEC2_RequestIDReplaysByteIdentically pins #1189's replay criterion: the value is the recorded
// request ID, so a replayed EC2 call renders the body it recorded and the replay reports no
// difference.
func TestEC2_RequestIDReplaysByteIdentically(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	ts.FreezeTime()

	ec2ReplayCall(t, ts, "CreateVpc", url.Values{"CidrBlock": {"10.91.0.0/16"}})
	ec2ReplayCall(t, ts, "DescribeVpcs", nil)

	results, err := replayEngineFor(ts, emulator.ReplayConfig{ValidateState: true}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Equal(t, results.TotalEvents, results.SuccessEvents)
	assert.Empty(t, results.Differences, "a replayed EC2 call renders the requestId it recorded: %s",
		replayDifferenceSummary(results))
}

// TestEC2_ErrorResponsesKeepTheirRequestID pins the error half: EC2's published error shape
// carries the ID as <RequestID> after <Errors> (errors-overview.html, "Example error response"),
// which the Query error writer renders; the success stamp adds no requestId to it.
func TestEC2_ErrorResponsesKeepTheirRequestID(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	resp := ec2Request(t, ts, map[string]string{"Action": "DeleteVpc", "VpcId": "vpc-0000000000000dead"})
	body := readBody(t, resp)
	assert.Contains(t, body, "</Errors><RequestID>")
	assert.NotContains(t, body, "<requestId>")
}
