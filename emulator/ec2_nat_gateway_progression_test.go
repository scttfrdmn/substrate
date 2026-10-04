package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A NAT gateway's state progresses through a seeded count of DescribeNatGateways observations
// (#1188), on the shared progression helper. Every test runs on a frozen clock: the only
// thing it may be about is the order of observations.

// natFixture is a subnet and an Elastic IP a NAT gateway can be created in.
type natFixture struct {
	subnetID     string
	allocationID string
}

// newNatFixture creates a VPC, a subnet in 10.4.1.0/24 and an Elastic IP.
func newNatFixture(t *testing.T, ts *httptest.Server) natFixture {
	t.Helper()
	var vpc struct {
		VpcID string `xml:"vpc>vpcId"`
	}
	natDecode(t, natCall(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.4.0.0/16"}), &vpc)
	var subnet struct {
		SubnetID string `xml:"subnet>subnetId"`
	}
	natDecode(t, natCall(t, ts, map[string]string{"Action": "CreateSubnet", "VpcId": vpc.VpcID, "CidrBlock": "10.4.1.0/24"}), &subnet)
	var eip struct {
		AllocationID string `xml:"allocationId"`
	}
	natDecode(t, natCall(t, ts, map[string]string{"Action": "AllocateAddress"}), &eip)
	return natFixture{subnetID: subnet.SubnetID, allocationID: eip.AllocationID}
}

// natCall issues one EC2 action and requires a 200, returning the body.
func natCall(t *testing.T, ts *httptest.Server, params map[string]string) []byte {
	t.Helper()
	status, body := natCallStatus(t, ts, params)
	require.Equalf(t, http.StatusOK, status, "%s: %s", params["Action"], body)
	return body
}

// natCallStatus issues one EC2 action and returns its status and body.
func natCallStatus(t *testing.T, ts *httptest.Server, params map[string]string) (int, []byte) {
	t.Helper()
	resp := ec2Request(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

// natDecode unmarshals an XML body, failing the test on an error.
func natDecode(t *testing.T, body []byte, v any) {
	t.Helper()
	require.NoErrorf(t, xml.Unmarshal(body, v), "decode %s", body)
}

// natCreate creates a public NAT gateway in f, returning its ID and the create body.
func natCreate(t *testing.T, ts *httptest.Server, f natFixture, extra map[string]string) (string, []byte) {
	t.Helper()
	params := map[string]string{"Action": "CreateNatGateway", "SubnetId": f.subnetID, "AllocationId": f.allocationID}
	for k, v := range extra {
		params[k] = v
	}
	body := natCall(t, ts, params)
	var out struct {
		ID string `xml:"natGateway>natGatewayId"`
	}
	natDecode(t, body, &out)
	require.NotEmpty(t, out.ID, "CreateNatGateway: %s", body)
	return out.ID, body
}

// natObserved is one gateway as DescribeNatGateways reports it.
type natObserved struct {
	ID             string `xml:"natGatewayId"`
	State          string `xml:"state"`
	FailureCode    string `xml:"failureCode"`
	FailureMessage string `xml:"failureMessage"`
	PrivateIP      string `xml:"natGatewayAddressSet>item>privateIp"`
}

// natDescribe returns what one DescribeNatGateways call reports, narrowed by extra.
func natDescribe(t *testing.T, ts *httptest.Server, extra map[string]string) []natObserved {
	t.Helper()
	params := map[string]string{"Action": "DescribeNatGateways"}
	for k, v := range extra {
		params[k] = v
	}
	var out struct {
		Items []natObserved `xml:"natGatewaySet>item"`
	}
	natDecode(t, natCall(t, ts, params), &out)
	return out.Items
}

// natStates returns the state of id across n consecutive describes.
func natStates(t *testing.T, ts *httptest.Server, id string, n int) []string {
	t.Helper()
	states := make([]string, 0, n)
	for range n {
		items := natDescribe(t, ts, map[string]string{"NatGatewayId.1": id})
		require.Len(t, items, 1)
		states = append(states, items[0].State)
	}
	return states
}

// natSeed POSTs a NAT gateway progression seed and returns the response status and body.
func natSeed(t *testing.T, baseURL, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(baseURL+"/v1/ec2/nat-gateway-state", "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(got)
}

// natSeedOK seeds and requires the seed to be accepted.
func natSeedOK(t *testing.T, baseURL, body string) {
	t.Helper()
	status, got := natSeed(t, baseURL, body)
	require.Equal(t, http.StatusOK, status, "seed: %s", got)
}

func TestEC2NatGateway_AnUnseededGatewayReadsAsItAlwaysHas(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)
	f := newNatFixture(t, ts)

	id, created := natCreate(t, ts, f, nil)
	assert.Contains(t, string(created), "<state>available</state>",
		"with no seed the default count is zero, so recorded runs replay byte-identically")
	assert.NotContains(t, string(created), "<clientToken>", "no clientToken unless one was sent")
	assert.Equal(t, []string{"available", "available"}, natStates(t, ts, id, 2))

	natCall(t, ts, map[string]string{"Action": "DeleteNatGateway", "NatGatewayId": id})
	assert.Equal(t, []string{"deleted"}, natStates(t, ts, id, 1))
}

func TestEC2NatGateway_ASeededGatewayIsBornPendingAndPollsToAvailable(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)
	f := newNatFixture(t, ts)
	natSeedOK(t, ts.URL, `{"natGatewayId":"*","pendingObservations":2}`)

	id, created := natCreate(t, ts, f, nil)
	assert.Contains(t, string(created), "<state>pending</state>",
		"a gateway created under a seed is born pending, as both published samples show")
	assert.Equal(t, []string{"pending", "pending", "available", "available"}, natStates(t, ts, id, 4),
		"the create spent no observation; two describes report pending, then it settles")
}

// A waiter that polls with a state filter must still advance the countdown: the observation is
// taken before the filters run, as DescribeSnapshots does.
func TestEC2NatGateway_AStateFilteredWaiterStillAdvancesTheCountdown(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)
	f := newNatFixture(t, ts)
	natSeedOK(t, ts.URL, `{"natGatewayId":"*","pendingObservations":2}`)
	id, _ := natCreate(t, ts, f, nil)

	available := map[string]string{"Filter.1.Name": "state", "Filter.1.Value.1": "available"}
	assert.Empty(t, natDescribe(t, ts, available), "poll 1: still pending")
	assert.Empty(t, natDescribe(t, ts, available), "poll 2: still pending")
	got := natDescribe(t, ts, available)
	require.Len(t, got, 1, "poll 3 sees it available")
	assert.Equal(t, id, got[0].ID)
}

func TestEC2NatGateway_AFailedGatewayCarriesItsPublishedCodeAndMessage(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)
	f := newNatFixture(t, ts)
	natSeedOK(t, ts.URL, `{"natGatewayId":"*","pendingObservations":1,"finalState":"failed","failureCode":"InvalidAllocationID.NotFound"}`)
	id, _ := natCreate(t, ts, f, nil)

	first := natDescribe(t, ts, map[string]string{"NatGatewayId.1": id})[0]
	assert.Equal(t, "pending", first.State)
	assert.Empty(t, first.FailureCode, "no failure detail while pending")

	failed := natDescribe(t, ts, map[string]string{"NatGatewayId.1": id})[0]
	assert.Equal(t, "failed", failed.State)
	assert.Equal(t, "InvalidAllocationID.NotFound", failed.FailureCode)
	assert.Equal(t, "Elastic IP address "+f.allocationID+" could not be associated with this NAT gateway", failed.FailureMessage,
		"the page's own message, with the gateway's allocation ID in place of eipalloc-xxxxxxxx")
}

func TestEC2NatGateway_AFailedSeedWithNoCodeDefaultsToInternalError(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)
	f := newNatFixture(t, ts)
	natSeedOK(t, ts.URL, `{"natGatewayId":"*","finalState":"failed"}`)
	id, _ := natCreate(t, ts, f, nil)

	failed := natDescribe(t, ts, map[string]string{"NatGatewayId.1": id})[0]
	assert.Equal(t, "failed", failed.State)
	assert.Equal(t, "InternalError", failed.FailureCode, "a failed gateway always carries a code")
	assert.NotEmpty(t, failed.FailureMessage)
}

func TestEC2NatGateway_ADeleteReportsDeletingThenDeleted(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)
	f := newNatFixture(t, ts)
	natSeedOK(t, ts.URL, `{"natGatewayId":"*","pendingObservations":1}`)
	id, _ := natCreate(t, ts, f, nil)
	require.Equal(t, []string{"pending", "available"}, natStates(t, ts, id, 2))

	natCall(t, ts, map[string]string{"Action": "DeleteNatGateway", "NatGatewayId": id})
	assert.Equal(t, []string{"deleting", "deleted", "deleted"}, natStates(t, ts, id, 3),
		"the delete restarts the countdown, and the same count reports deleting first")
}

func TestEC2NatGateway_TheSeedEndpointRefusesWhatThePageDoesNotPublish(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)
	for _, tc := range []struct {
		name, body, want string
	}{
		{"unpublished state", `{"natGatewayId":"*","finalState":"ready"}`, "ready"},
		{"negative count", `{"natGatewayId":"*","pendingObservations":-1}`, "pendingObservations"},
		{"failure detail without failed", `{"natGatewayId":"*","failureCode":"InternalError"}`, "failed"},
		{"unpublished failure code", `{"natGatewayId":"*","finalState":"failed","failureCode":"Boom"}`, "Boom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, got := natSeed(t, ts.URL, tc.body)
			assert.Equal(t, http.StatusBadRequest, status, got)
			assert.Contains(t, got, tc.want)
		})
	}
}

func TestEC2NatGateway_ANamedAllocationIDThatResolvesToNothingIsRefused(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)
	f := newNatFixture(t, ts)
	status, body := natCallStatus(t, ts, map[string]string{
		"Action": "CreateNatGateway", "SubnetId": f.subnetID, "AllocationId": "eipalloc-0000000000000000a",
	})
	assert.Equal(t, http.StatusBadRequest, status, "%s", body)
	assert.Contains(t, string(body), "<Code>InvalidAllocationID.NotFound</Code>")
	assert.Empty(t, natDescribe(t, ts, nil), "no gateway was built without its Elastic IP")
}

func TestEC2NatGateway_PrivateIpAddressIsHonoredInsideTheSubnet(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)
	f := newNatFixture(t, ts)

	id, created := natCreate(t, ts, f, map[string]string{"PrivateIpAddress": "10.4.1.26"})
	assert.Contains(t, string(created), "<privateIp>10.4.1.26</privateIp>")
	assert.Equal(t, "10.4.1.26", natDescribe(t, ts, map[string]string{"NatGatewayId.1": id})[0].PrivateIP)

	for _, bad := range []string{"10.9.9.9", "not-an-ip", "fe80::1"} {
		status, body := natCallStatus(t, ts, map[string]string{
			"Action": "CreateNatGateway", "SubnetId": f.subnetID, "PrivateIpAddress": bad, "ConnectivityType": "private",
		})
		assert.Equalf(t, http.StatusBadRequest, status, "%s: %s", bad, body)
		assert.Containsf(t, string(body), "<Code>InvalidParameterValue</Code>", "%s", bad)
	}
}

func TestEC2NatGateway_AClientTokenIsEchoedAndMakesTheCreateIdempotent(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)
	f := newNatFixture(t, ts)
	const token = "nat-token-1"

	id, first := natCreate(t, ts, f, map[string]string{"ClientToken": token})
	assert.Contains(t, string(first), "<clientToken>"+token+"</clientToken>")

	again, second := natCreate(t, ts, f, map[string]string{"ClientToken": token})
	assert.Equal(t, id, again, "a retry with the same token and parameters answers the first gateway")
	assert.Contains(t, string(second), "<clientToken>"+token+"</clientToken>")
	assert.Len(t, natDescribe(t, ts, nil), 1, "and creates no second gateway")

	status, body := natCallStatus(t, ts, map[string]string{
		"Action": "CreateNatGateway", "SubnetId": f.subnetID, "ClientToken": token, "ConnectivityType": "private",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, string(body), "<Code>IdempotentParameterMismatch</Code>")

	status, body = natCallStatus(t, ts, map[string]string{
		"Action": "CreateNatGateway", "SubnetId": f.subnetID, "ClientToken": strings.Repeat("x", 65),
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, string(body), "<Code>InvalidParameterValue</Code>", "a token over 64 characters")
}

// A store fault at any new read or write is an error, never a published refusal and never a 200.
func TestEC2NatGateway_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		params func(f natFixture) map[string]string
	}{
		{"elastic IP read", func(m *cfFaultStateManager) { m.failGet = "eip:" },
			func(f natFixture) map[string]string {
				return map[string]string{"Action": "CreateNatGateway", "SubnetId": f.subnetID, "AllocationId": f.allocationID}
			}},
		{"corrupt elastic IP", func(m *cfFaultStateManager) { m.corruptGet = "eip:" },
			func(f natFixture) map[string]string {
				return map[string]string{"Action": "CreateNatGateway", "SubnetId": f.subnetID, "AllocationId": f.allocationID}
			}},
		{"client token read", func(m *cfFaultStateManager) { m.failGet = "nat_token:" },
			func(f natFixture) map[string]string {
				return map[string]string{"Action": "CreateNatGateway", "SubnetId": f.subnetID, "ClientToken": "t"}
			}},
		{"client token write", func(m *cfFaultStateManager) { m.failPut = "nat_token:" },
			func(f natFixture) map[string]string {
				return map[string]string{"Action": "CreateNatGateway", "SubnetId": f.subnetID, "ClientToken": "t2"}
			}},
		{"progression read", func(m *cfFaultStateManager) { m.failGet = "status:" },
			func(f natFixture) map[string]string {
				return map[string]string{"Action": "CreateNatGateway", "SubnetId": f.subnetID}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			ts := newEC2TestServerWithState(t, fault, func(c *emulator.TimeController) { c.SetScale(0) })
			f := newNatFixture(t, ts)
			tc.arm(fault)
			status, body := natCallStatus(t, ts, tc.params(f))
			assert.GreaterOrEqualf(t, status, http.StatusInternalServerError, "%s: %s", tc.name, body)
		})
	}
}

// TestEC2NatGateway_ASeededProgressionReplaysIdentically records a seed, a create and the
// pending → available sequence, then replays the stream with the control-plane handler, so the
// recorded seed is re-applied in position (#1140) and every replayed observation must match.
func TestEC2NatGateway_ASeededProgressionReplaysIdentically(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.FreezeTime()

	natSeedOK(t, ts.URL, `{"natGatewayId":"*","pendingObservations":2}`)
	vpc := natReplayElement(t, ts, "CreateVpc", url.Values{"CidrBlock": {"10.5.0.0/16"}}, "vpcId")
	subnet := natReplayElement(t, ts, "CreateSubnet", url.Values{"VpcId": {vpc}, "CidrBlock": {"10.5.1.0/24"}}, "subnetId")
	alloc := natReplayElement(t, ts, "AllocateAddress", nil, "allocationId")
	id := natReplayElement(t, ts, "CreateNatGateway", url.Values{"SubnetId": {subnet}, "AllocationId": {alloc}}, "natGatewayId")

	states := make([]string, 0, 3)
	for range 3 {
		states = append(states, natReplayElement(t, ts, "DescribeNatGateways", url.Values{"NatGatewayId.1": {id}}, "state"))
	}
	require.Equal(t, []string{"pending", "pending", "available"}, states, "the recording observes the seeded sequence")

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Zero(t, results.FailedEvents)
	assert.Empty(t, results.Differences,
		"the replay re-applies the seed and reproduces pending, pending, available: %s", replayDifferenceSummary(results))
}

// natReplayElement issues one EC2 action against a test server and returns the text of the first
// element named name in its response.
func natReplayElement(t *testing.T, ts *emulator.TestServer, action string, extra url.Values, name string) string {
	t.Helper()
	body := ec2ReplayCall(t, ts, action, extra)
	start := strings.Index(string(body), "<"+name+">")
	require.GreaterOrEqualf(t, start, 0, "%s answered no <%s>: %s", action, name, body)
	rest := string(body)[start+len(name)+2:]
	end := strings.Index(rest, "</"+name+">")
	require.GreaterOrEqual(t, end, 0)
	return rest[:end]
}
