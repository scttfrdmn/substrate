package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The record-level locking of #1614's review: every rewrite of a VPC, network ACL, DHCP option
// set or flow log record holds one lock, so concurrent writers to the same record cannot lose
// each other's changes.

// ec2ConcurrentCall sends params from any goroutine and reports the status, never calling t's
// fatal methods off the test goroutine.
func ec2ConcurrentCall(ts *httptest.Server, params map[string]string) int {
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	if err != nil {
		return 0
	}
	req.Host = "ec2.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

// ec2RunConcurrently runs each of the workers n times, all at once, and requires every call to
// answer 200.
func ec2RunConcurrently(t *testing.T, ts *httptest.Server, n int, workers ...func(i int) map[string]string) {
	t.Helper()
	statuses := make(chan int, n*len(workers))
	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				statuses <- ec2ConcurrentCall(ts, w(i))
			}
		}()
	}
	wg.Wait()
	close(statuses)
	for s := range statuses {
		require.Equal(t, http.StatusOK, s)
	}
}

func TestEC2NetworkControls_ConcurrentVPCWritesKeepEveryChange(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	vpcID := ec2CreateVPC(t, ts, "10.77.0.0/16")
	custom := doptCreate(t, call, nil)
	const n = 20
	ec2RunConcurrently(t, ts, n,
		func(i int) map[string]string {
			return map[string]string{"Action": "CreateTags", "ResourceId.1": vpcID, "Tag.1.Key": "k" + strconv.Itoa(i), "Tag.1.Value": "v"}
		},
		func(i int) map[string]string {
			set := "default"
			if i == n-1 || i%2 == 0 {
				set = custom.ID
			}
			return map[string]string{"Action": "AssociateDhcpOptions", "VpcId": vpcID, "DhcpOptionsId": set}
		},
		func(i int) map[string]string {
			value := strconv.FormatBool(i == n-1 || i%2 == 1)
			return map[string]string{"Action": "ModifyVpcAttribute", "VpcId": vpcID, "EnableDnsHostnames.Value": value}
		},
	)
	vpcs := ec2DescribeVPCs(t, ts, map[string]string{"Action": "DescribeVpcs", "VpcId.1": vpcID})
	require.Len(t, vpcs, 1)
	assert.Len(t, vpcs[0].Tags, n, "no tag lost to a concurrent rewrite")
	assert.Equal(t, custom.ID, doptVPCSet(t, call, vpcID), "the last association stands")
	var attr struct {
		Value string `xml:"enableDnsHostnames>value"`
	}
	require.NoError(t, xml.Unmarshal(call(map[string]string{"Action": "DescribeVpcAttribute", "VpcId": vpcID,
		"Attribute": "enableDnsHostnames"}), &attr))
	assert.Equal(t, "true", attr.Value, "the last attribute change stands")
}

func TestEC2NetworkControls_ConcurrentACLWritesKeepEveryChange(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	acl := naclCreate(t, call, ec2CreateVPC(t, ts, "10.78.0.0/16"), nil)
	const n = 15
	ec2RunConcurrently(t, ts, n,
		func(i int) map[string]string {
			return naclEntryParams("CreateNetworkAclEntry", acl.ID, strconv.Itoa(100+i), nil)
		},
		func(i int) map[string]string {
			return map[string]string{"Action": "CreateTags", "ResourceId.1": acl.ID, "Tag.1.Key": "k" + strconv.Itoa(i), "Tag.1.Value": "v"}
		},
	)
	got, _ := naclList(t, call, map[string]string{"NetworkAclId.1": acl.ID})
	require.Len(t, got, 1)
	assert.Len(t, got[0].Entries, n+2, "every entry and the two catch-all denies")
	assert.Len(t, got[0].Tags, n, "no tag lost to a concurrent entry write")
}

func TestEC2NetworkControls_ConcurrentIdenticalFlowLogsCreateOne(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	vpcID := ec2CreateVPC(t, ts, "10.79.0.0/16")
	params := flCWL(vpcID)
	params["Action"] = "CreateFlowLogs"
	ec2RunConcurrently(t, ts, 5,
		func(int) map[string]string { return params },
		func(int) map[string]string { return params },
	)
	logs, _ := flList(t, call, ec2OneFilter("DescribeFlowLogs", "resource-id", vpcID))
	assert.Len(t, logs, 1, "the duplicate check and the write are one step")
}

// TestEC2NetworkControls_ARefusedTagWritePersistsNothing pins that a CreateTags refused for the
// tag limit leaves a synthesized default ACL and the default DHCP set unstored.
func TestEC2NetworkControls_ARefusedTagWritePersistsNothing(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	const vpcID = "vpc-0dddddddddddddddd"
	data, err := json.Marshal(emulator.EC2VPC{VPCID: vpcID, CIDRBlock: "10.84.0.0/16", State: "available",
		AccountID: "123456789012", Region: "us-east-1"})
	require.NoError(t, err)
	require.NoError(t, state.Put(context.Background(), "ec2", "vpc:123456789012/us-east-1/"+vpcID, data))
	acl := naclVPCDefault(t, call, vpcID)
	sets, _ := doptList(t, call, nil)
	require.Len(t, sets, 1)

	for _, id := range []string{acl.ID, sets[0].ID} {
		params := map[string]string{"Action": "CreateTags", "ResourceId.1": id}
		for i := 1; i <= 51; i++ {
			params["Tag."+strconv.Itoa(i)+".Key"] = "k" + strconv.Itoa(i)
			params["Tag."+strconv.Itoa(i)+".Value"] = "v"
		}
		ec2RequireRefusal(t, ts, params, "TagLimitExceeded")
	}
	raw, err := state.Get(context.Background(), "ec2", "acl:123456789012/us-east-1/"+acl.ID)
	require.NoError(t, err)
	assert.Nil(t, raw, "the refused write persisted no ACL")
	raw, err = state.Get(context.Background(), "ec2", "dopt:123456789012/us-east-1/"+sets[0].ID)
	require.NoError(t, err)
	assert.Nil(t, raw, "the refused write persisted no DHCP set")
}

// TestEC2NetworkControls_DeleteVpcRemovesItsFlowLogs pins API_DeleteVpc's "flow logs for deleted
// VPCs are eventually automatically removed", and that a deleted subnet's flow log is kept.
func TestEC2NetworkControls_DeleteVpcRemovesItsFlowLogs(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	call := ec2CallHTTP(t, func(p map[string]string) *http.Response { return ec2Request(t, ts, p) })
	vpcID := ec2CreateVPC(t, ts, "10.85.0.0/16")
	subnetID := ec2CreateSubnetIn(t, ts, vpcID, "10.85.1.0/24")
	other := ec2CreateVPC(t, ts, "10.86.0.0/16")
	require.Len(t, flCreate(t, call, flCWL(vpcID)).IDs, 1)
	require.Len(t, flCreate(t, call, flCWL(other)).IDs, 1)
	subnetLog := flCWL(subnetID)
	subnetLog["ResourceType"] = "Subnet"
	require.Len(t, flCreate(t, call, subnetLog).IDs, 1)

	ec2OK(t, ts, map[string]string{"Action": "DeleteSubnet", "SubnetId": subnetID})
	ec2OK(t, ts, map[string]string{"Action": "DeleteVpc", "VpcId": vpcID})
	logs, _ := flList(t, call, nil)
	var resources []string
	for _, fl := range logs {
		resources = append(resources, fl.ResourceID)
	}
	assert.ElementsMatch(t, []string{other, subnetID}, resources)
}
