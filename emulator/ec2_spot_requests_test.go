package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Spot Instance requests (#1436): RunInstances with InstanceMarketOptions, RequestSpotInstances,
// DescribeSpotInstanceRequests and CancelSpotInstanceRequests, and the persistent-request
// replacement a cancel-before-terminate path exists to prevent.

type ec2DescribedSpotRequest struct {
	ID           string `xml:"spotInstanceRequestId"`
	Type         string `xml:"type"`
	State        string `xml:"state"`
	StatusCode   string `xml:"status>code"`
	InstanceID   string `xml:"instanceId"`
	Interruption string `xml:"instanceInterruptionBehavior"`
	Product      string `xml:"productDescription"`
	LaunchedAZ   string `xml:"launchedAvailabilityZone"`
	ImageID      string `xml:"launchSpecification>imageId"`
	InstanceType string `xml:"launchSpecification>instanceType"`
	ValidUntil   string `xml:"validUntil"`
	Tags         []struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	} `xml:"tagSet>item"`
}

type ec2SpotInstance struct {
	InstanceID string `xml:"instanceId"`
	Lifecycle  string `xml:"instanceLifecycle"`
	RequestID  string `xml:"spotInstanceRequestId"`
	State      string `xml:"instanceState>name"`
	Tags       []struct {
		Key string `xml:"key"`
	} `xml:"tagSet>item"`
}

// ec2SpotRunParams is a persistent Spot launch through RunInstances, as spawn makes one.
func ec2SpotRunParams(extra map[string]string) map[string]string {
	params := map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "InstanceType": "t3.micro",
		"MinCount": "1", "MaxCount": "1",
		"InstanceMarketOptions.MarketType":                               "spot",
		"InstanceMarketOptions.SpotOptions.SpotInstanceType":             "persistent",
		"InstanceMarketOptions.SpotOptions.InstanceInterruptionBehavior": "stop",
		"TagSpecification.1.ResourceType":                                "instance",
		"TagSpecification.1.Tag.1.Key":                                   "spawn:managed",
		"TagSpecification.1.Tag.1.Value":                                 "true",
		"TagSpecification.2.ResourceType":                                "spot-instances-request",
		"TagSpecification.2.Tag.1.Key":                                   "spawn:request",
		"TagSpecification.2.Tag.1.Value":                                 "yes",
	}
	for k, v := range extra {
		params[k] = v
	}
	return params
}

func ec2RunSpot(t *testing.T, ts *httptest.Server, extra map[string]string) ec2SpotInstance {
	t.Helper()
	var run struct {
		Instances []ec2SpotInstance `xml:"instancesSet>item"`
	}
	ec2FleetXML(t, ts, ec2SpotRunParams(extra), &run)
	require.Len(t, run.Instances, 1)
	return run.Instances[0]
}

func ec2DescribeSpotRequests(t *testing.T, ts *httptest.Server, params map[string]string) ([]ec2DescribedSpotRequest, string) {
	t.Helper()
	full := map[string]string{"Action": "DescribeSpotInstanceRequests"}
	for k, v := range params {
		full[k] = v
	}
	var doc struct {
		Requests  []ec2DescribedSpotRequest `xml:"spotInstanceRequestSet>item"`
		NextToken string                    `xml:"nextToken"`
	}
	ec2FleetXML(t, ts, full, &doc)
	return doc.Requests, doc.NextToken
}

func ec2SpotRequest(t *testing.T, ts *httptest.Server, id string) ec2DescribedSpotRequest {
	t.Helper()
	got, _ := ec2DescribeSpotRequests(t, ts, map[string]string{"SpotInstanceRequestId.1": id})
	require.Len(t, got, 1)
	return got[0]
}

func ec2SpotInstances(t *testing.T, ts *httptest.Server, params map[string]string) []ec2SpotInstance {
	t.Helper()
	full := map[string]string{"Action": "DescribeInstances"}
	for k, v := range params {
		full[k] = v
	}
	var doc struct {
		Instances []ec2SpotInstance `xml:"reservationSet>item>instancesSet>item"`
	}
	ec2FleetXML(t, ts, full, &doc)
	return doc.Instances
}

func ec2Terminate(t *testing.T, ts *httptest.Server, id string) {
	t.Helper()
	ec2FleetXML(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": id}, nil)
}

func ec2SeedSpotOutcome(t *testing.T, ts *httptest.Server, body string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/ec2/spot-request-outcomes", "application/json", strings.NewReader(body))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestEC2_Spot_RunInstancesCreatesALinkedRequest is the issue's first bullet.
func TestEC2_Spot_RunInstancesCreatesALinkedRequest(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	inst := ec2RunSpot(t, ts, nil)

	assert.Equal(t, "spot", inst.Lifecycle)
	require.True(t, strings.HasPrefix(inst.RequestID, "sir-"), inst.RequestID)

	r := ec2SpotRequest(t, ts, inst.RequestID)
	assert.Equal(t, "persistent", r.Type)
	assert.Equal(t, "active", r.State)
	assert.Equal(t, "fulfilled", r.StatusCode)
	assert.Equal(t, inst.InstanceID, r.InstanceID)
	assert.Equal(t, "stop", r.Interruption)
	assert.Equal(t, "Linux/UNIX", r.Product)
	assert.Equal(t, "us-east-1a", r.LaunchedAZ)
	assert.Equal(t, ec2TestImage, r.ImageID)
	require.Len(t, r.Tags, 1, "the request carries the spot-instances-request tags, not the instance's")
	assert.Equal(t, "spawn:request", r.Tags[0].Key)

	described := ec2SpotInstances(t, ts, map[string]string{"InstanceId.1": inst.InstanceID})
	require.Len(t, described, 1)
	assert.Equal(t, "spot", described[0].Lifecycle)
	assert.Equal(t, inst.RequestID, described[0].RequestID)

	onDemand := ec2RunInstanceID(t, ts)
	lifecycle := ec2SpotInstances(t, ts, ec2Filter("instance-lifecycle", "spot"))
	require.Len(t, lifecycle, 1, "instance-lifecycle=spot excludes the On-Demand %s", onDemand)
	assert.Equal(t, inst.InstanceID, lifecycle[0].InstanceID)
	assert.Len(t, ec2SpotInstances(t, ts, ec2Filter("spot-instance-request-id", inst.RequestID)), 1)
}

// ec2RunInstanceID launches one On-Demand instance and returns its ID.
func ec2RunInstanceID(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	var run struct {
		ID string `xml:"instancesSet>item>instanceId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
	}, &run)
	require.NotEmpty(t, run.ID)
	return run.ID
}

// TestEC2_Spot_DescribeFilters covers the filters the issue names — state, instance-id, tag:* —
// and a few of the others.
func TestEC2_Spot_DescribeFilters(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	a := ec2RunSpot(t, ts, nil)
	b := ec2RunSpot(t, ts, map[string]string{
		"InstanceMarketOptions.SpotOptions.SpotInstanceType":             "one-time",
		"InstanceMarketOptions.SpotOptions.InstanceInterruptionBehavior": "terminate",
		"TagSpecification.2.Tag.1.Value":                                 "no",
	})
	ec2FleetXML(t, ts, map[string]string{"Action": "CancelSpotInstanceRequests", "SpotInstanceRequestId.1": b.RequestID}, nil)

	ids := func(params map[string]string) []string {
		got, _ := ec2DescribeSpotRequests(t, ts, params)
		out := make([]string, 0, len(got))
		for _, r := range got {
			out = append(out, r.ID)
		}
		return out
	}
	for _, tc := range []struct {
		name   string
		filter map[string]string
		want   []string
	}{
		{"state active", ec2Filter("state", "active"), []string{a.RequestID}},
		{"state cancelled", ec2Filter("state", "cancelled"), []string{b.RequestID}},
		{"instance-id", ec2Filter("instance-id", b.InstanceID), []string{b.RequestID}},
		{"tag:", ec2Filter("tag:spawn:request", "yes"), []string{a.RequestID}},
		{"tag: values OR", ec2Filter("tag:spawn:request", "yes", "no"), []string{a.RequestID, b.RequestID}},
		{"tag-key", ec2Filter("tag-key", "spawn:request"), []string{a.RequestID, b.RequestID}},
		{"type", ec2Filter("type", "one-time"), []string{b.RequestID}},
		{"status-code", ec2Filter("status-code", "request-canceled-and-instance-running"), []string{b.RequestID}},
		{"launch.image-id", ec2Filter("launch.image-id", ec2TestImage), []string{a.RequestID, b.RequestID}},
		{"launch.instance-type miss", ec2Filter("launch.instance-type", "m5.large"), nil},
		{"fault-code names none", ec2Filter("fault-code", "x"), nil},
		{"an inert name constrains nothing", ec2Filter("launch.monitoring-enabled", "true"), []string{a.RequestID, b.RequestID}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, ids(tc.filter))
		})
	}

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "DescribeSpotInstanceRequests", "Filter.1.Name": "no-such-filter", "Filter.1.Value.1": "x",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
}

// TestEC2_Spot_CancelDoesNotTerminate pins "Canceling a Spot Instance request does not
// terminate running Spot Instances", and that a cancelled persistent request launches no
// replacement when its instance is then terminated — the cancel-before-terminate order.
func TestEC2_Spot_CancelDoesNotTerminate(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	inst := ec2RunSpot(t, ts, nil)

	var cancelled struct {
		Items []struct {
			ID    string `xml:"spotInstanceRequestId"`
			State string `xml:"state"`
		} `xml:"spotInstanceRequestSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CancelSpotInstanceRequests", "SpotInstanceRequestId.1": inst.RequestID,
	}, &cancelled)
	require.Len(t, cancelled.Items, 1)
	assert.Equal(t, "cancelled", cancelled.Items[0].State)

	r := ec2SpotRequest(t, ts, inst.RequestID)
	assert.Equal(t, "cancelled", r.State)
	assert.Equal(t, "request-canceled-and-instance-running", r.StatusCode)
	running := ec2SpotInstances(t, ts, map[string]string{"InstanceId.1": inst.InstanceID})
	require.Len(t, running, 1)
	assert.NotEqual(t, "terminated", running[0].State, "cancelling leaves the instance running")

	ec2Terminate(t, ts, inst.InstanceID)
	r = ec2SpotRequest(t, ts, inst.RequestID)
	assert.Equal(t, "cancelled", r.State)
	assert.Equal(t, "instance-terminated-by-user", r.StatusCode)
	assert.Equal(t, inst.InstanceID, r.InstanceID, "no replacement: the request still names the terminated instance")
	assert.Len(t, ec2SpotInstances(t, ts, ec2Filter("instance-lifecycle", "spot")), 1,
		"a cancelled request launches nothing")
}

// TestEC2_Spot_PersistentRequestReplacesATerminatedInstance is the behavior the issue says
// matters most: terminating the instance of an active persistent request launches a replacement
// with no tags, and the request's InstanceId moves to it.
func TestEC2_Spot_PersistentRequestReplacesATerminatedInstance(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	inst := ec2RunSpot(t, ts, nil)
	require.Len(t, inst.Tags, 1, "the original instance carries its launch tags")

	ec2Terminate(t, ts, inst.InstanceID)

	r := ec2SpotRequest(t, ts, inst.RequestID)
	assert.Equal(t, "active", r.State)
	assert.Equal(t, "fulfilled", r.StatusCode)
	require.NotEqual(t, inst.InstanceID, r.InstanceID, "the request names the replacement")

	replacement := ec2SpotInstances(t, ts, map[string]string{"InstanceId.1": r.InstanceID})
	require.Len(t, replacement, 1)
	assert.Equal(t, "spot", replacement[0].Lifecycle)
	assert.Equal(t, inst.RequestID, replacement[0].RequestID)
	assert.Empty(t, replacement[0].Tags, "a replacement carries no tags: LaunchSpecification has no tag field")
	assert.NotEqual(t, "terminated", replacement[0].State)

	// And cancel-then-terminate ends the chain.
	ec2FleetXML(t, ts, map[string]string{"Action": "CancelSpotInstanceRequests", "SpotInstanceRequestId.1": inst.RequestID}, nil)
	ec2Terminate(t, ts, r.InstanceID)
	assert.Equal(t, r.InstanceID, ec2SpotRequest(t, ts, inst.RequestID).InstanceID)
}

// TestEC2_Spot_RequestsThatDoNotReplace covers the two requests whose terminated instance is not
// replaced: a one-time request, and a persistent one past its ValidUntil.
func TestEC2_Spot_RequestsThatDoNotReplace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		extra map[string]string
	}{
		{"one-time", map[string]string{
			"InstanceMarketOptions.SpotOptions.SpotInstanceType":             "one-time",
			"InstanceMarketOptions.SpotOptions.InstanceInterruptionBehavior": "terminate",
		}},
		{"persistent past ValidUntil", map[string]string{
			"InstanceMarketOptions.SpotOptions.ValidUntil": "2001-01-01T00:00:00Z",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			inst := ec2RunSpot(t, ts, tc.extra)
			ec2Terminate(t, ts, inst.InstanceID)
			r := ec2SpotRequest(t, ts, inst.RequestID)
			assert.Equal(t, "closed", r.State)
			assert.Equal(t, "instance-terminated-by-user", r.StatusCode)
			assert.Equal(t, inst.InstanceID, r.InstanceID)
		})
	}
}

// TestEC2_Spot_Refusals covers the published errors and the RunInstances option rules.
func TestEC2_Spot_Refusals(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	inst := ec2RunSpot(t, ts, nil)

	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"cancel an unknown request", map[string]string{
			"Action": "CancelSpotInstanceRequests", "SpotInstanceRequestId.1": "sir-0000dead",
		}, "InvalidSpotInstanceRequestID.NotFound"},
		{"cancel a malformed ID", map[string]string{
			"Action": "CancelSpotInstanceRequests", "SpotInstanceRequestId.1": "spot-1",
		}, "InvalidSpotInstanceRequestID.Malformed"},
		{"cancel naming nothing", map[string]string{"Action": "CancelSpotInstanceRequests"}, "MissingParameter"},
		{"describe an unknown request", map[string]string{
			"Action": "DescribeSpotInstanceRequests", "SpotInstanceRequestId.1": "sir-0000dead",
		}, "InvalidSpotInstanceRequestID.NotFound"},
		{"an ID list with MaxResults", map[string]string{
			"Action": "DescribeSpotInstanceRequests", "SpotInstanceRequestId.1": inst.RequestID, "MaxResults": "5",
		}, "InvalidParameterCombination"},
		{"RunInstances persistent with terminate", ec2SpotRunParams(map[string]string{
			"InstanceMarketOptions.SpotOptions.InstanceInterruptionBehavior": "terminate",
		}), "InvalidParameterCombination"},
		{"RunInstances one-time with ValidUntil", ec2SpotRunParams(map[string]string{
			"InstanceMarketOptions.SpotOptions.SpotInstanceType":             "one-time",
			"InstanceMarketOptions.SpotOptions.InstanceInterruptionBehavior": "terminate",
			"InstanceMarketOptions.SpotOptions.ValidUntil":                   "2030-01-01T00:00:00Z",
		}), "InvalidParameterCombination"},
		{"an unknown request type", ec2SpotRunParams(map[string]string{
			"InstanceMarketOptions.SpotOptions.SpotInstanceType": "sometimes",
		}), "InvalidParameterValue"},
		{"a max price below the floor", ec2SpotRunParams(map[string]string{
			"InstanceMarketOptions.SpotOptions.MaxPrice": "0.0001",
		}), "InvalidParameterValue"},
		{"RequestSpotInstances tags another type", map[string]string{
			"Action": "RequestSpotInstances", "LaunchSpecification.ImageId": ec2TestImage,
			"TagSpecification.1.ResourceType": "instance",
			"TagSpecification.1.Tag.1.Key":    "k", "TagSpecification.1.Tag.1.Value": "v",
		}, "InvalidParameterValue"},
		{"RequestSpotInstances an unknown AMI", map[string]string{
			"Action": "RequestSpotInstances", "LaunchSpecification.ImageId": "ami-12345678",
		}, "InvalidAMIID.NotFound"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, _ := ec2ErrorDetail(t, ts, tc.params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.code, code)
		})
	}

	// A cancel naming one good and one unknown ID cancels neither.
	_, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "CancelSpotInstanceRequests", "SpotInstanceRequestId.1": inst.RequestID,
		"SpotInstanceRequestId.2": "sir-0000dead",
	})
	assert.Equal(t, "InvalidSpotInstanceRequestID.NotFound", code)
	assert.Equal(t, "active", ec2SpotRequest(t, ts, inst.RequestID).State)
}

// TestEC2_Spot_RequestSpotInstances covers the legacy operation: one request per instance, tags
// on the requests, none on the instances.
func TestEC2_Spot_RequestSpotInstances(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	var created struct {
		Requests []ec2DescribedSpotRequest `xml:"spotInstanceRequestSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RequestSpotInstances", "InstanceCount": "2", "Type": "one-time",
		"LaunchSpecification.ImageId": ec2TestImageWindows, "LaunchSpecification.InstanceType": "m5.large",
		"TagSpecification.1.ResourceType": "spot-instances-request",
		"TagSpecification.1.Tag.1.Key":    "team", "TagSpecification.1.Tag.1.Value": "hpc",
	}, &created)
	require.Len(t, created.Requests, 2, "one request per instance")
	seen := map[string]bool{}
	for _, r := range created.Requests {
		assert.Equal(t, "active", r.State)
		assert.Equal(t, "fulfilled", r.StatusCode)
		assert.Equal(t, "one-time", r.Type)
		assert.Equal(t, "Windows", r.Product, "the AMI's platform decides the product description")
		assert.Equal(t, "m5.large", r.InstanceType)
		assert.NotEmpty(t, r.ValidUntil, "a one-time request is valid for seven days by default")
		require.Len(t, r.Tags, 1)
		require.NotEmpty(t, r.InstanceID)
		seen[r.InstanceID] = true
	}
	assert.Len(t, seen, 2, "each request names its own instance")
	for id := range seen {
		got := ec2SpotInstances(t, ts, map[string]string{"InstanceId.1": id})
		require.Len(t, got, 1)
		assert.Equal(t, "spot", got[0].Lifecycle)
		assert.Empty(t, got[0].Tags, "RequestSpotInstances tags the request, not the instance")
	}

	// CreateTags reaches a request by its sir- ID.
	ec2TagResource(t, ts, created.Requests[0].ID, "extra", "1")
	assert.Len(t, ec2SpotRequest(t, ts, created.Requests[0].ID).Tags, 2)
}

// TestEC2_Spot_DescribePaginates walks the listing a page at a time.
func TestEC2_Spot_DescribePaginates(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RequestSpotInstances", "InstanceCount": "3", "LaunchSpecification.ImageId": ec2TestImage,
	}, nil)
	all, token := ec2DescribeSpotRequests(t, ts, nil)
	require.Len(t, all, 3)
	assert.Empty(t, token)

	var walked []string
	params := map[string]string{"MaxResults": "2"}
	for {
		page, next := ec2DescribeSpotRequests(t, ts, params)
		for _, r := range page {
			walked = append(walked, r.ID)
		}
		if next == "" {
			break
		}
		params = map[string]string{"MaxResults": "2", "NextToken": next}
	}
	want := make([]string, 0, len(all))
	for _, r := range all {
		want = append(want, r.ID)
	}
	assert.Equal(t, want, walked)
}

// TestEC2_Spot_SeededCapacityShortfall pins the seed: RunInstances answers
// InsufficientInstanceCapacity, RequestSpotInstances leaves open requests, a persistent request's
// replacement leaves it open, and clearing the seed restores fulfillment.
func TestEC2_Spot_SeededCapacityShortfall(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	inst := ec2RunSpot(t, ts, nil)

	ec2SeedSpotOutcome(t, ts, `{"statusCode":"capacity-not-available"}`)

	status, code, _ := ec2ErrorDetail(t, ts, ec2SpotRunParams(nil))
	assert.Equal(t, http.StatusInternalServerError, status, "InsufficientInstanceCapacity is a server error")
	assert.Equal(t, "InsufficientInstanceCapacity", code)

	var created struct {
		Requests []ec2DescribedSpotRequest `xml:"spotInstanceRequestSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action": "RequestSpotInstances", "LaunchSpecification.ImageId": ec2TestImage,
	}, &created)
	require.Len(t, created.Requests, 1)
	assert.Equal(t, "open", created.Requests[0].State)
	assert.Equal(t, "capacity-not-available", created.Requests[0].StatusCode)
	assert.Empty(t, created.Requests[0].InstanceID)

	ec2Terminate(t, ts, inst.InstanceID)
	r := ec2SpotRequest(t, ts, inst.RequestID)
	assert.Equal(t, "open", r.State, "the persistent request reopens with no capacity for a replacement")
	assert.Equal(t, "capacity-not-available", r.StatusCode)
	assert.Empty(t, r.InstanceID)

	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/v1/ec2/spot-request-outcomes", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "spot", ec2RunSpot(t, ts, nil).Lifecycle, "a cleared seed fulfills again")

	resp, err = http.Post(ts.URL+"/v1/ec2/spot-request-outcomes", "application/json",
		strings.NewReader(`{"statusCode":"fulfilled"}`))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "only an unfulfilled status is seedable")
}

// TestEC2_Spot_SeedIsScopedByInstanceType pins the seed's key: a type-scoped seed leaves other
// types fulfilled.
func TestEC2_Spot_SeedIsScopedByInstanceType(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	ec2SeedSpotOutcome(t, ts, `{"instanceType":"p4d.24xlarge","statusCode":"capacity-not-available"}`)
	assert.Equal(t, "spot", ec2RunSpot(t, ts, nil).Lifecycle)
	_, code, _ := ec2ErrorDetail(t, ts, ec2SpotRunParams(map[string]string{"InstanceType": "p4d.24xlarge"}))
	assert.Equal(t, "InsufficientInstanceCapacity", code)
}
