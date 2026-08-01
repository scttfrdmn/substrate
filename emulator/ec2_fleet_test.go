package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

// fleetInstanceItem mirrors a CreateFleet fleetInstanceSet item. instanceIds is a
// list per pool, which is the shape callers most often flatten incorrectly.
type fleetInstanceItem struct {
	InstanceIDs  []string `xml:"instanceIds>item"`
	InstanceType string   `xml:"instanceType"`
	Lifecycle    string   `xml:"lifecycle"`
	LT           struct {
		Spec struct {
			LaunchTemplateID   string `xml:"launchTemplateId"`
			LaunchTemplateName string `xml:"launchTemplateName"`
		} `xml:"launchTemplateSpecification"`
		Overrides struct {
			InstanceType string `xml:"instanceType"`
			SubnetID     string `xml:"subnetId"`
			MaxPrice     string `xml:"maxPrice"`
			Priority     string `xml:"priority"`
		} `xml:"overrides"`
	} `xml:"launchTemplateAndOverrides"`
}

// fleetErrorItem mirrors a CreateFleet errorSet item.
type fleetErrorItem struct {
	ErrorCode    string `xml:"errorCode"`
	ErrorMessage string `xml:"errorMessage"`
	Lifecycle    string `xml:"lifecycle"`
	LT           struct {
		Overrides struct {
			InstanceType string `xml:"instanceType"`
		} `xml:"overrides"`
	} `xml:"launchTemplateAndOverrides"`
}

// createFleetResp mirrors the CreateFleet response.
type createFleetResp struct {
	FleetID   string              `xml:"fleetId"`
	Instances []fleetInstanceItem `xml:"fleetInstanceSet>item"`
	Errors    []fleetErrorItem    `xml:"errorSet>item"`
}

// describeFleetsResp mirrors the DescribeFleets response.
type describeFleetsResp struct {
	Fleets []struct {
		FleetID           string `xml:"fleetId"`
		FleetState        string `xml:"fleetState"`
		ActivityStatus    string `xml:"activityStatus"`
		Type              string `xml:"type"`
		FulfilledCapacity int    `xml:"fulfilledCapacity"`
		TargetCapacity    struct {
			TotalTargetCapacity       int    `xml:"totalTargetCapacity"`
			DefaultTargetCapacityType string `xml:"defaultTargetCapacityType"`
		} `xml:"targetCapacitySpecification"`
		Tags []struct {
			Key   string `xml:"key"`
			Value string `xml:"value"`
		} `xml:"tagSet>item"`
		Errors []fleetErrorItem `xml:"errorSet>item"`
	} `xml:"fleetSet>item"`
}

// ec2FleetXML sends an EC2 request and unmarshals the XML body into v, failing
// the test on a non-200 status.
func ec2FleetXML(t *testing.T, ts *httptest.Server, params map[string]string, v any) {
	t.Helper()
	resp := ec2Request(t, ts, params)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, body)
	}
	if v != nil {
		if err := xml.Unmarshal(body, v); err != nil {
			t.Fatalf("unmarshal %s: %v", body, err)
		}
	}
}

// newFleetLaunchTemplate creates a launch template and returns its ID.
func newFleetLaunchTemplate(t *testing.T, ts *httptest.Server, name string) string {
	t.Helper()
	var lt struct {
		LaunchTemplateID string `xml:"launchTemplate>launchTemplateId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":                          "CreateLaunchTemplate",
		"LaunchTemplateName":              name,
		"LaunchTemplateData.ImageId":      "ami-0fleet0000000001",
		"LaunchTemplateData.InstanceType": "t3.micro",
	}, &lt)
	if lt.LaunchTemplateID == "" {
		t.Fatal("CreateLaunchTemplate returned no launchTemplateId")
	}
	return lt.LaunchTemplateID
}

func TestEC2_CreateFleet_InstantFullFulfillment(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-full")

	var got createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.Version":          "1",
		"LaunchTemplateConfigs.1.Overrides.1.InstanceType":                     "c5.large",
		"TargetCapacitySpecification.TotalTargetCapacity":                      "3",
		"TargetCapacitySpecification.DefaultTargetCapacityType":                "on-demand",
	}, &got)

	if !strings.HasPrefix(got.FleetID, "fleet-") {
		t.Errorf("fleetId = %q, want fleet- prefix", got.FleetID)
	}
	if len(got.Instances) != 1 {
		t.Fatalf("fleetInstanceSet items = %d, want 1", len(got.Instances))
	}
	inst := got.Instances[0]
	if len(inst.InstanceIDs) != 3 {
		t.Errorf("instanceIds = %v, want 3 entries", inst.InstanceIDs)
	}
	if inst.InstanceType != "c5.large" {
		t.Errorf("instanceType = %q, want c5.large", inst.InstanceType)
	}
	if inst.Lifecycle != "on-demand" {
		t.Errorf("lifecycle = %q, want on-demand", inst.Lifecycle)
	}
	if inst.LT.Spec.LaunchTemplateID != ltID {
		t.Errorf("launchTemplateId = %q, want %q", inst.LT.Spec.LaunchTemplateID, ltID)
	}
	if inst.LT.Overrides.InstanceType != "c5.large" {
		t.Errorf("overrides.instanceType = %q, want c5.large", inst.LT.Overrides.InstanceType)
	}
	if len(got.Errors) != 0 {
		t.Errorf("errorSet = %+v, want empty on full fulfillment", got.Errors)
	}

	// The launched instances must be visible to DescribeInstances, which is the
	// whole point of routing through runInstances.
	var desc struct {
		Instances []struct {
			InstanceID   string `xml:"instanceId"`
			InstanceType string `xml:"instanceType"`
			State        string `xml:"instanceState>name"`
		} `xml:"reservationSet>item>instancesSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeInstances"}, &desc)
	seen := map[string]bool{}
	for _, d := range desc.Instances {
		seen[d.InstanceID] = true
		if d.InstanceType != "c5.large" {
			t.Errorf("DescribeInstances instanceType = %q, want c5.large", d.InstanceType)
		}
		if d.State != "running" {
			t.Errorf("DescribeInstances state = %q, want running", d.State)
		}
	}
	for _, id := range inst.InstanceIDs {
		if !seen[id] {
			t.Errorf("fleet instance %s not visible to DescribeInstances", id)
		}
	}
}

func TestEC2_CreateFleet_SeededPartialFulfillment(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-partial")

	seedFleetShortfall(t, ts, `{
		"launchTemplate": "`+ltID+`",
		"fulfill": 8,
		"errorCode": "InsufficientInstanceCapacity",
		"errorMessage": "no capacity",
		"lifecycle": "spot"
	}`)

	var got createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"LaunchTemplateConfigs.1.Overrides.1.InstanceType":                     "c5.large",
		"LaunchTemplateConfigs.1.Overrides.2.InstanceType":                     "c5.xlarge",
		"TargetCapacitySpecification.TotalTargetCapacity":                      "12",
		"TargetCapacitySpecification.DefaultTargetCapacityType":                "on-demand",
	}, &got)

	// 8 of 12 fulfilled, 4 short: round-robin gives each pool 6 wanted / 4 got.
	total := 0
	for _, g := range got.Instances {
		total += len(g.InstanceIDs)
	}
	if total != 8 {
		t.Errorf("fulfilled instances = %d, want 8", total)
	}
	if len(got.Instances) != 2 {
		t.Fatalf("fleetInstanceSet items = %d, want 2 (one per pool)", len(got.Instances))
	}
	if len(got.Errors) != 2 {
		t.Fatalf("errorSet items = %d, want 2 (both pools short)", len(got.Errors))
	}
	for _, e := range got.Errors {
		if e.ErrorCode != "InsufficientInstanceCapacity" {
			t.Errorf("errorCode = %q, want InsufficientInstanceCapacity", e.ErrorCode)
		}
		if e.ErrorMessage != "no capacity" {
			t.Errorf("errorMessage = %q, want seeded message", e.ErrorMessage)
		}
		if e.Lifecycle != "spot" {
			t.Errorf("lifecycle = %q, want seeded spot", e.Lifecycle)
		}
	}

	// The fleet must report the request in TotalTargetCapacity and the result in
	// FulfilledCapacity — conflating the two is the caller bug this models.
	var desc describeFleetsResp
	ec2FleetXML(t, ts, map[string]string{
		"Action":    "DescribeFleets",
		"FleetId.1": got.FleetID,
	}, &desc)
	if len(desc.Fleets) != 1 {
		t.Fatalf("fleetSet items = %d, want 1", len(desc.Fleets))
	}
	f := desc.Fleets[0]
	if f.TargetCapacity.TotalTargetCapacity != 12 {
		t.Errorf("totalTargetCapacity = %d, want 12 (the request)", f.TargetCapacity.TotalTargetCapacity)
	}
	if f.FulfilledCapacity != 8 {
		t.Errorf("fulfilledCapacity = %d, want 8 (the result)", f.FulfilledCapacity)
	}
	if f.ActivityStatus != "fulfilled" {
		t.Errorf("activityStatus = %q, want fulfilled (instant fleets report fulfilled even when short)", f.ActivityStatus)
	}
	if len(f.Errors) != 2 {
		t.Errorf("DescribeFleets errorSet items = %d, want 2", len(f.Errors))
	}
}

func TestEC2_CreateFleet_SeededZeroFulfillment(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-zero")

	// Wildcard seed with an alternate error code.
	seedFleetShortfall(t, ts, `{"fulfill": 0, "errorCode": "InsufficientFreeAddressesInSubnet"}`)

	var got createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "2",
	}, &got)

	if len(got.Instances) != 0 {
		t.Errorf("fleetInstanceSet = %+v, want empty when nothing is fulfilled", got.Instances)
	}
	if len(got.Errors) != 1 {
		t.Fatalf("errorSet items = %d, want 1", len(got.Errors))
	}
	if got.Errors[0].ErrorCode != "InsufficientFreeAddressesInSubnet" {
		t.Errorf("errorCode = %q, want InsufficientFreeAddressesInSubnet", got.Errors[0].ErrorCode)
	}
	if got.Errors[0].ErrorMessage == "" {
		t.Error("errorMessage is empty; a default should be derived from the code")
	}
	// A zero-fulfillment fleet still returns a fleet ID, so a caller that only
	// checks for an error sees success.
	if got.FleetID == "" {
		t.Error("fleetId is empty; CreateFleet succeeds even when it launches nothing")
	}

	// Clearing the seed restores full fulfillment.
	clearFleetShortfall(t, ts, "")
	var after createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "2",
	}, &after)
	if len(after.Instances) != 1 || len(after.Instances[0].InstanceIDs) != 2 {
		t.Errorf("after clearing seed, instances = %+v, want one pool of 2", after.Instances)
	}
	if len(after.Errors) != 0 {
		t.Errorf("after clearing seed, errorSet = %+v, want empty", after.Errors)
	}
}

func TestEC2_CreateFleet_NonInstantDefersInstances(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-maintain")

	for _, fleetType := range []string{"request", "maintain"} {
		t.Run(fleetType, func(t *testing.T) {
			var got createFleetResp
			ec2FleetXML(t, ts, map[string]string{
				"Action": "CreateFleet",
				"Type":   fleetType,
				"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
				"TargetCapacitySpecification.TotalTargetCapacity":                      "2",
			}, &got)

			if got.FleetID == "" {
				t.Fatal("fleetId is empty")
			}
			// AWS reports instances asynchronously for request/maintain fleets, so
			// a caller reading Instances here legitimately sees nothing.
			if len(got.Instances) != 0 {
				t.Errorf("fleetInstanceSet = %+v, want empty for a %s fleet", got.Instances, fleetType)
			}
			if len(got.Errors) != 0 {
				t.Errorf("errorSet = %+v, want empty for a %s fleet", got.Errors, fleetType)
			}

			// The fleet is still discoverable, and its capacity was launched.
			var desc describeFleetsResp
			ec2FleetXML(t, ts, map[string]string{"Action": "DescribeFleets", "FleetId.1": got.FleetID}, &desc)
			if len(desc.Fleets) != 1 {
				t.Fatalf("fleetSet items = %d, want 1", len(desc.Fleets))
			}
			if desc.Fleets[0].Type != fleetType {
				t.Errorf("type = %q, want %q", desc.Fleets[0].Type, fleetType)
			}
			if desc.Fleets[0].FulfilledCapacity != 2 {
				t.Errorf("fulfilledCapacity = %d, want 2", desc.Fleets[0].FulfilledCapacity)
			}
		})
	}
}

func TestEC2_CreateFleet_PrioritizedPoolOrdering(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-prioritized")

	var got createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action":                             "CreateFleet",
		"Type":                               "instant",
		"OnDemandOptions.AllocationStrategy": "prioritized",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"LaunchTemplateConfigs.1.Overrides.1.InstanceType":                     "c5.large",
		"LaunchTemplateConfigs.1.Overrides.1.Priority":                         "2",
		"LaunchTemplateConfigs.1.Overrides.2.InstanceType":                     "m5.large",
		"LaunchTemplateConfigs.1.Overrides.2.Priority":                         "1",
		"TargetCapacitySpecification.TotalTargetCapacity":                      "3",
	}, &got)

	if len(got.Instances) != 2 {
		t.Fatalf("fleetInstanceSet items = %d, want 2", len(got.Instances))
	}
	// Priority 1 is highest, so m5.large sorts first and takes the remainder.
	if got.Instances[0].InstanceType != "m5.large" {
		t.Errorf("first pool instanceType = %q, want m5.large (priority 1)", got.Instances[0].InstanceType)
	}
	if len(got.Instances[0].InstanceIDs) != 2 {
		t.Errorf("highest-priority pool got %d instances, want 2", len(got.Instances[0].InstanceIDs))
	}
	if len(got.Instances[1].InstanceIDs) != 1 {
		t.Errorf("second pool got %d instances, want 1", len(got.Instances[1].InstanceIDs))
	}
}

func TestEC2_CreateFleet_SpotLifecycleFromMaxPrice(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-spot")

	var got createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"LaunchTemplateConfigs.1.Overrides.1.InstanceType":                     "c5.large",
		"LaunchTemplateConfigs.1.Overrides.1.MaxPrice":                         "0.05",
		"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
		"TargetCapacitySpecification.SpotTargetCapacity":                       "1",
		"TargetCapacitySpecification.DefaultTargetCapacityType":                "spot",
	}, &got)

	if len(got.Instances) != 1 {
		t.Fatalf("fleetInstanceSet items = %d, want 1", len(got.Instances))
	}
	if got.Instances[0].Lifecycle != "spot" {
		t.Errorf("lifecycle = %q, want spot", got.Instances[0].Lifecycle)
	}
	if got.Instances[0].LT.Overrides.MaxPrice != "0.05" {
		t.Errorf("overrides.maxPrice = %q, want 0.05", got.Instances[0].LT.Overrides.MaxPrice)
	}
}

func TestEC2_CreateFleet_TagPropagation(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-tags")

	var got createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
		"TagSpecification.1.ResourceType":                                      "fleet",
		"TagSpecification.1.Tag.1.Key":                                         "Owner",
		"TagSpecification.1.Tag.1.Value":                                       "research",
		"TagSpecification.2.ResourceType":                                      "instance",
		"TagSpecification.2.Tag.1.Key":                                         "Name",
		"TagSpecification.2.Tag.1.Value":                                       "worker",
	}, &got)

	// Instance tags must land on the launched instances.
	var desc struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
			Tags       []struct {
				Key   string `xml:"key"`
				Value string `xml:"value"`
			} `xml:"tagSet>item"`
		} `xml:"reservationSet>item>instancesSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeInstances"}, &desc)
	if len(desc.Instances) != 1 {
		t.Fatalf("DescribeInstances returned %d instances, want 1", len(desc.Instances))
	}
	found := false
	for _, tag := range desc.Instances[0].Tags {
		if tag.Key == "Name" && tag.Value == "worker" {
			found = true
		}
		if tag.Key == "Owner" {
			t.Error("fleet-scoped tag leaked onto the instance")
		}
	}
	if !found {
		t.Errorf("instance tags = %+v, want Name=worker propagated from the fleet", desc.Instances[0].Tags)
	}

	// Fleet tags must land on the fleet.
	var fleets describeFleetsResp
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeFleets", "FleetId.1": got.FleetID}, &fleets)
	if len(fleets.Fleets) != 1 {
		t.Fatalf("fleetSet items = %d, want 1", len(fleets.Fleets))
	}
	if len(fleets.Fleets[0].Tags) != 1 || fleets.Fleets[0].Tags[0].Key != "Owner" {
		t.Errorf("fleet tags = %+v, want only Owner=research", fleets.Fleets[0].Tags)
	}
}

func TestEC2_CreateFleet_Errors(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-errors")

	tests := []struct {
		name     string
		params   map[string]string
		wantCode string
	}{
		{
			name: "missing target capacity",
			params: map[string]string{
				"Action": "CreateFleet",
				"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
			},
			wantCode: "MissingParameter",
		},
		{
			name: "missing launch template configs",
			params: map[string]string{
				"Action": "CreateFleet",
				"TargetCapacitySpecification.TotalTargetCapacity": "1",
			},
			wantCode: "MissingParameter",
		},
		{
			name: "unknown launch template id",
			params: map[string]string{
				"Action": "CreateFleet",
				"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": "lt-doesnotexist",
				"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
			},
			wantCode: "InvalidLaunchTemplateId.NotFound",
		},
		{
			name: "unknown launch template name",
			params: map[string]string{
				"Action": "CreateFleet",
				"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateName": "nope",
				"TargetCapacitySpecification.TotalTargetCapacity":                        "1",
			},
			wantCode: "InvalidLaunchTemplateName.NotFoundException",
		},
		{
			name: "invalid fleet type",
			params: map[string]string{
				"Action": "CreateFleet",
				"Type":   "bogus",
				"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
				"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
			},
			wantCode: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := ec2Request(t, ts, tt.params)
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("status = %d, want 400; body = %s", resp.StatusCode, body)
			}
			if !strings.Contains(string(body), tt.wantCode) {
				t.Errorf("body = %s, want error code %s", body, tt.wantCode)
			}
		})
	}
}

func TestEC2_DescribeFleets_InstantRequiresExplicitID(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-visibility")

	var instant createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
	}, &instant)

	var maintain createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "maintain",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
	}, &maintain)

	// Unfiltered DescribeFleets omits instant fleets, matching AWS.
	var all describeFleetsResp
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeFleets"}, &all)
	ids := map[string]bool{}
	for _, f := range all.Fleets {
		ids[f.FleetID] = true
	}
	if ids[instant.FleetID] {
		t.Error("unfiltered DescribeFleets returned an instant fleet; AWS requires an explicit fleet ID")
	}
	if !ids[maintain.FleetID] {
		t.Error("unfiltered DescribeFleets omitted the maintain fleet")
	}

	// Naming the instant fleet returns it.
	var named describeFleetsResp
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeFleets", "FleetId.1": instant.FleetID}, &named)
	if len(named.Fleets) != 1 || named.Fleets[0].FleetID != instant.FleetID {
		t.Errorf("DescribeFleets with explicit id returned %+v, want the instant fleet", named.Fleets)
	}

	// Filters apply.
	var filtered describeFleetsResp
	ec2FleetXML(t, ts, map[string]string{
		"Action":           "DescribeFleets",
		"Filter.1.Name":    "type",
		"Filter.1.Value.1": "maintain",
	}, &filtered)
	if len(filtered.Fleets) != 1 || filtered.Fleets[0].Type != "maintain" {
		t.Errorf("type filter returned %+v, want only the maintain fleet", filtered.Fleets)
	}

	var noMatch describeFleetsResp
	ec2FleetXML(t, ts, map[string]string{
		"Action":           "DescribeFleets",
		"Filter.1.Name":    "fleet-state",
		"Filter.1.Value.1": "deleted",
	}, &noMatch)
	if len(noMatch.Fleets) != 0 {
		t.Errorf("fleet-state=deleted returned %+v, want none", noMatch.Fleets)
	}
}

func TestEC2_DeleteFleets(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-delete")

	var fleet createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "maintain",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "2",
	}, &fleet)

	var del struct {
		Successful []struct {
			FleetID            string `xml:"fleetId"`
			CurrentFleetState  string `xml:"currentFleetState"`
			PreviousFleetState string `xml:"previousFleetState"`
		} `xml:"successfulFleetDeletionSet>item"`
		Unsuccessful []struct {
			FleetID string `xml:"fleetId"`
			Code    string `xml:"error>code"`
		} `xml:"unsuccessfulFleetDeletionSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":             "DeleteFleets",
		"FleetId.1":          fleet.FleetID,
		"FleetId.2":          "fleet-missing",
		"TerminateInstances": "true",
	}, &del)

	if len(del.Successful) != 1 || del.Successful[0].FleetID != fleet.FleetID {
		t.Fatalf("successfulFleetDeletionSet = %+v, want the created fleet", del.Successful)
	}
	if del.Successful[0].PreviousFleetState != "active" {
		t.Errorf("previousFleetState = %q, want active", del.Successful[0].PreviousFleetState)
	}
	if del.Successful[0].CurrentFleetState != "deleted_terminating" {
		t.Errorf("currentFleetState = %q, want deleted_terminating", del.Successful[0].CurrentFleetState)
	}
	if len(del.Unsuccessful) != 1 || del.Unsuccessful[0].Code != "fleetIdDoesNotExist" {
		t.Errorf("unsuccessfulFleetDeletionSet = %+v, want fleetIdDoesNotExist", del.Unsuccessful)
	}

	// TerminateInstances=true must actually terminate the fleet's instances.
	var desc struct {
		Instances []struct {
			State string `xml:"instanceState>name"`
		} `xml:"reservationSet>item>instancesSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeInstances"}, &desc)
	for i, d := range desc.Instances {
		if d.State != "terminated" && d.State != "shutting-down" {
			t.Errorf("instance %d state = %q, want terminated after DeleteFleets", i, d.State)
		}
	}

	// The fleet's recorded state reflects the deletion.
	var after describeFleetsResp
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeFleets", "FleetId.1": fleet.FleetID}, &after)
	if len(after.Fleets) != 1 || after.Fleets[0].FleetState != "deleted_terminating" {
		t.Errorf("DescribeFleets after delete = %+v, want fleetState deleted_terminating", after.Fleets)
	}
}

func TestEC2_DeleteFleets_WithoutTerminate(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-keep-instances")

	var fleet createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "maintain",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
	}, &fleet)

	var del struct {
		Successful []struct {
			CurrentFleetState string `xml:"currentFleetState"`
		} `xml:"successfulFleetDeletionSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":             "DeleteFleets",
		"FleetId.1":          fleet.FleetID,
		"TerminateInstances": "false",
	}, &del)

	if len(del.Successful) != 1 || del.Successful[0].CurrentFleetState != "deleted_running" {
		t.Fatalf("successfulFleetDeletionSet = %+v, want deleted_running", del.Successful)
	}

	// Instances outlive the fleet when TerminateInstances is false.
	var desc struct {
		Instances []struct {
			State string `xml:"instanceState>name"`
		} `xml:"reservationSet>item>instancesSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeInstances"}, &desc)
	if len(desc.Instances) != 1 || desc.Instances[0].State != "running" {
		t.Errorf("instances = %+v, want one still running", desc.Instances)
	}
}

func TestEC2_DeleteFleets_MissingFleetID(t *testing.T) {
	ts := newEC2TestServer(t)
	resp := ec2Request(t, ts, map[string]string{"Action": "DeleteFleets"})
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	if !strings.Contains(string(body), "MissingParameter") {
		t.Errorf("body = %s, want MissingParameter", body)
	}
}

// seedFleetShortfall POSTs a fleet shortfall seed to the control plane.
func seedFleetShortfall(t *testing.T, ts *httptest.Server, body string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/ec2/fleet-shortfall", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("seed fleet shortfall: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("seed fleet shortfall status = %d: %s", resp.StatusCode, got)
	}
}

// clearFleetShortfall DELETEs a fleet shortfall seed; an empty launchTemplate
// clears all seeds.
func clearFleetShortfall(t *testing.T, ts *httptest.Server, launchTemplate string) {
	t.Helper()
	u := ts.URL + "/v1/ec2/fleet-shortfall"
	if launchTemplate != "" {
		u += "?launchTemplate=" + launchTemplate
	}
	req, err := http.NewRequest(http.MethodDelete, u, nil)
	if err != nil {
		t.Fatalf("build clear request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("clear fleet shortfall: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("clear fleet shortfall status = %d: %s", resp.StatusCode, got)
	}
}

func TestEC2_SeedFleetShortfall_Validation(t *testing.T) {
	ts := newEC2TestServer(t)

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "valid", body: `{"fulfill":1}`, wantStatus: http.StatusOK},
		{name: "negative fulfill", body: `{"fulfill":-1}`, wantStatus: http.StatusBadRequest},
		{name: "malformed json", body: `{`, wantStatus: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := http.Post(ts.URL+"/v1/ec2/fleet-shortfall", "application/json", strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("post: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}

	// Template-scoped clear leaves the wildcard seed alone.
	seedFleetShortfall(t, ts, `{"launchTemplate":"lt-scoped","fulfill":0}`)
	clearFleetShortfall(t, ts, "lt-scoped")
	clearFleetShortfall(t, ts, "")
}

// fleetIDTagKey is the reserved tag CreateFleet stamps on its instances (#443).
const fleetIDTagKey = "aws:ec2:fleet-id"

// describedInstances mirrors the DescribeInstances fields the fleet-id tag tests
// assert on.
type describedInstances struct {
	Instances []struct {
		InstanceID string `xml:"instanceId"`
		Tags       []struct {
			Key   string `xml:"key"`
			Value string `xml:"value"`
		} `xml:"tagSet>item"`
	} `xml:"reservationSet>item>instancesSet>item"`
}

// instanceTag returns the value of key on the i-th described instance, and
// whether it was present at all — an absent tag and an empty value are different
// observations.
func (d describedInstances) instanceTag(i int, key string) (string, bool) {
	for _, tag := range d.Instances[i].Tags {
		if tag.Key == key {
			return tag.Value, true
		}
	}
	return "", false
}

// instanceIDs flattens every instance ID across a CreateFleet response's pools.
func (r createFleetResp) instanceIDs() []string {
	var ids []string
	for _, pool := range r.Instances {
		ids = append(ids, pool.InstanceIDs...)
	}
	return ids
}

func TestEC2_CreateFleet_AppliesFleetIDTag(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-id-tag")

	var got createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "2",
	}, &got)
	if got.FleetID == "" {
		t.Fatal("CreateFleet returned no fleetId")
	}
	if len(got.instanceIDs()) != 2 {
		t.Fatalf("launched %d instances, want 2", len(got.instanceIDs()))
	}

	var desc describedInstances
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeInstances"}, &desc)
	if len(desc.Instances) != 2 {
		t.Fatalf("DescribeInstances returned %d instances, want 2", len(desc.Instances))
	}
	for i := range desc.Instances {
		value, ok := desc.instanceTag(i, fleetIDTagKey)
		if !ok {
			t.Errorf("instance %s has no %s tag; tags = %+v",
				desc.Instances[i].InstanceID, fleetIDTagKey, desc.Instances[i].Tags)
			continue
		}
		if value != got.FleetID {
			t.Errorf("instance %s %s = %q, want %q",
				desc.Instances[i].InstanceID, fleetIDTagKey, value, got.FleetID)
		}
	}
}

// TestEC2_CreateFleet_FleetIDTagIsFilterable is the assertion that actually
// matters: the tag is only useful if DescribeInstances can filter on it. The key
// contains colons, so this also pins that the tag:<key> filter cuts only its own
// prefix rather than splitting on every colon.
func TestEC2_CreateFleet_FleetIDTagIsFilterable(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-id-filter")

	newFleet := func(capacity string) createFleetResp {
		var got createFleetResp
		ec2FleetXML(t, ts, map[string]string{
			"Action": "CreateFleet",
			"Type":   "instant",
			"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
			"TargetCapacitySpecification.TotalTargetCapacity":                      capacity,
		}, &got)
		return got
	}

	first := newFleet("2")
	second := newFleet("1")
	if first.FleetID == second.FleetID {
		t.Fatalf("both fleets share id %q", first.FleetID)
	}

	// Each fleet's filter must return exactly its own instances, never the other's.
	for _, fleet := range []createFleetResp{first, second} {
		var desc describedInstances
		ec2FleetXML(t, ts, map[string]string{
			"Action":           "DescribeInstances",
			"Filter.1.Name":    "tag:" + fleetIDTagKey,
			"Filter.1.Value.1": fleet.FleetID,
		}, &desc)

		want := fleet.instanceIDs()
		if len(desc.Instances) != len(want) {
			t.Fatalf("filter on %s returned %d instances, want %d",
				fleet.FleetID, len(desc.Instances), len(want))
		}
		for i := range desc.Instances {
			if !slices.Contains(want, desc.Instances[i].InstanceID) {
				t.Errorf("filter on %s returned %s, which belongs to another fleet",
					fleet.FleetID, desc.Instances[i].InstanceID)
			}
		}
	}
}

// TestEC2_CreateFleet_FleetIDTagCoexistsWithCallerTags guards the index
// arithmetic in fleetIDTagSpec: the reserved tag is appended past the
// passthrough tags, so reusing an occupied TagSpecification index would silently
// overwrite a caller's own launch-time tag.
func TestEC2_CreateFleet_FleetIDTagCoexistsWithCallerTags(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-id-coexist")

	var got createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
		"TagSpecification.1.ResourceType":                                      "instance",
		"TagSpecification.1.Tag.1.Key":                                         "Name",
		"TagSpecification.1.Tag.1.Value":                                       "worker",
		"TagSpecification.1.Tag.2.Key":                                         "Project",
		"TagSpecification.1.Tag.2.Value":                                       "parsl",
		"TagSpecification.2.ResourceType":                                      "fleet",
		"TagSpecification.2.Tag.1.Key":                                         "Owner",
		"TagSpecification.2.Tag.1.Value":                                       "research",
	}, &got)

	var desc describedInstances
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeInstances"}, &desc)
	if len(desc.Instances) != 1 {
		t.Fatalf("DescribeInstances returned %d instances, want 1", len(desc.Instances))
	}

	for key, want := range map[string]string{
		"Name":        "worker",
		"Project":     "parsl",
		fleetIDTagKey: got.FleetID,
	} {
		value, ok := desc.instanceTag(0, key)
		if !ok {
			t.Errorf("instance is missing tag %q; tags = %+v", key, desc.Instances[0].Tags)
			continue
		}
		if value != want {
			t.Errorf("instance tag %q = %q, want %q", key, value, want)
		}
	}
	if _, ok := desc.instanceTag(0, "Owner"); ok {
		t.Error("fleet-scoped tag leaked onto the instance")
	}
}

// TestEC2_CreateFleet_FleetIDTagAcrossPools pins that the tag is stamped per
// pool, not once per fleet: launchFleetPool runs for each pool, so a fleet with
// several overrides has several chances to drop it.
func TestEC2_CreateFleet_FleetIDTagAcrossPools(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-id-pools")

	var got createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"LaunchTemplateConfigs.1.Overrides.1.InstanceType":                     "t3.small",
		"LaunchTemplateConfigs.1.Overrides.2.InstanceType":                     "c5.large",
		"TargetCapacitySpecification.TotalTargetCapacity":                      "2",
	}, &got)
	if len(got.Instances) != 2 {
		t.Fatalf("fleetInstanceSet items = %d, want 2 pools", len(got.Instances))
	}

	var desc describedInstances
	ec2FleetXML(t, ts, map[string]string{
		"Action":           "DescribeInstances",
		"Filter.1.Name":    "tag:" + fleetIDTagKey,
		"Filter.1.Value.1": got.FleetID,
	}, &desc)
	if len(desc.Instances) != 2 {
		t.Errorf("filter returned %d instances, want 2 (one per pool)", len(desc.Instances))
	}
}

// TestEC2_CreateFleet_FleetIDTagZeroCapacity covers the pool that launches
// nothing: a fully-unfulfilled fleet must still succeed rather than erroring in
// the tag path.
func TestEC2_CreateFleet_FleetIDTagZeroCapacity(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-id-zero")
	seedFleetShortfall(t, ts, `{"fulfill":0}`)

	var got createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "2",
	}, &got)
	if len(got.instanceIDs()) != 0 {
		t.Errorf("launched %v, want no instances", got.instanceIDs())
	}

	var desc describedInstances
	ec2FleetXML(t, ts, map[string]string{
		"Action":           "DescribeInstances",
		"Filter.1.Name":    "tag:" + fleetIDTagKey,
		"Filter.1.Value.1": got.FleetID,
	}, &desc)
	if len(desc.Instances) != 0 {
		t.Errorf("filter returned %d instances, want 0", len(desc.Instances))
	}
}
