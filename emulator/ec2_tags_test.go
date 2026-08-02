package emulator_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ec2ReservedTagMessage is the wording real EC2 uses for a reserved tag key,
// pinned here so it cannot drift silently. See ec2ReservedTagPrefix's doc comment
// for its provenance: the CreateTags reference has an empty Errors section, so the
// string comes from observed real-AWS responses rather than the API model.
const ec2ReservedTagMessage = "Tag keys starting with 'aws:' are reserved for internal use"

// ec2TagTestInstance launches one instance and returns its ID.
func ec2TagTestInstance(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	ids := ec2RunInstanceIDs(t, ts, map[string]string{
		"Action":   "RunInstances",
		"ImageId":  "ami-0tagtest00000001",
		"MinCount": "1",
		"MaxCount": "1",
	})
	require.Len(t, ids, 1)
	return ids[0]
}

// ec2InstanceTagValue returns the value of key on instID as DescribeInstances
// reports it, and whether the tag was present at all — an absent tag and an empty
// value are different observations, and the difference is what these tests turn on.
func ec2InstanceTagValue(t *testing.T, ts *httptest.Server, instID, key string) (string, bool) {
	t.Helper()
	var desc describedInstances
	ec2FleetXML(t, ts, map[string]string{
		"Action":       "DescribeInstances",
		"InstanceId.1": instID,
	}, &desc)
	require.Len(t, desc.Instances, 1, "DescribeInstances should return %s", instID)
	return desc.instanceTag(0, key)
}

// TestEC2_CreateTags_RejectsReservedPrefix covers #452: substrate accepted tag keys
// using the "aws:" prefix that real EC2 reserves for itself, so a consumer's
// error branch for that rejection was unreachable and a test asserting it passed
// against substrate while the same code failed against AWS.
//
// The accepted cases are as much the point as the rejected ones. EC2 documents tag
// keys as case-sensitive, so "AWS:foo" is an ordinary user tag — a case-folded
// check would trade this infidelity for a new one in the other direction.
func TestEC2_CreateTags_RejectsReservedPrefix(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		wantReject bool
	}{
		{name: "lowercase reserved prefix", key: "aws:cloudformation:stack-name", wantReject: true},
		{name: "reserved prefix with empty suffix", key: "aws:", wantReject: true},
		{name: "the tag substrate stamps on fleet instances", key: "aws:ec2:fleet-id", wantReject: true},
		// Case-sensitivity: EC2 tag keys are case-sensitive, so none of these is
		// the reserved prefix and all three are legal user tags.
		{name: "uppercase is not the reserved prefix", key: "AWS:foo"},
		{name: "mixed case is not the reserved prefix", key: "Aws:foo"},
		// Near-misses on the delimiter.
		{name: "hyphen rather than colon", key: "aws-foo"},
		{name: "no delimiter at all", key: "awsfoo"},
		{name: "aws not at the start", key: "my-aws:foo"},
		{name: "an ordinary key", key: "Name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			instID := ec2TagTestInstance(t, ts)

			status, code, message := ec2ErrorDetail(t, ts, map[string]string{
				"Action":       "CreateTags",
				"ResourceId.1": instID,
				"Tag.1.Key":    tt.key,
				"Tag.1.Value":  "v",
			})

			if !tt.wantReject {
				require.Equal(t, http.StatusOK, status, "key %q should be accepted", tt.key)
				value, ok := ec2InstanceTagValue(t, ts, instID, tt.key)
				assert.True(t, ok, "tag %q should have been applied", tt.key)
				assert.Equal(t, "v", value)
				return
			}

			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Equal(t, ec2ReservedTagMessage, message)

			_, ok := ec2InstanceTagValue(t, ts, instID, tt.key)
			assert.False(t, ok, "rejected tag %q must not have been applied", tt.key)
		})
	}
}

// TestEC2_CreateTags_RejectionIsAllOrNothing pins the reason the check runs before
// the first resource is touched rather than inside the apply loop. CreateTags
// accepts up to 1000 resource IDs; rejecting partway through would leave the
// earlier resources tagged and the rest not, a state real EC2 never produces.
//
// Both dimensions are asserted at once: the legal tag in the same request is not
// applied either, and neither of the two named resources is modified.
func TestEC2_CreateTags_RejectionIsAllOrNothing(t *testing.T) {
	ts := newEC2TestServer(t)
	first := ec2TagTestInstance(t, ts)
	second := ec2TagTestInstance(t, ts)

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": first,
		"ResourceId.2": second,
		// A legal tag ahead of the reserved one, so a mid-loop implementation
		// would have already written it by the time it noticed.
		"Tag.1.Key":   "Name",
		"Tag.1.Value": "legal",
		"Tag.2.Key":   "aws:reserved",
		"Tag.2.Value": "v",
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterValue", code)

	for _, id := range []string{first, second} {
		if _, ok := ec2InstanceTagValue(t, ts, id, "Name"); ok {
			t.Errorf("instance %s has the legal tag from a rejected request", id)
		}
		if _, ok := ec2InstanceTagValue(t, ts, id, "aws:reserved"); ok {
			t.Errorf("instance %s has the reserved tag from a rejected request", id)
		}
	}
}

// TestEC2_DeleteTags_RejectsReservedPrefix covers the DeleteTags half of #452.
//
// The evidence here is weaker than for CreateTags and deliberately so: substrate
// found no captured real-AWS DeleteTags rejection, so the code and message are
// inherited from the CreateTags capture. What the documentation does state
// unambiguously is the outcome — a reserved tag "can't be edited or deleted" — and
// the test asserts that outcome: the tag survives the call.
func TestEC2_DeleteTags_RejectsReservedPrefix(t *testing.T) {
	ts := newEC2TestServer(t)

	// The tag has to exist to prove deletion was refused rather than a no-op, and
	// CreateTags now refuses to make one — so use the path that legitimately
	// stamps a reserved tag: a fleet launch.
	ltID := newFleetLaunchTemplate(t, ts, "delete-reserved-tag")
	var fleet createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
	}, &fleet)
	ids := fleet.instanceIDs()
	require.Len(t, ids, 1)
	instID := ids[0]

	before, ok := ec2InstanceTagValue(t, ts, instID, fleetIDTagKey)
	require.True(t, ok, "fleet instance should carry %s", fleetIDTagKey)

	status, code, message := ec2ErrorDetail(t, ts, map[string]string{
		"Action":       "DeleteTags",
		"ResourceId.1": instID,
		"Tag.1.Key":    fleetIDTagKey,
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
	assert.Equal(t, ec2ReservedTagMessage, message)

	after, ok := ec2InstanceTagValue(t, ts, instID, fleetIDTagKey)
	assert.True(t, ok, "reserved tag must survive a refused DeleteTags")
	assert.Equal(t, before, after)
}

// TestEC2_DeleteTags_OrdinaryKeyStillWorks is the regression guard on the check
// above: an ordinary tag must still be deletable.
func TestEC2_DeleteTags_OrdinaryKeyStillWorks(t *testing.T) {
	ts := newEC2TestServer(t)
	instID := ec2TagTestInstance(t, ts)

	status, _, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": instID,
		"Tag.1.Key":    "Name",
		"Tag.1.Value":  "keep-me",
	})
	require.Equal(t, http.StatusOK, status)
	_, ok := ec2InstanceTagValue(t, ts, instID, "Name")
	require.True(t, ok)

	status, _, _ = ec2ErrorDetail(t, ts, map[string]string{
		"Action":       "DeleteTags",
		"ResourceId.1": instID,
		"Tag.1.Key":    "Name",
	})
	require.Equal(t, http.StatusOK, status)

	if _, ok := ec2InstanceTagValue(t, ts, instID, "Name"); ok {
		t.Error("Name tag should have been deleted")
	}
}

// TestEC2_ReservedTagCheck_DoesNotBreakFleetTagging is the gate on the reserved-key
// check having landed in the right code path.
//
// Substrate stamps aws:ec2:fleet-id (#443) on every fleet instance. #468 extended the
// reserved-prefix check to every tag-on-create path, including the one fleets launch
// through, so the stamp now travels as an already-parsed tag appended after the check
// rather than as a TagSpecification param subject to it. If that split is ever
// collapsed the check rejects substrate's own tagging and silently undoes #443 — for
// an "instant" fleet this tag is the only route from a fleet back to its instances, so
// the failure surfaces as a fully-running fleet reporting as empty.
func TestEC2_ReservedTagCheck_DoesNotBreakFleetTagging(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "reserved-tag-fleet")

	var fleet createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "2",
	}, &fleet)
	require.NotEmpty(t, fleet.FleetID)
	require.Len(t, fleet.instanceIDs(), 2, "fleet should still launch its instances")

	// Every instance still carries the tag...
	for _, id := range fleet.instanceIDs() {
		value, ok := ec2InstanceTagValue(t, ts, id, fleetIDTagKey)
		assert.True(t, ok, "instance %s lost its %s tag", id, fleetIDTagKey)
		assert.Equal(t, fleet.FleetID, value)
	}

	// ...and the lookup the tag exists for still works.
	var desc describedInstances
	ec2FleetXML(t, ts, map[string]string{
		"Action":           "DescribeInstances",
		"Filter.1.Name":    "tag:" + fleetIDTagKey,
		"Filter.1.Value.1": fleet.FleetID,
	}, &desc)
	assert.Len(t, desc.Instances, 2,
		"filtering on %s must still find the fleet's instances", fleetIDTagKey)
}

// ec2TagTestSubnet creates a VPC and a subnet in it, returning the subnet ID — the
// setup CreateNatGateway needs before it can be asked to tag anything.
func ec2TagTestSubnet(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	var vpc struct {
		VPCID string `xml:"vpc>vpcId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":    "CreateVpc",
		"CidrBlock": "10.9.0.0/16",
	}, &vpc)
	require.NotEmpty(t, vpc.VPCID)

	var subnet struct {
		SubnetID string `xml:"subnet>subnetId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":    "CreateSubnet",
		"VpcId":     vpc.VPCID,
		"CidrBlock": "10.9.1.0/24",
	}, &subnet)
	require.NotEmpty(t, subnet.SubnetID)
	return subnet.SubnetID
}

// ec2InstanceCount returns how many instances DescribeInstances reports in total,
// which is how the tag-on-create tests assert that a rejected launch created nothing.
func ec2InstanceCount(t *testing.T, ts *httptest.Server) int {
	t.Helper()
	var desc describedInstances
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeInstances"}, &desc)
	return len(desc.Instances)
}

// TestEC2_RunInstances_RejectsReservedTagKeys covers #468: #452 closed the reserved
// "aws:" prefix on CreateTags and DeleteTags but not on tag-on-create, so the same key
// real EC2 refuses was accepted whenever it arrived through a TagSpecification.
//
// Both captures behind ec2ReservedTagMessage are of RunInstances tag-on-create, so
// this path has stronger provenance for the message than the one #452 shipped it on.
func TestEC2_RunInstances_RejectsReservedTagKeys(t *testing.T) {
	tests := []struct {
		name     string
		tagKey   string
		accepted bool
	}{
		{name: "reserved prefix", tagKey: "aws:foo"},
		{name: "the fleet-id key itself", tagKey: fleetIDTagKey},
		{name: "prefix alone", tagKey: "aws:"},
		// Tag keys are case-sensitive per the EC2 tagging documentation, so these are
		// ordinary user tags. A case-folded check would trade #452's infidelity for a
		// new one in the other direction — and the SQS attribute prefix in #472 is
		// case-INsensitive, so the two rules must not be unified.
		{name: "uppercase is an ordinary tag", tagKey: "AWS:foo", accepted: true},
		{name: "mixed case is an ordinary tag", tagKey: "Aws:foo", accepted: true},
		{name: "no colon is an ordinary tag", tagKey: "awsfoo", accepted: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			params := map[string]string{
				"Action":                          "RunInstances",
				"ImageId":                         "ami-0reserved00000001",
				"MinCount":                        "1",
				"MaxCount":                        "1",
				"TagSpecification.1.ResourceType": "instance",
				"TagSpecification.1.Tag.1.Key":    tc.tagKey,
				"TagSpecification.1.Tag.1.Value":  "v",
			}

			if tc.accepted {
				ids := ec2RunInstanceIDs(t, ts, params)
				require.Len(t, ids, 1)
				value, ok := ec2InstanceTagValue(t, ts, ids[0], tc.tagKey)
				assert.True(t, ok, "%s should have been applied", tc.tagKey)
				assert.Equal(t, "v", value)
				return
			}

			status, code, message := ec2ErrorDetail(t, ts, params)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Equal(t, ec2ReservedTagMessage, message)
			assert.Zero(t, ec2InstanceCount(t, ts),
				"a rejected launch must not create an instance")
		})
	}
}

// TestEC2_RunInstances_ReservedTagRejectionCreatesNothing pins the rollback rule: "If
// tags cannot be applied during resource creation, we roll back the resource creation
// process. This ensures that resources are either created with tags or not created at
// all."
//
// This is the assertion that fails if the check ever moves after the per-instance
// state.Put, which would leave instances behind that real EC2 never creates. MaxCount
// is 3 so a partial application is visible as a count rather than only as a tag.
func TestEC2_RunInstances_ReservedTagRejectionCreatesNothing(t *testing.T) {
	ts := newEC2TestServer(t)

	// One legal tag alongside one reserved key: the request is refused whole, so the
	// legal tag is not applied to anything either.
	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action":                          "RunInstances",
		"ImageId":                         "ami-0rollback00000001",
		"MinCount":                        "3",
		"MaxCount":                        "3",
		"TagSpecification.1.ResourceType": "instance",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
		"TagSpecification.1.Tag.2.Key":    "aws:internal",
		"TagSpecification.1.Tag.2.Value":  "x",
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterValue", code)
	assert.Zero(t, ec2InstanceCount(t, ts), "no instance may survive the rejection")

	// A second specification carrying the reserved key still fails the request, even
	// though the first specification is legal — the check sees every instance-scoped
	// tag, not just the first block's.
	status, code, _ = ec2ErrorDetail(t, ts, map[string]string{
		"Action":                          "RunInstances",
		"ImageId":                         "ami-0rollback00000002",
		"MinCount":                        "1",
		"MaxCount":                        "1",
		"TagSpecification.1.ResourceType": "instance",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
		"TagSpecification.2.ResourceType": "instance",
		"TagSpecification.2.Tag.1.Key":    "aws:internal",
		"TagSpecification.2.Tag.1.Value":  "x",
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterValue", code)
	assert.Zero(t, ec2InstanceCount(t, ts))

	// A volume-scoped reserved key does not fail an instance launch: substrate models
	// instance tags on this path, and a specification for a resource it does not tag is
	// skipped rather than checked. Stated as a deliberate limit, not an oversight.
	ids := ec2RunInstanceIDs(t, ts, map[string]string{
		"Action":                          "RunInstances",
		"ImageId":                         "ami-0rollback00000003",
		"MinCount":                        "1",
		"MaxCount":                        "1",
		"TagSpecification.1.ResourceType": "volume",
		"TagSpecification.1.Tag.1.Key":    "aws:volume",
		"TagSpecification.1.Tag.1.Value":  "x",
	})
	assert.Len(t, ids, 1)
}

// TestEC2_CreateImage_RejectsReservedTagKeys covers CreateImage's two tag scopes.
//
// Per the reference, ResourceType=image tags the AMI while ResourceType=snapshot tags
// "the snapshots that are created of the root volume and of other Amazon EBS volumes
// that are attached to the instance", so a reserved key is refused on either.
func TestEC2_CreateImage_RejectsReservedTagKeys(t *testing.T) {
	for _, resourceType := range []string{"image", "snapshot"} {
		t.Run(resourceType, func(t *testing.T) {
			ts := newEC2TestServer(t)
			instID := ec2TagTestInstance(t, ts)

			status, code, message := ec2ErrorDetail(t, ts, map[string]string{
				"Action":                          "CreateImage",
				"InstanceId":                      instID,
				"Name":                            "reserved-tag-ami",
				"TagSpecification.1.ResourceType": resourceType,
				"TagSpecification.1.Tag.1.Key":    "aws:foo",
				"TagSpecification.1.Tag.1.Value":  "v",
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Equal(t, ec2ReservedTagMessage, message)

			// Neither the AMI nor its backing snapshot may exist.
			var images struct {
				Images []struct {
					ImageID string `xml:"imageId"`
				} `xml:"imagesSet>item"`
			}
			ec2FleetXML(t, ts, map[string]string{"Action": "DescribeImages"}, &images)
			assert.Empty(t, images.Images, "a rejected CreateImage must not create an AMI")

			var snaps struct {
				Snapshots []struct {
					SnapshotID string `xml:"snapshotId"`
				} `xml:"snapshotSet>item"`
			}
			ec2FleetXML(t, ts, map[string]string{"Action": "DescribeSnapshots"}, &snaps)
			assert.Empty(t, snaps.Snapshots,
				"a rejected CreateImage must not leave its backing snapshot behind")
		})
	}
}

// TestEC2_CreateImage_HonorsTagResourceType pins the fidelity gain that came with
// routing CreateImage through the shared parser: snapshot-scoped tags land on the
// snapshot and image-scoped tags on the AMI. This path previously read
// TagSpecification.1's tags whatever they were scoped to, so a request tagging only
// its snapshots put those tags on the AMI instead.
func TestEC2_CreateImage_HonorsTagResourceType(t *testing.T) {
	ts := newEC2TestServer(t)
	instID := ec2TagTestInstance(t, ts)

	resp := ec2Request(t, ts, map[string]string{
		"Action":                          "CreateImage",
		"InstanceId":                      instID,
		"Name":                            "scoped-tag-ami",
		"TagSpecification.1.ResourceType": "image",
		"TagSpecification.1.Tag.1.Key":    "Scope",
		"TagSpecification.1.Tag.1.Value":  "ami",
		"TagSpecification.2.ResourceType": "snapshot",
		"TagSpecification.2.Tag.1.Key":    "Scope",
		"TagSpecification.2.Tag.1.Value":  "snap",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close() //nolint:errcheck

	var images struct {
		Images []struct {
			ImageID string `xml:"imageId"`
			Tags    []struct {
				Key   string `xml:"key"`
				Value string `xml:"value"`
			} `xml:"tagSet>item"`
		} `xml:"imagesSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeImages"}, &images)
	require.Len(t, images.Images, 1)
	require.Len(t, images.Images[0].Tags, 1, "only the image-scoped tag belongs on the AMI")
	assert.Equal(t, "Scope", images.Images[0].Tags[0].Key)
	assert.Equal(t, "ami", images.Images[0].Tags[0].Value)

	var snaps struct {
		Snapshots []struct {
			SnapshotID string `xml:"snapshotId"`
			Tags       []struct {
				Key   string `xml:"key"`
				Value string `xml:"value"`
			} `xml:"tagSet>item"`
		} `xml:"snapshotSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeSnapshots"}, &snaps)
	require.Len(t, snaps.Snapshots, 1)
	require.Len(t, snaps.Snapshots[0].Tags, 1,
		"only the snapshot-scoped tag belongs on the snapshot")
	assert.Equal(t, "snap", snaps.Snapshots[0].Tags[0].Value)
}

// TestEC2_CreateNatGateway_RejectsReservedTagKeys covers the third tag-on-create path.
func TestEC2_CreateNatGateway_RejectsReservedTagKeys(t *testing.T) {
	ts := newEC2TestServer(t)
	subnetID := ec2TagTestSubnet(t, ts)

	status, code, message := ec2ErrorDetail(t, ts, map[string]string{
		"Action":                          "CreateNatGateway",
		"SubnetId":                        subnetID,
		"ConnectivityType":                "private",
		"TagSpecification.1.ResourceType": "natgateway",
		"TagSpecification.1.Tag.1.Key":    "aws:foo",
		"TagSpecification.1.Tag.1.Value":  "v",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", code)
	assert.Equal(t, ec2ReservedTagMessage, message)

	var gws struct {
		Gateways []struct {
			NatGatewayID string `xml:"natGatewayId"`
		} `xml:"natGatewaySet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeNatGateways"}, &gws)
	assert.Empty(t, gws.Gateways, "a rejected CreateNatGateway must not create a gateway")

	// An ordinary key on the same path still works, so the check is a filter rather
	// than a blanket refusal.
	var created struct {
		NatGatewayID string `xml:"natGateway>natGatewayId"`
	}
	ec2FleetXML(t, ts, map[string]string{
		"Action":                          "CreateNatGateway",
		"SubnetId":                        subnetID,
		"ConnectivityType":                "private",
		"TagSpecification.1.ResourceType": "natgateway",
		"TagSpecification.1.Tag.1.Key":    "Env",
		"TagSpecification.1.Tag.1.Value":  "prod",
	}, &created)
	assert.NotEmpty(t, created.NatGatewayID)
}

// TestEC2_CreateFleet_RejectsCallersReservedTagKeys is the other half of the fleet
// gate: substrate's own aws:ec2:fleet-id stamp is exempt because it never rides the
// request path, but a *caller's* reserved key in the same fleet request is not.
//
// Without this the structural split would have opened a hole — a consumer could reach
// the unchecked path by asking a fleet to apply the tag for them.
func TestEC2_CreateFleet_RejectsCallersReservedTagKeys(t *testing.T) {
	tests := []struct {
		name         string
		resourceType string
	}{
		{name: "instance-scoped", resourceType: "instance"},
		{name: "fleet-scoped", resourceType: "fleet"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			ltID := newFleetLaunchTemplate(t, ts, "fleet-reserved-"+tc.resourceType)

			status, code, message := ec2ErrorDetail(t, ts, map[string]string{
				"Action": "CreateFleet",
				"Type":   "instant",
				"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
				"TargetCapacitySpecification.TotalTargetCapacity":                      "2",
				"TagSpecification.1.ResourceType":                                      tc.resourceType,
				"TagSpecification.1.Tag.1.Key":                                         "aws:mine",
				"TagSpecification.1.Tag.1.Value":                                       "v",
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Equal(t, ec2ReservedTagMessage, message)
			assert.Zero(t, ec2InstanceCount(t, ts),
				"a rejected fleet must not launch instances")
		})
	}
}

// TestEC2_CreateFleet_CallerTagsAndFleetIDTagCoexist pins that the split kept both
// halves working at once: the caller's legal launch tags reach the instances *and* the
// fleet-ID stamp is still applied alongside them.
//
// Under the old mechanism the stamp was written into a free TagSpecification.N index,
// so an off-by-one in that index hunt would silently overwrite a caller's tag. There
// is no index to get wrong now, and this test is what would catch it returning.
func TestEC2_CreateFleet_CallerTagsAndFleetIDTagCoexist(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-tags-coexist")

	var fleet createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "2",
		"TagSpecification.1.ResourceType":                                      "instance",
		"TagSpecification.1.Tag.1.Key":                                         "Env",
		"TagSpecification.1.Tag.1.Value":                                       "prod",
		"TagSpecification.1.Tag.2.Key":                                         "Team",
		"TagSpecification.1.Tag.2.Value":                                       "infra",
	}, &fleet)
	require.Len(t, fleet.instanceIDs(), 2)

	for _, id := range fleet.instanceIDs() {
		env, ok := ec2InstanceTagValue(t, ts, id, "Env")
		assert.True(t, ok, "instance %s lost the caller's Env tag", id)
		assert.Equal(t, "prod", env)

		team, ok := ec2InstanceTagValue(t, ts, id, "Team")
		assert.True(t, ok, "instance %s lost the caller's Team tag", id)
		assert.Equal(t, "infra", team)

		fleetID, ok := ec2InstanceTagValue(t, ts, id, fleetIDTagKey)
		assert.True(t, ok, "instance %s lost its %s tag", id, fleetIDTagKey)
		assert.Equal(t, fleet.FleetID, fleetID)
	}
}

// ec2TagLimit is the number of user tags EC2 allows on one resource, restated here
// because an external test package cannot see ec2MaxTagsPerResource. That is a feature:
// the tests assert the documented number rather than whatever the constant happens to
// hold, so editing the constant alone cannot make them pass.
//
// "Maximum number of tags per resource – 50", from the EC2 tagging documentation.
const ec2TagLimit = 50

// ec2TagLimitMessage is the wording substrate emits when a resource is at its tag
// limit, pinned so it cannot drift silently.
//
// Provenance is split, and the weaker half is the message. The *code*
// TagLimitExceeded is documented in EC2's client-error table ("You've reached the
// limit on the number of tags that you can assign to the specified resource"); the
// wire *message* is published nowhere, so this string is moto's rather than a
// captured response.
const ec2TagLimitMessage = "The maximum number of Tags for a resource has been reached."

// ec2NumberedTags returns count tags named key1..keyN, for filling a resource up to
// its limit.
func ec2NumberedTags(prefix string, count int) map[string]string {
	params := map[string]string{}
	for i := 1; i <= count; i++ {
		params[fmt.Sprintf("Tag.%d.Key", i)] = fmt.Sprintf("%s%d", prefix, i)
		params[fmt.Sprintf("Tag.%d.Value", i)] = "v"
	}
	return params
}

// ec2CreateTags issues a CreateTags request adding the given tags to id and returns
// the HTTP status, error code and message.
func ec2CreateTags(t *testing.T, ts *httptest.Server, id string, tags map[string]string) (int, string, string) {
	t.Helper()
	params := map[string]string{"Action": "CreateTags", "ResourceId.1": id}
	for k, v := range tags {
		params[k] = v
	}
	return ec2ErrorDetail(t, ts, params)
}

// ec2InstanceTagCount returns how many tags DescribeInstances reports on instID.
func ec2InstanceTagCount(t *testing.T, ts *httptest.Server, instID string) int {
	t.Helper()
	var desc describedInstances
	ec2FleetXML(t, ts, map[string]string{
		"Action":       "DescribeInstances",
		"InstanceId.1": instID,
	}, &desc)
	require.Len(t, desc.Instances, 1)
	return len(desc.Instances[0].Tags)
}

// TestEC2_CreateTags_EnforcesTagLimit covers #469: substrate accepted any number of
// tags, so a consumer that hit real EC2's 50-tag ceiling had no way to test the
// TagLimitExceeded branch — and, worse, a test asserting the rejection passed against
// substrate while the same code failed against AWS.
//
// "Maximum number of tags per resource – 50", from the tagging documentation.
func TestEC2_CreateTags_EnforcesTagLimit(t *testing.T) {
	ts := newEC2TestServer(t)
	instID := ec2TagTestInstance(t, ts)

	// Exactly 50 is legal.
	status, _, _ := ec2CreateTags(t, ts, instID, ec2NumberedTags("key", ec2TagLimit))
	require.Equal(t, http.StatusOK, status, "50 tags must be accepted")
	require.Equal(t, ec2TagLimit, ec2InstanceTagCount(t, ts, instID))

	// The 51st is not.
	status, code, message := ec2CreateTags(t, ts, instID, map[string]string{
		"Tag.1.Key":   "one-too-many",
		"Tag.1.Value": "v",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "TagLimitExceeded", code)
	assert.Equal(t, ec2TagLimitMessage, message)
	assert.Equal(t, ec2TagLimit, ec2InstanceTagCount(t, ts, instID),
		"the rejected tag must not have been applied")
}

// TestEC2_CreateTags_OverwriteAtLimitSucceeds is the subtle half of the counting rule,
// and the one a naive implementation gets wrong.
//
// The count is over the post-merge key *set*, so a key already present adds nothing
// and overwriting it on a resource already at 50 succeeds. Written as
// len(existing)+len(incoming) > 50 this would fail — real AWS permits it, as
// getmoto/moto#8151 reports.
func TestEC2_CreateTags_OverwriteAtLimitSucceeds(t *testing.T) {
	ts := newEC2TestServer(t)
	instID := ec2TagTestInstance(t, ts)

	status, _, _ := ec2CreateTags(t, ts, instID, ec2NumberedTags("key", ec2TagLimit))
	require.Equal(t, http.StatusOK, status)

	tests := []struct {
		name       string
		tagKey     string
		wantReject bool
	}{
		{name: "overwriting an existing key is allowed at the limit", tagKey: "key7"},
		{name: "adding a new key is not", tagKey: "key51", wantReject: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, code, _ := ec2CreateTags(t, ts, instID, map[string]string{
				"Tag.1.Key":   tc.tagKey,
				"Tag.1.Value": "changed",
			})

			if tc.wantReject {
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "TagLimitExceeded", code)
				return
			}

			require.Equal(t, http.StatusOK, status)
			value, ok := ec2InstanceTagValue(t, ts, instID, tc.tagKey)
			assert.True(t, ok)
			assert.Equal(t, "changed", value, "the overwrite must take effect")
			assert.Equal(t, ec2TagLimit, ec2InstanceTagCount(t, ts, instID),
				"an overwrite must not grow the tag count")
		})
	}
}

// TestEC2_CreateTags_LimitRejectionIsAllOrNothing pins that the limit is checked across
// every resource the request names before any of them is modified, matching the
// reserved-key check's ordering. CreateTags accepts up to 1000 IDs and a partial apply
// is a state real EC2 never produces.
func TestEC2_CreateTags_LimitRejectionIsAllOrNothing(t *testing.T) {
	ts := newEC2TestServer(t)
	roomy := ec2TagTestInstance(t, ts)
	full := ec2TagTestInstance(t, ts)

	// Only the second instance is at the limit.
	status, _, _ := ec2CreateTags(t, ts, full, ec2NumberedTags("key", ec2TagLimit))
	require.Equal(t, http.StatusOK, status)

	params := map[string]string{
		"Action":       "CreateTags",
		"ResourceId.1": roomy,
		"ResourceId.2": full,
		"Tag.1.Key":    "pushes-over",
		"Tag.1.Value":  "v",
	}
	status, code, _ := ec2ErrorDetail(t, ts, params)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "TagLimitExceeded", code)

	// Neither resource is modified — including the one that had room.
	_, ok := ec2InstanceTagValue(t, ts, roomy, "pushes-over")
	assert.False(t, ok,
		"the resource with room must not be tagged when another named resource is over")
	assert.Equal(t, ec2TagLimit, ec2InstanceTagCount(t, ts, full))
}

// TestEC2_TagLimit_ExemptsReservedKeys is #469's own acceptance criterion, asserted on a
// real fleet instance rather than a synthetic resource.
//
// "Tags with the aws: prefix do not count against your tags per resource limit." This is
// load-bearing rather than pedantic: substrate stamps aws:ec2:fleet-id on every fleet
// instance, so a counter that included reserved keys would reject a fleet instance
// carrying 50 user tags — a launch real EC2 accepts.
func TestEC2_TagLimit_ExemptsReservedKeys(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "tag-limit-exempt")

	var fleet createFleetResp
	ec2FleetXML(t, ts, map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
	}, &fleet)
	ids := fleet.instanceIDs()
	require.Len(t, ids, 1)
	instID := ids[0]

	// The instance already carries the reserved fleet-ID tag...
	_, ok := ec2InstanceTagValue(t, ts, instID, fleetIDTagKey)
	require.True(t, ok, "fleet instance should carry %s", fleetIDTagKey)

	// ...and can still take a full complement of 50 user tags on top of it.
	status, _, _ := ec2CreateTags(t, ts, instID, ec2NumberedTags("user", ec2TagLimit))
	require.Equal(t, http.StatusOK, status,
		"a reserved tag must not consume room from the user tag limit")

	// 50 user tags plus the reserved one: 51 tags on the resource, all legal.
	assert.Equal(t, ec2TagLimit+1, ec2InstanceTagCount(t, ts, instID))

	// The 51st *user* tag is still refused, so the exemption did not raise the ceiling.
	status, code, _ := ec2CreateTags(t, ts, instID, map[string]string{
		"Tag.1.Key":   "user51",
		"Tag.1.Value": "v",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "TagLimitExceeded", code)
}

// TestEC2_TagOnCreate_EnforcesTagLimit covers the limit on the tag-on-create paths,
// where a new resource's tag count is just the request's own.
func TestEC2_TagOnCreate_EnforcesTagLimit(t *testing.T) {
	// launchTagParams builds count instance-scoped launch tags.
	launchTagParams := func(count int) map[string]string {
		params := map[string]string{"TagSpecification.1.ResourceType": "instance"}
		for i := 1; i <= count; i++ {
			params[fmt.Sprintf("TagSpecification.1.Tag.%d.Key", i)] = fmt.Sprintf("key%d", i)
			params[fmt.Sprintf("TagSpecification.1.Tag.%d.Value", i)] = "v"
		}
		return params
	}

	t.Run("50 launch tags are accepted", func(t *testing.T) {
		ts := newEC2TestServer(t)
		params := map[string]string{
			"Action":   "RunInstances",
			"ImageId":  "ami-0taglimit0000001",
			"MinCount": "1",
			"MaxCount": "1",
		}
		for k, v := range launchTagParams(ec2TagLimit) {
			params[k] = v
		}
		ids := ec2RunInstanceIDs(t, ts, params)
		require.Len(t, ids, 1)
		assert.Equal(t, ec2TagLimit, ec2InstanceTagCount(t, ts, ids[0]))
	})

	t.Run("51 launch tags are rejected and nothing is created", func(t *testing.T) {
		ts := newEC2TestServer(t)
		params := map[string]string{
			"Action":   "RunInstances",
			"ImageId":  "ami-0taglimit0000002",
			"MinCount": "1",
			"MaxCount": "1",
		}
		for k, v := range launchTagParams(ec2TagLimit + 1) {
			params[k] = v
		}
		status, code, message := ec2ErrorDetail(t, ts, params)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "TagLimitExceeded", code)
		assert.Equal(t, ec2TagLimitMessage, message)
		assert.Zero(t, ec2InstanceCount(t, ts),
			"a launch rejected for the tag limit must not create an instance")
	})

	t.Run("51 tags on CreateNatGateway are rejected", func(t *testing.T) {
		ts := newEC2TestServer(t)
		subnetID := ec2TagTestSubnet(t, ts)
		params := map[string]string{
			"Action":                          "CreateNatGateway",
			"SubnetId":                        subnetID,
			"ConnectivityType":                "private",
			"TagSpecification.1.ResourceType": "natgateway",
		}
		for i := 1; i <= ec2TagLimit+1; i++ {
			params[fmt.Sprintf("TagSpecification.1.Tag.%d.Key", i)] = fmt.Sprintf("key%d", i)
			params[fmt.Sprintf("TagSpecification.1.Tag.%d.Value", i)] = "v"
		}
		status, code, _ := ec2ErrorDetail(t, ts, params)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "TagLimitExceeded", code)

		var gws struct {
			Gateways []struct {
				NatGatewayID string `xml:"natGatewayId"`
			} `xml:"natGatewaySet>item"`
		}
		ec2FleetXML(t, ts, map[string]string{"Action": "DescribeNatGateways"}, &gws)
		assert.Empty(t, gws.Gateways)
	})

	t.Run("51 tags on CreateImage are rejected", func(t *testing.T) {
		ts := newEC2TestServer(t)
		instID := ec2TagTestInstance(t, ts)
		params := map[string]string{
			"Action":                          "CreateImage",
			"InstanceId":                      instID,
			"Name":                            "over-limit-ami",
			"TagSpecification.1.ResourceType": "image",
		}
		for i := 1; i <= ec2TagLimit+1; i++ {
			params[fmt.Sprintf("TagSpecification.1.Tag.%d.Key", i)] = fmt.Sprintf("key%d", i)
			params[fmt.Sprintf("TagSpecification.1.Tag.%d.Value", i)] = "v"
		}
		status, code, _ := ec2ErrorDetail(t, ts, params)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "TagLimitExceeded", code)

		var images struct {
			Images []struct {
				ImageID string `xml:"imageId"`
			} `xml:"imagesSet>item"`
		}
		ec2FleetXML(t, ts, map[string]string{"Action": "DescribeImages"}, &images)
		assert.Empty(t, images.Images)
	})
}

// TestEC2_Fleet_TagLimitWithStampedTag is the fleet half of the exemption: a fleet whose
// caller names the full 50 instance tags still launches, because the fleet-ID tag
// substrate adds on top is reserved and excluded from the count.
//
// Under a counter that included reserved keys this launch fails at 51 — the exact
// failure #469 calls out, and one real EC2 does not produce.
func TestEC2_Fleet_TagLimitWithStampedTag(t *testing.T) {
	ts := newEC2TestServer(t)
	ltID := newFleetLaunchTemplate(t, ts, "fleet-tag-limit")

	params := map[string]string{
		"Action": "CreateFleet",
		"Type":   "instant",
		"LaunchTemplateConfigs.1.LaunchTemplateSpecification.LaunchTemplateId": ltID,
		"TargetCapacitySpecification.TotalTargetCapacity":                      "1",
		"TagSpecification.1.ResourceType":                                      "instance",
	}
	for i := 1; i <= ec2TagLimit; i++ {
		params[fmt.Sprintf("TagSpecification.1.Tag.%d.Key", i)] = fmt.Sprintf("key%d", i)
		params[fmt.Sprintf("TagSpecification.1.Tag.%d.Value", i)] = "v"
	}

	var fleet createFleetResp
	ec2FleetXML(t, ts, params, &fleet)
	ids := fleet.instanceIDs()
	require.Len(t, ids, 1, "a fleet with 50 caller tags must still launch")

	// 50 caller tags plus the reserved stamp.
	assert.Equal(t, ec2TagLimit+1, ec2InstanceTagCount(t, ts, ids[0]))
	fleetID, ok := ec2InstanceTagValue(t, ts, ids[0], fleetIDTagKey)
	assert.True(t, ok, "the stamp must survive a fully-tagged launch")
	assert.Equal(t, fleet.FleetID, fleetID)
}

// TestEC2_CreateTags_TagLimitAppliesToEveryTaggableResource pins that the limit is
// counted for every resource type substrate can tag, not just instances.
//
// The check and the apply step read the same state key through one function, so a
// resource type the apply step tags but the check could not resolve would be tagged
// past the limit. This table is what makes that impossible to introduce quietly: it
// walks all eight ID prefixes substrate recognizes.
//
// It asserts on the rejection rather than a tag read-back because, apart from
// instances and NAT gateways, substrate's Describe operations for these resources do
// not emit a tagSet at all — the rejection is the observation available.
func TestEC2_CreateTags_TagLimitAppliesToEveryTaggableResource(t *testing.T) {
	// eipAllocationID allocates an Elastic IP and returns its allocation ID.
	eipAllocationID := func(t *testing.T, ts *httptest.Server) string {
		t.Helper()
		var addr struct {
			AllocationID string `xml:"allocationId"`
		}
		ec2FleetXML(t, ts, map[string]string{"Action": "AllocateAddress", "Domain": "vpc"}, &addr)
		require.NotEmpty(t, addr.AllocationID)
		return addr.AllocationID
	}

	// vpcID creates a VPC and returns its ID, for the resources that need a parent.
	vpcID := func(t *testing.T, ts *httptest.Server) string {
		t.Helper()
		var vpc struct {
			VPCID string `xml:"vpc>vpcId"`
		}
		ec2FleetXML(t, ts, map[string]string{"Action": "CreateVpc", "CidrBlock": "10.7.0.0/16"}, &vpc)
		require.NotEmpty(t, vpc.VPCID)
		return vpc.VPCID
	}

	tests := []struct {
		name   string
		prefix string
		create func(t *testing.T, ts *httptest.Server) string
	}{
		{name: "instance", prefix: "i-", create: ec2TagTestInstance},
		{name: "subnet", prefix: "subnet-", create: ec2TagTestSubnet},
		{name: "vpc", prefix: "vpc-", create: vpcID},
		{name: "elastic ip", prefix: "eipalloc-", create: eipAllocationID},
		{
			name:   "security group",
			prefix: "sg-",
			create: func(t *testing.T, ts *httptest.Server) string {
				t.Helper()
				var sg struct {
					GroupID string `xml:"groupId"`
				}
				ec2FleetXML(t, ts, map[string]string{
					"Action":           "CreateSecurityGroup",
					"GroupName":        "tag-limit-sg",
					"GroupDescription": "tag limit",
					"VpcId":            vpcID(t, ts),
				}, &sg)
				require.NotEmpty(t, sg.GroupID)
				return sg.GroupID
			},
		},
		{
			name:   "internet gateway",
			prefix: "igw-",
			create: func(t *testing.T, ts *httptest.Server) string {
				t.Helper()
				var igw struct {
					ID string `xml:"internetGateway>internetGatewayId"`
				}
				ec2FleetXML(t, ts, map[string]string{"Action": "CreateInternetGateway"}, &igw)
				require.NotEmpty(t, igw.ID)
				return igw.ID
			},
		},
		{
			name:   "route table",
			prefix: "rtb-",
			create: func(t *testing.T, ts *httptest.Server) string {
				t.Helper()
				var rtb struct {
					ID string `xml:"routeTable>routeTableId"`
				}
				ec2FleetXML(t, ts, map[string]string{
					"Action": "CreateRouteTable",
					"VpcId":  vpcID(t, ts),
				}, &rtb)
				require.NotEmpty(t, rtb.ID)
				return rtb.ID
			},
		},
		{
			name:   "nat gateway",
			prefix: "nat-",
			create: func(t *testing.T, ts *httptest.Server) string {
				t.Helper()
				var gw struct {
					ID string `xml:"natGateway>natGatewayId"`
				}
				ec2FleetXML(t, ts, map[string]string{
					"Action":           "CreateNatGateway",
					"SubnetId":         ec2TagTestSubnet(t, ts),
					"ConnectivityType": "private",
				}, &gw)
				require.NotEmpty(t, gw.ID)
				return gw.ID
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			id := tc.create(t, ts)
			require.True(t, strings.HasPrefix(id, tc.prefix),
				"%s should have the %q prefix, got %s", tc.name, tc.prefix, id)

			status, _, _ := ec2CreateTags(t, ts, id, ec2NumberedTags("key", ec2TagLimit))
			require.Equal(t, http.StatusOK, status, "50 tags must be accepted on a %s", tc.name)

			status, code, message := ec2CreateTags(t, ts, id, map[string]string{
				"Tag.1.Key":   "one-too-many",
				"Tag.1.Value": "v",
			})
			assert.Equal(t, http.StatusBadRequest, status, "the 51st tag on a %s", tc.name)
			assert.Equal(t, "TagLimitExceeded", code)
			assert.Equal(t, ec2TagLimitMessage, message)
		})
	}
}

// TestEC2_CreateTags_UncountedResourceIDs pins that an ID the apply step will not touch
// is not counted against either.
//
// Substrate's CreateTags ignores an ID it cannot resolve rather than rejecting it, so
// counting one would refuse a request real EC2 accepts as a no-op — turning a
// silent-success infidelity into a spurious-rejection one. The two rows are the two
// ways an ID goes unresolved: a prefix substrate does not tag, and a well-formed prefix
// naming nothing.
func TestEC2_CreateTags_UncountedResourceIDs(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{name: "a prefix substrate does not tag", id: "ami-0notaggable000001"},
		{name: "a taggable prefix naming nothing", id: "i-0doesnotexist00001"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			status, _, _ := ec2CreateTags(t, ts, tc.id, ec2NumberedTags("key", ec2TagLimit+1))
			assert.Equal(t, http.StatusOK, status,
				"an ID the apply step ignores must not be counted against")
		})
	}
}
