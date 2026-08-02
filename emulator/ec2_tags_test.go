package emulator_test

import (
	"net/http"
	"net/http/httptest"
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

// TestEC2_ReservedTagCheck_DoesNotBreakFleetTagging is the gate on #452 having
// landed in the right code path.
//
// Substrate stamps aws:ec2:fleet-id (#443) by building RunInstances
// TagSpecification params, which runInstances parses itself rather than routing
// through CreateTags. Putting the reserved-prefix check in that parse would reject
// substrate's own fleet tagging and silently undo #443 — for an "instant" fleet
// this tag is the only route from a fleet back to its instances, so the failure
// would surface as a fully-running fleet reporting as empty.
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
