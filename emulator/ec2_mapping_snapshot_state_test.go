package emulator_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #732: a block device mapping naming a snapshot that is not usable yet is refused, on the
// two paths that consume a mapping rather than record one.
//
// CreateVolume already refused such a snapshot (#715) while a launch declaring the same
// snapshot as a mapping materialized a volume from it, so the two doors onto one rule
// disagreed: `aws ec2 create-volume --snapshot-id` failed and
// `aws ec2 run-instances --block-device-mappings` naming the same snapshot succeeded. A
// consumer restoring through a launch — which is what CDK's and Terraform's snapshot-backed
// volumes do — never reached its own error branch.

// ec2SnapshotMappingLaunch is a launch declaring one mapping restored from a snapshot.
func ec2SnapshotMappingLaunch(snapshotID string) map[string]string {
	return map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.1.Ebs.SnapshotId": snapshotID,
	}
}

// ec2SnapshotMappingRegisterImage registers an AMI whose root device restores from a snapshot,
// which is AWS's own second RegisterImage example.
func ec2SnapshotMappingRegisterImage(name, snapshotID string) map[string]string {
	return map[string]string{
		"Action": "RegisterImage", "Name": name, "RootDeviceName": "/dev/sda1",
		"BlockDeviceMapping.1.DeviceName":     "/dev/sda1",
		"BlockDeviceMapping.1.Ebs.SnapshotId": snapshotID,
	}
}

// ec2PendingSnapshot creates a snapshot and seeds it so every observation reports state.
//
// The order matters: the snapshot is created *before* the seed, so the create itself is not
// the thing under test — a snapshot born under a seed is born in the seeded state, which
// TestEC2_SnapshotProgression_PollsToCompleted covers.
func ec2PendingSnapshot(t *testing.T, ts *httptest.Server, state string) string {
	t.Helper()
	volumeID := ec2CreateSizedVolume(t, ts, 8, nil)
	snapshotID := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID
	switch state {
	case "pending":
		ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":9}`)
	default:
		ec2SeedSnapshotStatus(t, ts,
			`{"snapshotId":"*","pendingObservations":0,"finalState":"`+state+`"}`)
	}
	return snapshotID
}

// TestEC2_MappingSnapshot_LaunchRefusesAnUnusableSnapshot covers AWS's snapshot-states rule on
// the launch path, for all four states that are not `completed`.
//
// Every one of the four has AWS's own sentence behind it: pending and error say "a snapshot
// can't be used" outright, a recoverable snapshot "must first [be recovered] from the Recycle
// Bin", and a recovering one becomes "ready for use" only once it reaches completed. So the
// rule is "completed or refused", not "pending or error are refused".
func TestEC2_MappingSnapshot_LaunchRefusesAnUnusableSnapshot(t *testing.T) {
	t.Parallel()

	for _, state := range []string{"pending", "error", "recoverable", "recovering"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			volumeID := ec2CreateSizedVolume(t, ts, 8, nil)
			snapshotID := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID
			launch := ec2SnapshotMappingLaunch(snapshotID)

			// Unseeded the launch succeeds, which is what makes the refusal below a
			// consequence of the state and not of anything else in the request.
			require.Len(t, runInstancesLaunched(t, ts, launch), 1)

			if state == "pending" {
				ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":9}`)
			} else {
				ec2SeedSnapshotStatus(t, ts,
					`{"snapshotId":"*","pendingObservations":0,"finalState":"`+state+`"}`)
			}

			status, code, message := ec2ErrorDetail(t, ts, launch)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "IncorrectState", code,
				"the same code CreateVolume answers with for the same condition")
			assert.Contains(t, message, state, "the caller is told which state, so it can decide to wait")
			assert.Contains(t, message, snapshotID)
		})
	}
}

// TestEC2_MappingSnapshot_RegisterImageRefusesAnUnusableSnapshot covers the second door onto
// the rule. Registering an AMI from a snapshot of a root device volume is what AWS's own
// second RegisterImage example does, and an AMI recorded against a snapshot that never
// completed is one every later launch fails on.
func TestEC2_MappingSnapshot_RegisterImageRefusesAnUnusableSnapshot(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	snapshotID := ec2PendingSnapshot(t, ts, "pending")

	status, code, message := ec2ErrorDetail(t, ts,
		ec2SnapshotMappingRegisterImage("from-pending", snapshotID))
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "IncorrectState", code)
	assert.Contains(t, message, "pending")
	assert.Contains(t, message, snapshotID)

	// Nothing was registered: the check runs before the write, so a refused request leaves
	// no AMI naming a snapshot the caller was told it could not use.
	assert.Empty(t, ec2DescribedImageIDs(t, ts, nil))

	// Once the snapshot is usable, the same request is accepted — the refusal is about the
	// state and nothing else.
	ec2ClearSnapshotStatus(t, ts, "")
	var registered struct {
		ImageID string `xml:"imageId"`
	}
	ec2FleetXML(t, ts, ec2SnapshotMappingRegisterImage("from-completed", snapshotID), &registered)
	assert.NotEmpty(t, registered.ImageID)
}

// TestEC2_MappingSnapshot_RefusalWritesNothing pins the ordering the rest of the launch
// validator already honors: the refusal runs before ensureDefaultVPC, which commits a VPC,
// subnet, security group, gateway, route table and four index mutations.
//
// State left behind by a refused launch is worse than no rule at all, because the next request
// in the same test sees it.
func TestEC2_MappingSnapshot_RefusalWritesNothing(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	snapshotID := ec2PendingSnapshot(t, ts, "pending")

	_, code, _ := ec2ErrorDetail(t, ts, ec2SnapshotMappingLaunch(snapshotID))
	require.Equal(t, "IncorrectState", code)

	assert.Empty(t, ec2InstanceIDs(t, ts), "no instance")
	assert.Len(t, ec2DescribedVolumeIDs(t, ts, nil), 1,
		"only the volume the snapshot was taken from, so the mapping materialized nothing")
}

// TestEC2_MappingSnapshot_DoesNotSpendAPoll is what pins peekSnapshotStatus over
// observeSnapshotStatus, and it is the reason the observer is injected rather than read from
// the record.
//
// The countdown advances in exactly one place, so an observing read here would burn one
// observation *per mapping per request*: "pendingObservations: 3" would mean three polls in a
// test that only describes, one in a test that also tries a three-mapping launch, and
// something else again after a retry. A consumer's wait loop alternates a launch attempt and a
// describe, so it would race its own budget.
func TestEC2_MappingSnapshot_DoesNotSpendAPoll(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	volumeID := ec2CreateSizedVolume(t, ts, 8, nil)
	snapshotID := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID

	ec2SeedSnapshotStatus(t, ts, `{"snapshotId":"*","pendingObservations":1}`)

	// Three mappings naming the same snapshot, attempted twice: six observations, if the
	// rule consumed them.
	launch := map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1",
		"BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"BlockDeviceMapping.1.Ebs.SnapshotId": snapshotID,
		"BlockDeviceMapping.2.DeviceName":     "/dev/sdg",
		"BlockDeviceMapping.2.Ebs.SnapshotId": snapshotID,
		"BlockDeviceMapping.3.DeviceName":     "/dev/sdh",
		"BlockDeviceMapping.3.Ebs.SnapshotId": snapshotID,
	}
	for range 2 {
		_, code, _ := ec2ErrorDetail(t, ts, launch)
		require.Equal(t, "IncorrectState", code, "six mappings must not exhaust a one-poll budget")
	}

	assert.Equal(t, "pending", ec2DescribeOneSnapshot(t, ts, snapshotID).State,
		"the single budgeted observation is still unspent")
	assert.Equal(t, "completed", ec2DescribeOneSnapshot(t, ts, snapshotID).State)

	// And the launch the consumer was waiting to make now succeeds, all three volumes
	// materialized from the snapshot's size.
	launched := runInstancesLaunched(t, ts, launch)
	require.Len(t, launched, 1)
	assert.Len(t, ec2DescribedVolumeIDs(t, ts, map[string]string{
		"Filter.1.Name": "snapshot-id", "Filter.1.Value.1": snapshotID,
	}), 3)
}

// seedBlindStateManager passes every call through until it is armed, after which a Get against
// the snapshot control-plane namespace fails.
//
// Failing only that namespace is what makes the test specific: a store that failed every Get
// would never get past resolving the snapshot record, and the refusal a test then saw would be
// InvalidSnapshot.NotFound — the very collapse the assertion below exists to rule out.
type seedBlindStateManager struct {
	inner emulator.StateManager
	armed bool
}

// ec2SnapCtrlNamespace is the namespace the snapshot progression seeds live in. It is
// unexported, so the literal is repeated here; a rename that missed it would disarm this test
// rather than break it, which is why the assertion below also pins the status code.
const ec2SnapCtrlNamespaceLiteral = "ec2-snap-ctrl"

func (m *seedBlindStateManager) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.armed && namespace == ec2SnapCtrlNamespaceLiteral {
		return nil, errors.New("state store unavailable")
	}
	return m.inner.Get(ctx, namespace, key)
}

func (m *seedBlindStateManager) Put(ctx context.Context, namespace, key string, value []byte) error {
	return m.inner.Put(ctx, namespace, key, value)
}

func (m *seedBlindStateManager) Delete(ctx context.Context, namespace, key string) error {
	return m.inner.Delete(ctx, namespace, key)
}

func (m *seedBlindStateManager) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	return m.inner.List(ctx, namespace, prefix)
}

// TestEC2_MappingSnapshot_UnreadableStateIsNotPermission asserts that a state read the rule
// cannot complete answers InternalError / 500 rather than letting the mapping through.
//
// This is deliberately the opposite of the resolver's absent-on-error rule beside it, and the
// two are not in tension: an unresolvable snapshot is indistinguishable from an absent one, so
// reporting NotFound is honest, whereas an unreadable *state* is not evidence the snapshot is
// usable. Permitting the mapping would make the rule's absence indistinguishable from its
// satisfaction — the caller would see a launch succeed and could not tell whether it was
// checked. A 500 is also the only answer that tells an SDK to retry, which is the correct
// behavior for a transient store failure.
func TestEC2_MappingSnapshot_UnreadableStateIsNotPermission(t *testing.T) {
	t.Parallel()
	state := &seedBlindStateManager{inner: emulator.NewMemoryStateManager()}
	ts := newEC2TestServerWithState(t, state)

	volumeID := ec2CreateSizedVolume(t, ts, 8, nil)
	snapshotID := ec2CreateSnapshot(t, ts, map[string]string{"VolumeId": volumeID}).SnapshotID
	state.armed = true

	status, code, _ := ec2ErrorDetail(t, ts, ec2SnapshotMappingLaunch(snapshotID))
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Equal(t, "InternalError", code)

	assert.Empty(t, ec2InstanceIDs(t, ts), "and the launch wrote nothing")
}

// TestEC2_MappingSnapshot_LaunchTemplatesStayPermissive pins the exemption, which is a
// decision rather than an oversight.
//
// A template is not a launch: both create operations report mapping problems through AWS's
// documented `warning` member and refuse nothing (#693), so a 400 here would be substrate's
// invention. And a snapshot pending when a template is written may legitimately be completed
// by the time the template is used — refusing at write time would forbid the ordering AWS
// permits. The rule applies where the mapping is consumed, which the launch below shows.
func TestEC2_MappingSnapshot_LaunchTemplatesStayPermissive(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	snapshotID := ec2PendingSnapshot(t, ts, "pending")

	created, body := ltWarningRequest(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplate",
		"LaunchTemplateName":         "pending-snapshot",
		"LaunchTemplateData.ImageId": ec2TestImage,
		"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/sdf",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.SnapshotId": snapshotID,
	})
	require.NotEmpty(t, created.LaunchTemplateID, "the template is created")
	assert.Nil(t, created.Warning,
		"and not even warned about: the mapping is valid, only the snapshot is not ready: %s", body)

	version, versionBody := ltWarningRequest(t, ts, map[string]string{
		"Action":                     "CreateLaunchTemplateVersion",
		"LaunchTemplateName":         "pending-snapshot",
		"LaunchTemplateData.ImageId": ec2TestImage,
		"LaunchTemplateData.BlockDeviceMapping.1.DeviceName":     "/dev/sdg",
		"LaunchTemplateData.BlockDeviceMapping.1.Ebs.SnapshotId": snapshotID,
	})
	assert.Equal(t, int64(2), version.VersionNumber)
	assert.Nil(t, version.Warning, versionBody)

	// The launch from that template is where the state is read, so the rule is not escapable
	// by routing a mapping through a template.
	_, code, message := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "RunInstances", "MinCount": "1", "MaxCount": "1",
		"LaunchTemplate.LaunchTemplateName": "pending-snapshot",
	})
	assert.Equal(t, "IncorrectState", code)
	assert.Contains(t, message, snapshotID)

	// And the same template launches once the snapshot is usable, which is the ordering the
	// exemption exists to allow.
	ec2ClearSnapshotStatus(t, ts, "")
	require.Len(t, runInstancesLaunched(t, ts, map[string]string{
		"Action": "RunInstances", "MinCount": "1", "MaxCount": "1",
		"LaunchTemplate.LaunchTemplateName": "pending-snapshot",
	}), 1)
}
