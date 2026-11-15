package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ec2LaunchedPlacement is the part of one instancesSet>item #1525 and #1193 are about.
type ec2LaunchedPlacement struct {
	ID           string  `xml:"instanceId"`
	Architecture *string `xml:"architecture"`
	AZ           string  `xml:"placement>availabilityZone"`
	GroupName    *string `xml:"placement>groupName"`
	GroupID      *string `xml:"placement>groupId"`
}

// ec2RunOne launches one instance with params and returns its RunInstances item.
func ec2RunOne(t *testing.T, ts *httptest.Server, params map[string]string) ec2LaunchedPlacement {
	t.Helper()
	full := map[string]string{"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1"}
	for k, v := range params {
		full[k] = v
	}
	var doc struct {
		Items []ec2LaunchedPlacement `xml:"instancesSet>item"`
	}
	ec2FleetXML(t, ts, full, &doc)
	require.Len(t, doc.Items, 1)
	return doc.Items[0]
}

// ec2DescribeOne returns DescribeInstances' item for id.
func ec2DescribeOne(t *testing.T, ts *httptest.Server, id string) ec2LaunchedPlacement {
	t.Helper()
	var doc struct {
		Items []ec2LaunchedPlacement `xml:"reservationSet>item>instancesSet>item"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "DescribeInstances", "InstanceId.1": id}, &doc)
	require.Len(t, doc.Items, 1)
	return doc.Items[0]
}

// TestEC2_Instances_ReportArchitecture is #1525: Instance's architecture, "The architecture of
// the image", is reported by RunInstances and DescribeInstances — arm64 for a Graviton launch
// from the arm64 AMI, x86_64 for an Intel or AMD launch.
func TestEC2_Instances_ReportArchitecture(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, image, instanceType, want string
	}{
		{"Graviton on the arm64 AMI", ec2TestImageArm, "c7g.large", "arm64"},
		{"burstable Graviton", ec2TestImageArm, "t4g.micro", "arm64"},
		{"Intel", ec2TestImage, "c7i.xlarge", "x86_64"},
		{"AMD", ec2TestImage, "m7a.large", "x86_64"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			run := ec2RunOne(t, ts, map[string]string{"ImageId": tc.image, "InstanceType": tc.instanceType})
			require.NotNil(t, run.Architecture, "RunInstances reports architecture")
			assert.Equal(t, tc.want, *run.Architecture)
			desc := ec2DescribeOne(t, ts, run.ID)
			require.NotNil(t, desc.Architecture, "DescribeInstances reports architecture")
			assert.Equal(t, tc.want, *desc.Architecture)

			var img struct {
				Archs []string `xml:"imagesSet>item>architecture"`
			}
			ec2FleetXML(t, ts, map[string]string{"Action": "DescribeImages", "ImageId.1": tc.image}, &img)
			assert.Equal(t, []string{tc.want}, img.Archs, "the instance reports its image's architecture")
		})
	}

	t.Run("an instance stored before the field takes its type's", func(t *testing.T) {
		t.Parallel()
		state := emulator.NewMemoryStateManager()
		ts := newEC2TestServerWithState(t, state)
		run := ec2RunOne(t, ts, map[string]string{"ImageId": ec2TestImageArm, "InstanceType": "m7g.large"})
		ec2RewriteRecord(t, state, "instance:", run.ID, func(rec map[string]any) {
			require.Equal(t, "arm64", rec["architecture"], "a launch stores the architecture")
			delete(rec, "architecture")
		})
		desc := ec2DescribeOne(t, ts, run.ID)
		require.NotNil(t, desc.Architecture)
		assert.Equal(t, "arm64", *desc.Architecture)
	})
}

// ec2RewriteRecord applies edit to the stored JSON of the EC2 record under prefix whose key ends
// in id.
func ec2RewriteRecord(t *testing.T, state emulator.StateManager, prefix, id string, edit func(map[string]any)) {
	t.Helper()
	ctx := context.Background()
	keys, err := state.List(ctx, "ec2", prefix)
	require.NoError(t, err)
	for _, k := range keys {
		if !strings.HasSuffix(k, "/"+id) {
			continue
		}
		data, getErr := state.Get(ctx, "ec2", k)
		require.NoError(t, getErr)
		var rec map[string]any
		require.NoError(t, json.Unmarshal(data, &rec))
		edit(rec)
		out, marshalErr := json.Marshal(rec)
		require.NoError(t, marshalErr)
		require.NoError(t, state.Put(ctx, "ec2", k, out))
		return
	}
	t.Fatalf("no stored record under %s for %s", prefix, id)
}

// ec2CreatePlacementGroup creates a cluster placement group and returns its ID.
func ec2CreatePlacementGroup(t *testing.T, ts *httptest.Server, name string) string {
	t.Helper()
	var doc struct {
		ID string `xml:"placementGroup>groupId"`
	}
	ec2FleetXML(t, ts, map[string]string{"Action": "CreatePlacementGroup", "GroupName": name, "Strategy": "cluster"}, &doc)
	require.NotEmpty(t, doc.ID)
	return doc.ID
}

// TestEC2_Instances_ReportPlacementGroup is #1193's first half: the placement group a launch
// names, by GroupName or by GroupId, is stored and reported as placement's groupName and groupId;
// naming both is refused, since API_Placement publishes "you can specify GroupId or GroupName, but
// not both"; and a launch naming none renders no group.
func TestEC2_Instances_ReportPlacementGroup(t *testing.T) {
	t.Parallel()
	for _, form := range []string{"Placement.GroupName", "Placement.GroupId"} {
		t.Run("by "+form, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			pgID := ec2CreatePlacementGroup(t, ts, "workers")
			ref := map[string]string{"Placement.GroupName": "workers", "Placement.GroupId": pgID}[form]
			run := ec2RunOne(t, ts, map[string]string{"InstanceType": "c7i.xlarge", form: ref})
			for _, got := range []ec2LaunchedPlacement{run, ec2DescribeOne(t, ts, run.ID)} {
				require.NotNil(t, got.GroupName)
				require.NotNil(t, got.GroupID)
				assert.Equal(t, "workers", *got.GroupName)
				assert.Equal(t, pgID, *got.GroupID)
				assert.NotEmpty(t, got.AZ)
			}
		})
	}

	t.Run("no group renders none", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		run := ec2RunOne(t, ts, nil)
		desc := ec2DescribeOne(t, ts, run.ID)
		assert.Nil(t, desc.GroupName)
		assert.Nil(t, desc.GroupID)
	})

	for _, tc := range []struct {
		name   string
		params map[string]string
		code   string
	}{
		{"both forms", map[string]string{"Placement.GroupName": "workers", "Placement.GroupId": "PGID"},
			"InvalidParameterCombination"},
		{"an unknown name", map[string]string{"Placement.GroupName": "nope"}, "InvalidPlacementGroup.Unknown"},
		{"an unknown ID", map[string]string{"Placement.GroupId": "pg-0000000000000dead"}, "InvalidPlacementGroup.Unknown"},
	} {
		t.Run("refused: "+tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			pgID := ec2CreatePlacementGroup(t, ts, "workers")
			params := map[string]string{"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1"}
			for k, v := range tc.params {
				params[k] = strings.ReplaceAll(v, "PGID", pgID)
			}
			ec2RequireRefusal(t, ts, params, tc.code)
		})
	}
}

// TestEC2_DeletePlacementGroup_RefusesWhileInstancesRemain is #1193's second half,
// API_DeletePlacementGroup's "You must terminate all instances in the placement group before you
// can delete the placement group", answered with the client-error table's
// InvalidPlacementGroup.InUse.
func TestEC2_DeletePlacementGroup_RefusesWhileInstancesRemain(t *testing.T) {
	t.Parallel()
	for _, stop := range []bool{false, true} {
		name := "a running instance"
		if stop {
			name = "a stopped instance"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := newEC2TestServer(t)
			ec2CreatePlacementGroup(t, ts, "workers")
			run := ec2RunOne(t, ts, map[string]string{"InstanceType": "c7i.xlarge", "Placement.GroupName": "workers"})
			if stop {
				ec2OK(t, ts, map[string]string{"Action": "StopInstances", "InstanceId.1": run.ID})
			}
			del := map[string]string{"Action": "DeletePlacementGroup", "GroupName": "workers"}
			ec2RequireRefusalMessage(t, ts, del, "InvalidPlacementGroup.InUse",
				"The specified placement group is in use. If you are trying to delete a placement group, "+
					"ensure that its instances have been terminated.")

			ec2OK(t, ts, map[string]string{"Action": "TerminateInstances", "InstanceId.1": run.ID})
			ec2OK(t, ts, del)
		})
	}

	t.Run("an instance in another group does not hold it", func(t *testing.T) {
		t.Parallel()
		ts := newEC2TestServer(t)
		ec2CreatePlacementGroup(t, ts, "workers")
		ec2CreatePlacementGroup(t, ts, "others")
		ec2RunOne(t, ts, map[string]string{"InstanceType": "c7i.xlarge", "Placement.GroupName": "others"})
		ec2OK(t, ts, map[string]string{"Action": "DeletePlacementGroup", "GroupName": "workers"})
	})
}

// ec2PlacementFaultState fails every Get of a placement-group record once armed, so a store
// failure is reachable through the API.
type ec2PlacementFaultState struct {
	emulator.StateManager
	armed atomic.Bool
}

func (m *ec2PlacementFaultState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.armed.Load() && namespace == "ec2" && strings.HasPrefix(key, "placement_group:") {
		return nil, errors.New("state unavailable")
	}
	return m.StateManager.Get(ctx, namespace, key)
}

// TestEC2_PlacementGroup_StoreFailureIsNotUnknown pins #1193's discarded reads: a placement-group
// read that fails is an internal error on RunInstances and DeletePlacementGroup, not the
// InvalidPlacementGroup.Unknown a missing group answers.
func TestEC2_PlacementGroup_StoreFailureIsNotUnknown(t *testing.T) {
	t.Parallel()
	state := &ec2PlacementFaultState{StateManager: emulator.NewMemoryStateManager()}
	ts := newEC2TestServerWithState(t, state)
	pgID := ec2CreatePlacementGroup(t, ts, "workers")
	state.armed.Store(true)

	for _, params := range []map[string]string{
		{"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "Placement.GroupName": "workers"},
		{"Action": "RunInstances", "ImageId": ec2TestImage, "MinCount": "1", "MaxCount": "1", "Placement.GroupId": pgID},
		{"Action": "DeletePlacementGroup", "GroupName": "workers"},
	} {
		status, code, _ := ec2ErrorDetail(t, ts, params)
		assert.Equal(t, http.StatusInternalServerError, status, params["Action"])
		assert.NotEqual(t, "InvalidPlacementGroup.Unknown", code, params["Action"])
	}
}
