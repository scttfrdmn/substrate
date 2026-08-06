package emulator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// profileStackTemplate is #581's reproducer: a role, and an instance profile that
// references it. This is the shape every "EC2 instance with a role" template has, so
// the stack that could not be deleted was a very ordinary one.
const profileStackTemplate = `{
	"Resources": {
		"ProbeRole": {
			"Type": "AWS::IAM::Role",
			"Properties": {
				"RoleName": "probe-role-1",
				"AssumeRolePolicyDocument": {
					"Version": "2012-10-17",
					"Statement": [{
						"Effect": "Allow",
						"Principal": {"Service": "ec2.amazonaws.com"},
						"Action": "sts:AssumeRole"
					}]
				}
			}
		},
		"ProbeProfile": {
			"Type": "AWS::IAM::InstanceProfile",
			"Properties": {
				"InstanceProfileName": "probe-profile-1",
				"Roles": [{"Ref": "ProbeRole"}]
			}
		}
	}
}`

// iamCall dispatches one IAM operation with a JSON body and returns the response,
// which is how these tests observe IAM directly rather than through the stack.
//
// The returned error is deliberately ignored: the deployer's dispatch renders any
// status of 400 or above as a Go error, and half of these calls are asserting exactly
// such a refusal. A non-nil response is required instead, which is what separates a
// modeled refusal from the request never reaching the plugin at all — a routing
// failure returns no response.
func iamCall(t *testing.T, d *emulator.StackDeployer, streamID, op string,
	body map[string]string) *emulator.AWSResponse {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	resp, _ := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: "iam", Operation: op, Body: raw,
		Headers: map[string]string{}, Params: map[string]string{},
	}, streamID)
	require.NotNil(t, resp, "%s did not reach the IAM plugin", op)
	return resp
}

// TestCFN_AStackHoldingAnInstanceProfileDeletes is #581. The stack could not
// converge: DeleteRole succeeded while the profile still held the role, and
// DeleteInstanceProfile then refused with DeleteConflict — so the failure landed on
// the resource that was still *present*, and there was no RetainResources escape
// because retaining the profile would leave it behind forever. A second DeleteStack
// failed identically.
func TestCFN_AStackHoldingAnInstanceProfileDeletes(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	result, err := d.Deploy(ctx, profileStackTemplate, "pstack", nil)
	require.NoError(t, err)
	for _, res := range result.Resources {
		require.Empty(t, res.Error, "%s must deploy, or the delete proves nothing", res.LogicalID)
	}
	// Not vacuous: the profile really does hold the role, which is the condition
	// that made the delete fail.
	require.Contains(t, string(iamCall(t, d, "pstack", "GetInstanceProfile",
		map[string]string{"InstanceProfileName": "probe-profile-1"}).Body), "probe-role-1")

	require.NoError(t, d.DeleteStack(ctx, "pstack"),
		"a stack holding an instance profile must reach DELETE_COMPLETE")

	// Both are gone from IAM, not merely dropped from the stack record.
	assert.Equal(t, http.StatusNotFound, iamCall(t, d, "pstack", "GetRole",
		map[string]string{"RoleName": "probe-role-1"}).StatusCode)
	assert.Equal(t, http.StatusNotFound, iamCall(t, d, "pstack", "GetInstanceProfile",
		map[string]string{"InstanceProfileName": "probe-profile-1"}).StatusCode)

	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	for _, s := range stacks {
		assert.NotEqual(t, "pstack", s.StackName, "the stack record must be gone too")
	}
}

// TestCFN_TheProfileDetachIsDispatchedBeforeEitherDelete asserts the call order in
// the recorded events rather than trusting the priority table. The table already put
// the profile's delete before the role's — priority 2 before 1, swept descending —
// and the stack still failed, so the order is what has to be observed.
func TestCFN_TheProfileDetachIsDispatchedBeforeEitherDelete(t *testing.T) {
	d, _, _, store := newSweepDeployer(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, profileStackTemplate, "porder", nil)
	require.NoError(t, err)
	created := sweepOperations(t, store, "porder")

	require.NoError(t, d.DeleteStack(ctx, "porder"))

	deleted := sweepOperations(t, store, "porder")[len(created):]
	assert.Equal(t, []string{
		"RemoveRoleFromInstanceProfile", // the pre-step
		"DeleteInstanceProfile",         // priority 2, swept first
		"DeleteRole",                    // priority 1, and now unheld
	}, deleted, "the detach must precede both deletes")
}

// TestIAM_DeleteRoleRefusesARoleHeldByAnInstanceProfile is #581's first defect with
// no CloudFormation involved. DeleteRole succeeded on a role an instance profile
// held, which left the profile listing a role that no longer existed — the dangling
// reference that made the stack unfixable. The DeleteRole reference requires the
// instance profile be removed first, and DeleteConflict is 409.
func TestIAM_DeleteRoleRefusesARoleHeldByAnInstanceProfile(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	const stream = "direct"

	require.Equal(t, http.StatusOK, iamCall(t, d, stream, "CreateRole", map[string]string{
		"RoleName":                 "direct-role",
		"AssumeRolePolicyDocument": `{"Statement":[]}`,
	}).StatusCode)
	require.Equal(t, http.StatusOK, iamCall(t, d, stream, "CreateInstanceProfile",
		map[string]string{"InstanceProfileName": "direct-profile"}).StatusCode)
	require.Equal(t, http.StatusOK, iamCall(t, d, stream, "AddRoleToInstanceProfile",
		map[string]string{
			"InstanceProfileName": "direct-profile", "RoleName": "direct-role",
		}).StatusCode)

	refusal := iamCall(t, d, stream, "DeleteRole", map[string]string{"RoleName": "direct-role"})
	assert.Equal(t, http.StatusConflict, refusal.StatusCode, "DeleteConflict is 409")
	body := string(refusal.Body)
	assert.Contains(t, body, "DeleteConflict")
	// "The error message describes these entities" — so the profile must be named, or
	// a caller cannot tell what to detach from.
	assert.Contains(t, body, "direct-profile")

	// The role is still there: a refused delete must not have deleted anything.
	assert.Equal(t, http.StatusOK, iamCall(t, d, stream, "GetRole",
		map[string]string{"RoleName": "direct-role"}).StatusCode)

	// And it deletes once detached, which is the documented remedy.
	require.Equal(t, http.StatusOK, iamCall(t, d, stream, "RemoveRoleFromInstanceProfile",
		map[string]string{
			"InstanceProfileName": "direct-profile", "RoleName": "direct-role",
		}).StatusCode)
	assert.Equal(t, http.StatusOK, iamCall(t, d, stream, "DeleteRole",
		map[string]string{"RoleName": "direct-role"}).StatusCode)
}

// TestIAM_DeleteRoleRefusesUntilEveryProfileReleasesIt covers the case the sweep's
// one-profile shape hides: IAM allows a role in more than one instance profile, so a
// check that stopped at the first holder — or one that deleted after any single
// detach — would still leave a dangling reference.
func TestIAM_DeleteRoleRefusesUntilEveryProfileReleasesIt(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	const stream = "twoprofiles"

	require.Equal(t, http.StatusOK, iamCall(t, d, stream, "CreateRole", map[string]string{
		"RoleName":                 "shared-role",
		"AssumeRolePolicyDocument": `{"Statement":[]}`,
	}).StatusCode)
	for _, name := range []string{"holder-a", "holder-b"} {
		require.Equal(t, http.StatusOK, iamCall(t, d, stream, "CreateInstanceProfile",
			map[string]string{"InstanceProfileName": name}).StatusCode)
		require.Equal(t, http.StatusOK, iamCall(t, d, stream, "AddRoleToInstanceProfile",
			map[string]string{
				"InstanceProfileName": name, "RoleName": "shared-role",
			}).StatusCode)
	}

	both := iamCall(t, d, stream, "DeleteRole", map[string]string{"RoleName": "shared-role"})
	require.Equal(t, http.StatusConflict, both.StatusCode)
	assert.Contains(t, string(both.Body), "holder-a")
	assert.Contains(t, string(both.Body), "holder-b", "every holder must be named")

	require.Equal(t, http.StatusOK, iamCall(t, d, stream, "RemoveRoleFromInstanceProfile",
		map[string]string{
			"InstanceProfileName": "holder-a", "RoleName": "shared-role",
		}).StatusCode)

	one := iamCall(t, d, stream, "DeleteRole", map[string]string{"RoleName": "shared-role"})
	assert.Equal(t, http.StatusConflict, one.StatusCode,
		"one profile still holds it, so the delete is still refused")
	assert.Contains(t, string(one.Body), "holder-b")
	assert.NotContains(t, string(one.Body), "holder-a",
		"a released profile must not still be named")

	require.Equal(t, http.StatusOK, iamCall(t, d, stream, "RemoveRoleFromInstanceProfile",
		map[string]string{
			"InstanceProfileName": "holder-b", "RoleName": "shared-role",
		}).StatusCode)
	assert.Equal(t, http.StatusOK, iamCall(t, d, stream, "DeleteRole",
		map[string]string{"RoleName": "shared-role"}).StatusCode)
}

// TestCFN_AProfileWithNoRolesStillDeletes pins that the pre-step is empty rather
// than absent-safe by accident: a profile declaring no Roles emits no
// RemoveRoleFromInstanceProfile at all, so the hook adds no dispatch for the common
// case and cannot fail one.
func TestCFN_AProfileWithNoRolesStillDeletes(t *testing.T) {
	tmpl := `{"Resources":{"Bare":{"Type":"AWS::IAM::InstanceProfile",` +
		`"Properties":{"InstanceProfileName":"bare-profile"}}}}`

	d, _, _, store := newSweepDeployer(t)
	ctx := context.Background()

	_, err := d.Deploy(ctx, tmpl, "bare", nil)
	require.NoError(t, err)
	created := sweepOperations(t, store, "bare")

	require.NoError(t, d.DeleteStack(ctx, "bare"))

	deleted := sweepOperations(t, store, "bare")[len(created):]
	assert.Equal(t, []string{"DeleteInstanceProfile"}, deleted,
		"a profile with no roles must dispatch only its own delete")
	assert.Equal(t, http.StatusNotFound, iamCall(t, d, "bare", "GetInstanceProfile",
		map[string]string{"InstanceProfileName": "bare-profile"}).StatusCode)
}

// TestCFN_AGeneratedProfileNameDetachesItsGeneratedRole is the interaction with
// #560, and the reason PR A landed first. Neither name is in the template, so the
// pre-step's !Ref must resolve to the role's *generated* physical name — a detach
// naming the logical ID would silently do nothing and the delete would fail again.
func TestCFN_AGeneratedProfileNameDetachesItsGeneratedRole(t *testing.T) {
	tmpl := `{
		"Resources": {
			"GenRole": {
				"Type": "AWS::IAM::Role",
				"Properties": {"AssumeRolePolicyDocument": {"Statement": []}}
			},
			"GenProfile": {
				"Type": "AWS::IAM::InstanceProfile",
				"Properties": {"Roles": [{"Ref": "GenRole"}]}
			}
		}
	}`

	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	result, err := d.Deploy(ctx, tmpl, "genboth", nil)
	require.NoError(t, err)
	role := resourceByLogicalID(t, result, "GenRole").PhysicalID
	require.NotEqual(t, "GenRole", role, "the role's name must be generated, per #560")

	require.NoError(t, d.DeleteStack(ctx, "genboth"),
		"the detach must name the generated role, not the logical ID")
	assert.Equal(t, http.StatusNotFound, iamCall(t, d, "genboth", "GetRole",
		map[string]string{"RoleName": role}).StatusCode)
}
