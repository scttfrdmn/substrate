package emulator_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A group's policies apply to its members' requests. AWS: "a group's permissions
// apply to all of its users", and the SimulatePrincipalPolicy reference says the
// simulation "also includes all of the policies that are attached to groups that
// the user belongs to".
//
// Before this, authz.go read only "<kind>_policies:" and "<kind>_inline*:" for the
// resolved entity, so a user whose sole grant came from a group was implicitly
// denied. Nothing caught it because no operation could put a policy on a group.
//
// The same widening is applied to IAMPlugin.authorize, so both loaders answer the
// same for one ARN — the #411 split.

// authzGroupState builds state with one user, one group, the user in the group, and
// whichever grants the caller asks for. Membership is written on both sides, the
// way addGroupMembership does, because a test writing only one side would pass
// against a loader reading only the other.
type authzGroupState struct {
	// GroupManaged is a managed policy ARN attached to the group.
	GroupManaged string

	// GroupManagedDoc is the document stored for GroupManaged, when it is not a
	// bundled ARN.
	GroupManagedDoc *emulator.PolicyDocument

	// GroupInline is an inline document embedded in the group.
	GroupInline *emulator.PolicyDocument

	// UserInline is an inline document embedded in the user.
	UserInline *emulator.PolicyDocument

	// OmitMembership skips the membership write, so the same grants can be checked
	// with and without the user being a member.
	OmitMembership bool
}

func newAuthzGroupState(t *testing.T, userName, groupName string, spec authzGroupState) emulator.StateManager {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	ctx := context.Background()

	put := func(key string, value any) {
		t.Helper()
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, state.Put(ctx, "iam", key, raw))
	}

	put(emulator.IAMUserKeyForTest(authzTestAccount, userName), emulator.IAMUser{
		UserName: userName,
		UserID:   "AIDATEST",
		ARN:      "arn:aws:iam::123456789012:user/" + userName,
		Path:     "/",
	})
	put(emulator.IAMGroupKeyForTest(authzTestAccount, groupName), emulator.IAMGroup{
		GroupName: groupName,
		GroupID:   "AGPATEST",
		ARN:       "arn:aws:iam::123456789012:group/" + groupName,
		Path:      "/",
	})
	if !spec.OmitMembership {
		put(emulator.IAMGroupUsersKeyForTest(authzTestAccount, groupName), []string{userName})
		put(emulator.IAMUserGroupsKeyForTest(authzTestAccount, userName), []string{groupName})
	}
	if spec.GroupManaged != "" {
		put(emulator.IAMAttachedPoliciesKeyForTest(authzTestAccount, "group", groupName), []string{spec.GroupManaged})
		if spec.GroupManagedDoc != nil {
			put(emulator.IAMPolicyKeyForTest(spec.GroupManaged), emulator.IAMPolicy{
				PolicyName:       "grouppolicy",
				ARN:              spec.GroupManaged,
				Path:             "/",
				DefaultVersionID: "v1",
				IsAttachable:     true,
				Document:         *spec.GroupManagedDoc,
			})
		}
	}
	if spec.GroupInline != nil {
		put(emulator.IAMInlinePolicyKeyForTest(authzTestAccount, "group", groupName, "inline"), *spec.GroupInline)
		put(emulator.IAMInlinePolicyNamesKeyForTest(authzTestAccount, "group", groupName), []string{"inline"})
	}
	if spec.UserInline != nil {
		put(emulator.IAMInlinePolicyKeyForTest(authzTestAccount, "user", userName, "inline"), *spec.UserInline)
		put(emulator.IAMInlinePolicyNamesKeyForTest(authzTestAccount, "user", userName), []string{"inline"})
	}
	return state
}

func authzDoc(effect, action, resource string) *emulator.PolicyDocument {
	return &emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   effect,
			Action:   emulator.StringOrSlice{action},
			Resource: emulator.StringOrSlice{resource},
		}},
	}
}

// TestAuthController_GroupPolicyGrantsMember is the decisive test for the lane: a
// user with no policies of their own, in a group whose managed policy is the only
// grant, is allowed — and the identical state minus the membership is denied. The
// second half is what makes the first half about groups rather than about the
// policy simply being found somewhere.
func TestAuthController_GroupPolicyGrantsMember(t *testing.T) {
	t.Parallel()

	const groupPolicyARN = "arn:aws:iam::123456789012:policy/GroupS3Read"
	grant := authzDoc(emulator.IAMEffectAllow, "s3:GetObject", "*")
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	req := &emulator.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/bucket/key"}
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/jill")

	member := emulator.NewAuthController(newAuthzGroupState(t, "jill", "devs", authzGroupState{
		GroupManaged: groupPolicyARN, GroupManagedDoc: grant,
	}), logger)
	assert.NoError(t, member.CheckAccess(reqCtx, req),
		"the group's policy is the user's only grant, so a denial means groups are not read")

	nonMember := emulator.NewAuthController(newAuthzGroupState(t, "jill", "devs", authzGroupState{
		GroupManaged: groupPolicyARN, GroupManagedDoc: grant, OmitMembership: true,
	}), logger)
	err := nonMember.CheckAccess(reqCtx, req)
	require.Error(t, err, "a user not in the group must not receive its policy")
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDenied", awsErr.Code)
}

// TestAuthController_GroupInlinePolicyGrantsMember covers the other storage a group
// policy can live in. Managed and inline are separate reads, so one working proves
// nothing about the other.
func TestAuthController_GroupInlinePolicyGrantsMember(t *testing.T) {
	t.Parallel()

	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	state := newAuthzGroupState(t, "jill", "devs", authzGroupState{
		GroupInline: authzDoc(emulator.IAMEffectAllow, "s3:PutObject", "*"),
	})
	auth := emulator.NewAuthController(state, logger)

	assert.NoError(t, auth.CheckAccess(
		newAuthTestReqCtx("arn:aws:iam::123456789012:user/jill"),
		&emulator.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/bucket/key"}))
}

// TestAuthController_GroupDenyBeatsUserAllow is IAM's rule that an explicit Deny
// wins from wherever it comes. A group Deny overriding a user Allow proves the two
// document sets are evaluated together rather than the group's being consulted only
// when the user's produces no answer.
func TestAuthController_GroupDenyBeatsUserAllow(t *testing.T) {
	t.Parallel()

	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	state := newAuthzGroupState(t, "jill", "devs", authzGroupState{
		UserInline:  authzDoc(emulator.IAMEffectAllow, "s3:DeleteObject", "*"),
		GroupInline: authzDoc(emulator.IAMEffectDeny, "s3:DeleteObject", "*"),
	})
	auth := emulator.NewAuthController(state, logger)

	err := auth.CheckAccess(
		newAuthTestReqCtx("arn:aws:iam::123456789012:user/jill"),
		&emulator.AWSRequest{Service: "s3", Operation: "DeleteObject", Path: "/bucket/key"})
	require.Error(t, err)
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDenied", awsErr.Code)
}

// TestAuthController_GroupAdministratorAccessFastPath covers the short-circuit:
// AdministratorAccess allows everything from whichever source it is attached to, and
// the fast path returned before any group was read.
func TestAuthController_GroupAdministratorAccessFastPath(t *testing.T) {
	t.Parallel()

	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	state := newAuthzGroupState(t, "jill", "admins", authzGroupState{
		GroupManaged: "arn:aws:iam::aws:policy/AdministratorAccess",
	})
	auth := emulator.NewAuthController(state, logger)

	assert.NoError(t, auth.CheckAccess(
		newAuthTestReqCtx("arn:aws:iam::123456789012:user/jill"),
		&emulator.AWSRequest{Service: "dynamodb", Operation: "PutItem", Path: "/"}))
}

// TestAuthController_RoleIgnoresGroupMemberships pins the shape of the widening: a
// role cannot belong to a group, so a stray user_groups entry under a role's name
// must not grant it anything. Reading "<kind>_groups:" unconditionally would.
func TestAuthController_RoleIgnoresGroupMemberships(t *testing.T) {
	t.Parallel()

	state := emulator.NewMemoryStateManager()
	ctx := context.Background()
	put := func(key string, value any) {
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, state.Put(ctx, "iam", key, raw))
	}
	put(emulator.IAMRoleKeyForTest(authzTestAccount, "worker"), emulator.IAMRole{
		RoleName: "worker",
		RoleID:   "AROATEST",
		ARN:      "arn:aws:iam::123456789012:role/worker",
		Path:     "/",
	})
	put(emulator.IAMGroupKeyForTest(authzTestAccount, "devs"), emulator.IAMGroup{GroupName: "devs", ARN: "arn:aws:iam::123456789012:group/devs", Path: "/"})
	put(emulator.IAMInlinePolicyKeyForTest(authzTestAccount, "group", "devs", "inline"), *authzDoc(emulator.IAMEffectAllow, "s3:GetObject", "*"))
	put(emulator.IAMInlinePolicyNamesKeyForTest(authzTestAccount, "group", "devs"), []string{"inline"})
	// A membership keyed by the role's name, which nothing writes but state could hold.
	put(emulator.IAMUserGroupsKeyForTest(authzTestAccount, "worker"), []string{"devs"})

	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
	err := auth.CheckAccess(
		newAuthTestReqCtx("arn:aws:iam::123456789012:role/worker"),
		&emulator.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/bucket/key"})
	require.Error(t, err, "a role must not pick up a group's policies")
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDenied", awsErr.Code)
}

// TestGroupPolicy_BothLoadersAgree checks the same grant through both gates at
// once. IAMPlugin.authorize loads its documents independently of AuthController, so
// both had to learn about groups: teaching only one would refuse an IAM call granted
// through a group on one path and allow it on the other — the #411 split, one ARN
// with two answers, which is the failure mode a second loader always risks.
func TestGroupPolicy_BothLoadersAgree(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		spec       authzGroupState
		wantDenied bool
	}{
		{
			name: "group inline policy allows the action",
			spec: authzGroupState{GroupInline: authzDoc(emulator.IAMEffectAllow, "iam:ListUsers", "*")},
		},
		{
			name: "group managed policy allows the action",
			spec: authzGroupState{
				GroupManaged:    "arn:aws:iam::123456789012:policy/GroupIAMRead",
				GroupManagedDoc: authzDoc(emulator.IAMEffectAllow, "iam:ListUsers", "*"),
			},
		},
		{
			name: "the same grant on a group the user has left",
			spec: authzGroupState{
				GroupInline:    authzDoc(emulator.IAMEffectAllow, "iam:ListUsers", "*"),
				OmitMembership: true,
			},
			wantDenied: true,
		},
		{
			name: "a group deny overrides the user's own allow",
			spec: authzGroupState{
				UserInline:  authzDoc(emulator.IAMEffectAllow, "iam:ListUsers", "*"),
				GroupInline: authzDoc(emulator.IAMEffectDeny, "iam:ListUsers", "*"),
			},
			wantDenied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state := newAuthzGroupState(t, "jill", "iamadmins", tt.spec)
			logger := emulator.NewDefaultLogger(slog.LevelError, false)

			plugin := &emulator.IAMPlugin{}
			require.NoError(t, plugin.Initialize(context.Background(),
				emulator.PluginConfig{State: state, Logger: logger}))
			auth := emulator.NewAuthController(state, logger)

			reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/jill")
			pluginErr := emulator.IAMAuthorizeForTest(plugin, reqCtx, "iam:ListUsers", "*")
			controllerErr := auth.CheckAccess(reqCtx,
				&emulator.AWSRequest{Service: "iam", Operation: "ListUsers", Params: map[string]string{}})

			if tt.wantDenied {
				assert.Error(t, pluginErr, "IAMPlugin.authorize should deny")
				assert.Error(t, controllerErr, "CheckAccess should deny")
				return
			}
			assert.NoError(t, pluginErr, "IAMPlugin.authorize should allow")
			assert.NoError(t, controllerErr, "CheckAccess should allow")
		})
	}
}
