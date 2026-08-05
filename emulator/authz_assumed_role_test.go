package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// An assumed-role principal used to be allowed with no policies at all:
// loadPoliciesForPrincipal errored on any entity type outside user/role and
// CheckAccess failed open on that error. IAMPlugin.authorize denied in the same
// case. One ARN, two loaders, two opposite answers — and the shape STS itself
// mints, so every "call as a role" test was unenforced while looking enforced.

// newRoleAuthState builds a state manager holding a role, optionally with an
// attached policy and a permissions boundary.
func newRoleAuthState(t *testing.T, roleName string, doc *emulator.PolicyDocument, boundary *emulator.PolicyDocument) emulator.StateManager {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	ctx := context.Background()

	role := emulator.IAMRole{
		RoleName: roleName,
		RoleID:   "AROATEST",
		ARN:      "arn:aws:iam::123456789012:role/" + roleName,
		Path:     "/",
	}
	if boundary != nil {
		boundaryARN := "arn:aws:iam::123456789012:policy/Boundary"
		role.PermissionsBoundary = &emulator.IAMAttachedPolicy{
			PolicyARN:  boundaryARN,
			PolicyName: "Boundary",
		}
		polRaw, err := json.Marshal(emulator.IAMPolicy{
			PolicyName: "Boundary",
			ARN:        boundaryARN,
			Document:   *boundary,
		})
		require.NoError(t, err)
		require.NoError(t, state.Put(ctx, "iam", "policy:"+boundaryARN, polRaw))
	}

	roleRaw, err := json.Marshal(role)
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", "role:"+roleName, roleRaw))

	if doc != nil {
		policyARN := "arn:aws:iam::123456789012:policy/RolePolicy"
		arnsRaw, marshalErr := json.Marshal([]string{policyARN})
		require.NoError(t, marshalErr)
		require.NoError(t, state.Put(ctx, "iam", "role_policies:"+roleName, arnsRaw))
		polRaw, marshalErr := json.Marshal(emulator.IAMPolicy{
			PolicyName: "RolePolicy",
			ARN:        policyARN,
			Document:   *doc,
		})
		require.NoError(t, marshalErr)
		require.NoError(t, state.Put(ctx, "iam", "policy:"+policyARN, polRaw))
	}

	return state
}

// allowSQSListQueues is a policy allowing exactly one action.
func allowSQSListQueues() *emulator.PolicyDocument {
	return &emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   "Allow",
			Action:   emulator.StringOrSlice{"sqs:ListQueues"},
			Resource: emulator.StringOrSlice{"*"},
		}},
	}
}

func TestAuthController_AssumedRole(t *testing.T) {
	const sessionARN = "arn:aws:sts::123456789012:assumed-role/worker/sess1"

	tests := []struct {
		name        string
		principal   string
		roleName    string
		policy      *emulator.PolicyDocument
		boundary    *emulator.PolicyDocument
		operation   string
		wantDenied  bool
		wantMessage string
	}{
		{
			// The fail-open that started this. The role exists and holds no policy,
			// so the session is an implicit deny — as it is on AWS.
			name:       "role with no policy is denied",
			principal:  sessionARN,
			roleName:   "worker",
			operation:  "ListQueues",
			wantDenied: true,
		},
		{
			// Resolution has to drop the session name: parsePrincipalARN splits on
			// the first slash, so without that the lookup is
			// role_policies:worker/sess1 and every session of the same role is a
			// distinct nameless entity.
			name:      "role policy applies to the session",
			principal: sessionARN,
			roleName:  "worker",
			policy:    allowSQSListQueues(),
			operation: "ListQueues",
		},
		{
			name:       "role policy does not cover another action",
			principal:  sessionARN,
			roleName:   "worker",
			policy:     allowSQSListQueues(),
			operation:  "CreateQueue",
			wantDenied: true,
		},
		{
			// A session name may itself contain slashes or dots; only the first
			// segment is the role.
			name:      "a dotted session name still resolves to the role",
			principal: "arn:aws:sts::123456789012:assumed-role/worker/i-0abc.session",
			roleName:  "worker",
			policy:    allowSQSListQueues(),
			operation: "ListQueues",
		},
		{
			// A boundary must keep applying to exactly the principals that now
			// enforce, else switching enforcement on switches boundaries off.
			name:        "a permission boundary applies through the role",
			principal:   sessionARN,
			roleName:    "worker",
			policy:      allowSQSListQueues(),
			boundary:    &emulator.PolicyDocument{Version: "2012-10-17"},
			operation:   "ListQueues",
			wantDenied:  true,
			wantMessage: "blocked by permission boundary",
		},
		{
			// The opt-in: the session names a role that was never created, so
			// nothing is enforced. This is what keeps every existing caller green.
			name:      "a session whose role does not exist is not enforced",
			principal: "arn:aws:sts::123456789012:assumed-role/ghost/sess1",
			roleName:  "worker",
			operation: "ListQueues",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := newRoleAuthState(t, tt.roleName, tt.policy, tt.boundary)
			auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))

			reqCtx := &emulator.RequestContext{
				RequestID: "req-1",
				AccountID: "123456789012",
				Region:    "us-east-1",
				Principal: &emulator.Principal{ARN: tt.principal, Type: "AssumedRole"},
				Metadata:  make(map[string]interface{}),
			}
			req := &emulator.AWSRequest{Service: "sqs", Operation: tt.operation, Params: map[string]string{}}

			err := auth.CheckAccess(reqCtx, req)
			if !tt.wantDenied {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			assert.Equal(t, "AccessDeniedException", awsErr.Code)
			assert.Equal(t, 403, awsErr.HTTPStatus)
			if tt.wantMessage != "" {
				assert.Contains(t, awsErr.Message, tt.wantMessage)
			}
		})
	}
}

func TestAuthController_ExistenceIsTheOptIn(t *testing.T) {
	// Enforcement keys off whether the principal resolves to an IAM entity that
	// exists. A user that does exist and holds no policies is an implicit deny —
	// real IAM behavior, and the whole point. A principal naming nothing in state
	// is not enforced, which is what keeps the thousands of existing calls made
	// with a credential that never touched IAM passing.
	tests := []struct {
		name       string
		principal  string
		wantDenied bool
	}{
		{name: "user that exists", principal: "arn:aws:iam::123456789012:user/alice", wantDenied: true},
		{name: "user that does not exist", principal: "arn:aws:iam::123456789012:user/nobody"},
		{name: "role that does not exist", principal: "arn:aws:iam::123456789012:role/nosuchrole"},
		// A service principal, the account root, and a federated user hold no
		// policies substrate models. Each was previously an "unsupported entity
		// type" error that CheckAccess failed open on and IAMPlugin.authorize
		// denied; both now agree they are simply not enforced.
		{name: "account root", principal: "arn:aws:iam::123456789012:root"},
		{name: "service principal", principal: "cloudformation.amazonaws.com"},
		{name: "federated user", principal: "arn:aws:sts::123456789012:federated-user/dave"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := newAuthTestState(t, "alice", "", emulator.PolicyDocument{})
			auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))

			reqCtx := &emulator.RequestContext{
				RequestID: "req-1",
				AccountID: "123456789012",
				Region:    "us-east-1",
				Principal: &emulator.Principal{ARN: tt.principal, Type: "IAMUser"},
				Metadata:  make(map[string]interface{}),
			}
			req := &emulator.AWSRequest{Service: "sqs", Operation: "ListQueues", Params: map[string]string{}}

			err := auth.CheckAccess(reqCtx, req)
			if tt.wantDenied {
				require.Error(t, err)
				var awsErr *emulator.AWSError
				require.ErrorAs(t, err, &awsErr)
				assert.Equal(t, "AccessDeniedException", awsErr.Code)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestAuthController_StateFailureFailsOpen(t *testing.T) {
	// The one case that must stay loud and permissive. "I cannot reach the store"
	// and "this caller is nobody" both used to be the same error, and both allowed;
	// they are now distinguished, and the distinction is only worth anything if the
	// I/O arm keeps failing open. Denying on a storage blip would report a broken
	// backend as AccessDeniedException — a permanent-looking signal for a transient
	// fault, which is the wrong path to send a consumer's retry loop down.
	state := &errAfterGetsStateManager{
		inner:  newRoleAuthState(t, "worker", allowSQSListQueues(), nil),
		getErr: errors.New("state store unavailable"),
		allow:  0, // every Get fails, so entity resolution itself errors
	}
	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))

	reqCtx := &emulator.RequestContext{
		RequestID: "req-1",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Principal: &emulator.Principal{
			ARN:  "arn:aws:sts::123456789012:assumed-role/worker/sess1",
			Type: "AssumedRole",
		},
		Metadata: make(map[string]interface{}),
	}
	req := &emulator.AWSRequest{Service: "sqs", Operation: "ListQueues", Params: map[string]string{}}

	assert.NoError(t, auth.CheckAccess(reqCtx, req),
		"a state-store failure must fail open, not deny")
}

func TestIAMAuthorize_AgreesWithCheckAccess(t *testing.T) {
	// The asymmetry this closes: IAMPlugin.authorize denied an entity type it did
	// not handle where CheckAccess allowed it, so the same ARN got opposite answers
	// depending on which service it called. Both now route through one resolver.
	tests := []struct {
		name       string
		principal  string
		policy     *emulator.PolicyDocument
		wantDenied bool
	}{
		{
			name:       "assumed-role with no role policy",
			principal:  "arn:aws:sts::123456789012:assumed-role/worker/sess1",
			wantDenied: true,
		},
		{
			name:      "assumed-role with a role policy allowing the action",
			principal: "arn:aws:sts::123456789012:assumed-role/worker/sess1",
			policy: &emulator.PolicyDocument{
				Version: "2012-10-17",
				Statement: []emulator.PolicyStatement{{
					Effect:   "Allow",
					Action:   emulator.StringOrSlice{"iam:ListUsers"},
					Resource: emulator.StringOrSlice{"*"},
				}},
			},
		},
		{
			name:      "a principal that resolves to nothing is not enforced",
			principal: "arn:aws:iam::123456789012:root",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := newRoleAuthState(t, "worker", tt.policy, nil)
			logger := emulator.NewDefaultLogger(slog.LevelError, false)

			plugin := &emulator.IAMPlugin{}
			require.NoError(t, plugin.Initialize(context.Background(),
				emulator.PluginConfig{State: state, Logger: logger}))
			auth := emulator.NewAuthController(state, logger)

			reqCtx := &emulator.RequestContext{
				RequestID: "req-1",
				AccountID: "123456789012",
				Region:    "us-east-1",
				Principal: &emulator.Principal{ARN: tt.principal, Type: "AssumedRole"},
				Metadata:  make(map[string]interface{}),
			}

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
