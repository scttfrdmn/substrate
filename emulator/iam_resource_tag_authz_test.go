package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #804: `aws:ResourceTag/<key>` on an IAM request describes the resource the request names.
//
// Before this, [emulator.AuthController.CheckAccess] published no `aws:ResourceTag/<key>` at
// all for an entity-naming IAM request — the resolver answered the ARN with no tags — so an
// Allow conditioned on one granted nothing and, the direction that matters, a Deny conditioned
// on one silently stopped biting. For the six operations that resolve no resource, an arm in
// `resourceTagsFor` read the *caller's* record and published their tags as the resource's, so a
// caller tagged `team=platform` satisfied a condition written about an untagged entity.
//
// The tests below are one per direction, plus the two invariants the fix has to keep: the tags
// belong to the ARN they are published with, and both doors publish the same ones.

// The policies the caller's own record points at. Only the first holds the document under test;
// the other two exist to be *named by a request* so a condition can be written about their tags.
const (
	iamTagAuthzScopedPolicyARN = "arn:aws:iam::123456789012:policy/tag-scoped"
	iamTagAuthzTaggedPolicyARN = "arn:aws:iam::123456789012:policy/tagged"
	iamTagAuthzPlainPolicyARN  = "arn:aws:iam::123456789012:policy/plain"
)

// iamTagAuthzSLRService is the service principal whose service-linked role the fixture stores,
// so the resolver's SLR arm — which derives an ARN and holds no record — can be asked for tags.
const iamTagAuthzSLRService = "autoscaling.amazonaws.com"

// iamTagAuthzPlatform and iamTagAuthzProd are the tag sets the fixture stores, and what the
// assertions below expect to be published under `aws:ResourceTag/`.
var (
	iamTagAuthzPlatform = map[string]string{"team": "platform"}
	iamTagAuthzProd     = map[string]string{"team": "platform", "env": "prod"}
)

// newIAMTagAuthzState stores a tagged entity and an untagged one of every taggable IAM type,
// and attaches doc to the caller.
//
// The caller is tagged too, and that is deliberate: it is the ingredient of the false allow
// #804 reports. A condition satisfied by the caller's own `team=platform` while the named
// resource carries no tags cannot be told from a condition satisfied by the resource unless the
// caller carries the same tag, so the fixture gives them one.
func newIAMTagAuthzState(t *testing.T, doc emulator.PolicyDocument) emulator.StateManager {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	ctx := context.Background()

	put := func(key string, value any) {
		t.Helper()
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, state.Put(ctx, "iam", key, raw))
	}
	tags := func(m map[string]string) []emulator.IAMTag {
		out := make([]emulator.IAMTag, 0, len(m))
		for k, v := range m {
			out = append(out, emulator.IAMTag{Key: k, Value: v})
		}
		return out
	}

	put(emulator.IAMUserKeyForTest(iamResourceAccount, iamResourceCaller), emulator.IAMUser{
		UserName: iamResourceCaller,
		UserID:   "AIDACALLER",
		ARN:      emulator.IAMUserARNForTest(iamResourceAccount, "/", iamResourceCaller),
		Path:     "/",
		Tags:     tags(iamTagAuthzPlatform),
	})
	put(emulator.IAMUserKeyForTest(iamResourceAccount, "alice"), emulator.IAMUser{
		UserName: "alice",
		UserID:   "AIDAALICE",
		ARN:      emulator.IAMUserARNForTest(iamResourceAccount, "/", "alice"),
		Path:     "/",
		Tags:     tags(iamTagAuthzPlatform),
	})
	put(emulator.IAMUserKeyForTest(iamResourceAccount, "bob"), emulator.IAMUser{
		UserName: "bob",
		UserID:   "AIDABOB",
		ARN:      emulator.IAMUserARNForTest(iamResourceAccount, "/", "bob"),
		Path:     "/",
	})
	put(emulator.IAMRoleKeyForTest(iamResourceAccount, "prod-worker"), emulator.IAMRole{
		RoleName: "prod-worker",
		RoleID:   "AROAPROD",
		ARN:      emulator.IAMRoleARNForTest(iamResourceAccount, "/", "prod-worker"),
		Path:     "/",
		Tags:     tags(iamTagAuthzProd),
	})
	put(emulator.IAMRoleKeyForTest(iamResourceAccount, "dev-worker"), emulator.IAMRole{
		RoleName: "dev-worker",
		RoleID:   "AROADEV",
		ARN:      emulator.IAMRoleARNForTest(iamResourceAccount, "/", "dev-worker"),
		Path:     "/",
	})
	slrName := emulator.IAMSLRRoleNameForTest(iamTagAuthzSLRService, "")
	put(emulator.IAMRoleKeyForTest(iamResourceAccount, slrName), emulator.IAMRole{
		RoleName: slrName,
		RoleID:   "AROASLR",
		ARN: emulator.IAMRoleARNForTest(iamResourceAccount,
			emulator.IAMSLRPathForTest(iamTagAuthzSLRService), slrName),
		Path: emulator.IAMSLRPathForTest(iamTagAuthzSLRService),
		Tags: tags(iamTagAuthzPlatform),
	})
	put(emulator.IAMGroupKeyForTest(iamResourceAccount, "admins"), emulator.IAMGroup{
		GroupName: "admins",
		GroupID:   "AGPAADMINS",
		ARN:       emulator.IAMGroupARNForTest(iamResourceAccount, "/", "admins"),
		Path:      "/",
	})
	put(emulator.IAMInstanceProfileKeyForTest(iamResourceAccount, "app"), emulator.IAMInstanceProfile{
		InstanceProfileName: "app",
		InstanceProfileID:   "AIPAAPP",
		ARN:                 emulator.IAMInstanceProfileARNForTest(iamResourceAccount, "/", "app"),
		Path:                "/",
		Tags:                tags(iamTagAuthzPlatform),
	})
	put(emulator.IAMInstanceProfileKeyForTest(iamResourceAccount, "plain"), emulator.IAMInstanceProfile{
		InstanceProfileName: "plain",
		InstanceProfileID:   "AIPAPLAIN",
		ARN:                 emulator.IAMInstanceProfileARNForTest(iamResourceAccount, "/", "plain"),
		Path:                "/",
	})
	put(emulator.IAMPolicyKeyForTest(iamTagAuthzTaggedPolicyARN), emulator.IAMPolicy{
		PolicyName:       "tagged",
		PolicyID:         "ANPATAGGED",
		ARN:              iamTagAuthzTaggedPolicyARN,
		Path:             "/",
		DefaultVersionID: "v1",
		Tags:             tags(iamTagAuthzPlatform),
	})
	put(emulator.IAMPolicyKeyForTest(iamTagAuthzPlainPolicyARN), emulator.IAMPolicy{
		PolicyName:       "plain",
		PolicyID:         "ANPAPLAIN",
		ARN:              iamTagAuthzPlainPolicyARN,
		Path:             "/",
		DefaultVersionID: "v1",
	})

	put(emulator.IAMAttachedPoliciesKeyForTest(iamResourceAccount, "user", iamResourceCaller),
		[]string{iamTagAuthzScopedPolicyARN})
	put(emulator.IAMPolicyKeyForTest(iamTagAuthzScopedPolicyARN), emulator.IAMPolicy{
		PolicyName:       "tag-scoped",
		PolicyID:         "ANPASCOPED",
		ARN:              iamTagAuthzScopedPolicyARN,
		Path:             "/",
		DefaultVersionID: "v1",
		IsAttachable:     true,
		Document:         doc,
	})
	return state
}

// iamTagAuthzDoc builds a one-statement document conditioned on a resource tag.
func iamTagAuthzDoc(effect, action, key, value string) emulator.PolicyStatement {
	return emulator.PolicyStatement{
		Effect:   effect,
		Action:   emulator.StringOrSlice{action},
		Resource: emulator.StringOrSlice{"*"},
		Condition: map[string]map[string]emulator.StringOrSlice{
			"StringEquals": {"aws:ResourceTag/" + key: {value}},
		},
	}
}

// TestIAMResourceTag_ADenyOnAResourceTagBites is the direction #804 exists for.
//
// "Deny anything on a resource tagged env=prod" is the shape of every tag-based guardrail. With
// no `aws:ResourceTag/<key>` in the context the condition held vacuously false, so the Deny
// never applied and the Allow beneath it decided every request — a permission boundary that is
// enforced on AWS and inert here, which is the one direction substrate must not fail in.
func TestIAMResourceTag_ADenyOnAResourceTagBites(t *testing.T) {
	t.Parallel()

	state := newIAMTagAuthzState(t, emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"iam:*"},
				Resource: emulator.StringOrSlice{"*"},
			},
			iamTagAuthzDoc(emulator.IAMEffectDeny, "iam:TagRole", "env", "prod"),
		},
	})

	cases := []struct {
		name    string
		role    string
		refused bool
	}{
		{name: "the role tagged env=prod", role: "prod-worker", refused: true},
		{name: "a role that carries no tags", role: "dev-worker"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := iamResourceCheckAccess(t, state, iamResourcePrincipal(), &emulator.AWSRequest{
				Service:   "iam",
				Operation: "TagRole",
				Params:    map[string]string{"RoleName": tc.role},
			})
			if tc.refused {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

// TestIAMResourceTag_AnAllowIsGrantedOnTheMatchingResourceOnly sweeps every taggable IAM type.
//
// One statement — allow anything on a resource tagged team=platform — is put to a tagged and an
// untagged entity of each of the four types AWS documents a `Tags` member on. The tagged one is
// allowed and the untagged one refused, which is only possible if the tags published are the
// named resource's: the caller carries `team=platform` too, so a decision made from the caller's
// record would allow all eight.
//
// The policy pair is the case that has no record to read by name — a customer-managed policy is
// stored under its ARN — so it is the one that proves the ARN-keyed read, not just the path read.
func TestIAMResourceTag_AnAllowIsGrantedOnTheMatchingResourceOnly(t *testing.T) {
	t.Parallel()

	state := newIAMTagAuthzState(t, emulator.PolicyDocument{
		Version:   "2012-10-17",
		Statement: []emulator.PolicyStatement{iamTagAuthzDoc(emulator.IAMEffectAllow, "iam:*", "team", "platform")},
	})

	cases := []struct {
		name      string
		operation string
		params    map[string]string
		allowed   bool
	}{
		{"a tagged user", "GetUser", map[string]string{"UserName": "alice"}, true},
		{"an untagged user", "GetUser", map[string]string{"UserName": "bob"}, false},
		{"a tagged role", "GetRole", map[string]string{"RoleName": "prod-worker"}, true},
		{"an untagged role", "GetRole", map[string]string{"RoleName": "dev-worker"}, false},
		{"a tagged policy", "GetPolicy", map[string]string{"PolicyArn": iamTagAuthzTaggedPolicyARN}, true},
		{"an untagged policy", "GetPolicy", map[string]string{"PolicyArn": iamTagAuthzPlainPolicyARN}, false},
		{
			name:      "a tagged instance profile",
			operation: "GetInstanceProfile",
			params:    map[string]string{"InstanceProfileName": "app"},
			allowed:   true,
		},
		{
			name:      "an untagged instance profile",
			operation: "GetInstanceProfile",
			params:    map[string]string{"InstanceProfileName": "plain"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := iamResourceCheckAccess(t, state, iamResourcePrincipal(), &emulator.AWSRequest{
				Service:   "iam",
				Operation: tc.operation,
				Params:    tc.params,
			})
			if tc.allowed {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
		})
	}
}

// TestIAMResourceTag_TheCallersTagsAreNotTheResources is the false allow, from the other side.
//
// `resourceTagsFor`'s iam arm read [emulator.Principal.ARN] and published the caller's tags as
// the resource's, so a caller tagged team=platform was granted a statement written about a
// resource tagged team=platform whatever the resource actually carried — including on the six
// operations that name no resource at all, where there is no resource to carry anything. The
// caller's tags are `aws:PrincipalTag/<key>`, which has had a producer of its own since #771.
func TestIAMResourceTag_TheCallersTagsAreNotTheResources(t *testing.T) {
	t.Parallel()

	state := newIAMTagAuthzState(t, emulator.PolicyDocument{
		Version:   "2012-10-17",
		Statement: []emulator.PolicyStatement{iamTagAuthzDoc(emulator.IAMEffectAllow, "iam:*", "team", "platform")},
	})

	cases := []struct {
		name      string
		operation string
		params    map[string]string
	}{
		{
			name:      "an operation naming an untagged resource",
			operation: "GetRole",
			params:    map[string]string{"RoleName": "dev-worker"},
		},
		{
			name:      "an operation naming no resource",
			operation: "ListRoles",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := iamResourceCheckAccess(t, state, iamResourcePrincipal(), &emulator.AWSRequest{
				Service:   "iam",
				Operation: tc.operation,
				Params:    tc.params,
			})
			assert.Error(t, err, "the caller's own team=platform must not satisfy a resource-tag condition")
		})
	}
}

// TestIAMResourceTag_TheAccountWildcardCarriesNoTags pins what the six fall-through operations
// publish: nothing.
//
// Their resource is [emulator.IAMAuthzAccountResourceARNForTest] — every IAM resource in the
// account rather than one — so there is no record whose tags could describe it, and a condition
// on `aws:ResourceTag/<key>` cannot be satisfied for them. That is a real limit rather than a
// gap: AWS scopes these actions to `*` too, and the honest answer to "which resource's tags?"
// when the answer is "all of them" is none.
func TestIAMResourceTag_TheAccountWildcardCarriesNoTags(t *testing.T) {
	t.Parallel()

	state := newIAMTagAuthzState(t, emulator.PolicyDocument{Version: "2012-10-17"})
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	auth := emulator.NewAuthController(state, logger)

	plugin := &emulator.IAMPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(),
		emulator.PluginConfig{State: state, Logger: logger}))

	reqCtx := &emulator.RequestContext{
		RequestID: "req-1",
		AccountID: iamResourceAccount,
		Region:    "us-east-1",
		Principal: iamResourcePrincipal(),
		Metadata:  make(map[string]interface{}),
	}

	for _, operation := range []string{
		"ListUsers", "ListRoles", "ListGroups", "ListPolicies", "ListInstanceProfiles",
		"SimulateCustomPolicy",
	} {
		t.Run(operation, func(t *testing.T) {
			req := &emulator.AWSRequest{Service: "iam", Operation: operation}
			assert.Equal(t, []string{emulator.IAMAuthzAccountResourceARNForTest(iamResourceAccount)},
				emulator.AuthzResourceARNsForTest(auth, reqCtx, req))
			assert.Equal(t, []map[string]string{nil},
				emulator.AuthzResourceTagsForTest(auth, reqCtx, req), "the generic gate's tags")
			assert.Nil(t, emulator.IAMAuthzResourceTagsForTest(plugin, reqCtx, req), "the plugin door's tags")
		})
	}
}

// TestIAMResourceTag_BothDoorsPublishTheSameTags is #770's both-doors invariant, one condition
// key over.
//
// A tag read at one door and not the other means one policy gets two answers depending on which
// door the caller arrived at — #411, #714 and #745 are each an instance of that, and publishing
// `aws:ResourceTag/<key>` at [emulator.AuthController.CheckAccess] alone would have been the
// next one: an Allow conditioned on it would be granted at the gate and refused inside the
// handler, so the caller would see a 403 on a request their policy plainly allows.
func TestIAMResourceTag_BothDoorsPublishTheSameTags(t *testing.T) {
	t.Parallel()

	state := newIAMTagAuthzState(t, emulator.PolicyDocument{Version: "2012-10-17"})
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	auth := emulator.NewAuthController(state, logger)

	plugin := &emulator.IAMPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(),
		emulator.PluginConfig{State: state, Logger: logger}))

	reqCtx := &emulator.RequestContext{
		RequestID: "req-1",
		AccountID: iamResourceAccount,
		Region:    "us-east-1",
		Principal: iamResourcePrincipal(),
		Metadata:  make(map[string]interface{}),
	}

	cases := []struct {
		name      string
		operation string
		params    map[string]string
		want      map[string]string
	}{
		{
			name:      "a user, whose tags come from the read that recovers its path",
			operation: "GetUser",
			params:    map[string]string{"UserName": "alice"},
			want:      iamTagAuthzPlatform,
		},
		{
			name:      "a role",
			operation: "TagRole",
			params:    map[string]string{"RoleName": "prod-worker"},
			want:      iamTagAuthzProd,
		},
		{
			name:      "a policy, whose record is keyed by the ARN the request carries",
			operation: "ListPolicyTags",
			params:    map[string]string{"PolicyArn": iamTagAuthzTaggedPolicyARN},
			want:      iamTagAuthzPlatform,
		},
		{
			name:      "an instance profile",
			operation: "TagInstanceProfile",
			params:    map[string]string{"InstanceProfileName": "app"},
			want:      iamTagAuthzPlatform,
		},
		{
			// A service-linked role is a role and can be tagged like one, but its ARN is
			// derived from a service principal or a deletion-task ID rather than read, so its
			// tags are the resolver's one read by ARN rather than a read it already did.
			name:      "a service-linked role, whose ARN is derived rather than read",
			operation: "DeleteServiceLinkedRole",
			params: map[string]string{
				"RoleName": emulator.IAMSLRRoleNameForTest(iamTagAuthzSLRService, ""),
			},
			want: iamTagAuthzPlatform,
		},
		{
			// A group is not a taggable IAM resource: the `Group` data type documents no
			// `Tags` member, and the User Guide says it outright — "You can tag most IAM
			// resources, but not groups, assumed roles, access reports, or hardware-based MFA
			// devices." So a condition on aws:ResourceTag/<key> cannot be satisfied for a
			// group operation, here or on AWS.
			name:      "a group, which AWS does not let a caller tag",
			operation: "GetGroup",
			params:    map[string]string{"GroupName": "admins"},
		},
		{
			// SimulatePrincipalPolicy's PolicySourceArn may name a user, a role or a group,
			// so the row publishes no resource type and the ARN is passed through. A group
			// ARN therefore reaches the by-ARN read, which answers no tags for the reason
			// above.
			name:      "a group named by a finished ARN",
			operation: "SimulatePrincipalPolicy",
			params: map[string]string{
				"PolicySourceArn": emulator.IAMGroupARNForTest(iamResourceAccount, "/", "admins"),
			},
		},
		{
			// The same row given a name where it expects an ARN. There is no resource type to
			// mint from, so nothing is built and nothing is read.
			name:      "a policy source that is not an ARN at all",
			operation: "SimulatePrincipalPolicy",
			params:    map[string]string{"PolicySourceArn": "admins"},
		},
		{
			name:      "an entity that does not exist",
			operation: "GetRole",
			params:    map[string]string{"RoleName": "absent"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &emulator.AWSRequest{Service: "iam", Operation: tc.operation, Params: tc.params}
			gate := emulator.AuthzResourceTagsForTest(auth, reqCtx, req)
			door := emulator.IAMAuthzResourceTagsForTest(plugin, reqCtx, req)

			if tc.want == nil {
				assert.Equal(t, []map[string]string{nil}, gate, "the generic gate's tags")
				assert.Nil(t, door, "the plugin door's tags")
				return
			}
			assert.Equal(t, []map[string]string{tc.want}, gate, "the generic gate's tags")
			assert.Equal(t, tc.want, door, "the plugin door's tags")
		})
	}
}

// TestIAMResourceTag_AGateWithNoStateManagerPublishesNoTags is the defensive half.
//
// [emulator.AuthController] holds a [emulator.StateManager] that a caller may not have wired,
// and a decision must not panic for it. Answering no tags is also the safe direction: a
// condition on `aws:ResourceTag/<key>` that cannot be read goes unsatisfied, so an Allow grants
// nothing rather than a Deny failing to bite.
func TestIAMResourceTag_AGateWithNoStateManagerPublishesNoTags(t *testing.T) {
	t.Parallel()

	auth := emulator.NewAuthController(nil, emulator.NewDefaultLogger(slog.LevelError, false))
	reqCtx := &emulator.RequestContext{
		RequestID: "req-1",
		AccountID: iamResourceAccount,
		Region:    "us-east-1",
		Principal: iamResourcePrincipal(),
		Metadata:  make(map[string]interface{}),
	}

	for _, tc := range []struct {
		name   string
		params map[string]string
		req    *emulator.AWSRequest
	}{
		{
			name: "an entity named by name",
			req: &emulator.AWSRequest{
				Service: "iam", Operation: "GetRole",
				Params: map[string]string{"RoleName": "prod-worker"},
			},
		},
		{
			name: "a policy named by ARN",
			req: &emulator.AWSRequest{
				Service: "iam", Operation: "GetPolicy",
				Params: map[string]string{"PolicyArn": iamTagAuthzTaggedPolicyARN},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, []map[string]string{nil},
				emulator.AuthzResourceTagsForTest(auth, reqCtx, tc.req))
		})
	}
}

// TestIAMResourceTag_ThePluginDoorHonoursAResourceTagCondition is the same fix seen from a
// request rather than from the resolver.
//
// The server here wires no [emulator.AuthController], so every verdict is the plugin door's own
// — the door that published only the caller's keys before #804, and would therefore have
// refused a request the generic gate allowed.
func TestIAMResourceTag_ThePluginDoorHonoursAResourceTagCondition(t *testing.T) {
	srv := newIAMPluginDoorServer(t)

	for _, role := range []string{"prod-worker", "dev-worker"} {
		require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "CreateRole",
			map[string]any{"RoleName": role}).StatusCode)
	}
	require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "TagRole", map[string]any{
		"RoleName": "prod-worker",
		"Tags":     []map[string]string{{"Key": "env", "Value": "prod"}},
	}).StatusCode)

	keyID := trustSetupCaller(t, srv, "alice")
	require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "PutUserPolicy", map[string]any{
		"UserName":   "alice",
		"PolicyName": "prod-roles-only",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Action":"iam:GetRole","Resource":"*",` +
			`"Condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}}}]}`,
	}).StatusCode)

	call := func(t *testing.T, roleName string) int {
		t.Helper()
		raw, err := json.Marshal(map[string]any{"RoleName": roleName})
		require.NoError(t, err)
		r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
		r.Host = "iam.amazonaws.com"
		r.Header.Set("X-Amz-Target", "AmazonIdentityManagementService.GetRole")
		r.Header.Set("Content-Type", "application/x-amz-json-1.1")
		r.Header.Set("Authorization", trustAuthHeader(keyID, "iam"))
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		resp := w.Result()
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode
	}

	assert.Equal(t, http.StatusOK, call(t, "prod-worker"),
		"the role tagged env=prod, which the caller's statement is written about")
	assert.Equal(t, http.StatusForbidden, call(t, "dev-worker"),
		"a role carrying no tags, which the same statement must not reach")
}
