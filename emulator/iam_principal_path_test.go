package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The caller substrate could not find (#801).
//
// An IAM entity ARN puts the path and the friendly name in one component —
// arn:${Partition}:iam::${Account}:user/${UserNameWithPath}, from AWS's identifiers
// reference — so the name is its *last* segment. Substrate read the whole component as the
// name, which made `user/division/engineering/alice` resolve to a nonexistent entity called
// "division/engineering/alice": no policies, no existence, and therefore — by the
// existence-is-the-opt-in rule — no enforcement at all.
//
// Two defects masked each other, which is why both move in one change. The parse above is
// one; the other is that [resolvePrincipal] built a signed caller's ARN *without* the path,
// so a long-term IAM-user credential was enforced by accident while reporting an ARN that
// named no entity. Fixing only the parse leaves `aws:PrincipalArn` and GetCallerIdentity
// wrong; fixing only the ARN turns that accident into the false allow.
//
// Two callers reached the false allow without any accident, and both are covered below: a
// CloudFormation stack whose service role is at `/service-role/` — where AWS's own console
// creates one — had every resource call unenforced, and `sts:AssumeRole` on a role at any
// path answered NoSuchEntity for a perfectly valid ARN.

// iamPathTestPath is the path these cases store their entities at. Two segments, because a
// one-segment path would pass a resolver that dropped only the last.
const iamPathTestPath = "/division/engineering/"

// iamPathServiceRolePath is where AWS's console creates a CloudFormation service role.
const iamPathServiceRolePath = "/service-role/"

// newIAMPathUserState seeds one user at [iamPathTestPath], optionally tagged and optionally
// holding one attached policy, and returns the state plus the ARN a resolved principal now
// carries.
func newIAMPathUserState(t *testing.T, userName string, tags []emulator.IAMTag,
	doc *emulator.PolicyDocument) (emulator.StateManager, string) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	ctx := context.Background()

	put := func(key string, value any) {
		t.Helper()
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, state.Put(ctx, "iam", key, raw))
	}

	arn := emulator.IAMUserARNForTest(authzTestAccount, iamPathTestPath, userName)
	put(emulator.IAMUserKeyForTest(authzTestAccount, userName), emulator.IAMUser{
		UserName: userName,
		UserID:   "AIDAPATHFULCALLER123",
		ARN:      arn,
		Path:     iamPathTestPath,
		Tags:     tags,
	})

	if doc != nil {
		const policyARN = "arn:aws:iam::" + authzTestAccount + ":policy/PathfulCaller"
		put(emulator.IAMAttachedPoliciesKeyForTest(authzTestAccount, "user", userName),
			[]string{policyARN})
		put(emulator.IAMPolicyKeyForTest(policyARN), emulator.IAMPolicy{
			PolicyName:       "PathfulCaller",
			PolicyID:         "ANPAPATHFUL",
			ARN:              policyARN,
			Path:             "/",
			DefaultVersionID: "v1",
			IsAttachable:     true,
			Document:         *doc,
		})
	}
	return state, arn
}

// iamPathPrincipal is the pathful caller as a long-term IAM-user credential now resolves
// them: the real ARN, with the friendly name recorded beside it.
func iamPathPrincipal(userName, arn string, tags map[string]string) *emulator.Principal {
	return &emulator.Principal{
		ARN:      arn,
		Type:     "IAMUser",
		UserName: userName,
		UserID:   "AIDAPATHFULCALLER123",
		Tags:     tags,
	}
}

// allowOneAction is a policy granting exactly one action on everything.
func allowOneAction(action string) *emulator.PolicyDocument {
	return &emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   emulator.IAMEffectAllow,
			Action:   emulator.StringOrSlice{action},
			Resource: emulator.StringOrSlice{"*"},
		}},
	}
}

func TestServer_ResolvePrincipal_TheARNCarriesTheUsersPath(t *testing.T) {
	// End to end, because the two halves of the defect are in different files: the
	// server resolves the credential to a principal, and the ARN it reports has to be
	// the one IAM stored — the string `aws:PrincipalArn` conditions and
	// GetCallerIdentity both publish.
	srv, capture := newPrincipalTestServer(t)

	require.Equal(t, http.StatusOK, principalIAMCall(t, srv, "CreateUser", map[string]any{
		"UserName": "alice",
		"Path":     iamPathTestPath,
	}).StatusCode)

	resp := principalIAMCall(t, srv, "CreateAccessKey", map[string]any{"UserName": "alice"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var created struct {
		AccessKeyID string `xml:"CreateAccessKeyResult>AccessKey>AccessKeyId"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&created))
	require.NoError(t, resp.Body.Close())
	require.NotEmpty(t, created.AccessKeyID)

	// IAM's own answer for the same user, so the assertion is not the resolver agreeing
	// with itself.
	stored := principalIAMCall(t, srv, "GetUser", map[string]any{"UserName": "alice"})
	require.Equal(t, http.StatusOK, stored.StatusCode)
	var got struct {
		ARN string `xml:"GetUserResult>User>Arn"`
	}
	require.NoError(t, xml.NewDecoder(stored.Body).Decode(&got))
	require.NoError(t, stored.Body.Close())
	require.Equal(t, "arn:aws:iam::123456789012:user/division/engineering/alice", got.ARN)

	call := principalDynamoCall(t, srv, created.AccessKeyID)
	require.NoError(t, call.Body.Close())
	require.Equal(t, http.StatusOK, call.StatusCode)

	require.NotNil(t, capture.principal)
	assert.Equal(t, got.ARN, capture.principal.ARN,
		"the principal's ARN is the entity's own, path and all")
	// And the name stays the friendly one: it is recorded on the access key rather than
	// re-derived from the ARN, which is what keeps `aws:username` at "alice" (#745).
	assert.Equal(t, "alice", capture.principal.UserName)
}

func TestResolvePrincipal_AMissingUserRecordKeepsThePathlessARN(t *testing.T) {
	// The fallback, and the reason reading the path cannot be required: an access key
	// whose user has been deleted, or one written by a version that stored no path,
	// still identifies a principal. normalisePath renders the empty path as a single
	// slash, so the ARN is the exact string this line produced before #801.
	state := emulator.NewMemoryStateManager()
	keyRaw, err := json.Marshal(emulator.IAMAccessKey{
		AccessKeyID: "AKIANOUSERRECORD0001",
		Status:      "Active",
		UserName:    "ghost",
		AccountID:   authzTestAccount,
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(context.Background(), "iam",
		emulator.IAMAccessKeyKeyForTest("AKIANOUSERRECORD0001"), keyRaw))

	principal, _ := emulator.ResolvePrincipalForTest(state, authzTestAccount, "AKIANOUSERRECORD0001")
	require.NotNil(t, principal)
	assert.Equal(t, "arn:aws:iam::123456789012:user/ghost", principal.ARN)
	assert.Equal(t, "ghost", principal.UserName)
	assert.Empty(t, principal.Tags)
}

func TestCheckAccess_ACallerAtAPathIsEnforced(t *testing.T) {
	// #801's headline: every one of these was **allowed** before, because the caller
	// resolved to no entity and a principal that resolves to nothing is not enforced.
	// The deny rows are the ones that matter — a policy that refuses is the whole
	// product, and it was refusing nobody.
	tests := []struct {
		name       string
		policy     *emulator.PolicyDocument
		operation  string
		wantDenied bool
	}{
		{
			// The "forgot to attach the policy" case, which on AWS is an implicit deny.
			name:       "a pathful user with no policy at all",
			operation:  "ListQueues",
			wantDenied: true,
		},
		{
			name:      "a pathful user whose policy allows the action",
			policy:    allowOneAction("sqs:ListQueues"),
			operation: "ListQueues",
		},
		{
			name:       "a pathful user whose policy does not cover the action",
			policy:     allowOneAction("sqs:ListQueues"),
			operation:  "CreateQueue",
			wantDenied: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, arn := newIAMPathUserState(t, "alice", nil, tt.policy)
			auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))

			err := auth.CheckAccess(&emulator.RequestContext{
				RequestID: "req-1",
				AccountID: authzTestAccount,
				Region:    "us-east-1",
				Principal: iamPathPrincipal("alice", arn, nil),
				Metadata:  make(map[string]interface{}),
			}, &emulator.AWSRequest{Service: "sqs", Operation: tt.operation, Params: map[string]string{}})

			if !tt.wantDenied {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			assert.Equal(t, "AccessDeniedException", awsErr.Code)
		})
	}
}

func TestBothDoorsAgreeOnACallerAtAPath(t *testing.T) {
	// One caller, two gates, one answer — #411's rule applied to the pathful caller.
	// Both doors resolve the principal through iamEntityForPrincipalARN, so this asserts
	// that the one fix reached both rather than that two agree by coincidence.
	tests := []struct {
		name       string
		policy     *emulator.PolicyDocument
		wantDenied bool
	}{
		{name: "the action the policy allows", policy: allowOneAction("iam:ListUsers")},
		{name: "no policy at all", wantDenied: true},
		{name: "a policy covering another action", policy: allowOneAction("iam:GetUser"), wantDenied: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, arn := newIAMPathUserState(t, "alice", nil, tt.policy)
			logger := emulator.NewDefaultLogger(slog.LevelError, false)
			auth := emulator.NewAuthController(state, logger)
			plugin := &emulator.IAMPlugin{}
			require.NoError(t, plugin.Initialize(context.Background(),
				emulator.PluginConfig{State: state, Logger: logger}))

			reqCtx := &emulator.RequestContext{
				RequestID: "req-1",
				AccountID: authzTestAccount,
				Region:    "us-east-1",
				Principal: iamPathPrincipal("alice", arn, nil),
				Metadata:  make(map[string]interface{}),
			}

			gateErr := auth.CheckAccess(reqCtx,
				&emulator.AWSRequest{Service: "iam", Operation: "ListUsers", Params: map[string]string{}})
			doorErr := emulator.IAMAuthorizeForTest(plugin, reqCtx, "iam:ListUsers", "*")

			if tt.wantDenied {
				assert.Error(t, gateErr, "CheckAccess should deny")
				assert.Error(t, doorErr, "IAMPlugin.authorize should deny")
				return
			}
			assert.NoError(t, gateErr, "CheckAccess should allow")
			assert.NoError(t, doorErr, "IAMPlugin.authorize should allow")
		})
	}
}

func TestIAMPrincipalTags_ACallerAtAPathPublishesItsTags(t *testing.T) {
	// The tag reader looks the entity up by the same name the policy loader does, so it
	// was broken in exactly the same way: a pathful caller published no
	// `aws:PrincipalTag/<key>` at all, which for a negated operator is a false allow
	// (#771's direction, reached through #801's parse).
	state, arn := newIAMPathUserState(t, "alice",
		[]emulator.IAMTag{{Key: "team", Value: "blue"}}, nil)

	assert.Equal(t, map[string]string{"team": "blue"}, emulator.IAMPrincipalTagsForTest(state, arn))

	// And the condition context built from the resolved principal carries it, which is
	// what a policy actually reads.
	got := emulator.AuthzPrincipalContextForTest(iamPathPrincipal("alice", arn,
		map[string]string{"team": "blue"}))
	assert.Equal(t, "blue", got["aws:PrincipalTag/team"])
	assert.Equal(t, arn, got["aws:PrincipalArn"],
		"the condition key publishes the entity's real ARN")
}

func TestCheckAccess_APathfulCallersOwnResourceARNCarriesThePath(t *testing.T) {
	// The interaction worth stating, because it looks like a regression and is not: the
	// caller's own resource ARN carries their path, so a statement scoped to
	// `user/${aws:username}` does not match a user at /division/engineering/. AWS behaves
	// the same way, which is why its own IAMUserChangePassword names *two* resources —
	// the second, `user/*/${aws:username}`, exists precisely for a user at a path.
	tests := []struct {
		name     string
		policy   *emulator.PolicyDocument
		attached string
		allowed  bool
	}{
		{
			name:     "the bundled IAMUserChangePassword, whose second resource covers a path",
			attached: "arn:aws:iam::aws:policy/IAMUserChangePassword",
			allowed:  true,
		},
		{
			name: "a hand-written policy naming only the default-path form",
			policy: &emulator.PolicyDocument{
				Version: "2012-10-17",
				Statement: []emulator.PolicyStatement{{
					Effect:   emulator.IAMEffectAllow,
					Action:   emulator.StringOrSlice{"iam:ChangePassword"},
					Resource: emulator.StringOrSlice{"arn:aws:iam::*:user/${aws:username}"},
				}},
			},
			allowed: false,
		},
		{
			name: "the same policy written for the path the user is stored at",
			policy: &emulator.PolicyDocument{
				Version: "2012-10-17",
				Statement: []emulator.PolicyStatement{{
					Effect: emulator.IAMEffectAllow,
					Action: emulator.StringOrSlice{"iam:ChangePassword"},
					Resource: emulator.StringOrSlice{
						"arn:aws:iam::*:user" + iamPathTestPath + "${aws:username}",
					},
				}},
			},
			allowed: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, arn := newIAMPathUserState(t, "alice", nil, tt.policy)
			if tt.attached != "" {
				raw, err := json.Marshal([]string{tt.attached})
				require.NoError(t, err)
				require.NoError(t, state.Put(context.Background(), "iam",
					emulator.IAMAttachedPoliciesKeyForTest(authzTestAccount, "user", "alice"), raw))
			}
			auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))

			err := auth.CheckAccess(&emulator.RequestContext{
				RequestID: "req-1",
				AccountID: authzTestAccount,
				Region:    "us-east-1",
				Principal: iamPathPrincipal("alice", arn, nil),
				Metadata:  make(map[string]interface{}),
			}, &emulator.AWSRequest{Service: "iam", Operation: "ChangePassword", Params: map[string]string{}})

			if tt.allowed {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
		})
	}
}

func TestSTS_AssumeRole_ARoleAtAPathCanBeAssumed(t *testing.T) {
	// The hard failure #801 caused: the role's own ARN was rejected as naming no role, so
	// a stack, a CDK app or a script that assumed a `/service-role/` role could not run
	// at all. Nothing about the ARN was wrong — substrate read past the path and looked up
	// a role called "service-role/CfnRole".
	srv, capture := newPrincipalTestServer(t)

	require.Equal(t, http.StatusOK, principalIAMCall(t, srv, "CreateRole", map[string]any{
		"RoleName": "CfnRole",
		"Path":     iamPathServiceRolePath,
		"Tags":     []map[string]string{{"Key": "team", "Value": "blue"}},
	}).StatusCode)

	roleARN := "arn:aws:iam::123456789012:role" + iamPathServiceRolePath + "CfnRole"
	r := httptest.NewRequest(http.MethodPost,
		"/?Action=AssumeRole&RoleArn="+roleARN+"&RoleSessionName=sess1", nil)
	r.Host = "sts.amazonaws.com"
	r.Header.Set("Authorization", principalAuthHeader("AKIAIOSFODNN7EXAMPLE", "sts"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	var assumed struct {
		AccessKeyID   string `xml:"AssumeRoleResult>Credentials>AccessKeyId"`
		AssumedRoleID string `xml:"AssumeRoleResult>AssumedRoleUser>AssumedRoleId"`
		ARN           string `xml:"AssumeRoleResult>AssumedRoleUser>Arn"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &assumed))
	// AWS's assumed-role ARN format is
	// arn:aws:sts::${Account}:assumed-role/${RoleName}/${RoleSessionName} — two segments
	// that exclude the role's path. So the session names the friendly role name even
	// though the role it came from lives at a path.
	assert.Equal(t, "arn:aws:sts::123456789012:assumed-role/CfnRole/sess1", assumed.ARN)
	require.True(t, strings.HasSuffix(assumed.AssumedRoleID, ":sess1"),
		"AssumedRoleId was %q", assumed.AssumedRoleID)

	// And the session resolves back to the role behind it, so the role's tags reach the
	// decision — the assumed-role ARN's *first* segment is the role name, which is the
	// reading the user/role arm deliberately does not share.
	require.NotEmpty(t, assumed.AccessKeyID)
	resp := principalDynamoCall(t, srv, assumed.AccessKeyID)
	require.NoError(t, resp.Body.Close())
	require.NotNil(t, capture.principal)
	assert.Equal(t, map[string]string{"team": "blue"}, capture.principal.Tags)
}

func TestCheckAccess_AnAssumedRoleSessionFindsARoleAtAPath(t *testing.T) {
	// The same unwrapping on the authorization side, pinned: the session ARN carries no
	// path, the role record does, and the policies must still be found. A resolver that
	// took the last segment of an assumed-role ARN would look up a role named "sess1".
	state := emulator.NewMemoryStateManager()
	cfnSeedRoleAtPath(t, state, iamPathServiceRolePath, "CfnRole", []string{"sqs:ListQueues"})

	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
	reqCtx := func() *emulator.RequestContext {
		return &emulator.RequestContext{
			RequestID: "req-1",
			AccountID: authzTestAccount,
			Region:    "us-east-1",
			Principal: &emulator.Principal{
				ARN:  "arn:aws:sts::123456789012:assumed-role/CfnRole/sess1",
				Type: "AssumedRole",
			},
			Metadata: make(map[string]interface{}),
		}
	}

	assert.NoError(t, auth.CheckAccess(reqCtx(),
		&emulator.AWSRequest{Service: "sqs", Operation: "ListQueues", Params: map[string]string{}}),
		"the role's policy applies to the session")
	assert.Error(t, auth.CheckAccess(reqCtx(),
		&emulator.AWSRequest{Service: "sqs", Operation: "CreateQueue", Params: map[string]string{}}),
		"and it does not cover an action the policy omits")
}

func TestGetCallerIdentity_ReportsTheEntitysRealARN(t *testing.T) {
	// GetCallerIdentity is how a consumer — and every "am I who I think I am" check in a
	// CDK app — reads the caller back. It publishes Principal.ARN verbatim, so a pathful
	// caller reported an ARN that named no entity until the resolver carried the path.
	srv, _ := newPrincipalTestServer(t)

	require.Equal(t, http.StatusOK, principalIAMCall(t, srv, "CreateUser", map[string]any{
		"UserName": "alice",
		"Path":     iamPathTestPath,
	}).StatusCode)
	resp := principalIAMCall(t, srv, "CreateAccessKey", map[string]any{"UserName": "alice"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var created struct {
		AccessKeyID string `xml:"CreateAccessKeyResult>AccessKey>AccessKeyId"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&created))
	require.NoError(t, resp.Body.Close())

	r := httptest.NewRequest(http.MethodPost, "/?Action=GetCallerIdentity", nil)
	r.Host = "sts.amazonaws.com"
	r.Header.Set("Authorization", principalAuthHeader(created.AccessKeyID, "sts"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	var identity struct {
		UserID  string `xml:"GetCallerIdentityResult>UserId"`
		Account string `xml:"GetCallerIdentityResult>Account"`
		ARN     string `xml:"GetCallerIdentityResult>Arn"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &identity))
	assert.Equal(t, "arn:aws:iam::123456789012:user/division/engineering/alice", identity.ARN)
	assert.Equal(t, "123456789012", identity.Account)
	// UserId is asserted only as "not the name-with-path": reporting a name here at all
	// is a divergence from AWS, which documents the unique ID, and is tracked as #805.
	assert.NotContains(t, identity.UserID, "/")
}

func TestIAMEntityForPrincipalARN_WhatEachARNFormResolvesTo(t *testing.T) {
	// The parse itself, pinned in both directions, because a decision test cannot see the
	// difference: an ARN that resolves to nothing is *allowed*, exactly like an ARN that
	// resolves to an entity a policy permits. That indistinguishability is what let #801
	// live.
	tests := []struct {
		name     string
		arn      string
		wantKind string
		wantName string
		wantOK   bool
	}{
		{
			name:     "a user at the default path",
			arn:      "arn:aws:iam::123456789012:user/alice",
			wantKind: "user",
			wantName: "alice",
			wantOK:   true,
		},
		{
			// The friendly name is the ARN's last segment, per AWS's
			// arn:${Partition}:iam::${Account}:user/${UserNameWithPath}. Reading the whole
			// component gave "division/engineering/alice", which is stored nowhere.
			name:     "a user at a path is the last segment",
			arn:      "arn:aws:iam::123456789012:user/division/engineering/alice",
			wantKind: "user",
			wantName: "alice",
			wantOK:   true,
		},
		{
			name:     "a role at the path AWS's console uses for a service role",
			arn:      "arn:aws:iam::123456789012:role/service-role/CfnRole",
			wantKind: "role",
			wantName: "CfnRole",
			wantOK:   true,
		},
		{
			// The documented exception, kept deliberately:
			// arn:aws:sts::${Account}:assumed-role/${RoleName}/${RoleSessionName} is two
			// segments and the role's path does not appear in it, so the role name is the
			// *first* one. Taking the last would look up a role named after the session.
			name:     "an assumed-role session names the role first, not last",
			arn:      "arn:aws:sts::123456789012:assumed-role/CfnRole/sess1",
			wantKind: "role",
			wantName: "CfnRole",
			wantOK:   true,
		},
		{
			// An ARN ending in a slash names a path and no entity. Resolving it to the
			// empty name would key the lookup at user:, which is a real key shape.
			name:   "an ARN ending in a slash names no entity",
			arn:    "arn:aws:iam::123456789012:user/division/engineering/",
			wantOK: false,
		},
		{
			name:   "the account root is not an entity policies attach to",
			arn:    "arn:aws:iam::123456789012:root",
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account, kind, name, ok := emulator.IAMEntityForPrincipalARNForTest(tt.arn)
			require.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				return
			}
			assert.Equal(t, authzTestAccount, account)
			assert.Equal(t, tt.wantKind, kind)
			assert.Equal(t, tt.wantName, name)
		})
	}
}

func TestIAMPrincipalTags_AnUnreadableEntityRecordYieldsNoTags(t *testing.T) {
	// A record substrate cannot decode must not take the request down with it: the tags
	// come out empty, which denies anything conditioned on one, and the read is the same
	// one the caller's ARN takes its path from — so a corrupt record has to be survivable
	// on both (#801).
	state := emulator.NewMemoryStateManager()
	require.NoError(t, state.Put(context.Background(), "iam",
		emulator.IAMUserKeyForTest(authzTestAccount, "alice"), []byte("not json at all")))

	assert.Empty(t, emulator.IAMPrincipalTagsForTest(state,
		"arn:aws:iam::"+authzTestAccount+":user/division/engineering/alice"))

	// And a signed request still identifies its caller, falling back to the path-less ARN
	// exactly as it does when the record is absent.
	keyRaw, err := json.Marshal(emulator.IAMAccessKey{
		AccessKeyID: "AKIACORRUPTRECORD001",
		Status:      "Active",
		UserName:    "alice",
		AccountID:   authzTestAccount,
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(context.Background(), "iam",
		emulator.IAMAccessKeyKeyForTest("AKIACORRUPTRECORD001"), keyRaw))

	principal, _ := emulator.ResolvePrincipalForTest(state, authzTestAccount, "AKIACORRUPTRECORD001")
	require.NotNil(t, principal)
	assert.Equal(t, "arn:aws:iam::123456789012:user/alice", principal.ARN)
}

func TestIAMCallerUserName_WhoAnAbsentUserNameMeans(t *testing.T) {
	// GetUser, CreateAccessKey and ListAccessKeys each document UserName as optional, with
	// AWS deriving it "implicitly based on the AWS access key ID used to sign the request".
	// Deriving it by reading the whole ARN component was the same defect one layer up: a
	// pathful caller's implicit name became "division/engineering/alice", a user that does
	// not exist.
	tests := []struct {
		name      string
		principal *emulator.Principal
		want      string
	}{
		{
			name: "no principal at all has no implicit name",
		},
		{
			name:      "the recorded name is preferred over the ARN",
			principal: iamPathPrincipal("alice", "arn:aws:iam::123456789012:user/division/engineering/alice", nil),
			want:      "alice",
		},
		{
			// A principal substrate did not mint the credential for — CloudFormation
			// builds one from a stack's creator ARN — carries no recorded name, so the
			// ARN is the fallback and its last segment is the name.
			name:      "an unrecorded name falls back to the ARN's friendly name",
			principal: &emulator.Principal{ARN: "arn:aws:iam::123456789012:user/division/engineering/alice"},
			want:      "alice",
		},
		{
			// An assumed role is not a user and has no implicit user name; "" is what
			// makes the handler answer AWS's ValidationError rather than look up a user
			// named after a role session.
			name:      "an assumed role has no implicit user name",
			principal: &emulator.Principal{ARN: "arn:aws:sts::123456789012:assumed-role/CfnRole/sess1", Type: "AssumedRole"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, emulator.IAMCallerUserNameForTest(tt.principal))
		})
	}
}

func TestIAM_APathfulCallerReadsItselfWithNoUserName(t *testing.T) {
	// End to end for the same rule, on the operation a consumer actually reaches: `aws iam
	// get-user` with no argument. Both halves of #801 are in play — the caller's ARN now
	// carries the path, and the implicit name has to be the friendly segment of it.
	srv, _ := newPrincipalTestServer(t)

	require.Equal(t, http.StatusOK, principalIAMCall(t, srv, "CreateUser", map[string]any{
		"UserName": "alice",
		"Path":     iamPathTestPath,
	}).StatusCode)
	// She needs a policy, because she is now enforced — which is the rest of this change.
	require.Equal(t, http.StatusOK, principalIAMCall(t, srv, "PutUserPolicy", map[string]any{
		"UserName":   "alice",
		"PolicyName": "SelfService",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Action":["iam:GetUser","iam:CreateAccessKey","iam:ListAccessKeys"],"Resource":"*"}]}`,
	}).StatusCode)

	resp := principalIAMCall(t, srv, "CreateAccessKey", map[string]any{"UserName": "alice"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var created struct {
		AccessKeyID string `xml:"CreateAccessKeyResult>AccessKey>AccessKeyId"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&created))
	require.NoError(t, resp.Body.Close())

	// Signed as alice, with no UserName in the body.
	implicit := iamPathCallAs(t, srv, created.AccessKeyID, "GetUser", map[string]any{})
	require.Equal(t, http.StatusOK, implicit.StatusCode, "an implicit UserName must resolve")
	var got struct {
		UserName string `xml:"GetUserResult>User>UserName"`
		ARN      string `xml:"GetUserResult>User>Arn"`
	}
	require.NoError(t, xml.NewDecoder(implicit.Body).Decode(&got))
	require.NoError(t, implicit.Body.Close())
	assert.Equal(t, "alice", got.UserName)
	assert.Equal(t, "arn:aws:iam::123456789012:user/division/engineering/alice", got.ARN)

	// The other two operations documenting the same optional parameter.
	keys := iamPathCallAs(t, srv, created.AccessKeyID, "ListAccessKeys", map[string]any{})
	require.NoError(t, keys.Body.Close())
	assert.Equal(t, http.StatusOK, keys.StatusCode)

	minted := iamPathCallAs(t, srv, created.AccessKeyID, "CreateAccessKey", map[string]any{})
	require.NoError(t, minted.Body.Close())
	assert.Equal(t, http.StatusOK, minted.StatusCode)
}

// iamPathCallAs issues an IAM request signed with a specific access key, so the handler
// resolves a principal and can derive an implicit UserName from it.
func iamPathCallAs(t *testing.T, srv *emulator.Server, accessKeyID, operation string,
	body any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(raw)))
	r.Host = "iam.amazonaws.com"
	r.Header.Set("X-Amz-Target", "AmazonIdentityManagementService."+operation)
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("Authorization", principalAuthHeader(accessKeyID, "iam"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

func TestCFN_AServiceRoleAtAPathIsEnforced(t *testing.T) {
	// The live-reachable false allow, end to end. cfn_deployer builds a role principal
	// from the stack's RoleARN verbatim, so a stack whose service role is at
	// `/service-role/` — where AWS's console creates one — had *every* resource call
	// unenforced: this template deployed cleanly under a role granting nothing.
	tests := []struct {
		name       string
		actions    []string
		wantStatus string
	}{
		{
			name:       "a role at a path that cannot create the bucket rolls the stack back",
			actions:    []string{"s3:ListBucket"},
			wantStatus: "ROLLBACK_COMPLETE",
		},
		{
			name:       "a role at a path that can create the bucket deploys",
			actions:    []string{"s3:*"},
			wantStatus: "CREATE_COMPLETE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			cfnSeedRoleAtPath(t, ts.StateManager(), iamPathServiceRolePath, "CfnRole", tt.actions)
			client := &cfnRoleTestClient{t: t, baseURL: ts.URL}

			roleARN := "arn:aws:iam::123456789012:role" + iamPathServiceRolePath + "CfnRole"
			code, body := client.call("CreateStack", map[string]string{
				"StackName":    "pathrolestack",
				"TemplateBody": cfnRoleBucketTemplate,
				"RoleARN":      roleARN,
			})
			require.Equal(t, http.StatusOK, code, "body: %s", body)

			_, body = client.call("DescribeStacks", map[string]string{"StackName": "pathrolestack"})
			status, reportedRole, reason := cfnStackStatus(t, body)
			assert.Equal(t, tt.wantStatus, status, "reason: %s", reason)
			assert.Equal(t, roleARN, reportedRole)
			if tt.wantStatus == "ROLLBACK_COMPLETE" {
				assert.Contains(t, reason, "AccessDenied",
					"the denial must name itself, not just a status")
			}
		})
	}
}
