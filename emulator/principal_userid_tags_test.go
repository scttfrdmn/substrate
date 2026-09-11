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

// The caller substrate could not describe (#771).
//
// `aws:PrincipalArn` and `aws:username` were published; `aws:userid` and
// `aws:PrincipalTag/<key>` were not, so a policy conditioning on either was evaluated
// against a context that did not hold it. For a positive operator that is a false deny; for
// a negated one it is a false *allow*, since AWS answers true for a negated operator over an
// absent key — the direction that matters, and the same shape as the `aws:PrincipalArn`
// exemption #745 fixed.
//
// Both keys arrive the way #745 established rather than by deriving anything from the ARN:
// the unique ID is **recorded** when the credential is minted, because only the AssumeRole
// call that made a session knows the `<role-id>:<session-name>` pairing AWS publishes for
// it, and the tags are **read** when the credential is resolved, because `TagUser` changes
// them after the key exists.
//
// The forms asserted below are AWS's own, from the principal-key table in *IAM policy
// elements: Variables and tags*: an IAM user's `aws:userid` is its "unique ID", an assumed
// role's is "{role-id}:{caller-specified-role-name} where role-id is the unique id of the
// role and the caller-specified-role-name is specified by the RoleSessionName parameter",
// and the account root's is the account ID — the one kind substrate models no principal for,
// since an unauthenticated caller resolves to a nil principal.

// principalTagStatement allows one action on everything, conditioned on a caller key. It is
// the least-privilege shape a consumer writes when a tag is what decides.
func principalTagStatement(action, key, value string) emulator.PolicyStatement {
	return emulator.PolicyStatement{
		Effect:   "Allow",
		Action:   emulator.StringOrSlice{action},
		Resource: emulator.StringOrSlice{"*"},
		Condition: map[string]map[string]emulator.StringOrSlice{
			"StringEquals": {key: emulator.StringOrSlice{value}},
		},
	}
}

// newPrincipalKeyState seeds one user holding one inline-equivalent customer policy, which
// is the smallest state in which both authorization doors evaluate a real document.
func newPrincipalKeyState(t *testing.T, userName string, statements ...emulator.PolicyStatement) emulator.StateManager {
	t.Helper()
	const policyARN = "arn:aws:iam::" + authzTestAccount + ":policy/principal-keys"
	return newAuthTestState(t, userName, policyARN, emulator.PolicyDocument{
		Version:   "2012-10-17",
		Statement: statements,
	})
}

// principalKeyReqCtx is a caller carrying the unique ID and tags under test.
func principalKeyReqCtx(userName, userID string, tags map[string]string) *emulator.RequestContext {
	return &emulator.RequestContext{
		RequestID: "req-1",
		AccountID: authzTestAccount,
		Region:    "us-east-1",
		Principal: &emulator.Principal{
			ARN:      "arn:aws:iam::" + authzTestAccount + ":user/" + userName,
			Type:     "IAMUser",
			UserName: userName,
			UserID:   userID,
			Tags:     tags,
		},
		Metadata: make(map[string]interface{}),
	}
}

func TestAuthzPrincipalContext_PublishesTheUniqueIDAndTagsOrNothingAtAll(t *testing.T) {
	// The absent-versus-empty distinction, which no allow/deny assertion can see: a key
	// substrate has no value for is left out, so a policy can test for it with Null.
	tests := []struct {
		name      string
		principal *emulator.Principal
		want      map[string]string
		absent    []string
	}{
		{
			name: "an IAM user publishes its AIDA and its tags",
			principal: &emulator.Principal{
				ARN:      "arn:aws:iam::123456789012:user/alice",
				Type:     "IAMUser",
				UserName: "alice",
				UserID:   "AIDAEXAMPLE1234567890",
				Tags:     map[string]string{"team": "blue", "cost-center": "12345"},
			},
			want: map[string]string{
				"aws:PrincipalArn":             "arn:aws:iam::123456789012:user/alice",
				"aws:username":                 "alice",
				"aws:userid":                   "AIDAEXAMPLE1234567890",
				"aws:PrincipalTag/team":        "blue",
				"aws:PrincipalTag/cost-center": "12345",
			},
		},
		{
			// AWS: "{role-id}:{caller-specified-role-name}". aws:username stays absent
			// for an assumed role, which #745 already settled.
			name: "an assumed role publishes the role ID paired with the session name",
			principal: &emulator.Principal{
				ARN:    "arn:aws:sts::123456789012:assumed-role/worker/sess1",
				Type:   "AssumedRole",
				UserID: "AROAEXAMPLE1234567890:sess1",
				Tags:   map[string]string{"team": "blue"},
			},
			want: map[string]string{
				"aws:PrincipalArn":      "arn:aws:sts::123456789012:assumed-role/worker/sess1",
				"aws:userid":            "AROAEXAMPLE1234567890:sess1",
				"aws:PrincipalTag/team": "blue",
			},
			absent: []string{"aws:username"},
		},
		{
			// A registry hit with no IAM entity behind it: the ARN's last segment is the
			// access key ID, so there is no unique ID to publish and no entity to read
			// tags from. Publishing a guess would be worse than publishing nothing.
			name: "a credential with no IAM entity publishes neither key",
			principal: &emulator.Principal{
				ARN:  "arn:aws:iam::123456789012:user/AKIAIOSFODNN7EXAMPLE",
				Type: "IAMUser",
			},
			want:   map[string]string{"aws:PrincipalArn": "arn:aws:iam::123456789012:user/AKIAIOSFODNN7EXAMPLE"},
			absent: []string{"aws:userid", "aws:username"},
		},
		{
			// A tag that exists with an empty value is published as one. condKeyPresent
			// reads an empty value as absent, which is what makes a Null condition answer
			// for it the way it does for every other key.
			name: "an empty tag value is published as an empty value",
			principal: &emulator.Principal{
				ARN:    "arn:aws:iam::123456789012:user/alice",
				Type:   "IAMUser",
				UserID: "AIDAEXAMPLE1234567890",
				Tags:   map[string]string{"team": ""},
			},
			want: map[string]string{
				"aws:PrincipalArn":      "arn:aws:iam::123456789012:user/alice",
				"aws:userid":            "AIDAEXAMPLE1234567890",
				"aws:PrincipalTag/team": "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := emulator.AuthzPrincipalContextForTest(tt.principal)
			for key, value := range tt.want {
				assert.Equal(t, value, got[key], "key %s", key)
			}
			assert.Len(t, got, len(tt.want), "no key beyond the ones asserted: %v", got)
			for _, key := range tt.absent {
				assert.NotContains(t, got, key)
			}
		})
	}
}

func TestServer_ResolvePrincipal_CarriesTheAIDAAWSPublishesForAnIAMUser(t *testing.T) {
	srv, capture := newPrincipalTestServer(t)
	keyID := principalCreateAccessKey(t, srv, "alice")

	// The value the user's own record reports, so the decision and `GetUser` agree
	// rather than each holding a plausible ID of its own.
	userID := principalGetUserID(t, srv, "alice")
	require.True(t, strings.HasPrefix(userID, "AIDA"), "GetUser reported %q", userID)

	resp := principalDynamoCall(t, srv, keyID)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.NotNil(t, capture.principal)
	assert.Equal(t, userID, capture.principal.UserID)
	assert.Empty(t, capture.principal.Tags, "an untagged user carries no tags")
}

func TestServer_ResolvePrincipal_PairsTheRoleIDWithTheSessionName(t *testing.T) {
	srv, capture := newPrincipalTestServer(t)

	require.Equal(t, http.StatusOK, principalIAMCall(t, srv, "CreateRole", map[string]any{
		"RoleName": "worker",
		"Tags":     []map[string]string{{"Key": "team", "Value": "blue"}},
	}).StatusCode)

	r := httptest.NewRequest(http.MethodPost,
		"/?Action=AssumeRole&RoleArn=arn:aws:iam::123456789012:role/worker&RoleSessionName=sess1", nil)
	r.Host = "sts.amazonaws.com"
	r.Header.Set("Authorization", principalAuthHeader("AKIAIOSFODNN7EXAMPLE", "sts"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)

	var assumed struct {
		Credentials struct {
			AccessKeyID string `xml:"AccessKeyId"`
		} `xml:"AssumeRoleResult>Credentials"`
		AssumedRoleUser struct {
			AssumedRoleID string `xml:"AssumedRoleId"`
		} `xml:"AssumeRoleResult>AssumedRoleUser"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &assumed))
	require.NotEmpty(t, assumed.Credentials.AccessKeyID)
	// AWS publishes the same string in both places, and that is the point of recording
	// it: the session's ARN carries the role's *name*, so the pairing cannot be
	// reconstructed from the credential afterwards.
	require.True(t, strings.HasSuffix(assumed.AssumedRoleUser.AssumedRoleID, ":sess1"),
		"AssumedRoleId was %q", assumed.AssumedRoleUser.AssumedRoleID)

	resp := principalDynamoCall(t, srv, assumed.Credentials.AccessKeyID)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.NotNil(t, capture.principal)
	assert.Equal(t, assumed.AssumedRoleUser.AssumedRoleID, capture.principal.UserID)
	// The role's own tags. Session tags are not modeled — AssumeRole reads no Tags
	// parameter — so this is a documented narrowing rather than AWS's full answer.
	assert.Equal(t, map[string]string{"team": "blue"}, capture.principal.Tags)
}

func TestServer_ResolvePrincipal_ASessionTokenKeepsTheCallersOwnUniqueID(t *testing.T) {
	// GetSessionToken returns credentials for the same IAM user, so aws:userid keeps its
	// value across the call rather than becoming a session pairing — the same reasoning
	// that made the session carry UserName in #745.
	srv, capture := newPrincipalTestServer(t)
	keyID := principalCreateAccessKey(t, srv, "alice")
	userID := principalGetUserID(t, srv, "alice")

	r := httptest.NewRequest(http.MethodPost, "/?Action=GetSessionToken", nil)
	r.Host = "sts.amazonaws.com"
	r.Header.Set("Authorization", principalAuthHeader(keyID, "sts"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)

	var session struct {
		Credentials struct {
			AccessKeyID string `xml:"AccessKeyId"`
		} `xml:"GetSessionTokenResult>Credentials"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &session))
	require.NotEmpty(t, session.Credentials.AccessKeyID)

	resp := principalDynamoCall(t, srv, session.Credentials.AccessKeyID)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.NotNil(t, capture.principal)
	assert.Equal(t, userID, capture.principal.UserID)
}

func TestServer_ResolvePrincipal_TagsFollowTheEntityNotTheCredential(t *testing.T) {
	// Why the tags are read per request rather than copied onto the access key: a
	// long-lived key would otherwise authorize against whatever tags its user held on the
	// day it was created, and a consumer's `TagUser` — or the `UntagUser` that revokes an
	// exemption — would have no effect until the key was rotated.
	srv, capture := newPrincipalTestServer(t)
	keyID := principalCreateAccessKey(t, srv, "alice")

	resp := principalDynamoCall(t, srv, keyID)
	require.NoError(t, resp.Body.Close())
	require.NotNil(t, capture.principal)
	require.Empty(t, capture.principal.Tags)

	require.Equal(t, http.StatusOK, principalIAMCall(t, srv, "TagUser", map[string]any{
		"UserName": "alice",
		"Tags":     []map[string]string{{"Key": "team", "Value": "blue"}},
	}).StatusCode)

	resp = principalDynamoCall(t, srv, keyID)
	require.NoError(t, resp.Body.Close())
	require.NotNil(t, capture.principal)
	assert.Equal(t, map[string]string{"team": "blue"}, capture.principal.Tags,
		"a tag applied after the key was minted reaches the decision")

	require.Equal(t, http.StatusOK, principalIAMCall(t, srv, "UntagUser", map[string]any{
		"UserName": "alice",
		"TagKeys":  []string{"team"},
	}).StatusCode)

	resp = principalDynamoCall(t, srv, keyID)
	require.NoError(t, resp.Body.Close())
	require.NotNil(t, capture.principal)
	assert.Empty(t, capture.principal.Tags, "and removing it reaches the decision too")
}

func TestResolvePrincipal_ARecordWrittenBeforeTheUniqueIDHasNone(t *testing.T) {
	// The #737 fallback shape, applied to the new field: a record from an older substrate
	// reads back with it empty, and the reader publishes no key rather than an empty one.
	tests := []struct {
		name      string
		namespace string
		key       string
		raw       string
		accessKey string
	}{
		{
			name:      "an access key record with no UserId",
			namespace: "iam",
			key:       "accesskey:AKIAOLDRECORD0000001",
			raw:       `{"AccessKeyId":"AKIAOLDRECORD0000001","UserName":"alice","AccountId":"123456789012"}`,
			accessKey: "AKIAOLDRECORD0000001",
		},
		{
			name:      "a session record with no PrincipalId",
			namespace: "sts",
			key:       "session:ASIAOLDRECORD0000001",
			raw: `{"AccessKeyId":"ASIAOLDRECORD0000001",` +
				`"PrincipalArn":"arn:aws:sts::123456789012:assumed-role/worker/sess1",` +
				`"AccountId":"123456789012"}`,
			accessKey: "ASIAOLDRECORD0000001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := emulator.NewMemoryStateManager()
			require.NoError(t, state.Put(context.Background(), tt.namespace, tt.key, []byte(tt.raw)))

			principal, _ := emulator.ResolvePrincipalForTest(state, authzTestAccount, tt.accessKey)
			require.NotNil(t, principal, "the credential still identifies its principal")
			assert.Empty(t, principal.UserID)
			assert.NotContains(t, emulator.AuthzPrincipalContextForTest(principal), "aws:userid")
		})
	}
}

func TestIAMPrincipalTags_ReadsEitherEntityAndNothingElse(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	ctx := context.Background()

	userRaw, err := json.Marshal(emulator.IAMUser{
		UserName: "alice",
		ARN:      "arn:aws:iam::" + authzTestAccount + ":user/alice",
		Path:     "/",
		Tags:     []emulator.IAMTag{{Key: "team", Value: "blue"}},
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", emulator.IAMUserKeyForTest(authzTestAccount, "alice"), userRaw))

	roleRaw, err := json.Marshal(emulator.IAMRole{
		RoleName: "worker",
		ARN:      "arn:aws:iam::" + authzTestAccount + ":role/worker",
		Path:     "/",
		Tags:     []emulator.IAMTag{{Key: "team", Value: "green"}, {Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", emulator.IAMRoleKeyForTest(authzTestAccount, "worker"), roleRaw))

	untaggedRaw, err := json.Marshal(emulator.IAMUser{
		UserName: "bob", ARN: "arn:aws:iam::" + authzTestAccount + ":user/bob", Path: "/",
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", emulator.IAMUserKeyForTest(authzTestAccount, "bob"), untaggedRaw))

	tests := []struct {
		name string
		arn  string
		want map[string]string
	}{
		{
			name: "a user",
			arn:  "arn:aws:iam::" + authzTestAccount + ":user/alice",
			want: map[string]string{"team": "blue"},
		},
		{
			// An assumed-role ARN reads the *role's* record, which is the unwrapping
			// resolveIAMEntity already does and the reason both share one mapping.
			name: "an assumed-role session reads the role behind it",
			arn:  "arn:aws:sts::" + authzTestAccount + ":assumed-role/worker/sess1",
			want: map[string]string{"team": "green", "env": "prod"},
		},
		{
			name: "the role itself",
			arn:  "arn:aws:iam::" + authzTestAccount + ":role/worker",
			want: map[string]string{"team": "green", "env": "prod"},
		},
		{
			name: "a user with no tags",
			arn:  "arn:aws:iam::" + authzTestAccount + ":user/bob",
		},
		{
			name: "a user with no record",
			arn:  "arn:aws:iam::" + authzTestAccount + ":user/ghost",
		},
		{
			// Another account's user of the same name, which is a different record and
			// must not inherit this one's tags (#737).
			name: "the same name in another account",
			arn:  "arn:aws:iam::210987654321:user/alice",
		},
		{
			// Not an entity substrate models as holding tags. The account root is here
			// because it is the one AWS documents an aws:userid for that substrate has no
			// principal for at all.
			name: "the account root",
			arn:  "arn:aws:iam::" + authzTestAccount + ":root",
		},
		{
			name: "a service principal",
			arn:  "lambda.amazonaws.com",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := emulator.IAMPrincipalTagsForTest(state, tt.arn)
			if tt.want == nil {
				assert.Nil(t, got)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCheckAccess_APrincipalTagConditionDecidesTheRequest(t *testing.T) {
	// The consumer-facing shape: one policy, reused across a fleet, that grants only to
	// the team the resource belongs to. Before this the condition could not be evaluated
	// at all, so the Allow never fired and every caller was refused.
	state := newPrincipalKeyState(t, "alice",
		principalTagStatement("s3:GetObject", "aws:PrincipalTag/team", "blue"))
	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
	req := &emulator.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/bucket/key"}

	blue := principalKeyReqCtx("alice", "AIDAEXAMPLE1234567890", map[string]string{"team": "blue"})
	assert.NoError(t, auth.CheckAccess(blue, req), "the tag the statement names")

	red := principalKeyReqCtx("alice", "AIDAEXAMPLE1234567890", map[string]string{"team": "red"})
	assert.Error(t, auth.CheckAccess(red, req), "another team's tag")

	none := principalKeyReqCtx("alice", "AIDAEXAMPLE1234567890", nil)
	assert.Error(t, auth.CheckAccess(none, req), "no tag at all")
}

func TestCheckAccess_AUseridConditionDecidesTheRequest(t *testing.T) {
	state := newPrincipalKeyState(t, "alice",
		principalTagStatement("s3:GetObject", "aws:userid", "AIDAEXAMPLE1234567890"))
	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
	req := &emulator.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/bucket/key"}

	assert.NoError(t, auth.CheckAccess(
		principalKeyReqCtx("alice", "AIDAEXAMPLE1234567890", nil), req))
	assert.Error(t, auth.CheckAccess(
		principalKeyReqCtx("alice", "AIDAOTHERUSER12345678", nil), req),
		"a different unique ID is a different principal, even under the same name")
	// And the key is absent rather than empty for a caller substrate has no ID for, so
	// the positive operator finds nothing to match — not an empty string that happens to
	// differ from the value.
	assert.Error(t, auth.CheckAccess(principalKeyReqCtx("alice", "", nil), req))
}

func TestCheckAccess_ANullConditionSeesTheKeysThatAreNowPublished(t *testing.T) {
	// The compatibility direction, stated because it inverts: a policy asserting the
	// *absence* of either key stops matching for a caller that now has one. AWS answers
	// the same way, which is why this is the fix rather than a regression.
	deny := emulator.PolicyStatement{
		Effect:   "Allow",
		Action:   emulator.StringOrSlice{"s3:GetObject"},
		Resource: emulator.StringOrSlice{"*"},
		Condition: map[string]map[string]emulator.StringOrSlice{
			"Null": {"aws:userid": emulator.StringOrSlice{"true"}},
		},
	}
	state := newPrincipalKeyState(t, "alice", deny)
	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
	req := &emulator.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/bucket/key"}

	assert.Error(t, auth.CheckAccess(
		principalKeyReqCtx("alice", "AIDAEXAMPLE1234567890", nil), req),
		"the key is present, so `Null: true` no longer matches")
	assert.NoError(t, auth.CheckAccess(principalKeyReqCtx("alice", "", nil), req),
		"and it still matches a caller substrate has no unique ID for")
}

func TestCheckAccess_APrincipalTagResolvesAsAPolicyVariable(t *testing.T) {
	// AWS's own example: "${aws:PrincipalTag/team} allows the actions only if the bucket
	// name ends with a team name from the team principal tag." Substitution needed no
	// change for this — [resolvePolicyVariable] reads any single-valued key through
	// condResolveKey — so what is asserted here is that publishing the key is the whole
	// of what was missing.
	state := newPrincipalKeyState(t, "alice", emulator.PolicyStatement{
		Effect:   "Allow",
		Action:   emulator.StringOrSlice{"s3:GetObject"},
		Resource: emulator.StringOrSlice{"arn:aws:s3:::reports-${aws:PrincipalTag/team}/*"},
	})
	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
	blue := principalKeyReqCtx("alice", "AIDAEXAMPLE1234567890", map[string]string{"team": "blue"})

	assert.NoError(t, auth.CheckAccess(blue,
		&emulator.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/reports-blue/q3.csv"}))
	assert.Error(t, auth.CheckAccess(blue,
		&emulator.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/reports-red/q3.csv"}),
		"another team's bucket")

	// A variable with no value voids the element: AWS's "There is no equal or like
	// value", so the Resource matches nothing rather than matching everything.
	untagged := principalKeyReqCtx("alice", "AIDAEXAMPLE1234567890", nil)
	assert.Error(t, auth.CheckAccess(untagged,
		&emulator.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/reports-blue/q3.csv"}))
}

func TestBothDoorsAgreeOnTheCallersUniqueIDAndTags(t *testing.T) {
	// One caller, two gates, one answer — the #411/#714 requirement applied to the
	// condition context. The keys are published by authzPrincipalContext, which both
	// doors call, so this asserts the wiring rather than a second implementation.
	tests := []struct {
		name       string
		userID     string
		tags       map[string]string
		wantDenied bool
	}{
		{name: "the tag and the ID the statement names", userID: "AIDAEXAMPLE1234567890", tags: map[string]string{"team": "blue"}},
		{name: "the wrong team", userID: "AIDAEXAMPLE1234567890", tags: map[string]string{"team": "red"}, wantDenied: true},
		{name: "the wrong unique ID", userID: "AIDAOTHERUSER12345678", tags: map[string]string{"team": "blue"}, wantDenied: true},
		{name: "no unique ID and no tags", wantDenied: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := newPrincipalKeyState(t, "alice",
				emulator.PolicyStatement{
					Effect:   "Allow",
					Action:   emulator.StringOrSlice{"iam:GetUser"},
					Resource: emulator.StringOrSlice{"*"},
					Condition: map[string]map[string]emulator.StringOrSlice{
						"StringEquals": {
							"aws:PrincipalTag/team": emulator.StringOrSlice{"blue"},
							"aws:userid":            emulator.StringOrSlice{"AIDAEXAMPLE1234567890"},
						},
					},
				})

			logger := emulator.NewDefaultLogger(slog.LevelError, false)
			auth := emulator.NewAuthController(state, logger)
			plugin := &emulator.IAMPlugin{}
			require.NoError(t, plugin.Initialize(context.Background(),
				emulator.PluginConfig{State: state, Logger: logger}))

			reqCtx := principalKeyReqCtx("alice", tt.userID, tt.tags)
			gateErr := auth.CheckAccess(reqCtx, &emulator.AWSRequest{
				Service: "iam", Operation: "GetUser", Params: map[string]string{"UserName": "alice"},
			})
			pluginErr := emulator.IAMAuthorizeWithForTest(plugin, reqCtx, "iam:GetUser", "*", nil)

			if tt.wantDenied {
				assert.Error(t, gateErr, "CheckAccess should deny")
				assert.Error(t, pluginErr, "IAMPlugin.authorizeWith should deny")
				return
			}
			assert.NoError(t, gateErr, "CheckAccess should allow")
			assert.NoError(t, pluginErr, "IAMPlugin.authorizeWith should allow")
		})
	}
}

// principalGetUserID reports the UserId GetUser renders for a user, which is the value the
// decision has to agree with.
func principalGetUserID(t *testing.T, srv *emulator.Server, userName string) string {
	t.Helper()
	resp := principalIAMCall(t, srv, "GetUser", map[string]any{"UserName": userName})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var parsed struct {
		UserID string `xml:"GetUserResult>User>UserId"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&parsed))
	require.NoError(t, resp.Body.Close())
	require.NotEmpty(t, parsed.UserID)
	return parsed.UserID
}
