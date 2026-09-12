package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// What GetCallerIdentity's UserId is (#805).
//
// AWS's identifiers reference documents three forms, and names GetCallerIdentity as the
// way to obtain one:
//
//	an IAM user       AIDA…                      the user's unique ID
//	an assumed role   AROA…:<role-session-name>  the role's, plus the session name the caller chose
//	the account root  the account ID
//
// Substrate reported the caller's *friendly name* — "alice" — which is a shape AWS never
// produces, and which disagreed with substrate's own `aws:userid`, published from the same
// recorded field since #771. Arn, Account and the response shape do not move; only UserId.

// callerIdentity is a decoded GetCallerIdentity response, so a case asserts on the members
// AWS documents rather than on a substring of XML.
type callerIdentity struct {
	UserID  string
	Account string
	ARN     string
}

// newCallerIdentityServer is [newTrustPolicyTestServer] plus the state behind the server,
// which a case seeding a record no current version would write needs to reach.
func newCallerIdentityServer(t *testing.T) (*emulator.Server, emulator.StateManager) {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))

	iamPlugin := &emulator.IAMPlugin{}
	require.NoError(t, iamPlugin.Initialize(context.TODO(),
		emulator.PluginConfig{State: state, Logger: logger}))
	registry.Register(iamPlugin)

	stsPlugin := &emulator.STSPlugin{}
	require.NoError(t, stsPlugin.Initialize(context.TODO(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(stsPlugin)

	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger, emulator.ServerOptions{
		Auth: emulator.NewAuthController(state, logger),
	})
	return srv, state
}

// callerIdentityFor calls GetCallerIdentity as accessKeyID, or unsigned when it is empty,
// and decodes the result. It builds the request itself rather than going through
// [trustGetCallerIdentity], which always signs — and an unsigned request is one of the
// three cases the account root's form has to be asserted on.
func callerIdentityFor(t *testing.T, srv *emulator.Server, accessKeyID string) callerIdentity {
	t.Helper()
	q := url.Values{}
	q.Set("Action", "GetCallerIdentity")
	q.Set("Version", "2011-06-15")
	r := httptest.NewRequest(http.MethodPost, "/?"+q.Encode(), nil)
	r.Host = "sts.amazonaws.com"
	if accessKeyID != "" {
		r.Header.Set("Authorization", trustAuthHeader(accessKeyID, "sts"))
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	var parsed struct {
		UserID  string `xml:"GetCallerIdentityResult>UserId"`
		Account string `xml:"GetCallerIdentityResult>Account"`
		ARN     string `xml:"GetCallerIdentityResult>Arn"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &parsed), "body: %s", w.Body.String())
	return callerIdentity{UserID: parsed.UserID, Account: parsed.Account, ARN: parsed.ARN}
}

// seedIAMRecord writes a record directly, so a case can present a credential exactly as an
// older version of substrate wrote it.
func seedIAMRecord(t *testing.T, state emulator.StateManager, namespace, key string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, state.Put(context.Background(), namespace, key, raw))
}

func TestGetCallerIdentity_AnIAMUserReportsTheirUniqueID(t *testing.T) {
	// The first documented form. The friendly name is what substrate reported; the unique
	// ID is what AWS documents and what a consumer reading UserId asked for.
	srv, _ := newCallerIdentityServer(t)
	key := trustSetupCaller(t, srv, "alice")

	got := callerIdentityFor(t, srv, key)
	assert.True(t, strings.HasPrefix(got.UserID, "AIDA"),
		"an IAM user's unique ID is an AIDA…: %q", got.UserID)
	assert.NotEqual(t, "alice", got.UserID, "a friendly name is not a unique ID")

	// The members that do not move.
	assert.Equal(t, "arn:aws:iam::123456789012:user/alice", got.ARN)
	assert.Equal(t, "123456789012", got.Account)
}

func TestGetCallerIdentity_AgreesWithAWSUserID(t *testing.T) {
	// The other half of the same claim: UserId and `aws:userid` are one recorded value, so
	// a policy conditioned on aws:userid and an assertion on UserId cannot disagree about
	// who the caller is. They did, because only one of them read the record (#771).
	srv, state := newCallerIdentityServer(t)
	key := trustSetupCaller(t, srv, "alice")

	got := callerIdentityFor(t, srv, key)
	principal, _ := emulator.ResolvePrincipalForTest(state, "123456789012", key)
	require.NotNil(t, principal)
	assert.Equal(t, principal.UserID, got.UserID)
	assert.Equal(t, got.UserID,
		emulator.AuthzPrincipalContextForTest(principal)["aws:userid"])
}

func TestGetCallerIdentity_AnAssumedRoleReportsTheRoleIDAndSession(t *testing.T) {
	// The second form, "{role-id}:{caller-specified-role-name}", whose second half is the
	// RoleSessionName the caller passed. Substrate reported "worker/sess1" — the ARN's last
	// two segments joined by a slash, which is neither the role ID nor a documented shape.
	srv, _ := newCallerIdentityServer(t)
	callerKey := trustSetupCaller(t, srv, "assumer")
	trustCreateRole(t, srv, "worker",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow",`+
			`"Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":"sts:AssumeRole"}]}`)

	resp := trustAssumeRole(t, srv, callerKey,
		"arn:aws:iam::123456789012:role/worker", "sess1", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	sessionKey := trustSessionKeyFrom(t, resp)

	got := callerIdentityFor(t, srv, sessionKey)
	roleID, session, found := strings.Cut(got.UserID, ":")
	require.True(t, found, "an assumed role's unique ID is <role-id>:<session>: %q", got.UserID)
	assert.True(t, strings.HasPrefix(roleID, "AROA"),
		"the first half is the role's AROA…: %q", roleID)
	assert.Equal(t, "sess1", session, "the second half is the caller's RoleSessionName")

	// AWS's assumed-role ARN, unchanged by this.
	assert.Equal(t, "arn:aws:sts::123456789012:assumed-role/worker/sess1", got.ARN)
}

func TestGetCallerIdentity_TheAccountRootReportsTheAccountID(t *testing.T) {
	// The third form, and the one path where all three members come from the account
	// rather than from an entity: a request that resolves to no principal at all. The
	// second case is substrate's own documented access key, which belongs to no IAM
	// entity — so this pins the response a consumer following the quickstart sees.
	srv, _ := newCallerIdentityServer(t)

	for _, tc := range []struct{ name, key string }{
		{"an unsigned request", ""},
		{"a key belonging to no IAM entity", "AKIAIOSFODNN7EXAMPLE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := callerIdentityFor(t, srv, tc.key)
			assert.Equal(t, "123456789012", got.UserID)
			assert.Equal(t, "123456789012", got.Account)
			assert.Equal(t, "arn:aws:iam::123456789012:root", got.ARN)
		})
	}
}

func TestGetCallerIdentity_ACredentialMintedBeforeTheIDWasRecordedStillReportsOne(t *testing.T) {
	// #771 began recording the unique ID on the credential, so the common path needs no
	// lookup. A key written before that has the field empty, and the entity's own record is
	// the fallback: GetCallerIdentity is not a hot path, and reporting a name here would be
	// the same divergence one version later.
	srv, state := newCallerIdentityServer(t)

	// The user is at a path, which keeps this honest about #801 too: the fallback has to
	// find the record by friendly name even though the ARN carries the path.
	seedIAMRecord(t, state, "iam", emulator.IAMUserKeyForTest("123456789012", "legacy"),
		emulator.IAMUser{
			UserName: "legacy",
			UserID:   "AIDAOLDCREDENTIAL123",
			ARN: emulator.IAMUserARNForTest("123456789012",
				"/division/engineering/", "legacy"),
			Path: "/division/engineering/",
		})
	// The access key as an older version wrote it: no UserId on the credential itself.
	seedIAMRecord(t, state, "iam", emulator.IAMAccessKeyKeyForTest("AKIAPRE771CREDENTIAL"),
		emulator.IAMAccessKey{
			AccessKeyID: "AKIAPRE771CREDENTIAL",
			Status:      "Active",
			UserName:    "legacy",
			AccountID:   "123456789012",
		})

	got := callerIdentityFor(t, srv, "AKIAPRE771CREDENTIAL")
	assert.Equal(t, "AIDAOLDCREDENTIAL123", got.UserID)
	assert.Equal(t, "arn:aws:iam::123456789012:user/division/engineering/legacy", got.ARN)
}

func TestGetCallerIdentity_ASessionMintedBeforeTheIDWasRecordedRebuildsIt(t *testing.T) {
	// The same fallback for the other credential type. The role's record supplies the
	// AROA…; the session name is the only half of the ID the ARN carries, so the documented
	// pairing is rebuilt from both — the same string assumeRole composes when it mints a
	// session today.
	srv, state := newCallerIdentityServer(t)

	seedIAMRecord(t, state, "iam", emulator.IAMRoleKeyForTest("123456789012", "worker"),
		emulator.IAMRole{
			RoleName: "worker",
			RoleID:   "AROAOLDSESSION456789",
			ARN:      "arn:aws:iam::123456789012:role/worker",
			Path:     "/",
		})
	seedIAMRecord(t, state, "sts", "session:ASIAPRE771SESSION001",
		emulator.STSSessionCredentials{
			AccessKeyID:  "ASIAPRE771SESSION001",
			PrincipalARN: "arn:aws:sts::123456789012:assumed-role/worker/oldsess",
			AccountID:    "123456789012",
			Expiration:   time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC),
		})

	got := callerIdentityFor(t, srv, "ASIAPRE771SESSION001")
	assert.Equal(t, "AROAOLDSESSION456789:oldsess", got.UserID)
	assert.Equal(t, "arn:aws:sts::123456789012:assumed-role/worker/oldsess", got.ARN)
}

func TestGetCallerIdentity_AnUnreadableEntityRecordFallsBackToTheName(t *testing.T) {
	// The read the pre-#771 fallback performs can fail, and a failed read is not an
	// identity: reporting the name is the same answer as for an entity that is gone, which
	// is better than an empty member or a 500 from an operation whose job is to say who the
	// caller is.
	srv, state := newCallerIdentityServer(t)

	require.NoError(t, state.Put(context.Background(), "iam",
		emulator.IAMUserKeyForTest("123456789012", "corrupt"), []byte("not json at all")))
	seedIAMRecord(t, state, "iam", emulator.IAMAccessKeyKeyForTest("AKIACORRUPTRECORD001"),
		emulator.IAMAccessKey{
			AccessKeyID: "AKIACORRUPTRECORD001",
			Status:      "Active",
			UserName:    "corrupt",
			AccountID:   "123456789012",
		})

	got := callerIdentityFor(t, srv, "AKIACORRUPTRECORD001")
	assert.Equal(t, "corrupt", got.UserID)
	assert.Equal(t, "arn:aws:iam::123456789012:user/corrupt", got.ARN)
}

func TestGetCallerIdentity_ASessionWhoseRoleIsGoneReportsTheARNsName(t *testing.T) {
	// The case the fallback exists for, on the session side: a pre-#771 session outliving
	// the role it was minted from — a CloudFormation stack whose service role was deleted
	// under it, for one. There is no `AROA…` left to read, so the session reports what
	// substrate reported before #805 rather than nothing at all.
	srv, state := newCallerIdentityServer(t)

	seedIAMRecord(t, state, "sts", "session:ASIAORPHANSESSION01",
		emulator.STSSessionCredentials{
			AccessKeyID:  "ASIAORPHANSESSION01",
			PrincipalARN: "arn:aws:sts::123456789012:assumed-role/worker/oldsess",
			AccountID:    "123456789012",
			Expiration:   time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC),
		})

	got := callerIdentityFor(t, srv, "ASIAORPHANSESSION01")
	assert.Equal(t, "worker/oldsess", got.UserID)
}

func TestGetCallerIdentity_APrincipalOfAKindWithNoPoliciesReportsItsName(t *testing.T) {
	// A principal whose ARN names no policy-holding entity at all — a federated user, a
	// service principal, the account root. iamEntityForPrincipalARN refuses these
	// deliberately, since substrate does not model them as entities, so there is no record
	// to read an ID from and the ARN's own name is the answer.
	srv, state := newCallerIdentityServer(t)

	seedIAMRecord(t, state, "sts", "session:ASIAFEDERATEDSESS01",
		emulator.STSSessionCredentials{
			AccessKeyID:  "ASIAFEDERATEDSESS01",
			PrincipalARN: "arn:aws:sts::123456789012:federated-user/Cathy",
			AccountID:    "123456789012",
			Expiration:   time.Date(2025, 1, 1, 13, 0, 0, 0, time.UTC),
		})

	got := callerIdentityFor(t, srv, "ASIAFEDERATEDSESS01")
	assert.Equal(t, "Cathy", got.UserID)
	assert.Equal(t, "arn:aws:sts::123456789012:federated-user/Cathy", got.ARN)
}

func TestGetCallerIdentity_APrincipalWithNoEntityReportsAName(t *testing.T) {
	// Substrate's own choice, stated rather than inherited: on AWS every caller has a
	// unique ID, so this case cannot arise there. It arises here for a credential that
	// resolves to a principal but to no IAM entity — a user deleted after a pre-#771 key
	// was minted for them. Reporting the friendly name keeps the value substrate published
	// before this change; an empty member would be indistinguishable from a bug to a
	// consumer asserting on UserId.
	srv, state := newCallerIdentityServer(t)

	seedIAMRecord(t, state, "iam", emulator.IAMAccessKeyKeyForTest("AKIADELETEDUSERKEY01"),
		emulator.IAMAccessKey{
			AccessKeyID: "AKIADELETEDUSERKEY01",
			Status:      "Active",
			UserName:    "ghost",
			AccountID:   "123456789012",
		})

	got := callerIdentityFor(t, srv, "AKIADELETEDUSERKEY01")
	assert.Equal(t, "ghost", got.UserID, "something that names the caller, rather than \"\"")
	assert.Equal(t, "arn:aws:iam::123456789012:user/ghost", got.ARN)
}
