package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #770: substrate authorized every IAM request against `arn:aws:iam::<account>:*`, a literal
// asterisk in the resource position that no statement naming a user, a role, a group or a path
// can match. So a policy scoped to `arn:aws:iam::123456789012:user/alice` granted nothing, and
// AWS's own IAMUserChangePassword — whose whole Resource is `user/${aws:username}` — allowed
// nothing even after #745 taught the evaluator to substitute the variable.
//
// The tests below are in four groups: the table's citation against AWS's own published data,
// the two policies the defect was reported through, the path the ARN embeds, and the invariant
// that both authorization doors decide one request against one resource.

// iamResourceAccount is the account every case here authorizes in.
const iamResourceAccount = "123456789012"

// iamResourcePath is a non-default path, which is the interesting case: an IAM ARN embeds the
// entity's path, only the Create* operations carry it on the wire, and every gate has the name
// in hand without the record. A resolver that minted from the name alone would answer
// `user/alice` for an entity stored at `/division/engineering/`.
const iamResourcePath = "/division/engineering/"

// iamResourceCaller is the user every case here authorizes *as*, and it is deliberately stored
// at the default path while every entity a request names carries [iamResourcePath].
//
// The split is not decoration, though the reason for it changed with #801. It was that a
// principal ARN carrying a path resolved to no IAM entity at all, so such a caller was
// unenforced and every assertion here would have passed vacuously. That gap is closed — a
// pathful caller is enforced, and `emulator/iam_principal_path_test.go` covers them — and the
// caller stays at the default path for a second reason the cases below depend on: a pathful
// caller's own resource ARN carries the path, so `user/${aws:username}` does not match it. That
// is AWS's behavior, which is why its IAMUserChangePassword names `user/*/${aws:username}` as
// well, and it would make the headline case here assert the path rule rather than #770's.
const iamResourceCaller = "caller"

// iamARNPlaceholder matches the trailing `${…WithPath}` in an AWS ARN format, the one component
// that stands for the resource's own name.
var iamARNPlaceholder = regexp.MustCompile(`\$\{[A-Za-z]+WithPath\}`)

// iamResourceMinters is how substrate builds an ARN for each resource type the table names,
// keyed the way AWS spells the type.
var iamResourceMinters = map[string]func(accountID, path, name string) string{
	"user":             emulator.IAMUserARNForTest,
	"role":             emulator.IAMRoleARNForTest,
	"group":            emulator.IAMGroupARNForTest,
	"policy":           emulator.IAMPolicyARNForTest,
	"instance-profile": emulator.IAMInstanceProfileARNForTest,
}

// TestIAMAuthzOperationResource_EveryRowMatchesWhatAWSPublishes is #770's citation, checked
// rather than asserted in prose.
//
// The table decides which resource a request is authorized against, so a row naming a type AWS
// does not publish for that action would make substrate refuse — or grant — against a resource
// AWS would never use. Both directions are covered: every row is one AWS publishes, and every
// action AWS publishes *no* resource types for is absent from the table, because such an action
// is authorized against the account wildcard and a row for it would narrow it wrongly.
//
// The data is the vendored Service Reference Information snapshot (#797), which is AWS's own
// machine-readable publication of the Service Authorization Reference — whose HTML pages render
// their tables in JavaScript and cannot be read.
func TestIAMAuthzOperationResource_EveryRowMatchesWhatAWSPublishes(t *testing.T) {
	t.Parallel()

	rows := emulator.IAMAuthzOperationResourceRowsForTest()
	require.NotEmpty(t, rows)

	t.Run("every row names a type AWS publishes for the action", func(t *testing.T) {
		t.Parallel()
		for _, row := range rows {
			types, ok := emulator.AuthzActionResourceTypesForTest("iam", row.Operation)
			require.True(t, ok, "%s is not an action AWS's reference publishes", row.Operation)
			if row.Type == "" {
				// A row with no Type carries a finished ARN whose type varies with the
				// caller's argument. AWS must still publish at least one type for the
				// action, or the request belongs on the account-wildcard path.
				assert.NotEmpty(t, types,
					"%s has no Type, so AWS must publish resource types for it", row.Operation)
				continue
			}
			assert.True(t,
				emulator.AuthzActionSupportsResourceTypeForTest("iam", row.Operation, row.Type),
				"AWS publishes %v for iam:%s, not %q", types, row.Operation, row.Type)
		}
	})

	t.Run("an action AWS scopes to the account is absent", func(t *testing.T) {
		t.Parallel()
		named := make(map[string]struct{}, len(rows))
		for _, row := range rows {
			named[row.Operation] = struct{}{}
		}
		for operation, types := range emulator.AuthzServiceActionResourcesForTest("iam") {
			if len(types) > 0 {
				continue
			}
			_, present := named[operation]
			assert.False(t, present,
				"AWS publishes no resource types for iam:%s, so it must stay on the account path",
				operation)
		}
	})

	t.Run("every minted ARN has the shape AWS documents", func(t *testing.T) {
		t.Parallel()
		for resourceType, mint := range iamResourceMinters {
			formats, ok := emulator.AuthzResourceARNFormatsForTest("iam", resourceType)
			require.True(t, ok, "AWS publishes no ARN format for iam %s", resourceType)
			require.Len(t, formats, 1, "%s: one format expected, got %v", resourceType, formats)

			// The placeholders are prose, not a template — substituting them is the test's
			// own reading, which is why it is done here and not in the resolver.
			want := strings.NewReplacer(
				"${Partition}", "aws",
				"${Account}", iamResourceAccount,
			).Replace(formats[0])
			want = iamARNPlaceholder.ReplaceAllString(want, "alice")
			assert.Equal(t, want, mint(iamResourceAccount, "/", "alice"),
				"%s: substrate's ARN must match AWS's published format", resourceType)
		}
	})

	t.Run("a type substrate mints for is one it can read a path for", func(t *testing.T) {
		t.Parallel()
		for _, row := range rows {
			if row.Type == "" {
				continue
			}
			_, ok := iamResourceMinters[row.Type]
			assert.True(t, ok, "no ARN minter for the %q rows (%s)", row.Type, row.Operation)
		}
	})
}

// newIAMResourceState returns a state holding the caller, alice, bob, a role, a group and an
// instance profile — every entity the cases below name — with policyARN attached to the caller.
//
// Every entity but the caller is stored at [iamResourcePath], which is what makes the fixture
// worth having: they are stored the way IAM stores them, so a resolver that ignored the record
// would produce a `/`-path ARN and every assertion about a path-scoped statement would fail.
func newIAMResourceState(t *testing.T, policyARN string, doc *emulator.PolicyDocument) emulator.StateManager {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	ctx := context.Background()

	put := func(key string, value any) {
		t.Helper()
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, state.Put(ctx, "iam", key, raw))
	}

	put(emulator.IAMUserKeyForTest(iamResourceAccount, iamResourceCaller), emulator.IAMUser{
		UserName: iamResourceCaller,
		UserID:   "AIDACALLER",
		ARN:      emulator.IAMUserARNForTest(iamResourceAccount, "/", iamResourceCaller),
		Path:     "/",
	})
	for _, name := range []string{"alice", "bob"} {
		put(emulator.IAMUserKeyForTest(iamResourceAccount, name), emulator.IAMUser{
			UserName: name,
			UserID:   "AIDA" + strings.ToUpper(name),
			ARN:      emulator.IAMUserARNForTest(iamResourceAccount, iamResourcePath, name),
			Path:     iamResourcePath,
		})
	}
	put(emulator.IAMRoleKeyForTest(iamResourceAccount, "worker"), emulator.IAMRole{
		RoleName: "worker",
		RoleID:   "AROAWORKER",
		ARN:      emulator.IAMRoleARNForTest(iamResourceAccount, iamResourcePath, "worker"),
		Path:     iamResourcePath,
	})
	put(emulator.IAMGroupKeyForTest(iamResourceAccount, "admins"), emulator.IAMGroup{
		GroupName: "admins",
		GroupID:   "AGPAADMINS",
		ARN:       emulator.IAMGroupARNForTest(iamResourceAccount, iamResourcePath, "admins"),
		Path:      iamResourcePath,
	})
	put(emulator.IAMInstanceProfileKeyForTest(iamResourceAccount, "app"), emulator.IAMInstanceProfile{
		InstanceProfileName: "app",
		InstanceProfileID:   "AIPAAPP",
		ARN:                 emulator.IAMInstanceProfileARNForTest(iamResourceAccount, iamResourcePath, "app"),
		Path:                iamResourcePath,
	})

	if policyARN != "" {
		put(emulator.IAMAttachedPoliciesKeyForTest(iamResourceAccount, "user", iamResourceCaller),
			[]string{policyARN})
		if doc != nil {
			put(emulator.IAMPolicyKeyForTest(policyARN), emulator.IAMPolicy{
				PolicyName:       "scoped",
				PolicyID:         "ANPASCOPED",
				ARN:              policyARN,
				Path:             "/",
				DefaultVersionID: "v1",
				IsAttachable:     true,
				Document:         *doc,
			})
		}
	}
	return state
}

// iamResourceCheckAccess decides one IAM request as the given principal against state.
func iamResourceCheckAccess(t *testing.T, state emulator.StateManager,
	principal *emulator.Principal, req *emulator.AWSRequest) error {
	t.Helper()
	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
	return auth.CheckAccess(&emulator.RequestContext{
		RequestID: "req-1",
		AccountID: iamResourceAccount,
		Region:    "us-east-1",
		Principal: principal,
		Metadata:  make(map[string]interface{}),
	}, req)
}

// iamResourcePrincipal is the caller, as a long-term IAM user credential resolves them.
func iamResourcePrincipal() *emulator.Principal {
	return &emulator.Principal{
		ARN:      emulator.IAMUserARNForTest(iamResourceAccount, "/", iamResourceCaller),
		Type:     "IAMUser",
		UserName: iamResourceCaller,
	}
}

// iamResourceNamelessPrincipal is the same caller with no recorded user name, which is what a
// role session resolves to.
//
// It is the control every "the caller is the resource" case needs: with no name there is no user
// ARN to mint, so the request falls back to the account wildcard and a statement scoped to a
// user matches nothing.
func iamResourceNamelessPrincipal() *emulator.Principal {
	return &emulator.Principal{
		ARN:  emulator.IAMUserARNForTest(iamResourceAccount, "/", iamResourceCaller),
		Type: "IAMUser",
	}
}

// TestCheckAccess_IAMUserChangePasswordGrantsTheCallersOwnPassword is #770's headline
// criterion, and it is asserted with **no hand-written policy**: the document is AWS's own
// IAMUserChangePassword as substrate bundles it.
//
// Its Resource is `arn:aws:iam::*:user/${aws:username}` and `arn:aws:iam::*:user/*/…`, so it
// grants nothing at all unless the request's resource is the caller's own user ARN. Against the
// old flat `arn:aws:iam::123456789012:*` it matched nothing, which is why #745's variable
// substitution left the policy still granting nothing (`docs/services.md`).
//
// ChangePassword carries no UserName — AWS: it "changes the password of the IAM user who is
// calling this operation" — so the caller's own user is the resource by documentation rather
// than by inference, which is what [emulator.IAMAuthzResourceForTest]'s CallerIsResource rows
// encode. The second case is the control: a caller substrate cannot name resolves to no user
// ARN, and the policy then grants nothing, so the first case is not passing on a wildcard.
func TestCheckAccess_IAMUserChangePasswordGrantsTheCallersOwnPassword(t *testing.T) {
	t.Parallel()

	const bundled = "arn:aws:iam::aws:policy/IAMUserChangePassword"

	cases := []struct {
		name      string
		principal *emulator.Principal
		allowed   bool
	}{
		{
			name:      "the caller's own password",
			principal: iamResourcePrincipal(),
			allowed:   true,
		},
		{
			name:      "a caller with no recorded user name",
			principal: iamResourceNamelessPrincipal(),
			allowed:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := newIAMResourceState(t, bundled, nil)
			err := iamResourceCheckAccess(t, state, tc.principal,
				&emulator.AWSRequest{Service: "iam", Operation: "ChangePassword"})
			if tc.allowed {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			assert.Equal(t, "AccessDenied", awsErr.Code)
		})
	}
}

// TestCheckAccess_IAMStatementScopedToOneUser is the other half of the defect, in the plainest
// shape a policy author writes: an Allow whose Resource names one user.
//
// Before #770 all three rows were denied, because the resource every one of them was decided
// against was `arn:aws:iam::123456789012:*`. The ListUsers row is the one that must stay denied:
// AWS publishes no resource types for it, so a statement scoped to one user grants no listing —
// and a resolver that answered "the caller's own user" for every unnamed operation would have
// turned this policy into a grant AWS does not give.
func TestCheckAccess_IAMStatementScopedToOneUser(t *testing.T) {
	t.Parallel()

	const policyARN = "arn:aws:iam::123456789012:policy/AliceOnly"
	aliceOnly := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect: emulator.IAMEffectAllow,
			Action: emulator.StringOrSlice{"iam:GetUser", "iam:ListUsers"},
			Resource: emulator.StringOrSlice{
				emulator.IAMUserARNForTest(iamResourceAccount, iamResourcePath, "alice"),
			},
		}},
	}

	cases := []struct {
		name      string
		operation string
		params    map[string]string
		allowed   bool
	}{
		{"GetUser on the named user", "GetUser", map[string]string{"UserName": "alice"}, true},
		{"GetUser on another user", "GetUser", map[string]string{"UserName": "bob"}, false},
		{
			// No UserName, so AWS's "This parameter is optional. If it is not included, it
			// defaults to the user making the request" applies: the resource is the caller's
			// own ARN, which this policy does not name — so the grant on alice does not leak
			// into a self-describing call.
			name:      "GetUser with no name is the caller, not the named user",
			operation: "GetUser",
			allowed:   false,
		},
		{"ListUsers names no resource", "ListUsers", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := newIAMResourceState(t, policyARN, &aliceOnly)
			err := iamResourceCheckAccess(t, state, iamResourcePrincipal(),
				&emulator.AWSRequest{Service: "iam", Operation: tc.operation, Params: tc.params})
			if tc.allowed {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
		})
	}
}

// TestIAMAuthzResource_PathComesFromTheEntityRecord pins the read the resolver exists to do.
//
// An IAM ARN embeds the entity's path and the gates hold only the name, so the path is read
// from the entity's own record. Minting from the name alone would answer `user/alice` for a
// user stored at `/division/engineering/`, which matches a statement scoped to the real ARN
// nowhere — and, worse, would match a Deny scoped to `user/*` that AWS would not apply.
//
// Both rows use the same request. The difference is only which ARN the policy names, so a
// resolver that dropped the path would flip both answers.
func TestIAMAuthzResource_PathComesFromTheEntityRecord(t *testing.T) {
	t.Parallel()

	const policyARN = "arn:aws:iam::123456789012:policy/ByPath"
	allowResource := func(resource string) *emulator.PolicyDocument {
		return &emulator.PolicyDocument{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"iam:GetRole"},
				Resource: emulator.StringOrSlice{resource},
			}},
		}
	}

	cases := []struct {
		name     string
		resource string
		allowed  bool
	}{
		{
			name:     "the stored path",
			resource: emulator.IAMRoleARNForTest(iamResourceAccount, iamResourcePath, "worker"),
			allowed:  true,
		},
		{
			name:     "the default path the name alone would produce",
			resource: emulator.IAMRoleARNForTest(iamResourceAccount, "/", "worker"),
			allowed:  false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := newIAMResourceState(t, policyARN, allowResource(tc.resource))
			err := iamResourceCheckAccess(t, state, iamResourcePrincipal(),
				&emulator.AWSRequest{
					Service:   "iam",
					Operation: "GetRole",
					Params:    map[string]string{"RoleName": "worker"},
				})
			if tc.allowed {
				assert.NoError(t, err)
				return
			}
			assert.Error(t, err)
		})
	}
}

// iamResourceDoorCase is one row of the both-doors sweep: a request, and the ARN both doors
// must decide it against.
type iamResourceDoorCase struct {
	operation string
	params    map[string]string
	want      string
}

// iamResourceDoorCases derives one case per table row, so a row added later is swept without
// this test being edited.
//
// The name it feeds each row is whichever entity of that type the fixture holds, and the
// expected ARN is minted independently of the resolver — from the type and the path the fixture
// stored — so the assertion is not two calls to one function agreeing with themselves.
func iamResourceDoorCases(t *testing.T) []iamResourceDoorCase {
	t.Helper()

	names := map[string]string{
		"user":             "alice",
		"role":             "worker",
		"group":            "admins",
		"instance-profile": "app",
	}
	cases := make([]iamResourceDoorCase, 0, len(emulator.IAMAuthzOperationResourceRowsForTest()))
	for _, row := range emulator.IAMAuthzOperationResourceRowsForTest() {
		param := ""
		if len(row.NameParams) > 0 {
			param = row.NameParams[0]
		}

		switch {
		case strings.HasSuffix(param, "Arn"):
			// PolicyArn and PolicySourceArn arrive finished, so the resolver must pass the
			// value through untouched — no record to read and no path to recover.
			arn := emulator.IAMPolicyARNForTest(iamResourceAccount, "/", "scoped")
			if row.Type == "" {
				arn = emulator.IAMUserARNForTest(iamResourceAccount, iamResourcePath, "alice")
			}
			cases = append(cases, iamResourceDoorCase{
				operation: row.Operation,
				params:    map[string]string{param: arn},
				want:      arn,
			})

		case row.Type == "policy":
			// Only CreatePolicy names a policy rather than carrying its ARN, and its Path is
			// on the wire — a customer-managed policy's record is keyed by the ARN the path
			// is already part of, so there is nothing to read back by name.
			cases = append(cases, iamResourceDoorCase{
				operation: row.Operation,
				params:    map[string]string{param: "scoped", "Path": iamResourcePath},
				want:      emulator.IAMPolicyARNForTest(iamResourceAccount, iamResourcePath, "scoped"),
			})

		case param != "":
			name, ok := names[row.Type]
			require.True(t, ok, "no fixture entity of type %q for %s", row.Type, row.Operation)
			mint, ok := iamResourceMinters[row.Type]
			require.True(t, ok, "no minter for type %q", row.Type)
			cases = append(cases, iamResourceDoorCase{
				operation: row.Operation,
				params:    map[string]string{param: name},
				want:      mint(iamResourceAccount, iamResourcePath, name),
			})

		default:
			// ChangePassword: no name on the wire at all, so the caller is the resource.
			require.True(t, row.CallerIsResource,
				"%s carries no name and does not name the caller", row.Operation)
			cases = append(cases, iamResourceDoorCase{
				operation: row.Operation,
				want:      emulator.IAMUserARNForTest(iamResourceAccount, "/", iamResourceCaller),
			})
		}
	}
	return cases
}

// TestIAMAuthzResource_BothDoorsAgree is #770's fourth criterion, and the reason the resolver
// takes the request rather than a handler's local name.
//
// An IAM request passes two gates: [emulator.AuthController.CheckAccess] before dispatch, and
// the IAM plugin's own gate inside the handler. They used to name different resources for one
// request — a literal `"*"` at 48 plugin call sites against `arn:aws:iam::<account>:*` at the
// generic gate — which is the one-request-two-answers failure #411, #714 and #745 are each an
// instance of. Both now ask one resolver, and this sweeps every row of the table to say so.
//
// The expected ARN is built from the fixture rather than from the resolver, so a resolver that
// answered consistently *and wrongly* fails here.
func TestIAMAuthzResource_BothDoorsAgree(t *testing.T) {
	t.Parallel()

	state := newIAMResourceState(t, "", nil)
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

	for _, tc := range iamResourceDoorCases(t) {
		t.Run(tc.operation, func(t *testing.T) {
			req := &emulator.AWSRequest{Service: "iam", Operation: tc.operation, Params: tc.params}
			gate := emulator.AuthzResourceARNsForTest(auth, reqCtx, req)
			door := emulator.IAMAuthzResourceForTest(plugin, reqCtx, req)

			assert.Equal(t, []string{tc.want}, gate, "the generic gate's resource")
			assert.Equal(t, tc.want, door, "the plugin door's resource")
		})
	}
}

// TestIAMAuthzResource_UnresolvedOperationsStayOnTheAccountPath pins the fallback, which is the
// half of the change that must *not* narrow anything.
//
// An operation AWS publishes no resource types for, one substrate does not route at all, and one
// whose name parameter the request left empty all authorize against every IAM resource in the
// account. That string is deliberately not a bare `*`: an empty or bare-wildcard resource is
// what makes a statement match unconditionally, which is harmless for an Allow and wrong for a
// Deny.
func TestIAMAuthzResource_UnresolvedOperationsStayOnTheAccountPath(t *testing.T) {
	t.Parallel()

	state := newIAMResourceState(t, "", nil)
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	auth := emulator.NewAuthController(state, logger)

	plugin := &emulator.IAMPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(),
		emulator.PluginConfig{State: state, Logger: logger}))

	want := emulator.IAMAuthzAccountResourceARNForTest(iamResourceAccount)
	require.Equal(t, "arn:aws:iam::"+iamResourceAccount+":*", want)

	cases := []struct {
		name      string
		operation string
		params    map[string]string
		principal *emulator.Principal
	}{
		{"an action AWS scopes to the account", "ListUsers", nil, iamResourcePrincipal()},
		{"an action substrate does not route", "GetAccountSummary", nil, iamResourcePrincipal()},
		{"a named operation with no name", "GetRole", nil, iamResourcePrincipal()},
		{
			name:      "a caller-is-resource operation with no caller name",
			operation: "ChangePassword",
			principal: iamResourceNamelessPrincipal(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reqCtx := &emulator.RequestContext{
				RequestID: "req-1",
				AccountID: iamResourceAccount,
				Region:    "us-east-1",
				Principal: tc.principal,
				Metadata:  make(map[string]interface{}),
			}
			req := &emulator.AWSRequest{Service: "iam", Operation: tc.operation, Params: tc.params}
			assert.Equal(t, []string{want}, emulator.AuthzResourceARNsForTest(auth, reqCtx, req))
			assert.Equal(t, want, emulator.IAMAuthzResourceForTest(plugin, reqCtx, req))
		})
	}
}

// newIAMPluginDoorServer returns a server with the IAM plugin registered and **no**
// [emulator.AuthController] wired.
//
// That omission is the point: the server resolves the caller from the access key either way
// (see resolvePrincipal), but skips the generic gate, so the only door left is the IAM plugin's
// own. A test that ran with both wired would pass on the generic gate's verdict and say nothing
// about whether the handler is gated — which is exactly what was missing for the six
// instance-profile operations.
func newIAMPluginDoorServer(t *testing.T) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))

	plugin := &emulator.IAMPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(),
		emulator.PluginConfig{State: state, Logger: logger}))
	registry.Register(plugin)

	return emulator.NewServer(*cfg, registry, store, state, tc, logger, emulator.ServerOptions{})
}

// TestIAMPlugin_InstanceProfileOperationsAreGated covers the six handlers that called no
// authorization gate at all.
//
// The generic gate ran upstream of them, so a request was decided once rather than not at all —
// but "both doors agree" cannot be asserted for `instance-profile` while one door is missing,
// and AddRoleToInstanceProfile is the classic privilege-escalation step: attaching a more
// privileged role to a profile an instance already carries. A policy scoped to one profile now
// governs it, which needs both the gate and the resource #770 resolves.
//
// The server here wires no AuthController, so every verdict below is the plugin door's own. The
// setup calls use the unenforced AKIA key, so the policy under test cannot block the fixture
// that creates it.
func TestIAMPlugin_InstanceProfileOperationsAreGated(t *testing.T) {
	srv := newIAMPluginDoorServer(t)

	for _, name := range []string{"app", "other"} {
		require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "CreateInstanceProfile",
			map[string]any{"InstanceProfileName": name}).StatusCode)
	}
	require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "CreateRole",
		map[string]any{"RoleName": "worker"}).StatusCode)

	keyID := trustSetupCaller(t, srv, "alice")
	require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "PutUserPolicy", map[string]any{
		"UserName":   "alice",
		"PolicyName": "app-profile-only",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Action":["iam:AddRoleToInstanceProfile","iam:GetInstanceProfile"],` +
			`"Resource":"arn:aws:iam::123456789012:instance-profile/app"}]}`,
	}).StatusCode)

	call := func(t *testing.T, operation string, body any) int {
		t.Helper()
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
		r.Host = "iam.amazonaws.com"
		r.Header.Set("X-Amz-Target", "AmazonIdentityManagementService."+operation)
		r.Header.Set("Content-Type", "application/x-amz-json-1.1")
		r.Header.Set("Authorization", trustAuthHeader(keyID, "iam"))
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		resp := w.Result()
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode
	}

	cases := []struct {
		name       string
		operation  string
		body       any
		wantStatus int
	}{
		{
			name:       "GetInstanceProfile on the named profile",
			operation:  "GetInstanceProfile",
			body:       map[string]any{"InstanceProfileName": "app"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetInstanceProfile on another profile",
			operation:  "GetInstanceProfile",
			body:       map[string]any{"InstanceProfileName": "other"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "AddRoleToInstanceProfile on the named profile",
			operation:  "AddRoleToInstanceProfile",
			body:       map[string]any{"InstanceProfileName": "app", "RoleName": "worker"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "AddRoleToInstanceProfile on another profile",
			operation:  "AddRoleToInstanceProfile",
			body:       map[string]any{"InstanceProfileName": "other", "RoleName": "worker"},
			wantStatus: http.StatusForbidden,
		},
		{
			// The remaining four are granted by nothing, and every one of them was ungated.
			// RemoveRoleFromInstanceProfile is the other half of the escalation pair:
			// detaching a role is how a caller would strip the profile an instance depends on.
			name:       "RemoveRoleFromInstanceProfile is refused",
			operation:  "RemoveRoleFromInstanceProfile",
			body:       map[string]any{"InstanceProfileName": "app", "RoleName": "worker"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "DeleteInstanceProfile is refused",
			operation:  "DeleteInstanceProfile",
			body:       map[string]any{"InstanceProfileName": "app"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "CreateInstanceProfile is refused",
			operation:  "CreateInstanceProfile",
			body:       map[string]any{"InstanceProfileName": "third"},
			wantStatus: http.StatusForbidden,
		},
		{
			// AWS publishes no resource types for the list, so it is decided against every
			// IAM resource in the account — which a policy naming one profile does not cover.
			name:       "ListInstanceProfiles is refused",
			operation:  "ListInstanceProfiles",
			body:       map[string]any{},
			wantStatus: http.StatusForbidden,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantStatus, call(t, tc.operation, tc.body))
		})
	}
}
