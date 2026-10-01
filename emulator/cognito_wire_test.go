package emulator_test

import (
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cognitoWireClock is the instant the two setups below start the simulated clock at, so every
// projected timestamp has a known value. Non-zero on purpose: cognitoTimeOrNil omits the zero time,
// and a test whose clock was the zero time would see every timestamp member absent and call that a
// pass.
//
// The controller advances a small deterministic step per observation rather than standing still, so
// the assertions below bound the value rather than equate it — a second's tolerance over a run that
// makes a few dozen observations, and still nothing read off the wall clock.
var cognitoWireClock = time.Unix(1700000000, 0).UTC()

const (
	cognitoWireAccount = "123456789012"
	cognitoWireRegion  = "us-east-1"
)

// setupCognitoWirePlugin returns the Cognito IDP plugin, a request context and the state manager
// behind it.
//
// The state manager is handed back so TestCognitoWire_ProjectionLeavesTheRecordIntact can read the
// records the responses are projected from. The other Cognito tests also call HandleRequest directly;
// this file does the same, because what is under test is the bytes of a response body and a server
// adds nothing to those.
func setupCognitoWirePlugin(t *testing.T) (*emulator.CognitoIDPPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.CognitoIDPPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(cognitoWireClock)},
	}), "emulator.CognitoIDPPlugin.Initialize")
	return p, cognitoWireContext("req-cognito-idp-wire"), state
}

// setupCognitoIdentityWirePlugin is setupCognitoWirePlugin for the 2014-06-30 identity-pool service,
// which is a separate plugin on a separate host with its own state namespace.
func setupCognitoIdentityWirePlugin(t *testing.T) (*emulator.CognitoIdentityPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.CognitoIdentityPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(cognitoWireClock)},
	}), "emulator.CognitoIdentityPlugin.Initialize")
	return p, cognitoWireContext("req-cognito-identity-wire"), state
}

// cognitoWireContext builds the request context both setups hand back.
func cognitoWireContext(requestID string) *emulator.RequestContext {
	return &emulator.RequestContext{
		AccountID: cognitoWireAccount,
		Region:    cognitoWireRegion,
		RequestID: requestID,
	}
}

// cognitoWire issues op against either Cognito plugin and returns the raw response body, failing the
// test on anything but 200.
//
// Raw bytes rather than a decoded struct on purpose: what is under test is which members the body
// has, and a decode into a Go type is exactly the step that hides an extra one.
func cognitoWire(t *testing.T, p emulator.Plugin, ctx *emulator.RequestContext, service, op string, body map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err, "marshal %s body", op)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   service,
		Operation: op,
		Body:      raw,
	})
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// cognitoWireIDP and cognitoWireIdentity are cognitoWire bound to each service's name.
func cognitoWireIDP(t *testing.T, p *emulator.CognitoIDPPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	return cognitoWire(t, p, ctx, "cognito-idp", op, body)
}

func cognitoWireIdentity(t *testing.T, p *emulator.CognitoIdentityPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	return cognitoWire(t, p, ctx, "cognito-identity", op, body)
}

// cognitoWireOpaque names the published members whose own keys are caller data rather than shape, so
// the walker below reports them as one member each instead of descending into them.
//
// Without this a pool tagged `env=prod` would report a member named `UserPoolTags/env`, and the
// expectation would have to enumerate the test's own inputs — which would make the assertion about
// the fixture rather than about the shape. Policies, LambdaConfig and the Schema entries are
// `interface{}` passthroughs on the record for the same reason: substrate stores what the caller sent
// and does not model their interiors.
var cognitoWireOpaque = []string{
	"IdentityPoolTags", "LambdaConfig", "Policies", "Roles", "SchemaAttributes", "UserPoolTags",
}

// cognitoWireMembers walks a decoded response object and returns every member path inside it.
//
// A nested object contributes both its own name and its members' paths ("Attributes[]/Name"), so an
// extra member anywhere under the record fails, not only at the top. A list contributes the union of
// its items' paths, so an extra member on any item of a page fails, not just on the first.
func cognitoWireMembers(obj map[string]any) []string {
	out := map[string]bool{}
	cognitoWireWalk("", obj, out)
	return slices.Sorted(maps.Keys(out))
}

// cognitoWireWalk collects obj's member paths into out, prefixed by prefix.
func cognitoWireWalk(prefix string, obj map[string]any, out map[string]bool) {
	for name, value := range obj {
		path := prefix + name
		out[path] = true
		if slices.Contains(cognitoWireOpaque, name) {
			continue
		}
		switch v := value.(type) {
		case map[string]any:
			cognitoWireWalk(path+"/", v, out)
		case []any:
			for _, item := range v {
				if nested, ok := item.(map[string]any); ok {
					cognitoWireWalk(path+"[]/", nested, out)
				}
			}
		}
	}
}

// cognitoWireObject returns the record object a response carries under member, decoded.
func cognitoWireObject(t *testing.T, op string, body []byte, member string) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out), "%s: decode response: %s", op, body)
	raw, ok := out[member]
	require.True(t, ok, "%s must answer a %s member: %s", op, member, body)
	record, ok := raw.(map[string]any)
	require.True(t, ok, "%s: %s is not an object: %s", op, member, body)
	return record
}

// cognitoWireOnlyListed returns the single record object a list response carries under member,
// requiring that the list holds exactly one.
func cognitoWireOnlyListed(t *testing.T, op string, body []byte, member string) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out), "%s: decode response: %s", op, body)
	raw, ok := out[member]
	require.True(t, ok, "%s must answer a %s member: %s", op, member, body)
	list, ok := raw.([]any)
	require.True(t, ok, "%s: %s is not a list: %s", op, member, body)
	require.Len(t, list, 1, "%s must answer one record: %s", op, body)
	record, ok := list[0].(map[string]any)
	require.True(t, ok, "%s: %s[0] is not an object: %s", op, member, body)
	return record
}

// cognitoWireAssertMembers requires that a record object's members are exactly the published ones.
//
// Equality rather than a set of NotContains assertions, which is what this block needs over an
// absence check: five of the members #756 removed from these shapes are outside
// scripts/check-wire-bookkeeping.sh's AccountID/Region/CreatedAt/UpdatedAt/EverTagged list, and a
// *rename* — UserPoolId for the published Id, Tags for UserPoolTags — is invisible to an absence
// check altogether. Equality also cannot go vacuous the way an absence check on a member the body
// could not have carried does.
func cognitoWireAssertMembers(t *testing.T, op string, record map[string]any, want []string) {
	t.Helper()
	assert.Equal(t, want, cognitoWireMembers(record),
		"%s must carry exactly the published members", op)
}

// cognitoWireTime requires a projected timestamp to be a JSON number carrying the simulated clock's
// instant, within a tolerance.
//
// A number rather than a string is the whole point of the epoch change: every one of these members is
// documented "Amazon Cognito returns this timestamp in UNIX epoch time format", and a Go time.Time
// marshals to a quoted RFC3339 string the SDK's decoder rejects. Bounded rather than equated because
// TimeController advances a deterministic step per observation; a test demanding the seeded instant
// exactly fails on the second request of a run.
func cognitoWireTime(t *testing.T, op string, record map[string]any, member string, offset time.Duration) {
	t.Helper()
	raw, ok := record[member]
	require.True(t, ok, "%s must report %s", op, member)
	secs, ok := raw.(float64)
	require.True(t, ok, "%s: %s must be a JSON number in epoch seconds, got %T (%v)", op, member, raw, raw)
	assert.WithinDuration(t, cognitoWireClock.Add(offset), time.Unix(0, int64(secs*1e9)), time.Second,
		"%s: %s", op, member)
}

// cognitoWireRecord returns the record at key as raw JSON, so a member with no published home can be
// read without a Go type deciding which members exist.
func cognitoWireRecord(t *testing.T, state emulator.StateManager, namespace, key string) map[string]json.RawMessage {
	t.Helper()
	data, err := state.Get(t.Context(), namespace, key)
	require.NoError(t, err, "state.Get %s/%s", namespace, key)
	require.NotNil(t, data, "no record stored at %s/%s", namespace, key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

// cognitoWireCreatePool creates one user pool with every member of UserPoolType that substrate models
// populated, and returns its published id.
//
// Every optional member is given a value on purpose: Policies, LambdaConfig, SchemaAttributes and
// UserPoolTags are all `,omitempty` in the projection, so a pool created bare would report none of
// them and the equality assertion would be weaker than it looks.
func cognitoWireCreatePool(t *testing.T, p *emulator.CognitoIDPPlugin, ctx *emulator.RequestContext, name string) string {
	t.Helper()
	body := cognitoWireIDP(t, p, ctx, "CreateUserPool", cognitoWirePoolInput(name))
	record := cognitoWireObject(t, "CreateUserPool", body, "UserPool")
	id, ok := record["Id"].(string)
	require.True(t, ok, "CreateUserPool must report Id as a string: %s", body)
	require.NotEmpty(t, id)
	return id
}

// cognitoWirePoolInput is the CreateUserPool request cognitoWireCreatePool sends.
func cognitoWirePoolInput(name string) map[string]any {
	return map[string]any{
		"PoolName":         name,
		"MfaConfiguration": "ON",
		"Policies":         map[string]any{"PasswordPolicy": map[string]any{"MinimumLength": 12}},
		"LambdaConfig":     map[string]any{"PreSignUp": "arn:aws:lambda:us-east-1:123456789012:function:pre"},
		"Schema":           []any{map[string]any{"Name": "email", "Required": true}},
		"UserPoolTags":     map[string]any{"env": "prod"},
	}
}

// cognitoWirePoolMembers is API_UserPoolType's members, less the twenty-five substrate does not
// model. Id and UserPoolTags are the two names #756 corrected: the record tagged them UserPoolId and
// Tags, neither of which UserPoolType publishes.
var cognitoWirePoolMembers = []string{
	"Arn", "CreationDate", "Id", "LambdaConfig", "LastModifiedDate", "MfaConfiguration",
	"Name", "Policies", "SchemaAttributes", "Status", "UserPoolTags",
}

// TestCognitoWire_UserPoolResponsesCarryOnlyPublishedMembers covers both sites that answer a CognitoUserPool.
//
// The record carries AccountID, Region, ProviderName and CreationDate, and reached the wire whole
// before #756 — CreateUserPool's response struct embedded it. Neither bookkeeping member is
// `,omitempty`, so an unprojected record reports both unconditionally and the equality assertion is
// non-vacuous at both sites. `ever_tagged` is `,omitempty` and is covered by
// TestCognitoWire_UserPoolOmitsEverTaggedOnceItIsSet, which tags first so the flag is actually true;
// that is the test scripts/wire-bookkeeping-projected.txt cites for this record, for that reason.
func TestCognitoWire_UserPoolResponsesCarryOnlyPublishedMembers(t *testing.T) {
	p, ctx, _ := setupCognitoWirePlugin(t)
	want := cognitoWirePoolMembers

	createBody := cognitoWireIDP(t, p, ctx, "CreateUserPool", cognitoWirePoolInput("wire-pool"))
	created := cognitoWireObject(t, "CreateUserPool", createBody, "UserPool")
	cognitoWireAssertMembers(t, "CreateUserPool", created, want)
	cognitoWireTime(t, "CreateUserPool", created, "CreationDate", 0)
	cognitoWireTime(t, "CreateUserPool", created, "LastModifiedDate", 0)
	poolID, ok := created["Id"].(string)
	require.True(t, ok, "CreateUserPool must report Id as a string: %s", createBody)
	assert.Equal(t, cognitoWireRegion+"_", poolID[:len(cognitoWireRegion)+1],
		"the published Id is the minted {region}_{suffix} pool id")

	describeBody := cognitoWireIDP(t, p, ctx, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	described := cognitoWireObject(t, "DescribeUserPool", describeBody, "UserPool")
	cognitoWireAssertMembers(t, "DescribeUserPool", described, want)
	cognitoWireTime(t, "DescribeUserPool", described, "CreationDate", 0)
	assert.Equal(t, poolID, described["Id"], "DescribeUserPool reports the same Id CreateUserPool did")

	// ListUserPools is API_UserPoolDescriptionType rather than UserPoolType, so it has its own
	// expectation — and asserting it here is the agreement half of #1286: before it, the summary
	// reported `Id` while the two operations above reported `UserPoolId`, so one service answered the
	// same identifier under two member names depending on which door a caller knocked on.
	listBody := cognitoWireIDP(t, p, ctx, "ListUserPools", map[string]any{"MaxResults": 10})
	listed := cognitoWireOnlyListed(t, "ListUserPools", listBody, "UserPools")
	cognitoWireAssertMembers(t, "ListUserPools", listed,
		[]string{"CreationDate", "Id", "LambdaConfig", "LastModifiedDate", "Name", "Status"})
	cognitoWireTime(t, "ListUserPools", listed, "CreationDate", 0)
	assert.Equal(t, poolID, listed["Id"], "ListUserPools agrees with CreateUserPool on the member name")
}

// TestCognitoWire_UserPoolClientResponsesCarryOnlyPublishedMembers covers all three sites that answer a CognitoUserPoolClient.
//
// UpdateUserPoolClient is included because it answers the full shape rather than an empty body, so it
// leaked the same two members the create did.
func TestCognitoWire_UserPoolClientResponsesCarryOnlyPublishedMembers(t *testing.T) {
	p, ctx, _ := setupCognitoWirePlugin(t)
	poolID := cognitoWireCreatePool(t, p, ctx, "client-wire-pool")

	// API_UserPoolClientType's members, less the twenty substrate does not model. UserPoolId stays
	// here, unlike on UserType: UserPoolClientType does publish it.
	want := []string{"ClientId", "ClientName", "ClientSecret", "CreationDate", "ExplicitAuthFlows", "UserPoolId"}
	input := map[string]any{
		"UserPoolId":        poolID,
		"ClientName":        "wire-client",
		"GenerateSecret":    true,
		"ExplicitAuthFlows": []any{"ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"},
	}

	createBody := cognitoWireIDP(t, p, ctx, "CreateUserPoolClient", input)
	created := cognitoWireObject(t, "CreateUserPoolClient", createBody, "UserPoolClient")
	cognitoWireAssertMembers(t, "CreateUserPoolClient", created, want)
	cognitoWireTime(t, "CreateUserPoolClient", created, "CreationDate", 0)
	clientID, ok := created["ClientId"].(string)
	require.True(t, ok, "CreateUserPoolClient must report ClientId as a string: %s", createBody)

	describeBody := cognitoWireIDP(t, p, ctx, "DescribeUserPoolClient",
		map[string]any{"UserPoolId": poolID, "ClientId": clientID})
	described := cognitoWireObject(t, "DescribeUserPoolClient", describeBody, "UserPoolClient")
	cognitoWireAssertMembers(t, "DescribeUserPoolClient", described, want)
	cognitoWireTime(t, "DescribeUserPoolClient", described, "CreationDate", 0)

	updateBody := cognitoWireIDP(t, p, ctx, "UpdateUserPoolClient", map[string]any{
		"UserPoolId":        poolID,
		"ClientId":          clientID,
		"ClientName":        "wire-client-renamed",
		"ExplicitAuthFlows": []any{"ALLOW_USER_PASSWORD_AUTH"},
	})
	updated := cognitoWireObject(t, "UpdateUserPoolClient", updateBody, "UserPoolClient")
	cognitoWireAssertMembers(t, "UpdateUserPoolClient", updated, want)
	cognitoWireTime(t, "UpdateUserPoolClient", updated, "CreationDate", 0)

	// ListUserPoolClients answers API_UserPoolClientDescription, which has no timestamp at all. It
	// already built its own summary before #756; this is the pin that it stays that way.
	listBody := cognitoWireIDP(t, p, ctx, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
	listed := cognitoWireOnlyListed(t, "ListUserPoolClients", listBody, "UserPoolClients")
	cognitoWireAssertMembers(t, "ListUserPoolClients", listed, []string{"ClientId", "ClientName", "UserPoolId"})
}

// TestCognitoWire_GroupResponsesCarryOnlyPublishedMembers covers all four sites that answer a CognitoGroup.
func TestCognitoWire_GroupResponsesCarryOnlyPublishedMembers(t *testing.T) {
	p, ctx, _ := setupCognitoWirePlugin(t)
	poolID := cognitoWireCreatePool(t, p, ctx, "group-wire-pool")

	// API_GroupType's members, less LastModifiedDate, which no Cognito record but the user pool keeps.
	want := []string{"CreationDate", "Description", "GroupName", "Precedence", "RoleArn", "UserPoolId"}
	input := map[string]any{
		"UserPoolId":  poolID,
		"GroupName":   "wire-admins",
		"Description": "the group this test projects",
		"RoleArn":     "arn:aws:iam::123456789012:role/wire-admins",
		"Precedence":  3,
	}

	createBody := cognitoWireIDP(t, p, ctx, "CreateGroup", input)
	created := cognitoWireObject(t, "CreateGroup", createBody, "Group")
	cognitoWireAssertMembers(t, "CreateGroup", created, want)
	cognitoWireTime(t, "CreateGroup", created, "CreationDate", 0)
	assert.Equal(t, float64(3), created["Precedence"], "the stored precedence, as a JSON number")

	getBody := cognitoWireIDP(t, p, ctx, "GetGroup", map[string]any{"UserPoolId": poolID, "GroupName": "wire-admins"})
	got := cognitoWireObject(t, "GetGroup", getBody, "Group")
	cognitoWireAssertMembers(t, "GetGroup", got, want)
	cognitoWireTime(t, "GetGroup", got, "CreationDate", 0)

	listBody := cognitoWireIDP(t, p, ctx, "ListGroups", map[string]any{"UserPoolId": poolID})
	listed := cognitoWireOnlyListed(t, "ListGroups", listBody, "Groups")
	cognitoWireAssertMembers(t, "ListGroups", listed, want)
	cognitoWireTime(t, "ListGroups", listed, "CreationDate", 0)

	// AdminListGroupsForUser is the fourth site, and the one that makes CognitoUser.Groups unnecessary
	// on the wire: this is the published operation for reporting a user's groups.
	cognitoWireIDP(t, p, ctx, "AdminCreateUser", map[string]any{"UserPoolId": poolID, "Username": "wire-member"})
	cognitoWireIDP(t, p, ctx, "AdminAddUserToGroup",
		map[string]any{"UserPoolId": poolID, "Username": "wire-member", "GroupName": "wire-admins"})
	forUserBody := cognitoWireIDP(t, p, ctx, "AdminListGroupsForUser",
		map[string]any{"UserPoolId": poolID, "Username": "wire-member"})
	forUser := cognitoWireOnlyListed(t, "AdminListGroupsForUser", forUserBody, "Groups")
	cognitoWireAssertMembers(t, "AdminListGroupsForUser", forUser, want)
	cognitoWireTime(t, "AdminListGroupsForUser", forUser, "CreationDate", 0)
}

// TestCognitoWire_UserResponsesCarryOnlyPublishedMembers covers both sites that answer a CognitoUser, plus the two AdminGetUser
// timestamps that are the same change at a site which already projected.
//
// The record carries UserPoolID and Groups as well as AccountID and Region, and API_UserType
// publishes none of the four, so the equality assertion here removes more than the bookkeeping pair.
func TestCognitoWire_UserResponsesCarryOnlyPublishedMembers(t *testing.T) {
	p, ctx, _ := setupCognitoWirePlugin(t)
	poolID := cognitoWireCreatePool(t, p, ctx, "user-wire-pool")

	// API_UserType's members, less MFAOptions, which substrate records no enrollment for.
	want := []string{
		"Attributes", "Attributes[]/Name", "Attributes[]/Value", "Enabled", "UserCreateDate",
		"UserLastModifiedDate", "UserStatus", "Username",
	}
	input := map[string]any{
		"UserPoolId":     poolID,
		"Username":       "wire-user",
		"UserAttributes": []any{map[string]any{"Name": "email", "Value": "wire@example.com"}},
	}

	createBody := cognitoWireIDP(t, p, ctx, "AdminCreateUser", input)
	created := cognitoWireObject(t, "AdminCreateUser", createBody, "User")
	cognitoWireAssertMembers(t, "AdminCreateUser", created, want)
	cognitoWireTime(t, "AdminCreateUser", created, "UserCreateDate", 0)
	cognitoWireTime(t, "AdminCreateUser", created, "UserLastModifiedDate", 0)

	listBody := cognitoWireIDP(t, p, ctx, "ListUsers", map[string]any{"UserPoolId": poolID})
	listed := cognitoWireOnlyListed(t, "ListUsers", listBody, "Users")
	cognitoWireAssertMembers(t, "ListUsers", listed, want)
	cognitoWireTime(t, "ListUsers", listed, "UserCreateDate", 0)

	// AdminGetUser reports the user's members at the top level rather than under a User, and under
	// UserAttributes rather than Attributes, so the whole response body is the record object here.
	// It built its own shape before #756 and only the two timestamps changed; its Response Syntax
	// states them as `number`, with a sample response of 1.682955829578E9.
	getBody := cognitoWireIDP(t, p, ctx, "AdminGetUser", map[string]any{"UserPoolId": poolID, "Username": "wire-user"})
	var got map[string]any
	require.NoError(t, json.Unmarshal(getBody, &got), "AdminGetUser: decode response: %s", getBody)
	cognitoWireAssertMembers(t, "AdminGetUser", got, []string{
		"Enabled", "UserAttributes", "UserAttributes[]/Name", "UserAttributes[]/Value",
		"UserCreateDate", "UserLastModifiedDate", "UserStatus", "Username",
	})
	cognitoWireTime(t, "AdminGetUser", got, "UserCreateDate", 0)
	cognitoWireTime(t, "AdminGetUser", got, "UserLastModifiedDate", 0)
}

// TestCognitoWire_IdentityPoolResponsesCarryOnlyPublishedMembers covers all four sites that answer a CognitoIdentityPool, plus
// GetCredentialsForIdentity's Expiration.
//
// This record is the pin-only one: every site already built a local response struct before #756, so
// its two bookkeeping members were reachable-by-declaration with nothing asserting they stayed shut —
// the position ECS's records were in at #1308. The equality assertions below are that assertion.
func TestCognitoWire_IdentityPoolResponsesCarryOnlyPublishedMembers(t *testing.T) {
	p, ctx, _ := setupCognitoIdentityWirePlugin(t)

	createBody := cognitoWireIdentity(t, p, ctx, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "wire-identity-pool",
		"AllowUnauthenticatedIdentities": true,
		"IdentityPoolTags":               map[string]any{"env": "prod"},
	})
	var created map[string]any
	require.NoError(t, json.Unmarshal(createBody, &created), "CreateIdentityPool: decode: %s", createBody)
	cognitoWireAssertMembers(t, "CreateIdentityPool", created, []string{
		"AllowUnauthenticatedIdentities", "IdentityPoolId", "IdentityPoolName", "IdentityPoolTags",
	})
	poolID, ok := created["IdentityPoolId"].(string)
	require.True(t, ok, "CreateIdentityPool must report IdentityPoolId as a string: %s", createBody)

	// Roles is set before the describe on purpose: it was in DescribeIdentityPool's response until
	// #756 and API_DescribeIdentityPool does not publish it, so a pool with no roles would make its
	// absence here vacuous.
	cognitoWireIdentity(t, p, ctx, "SetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": poolID,
		"Roles":          map[string]any{"authenticated": "arn:aws:iam::123456789012:role/wire-auth"},
	})

	describeBody := cognitoWireIdentity(t, p, ctx, "DescribeIdentityPool", map[string]any{"IdentityPoolId": poolID})
	var described map[string]any
	require.NoError(t, json.Unmarshal(describeBody, &described), "DescribeIdentityPool: decode: %s", describeBody)
	cognitoWireAssertMembers(t, "DescribeIdentityPool", described, []string{
		"AllowUnauthenticatedIdentities", "IdentityPoolId", "IdentityPoolName", "IdentityPoolTags",
	})

	// GetIdentityPoolRoles is where the role mapping is published, which is what makes dropping it
	// from the describe a projection rather than a loss.
	rolesBody := cognitoWireIdentity(t, p, ctx, "GetIdentityPoolRoles", map[string]any{"IdentityPoolId": poolID})
	var roles map[string]any
	require.NoError(t, json.Unmarshal(rolesBody, &roles), "GetIdentityPoolRoles: decode: %s", rolesBody)
	cognitoWireAssertMembers(t, "GetIdentityPoolRoles", roles, []string{"IdentityPoolId", "Roles"})
	assert.Equal(t, map[string]any{"authenticated": "arn:aws:iam::123456789012:role/wire-auth"},
		roles["Roles"], "the roles the describe no longer reports are still answered here")

	listBody := cognitoWireIdentity(t, p, ctx, "ListIdentityPools", map[string]any{"MaxResults": 10})
	listed := cognitoWireOnlyListed(t, "ListIdentityPools", listBody, "IdentityPools")
	cognitoWireAssertMembers(t, "ListIdentityPools", listed, []string{"IdentityPoolId", "IdentityPoolName"})

	// API_Credentials' four members. Expiration was an RFC3339 string until #756 and is the one
	// timestamp in this service — and the only member in either that is not the clock itself, since
	// the credentials expire an hour out.
	credsBody := cognitoWireIdentity(t, p, ctx, "GetCredentialsForIdentity", map[string]any{"IdentityId": poolID})
	creds := cognitoWireObject(t, "GetCredentialsForIdentity", credsBody, "Credentials")
	cognitoWireAssertMembers(t, "GetCredentialsForIdentity", creds,
		[]string{"AccessKeyId", "Expiration", "SecretKey", "SessionToken"})
	cognitoWireTime(t, "GetCredentialsForIdentity", creds, "Expiration", time.Hour)
}

// TestCognitoWire_EverTaggedIsNotReported is the one subtest that needs a tagged resource.
//
// `ever_tagged` is `,omitempty` on CognitoUserPool, so it is absent from an untagged pool's body
// whatever the projection does — asserting its absence without tagging first would be vacuous. No
// cognito-idp operation sets it (there is no tag operation on that plugin at all), so the only door is
// the Resource Groups Tagging API's TagResources, which tagging_plugin.go routes to the pool through
// scanCognitoUserPools. That is why this subtest runs a second plugin over the same state manager
// rather than the Cognito plugin alone.
//
// This is the test scripts/wire-bookkeeping-projected.txt cites for CognitoUserPool, on the rule that
// file's header states: a record declaring EverTagged is cited to the test that sets the flag before
// asserting its absence. So it repeats the equality assertion rather than only the three named
// absences, to be non-vacuous for every member the record declares on its own.
func TestCognitoWire_UserPoolOmitsEverTaggedOnceItIsSet(t *testing.T) {
	p, ctx, state := setupCognitoWirePlugin(t)
	tagging := &emulator.TaggingPlugin{}
	require.NoError(t, tagging.Initialize(t.Context(), emulator.PluginConfig{
		State:  state,
		Logger: emulator.NewDefaultLogger(slog.LevelError, false),
	}), "emulator.TaggingPlugin.Initialize")

	poolID := cognitoWireCreatePool(t, p, ctx, "ever-tagged-pool")
	arn := "arn:aws:cognito-idp:" + cognitoWireRegion + ":" + cognitoWireAccount + ":userpool/" + poolID

	cognitoWireTagging(t, tagging, ctx, "TagResources", map[string]any{
		"ResourceARNList": []any{arn},
		"Tags":            map[string]any{"wire": "yes"},
	})

	// The anchor: GetResources proves the tag landed through the scanner that reads EverTagged off the
	// record, so the flag is true and its absence from the body below is the projection's doing rather
	// than the flag's.
	resources := cognitoWireTagging(t, tagging, ctx, "GetResources", map[string]any{})
	assert.Contains(t, string(resources), arn, "the tagged pool is discoverable through GetResources")
	assert.Contains(t, string(resources), `"wire"`, "GetResources reports the tag that was just applied")

	describeBody := cognitoWireIDP(t, p, ctx, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	described := cognitoWireObject(t, "DescribeUserPool", describeBody, "UserPool")
	cognitoWireAssertMembers(t, "DescribeUserPool", described, cognitoWirePoolMembers)
	for _, member := range []string{"ever_tagged", "AccountID", "Region", "ProviderName"} {
		assert.NotContains(t, described, member,
			"DescribeUserPool reports substrate's internal %q member", member)
	}
	assert.Equal(t, map[string]any{"env": "prod", "wire": "yes"}, described["UserPoolTags"],
		"the tag is reported under the published name, merged into the pool's own")

	// The record kept the flag the body does not carry, which is the whole reason the baseline line
	// stays: scanCognitoUserPools reads EverTagged back on the next GetResources.
	pool := cognitoWireRecordByPrefix(t, state, "cognito-idp",
		"userpool:"+cognitoWireAccount+"/"+cognitoWireRegion+"/")
	assert.JSONEq(t, `true`, string(pool["ever_tagged"]), "the record keeps ever_tagged")
}

// cognitoWireTagging issues a Resource Groups Tagging API operation and returns the raw body.
func cognitoWireTagging(t *testing.T, p *emulator.TaggingPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err, "marshal %s body", op)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "tagging",
		Operation: op,
		Body:      raw,
		Headers:   map[string]string{"x-amz-target": "ResourceGroupsTaggingAPI_20170126." + op},
	})
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// TestCognitoWire_UnsetTimestampIsOmittedRatherThanNull covers the one arm of cognitoTimeOrNil no
// request reaches, because every Cognito handler stamps its record from the simulated clock.
//
// It is what the pointer buys, and the argument is Glue's at #1310: `,omitempty` has no effect on a
// struct type, and a zero EpochSeconds marshals as JSON null, so a bare field would report
// `"CreationDate":null` for a record whose time is zero — a member all five shapes mark Required: No
// and AWS therefore omits. The arm is reachable from a state encoding this substrate did not write: a
// snapshot from an older version, or one a replayed event log restored that predates the field.
func TestCognitoWire_UnsetTimestampIsOmittedRatherThanNull(t *testing.T) {
	assert.Nil(t, emulator.CognitoTimeOrNilForTest(time.Time{}),
		"the zero time must project to nil, so the member is omitted rather than reported null")

	set := emulator.CognitoTimeOrNilForTest(cognitoWireClock)
	require.NotNil(t, set, "a set time must project to a value, or the nil case above proves nothing")

	marshaled, err := json.Marshal(struct {
		CreationDate *emulator.EpochSeconds `json:"CreationDate,omitempty"`
	}{CreationDate: set})
	require.NoError(t, err, "marshal the projected timestamp")
	assert.JSONEq(t, `{"CreationDate":1700000000}`, string(marshaled),
		"a set time renders as the epoch-seconds number Cognito's JSON protocol publishes")
}

// TestCognitoWire_ProjectionLeavesTheRecordIntact is the other half of the argument for projecting
// rather than retagging or dropping the fields.
//
// Every member the responses above no longer carry is still in state, with its stored encoding
// unchanged: `json:"-"` and retyping a field in place both change the format of every recorded run,
// because MemoryStateManager snapshots these bytes and a replay reads them back (see ecr_wire.go).
// The timestamps are asserted to still be RFC3339 strings for exactly that reason — EpochSeconds lives
// in the wire structs, not on the records. AccountID, Region, Tags and EverTagged are also what
// TaggingPlugin.scanCognitoUserPools reads to answer GetResources, and ProviderName is what the
// CloudFormation deployer's ProviderName/ProviderURL attributes were read from before #756 derived
// them.
func TestCognitoWire_ProjectionLeavesTheRecordIntact(t *testing.T) {
	p, ctx, state := setupCognitoWirePlugin(t)
	poolID := cognitoWireCreatePool(t, p, ctx, "record-wire-pool")
	cognitoWireIDP(t, p, ctx, "CreateUserPoolClient",
		map[string]any{"UserPoolId": poolID, "ClientName": "record-client", "GenerateSecret": true})
	cognitoWireIDP(t, p, ctx, "CreateGroup", map[string]any{"UserPoolId": poolID, "GroupName": "record-group"})
	cognitoWireIDP(t, p, ctx, "AdminCreateUser", map[string]any{"UserPoolId": poolID, "Username": "record-user"})
	cognitoWireIDP(t, p, ctx, "AdminAddUserToGroup",
		map[string]any{"UserPoolId": poolID, "Username": "record-user", "GroupName": "record-group"})

	// The key shape cognito_idp_types.go builds: a per-record prefix, then the account and Region the
	// record is already scoped by — which is why nothing reads the stored AccountID and Region back and
	// why they are discharged by projecting rather than by removal.
	scoped := ":" + cognitoWireAccount + "/" + cognitoWireRegion + "/"
	pool := cognitoWireRecordByPrefix(t, state, "cognito-idp", "userpool"+scoped)
	for member, want := range map[string]string{
		"AccountID":    `"` + cognitoWireAccount + `"`,
		"Region":       `"` + cognitoWireRegion + `"`,
		"UserPoolId":   `"` + poolID + `"`,
		"ProviderName": `"cognito-idp.` + cognitoWireRegion + `.amazonaws.com/` + poolID + `"`,
	} {
		assert.JSONEq(t, want, string(pool[member]), "the user pool record keeps %s", member)
	}
	assert.JSONEq(t, `{"env":"prod"}`, string(pool["Tags"]),
		"the record keeps the tag map under its own name, whatever the wire calls it")
	cognitoWireRecordTime(t, "CognitoUserPool", pool, "CreationDate")
	cognitoWireRecordTime(t, "CognitoUserPool", pool, "LastModifiedDate")

	client := cognitoWireRecordByPrefix(t, state, "cognito-idp", "userpoolclient"+scoped)
	assert.JSONEq(t, `"`+cognitoWireAccount+`"`, string(client["AccountID"]))
	assert.JSONEq(t, `"`+cognitoWireRegion+`"`, string(client["Region"]))
	cognitoWireRecordTime(t, "CognitoUserPoolClient", client, "CreationDate")

	group := cognitoWireRecordByPrefix(t, state, "cognito-idp", "userpoolgroup"+scoped)
	assert.JSONEq(t, `"`+cognitoWireAccount+`"`, string(group["AccountID"]))
	assert.JSONEq(t, `"`+cognitoWireRegion+`"`, string(group["Region"]))
	cognitoWireRecordTime(t, "CognitoGroup", group, "CreationDate")

	user := cognitoWireRecordByPrefix(t, state, "cognito-idp", "user"+scoped)
	assert.JSONEq(t, `"`+cognitoWireAccount+`"`, string(user["AccountID"]))
	assert.JSONEq(t, `"`+cognitoWireRegion+`"`, string(user["Region"]))
	assert.JSONEq(t, `"`+poolID+`"`, string(user["UserPoolId"]),
		"the user record keeps the pool it belongs to, which UserType does not publish")
	assert.JSONEq(t, `["record-group"]`, string(user["Groups"]),
		"the user record keeps its group membership, which AdminListGroupsForUser publishes instead")
	cognitoWireRecordTime(t, "CognitoUser", user, "UserCreateDate")

	ip, ictx, istate := setupCognitoIdentityWirePlugin(t)
	cognitoWireIdentity(t, ip, ictx, "CreateIdentityPool", map[string]any{"IdentityPoolName": "record-identity-pool"})
	identityPool := cognitoWireRecordByPrefix(t, istate, "cognito-identity", "identitypool"+scoped)
	assert.JSONEq(t, `"`+cognitoWireAccount+`"`, string(identityPool["AccountID"]))
	assert.JSONEq(t, `"`+cognitoWireRegion+`"`, string(identityPool["Region"]))
	cognitoWireRecordTime(t, "CognitoIdentityPool", identityPool, "CreationDate")
}

// cognitoWireRecordByPrefix returns the one record in namespace whose key starts with prefix.
//
// By prefix rather than by an exact key because four of the five keys end in a minted id, and
// rebuilding each key here would duplicate cognito_idp_types.go's key helpers in a test.
func cognitoWireRecordByPrefix(t *testing.T, state emulator.StateManager, namespace, prefix string) map[string]json.RawMessage {
	t.Helper()
	keys, err := state.List(t.Context(), namespace, prefix)
	require.NoError(t, err, "state.List %s/%s", namespace, prefix)
	var matched []string
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			matched = append(matched, key)
		}
	}
	require.Len(t, matched, 1, "want one record under %s/%s, got %v", namespace, prefix, keys)
	return cognitoWireRecord(t, state, namespace, matched[0])
}

// cognitoWireRecordTime requires a persisted timestamp to still be the RFC3339 string the record has
// always stored, which is what keeps a run recorded by an earlier substrate replayable.
func cognitoWireRecordTime(t *testing.T, record string, fields map[string]json.RawMessage, member string) {
	t.Helper()
	raw, ok := fields[member]
	require.True(t, ok, "%s must persist %s", record, member)
	var stamp string
	require.NoError(t, json.Unmarshal(raw, &stamp),
		"%s.%s must stay a JSON string in state, not become an epoch number: %s", record, member, raw)
	parsed, err := time.Parse(time.RFC3339Nano, stamp)
	require.NoError(t, err, "%s.%s is not an RFC3339 instant: %q", record, member, stamp)
	assert.WithinDuration(t, cognitoWireClock, parsed, time.Second, "%s.%s", record, member)
}
