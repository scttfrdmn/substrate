package emulator_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The three operations whose own reference page publishes that an update is a full replacement, each
// asserted in the shape a merge cannot pass: create with every optional member set, update naming only
// the required ones, then read back and find each optional member at its default (#1089).
//
// Restating the required members on the update is not a concession to the emulator — `UpdateSchedule`
// marks the same three `Required: Yes` as the create (#1008), and `UpdateUserPool*` require the
// identifiers. What the tests omit is the *optional* members, which is the whole question.
//
// These live in one file rather than beside each plugin's tests because the property is one published
// sentence read twice, and a reader checking whether substrate applies it should not have to find two
// files to see both halves. The negative half is here too: the other 38 `update*` handlers in the tree
// are deliberately unasserted, because #671 forbids extending a published statement by analogy.

func TestSchedulerUpdateReplacesRatherThanMerges(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// Every optional member `CreateSchedule` publishes, set to a non-default value.
	createBody := `{
		"ScheduleExpression": "rate(5 minutes)",
		"ScheduleExpressionTimezone": "Europe/Dublin",
		"State": "DISABLED",
		"Description": "the original description",
		"Target": {
			"Arn": "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
			"RoleArn": "arn:aws:iam::123456789012:role/my-role",
			"Input": "{\"k\":\"v\"}",
			"RetryPolicy": {"MaximumEventAgeInSeconds": 120, "MaximumRetryAttempts": 3}
		},
		"FlexibleTimeWindow": {"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 15},
		"ClientToken": "create-token"
	}`
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/replace-test", createBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The update names the three required members and nothing else.
	updateBody := `{
		"ScheduleExpression": "cron(0 12 * * ? *)",
		"Target": {"Arn": "arn:aws:sqs:us-east-1:123456789012:my-queue", "RoleArn": "arn:aws:iam::123456789012:role/my-role"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`
	resp = schedulerRequest(t, ts, http.MethodPut, "/schedules/replace-test", updateBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules/replace-test", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSchedulerBody(t, resp)

	var got struct {
		ScheduleExpression         string `json:"ScheduleExpression"`
		ScheduleExpressionTimezone string `json:"ScheduleExpressionTimezone"`
		State                      string `json:"State"`
		Description                string `json:"Description"`
		Target                     struct {
			Arn         string          `json:"Arn"`
			Input       string          `json:"Input"`
			RetryPolicy json.RawMessage `json:"RetryPolicy"`
		} `json:"Target"`
		FlexibleTimeWindow struct {
			Mode                   string `json:"Mode"`
			MaximumWindowInMinutes int32  `json:"MaximumWindowInMinutes"`
		} `json:"FlexibleTimeWindow"`
	}
	require.NoError(t, json.Unmarshal(body, &got))

	assert.Equal(t, "cron(0 12 * * ? *)", got.ScheduleExpression, "the member the update named")
	assert.Empty(t, got.ScheduleExpressionTimezone, "an omitted timezone reverts to the system default")
	assert.Equal(t, "ENABLED", got.State, "an omitted State takes the documented ENABLED default, not the stored DISABLED")
	assert.Empty(t, got.Description, "an omitted description reverts")
	assert.Equal(t, "arn:aws:sqs:us-east-1:123456789012:my-queue", got.Target.Arn)
	assert.Empty(t, got.Target.Input, "an omitted nested Input reverts")
	assert.Nil(t, got.Target.RetryPolicy, "an omitted nested RetryPolicy reverts")
	assert.Equal(t, "OFF", got.FlexibleTimeWindow.Mode)
	assert.Zero(t, got.FlexibleTimeWindow.MaximumWindowInMinutes, "an omitted window reverts with its Mode")
}

// TestSchedulerGetScheduleOmitsClientToken pins the removal of a response element AWS does not publish.
//
// `API_GetSchedule` publishes fifteen response elements and `ClientToken` is not among them; it is a
// request-only idempotency token. It came off the wire with the full-replace fix rather than on its own
// because an omitted member now reverts, which would have made an unpublished field start changing
// under callers who never named it.
func TestSchedulerGetScheduleOmitsClientToken(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	createBody := `{
		"ScheduleExpression": "rate(1 hour)",
		"Target": {"Arn": "arn:aws:sqs:us-east-1:123456789012:q", "RoleArn": "arn:aws:iam::123456789012:role/r"},
		"FlexibleTimeWindow": {"Mode": "OFF"},
		"ClientToken": "a-token-the-read-must-not-report"
	}`
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/token-test", createBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules/token-test", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSchedulerBody(t, resp)

	var members map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &members))
	assert.NotContains(t, members, "ClientToken", "GetSchedule publishes no ClientToken element")
	assert.NotContains(t, string(body), "a-token-the-read-must-not-report")
}

// TestSchedulerCreateAppliesTheDocumentedStateDefault asserts the default the update now shares.
//
// The API Reference publishes no default for `State` on any of the four pages carrying it; the User
// Guide does — "By default, the EventBridge Scheduler enables your schedule". Both doors resolve it
// through one function, which is why asserting it at the create is asserting it at the update.
func TestSchedulerCreateAppliesTheDocumentedStateDefault(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	createBody := `{
		"ScheduleExpression": "rate(1 hour)",
		"Target": {"Arn": "arn:aws:sqs:us-east-1:123456789012:q", "RoleArn": "arn:aws:iam::123456789012:role/r"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/state-default", createBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = schedulerRequest(t, ts, http.MethodGet, "/schedules/state-default", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got struct {
		State string `json:"State"`
	}
	require.NoError(t, json.Unmarshal(readSchedulerBody(t, resp), &got))
	assert.Equal(t, "ENABLED", got.State)
}

// TestCognitoUpdateUserPoolReplacesRatherThanMerges reads back through DescribeUserPool.
//
// Cognito publishes `ListTagsForResource`, `TagResource` and `UntagResource` for cognito-idp, but
// substrate routes none of the three (#1135), so the pool's own read is the only door a test has to the
// tag set — which is enough, since `UserPoolType` publishes the tags as a response member.
//
// The tag assertions below read the member under the name substrate currently emits, `Tags`, so that they
// pin the replace-not-merge property rather than accidentally pinning the wrong name. `UserPoolType`
// publishes it as `UserPoolTags`; that is #1136, and these assertions move with it.
func TestCognitoUpdateUserPoolReplacesRatherThanMerges(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)

	createResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "CreateUserPool", map[string]any{
		"PoolName":         "replace-pool",
		"MfaConfiguration": "ON",
		"Policies":         map[string]any{"PasswordPolicy": map[string]any{"MinimumLength": 12}},
		"LambdaConfig":     map[string]any{"PreSignUp": "arn:aws:lambda:us-east-1:123456789012:function:pre"},
		"UserPoolTags":     map[string]any{"env": "prod"},
	}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, createResp.StatusCode)
	var createOut struct {
		UserPool struct {
			UserPoolID string `json:"UserPoolId"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(createResp.Body, &createOut))
	poolID := createOut.UserPool.UserPoolID
	require.NotEmpty(t, poolID)

	// The update names only the identifier and the name, so every other published member must revert.
	updateResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "UpdateUserPool", map[string]any{
		"UserPoolId": poolID,
		"PoolName":   "replace-pool-renamed",
	}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, updateResp.StatusCode)
	assert.Empty(t, updateResp.Body, "API_UpdateUserPool publishes an empty HTTP body, not a member-less object")

	describeResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "DescribeUserPool", map[string]any{
		"UserPoolId": poolID,
	}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, describeResp.StatusCode)

	var pool map[string]json.RawMessage
	var wrapper struct {
		UserPool map[string]json.RawMessage `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(describeResp.Body, &wrapper))
	pool = wrapper.UserPool

	var name, mfa string
	require.NoError(t, json.Unmarshal(pool["Name"], &name))
	require.NoError(t, json.Unmarshal(pool["MfaConfiguration"], &mfa))
	assert.Equal(t, "replace-pool-renamed", name, "the member the update named")
	assert.Equal(t, "OFF", mfa, "an omitted MfaConfiguration reverts to substrate's OFF reading, not the stored ON")
	assert.NotContains(t, pool, "Policies", "an omitted Policies reverts")
	assert.NotContains(t, pool, "LambdaConfig", "an omitted LambdaConfig reverts")
	assert.NotContains(t, pool, "Tags", "an omitted UserPoolTags reverts")

	// Full replacement governs only the members the operation publishes. `Schema` is absent from
	// `API_UpdateUserPool`'s Request Syntax, and so are these, so the update cannot reset them.
	for _, preserved := range []string{"Arn", "ProviderName", "Status", "CreationDate"} {
		assert.Contains(t, pool, preserved, "%s is not settable by UpdateUserPool and must survive it", preserved)
	}
}

// TestCognitoUpdateUserPoolTagsRoundTrip asserts that a tag set named on the update replaces the
// stored one outright rather than merging into it.
func TestCognitoUpdateUserPoolTagsRoundTrip(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)

	createResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "CreateUserPool", map[string]any{
		"PoolName":     "tag-pool",
		"UserPoolTags": map[string]any{"keep": "a", "drop": "b"},
	}))
	require.NoError(t, err)
	var createOut struct {
		UserPool struct {
			UserPoolID string `json:"UserPoolId"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(createResp.Body, &createOut))

	_, err = p.HandleRequest(ctx, cognitoIDPRequest(t, "UpdateUserPool", map[string]any{
		"UserPoolId":   createOut.UserPool.UserPoolID,
		"PoolName":     "tag-pool",
		"UserPoolTags": map[string]any{"keep": "c"},
	}))
	require.NoError(t, err)

	describeResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "DescribeUserPool", map[string]any{
		"UserPoolId": createOut.UserPool.UserPoolID,
	}))
	require.NoError(t, err)
	var out struct {
		UserPool struct {
			Tags map[string]string `json:"Tags"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(describeResp.Body, &out))
	assert.Equal(t, map[string]string{"keep": "c"}, out.UserPool.Tags,
		"the update's tag set replaces the stored one; `drop` is not merged forward")
}

// TestCognitoUpdateUserPoolClientReplacesRatherThanMerges covers the second page carrying the Important
// box verbatim, four hundred lines from the first in the same file.
func TestCognitoUpdateUserPoolClientReplacesRatherThanMerges(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)
	poolID := createCognitoPool(t, p, ctx, "replace-client-pool")

	createResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "CreateUserPoolClient", map[string]any{
		"UserPoolId":        poolID,
		"ClientName":        "orig-name",
		"ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH"},
	}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, createResp.StatusCode)
	var createOut struct {
		UserPoolClient struct {
			ClientID          string   `json:"ClientId"`
			ExplicitAuthFlows []string `json:"ExplicitAuthFlows"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(createResp.Body, &createOut))
	require.Equal(t, []string{"ALLOW_USER_PASSWORD_AUTH"}, createOut.UserPoolClient.ExplicitAuthFlows,
		"a named flow set is stored as given")

	// The update names only the identifiers, so both published optional members take their defaults.
	updateResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "UpdateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   createOut.UserPoolClient.ClientID,
	}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, updateResp.StatusCode)

	var updateOut struct {
		UserPoolClient struct {
			ClientName        string   `json:"ClientName"`
			ExplicitAuthFlows []string `json:"ExplicitAuthFlows"`
		} `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(updateResp.Body, &updateOut))
	assert.Empty(t, updateOut.UserPoolClient.ClientName, "an omitted ClientName has no published default and clears")
	assert.Equal(t, []string{"ALLOW_REFRESH_TOKEN_AUTH", "ALLOW_USER_SRP_AUTH", "ALLOW_CUSTOM_AUTH"},
		updateOut.UserPoolClient.ExplicitAuthFlows,
		"an omitted ExplicitAuthFlows takes the three published defaults, not the stored ALLOW_USER_PASSWORD_AUTH")

	// The published sentence conditions on "if you don't specify a value", and `[]` is one.
	emptyResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "UpdateUserPoolClient", map[string]any{
		"UserPoolId":        poolID,
		"ClientId":          createOut.UserPoolClient.ClientID,
		"ExplicitAuthFlows": []string{},
	}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, emptyResp.StatusCode)
	var emptyOut struct {
		UserPoolClient map[string]json.RawMessage `json:"UserPoolClient"`
	}
	require.NoError(t, json.Unmarshal(emptyResp.Body, &emptyOut))
	// The member is absent rather than `[]` because the persisted field carries `omitempty`; see
	// cognito_idp_update_replace.go for why that is #756's divergence and not this change's. What matters
	// here is the distinction the defaults would erase.
	assert.NotContains(t, emptyOut.UserPoolClient, "ExplicitAuthFlows",
		"an explicit empty array is a value the caller specified and does not acquire the defaults")
}

// TestUpdateHandlersAreNotFlippedByAnalogy records the negative half of #1089 as an assertion rather
// than as prose.
//
// 41 `update*` handlers live in `emulator/` and 29 of them guard an assignment on a non-empty request
// member. Exactly three pages publish that an update is a full replacement, and only those three lost
// their guards. The other 26 keep them, because #671's binding scope decision is that substrate models
// only what an operation's own page states — a published sentence is not extended to a sibling by
// analogy, however tempting the symmetry.
//
// Step Functions is the sharpest instance, because its page argues the other way rather than merely
// staying silent. `API_UpdateStateMachine` opens "Updates an existing state machine by modifying its
// `definition`, `roleArn`, `loggingConfiguration`, or `EncryptionConfiguration`", publishes both
// `definition` and `roleArn` as `Required: No`, and then publishes `MissingRequiredParameter`: "This
// error occurs if both `definition` and `roleArn` are not specified." A request naming only `roleArn` is
// therefore explicitly legal — which full replacement would turn into a request that blanks the
// definition, leaving a state machine the service could not execute. So the merge here is what the page
// describes, and flipping it because Cognito's page says otherwise would be the borrowing #671 forbids.
func TestUpdateHandlersAreNotFlippedByAnalogy(t *testing.T) {
	p, ctx := setupStepFunctionsPlugin(t)

	createResp, err := p.HandleRequest(ctx, sfnRequest("CreateStateMachine", map[string]any{
		"name":       "NoAnalogy",
		"definition": testSMDefinition,
		"roleArn":    "arn:aws:iam::123456789012:role/original-role",
		"type":       "STANDARD",
	}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, createResp.StatusCode)
	arn, ok := sfnBody(t, createResp)["stateMachineArn"].(string)
	require.True(t, ok)

	// Legal per the page's own MissingRequiredParameter wording: one of the two, not both.
	updateResp, err := p.HandleRequest(ctx, sfnRequest("UpdateStateMachine", map[string]any{
		"stateMachineArn": arn,
		"roleArn":         "arn:aws:iam::123456789012:role/new-role",
	}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, updateResp.StatusCode)

	describeResp, err := p.HandleRequest(ctx, sfnRequest("DescribeStateMachine", map[string]any{
		"stateMachineArn": arn,
	}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, describeResp.StatusCode)
	described := sfnBody(t, describeResp)
	assert.Equal(t, "arn:aws:iam::123456789012:role/new-role", described["roleArn"],
		"the member the update named")
	assert.Equal(t, testSMDefinition, described["definition"],
		"the omitted definition survives; UpdateStateMachine's page publishes a merge and #671 forbids "+
			"importing Cognito's full-replacement sentence")
}
