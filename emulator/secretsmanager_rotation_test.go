package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// RotateSecret's parameters, its VersionId and its two refusals — #952.
//
// The defect these cases exist for is not "a thin response". RotateSecret read one of its seven
// parameters and then set RotationEnabled = true on any secret it could load, so substrate reported
// rotation configured for a secret with no rotation function — a state AWS refuses to create — and a
// consumer's error path was unreachable while its nominal path was indistinguishable from substrate's.
// Three assertion styles follow from that, and none of them substitutes for another:
//
//  1. **A refusal is asserted together with the state it did not write.** A status-only assertion would
//     have passed against a handler that answered 400 after saving, which is precisely what the old one
//     did on its way to a 200. So the bare-call case re-describes the secret and checks RotationEnabled
//     is still the raw four bytes "null".
//
//  2. **The response's VersionId is compared to the token that was sent.** All four of AWS's samples say
//     "the ClientRequestToken field becomes the VersionId of the new version created during the
//     rotation", so the assertion is equality with the request, not a shape check — a minted value of
//     the right shape would be the #856 defect class, not a fix.
//
//  3. **RotationRules' presence and absence are read off the raw bytes**, following #930's precedent.
//     AWS documents this member as omitted rather than nulled — "if the secret never had rotation turned
//     on, this field is omitted" — and a decoded struct collapses absent into zero-valued, which is the
//     distinction the omission is about.
//
// Every call goes over the wire and every secret is created through CreateSecret, per #765.

// smRotationLambda is a plausible rotation-function ARN. Nothing at that address exists, and nothing
// needs to: substrate records which function a rotation would run and never runs one, which is the
// boundary CLAUDE.md draws.
const smRotationLambda = "arn:aws:lambda:us-east-1:" + taggingTestAccount + ":function:rotator"

// smRotate posts RotateSecret and returns the status, the decoded {ARN, Name, VersionId} and the bare
// error code a refusal carries.
func smRotate(t *testing.T, ts *emulator.TestServer, body map[string]any) (int, struct {
	ARN       string `json:"ARN"`
	Name      string `json:"Name"`
	VersionID string `json:"VersionId"`
}, string) {
	t.Helper()
	var out struct {
		ARN       string `json:"ARN"`
		Name      string `json:"Name"`
		VersionID string `json:"VersionId"`
	}
	status, raw, code := smRawCall(t, ts, smTarget, "RotateSecret", body)
	require.NoError(t, json.Unmarshal([]byte(raw), &out), "decode RotateSecret %v", body)
	return status, out, code
}

func TestSMRotation_ABareCallIsRefusedAndWritesNothing(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "bare-rotation", nil)

	// This exact call answered 200 before #952. AWS refuses it: "you tried to enable rotation on a secret
	// that doesn't already have a Lambda function ARN configured and you didn't include such an ARN as a
	// parameter in this call" is the second of InvalidRequestException's three published causes. The
	// missing ClientRequestToken is refused first, so the token is supplied here — the refusal under test
	// is the rotation function's.
	status, _, code := smRotate(t, ts, map[string]any{
		"SecretId":           arn,
		"ClientRequestToken": smRotationToken,
	})
	assert.Equal(t, "InvalidRequestException", code)
	assert.Equal(t, http.StatusBadRequest, status)

	// The half a status assertion cannot reach. The old handler saved RotationEnabled = true before
	// answering, so a refusal that still wrote would look identical from the response alone.
	members := smDescribeMembers(t, ts, arn)
	require.Contains(t, members, "RotationEnabled")
	assert.Equal(t, "null", string(members["RotationEnabled"]),
		"the refused rotation left the secret never-configured")
	assert.NotContains(t, members, "RotationLambdaARN")
	assert.NotContains(t, members, "RotationRules")
}

func TestSMRotation_AnOmittedClientRequestTokenIsRefused(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "tokenless", nil)

	// **Substrate's reading**, and the only decision here AWS publishes no code for. ClientRequestToken is
	// "Required: No" because "the CLI or SDK generates a random UUID for you", but the page is explicit
	// about the caller substrate is: "if you generate a raw HTTP request to the Secrets Manager service
	// endpoint, then you must generate a ClientRequestToken and include it in the request." Minting one
	// would put a nondeterministic value in a response body (#856) and echoing an empty one would answer a
	// VersionId of "" where AWS publishes a minimum length of 32.
	//
	// Note the ARN is supplied, so this cannot pass by reaching the no-rotation-function refusal instead:
	// the code distinguishes them.
	status, _, code := smRotate(t, ts, map[string]any{
		"SecretId":          arn,
		"RotationLambdaARN": smRotationLambda,
	})
	assert.Equal(t, "InvalidParameterException", code)
	assert.Equal(t, http.StatusBadRequest, status)

	members := smDescribeMembers(t, ts, arn)
	assert.Equal(t, "null", string(members["RotationEnabled"]), "and nothing was written")
}

func TestSMRotation_TheVersionIDIsTheClientRequestTokenThatWasSent(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "versioned-rotation", nil)

	const token = "12345678-90ab-cdef-1234-567890abcdef"
	status, out, code := smRotate(t, ts, map[string]any{
		"SecretId":           arn,
		"ClientRequestToken": token,
		"RotationLambdaARN":  smRotationLambda,
	})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)

	// Equality with the request, not a shape check. "The ClientRequestToken field becomes the VersionId of
	// the new version created during the rotation", stated in all four of AWS's examples and borne out by
	// all four of its sample responses — which is also what makes the value reproducible on replay.
	assert.Equal(t, token, out.VersionID)
	assert.Equal(t, arn, out.ARN)
	assert.Equal(t, "versioned-rotation", out.Name)
}

func TestSMRotation_TheConfigurationRoundTripsThroughDescribeSecret(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "configured", nil)

	// Absent first. RotationRules is one of the four members AWS documents an omission for — "if the
	// secret never had rotation turned on, this field is omitted" — so the before-and-after pair is what
	// establishes that substrate honors the omission rather than emitting an empty object.
	before := smDescribeMembers(t, ts, arn)
	require.NotContains(t, before, "RotationRules")
	require.NotContains(t, before, "RotationLambdaARN")

	_, _, code := smRotate(t, ts, map[string]any{
		"SecretId":           arn,
		"ClientRequestToken": smRotationToken,
		"RotationLambdaARN":  smRotationLambda,
		"RotationRules": map[string]any{
			"ScheduleExpression": "cron(0 8 1 * ? *)",
			"Duration":           "2h",
		},
	})
	require.Empty(t, code)

	after := smDescribeMembers(t, ts, arn)
	assert.JSONEq(t, `"`+smRotationLambda+`"`, string(after["RotationLambdaARN"]))
	// The whole member, not one field of it: an assertion on ScheduleExpression alone would pass against a
	// handler that dropped Duration, and AWS's own RotationSchedule example pairs the two.
	assert.JSONEq(t, `{"ScheduleExpression":"cron(0 8 1 * ? *)","Duration":"2h"}`,
		string(after["RotationRules"]))
	assert.Equal(t, "true", string(after["RotationEnabled"]))
}

func TestSMRotation_AnIntervalScheduleIsReportedAsANumber(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "interval", nil)

	// AWS types AutomaticallyAfterDays as Long, so the raw bytes must be 30 rather than "30". This is
	// asserted separately from the ScheduleExpression case because the two are mutually exclusive and
	// cannot appear in one response.
	_, _, code := smRotate(t, ts, map[string]any{
		"SecretId":           arn,
		"ClientRequestToken": smRotationToken,
		"RotationLambdaARN":  smRotationLambda,
		"RotationRules":      map[string]any{"AutomaticallyAfterDays": 30},
	})
	require.Empty(t, code)

	assert.JSONEq(t, `{"AutomaticallyAfterDays":30}`,
		string(smDescribeMembers(t, ts, arn)["RotationRules"]))
}

func TestSMRotation_TheStoredFunctionSatisfiesASecondCall(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "already-configured", nil)

	_, _, code := smRotate(t, ts, map[string]any{
		"SecretId":           arn,
		"ClientRequestToken": smRotationToken,
		"RotationLambdaARN":  smRotationLambda,
	})
	require.Empty(t, code)

	// "If you don't include the configuration parameters, the operation starts a rotation with the values
	// already stored in the secret." So the bare call refused in the first case succeeds here, and the
	// difference is the record — which is what makes the refusal a statement about state rather than about
	// the request alone.
	const second = "99999999-8888-7777-6666-555555555555"
	status, out, code := smRotate(t, ts, map[string]any{
		"SecretId":           arn,
		"ClientRequestToken": second,
	})
	require.Empty(t, code)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, second, out.VersionID)

	// And the stored ARN is unchanged rather than cleared by a call that named none.
	assert.JSONEq(t, `"`+smRotationLambda+`"`,
		string(smDescribeMembers(t, ts, arn)["RotationLambdaARN"]))
}

func TestSMRotation_AnEmptyRotationLambdaARNIsNotNamingOne(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "empty-arn", nil)

	// RotationLambdaARN's published length minimum is 0, so "" is a value a caller can legitimately send
	// and it is not "such an ARN as a parameter in this call". Refusing it is what keeps the cause's
	// wording true; accepting it would store an empty ARN and report rotation as configured.
	_, _, code := smRotate(t, ts, map[string]any{
		"SecretId":           arn,
		"ClientRequestToken": smRotationToken,
		"RotationLambdaARN":  "",
	})
	assert.Equal(t, "InvalidRequestException", code)
}

func TestSMRotation_AScheduledSecretIsRefusedDifferentlyFromAnAbsentOne(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "doomed-rotation", nil)

	// Configure rotation first, so the refusal under test cannot be the no-rotation-function one.
	_, _, code := smRotate(t, ts, map[string]any{
		"SecretId":           arn,
		"ClientRequestToken": smRotationToken,
		"RotationLambdaARN":  smRotationLambda,
	})
	require.Empty(t, code)

	_, _, code = smRawCall(t, ts, smTarget, "DeleteSecret", map[string]any{"SecretId": arn})
	require.Empty(t, code, "DeleteSecret opens the recovery window")

	// The first of InvalidRequestException's three published causes, decidable only because #953 modeled
	// the recovery window. The pair is asserted together rather than each alone: before #953 both answered
	// ResourceNotFoundException, so either assertion in isolation would have passed on the wrong code.
	_, _, scheduled := smRotate(t, ts, map[string]any{
		"SecretId":           arn,
		"ClientRequestToken": smRotationToken,
	})
	assert.Equal(t, "InvalidRequestException", scheduled, "a secret in its recovery window still exists")

	absent := "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":secret:never-existed"
	_, _, missing := smRotate(t, ts, map[string]any{
		"SecretId":           absent,
		"ClientRequestToken": smRotationToken,
	})
	assert.Equal(t, "ResourceNotFoundException", missing, "one that never existed does not")
}

func TestSMRotation_AnUnusableScheduleIsRefused(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "bad-schedule", nil)

	// Both refusals are AWS's rules; the *code* is substrate's reading, AWS publishing none for either.
	// The mutual exclusion is stated in AutomaticallyAfterDays' own description — "in RotateSecret, you
	// can set the rotation schedule in RotationRules with AutomaticallyAfterDays or ScheduleExpression,
	// but not both" — and the range is its "Minimum value of 1. Maximum value of 1000."
	for _, tc := range []struct {
		name  string
		rules map[string]any
	}{
		{"both forms of schedule", map[string]any{
			"AutomaticallyAfterDays": 30, "ScheduleExpression": "rate(30 days)"}},
		{"zero days", map[string]any{"AutomaticallyAfterDays": 0}},
		{"beyond a thousand days", map[string]any{"AutomaticallyAfterDays": 1001}},
		{"negative days", map[string]any{"AutomaticallyAfterDays": -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, _, code := smRotate(t, ts, map[string]any{
				"SecretId":           arn,
				"ClientRequestToken": smRotationToken,
				"RotationLambdaARN":  smRotationLambda,
				"RotationRules":      tc.rules,
			})
			assert.Equal(t, "InvalidParameterException", code)
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}

	// The schedule is refused before the record is read, so none of the four wrote anything — including
	// the rotation function they all named, which would otherwise have made the secret rotatable by a
	// call that was refused.
	members := smDescribeMembers(t, ts, arn)
	assert.Equal(t, "null", string(members["RotationEnabled"]))
	assert.NotContains(t, members, "RotationLambdaARN")
}

func TestSMRotation_ABodyThatIsNotAnObjectIsRefused(t *testing.T) {
	ts := smTagServer(t)

	// The same guard every Secrets Manager handler carries, pinned here so #950's audit of the code the
	// whole service answers for a malformed body has this operation on the record too. Marshaled, each of
	// these is valid JSON and none is an object, so the handler's own Unmarshal is what fails.
	for _, body := range []any{[]any{}, "SecretId", 7} {
		resp := signedRequest(t, ts, smTarget, taggingTestAccount, "RotateSecret", body)
		status, code := decodeAWSResponse(t, resp, nil)
		assert.Equal(t, "InvalidRequestException", code)
		assert.Equal(t, http.StatusBadRequest, status)
	}
}

// smCFNStack posts a CloudFormation CreateStack over the wire, signed as account, and returns the
// status and body.
//
// The query protocol is built here rather than through [signedRequest], which marshals JSON. Going over
// the wire matters for the same reason it does everywhere else in this file: a StackDeployer a Go caller
// drives directly is not the observable, and the assertion that a rotation schedule reached the secret
// has to be a Secrets Manager call against the state the deploy wrote.
func smCFNStack(t *testing.T, ts *emulator.TestServer, account, action string, params map[string]string) (int, string) {
	t.Helper()
	form := url.Values{"Action": {action}, "Version": {"2010-05-15"}}
	for k, v := range params {
		form.Set(k, v)
	}
	body := []byte(form.Encode())
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(body))
	require.NoError(t, err)
	req.Host = "cloudformation.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	creds, ok := ts.CredentialsFor(account)
	require.True(t, ok, "no credential registered for account %s", account)
	signAs(req, creds, "cloudformation", "us-east-1", body)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

func TestSMRotation_ARotationScheduleCarriesTheTemplatesFunctionAndSchedule(t *testing.T) {
	ts := smTagServer(t)

	// HostedRotationLambda rather than RotationLambdaARN, because that is the route whose ARN substrate
	// composes rather than copies — "create a new rotation function using HostedRotationLambda" — and the
	// composition is the reading worth pinning. RotateImmediatelyOnUpdate is explicit and false so the
	// pass-through of a non-default value is exercised, not just the default.
	const template = `{"Resources":{` +
		`"Password":{"Type":"AWS::SecretsManager::Secret",` +
		`"Properties":{"Name":"cfn-rotated","SecretString":"initial"}},` +
		`"Schedule":{"Type":"AWS::SecretsManager::RotationSchedule",` +
		`"Properties":{"SecretId":{"Ref":"Password"},` +
		`"RotateImmediatelyOnUpdate":false,` +
		`"HostedRotationLambda":{"RotationType":"MySQLSingleUser","RotationLambdaName":"cfn-rotator"},` +
		`"RotationRules":{"Duration":"2h","ScheduleExpression":"cron(0 1 * * ? *)"}}}}}`

	status, body := smCFNStack(t, ts, taggingTestAccount, "CreateStack", map[string]string{
		"StackName":    "rotation-passthrough",
		"TemplateBody": template,
	})
	require.Equal(t, http.StatusOK, status, "body was %s", body)

	// The whole point of the pass-through: the secret the stack created carries what the template said,
	// read back through Secrets Manager's own call rather than out of the DeployResult.
	members := smDescribeMembers(t, ts, "cfn-rotated")
	assert.JSONEq(t,
		`"arn:aws:lambda:us-east-1:`+taggingTestAccount+`:function:cfn-rotator"`,
		string(members["RotationLambdaARN"]),
		"a HostedRotationLambda's RotationLambdaName is composed into a function ARN")
	assert.JSONEq(t, `{"Duration":"2h","ScheduleExpression":"cron(0 1 * * ? *)"}`,
		string(members["RotationRules"]))
	assert.Equal(t, "true", string(members["RotationEnabled"]))
}

func TestSMRotation_ARotationScheduleNamingNoFunctionFailsVisibly(t *testing.T) {
	ts := smTagServer(t)

	// deploySecretRotationSchedule reports a routing refusal by setting DeployedResource.Error and
	// returning a nil error, which becomes CREATE_FAILED with a reason. Without this case the refusal
	// would be a silent per-resource failure inside a stack that reported itself created — the outcome
	// #519 established the derivation for, and the reason the pass-through above ships with the refusal
	// rather than after it.
	const template = `{"Resources":{` +
		`"Password":{"Type":"AWS::SecretsManager::Secret",` +
		`"Properties":{"Name":"cfn-unrotatable","SecretString":"initial"}},` +
		`"Schedule":{"Type":"AWS::SecretsManager::RotationSchedule",` +
		`"Properties":{"SecretId":{"Ref":"Password"},` +
		`"RotationRules":{"AutomaticallyAfterDays":30}}}}}`

	status, body := smCFNStack(t, ts, taggingTestAccount, "CreateStack", map[string]string{
		"StackName":    "rotation-no-function",
		"TemplateBody": template,
	})
	require.Equal(t, http.StatusOK, status, "body was %s", body)

	status, body = smCFNStack(t, ts, taggingTestAccount, "DescribeStackResources", map[string]string{
		"StackName": "rotation-no-function",
	})
	require.Equal(t, http.StatusOK, status, "body was %s", body)
	assert.Contains(t, body, "CREATE_FAILED")
	assert.Contains(t, body, "InvalidRequestException")

	// The failed resource takes the stack down with it, and the rollback removes the secret the schedule
	// was for — so the refusal is not silent at either level. Asserted through smRawCall rather than
	// smDescribeMembers, which requires a successful describe.
	_, _, code := smRawCall(t, ts, smTarget, "DescribeSecret", map[string]any{"SecretId": "cfn-unrotatable"})
	assert.Equal(t, "ResourceNotFoundException", code,
		"the rollback removed the secret rather than leaving a half-configured one")
}

func TestSMRotation_AnUnresolvableIdentifierIsRefusedBeforeAnyLookup(t *testing.T) {
	ts := smTagServer(t)

	// Ordering, which is not a detail: the token is validated before the identifier and the identifier
	// before the record, so each refusal is reachable and a caller's typo does not surface as a
	// ResourceNotFoundException it would then hunt for a secret to explain.
	for _, id := range []string{"", "arn:aws:s3:::not-a-secret"} {
		status, _, code := smRotate(t, ts, map[string]any{
			"SecretId":           id,
			"ClientRequestToken": smRotationToken,
			"RotationLambdaARN":  smRotationLambda,
		})
		assert.Equal(t, "InvalidParameterException", code, "identifier %q", id)
		assert.Equal(t, http.StatusBadRequest, status)
	}
}
