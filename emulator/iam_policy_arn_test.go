package emulator_test

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Attach-time PolicyArn validation (#499).
//
// The shape of these tests follows the decision: a malformed ARN is refused, a well-formed one
// that resolves nowhere succeeds with a warning, and an ARN GetPolicy resolves is never refused
// — that last one is #499's hard acceptance criterion, so it is asserted for every attach
// operation and for both a bundled and a created policy.
//
// All three of AttachUserPolicy, AttachRolePolicy and AttachGroupPolicy are driven from the
// same tables. #499's report was that the check existed on none of them; shipping it on two of
// three would be the same defect with a smaller surface.

// iamAttachTarget names one attach operation and the entity it needs, so a table can drive all
// three without three copies of every case.
type iamAttachTarget struct {
	operation string
	// setup creates the entity and returns the request field naming it.
	setup func(t *testing.T, srv *emulator.Server) (field, name string)
}

// iamAttachTargets is the three attach operations, each with the entity it attaches to.
var iamAttachTargets = []iamAttachTarget{
	{
		operation: "AttachUserPolicy",
		setup: func(t *testing.T, srv *emulator.Server) (string, string) {
			t.Helper()
			resp := iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "jill"})
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
			return "UserName", "jill"
		},
	},
	{
		operation: "AttachRolePolicy",
		setup: func(t *testing.T, srv *emulator.Server) (string, string) {
			t.Helper()
			resp := iamRequest(t, srv, "CreateRole", map[string]any{
				"RoleName":                 "worker",
				"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			})
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
			return "RoleName", "worker"
		},
	},
	{
		operation: "AttachGroupPolicy",
		setup: func(t *testing.T, srv *emulator.Server) (string, string) {
			t.Helper()
			resp := iamRequest(t, srv, "CreateGroup", map[string]any{"GroupName": "devs"})
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
			return "GroupName", "devs"
		},
	},
}

// iamAttach runs one attach operation against target's entity and returns the response.
func iamAttach(t *testing.T, srv *emulator.Server, target iamAttachTarget, policyARN string) *http.Response {
	t.Helper()
	field, name := target.setup(t, srv)
	return iamRequest(t, srv, target.operation, map[string]any{
		field:       name,
		"PolicyArn": policyARN,
	})
}

func TestAttachPolicy_RefusesAMalformedARN(t *testing.T) {
	// Every case here is a mistake a consumer actually makes: the bare name from the console,
	// an ARN for the wrong service, a resource type that is not a policy, a slash-terminated
	// ARN with no name, an account that is not twelve digits.
	// wantMessage is asserted, not only the code: the length bound and the pattern refuse for
	// different reasons and say so, and a test checking only "InvalidInput" would pass with
	// either bound removed entirely.
	cases := []struct {
		name        string
		policyARN   string
		wantMessage string
	}{
		{"a bare policy name", "AmazonS3ReadOnlyAccess", "is not valid"},
		{"not an ARN at all", "not-an-arn-at-all-but-long-enough", "is not valid"},
		{"the wrong service", "arn:aws:s3:::my-bucket/AmazonS3ReadOnlyAccess", "is not valid"},
		{"the wrong resource type", "arn:aws:iam::123456789012:role/AmazonS3ReadOnlyAccess", "is not valid"},
		{"a region where IAM has none", "arn:aws:iam:us-east-1:123456789012:policy/thing", "is not valid"},
		{"no policy name", "arn:aws:iam::aws:policy/service-role/", "is not valid"},
		{"a thirteen-digit account", "arn:aws:iam::1234567890123:policy/thing", "is not valid"},
		{"an eleven-digit account", "arn:aws:iam::12345678901:policy/thing", "is not valid"},
		// arnType's own bounds, min 20 and max 2048. Both are refused on size rather than by
		// the pattern, so the message names the length.
		{"shorter than arnType allows", "arn:aws:iam::a", "between 20 and 2048"},
		{"longer than arnType allows",
			"arn:aws:iam::aws:policy/" + strings.Repeat("x", 2048), "between 20 and 2048"},
	}

	for _, target := range iamAttachTargets {
		for _, tc := range cases {
			t.Run(target.operation+"/"+tc.name, func(t *testing.T) {
				srv := newIAMTestServer(t)
				resp := iamAttach(t, srv, target, tc.policyARN)

				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
				var result map[string]any
				decodeIAMXML(t, resp, &result)
				assert.Equal(t, "InvalidInput", result["__type"],
					"the model declares InvalidInputException for all three attach operations")
				message, _ := result["message"].(string)
				assert.Contains(t, message, tc.wantMessage)
			})
		}
	}
}

func TestAttachPolicy_AnARNGetPolicyResolvesIsNeverRefused(t *testing.T) {
	// #499's hard acceptance criterion. A bundled ARN and a created one both resolve through
	// GetPolicy, so neither may be refused — and neither may warn, because warning about a
	// policy the emulator holds would train a consumer to ignore the warning.
	for _, target := range iamAttachTargets {
		t.Run(target.operation+"/bundled", func(t *testing.T) {
			logs := &iamWarnLog{}
			srv := newIAMTestServerWith(t, emulator.NewMemoryStateManager(), logs)

			resp := iamAttach(t, srv, target, "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
			assert.Empty(t, logs.warningsAbout("policyArn"),
				"a bundled policy resolves, so there is nothing to warn about")
		})

		t.Run(target.operation+"/created", func(t *testing.T) {
			logs := &iamWarnLog{}
			srv := newIAMTestServerWith(t, emulator.NewMemoryStateManager(), logs)
			arn := iamCreatePolicyForVersions(t, srv, "team-policy",
				`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)

			resp := iamAttach(t, srv, target, arn)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
			assert.Empty(t, logs.warningsAbout("policyArn"),
				"a created policy resolves through state, so there is nothing to warn about")
		})
	}
}

func TestAttachPolicy_AnUnbundledAWSManagedARNSucceedsWithAWarning(t *testing.T) {
	// Substrate bundles 52 of roughly 1,200 AWS managed policies. AmazonAthenaFullAccess is
	// real and is not one of the 52, which is exactly the case that must not be refused:
	// refusing it would fail an attach that succeeds against AWS.
	const arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
	_, bundled := emulator.GetManagedPolicy(arn)
	require.False(t, bundled,
		"this test needs an ARN the catalog does not hold; pick another if it gets bundled")

	for _, target := range iamAttachTargets {
		t.Run(target.operation, func(t *testing.T) {
			logs := &iamWarnLog{}
			srv := newIAMTestServerWith(t, emulator.NewMemoryStateManager(), logs)

			resp := iamAttach(t, srv, target, arn)
			assert.Equal(t, http.StatusOK, resp.StatusCode, "AWS accepts this attach, so substrate must")
			require.NoError(t, resp.Body.Close())

			warns := logs.warningsAbout("does not bundle")
			require.Len(t, warns, 1, "the gap has to be visible somewhere, and the log is where")
			assert.Contains(t, warns[0], target.operation,
				"the warning names the operation that logged it")
			assert.Contains(t, warns[0], arn, "and the ARN it is about")
		})
	}
}

func TestAttachPolicy_ACustomerManagedARNThatResolvesNowhereWarnsDifferently(t *testing.T) {
	// A customer-managed ARN resolving nowhere means no CreatePolicy call ever made it, which
	// is a likelier mistake than an unbundled AWS policy — so it gets its own message rather
	// than being folded into the expected case.
	for _, target := range iamAttachTargets {
		t.Run(target.operation, func(t *testing.T) {
			logs := &iamWarnLog{}
			srv := newIAMTestServerWith(t, emulator.NewMemoryStateManager(), logs)

			resp := iamAttach(t, srv, target, "arn:aws:iam::123456789012:policy/never-created")
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())

			assert.Len(t, logs.warningsAbout("resolves in neither"), 1)
			assert.Empty(t, logs.warningsAbout("does not bundle"),
				"a customer-managed ARN is not the unbundled-AWS-policy case")
		})
	}
}

func TestAttachPolicy_AnUnknownEntityDoesNotWarnAboutTheARN(t *testing.T) {
	// The shape check runs before the entity lookup, because it needs no state; the warning
	// runs after, so an attach that is going to fail with NoSuchEntity does not also produce a
	// warning about a policy nothing was going to be attached to.
	cases := []struct {
		operation string
		field     string
	}{
		{"AttachUserPolicy", "UserName"},
		{"AttachRolePolicy", "RoleName"},
		{"AttachGroupPolicy", "GroupName"},
	}

	for _, tc := range cases {
		t.Run(tc.operation, func(t *testing.T) {
			logs := &iamWarnLog{}
			srv := newIAMTestServerWith(t, emulator.NewMemoryStateManager(), logs)

			resp := iamRequest(t, srv, tc.operation, map[string]any{
				tc.field:    "ghost",
				"PolicyArn": "arn:aws:iam::123456789012:policy/never-created",
			})
			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, "NoSuchEntity", result["__type"])
			assert.Empty(t, logs.messages(), "nothing was attached, so nothing is worth warning about")
		})
	}
}

func TestAttachPolicy_TheShapeCheckPrecedesAuthorization(t *testing.T) {
	// A malformed ARN is refused on its shape whatever the caller may do, so the refusal is a
	// property of the request rather than of the caller's permissions. This pins the order:
	// swapping the two would report AccessDenied for a request that is invalid regardless.
	for _, target := range iamAttachTargets {
		t.Run(target.operation, func(t *testing.T) {
			srv := newIAMTestServer(t)
			resp := iamAttach(t, srv, target, "AmazonS3ReadOnlyAccess")
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		})
	}
}

func TestAttachPolicy_AcceptsTheARNShapesAWSDoes(t *testing.T) {
	// Shapes that are well-formed but that substrate does not otherwise model: a non-default
	// partition, a nested path, and a name using the full policyNameType character set. Each
	// must be accepted — refusing one would refuse an ARN that is valid at AWS.
	cases := []struct {
		name      string
		policyARN string
	}{
		{"the China partition", "arn:aws-cn:iam::aws:policy/AmazonS3ReadOnlyAccess"},
		{"GovCloud", "arn:aws-us-gov:iam::aws:policy/AmazonS3ReadOnlyAccess"},
		{"an ISO partition", "arn:aws-iso-b:iam::123456789012:policy/thing"},
		{"a nested path", "arn:aws:iam::123456789012:policy/team/infra/deployer"},
		{"the full name charset", "arn:aws:iam::123456789012:policy/name+with=odd,chars.@-ok"},
		{"a service-role path", "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newIAMTestServer(t)
			resp := iamAttach(t, srv, iamAttachTargets[0], tc.policyARN)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		})
	}
}

func TestAttachPolicy_TheAttachedARNIsReadableBack(t *testing.T) {
	// The warning must not become a refusal by accident: an unresolvable ARN that warns is
	// still recorded, so ListAttachedUserPolicies reports it.
	const arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
	logs := &iamWarnLog{}
	srv := newIAMTestServerWith(t, emulator.NewMemoryStateManager(), logs)

	resp := iamAttach(t, srv, iamAttachTargets[0], arn)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamRequest(t, srv, "ListAttachedUserPolicies", map[string]any{"UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)

	attached, ok := result["AttachedPolicies"].([]any)
	require.True(t, ok)
	require.Len(t, attached, 1)
	entry, ok := attached[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, arn, entry["PolicyArn"])
}

func TestAttachPolicy_OverTheQueryProtocol(t *testing.T) {
	// A real client posts a form body, and the refusal has to survive that path: a check that
	// only fires for a hand-built JSON body is a check no SDK caller reaches.
	srv := newIAMTestServer(t)
	resp := iamFormRequest(t, srv, "CreateUser", map[string]string{"UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamFormRequest(t, srv, "AttachUserPolicy", map[string]string{
		"UserName":  "jill",
		"PolicyArn": "AmazonS3ReadOnlyAccess",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, "InvalidInput", result["__type"])

	resp = iamFormRequest(t, srv, "AttachUserPolicy", map[string]string{
		"UserName":  "jill",
		"PolicyArn": "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

// iamWarnLog is an [emulator.Logger] that keeps the Warn messages it is given, so a test can
// assert on a gap the operation reports only in a log.
type iamWarnLog struct {
	mu    sync.Mutex
	warns []string
}

func (l *iamWarnLog) Debug(_ string, _ ...any) {}
func (l *iamWarnLog) Info(_ string, _ ...any)  {}
func (l *iamWarnLog) Error(_ string, _ ...any) {}

func (l *iamWarnLog) Warn(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// The key-value args are folded into the line so a test can assert on the ARN the warning
	// names, not only on the message it carries.
	line := msg
	for _, arg := range args {
		line += " " + fmt.Sprint(arg)
	}
	l.warns = append(l.warns, line)
}

func (l *iamWarnLog) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.warns...)
}

// warningsAbout returns the collected warnings containing substr.
func (l *iamWarnLog) warningsAbout(substr string) []string {
	var out []string
	for _, msg := range l.messages() {
		if strings.Contains(msg, substr) {
			out = append(out, msg)
		}
	}
	return out
}
