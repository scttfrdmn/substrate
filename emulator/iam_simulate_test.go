package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The IAM policy simulator (#579): SimulatePrincipalPolicy and SimulateCustomPolicy.
//
// The responses here are decoded into a typed struct rather than through decodeIAMXML,
// because the members that matter most are the ones that are legitimately *empty*.
// "MatchedStatements is empty" is a real assertion: a handler reporting a matched
// statement for an implicitDeny would be wrong, and so would one that never reported
// any. A pointer member likewise makes *absence* observable, which is the whole point
// of PermissionsBoundaryDecisionDetail and OrganizationsDecisionDetail below.

// simulateXMLStatement mirrors one MatchedStatements member.
type simulateXMLStatement struct {
	SourcePolicyID   string `xml:"SourcePolicyId"`
	SourcePolicyType string `xml:"SourcePolicyType"`
}

// simulateXMLResult mirrors one EvaluationResult member.
type simulateXMLResult struct {
	ActionName           string                 `xml:"EvalActionName"`
	ResourceName         string                 `xml:"EvalResourceName"`
	Decision             string                 `xml:"EvalDecision"`
	MatchedStatements    []simulateXMLStatement `xml:"MatchedStatements>member"`
	MissingContextValues []string               `xml:"MissingContextValues>member"`

	// BoundaryDetail is a pointer so its absence is observable: the API's
	// AllowedByPermissionsBoundary=false means "the boundary blocked this", so an
	// entity with no boundary must not report the element at all.
	BoundaryDetail *struct {
		Allowed bool `xml:"AllowedByPermissionsBoundary"`
	} `xml:"PermissionsBoundaryDecisionDetail"`

	// OrganizationsDetail must stay absent: substrate never evaluates an SCP, so
	// reporting the element would report a bound it does not enforce.
	OrganizationsDetail *struct {
		Allowed bool `xml:"AllowedByOrganizations"`
	} `xml:"OrganizationsDecisionDetail"`
}

// simulateXMLResponse mirrors a SimulatePolicyResponse from either operation, so one
// decoder serves both.
type simulateXMLResponse struct {
	PrincipalResults []simulateXMLResult `xml:"SimulatePrincipalPolicyResult>EvaluationResults>member"`
	CustomResults    []simulateXMLResult `xml:"SimulateCustomPolicyResult>EvaluationResults>member"`
	PrincipalTrunc   bool                `xml:"SimulatePrincipalPolicyResult>IsTruncated"`
	CustomTrunc      bool                `xml:"SimulateCustomPolicyResult>IsTruncated"`
	PrincipalMarker  string              `xml:"SimulatePrincipalPolicyResult>Marker"`
	CustomMarker     string              `xml:"SimulateCustomPolicyResult>Marker"`
}

// results returns whichever operation's results the response carried.
func (r simulateXMLResponse) results() []simulateXMLResult {
	if len(r.PrincipalResults) > 0 {
		return r.PrincipalResults
	}
	return r.CustomResults
}

// page returns whichever operation's IsTruncated and Marker the response carried.
func (r simulateXMLResponse) page() (bool, string) {
	if r.PrincipalTrunc || r.PrincipalMarker != "" {
		return r.PrincipalTrunc, r.PrincipalMarker
	}
	return r.CustomTrunc, r.CustomMarker
}

// byAction indexes the results by action name, for the single-resource cases.
func (r simulateXMLResponse) byAction() map[string]simulateXMLResult {
	results := r.results()
	out := make(map[string]simulateXMLResult, len(results))
	for _, result := range results {
		out[result.ActionName] = result
	}
	return out
}

// decisions maps each result's action to its decision, which is the shape most
// assertions here want: one call, several actions, several answers.
func (r simulateXMLResponse) decisions() map[string]string {
	results := r.results()
	out := make(map[string]string, len(results))
	for _, result := range results {
		out[result.ActionName] = result.Decision
	}
	return out
}

// iamRequireOK runs an IAM setup call and fails the test unless it succeeded, so a
// simulation never asserts against state that was never written.
func iamRequireOK(t *testing.T, srv *emulator.Server, operation string, body any) {
	t.Helper()
	resp := iamRequest(t, srv, operation, body)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", operation, raw)
}

// iamDecodeSimulation reads a simulation response body, requiring 200.
func iamDecodeSimulation(t *testing.T, resp *http.Response) simulateXMLResponse {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", raw)

	// An unrouted operation answers InvalidAction, which the status check above already
	// catches — but name it, because an InvalidAction that ever came back 200 would
	// decode to zero results and read as an empty page rather than a routing failure.
	// That is #636 exactly.
	require.NotContains(t, string(raw), "InvalidAction")

	var decoded simulateXMLResponse
	require.NoError(t, xml.Unmarshal(raw, &decoded), "body: %s", raw)
	return decoded
}

// iamSimulate posts a simulation through the JSON-body path and decodes it.
func iamSimulate(t *testing.T, srv *emulator.Server, operation string, body map[string]any) simulateXMLResponse {
	t.Helper()
	return iamDecodeSimulation(t, iamRequest(t, srv, operation, body))
}

// iamPolicyJSON marshals a one-statement policy document to the string form the
// PolicyInputList members take.
func iamPolicyJSON(t *testing.T, effect string, actions []string, resource string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Effect":   effect,
			"Action":   actions,
			"Resource": resource,
		}},
	})
	require.NoError(t, err)
	return string(raw)
}

// TestSimulatePrincipalPolicy_ThreeDecisionsInOneResponse is the decisive test.
//
// One user carries a managed allow (through a group), an inline deny, and nothing at
// all for a third action. All three actions are simulated in a single call, so a
// handler that returns a uniform answer — or that collapses explicitDeny and
// implicitDeny into one "denied" — fails here. #579 is explicit that collapsing them
// "would be worse than none, because it would look like coverage": a consumer
// asserting "the policy explicitly forbids this" would pass on what is really a
// missing grant.
func TestSimulatePrincipalPolicy_ThreeDecisionsInOneResponse(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	iamCreateGroupAndUser(t, srv, "devs", "jill")
	iamRequireOK(t, srv, "AddUserToGroup", map[string]any{"GroupName": "devs", "UserName": "jill"})
	// The allow arrives through the *group*, which is why the group operations had to
	// land first: AWS includes a user's group policies in the simulation, and before
	// this release no operation could put a policy on a group at all.
	iamRequireOK(t, srv, "AttachGroupPolicy", map[string]any{
		"GroupName": "devs",
		"PolicyArn": "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
	})
	iamRequireOK(t, srv, "PutUserPolicy", map[string]any{
		"UserName":       "jill",
		"PolicyName":     "nodelete",
		"PolicyDocument": iamPolicyJSON(t, "Deny", []string{"s3:DeleteObject"}, "*"),
	})

	got := iamSimulate(t, srv, "SimulatePrincipalPolicy", map[string]any{
		"PolicySourceArn": "arn:aws:iam::123456789012:user/jill",
		"ActionNames":     []string{"s3:GetObject", "s3:DeleteObject", "s3:PutObject"},
		"ResourceArns":    []string{"arn:aws:s3:::my-bucket/key"},
	})

	require.Len(t, got.results(), 3)
	assert.Equal(t, map[string]string{
		"s3:GetObject":    "allowed",
		"s3:DeleteObject": "explicitDeny",
		"s3:PutObject":    "implicitDeny",
	}, got.decisions())

	byAction := got.byAction()

	// The allow must name the group's managed policy by *name*, per the reference's
	// sample response — not by ARN.
	allow := byAction["s3:GetObject"]
	require.Len(t, allow.MatchedStatements, 1)
	assert.Equal(t, "AmazonS3ReadOnlyAccess", allow.MatchedStatements[0].SourcePolicyID)
	assert.Equal(t, "IAM Policy", allow.MatchedStatements[0].SourcePolicyType)

	// "the deny statement is the only entry included in the result" — one statement,
	// and it is the inline one.
	deny := byAction["s3:DeleteObject"]
	require.Len(t, deny.MatchedStatements, 1)
	assert.Equal(t, "nodelete", deny.MatchedStatements[0].SourcePolicyID)

	// An implicit deny matched nothing, so it reports nothing. A statement here would
	// name a policy that did not decide.
	assert.Empty(t, byAction["s3:PutObject"].MatchedStatements)

	// No boundary is attached and no SCP is evaluated, so neither detail element may
	// appear on any result: a present-and-false boundary detail claims a block that
	// never happened.
	for _, r := range got.results() {
		assert.Nil(t, r.BoundaryDetail, r.ActionName)
		assert.Nil(t, r.OrganizationsDetail, r.ActionName)
	}

	// The resource is reported back on each result, which is what lets a caller
	// correlate an answer when several resources were named.
	assert.Equal(t, "arn:aws:s3:::my-bucket/key", allow.ResourceName)
}

// TestSimulatePrincipalPolicy_AgreesWithTheEnforcedDecision pins the property that
// makes the simulator worth having: it runs the same evaluator the request gate
// enforces with.
//
// A simulated decision that could disagree with an enforced one would be worse than no
// simulator at all, because a consumer would trust the preflight and then be refused —
// or the reverse. Here the same user, the same policy and the same two actions go
// through both paths, so a second evaluator drifting from the first fails.
func TestSimulatePrincipalPolicy_AgreesWithTheEnforcedDecision(t *testing.T) {
	t.Parallel()
	srv := newTrustPolicyTestServer(t)

	// The caller is granted iam:ListUsers and the simulate operation but nothing else,
	// so the enforced answer for iam:DeleteUser is a real denial rather than the
	// not-enforced fallback resolveIAMEntity takes for an unknown principal.
	require.Equal(t, http.StatusOK,
		trustIAMCall(t, srv, "CreateUser", map[string]any{"UserName": "jill"}).StatusCode)
	require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "PutUserPolicy", map[string]any{
		"UserName":   "jill",
		"PolicyName": "listonly",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Action":["iam:ListUsers","iam:SimulatePrincipalPolicy"],"Resource":"*"}]}`,
	}).StatusCode)

	resp := trustIAMCall(t, srv, "CreateAccessKey", map[string]any{"UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	var created struct {
		AccessKeyID string `xml:"CreateAccessKeyResult>AccessKey>AccessKeyId"`
	}
	require.NoError(t, xml.Unmarshal(raw, &created))
	accessKeyID := created.AccessKeyID
	require.NotEmpty(t, accessKeyID)

	simulated := iamDecodeSimulation(t, iamCallAs(t, srv, accessKeyID, "SimulatePrincipalPolicy",
		map[string]any{
			"PolicySourceArn": "arn:aws:iam::123456789012:user/jill",
			"ActionNames":     []string{"iam:ListUsers", "iam:DeleteUser"},
		}))
	assert.Equal(t, map[string]string{
		"iam:ListUsers":  "allowed",
		"iam:DeleteUser": "implicitDeny",
	}, simulated.decisions())

	// And the enforced path agrees, action for action.
	listed := iamCallAs(t, srv, accessKeyID, "ListUsers", map[string]any{})
	require.NoError(t, listed.Body.Close())
	assert.Equal(t, http.StatusOK, listed.StatusCode, "the simulation answered allowed")

	deleted := iamCallAs(t, srv, accessKeyID, "DeleteUser", map[string]any{"UserName": "jill"})
	require.NoError(t, deleted.Body.Close())
	assert.Equal(t, http.StatusForbidden, deleted.StatusCode, "the simulation answered implicitDeny")
}

// TestSimulateCustomPolicy_NeedsNoEntity covers the operation's defining difference:
// it resolves no principal, which is why the model declares NoSuchEntity for
// SimulatePrincipalPolicy and not for this one.
func TestSimulateCustomPolicy_NeedsNoEntity(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames": []string{"s3:GetObject", "s3:PutObject"},
		"PolicyInputList": []string{
			iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"),
		},
	})

	assert.Equal(t, map[string]string{
		"s3:GetObject": "allowed",
		"s3:PutObject": "implicitDeny",
	}, got.decisions())

	// A policy supplied as a string is named by its position, per the sample
	// response: "PolicyInputList.1".
	allow := got.byAction()["s3:GetObject"]
	require.Len(t, allow.MatchedStatements, 1)
	assert.Equal(t, "PolicyInputList.1", allow.MatchedStatements[0].SourcePolicyID)
	assert.Equal(t, "IAM Policy", allow.MatchedStatements[0].SourcePolicyType)
}

// TestSimulateCustomPolicy_PolicyInputsAreNamedByPosition pins that the position is
// the input's own index, so a caller can tell which of several documents decided.
func TestSimulateCustomPolicy_PolicyInputsAreNamedByPosition(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames": []string{"s3:PutObject"},
		"PolicyInputList": []string{
			iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"),
			iamPolicyJSON(t, "Allow", []string{"s3:PutObject"}, "*"),
		},
	})

	require.Len(t, got.results(), 1)
	require.Len(t, got.results()[0].MatchedStatements, 1)
	assert.Equal(t, "PolicyInputList.2", got.results()[0].MatchedStatements[0].SourcePolicyID)
}

// TestSimulate_ResourcePolicyIsReportedAsSuch covers the second SourcePolicyType
// value: a statement from a resource-based policy reports "Resource Policy", which is
// how a caller tells a bucket policy's grant from an identity policy's.
func TestSimulate_ResourcePolicyIsReportedAsSuch(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames":     []string{"s3:GetObject"},
		"PolicyInputList": []string{iamPolicyJSON(t, "Allow", []string{"s3:ListBucket"}, "*")},
		"ResourcePolicy":  iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"),
	})

	require.Len(t, got.results(), 1)
	require.Len(t, got.results()[0].MatchedStatements, 1)
	assert.Equal(t, "ResourcePolicy", got.results()[0].MatchedStatements[0].SourcePolicyID)
	assert.Equal(t, "Resource Policy", got.results()[0].MatchedStatements[0].SourcePolicyType)
}

// TestSimulate_BoundaryFlipsAllowedToImplicitDeny covers the permissions boundary.
//
// A boundary that does not allow turns an allow into an *implicit* deny, not an
// explicit one — the boundary caps what is reachable rather than denying the action by
// name — and the detail element must then report false so a caller can tell why.
func TestSimulate_BoundaryFlipsAllowedToImplicitDeny(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// boundaryAllows is the action the supplied boundary permits; empty means no
		// boundary was supplied at all.
		boundaryAllows string
		wantDecision   string
		wantDetail     bool
		wantAllowed    bool
		wantMatched    bool
	}{
		{
			name:         "no boundary supplied leaves the identity decision alone",
			wantDecision: "allowed",
			wantDetail:   false,
			wantMatched:  true,
		},
		{
			name:           "a boundary that allows the action leaves it allowed",
			boundaryAllows: "s3:*",
			wantDecision:   "allowed",
			wantDetail:     true,
			wantAllowed:    true,
			wantMatched:    true,
		},
		{
			// The boundary allows something else entirely, so the requested action is
			// outside it.
			name:           "a boundary that does not allow the action denies it implicitly",
			boundaryAllows: "ec2:DescribeInstances",
			wantDecision:   "implicitDeny",
			wantDetail:     true,
			wantAllowed:    false,
			wantMatched:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)

			body := map[string]any{
				"ActionNames":     []string{"s3:GetObject"},
				"PolicyInputList": []string{iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*")},
			}
			if tc.boundaryAllows != "" {
				body["PermissionsBoundaryPolicyInputList"] = []string{
					iamPolicyJSON(t, "Allow", []string{tc.boundaryAllows}, "*"),
				}
			}

			got := iamSimulate(t, srv, "SimulateCustomPolicy", body)
			require.Len(t, got.results(), 1)
			result := got.results()[0]

			assert.Equal(t, tc.wantDecision, result.Decision)
			if tc.wantDetail {
				require.NotNil(t, result.BoundaryDetail)
				assert.Equal(t, tc.wantAllowed, result.BoundaryDetail.Allowed)
			} else {
				assert.Nil(t, result.BoundaryDetail,
					"no boundary applied, so the detail element must be absent")
			}

			if tc.wantMatched {
				assert.NotEmpty(t, result.MatchedStatements)
			} else {
				// The identity statement did not decide the outcome, so reporting it
				// would tell the caller the request was allowed by it.
				assert.Empty(t, result.MatchedStatements)
			}
		})
	}
}

// TestSimulate_BoundaryLeavesAnExplicitDenyExplicit pins that the boundary arm only
// touches an *allow*.
//
// An identity policy's explicit deny is already the final answer, and downgrading it
// to implicitDeny would lose the distinction the operation exists to report.
func TestSimulate_BoundaryLeavesAnExplicitDenyExplicit(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames":     []string{"s3:GetObject"},
		"PolicyInputList": []string{iamPolicyJSON(t, "Deny", []string{"s3:GetObject"}, "*")},
		"PermissionsBoundaryPolicyInputList": []string{
			iamPolicyJSON(t, "Allow", []string{"s3:*"}, "*"),
		},
	})

	require.Len(t, got.results(), 1)
	assert.Equal(t, "explicitDeny", got.results()[0].Decision)
	require.NotNil(t, got.results()[0].BoundaryDetail)
	assert.True(t, got.results()[0].BoundaryDetail.Allowed)
	assert.NotEmpty(t, got.results()[0].MatchedStatements,
		"the deny statement decided, so it must still be named")
}

// TestSimulatePrincipalPolicy_UsesTheEntitysStoredBoundary covers the other half of
// the boundary rule.
//
// AWS: "if a permissions boundary is attached to an entity and you pass in a different
// permissions boundary policy using this parameter, then the new permissions boundary
// policy is used for the simulation." So with no PermissionsBoundaryPolicyInputList the
// entity's own attached boundary applies, and with one it does not.
func TestSimulatePrincipalPolicy_UsesTheEntitysStoredBoundary(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	iamRequireOK(t, srv, "CreateUser", map[string]any{"UserName": "jill"})
	iamRequireOK(t, srv, "PutUserPolicy", map[string]any{
		"UserName":       "jill",
		"PolicyName":     "s3full",
		"PolicyDocument": iamPolicyJSON(t, "Allow", []string{"s3:*"}, "*"),
	})
	// AmazonS3ReadOnlyAccess as a boundary permits s3:Get*/List*/Describe* but not a
	// write, so it caps the s3:* grant above.
	iamRequireOK(t, srv, "PutUserPermissionsBoundary", map[string]any{
		"UserName":            "jill",
		"PermissionsBoundary": "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
	})

	got := iamSimulate(t, srv, "SimulatePrincipalPolicy", map[string]any{
		"PolicySourceArn": "arn:aws:iam::123456789012:user/jill",
		"ActionNames":     []string{"s3:GetObject", "s3:PutObject"},
	})

	assert.Equal(t, map[string]string{
		"s3:GetObject": "allowed",
		"s3:PutObject": "implicitDeny",
	}, got.decisions())
	for _, r := range got.results() {
		require.NotNil(t, r.BoundaryDetail, r.ActionName)
		assert.Equal(t, r.ActionName == "s3:GetObject", r.BoundaryDetail.Allowed, r.ActionName)
	}

	// A supplied boundary overrides the stored one, so the same call with a permissive
	// boundary allows the write.
	overridden := iamSimulate(t, srv, "SimulatePrincipalPolicy", map[string]any{
		"PolicySourceArn": "arn:aws:iam::123456789012:user/jill",
		"ActionNames":     []string{"s3:PutObject"},
		"PermissionsBoundaryPolicyInputList": []string{
			iamPolicyJSON(t, "Allow", []string{"s3:*"}, "*"),
		},
	})
	require.Len(t, overridden.results(), 1)
	assert.Equal(t, "allowed", overridden.results()[0].Decision)
}

// TestSimulate_MissingContextValues covers #579's fourth point: a conditional grant
// must not read as a clean answer.
//
// A statement ruled out only by a condition on a key the request supplied no value for
// is the reportable case — the answer would change given a value — and without the list
// a caller sees an implicitDeny with no indication of that.
func TestSimulate_MissingContextValues(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	conditional := `{"Version":"2012-10-17","Statement":[{"Sid":"MFAOnly",` +
		`"Effect":"Allow","Action":"s3:GetObject","Resource":"*",` +
		`"Condition":{"Bool":{"aws:MultiFactorAuthPresent":"true"}}}]}`

	got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames":     []string{"s3:GetObject"},
		"PolicyInputList": []string{conditional},
	})
	require.Len(t, got.results(), 1)
	assert.Equal(t, "implicitDeny", got.results()[0].Decision)
	assert.Equal(t, []string{"aws:MultiFactorAuthPresent"}, got.results()[0].MissingContextValues)

	// A statement about a *different* action names keys that tell this caller nothing,
	// so they must not be reported.
	unrelated := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"ec2:RunInstances","Resource":"*",` +
		`"Condition":{"StringEquals":{"ec2:InstanceType":"t3.micro"}}}]}`
	quiet := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames":     []string{"s3:GetObject"},
		"PolicyInputList": []string{unrelated},
	})
	require.Len(t, quiet.results(), 1)
	assert.Empty(t, quiet.results()[0].MissingContextValues)
}

// TestSimulate_ContextEntriesSatisfyACondition is the other side of the same rule:
// once the caller supplies the key, the conditional grant decides and the key is no
// longer missing.
//
// It is also the only test of the nested query-protocol shape
// ContextEntries.member.1.ContextKeyValues.member.1, which is why iamMemberStructs
// keeps its suffixes verbatim (#639). There is no JSON-body form of a nested member
// list at all, so this member can only be driven through a form body.
func TestSimulate_ContextEntriesSatisfyACondition(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	conditional := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"s3:GetObject","Resource":"*",` +
		`"Condition":{"Bool":{"aws:MultiFactorAuthPresent":"true"}}}]}`

	got := iamDecodeSimulation(t, iamFormRequest(t, srv, "SimulateCustomPolicy", map[string]string{
		"ActionNames.member.1":                              "s3:GetObject",
		"PolicyInputList.member.1":                          conditional,
		"ContextEntries.member.1.ContextKeyName":            "aws:MultiFactorAuthPresent",
		"ContextEntries.member.1.ContextKeyValues.member.1": "true",
		"ContextEntries.member.1.ContextKeyType":            "boolean",
	}))

	require.Len(t, got.results(), 1)
	assert.Equal(t, "allowed", got.results()[0].Decision)
	assert.Empty(t, got.results()[0].MissingContextValues,
		"the caller supplied the key, so it is not missing")
}

// TestSimulate_CallerArnPopulatesPrincipalArn covers a condition key the simulation
// determines for itself.
//
// A policy conditioned on aws:PrincipalArn would otherwise report the key as missing
// when the caller had in fact told substrate what it is — through CallerArn, or through
// PolicySourceArn, which CallerArn defaults to.
func TestSimulate_CallerArnPopulatesPrincipalArn(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	iamRequireOK(t, srv, "CreateUser", map[string]any{"UserName": "jill"})
	iamRequireOK(t, srv, "PutUserPolicy", map[string]any{
		"UserName":   "jill",
		"PolicyName": "selfonly",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Action":"s3:GetObject","Resource":"*","Condition":{"ArnEquals":` +
			`{"aws:PrincipalArn":"arn:aws:iam::123456789012:user/jill"}}}]}`,
	})

	// CallerArn defaults to PolicySourceArn, so the condition is satisfied without the
	// caller naming the key at all.
	got := iamSimulate(t, srv, "SimulatePrincipalPolicy", map[string]any{
		"PolicySourceArn": "arn:aws:iam::123456789012:user/jill",
		"ActionNames":     []string{"s3:GetObject"},
	})
	require.Len(t, got.results(), 1)
	assert.Equal(t, "allowed", got.results()[0].Decision)
	assert.Empty(t, got.results()[0].MissingContextValues,
		"the simulation knows the principal, so the key is not missing")

	// A different CallerArn is a different principal, so the same policy no longer
	// matches — which proves the value came from CallerArn rather than the key being
	// ignored outright.
	other := iamSimulate(t, srv, "SimulatePrincipalPolicy", map[string]any{
		"PolicySourceArn": "arn:aws:iam::123456789012:user/jill",
		"CallerArn":       "arn:aws:iam::123456789012:user/bob",
		"ActionNames":     []string{"s3:GetObject"},
	})
	require.Len(t, other.results(), 1)
	assert.Equal(t, "implicitDeny", other.results()[0].Decision)
}

// TestSimulate_ResourceArnsDefaultToStar covers the documented default: "If this
// parameter is not provided, then the value defaults to * (all resources)."
//
// A default of "" instead would silently match every statement regardless of its
// Resource element, because resourceMatches treats an empty resource as a match — so
// the decision, not just the reported EvalResourceName, is asserted here.
func TestSimulate_ResourceArnsDefaultToStar(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames": []string{"s3:GetObject"},
		"PolicyInputList": []string{
			iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "arn:aws:s3:::other/*"),
		},
	})

	require.Len(t, got.results(), 1)
	assert.Equal(t, "*", got.results()[0].ResourceName)
	// The policy grants the action only on some *other* bucket, so a simulation
	// against "*" is not allowed. Were the default "", it would be.
	assert.Equal(t, "implicitDeny", got.results()[0].Decision)
}

// TestSimulate_EveryActionAgainstEveryResource covers the cross product the reference
// describes: each API in ActionNames is evaluated against each resource in
// ResourceArns.
func TestSimulate_EveryActionAgainstEveryResource(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames": []string{"s3:GetObject", "s3:PutObject"},
		"ResourceArns": []string{
			"arn:aws:s3:::allowed/key",
			"arn:aws:s3:::denied/key",
		},
		"PolicyInputList": []string{
			iamPolicyJSON(t, "Allow", []string{"s3:*"}, "arn:aws:s3:::allowed/*"),
		},
	})

	require.Len(t, got.results(), 4, "two actions × two resources")
	type pair struct{ action, resource string }
	decisions := make(map[pair]string, 4)
	for _, r := range got.results() {
		decisions[pair{r.ActionName, r.ResourceName}] = r.Decision
	}
	assert.Equal(t, map[pair]string{
		{"s3:GetObject", "arn:aws:s3:::allowed/key"}: "allowed",
		{"s3:GetObject", "arn:aws:s3:::denied/key"}:  "implicitDeny",
		{"s3:PutObject", "arn:aws:s3:::allowed/key"}: "allowed",
		{"s3:PutObject", "arn:aws:s3:::denied/key"}:  "implicitDeny",
	}, decisions)
}

// TestSimulate_Pagination covers MaxItems and the Marker cursor over the flattened
// result list.
//
// MaxItems defaults to 100 and is valid 1–1000, from the model's maxItemsType and the
// reference's "the number of items defaults to 100".
func TestSimulate_Pagination(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	body := map[string]any{
		"ActionNames":     []string{"s3:GetObject", "s3:PutObject", "s3:DeleteObject"},
		"PolicyInputList": []string{iamPolicyJSON(t, "Allow", []string{"s3:*"}, "*")},
		"MaxItems":        2,
	}
	first := iamSimulate(t, srv, "SimulateCustomPolicy", body)
	require.Len(t, first.results(), 2)
	truncated, marker := first.page()
	assert.True(t, truncated)
	require.NotEmpty(t, marker)

	body["Marker"] = marker
	second := iamSimulate(t, srv, "SimulateCustomPolicy", body)
	require.Len(t, second.results(), 1)
	secondTruncated, secondMarker := second.page()
	assert.False(t, secondTruncated)
	assert.Empty(t, secondMarker)

	// The pages must be disjoint and cover every action — a cursor that silently
	// restarted would still produce the counts above.
	seen := map[string]bool{}
	for _, r := range append(first.results(), second.results()...) {
		assert.False(t, seen[r.ActionName], "%s appeared twice", r.ActionName)
		seen[r.ActionName] = true
	}
	assert.Len(t, seen, 3)

	// With no MaxItems every result fits on one page, because the default is 100.
	delete(body, "MaxItems")
	delete(body, "Marker")
	all := iamSimulate(t, srv, "SimulateCustomPolicy", body)
	assert.Len(t, all.results(), 3)
	allTruncated, _ := all.page()
	assert.False(t, allTruncated)
}

// TestSimulate_ActionsAreNotCappedAt25 is the direct form of the correction #579
// needs.
//
// #579 asserted that a call "caps at 25 actions"; no action-count limit appears in the
// API reference or the model, so modeling one would refuse requests AWS accepts. Thirty
// actions in one call therefore get thirty answers on one page — which also pins
// MaxItems' default at 100 rather than 25, since a default of 25 would truncate here.
func TestSimulate_ActionsAreNotCappedAt25(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	actions := make([]string, 0, 30)
	for i := 0; i < 30; i++ {
		actions = append(actions, fmt.Sprintf("s3:GetObjectVersion%d", i))
	}

	got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames":     actions,
		"PolicyInputList": []string{iamPolicyJSON(t, "Allow", []string{"s3:*"}, "*")},
	})
	require.Len(t, got.results(), 30)
	truncated, _ := got.page()
	assert.False(t, truncated)
}

// TestSimulate_RefusesBadInput covers the InvalidInput arms and the one NoSuchEntity.
//
// The NoSuchEntity case is asymmetric on purpose: SimulateCustomPolicy resolves no
// entity, so the model does not declare the error for it, and answering it there would
// report a failure to find something the operation never looks for.
func TestSimulate_RefusesBadInput(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamRequireOK(t, srv, "CreateUser", map[string]any{"UserName": "jill"})

	allowAll := iamPolicyJSON(t, "Allow", []string{"s3:*"}, "*")

	tests := []struct {
		name      string
		operation string
		body      map[string]any
		wantCode  string
		wantHTTP  int
	}{
		{
			name:      "the principal form requires PolicySourceArn",
			operation: "SimulatePrincipalPolicy",
			body:      map[string]any{"ActionNames": []string{"s3:GetObject"}},
			wantCode:  "InvalidInput",
			wantHTTP:  http.StatusBadRequest,
		},
		{
			name:      "the principal form requires ActionNames",
			operation: "SimulatePrincipalPolicy",
			body: map[string]any{
				"PolicySourceArn": "arn:aws:iam::123456789012:user/jill",
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:      "the custom form requires PolicyInputList",
			operation: "SimulateCustomPolicy",
			body:      map[string]any{"ActionNames": []string{"s3:GetObject"}},
			wantCode:  "InvalidInput",
			wantHTTP:  http.StatusBadRequest,
		},
		{
			name:      "the custom form requires ActionNames",
			operation: "SimulateCustomPolicy",
			body:      map[string]any{"PolicyInputList": []string{allowAll}},
			wantCode:  "InvalidInput",
			wantHTTP:  http.StatusBadRequest,
		},
		{
			// "Each operation must include the service identifier, such as
			// iam:CreateUser." An unprefixed action can never match a statement, so
			// answering implicitDeny would report a decision about a malformed request.
			name:      "an action with no service prefix is refused",
			operation: "SimulateCustomPolicy",
			body: map[string]any{
				"ActionNames":     []string{"GetObject"},
				"PolicyInputList": []string{allowAll},
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			// The reference reserves PolicyEvaluation 500 for a policy that "could not
			// be successfully evaluated"; a document that will not parse is the
			// caller's error.
			name:      "an unparseable policy is the caller's error, not PolicyEvaluation",
			operation: "SimulateCustomPolicy",
			body: map[string]any{
				"ActionNames":     []string{"s3:GetObject"},
				"PolicyInputList": []string{"{not json"},
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:      "MaxItems above the range is refused",
			operation: "SimulateCustomPolicy",
			body: map[string]any{
				"ActionNames":     []string{"s3:GetObject"},
				"PolicyInputList": []string{allowAll},
				"MaxItems":        1001,
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:      "MaxItems below the range is refused",
			operation: "SimulateCustomPolicy",
			body: map[string]any{
				"ActionNames":     []string{"s3:GetObject"},
				"PolicyInputList": []string{allowAll},
				"MaxItems":        -1,
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:      "an ARN that names no IAM entity type is refused",
			operation: "SimulatePrincipalPolicy",
			body: map[string]any{
				"PolicySourceArn": "arn:aws:s3:::my-bucket",
				"ActionNames":     []string{"s3:GetObject"},
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:      "an entity ARN with no name is refused",
			operation: "SimulatePrincipalPolicy",
			body: map[string]any{
				"PolicySourceArn": "arn:aws:iam::123456789012:user",
				"ActionNames":     []string{"s3:GetObject"},
			},
			wantCode: "InvalidInput",
			wantHTTP: http.StatusBadRequest,
		},
		{
			name:      "an unknown principal is NoSuchEntity",
			operation: "SimulatePrincipalPolicy",
			body: map[string]any{
				"PolicySourceArn": "arn:aws:iam::123456789012:user/ghost",
				"ActionNames":     []string{"s3:GetObject"},
			},
			wantCode: "NoSuchEntity",
			wantHTTP: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp := iamRequest(t, srv, tc.operation, tc.body)
			require.Equal(t, tc.wantHTTP, resp.StatusCode)
			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, tc.wantCode, result["__type"])
		})
	}
}

// TestSimulateCustomPolicy_HasNoNoSuchEntityPath pins the asymmetry directly: the
// custom form ignores PolicySourceArn entirely, so naming a nonexistent entity cannot
// produce NoSuchEntity.
func TestSimulateCustomPolicy_HasNoNoSuchEntityPath(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"PolicySourceArn": "arn:aws:iam::123456789012:user/ghost",
		"ActionNames":     []string{"s3:GetObject"},
		"PolicyInputList": []string{iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*")},
	})
	require.Len(t, got.results(), 1)
	assert.Equal(t, "allowed", got.results()[0].Decision)
}

// TestSimulatePrincipalPolicy_RoleAndGroupSources covers the other two entity types
// the reference names: "The entity can be an IAM user, group, or role."
//
// A role must ignore group memberships — nothing calls AWS *as* a group, so a role's
// permissions never include one — and a group simulated directly reports its own
// policies.
func TestSimulatePrincipalPolicy_RoleAndGroupSources(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	iamRequireOK(t, srv, "CreateRole", map[string]any{
		"RoleName":                 "worker",
		"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
	})
	iamRequireOK(t, srv, "PutRolePolicy", map[string]any{
		"RoleName":       "worker",
		"PolicyName":     "readonly",
		"PolicyDocument": iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"),
	})
	iamRequireOK(t, srv, "CreateGroup", map[string]any{"GroupName": "devs"})
	iamRequireOK(t, srv, "PutGroupPolicy", map[string]any{
		"GroupName":      "devs",
		"PolicyName":     "listers",
		"PolicyDocument": iamPolicyJSON(t, "Allow", []string{"s3:ListBucket"}, "*"),
	})

	role := iamSimulate(t, srv, "SimulatePrincipalPolicy", map[string]any{
		"PolicySourceArn": "arn:aws:iam::123456789012:role/worker",
		"ActionNames":     []string{"s3:GetObject", "s3:ListBucket"},
	})
	assert.Equal(t, map[string]string{
		"s3:GetObject":  "allowed",
		"s3:ListBucket": "implicitDeny",
	}, role.decisions())

	group := iamSimulate(t, srv, "SimulatePrincipalPolicy", map[string]any{
		"PolicySourceArn": "arn:aws:iam::123456789012:group/devs",
		"ActionNames":     []string{"s3:GetObject", "s3:ListBucket"},
	})
	assert.Equal(t, map[string]string{
		"s3:GetObject":  "implicitDeny",
		"s3:ListBucket": "allowed",
	}, group.decisions())
	listed := group.byAction()["s3:ListBucket"]
	require.Len(t, listed.MatchedStatements, 1)
	assert.Equal(t, "listers", listed.MatchedStatements[0].SourcePolicyID)
}

// TestSimulatePrincipalPolicy_AdministratorAccessNamesItself pins that the simulator
// does *not* take loadPoliciesForPrincipal's AdministratorAccess shortcut.
//
// That fast path synthesizes an allow-all document, which has no name — and the whole
// output of a simulation is which policy decided. The catalog document is evaluated
// instead, so the answer is the same and the report is usable.
func TestSimulatePrincipalPolicy_AdministratorAccessNamesItself(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	iamRequireOK(t, srv, "CreateUser", map[string]any{"UserName": "admin"})
	iamRequireOK(t, srv, "AttachUserPolicy", map[string]any{
		"UserName":  "admin",
		"PolicyArn": "arn:aws:iam::aws:policy/AdministratorAccess",
	})

	got := iamSimulate(t, srv, "SimulatePrincipalPolicy", map[string]any{
		"PolicySourceArn": "arn:aws:iam::123456789012:user/admin",
		"ActionNames":     []string{"ec2:TerminateInstances"},
	})
	require.Len(t, got.results(), 1)
	assert.Equal(t, "allowed", got.results()[0].Decision)
	require.Len(t, got.results()[0].MatchedStatements, 1)
	assert.Equal(t, "AdministratorAccess", got.results()[0].MatchedStatements[0].SourcePolicyID)
}

// TestSimulatePrincipalPolicy_UnbundledManagedPolicyContributesNothing covers the
// catalog gap honestly.
//
// Substrate bundles 52 of the ~1,200 AWS managed policies, so an attachment naming an
// unbundled one has an unknown document. It must contribute no statements rather than a
// guess, and it must not fail the simulation: an attach of an unbundled-but-real ARN
// succeeds, so a simulation of the resulting user has to answer something.
func TestSimulatePrincipalPolicy_UnbundledManagedPolicyContributesNothing(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	// AmazonAthenaFullAccess is a real AWS managed policy the catalog does not carry,
	// so nothing resolves its document. Asserted rather than assumed, because a later
	// release adding it to the catalog would otherwise turn this test green for the
	// wrong reason.
	const unbundled = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
	_, bundled := emulator.GetManagedPolicy(unbundled)
	require.False(t, bundled, "this test needs a policy the catalog does not carry")

	iamRequireOK(t, srv, "CreateUser", map[string]any{"UserName": "jill"})
	iamRequireOK(t, srv, "AttachUserPolicy", map[string]any{
		"UserName":  "jill",
		"PolicyArn": unbundled,
	})

	got := iamSimulate(t, srv, "SimulatePrincipalPolicy", map[string]any{
		"PolicySourceArn": "arn:aws:iam::123456789012:user/jill",
		"ActionNames":     []string{"athena:StartQueryExecution"},
	})
	require.Len(t, got.results(), 1)
	assert.Equal(t, "implicitDeny", got.results()[0].Decision)
	assert.Empty(t, got.results()[0].MatchedStatements)
}

// TestSimulatePrincipalPolicy_CustomerManagedPolicyIsResolved covers the other half of
// the resolution order: a policy created through CreatePolicy resolves from state, with
// its own name reported.
func TestSimulatePrincipalPolicy_CustomerManagedPolicyIsResolved(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	iamRequireOK(t, srv, "CreateUser", map[string]any{"UserName": "jill"})

	// The ARN is read back rather than constructed: an unsigned request resolves to
	// the fallback account, so a hardcoded 123456789012 ARN would attach an
	// unresolvable policy and the test would pass for the wrong reason — an attach of
	// an unknown ARN succeeds by design.
	created := iamRequest(t, srv, "CreatePolicy", map[string]any{
		"PolicyName":     "listbuckets",
		"PolicyDocument": iamPolicyJSON(t, "Allow", []string{"s3:ListAllMyBuckets"}, "*"),
	})
	raw, err := io.ReadAll(created.Body)
	require.NoError(t, err)
	require.NoError(t, created.Body.Close())
	require.Equal(t, http.StatusOK, created.StatusCode, "body: %s", raw)
	var policy struct {
		ARN string `xml:"CreatePolicyResult>Policy>Arn"`
	}
	require.NoError(t, xml.Unmarshal(raw, &policy))
	require.NotEmpty(t, policy.ARN)

	iamRequireOK(t, srv, "AttachUserPolicy", map[string]any{
		"UserName":  "jill",
		"PolicyArn": policy.ARN,
	})

	got := iamSimulate(t, srv, "SimulatePrincipalPolicy", map[string]any{
		"PolicySourceArn": "arn:aws:iam::123456789012:user/jill",
		"ActionNames":     []string{"s3:ListAllMyBuckets"},
	})
	require.Len(t, got.results(), 1)
	assert.Equal(t, "allowed", got.results()[0].Decision)
	require.Len(t, got.results()[0].MatchedStatements, 1)
	assert.Equal(t, "listbuckets", got.results()[0].MatchedStatements[0].SourcePolicyID)
}

// TestSimulatePrincipalPolicy_PolicyInputListAddsToTheEntitys covers the combination:
// the principal form takes PolicyInputList too, and those documents are evaluated
// *alongside* the entity's own rather than replacing them.
//
// This is how a consumer asks "what would happen if I also attached this?", which is
// why the member exists on both operations.
func TestSimulatePrincipalPolicy_PolicyInputListAddsToTheEntitys(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	iamRequireOK(t, srv, "CreateUser", map[string]any{"UserName": "jill"})
	iamRequireOK(t, srv, "PutUserPolicy", map[string]any{
		"UserName":       "jill",
		"PolicyName":     "getter",
		"PolicyDocument": iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "*"),
	})

	got := iamSimulate(t, srv, "SimulatePrincipalPolicy", map[string]any{
		"PolicySourceArn": "arn:aws:iam::123456789012:user/jill",
		"ActionNames":     []string{"s3:GetObject", "s3:PutObject"},
		"PolicyInputList": []string{
			iamPolicyJSON(t, "Allow", []string{"s3:PutObject"}, "*"),
		},
	})
	assert.Equal(t, map[string]string{
		"s3:GetObject": "allowed",
		"s3:PutObject": "allowed",
	}, got.decisions())

	// Each grant is still attributed to the document it came from, which is what makes
	// the "if I also attached this" answer readable.
	byAction := got.byAction()
	require.Len(t, byAction["s3:GetObject"].MatchedStatements, 1)
	assert.Equal(t, "getter", byAction["s3:GetObject"].MatchedStatements[0].SourcePolicyID)
	require.Len(t, byAction["s3:PutObject"].MatchedStatements, 1)
	assert.Equal(t, "PolicyInputList.1", byAction["s3:PutObject"].MatchedStatements[0].SourcePolicyID)
}

// TestSimulate_OperationsAreAuthorized covers the authorization arm on both
// operations.
//
// iam_managed.go grants iam:SimulateCustomPolicy and iam:SimulatePrincipalPolicy in
// its bundled policies, so a caller without them must be refused — otherwise the grant
// means nothing.
func TestSimulate_OperationsAreAuthorized(t *testing.T) {
	t.Parallel()

	for _, op := range []string{"SimulatePrincipalPolicy", "SimulateCustomPolicy"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			srv := newTrustPolicyTestServer(t)
			accessKey := trustSetupCallerWithoutAssume(t, srv, "ungranted")

			resp := iamCallAs(t, srv, accessKey, op, map[string]any{
				"PolicySourceArn": "arn:aws:iam::123456789012:user/ungranted",
				"ActionNames":     []string{"s3:GetObject"},
				"PolicyInputList": []string{`{"Version":"2012-10-17","Statement":[]}`},
			})
			require.Equal(t, http.StatusForbidden, resp.StatusCode)
			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, emulator.IAMAccessDeniedCodeForTest, result["__type"])
		})
	}
}

// TestSimulate_MarkerEdges covers the cursor's tolerant arms.
//
// paginateIAMKeys treats an undecodable Marker as the start of the list rather than
// as an error, and the simulator matches it: refusing a cursor a caller
// round-tripped would be worse than restarting a page. A cursor past the end
// likewise returns an empty final page rather than indexing out of range.
func TestSimulate_MarkerEdges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		marker     string
		wantCount  int
		wantAction string
	}{
		{
			name:       "a marker substrate did not mint restarts the list",
			marker:     "not-base64!!",
			wantCount:  1,
			wantAction: "s3:GetObject",
		},
		{
			name:       "base64 of a non-number restarts the list",
			marker:     base64.StdEncoding.EncodeToString([]byte("ninety")),
			wantCount:  1,
			wantAction: "s3:GetObject",
		},
		{
			name:      "a marker past the end is an empty final page",
			marker:    base64.StdEncoding.EncodeToString([]byte("99")),
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)

			got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
				"ActionNames":     []string{"s3:GetObject", "s3:PutObject"},
				"PolicyInputList": []string{iamPolicyJSON(t, "Allow", []string{"s3:*"}, "*")},
				"MaxItems":        1,
				"Marker":          tc.marker,
			})
			require.Len(t, got.results(), tc.wantCount)
			if tc.wantAction != "" {
				assert.Equal(t, tc.wantAction, got.results()[0].ActionName)
			}
			if tc.wantCount == 0 {
				truncated, marker := got.page()
				assert.False(t, truncated)
				assert.Empty(t, marker)
			}
		})
	}
}

// TestSimulate_ContextEntryWithNoValueIsStillSet pins the present-vs-absent
// distinction on a context key, the same one iamMemberList is careful about.
//
// A key named with no ContextKeyValues was supplied by the caller, so it must not be
// reported as missing — the Null operator exists precisely to test for an empty
// value, and reporting it as missing would tell the caller to supply what they
// already did.
func TestSimulate_ContextEntryWithNoValueIsStillSet(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	nullConditioned := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"s3:GetObject","Resource":"*",` +
		`"Condition":{"Null":{"aws:TokenIssueTime":"true"}}}]}`

	got := iamDecodeSimulation(t, iamFormRequest(t, srv, "SimulateCustomPolicy", map[string]string{
		"ActionNames.member.1":                   "s3:GetObject",
		"PolicyInputList.member.1":               nullConditioned,
		"ContextEntries.member.1.ContextKeyName": "aws:TokenIssueTime",
	}))

	require.Len(t, got.results(), 1)
	assert.Equal(t, "allowed", got.results()[0].Decision,
		"the key is set to the empty string, which is what Null:true tests for")
	assert.Empty(t, got.results()[0].MissingContextValues)

	// An entry with no ContextKeyName names nothing, so it contributes nothing rather
	// than a key called "".
	unnamed := iamDecodeSimulation(t, iamFormRequest(t, srv, "SimulateCustomPolicy", map[string]string{
		"ActionNames.member.1":                              "s3:GetObject",
		"PolicyInputList.member.1":                          nullConditioned,
		"ContextEntries.member.1.ContextKeyValues.member.1": "whatever",
		"ContextEntries.member.1.ContextKeyType":            "string",
		"ContextEntries.member.2.ContextKeyName":            "aws:TokenIssueTime",
		"ContextEntries.member.2.ContextKeyValues.member.1": "2026-01-01T00:00:00Z",
		"ContextEntries.member.2.ContextKeyType":            "date",
	}))
	require.Len(t, unnamed.results(), 1)
	assert.Equal(t, "implicitDeny", unnamed.results()[0].Decision,
		"the named key now holds a value, so Null:true is false")
}

// TestSimulate_ResourceOwnerPopulatesResourceAccount covers the second condition key
// the simulation derives, for the same reason as aws:PrincipalArn: the caller told
// substrate the account through ResourceOwner, so reporting the key as missing would
// be wrong.
func TestSimulate_ResourceOwnerPopulatesResourceAccount(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	sameAccount := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Action":"s3:GetObject","Resource":"*","Condition":{"StringEquals":` +
		`{"aws:ResourceAccount":"123456789012"}}}]}`

	got := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames":     []string{"s3:GetObject"},
		"PolicyInputList": []string{sameAccount},
		"ResourceOwner":   "123456789012",
	})
	require.Len(t, got.results(), 1)
	assert.Equal(t, "allowed", got.results()[0].Decision)
	assert.Empty(t, got.results()[0].MissingContextValues)

	// Without it the key is genuinely unknown, so it is reported rather than guessed
	// from the request's own account.
	absent := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
		"ActionNames":     []string{"s3:GetObject"},
		"PolicyInputList": []string{sameAccount},
	})
	require.Len(t, absent.results(), 1)
	assert.Equal(t, "implicitDeny", absent.results()[0].Decision)
	assert.Equal(t, []string{"aws:ResourceAccount"}, absent.results()[0].MissingContextValues)
}

// TestSimulateWire_OverTheQueryProtocol drives both operations the way a client does.
//
// Every list parameter these operations take arrives as prefix.member.N, so before
// #639 nothing reached the handler at all — ActionNames, a *required* member, was nil
// on every real request. And an operation that is implemented but unroutable answers
// InvalidAction with a fully green unit suite, which was #636. This test is the one
// that catches either without leaving the package.
func TestSimulateWire_OverTheQueryProtocol(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	for _, setup := range []struct {
		operation string
		params    map[string]string
	}{
		{"CreateUser", map[string]string{"UserName": "jill"}},
		{"PutUserPolicy", map[string]string{
			"UserName":   "jill",
			"PolicyName": "nodelete",
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Deny",` +
				`"Action":"s3:DeleteObject","Resource":"*"}]}`,
		}},
	} {
		resp := iamFormRequest(t, srv, setup.operation, setup.params)
		require.Equal(t, http.StatusOK, resp.StatusCode, setup.operation)
		require.NoError(t, resp.Body.Close())
	}

	principal := iamDecodeSimulation(t, iamFormRequest(t, srv, "SimulatePrincipalPolicy",
		map[string]string{
			"PolicySourceArn":       "arn:aws:iam::123456789012:user/jill",
			"ActionNames.member.1":  "s3:DeleteObject",
			"ActionNames.member.2":  "s3:GetObject",
			"ResourceArns.member.1": "arn:aws:s3:::my-bucket/key",
		}))
	assert.Equal(t, map[string]string{
		"s3:DeleteObject": "explicitDeny",
		"s3:GetObject":    "implicitDeny",
	}, principal.decisions())

	custom := iamDecodeSimulation(t, iamFormRequest(t, srv, "SimulateCustomPolicy",
		map[string]string{
			"ActionNames.member.1": "s3:GetObject",
			"PolicyInputList.member.1": `{"Version":"2012-10-17","Statement":` +
				`[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`,
		}))
	require.Len(t, custom.results(), 1)
	assert.Equal(t, "allowed", custom.results()[0].Decision)

	// MaxItems is a scalar over the same wire, so it arrives as a *string* — the #642
	// half of the same defect. A page of one out of two proves it was read.
	paged := iamDecodeSimulation(t, iamFormRequest(t, srv, "SimulateCustomPolicy",
		map[string]string{
			"ActionNames.member.1": "s3:GetObject",
			"ActionNames.member.2": "s3:PutObject",
			"PolicyInputList.member.1": `{"Version":"2012-10-17","Statement":` +
				`[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
			"MaxItems": "1",
		}))
	require.Len(t, paged.results(), 1)
	truncated, marker := paged.page()
	assert.True(t, truncated)
	assert.NotEmpty(t, marker)
}
