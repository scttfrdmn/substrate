package emulator_test

import (
	"bytes"
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// policyVarDoc is one statement with the version that admits variables, which is what
// every case below is about except the one that deliberately omits it.
func policyVarDoc(stmt emulator.PolicyStatement) []emulator.PolicyDocument {
	return []emulator.PolicyDocument{{Version: "2012-10-17", Statement: []emulator.PolicyStatement{stmt}}}
}

// TestEvaluate_ResourceVariableResolves is the positive direction of #745: a variable
// with a value is substituted, so a statement scoped to the caller's own prefix allows
// their object and refuses somebody else's.
//
// This is the shape of every "let a user manage their own things" policy, including AWS's
// own IAMUserChangePassword, and before #745 it allowed nothing at all: the literal
// `${aws:username}` was compared against an ARN that never contains one.
func TestEvaluate_ResourceVariableResolves(t *testing.T) {
	docs := policyVarDoc(emulator.PolicyStatement{
		Effect:   emulator.IAMEffectAllow,
		Action:   emulator.StringOrSlice{"s3:GetObject"},
		Resource: emulator.StringOrSlice{"arn:aws:s3:::home/${aws:username}/*"},
	})

	tests := []struct {
		name     string
		resource string
		want     string
	}{
		{"the caller's own prefix", "arn:aws:s3:::home/alice/report.txt", emulator.DecisionAllow},
		{"somebody else's prefix", "arn:aws:s3:::home/bob/report.txt", emulator.DecisionImplicitDeny},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := emulator.Evaluate(docs, emulator.EvaluationRequest{
				Action:   "s3:GetObject",
				Resource: tt.resource,
				Context:  map[string]string{"aws:username": "alice"},
			})
			assert.Equal(t, tt.want, result.Decision)
		})
	}
}

// TestEvaluate_UnresolvedVariableMatchesNoResource pins AWS's rule for an unresolved
// variable in both resource elements, which is one rule stated once for the pair: "If a
// variable that has no value in the authorization context is used as part of the Resource
// or NotResource element of a policy, the resource that includes a policy variable with no
// value will not match any resource."
//
// So the two directions of the criterion fall out of one skip: as a Resource the entry
// grants nothing, and as a NotResource it excludes nothing — which is what makes the
// negated element match every resource. Substrate answered both correctly before #745 by
// leaving the `${…}` in place and comparing it literally against an ARN that never
// contains one; this pins the answer now that the rule is applied deliberately, so it
// cannot regress once a resolved value can itself hold a wildcard.
func TestEvaluate_UnresolvedVariableMatchesNoResource(t *testing.T) {
	t.Run("Resource grants nothing", func(t *testing.T) {
		docs := policyVarDoc(emulator.PolicyStatement{
			Effect:   emulator.IAMEffectAllow,
			Action:   emulator.StringOrSlice{"s3:GetObject"},
			Resource: emulator.StringOrSlice{"arn:aws:s3:::home/${aws:username}/*"},
		})
		result := emulator.Evaluate(docs, emulator.EvaluationRequest{
			Action:   "s3:GetObject",
			Resource: "arn:aws:s3:::home/alice/report.txt",
		})
		assert.Equal(t, emulator.DecisionImplicitDeny, result.Decision)
	})

	t.Run("NotResource excludes nothing", func(t *testing.T) {
		docs := policyVarDoc(emulator.PolicyStatement{
			Effect:      emulator.IAMEffectAllow,
			Action:      emulator.StringOrSlice{"s3:GetObject"},
			NotResource: emulator.StringOrSlice{"arn:aws:s3:::home/${aws:username}/*"},
		})
		result := emulator.Evaluate(docs, emulator.EvaluationRequest{
			Action:   "s3:GetObject",
			Resource: "arn:aws:s3:::home/alice/report.txt",
		})
		assert.Equal(t, emulator.DecisionAllow, result.Decision)
	})
}

// TestEvaluate_ConditionVariableResolves is the divergence #745 exists for, and its
// negated half is the dangerous one: with the variable compared literally, a resolved
// value could not match, so `StringNotEquals` was satisfied by every caller — a false
// deny on a Deny statement and a false *allow* on an Allow.
//
// The issue's own framing has this backwards, describing the unresolved case as the
// divergence. Substituting is what makes the negated form exclude the caller it names.
func TestEvaluate_ConditionVariableResolves(t *testing.T) {
	tests := []struct {
		name     string
		operator string
		owner    string
		want     string
	}{
		{"StringEquals matches the caller's own value", "StringEquals", "alice", emulator.DecisionAllow},
		{"StringEquals refuses another's", "StringEquals", "bob", emulator.DecisionImplicitDeny},
		{"StringNotEquals excludes the caller it names", "StringNotEquals", "alice", emulator.DecisionImplicitDeny},
		{"StringNotEquals admits everybody else", "StringNotEquals", "bob", emulator.DecisionAllow},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docs := policyVarDoc(emulator.PolicyStatement{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"s3:GetObject"},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					tt.operator: {"s3:owner": {"${aws:username}"}},
				},
			})
			result := emulator.Evaluate(docs, emulator.EvaluationRequest{
				Action:   "s3:GetObject",
				Resource: "arn:aws:s3:::mybucket/file.txt",
				Context:  map[string]string{"aws:username": "alice", "s3:owner": tt.owner},
			})
			assert.Equal(t, tt.want, result.Decision)
		})
	}
}

// TestEvaluate_UnresolvedConditionVariableIsNull pins the other half of the criterion,
// which for a condition value is *not* the resource rule: AWS says the value "is
// effectively null. There is no equal or like value", and then that "inverted condition
// operators like StringNotEquals or StringNotLike do match against a null value".
//
// So the positive form matches nothing and the negated form matches everything, which is
// exactly the asymmetry the resource elements do not have.
func TestEvaluate_UnresolvedConditionVariableIsNull(t *testing.T) {
	tests := []struct {
		name     string
		operator string
		want     string
	}{
		{"positive form matches nothing", "StringEquals", emulator.DecisionImplicitDeny},
		{"negated form matches a null value", "StringNotEquals", emulator.DecisionAllow},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docs := policyVarDoc(emulator.PolicyStatement{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"s3:GetObject"},
				Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					tt.operator: {"s3:owner": {"${aws:username}"}},
				},
			})
			result := emulator.Evaluate(docs, emulator.EvaluationRequest{
				Action:   "s3:GetObject",
				Resource: "arn:aws:s3:::mybucket/file.txt",
				// No aws:username, so the variable has no value. s3:owner is present, so
				// the answer is about the *policy's* null value and not about an absent
				// request key, which condEvaluate answers separately.
				Context: map[string]string{"s3:owner": "alice"},
			})
			assert.Equal(t, tt.want, result.Decision)
		})
	}
}

// TestEvaluate_VariablesNeedTheVersionElement pins AWS's gate: "Variables were introduced
// in version 2012-10-17… If you don't include the Version element and set it to an
// appropriate version date, variables like ${aws:username} are treated as literal strings
// in the policy."
//
// A document with no Version therefore compares the text as written, so the statement
// matches only a resource that literally contains `${aws:username}` — and refuses the one
// the variable would have resolved to.
func TestEvaluate_VariablesNeedTheVersionElement(t *testing.T) {
	stmt := emulator.PolicyStatement{
		Effect:   emulator.IAMEffectAllow,
		Action:   emulator.StringOrSlice{"s3:GetObject"},
		Resource: emulator.StringOrSlice{"arn:aws:s3:::home/${aws:username}/*"},
	}
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{"2012-10-17 substitutes", "2012-10-17", emulator.DecisionAllow},
		{"2008-10-17 does not", "2008-10-17", emulator.DecisionImplicitDeny},
		{"no Version element does not", "", emulator.DecisionImplicitDeny},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docs := []emulator.PolicyDocument{{
				Version:   tt.version,
				Statement: []emulator.PolicyStatement{stmt},
			}}
			result := emulator.Evaluate(docs, emulator.EvaluationRequest{
				Action:   "s3:GetObject",
				Resource: "arn:aws:s3:::home/alice/report.txt",
				Context:  map[string]string{"aws:username": "alice"},
			})
			assert.Equal(t, tt.want, result.Decision)
		})
	}
}

// TestEvaluate_VariableEscapesStayLiteral pins the hazard substitution introduces, which
// AWS's escapes exist precisely to avoid: `${*}` "use where you need an * (asterisk)
// character". Substituting it and handing the result to the glob matcher would turn the
// author's deliberate escape into a wildcard, widening the statement — the one direction an
// escape must never take.
//
// A substituted *value* containing a wildcard is literal for the same reason pointed the
// other way: a tag value is data the request carried, not pattern the author wrote, so a
// caller who can set a tag cannot widen the policy that reads it.
func TestEvaluate_VariableEscapesStayLiteral(t *testing.T) {
	t.Run("${*} is an asterisk, not a wildcard", func(t *testing.T) {
		docs := policyVarDoc(emulator.PolicyStatement{
			Effect:   emulator.IAMEffectAllow,
			Action:   emulator.StringOrSlice{"s3:GetObject"},
			Resource: emulator.StringOrSlice{"arn:aws:s3:::home/${*}"},
		})
		allowed := emulator.Evaluate(docs, emulator.EvaluationRequest{
			Action:   "s3:GetObject",
			Resource: "arn:aws:s3:::home/*",
		})
		assert.Equal(t, emulator.DecisionAllow, allowed.Decision, "the literal asterisk must match")

		refused := emulator.Evaluate(docs, emulator.EvaluationRequest{
			Action:   "s3:GetObject",
			Resource: "arn:aws:s3:::home/alice",
		})
		assert.Equal(t, emulator.DecisionImplicitDeny, refused.Decision,
			"a wildcard read of ${*} would have allowed every object under home/")
	})

	t.Run("a wildcard inside a resolved value is text", func(t *testing.T) {
		docs := policyVarDoc(emulator.PolicyStatement{
			Effect:   emulator.IAMEffectAllow,
			Action:   emulator.StringOrSlice{"s3:GetObject"},
			Resource: emulator.StringOrSlice{"arn:aws:s3:::home/${aws:username}"},
		})
		result := emulator.Evaluate(docs, emulator.EvaluationRequest{
			Action:   "s3:GetObject",
			Resource: "arn:aws:s3:::home/alice",
			Context:  map[string]string{"aws:username": "*"},
		})
		assert.Equal(t, emulator.DecisionImplicitDeny, result.Decision)
	})
}

// TestEvaluate_VariableDefaultValue pins AWS's documented default syntax: "To add a
// default value to a variable, surround the default value with single quotes (' '), and
// separate the variable text and the default value with a comma and space (, )".
//
// A variable with a default is never unresolved, which is the whole point of writing one —
// so the statement matches the default's prefix rather than nothing.
func TestEvaluate_VariableDefaultValue(t *testing.T) {
	docs := policyVarDoc(emulator.PolicyStatement{
		Effect:   emulator.IAMEffectAllow,
		Action:   emulator.StringOrSlice{"s3:GetObject"},
		Resource: emulator.StringOrSlice{"arn:aws:s3:::${aws:PrincipalTag/team, 'company-wide'}/*"},
	})

	tests := []struct {
		name     string
		ctx      map[string]string
		resource string
		want     string
	}{
		{
			name:     "the tag's value wins when it has one",
			ctx:      map[string]string{"aws:PrincipalTag/team": "platform"},
			resource: "arn:aws:s3:::platform/file.txt",
			want:     emulator.DecisionAllow,
		},
		{
			name:     "the default applies when it does not",
			ctx:      nil,
			resource: "arn:aws:s3:::company-wide/file.txt",
			want:     emulator.DecisionAllow,
		},
		{
			name:     "and the default is not a wildcard",
			ctx:      nil,
			resource: "arn:aws:s3:::platform/file.txt",
			want:     emulator.DecisionImplicitDeny,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := emulator.Evaluate(docs, emulator.EvaluationRequest{
				Action:   "s3:GetObject",
				Resource: tt.resource,
				Context:  tt.ctx,
			})
			assert.Equal(t, tt.want, result.Decision)
		})
	}
}

// TestEvaluate_VariableNamesAreCaseInsensitive pins AWS's rule for the name inside the
// braces, which is the rule for the condition key it names: "Key names are
// case-insensitive. For example, aws:CurrentTime is equivalent to AWS:currenttime."
//
// It matters because a producer's spelling and a policy author's need not agree, and
// substrate's own producers write `aws:username` while AWS's documentation and examples
// mix `aws:username` and `aws:userName`.
func TestEvaluate_VariableNamesAreCaseInsensitive(t *testing.T) {
	for _, spelling := range []string{"aws:username", "aws:userName", "AWS:USERNAME"} {
		t.Run(spelling, func(t *testing.T) {
			docs := policyVarDoc(emulator.PolicyStatement{
				Effect:   emulator.IAMEffectAllow,
				Action:   emulator.StringOrSlice{"s3:GetObject"},
				Resource: emulator.StringOrSlice{"arn:aws:s3:::home/${" + spelling + "}/*"},
			})
			result := emulator.Evaluate(docs, emulator.EvaluationRequest{
				Action:   "s3:GetObject",
				Resource: "arn:aws:s3:::home/alice/report.txt",
				Context:  map[string]string{"aws:username": "alice"},
			})
			assert.Equal(t, emulator.DecisionAllow, result.Decision)
		})
	}
}

// TestEvaluate_VariablesOnlyInTheElementsAWSSubstitutes pins the two limits AWS states,
// both of which narrow where substitution runs.
//
// Action is not on AWS's list of elements variables resolve in, and neither is a Numeric
// condition: "You can't use a policy variable with other operators, such as Numeric, Date,
// Boolean, Binary, IP Address, or Null operators." So both compare the text as written,
// which for a Numeric value is a comparison that cannot be made.
func TestEvaluate_VariablesOnlyInTheElementsAWSSubstitutes(t *testing.T) {
	t.Run("Action is compared as written", func(t *testing.T) {
		docs := policyVarDoc(emulator.PolicyStatement{
			Effect:   emulator.IAMEffectAllow,
			Action:   emulator.StringOrSlice{"s3:${aws:username}"},
			Resource: emulator.StringOrSlice{"*"},
		})
		result := emulator.Evaluate(docs, emulator.EvaluationRequest{
			Action:   "s3:GetObject",
			Resource: "arn:aws:s3:::mybucket/file.txt",
			Context:  map[string]string{"aws:username": "GetObject"},
		})
		assert.Equal(t, emulator.DecisionImplicitDeny, result.Decision)
	})

	t.Run("a Numeric value is compared as written", func(t *testing.T) {
		docs := policyVarDoc(emulator.PolicyStatement{
			Effect:   emulator.IAMEffectAllow,
			Action:   emulator.StringOrSlice{"s3:ListBucket"},
			Resource: emulator.StringOrSlice{"*"},
			Condition: map[string]map[string]emulator.StringOrSlice{
				"NumericEquals": {"s3:max-keys": {"${aws:username}"}},
			},
		})
		result := emulator.Evaluate(docs, emulator.EvaluationRequest{
			Action:   "s3:ListBucket",
			Resource: "arn:aws:s3:::mybucket",
			Context:  map[string]string{"aws:username": "10", "s3:max-keys": "10"},
		})
		assert.Equal(t, emulator.DecisionImplicitDeny, result.Decision)
	})
}

// TestEvaluate_VariableInsideAnARNCondition pins that substitution and AWS's
// component-by-component ARN comparison compose: "Each of the six colon-delimited
// components of the ARN is checked separately", and a `*` a variable resolved to is still
// text inside whichever component it landed in.
func TestEvaluate_VariableInsideAnARNCondition(t *testing.T) {
	docs := policyVarDoc(emulator.PolicyStatement{
		Effect:   emulator.IAMEffectAllow,
		Action:   emulator.StringOrSlice{"sns:Publish"},
		Resource: emulator.StringOrSlice{"*"},
		Condition: map[string]map[string]emulator.StringOrSlice{
			"ArnLike": {"aws:SourceArn": {"arn:aws:sns:*:*:${aws:username}-*"}},
		},
	})

	tests := []struct {
		name   string
		source string
		want   string
	}{
		{"the caller's own topic", "arn:aws:sns:us-east-1:123456789012:alice-alerts", emulator.DecisionAllow},
		{"another's topic", "arn:aws:sns:us-east-1:123456789012:bob-alerts", emulator.DecisionImplicitDeny},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := emulator.Evaluate(docs, emulator.EvaluationRequest{
				Action:   "sns:Publish",
				Resource: "arn:aws:sns:us-east-1:123456789012:alice-alerts",
				Context:  map[string]string{"aws:username": "alice", "aws:SourceArn": tt.source},
			})
			assert.Equal(t, tt.want, result.Decision)
		})
	}
}

// TestCheckAccess_UsernameVariableScopesToTheCaller joins the two halves of #745 at the
// enforcement door: the producer writes aws:username from [Principal.UserName], and the
// substituter resolves the `${aws:username}` a policy names.
//
// The policy is the shape AWS's own documentation gives for "a bucket prefix per user",
// and before this release it allowed nothing at all — so a consumer testing a
// perfectly ordinary IaC policy saw AccessDenied for a request AWS permits.
func TestCheckAccess_UsernameVariableScopesToTheCaller(t *testing.T) {
	ownPrefixOnly := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{{
			Effect:   "Allow",
			Action:   emulator.StringOrSlice{"s3:*"},
			Resource: emulator.StringOrSlice{"arn:aws:s3:::home/${aws:username}/*"},
		}},
	}
	policyARN := "arn:aws:iam::123456789012:policy/OwnPrefixOnly"

	tests := []struct {
		name      string
		principal *emulator.Principal
		path      string
		allowed   bool
	}{
		{
			name: "the caller's own prefix",
			principal: &emulator.Principal{
				ARN: "arn:aws:iam::123456789012:user/alice", Type: "IAMUser", UserName: "alice",
			},
			path:    "/home/alice/report.txt",
			allowed: true,
		},
		{
			name: "another user's prefix",
			principal: &emulator.Principal{
				ARN: "arn:aws:iam::123456789012:user/alice", Type: "IAMUser", UserName: "alice",
			},
			path:    "/home/bob/report.txt",
			allowed: false,
		},
		{
			// A principal with no recorded user name publishes no aws:username, so the
			// variable does not resolve and the statement matches nothing — which is
			// AWS's rule for an unresolved variable in a Resource, and the reason this
			// direction is safe to leave to the absent key.
			name: "a principal with no user name",
			principal: &emulator.Principal{
				ARN: "arn:aws:iam::123456789012:user/alice", Type: "IAMUser",
			},
			path:    "/home/alice/report.txt",
			allowed: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := newAuthTestState(t, "alice", policyARN, ownPrefixOnly)
			auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))

			reqCtx := &emulator.RequestContext{
				RequestID: "req-1",
				AccountID: authzTestAccount,
				Region:    "us-east-1",
				Principal: tt.principal,
				Metadata:  make(map[string]interface{}),
			}
			err := auth.CheckAccess(reqCtx, &emulator.AWSRequest{
				Service: "s3", Operation: "GetObject", Path: tt.path,
			})
			if tt.allowed {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
		})
	}
}

// TestServer_ResolvePrincipal_CarriesTheUserName is the producer half end to end: the name
// has to survive the credential lookup, because that is the only place it exists.
//
// The three cases are AWS's own table. A long-term key names its user; an assumed-role
// session names none, where `aws:username` reads "(not present)"; and a GetSessionToken
// session names the *same* user, because its principal is unchanged — which is why
// presence cannot be keyed off Principal.Type, which reads "AssumedRole" for both STS
// forms (#745).
func TestServer_ResolvePrincipal_CarriesTheUserName(t *testing.T) {
	t.Run("a long-term key names its user", func(t *testing.T) {
		srv, capture := newPrincipalTestServer(t)
		keyID := principalCreateAccessKey(t, srv, "alice")

		resp := principalDynamoCall(t, srv, keyID)
		require.NoError(t, resp.Body.Close())
		require.NotNil(t, capture.principal)
		assert.Equal(t, "alice", capture.principal.UserName)
	})

	t.Run("an assumed-role session names none", func(t *testing.T) {
		srv, capture := newPrincipalTestServer(t)
		require.Equal(t, http.StatusOK,
			principalIAMCall(t, srv, "CreateRole", map[string]any{"RoleName": "worker"}).StatusCode)
		keyID := principalCreateAccessKey(t, srv, "alice")

		sessionKey := principalSTSCall(t, srv, keyID,
			"Action=AssumeRole&RoleArn=arn:aws:iam::123456789012:role/worker&RoleSessionName=sess1")

		resp := principalDynamoCall(t, srv, sessionKey)
		require.NoError(t, resp.Body.Close())
		require.NotNil(t, capture.principal)
		assert.Empty(t, capture.principal.UserName,
			"the session name is not a user name, and AWS publishes no aws:username here")
	})

	t.Run("a GetSessionToken session names the same user", func(t *testing.T) {
		srv, capture := newPrincipalTestServer(t)
		keyID := principalCreateAccessKey(t, srv, "alice")

		sessionKey := principalSTSCall(t, srv, keyID, "Action=GetSessionToken")

		resp := principalDynamoCall(t, srv, sessionKey)
		require.NoError(t, resp.Body.Close())
		require.NotNil(t, capture.principal)
		assert.Equal(t, "alice", capture.principal.UserName)
	})
}

// principalSTSCall issues a query-protocol STS call signed with accessKeyID and returns
// the access key ID of the session it minted.
//
// The Credentials element is found by walking the document rather than by an XML path,
// because AssumeRole and GetSessionToken wrap it in differently-named results and one
// walk serves both.
func principalSTSCall(t *testing.T, srv *emulator.Server, accessKeyID, query string) string {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/?"+query, nil)
	r.Host = "sts.amazonaws.com"
	r.Header.Set("Authorization", principalAuthHeader(accessKeyID, "sts"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var minted struct {
		AccessKeyID string `xml:"AccessKeyId"`
	}
	decoder := xml.NewDecoder(bytes.NewReader(w.Body.Bytes()))
	for {
		tok, err := decoder.Token()
		require.NoError(t, err, "no Credentials in %s", w.Body.String())
		start, ok := tok.(xml.StartElement)
		if ok && start.Name.Local == "Credentials" {
			require.NoError(t, decoder.DecodeElement(&minted, &start))
			break
		}
	}
	require.NotEmpty(t, minted.AccessKeyID, w.Body.String())
	return minted.AccessKeyID
}

// TestSimulateCustomPolicy_ResolvesUsernameFromCallerArn is the reachable-today half of
// #745: SimulateCustomPolicy runs the same evaluator, so a caller naming ${aws:username}
// in a policy got implicitDeny here and allowed on AWS with no producer involved at all.
//
// The simulator is also the one place substrate derives a user name from an ARN, because
// CallerArn is the caller's own assertion of who to simulate as — see [simulationUserName].
func TestSimulateCustomPolicy_ResolvesUsernameFromCallerArn(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	policy := iamPolicyJSON(t, "Allow", []string{"s3:GetObject"}, "arn:aws:s3:::home/${aws:username}/*")

	tests := []struct {
		name      string
		callerArn string
		resource  string
		want      string
	}{
		{
			name:      "a user ARN resolves the variable",
			callerArn: "arn:aws:iam::123456789012:user/alice",
			resource:  "arn:aws:s3:::home/alice/report.txt",
			want:      "allowed",
		},
		{
			name:      "a path does not become part of the name",
			callerArn: "arn:aws:iam::123456789012:user/division/engineering/alice",
			resource:  "arn:aws:s3:::home/alice/report.txt",
			want:      "allowed",
		},
		{
			name:      "another user's object is refused",
			callerArn: "arn:aws:iam::123456789012:user/alice",
			resource:  "arn:aws:s3:::home/bob/report.txt",
			want:      "implicitDeny",
		},
		{
			name: "a role session has no user name, so the variable does not resolve",
			// AWS's own table reads "(not present)" for aws:username on an assumed
			// role, so the statement matches nothing rather than matching the session
			// name.
			callerArn: "arn:aws:sts::123456789012:assumed-role/deploy/alice",
			resource:  "arn:aws:s3:::home/alice/report.txt",
			want:      "implicitDeny",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := iamSimulate(t, srv, "SimulateCustomPolicy", map[string]any{
				"PolicyInputList": []string{policy},
				"ActionNames":     []string{"s3:GetObject"},
				"ResourceArns":    []string{tt.resource},
				"CallerArn":       tt.callerArn,
			})
			require.Equal(t, tt.want, resp.byAction()["s3:GetObject"].Decision)
		})
	}
}
