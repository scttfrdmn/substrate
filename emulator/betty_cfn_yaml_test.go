package emulator_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestCFN_YAMLShortForm_MatchesLongForm is #516's gate, and it asserts the
// invariant rather than any one expansion: a template written with the YAML short
// forms must deploy to exactly what the Fn::-prefixed long forms deploy to.
//
// Before the fix, go.yaml.in/yaml/v3 dropped the tag and kept the scalar, so
// `!Sub 'x-${P}'` reached the resolver as the literal "x-${P}" and no downstream
// code could tell it from a literal the template really did intend.
func TestCFN_YAMLShortForm_MatchesLongForm(t *testing.T) {
	cases := []struct {
		name  string
		short string
		long  string
		want  string
	}{
		{
			name:  "Ref",
			short: `!Ref P`,
			long:  `{Ref: P}`,
			want:  "live",
		},
		{
			name:  "Sub",
			short: `!Sub 'b-${P}'`,
			long:  `{Fn::Sub: 'b-${P}'}`,
			want:  "b-live",
		},
		{
			name:  "SubWithVarMap",
			short: `!Sub ['b-${Who}', {Who: !Ref P}]`,
			long:  `{Fn::Sub: ['b-${Who}', {Who: {Ref: P}}]}`,
			want:  "b-live",
		},
		{
			name:  "Join",
			short: `!Join ['-', ['b', !Ref P]]`,
			long:  `{Fn::Join: ['-', ['b', {Ref: P}]]}`,
			want:  "b-live",
		},
		{
			name:  "Select",
			short: `!Select ['1', ['zero', !Ref P]]`,
			long:  `{Fn::Select: ['1', ['zero', {Ref: P}]]}`,
			want:  "live",
		},
		{
			name:  "Split",
			short: `!Split [',', 'live,dead']`,
			long:  `{Fn::Split: [',', 'live,dead']}`,
			want:  "live",
		},
		{
			// A node carries at most one tag, so `!Base64 !Ref P` is not legal
			// YAML — which is why AWS's own examples write the outer function in
			// long form (`Fn::Base64: !Sub |…`). That nesting is covered by
			// TestCFN_YAMLShortForm_TagUnderLongFormKey.
			name:  "Base64",
			short: `!Base64 live`,
			long:  `{Fn::Base64: live}`,
			want:  "bGl2ZQ==", // base64("live")
		},
		{
			name:  "IfTrueBranch",
			short: `!If [IsLive, !Ref P, 'dead']`,
			long:  `{Fn::If: [IsLive, {Ref: P}, 'dead']}`,
			want:  "live",
		},
		{
			name:  "Pseudoparameter",
			short: `!Sub 'b-${AWS::Region}'`,
			long:  `{Fn::Sub: 'b-${AWS::Region}'}`,
			want:  "b-us-east-1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shortID := resolveExpr(t, tc.short)
			longID := resolveExpr(t, tc.long)

			assert.Equal(t, tc.want, shortID, "short form resolved wrongly")
			assert.Equal(t, shortID, longID,
				"the short and long forms of the same intrinsic must be indistinguishable")
		})
	}
}

// TestCFN_YAMLShortForm_ConditionsMatchLongForm covers the condition-only
// intrinsics, which resolve through evalConditionExpr rather than resolveValue
// and so are a separate code path.
func TestCFN_YAMLShortForm_ConditionsMatchLongForm(t *testing.T) {
	cases := []struct {
		name  string
		short string
		long  string
		want  string
	}{
		{
			name:  "Equals",
			short: `!Equals [!Ref P, 'live']`,
			long:  `{Fn::Equals: [{Ref: P}, 'live']}`,
			want:  "chosen",
		},
		{
			// Verbatim from the consumer's ecs_worker.yml:109 — three tags deep,
			// which is what forces the expansion to run children before parents.
			name:  "NotEqualsRef",
			short: `!Not [!Equals [!Ref P, '']]`,
			long:  `{Fn::Not: [{Fn::Equals: [{Ref: P}, '']}]}`,
			want:  "chosen",
		},
		{
			name:  "And",
			short: `!And [!Equals [!Ref P, 'live'], !Not [!Equals [!Ref P, '']]]`,
			long:  `{Fn::And: [{Fn::Equals: [{Ref: P}, 'live']}, {Fn::Not: [{Fn::Equals: [{Ref: P}, '']}]}]}`,
			want:  "chosen",
		},
		{
			name:  "Or",
			short: `!Or [!Equals [!Ref P, 'nope'], !Equals [!Ref P, 'live']]`,
			long:  `{Fn::Or: [{Fn::Equals: [{Ref: P}, 'nope']}, {Fn::Equals: [{Ref: P}, 'live']}]}`,
			want:  "chosen",
		},
		{
			name:  "FalseCondition",
			short: `!Equals [!Ref P, 'nope']`,
			long:  `{Fn::Equals: [{Ref: P}, 'nope']}`,
			want:  "fallback",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shortID := resolveCondition(t, tc.short)
			longID := resolveCondition(t, tc.long)

			assert.Equal(t, tc.want, shortID, "short form resolved wrongly")
			assert.Equal(t, shortID, longID,
				"the short and long forms of the same condition must be indistinguishable")
		})
	}
}

// TestCFN_YAMLShortForm_ConditionTagReferencesAnotherCondition covers !Condition,
// which is the one short form whose long key carries no "Fn::" prefix.
func TestCFN_YAMLShortForm_ConditionTagReferencesAnotherCondition(t *testing.T) {
	for name, inner := range map[string]string{
		"short": `!Condition IsLive`,
		"long":  `{Condition: IsLive}`,
	} {
		t.Run(name, func(t *testing.T) {
			d := newTestDeployer(t)
			tmpl := `
Parameters:
  P: {Type: String, Default: live}
Conditions:
  IsLive: !Equals [!Ref P, 'live']
  AlsoLive: ` + inner + `
Resources:
  B:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !If [AlsoLive, 'chosen', 'fallback']
`
			result, err := d.Deploy(context.Background(), tmpl, "cond-"+name, nil)
			require.NoError(t, err)
			require.Len(t, result.Resources, 1)
			assert.Equal(t, "chosen", result.Resources[0].PhysicalID)
		})
	}
}

// TestCFN_YAMLShortForm_GetAttSplitsOnFirstPeriod pins !GetAtt as the one
// irregular expansion: it takes a dotted scalar where Fn::GetAtt takes a
// two-element list, and the split is on the *first* period only, because an
// attribute name may itself contain periods. The AWS reference's own example is
// `!GetAtt myELB.SourceSecurityGroup.OwnerAlias` →
// ["myELB", "SourceSecurityGroup.OwnerAlias"].
func TestCFN_YAMLShortForm_GetAttSplitsOnFirstPeriod(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{name: "DottedScalar", expr: `!GetAtt Role.Arn`},
		{name: "SequenceFormPassesThrough", expr: `!GetAtt [Role, Arn]`},
		{name: "LongForm", expr: `{Fn::GetAtt: [Role, Arn]}`},
	}

	var want string
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeployer(t)
			// The assertion runs through Outputs, which resolveValue handles
			// directly: the resolved ARN is not a legal bucket name, and asserting
			// through one would need Fn::Select over Fn::Split, which resolves to
			// its first element only (see docs/services.md).
			tmpl := `
Resources:
  Role:
    Type: AWS::IAM::Role
    Properties:
      RoleName: getatt-role
      AssumeRolePolicyDocument: {Version: '2012-10-17', Statement: []}
Outputs:
  RoleArn:
    Value: ` + tc.expr + `
`
			result, err := d.Deploy(context.Background(), tmpl, "getatt-"+tc.name, nil)
			require.NoError(t, err)

			arn := result.Outputs["RoleArn"]
			assert.Equal(t, "arn:aws:iam::123456789012:role/getatt-role", arn,
				"GetAtt Arn should resolve to the role's ARN")
			if i == 0 {
				want = arn
			} else {
				assert.Equal(t, want, arn, "every !GetAtt spelling must agree")
			}
		})
	}
}

// TestCFN_YAMLShortForm_TagUnderLongFormKey covers a short form nested under a
// long-form key. YAML allows one tag per node, so an outer short form cannot take
// a tagged argument directly — `!Base64 !Sub 'x'` is a parse error — and AWS's own
// examples spell the outer function long-form for exactly that reason. The
// expander has to descend into a long-form mapping's values, not only into
// sequence entries.
func TestCFN_YAMLShortForm_TagUnderLongFormKey(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `
Parameters:
  P: {Type: String, Default: live}
Resources:
  B:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: probe-bucket
Outputs:
  Encoded:
    Value:
      Fn::Base64: !Sub 'y-${P}'
`
	result, err := d.Deploy(context.Background(), tmpl, "tag-under-long", nil)
	require.NoError(t, err)
	// base64("y-live")
	assert.Equal(t, "eS1saXZl", result.Outputs["Encoded"])
}

// TestCFN_YAMLShortForm_GetAttWithNestedAttributeName asserts the split keeps a
// dotted attribute name whole. GetAtt on an unknown attribute resolves to the
// empty string, so this asserts through the expansion rather than the resolver:
// were the split on the last period, the logical ID would be "Role.Outputs"
// and the resource lookup would miss for a different reason.
func TestCFN_YAMLShortForm_GetAttWithNestedAttributeName(t *testing.T) {
	tmpl := `
Resources:
  Role:
    Type: AWS::IAM::Role
    Properties:
      RoleName: nested-role
      AssumeRolePolicyDocument: {Version: '2012-10-17', Statement: []}
  B:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Join ['-', ['b', !GetAtt Role.Outputs.Nested]]
`
	shortID := deployOneBucket(t, tmpl, "getatt-nested-short")

	tmpl = `
Resources:
  Role:
    Type: AWS::IAM::Role
    Properties:
      RoleName: nested-role
      AssumeRolePolicyDocument: {Version: '2012-10-17', Statement: []}
  B:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: {Fn::Join: ['-', ['b', {Fn::GetAtt: [Role, 'Outputs.Nested']}]]}
`
	longID := deployOneBucket(t, tmpl, "getatt-nested-long")

	assert.Equal(t, longID, shortID,
		"a dotted attribute name must survive the split intact")
}

// TestCFN_FnSub_LiteralEscape covers Fn::Sub's ${!Literal} escape, which emits
// "${Literal}" verbatim with no substitution.
//
// It was unreachable before the expander: the consumer's ec2_worker.yml writes
// `!Sub 'parsl-worker-${WorkflowId}-${BlockId}-${!Count.Index}'`, and with the
// whole !Sub discarded the escape never reached substituteTemplate. Expanding the
// tag is what makes it reachable, which is why it ships in the same change.
func TestCFN_FnSub_LiteralEscape(t *testing.T) {
	cases := []struct {
		name string
		expr string
		want string
	}{
		{
			name: "EscapedRefIsLiteral",
			expr: `!Sub 'b-${P}-${!Count.Index}'`,
			want: "b-live-${Count.Index}",
		},
		{
			name: "EscapeOnly",
			expr: `!Sub '${!Literal}'`,
			want: "${Literal}",
		},
		{
			name: "EscapeShieldsARealParameter",
			expr: `!Sub '${!P}'`,
			want: "${P}",
		},
		{
			name: "LongFormEscapesToo",
			expr: `{Fn::Sub: 'b-${P}-${!Count.Index}'}`,
			want: "b-live-${Count.Index}",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The bucket name is not a legal one, so assert through an SQS queue
			// tag: S3 lowercases and the plugin would refuse the braces.
			d := newTestDeployer(t)
			tmpl := `
Parameters:
  P: {Type: String, Default: live}
Resources:
  Q:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: escape-queue
      Tags:
        - Key: Name
          Value: ` + tc.expr + `
  B:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Join ['|', ['x', ` + tc.expr + `]]
`
			result, err := d.Deploy(context.Background(), tmpl, "escape-"+tc.name, nil)
			require.NoError(t, err)

			var bucket string
			for _, r := range result.Resources {
				if r.LogicalID == "B" {
					bucket = r.PhysicalID
				}
			}
			// deployS3Bucket lowercases the name it sends, so compare against the
			// lowercased expectation rather than weakening the assertion.
			assert.Equal(t, strings.ToLower("x|"+tc.want), bucket)
		})
	}
}

// TestCFN_YAMLShortForm_UnrecognizedTagIsWarnedNotDropped pins the decision for a
// tag the expander does not know: leave the node's value in place and warn.
//
// Dropping it silently is exactly the defect #516 reports, and refusing the
// template would reject templates real CloudFormation accepts, since a macro or a
// transform may introduce tags substrate has never heard of.
func TestCFN_YAMLShortForm_UnrecognizedTagIsWarnedNotDropped(t *testing.T) {
	var buf bytes.Buffer
	d := newTestDeployerWithLogger(t, newTestLogger(&buf, slog.LevelWarn))

	tmpl := `
Resources:
  B:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Mystery keepme
`
	result, err := d.Deploy(context.Background(), tmpl, "mystery", nil)
	require.NoError(t, err, "an unrecognized tag must not refuse the template")
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "keepme", result.Resources[0].PhysicalID,
		"the tagged node's value must survive")

	assert.Contains(t, buf.String(), "unrecognized YAML tag")
	assert.Contains(t, buf.String(), "!Mystery",
		"the warning must name the tag, or an operator cannot act on it")
}

// TestCFN_YAMLShortForm_GetAttWithoutAPeriod pins that a !GetAtt scalar carrying no
// period is passed through rather than split into a one-element list.
//
// Such a template is malformed either way — Fn::GetAtt needs both a logical ID and
// an attribute — so the expander leaves it for the resolver to reject rather than
// inventing an empty attribute name, which would turn a template error into a
// silently empty property.
func TestCFN_YAMLShortForm_GetAttWithoutAPeriod(t *testing.T) {
	assert.Empty(t, resolveExpr(t, "!GetAtt NoPeriodHere"),
		"a !GetAtt with no attribute must not resolve to anything")
}

// TestCFN_MalformedYAMLIsRefused pins that a template which is neither valid JSON
// nor valid YAML is refused with an error rather than deployed as an empty stack.
func TestCFN_MalformedYAMLIsRefused(t *testing.T) {
	cases := []struct {
		name, tmpl string
	}{
		{
			// Two tags on one node. A YAML node carries at most one, so this is a
			// parse error — which is why AWS's examples spell the outer function
			// long-form as `Fn::Base64: !Ref P`.
			name: "TwoTagsOnOneNode",
			tmpl: "Resources:\n  B:\n    Type: AWS::S3::Bucket\n    Properties:\n" +
				"      BucketName: !Base64 !Ref P\n",
		},
		{
			// Valid YAML, but a scalar where the decoder needs a mapping.
			name: "ResourcesIsAScalar",
			tmpl: "Resources: not-a-mapping\n",
		},
		{
			name: "UnclosedFlowSequence",
			tmpl: "Resources:\n  B:\n    Type: AWS::S3::Bucket\n    Properties:\n" +
				"      Tags: [oops\n",
		},
		{
			name: "Empty",
			tmpl: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeployer(t)
			_, err := d.Deploy(context.Background(), tc.tmpl, "malformed-"+tc.name, nil)
			require.Error(t, err, "a template substrate cannot parse must be refused")
			assert.Contains(t, err.Error(), "parse template",
				"the error must say the template could not be parsed")
		})
	}
}

// TestCFN_DispatchErrorPrecedence pins cfnDispatchError's precedence and the
// degraded shapes of a refusal reason.
//
// The two body-derived arms are exercised through deployed stacks in
// betty_cfn_failure_test.go, which is where they belong. The remaining arms are
// asserted here against the function, because they cannot be reached from any
// template: the only two response-style plugins a stack dispatches to are S3 and
// IAM, and s3ErrorResponseWith and iamErrorResponse both always write a <Code> and
// a <Message>. Unreachable or not, an empty reason for a 4xx is exactly the
// CREATE_COMPLETE-for-a-refused-resource defect this release fixes, so the
// fallbacks have to hold.
func TestCFN_DispatchErrorPrecedence(t *testing.T) {
	cases := []struct {
		name     string
		status   int // 0 means no response at all
		body     string
		routeErr error
		want     string
	}{
		{
			// routeErr wins even against a 4xx, because an *AWSError carries the
			// plugin's own code and message where a status is a summary of it.
			name:     "RouteErrorTakesPrecedenceOverTheStatus",
			status:   http.StatusBadRequest,
			body:     `<Error><Code>FromBody</Code><Message>from the body</Message></Error>`,
			routeErr: errors.New("from the error return"),
			want:     "from the error return",
		},
		{
			name:     "RouteErrorWithNoResponseAtAll",
			status:   0,
			routeErr: errors.New("routing failed"),
			want:     "routing failed",
		},
		{
			name:   "CodeAndMessage",
			status: http.StatusBadRequest,
			body:   `<Error><Code>InvalidBucketName</Code><Message>The specified bucket is not valid.</Message></Error>`,
			want:   "InvalidBucketName: The specified bucket is not valid.",
		},
		{
			// A code with no message: the status stands in for the missing prose
			// rather than being dropped, so the reason still says what happened.
			name:   "CodeWithoutAMessage",
			status: http.StatusConflict,
			body:   `<Error><Code>BucketAlreadyExists</Code></Error>`,
			want:   "BucketAlreadyExists (HTTP 409)",
		},
		{
			// Neither element. The status alone is a poor reason, but it is a
			// reason, and a resource that reports it is CREATE_FAILED rather than
			// CREATE_COMPLETE.
			name:   "NeitherCodeNorMessage",
			status: http.StatusForbidden,
			body:   `<html>not an AWS error document</html>`,
			want:   "request failed with HTTP 403",
		},
		{
			name:   "NoBodyAtAll",
			status: http.StatusNotFound,
			want:   "request failed with HTTP 404",
		},
		{
			// A message with no code falls to the status too: without a code there
			// is nothing to name, and prose alone reads as though substrate
			// invented it.
			name:   "MessageWithoutACode",
			status: http.StatusBadRequest,
			body:   `<Error><Message>something went wrong</Message></Error>`,
			want:   "request failed with HTTP 400",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := emulator.CFNDispatchErrorForTest(tc.status, tc.body, tc.routeErr)
			require.Error(t, err, "a refusal must always produce a reason")
			assert.Equal(t, tc.want, err.Error())
		})
	}
}

// TestCFN_DispatchErrorAcceptsSuccess is the negative control for the status
// check: every status below 400 has to stay a success, or the check would report
// CREATE_FAILED for every stack substrate deploys. 399 and 400 are both present
// because an off-by-one at the threshold is the mutation this pins.
func TestCFN_DispatchErrorAcceptsSuccess(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusCreated, http.StatusNoContent,
		http.StatusMovedPermanently, http.StatusNotModified, 399} {
		assert.NoError(t, emulator.CFNDispatchErrorForTest(status, "", nil),
			"HTTP %d must not be reported as a resource failure", status)
	}
	assert.NoError(t, emulator.CFNDispatchErrorForTest(0, "", nil),
		"no response and no error is a success: several deploy paths discard the response")
	assert.Error(t, emulator.CFNDispatchErrorForTest(http.StatusBadRequest, "", nil),
		"400 is the first status that is a failure")
}

// TestCFN_YAMLShortForm_ConsumerTemplate deploys the whole ecs_worker.yml from
// parsl-aws-provider, which is #516's end-to-end case. The reporter's stack
// produced a task-definition family containing the unevaluated condition array
// verbatim:
//
//	["HasTaskFamily","TaskFamily","parsl-task-${WorkflowId}-${JobId}"]
//
// The template is checked in as testdata so it cannot drift out from under the
// assertion.
func TestCFN_YAMLShortForm_ConsumerTemplate(t *testing.T) {
	body, err := os.ReadFile("testdata/cfn/ecs_worker.yml")
	require.NoError(t, err)

	d := newFullTestDeployer(t)
	result, err := d.Deploy(context.Background(), string(body), "ecs-worker", map[string]string{
		"WorkflowId":     "wf1",
		"JobId":          "job1",
		"ContainerImage": "public.ecr.aws/parsl/worker:latest",
	})
	require.NoError(t, err)

	byLogicalID := map[string]emulator.DeployedResource{}
	for _, r := range result.Resources {
		byLogicalID[r.LogicalID] = r
	}

	// The physical ID is the task-definition ARN, whose last segment is
	// "<family>:<revision>" — which is the string #516 quotes as carrying the raw
	// condition array.
	td, ok := byLogicalID["TaskDefinition"]
	require.True(t, ok, "the task definition should have been deployed")
	assert.Equal(t, "arn:aws:ecs:us-east-1:123456789012:task-definition/parsl-task-wf1-job1:1",
		td.PhysicalID,
		"Family: !If [HasTaskFamily, !Ref TaskFamily, !Sub '…'] must pick the !Sub branch")
	assert.NotContains(t, td.PhysicalID, "HasTaskFamily",
		"the family must not be the raw, unevaluated condition array")
	assert.NotContains(t, td.PhysicalID, "${",
		"the family must not carry an unsubstituted placeholder")

	cluster, ok := byLogicalID["ECSCluster"]
	require.True(t, ok)
	assert.Equal(t, "parsl-cluster-wf1", cluster.PhysicalID,
		"ClusterName: !If [HasClusterName, !Ref ClusterName, !Sub '…']")

	lg, ok := byLogicalID["LogGroup"]
	require.True(t, ok)
	assert.Equal(t, "/aws/ecs/parsl-wf1-job1", lg.PhysicalID)

	// Every resource in the template deployed without an error.
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s failed: %s", r.LogicalID, r.Error)
	}
}

// TestCFN_EmptyStringParameterDefaultIsDeclared pins that `Default: ”` declares a
// parameter whose value is the empty string, not a parameter with no default.
//
// This is the idiom for an optional parameter, and every
// `!Not [!Equals [!Ref X, ”]]` condition exists to test for it — 21 times across
// the five templates in parsl-aws-provider. buildCFNContext recorded a default
// only when it was non-empty, so such a parameter was left undeclared, Ref fell
// through to echoing the parameter's own name back, and the condition **inverted**:
// a parameter the caller did not set read as set.
func TestCFN_EmptyStringParameterDefaultIsDeclared(t *testing.T) {
	cases := []struct {
		name string
		tmpl string
	}{
		{
			name: "ShortForm",
			tmpl: `
Parameters:
  Optional: {Type: String, Default: ''}
Conditions:
  HasOptional: !Not [!Equals [!Ref Optional, '']]
Resources:
  B:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: probe-bucket
Outputs:
  V:
    Value: !If [HasOptional, !Ref Optional, 'unset']
`,
		},
		{
			name: "LongForm",
			tmpl: `{
				"Parameters": {"Optional": {"Type": "String", "Default": ""}},
				"Conditions": {
					"HasOptional": {"Fn::Not": [{"Fn::Equals": [{"Ref": "Optional"}, ""]}]}
				},
				"Resources": {"B": {"Type": "AWS::S3::Bucket",
				                    "Properties": {"BucketName": "probe-bucket"}}},
				"Outputs": {
					"V": {"Value": {"Fn::If": ["HasOptional", {"Ref": "Optional"}, "unset"]}}
				}
			}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Unset: the condition must be false, and Ref must not echo the name.
			assert.Equal(t, "unset", deployOneOutput(t, tc.tmpl, "emptydefault-"+tc.name),
				"an unset optional parameter must read as unset")

			// Set: the same template with the parameter supplied takes the other
			// branch, which proves the condition is being evaluated rather than
			// merely defaulting to false.
			d := newTestDeployer(t)
			result, err := d.Deploy(context.Background(), tc.tmpl, "setdefault-"+tc.name,
				map[string]string{"Optional": "supplied"})
			require.NoError(t, err)
			assert.Equal(t, "supplied", result.Outputs["V"])
		})
	}
}

// TestCFN_ConditionReferencingConditionIsOrderIndependent pins that a condition
// referencing another by name resolves the same way however the template declares
// them.
//
// evaluateConditions walked tmpl.Conditions in Go map order, so a condition
// evaluated before the one it references read the referent's zero value. The same
// template then deployed differently from one run to the next — nondeterminism,
// which is the one outcome an emulator built on deterministic replay must never
// produce. Two names per case, ordered both ways, so neither declaration order nor
// alphabetical order can accidentally satisfy the test.
func TestCFN_ConditionReferencingConditionIsOrderIndependent(t *testing.T) {
	// A three-deep chain: Third → Second → First. Whichever order the map is
	// walked in, every condition must see its referent's real value.
	cases := []struct {
		name       string
		conditions string
	}{
		{
			name: "ReferentDeclaredFirst",
			conditions: `
  AFirst: !Equals [!Ref P, 'live']
  BSecond: !Condition AFirst
  CThird: !Condition BSecond`,
		},
		{
			name: "ReferentDeclaredLast",
			conditions: `
  CThird: !Condition BSecond
  BSecond: !Condition AFirst
  AFirst: !Equals [!Ref P, 'live']`,
		},
		{
			name: "ReferentSortsAfterItsUser",
			conditions: `
  ZReferent: !Equals [!Ref P, 'live']
  AUser: !Condition ZReferent
  CThird: !Condition AUser`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			third := "CThird"
			tmpl := `
Parameters:
  P: {Type: String, Default: live}
Conditions:` + tc.conditions + `
Resources:
  B:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: probe-bucket
Outputs:
  V:
    Value: !If [` + third + `, 'chosen', 'fallback']
`
			// Deployed repeatedly, because the defect was a map-order race: one
			// run could easily pass by luck.
			for i := 0; i < 8; i++ {
				assert.Equal(t, "chosen",
					deployOneOutput(t, tmpl, "condorder-"+tc.name+"-"+strconv.Itoa(i)),
					"a condition referencing another must not depend on map order")
			}
		})
	}
}

// TestCFN_ConditionCycleTerminates pins that a circular condition reference
// degrades to false rather than recursing forever. Real CloudFormation rejects the
// template at validation time; substrate has no validation pass, so the guarantee
// that matters here is that the deployment terminates and does so identically every
// time.
//
// Negated is the case that makes the *sorted* walk in evaluateConditions
// load-bearing rather than merely tidy. Where a plain cycle resolves both members to
// false whichever is entered first, a cycle through Fn::Not resolves them to
// opposite values, so the answer depends on which member the walk enters — Go's map
// order, absent the sort.
//
// Both members are asserted, because the entry point is only visible in the one the
// walk reaches second: an unsorted walk still resolves A to true either way, and
// leaves B disagreeing with itself across runs.
func TestCFN_ConditionCycleTerminates(t *testing.T) {
	cases := []struct {
		name       string
		conditions string
		// wantA and wantB are the values of A and B — the outcome that must hold
		// on every run, not merely on most of them.
		wantA, wantB string
	}{
		{
			name: "Plain",
			conditions: `
  A: !Condition B
  B: !Condition A`,
			wantA: "false", wantB: "false",
		},
		{
			// The sorted walk always enters at A, which asks for B; B asks back
			// for A, meets the cycle guard and takes false, so B memoizes false
			// and A is !false = true. Entering at B instead inverts only B: B
			// would ask for A, A's inner B would hit the guard, A would memoize
			// true and B would read that true. So A is true either way and B is
			// the member that betrays the entry point.
			name: "Negated",
			conditions: `
  A: !Not [!Condition B]
  B: !Condition A`,
			wantA: "true", wantB: "false",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmpl := `
Conditions:` + tc.conditions + `
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: probe-bucket
Outputs:
  VA:
    Value: !If [A, 'true', 'false']
  VB:
    Value: !If [B, 'true', 'false']
`
			for i := 0; i < 8; i++ {
				d := newTestDeployer(t)
				result, err := d.Deploy(context.Background(), tmpl,
					"condcycle-"+tc.name+"-"+strconv.Itoa(i), nil)
				require.NoError(t, err)
				assert.Equal(t, tc.wantA, result.Outputs["VA"],
					"a condition cycle must terminate, and resolve identically every run")
				assert.Equal(t, tc.wantB, result.Outputs["VB"],
					"the member the walk reaches second is where map order shows")
			}
		})
	}
}

// TestCFN_ShortFormScalarStaysAString pins that a short form's value is decoded as
// a string rather than having its type resolved by YAML.
//
// The expander replaces the tagged node's tag rather than clearing it. Clearing it
// would let the decoder resolve the scalar afresh: `!Sub 12345` would arrive as an
// int and `!Ref 2026-08-02` as a time.Time. resolveValue handles ints, so the first
// merely round-trips by luck; a time.Time matches none of its cases and resolves to
// the empty string, silently emptying the property. CloudFormation values are
// strings, and the long forms are unquoted the same way, so each pair below must
// agree.
func TestCFN_ShortFormScalarStaysAString(t *testing.T) {
	cases := []struct {
		name string
		// expr is written unquoted on purpose: quoting it would make the YAML
		// scalar a string regardless and the test would prove nothing.
		expr string
		want string
	}{
		{name: "Integer", expr: "!Sub 12345", want: "12345"},
		{name: "Float", expr: "!Sub 1.50", want: "1.50"},
		{name: "Boolean", expr: "!Sub true", want: "true"},
		{name: "DateLooking", expr: "!Sub 2026-08-02", want: "2026-08-02"},
		{name: "SexagesimalLooking", expr: "!Sub 1:30", want: "1:30"},
		{name: "LeadingZero", expr: "!Sub 007", want: "007"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, resolveExpr(t, tc.expr),
				"a short form's scalar must reach the resolver as written")
		})
	}
}

// TestCFN_ConditionIsFrozenBeforeResourcesDeploy pins that a condition's value is
// fixed for the whole deployment.
//
// AWS evaluates conditions once, "when creating or updating a stack", and forbids a
// condition from referencing "resource logical IDs or their attributes" — so a
// condition's value cannot legitimately change partway through. substrate has no
// validation pass to reject the reference, which makes the freeze the thing that has
// to hold: were each read re-evaluated, a template like this one would see
// 'before-deploy' in a property resolved early and 'after-deploy' in an output
// resolved last, and the same template would resolve differently depending on the
// resource ordering.
func TestCFN_ConditionIsFrozenBeforeResourcesDeploy(t *testing.T) {
	// Ref Bucket resolves to the parameter's own name until the bucket deploys, so
	// the condition is false at evaluation time and would flip to true afterwards.
	tmpl := `
Conditions:
  BucketIsNamed: !Equals [!Ref Bucket, 'probe-bucket']
Resources:
  Bucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: probe-bucket
Outputs:
  V:
    Value: !If [BucketIsNamed, 'after-deploy', 'before-deploy']
`
	assert.Equal(t, "before-deploy", deployOneOutput(t, tmpl, "condfrozen"),
		"a condition evaluated before any resource deployed must keep that value")
}

// TestCFN_JSONTemplateUnaffected pins that the JSON path is untouched. JSON
// cannot carry YAML tags, so the expander has nothing to do there — and a JSON
// template whose keys happen to look like tags must be left exactly as it is.
func TestCFN_JSONTemplateUnaffected(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"Parameters": {"P": {"Type": "String", "Default": "live"}},
		"Resources": {
			"B": {"Type": "AWS::S3::Bucket",
			      "Properties": {"BucketName": {"Fn::Sub": "b-${P}"}}},
			"Q": {"Type": "AWS::SQS::Queue",
			      "Properties": {"QueueName": "json-queue",
			                     "Tags": [{"Key": "Literal", "Value": "!Sub not-a-tag"}]}}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "json-untouched", nil)
	require.NoError(t, err)

	for _, r := range result.Resources {
		if r.LogicalID == "B" {
			assert.Equal(t, "b-live", r.PhysicalID)
		}
		assert.Empty(t, r.Error)
	}
}

// --- helpers ----------------------------------------------------------------

// resolveExpr deploys a template whose single output is expr and returns the
// resolved value.
//
// The assertion runs through an output rather than a resource property because
// resolveValue is the same code path for both, and an output carries none of a
// property's naming constraints — deployS3Bucket lowercases a bucket name, which
// would silently mask a case difference in, say, a Base64 result.
func resolveExpr(t *testing.T, expr string) string {
	t.Helper()
	tmpl := `
Parameters:
  P: {Type: String, Default: live}
Conditions:
  IsLive: !Equals [!Ref P, 'live']
Resources:
  B:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: expr-bucket
Outputs:
  V:
    Value: ` + expr + `
`
	return deployOneOutput(t, tmpl, "expr")
}

// resolveCondition deploys a template whose output is chosen by a condition
// defined as expr, and returns the resolved value.
func resolveCondition(t *testing.T, expr string) string {
	t.Helper()
	tmpl := `
Parameters:
  P: {Type: String, Default: live}
Conditions:
  C: ` + expr + `
Resources:
  B:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: cond-bucket
Outputs:
  V:
    Value: !If [C, 'chosen', 'fallback']
`
	return deployOneOutput(t, tmpl, "cond")
}

// deployOneOutput deploys tmpl and returns the resolved value of its output "V".
func deployOneOutput(t *testing.T, tmpl, streamID string) string {
	t.Helper()
	d := newTestDeployer(t)
	result, err := d.Deploy(context.Background(), tmpl, streamID, nil)
	require.NoError(t, err)
	for _, r := range result.Resources {
		require.Empty(t, r.Error, "resource %s failed", r.LogicalID)
	}
	v, ok := result.Outputs["V"]
	require.True(t, ok, "output V was not resolved")
	return v
}

// deployOneBucket deploys tmpl and returns the physical ID of its single bucket.
func deployOneBucket(t *testing.T, tmpl, streamID string) string {
	t.Helper()
	d := newTestDeployer(t)
	result, err := d.Deploy(context.Background(), tmpl, streamID, nil)
	require.NoError(t, err)
	for _, r := range result.Resources {
		if r.Type == "AWS::S3::Bucket" {
			return r.PhysicalID
		}
	}
	t.Fatalf("no bucket in %d deployed resources", len(result.Resources))
	return ""
}

// newTestDeployerWithStore builds a minimal S3-only deployer alongside the event
// store it records into, so a test can assert which requests were actually sent.
func newTestDeployerWithStore(t *testing.T) (*emulator.StackDeployer, *emulator.EventStore) {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})

	s3Plugin := &emulator.S3Plugin{}
	require.NoError(t, s3Plugin.Initialize(context.Background(), emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
		},
	}))
	registry.Register(s3Plugin)

	return emulator.NewStackDeployer(registry, store, state, tc, logger, costs), store
}

// newTestDeployerWithLogger builds a minimal deployer whose logger is the one
// supplied, so a test can assert on what was logged.
func newTestDeployerWithLogger(t *testing.T, logger emulator.Logger) *emulator.StackDeployer {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})

	s3Plugin := &emulator.S3Plugin{}
	require.NoError(t, s3Plugin.Initialize(context.Background(), emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
		},
	}))
	registry.Register(s3Plugin)

	return emulator.NewStackDeployer(registry, store, state, tc, logger, costs)
}
