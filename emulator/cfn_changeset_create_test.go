package emulator_test

import (
	"context"
	"html"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cfnFullServer is a test server carrying every default plugin, driven in process through its
// handler: a CDK bootstrap template reaches KMS, ECR, IAM, SSM and S3, which the CloudFormation
// test server's three-plugin registry cannot serve.
type cfnFullServer struct {
	h http.Handler
}

func newCFNFullServer(t *testing.T) *cfnFullServer {
	t.Helper()
	return &cfnFullServer{h: emulator.StartTestServer(t).ControlPlaneHandler()}
}

// do sends one request with the given Host and returns the status and body.
func (s *cfnFullServer) do(t *testing.T, method, rawURL string, headers map[string]string, body string) (int, string) {
	t.Helper()
	r := httptest.NewRequest(method, rawURL, strings.NewReader(body))
	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	r.Host = u.Host
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	s.h.ServeHTTP(w, r)
	out, err := io.ReadAll(w.Body)
	require.NoError(t, err)
	return w.Code, string(out)
}

// cfn sends one CloudFormation action.
func (s *cfnFullServer) cfn(t *testing.T, action string, params map[string]string) (int, string) {
	t.Helper()
	form := url.Values{"Action": {action}, "Version": {"2010-05-15"}}
	for k, v := range params {
		form.Set(k, v)
	}
	return s.do(t, http.MethodPost, "http://cloudformation.us-east-1.amazonaws.com/",
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, form.Encode())
}

// cfnOK sends one CloudFormation action and requires a 200.
func (s *cfnFullServer) cfnOK(t *testing.T, action string, params map[string]string) string {
	t.Helper()
	code, body := s.cfn(t, action, params)
	require.Equalf(t, http.StatusOK, code, "%s: %s", action, body)
	return body
}

// cfnRefused sends one CloudFormation action and requires the given status and error code.
func (s *cfnFullServer) cfnRefused(t *testing.T, action string, params map[string]string, status int, code string) string {
	t.Helper()
	got, body := s.cfn(t, action, params)
	require.Equalf(t, status, got, "%s: %s", action, body)
	require.Equalf(t, code, xmlField(body, "Code"), "%s: %s", action, body)
	return xmlField(body, "Message")
}

// s3 sends one S3 request on the global path-style endpoint.
func (s *cfnFullServer) s3(t *testing.T, method, path, body string) (int, string) {
	t.Helper()
	return s.do(t, method, "http://s3.amazonaws.com"+path, nil, body)
}

// ssmParameter returns an SSM parameter's value.
func (s *cfnFullServer) ssmParameter(t *testing.T, name string) string {
	t.Helper()
	code, body := s.do(t, http.MethodPost, "http://ssm.us-east-1.amazonaws.com/", map[string]string{
		"Content-Type": "application/x-amz-json-1.1",
		"X-Amz-Target": "AmazonSSM.GetParameter",
	}, `{"Name":"`+name+`"}`)
	require.Equalf(t, http.StatusOK, code, "GetParameter %s: %s", name, body)
	m := regexp.MustCompile(`"Value":"([^"]*)"`).FindStringSubmatch(body)
	require.NotNil(t, m, body)
	return m[1]
}

// xmlField returns the unescaped text of the first element named name, or "".
func xmlField(body, name string) string {
	m := regexp.MustCompile(`<` + name + `>([^<]*)</` + name + `>`).FindStringSubmatch(body)
	if m == nil {
		return ""
	}
	return html.UnescapeString(m[1])
}

// xmlFields returns the unescaped text of every element named name, in order.
func xmlFields(body, name string) []string {
	var out []string
	for _, m := range regexp.MustCompile(`<`+name+`>([^<]*)</`+name+`>`).FindAllStringSubmatch(body, -1) {
		out = append(out, html.UnescapeString(m[1]))
	}
	return out
}

// cfnOutput returns one stack output's value from a DescribeStacks body.
func cfnOutput(body, key string) string {
	m := regexp.MustCompile(`<OutputKey>` + regexp.QuoteMeta(key) + `</OutputKey><OutputValue>([^<]*)</OutputValue>`).
		FindStringSubmatch(body)
	if m == nil {
		return ""
	}
	return m[1]
}

const cfnCreateCSTemplate = `{"Resources":{"Data":{"Type":"AWS::S3::Bucket",` +
	`"Properties":{"BucketName":"cs-create-bucket"}}},` +
	`"Outputs":{"BucketName":{"Value":{"Ref":"Data"}}}}`

// TestCFNChangeSet_CreateMakesAReviewStack is #1606's lifecycle: a CREATE change set creates the
// stack in REVIEW_IN_PROGRESS, executing it creates the stack's resources, and the change set then
// reports EXECUTE_COMPLETE while the stack's other change sets are deleted. Every later call names
// the stack and change set by the ARNs CreateChangeSet answered, which is how the CDK names them.
func TestCFNChangeSet_CreateMakesAReviewStack(t *testing.T) {
	s := newCFNFullServer(t)
	created := s.cfnOK(t, "CreateChangeSet", map[string]string{
		"StackName": "fresh", "ChangeSetName": "first", "ChangeSetType": "CREATE",
		"TemplateBody": cfnCreateCSTemplate, "Description": "CDK Changeset for execution 1",
		"Capabilities.member.1": "CAPABILITY_IAM",
		"Tags.member.1.Key":     "team", "Tags.member.1.Value": "core",
	})
	stackID, csID := xmlField(created, "StackId"), xmlField(created, "Id")
	require.Contains(t, stackID, ":stack/fresh/")
	require.Contains(t, csID, ":changeSet/first/")
	byARN := map[string]string{"StackName": stackID, "ChangeSetName": csID}

	// The stack exists, in review, with no template and no resources.
	described := s.cfnOK(t, "DescribeStacks", map[string]string{"StackName": "fresh"})
	assert.Equal(t, "REVIEW_IN_PROGRESS", xmlField(described, "StackStatus"))
	reviewCreated := xmlField(described, "CreationTime")
	assert.NotEmpty(t, reviewCreated)
	assert.NotContains(t, described, "<LastUpdatedTime>", "a stack in review has never been updated")
	assert.Contains(t, s.cfnOK(t, "ListStacks", nil),
		"<StackName>fresh</StackName><CreationTime>"+reviewCreated+"</CreationTime><StackStatus>REVIEW_IN_PROGRESS</StackStatus>")
	events := s.cfnOK(t, "DescribeStackEvents", map[string]string{"StackName": stackID})
	assert.Equal(t, []string{"REVIEW_IN_PROGRESS"}, xmlFields(events, "ResourceStatus"))
	assert.Equal(t, []string{"User Initiated"}, xmlFields(events, "ResourceStatusReason"))
	assert.Empty(t, xmlFields(s.cfnOK(t, "ListStackResources", map[string]string{"StackName": "fresh"}), "LogicalResourceId"))
	code, _ := s.s3(t, http.MethodHead, "/cs-create-bucket", "")
	assert.Equal(t, http.StatusNotFound, code, "nothing is created before the change set executes")

	cs := s.cfnOK(t, "DescribeChangeSet", byARN)
	assert.Equal(t, "CREATE_COMPLETE", xmlField(cs, "Status"))
	assert.Equal(t, "AVAILABLE", xmlField(cs, "ExecutionStatus"))
	assert.Equal(t, "CDK Changeset for execution 1", xmlField(cs, "Description"))
	assert.Contains(t, cs, "<Capabilities><member>CAPABILITY_IAM</member></Capabilities>")
	assert.Equal(t, []string{"Add"}, xmlFields(cs, "Action"), "a CREATE change set is diffed against an empty template")

	// A stack in review has no template to update.
	msg := s.cfnRefused(t, "UpdateStack", map[string]string{"StackName": "fresh", "TemplateBody": cfnCreateCSTemplate},
		http.StatusBadRequest, "ValidationError")
	assert.Equal(t, "Stack:"+stackID+" is in REVIEW_IN_PROGRESS state and can not be updated.", msg)
	s.cfnRefused(t, "CreateChangeSet", map[string]string{"StackName": "fresh", "ChangeSetName": "upd",
		"ChangeSetType": "UPDATE", "TemplateBody": cfnCreateCSTemplate}, http.StatusBadRequest, "ValidationError")

	// A second CREATE change set on the stack in review is accepted.
	s.cfnOK(t, "CreateChangeSet", map[string]string{"StackName": "fresh", "ChangeSetName": "second",
		"ChangeSetType": "CREATE", "TemplateBody": cfnCreateCSTemplate})

	s.cfnOK(t, "ExecuteChangeSet", byARN)

	described = s.cfnOK(t, "DescribeStacks", map[string]string{"StackName": stackID})
	assert.Equal(t, "CREATE_COMPLETE", xmlField(described, "StackStatus"))
	assert.Equal(t, reviewCreated, xmlField(described, "CreationTime"),
		"the stack was created when the change set put it in review")
	assert.Equal(t, "cs-create-bucket", cfnOutput(described, "BucketName"))
	assert.Contains(t, described, "<Key>team</Key><Value>core</Value>", "the change set's tags are the stack's")
	code, _ = s.s3(t, http.MethodHead, "/cs-create-bucket", "")
	assert.Equal(t, http.StatusOK, code)

	statuses := xmlFields(s.cfnOK(t, "DescribeStackEvents", map[string]string{"StackName": "fresh"}), "ResourceStatus")
	require.GreaterOrEqual(t, len(statuses), 3)
	assert.Equal(t, []string{"CREATE_IN_PROGRESS", "REVIEW_IN_PROGRESS"}, statuses[len(statuses)-2:],
		"newest first, so the review is the oldest event and the create opened after it")
	assert.Equal(t, "CREATE_COMPLETE", statuses[0])

	// The executed change set remains and says so; the stack's other one is gone.
	cs = s.cfnOK(t, "DescribeChangeSet", byARN)
	assert.Equal(t, "EXECUTE_COMPLETE", xmlField(cs, "ExecutionStatus"))
	s.cfnRefused(t, "DescribeChangeSet", map[string]string{"StackName": "fresh", "ChangeSetName": "second"},
		http.StatusNotFound, "ChangeSetNotFound")
	assert.Equal(t, []string{"first"}, xmlFields(s.cfnOK(t, "ListChangeSets", map[string]string{"StackName": "fresh"}), "ChangeSetName"))
	msg = s.cfnRefused(t, "ExecuteChangeSet", byARN, http.StatusBadRequest, "InvalidChangeSetStatus")
	assert.Equal(t, "ChangeSet [first] cannot be executed in its current execution status of [EXECUTE_COMPLETE]", msg)

	// The stack now exists, so CREATE is refused with the observed wording.
	msg = s.cfnRefused(t, "CreateChangeSet", map[string]string{"StackName": "fresh", "ChangeSetName": "again",
		"ChangeSetType": "CREATE", "TemplateBody": cfnCreateCSTemplate}, http.StatusBadRequest, "ValidationError")
	assert.Equal(t, "Stack [fresh] already exists and cannot be created again with the changeSet [again].", msg)
}

// TestCFNChangeSet_DeleteStackRemovesAReviewStack pins that DeleteStack removes a stack in review
// with its change sets, so the CDK's retry — delete the stale stack, create the change set again
// under the same names — is not refused with AlreadyExists by the change set it left.
func TestCFNChangeSet_DeleteStackRemovesAReviewStack(t *testing.T) {
	s := newCFNFullServer(t)
	params := map[string]string{"StackName": "stale", "ChangeSetName": "cdk-deploy-change-set",
		"ChangeSetType": "CREATE", "TemplateBody": cfnCreateCSTemplate}
	s.cfnOK(t, "CreateChangeSet", params)
	s.cfnOK(t, "DeleteStack", map[string]string{"StackName": "stale"})
	msg := s.cfnRefused(t, "DescribeStacks", map[string]string{"StackName": "stale"}, http.StatusBadRequest, "ValidationError")
	assert.Equal(t, "Stack with id stale does not exist", msg, "the CDK's not-found test reads exactly this message")
	assert.NotContains(t, s.cfnOK(t, "ListStacks", nil), "<StackName>stale</StackName>")

	s.cfnOK(t, "CreateChangeSet", params)
	cs := s.cfnOK(t, "DescribeChangeSet", map[string]string{"StackName": "stale", "ChangeSetName": "cdk-deploy-change-set"})
	assert.Equal(t, "AVAILABLE", xmlField(cs, "ExecutionStatus"))
}

// TestCFNChangeSet_EmptyUpdateFails pins the empty change set: an UPDATE that changes nothing is
// FAILED with AWS's reason and cannot be executed, which the CDK reads as "no changes".
func TestCFNChangeSet_EmptyUpdateFails(t *testing.T) {
	const withOutput = `{"Resources":{"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cs-empty-bucket"}}},` +
		`"Outputs":{"BucketName":{"Value":{"Ref":"Data"}},"Extra":{"Value":"x"}}}`
	const base = `{"Resources":{"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cs-empty-bucket"}}},` +
		`"Outputs":{"BucketName":{"Value":{"Ref":"Data"}}}}`
	tests := []struct {
		name      string
		params    map[string]string
		wantEmpty bool
	}{
		{name: "same template", params: map[string]string{"TemplateBody": base}, wantEmpty: true},
		{name: "previous template", params: map[string]string{"UsePreviousTemplate": "true"}, wantEmpty: true},
		{name: "the stack's own tags", params: map[string]string{"TemplateBody": base,
			"Tags.member.1.Key": "env", "Tags.member.1.Value": "dev"}, wantEmpty: true},
		{name: "changed tags", params: map[string]string{"TemplateBody": base,
			"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod"}},
		{name: "an added output", params: map[string]string{"TemplateBody": withOutput}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newCFNFullServer(t)
			s.cfnOK(t, "CreateStack", map[string]string{"StackName": "steady", "TemplateBody": base,
				"Tags.member.1.Key": "env", "Tags.member.1.Value": "dev"})
			params := map[string]string{"StackName": "steady", "ChangeSetName": "noop"}
			for k, v := range tc.params {
				params[k] = v
			}
			s.cfnOK(t, "CreateChangeSet", params)
			ref := map[string]string{"StackName": "steady", "ChangeSetName": "noop"}
			cs := s.cfnOK(t, "DescribeChangeSet", ref)
			listed := s.cfnOK(t, "ListChangeSets", map[string]string{"StackName": "steady"})
			if !tc.wantEmpty {
				assert.Equal(t, "CREATE_COMPLETE", xmlField(cs, "Status"))
				assert.Equal(t, "AVAILABLE", xmlField(listed, "ExecutionStatus"))
				s.cfnOK(t, "ExecuteChangeSet", ref)
				return
			}
			assert.Equal(t, "FAILED", xmlField(cs, "Status"))
			assert.Equal(t, "UNAVAILABLE", xmlField(cs, "ExecutionStatus"))
			assert.Equal(t, "The submitted information didn't contain changes. "+
				"Submit different information to create a change set.", xmlField(cs, "StatusReason"))
			assert.Equal(t, "FAILED", xmlField(listed, "Status"))
			assert.Equal(t, "UNAVAILABLE", xmlField(listed, "ExecutionStatus"))
			s.cfnRefused(t, "ExecuteChangeSet", ref, http.StatusBadRequest, "InvalidChangeSetStatus")
			s.cfnOK(t, "DeleteChangeSet", ref)
		})
	}
}

// TestCFNChangeSet_Refusals covers the CreateChangeSet requests substrate refuses before recording
// anything.
func TestCFNChangeSet_Refusals(t *testing.T) {
	tests := []struct {
		name     string
		params   map[string]string
		wantCode string
		wantMsg  string
	}{
		{name: "IMPORT", params: map[string]string{"ChangeSetType": "IMPORT", "TemplateBody": cfnCreateCSTemplate},
			wantCode: "ValidationError",
			wantMsg:  "ChangeSetType IMPORT is not emulated: substrate does not import existing resources into a stack"},
		{name: "an unknown type", params: map[string]string{"ChangeSetType": "MERGE", "TemplateBody": cfnCreateCSTemplate},
			wantCode: "ValidationError",
			wantMsg: "1 validation error detected: Value 'MERGE' at 'changeSetType' failed to satisfy constraint: " +
				"Member must satisfy enum value set: [CREATE, UPDATE, IMPORT]"},
		{name: "DELETE on an update", params: map[string]string{"OnStackFailure": "DELETE", "TemplateBody": cfnCreateCSTemplate},
			wantCode: "ValidationError", wantMsg: "OnStackFailure DELETE is only valid for a CREATE change set"},
		{name: "an unknown OnStackFailure", params: map[string]string{"ChangeSetType": "CREATE", "OnStackFailure": "SHRUG",
			"TemplateBody": cfnCreateCSTemplate}, wantCode: "ValidationError"},
		{name: "CREATE with no template", params: map[string]string{"ChangeSetType": "CREATE"}, wantCode: "MissingParameter"},
		{name: "UPDATE of no stack", params: map[string]string{"TemplateBody": cfnCreateCSTemplate},
			wantCode: "ValidationError", wantMsg: "Stack with id nowhere does not exist"},
		{name: "a RoleARN too short", params: map[string]string{"ChangeSetType": "CREATE", "RoleARN": "arn:short",
			"TemplateBody": cfnCreateCSTemplate}, wantCode: "ValidationError"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newCFNFullServer(t)
			params := map[string]string{"StackName": "nowhere", "ChangeSetName": "cs"}
			for k, v := range tc.params {
				params[k] = v
			}
			msg := s.cfnRefused(t, "CreateChangeSet", params, http.StatusBadRequest, tc.wantCode)
			if tc.wantMsg != "" {
				assert.Equal(t, tc.wantMsg, msg)
			}
			s.cfnRefused(t, "DescribeStacks", map[string]string{"StackName": "nowhere"}, http.StatusBadRequest, "ValidationError")
		})
	}
}

// TestCFNChangeSet_CreateFailureActions pins that executing a CREATE change set fails the way a
// CreateStack does, under the change set's OnStackFailure or the execution's DisableRollback.
func TestCFNChangeSet_CreateFailureActions(t *testing.T) {
	const failing = `{"Resources":{"Bad":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"no"}}}}`
	tests := []struct {
		name       string
		create     map[string]string
		execute    map[string]string
		wantStatus string // "" when the stack is deleted
		wantExec   string // the change set's ExecutionStatus afterwards, "" when it is gone
		refused    bool
	}{
		{name: "rolls back by default", wantStatus: "ROLLBACK_COMPLETE", wantExec: "EXECUTE_FAILED"},
		{name: "DisableRollback true", execute: map[string]string{"DisableRollback": "true"},
			wantStatus: "CREATE_FAILED", wantExec: "EXECUTE_FAILED"},
		{name: "DisableRollback false", execute: map[string]string{"DisableRollback": "false"},
			wantStatus: "ROLLBACK_COMPLETE", wantExec: "EXECUTE_FAILED"},
		{name: "OnStackFailure DO_NOTHING", create: map[string]string{"OnStackFailure": "DO_NOTHING"},
			wantStatus: "CREATE_FAILED", wantExec: "EXECUTE_FAILED"},
		{name: "OnStackFailure DELETE", create: map[string]string{"OnStackFailure": "DELETE"}},
		{name: "both", create: map[string]string{"OnStackFailure": "ROLLBACK"},
			execute: map[string]string{"DisableRollback": "true"}, refused: true},
		{name: "a DisableRollback that is no boolean", execute: map[string]string{"DisableRollback": "maybe"}, refused: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newCFNFullServer(t)
			create := map[string]string{"StackName": "doomed", "ChangeSetName": "cs", "ChangeSetType": "CREATE",
				"TemplateBody": failing}
			for k, v := range tc.create {
				create[k] = v
			}
			s.cfnOK(t, "CreateChangeSet", create)
			execute := map[string]string{"StackName": "doomed", "ChangeSetName": "cs"}
			for k, v := range tc.execute {
				execute[k] = v
			}
			if tc.refused {
				s.cfnRefused(t, "ExecuteChangeSet", execute, http.StatusBadRequest, "ValidationError")
				assert.Equal(t, "REVIEW_IN_PROGRESS",
					xmlField(s.cfnOK(t, "DescribeStacks", map[string]string{"StackName": "doomed"}), "StackStatus"))
				return
			}
			s.cfnOK(t, "ExecuteChangeSet", execute)
			ref := map[string]string{"StackName": "doomed", "ChangeSetName": "cs"}
			if tc.wantStatus == "" {
				s.cfnRefused(t, "DescribeStacks", map[string]string{"StackName": "doomed"}, http.StatusBadRequest, "ValidationError")
				s.cfnRefused(t, "DescribeChangeSet", ref, http.StatusNotFound, "ChangeSetNotFound")
				return
			}
			assert.Equal(t, tc.wantStatus, xmlField(s.cfnOK(t, "DescribeStacks", map[string]string{"StackName": "doomed"}), "StackStatus"))
			assert.Equal(t, tc.wantExec, xmlField(s.cfnOK(t, "DescribeChangeSet", ref), "ExecutionStatus"))
		})
	}
}

// TestCFNChangeSet_RoleARNIsTheStacks pins that a change set's RoleARN is the role its execution
// runs as and the stack's role afterwards: "CloudFormation uses this role for all future operations
// on the stack".
func TestCFNChangeSet_RoleARNIsTheStacks(t *testing.T) {
	const role = "arn:aws:iam::123456789012:role/cfn-exec"
	s := newCFNFullServer(t)
	s.cfnOK(t, "CreateChangeSet", map[string]string{"StackName": "roled", "ChangeSetName": "cs",
		"ChangeSetType": "CREATE", "TemplateBody": cfnCreateCSTemplate, "RoleARN": role})
	s.cfnOK(t, "ExecuteChangeSet", map[string]string{"StackName": "roled", "ChangeSetName": "cs"})
	assert.Equal(t, role, xmlField(s.cfnOK(t, "DescribeStacks", map[string]string{"StackName": "roled"}), "RoleARN"))

	// An UPDATE change set's role replaces it.
	const next = "arn:aws:iam::123456789012:role/cfn-exec-2"
	s.cfnOK(t, "CreateChangeSet", map[string]string{"StackName": "roled", "ChangeSetName": "upd",
		"TemplateBody": strings.Replace(cfnCreateCSTemplate, `"Outputs":{`, `"Outputs":{"Two":{"Value":"2"},`, 1),
		"RoleARN":      next})
	s.cfnOK(t, "ExecuteChangeSet", map[string]string{"StackName": "roled", "ChangeSetName": "upd"})
	described := s.cfnOK(t, "DescribeStacks", map[string]string{"StackName": "roled"})
	assert.Equal(t, "UPDATE_COMPLETE", xmlField(described, "StackStatus"))
	assert.Equal(t, next, xmlField(described, "RoleARN"))
}

// TestCFNChangeSet_CDKBootstrapTemplate deploys the real CDK bootstrap template the way
// `cdk bootstrap` does — a CREATE change set, then ExecuteChangeSet — and asserts what the CDK
// reads back: the stack's outputs and the bootstrap-version SSM parameter. It also asserts the
// staging bucket carries the policy, lifecycle rules and Block Public Access the template declares,
// which were stubbed or ignored before #1606, and that bootstrapping again is an empty change set.
func TestCFNChangeSet_CDKBootstrapTemplate(t *testing.T) {
	body, err := os.ReadFile("testdata/cfn/cdk_bootstrap_template.yaml")
	require.NoError(t, err)
	s := newCFNFullServer(t)
	params := map[string]string{"StackName": "CDKToolkit", "ChangeSetName": "cdk-deploy-change-set",
		"ChangeSetType": "CREATE", "TemplateBody": string(body),
		"Capabilities.member.1": "CAPABILITY_IAM", "Capabilities.member.2": "CAPABILITY_NAMED_IAM",
		"Capabilities.member.3": "CAPABILITY_AUTO_EXPAND"}
	s.cfnOK(t, "CreateChangeSet", params)
	cs := s.cfnOK(t, "DescribeChangeSet", map[string]string{"StackName": "CDKToolkit", "ChangeSetName": "cdk-deploy-change-set"})
	require.Equal(t, "CREATE_COMPLETE", xmlField(cs, "Status"), cs)
	assert.Contains(t, xmlFields(cs, "LogicalResourceId"), "StagingBucketPolicy")
	assert.NotContains(t, xmlFields(cs, "LogicalResourceId"), "CdkBoostrapPermissionsBoundaryPolicy",
		"a resource whose condition is false is not an Add")
	s.cfnOK(t, "ExecuteChangeSet", map[string]string{"StackName": "CDKToolkit", "ChangeSetName": "cdk-deploy-change-set"})

	described := s.cfnOK(t, "DescribeStacks", map[string]string{"StackName": "CDKToolkit"})
	require.Equal(t, "CREATE_COMPLETE", xmlField(described, "StackStatus"), described)
	const bucket = "cdk-hnb659fds-assets-123456789012-us-east-1"
	assert.Equal(t, bucket, cfnOutput(described, "BucketName"))
	assert.Equal(t, bucket+".s3.us-east-1.amazonaws.com", cfnOutput(described, "BucketDomainName"))
	assert.Equal(t, "34", cfnOutput(described, "BootstrapVersion"))
	assert.Equal(t, "cdk-hnb659fds-container-assets-123456789012-us-east-1", cfnOutput(described, "ImageRepositoryName"))
	assert.True(t, strings.HasPrefix(cfnOutput(described, "FileAssetKeyArn"), "arn:aws:kms:us-east-1:123456789012:key/"),
		cfnOutput(described, "FileAssetKeyArn"))
	assert.Equal(t, "34", s.ssmParameter(t, "/cdk-bootstrap/hnb659fds/version"))

	resources := s.cfnOK(t, "ListStackResources", map[string]string{"StackName": "CDKToolkit"})
	assert.NotContains(t, resources, "CREATE_FAILED", resources)
	assert.Contains(t, xmlFields(resources, "LogicalResourceId"), "StagingBucketPolicy")
	assert.NotContains(t, xmlFields(resources, "LogicalResourceId"), "CdkBoostrapPermissionsBoundaryPolicy",
		"its condition is false by default")

	code, policy := s.s3(t, http.MethodGet, "/"+bucket+"?policy", "")
	require.Equal(t, http.StatusOK, code, policy)
	assert.Contains(t, policy, `"AllowSSLRequestsOnly"`)
	assert.Contains(t, policy, `"arn:aws:s3:::`+bucket+`/*"`)
	code, pab := s.s3(t, http.MethodGet, "/"+bucket+"?publicAccessBlock", "")
	require.Equal(t, http.StatusOK, code, pab)
	assert.Contains(t, pab, "<BlockPublicPolicy>true</BlockPublicPolicy>")
	assert.Contains(t, pab, "<RestrictPublicBuckets>true</RestrictPublicBuckets>")
	code, lifecycle := s.s3(t, http.MethodGet, "/"+bucket+"?lifecycle", "")
	require.Equal(t, http.StatusOK, code, lifecycle)
	assert.Contains(t, lifecycle, "<ID>CleanupOldVersions</ID>")
	assert.Contains(t, lifecycle, "<NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays></NoncurrentVersionExpiration>")
	assert.Contains(t, lifecycle, "<AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload>")

	// Bootstrapping again: the CDK deletes the executed change set first, then creates an UPDATE
	// change set of the same template, which has nothing to change.
	s.cfnOK(t, "DeleteChangeSet", map[string]string{"StackName": "CDKToolkit", "ChangeSetName": "cdk-deploy-change-set"})
	params["ChangeSetType"] = "UPDATE"
	s.cfnOK(t, "CreateChangeSet", params)
	cs = s.cfnOK(t, "DescribeChangeSet", map[string]string{"StackName": "CDKToolkit", "ChangeSetName": "cdk-deploy-change-set"})
	assert.Equal(t, "FAILED", xmlField(cs, "Status"))
	assert.True(t, strings.HasPrefix(xmlField(cs, "StatusReason"), "The submitted information didn't contain changes."))
}

// TestCFNChangeSet_InProcess pins the deployer-level API an in-process caller reaches: the
// refusals are classified by their sentinels, and a CREATE change set whose stack stopped being in
// review before it was executed cannot be executed.
func TestCFNChangeSet_InProcess(t *testing.T) {
	ctx := context.Background()
	d := newTestDeployer(t)

	_, err := d.CreateChangeSetWithOptions(ctx, "inproc", "imp", cfnCreateCSTemplate, nil,
		emulator.CFNChangeSetOptions{Type: emulator.CFNChangeSetTypeImport})
	require.ErrorIs(t, err, emulator.ErrCFNChangeSetTypeUnsupported)

	cs, err := d.CreateChangeSetWithOptions(ctx, "inproc", "create", cfnCreateCSTemplate, nil,
		emulator.CFNChangeSetOptions{Type: emulator.CFNChangeSetTypeCreate, Description: "first"})
	require.NoError(t, err)
	assert.Equal(t, "AVAILABLE", cs.ExecutionStatus)
	assert.Equal(t, "CREATE", cs.Type)

	// A deploy of the same name — which the deployer, unlike CreateStack on the wire, does not
	// refuse — takes the stack out of review, so the change set no longer describes a create.
	_, err = d.Deploy(ctx, cfnCreateCSTemplate, "inproc", nil)
	require.NoError(t, err)
	_, err = d.ExecuteChangeSet(ctx, "inproc", "create")
	require.ErrorIs(t, err, emulator.ErrCFNChangeSetNotExecutable)

	_, err = d.CreateChangeSetWithOptions(ctx, "inproc", "again", cfnCreateCSTemplate, nil,
		emulator.CFNChangeSetOptions{Type: emulator.CFNChangeSetTypeCreate})
	require.ErrorIs(t, err, emulator.ErrCFNStackAlreadyExists)

	empty, err := d.CreateChangeSet(ctx, "inproc", "noop", cfnCreateCSTemplate, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "FAILED", empty.Status)
	_, err = d.ExecuteChangeSetWithOptions(ctx, "inproc", "noop", emulator.CFNExecuteOptions{})
	require.ErrorIs(t, err, emulator.ErrCFNChangeSetNotExecutable)
}
