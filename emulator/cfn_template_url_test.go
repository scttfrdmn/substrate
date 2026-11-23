package emulator_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cfnURLTemplate is the template the TemplateURL tests upload, small and with one output so a
// deploy from it is observable.
const cfnURLTemplate = `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"from-url"}}},` +
	`"Outputs":{"Queue":{"Value":{"Fn::GetAtt":["Q","QueueName"]}}}}`

// cfnUploadTemplate creates bucket and puts body at key.
func cfnUploadTemplate(t *testing.T, s *cfnFullServer, bucket, key, body string) {
	t.Helper()
	code, out := s.s3(t, http.MethodPut, "/"+bucket, "")
	require.Equalf(t, http.StatusOK, code, "CreateBucket %s: %s", bucket, out)
	code, out = s.s3(t, http.MethodPut, "/"+bucket+"/"+key, body)
	require.Equalf(t, http.StatusOK, code, "PutObject %s/%s: %s", bucket, key, out)
}

// TestCFNTemplateURL_ReadsSubstratesS3 pins #1606's TemplateURL: every S3 URL form CloudFormation
// is given a template in is read from substrate's own S3, as is a path-style URL on the endpoint
// the request itself was sent to — the URL the CDK builds when it is pointed at an emulator.
func TestCFNTemplateURL_ReadsSubstratesS3(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		url    string
	}{
		{name: "path-style global", bucket: "tmpl-a", url: "https://s3.amazonaws.com/tmpl-a/stacks/app.json"},
		{name: "path-style regional", bucket: "tmpl-b", url: "https://s3.us-east-1.amazonaws.com/tmpl-b/stacks/app.json"},
		{name: "path-style legacy dash", bucket: "tmpl-c", url: "https://s3-us-west-2.amazonaws.com/tmpl-c/stacks/app.json"},
		{name: "virtual-hosted global", bucket: "tmpl-d", url: "https://tmpl-d.s3.amazonaws.com/stacks/app.json"},
		{name: "virtual-hosted regional", bucket: "tmpl-e", url: "https://tmpl-e.s3.eu-west-1.amazonaws.com/stacks/app.json"},
		{name: "virtual-hosted dotted bucket", bucket: "tmpl.f.example",
			url: "https://tmpl.f.example.s3.us-east-1.amazonaws.com/stacks/app.json"},
		{name: "virtual-hosted legacy dash", bucket: "tmpl-g", url: "https://tmpl-g.s3-us-west-2.amazonaws.com/stacks/app.json"},
		{name: "the request's own endpoint", bucket: "tmpl-h",
			url: "http://cloudformation.us-east-1.amazonaws.com/tmpl-h/stacks/app.json"},
		{name: "an escaped key", bucket: "tmpl-i", url: "https://s3.amazonaws.com/tmpl-i/stacks%2Fapp.json"},
		{name: "dual-stack", bucket: "tmpl-j", url: "https://s3.dualstack.us-east-1.amazonaws.com/tmpl-j/stacks/app.json"},
		{name: "FIPS", bucket: "tmpl-k", url: "https://s3-fips.us-gov-west-1.amazonaws.com/tmpl-k/stacks/app.json"},
		{name: "FIPS dual-stack virtual-hosted", bucket: "tmpl-l",
			url: "https://tmpl-l.s3-fips.dualstack.us-east-1.amazonaws.com/stacks/app.json"},
		{name: "a presigned URL", bucket: "tmpl-m",
			url: "https://s3.amazonaws.com/tmpl-m/stacks/app.json?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=900&X-Amz-Signature=abc"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newCFNFullServer(t)
			cfnUploadTemplate(t, s, tc.bucket, "stacks/app.json", cfnURLTemplate)
			s.cfnOK(t, "CreateStack", map[string]string{"StackName": "by-url", "TemplateURL": tc.url})
			described := s.cfnOK(t, "DescribeStacks", map[string]string{"StackName": "by-url"})
			assert.Equal(t, "CREATE_COMPLETE", xmlField(described, "StackStatus"))
			assert.Equal(t, "from-url", cfnOutput(described, "Queue"))
			assert.Equal(t, cfnURLTemplate, xmlField(s.cfnOK(t, "GetTemplate", map[string]string{"StackName": "by-url"}), "TemplateBody"),
				"the stack's template is the object's body")
		})
	}
}

// TestCFNTemplateURL_UpdateAndChangeSet pins that UpdateStack and CreateChangeSet read TemplateURL
// too: the CDK deploys every stack by URL, through a change set.
func TestCFNTemplateURL_UpdateAndChangeSet(t *testing.T) {
	s := newCFNFullServer(t)
	const v2 = `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"from-url"}},` +
		`"R":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"from-url-2"}}}}`
	cfnUploadTemplate(t, s, "tmpl-up", "v1.json", cfnURLTemplate)
	code, out := s.s3(t, http.MethodPut, "/tmpl-up/v2.json", v2)
	require.Equal(t, http.StatusOK, code, out)

	s.cfnOK(t, "CreateChangeSet", map[string]string{"StackName": "url-cs", "ChangeSetName": "create",
		"ChangeSetType": "CREATE", "TemplateURL": "https://s3.amazonaws.com/tmpl-up/v1.json"})
	s.cfnOK(t, "ExecuteChangeSet", map[string]string{"StackName": "url-cs", "ChangeSetName": "create"})
	s.cfnOK(t, "CreateChangeSet", map[string]string{"StackName": "url-cs", "ChangeSetName": "update",
		"TemplateURL": "https://s3.amazonaws.com/tmpl-up/v2.json"})
	cs := s.cfnOK(t, "DescribeChangeSet", map[string]string{"StackName": "url-cs", "ChangeSetName": "update"})
	assert.Equal(t, []string{"R"}, xmlFields(cs, "LogicalResourceId"))

	s.cfnOK(t, "UpdateStack", map[string]string{"StackName": "url-cs", "TemplateURL": "https://s3.amazonaws.com/tmpl-up/v2.json"})
	assert.Equal(t, "UPDATE_COMPLETE", xmlField(s.cfnOK(t, "DescribeStacks", map[string]string{"StackName": "url-cs"}), "StackStatus"))
}

// TestCFNTemplateURL_Refusals covers the TemplateURLs CloudFormation refuses, each with the
// ValidationError it answers and no stack left behind.
func TestCFNTemplateURL_Refusals(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]string
		wantMsg string
	}{
		{name: "not S3", params: map[string]string{"TemplateURL": "https://example.com/tmpl/app.json"},
			wantMsg: "TemplateURL must be a supported URL."},
		{name: "S3 over http", params: map[string]string{"TemplateURL": "http://s3.amazonaws.com/tmpl/app.json"},
			wantMsg: "TemplateURL must be a supported URL."},
		{name: "an s3 URI", params: map[string]string{"TemplateURL": "s3://tmpl/app.json"},
			wantMsg: "TemplateURL must be a supported URL."},
		{name: "a bucket with no key", params: map[string]string{"TemplateURL": "https://s3.amazonaws.com/tmpl"},
			wantMsg: "TemplateURL must be a supported URL."},
		{name: "a virtual-hosted bucket with no key", params: map[string]string{"TemplateURL": "https://tmpl.s3.amazonaws.com/"},
			wantMsg: "TemplateURL must be a supported URL."},
		{name: "a website endpoint", params: map[string]string{"TemplateURL": "https://tmpl.s3-website-us-east-1.amazonaws.com/app.json"},
			wantMsg: "TemplateURL must be a supported URL."},
		{name: "a dotted website endpoint", params: map[string]string{"TemplateURL": "https://tmpl.s3-website.eu-west-1.amazonaws.com/app.json"},
			wantMsg: "TemplateURL must be a supported URL."},
		{name: "the accelerate endpoint", params: map[string]string{"TemplateURL": "https://tmpl.s3-accelerate.amazonaws.com/app.json"},
			wantMsg: "TemplateURL must be a supported URL."},
		{name: "a label that is no Region", params: map[string]string{"TemplateURL": "https://s3-anything.amazonaws.com/tmpl/app.json"},
			wantMsg: "TemplateURL must be a supported URL."},
		{name: "an unknown query parameter", params: map[string]string{"TemplateURL": "https://s3.amazonaws.com/tmpl/app.json?partNumber=1"},
			wantMsg: "TemplateURL must be a supported URL."},
		{name: "an empty versionId", params: map[string]string{"TemplateURL": "https://s3.amazonaws.com/tmpl/app.json?versionId="},
			wantMsg: "TemplateURL must be a supported URL."},
		{name: "not a URL", params: map[string]string{"TemplateURL": "::"},
			wantMsg: "TemplateURL must be a supported URL."},
		{name: "a missing key", params: map[string]string{"TemplateURL": "https://s3.amazonaws.com/tmpl/absent.json"},
			wantMsg: "S3 error: The specified key does not exist."},
		{name: "a missing bucket", params: map[string]string{"TemplateURL": "https://s3.amazonaws.com/no-such-tmpl/app.json"},
			wantMsg: "S3 error: The specified bucket does not exist."},
		{name: "an empty object", params: map[string]string{"TemplateURL": "https://s3.amazonaws.com/tmpl/empty.json"},
			wantMsg: "S3 error: the template object is empty"},
		{name: "an object over 1 MB", params: map[string]string{"TemplateURL": "https://s3.amazonaws.com/tmpl/huge.json"},
			wantMsg: "The template at TemplateURL exceeds the 1 MB maximum size"},
		{name: "both body and URL", params: map[string]string{"TemplateURL": "https://s3.amazonaws.com/tmpl/app.json",
			"TemplateBody": cfnURLTemplate}, wantMsg: "Specify only one of TemplateBody and TemplateURL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newCFNFullServer(t)
			cfnUploadTemplate(t, s, "tmpl", "app.json", cfnURLTemplate)
			code, out := s.s3(t, http.MethodPut, "/tmpl/empty.json", "")
			require.Equal(t, http.StatusOK, code, out)
			code, out = s.s3(t, http.MethodPut, "/tmpl/huge.json", `{"Resources":{}}`+strings.Repeat(" ", 1<<20))
			require.Equal(t, http.StatusOK, code, out)

			for _, action := range []string{"CreateStack", "CreateChangeSet"} {
				params := map[string]string{"StackName": "refused", "ChangeSetName": "cs", "ChangeSetType": "CREATE"}
				for k, v := range tc.params {
					params[k] = v
				}
				msg := s.cfnRefused(t, action, params, http.StatusBadRequest, "ValidationError")
				assert.Equal(t, tc.wantMsg, msg, action)
			}
			s.cfnRefused(t, "DescribeStacks", map[string]string{"StackName": "refused"}, http.StatusBadRequest, "ValidationError")
		})
	}
}

// TestCFNTemplateURL_VersionID pins that a TemplateURL's versionId reads that version of the object
// rather than the current one, and that a version the bucket does not hold is S3's own refusal.
func TestCFNTemplateURL_VersionID(t *testing.T) {
	s := newCFNFullServer(t)
	code, out := s.s3(t, http.MethodPut, "/tmpl-v", "")
	require.Equal(t, http.StatusOK, code, out)
	code, out = s.s3(t, http.MethodPut, "/tmpl-v?versioning",
		"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>")
	require.Equal(t, http.StatusOK, code, out)
	put := func(body string) string {
		t.Helper()
		r := httptest.NewRequest(http.MethodPut, "http://s3.amazonaws.com/tmpl-v/app.json", strings.NewReader(body))
		r.Host = "s3.amazonaws.com"
		w := httptest.NewRecorder()
		s.h.ServeHTTP(w, r)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		v := w.Header().Get("x-amz-version-id")
		require.NotEmpty(t, v)
		return v
	}
	first := put(cfnURLTemplate)
	put(strings.Replace(cfnURLTemplate, "from-url", "from-url-latest", 1))

	s.cfnOK(t, "CreateStack", map[string]string{"StackName": "versioned",
		"TemplateURL": "https://s3.amazonaws.com/tmpl-v/app.json?versionId=" + first})
	assert.Equal(t, "from-url", cfnOutput(s.cfnOK(t, "DescribeStacks", map[string]string{"StackName": "versioned"}), "Queue"),
		"the named version, not the current one")

	msg := s.cfnRefused(t, "CreateStack", map[string]string{"StackName": "unversioned",
		"TemplateURL": "https://s3.amazonaws.com/tmpl-v/app.json?versionId=nope"}, http.StatusBadRequest, "ValidationError")
	assert.True(t, strings.HasPrefix(msg, "S3 error: "), msg)
}
