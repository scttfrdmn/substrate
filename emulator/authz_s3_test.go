package emulator_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// s3AuthzPolicy is an unconditional Allow of actions on every resource, optionally followed by
// extra statements.
func s3AuthzPolicy(actions []string, extra ...emulator.PolicyStatement) emulator.PolicyDocument {
	return emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: append([]emulator.PolicyStatement{{
			Effect:   "Allow",
			Action:   emulator.StringOrSlice(actions),
			Resource: emulator.StringOrSlice{"*"},
		}}, extra...),
	}
}

// s3AuthzCheck runs CheckAccess for req as a user holding doc and returns the error, if any.
func s3AuthzCheck(t *testing.T, user string, doc emulator.PolicyDocument, req *emulator.AWSRequest) error {
	t.Helper()
	policyARN := "arn:aws:iam::123456789012:policy/" + user + "-policy"
	state := newAuthTestState(t, user, policyARN, doc)
	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
	return auth.CheckAccess(newAuthTestReqCtx("arn:aws:iam::123456789012:user/"+user), req)
}

// requireS3Denied requires err to be an AccessDenied naming wantAction.
func requireS3Denied(t *testing.T, err error, wantAction string) {
	t.Helper()
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDenied", awsErr.Code)
	assert.Contains(t, awsErr.Message, wantAction)
}

// TestS3Authz_RequestTagsReadByTheHandlersParser is review item 1 of #1478: aws:RequestTag is read
// from x-amz-tagging with the parser the handler stores the set with. url.ParseQuery refused a ";"
// and so read no tags at all, which let a request carrying env=prod past a Deny on env=prod; and a
// set the parser cannot read, or that names a key twice, now fails closed.
func TestS3Authz_RequestTagsReadByTheHandlersParser(t *testing.T) {
	denyProd := emulator.PolicyStatement{
		Effect:   "Deny",
		Action:   emulator.StringOrSlice{"s3:PutObject"},
		Resource: emulator.StringOrSlice{"*"},
		Condition: map[string]map[string]emulator.StringOrSlice{
			"StringEquals": {"aws:RequestTag/env": {"prod"}},
		},
	}
	doc := s3AuthzPolicy([]string{"s3:PutObject", "s3:PutObjectTagging"}, denyProd)

	cases := []struct {
		name    string
		tagging string
		denied  bool
		why     string
	}{
		{name: "plain allowed tag", tagging: "env=dev", denied: false},
		{name: "denied tag", tagging: "env=prod", denied: true, why: "s3:PutObject"},
		{name: "semicolon in another value", tagging: "env=prod&owner=a;b", denied: true, why: "s3:PutObject"},
		{name: "not query-encoded", tagging: "%zz=1", denied: true, why: "could not be read"},
		{name: "duplicate key", tagging: "env=dev&env=prod", denied: true, why: "could not be read"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s3AuthzCheck(t, "tagparse", doc, &emulator.AWSRequest{
				Service: "s3", Operation: "PutObject", Path: "/b/k",
				Headers: map[string]string{"x-amz-tagging": tc.tagging},
			})
			if !tc.denied {
				assert.NoError(t, err)
				return
			}
			requireS3Denied(t, err, tc.why)
		})
	}
}

// TestS3Authz_UnreadableCreateBucketBodyFailsClosed asserts a CreateBucket body that does not parse
// is denied rather than evaluated as carrying no tags.
func TestS3Authz_UnreadableCreateBucketBodyFailsClosed(t *testing.T) {
	err := s3AuthzCheck(t, "badbody", s3AuthzPolicy([]string{"s3:*"}), &emulator.AWSRequest{
		Service: "s3", Operation: "CreateBucket", Path: "/bad-body",
		Headers: map[string]string{}, Body: []byte("not xml"),
	})
	requireS3Denied(t, err, "could not be read")
}

// TestS3Authz_DeleteBucketOwnershipControlsAction is review item 2: there is no
// s3:DeleteBucketOwnershipControls action; the operation is authorized as
// s3:PutBucketOwnershipControls, as API_DeleteBucketOwnershipControls states.
func TestS3Authz_DeleteBucketOwnershipControlsAction(t *testing.T) {
	req := func() *emulator.AWSRequest {
		return &emulator.AWSRequest{
			Service: "s3", Operation: "DeleteBucketOwnershipControls", Path: "/owned",
			Headers: map[string]string{}, Params: map[string]string{"ownershipControls": ""},
		}
	}
	assert.NoError(t, s3AuthzCheck(t, "putter", s3AuthzPolicy([]string{"s3:PutBucketOwnershipControls"}), req()))
	requireS3Denied(t,
		s3AuthzCheck(t, "deleter", s3AuthzPolicy([]string{"s3:DeleteBucketOwnershipControls"}), req()),
		"s3:PutBucketOwnershipControls")
}

// TestS3Authz_CreateSideEffectsNeedTheirOwnPermission is review item 6: a create that also tags, or
// sets ownership controls, needs the permission its page names for that effect, and a create
// applying none needs nothing extra.
func TestS3Authz_CreateSideEffectsNeedTheirOwnPermission(t *testing.T) {
	putObject := func(tagging string) *emulator.AWSRequest {
		h := map[string]string{}
		if tagging != "" {
			h["x-amz-tagging"] = tagging
		}
		return &emulator.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/b/k", Headers: h}
	}
	createBucket := func(body []byte, ownership string) *emulator.AWSRequest {
		h := map[string]string{}
		if ownership != "" {
			h["x-amz-object-ownership"] = ownership
		}
		return &emulator.AWSRequest{Service: "s3", Operation: "CreateBucket", Path: "/side-effects", Headers: h, Body: body}
	}
	tagged := createBucketBody("", [2]string{"team", "infra"})

	cases := []struct {
		name    string
		actions []string
		req     *emulator.AWSRequest
		missing string // empty when the request must be allowed
	}{
		{name: "PutObject untagged", actions: []string{"s3:PutObject"}, req: putObject("")},
		{name: "PutObject tagged without PutObjectTagging", actions: []string{"s3:PutObject"}, req: putObject("a=1"), missing: "s3:PutObjectTagging"},
		{name: "PutObject tagged with PutObjectTagging", actions: []string{"s3:PutObject", "s3:PutObjectTagging"}, req: putObject("a=1")},
		// The create itself is authorized as s3:CreateMultipartUpload here, a pre-existing mapping
		// (AWS authorizes it as s3:PutObject; #1489); the point is that nothing beyond it is required.
		{name: "CreateMultipartUpload tagged: no extra permission is published", actions: []string{"s3:PutObject", "s3:CreateMultipartUpload"},
			req: &emulator.AWSRequest{Service: "s3", Operation: "CreateMultipartUpload", Path: "/b/k",
				Headers: map[string]string{"x-amz-tagging": "a=1"}, Params: map[string]string{"uploads": ""}}},
		{name: "CreateBucket plain", actions: []string{"s3:CreateBucket"}, req: createBucket(nil, "")},
		{name: "CreateBucket tagged without TagResource", actions: []string{"s3:CreateBucket"}, req: createBucket(tagged, ""), missing: "s3:TagResource"},
		{name: "CreateBucket tagged with TagResource", actions: []string{"s3:CreateBucket", "s3:TagResource"}, req: createBucket(tagged, "")},
		{name: "CreateBucket ownership without PutBucketOwnershipControls", actions: []string{"s3:CreateBucket"}, req: createBucket(nil, "ObjectWriter"), missing: "s3:PutBucketOwnershipControls"},
		{name: "CreateBucket ownership with PutBucketOwnershipControls", actions: []string{"s3:CreateBucket", "s3:PutBucketOwnershipControls"}, req: createBucket(nil, "ObjectWriter")},
		{name: "CreateBucket both, ownership missing", actions: []string{"s3:CreateBucket", "s3:TagResource"}, req: createBucket(tagged, "ObjectWriter"), missing: "s3:PutBucketOwnershipControls"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s3AuthzCheck(t, "sidefx", s3AuthzPolicy(tc.actions), tc.req)
			if tc.missing == "" {
				assert.NoError(t, err)
				return
			}
			requireS3Denied(t, err, tc.missing)
		})
	}
}

// TestS3Authz_TagOnCreateSeesTheRequestTags asserts the second pass evaluates aws:RequestTag, so a
// tagging grant scoped to a tag value admits only that value.
func TestS3Authz_TagOnCreateSeesTheRequestTags(t *testing.T) {
	doc := emulator.PolicyDocument{
		Version: "2012-10-17",
		Statement: []emulator.PolicyStatement{
			{Effect: "Allow", Action: emulator.StringOrSlice{"s3:PutObject"}, Resource: emulator.StringOrSlice{"*"}},
			{
				Effect: "Allow", Action: emulator.StringOrSlice{"s3:PutObjectTagging"}, Resource: emulator.StringOrSlice{"*"},
				Condition: map[string]map[string]emulator.StringOrSlice{
					"StringEquals": {"aws:RequestTag/team": {"infra"}},
				},
			},
		},
	}
	put := func(tagging string) *emulator.AWSRequest {
		return &emulator.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/b/k",
			Headers: map[string]string{"x-amz-tagging": tagging}}
	}
	assert.NoError(t, s3AuthzCheck(t, "scoped", doc, put("team=infra")))
	requireS3Denied(t, s3AuthzCheck(t, "scoped", doc, put("team=web")), "s3:PutObjectTagging")
}
