package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// newAuthTestState builds a MemoryStateManager with a pre-created user and
// optionally an attached managed/custom policy.
func newAuthTestState(t *testing.T, userName, policyARN string, policyDoc substrate.PolicyDocument) substrate.StateManager {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	ctx := context.Background()

	user := substrate.IAMUser{
		UserName: userName,
		UserID:   "AIDATEST",
		ARN:      "arn:aws:iam::123456789012:user/" + userName,
		Path:     "/",
	}
	userRaw, _ := json.Marshal(user)
	require.NoError(t, state.Put(ctx, "iam", "user:"+userName, userRaw))

	if policyARN != "" {
		// Attach the policy to the user.
		arns := []string{policyARN}
		arnsRaw, _ := json.Marshal(arns)
		require.NoError(t, state.Put(ctx, "iam", "user_policies:"+userName, arnsRaw))

		// Store the policy document if it's a custom (non-managed) ARN.
		if len(policyDoc.Statement) > 0 {
			pol := substrate.IAMPolicy{
				PolicyName:       "testpolicy",
				PolicyID:         "ANPATEST",
				ARN:              policyARN,
				Path:             "/",
				DefaultVersionID: "v1",
				IsAttachable:     true,
				Document:         policyDoc,
			}
			polRaw, _ := json.Marshal(pol)
			require.NoError(t, state.Put(ctx, "iam", "policy:"+policyARN, polRaw))
		}
	}

	return state
}

func newAuthTestReqCtx(principal string) *substrate.RequestContext {
	return &substrate.RequestContext{
		RequestID: "req-test",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Principal: &substrate.Principal{ARN: principal, Type: "IAMUser"},
		Metadata:  make(map[string]interface{}),
	}
}

func TestAuthController_NilPrincipal_Bypass(t *testing.T) {
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)

	reqCtx := &substrate.RequestContext{AccountID: "123456789012", Metadata: make(map[string]interface{})}
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/bucket/key"}

	err := auth.CheckAccess(reqCtx, req)
	assert.NoError(t, err, "nil Principal should always pass")
}

func TestAuthController_Allow(t *testing.T) {
	allowAll := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{{
			Effect:   "Allow",
			Action:   substrate.StringOrSlice{"s3:*"},
			Resource: substrate.StringOrSlice{"*"},
		}},
	}
	policyARN := "arn:aws:iam::123456789012:policy/AllowAllS3"
	state := newAuthTestState(t, "alice", policyARN, allowAll)
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)

	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/alice")
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/my-bucket/key.txt"}

	err := auth.CheckAccess(reqCtx, req)
	assert.NoError(t, err)
}

func TestAuthController_Deny(t *testing.T) {
	denyAll := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{{
			Effect:   "Deny",
			Action:   substrate.StringOrSlice{"s3:*"},
			Resource: substrate.StringOrSlice{"*"},
		}},
	}
	policyARN := "arn:aws:iam::123456789012:policy/DenyAllS3"
	state := newAuthTestState(t, "bob", policyARN, denyAll)
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)

	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/bob")
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/bucket/key"}

	err := auth.CheckAccess(reqCtx, req)
	require.Error(t, err)
	var awsErr *substrate.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDeniedException", awsErr.Code)
	assert.Equal(t, 403, awsErr.HTTPStatus)
}

func TestAuthController_ImplicitDeny(t *testing.T) {
	// User exists but has no policies → implicit deny.
	state := newAuthTestState(t, "charlie", "", substrate.PolicyDocument{})
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)

	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/charlie")
	req := &substrate.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/bucket/key"}

	err := auth.CheckAccess(reqCtx, req)
	require.Error(t, err)
	var awsErr *substrate.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDeniedException", awsErr.Code)
}

func TestAuthController_CrossService_S3Deny(t *testing.T) {
	// User has IAM permissions but not S3 permissions.
	iamOnly := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{{
			Effect:   "Allow",
			Action:   substrate.StringOrSlice{"iam:*"},
			Resource: substrate.StringOrSlice{"*"},
		}},
	}
	policyARN := "arn:aws:iam::123456789012:policy/IAMOnly"
	state := newAuthTestState(t, "dave", policyARN, iamOnly)
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)

	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/dave")
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/bucket/key"}

	err := auth.CheckAccess(reqCtx, req)
	require.Error(t, err)
	var awsErr *substrate.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDeniedException", awsErr.Code)
}

func TestAuthController_InlinePolicy_Allow(t *testing.T) {
	// User has an inline policy (no managed policies).
	state := substrate.NewMemoryStateManager()
	ctx := context.Background()

	user := substrate.IAMUser{
		UserName: "eve",
		UserID:   "AIDATEST",
		ARN:      "arn:aws:iam::123456789012:user/eve",
		Path:     "/",
	}
	userRaw, _ := json.Marshal(user)
	require.NoError(t, state.Put(ctx, "iam", "user:eve", userRaw))

	// Inline policy document stored directly.
	inlineDoc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{{
			Effect:   "Allow",
			Action:   substrate.StringOrSlice{"s3:GetObject"},
			Resource: substrate.StringOrSlice{"*"},
		}},
	}
	docRaw, _ := json.Marshal(inlineDoc)
	require.NoError(t, state.Put(ctx, "iam", "user_inline:eve:ReadPolicy", docRaw))
	namesRaw, _ := json.Marshal([]string{"ReadPolicy"})
	require.NoError(t, state.Put(ctx, "iam", "user_inline_names:eve", namesRaw))

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)

	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/eve")
	req := &substrate.AWSRequest{Service: "s3", Operation: "GetObject", Path: "/bucket/key"}

	err := auth.CheckAccess(reqCtx, req)
	assert.NoError(t, err)
}

func TestAuthController_PermissionBoundary_Deny(t *testing.T) {
	// User has allow-all policy but a restrictive permission boundary.
	state := substrate.NewMemoryStateManager()
	ctx := context.Background()

	// The boundary policy denies s3:PutObject.
	boundaryARN := "arn:aws:iam::123456789012:policy/S3ReadOnlyBoundary"
	boundaryDoc := substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{{
			Effect:   "Allow",
			Action:   substrate.StringOrSlice{"s3:GetObject"},
			Resource: substrate.StringOrSlice{"*"},
		}},
	}
	boundary := substrate.IAMPolicy{
		PolicyName:       "S3ReadOnlyBoundary",
		PolicyID:         "ANPABOUND",
		ARN:              boundaryARN,
		Path:             "/",
		DefaultVersionID: "v1",
		IsAttachable:     true,
		Document:         boundaryDoc,
	}
	boundaryRaw, _ := json.Marshal(boundary)
	require.NoError(t, state.Put(ctx, "iam", "policy:"+boundaryARN, boundaryRaw))

	// User has allow-all managed policy and the boundary set.
	allowAllARN := "arn:aws:iam::aws:policy/AdministratorAccess"
	arnsRaw, _ := json.Marshal([]string{allowAllARN})

	boundaryRef := &substrate.IAMAttachedPolicy{PolicyARN: boundaryARN, PolicyName: "S3ReadOnlyBoundary"}
	user := substrate.IAMUser{
		UserName:            "frank",
		UserID:              "AIDAFRANK",
		ARN:                 "arn:aws:iam::123456789012:user/frank",
		Path:                "/",
		PermissionsBoundary: boundaryRef,
	}
	userRaw, _ := json.Marshal(user)
	require.NoError(t, state.Put(ctx, "iam", "user:frank", userRaw))
	require.NoError(t, state.Put(ctx, "iam", "user_policies:frank", arnsRaw))

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)

	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/frank")
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/bucket/key"}

	// PutObject is not in boundary's Allow → should be denied despite AdministratorAccess.
	err := auth.CheckAccess(reqCtx, req)
	require.Error(t, err)
	var awsErr *substrate.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDeniedException", awsErr.Code)
	assert.Contains(t, awsErr.Message, "permission boundary")
}

// ---- ABAC helpers --------------------------------------------------------

// newABACPolicy builds a PolicyDocument with a single Allow or Deny statement
// that includes a StringEquals condition.
func newABACPolicy(effect, action, resource, condKey, condVal string) substrate.PolicyDocument {
	return substrate.PolicyDocument{
		Version: "2012-10-17",
		Statement: []substrate.PolicyStatement{{
			Effect:   effect,
			Action:   substrate.StringOrSlice{action},
			Resource: substrate.StringOrSlice{resource},
			Condition: map[string]map[string]substrate.StringOrSlice{
				"StringEquals": {condKey: {condVal}},
			},
		}},
	}
}

// putS3Bucket stores a minimal S3Bucket in state so resource-tag lookups work.
func putS3BucketWithTags(t *testing.T, state substrate.StateManager, name string, tags map[string]string) {
	t.Helper()
	b := substrate.S3Bucket{Name: name, Region: "us-east-1", Tags: tags}
	raw, _ := json.Marshal(b)
	require.NoError(t, state.Put(context.Background(), "s3", "bucket:"+name, raw))
}

// ---- ABAC tests ----------------------------------------------------------

func TestABAC_ResourceTag_Allow(t *testing.T) {
	// Policy: allow s3:PutObject only when aws:ResourceTag/Env == "prod".
	policyDoc := newABACPolicy("Allow", "s3:PutObject", "*", "aws:ResourceTag/Env", "prod")
	policyARN := "arn:aws:iam::123456789012:policy/ProdOnly"
	state := newAuthTestState(t, "alice", policyARN, policyDoc)

	// Bucket tagged Env=prod → should be allowed.
	putS3BucketWithTags(t, state, "my-bucket", map[string]string{"Env": "prod"})

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/alice")
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/my-bucket/key"}

	err := auth.CheckAccess(reqCtx, req)
	assert.NoError(t, err)
}

func TestABAC_ResourceTag_Deny(t *testing.T) {
	// Policy: allow s3:PutObject only when aws:ResourceTag/Env == "prod".
	policyDoc := newABACPolicy("Allow", "s3:PutObject", "*", "aws:ResourceTag/Env", "prod")
	policyARN := "arn:aws:iam::123456789012:policy/ProdOnly"
	state := newAuthTestState(t, "bob", policyARN, policyDoc)

	// Bucket tagged Env=dev → condition fails → implicit deny.
	putS3BucketWithTags(t, state, "dev-bucket", map[string]string{"Env": "dev"})

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/bob")
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/dev-bucket/key"}

	err := auth.CheckAccess(reqCtx, req)
	require.Error(t, err)
	var awsErr *substrate.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDeniedException", awsErr.Code)
}

func TestABAC_ResourceTag_Missing(t *testing.T) {
	// Policy requires resource tag; bucket has no tags → condition not satisfied → deny.
	policyDoc := newABACPolicy("Allow", "s3:PutObject", "*", "aws:ResourceTag/Env", "prod")
	policyARN := "arn:aws:iam::123456789012:policy/ProdOnly"
	state := newAuthTestState(t, "carol", policyARN, policyDoc)

	// Bucket with no tags stored.
	putS3BucketWithTags(t, state, "untagged-bucket", nil)

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/carol")
	req := &substrate.AWSRequest{Service: "s3", Operation: "PutObject", Path: "/untagged-bucket/key"}

	err := auth.CheckAccess(reqCtx, req)
	require.Error(t, err)
	var awsErr *substrate.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDeniedException", awsErr.Code)
}

func TestABAC_RequestTag_Allow(t *testing.T) {
	// Policy: allow s3:PutObject only when aws:RequestTag/Env == "prod".
	policyDoc := newABACPolicy("Allow", "s3:PutObject", "*", "aws:RequestTag/Env", "prod")
	policyARN := "arn:aws:iam::123456789012:policy/ReqTagProd"
	state := newAuthTestState(t, "dave", policyARN, policyDoc)

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/dave")
	req := &substrate.AWSRequest{
		Service:   "s3",
		Operation: "PutObject",
		Path:      "/my-bucket/key",
		Headers:   map[string]string{"x-amz-tagging": "Env=prod"},
	}

	err := auth.CheckAccess(reqCtx, req)
	assert.NoError(t, err)
}

func TestABAC_RequestTag_Deny(t *testing.T) {
	// Policy requires aws:RequestTag/Env == "prod"; request carries Env=dev.
	policyDoc := newABACPolicy("Allow", "s3:PutObject", "*", "aws:RequestTag/Env", "prod")
	policyARN := "arn:aws:iam::123456789012:policy/ReqTagProd"
	state := newAuthTestState(t, "eve", policyARN, policyDoc)

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/eve")
	req := &substrate.AWSRequest{
		Service:   "s3",
		Operation: "PutObject",
		Path:      "/my-bucket/key",
		Headers:   map[string]string{"x-amz-tagging": "Env=dev"},
	}

	err := auth.CheckAccess(reqCtx, req)
	require.Error(t, err)
	var awsErr *substrate.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDeniedException", awsErr.Code)
}

func TestABAC_IAMRole_ResourceTag(t *testing.T) {
	// Policy: allow iam:PassRole only when aws:ResourceTag/Team == "infra".
	policyDoc := newABACPolicy("Allow", "iam:PassRole", "*", "aws:ResourceTag/Team", "infra")
	policyARN := "arn:aws:iam::123456789012:policy/InfraTeam"

	state := substrate.NewMemoryStateManager()
	ctx := context.Background()

	user := substrate.IAMUser{
		UserName: "frank",
		UserID:   "AIDAFRANK",
		ARN:      "arn:aws:iam::123456789012:user/frank",
		Path:     "/",
	}
	userRaw, _ := json.Marshal(user)
	require.NoError(t, state.Put(ctx, "iam", "user:frank", userRaw))
	arnsRaw, _ := json.Marshal([]string{policyARN})
	require.NoError(t, state.Put(ctx, "iam", "user_policies:frank", arnsRaw))

	pol := substrate.IAMPolicy{
		PolicyName:       "InfraTeam",
		PolicyID:         "ANPAINF",
		ARN:              policyARN,
		Path:             "/",
		DefaultVersionID: "v1",
		IsAttachable:     true,
		Document:         policyDoc,
	}
	polRaw, _ := json.Marshal(pol)
	require.NoError(t, state.Put(ctx, "iam", "policy:"+policyARN, polRaw))

	// Role tagged Team=infra.
	role := substrate.IAMRole{
		RoleName: "my-role",
		RoleID:   "AROATEST",
		ARN:      "arn:aws:iam::123456789012:role/my-role",
		Path:     "/",
		Tags:     []substrate.IAMTag{{Key: "Team", Value: "infra"}},
	}
	roleRaw, _ := json.Marshal(role)
	require.NoError(t, state.Put(ctx, "iam", "role:my-role", roleRaw))

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)

	// The principal is a user whose principal ARN maps to a user entity.
	// For the resource-tag lookup the auth controller uses the principal's entity.
	// We test that the role's tags flow into condCtx; to exercise addResourceTags
	// for IAM roles we need the principal to be a role.
	reqCtx := &substrate.RequestContext{
		RequestID: "req-role",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Principal: &substrate.Principal{ARN: "arn:aws:iam::123456789012:role/my-role", Type: "IAMRole"},
		Metadata:  make(map[string]interface{}),
	}
	// Attach the policy list to the role too.
	require.NoError(t, state.Put(ctx, "iam", "role_policies:my-role", arnsRaw))

	req := &substrate.AWSRequest{Service: "iam", Operation: "PassRole", Path: "/"}
	err := auth.CheckAccess(reqCtx, req)
	assert.NoError(t, err)
}

func TestABAC_EC2_ResourceTag(t *testing.T) {
	// Policy: allow ec2:TerminateInstances only when aws:ResourceTag/Env == "test".
	policyDoc := newABACPolicy("Allow", "ec2:TerminateInstances", "*", "aws:ResourceTag/Env", "test")
	policyARN := "arn:aws:iam::123456789012:policy/EC2Test"
	state := newAuthTestState(t, "grace", policyARN, policyDoc)

	// Store an EC2 instance tagged Env=test.
	inst := substrate.EC2Instance{
		InstanceID: "i-abc123",
		AccountID:  "123456789012",
		Region:     "us-east-1",
		Tags:       []substrate.EC2Tag{{Key: "Env", Value: "test"}},
	}
	instRaw, _ := json.Marshal(inst)
	require.NoError(t, state.Put(context.Background(), "ec2", "instance:123456789012/us-east-1/i-abc123", instRaw))

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/grace")
	req := &substrate.AWSRequest{
		Service:   "ec2",
		Operation: "TerminateInstances",
		Path:      "/",
		Params:    map[string]string{"InstanceId.1": "i-abc123"},
	}

	err := auth.CheckAccess(reqCtx, req)
	assert.NoError(t, err)
}

func TestABAC_Lambda_ResourceTag(t *testing.T) {
	policyDoc := newABACPolicy("Allow", "lambda:InvokeFunction", "*", "aws:ResourceTag/Env", "prod")
	policyARN := "arn:aws:iam::123456789012:policy/LambdaProd"
	state := newAuthTestState(t, "heidi", policyARN, policyDoc)

	fn := substrate.LambdaFunction{
		FunctionName: "prod-func",
		FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:prod-func",
		Tags:         map[string]string{"Env": "prod"},
	}
	fnRaw, _ := json.Marshal(fn)
	require.NoError(t, state.Put(context.Background(), "lambda", "function:prod-func", fnRaw))

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/heidi")
	req := &substrate.AWSRequest{
		Service:   "lambda",
		Operation: "InvokeFunction",
		Path:      "/2015-03-31/functions/prod-func/invocations",
	}
	assert.NoError(t, auth.CheckAccess(reqCtx, req))
}

func TestABAC_DynamoDB_ResourceTag(t *testing.T) {
	policyDoc := newABACPolicy("Allow", "dynamodb:PutItem", "*", "aws:ResourceTag/Tier", "premium")
	policyARN := "arn:aws:iam::123456789012:policy/DynamoPremium"
	state := newAuthTestState(t, "ivan", policyARN, policyDoc)

	tbl := substrate.DynamoDBTable{
		TableName:   "orders",
		TableARN:    "arn:aws:dynamodb:us-east-1:123456789012:table/orders",
		TableStatus: "ACTIVE",
		Tags:        map[string]string{"Tier": "premium"},
	}
	tblRaw, _ := json.Marshal(tbl)
	require.NoError(t, state.Put(context.Background(), "dynamodb", "table:123456789012/orders", tblRaw))

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/ivan")
	req := &substrate.AWSRequest{
		Service:   "dynamodb",
		Operation: "PutItem",
		Path:      "/",
		Params:    map[string]string{"TableName": "orders"},
	}
	assert.NoError(t, auth.CheckAccess(reqCtx, req))
}

func TestABAC_SQS_ResourceTag(t *testing.T) {
	policyDoc := newABACPolicy("Allow", "sqs:SendMessage", "*", "aws:ResourceTag/Team", "ops")
	policyARN := "arn:aws:iam::123456789012:policy/SQSOps"
	state := newAuthTestState(t, "judy", policyARN, policyDoc)

	q := substrate.SQSQueue{
		QueueName: "ops-queue",
		QueueURL:  "https://sqs.us-east-1.amazonaws.com/123456789012/ops-queue",
		QueueARN:  "arn:aws:sqs:us-east-1:123456789012:ops-queue",
		Tags:      map[string]string{"Team": "ops"},
	}
	qRaw, _ := json.Marshal(q)
	require.NoError(t, state.Put(context.Background(), "sqs", "queue:ops-queue", qRaw))

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/judy")
	req := &substrate.AWSRequest{
		Service:   "sqs",
		Operation: "SendMessage",
		Path:      "/",
		Params:    map[string]string{"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/ops-queue"},
	}
	assert.NoError(t, auth.CheckAccess(reqCtx, req))
}

func TestABAC_EC2_RequestTag(t *testing.T) {
	policyDoc := newABACPolicy("Allow", "ec2:RunInstances", "*", "aws:RequestTag/CostCenter", "eng")
	policyARN := "arn:aws:iam::123456789012:policy/EC2CostCenter"
	state := newAuthTestState(t, "karl", policyARN, policyDoc)

	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	auth := substrate.NewAuthController(state, logger)
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/karl")
	req := &substrate.AWSRequest{
		Service:   "ec2",
		Operation: "RunInstances",
		Path:      "/",
		Params: map[string]string{
			"TagSpecification.1.Tag.1.Key":   "CostCenter",
			"TagSpecification.1.Tag.1.Value": "eng",
		},
	}
	assert.NoError(t, auth.CheckAccess(reqCtx, req))
}
