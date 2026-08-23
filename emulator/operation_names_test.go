package emulator_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A REST service names its operation with nothing but the shape of its URL, so
// extractOperation's last-resort fallback named every such request after its HTTP
// method. That was inert until #411 made the evaluator reachable; then a policy
// granting lambda:InvokeFunction was refused, because the action evaluated was
// lambda:POST (#572). #480 had already fixed this for S3 alone.

// httpVerbs is the set a resolved operation name must not be a member of.
var httpVerbs = map[string]bool{
	http.MethodGet: true, http.MethodPut: true, http.MethodPost: true,
	http.MethodDelete: true, http.MethodHead: true, http.MethodPatch: true,
}

// parseWire runs a request through the wire parser the way the server does and
// returns the operation the pipeline sees.
func parseWire(t *testing.T, method, host, path, body string) *emulator.AWSRequest {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Host = host
	req, _, err := emulator.ParseAWSRequest(r)
	require.NoError(t, err)
	return req
}

func TestParseAWSRequest_ResolvesRESTOperationNames(t *testing.T) {
	// One representative operation per REST service. Each of these entered the
	// pipeline named "POST"/"PUT" before #572, so every row fails on the parent
	// commit — that is what makes this a gate rather than a description.
	tests := []struct {
		name, method, host, path, wantOp string
	}{
		{"lambda CreateFunction", "POST", "lambda.us-east-1.amazonaws.com",
			"/2015-03-31/functions", "CreateFunction"},
		{"lambda Invoke", "POST", "lambda.us-east-1.amazonaws.com",
			"/2015-03-31/functions/fn/invocations", "Invoke"},
		{"lambda DeleteFunction", "DELETE", "lambda.us-east-1.amazonaws.com",
			"/2015-03-31/functions/fn", "DeleteFunction"},
		{"apigateway CreateRestApi", "POST", "apigateway.us-east-1.amazonaws.com",
			"/restapis", "CreateRestApi"},
		{"apigateway UpdateRestApi", "PATCH", "apigateway.us-east-1.amazonaws.com",
			"/restapis/abc", "UpdateRestApi"},
		{"apigatewayv2 CreateApi", "POST", "apigateway.us-east-1.amazonaws.com",
			"/v2/apis", "CreateApi"},
		{"appsync CreateGraphqlApi", "POST", "appsync.us-east-1.amazonaws.com",
			"/v1/apis", "CreateGraphqlApi"},
		{"efs CreateFileSystem", "POST", "elasticfilesystem.us-east-1.amazonaws.com",
			"/2015-02-01/file-systems", "CreateFileSystem"},
		{"route53 CreateHostedZone", "POST", "route53.amazonaws.com",
			"/2013-04-01/hostedzone", "CreateHostedZone"},
		{"sesv2 CreateEmailIdentity", "POST", "email.us-east-1.amazonaws.com",
			"/v2/email/identities", "CreateEmailIdentity"},
		{"cloudfront CreateDistribution", "POST", "cloudfront.amazonaws.com",
			"/2020-05-31/distribution", "CreateDistribution"},
		{"msk CreateCluster", "POST", "kafka.us-east-1.amazonaws.com",
			"/v1/clusters", "CreateCluster"},
		{"omics StartRun", "POST", "omics.us-east-1.amazonaws.com",
			"/run", "StartRun"},
		{"scheduler CreateSchedule", "POST", "scheduler.us-east-1.amazonaws.com",
			"/schedules/nightly", "CreateSchedule"},
		{"emrserverless CreateApplication", "POST", "emr-serverless.us-east-1.amazonaws.com",
			"/applications", "CreateApplication"},
		{"bedrock-runtime InvokeModel", "POST", "bedrock-runtime.us-east-1.amazonaws.com",
			"/model/anthropic.claude-v2/invoke", "InvokeModel"},
		// S3 is the precedent (#480) and must keep working.
		{"s3 CreateBucket", "PUT", "s3.us-east-1.amazonaws.com",
			"/mybucket", "CreateBucket"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := parseWire(t, tt.method, tt.host, tt.path, "{}")
			assert.Equal(t, tt.wantOp, req.Operation,
				"the pipeline must see the API operation, not the HTTP verb")
			assert.Equal(t, tt.method, req.HTTPMethod,
				"the verb is preserved separately, because a REST resolver keys on verb and path")
		})
	}
}

func TestParseAWSRequest_LeavesUnresolvableOperationsAlone(t *testing.T) {
	// A verb is a more honest answer than a wrong operation name: an unroutable
	// path must reach the plugin and be refused there, not be authorized as
	// something else.
	tests := []struct {
		name, method, host, path string
	}{
		{"a lambda path no resolver claims", "POST",
			"lambda.us-east-1.amazonaws.com", "/2015-03-31/no-such-collection"},
		{"a service with no resolver at all", "POST",
			"unknownservice.us-east-1.amazonaws.com", "/whatever"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := parseWire(t, tt.method, tt.host, tt.path, "{}")
			assert.NotEmpty(t, req.Operation, "an unresolved operation is still named")
			// HTTPMethod is the verb of the request unconditionally, whether or not
			// the name resolved. resolveOperationName also backfills it, which makes
			// this incidentally true for the 18 services that have a resolver — so
			// assert it where nothing backfills, or the field's contract holds only
			// by accident and the next REST service added without a table entry
			// reaches its plugin with no verb at all.
			assert.Equal(t, tt.method, req.HTTPMethod,
				"the verb is recorded off the wire, not derived from resolution")
			if !httpVerbs[req.Operation] {
				// A resolver that answers UnknownOperation is also acceptable — what
				// must not happen is a *different* real operation's name.
				assert.NotContains(t, []string{"CreateFunction", "Invoke", "DeleteFunction"},
					req.Operation, "an unroutable path must not borrow another operation's name")
			}
		})
	}
}

func TestParseAWSRequest_OperationResolutionIsIdempotent(t *testing.T) {
	// The load-bearing property. Each plugin still resolves at the top of its own
	// HandleRequest, so resolution happens twice for every wire request; if the
	// second pass did not agree with the first, routing would break. That is why
	// the verb lives in HTTPMethod rather than being overwritten in Operation.
	tests := []struct {
		name, method, host, path string
	}{
		{"lambda", "POST", "lambda.us-east-1.amazonaws.com", "/2015-03-31/functions/fn/invocations"},
		{"apigateway", "POST", "apigateway.us-east-1.amazonaws.com", "/restapis"},
		{"appsync", "POST", "appsync.us-east-1.amazonaws.com", "/v1/apis"},
		{"route53", "POST", "route53.amazonaws.com", "/2013-04-01/hostedzone"},
		{"s3", "PUT", "s3.us-east-1.amazonaws.com", "/mybucket"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := parseWire(t, tt.method, tt.host, tt.path, "{}")
			first := req.Operation
			require.False(t, httpVerbs[first], "precondition: the name resolved")

			// Resolving again must be a no-op, however many times it runs.
			for range 3 {
				emulator.ResolveOperationNameForTest(req)
				assert.Equal(t, first, req.Operation, "resolution must be idempotent")
			}
		})
	}
}

func TestCheckAccess_EvaluatesTheRealActionName(t *testing.T) {
	// #572's consequence, and the reason it blocks the release: with the evaluator
	// now reachable, a scoped policy naming the real action was denied because the
	// action evaluated was <service>:POST. A wildcard policy masked it, so only a
	// scoped one measures anything.
	tests := []struct {
		name      string
		allow     []string
		method    string
		host      string
		path      string
		wantAllow bool
		wantInMsg string
	}{
		{
			// The headline case. Before #572 this was denied.
			name:      "a policy naming Invoke allows an invoke",
			allow:     []string{"lambda:InvokeFunction"},
			method:    "POST",
			host:      "lambda.us-east-1.amazonaws.com",
			path:      "/2015-03-31/functions/fn/invocations",
			wantAllow: true,
		},
		{
			// The denial must still happen, and must name the action a reader can
			// act on — a message saying "lambda:POST" tells them nothing to fix.
			name:      "a policy without Invoke denies it by its real name",
			allow:     []string{"lambda:GetFunction"},
			method:    "POST",
			host:      "lambda.us-east-1.amazonaws.com",
			path:      "/2015-03-31/functions/fn/invocations",
			wantAllow: false,
			wantInMsg: "lambda:InvokeFunction",
		},
		{
			// Two operations that share a verb and differ only by path must be
			// separately grantable, which is the whole point of resolving the name.
			name:      "CreateFunction and Invoke are distinct grants",
			allow:     []string{"lambda:CreateFunction"},
			method:    "POST",
			host:      "lambda.us-east-1.amazonaws.com",
			path:      "/2015-03-31/functions/fn/invocations",
			wantAllow: false,
			wantInMsg: "lambda:InvokeFunction",
		},
		{
			// API Gateway v1 is the exception that makes this service-specific: its
			// Service Authorization Reference defines apigateway:GET/POST/PUT/…
			// and no apigateway:CreateRestApi at all, because permissions map to the
			// HTTP method used against a management resource. So resolving the
			// operation name must not change the action here.
			name:      "an apigateway policy naming the verb allows it",
			allow:     []string{"apigateway:POST"},
			method:    "POST",
			host:      "apigateway.us-east-1.amazonaws.com",
			path:      "/restapis",
			wantAllow: true,
		},
		{
			name:      "an apigateway policy naming an operation does not grant it",
			allow:     []string{"apigateway:CreateRestApi"},
			method:    "POST",
			host:      "apigateway.us-east-1.amazonaws.com",
			path:      "/restapis",
			wantAllow: false,
			wantInMsg: "apigateway:POST",
		},
		{
			// apigatewayv2 is a different service prefix and does use operation
			// names, so the two must not be conflated.
			name:      "an apigatewayv2 policy naming CreateApi allows it",
			allow:     []string{"apigatewayv2:CreateApi"},
			method:    "POST",
			host:      "apigateway.us-east-1.amazonaws.com",
			path:      "/v2/apis",
			wantAllow: true,
		},
		{
			// S3's list operations authorize against the bucket, not the listing:
			// "You must have permission to perform the s3:ListBucket action".
			name:      "s3:ListBucket allows a ListObjectsV2",
			allow:     []string{"s3:ListBucket"},
			method:    "GET",
			host:      "mybucket.s3.us-east-1.amazonaws.com",
			path:      "/?list-type=2",
			wantAllow: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := emulator.NewMemoryStateManager()
			seedUserWithPolicy(t, state, "alice", tt.allow)

			auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(0, false))
			req := parseWire(t, tt.method, tt.host, tt.path, "{}")
			reqCtx := &emulator.RequestContext{
				AccountID: "123456789012",
				Region:    "us-east-1",
				Principal: &emulator.Principal{
					ARN:  "arn:aws:iam::123456789012:user/alice",
					Type: "User",
				},
			}

			err := auth.CheckAccess(reqCtx, req)
			if tt.wantAllow {
				assert.NoError(t, err, "a policy naming the real action must allow it")
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantInMsg,
				"the denial must name the action a caller has to grant")
		})
	}
}

// seedUserWithPolicy writes an IAM user and an attached policy allowing actions
// on every resource. Written straight to state because what is under test is the
// evaluation, not IAM's own request handling.
func seedUserWithPolicy(t *testing.T, state emulator.StateManager, user string, actions []string) {
	t.Helper()
	ctx := t.Context()

	userRaw, err := json.Marshal(emulator.IAMUser{
		UserName: user,
		ARN:      "arn:aws:iam::123456789012:user/" + user,
		Path:     "/",
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", emulator.IAMUserKeyForTest(authzTestAccount, user), userRaw))

	policyARN := "arn:aws:iam::123456789012:policy/" + user + "Policy"
	arnsRaw, err := json.Marshal([]string{policyARN})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", emulator.IAMAttachedPoliciesKeyForTest(authzTestAccount, "user", user), arnsRaw))

	polRaw, err := json.Marshal(emulator.IAMPolicy{
		PolicyName: user + "Policy",
		ARN:        policyARN,
		Document: emulator.PolicyDocument{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{{
				Effect:   "Allow",
				Action:   emulator.StringOrSlice(actions),
				Resource: emulator.StringOrSlice{"*"},
			}},
		},
	})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", emulator.IAMPolicyKeyForTest(policyARN), polRaw))
}

func TestFaultRule_CanNameARESTOperation(t *testing.T) {
	// #480's complaint, for the 17 services its S3-only fix did not reach: a rule
	// naming an operation matched nothing, because the request was named after its
	// HTTP method — while a rule naming "POST" took out every call to the service
	// at once.
	ts := emulator.StartTestServer(t)

	cfg := `{"enabled":true,"rules":[{"service":"lambda","operation":"CreateFunction",` +
		`"fault_type":"error","error_code":"ServiceException","http_status":500}]}`
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		ts.URL+"/v1/fault/rules", strings.NewReader(cfg))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// The operation the rule names must be hit...
	create := lambdaCall(t, ts, http.MethodPost, "/2015-03-31/functions",
		`{"FunctionName":"fn","Runtime":"python3.12","Handler":"h",`+
			`"Role":"arn:aws:iam::123456789012:role/r","Code":{"ZipFile":"eA=="}}`)
	assert.Equal(t, http.StatusInternalServerError, create,
		"a rule naming CreateFunction must match the create")

	// ...and an operation it does not name, on the same service, must not be. Before
	// #572 both were named "POST", so no rule could tell them apart.
	list := lambdaCall(t, ts, http.MethodGet, "/2015-03-31/functions", "")
	assert.NotEqual(t, http.StatusInternalServerError, list,
		"the rule must not take out every call to the service")
}

// lambdaCall issues a Lambda REST request with the Host header the parser needs
// to resolve the service, and returns the status code.
func lambdaCall(t *testing.T, ts *emulator.TestServer, method, path, body string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Host = "lambda.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode
}

func TestCFNDispatch_RecordsRealOperationNames(t *testing.T) {
	// A template's resource calls are built in-process, so they never pass through
	// ParseAWSRequest: a bucket was dispatched as "PUT" and a function as "POST".
	// That is what made a CloudFormation stack's calls unauthorizable against a
	// real policy — s3:PUT matches no grant a consumer would write — and it is why
	// the deployer resolves the name too.
	d, store := newS3TestDeployer(t)

	const template = `{"Resources":{"Data":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":"opname-bucket"}}}}`
	result, err := d.Deploy(t.Context(), template, "opnames", nil)
	require.NoError(t, err)
	require.Empty(t, findResource(t, result, "Data").Error)

	events, err := store.GetEvents(t.Context(), emulator.EventFilter{Service: "s3"})
	require.NoError(t, err)
	require.NotEmpty(t, events, "the deploy must have dispatched an S3 call")

	var names []string
	for _, ev := range events {
		if ev.Request != nil {
			names = append(names, ev.Request.Operation)
		}
	}
	assert.Contains(t, names, "CreateBucket",
		"a stack's bucket creation is recorded and authorized as CreateBucket, not PUT")
	assert.NotContains(t, names, "PUT",
		"no dispatched call may reach the pipeline named after its HTTP verb")
}

// newS3TestDeployer builds a StackDeployer with only the S3 plugin registered and
// an in-memory event store, which is the view of what a deploy actually dispatched.
func newS3TestDeployer(t *testing.T) (*emulator.StackDeployer, *emulator.EventStore) {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled: true, Backend: "memory", IncludeBodies: true,
	})
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})

	p := &emulator.S3Plugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)

	return emulator.NewStackDeployer(registry, store, state, tc, logger, costs), store
}
