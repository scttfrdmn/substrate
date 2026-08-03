package emulator_test

import (
	"context"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// cfnTestServer bundles the CloudFormation wire server with the state and
// registry behind it, so a test can assert both over the wire and in process.
type cfnTestServer struct {
	srv      *emulator.Server
	state    emulator.StateManager
	registry *emulator.PluginRegistry
	tc       *emulator.TimeController
}

// newCFNTestServer builds a server with the CloudFormation plugin registered
// alongside the S3 and IAM plugins.
//
// The S3 and IAM plugins are registered deliberately rather than for
// convenience: the point of #483's fix is that a stack created over the wire
// deploys into the same registry the server routes with, so a template's bucket
// has to be reachable through the S3 plugin afterwards. A CFN-only registry
// could not tell a shared world from a private one.
func newCFNTestServer(t *testing.T) *cfnTestServer {
	t.Helper()
	cfg := emulator.DefaultConfig()
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	registry := emulator.NewPluginRegistry()

	s3p := &emulator.S3Plugin{}
	require.NoError(t, s3p.Initialize(context.Background(), emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
			"registry":        registry,
		},
	}))
	registry.Register(s3p)

	iamp := &emulator.IAMPlugin{}
	require.NoError(t, iamp.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(iamp)

	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})

	cfnp := &emulator.CloudFormationPlugin{}
	require.NoError(t, cfnp.Initialize(context.Background(), emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"registry":        registry,
			"event_store":     store,
		},
	}))
	registry.Register(cfnp)

	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger,
		emulator.ServerOptions{Costs: emulator.NewCostController(emulator.CostConfig{Enabled: true})})

	return &cfnTestServer{srv: srv, state: state, registry: registry, tc: tc}
}

// cfnAuthHeader is a SigV4 Authorization header carrying a fake AKIA access
// key, which extractAccount maps to the well-known test account 123456789012.
const cfnAuthHeader = "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20260101/" +
	"us-east-1/cloudformation/aws4_request, SignedHeaders=host, Signature=fake"

// cfnRequest posts a CloudFormation query-protocol request and returns the
// status code and body.
func cfnRequest(t *testing.T, ts *cfnTestServer, params map[string]string) (int, string) {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	r := httptest.NewRequest(http.MethodPost, "http://cloudformation.us-east-1.amazonaws.com/",
		strings.NewReader(form.Encode()))
	r.Host = "cloudformation.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// Signing with an AKIA key resolves the request to the well-known test
	// account, which is what a signed CLI or SDK request carries; without it the
	// stack ARNs would name the zero account.
	r.Header.Set("Authorization", cfnAuthHeader)
	w := httptest.NewRecorder()
	ts.srv.ServeHTTP(w, r)
	body, err := io.ReadAll(w.Body)
	require.NoError(t, err)
	return w.Code, string(body)
}

// cfnAction posts an Action=<name> request with the supplied extra parameters.
func cfnAction(t *testing.T, ts *cfnTestServer, action string, params map[string]string) (int, string) {
	t.Helper()
	all := map[string]string{"Action": action, "Version": "2010-05-15"}
	for k, v := range params {
		all[k] = v
	}
	return cfnRequest(t, ts, all)
}

// cfnErrorCode extracts the error code from an <ErrorResponse> document.
func cfnErrorCode(t *testing.T, body string) string {
	t.Helper()
	var doc struct {
		Code string `xml:"Error>Code"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body was %s", body)
	return doc.Code
}

// The templates below are the smallest ones that exercise each path.
const (
	cfnEmptyTemplate  = `{"Resources":{}}`
	cfnBucketTemplate = `{"Resources":{"Data":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":"cfn-wire-bucket"}}},` +
		`"Outputs":{"BucketName":{"Value":{"Ref":"Data"}}}}`
	cfnBucketRoleTemplate = `{"Resources":{` +
		`"Data":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-shared-bucket"}},` +
		`"Runner":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"cfn-shared-role",` +
		`"AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[]}}}}}`
	cfnParamTemplate = `{"Parameters":{"BucketSuffix":{"Type":"String","Default":"default"}},` +
		`"Resources":{"Data":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":{"Fn::Sub":"cfn-param-${BucketSuffix}"}}}}}`
)

// TestCFNPlugin_CreateStack is #483's gate: the reporter's exact reproduction.
//
// Before the plugin existed, PluginRegistry.RouteRequest had no entry for
// "cloudformation" and answered ServiceNotAvailable with HTTP 501 for every
// CloudFormation call, even though the whole stack model was already
// implemented and reachable from Go.
func TestCFNPlugin_CreateStack(t *testing.T) {
	ts := newCFNTestServer(t)

	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "probe",
		"TemplateBody": cfnEmptyTemplate,
	})

	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.NotContains(t, body, "ServiceNotAvailable")

	var doc struct {
		StackID string `xml:"CreateStackResult>StackId"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc))
	assert.Contains(t, doc.StackID, "arn:aws:cloudformation:us-east-1:123456789012:stack/probe/")
}

// TestCFNPlugin_UnregisteredServiceStillRefused pins that the fix is a
// registration and not a change to the routing fallback: an unemulated service
// still reports ServiceNotAvailable.
func TestCFNPlugin_UnregisteredServiceStillRefused(t *testing.T) {
	ts := newCFNTestServer(t)

	r := httptest.NewRequest(http.MethodPost, "http://madeupservice.us-east-1.amazonaws.com/",
		strings.NewReader("Action=DoThing"))
	r.Host = "madeupservice.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	ts.srv.ServeHTTP(w, r)

	assert.Equal(t, http.StatusNotImplemented, w.Code)
	assert.Contains(t, w.Body.String(), "ServiceNotAvailable")
}

// TestCFNPlugin_StackSharesStateWithOtherPlugins is the assertion that proves
// the adapter deploys into the same emulator rather than a private world: the
// bucket a template declares is served by the S3 plugin over the wire, and the
// role by the IAM plugin.
func TestCFNPlugin_StackSharesStateWithOtherPlugins(t *testing.T) {
	ts := newCFNTestServer(t)

	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "shared",
		"TemplateBody": cfnBucketRoleTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	// DescribeStacks reports the stack complete with both resources.
	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "shared"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<StackStatus>CREATE_COMPLETE</StackStatus>")

	code, body = cfnAction(t, ts, "DescribeStackResources", map[string]string{"StackName": "shared"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "cfn-shared-bucket")
	assert.Contains(t, body, "cfn-shared-role")
	assert.Contains(t, body, "AWS::S3::Bucket")
	assert.Contains(t, body, "AWS::IAM::Role")

	// The bucket is reachable through the S3 plugin over the wire.
	head := httptest.NewRequest(http.MethodHead, "http://s3.amazonaws.com/cfn-shared-bucket", nil)
	head.Host = "s3.amazonaws.com"
	hw := httptest.NewRecorder()
	ts.srv.ServeHTTP(hw, head)
	assert.Equal(t, http.StatusOK, hw.Code, "the stack's bucket should be a real S3 bucket")

	// And the role through the IAM plugin.
	form := url.Values{"Action": {"GetRole"}, "RoleName": {"cfn-shared-role"}, "Version": {"2010-05-08"}}
	ir := httptest.NewRequest(http.MethodPost, "http://iam.amazonaws.com/", strings.NewReader(form.Encode()))
	ir.Host = "iam.amazonaws.com"
	ir.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	iw := httptest.NewRecorder()
	ts.srv.ServeHTTP(iw, ir)
	assert.Equal(t, http.StatusOK, iw.Code, "the stack's role should be a real IAM role")
	assert.Contains(t, iw.Body.String(), "cfn-shared-role")
}

// TestCFNPlugin_WireStackVisibleInProcess asserts the wire and the in-process
// StackDeployer see one set of stacks, which is the property that makes the
// adapter an adapter.
func TestCFNPlugin_WireStackVisibleInProcess(t *testing.T) {
	ts := newCFNTestServer(t)

	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "crossover",
		"TemplateBody": cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	deployer := emulator.NewStackDeployer(ts.registry, nil, ts.state, ts.tc,
		emulator.NewDefaultLogger(slog.LevelError, false),
		emulator.NewCostController(emulator.CostConfig{Enabled: false}))
	stacks, err := deployer.ListStacks(context.Background())
	require.NoError(t, err)

	require.Len(t, stacks, 1)
	assert.Equal(t, "crossover", stacks[0].StackName)
	assert.Equal(t, "CREATE_COMPLETE", stacks[0].Status)
	assert.Equal(t, cfnBucketTemplate, stacks[0].TemplateBody)
	require.Len(t, stacks[0].Resources, 1)
	assert.Equal(t, "cfn-wire-bucket", stacks[0].Resources[0].PhysicalID)
}

// TestCFNPlugin_DescribeStacks covers the listing and the not-found code.
func TestCFNPlugin_DescribeStacks(t *testing.T) {
	ts := newCFNTestServer(t)
	for _, name := range []string{"alpha", "beta"} {
		code, body := cfnAction(t, ts, "CreateStack", map[string]string{
			"StackName":    name,
			"TemplateBody": cfnEmptyTemplate,
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
	}

	t.Run("no name lists all", func(t *testing.T) {
		code, body := cfnAction(t, ts, "DescribeStacks", nil)
		require.Equal(t, http.StatusOK, code, "body was %s", body)

		var doc struct {
			Names []string `xml:"DescribeStacksResult>Stacks>member>StackName"`
		}
		require.NoError(t, xml.Unmarshal([]byte(body), &doc))
		assert.ElementsMatch(t, []string{"alpha", "beta"}, doc.Names)
	})

	t.Run("unknown name is a ValidationError", func(t *testing.T) {
		// DescribeStacks documents this explicitly: "If the stack does not exist,
		// a ValidationError is returned."
		code, body := cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "absent"})
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
	})

	t.Run("missing StackName on CreateStack", func(t *testing.T) {
		code, body := cfnAction(t, ts, "CreateStack", map[string]string{"TemplateBody": cfnEmptyTemplate})
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "MissingParameter", cfnErrorCode(t, body))
	})
}

// TestCFNPlugin_CreateStack_DuplicateName pins the documented duplicate-name
// code, which is AlreadyExists at 400 — not AlreadyExistsException.
func TestCFNPlugin_CreateStack_DuplicateName(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "twice",
		"TemplateBody": cfnEmptyTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "twice",
		"TemplateBody": cfnEmptyTemplate,
	})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "AlreadyExists", cfnErrorCode(t, body))
}

// TestCFNPlugin_CreateStack_UnparseableTemplate asserts a template that will not
// parse is a ValidationError rather than an InternalFailure.
func TestCFNPlugin_CreateStack_UnparseableTemplate(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "broken",
		"TemplateBody": `{"Resources": [this is not a template`,
	})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
}

// TestCFNPlugin_CreateStack_TemplateURLRefused asserts TemplateURL is refused
// rather than silently ignored: fetching a template is a network read substrate
// does not perform, and a caller who passed a URL got no resources at all.
func TestCFNPlugin_CreateStack_TemplateURLRefused(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":   "remote",
		"TemplateURL": "https://example.invalid/template.json",
	})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
	assert.Contains(t, body, "TemplateBody")
}

// TestCFNPlugin_ResourceFailureReported asserts a resource that failed to deploy
// is reported CREATE_FAILED with its reason, not CREATE_COMPLETE.
//
// The path is reachable because every deploy helper records the routing error on
// DeployedResource.Error and then returns a nil error, so a stack completes with
// a broken resource inside it. A stack reporting every resource complete
// regardless is the failure mode this pins: a consumer asserting on a resource's
// status would be told a queue exists that does not.
func TestCFNPlugin_ResourceFailureReported(t *testing.T) {
	ts := newCFNTestServer(t)

	// This registry holds S3, IAM and CloudFormation but not SQS, so the queue's
	// dispatch comes back ServiceNotAvailable while the bucket beside it deploys.
	require.NotContains(t, ts.registry.Names(), "sqs")
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName": "half-broken",
		"TemplateBody": `{"Resources":{` +
			`"Good":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-good-bucket"}},` +
			`"Bad":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"cfn-doomed-queue"}}}}`,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "DescribeStackResources", map[string]string{"StackName": "half-broken"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	var doc struct {
		Resources []struct {
			LogicalID string `xml:"LogicalResourceId"`
			Status    string `xml:"ResourceStatus"`
			Reason    string `xml:"ResourceStatusReason"`
		} `xml:"DescribeStackResourcesResult>StackResources>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc))
	require.Len(t, doc.Resources, 2)

	byLogical := map[string]struct {
		LogicalID string `xml:"LogicalResourceId"`
		Status    string `xml:"ResourceStatus"`
		Reason    string `xml:"ResourceStatusReason"`
	}{}
	for _, r := range doc.Resources {
		byLogical[r.LogicalID] = r
	}
	assert.Equal(t, "CREATE_FAILED", byLogical["Bad"].Status)
	assert.NotEmpty(t, byLogical["Bad"].Reason, "a failure must carry its reason")
	assert.Equal(t, "CREATE_COMPLETE", byLogical["Good"].Status)
	assert.Empty(t, byLogical["Good"].Reason)
}

// TestCFNPlugin_GetTemplate_ChangeSet asserts GetTemplate reads the change set's
// template when ChangeSetName is supplied and the stack's otherwise, so the two
// arms cannot be confused for one another.
func TestCFNPlugin_GetTemplate_ChangeSet(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "two-templates",
		"TemplateBody": cfnEmptyTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	code, body = cfnAction(t, ts, "CreateChangeSet", map[string]string{
		"StackName":     "two-templates",
		"ChangeSetName": "pending",
		"TemplateBody":  cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	var doc struct {
		TemplateBody string `xml:"GetTemplateResult>TemplateBody"`
	}

	// The stack still reports the deployed template, not the pending one.
	code, body = cfnAction(t, ts, "GetTemplate", map[string]string{"StackName": "two-templates"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	require.NoError(t, xml.Unmarshal([]byte(body), &doc))
	assert.Equal(t, cfnEmptyTemplate, doc.TemplateBody)

	// Naming the change set reports the pending template.
	code, body = cfnAction(t, ts, "GetTemplate", map[string]string{
		"StackName":     "two-templates",
		"ChangeSetName": "pending",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	require.NoError(t, xml.Unmarshal([]byte(body), &doc))
	assert.Equal(t, cfnBucketTemplate, doc.TemplateBody)
}

// TestCFNPlugin_Parameters_ManyMembers asserts the Parameters.member.N walk does
// not stop after the first entry. A one-parameter test passes under an off-by-one
// that drops every later member, which is why this uses four.
func TestCFNPlugin_Parameters_ManyMembers(t *testing.T) {
	ts := newCFNTestServer(t)
	tmpl := `{"Parameters":{` +
		`"A":{"Type":"String","Default":"da"},"B":{"Type":"String","Default":"db"},` +
		`"C":{"Type":"String","Default":"dc"},"D":{"Type":"String","Default":"dd"}},` +
		`"Resources":{"Data":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":{"Fn::Sub":"cfn-${A}-${B}-${C}-${D}"}}}}}`

	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":                          "four-params",
		"TemplateBody":                       tmpl,
		"Parameters.member.1.ParameterKey":   "A",
		"Parameters.member.1.ParameterValue": "sa",
		"Parameters.member.2.ParameterKey":   "B",
		"Parameters.member.2.ParameterValue": "sb",
		"Parameters.member.3.ParameterKey":   "C",
		"Parameters.member.3.ParameterValue": "sc",
		"Parameters.member.4.ParameterKey":   "D",
		"Parameters.member.4.ParameterValue": "sd",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	head := httptest.NewRequest(http.MethodHead, "http://s3.amazonaws.com/cfn-sa-sb-sc-sd", nil)
	head.Host = "s3.amazonaws.com"
	hw := httptest.NewRecorder()
	ts.srv.ServeHTTP(hw, head)
	assert.Equal(t, http.StatusOK, hw.Code, "every supplied parameter should have been read")

	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "four-params"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	var doc struct {
		Params []struct {
			Key   string `xml:"ParameterKey"`
			Value string `xml:"ParameterValue"`
		} `xml:"DescribeStacksResult>Stacks>member>Parameters>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc))
	require.Len(t, doc.Params, 4)
	assert.Equal(t, "sd", doc.Params[3].Value)
}

// TestCFNPlugin_ListStacks covers the summary shape and the status filter.
func TestCFNPlugin_ListStacks(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "listed",
		"TemplateBody": cfnEmptyTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "ListStacks", nil)
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	var doc struct {
		Summaries []struct {
			StackName    string `xml:"StackName"`
			StackStatus  string `xml:"StackStatus"`
			StackID      string `xml:"StackId"`
			CreationTime string `xml:"CreationTime"`
		} `xml:"ListStacksResult>StackSummaries>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc))
	require.Len(t, doc.Summaries, 1)
	assert.Equal(t, "listed", doc.Summaries[0].StackName)
	assert.Equal(t, "CREATE_COMPLETE", doc.Summaries[0].StackStatus)
	assert.Equal(t, "2026-01-01T12:00:00Z", doc.Summaries[0].CreationTime)

	t.Run("status filter selects", func(t *testing.T) {
		code, body := cfnAction(t, ts, "ListStacks", map[string]string{
			"StackStatusFilter.member.1": "CREATE_COMPLETE",
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.Contains(t, body, "<StackName>listed</StackName>")
	})

	t.Run("status filter excludes", func(t *testing.T) {
		code, body := cfnAction(t, ts, "ListStacks", map[string]string{
			"StackStatusFilter.member.1": "DELETE_COMPLETE",
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.NotContains(t, body, "<StackName>listed</StackName>")
	})
}

// TestCFNPlugin_GetTemplate asserts the body round-trips byte-for-byte.
func TestCFNPlugin_GetTemplate(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "templated",
		"TemplateBody": cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "GetTemplate", map[string]string{"StackName": "templated"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	var doc struct {
		TemplateBody string   `xml:"GetTemplateResult>TemplateBody"`
		Stages       []string `xml:"GetTemplateResult>StagesAvailable>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc))
	assert.Equal(t, cfnBucketTemplate, doc.TemplateBody)
	assert.Equal(t, []string{"Original"}, doc.Stages)

	t.Run("unknown stack", func(t *testing.T) {
		code, body := cfnAction(t, ts, "GetTemplate", map[string]string{"StackName": "absent"})
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
	})
}

// TestCFNPlugin_DeleteStack asserts the stack stops being reported, and that
// deleting an absent stack succeeds — DeleteStack documents no not-found error.
func TestCFNPlugin_DeleteStack(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "doomed",
		"TemplateBody": cfnEmptyTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "DeleteStack", map[string]string{"StackName": "doomed"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "doomed"})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ValidationError", cfnErrorCode(t, body))

	code, body = cfnAction(t, ts, "DeleteStack", map[string]string{"StackName": "never-existed"})
	assert.Equal(t, http.StatusOK, code, "body was %s", body)
}

// TestCFNPlugin_DeleteStackSweepsItsResources is the wire half of #518: the
// resource goes with the stack, and a resource that cannot be deleted leaves the
// stack visible in DELETE_FAILED while the call itself still answers 200.
//
// The 200 is the point of the second half. Real DeleteStack "returns success" and
// the stack reaches DELETE_FAILED asynchronously, which a caller learns by polling
// DescribeStacks — so raising the failure on the call would both break a caller
// following AWS semantics and, being a 500, make an SDK retry the delete and sweep
// a stack already in DELETE_FAILED.
func TestCFNPlugin_DeleteStackSweepsItsResources(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "swept",
		"TemplateBody": cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	require.Equal(t, http.StatusOK, cfnHeadBucket(t, ts, "cfn-wire-bucket"),
		"the stack's bucket exists before the delete")

	code, body = cfnAction(t, ts, "DeleteStack", map[string]string{"StackName": "swept"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Equal(t, http.StatusNotFound, cfnHeadBucket(t, ts, "cfn-wire-bucket"),
		"the bucket is deleted with its stack (#518)")

	t.Run("a refused delete answers 200 and reports DELETE_FAILED", func(t *testing.T) {
		code, body := cfnAction(t, ts, "CreateStack", map[string]string{
			"StackName":    "stuck",
			"TemplateBody": cfnBucketTemplate,
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)

		// A bucket holding an object the stack did not create is refused with
		// BucketNotEmpty, which is a real refusal rather than an injected one.
		put := httptest.NewRequest(http.MethodPut,
			"http://s3.amazonaws.com/cfn-wire-bucket/keeper.txt", strings.NewReader("x"))
		put.Host = "s3.amazonaws.com"
		pw := httptest.NewRecorder()
		ts.srv.ServeHTTP(pw, put)
		require.Equal(t, http.StatusOK, pw.Code)

		code, body = cfnAction(t, ts, "DeleteStack", map[string]string{"StackName": "stuck"})
		require.Equal(t, http.StatusOK, code, "body was %s", body)

		code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "stuck"})
		require.Equal(t, http.StatusOK, code, "the stack stays visible so a caller can retry")
		assert.Contains(t, body, "<StackStatus>DELETE_FAILED</StackStatus>")
		assert.Equal(t, http.StatusOK, cfnHeadBucket(t, ts, "cfn-wire-bucket"),
			"the bucket the sweep could not delete is still there")
	})
}

// cfnHeadBucket reports the status of a HEAD against a bucket, which is how a test
// observes whether a swept resource is really gone. It goes through the same server
// the stack deployed into: a bucket read from a registry of its own would be a
// different bucket.
func cfnHeadBucket(t *testing.T, ts *cfnTestServer, bucket string) int {
	t.Helper()
	r := httptest.NewRequest(http.MethodHead, "http://s3.amazonaws.com/"+bucket, nil)
	r.Host = "s3.amazonaws.com"
	w := httptest.NewRecorder()
	ts.srv.ServeHTTP(w, r)
	return w.Code
}

// TestCFNPlugin_UpdateStack asserts the status moves to UPDATE_COMPLETE and that
// the new template's resources are deployed.
func TestCFNPlugin_UpdateStack(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "evolving",
		"TemplateBody": cfnEmptyTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "UpdateStack", map[string]string{
		"StackName":    "evolving",
		"TemplateBody": cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "evolving"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<StackStatus>UPDATE_COMPLETE</StackStatus>")

	head := httptest.NewRequest(http.MethodHead, "http://s3.amazonaws.com/cfn-wire-bucket", nil)
	head.Host = "s3.amazonaws.com"
	hw := httptest.NewRecorder()
	ts.srv.ServeHTTP(hw, head)
	assert.Equal(t, http.StatusOK, hw.Code)

	t.Run("unknown stack", func(t *testing.T) {
		code, body := cfnAction(t, ts, "UpdateStack", map[string]string{
			"StackName":    "absent",
			"TemplateBody": cfnEmptyTemplate,
		})
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
	})

	// An update carrying no template reuses the deployed one, which is what
	// UsePreviousTemplate means: a parameter-only change must not blank the stack.
	t.Run("no template reuses the previous one", func(t *testing.T) {
		code, body := cfnAction(t, ts, "UpdateStack", map[string]string{"StackName": "evolving"})
		require.Equal(t, http.StatusOK, code, "body was %s", body)

		code, body = cfnAction(t, ts, "GetTemplate", map[string]string{"StackName": "evolving"})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		var doc struct {
			TemplateBody string `xml:"GetTemplateResult>TemplateBody"`
		}
		require.NoError(t, xml.Unmarshal([]byte(body), &doc))
		assert.Equal(t, cfnBucketTemplate, doc.TemplateBody)
	})
}

// TestCFNPlugin_Parameters asserts Parameters.member.N reaches template parameter
// resolution, which is the encoding every SDK sends the parameter list in.
func TestCFNPlugin_Parameters(t *testing.T) {
	ts := newCFNTestServer(t)

	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":                          "parameterized",
		"TemplateBody":                       cfnParamTemplate,
		"Parameters.member.1.ParameterKey":   "BucketSuffix",
		"Parameters.member.1.ParameterValue": "supplied",
		"Parameters.member.2.ParameterKey":   "Unused",
		"Parameters.member.2.ParameterValue": "ignored",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	// The supplied value, not the template default, decided the bucket name.
	head := httptest.NewRequest(http.MethodHead, "http://s3.amazonaws.com/cfn-param-supplied", nil)
	head.Host = "s3.amazonaws.com"
	hw := httptest.NewRecorder()
	ts.srv.ServeHTTP(hw, head)
	require.Equal(t, http.StatusOK, hw.Code, "the parameter should have reached Fn::Sub")

	// And DescribeStacks reports the resolved parameters.
	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "parameterized"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<ParameterKey>BucketSuffix</ParameterKey>")
	assert.Contains(t, body, "<ParameterValue>supplied</ParameterValue>")
}

// TestCFNPlugin_Parameters_UsePreviousValue asserts a member marked
// UsePreviousValue keeps the deployed value instead of overwriting it with the
// empty string the request carries.
func TestCFNPlugin_Parameters_UsePreviousValue(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":                          "previous",
		"TemplateBody":                       cfnParamTemplate,
		"Parameters.member.1.ParameterKey":   "BucketSuffix",
		"Parameters.member.1.ParameterValue": "original",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "UpdateStack", map[string]string{
		"StackName":                            "previous",
		"TemplateBody":                         cfnParamTemplate,
		"Parameters.member.1.ParameterKey":     "BucketSuffix",
		"Parameters.member.1.UsePreviousValue": "true",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "previous"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<ParameterValue>original</ParameterValue>")
}

// TestCFNPlugin_ChangeSetFlow walks the change-set family over the wire and
// asserts the change is visible only after the set executes.
func TestCFNPlugin_ChangeSetFlow(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "staged",
		"TemplateBody": cfnEmptyTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "CreateChangeSet", map[string]string{
		"StackName":     "staged",
		"ChangeSetName": "add-bucket",
		"TemplateBody":  cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	var created struct {
		ID      string `xml:"CreateChangeSetResult>Id"`
		StackID string `xml:"CreateChangeSetResult>StackId"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &created))
	assert.Contains(t, created.ID, ":changeSet/add-bucket/")

	// The change set describes the pending Add without having applied it.
	code, body = cfnAction(t, ts, "DescribeChangeSet", map[string]string{
		"StackName":     "staged",
		"ChangeSetName": "add-bucket",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	var describe struct {
		Status          string `xml:"DescribeChangeSetResult>Status"`
		ExecutionStatus string `xml:"DescribeChangeSetResult>ExecutionStatus"`
		Changes         []struct {
			Type      string `xml:"Type"`
			Action    string `xml:"ResourceChange>Action"`
			LogicalID string `xml:"ResourceChange>LogicalResourceId"`
			Type2     string `xml:"ResourceChange>ResourceType"`
		} `xml:"DescribeChangeSetResult>Changes>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &describe))
	assert.Equal(t, "CREATE_COMPLETE", describe.Status)
	assert.Equal(t, "AVAILABLE", describe.ExecutionStatus)
	require.Len(t, describe.Changes, 1)
	assert.Equal(t, "Resource", describe.Changes[0].Type)
	assert.Equal(t, "Add", describe.Changes[0].Action)
	assert.Equal(t, "Data", describe.Changes[0].LogicalID)
	assert.Equal(t, "AWS::S3::Bucket", describe.Changes[0].Type2)

	head := httptest.NewRequest(http.MethodHead, "http://s3.amazonaws.com/cfn-wire-bucket", nil)
	head.Host = "s3.amazonaws.com"
	hw := httptest.NewRecorder()
	ts.srv.ServeHTTP(hw, head)
	require.Equal(t, http.StatusNotFound, hw.Code, "a described change set must not have been applied")

	// ListChangeSets reports it while it is pending.
	code, body = cfnAction(t, ts, "ListChangeSets", map[string]string{"StackName": "staged"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<ChangeSetName>add-bucket</ChangeSetName>")

	// Executing it applies the change and consumes the set.
	code, body = cfnAction(t, ts, "ExecuteChangeSet", map[string]string{
		"StackName":     "staged",
		"ChangeSetName": "add-bucket",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	hw = httptest.NewRecorder()
	head = httptest.NewRequest(http.MethodHead, "http://s3.amazonaws.com/cfn-wire-bucket", nil)
	head.Host = "s3.amazonaws.com"
	ts.srv.ServeHTTP(hw, head)
	assert.Equal(t, http.StatusOK, hw.Code, "the executed change should be visible")

	code, body = cfnAction(t, ts, "DescribeChangeSet", map[string]string{
		"StackName":     "staged",
		"ChangeSetName": "add-bucket",
	})
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "ChangeSetNotFound", cfnErrorCode(t, body), "body was %s", body)
}

// TestCFNPlugin_ChangeSetErrors covers the codes and their statuses. The
// change-set family reports 404 for a missing set, unlike the stack family's 400.
func TestCFNPlugin_ChangeSetErrors(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "cs-errors",
		"TemplateBody": cfnEmptyTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	tests := []struct {
		name       string
		action     string
		params     map[string]string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "CreateChangeSet without ChangeSetName",
			action:     "CreateChangeSet",
			params:     map[string]string{"StackName": "cs-errors", "TemplateBody": cfnEmptyTemplate},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name:       "CreateChangeSet on an unknown stack",
			action:     "CreateChangeSet",
			params:     map[string]string{"StackName": "absent", "ChangeSetName": "cs", "TemplateBody": cfnEmptyTemplate},
			wantStatus: http.StatusBadRequest,
			wantCode:   "ValidationError",
		},
		{
			name:       "CreateChangeSet with ChangeSetType CREATE",
			action:     "CreateChangeSet",
			params:     map[string]string{"StackName": "cs-errors", "ChangeSetName": "cs", "TemplateBody": cfnEmptyTemplate, "ChangeSetType": "CREATE"},
			wantStatus: http.StatusBadRequest,
			wantCode:   "ValidationError",
		},
		{
			name:       "DescribeChangeSet on an unknown set",
			action:     "DescribeChangeSet",
			params:     map[string]string{"StackName": "cs-errors", "ChangeSetName": "absent"},
			wantStatus: http.StatusNotFound,
			wantCode:   "ChangeSetNotFound",
		},
		{
			name:       "ExecuteChangeSet on an unknown set",
			action:     "ExecuteChangeSet",
			params:     map[string]string{"StackName": "cs-errors", "ChangeSetName": "absent"},
			wantStatus: http.StatusNotFound,
			wantCode:   "ChangeSetNotFound",
		},
		{
			name:       "ListChangeSets requires StackName",
			action:     "ListChangeSets",
			params:     nil,
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, body := cfnAction(t, ts, tc.action, tc.params)
			assert.Equal(t, tc.wantStatus, code, "body was %s", body)
			assert.Equal(t, tc.wantCode, cfnErrorCode(t, body))
		})
	}
}

// TestCFNPlugin_DeleteChangeSet asserts a set can be discarded unexecuted, and
// that deleting an absent one succeeds — DeleteChangeSet documents no
// not-found error.
func TestCFNPlugin_DeleteChangeSet(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "discard",
		"TemplateBody": cfnEmptyTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	code, body = cfnAction(t, ts, "CreateChangeSet", map[string]string{
		"StackName":     "discard",
		"ChangeSetName": "unwanted",
		"TemplateBody":  cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "DeleteChangeSet", map[string]string{
		"StackName":     "discard",
		"ChangeSetName": "unwanted",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "ListChangeSets", map[string]string{"StackName": "discard"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.NotContains(t, body, "unwanted")

	code, body = cfnAction(t, ts, "DeleteChangeSet", map[string]string{
		"StackName":     "discard",
		"ChangeSetName": "never-existed",
	})
	assert.Equal(t, http.StatusOK, code, "body was %s", body)
}

// TestCFNPlugin_ChangeSetARNAccepted asserts a change-set ARN works where a bare
// name does, and with no StackName — the form DescribeChangeSet documents and
// the one the CLI echoes back from CreateChangeSet.
func TestCFNPlugin_ChangeSetARNAccepted(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "arn-addressed",
		"TemplateBody": cfnEmptyTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	code, body = cfnAction(t, ts, "CreateChangeSet", map[string]string{
		"StackName":     "arn-addressed",
		"ChangeSetName": "by-arn",
		"TemplateBody":  cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	var created struct {
		ID string `xml:"CreateChangeSetResult>Id"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &created))
	require.NotEmpty(t, created.ID)

	code, body = cfnAction(t, ts, "DescribeChangeSet", map[string]string{"ChangeSetName": created.ID})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<ChangeSetName>by-arn</ChangeSetName>")
	assert.Contains(t, body, "<StackName>arn-addressed</StackName>")
}

// TestCFNPlugin_DriftFlow walks the drift family over the wire. A resource
// deleted outside CloudFormation is the drift substrate models.
func TestCFNPlugin_DriftFlow(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "drifty",
		"TemplateBody": cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	t.Run("in sync before any change", func(t *testing.T) {
		code, body := cfnAction(t, ts, "DetectStackDrift", map[string]string{"StackName": "drifty"})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		var doc struct {
			ID string `xml:"DetectStackDriftResult>StackDriftDetectionId"`
		}
		require.NoError(t, xml.Unmarshal([]byte(body), &doc))
		require.NotEmpty(t, doc.ID)

		code, body = cfnAction(t, ts, "DescribeStackDriftDetectionStatus",
			map[string]string{"StackDriftDetectionId": doc.ID})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.Contains(t, body, "<DetectionStatus>DETECTION_COMPLETE</DetectionStatus>")
		assert.Contains(t, body, "<StackDriftStatus>IN_SYNC</StackDriftStatus>")
		assert.Contains(t, body, "<DriftedStackResourceCount>0</DriftedStackResourceCount>")
	})

	t.Run("deleting the bucket outside CFN drifts the stack", func(t *testing.T) {
		del := httptest.NewRequest(http.MethodDelete, "http://s3.amazonaws.com/cfn-wire-bucket", nil)
		del.Host = "s3.amazonaws.com"
		dw := httptest.NewRecorder()
		ts.srv.ServeHTTP(dw, del)
		require.Equal(t, http.StatusNoContent, dw.Code, "body was %s", dw.Body.String())

		code, body := cfnAction(t, ts, "DescribeStackResourceDrifts", map[string]string{"StackName": "drifty"})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.Contains(t, body, "<StackResourceDriftStatus>DELETED</StackResourceDriftStatus>")
		assert.Contains(t, body, "<LogicalResourceId>Data</LogicalResourceId>")

		code, body = cfnAction(t, ts, "DescribeStackResourceDrifts", map[string]string{
			"StackName": "drifty",
			"StackResourceDriftStatusFilters.member.1": "IN_SYNC",
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.NotContains(t, body, "<LogicalResourceId>Data</LogicalResourceId>",
			"the filter should exclude the DELETED entry")
	})

	t.Run("drift errors", func(t *testing.T) {
		code, body := cfnAction(t, ts, "DetectStackDrift", map[string]string{"StackName": "absent"})
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "ValidationError", cfnErrorCode(t, body))

		code, body = cfnAction(t, ts, "DescribeStackResourceDrifts", nil)
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "MissingParameter", cfnErrorCode(t, body))

		code, body = cfnAction(t, ts, "DescribeStackDriftDetectionStatus",
			map[string]string{"StackDriftDetectionId": "req-absent"})
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
	})
}

// TestCFNPlugin_DescribeStackResources covers the two lookup keys and the
// documented refusal to accept both.
func TestCFNPlugin_DescribeStackResources(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "resourceful",
		"TemplateBody": cfnBucketRoleTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	t.Run("by logical ID", func(t *testing.T) {
		code, body := cfnAction(t, ts, "DescribeStackResources", map[string]string{
			"StackName":         "resourceful",
			"LogicalResourceId": "Runner",
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.Contains(t, body, "cfn-shared-role")
		assert.NotContains(t, body, "cfn-shared-bucket")
	})

	t.Run("by physical ID with no stack name", func(t *testing.T) {
		code, body := cfnAction(t, ts, "DescribeStackResources", map[string]string{
			"PhysicalResourceId": "cfn-shared-bucket",
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.Contains(t, body, "<LogicalResourceId>Data</LogicalResourceId>")
		assert.NotContains(t, body, "cfn-shared-role")
	})

	t.Run("both keys is a ValidationError", func(t *testing.T) {
		code, body := cfnAction(t, ts, "DescribeStackResources", map[string]string{
			"StackName":          "resourceful",
			"PhysicalResourceId": "cfn-shared-bucket",
		})
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "ValidationError", cfnErrorCode(t, body))
	})

	t.Run("neither key is a MissingParameter", func(t *testing.T) {
		code, body := cfnAction(t, ts, "DescribeStackResources", nil)
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "MissingParameter", cfnErrorCode(t, body))
	})
}

// TestCFNPlugin_DescribeStackEvents pins the scope decision deliberately rather
// than leaving it incidental.
//
// CFNStackState carries one Status string; there is no per-resource event model,
// so a plausible CREATE_IN_PROGRESS/CREATE_COMPLETE sequence would be invented
// observations. A caller gets a refusal they can see instead. If someone later
// adds a real event model, this test is the one that must be changed knowingly.
func TestCFNPlugin_DescribeStackEvents(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "eventful",
		"TemplateBody": cfnEmptyTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "DescribeStackEvents", map[string]string{"StackName": "eventful"})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "UnsupportedOperation", cfnErrorCode(t, body))
}

// TestCFNPlugin_UnknownAction asserts an unmodelled action is InvalidAction, not
// a panic or a silent success.
func TestCFNPlugin_UnknownAction(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStackSet", map[string]string{"StackSetName": "s"})
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "InvalidAction", cfnErrorCode(t, body))
}

// TestCFNPlugin_ErrorProtocolIsQueryXML asserts an error carries the code where
// a Query-protocol SDK reads it. CloudFormation was absent from
// serviceErrorProtocols, which would have left the shape to Content-Type
// sniffing on a form-encoded request.
func TestCFNPlugin_ErrorProtocolIsQueryXML(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "absent"})
	require.Equal(t, http.StatusBadRequest, code)

	var doc struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Type    string   `xml:"Error>Type"`
		Code    string   `xml:"Error>Code"`
		Message string   `xml:"Error>Message"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body was %s", body)
	assert.Equal(t, "ErrorResponse", doc.XMLName.Local)
	assert.Equal(t, "Sender", doc.Type)
	assert.Equal(t, "ValidationError", doc.Code)
	assert.Contains(t, doc.Message, "absent")
}

// TestCFNPlugin_RegisteredByDefault asserts the plugin is in the default set, so
// a consumer running `substrate server` reaches it without extra wiring — the
// configuration #483 actually reported against.
func TestCFNPlugin_RegisteredByDefault(t *testing.T) {
	ts := emulator.StartTestServer(t)
	assert.Contains(t, ts.Registry().Names(), "cloudformation")
}

// TestCFNPlugin_Initialize_RequiresRegistry asserts the one hard dependency is
// checked rather than deferred to a nil dereference at request time.
func TestCFNPlugin_Initialize_RequiresRegistry(t *testing.T) {
	p := &emulator.CloudFormationPlugin{}
	err := p.Initialize(context.Background(), emulator.PluginConfig{
		State:  emulator.NewMemoryStateManager(),
		Logger: emulator.NewDefaultLogger(slog.LevelError, false),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "registry")
}

// TestCFNPlugin_Initialize_NilStoreAndCosts asserts a stack deploys when neither
// an EventStore nor a CostController is supplied.
//
// This is not hypothetical: RegisterDefaultPlugins documents store as optional
// and debug_ui.go's state-reset path passes nil, while no caller passes a
// CostController at all. StackDeployer.dispatch dereferences both on every
// resource it deploys, so Initialize substitutes disabled instances rather than
// storing nil.
func TestCFNPlugin_Initialize_NilStoreAndCosts(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	registry := emulator.NewPluginRegistry()

	s3p := &emulator.S3Plugin{}
	require.NoError(t, s3p.Initialize(context.Background(), emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
		},
	}))
	registry.Register(s3p)

	p := &emulator.CloudFormationPlugin{}
	require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"registry":        registry,
			// event_store and cost_controller deliberately absent.
		},
	}))
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	srv := emulator.NewServer(*cfg, registry,
		emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false}), state, tc, logger)

	form := url.Values{
		"Action":       {"CreateStack"},
		"StackName":    {"nil-deps"},
		"TemplateBody": {cfnBucketTemplate},
	}
	r := httptest.NewRequest(http.MethodPost, "http://cloudformation.us-east-1.amazonaws.com/",
		strings.NewReader(form.Encode()))
	r.Host = "cloudformation.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code, "body was %s", w.Body.String())
}

// TestCFNPlugin_StackIDIsStable asserts the stack ARN does not change between
// calls. A UUID from a clock or a PRNG would make a recorded response
// unreproducible on replay, which is the property substrate exists to hold.
func TestCFNPlugin_StackIDIsStable(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "stable",
		"TemplateBody": cfnEmptyTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	var created struct {
		StackID string `xml:"CreateStackResult>StackId"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &created))

	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "stable"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	var described struct {
		StackID string `xml:"DescribeStacksResult>Stacks>member>StackId"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &described))

	assert.Equal(t, created.StackID, described.StackID)

	code, body = cfnAction(t, ts, "ListStacks", nil)
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, created.StackID)
}

// TestCFNPlugin_Outputs asserts resolved template outputs reach DescribeStacks,
// which is how a consumer reads a stack's exported values.
func TestCFNPlugin_Outputs(t *testing.T) {
	ts := newCFNTestServer(t)
	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "outputting",
		"TemplateBody": cfnBucketTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "outputting"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	var doc struct {
		Outputs []struct {
			Key   string `xml:"OutputKey"`
			Value string `xml:"OutputValue"`
		} `xml:"DescribeStacksResult>Stacks>member>Outputs>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc))
	require.Len(t, doc.Outputs, 1)
	assert.Equal(t, "BucketName", doc.Outputs[0].Key)
	assert.Equal(t, "cfn-wire-bucket", doc.Outputs[0].Value)
}
