package emulator_test

import (
	"context"
	"encoding/json"
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

// The tests in this file are #517's gate, and their design is driven by why #517
// survived #483's review.
//
// #483 shipped a shared-state test (TestCFNPlugin_StackSharesStateWithOtherPlugins)
// asserting a stack's bucket through the S3 plugin and its role through IAM. Both
// of those plugins key their state *unpartitioned* — "bucket:<name>",
// "role:<name>" — so a resource written under the wrong account and region is
// still found by a read under the right one. The stack's resources were being
// written into substrate's default partition regardless of who asked, and that test
// could not see it.
//
// EC2, ECS, CloudWatch Logs, DynamoDB, SQS and SNS embed the account — and for
// some of them the region — in their state keys. Those are the only honest
// subjects for this fix, so every case below uses one.

// newCFNIdentityTestServer builds a server with CloudFormation registered
// alongside the *partitioned* plugins: EC2, ECS, CloudWatch Logs, DynamoDB, SQS
// and SNS. S3 and IAM are deliberately absent — a stack whose resources are all
// unpartitioned cannot distinguish a threaded identity from a hardcoded one.
func newCFNIdentityTestServer(t *testing.T) *cfnTestServer {
	t.Helper()
	cfg := emulator.DefaultConfig()
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	registry := emulator.NewPluginRegistry()

	opts := emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
			"registry":        registry,
		},
	}
	for _, p := range []emulator.Plugin{
		&emulator.EC2Plugin{},
		&emulator.ECSPlugin{},
		&emulator.CloudWatchLogsPlugin{},
		// DynamoDB, SQS and SNS are here for the drift cases alone: drift needs a
		// resource type with an existence checker, and of those types these three
		// are the partitioned ones. EC2 instances have no checker, so a stack of
		// them reports NOT_CHECKED and could not tell the partitions apart.
		&emulator.DynamoDBPlugin{},
		&emulator.SQSPlugin{},
		&emulator.SNSPlugin{},
	} {
		require.NoError(t, p.Initialize(context.Background(), opts))
		registry.Register(p)
	}

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

// cfnIdentityRequest posts a query-protocol request to any service, optionally
// signed and optionally to a non-default region, so a test can vary exactly the
// two things #517 is about.
//
// An empty authHeader means an unsigned request, which resolves to the fallback
// account 000000000000 — the identity #517's reporter had, since their client
// signed nothing.
func cfnIdentityRequest(t *testing.T, ts *cfnTestServer, service, region, authHeader string, params map[string]string) (int, string) {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	host := service + "." + region + ".amazonaws.com"
	r := httptest.NewRequest(http.MethodPost, "http://"+host+"/", strings.NewReader(form.Encode()))
	r.Host = host
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if authHeader != "" {
		r.Header.Set("Authorization", authHeader)
	}
	w := httptest.NewRecorder()
	ts.srv.ServeHTTP(w, r)
	return w.Code, w.Body.String()
}

// cfnJSONRequest posts a JSON-protocol request, for the services that use one
// (ECS and CloudWatch Logs).
func cfnJSONRequest(t *testing.T, ts *cfnTestServer, service, region, target, authHeader, body string) (int, string) {
	t.Helper()
	host := service + "." + region + ".amazonaws.com"
	r := httptest.NewRequest(http.MethodPost, "http://"+host+"/", strings.NewReader(body))
	r.Host = host
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("X-Amz-Target", target)
	if authHeader != "" {
		r.Header.Set("Authorization", authHeader)
	}
	w := httptest.NewRecorder()
	ts.srv.ServeHTTP(w, r)
	return w.Code, w.Body.String()
}

// signedAuthHeader builds an AKIA-signed Authorization header, which resolves to
// the well-known test account 123456789012.
func signedAuthHeader(service, region string) string {
	return "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20260101/" +
		region + "/" + service + "/aws4_request, SignedHeaders=host, Signature=fake"
}

// cfnXMLValue extracts the text of the first tag element in an XML body. Enough
// for a single-valued response field, and it keeps these tests reading as the
// wire assertions they are rather than as unmarshalling exercises.
func cfnXMLValue(t *testing.T, body, tag string) string {
	t.Helper()
	open, close := "<"+tag+">", "</"+tag+">"
	i := strings.Index(body, open)
	require.GreaterOrEqual(t, i, 0, "no %s in %s", tag, body)
	rest := body[i+len(open):]
	j := strings.Index(rest, close)
	require.GreaterOrEqual(t, j, 0, "unterminated %s in %s", tag, body)
	return rest[:j]
}

// cfnPhysicalID extracts the PhysicalResourceId of a stack's single resource from
// a DescribeStackResources body.
func cfnPhysicalID(t *testing.T, body string) string {
	t.Helper()
	return cfnXMLValue(t, body, "PhysicalResourceId")
}

const cfnInstanceTemplate = `{"Resources":{"I":{"Type":"AWS::EC2::Instance",` +
	`"Properties":{"ImageId":"ami-12345678","InstanceType":"t3.micro"}}}}`

// TestCFNIdentity_UnsignedCallerFindsItsOwnInstance is #517's reproduction, and
// it fails before the fix.
//
// An unsigned request resolves to account 000000000000. The stack ARN named that
// account, but the deployer dispatched RunInstances under substrate's default
// 123456789012, and the EC2 plugin keys an instance by account and region — so
// DescribeInstances, correctly scoped to the caller, reported nothing. The
// reporter read that as EC2 support being stubbed out; it was a partition split.
func TestCFNIdentity_UnsignedCallerFindsItsOwnInstance(t *testing.T) {
	ts := newCFNIdentityTestServer(t)

	code, body := cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", "", map[string]string{
		"Action":       "CreateStack",
		"Version":      "2010-05-15",
		"StackName":    "unsigned",
		"TemplateBody": cfnInstanceTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, ":000000000000:stack/unsigned",
		"the stack ARN should name the unsigned caller's account")

	code, body = cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", "", map[string]string{
		"Action":    "DescribeStackResources",
		"Version":   "2010-05-15",
		"StackName": "unsigned",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	instanceID := cfnPhysicalID(t, body)
	require.True(t, strings.HasPrefix(instanceID, "i-"), "physical ID was %q", instanceID)

	// The gate: the same caller that created the stack must see the instance.
	code, body = cfnIdentityRequest(t, ts, "ec2", "us-east-1", "", map[string]string{
		"Action":  "DescribeInstances",
		"Version": "2016-11-15",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, instanceID,
		"the unsigned caller's DescribeInstances must find the instance its own stack created")

	// And by ID, which is the lookup that answered InvalidInstanceID.NotFound.
	code, body = cfnIdentityRequest(t, ts, "ec2", "us-east-1", "", map[string]string{
		"Action":       "DescribeInstances",
		"Version":      "2016-11-15",
		"InstanceId.1": instanceID,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.NotContains(t, body, "InvalidInstanceID.NotFound")
	assert.Contains(t, body, instanceID)
}

// TestCFNIdentity_PartitionedServices covers one resource per service family the
// reporter named. Each of these plugins embeds the account and region in its state
// key, so each is a resource that vanished; S3 and IAM would pass either way.
func TestCFNIdentity_PartitionedServices(t *testing.T) {
	cases := []struct {
		name     string
		tmpl     string
		wantID   string
		readBack func(t *testing.T, ts *cfnTestServer) (int, string)
	}{
		{
			name: "ECSCluster",
			tmpl: `{"Resources":{"C":{"Type":"AWS::ECS::Cluster",` +
				`"Properties":{"ClusterName":"identity-cluster"}}}}`,
			wantID: "identity-cluster",
			readBack: func(t *testing.T, ts *cfnTestServer) (int, string) {
				return cfnJSONRequest(t, ts, "ecs", "us-east-1",
					"AmazonEC2ContainerServiceV20141113.ListClusters", "", `{}`)
			},
		},
		{
			name: "LogsLogGroup",
			tmpl: `{"Resources":{"G":{"Type":"AWS::Logs::LogGroup",` +
				`"Properties":{"LogGroupName":"/identity/group"}}}}`,
			wantID: "/identity/group",
			readBack: func(t *testing.T, ts *cfnTestServer) (int, string) {
				return cfnJSONRequest(t, ts, "logs", "us-east-1",
					"Logs_20140328.DescribeLogGroups", "", `{}`)
			},
		},
		{
			name: "EC2LaunchTemplate",
			tmpl: `{"Resources":{"T":{"Type":"AWS::EC2::LaunchTemplate",` +
				`"Properties":{"LaunchTemplateName":"identity-lt",` +
				`"LaunchTemplateData":{"ImageId":"ami-12345678","InstanceType":"t3.micro"}}}}}`,
			wantID: "identity-lt",
			readBack: func(t *testing.T, ts *cfnTestServer) (int, string) {
				return cfnIdentityRequest(t, ts, "ec2", "us-east-1", "", map[string]string{
					"Action":  "DescribeLaunchTemplates",
					"Version": "2016-11-15",
				})
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := newCFNIdentityTestServer(t)

			code, body := cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", "", map[string]string{
				"Action":       "CreateStack",
				"Version":      "2010-05-15",
				"StackName":    "partitioned",
				"TemplateBody": tc.tmpl,
			})
			require.Equal(t, http.StatusOK, code, "body was %s", body)

			code, body = tc.readBack(t, ts)
			require.Equal(t, http.StatusOK, code, "body was %s", body)
			assert.Contains(t, body, tc.wantID,
				"the unsigned caller must see the resource its own stack created")
		})
	}
}

// TestCFNIdentity_NonDefaultRegion is the case that a caller in substrate's own
// default account still exercises: partitioning is by account *and* region, so a
// stack created in eu-west-1 has to land in eu-west-1 and be absent from
// us-east-1. A test that only ever used the default region would pass with the
// region hardcoded.
func TestCFNIdentity_NonDefaultRegion(t *testing.T) {
	ts := newCFNIdentityTestServer(t)

	code, body := cfnIdentityRequest(t, ts, "cloudformation", "eu-west-1",
		signedAuthHeader("cloudformation", "eu-west-1"), map[string]string{
			"Action":       "CreateStack",
			"Version":      "2010-05-15",
			"StackName":    "euwest",
			"TemplateBody": cfnInstanceTemplate,
		})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "arn:aws:cloudformation:eu-west-1:")

	code, body = cfnIdentityRequest(t, ts, "cloudformation", "eu-west-1",
		signedAuthHeader("cloudformation", "eu-west-1"), map[string]string{
			"Action":    "DescribeStackResources",
			"Version":   "2010-05-15",
			"StackName": "euwest",
		})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	instanceID := cfnPhysicalID(t, body)

	code, body = cfnIdentityRequest(t, ts, "ec2", "eu-west-1",
		signedAuthHeader("ec2", "eu-west-1"), map[string]string{
			"Action": "DescribeInstances", "Version": "2016-11-15",
		})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, instanceID, "the instance belongs to eu-west-1")

	// Absent from another region, which is the half that proves partitioning still
	// holds rather than having been widened away.
	code, body = cfnIdentityRequest(t, ts, "ec2", "us-east-1",
		signedAuthHeader("ec2", "us-east-1"), map[string]string{
			"Action": "DescribeInstances", "Version": "2016-11-15",
		})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.NotContains(t, body, instanceID,
		"a eu-west-1 instance must not be visible in us-east-1")
}

// TestCFNIdentity_SignedAndUnsignedAreIsolated pins that the fix threads identity
// rather than widening a lookup. The same template deployed by a signed caller
// (123456789012) and an unsigned one (000000000000) yields two instances, each
// visible only to the caller that created it.
func TestCFNIdentity_SignedAndUnsignedAreIsolated(t *testing.T) {
	ts := newCFNIdentityTestServer(t)
	signed := signedAuthHeader("cloudformation", "us-east-1")

	create := func(auth, stackName string) string {
		code, body := cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", auth,
			map[string]string{
				"Action": "CreateStack", "Version": "2010-05-15",
				"StackName": stackName, "TemplateBody": cfnInstanceTemplate,
			})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		code, body = cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", auth,
			map[string]string{
				"Action": "DescribeStackResources", "Version": "2010-05-15",
				"StackName": stackName,
			})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		return cfnPhysicalID(t, body)
	}

	signedID := create(signed, "by-signed")
	unsignedID := create("", "by-unsigned")
	require.NotEqual(t, signedID, unsignedID, "two stacks, two instances")

	describe := func(auth string) string {
		code, body := cfnIdentityRequest(t, ts, "ec2", "us-east-1", auth,
			map[string]string{"Action": "DescribeInstances", "Version": "2016-11-15"})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		return body
	}

	signedView := describe(signedAuthHeader("ec2", "us-east-1"))
	assert.Contains(t, signedView, signedID)
	assert.NotContains(t, signedView, unsignedID,
		"the signed caller must not see the unsigned caller's instance")

	unsignedView := describe("")
	assert.Contains(t, unsignedView, unsignedID)
	assert.NotContains(t, unsignedView, signedID,
		"the unsigned caller must not see the signed caller's instance")
}

// TestCFNIdentity_PseudoParametersMatchTheStackARN is the buildCFNContext half of
// the fix, and it fails if only dispatch is threaded.
//
// AWS::AccountId and AWS::Region were resolved against substrate's defaults too,
// from a second hardcode neither issue named. A template naming itself after its
// own account and region would disagree with the stack ARN reported by the very
// call that created it.
func TestCFNIdentity_PseudoParametersMatchTheStackARN(t *testing.T) {
	ts := newCFNIdentityTestServer(t)

	tmpl := `{"Resources":{"G":{"Type":"AWS::Logs::LogGroup","Properties":{
		"LogGroupName":{"Fn::Sub":"/g-${AWS::AccountId}-${AWS::Region}"}}}}}`

	code, body := cfnIdentityRequest(t, ts, "cloudformation", "eu-west-1", "", map[string]string{
		"Action": "CreateStack", "Version": "2010-05-15",
		"StackName": "pseudo", "TemplateBody": tmpl,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	require.Contains(t, body, "arn:aws:cloudformation:eu-west-1:000000000000:stack/pseudo",
		"the stack ARN names the caller")

	code, body = cfnIdentityRequest(t, ts, "cloudformation", "eu-west-1", "", map[string]string{
		"Action": "DescribeStackResources", "Version": "2010-05-15",
		"StackName": "pseudo",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Equal(t, "/g-000000000000-eu-west-1", cfnPhysicalID(t, body),
		"the pseudo-parameters must resolve to the caller's identity, not substrate's defaults")
}

// TestCFNIdentity_DriftFindsTheCallersResources pins the drift paths, which
// resolve each resource by the same partitioned state key. Left on the default
// identity they would report every resource of another caller's stack as DELETED —
// a false drift report, which is worse than none.
//
// The table covers all three drift-checkable types whose state keys are
// partitioned. EC2 instances are not among them: they have no existence checker
// and report NOT_CHECKED, which would pass whichever partition drift looked in.
//
// Each case asserts IN_SYNC *and then* MODIFIED, because they fail differently. A
// wrong-partition existence check finds nothing and reports DELETED — loud. A
// wrong-partition comparator finds nothing, compares nothing, and reports no
// difference — drift silently blind, which IN_SYNC alone cannot distinguish from
// drift working. Both reads are separately hardcoded, so both are asserted.
func TestCFNIdentity_DriftFindsTheCallersResources(t *testing.T) {
	// A non-default region throughout, because the SNS key embeds the region as
	// well as the account: run in us-east-1 and a comparator that threaded only the
	// account would still find the topic, since us-east-1 is substrate's default.
	const region = "eu-west-1"

	cases := []struct {
		name string
		tmpl string
		// namespace and key locate the resource in the *unsigned* caller's
		// partition, which is where it must be for drift to have any chance.
		namespace string
		key       string
		// mutate changes the live resource behind CloudFormation's back, which is
		// the drift substrate models.
		mutate   func(m map[string]any)
		wantPath string
	}{
		{
			name: "DynamoDBTable",
			tmpl: `{"Resources":{"T":{"Type":"AWS::DynamoDB::Table","Properties":{
				"TableName":"drift-table","BillingMode":"PAY_PER_REQUEST",
				"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],
				"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}}}}`,
			namespace: "dynamodb",
			key:       "table:000000000000/drift-table",
			mutate: func(m map[string]any) {
				m["BillingModeSummary"] = map[string]any{"BillingMode": "PROVISIONED"}
			},
			wantPath: "/BillingMode",
		},
		{
			name: "SQSQueue",
			tmpl: `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{
				"QueueName":"drift-queue","VisibilityTimeout":45}}}}`,
			namespace: "sqs",
			key:       "queue:000000000000/drift-queue",
			mutate: func(m map[string]any) {
				attrs, _ := m["Attributes"].(map[string]any)
				if attrs == nil {
					attrs = map[string]any{}
				}
				attrs["VisibilityTimeout"] = "90"
				m["Attributes"] = attrs
			},
			wantPath: "/VisibilityTimeout",
		},
		{
			// The only comparator keyed by region as well as account, so it is the
			// one that would survive threading the account alone.
			//
			// Its DisplayName carries both pseudo-parameters on purpose. A comparator
			// reads the live value from state but resolves the *expected* value out
			// of the template, through a context built separately from the deploy
			// path's — a third identity read, and the only place either
			// pseudo-parameter is resolved twice. Deploy stamps
			// "000000000000 in eu-west-1"; a comparator resolving against the
			// defaults expects "123456789012 in us-east-1" and reports MODIFIED on a
			// topic nobody touched. A false drift report is the failure mode, so the
			// IN_SYNC half of this case is what pins it.
			name: "SNSTopic",
			tmpl: `{"Resources":{"Tp":{"Type":"AWS::SNS::Topic","Properties":{
				"TopicName":"drift-topic",
				"DisplayName":{"Fn::Sub":"${AWS::AccountId} in ${AWS::Region}"}}}}}`,
			namespace: "sns",
			key:       "topic:000000000000/" + region + "/drift-topic",
			mutate: func(m map[string]any) {
				m["Attributes"] = map[string]any{"DisplayName": "Renamed"}
			},
			wantPath: "/DisplayName",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := newCFNIdentityTestServer(t)

			code, body := cfnIdentityRequest(t, ts, "cloudformation", region, "",
				map[string]string{
					"Action": "CreateStack", "Version": "2010-05-15",
					"StackName": "drift", "TemplateBody": tc.tmpl,
				})
			require.Equal(t, http.StatusOK, code, "body was %s", body)

			drifts := func() string {
				code, body := cfnIdentityRequest(t, ts, "cloudformation", region, "",
					map[string]string{
						"Action": "DescribeStackResourceDrifts", "Version": "2010-05-15",
						"StackName": "drift",
					})
				require.Equal(t, http.StatusOK, code, "body was %s", body)
				return body
			}

			body = drifts()
			assert.Contains(t, body, "<StackResourceDriftStatus>IN_SYNC</StackResourceDriftStatus>",
				"the resource exists, so it is in sync; DELETED means the existence "+
					"check looked in the wrong partition")
			assert.NotContains(t, body, "DELETED")

			// DetectStackDrift is the asynchronous sibling and a separate handler.
			// It reports no per-resource statuses, only a count and a stack-level
			// verdict, so its own assertion is the count: on the wrong identity
			// every resource reads as DELETED and the stack comes back DRIFTED.
			code, body = cfnIdentityRequest(t, ts, "cloudformation", region, "",
				map[string]string{
					"Action": "DetectStackDrift", "Version": "2010-05-15",
					"StackName": "drift",
				})
			require.Equal(t, http.StatusOK, code, "body was %s", body)
			detectionID := cfnXMLValue(t, body, "StackDriftDetectionId")

			code, body = cfnIdentityRequest(t, ts, "cloudformation", region, "",
				map[string]string{
					"Action": "DescribeStackDriftDetectionStatus", "Version": "2010-05-15",
					"StackDriftDetectionId": detectionID,
				})
			require.Equal(t, http.StatusOK, code, "body was %s", body)
			assert.Contains(t, body, "<DetectionStatus>DETECTION_COMPLETE</DetectionStatus>")
			assert.Contains(t, body, "<StackDriftStatus>IN_SYNC</StackDriftStatus>",
				"detection must look in the partition the resource was deployed into")
			assert.Contains(t, body, "<DriftedStackResourceCount>0</DriftedStackResourceCount>")

			raw, err := ts.state.Get(context.Background(), tc.namespace, tc.key)
			require.NoError(t, err)
			require.NotNil(t, raw, "the resource must be in the unsigned caller's partition")
			var live map[string]any
			require.NoError(t, json.Unmarshal(raw, &live))
			tc.mutate(live)
			mutated, err := json.Marshal(live)
			require.NoError(t, err)
			require.NoError(t, ts.state.Put(context.Background(), tc.namespace, tc.key, mutated))

			body = drifts()
			assert.Contains(t, body, "<StackResourceDriftStatus>MODIFIED</StackResourceDriftStatus>",
				"the comparator must read the partition the resource is in")
			assert.Contains(t, body, "<PropertyPath>"+tc.wantPath+"</PropertyPath>")
		})
	}
}

// TestCFNIdentity_ExecuteChangeSetUsesTheExecutingCaller covers the third
// deploying entry point. ExecuteChangeSet routes through UpdateStack to Deploy, so
// it creates resources and needs the caller's identity exactly as CreateStack
// does — and it is the path a CDK deploy actually takes.
func TestCFNIdentity_ExecuteChangeSetUsesTheExecutingCaller(t *testing.T) {
	ts := newCFNIdentityTestServer(t)

	// A stack to change: created empty so the change set is what deploys the
	// instance.
	code, body := cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", "", map[string]string{
		"Action": "CreateStack", "Version": "2010-05-15",
		"StackName": "cs", "TemplateBody": `{"Resources":{}}`,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", "", map[string]string{
		"Action": "CreateChangeSet", "Version": "2010-05-15",
		"StackName": "cs", "ChangeSetName": "add-instance",
		"TemplateBody": cfnInstanceTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", "", map[string]string{
		"Action": "ExecuteChangeSet", "Version": "2010-05-15",
		"StackName": "cs", "ChangeSetName": "add-instance",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", "", map[string]string{
		"Action": "DescribeStackResources", "Version": "2010-05-15", "StackName": "cs",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	instanceID := cfnPhysicalID(t, body)

	code, body = cfnIdentityRequest(t, ts, "ec2", "us-east-1", "", map[string]string{
		"Action": "DescribeInstances", "Version": "2016-11-15",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, instanceID,
		"an instance created by executing a change set belongs to the caller that executed it")
}

// TestCFNIdentity_UpdateStackUsesTheUpdatingCaller covers UpdateStack as its own
// entry point rather than through a change set.
//
// The change-set test above reaches UpdateStack indirectly, so it would pass with
// UpdateStack itself left on the default identity as long as ExecuteChangeSet were
// threaded. Adding a resource by updating a stack directly is what a
// `aws cloudformation update-stack` or a Terraform-managed template does, and it
// has to land in the caller's partition too.
func TestCFNIdentity_UpdateStackUsesTheUpdatingCaller(t *testing.T) {
	ts := newCFNIdentityTestServer(t)

	code, body := cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", "", map[string]string{
		"Action": "CreateStack", "Version": "2010-05-15",
		"StackName": "upd", "TemplateBody": `{"Resources":{}}`,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", "", map[string]string{
		"Action": "UpdateStack", "Version": "2010-05-15",
		"StackName": "upd", "TemplateBody": cfnInstanceTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnIdentityRequest(t, ts, "cloudformation", "us-east-1", "", map[string]string{
		"Action": "DescribeStackResources", "Version": "2010-05-15", "StackName": "upd",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	instanceID := cfnPhysicalID(t, body)
	require.True(t, strings.HasPrefix(instanceID, "i-"), "physical ID was %q", instanceID)

	code, body = cfnIdentityRequest(t, ts, "ec2", "us-east-1", "", map[string]string{
		"Action": "DescribeInstances", "Version": "2016-11-15",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, instanceID,
		"an instance added by an update belongs to the caller that updated the stack")
}

// TestCFNIdentity_UnpartitionedServicesStillWork is the regression guard for
// #483's own test. S3 and IAM keys carry no account or region, so they worked
// before the fix and must keep working after it: the fix threads identity, and
// must not have made an unpartitioned lookup depend on one.
func TestCFNIdentity_UnpartitionedServicesStillWork(t *testing.T) {
	ts := newCFNTestServer(t)

	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "unpartitioned",
		"TemplateBody": cfnBucketRoleTemplate,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	// Read back as an *unsigned* caller, which is a different account from the
	// signed one that created the stack. An unpartitioned key is found anyway,
	// which is the pre-existing behavior this fix must not change.
	head := httptest.NewRequest(http.MethodHead, "http://s3.amazonaws.com/cfn-shared-bucket", nil)
	head.Host = "s3.amazonaws.com"
	hw := httptest.NewRecorder()
	ts.srv.ServeHTTP(hw, head)
	assert.Equal(t, http.StatusOK, hw.Code,
		"an S3 bucket is keyed without an account, so it is visible across partitions")
}

// TestCFNIdentity_InProcessDeployerDefaultsToSubstratesOwn pins the recorded
// decision that a deployer built without an identity uses substrate's defaults.
//
// BettyClient is the in-process validation client and its callers never sign a
// request, so there is no caller identity to thread — the defaults are the answer
// rather than a placeholder for one. The default is load-bearing: were it the zero
// value, every in-process deploy would write into account "" and the resources
// would be unreachable from any read.
func TestCFNIdentity_InProcessDeployerDefaultsToSubstratesOwn(t *testing.T) {
	ts := newCFNIdentityTestServer(t)

	d := emulator.NewStackDeployer(ts.registry,
		emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false}),
		ts.state, ts.tc, emulator.NewDefaultLogger(slog.LevelError, false),
		emulator.NewCostController(emulator.CostConfig{Enabled: false}))

	result, err := d.Deploy(context.Background(), cfnInstanceTemplate, "in-process", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	require.Empty(t, result.Resources[0].Error)
	instanceID := result.Resources[0].PhysicalID

	// Visible to a default-partition read: signed (123456789012) in us-east-1.
	code, body := cfnIdentityRequest(t, ts, "ec2", "us-east-1",
		signedAuthHeader("ec2", "us-east-1"), map[string]string{
			"Action": "DescribeInstances", "Version": "2016-11-15",
		})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, instanceID,
		"an in-process deploy lands in substrate's default partition")
}

// TestCFNIdentity_ExplicitIdentityOverridesTheDefault pins the option itself, so
// an in-process caller that does need another partition has a seam and it is
// tested. This is what the BettyClient comment points at.
func TestCFNIdentity_ExplicitIdentityOverridesTheDefault(t *testing.T) {
	ts := newCFNIdentityTestServer(t)

	d := emulator.NewStackDeployer(ts.registry,
		emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false}),
		ts.state, ts.tc, emulator.NewDefaultLogger(slog.LevelError, false),
		emulator.NewCostController(emulator.CostConfig{Enabled: false}),
		emulator.WithDeployerIdentity("999988887777", "ap-southeast-2"))

	tmpl := `{"Resources":{"G":{"Type":"AWS::Logs::LogGroup","Properties":{
		"LogGroupName":{"Fn::Sub":"/g-${AWS::AccountId}-${AWS::Region}"}}}}}`
	result, err := d.Deploy(context.Background(), tmpl, "explicit", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	require.Empty(t, result.Resources[0].Error)
	assert.Equal(t, "/g-999988887777-ap-southeast-2", result.Resources[0].PhysicalID,
		"the option must reach the pseudo-parameters as well as the dispatch")

	// And the log group is in that partition, not the default one.
	code, body := cfnJSONRequest(t, ts, "logs", "us-east-1",
		"Logs_20140328.DescribeLogGroups", signedAuthHeader("logs", "us-east-1"), `{}`)
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.NotContains(t, body, "/g-999988887777-ap-southeast-2",
		"a default-partition read must not see it")
}
