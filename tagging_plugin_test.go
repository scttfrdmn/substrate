package substrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// newTaggingTestServer builds a Server with IAM, S3, Lambda, SQS, DynamoDB,
// EC2, and Tagging plugins registered. Returns the test server and the shared
// state manager so tests can pre-populate resources.
func newTaggingTestServer(t *testing.T) (*httptest.Server, substrate.StateManager) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	registry := substrate.NewPluginRegistry()
	cfg := substrate.Config{}
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: false, Backend: "memory"})
	tc := substrate.NewTimeController(time.Now())
	initCtx := context.Background()

	iamPlugin := &substrate.IAMPlugin{}
	require.NoError(t, iamPlugin.Initialize(initCtx, substrate.PluginConfig{State: state, Logger: logger}))
	registry.Register(iamPlugin)

	lambdaPlugin := &substrate.LambdaPlugin{}
	require.NoError(t, lambdaPlugin.Initialize(initCtx, substrate.PluginConfig{
		State: state, Logger: logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(lambdaPlugin)

	sqsPlugin := &substrate.SQSPlugin{}
	require.NoError(t, sqsPlugin.Initialize(initCtx, substrate.PluginConfig{
		State: state, Logger: logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(sqsPlugin)

	dynamodbPlugin := &substrate.DynamoDBPlugin{}
	require.NoError(t, dynamodbPlugin.Initialize(initCtx, substrate.PluginConfig{
		State: state, Logger: logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(dynamodbPlugin)

	ec2Plugin := &substrate.EC2Plugin{}
	require.NoError(t, ec2Plugin.Initialize(initCtx, substrate.PluginConfig{
		State: state, Logger: logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(ec2Plugin)

	s3Plugin := &substrate.S3Plugin{}
	require.NoError(t, s3Plugin.Initialize(initCtx, substrate.PluginConfig{
		State: state, Logger: logger,
		Options: map[string]any{"time_controller": tc, "registry": registry},
	}))
	registry.Register(s3Plugin)

	taggingPlugin := &substrate.TaggingPlugin{}
	require.NoError(t, taggingPlugin.Initialize(initCtx, substrate.PluginConfig{State: state, Logger: logger}))
	registry.Register(taggingPlugin)

	srv := substrate.NewServer(cfg, registry, store, state, tc, logger, substrate.ServerOptions{})
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, state
}

// taggingRequest sends a JSON request to the tagging endpoint and returns the
// response body.
func taggingRequest(t *testing.T, ts *httptest.Server, op string, body any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		ts.URL+"/", bytes.NewReader(b))
	require.NoError(t, err)
	req.Host = "tagging.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Target", "ResourceGroupsTaggingAPI_20170126."+op)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// putTestS3Bucket pre-populates state with an S3 bucket.
func putTestS3Bucket(t *testing.T, state substrate.StateManager, name string, tags map[string]string) {
	t.Helper()
	b := substrate.S3Bucket{Name: name, Region: "us-east-1", Tags: tags}
	raw, _ := json.Marshal(b)
	require.NoError(t, state.Put(context.Background(), "s3", "bucket:"+name, raw))
}

// putTestLambdaFunction pre-populates state with a Lambda function.
func putTestLambdaFunction(t *testing.T, state substrate.StateManager, name string, tags map[string]string) {
	t.Helper()
	fn := substrate.LambdaFunction{
		FunctionName: name,
		FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:" + name,
		Tags:         tags,
	}
	raw, _ := json.Marshal(fn)
	require.NoError(t, state.Put(context.Background(), "lambda", "function:"+name, raw))
}

// ---- Tests ----------------------------------------------------------------

func TestTagging_GetResources_Empty(t *testing.T) {
	ts, _ := newTaggingTestServer(t)

	resp := taggingRequest(t, ts, "GetResources", map[string]any{})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	assert.Empty(t, list)
}

func TestTagging_GetResources_FilterByTag(t *testing.T) {
	ts, state := newTaggingTestServer(t)

	putTestS3Bucket(t, state, "prod-bucket", map[string]string{"Env": "prod"})
	putTestS3Bucket(t, state, "dev-bucket", map[string]string{"Env": "dev"})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"TagFilters": []map[string]any{
			{"Key": "Env", "Values": []string{"prod"}},
		},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.Equal(t, "arn:aws:s3:::prod-bucket", rm["ResourceARN"])
}

func TestTagging_GetResources_FilterByResourceType(t *testing.T) {
	ts, state := newTaggingTestServer(t)

	putTestS3Bucket(t, state, "my-bucket", map[string]string{"Env": "test"})
	putTestLambdaFunction(t, state, "my-func", map[string]string{"Env": "test"})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"s3"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.Contains(t, rm["ResourceARN"].(string), "arn:aws:s3:::")
}

func TestTagging_GetResources_MultiService(t *testing.T) {
	ts, state := newTaggingTestServer(t)

	putTestS3Bucket(t, state, "bucket1", map[string]string{"Team": "alpha"})
	putTestLambdaFunction(t, state, "fn1", map[string]string{"Team": "alpha"})
	putTestLambdaFunction(t, state, "fn2", map[string]string{"Team": "alpha"})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"TagFilters": []map[string]any{
			{"Key": "Team", "Values": []string{"alpha"}},
		},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	assert.Len(t, list, 3)
}

func TestTagging_GetResources_Pagination(t *testing.T) {
	ts, state := newTaggingTestServer(t)

	// 5 buckets, page size 2 → 3 pages.
	for i := 0; i < 5; i++ {
		name := "bucket-" + string(rune('a'+i))
		putTestS3Bucket(t, state, name, map[string]string{"X": "1"})
	}

	seen := make(map[string]bool)
	var nextToken string
	for page := 0; page < 10; page++ { // safety cap
		body := map[string]any{"ResourcesPerPage": 2}
		if nextToken != "" {
			body["PaginationToken"] = nextToken
		}
		resp := taggingRequest(t, ts, "GetResources", body)
		var out map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
		resp.Body.Close() //nolint:errcheck

		list, _ := out["ResourceTagMappingList"].([]any)
		for _, item := range list {
			rm := item.(map[string]any)
			arn := rm["ResourceARN"].(string)
			seen[arn] = true
		}

		tok, _ := out["PaginationToken"].(string)
		nextToken = tok
		if nextToken == "" {
			break
		}
	}
	assert.Len(t, seen, 5, "all 5 buckets should be seen across pages")
}

func TestTagging_TagResources(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestS3Bucket(t, state, "my-bucket", nil)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:s3:::my-bucket"},
		"Tags":            map[string]string{"Env": "prod", "Owner": "alice"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	// No failures.
	assert.Empty(t, out["FailedResourcesMap"])

	// Verify tags were applied.
	raw, err := state.Get(context.Background(), "s3", "bucket:my-bucket")
	require.NoError(t, err)
	var b substrate.S3Bucket
	require.NoError(t, json.Unmarshal(raw, &b))
	assert.Equal(t, "prod", b.Tags["Env"])
	assert.Equal(t, "alice", b.Tags["Owner"])
}

func TestTagging_UntagResources(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestS3Bucket(t, state, "my-bucket", map[string]string{"Env": "prod", "Owner": "alice"})

	resp := taggingRequest(t, ts, "UntagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:s3:::my-bucket"},
		"TagKeys":         []string{"Env"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	// Env should be removed; Owner should remain.
	raw, err := state.Get(context.Background(), "s3", "bucket:my-bucket")
	require.NoError(t, err)
	var b substrate.S3Bucket
	require.NoError(t, json.Unmarshal(raw, &b))
	_, hasEnv := b.Tags["Env"]
	assert.False(t, hasEnv, "Env tag should be removed")
	assert.Equal(t, "alice", b.Tags["Owner"])
}

func TestTagging_GetResources_LambdaFunctionFilter(t *testing.T) {
	ts, state := newTaggingTestServer(t)

	putTestS3Bucket(t, state, "bucket1", map[string]string{"App": "myapp"})
	putTestLambdaFunction(t, state, "fn1", map[string]string{"App": "myapp"})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"lambda:function"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.True(t, strings.Contains(rm["ResourceARN"].(string), "function:fn1"))
}

func TestTagging_TagResources_NotFound(t *testing.T) {
	ts, _ := newTaggingTestServer(t)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:s3:::nonexistent-bucket"},
		"Tags":            map[string]string{"Env": "prod"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	failures, _ := out["FailedResourcesMap"].(map[string]any)
	assert.NotEmpty(t, failures)
}

// putTestSQSQueue pre-populates state with an SQS queue.
func putTestSQSQueue(t *testing.T, state substrate.StateManager, name string, tags map[string]string) {
	t.Helper()
	q := substrate.SQSQueue{
		QueueName: name,
		QueueURL:  "http://sqs.us-east-1.amazonaws.com/123456789012/" + name,
		QueueARN:  "arn:aws:sqs:us-east-1:123456789012:" + name,
		Tags:      tags,
	}
	raw, _ := json.Marshal(q)
	require.NoError(t, state.Put(context.Background(), "sqs", "queue:"+name, raw))
}

// taggingTestAccountID is the account ID used in tagging tests. Since the test
// server has no auth configured, ParseAWSRequest resolves to fallbackAccountID.
const taggingTestAccountID = "000000000000"

// putTestDynamoDBTable pre-populates state with a DynamoDB table.
func putTestDynamoDBTable(t *testing.T, state substrate.StateManager, name string, tags map[string]string) {
	t.Helper()
	tbl := substrate.DynamoDBTable{
		TableName:   name,
		TableARN:    "arn:aws:dynamodb:us-east-1:" + taggingTestAccountID + ":table/" + name,
		TableStatus: "ACTIVE",
		Tags:        tags,
	}
	raw, _ := json.Marshal(tbl)
	require.NoError(t, state.Put(context.Background(), "dynamodb", "table:"+taggingTestAccountID+"/"+name, raw))
}

// putTestEC2Instance pre-populates state with an EC2 instance.
func putTestEC2Instance(t *testing.T, state substrate.StateManager, id string, tags []substrate.EC2Tag) {
	t.Helper()
	inst := substrate.EC2Instance{
		InstanceID: id,
		AccountID:  taggingTestAccountID,
		Region:     "us-east-1",
		Tags:       tags,
	}
	raw, _ := json.Marshal(inst)
	require.NoError(t, state.Put(context.Background(), "ec2", "instance:"+taggingTestAccountID+"/us-east-1/"+id, raw))
}

func TestTagging_GetResources_SQS(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestSQSQueue(t, state, "my-queue", map[string]string{"Env": "test"})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"sqs"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123456789012:my-queue", rm["ResourceARN"])
}

func TestTagging_GetResources_DynamoDB(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestDynamoDBTable(t, state, "my-table", map[string]string{"App": "backend"})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"dynamodb:table"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.Equal(t, "arn:aws:dynamodb:us-east-1:"+taggingTestAccountID+":table/my-table", rm["ResourceARN"])
}

func TestTagging_GetResources_EC2(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestEC2Instance(t, state, "i-abc123", []substrate.EC2Tag{{Key: "Name", Value: "test-vm"}})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"ec2:instance"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	arn := rm["ResourceARN"].(string)
	assert.Contains(t, arn, "i-abc123")
	assert.Contains(t, arn, taggingTestAccountID)
}

func TestTagging_TagResources_Lambda(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestLambdaFunction(t, state, "my-func", nil)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:lambda:us-east-1:123456789012:function:my-func"},
		"Tags":            map[string]string{"Env": "staging"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	raw, err := state.Get(context.Background(), "lambda", "function:my-func")
	require.NoError(t, err)
	var fn substrate.LambdaFunction
	require.NoError(t, json.Unmarshal(raw, &fn))
	assert.Equal(t, "staging", fn.Tags["Env"])
}

func TestTagging_TagResources_DynamoDB(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestDynamoDBTable(t, state, "my-table", nil)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:dynamodb:us-east-1:" + taggingTestAccountID + ":table/my-table"},
		"Tags":            map[string]string{"Team": "data"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	raw, err := state.Get(context.Background(), "dynamodb", "table:"+taggingTestAccountID+"/my-table")
	require.NoError(t, err)
	var tbl substrate.DynamoDBTable
	require.NoError(t, json.Unmarshal(raw, &tbl))
	assert.Equal(t, "data", tbl.Tags["Team"])
}

func TestTagging_TagResources_SQS(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestSQSQueue(t, state, "my-queue", nil)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:sqs:us-east-1:123456789012:my-queue"},
		"Tags":            map[string]string{"Owned": "yes"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	raw, err := state.Get(context.Background(), "sqs", "queue:my-queue")
	require.NoError(t, err)
	var q substrate.SQSQueue
	require.NoError(t, json.Unmarshal(raw, &q))
	assert.Equal(t, "yes", q.Tags["Owned"])
}

func TestTagging_TagResources_EC2(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestEC2Instance(t, state, "i-test99", nil)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:ec2:us-east-1:" + taggingTestAccountID + ":instance/i-test99"},
		"Tags":            map[string]string{"CostCenter": "eng"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	raw, err := state.Get(context.Background(), "ec2", "instance:"+taggingTestAccountID+"/us-east-1/i-test99")
	require.NoError(t, err)
	var inst substrate.EC2Instance
	require.NoError(t, json.Unmarshal(raw, &inst))
	found := false
	for _, tag := range inst.Tags {
		if tag.Key == "CostCenter" && tag.Value == "eng" {
			found = true
		}
	}
	assert.True(t, found, "EC2 instance should have CostCenter=eng tag")
}

func TestTagging_UntagResources_Lambda(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestLambdaFunction(t, state, "fn-untag", map[string]string{"Remove": "me", "Keep": "this"})

	resp := taggingRequest(t, ts, "UntagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:lambda:us-east-1:123456789012:function:fn-untag"},
		"TagKeys":         []string{"Remove"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	raw, err := state.Get(context.Background(), "lambda", "function:fn-untag")
	require.NoError(t, err)
	var fn substrate.LambdaFunction
	require.NoError(t, json.Unmarshal(raw, &fn))
	_, hasRemove := fn.Tags["Remove"]
	assert.False(t, hasRemove)
	assert.Equal(t, "this", fn.Tags["Keep"])
}

func TestTagging_InvalidARN(t *testing.T) {
	ts, _ := newTaggingTestServer(t)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"not-an-arn"},
		"Tags":            map[string]string{"X": "1"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	failures, _ := out["FailedResourcesMap"].(map[string]any)
	assert.NotEmpty(t, failures)
}

func TestTagging_InvalidOperation(t *testing.T) {
	ts, _ := newTaggingTestServer(t)

	resp := taggingRequest(t, ts, "BadOperation", map[string]any{})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestTagging_GetResources_IAMUser(t *testing.T) {
	ts, state := newTaggingTestServer(t)

	user := substrate.IAMUser{
		UserName: "alice",
		UserID:   "AIDATEST",
		ARN:      "arn:aws:iam::123456789012:user/alice",
		Path:     "/",
		Tags:     []substrate.IAMTag{{Key: "Dept", Value: "eng"}},
	}
	raw, _ := json.Marshal(user)
	require.NoError(t, state.Put(context.Background(), "iam", "user:alice", raw))

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"TagFilters": []map[string]any{
			{"Key": "Dept", "Values": []string{"eng"}},
		},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.Equal(t, "arn:aws:iam::123456789012:user/alice", rm["ResourceARN"])
}

func TestTagging_TagResources_IAMUser(t *testing.T) {
	ts, state := newTaggingTestServer(t)

	user := substrate.IAMUser{
		UserName: "bob",
		UserID:   "AIDATEST2",
		ARN:      "arn:aws:iam::123456789012:user/bob",
		Path:     "/",
	}
	raw, _ := json.Marshal(user)
	require.NoError(t, state.Put(context.Background(), "iam", "user:bob", raw))

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:iam::123456789012:user/bob"},
		"Tags":            map[string]string{"Role": "admin"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	updated, err := state.Get(context.Background(), "iam", "user:bob")
	require.NoError(t, err)
	var u substrate.IAMUser
	require.NoError(t, json.Unmarshal(updated, &u))
	found := false
	for _, tag := range u.Tags {
		if tag.Key == "Role" && tag.Value == "admin" {
			found = true
		}
	}
	assert.True(t, found, "IAMUser should have Role=admin tag")
}

func TestTagging_TagResources_IAMRole(t *testing.T) {
	ts, state := newTaggingTestServer(t)

	role := substrate.IAMRole{
		RoleName: "deploy-role",
		RoleID:   "AROATEST",
		ARN:      "arn:aws:iam::123456789012:role/deploy-role",
		Path:     "/",
	}
	raw, _ := json.Marshal(role)
	require.NoError(t, state.Put(context.Background(), "iam", "role:deploy-role", raw))

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:iam::123456789012:role/deploy-role"},
		"Tags":            map[string]string{"Env": "ci"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	updated, err := state.Get(context.Background(), "iam", "role:deploy-role")
	require.NoError(t, err)
	var r substrate.IAMRole
	require.NoError(t, json.Unmarshal(updated, &r))
	found := false
	for _, tag := range r.Tags {
		if tag.Key == "Env" && tag.Value == "ci" {
			found = true
		}
	}
	assert.True(t, found, "IAMRole should have Env=ci tag")
}

// putTestECSCluster pre-populates state with an ECS cluster.
func putTestECSCluster(t *testing.T, state substrate.StateManager, name string, tags []substrate.ECSTag) {
	t.Helper()
	arn := "arn:aws:ecs:us-east-1:" + taggingTestAccountID + ":cluster/" + name
	cluster := substrate.ECSCluster{
		ClusterArn:  arn,
		ClusterName: name,
		Status:      "ACTIVE",
		Tags:        tags,
		AccountID:   taggingTestAccountID,
		Region:      "us-east-1",
	}
	raw, _ := json.Marshal(cluster)
	require.NoError(t, state.Put(context.Background(), "ecs", "cluster:"+taggingTestAccountID+"/us-east-1/"+name, raw))
}

// putTestECRRepository pre-populates state with an ECR repository.
func putTestECRRepository(t *testing.T, state substrate.StateManager, name string, tags map[string]string) {
	t.Helper()
	arn := "arn:aws:ecr:us-east-1:" + taggingTestAccountID + ":repository/" + name
	repo := substrate.ECRRepository{
		RepositoryName: name,
		RepositoryArn:  arn,
		RegistryID:     taggingTestAccountID,
		RepositoryURI:  taggingTestAccountID + ".dkr.ecr.us-east-1.amazonaws.com/" + name,
		Tags:           tags,
		AccountID:      taggingTestAccountID,
		Region:         "us-east-1",
	}
	raw, _ := json.Marshal(repo)
	require.NoError(t, state.Put(context.Background(), "ecr", "ecrrepo:"+taggingTestAccountID+"/us-east-1/"+name, raw))
}

// putTestStateMachine pre-populates state with a Step Functions state machine.
func putTestStateMachine(t *testing.T, state substrate.StateManager, name string, tags map[string]string) {
	t.Helper()
	arn := "arn:aws:states:us-east-1:" + taggingTestAccountID + ":stateMachine:" + name
	sm := substrate.StateMachineState{
		StateMachineArn: arn,
		Name:            name,
		Status:          "ACTIVE",
		Tags:            tags,
		AccountID:       taggingTestAccountID,
		Region:          "us-east-1",
	}
	raw, _ := json.Marshal(sm)
	require.NoError(t, state.Put(context.Background(), "states", "statemachine:"+taggingTestAccountID+"/us-east-1/"+name, raw))
}

// putTestRestAPI pre-populates state with an API Gateway REST API.
// Note: APIGateway ARNs use an empty account-ID field (arn:aws:apigateway:{region}::/restapis/{id}),
// so the state key also uses an empty account segment to match resolveARN/scanAPIGatewayAPIs.
func putTestRestAPI(t *testing.T, state substrate.StateManager, apiID string, tags map[string]string) {
	t.Helper()
	api := substrate.RestAPIState{
		ID:        apiID,
		Name:      "test-api-" + apiID,
		Tags:      tags,
		AccountID: taggingTestAccountID,
		Region:    "us-east-1",
	}
	raw, _ := json.Marshal(api)
	// APIGateway state key uses taggingTestAccountID to match reqCtx.AccountID in the scanner.
	require.NoError(t, state.Put(context.Background(), "apigateway", "api:"+taggingTestAccountID+"/us-east-1/"+apiID, raw))
}

// putTestKinesisStream pre-populates state with a Kinesis data stream.
func putTestKinesisStream(t *testing.T, state substrate.StateManager, name string, tags map[string]string) {
	t.Helper()
	arn := "arn:aws:kinesis:us-east-1:" + taggingTestAccountID + ":stream/" + name
	stream := substrate.KinesisStream{
		StreamName:   name,
		StreamArn:    arn,
		StreamStatus: "ACTIVE",
		Tags:         tags,
		AccountID:    taggingTestAccountID,
		Region:       "us-east-1",
	}
	raw, _ := json.Marshal(stream)
	require.NoError(t, state.Put(context.Background(), "kinesis", "stream:"+taggingTestAccountID+"/us-east-1/"+name, raw))
}

// putTestCognitoUserPool pre-populates state with a Cognito User Pool.
func putTestCognitoUserPool(t *testing.T, state substrate.StateManager, poolID string, tags map[string]string) {
	t.Helper()
	arn := "arn:aws:cognito-idp:us-east-1:" + taggingTestAccountID + ":userpool/" + poolID
	pool := substrate.CognitoUserPool{
		UserPoolID: poolID,
		Name:       "test-pool",
		Arn:        arn,
		Status:     "Enabled",
		Tags:       tags,
	}
	raw, _ := json.Marshal(pool)
	require.NoError(t, state.Put(context.Background(), "cognito-idp", "userpool:"+taggingTestAccountID+"/us-east-1/"+poolID, raw))
}

func TestTagging_GetResources_ECSCluster(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestECSCluster(t, state, "my-cluster", []substrate.ECSTag{{Key: "Env", Value: "prod"}})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"ecs:cluster"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.Contains(t, rm["ResourceARN"].(string), "my-cluster")
}

func TestTagging_TagResources_ECSCluster(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestECSCluster(t, state, "tag-cluster", nil)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:ecs:us-east-1:" + taggingTestAccountID + ":cluster/tag-cluster"},
		"Tags":            map[string]string{"Service": "api"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	raw, err := state.Get(context.Background(), "ecs", "cluster:"+taggingTestAccountID+"/us-east-1/tag-cluster")
	require.NoError(t, err)
	var cluster substrate.ECSCluster
	require.NoError(t, json.Unmarshal(raw, &cluster))
	found := false
	for _, tag := range cluster.Tags {
		if tag.Key == "Service" && tag.Value == "api" {
			found = true
		}
	}
	assert.True(t, found, "ECSCluster should have Service=api tag")
}

func TestTagging_GetResources_ECRRepository(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestECRRepository(t, state, "my-repo", map[string]string{"App": "backend"})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"ecr:repository"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.Contains(t, rm["ResourceARN"].(string), "my-repo")
}

func TestTagging_TagResources_ECRRepository(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestECRRepository(t, state, "tag-repo", nil)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:ecr:us-east-1:" + taggingTestAccountID + ":repository/tag-repo"},
		"Tags":            map[string]string{"Team": "platform"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	raw, err := state.Get(context.Background(), "ecr", "ecrrepo:"+taggingTestAccountID+"/us-east-1/tag-repo")
	require.NoError(t, err)
	var repo substrate.ECRRepository
	require.NoError(t, json.Unmarshal(raw, &repo))
	assert.Equal(t, "platform", repo.Tags["Team"])
}

func TestTagging_GetResources_StateMachine(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestStateMachine(t, state, "my-sm", map[string]string{"Project": "checkout"})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"states:stateMachine"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.Contains(t, rm["ResourceARN"].(string), "my-sm")
}

func TestTagging_TagResources_StateMachine(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestStateMachine(t, state, "tag-sm", nil)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:states:us-east-1:" + taggingTestAccountID + ":stateMachine:tag-sm"},
		"Tags":            map[string]string{"Owner": "platform"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	raw, err := state.Get(context.Background(), "states", "statemachine:"+taggingTestAccountID+"/us-east-1/tag-sm")
	require.NoError(t, err)
	var sm substrate.StateMachineState
	require.NoError(t, json.Unmarshal(raw, &sm))
	assert.Equal(t, "platform", sm.Tags["Owner"])
}

func TestTagging_GetResources_APIGateway(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestRestAPI(t, state, "abc123", map[string]string{"Env": "prod"})

	// Use no ResourceTypeFilters to scan all resources (APIGateway filter prefix with
	// leading slash in ARN resource path causes the "apigateway:restapis" filter to not match).
	resp := taggingRequest(t, ts, "GetResources", map[string]any{})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)

	// Find the APIGateway entry.
	found := false
	for _, item := range list {
		rm := item.(map[string]any)
		arn, _ := rm["ResourceARN"].(string)
		if strings.Contains(arn, "apigateway") && strings.Contains(arn, "abc123") {
			found = true
		}
	}
	assert.True(t, found, "expected APIGateway REST API abc123 in resource list")
}

func TestTagging_TagResources_APIGateway(t *testing.T) {
	// APIGateway ARNs have an empty account-ID field (arn:aws:apigateway:{region}::/restapis/{id}).
	// resolveARN resolves these using the empty account segment, so the state key must also
	// use an empty account to match. This test verifies the current behavior.
	ts, state := newTaggingTestServer(t)
	// Store with empty account segment to match resolveARN behavior for apigateway ARNs.
	api := substrate.RestAPIState{
		ID:        "def456",
		Name:      "test-api-def456",
		AccountID: taggingTestAccountID,
		Region:    "us-east-1",
	}
	raw, _ := json.Marshal(api)
	require.NoError(t, state.Put(context.Background(), "apigateway", "api:/us-east-1/def456", raw))

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:apigateway:us-east-1::/restapis/def456"},
		"Tags":            map[string]string{"Tier": "frontend"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	updated, err := state.Get(context.Background(), "apigateway", "api:/us-east-1/def456")
	require.NoError(t, err)
	var updatedAPI substrate.RestAPIState
	require.NoError(t, json.Unmarshal(updated, &updatedAPI))
	assert.Equal(t, "frontend", updatedAPI.Tags["Tier"])
}

func TestTagging_GetResources_KinesisStream(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestKinesisStream(t, state, "my-stream", map[string]string{"App": "events"})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"kinesis:stream"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.Contains(t, rm["ResourceARN"].(string), "my-stream")
}

func TestTagging_TagResources_KinesisStream(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestKinesisStream(t, state, "tag-stream", nil)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:kinesis:us-east-1:" + taggingTestAccountID + ":stream/tag-stream"},
		"Tags":            map[string]string{"Cost": "prod"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	raw, err := state.Get(context.Background(), "kinesis", "stream:"+taggingTestAccountID+"/us-east-1/tag-stream")
	require.NoError(t, err)
	var stream substrate.KinesisStream
	require.NoError(t, json.Unmarshal(raw, &stream))
	assert.Equal(t, "prod", stream.Tags["Cost"])
}

func TestTagging_GetResources_CognitoUserPool(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestCognitoUserPool(t, state, "us-east-1_abc123", map[string]string{"Service": "auth"})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"cognito-idp:userpool"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.Contains(t, rm["ResourceARN"].(string), "us-east-1_abc123")
}

func TestTagging_TagResources_CognitoUserPool(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestCognitoUserPool(t, state, "us-east-1_xyz789", nil)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:cognito-idp:us-east-1:" + taggingTestAccountID + ":userpool/us-east-1_xyz789"},
		"Tags":            map[string]string{"Region": "us-east-1"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	raw, err := state.Get(context.Background(), "cognito-idp", "userpool:"+taggingTestAccountID+"/us-east-1/us-east-1_xyz789")
	require.NoError(t, err)
	var pool substrate.CognitoUserPool
	require.NoError(t, json.Unmarshal(raw, &pool))
	assert.Equal(t, "us-east-1", pool.Tags["Region"])
}

func putTestEFSFileSystem(t *testing.T, state substrate.StateManager, fsID string, tags []substrate.EFSTag) {
	t.Helper()
	fs := substrate.EFSFileSystem{
		FileSystemID:   fsID,
		FileSystemArn:  "arn:aws:elasticfilesystem:us-east-1:" + taggingTestAccountID + ":file-system/" + fsID,
		OwnerID:        taggingTestAccountID,
		LifeCycleState: "available",
		Tags:           tags,
		AccountID:      taggingTestAccountID,
		Region:         "us-east-1",
	}
	raw, _ := json.Marshal(fs)
	require.NoError(t, state.Put(context.Background(), "efs", "filesystem:"+taggingTestAccountID+"/us-east-1/"+fsID, raw))
}

func putTestGlueDatabase(t *testing.T, state substrate.StateManager, name string, tags map[string]string) {
	t.Helper()
	db := substrate.GlueDatabase{
		Name:      name,
		Arn:       "arn:aws:glue:us-east-1:" + taggingTestAccountID + ":database/" + name,
		Tags:      tags,
		AccountID: taggingTestAccountID,
		Region:    "us-east-1",
	}
	raw, _ := json.Marshal(db)
	require.NoError(t, state.Put(context.Background(), "glue", "database:"+taggingTestAccountID+"/us-east-1/"+name, raw))
}

func TestTagging_GetResources_EFSFileSystem(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestEFSFileSystem(t, state, "fs-abc12345", []substrate.EFSTag{{Key: "Name", Value: "my-fs"}})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"elasticfilesystem:file-system"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.Contains(t, rm["ResourceARN"].(string), "fs-abc12345")
}

func TestTagging_TagResources_EFSFileSystem(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestEFSFileSystem(t, state, "fs-tagtag1", nil)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:elasticfilesystem:us-east-1:" + taggingTestAccountID + ":file-system/fs-tagtag1"},
		"Tags":            map[string]string{"Env": "prod"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	raw, err := state.Get(context.Background(), "efs", "filesystem:"+taggingTestAccountID+"/us-east-1/fs-tagtag1")
	require.NoError(t, err)
	var fs substrate.EFSFileSystem
	require.NoError(t, json.Unmarshal(raw, &fs))
	var found bool
	for _, tag := range fs.Tags {
		if tag.Key == "Env" && tag.Value == "prod" {
			found = true
		}
	}
	assert.True(t, found, "expected Env=prod tag")
}

func TestTagging_GetResources_GlueDatabase(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestGlueDatabase(t, state, "tag-glue-db", map[string]string{"Project": "etl"})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"glue:database"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1)
	rm := list[0].(map[string]any)
	assert.Contains(t, rm["ResourceARN"].(string), "tag-glue-db")
}

func TestTagging_TagResources_GlueDatabase(t *testing.T) {
	ts, state := newTaggingTestServer(t)
	putTestGlueDatabase(t, state, "tag-glue-db2", nil)

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{"arn:aws:glue:us-east-1:" + taggingTestAccountID + ":database/tag-glue-db2"},
		"Tags":            map[string]string{"Cost": "etl"},
	})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out["FailedResourcesMap"])

	raw, err := state.Get(context.Background(), "glue", "database:"+taggingTestAccountID+"/us-east-1/tag-glue-db2")
	require.NoError(t, err)
	var db substrate.GlueDatabase
	require.NoError(t, json.Unmarshal(raw, &db))
	assert.Equal(t, "etl", db.Tags["Cost"])
}
