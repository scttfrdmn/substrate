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
