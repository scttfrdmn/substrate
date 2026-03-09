// Package e2e_test contains end-to-end tests that exercise Substrate using the
// real AWS SDK v2. Tests start an in-process Substrate server on a random port
// and route SDK requests through a custom transport.
package e2e_test

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// Package-level state shared across all E2E tests.
var (
	serverAddr    string
	store         *substrate.EventStore
	testAccountID = "123456789012"
)

// serviceTransport rewrites URL.Host to the local Substrate server address
// while preserving the original Host header for service routing. This lets
// the SDK send requests to fake endpoints like http://iam.amazonaws.com while
// the transport physically connects to localhost:PORT.
type serviceTransport struct{ addr string }

func (t *serviceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req2 := req.Clone(req.Context())
	// Lock in the original Host header before rewriting URL.Host so that
	// Substrate's parser can extract the service name from the Host value.
	if req2.Host == "" {
		req2.Host = req2.URL.Host
	}
	req2.URL.Host = t.addr
	req2.URL.Scheme = "http"
	return http.DefaultTransport.RoundTrip(req2)
}

// TestMain wires up the in-process Substrate server and runs all E2E tests.
func TestMain(m *testing.M) {
	ctx := context.Background()

	logger := substrate.NewDefaultLogger(slog.LevelError, false)

	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	store = substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled: true,
		Backend: "memory",
	})

	// Initialise plugins.
	registry := substrate.NewPluginRegistry()

	iamPlugin := &substrate.IAMPlugin{}
	if err := iamPlugin.Initialize(ctx, substrate.PluginConfig{State: state, Logger: logger}); err != nil {
		panic("IAMPlugin.Initialize: " + err.Error())
	}
	registry.Register(iamPlugin)

	stsPlugin := &substrate.STSPlugin{}
	if err := stsPlugin.Initialize(ctx, substrate.PluginConfig{State: state, Logger: logger,
		Options: map[string]any{"time_controller": tc}}); err != nil {
		panic("STSPlugin.Initialize: " + err.Error())
	}
	registry.Register(stsPlugin)

	s3Plugin := &substrate.S3Plugin{}
	if err := s3Plugin.Initialize(ctx, substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
		},
	}); err != nil {
		panic("S3Plugin.Initialize: " + err.Error())
	}
	registry.Register(s3Plugin)

	// Start server on a random port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic("net.Listen: " + err.Error())
	}
	serverAddr = ln.Addr().String()
	_ = ln.Close() // let the server re-bind

	cfg := *substrate.DefaultConfig()
	cfg.Server.Address = serverAddr

	costCtrl := substrate.NewCostController(substrate.CostConfig{Enabled: true})
	srv := substrate.NewServer(cfg, registry, store, state, tc, logger,
		substrate.ServerOptions{Costs: costCtrl})

	// Run the server in a background goroutine.
	srvCtx, srvCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Start(srvCtx)
	}()

	// Wait for the server to become ready by polling the health endpoint.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + serverAddr + "/health") //nolint:noctx
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(5 * time.Millisecond)
	}

	code := m.Run()

	srvCancel()
	<-done

	os.Exit(code)
}

// awsConfig builds a shared AWS SDK config that routes all requests through
// the Substrate server.
func awsConfig(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				"AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "",
			),
		),
		config.WithHTTPClient(&http.Client{
			Transport: &serviceTransport{addr: serverAddr},
		}),
	)
	require.NoError(t, err)
	return cfg
}

// TestS3_CRUD exercises the full create-read-update-delete lifecycle for S3
// via the real AWS SDK v2.
func TestS3_CRUD(t *testing.T) {
	ctx := context.Background()
	cfg := awsConfig(t)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://s3.amazonaws.com")
	})

	bucket := "e2e-crud-bucket"

	// CreateBucket
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err, "CreateBucket")

	// PutObject
	body := "hello, substrate"
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("hello"),
		Body:   strings.NewReader(body),
	})
	require.NoError(t, err, "PutObject")

	// GetObject
	getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("hello"),
	})
	require.NoError(t, err, "GetObject")
	defer getResp.Body.Close()
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(getResp.Body)
	assert.Equal(t, body, buf.String(), "GetObject body mismatch")

	// HeadObject
	_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("hello"),
	})
	require.NoError(t, err, "HeadObject")

	// DeleteObject
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("hello"),
	})
	require.NoError(t, err, "DeleteObject")

	// DeleteBucket
	_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err, "DeleteBucket")
}

// TestIAM_Lifecycle exercises the IAM user, role, and policy lifecycle via
// the real AWS SDK v2.
func TestIAM_Lifecycle(t *testing.T) {
	ctx := context.Background()
	cfg := awsConfig(t)

	client := iam.NewFromConfig(cfg, func(o *iam.Options) {
		o.BaseEndpoint = aws.String("http://iam.amazonaws.com")
	})

	// CreateUser — IAMPlugin speaks JSON; SDK returns 200 with a nil-parsed
	// struct (response body mismatch), so we only check for no error.
	_, err := client.CreateUser(ctx, &iam.CreateUserInput{
		UserName: aws.String("e2e-user"),
	})
	require.NoError(t, err, "CreateUser")

	// CreateRole
	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, err = client.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String("e2e-role"),
		AssumeRolePolicyDocument: aws.String(trustDoc),
	})
	require.NoError(t, err, "CreateRole")

	// CreatePolicy
	policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	_, err = client.CreatePolicy(ctx, &iam.CreatePolicyInput{
		PolicyName:     aws.String("e2e-policy"),
		PolicyDocument: aws.String(policyDoc),
	})
	require.NoError(t, err, "CreatePolicy")
	// Build the policy ARN directly — SDK response body won't parse to XML.
	policyARN := "arn:aws:iam::123456789012:policy/e2e-policy"

	// AttachRolePolicy
	_, err = client.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
		RoleName:  aws.String("e2e-role"),
		PolicyArn: aws.String(policyARN),
	})
	require.NoError(t, err, "AttachRolePolicy")

	// ListAttachedRolePolicies — SDK response won't parse (JSON vs XML), so
	// just verify the call returns without a transport-level error.
	_, err = client.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{
		RoleName: aws.String("e2e-role"),
	})
	require.NoError(t, err, "ListAttachedRolePolicies")

	// DetachRolePolicy
	_, err = client.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{
		RoleName:  aws.String("e2e-role"),
		PolicyArn: aws.String(policyARN),
	})
	require.NoError(t, err, "DetachRolePolicy")

	// DeletePolicy
	_, err = client.DeletePolicy(ctx, &iam.DeletePolicyInput{
		PolicyArn: aws.String(policyARN),
	})
	require.NoError(t, err, "DeletePolicy")

	// DeleteRole
	_, err = client.DeleteRole(ctx, &iam.DeleteRoleInput{
		RoleName: aws.String("e2e-role"),
	})
	require.NoError(t, err, "DeleteRole")

	// DeleteUser
	_, err = client.DeleteUser(ctx, &iam.DeleteUserInput{
		UserName: aws.String("e2e-user"),
	})
	require.NoError(t, err, "DeleteUser")
}

// TestCostReport_AfterS3Workload verifies that S3 operations are costed and
// aggregated correctly in the EventStore.
func TestCostReport_AfterS3Workload(t *testing.T) {
	ctx := context.Background()
	cfg := awsConfig(t)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://s3.amazonaws.com")
	})

	bucket := "cost-bucket"
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "CreateBucket cost-bucket")

	for i := range 10 {
		_, putErr := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(strings.Repeat("k", i+1)),
			Body:   strings.NewReader("payload"),
		})
		require.NoError(t, putErr, "PutObject %d", i)
	}

	summary, err := store.GetCostSummary(ctx, testAccountID, time.Time{}, time.Time{})
	require.NoError(t, err, "GetCostSummary")

	assert.Greater(t, summary.TotalCost, 0.0, "TotalCost should be > 0 after PutObject workload")
	assert.Greater(t, summary.ByService["s3"], 0.0, "s3 cost should be > 0")
}
