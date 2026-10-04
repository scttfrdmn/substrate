package emulator_test

import (
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// An MSK cluster ARN names one cluster, not a cluster name (#1204).
//
// The ARN is `…:cluster/{name}/{uuid}-{n}`, and the UUID is what tells a cluster from a later one
// that reuses its name. Resolution used to take the name and discard the UUID, so an ARN naming a
// deleted cluster answered as its replacement: a wrong-resource answer reported as success.

// mskARNCall issues one MSK request and returns its status and body, or the refusal's status and code.
func mskARNCall(t *testing.T, p *emulator.MSKPlugin, ctx *emulator.RequestContext, method, path string) (int, string) {
	t.Helper()
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service: "kafka", HTTPMethod: method, Path: path,
		Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
	})
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return awsErr.HTTPStatus, awsErr.Code
	}
	require.NoError(t, err, "%s %s", method, path)
	return resp.StatusCode, string(resp.Body)
}

func TestMSK_AnARNForADeletedClusterDoesNotResolveToItsReplacement(t *testing.T) {
	t.Parallel()
	p := &emulator.MSKPlugin{}
	ctx, _ := wireSetup(t, p, "req-msk-identity")
	create := func() string {
		return mintedIDMember(t, wireREST(t, p, ctx, "kafka", http.MethodPost, "/v1/clusters",
			mskMinimalCluster("reused")), "clusterArn")
	}

	first := create()
	wireREST(t, p, ctx, "kafka", http.MethodDelete, "/v1/clusters/"+first, nil)
	// The clock is frozen, so the second create happens at the same instant as the first: the old
	// name-and-nanoseconds UUID minted the same ARN twice here.
	second := create()
	require.NotEqual(t, first, second, "a re-created cluster must not reuse its predecessor's ARN")

	for _, path := range []string{"", "/nodes", "/bootstrap-brokers"} {
		status, code := mskARNCall(t, p, ctx, http.MethodGet, "/v1/clusters/"+first+path)
		require.Equalf(t, http.StatusNotFound, status, "GET …%s with the deleted cluster's ARN answered %s", path, code)
		require.Equal(t, "NotFoundException", code, "GET …%s", path)
	}
	status, code := mskARNCall(t, p, ctx, http.MethodGet, "/api/v2/clusters/"+first)
	require.Equal(t, http.StatusNotFound, status, "DescribeClusterV2 with the deleted cluster's ARN answered %s", code)
	status, code = mskARNCall(t, p, ctx, http.MethodDelete, "/v1/clusters/"+first)
	require.Equal(t, http.StatusNotFound, status, "DeleteCluster with the deleted cluster's ARN answered %s", code)

	// The replacement still answers under its own ARN, and only under it.
	status, body := mskARNCall(t, p, ctx, http.MethodGet, "/v1/clusters/"+second)
	require.Equal(t, http.StatusOK, status, "DescribeCluster with the live ARN: %s", body)
	require.Contains(t, body, second)
}

// A store read fault while resolving an ARN is an error, not "cluster not found".
func TestMSK_AStoreFaultResolvingAnARNIsAnError(t *testing.T) {
	t.Parallel()
	fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	p := &emulator.MSKPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State: fault, Logger: emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}))
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-msk-fault", IDs: emulator.NewIDMint("req-msk-fault")}
	arn := mintedIDMember(t, wireREST(t, p, ctx, "kafka", http.MethodPost, "/v1/clusters",
		mskMinimalCluster("faulty")), "clusterArn")

	fault.failGet = "cluster:"
	_, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service: "kafka", HTTPMethod: http.MethodGet, Path: "/v1/clusters/" + arn, Params: map[string]string{},
	})
	require.Error(t, err)
	var awsErr *emulator.AWSError
	require.Falsef(t, errors.As(err, &awsErr), "a store fault was answered as the published %v", awsErr)
}
