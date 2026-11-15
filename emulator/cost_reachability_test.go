package emulator_test

import (
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// costKeysNotRoutedOperations are cost-table keys whose operation half is not a routed operation
// name, each with the reason it is still reachable. A key outside this list must name a routed
// (service, operation) pair, and an entry here must still be in the table.
var costKeysNotRoutedOperations = map[string]string{
	"opensearch/PUT": "OpenSearch dispatches on the HTTP method and is kept out of operationResolvers " +
		"(its IAM actions are the per-verb es:ESHttp* family), so req.Operation is the verb",
	"opensearch/POST": "as opensearch/PUT",
	"execute-api/RESTAPIRequest": "execute-api routes no operation names; the proxy charges this key on " +
		"the request's behalf for a REST API (cost_internal.go)",
	"execute-api/HTTPAPIRequest": "as execute-api/RESTAPIRequest, for an HTTP API",
}

// TestCostTable_EveryKeyIsReachable is the gate #1202 asks for: every key in the built-in cost table
// is matched by some request substrate routes. Five keys — two OpenSearch entries and three EFS
// entries keyed by the signing name rather than the plugin name — survived for releases because
// nothing checked, and each reported a cost of zero for the workload it was written to price.
func TestCostTable_EveryKeyIsReachable(t *testing.T) {
	t.Parallel()

	table := emulator.DefaultCostTableForTest()
	keys := make([]string, 0, len(table))
	for k := range table {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		svc, op, hasOp := strings.Cut(key, "/")
		routed := emulator.RoutedOperations(svc)
		if !assert.NotNilf(t, routed, "cost key %q names service %q, which is not a registered plugin", key, svc) {
			continue
		}
		if !hasOp {
			continue
		}
		if _, allowed := costKeysNotRoutedOperations[key]; allowed {
			continue
		}
		assert.Truef(t, slices.Contains(routed, op),
			"cost key %q names operation %q, which %s does not route; no request can ever match it", key, op, svc)
	}

	for key := range costKeysNotRoutedOperations {
		_, ok := table[key]
		assert.Truef(t, ok, "allowlisted cost key %q is no longer in the table; remove it from the list", key)
	}
}

// TestCost_FixedKeysPrice asserts the three re-keyed families price the requests they name.
func TestCost_FixedKeysPrice(t *testing.T) {
	t.Parallel()
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})

	for _, tc := range []struct{ service, operation string }{
		{"msk", "CreateCluster"},
		{"msk", "CreateClusterV2"},
		{"efs", "CreateFileSystem"},
		{"efs", "CreateAccessPoint"},
		{"efs", "CreateMountTarget"},
		{"opensearch", "PUT"},
		{"opensearch", "POST"},
	} {
		got := costs.CostForRequest(&emulator.AWSRequest{Service: tc.service, Operation: tc.operation})
		assert.Positivef(t, got, "%s/%s is charged", tc.service, tc.operation)
	}
	assert.Equal(t,
		costs.CostForRequest(&emulator.AWSRequest{Service: "msk", Operation: "CreateCluster"}),
		costs.CostForRequest(&emulator.AWSRequest{Service: "msk", Operation: "CreateClusterV2"}),
		"a cluster costs the same whichever API version created it")
	assert.Zero(t, costs.CostForRequest(&emulator.AWSRequest{Service: "opensearch", Operation: "GET"}),
		"an OpenSearch read is free")
}

// TestCost_OpenSearchWriteIsAttributed drives a document write over the wire and asserts its event
// carries a non-zero cost. The event records the verb as the operation — OpenSearch has no resolver,
// by the catalog's decision — which is the name the re-keyed entry matches.
func TestCost_OpenSearchWriteIsAttributed(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	status, body := osDo(t, ts, http.MethodPut, "/costed/_doc/1", `{"a":1}`)
	require.Equal(t, http.StatusCreated, status, body)

	events, err := ts.Store().GetEvents(t.Context(), emulator.EventFilter{Service: "opensearch"})
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "PUT", events[0].Operation)
	assert.Positive(t, events[0].Cost, "a document write is charged")
}

// TestCost_ExecuteAPIChargesTheStageAndTheLambda drives a request through a REST API stage and
// asserts the one event it records carries both the stage charge and the proxied Lambda
// invocation's. Before #1202 both were zero: execute-api had no cost key, and the invocation went
// through PluginRegistry.RouteRequest, which consults no cost controller.
func TestCost_ExecuteAPIChargesTheStageAndTheLambda(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)

	const fnName = "cost-proxy-fn"
	proxyCreateLambdaFunction(t, ts, fnName)
	apiID := proxyCreateRESTAPI(t, ts)
	resourceID := proxyGetRootResourceID(t, ts, apiID)
	proxyPutMethod(t, ts, apiID, resourceID, "GET")
	lambdaARN := "arn:aws:lambda:us-east-1:123456789012:function:" + fnName
	proxyCreateIntegration(t, ts, apiID, resourceID, "GET",
		"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"+lambdaARN+"/invocations")

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/prod/", nil)
	require.NoError(t, err)
	req.Host = fmt.Sprintf("%s.execute-api.us-east-1.amazonaws.com", apiID)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	events, err := ts.Store().GetEvents(t.Context(), emulator.EventFilter{Service: "execute-api"})
	require.NoError(t, err)
	require.Len(t, events, 1)

	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})
	stage := costs.CostForRequest(&emulator.AWSRequest{Service: "execute-api", Operation: "RESTAPIRequest"})
	invoke := costs.CostForRequest(&emulator.AWSRequest{Service: "lambda", Operation: "Invoke"})
	require.Positive(t, stage)
	require.Positive(t, invoke)
	assert.InDelta(t, stage+invoke, events[0].Cost, 1e-15, "the stage and the Lambda it invoked are both charged")
}

// TestCost_HTTPAPIRequestsAreMeteredIn512KB pins the metering the API Gateway pricing page's HTTP API
// example applies: one billable request per started 512 KB, and a REST API request is always one.
func TestCost_HTTPAPIRequestsAreMeteredIn512KB(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		isV2    bool
		bodyLen int
		want    int
	}{
		{false, 4 << 20, 1},
		{true, 0, 1},
		{true, 512 << 10, 1},
		{true, 512<<10 + 1, 2},
		{true, 4608 << 10, 9},
	} {
		assert.Equalf(t, tc.want, emulator.ExecuteAPICostUnitsForTest(tc.isV2, tc.bodyLen), "v2=%v body=%d", tc.isV2, tc.bodyLen)
	}
}
