package emulator_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestOpenSearchDomainFromHost asserts the domain segment for every host form the developer guide
// documents, and the `_default` every other host resolves to (#1200).
func TestOpenSearchDomainFromHost(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ host, want string }{
		{"search-alpha-abc123.us-east-1.es.amazonaws.com", "alpha"},
		{"search-alpha-abc123.us-east-1.es.amazonaws.com:443", "alpha"},
		{"SEARCH-Alpha-ABC123.us-east-1.es.amazonaws.com", "alpha"},
		{"search-my-app-logs-abc123.us-east-1.es.amazonaws.com", "my-app-logs"},
		{"vpc-alpha-abc123.eu-west-1.es.amazonaws.com", "alpha"},
		{"col1.us-east-1.aoss.amazonaws.com", "aoss.col1"},
		{"search-alpha.us-east-1.es.amazonaws.com", "_default"},   // no identifier
		{"search-alpha-.us-east-1.es.amazonaws.com", "_default"},  // empty identifier
		{"search--abc123.us-east-1.es.amazonaws.com", "_default"}, // empty name
		{"my-domain.us-east-1.es.amazonaws.com", "_default"},      // neither prefix
		{"es.us-east-1.amazonaws.com", "_default"},                // control-plane host
		{"a.b.us-east-1.es.amazonaws.com", "_default"},            // four labels
		{".us-east-1.es.amazonaws.com", "_default"},               // empty first label
		{"search-alpha-abc123.us-east-1.other.amazonaws.com", "_default"},
		{"localhost:4566", "_default"},
		{"", "_default"},
	} {
		assert.Equalf(t, tc.want, emulator.OpenSearchDomainFromHostForTest(tc.host), "host %q", tc.host)
	}
}

// TestOpenSearchControlPlaneOperation asserts the template match names published operations, prefers
// the template with more literal segments, and names nothing for an unpublished path.
func TestOpenSearchControlPlaneOperation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ method, path, want string }{
		{"POST", "/2021-01-01/opensearch/domain", "CreateDomain"},
		{"GET", "/2021-01-01/opensearch/domain/d", "DescribeDomain"},
		{"DELETE", "/2021-01-01/opensearch/domain/d", "DeleteDomain"},
		{"POST", "/2021-01-01/opensearch/domain-info", "DescribeDomains"},
		{"GET", "/2021-01-01/domain", "ListDomainNames"},
		{"GET", "/2021-01-01/opensearch/domain/d/config", "DescribeDomainConfig"},
		{"POST", "/2021-01-01/opensearch/domain/d/config", "UpdateDomainConfig"},
		{"POST", "/2015-01-01/es/domain", "CreateElasticsearchDomain"},
		{"GET", "/2015-01-01/domain", "ListDomainNames"},
		{"PATCH", "/2021-01-01/opensearch/domain", ""},       // no such method
		{"GET", "/2021-01-01/opensearch/domain//config", ""}, // an empty label
		{"GET", "/2021-01-01/nothing/here/at/all", ""},
	} {
		assert.Equalf(t, tc.want, emulator.OpenSearchControlPlaneOperationForTest(tc.method, tc.path), "%s %s", tc.method, tc.path)
	}
}

// TestOpenSearchControlPlaneRefusalNamesAnUnmatchedPath asserts a control-plane path that matches no
// template is still refused in the AWS envelope, naming the method and path instead.
func TestOpenSearchControlPlaneRefusalNamesAnUnmatchedPath(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	status, hdr, body := osCall(t, ts, "es.us-east-1.amazonaws.com", "", "GET", "/2021-01-01/nothing", "")
	assert.Equal(t, 404, status, body)
	assert.Equal(t, "UnknownOperationException", hdr.Get("X-Amzn-ErrorType"))
	assert.Contains(t, body, "GET /2021-01-01/nothing is not supported")
}

// TestChargeOnBehalf covers the guard that refuses a degenerate charge and the pricing of what
// accumulates, including on a nil controller.
func TestChargeOnBehalf(t *testing.T) {
	t.Parallel()
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})
	invoke := &emulator.AWSRequest{Service: "lambda", Operation: "Invoke"}
	unit := costs.CostForRequest(invoke)
	require.Positive(t, unit)

	// Nothing is recorded for a nil context, a context without metadata, a nil request or a
	// non-positive count.
	emulator.ChargeOnBehalfForTest(nil, invoke, 1)
	bare := &emulator.RequestContext{}
	emulator.ChargeOnBehalfForTest(bare, invoke, 1)
	assert.Zero(t, emulator.CostOnBehalfForTest(costs, bare))

	ctx := &emulator.RequestContext{Metadata: map[string]interface{}{}}
	emulator.ChargeOnBehalfForTest(ctx, nil, 1)
	emulator.ChargeOnBehalfForTest(ctx, invoke, 0)
	assert.Zero(t, emulator.CostOnBehalfForTest(costs, ctx))

	emulator.ChargeOnBehalfForTest(ctx, invoke, 1)
	emulator.ChargeOnBehalfForTest(ctx, invoke, 3)
	assert.InDelta(t, 4*unit, emulator.CostOnBehalfForTest(costs, ctx), 1e-15)

	assert.Zero(t, emulator.CostOnBehalfForTest(nil, ctx), "a nil controller prices nothing")
	assert.Zero(t, emulator.CostOnBehalfForTest(costs, nil))
}

// TestBuildV2ProxyEvent_RawQueryStringEncodesEveryKey extends the determinism test to keys that need
// encoding and to a repeated key whose multi-value entry is empty.
func TestBuildV2ProxyEvent_RawQueryStringEncodesEveryKey(t *testing.T) {
	t.Parallel()
	req := &emulator.AWSRequest{
		Operation:        "GET",
		Params:           map[string]string{"a b": "c+d", "k": "v", "é": "ü"},
		MultiValueParams: map[string][]string{"k": {}},
		Headers:          map[string]string{},
	}
	data, err := emulator.BuildV2ProxyEventForTest(req, "api", "prod", "/r")
	require.NoError(t, err)
	var event struct {
		RawQueryString string `json:"rawQueryString"`
	}
	require.NoError(t, json.Unmarshal(data, &event))
	assert.Equal(t, "a+b=c%2Bd&k=v&%C3%A9=%C3%BC", event.RawQueryString)
}
