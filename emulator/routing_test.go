package emulator_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// routingTestRegistry boots the default plugin registry and returns the set of
// registered plugin names, so a routing assertion can be made against what is
// actually reachable rather than against a name written down twice.
func routingTestRegistry(t *testing.T) map[string]bool {
	t.Helper()

	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Unix(0, 0).UTC())
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false})
	require.NoError(t, emulator.RegisterDefaultPlugins(
		context.Background(), registry, state, tc, logger, store, nil))

	names := registry.Names()
	require.NotEmpty(t, names, "the registry must hold plugins for this to assert anything")
	registered := make(map[string]bool, len(names))
	for _, n := range names {
		registered[n] = true
	}
	return registered
}

// TestPluginRouting_MatchesTheRegistry pins the routing table to the registry in
// both directions. The documentation generator enforces the same property, but it
// runs only under `make docs-reference-check`; a plugin registered without routing
// is a plugin no SDK can address, which belongs in the test suite.
func TestPluginRouting_MatchesTheRegistry(t *testing.T) {
	t.Parallel()

	registered := routingTestRegistry(t)
	routing := emulator.PluginRoutingCatalog()

	for name := range registered {
		assert.Contains(t, routing, name,
			"plugin %q is registered but has no entry in emulator/routing.go, so nothing "+
				"asserts that a real client can address it", name)
	}
	for name := range routing {
		assert.True(t, registered[name],
			"routing entry %q names no registered plugin; remove it", name)
	}
}

// TestPluginRouting_EveryIdentifierReachesARegisteredPlugin is the sweep #739 asks
// for: for every plugin, every identifier a real AWS client sends — the X-Amz-Target
// namespace, each endpoint host, each SigV4 signing name — must resolve to a
// *registered* plugin.
//
// Asserting registration rather than string-comparing the name is the point.
// Substrate resolves a service by reducing those three signals to a lowercase
// string, and a reduction that lands on nothing (or on a label like "com",
// "ingest" or "global") answers ServiceNotAvailable while every unit test over the
// plugin's own handlers stays green. That is how SSOPlugin (#561),
// OrganizationsPlugin and ConfigServicePlugin (#580) and EventBridgePlugin (#734)
// each shipped registered and unreachable, and how CloudWatch, Health, CloudTrail
// and Timestream were still unreachable at v0.108.0 (#739).
//
// The target is signal #1, which is why this hides: an unrecognized namespace
// short-circuits the host and signing-name paths that would both have answered
// correctly. And substrate is always reached with --endpoint-url, where the host is
// localhost, so the target is the only signal a caller actually supplies.
func TestPluginRouting_EveryIdentifierReachesARegisteredPlugin(t *testing.T) {
	t.Parallel()

	registered := routingTestRegistry(t)

	for name, route := range emulator.PluginRoutingCatalog() {
		name, route := name, route
		want := name
		if route.RoutesTo != "" {
			want = route.RoutesTo
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if route.TargetPrefix != "" {
				t.Run("target", func(t *testing.T) {
					t.Parallel()
					r := httptest.NewRequest(http.MethodPost, "http://localhost:4566/", nil)
					r.Host = "localhost:4566"
					r.Header.Set("X-Amz-Target", route.TargetPrefix+".ListSomething")

					req, _, err := emulator.ParseAWSRequest(r)
					require.NoError(t, err)
					assert.Equal(t, want, req.Service,
						"X-Amz-Target %q must route to %q", route.TargetPrefix, want)
					assert.True(t, registered[req.Service],
						"X-Amz-Target %q reduces to %q, which is not a registered plugin: "+
							"every client sending it gets ServiceNotAvailable",
						route.TargetPrefix, req.Service)
				})
			}

			for _, host := range route.Hosts {
				host := host
				t.Run("host/"+host, func(t *testing.T) {
					t.Parallel()
					r := httptest.NewRequest(http.MethodPost, "http://"+host+"/", nil)
					r.Host = host

					req, _, err := emulator.ParseAWSRequest(r)
					require.NoError(t, err)
					assert.Equal(t, want, req.Service, "host %q must route to %q", host, want)
					assert.True(t, registered[req.Service],
						"host %q reduces to %q, which is not a registered plugin", host, req.Service)
				})
			}

			for _, signing := range route.SigningNames {
				signing := signing
				t.Run("signing/"+signing, func(t *testing.T) {
					t.Parallel()
					r := httptest.NewRequest(http.MethodPost, "http://localhost:4566/", nil)
					r.Host = "localhost:4566"
					r.Header.Set("Authorization",
						"AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/us-east-1/"+
							signing+"/aws4_request, SignedHeaders=host, Signature=fake")

					req, _, err := emulator.ParseAWSRequest(r)
					require.NoError(t, err)
					assert.Equal(t, want, req.Service,
						"SigV4 signing name %q must route to %q", signing, want)
					assert.True(t, registered[req.Service],
						"SigV4 signing name %q reduces to %q, which is not a registered plugin",
						signing, req.Service)
				})
			}
		})
	}
}

// TestPluginRouting_RowsAreComplete asserts the table's own invariants, so a row
// added without them fails here rather than silently weakening the sweep above.
//
// A row with no host and no signing name asserts nothing, and a JSON plugin with
// no target prefix leaves the one signal a --endpoint-url caller supplies untested
// — which is exactly the gap CloudWatch sat in, present in smithyServiceAliases
// and absent from targetServiceAliases. Every row cites a source, and the citation
// must name *which* source: botocore and aws-sdk-go-v2 disagree about CloudTrail's
// namespace, so a table citing "the SDK" would have recorded a prefix half the
// world's clients do not send. A row that deliberately routes elsewhere, or that
// leaves an identifier out, has to say why in prose.
func TestPluginRouting_RowsAreComplete(t *testing.T) {
	t.Parallel()

	routing := emulator.PluginRoutingCatalog()
	for name, route := range routing {
		name, route := name, route
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.NotEmpty(t, route.Display, "row must carry a display name for docs/services.md")
			assert.NotEmpty(t, route.Protocol, "row must name the protocol the plugin speaks")
			assert.NotEmpty(t, route.Source, "row must cite where its identifiers were read from")
			assert.NotEmpty(t, route.Hosts, "row must give at least one example endpoint host")
			assert.NotEmpty(t, route.SigningNames, "row must give at least one SigV4 signing name")

			if route.Protocol == "JSON" {
				assert.NotEmpty(t, route.TargetPrefix,
					"a JSON plugin is addressed by X-Amz-Target, which is the only signal a "+
						"--endpoint-url caller supplies; record the prefix its model declares")
			}
			if route.RoutesTo != "" {
				assert.Contains(t, routing, route.RoutesTo,
					"RoutesTo must name a plugin in this table")
				assert.NotEmpty(t, route.Why, "a row that routes elsewhere must explain why")
			}
			for _, host := range route.Hosts {
				assert.True(t, strings.HasSuffix(host, ".amazonaws.com"),
					"host %q must be a complete example endpoint host", host)
			}
		})
	}
}

// TestPluginRouting_TheFourMissesFoundBy739 pins each of the four reachability
// misses the #739 sweep turned up, separately from the table-driven sweep, so the
// reason each one hid is recorded next to the assertion.
//
// Every one of these fails against v0.108.0. Each was confirmed against a real
// client before the fix — the AWS CLI, not aws-sdk-go-v2, because the Go SDK
// already routed CloudWatch and CloudTrail correctly, which is precisely why
// substrate's suite and test/e2e were green over endpoints no CLI could reach.
func TestPluginRouting_TheFourMissesFoundBy739(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// target and host are the identifier under test; exactly one is set.
		target string
		host   string
		want   string
		why    string
	}{
		{
			name:   "cloudwatch target prefix",
			target: "GraniteServiceVersion20100801.ListMetrics",
			want:   "monitoring",
			why: "CloudWatch's model declares protocols ['smithy-rpc-v2-cbor','json','query'] " +
				"and botocore resolves json first, so every AWS CLI and boto3 call sends this " +
				"target. The name was in smithyServiceAliases for the CBOR URL path — the path " +
				"aws-sdk-go-v2 takes — and absent from targetServiceAliases.",
		},
		{
			name:   "health target prefix",
			target: "AWSHealth_20160804.DescribeEvents",
			want:   "health",
			why: "The table held an invented \"healthservice\" in the slot the real prefix " +
				"needed: a guessed name no client sends, which is the #561 failure repeated.",
		},
		{
			name: "health global endpoint",
			host: "global.health.amazonaws.com",
			want: "health",
			why: "Health is not regionalized, so its partition endpoint carries a literal " +
				"\"global\" label ahead of the service name and reduced to \"global\".",
		},
		{
			name:   "cloudtrail fully-qualified target prefix",
			target: "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.DescribeTrails",
			want:   "cloudtrail",
			why: "botocore sends the model's fully-qualified namespace, which splitting at the " +
				"first dot reduced to \"com\". aws-sdk-go-v2 sends the terse CloudTrail_20131101 " +
				"and always worked, so only a CLI or boto3 caller saw the failure. An alias " +
				"cannot fix it — \"com\" prefixes every fully-qualified namespace.",
		},
		{
			name:   "cloudtrail terse target prefix still resolves",
			target: "CloudTrail_20131101.DescribeTrails",
			want:   "cloudtrail",
			why:    "The reduction must not break the form aws-sdk-go-v2 sends.",
		},
		{
			name: "timestream ingest endpoint",
			host: "ingest.timestream.us-east-1.amazonaws.com",
			want: "timestream",
			why: "Endpoint discovery hands a client a host whose first label is the operation " +
				"class, so it reduced to \"ingest\". A --endpoint-url caller was unaffected, " +
				"which is why the suite never saw it.",
		},
		{
			name: "timestream query endpoint",
			host: "query.timestream.us-east-1.amazonaws.com",
			want: "timestream",
			why:  "The query endpoint reduced to \"query\" for the same reason.",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			host := tt.host
			if host == "" {
				host = "localhost:4566"
			}
			r := httptest.NewRequest(http.MethodPost, "http://"+host+"/", nil)
			r.Host = host
			if tt.target != "" {
				r.Header.Set("X-Amz-Target", tt.target)
			}

			req, _, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, tt.want, req.Service, tt.why)
		})
	}
}

// TestPluginRouting_DottedTargetKeepsTheOperation guards the other half of the
// CloudTrail change: reducing a dot-qualified namespace must not disturb the
// operation, which is still whatever follows the final dot.
func TestPluginRouting_DottedTargetKeepsTheOperation(t *testing.T) {
	t.Parallel()

	for target, want := range map[string]string{
		"com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.DescribeTrails": "DescribeTrails",
		"CloudTrail_20131101.CreateTrail":                                       "CreateTrail",
		"DynamoDB_20120810.GetItem":                                             "GetItem",
	} {
		target, want := target, want
		t.Run(want, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodPost, "http://localhost:4566/", nil)
			r.Host = "localhost:4566"
			r.Header.Set("X-Amz-Target", target)

			req, _, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err)
			assert.Equal(t, want, req.Operation)
		})
	}
}
