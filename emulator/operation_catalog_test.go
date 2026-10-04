package emulator_test

// The exported operation catalog (#1095).
//
// The catalog is generated from each plugin's dispatch switch by
// cmd/gen-operation-catalog, whose own tests cover the extraction. These cover the read
// side — what a consumer sees — and the two facts the tree now asserts rather than leaving
// a reader to re-derive: that the catalog covers exactly the registered plugins, and how
// many operations substrate routes.

import (
	"context"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// routedOperationTotal is the number of operations substrate routes.
	//
	// #1095's fifth criterion asks that this become a fact the tree asserts. Two careful
	// hand counts of the same tree had produced 945 and 950, a later one ≥1,009, and none
	// could be checked — which was the argument for generating the catalog in the first
	// place. Updating this figure is the deliberate half of adding or removing an
	// operation; `make operation-catalog` regenerates the catalog and this test then says
	// by how much the total moved.
	routedOperationTotal = 1032

	// routedPluginTotal is the number of plugins RegisterDefaultPlugins registers. The
	// coverage matrix in docs/services.md reports the same figure from the same source.
	routedPluginTotal = 67
)

// registeredPluginNames boots the default registry and returns the names it files plugins
// under — the same keys the catalog uses.
func registeredPluginNames(t *testing.T) []string {
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
	slices.Sort(names)
	return names
}

// TestOperationCatalog_CoversEveryRegisteredPluginAndNothingElse is the assertion that makes
// the catalog usable as a source of truth rather than as a snapshot.
//
// A plugin missing from it would report zero routed operations, which reads as a service
// that routes nothing rather than as a hole in the catalog — and a drift check built on it
// would pass by having nothing to check. The generator refuses to emit either kind of
// mismatch; this is the run-time half, against the registry the server actually boots.
func TestOperationCatalog_CoversEveryRegisteredPluginAndNothingElse(t *testing.T) {
	t.Parallel()

	assert.Equal(t, registeredPluginNames(t), emulator.RoutedServices(),
		"the catalog and the registry must name the same plugins; run `make operation-catalog`")
}

// TestOperationCatalog_AssertsHowManyOperationsSubstrateRoutes settles #1095's 945-vs-950
// disagreement by making the count a fact that fails when it changes.
func TestOperationCatalog_AssertsHowManyOperationsSubstrateRoutes(t *testing.T) {
	t.Parallel()

	services := emulator.RoutedServices()
	assert.Len(t, services, routedPluginTotal)

	total := 0
	for _, service := range services {
		total += len(emulator.RoutedOperations(service))
	}
	assert.Equal(t, routedOperationTotal, total,
		"substrate routes %d operations, not %d; update routedOperationTotal deliberately",
		total, routedOperationTotal)
}

// TestOperationCatalog_ReportsTheOperationsTheDispatchSwitchRoutes spot-checks services
// whose figures were in dispute, and the two directions a bad extraction shows up in.
//
// EC2 is the one #1095 names: its dispatch switch has 92 cases where a naive `case "…"`
// grep over ec2_plugin.go returns 106, because filter and attribute names are `case` labels
// too. Config Service and Organizations are the opposite error — they route through claim
// chains, so *none* of their operations appears in a switch inside HandleRequest, and every
// hand count missed all 59.
func TestOperationCatalog_ReportsTheOperationsTheDispatchSwitchRoutes(t *testing.T) {
	t.Parallel()

	counts := map[string]int{
		"ec2":                  92,
		"cloudfront":           17,
		"iam":                  74,
		"s3":                   47,
		"apigateway":           41,
		"organizations":        34,
		"config":               25,
		"dynamodb":             26,
		"sts":                  3,
		"elasticloadbalancing": 24,
	}
	for service, want := range counts {
		assert.Len(t, emulator.RoutedOperations(service), want, "%s", service)
	}

	// A sample of names, because a count alone would pass on a catalog of the right size
	// holding the wrong strings.
	present := map[string]string{
		"ec2":                  "RunInstances",
		"s3":                   "PutObject",
		"organizations":        "CreateAccount",
		"config":               "PutConfigurationRecorder",
		"elasticloadbalancing": "CreateLoadBalancer",

		// CloudFront's GetInvalidation is routed by a strings.HasPrefix guard ahead of the
		// switch, because parseCloudFrontOperation encodes the invalidation id into the
		// operation name ("GetInvalidation:"+invID) so one value carries both ids. The first
		// catalog missed it, and what found the miss was #1015's reverse check: the docs
		// claimed the operation and the catalog did not.
		"cloudfront": "GetInvalidation",
	}
	for service, op := range present {
		assert.Contains(t, emulator.RoutedOperations(service), op)
	}

	assert.NotContains(t, emulator.RoutedOperations("ec2"), "instance-state-name",
		"a filter name is not an operation, which is what separates the catalog from a grep")
	assert.NotContains(t, emulator.RoutedOperations("ec2"), "CreateBucket",
		"an operation belongs to the plugin that routes it")
}

// TestOperationCatalog_TellsAnUnknownServiceFromOneThatRoutesNothing pins the distinction
// the accessor's doc comment promises, and which a caller has to rely on: nil for "not a
// plugin", empty for "routes no operation names".
//
// Two plugins are in the second case by protocol rather than by omission. execute-api serves
// a deployed API's own routes, where AWS's action is the single execute-api:Invoke; opensearch
// dispatches on the HTTP method plus path shape, which is why it is also the one service
// absent from operationResolvers and why AWS publishes per-verb es:ESHttp* actions for it. If
// either ever gains a named operation the generator fails rather than quietly growing the
// entry, so this test and that refusal are the two halves of one decision.
func TestOperationCatalog_TellsAnUnknownServiceFromOneThatRoutesNothing(t *testing.T) {
	t.Parallel()

	assert.Nil(t, emulator.RoutedOperations("not-a-service"),
		"an unregistered name is nil, not empty")

	for _, service := range []string{"execute-api", "opensearch"} {
		ops := emulator.RoutedOperations(service)
		assert.NotNil(t, ops, "%s is a registered plugin", service)
		assert.Empty(t, ops, "%s routes no operation names", service)
	}
}

// TestOperationCatalog_ReturnsCopies guards the generated table against a caller. Both
// accessors read package-level data, so handing out the slice itself would let one consumer
// reorder or truncate what the next one sees — the kind of shared mutable state the package
// otherwise does not have.
func TestOperationCatalog_ReturnsCopies(t *testing.T) {
	t.Parallel()

	first := emulator.RoutedOperations("sts")
	require.NotEmpty(t, first)
	original := slices.Clone(first)
	for i := range first {
		first[i] = "clobbered"
	}
	assert.Equal(t, original, emulator.RoutedOperations("sts"),
		"a caller's writes must not reach the catalog")

	services := emulator.RoutedServices()
	require.NotEmpty(t, services)
	services[0] = "clobbered"
	assert.NotContains(t, emulator.RoutedServices(), "clobbered")
}

// TestOperationCatalog_IsSortedAndFreeOfEmptyNames covers the shape every consumer is
// entitled to assume: #1015's drift check will compare the catalog against documentation
// tables, and an unsorted or duplicated list turns that comparison into noise.
func TestOperationCatalog_IsSortedAndFreeOfEmptyNames(t *testing.T) {
	t.Parallel()

	services := emulator.RoutedServices()
	assert.True(t, slices.IsSorted(services), "RoutedServices is sorted")

	for _, service := range services {
		assert.NotEmpty(t, service, "a registry key is never empty")
		ops := emulator.RoutedOperations(service)
		assert.True(t, slices.IsSorted(ops), "%s operations are sorted", service)
		assert.Equal(t, len(ops), len(slices.Compact(slices.Clone(ops))),
			"%s operations are free of duplicates", service)
		for _, op := range ops {
			assert.NotEmpty(t, op, "%s routes an empty operation name", service)
		}
	}
}
