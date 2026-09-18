package emulator_test

// The accelerated half of the instance-type catalog (#896).
//
// Three families used to carry one size each — p3.2xlarge, g4dn.xlarge, inf1.xlarge — which
// is the one place the catalog's documented "complete per family" invariant did not hold, so
// p3.8xlarge answered InvalidInstanceType for a type that plainly exists. Eight further
// families were absent entirely, g6 among them, which is what #891, #892 and #894 all probe.
//
// Every assertion here goes through HTTP. The catalog is package-private and the point is
// what the three operations sharing it report, not what the table says.

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ec2AcceleratedTypes is every type the accelerated families contribute to the catalog, in
// catalog order.
//
// Spelled out rather than derived from the package's own table, so a size dropped from the
// table fails a test instead of quietly shrinking the expectation with it.
var ec2AcceleratedTypes = []string{
	"p3.2xlarge", "p3.8xlarge", "p3.16xlarge",
	"p4d.24xlarge",
	"p4de.24xlarge",
	"p5.4xlarge", "p5.48xlarge",
	"g4dn.xlarge", "g4dn.2xlarge", "g4dn.4xlarge", "g4dn.8xlarge", "g4dn.12xlarge", "g4dn.16xlarge",
	"g5.xlarge", "g5.2xlarge", "g5.4xlarge", "g5.8xlarge", "g5.12xlarge", "g5.16xlarge",
	"g5.24xlarge", "g5.48xlarge",
	"g6.xlarge", "g6.2xlarge", "g6.4xlarge", "g6.8xlarge", "g6.12xlarge", "g6.16xlarge",
	"g6.24xlarge", "g6.48xlarge",
	"inf1.xlarge", "inf1.2xlarge", "inf1.6xlarge", "inf1.24xlarge",
	"inf2.xlarge", "inf2.8xlarge", "inf2.24xlarge", "inf2.48xlarge",
	"trn1.2xlarge", "trn1.32xlarge",
	"trn2.3xlarge", "trn2.48xlarge",
}

// ec2CatalogTypesFromDescribe returns every instance type DescribeInstanceTypes reports.
func ec2CatalogTypesFromDescribe(t *testing.T, ts *httptest.Server) []string {
	t.Helper()
	var doc struct {
		XMLName xml.Name `xml:"DescribeInstanceTypesResponse"`
		Types   []string `xml:"instanceTypeSet>item>instanceType"`
	}
	ec2DescribeXML(t, ts, map[string]string{"Action": "DescribeInstanceTypes"}, &doc)
	return doc.Types
}

// TestEC2_AcceleratedCatalog_TheThreeCatalogOperationsAgree asserts that
// DescribeInstanceTypes, DescribeInstanceTypeOfferings and DescribeSpotPriceHistory report
// exactly the same set of types, and that the set contains every accelerated type.
//
// The three walk one catalog built by one function, so this is one test rather than three:
// the hazard it guards is the one that function exists to prevent — the eight-type catalog
// #234 shipped kept its spot prices in a parallel map, and a type missing from that map was
// silently dropped from the price history while the other two operations still reported it.
// Adding a second family table is exactly the change that could reintroduce it.
func TestEC2_AcceleratedCatalog_TheThreeCatalogOperationsAgree(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)

	described := ec2CatalogTypesFromDescribe(t, ts)

	// LocationType=region gives one offering per type rather than one per zone, so the
	// comparison is against a set of the same shape.
	var offerings struct {
		XMLName xml.Name `xml:"DescribeInstanceTypeOfferingsResponse"`
		Types   []string `xml:"instanceTypeOfferingSet>item>instanceType"`
	}
	ec2DescribeXML(t, ts, map[string]string{
		"Action": "DescribeInstanceTypeOfferings", "LocationType": "region",
	}, &offerings)

	// One zone, for the same reason.
	var prices struct {
		XMLName xml.Name `xml:"DescribeSpotPriceHistoryResponse"`
		Types   []string `xml:"spotPriceHistorySet>item>instanceType"`
	}
	ec2DescribeXML(t, ts, map[string]string{
		"Action": "DescribeSpotPriceHistory", "AvailabilityZone": "us-east-1a",
	}, &prices)

	assert.Equal(t, described, offerings.Types,
		"DescribeInstanceTypeOfferings walks the same catalog in the same order")
	assert.Equal(t, described, prices.Types,
		"DescribeSpotPriceHistory walks the same catalog in the same order")

	for _, want := range ec2AcceleratedTypes {
		assert.Contains(t, described, want)
	}
}

// TestEC2_AcceleratedCatalog_EveryAcceleratedTypeIsAssertable asserts each accelerated type
// through InstanceType.N, which is the parameter that used to refuse them.
//
// One request per type rather than one for all of them: InstanceType.N collects every miss
// into a single error, so a batch request would report "these do not exist" without saying
// which assertion is the one that regressed.
func TestEC2_AcceleratedCatalog_EveryAcceleratedTypeIsAssertable(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	for _, name := range ec2AcceleratedTypes {
		t.Run(name, func(t *testing.T) {
			var doc struct {
				XMLName xml.Name `xml:"DescribeInstanceTypesResponse"`
				Types   []string `xml:"instanceTypeSet>item>instanceType"`
			}
			ec2DescribeXML(t, ts, map[string]string{
				"Action": "DescribeInstanceTypes", "InstanceType.1": name,
			}, &doc)
			assert.Equal(t, []string{name}, doc.Types)
		})
	}
}

// TestEC2_AcceleratedCatalog_ReportsTheGuidesSpecs asserts the vCPU, memory and accelerator
// figures for the sizes where the AWS guide's numbers are the ones a hand-written table is
// most likely to get wrong.
//
// The accelerator count is not monotonic in size in three families and AWS publishes it that
// way: g4dn, g5 and g6 each have a 12xlarge carrying four and a 16xlarge carrying one, and
// g5/g6 have a 24xlarge carrying four below a 48xlarge carrying eight. inf2's count runs
// 1, 1, 6, 12. Anything that "corrected" those to powers of two would be wrong, so each is
// pinned.
func TestEC2_AcceleratedCatalog_ReportsTheGuidesSpecs(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	type instanceTypeItem struct {
		InstanceType string `xml:"instanceType"`
		VCpus        int    `xml:"vCpuInfo>defaultVCpus"`
		MemoryMiB    int    `xml:"memoryInfo>sizeInMiB"`
		GPUs         int    `xml:"gpuInfo>gpus>item>count"`
	}

	for _, tc := range []struct {
		name      string
		vcpus     int
		memoryMiB int
		gpus      int
	}{
		// p3, completed. The 2xlarge is #234's row, unchanged.
		{"p3.2xlarge", 8, 62464, 1},
		{"p3.8xlarge", 32, 249856, 4},
		{"p3.16xlarge", 64, 499712, 8},
		// The whole of p4d and p4de. AWS lists them as two families, not two sizes.
		{"p4d.24xlarge", 96, 1179648, 8},
		{"p4de.24xlarge", 96, 1179648, 8},
		{"p5.4xlarge", 16, 262144, 1},
		{"p5.48xlarge", 192, 2097152, 8},
		// g4dn's 12xlarge/16xlarge inversion, and its #234 row.
		{"g4dn.xlarge", 4, 16384, 1},
		{"g4dn.12xlarge", 48, 196608, 4},
		{"g4dn.16xlarge", 64, 262144, 1},
		// g5's two inversions.
		{"g5.12xlarge", 48, 196608, 4},
		{"g5.16xlarge", 64, 262144, 1},
		{"g5.24xlarge", 96, 393216, 4},
		{"g5.48xlarge", 192, 786432, 8},
		// The type #891, #892 and #894 probe, and g6's inversions.
		{"g6.xlarge", 4, 16384, 1},
		{"g6.12xlarge", 48, 196608, 4},
		{"g6.16xlarge", 64, 262144, 1},
		{"g6.24xlarge", 96, 393216, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var doc struct {
				XMLName xml.Name           `xml:"DescribeInstanceTypesResponse"`
				Items   []instanceTypeItem `xml:"instanceTypeSet>item"`
			}
			ec2DescribeXML(t, ts, map[string]string{
				"Action": "DescribeInstanceTypes", "InstanceType.1": tc.name,
			}, &doc)
			require.Len(t, doc.Items, 1)
			assert.Equal(t, tc.vcpus, doc.Items[0].VCpus)
			assert.Equal(t, tc.memoryMiB, doc.Items[0].MemoryMiB)
			assert.Equal(t, tc.gpus, doc.Items[0].GPUs)
		})
	}
}

// TestEC2_AcceleratedCatalog_InferentiaAndTrainiumReportNeuronInfo asserts that all twelve
// Inferentia and Trainium types report their accelerator count through neuronInfo and none
// of them through gpuInfo (#1029).
//
// #234's reading — "real EC2 does not report [Inferentia] through gpuInfo" — still holds and
// is the first half of this. Its second half, that the count therefore reaches no member at
// all, does not follow: InstanceTypeInfo splits accelerators across five members, and AWS's
// own note on API_InferenceAcceleratorInfo ("Amazon Elastic Inference is no longer
// available") leaves neuronInfo as the live member for a device the Neuron SDK drives. So a
// count substrate already held, and dropped at catalog-build time, is now reported where AWS
// reports it.
//
// Both halves are asserted per type: the count through a decoder, and gpuInfo's absence on
// raw bytes, because a decoder cannot tell an absent gpuInfo from one carrying a zero count
// and zero is exactly what the wrong fix produces. The counts are the guide's own and three
// of them are the non-obvious ones — inf1.24xlarge 16 where inf2.48xlarge is 12, and inf2's
// 1, 1, 6, 12 ladder.
func TestEC2_AcceleratedCatalog_InferentiaAndTrainiumReportNeuronInfo(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	for _, tc := range []struct {
		name    string
		devices int
	}{
		{"inf1.xlarge", 1}, {"inf1.2xlarge", 1}, {"inf1.6xlarge", 4}, {"inf1.24xlarge", 16},
		{"inf2.xlarge", 1}, {"inf2.8xlarge", 1}, {"inf2.24xlarge", 6}, {"inf2.48xlarge", 12},
		{"trn1.2xlarge", 1}, {"trn1.32xlarge", 16},
		{"trn2.3xlarge", 1}, {"trn2.48xlarge", 16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{
				"Action": "DescribeInstanceTypes", "InstanceType.1": tc.name,
			}
			var doc struct {
				XMLName xml.Name `xml:"DescribeInstanceTypesResponse"`
				Items   []struct {
					InstanceType  string `xml:"instanceType"`
					NeuronDevices int    `xml:"neuronInfo>neuronDevices>item>count"`
				} `xml:"instanceTypeSet>item"`
			}
			ec2DescribeXML(t, ts, params, &doc)
			require.Len(t, doc.Items, 1)
			assert.Equal(t, tc.name, doc.Items[0].InstanceType)
			assert.Equal(t, tc.devices, doc.Items[0].NeuronDevices)

			body := ec2DescribeBody(t, ts, params)
			assert.NotContains(t, body, "gpuInfo")
			// The whole element, open tag to close: count is the only child, so this is
			// also the assertion that name, coreInfo, memoryInfo and
			// totalNeuronDeviceMemoryInMiB are omitted rather than invented. AWS
			// publishes not one valid value or example between the four of them.
			assert.Contains(t, body, fmt.Sprintf(
				"<neuronInfo><neuronDevices><item><count>%d</count></item>"+
					"</neuronDevices></neuronInfo>", tc.devices))
		})
	}

	// The control, in both directions: a NVIDIA family in the same catalog reports gpuInfo
	// and no neuronInfo, so neither assertion above is about the element never being
	// rendered at all.
	body := ec2DescribeBody(t, ts, map[string]string{
		"Action": "DescribeInstanceTypes", "InstanceType.1": "g6.xlarge",
	})
	assert.Contains(t, body, "gpuInfo")
	assert.NotContains(t, body, "neuronInfo")

	// And a non-accelerated type reports neither, which is what makes the two members a
	// property of the family rather than of the response shape.
	body = ec2DescribeBody(t, ts, map[string]string{
		"Action": "DescribeInstanceTypes", "InstanceType.1": "c5.xlarge",
	})
	assert.NotContains(t, body, "gpuInfo")
	assert.NotContains(t, body, "neuronInfo")
}

// TestEC2_AcceleratedCatalog_SpotPriceIsAFixedRatePerGiB asserts the calibration rule the
// catalog documents — within a family the stub price is a fixed rate per GiB of memory — for
// every accelerated family.
//
// The rule is what keeps the prices monotonic in size, and it is also the only check on a
// column of hand-computed decimals: a mistyped digit in one row breaks the family's rate
// while leaving every other assertion in this file passing. The three values #234 seeded
// (p3.2xlarge 0.918, g4dn.xlarge 0.188, inf1.xlarge 0.076) are the rows each of those
// families is calibrated from, so they are asserted verbatim as well.
func TestEC2_AcceleratedCatalog_SpotPriceIsAFixedRatePerGiB(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServerFrozenClock(t)

	type priceItem struct {
		InstanceType string `xml:"instanceType"`
		SpotPrice    string `xml:"spotPrice"`
	}
	type sizeItem struct {
		InstanceType string `xml:"instanceType"`
		MemoryMiB    int    `xml:"memoryInfo>sizeInMiB"`
	}

	var sizes struct {
		XMLName xml.Name   `xml:"DescribeInstanceTypesResponse"`
		Items   []sizeItem `xml:"instanceTypeSet>item"`
	}
	ec2DescribeXML(t, ts, map[string]string{"Action": "DescribeInstanceTypes"}, &sizes)
	memory := map[string]int{}
	for _, it := range sizes.Items {
		memory[it.InstanceType] = it.MemoryMiB
	}

	var prices struct {
		XMLName xml.Name    `xml:"DescribeSpotPriceHistoryResponse"`
		Items   []priceItem `xml:"spotPriceHistorySet>item"`
	}
	ec2DescribeXML(t, ts, map[string]string{
		"Action": "DescribeSpotPriceHistory", "AvailabilityZone": "us-east-1a",
	}, &prices)
	price := map[string]string{}
	for _, it := range prices.Items {
		price[it.InstanceType] = it.SpotPrice
	}

	assert.Equal(t, "0.918", price["p3.2xlarge"], "#234's seeded value must not move")
	assert.Equal(t, "0.188", price["g4dn.xlarge"], "#234's seeded value must not move")
	assert.Equal(t, "0.076", price["inf1.xlarge"], "#234's seeded value must not move")

	// Rate per family, taken from the family's first size and checked against the rest.
	// p3's rate is 0.918/61, which is not a terminating decimal, hence the tolerance —
	// every other family's is exact.
	for _, family := range []struct {
		name  string
		sizes []string
	}{
		{"p3", []string{"p3.2xlarge", "p3.8xlarge", "p3.16xlarge"}},
		{"p5", []string{"p5.4xlarge", "p5.48xlarge"}},
		{"g4dn", []string{
			"g4dn.xlarge", "g4dn.2xlarge", "g4dn.4xlarge", "g4dn.8xlarge",
			"g4dn.12xlarge", "g4dn.16xlarge",
		}},
		{"g5", []string{
			"g5.xlarge", "g5.2xlarge", "g5.4xlarge", "g5.8xlarge", "g5.12xlarge",
			"g5.16xlarge", "g5.24xlarge", "g5.48xlarge",
		}},
		{"g6", []string{
			"g6.xlarge", "g6.2xlarge", "g6.4xlarge", "g6.8xlarge", "g6.12xlarge",
			"g6.16xlarge", "g6.24xlarge", "g6.48xlarge",
		}},
		{"inf1", []string{"inf1.xlarge", "inf1.2xlarge", "inf1.6xlarge", "inf1.24xlarge"}},
		{"inf2", []string{"inf2.xlarge", "inf2.8xlarge", "inf2.24xlarge", "inf2.48xlarge"}},
		{"trn1", []string{"trn1.2xlarge", "trn1.32xlarge"}},
		{"trn2", []string{"trn2.3xlarge", "trn2.48xlarge"}},
	} {
		t.Run(family.name, func(t *testing.T) {
			var rate float64
			var previous float64
			for i, name := range family.sizes {
				require.Contains(t, memory, name)
				require.Contains(t, price, name)
				amount, err := strconv.ParseFloat(price[name], 64)
				require.NoError(t, err, "%s spot price %q", name, price[name])
				gib := float64(memory[name]) / 1024
				if i == 0 {
					rate = amount / gib
				} else {
					assert.InDelta(t, rate, amount/gib, 1e-9,
						"%s is off the family's rate per GiB", name)
					assert.Greater(t, amount, previous, "%s must cost more than the size below it", name)
				}
				previous = amount
			}
		})
	}
}

// TestEC2_RunInstances_AcceptsATypeDescribeInstanceTypesRefuses pins the divergence the
// catalog widening does not close, so it is a recorded decision rather than an oversight.
//
// RunInstances stores InstanceType verbatim and validates it against nothing, so a type
// outside the catalog launches and DescribeInstances reports it, while DescribeInstanceTypes
// refuses the same string in the same session. Real EC2 refuses at launch. Substrate does
// not, because the catalog is deliberately not exhaustive — m7i.large, t2.micro, c7i.xlarge,
// t4g.nano, m7i.xlarge and c6a.xlarge are all launched by fixtures in this repository and
// none is in the catalog — so validating at launch would refuse types AWS plainly offers,
// which is the failure the completeness invariant exists to prevent, moved to the operation
// that creates state. The divergence is recorded in docs/services.md with that reason.
//
// This test asserts the divergence rather than the fix, so closing it later is a deliberate
// edit here and not a silent behavior change.
func TestEC2_RunInstances_AcceptsATypeDescribeInstanceTypesRefuses(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	const unmodelled = "m7i.large"

	var launched struct {
		XMLName  xml.Name `xml:"RunInstancesResponse"`
		Types    []string `xml:"instancesSet>item>instanceType"`
		Instance []string `xml:"instancesSet>item>instanceId"`
	}
	ec2DescribeXML(t, ts, map[string]string{
		"Action":       "RunInstances",
		"ImageId":      ec2TestImage,
		"MinCount":     "1",
		"MaxCount":     "1",
		"InstanceType": unmodelled,
	}, &launched)
	require.Len(t, launched.Instance, 1)
	assert.Equal(t, []string{unmodelled}, launched.Types)

	var described struct {
		XMLName xml.Name `xml:"DescribeInstancesResponse"`
		Types   []string `xml:"reservationSet>item>instancesSet>item>instanceType"`
	}
	ec2DescribeXML(t, ts, map[string]string{"Action": "DescribeInstances"}, &described)
	assert.Equal(t, []string{unmodelled}, described.Types,
		"the launch is not a fiction: the instance is reported with the unmodelled type")

	status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
		"Action": "DescribeInstanceTypes", "InstanceType.1": unmodelled,
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidInstanceType", code,
		"the same string the launch accepted is refused here, which is the recorded divergence")
}
