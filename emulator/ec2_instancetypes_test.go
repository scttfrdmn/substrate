package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ec2CatalogTypes returns every instance type DescribeInstanceTypes reports, sorted.
//
// The catalog is package-private, so these tests read it back through the API rather than
// referencing the Go variable. That is deliberate beyond the package boundary: the
// assertions then hold on the observation a consumer makes, which is the thing #485 was
// filed about.
func ec2CatalogTypes(t *testing.T, ts *httptest.Server) []string {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeInstanceTypes"})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		InstanceTypes []struct {
			InstanceType string `xml:"instanceType"`
		} `xml:"instanceTypeSet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	types := make([]string, 0, len(result.InstanceTypes))
	for _, it := range result.InstanceTypes {
		types = append(types, it.InstanceType)
	}
	sort.Strings(types)
	return types
}

// catalogSize returns the number of types in the seeded instance-type catalog.
func catalogSize(t *testing.T, ts *httptest.Server) int {
	t.Helper()
	return len(ec2CatalogTypes(t, ts))
}

// ec2Offerings returns the (instanceType, location, locationType) triples
// DescribeInstanceTypeOfferings reports for params.
func ec2Offerings(t *testing.T, ts *httptest.Server, params map[string]string) []ec2Offering {
	t.Helper()
	params["Action"] = "DescribeInstanceTypeOfferings"
	resp := ec2Request(t, ts, params)
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		Offerings []ec2Offering `xml:"instanceTypeOfferingSet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	return result.Offerings
}

// ec2Offering is one item of a DescribeInstanceTypeOfferings response.
type ec2Offering struct {
	InstanceType string `xml:"instanceType"`
	LocationType string `xml:"locationType"`
	Location     string `xml:"location"`
}

// offeringTypes returns the distinct instance types in a set of offerings, sorted.
func offeringTypes(offerings []ec2Offering) []string {
	seen := map[string]bool{}
	var types []string
	for _, o := range offerings {
		if !seen[o.InstanceType] {
			seen[o.InstanceType] = true
			types = append(types, o.InstanceType)
		}
	}
	sort.Strings(types)
	return types
}

// TestEC2_DescribeInstanceTypes_UnknownTypeRejected covers #485 item 1: an instance type
// absent from the catalog was answered with HTTP 200 and an empty list, where real AWS
// raises InvalidInstanceType.
//
// This is the same failure shape as #391 (an unknown instance ID returned 200 and an
// empty list rather than InvalidInstanceID.NotFound), and it matters for the same reason:
// an empty list is indistinguishable from "the type exists but matched nothing", so a
// consumer validating a user-supplied instance type gets no signal at all and their
// validation branch is unreachable.
func TestEC2_DescribeInstanceTypes_UnknownTypeRejected(t *testing.T) {
	ts := newEC2TestServer(t)

	status, code, message := ec2ErrorDetail(t, ts, map[string]string{
		"Action":         "DescribeInstanceTypes",
		"InstanceType.1": "zz9.nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidInstanceType", code)
	// Verbatim from the real us-east-1 capture reported in #485.
	assert.Equal(t, "The following supplied instance types do not exist: [zz9.nonexistent]", message)
}

// TestEC2_DescribeInstanceTypes_UnknownTypesListedTogether pins that every unknown type
// is collected into one error, in request order, rather than raising on the first.
//
// The captured message carries a bracketed list, so a per-type error would be the wrong
// shape. Request order rather than sorted order, because that is the only ordering the
// caller can predict.
func TestEC2_DescribeInstanceTypes_UnknownTypesListedTogether(t *testing.T) {
	ts := newEC2TestServer(t)

	status, code, message := ec2ErrorDetail(t, ts, map[string]string{
		"Action":         "DescribeInstanceTypes",
		"InstanceType.1": "zz9.nonexistent",
		"InstanceType.2": "aa1.bogus",
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidInstanceType", code)
	assert.Equal(t, "The following supplied instance types do not exist: [zz9.nonexistent, aa1.bogus]", message)
}

// TestEC2_DescribeInstanceTypes_MixedKnownAndUnknown pins that one bad type fails the
// whole request rather than returning the good ones.
//
// The message is an assertion that the *supplied* types exist, so a partial answer would
// let a caller that checks only the result set miss the bad input entirely — which is the
// defect #485 reported, just narrowed to one element of the request.
func TestEC2_DescribeInstanceTypes_MixedKnownAndUnknown(t *testing.T) {
	ts := newEC2TestServer(t)

	status, code, message := ec2ErrorDetail(t, ts, map[string]string{
		"Action":         "DescribeInstanceTypes",
		"InstanceType.1": "m5.xlarge",
		"InstanceType.2": "zz9.nonexistent",
	})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidInstanceType", code)
	assert.Equal(t, "The following supplied instance types do not exist: [zz9.nonexistent]", message,
		"only the unknown type belongs in the message")
}

// TestEC2_DescribeInstanceTypes_KnownTypeUnaffected is the regression guard on the
// rejection: a type in the catalog still returns its full item, and no filter still
// returns the whole catalog.
func TestEC2_DescribeInstanceTypes_KnownTypeUnaffected(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action":         "DescribeInstanceTypes",
		"InstanceType.1": "m5.xlarge",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result struct {
		InstanceTypes []struct {
			InstanceType string `xml:"instanceType"`
			VCpuInfo     struct {
				DefaultVCpus int `xml:"defaultVCpus"`
			} `xml:"vCpuInfo"`
			MemoryInfo struct {
				SizeInMiB int `xml:"sizeInMiB"`
			} `xml:"memoryInfo"`
		} `xml:"instanceTypeSet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.InstanceTypes, 1)
	assert.Equal(t, "m5.xlarge", result.InstanceTypes[0].InstanceType)
	assert.Equal(t, 4, result.InstanceTypes[0].VCpuInfo.DefaultVCpus)
	assert.Equal(t, 16384, result.InstanceTypes[0].MemoryInfo.SizeInMiB)
}

// TestEC2_InstanceTypeCatalog_Specs pins a handful of vCPU and memory figures against the
// AWS instance-type guides.
//
// The catalog is generated from per-family size lists rather than hand-written rows, so a
// mistake in the generation would move every type at once — these rows are the fixed
// points that catch it. They deliberately span the shapes that differ: the smallest
// burstable size (sub-GiB memory), the size ladders that are *not* shared between an
// Intel family and its AMD sibling (c5 has 9xlarge and 18xlarge where c5a has 8xlarge and
// 16xlarge), the memory-optimized ratio, and the accelerated single sizes.
func TestEC2_InstanceTypeCatalog_Specs(t *testing.T) {
	tests := []struct {
		instanceType string
		vcpus        int
		memoryMiB    int
		gpus         int
	}{
		{instanceType: "t3.nano", vcpus: 2, memoryMiB: 512},
		{instanceType: "t3.micro", vcpus: 2, memoryMiB: 1024},
		{instanceType: "t3a.medium", vcpus: 2, memoryMiB: 4096},
		{instanceType: "t3.2xlarge", vcpus: 8, memoryMiB: 32768},
		{instanceType: "m5.large", vcpus: 2, memoryMiB: 8192},
		{instanceType: "m5.24xlarge", vcpus: 96, memoryMiB: 393216},
		{instanceType: "m5a.xlarge", vcpus: 4, memoryMiB: 16384},
		{instanceType: "c5.large", vcpus: 2, memoryMiB: 4096},
		{instanceType: "c5.9xlarge", vcpus: 36, memoryMiB: 73728},
		{instanceType: "c5.18xlarge", vcpus: 72, memoryMiB: 147456},
		{instanceType: "c5a.8xlarge", vcpus: 32, memoryMiB: 65536},
		{instanceType: "c5a.16xlarge", vcpus: 64, memoryMiB: 131072},
		{instanceType: "r5.large", vcpus: 2, memoryMiB: 16384},
		{instanceType: "r5.24xlarge", vcpus: 96, memoryMiB: 786432},
		{instanceType: "p3.2xlarge", vcpus: 8, memoryMiB: 62464, gpus: 1},
		{instanceType: "g4dn.xlarge", vcpus: 4, memoryMiB: 16384, gpus: 1},
		// inf1's accelerator is an Inferentia chip, which real EC2 does not report
		// through gpuInfo, so the GPU count stays zero.
		{instanceType: "inf1.xlarge", vcpus: 4, memoryMiB: 8192},
	}

	ts := newEC2TestServer(t)
	for _, tt := range tests {
		t.Run(tt.instanceType, func(t *testing.T) {
			resp := ec2Request(t, ts, map[string]string{
				"Action":         "DescribeInstanceTypes",
				"InstanceType.1": tt.instanceType,
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var result struct {
				InstanceTypes []struct {
					InstanceType string `xml:"instanceType"`
					VCpuInfo     struct {
						DefaultVCpus int `xml:"defaultVCpus"`
					} `xml:"vCpuInfo"`
					MemoryInfo struct {
						SizeInMiB int `xml:"sizeInMiB"`
					} `xml:"memoryInfo"`
					GpuInfo *struct {
						Count int `xml:"gpus>item>count"`
					} `xml:"gpuInfo"`
				} `xml:"instanceTypeSet>item"`
			}
			require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
			require.Len(t, result.InstanceTypes, 1)
			got := result.InstanceTypes[0]
			assert.Equal(t, tt.vcpus, got.VCpuInfo.DefaultVCpus, "vCPUs")
			assert.Equal(t, tt.memoryMiB, got.MemoryInfo.SizeInMiB, "memory MiB")
			if tt.gpus == 0 {
				assert.Nil(t, got.GpuInfo, "gpuInfo must be omitted for a type with no accelerator")
				return
			}
			require.NotNil(t, got.GpuInfo)
			assert.Equal(t, tt.gpus, got.GpuInfo.Count, "GPUs")
		})
	}
}

// TestEC2_InstanceTypeCatalog_WholeFamilies covers #485 item 3: the eight-type catalog
// split families mid-way, so which sizes were present was incidental to #234's consumer.
//
// The ten types #485 names as the ones parsl-aws-provider references are asserted
// present, five of which were absent before. Whole families matter beyond coverage: item
// 1 refuses a type the catalog lacks, so a catalog that stopped at c5.xlarge would answer
// InvalidInstanceType for c5.large — the right code for a bogus type and the wrong one
// for a real one.
func TestEC2_InstanceTypeCatalog_WholeFamilies(t *testing.T) {
	ts := newEC2TestServer(t)
	present := map[string]bool{}
	for _, it := range ec2CatalogTypes(t, ts) {
		present[it] = true
	}

	// The ten types #485 lists, with the five it reported ABSENT marked.
	for _, want := range []string{
		"t3.micro",   // present before
		"t3.small",   // was absent
		"t3.medium",  // was absent
		"t3a.small",  // was absent
		"t3a.medium", // was absent
		"m5.large",   // present before
		"m5.xlarge",  // was absent
		"m5a.large",  // was absent
		"c5.large",   // was absent
		"c5.xlarge",  // present before
	} {
		assert.True(t, present[want], "#485 names %s; it must be in the catalog", want)
	}

	// Every family is complete, so no family has a hole a caller could trip on.
	families := map[string][]string{
		"t3":  {"nano", "micro", "small", "medium", "large", "xlarge", "2xlarge"},
		"t3a": {"nano", "micro", "small", "medium", "large", "xlarge", "2xlarge"},
		"m5":  {"large", "xlarge", "2xlarge", "4xlarge", "8xlarge", "12xlarge", "16xlarge", "24xlarge"},
		"m5a": {"large", "xlarge", "2xlarge", "4xlarge", "8xlarge", "12xlarge", "16xlarge", "24xlarge"},
		"c5":  {"large", "xlarge", "2xlarge", "4xlarge", "9xlarge", "12xlarge", "18xlarge", "24xlarge"},
		"c5a": {"large", "xlarge", "2xlarge", "4xlarge", "8xlarge", "12xlarge", "16xlarge", "24xlarge"},
		"r5":  {"large", "xlarge", "2xlarge", "4xlarge", "8xlarge", "12xlarge", "16xlarge", "24xlarge"},
	}
	for family, sizes := range families {
		for _, size := range sizes {
			assert.True(t, present[family+"."+size], "%s.%s missing from the catalog", family, size)
		}
	}

	// Bare-metal sizes are deliberately excluded: they are real types, but nothing else in
	// the plugin models their behavior, so returning them would advertise fidelity that
	// is not there. Pinned so the exclusion is a decision rather than an oversight.
	for _, metal := range []string{"m5.metal", "c5.metal", "r5.metal"} {
		assert.False(t, present[metal], "%s should not be in the catalog", metal)
	}
}

// TestEC2_SpotPriceCatalog_Parity pins that every catalog type has a spot price.
//
// The catalog and the prices were two parallel structures with the same eight keys, and a
// type in one and not the other was the next bug: describeSpotPriceHistory silently
// skipped any type absent from the price map, so a caller would see the type from
// DescribeInstanceTypes and no price for it. They are now generated together; this asserts
// the property that made that safe, through the API rather than the Go types.
func TestEC2_SpotPriceCatalog_Parity(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action":           "DescribeSpotPriceHistory",
		"AvailabilityZone": "us-east-1a",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		Items []struct {
			InstanceType string `xml:"instanceType"`
			SpotPrice    string `xml:"spotPrice"`
		} `xml:"spotPriceHistorySet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))

	priced := map[string]string{}
	for _, item := range result.Items {
		priced[item.InstanceType] = item.SpotPrice
	}
	for _, it := range ec2CatalogTypes(t, ts) {
		price, ok := priced[it]
		assert.True(t, ok, "%s is in the instance-type catalog but has no spot price", it)
		assert.NotEmpty(t, price, "%s has an empty spot price", it)
	}
	assert.Len(t, priced, catalogSize(t, ts), "spot prices exist for types not in the catalog")
}

// TestEC2_SpotPriceCatalog_PreservesSeededPrices pins the eight prices #234's catalog
// shipped, unchanged by the widening.
//
// The prices are deterministic stubs, not researched AWS figures, so the reason to pin
// them is fixture stability rather than fidelity: a consumer with a recorded run must not
// see its numbers move because the catalog grew.
func TestEC2_SpotPriceCatalog_PreservesSeededPrices(t *testing.T) {
	seeded := map[string]string{
		"t3.micro":    "0.0042",
		"c5.xlarge":   "0.068",
		"c5.2xlarge":  "0.136",
		"m5.large":    "0.038",
		"r5.xlarge":   "0.076",
		"p3.2xlarge":  "0.918",
		"g4dn.xlarge": "0.188",
		"inf1.xlarge": "0.076",
	}

	ts := newEC2TestServer(t)
	for instanceType, want := range seeded {
		t.Run(instanceType, func(t *testing.T) {
			resp := ec2Request(t, ts, map[string]string{
				"Action":           "DescribeSpotPriceHistory",
				"InstanceType.1":   instanceType,
				"AvailabilityZone": "us-east-1a",
			})
			defer resp.Body.Close() //nolint:errcheck
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var result struct {
				Items []struct {
					SpotPrice string `xml:"spotPrice"`
				} `xml:"spotPriceHistorySet>item"`
			}
			require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
			require.Len(t, result.Items, 1)
			assert.Equal(t, want, result.Items[0].SpotPrice)
		})
	}
}

// TestEC2_DescribeSpotPriceHistory_UnknownTypeIsEmpty pins that InstanceType.N here is a
// filter, not an assertion.
//
// The reference describes it as filtering the results, unlike DescribeInstanceTypes' same-
// named parameter, so an unknown type is an empty history and HTTP 200. Asserted
// deliberately: the two operations spell the parameter identically and behave differently,
// which is exactly the kind of thing a later change would "tidy" into consistency.
func TestEC2_DescribeSpotPriceHistory_UnknownTypeIsEmpty(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action":         "DescribeSpotPriceHistory",
		"InstanceType.1": "zz9.nonexistent",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result struct {
		Items []struct {
			InstanceType string `xml:"instanceType"`
		} `xml:"spotPriceHistorySet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	assert.Empty(t, result.Items)
}

// TestEC2_DescribeInstanceTypeOfferings_InstanceTypeFilter covers #485 item 2: the
// instance-type filter was unreachable dead code, so every offerings query returned the
// whole catalog in every AZ regardless of the filter.
//
// The cause was that the handler built its type filter from an `InstanceType.N` parameter
// the operation does not have — the reference's parameters are DryRun, Filter.N,
// LocationType, MaxResults and NextToken, and botocore rejects `InstanceTypes` outright —
// so the filter map was always empty. A caller asking "is m5.xlarge offered in this AZ?"
// got yes for any input, including nonsense.
func TestEC2_DescribeInstanceTypeOfferings_InstanceTypeFilter(t *testing.T) {
	tests := []struct {
		name      string
		values    map[string]string
		wantTypes []string
		// wantAllCatalog asserts the answer is the whole catalog, for the no-filter case.
		wantAllCatalog bool
		// wantEmpty asserts zero offerings and HTTP 200.
		wantEmpty bool
	}{
		{
			name:      "one exact type",
			values:    map[string]string{"Filter.1.Name": "instance-type", "Filter.1.Value.1": "m5.xlarge"},
			wantTypes: []string{"m5.xlarge"},
		},
		{
			name: "two values are an OR",
			values: map[string]string{
				"Filter.1.Name":    "instance-type",
				"Filter.1.Value.1": "m5.xlarge",
				"Filter.1.Value.2": "c5.large",
			},
			wantTypes: []string{"c5.large", "m5.xlarge"},
		},
		{
			// The reference documents this form on DescribeInstanceTypes' instance-type
			// filter: "The instance type (for example c5.2xlarge or c5*)".
			name:   "trailing wildcard selects a family",
			values: map[string]string{"Filter.1.Name": "instance-type", "Filter.1.Value.1": "c5.*"},
			wantTypes: []string{
				"c5.12xlarge", "c5.18xlarge", "c5.24xlarge", "c5.2xlarge",
				"c5.4xlarge", "c5.9xlarge", "c5.large", "c5.xlarge",
			},
		},
		{
			// c5* without the dot also matches the c5a family, since * matches any
			// characters — including the "a".
			name:   "wildcard without the dot spans sibling families",
			values: map[string]string{"Filter.1.Name": "instance-type", "Filter.1.Value.1": "c5*"},
			wantTypes: []string{
				"c5.12xlarge", "c5.18xlarge", "c5.24xlarge", "c5.2xlarge",
				"c5.4xlarge", "c5.9xlarge", "c5.large", "c5.xlarge",
				"c5a.12xlarge", "c5a.16xlarge", "c5a.24xlarge", "c5a.2xlarge",
				"c5a.4xlarge", "c5a.8xlarge", "c5a.large", "c5a.xlarge",
			},
		},
		{
			name:      "a mid-string wildcard",
			values:    map[string]string{"Filter.1.Name": "instance-type", "Filter.1.Value.1": "*.24xlarge"},
			wantTypes: []string{"c5.24xlarge", "c5a.24xlarge", "m5.24xlarge", "m5a.24xlarge", "r5.24xlarge"},
		},
		{
			// EC2 documents API filter values as case-sensitive.
			name:      "filter values are case sensitive",
			values:    map[string]string{"Filter.1.Name": "instance-type", "Filter.1.Value.1": "M5.XLarge"},
			wantEmpty: true,
		},
		{
			// #485's real-AWS diff: 0 offerings, not an error. A filter that matches
			// nothing is a legitimate empty answer, unlike InstanceType.N on
			// DescribeInstanceTypes, which asserts the type exists.
			name:      "an unknown type yields zero offerings",
			values:    map[string]string{"Filter.1.Name": "instance-type", "Filter.1.Value.1": "zz9.bogus"},
			wantEmpty: true,
		},
		{
			name:           "no filter returns the whole catalog",
			values:         map[string]string{},
			wantAllCatalog: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			offerings := ec2Offerings(t, ts, tt.values)

			switch {
			case tt.wantEmpty:
				assert.Empty(t, offerings, "an unmatched filter must be an empty answer, not an error")
			case tt.wantAllCatalog:
				assert.Equal(t, ec2CatalogTypes(t, ts), offeringTypes(offerings))
				assert.Len(t, offerings, catalogSize(t, ts)*3, "one offering per type per seeded AZ")
			default:
				assert.Equal(t, tt.wantTypes, offeringTypes(offerings))
				assert.Len(t, offerings, len(tt.wantTypes)*3, "one offering per type per seeded AZ")
			}
		})
	}
}

// TestEC2_DescribeInstanceTypeOfferings_LocationValues pins that all of a location
// filter's values are read, not just Value.1, and that the two filters AND together.
//
// The old handler read `Filter.N.Value.1` for the location and discarded the rest, so a
// two-AZ query silently answered for one AZ.
func TestEC2_DescribeInstanceTypeOfferings_LocationValues(t *testing.T) {
	ts := newEC2TestServer(t)

	offerings := ec2Offerings(t, ts, map[string]string{
		"Filter.1.Name":    "location",
		"Filter.1.Value.1": "us-east-1a",
		"Filter.1.Value.2": "us-east-1c",
		"Filter.2.Name":    "instance-type",
		"Filter.2.Value.1": "m5.large",
	})
	require.Len(t, offerings, 2, "one type in two of the three AZs")
	locations := []string{offerings[0].Location, offerings[1].Location}
	sort.Strings(locations)
	assert.Equal(t, []string{"us-east-1a", "us-east-1c"}, locations)
	for _, o := range offerings {
		assert.Equal(t, "m5.large", o.InstanceType)
	}
}

// TestEC2_DescribeInstanceTypeOfferings_LocationType covers the top-level LocationType
// parameter, which #485 mistook for a filter name.
//
// The reference lists it as a separate parameter with four valid values, and lists exactly
// two filter names (instance-type, location) — so `location-type` is not a filter at all.
// region and availability-zone are modeled; availability-zone-id and outpost are refused
// rather than silently answered as zone names, since a caller matching the locationType
// against the location would otherwise mis-read zone names as AZ IDs or Outpost ARNs.
func TestEC2_DescribeInstanceTypeOfferings_LocationType(t *testing.T) {
	ts := newEC2TestServer(t)

	t.Run("region gives one offering per type", func(t *testing.T) {
		offerings := ec2Offerings(t, ts, map[string]string{"LocationType": "region"})
		assert.Len(t, offerings, catalogSize(t, ts))
		for _, o := range offerings {
			assert.Equal(t, "region", o.LocationType)
			assert.Equal(t, "us-east-1", o.Location)
		}
	})

	t.Run("availability-zone is the default", func(t *testing.T) {
		defaulted := ec2Offerings(t, ts, map[string]string{})
		explicit := ec2Offerings(t, ts, map[string]string{"LocationType": "availability-zone"})
		assert.Equal(t, defaulted, explicit)
		require.NotEmpty(t, defaulted)
		assert.Equal(t, "availability-zone", defaulted[0].LocationType)
	})

	t.Run("region honors a location filter naming the region", func(t *testing.T) {
		offerings := ec2Offerings(t, ts, map[string]string{
			"LocationType":     "region",
			"Filter.1.Name":    "location",
			"Filter.1.Value.1": "us-east-1",
			"Filter.2.Name":    "instance-type",
			"Filter.2.Value.1": "t3.micro",
		})
		require.Len(t, offerings, 1)
		assert.Equal(t, "us-east-1", offerings[0].Location)
	})

	for _, unmodelled := range []string{"availability-zone-id", "outpost"} {
		t.Run(unmodelled+" is refused", func(t *testing.T) {
			status, code, message := ec2ErrorDetail(t, ts, map[string]string{
				"Action":       "DescribeInstanceTypeOfferings",
				"LocationType": unmodelled,
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Contains(t, message, "not modeled by substrate",
				"the message must name substrate, so the divergence is not read as AWS behavior")
		})
	}

	t.Run("a value outside the documented set is refused", func(t *testing.T) {
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{
			"Action":       "DescribeInstanceTypeOfferings",
			"LocationType": "planet",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InvalidParameterValue", code)
	})
}

// TestEC2_DescribeInstanceTypeOfferings_UnknownFilterRefused pins that a filter name the
// operation does not document is refused rather than ignored.
//
// Silently ignoring a filter is precisely how #485's defect went unnoticed: the query
// looked like it had been narrowed and had not been. The operation documents exactly two
// filter names, so anything else — including `location-type`, which #485 believed was one
// — is a bad request.
func TestEC2_DescribeInstanceTypeOfferings_UnknownFilterRefused(t *testing.T) {
	for _, name := range []string{"location-type", "instance-family", "vcpu-info.default-vcpus"} {
		t.Run(name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			status, code, message := ec2ErrorDetail(t, ts, map[string]string{
				"Action":           "DescribeInstanceTypeOfferings",
				"Filter.1.Name":    name,
				"Filter.1.Value.1": "whatever",
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterValue", code)
			assert.Contains(t, message, name, "the message should name the offending filter")
		})
	}
}

// TestEC2_DescribeInstanceTypeOfferings_LocationsMatchDescribeAvailabilityZones pins that
// an offerings query filtered by a zone DescribeAvailabilityZones reported returns
// something.
//
// Both derive their zone list from one place. Two independent lists is a plausible-looking
// bug: a caller enumerating zones and then asking which types each offers would get an
// empty answer for a zone that exists.
func TestEC2_DescribeInstanceTypeOfferings_LocationsMatchDescribeAvailabilityZones(t *testing.T) {
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeAvailabilityZones"})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var zones struct {
		Zones []struct {
			ZoneName string `xml:"zoneName"`
		} `xml:"availabilityZoneInfo>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&zones))
	require.NotEmpty(t, zones.Zones)

	for _, z := range zones.Zones {
		offerings := ec2Offerings(t, ts, map[string]string{
			"Filter.1.Name":    "location",
			"Filter.1.Value.1": z.ZoneName,
		})
		assert.Len(t, offerings, catalogSize(t, ts), "zone %s offers no instance types", z.ZoneName)
	}
}

// TestEC2_FilterWildcards covers the wildcard rules EC2 documents for API filter values,
// through the instance-type filter.
//
// The rules, from EC2's resource-filtering documentation: "An asterisk (*) matches zero or
// more characters, and a question mark (?) matches zero or one character", and a literal
// wildcard is escaped with a backslash. The '?' rule is the one that rules out
// path.Match, whose '?' matches exactly one character — the doc's own example is that
// against prod/prods/production, "prod?" matches only prod and prods.
func TestEC2_FilterWildcards(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantTypes []string
	}{
		{
			name:      "asterisk matches zero characters",
			value:     "m5.large*",
			wantTypes: []string{"m5.large"},
		},
		{
			name:      "question mark matches zero or one character",
			value:     "t3?.micro",
			wantTypes: []string{"t3.micro", "t3a.micro"},
		},
		{
			name:      "a bare asterisk matches everything",
			value:     "*",
			wantTypes: nil, // the whole catalog; asserted below
		},
		{
			name:      "an escaped asterisk is a literal",
			value:     `m5.larg\*`,
			wantTypes: nil, // matches nothing; no type contains an asterisk
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := newEC2TestServer(t)
			offerings := ec2Offerings(t, ts, map[string]string{
				"Filter.1.Name":    "instance-type",
				"Filter.1.Value.1": tt.value,
			})
			switch {
			case tt.value == "*":
				assert.Equal(t, ec2CatalogTypes(t, ts), offeringTypes(offerings))
			case strings.Contains(tt.value, `\`):
				assert.Empty(t, offerings, "an escaped wildcard must not match as a wildcard")
			default:
				assert.Equal(t, tt.wantTypes, offeringTypes(offerings))
			}
		})
	}
}
