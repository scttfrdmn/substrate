package emulator_test

import (
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The AWS Graviton (arm64) families in the instance-type catalog, and Auto Scaling's
// attribute-based selection of them (#1497).

// gravitonType is one instanceTypeSet item, decoded for the members this file asserts.
type gravitonType struct {
	InstanceType string   `xml:"instanceType"`
	VCpus        int      `xml:"vCpuInfo>defaultVCpus"`
	MemoryMiB    int      `xml:"memoryInfo>sizeInMiB"`
	Archs        []string `xml:"processorInfo>supportedArchitectures>item"`
	UsageClasses []string `xml:"supportedUsageClasses>item"`
	Current      bool     `xml:"currentGeneration"`
}

// gravitonFamilies are the thirteen families #1497 added and the sixteen #1509 added — the
// gd and gn variants and g5g — as the guide pages name them.
var gravitonFamilies = []string{
	"t4g", "m6g", "m7g", "m8g", "m9g", "c6g", "c7g", "c8g", "c9g", "r6g", "r7g", "r8g", "r9g",
	"m6gd", "m7gd", "m8gd", "m9gd", "c6gd", "c7gd", "c8gd", "c9gd", "r6gd", "r7gd", "r8gd", "r9gd",
	"c6gn", "c7gn", "c8gn", "g5g",
}

func describeGravitonTypes(t *testing.T, params map[string]string) []gravitonType {
	t.Helper()
	ts := newEC2TestServer(t)
	params["Action"] = "DescribeInstanceTypes"
	var doc struct {
		Items []gravitonType `xml:"instanceTypeSet>item"`
	}
	ec2DescribeXML(t, ts, params, &doc)
	return doc.Items
}

// TestEC2_Graviton_PublishedSpecs pins sizes from each Graviton generation against the
// figures the general purpose, compute optimized and memory optimized guide pages publish,
// including both ends of every ladder: medium is the smallest non-burstable size, and the
// Graviton4 and Graviton5 families reach 48xlarge where Graviton2 and Graviton3 stop at
// 16xlarge.
func TestEC2_Graviton_PublishedSpecs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		instanceType string
		vcpus        int
		memoryMiB    int
	}{
		{"t4g.nano", 2, 512},
		{"t4g.2xlarge", 8, 32768},
		{"m6g.medium", 1, 4096},
		{"m6g.16xlarge", 64, 262144},
		{"c7g.large", 2, 4096},
		{"c7g.16xlarge", 64, 131072},
		{"r7g.medium", 1, 8192},
		{"m8g.48xlarge", 192, 786432},
		{"c8g.24xlarge", 96, 196608},
		{"r8g.48xlarge", 192, 1572864},
		{"m9g.large", 2, 8192},
		{"c9g.48xlarge", 192, 393216},
		{"r9g.24xlarge", 96, 786432},
	}
	params := map[string]string{}
	for i, tc := range tests {
		params["InstanceType."+strconv.Itoa(i+1)] = tc.instanceType
	}
	got := map[string]gravitonType{}
	for _, it := range describeGravitonTypes(t, params) {
		got[it.InstanceType] = it
	}
	require.Len(t, got, len(tests), "every named Graviton type is in the catalog, none is refused")
	for _, tc := range tests {
		it := got[tc.instanceType]
		assert.Equal(t, tc.vcpus, it.VCpus, "%s vCPUs", tc.instanceType)
		assert.Equal(t, tc.memoryMiB, it.MemoryMiB, "%s memory", tc.instanceType)
		assert.Equal(t, []string{"arm64"}, it.Archs, "%s architecture", tc.instanceType)
		assert.Equal(t, []string{"on-demand", "spot"}, it.UsageClasses, "%s usage classes", tc.instanceType)
		assert.True(t, it.Current, "%s is current generation", tc.instanceType)
	}
}

// TestEC2_Graviton_UnpublishedSizesAreRefused pins the edges of the transcription: bare metal
// is excluded as it is for every family, Graviton2 and Graviton3 publish no 24xlarge — their gd
// and gn variants included — and g5g starts at xlarge.
func TestEC2_Graviton_UnpublishedSizesAreRefused(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	for _, typ := range []string{
		"c7g.metal", "m8g.metal-24xl", "c7g.24xlarge", "m6g.nano",
		"c7gd.metal", "c6gn.24xlarge", "m6gd.24xlarge", "g5g.metal", "g5g.large",
	} {
		status, code, _ := ec2ErrorDetail(t, ts, map[string]string{"Action": "DescribeInstanceTypes", "InstanceType.1": typ})
		assert.Equal(t, http.StatusBadRequest, status, typ)
		assert.Equal(t, "InvalidInstanceType", code, typ)
	}
}

// TestEC2_Graviton_ArchitectureFilterSelectsExactlyTheGravitonFamilies is the issue's second
// observation: processor-info.supported-architecture=arm64 matched nothing. It now matches the
// 254 Graviton types and only them — 115 from #1497, 134 gd and gn sizes and five g5g sizes from
// #1509 — and x86_64 still matches every other type.
func TestEC2_Graviton_ArchitectureFilterSelectsExactlyTheGravitonFamilies(t *testing.T) {
	t.Parallel()
	arm := describeGravitonTypes(t, map[string]string{
		"Filter.1.Name": "processor-info.supported-architecture", "Filter.1.Value.1": "arm64",
	})
	require.Len(t, arm, 254)
	for _, it := range arm {
		family, _, _ := strings.Cut(it.InstanceType, ".")
		assert.Contains(t, gravitonFamilies, family)
	}
	x86 := describeGravitonTypes(t, map[string]string{
		"Filter.1.Name": "processor-info.supported-architecture", "Filter.1.Value.1": "x86_64",
	})
	assert.Len(t, x86, 349-254, "the x86 families and the x86 accelerated ones")
}

// TestEC2_Graviton_OfferingsAndSpotPrices pins that the Graviton types reach the other two
// operations that walk the catalog, which is what one catalog for all three guarantees.
func TestEC2_Graviton_OfferingsAndSpotPrices(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	offerings := ec2Offerings(t, ts, map[string]string{
		"Filter.1.Name": "instance-type", "Filter.1.Value.1": "c8g.large",
	})
	assert.Len(t, offerings, 3, "one per seeded zone")

	var prices struct {
		Items []struct {
			InstanceType string `xml:"instanceType"`
			SpotPrice    string `xml:"spotPrice"`
		} `xml:"spotPriceHistorySet>item"`
	}
	ec2DescribeXML(t, ts, map[string]string{
		"Action": "DescribeSpotPriceHistory", "InstanceType.1": "m7g.xlarge",
	}, &prices)
	require.NotEmpty(t, prices.Items)
	for _, p := range prices.Items {
		assert.Equal(t, "m7g.xlarge", p.InstanceType)
		assert.NotEmpty(t, p.SpotPrice)
	}
}

// TestEC2_Graviton_RunInstancesOnArmImage launches a Graviton type from the bundled arm64 AMI
// and reads both back.
func TestEC2_Graviton_RunInstancesOnArmImage(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	var run struct {
		Type    string `xml:"instancesSet>item>instanceType"`
		ImageID string `xml:"instancesSet>item>imageId"`
	}
	ec2DescribeXML(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImageArm, "InstanceType": "c7g.large",
		"MinCount": "1", "MaxCount": "1",
	}, &run)
	assert.Equal(t, "c7g.large", run.Type)
	assert.Equal(t, ec2TestImageArm, run.ImageID)
}

// TestAutoScaling_GravitonMatching is the selection half of #1497, without launching:
// amazon-web-services selects the Graviton families, the AMI's architecture narrows the
// catalog before any requirement is evaluated, and t4g counts as burstable.
func TestAutoScaling_GravitonMatching(t *testing.T) {
	t.Parallel()
	f := func(v float64) *float64 { return &v }
	tests := []struct {
		name string
		arch string
		req  emulator.AutoScalingInstanceRequirements
		want []string
	}{
		{"the issue's requirements on an arm64 AMI", "arm64", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(2), Max: f(2)}, MemoryMiB: emulator.AutoScalingRange{Min: f(4096)},
			CPUManufacturers: []string{"amazon-web-services"}, BurstablePerformance: "excluded",
			AllowedInstanceTypes: []string{"c8g.*", "m8g.*", "c7g.*", "m7g.*"},
		}, []string{"c7g.large", "c8g.large", "m7g.large", "m8g.large"}},
		{"a wildcard allows one family", "arm64", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(2), Max: f(4)}, MemoryMiB: emulator.AutoScalingRange{Min: f(1)},
			AllowedInstanceTypes: []string{"c8g.*"},
		}, []string{"c8g.large", "c8g.xlarge"}},
		{"t4g is burstable", "arm64", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(2), Max: f(2)}, MemoryMiB: emulator.AutoScalingRange{Min: f(1024), Max: f(2048)},
			BurstablePerformance: "required",
		}, []string{"t4g.micro", "t4g.small"}},
		// LocalStorage defaults to included, so the gd variants match beside their base family,
		// and the gn ones by the same figures (#1509).
		{"burstable excluded by default keeps t4g out", "arm64", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(2), Max: f(2)}, MemoryMiB: emulator.AutoScalingRange{Min: f(4096), Max: f(4096)},
		}, []string{
			"c6g.large", "c6gd.large", "c6gn.large", "c7g.large", "c7gd.large", "c7gn.large",
			"c8g.large", "c8gd.large", "c8gn.large", "c9g.large", "c9gd.large",
		}},
		{"amazon-web-services on an x86_64 AMI matches nothing", "x86_64", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(2)}, MemoryMiB: emulator.AutoScalingRange{Min: f(1)},
			CPUManufacturers: []string{"amazon-web-services"},
		}, nil},
		{"intel on an arm64 AMI matches nothing", "arm64", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(2)}, MemoryMiB: emulator.AutoScalingRange{Min: f(1)},
			CPUManufacturers: []string{"intel"}, AllowedInstanceTypes: []string{"m5.*"},
		}, nil},
		{"an x86_64 AMI with no manufacturer named never reaches Graviton", "x86_64", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(1), Max: f(2)}, MemoryMiB: emulator.AutoScalingRange{Min: f(4096), Max: f(4096)},
		}, []string{"c5.large", "c5a.large"}},
		// Anything but arm64 narrows as x86_64, so a group whose AMI says nothing useful keeps
		// the type it chose before the Graviton families existed — not a 1-vCPU c6g.medium.
		{"i386 narrows as x86_64", "i386", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(1)}, MemoryMiB: emulator.AutoScalingRange{Min: f(2048), Max: f(4096)},
		}, []string{"c5.large", "c5a.large"}},
		{"an empty architecture narrows as x86_64", "", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(1)}, MemoryMiB: emulator.AutoScalingRange{Min: f(2048), Max: f(4096)},
		}, []string{"c5.large", "c5a.large"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, emulator.AutoScalingMatchInstanceTypesForTest(&tc.req, tc.arch))
		})
	}
}

// gravitonASG creates a launch template on the ECS-optimized AL2023 arm64 AMI — the parameter
// aws-elastic-globus-compute resolves — and a group using the issue's MixedInstancesPolicy with
// the override's manufacturer and allowed types as given, at desired capacity zero.
func gravitonASG(e *asgEnv, group string, manufacturer string, allowed ...string) {
	e.t.Helper()
	const lt = "graviton-lt"
	e.ec2XML("CreateLaunchTemplate", map[string]string{
		"LaunchTemplateName": lt,
		"LaunchTemplateData.ImageId": emulator.BundledImageID("us-east-1",
			"/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id"),
	}, nil)
	const (
		mip = "MixedInstancesPolicy."
		ir  = mip + "LaunchTemplate.Overrides.member.1.InstanceRequirements."
		dst = mip + "InstancesDistribution."
	)
	params := map[string]string{
		"AutoScalingGroupName": group,
		"MinSize":              "0",
		"MaxSize":              "3",
		"DesiredCapacity":      "0",
		"VPCZoneIdentifier":    e.subnetA,
		mip + "LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateName": lt,
		mip + "LaunchTemplate.LaunchTemplateSpecification.Version":            "$Default",
		ir + "VCpuCount.Min":                        "2",
		ir + "VCpuCount.Max":                        "16",
		ir + "MemoryMiB.Min":                        "4096",
		ir + "CpuManufacturers.member.1":            manufacturer,
		ir + "BurstablePerformance":                 "excluded",
		dst + "OnDemandPercentageAboveBaseCapacity": "0",
		dst + "SpotAllocationStrategy":              "price-capacity-optimized",
	}
	for i, a := range allowed {
		params[ir+"AllowedInstanceTypes.member."+strconv.Itoa(i+1)] = a
	}
	e.mustASG("CreateAutoScalingGroup", params)
}

// TestAutoScaling_GravitonSpotLaunchesInService is the issue's repro end to end: the
// MixedInstancesPolicy it quotes, on an arm64 AMI, scaled to one by SetDesiredCapacity, launches
// a Spot Graviton instance that is InService — where it recorded a Failed activity.
func TestAutoScaling_GravitonSpotLaunchesInService(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	gravitonASG(e, "g", "amazon-web-services", "c8g.*", "m8g.*", "c7g.*", "m7g.*")
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "g", "DesiredCapacity": "1"})

	g := e.group("g")
	require.Len(t, g.Instances, 1)
	assert.Equal(t, "c7g.large", g.Instances[0].InstanceType,
		"the smallest matching type, deterministically: 2 vCPUs and 4 GiB, c7g before c8g by name")
	assert.Equal(t, "InService", g.Instances[0].LifecycleState)

	acts := e.activities(map[string]string{"AutoScalingGroupName": "g"})
	require.NotEmpty(t, acts)
	assert.Equal(t, "Successful", acts[0].StatusCode, "%s", acts[0].StatusMessage)

	insts := e.ec2GroupInstances("g")
	require.Len(t, insts, 1)
	assert.Equal(t, "c7g.large", insts[0].Type)
	assert.Equal(t, "spot", insts[0].Lifecycle, "OnDemandPercentageAboveBaseCapacity 0 is all Spot")
	assert.Equal(t, "running", insts[0].State)
}

// TestAutoScaling_GravitonAMIRefusesAnX86Selection is the other side of the architecture rule:
// the consumer's workaround for #1497 — the same group retargeted at intel and m5.* — launched
// x86 instances from an arm64 AMI. Auto Scaling launches "with a compatible CPU architecture
// based on the AMI", so no type matches, as on AWS.
func TestAutoScaling_GravitonAMIRefusesAnX86Selection(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	gravitonASG(e, "x86", "intel", "m5.*")
	e.mustASG("SetDesiredCapacity", map[string]string{"AutoScalingGroupName": "x86", "DesiredCapacity": "1"})

	assert.Empty(t, e.group("x86").Instances)
	acts := e.activities(map[string]string{"AutoScalingGroupName": "x86"})
	require.NotEmpty(t, acts)
	assert.Equal(t, "Failed", acts[0].StatusCode)
	assert.Equal(t, "No instance types match the instance requirements of the mixed instances policy. Launching EC2 instance failed.",
		acts[0].StatusMessage)
}

// TestAutoScaling_RegisteredImageWithoutArchitectureStaysX86 is the regression the review of
// #1497 found: an AMI registered without Architecture reports RegisterImage's published
// default, i386, which no catalog type reports. Left unnarrowed it would select the new 1-vCPU
// Graviton .medium types for a group that chose c5.large before; it narrows as x86_64 instead.
func TestAutoScaling_RegisteredImageWithoutArchitectureStaysX86(t *testing.T) {
	t.Parallel()
	e := newASGEnv(t)
	var img struct {
		ID string `xml:"imageId"`
	}
	e.ec2XML("RegisterImage", map[string]string{"Name": "no-arch", "RootDeviceName": "/dev/sda1"}, &img)
	require.NotEmpty(t, img.ID)
	e.ec2XML("CreateLaunchTemplate", map[string]string{
		"LaunchTemplateName": "no-arch-lt", "LaunchTemplateData.ImageId": img.ID,
	}, nil)

	const (
		mip = "MixedInstancesPolicy."
		ir  = mip + "LaunchTemplate.Overrides.member.1.InstanceRequirements."
	)
	e.mustASG("CreateAutoScalingGroup", map[string]string{
		"AutoScalingGroupName": "no-arch",
		"MinSize":              "0",
		"MaxSize":              "1",
		"DesiredCapacity":      "1",
		"VPCZoneIdentifier":    e.subnetA,
		mip + "LaunchTemplate.LaunchTemplateSpecification.LaunchTemplateName": "no-arch-lt",
		ir + "VCpuCount.Min": "1",
		ir + "MemoryMiB.Min": "2048",
	})
	g := e.group("no-arch")
	require.Len(t, g.Instances, 1)
	assert.Equal(t, "c5.large", g.Instances[0].InstanceType, "the x86 choice it made before the Graviton families")
}

// TestEC2_Graviton_VariantSpecs pins sizes from each #1509 family against the guide pages'
// figures, and g5g's GPU count, which is reported through gpuInfo and steps from one to two at
// the 16xlarge.
func TestEC2_Graviton_VariantSpecs(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	tests := []struct {
		instanceType string
		vcpus        int
		memoryMiB    int
		gpus         int
	}{
		{"m6gd.medium", 1, 4096, 0},
		{"m7gd.16xlarge", 64, 262144, 0},
		{"m8gd.48xlarge", 192, 786432, 0},
		{"m9gd.24xlarge", 96, 393216, 0},
		{"c6gd.large", 2, 4096, 0},
		{"c7gd.12xlarge", 48, 98304, 0},
		{"c8gd.24xlarge", 96, 196608, 0},
		{"c9gd.48xlarge", 192, 393216, 0},
		{"r6gd.medium", 1, 8192, 0},
		{"r7gd.8xlarge", 32, 262144, 0},
		{"r8gd.48xlarge", 192, 1572864, 0},
		{"r9gd.xlarge", 4, 32768, 0},
		{"c6gn.16xlarge", 64, 131072, 0},
		{"c7gn.medium", 1, 2048, 0},
		{"c8gn.48xlarge", 192, 393216, 0},
		{"g5g.xlarge", 4, 8192, 1},
		{"g5g.8xlarge", 32, 65536, 1},
		{"g5g.16xlarge", 64, 131072, 2},
	}
	for _, tc := range tests {
		t.Run(tc.instanceType, func(t *testing.T) {
			var doc struct {
				Items []struct {
					InstanceType string   `xml:"instanceType"`
					VCpus        int      `xml:"vCpuInfo>defaultVCpus"`
					MemoryMiB    int      `xml:"memoryInfo>sizeInMiB"`
					Archs        []string `xml:"processorInfo>supportedArchitectures>item"`
					GPUs         int      `xml:"gpuInfo>gpus>item>count"`
					Current      bool     `xml:"currentGeneration"`
				} `xml:"instanceTypeSet>item"`
			}
			ec2DescribeXML(t, ts, map[string]string{"Action": "DescribeInstanceTypes", "InstanceType.1": tc.instanceType}, &doc)
			require.Len(t, doc.Items, 1)
			it := doc.Items[0]
			assert.Equal(t, tc.vcpus, it.VCpus)
			assert.Equal(t, tc.memoryMiB, it.MemoryMiB)
			assert.Equal(t, []string{"arm64"}, it.Archs)
			assert.Equal(t, tc.gpus, it.GPUs)
			assert.True(t, it.Current)
		})
	}
}

// TestAutoScaling_GravitonVariantAttributes pins how attribute-based selection reads the #1509
// families: the gd variants carry instance store, so LocalStorage selects or drops them while the
// gn variants follow their EBS-only base; all are amazon-web-services; g5g is a GPU type an arm64
// AMI can select.
func TestAutoScaling_GravitonVariantAttributes(t *testing.T) {
	t.Parallel()
	f := func(v float64) *float64 { return &v }
	twoByFour := func(local string) emulator.AutoScalingInstanceRequirements {
		return emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(2), Max: f(2)}, MemoryMiB: emulator.AutoScalingRange{Min: f(4096), Max: f(4096)},
			CPUManufacturers: []string{"amazon-web-services"}, AllowedInstanceTypes: []string{"c6g*", "c7g*"},
			LocalStorage: local,
		}
	}
	tests := []struct {
		name string
		req  emulator.AutoScalingInstanceRequirements
		want []string
	}{
		{"included by default", twoByFour(""), []string{"c6g.large", "c6gd.large", "c6gn.large", "c7g.large", "c7gd.large", "c7gn.large"}},
		{"required keeps only gd", twoByFour("required"), []string{"c6gd.large", "c7gd.large"}},
		{"excluded drops gd", twoByFour("excluded"), []string{"c6g.large", "c6gn.large", "c7g.large", "c7gn.large"}},
		{"a GPU on an arm64 AMI is g5g", emulator.AutoScalingInstanceRequirements{
			VCpuCount: emulator.AutoScalingRange{Min: f(4), Max: f(8)}, MemoryMiB: emulator.AutoScalingRange{Min: f(1)},
			AcceleratorTypes: []string{"gpu"}, CPUManufacturers: []string{"amazon-web-services"},
		}, []string{"g5g.xlarge", "g5g.2xlarge"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, emulator.AutoScalingMatchInstanceTypesForTest(&tc.req, "arm64"))
		})
	}
}
