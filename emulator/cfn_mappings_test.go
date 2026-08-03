package emulator_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// regionAMIMappings is the region→AMI shape the Fn::FindInMap reference leads
// with, plus a list-valued leaf, since "the values can be of type String or
// List".
var regionAMIMappings = map[string]map[string]map[string]interface{}{
	"AWSInstanceType2Arch": {
		"t3.micro": {"Arch": "HVM64"},
		"t4g.nano": {"Arch": "ARM64"},
	},
	"AWSRegionArch2AMI": {
		"us-east-1": {"HVM64": "ami-0e1", "ARM64": "ami-0a1"},
		"eu-west-1": {"HVM64": "ami-0e2", "ARM64": "ami-0a2"},
	},
	"SecurityGroups": {
		"Dev":  {"Ids": []interface{}{"sg-dev1"}},
		"Prod": {"Ids": []interface{}{"sg-prod1", "sg-prod2"}},
	},
}

// TestCFN_FindInMap_Conventions pins every branch of the lookup, including the
// two a resolver returning a string cannot otherwise distinguish: a key that is
// missing and a key whose value is the empty string.
func TestCFN_FindInMap_Conventions(t *testing.T) {
	t.Parallel()

	findInMap := func(args ...interface{}) map[string]interface{} {
		return map[string]interface{}{"Fn::FindInMap": args}
	}
	ref := func(name string) map[string]interface{} {
		return map[string]interface{}{"Ref": name}
	}

	tests := []struct {
		name         string
		value        interface{}
		region       string
		params       map[string]string
		want         string
		wantFailures int
	}{
		{
			name:   "three literal keys",
			value:  findInMap("AWSRegionArch2AMI", "us-east-1", "HVM64"),
			region: "us-east-1",
			want:   "ami-0e1",
		},
		{
			name:   "top-level key from a pseudo-parameter",
			value:  findInMap("AWSRegionArch2AMI", ref("AWS::Region"), "HVM64"),
			region: "eu-west-1",
			want:   "ami-0e2",
		},
		{
			name: "nested FindInMap as the second-level key",
			// The documented region→arch→AMI form, which is the reason
			// Fn::FindInMap is one of the two functions permitted inside a
			// Fn::FindInMap's own arguments.
			value: findInMap("AWSRegionArch2AMI", ref("AWS::Region"),
				findInMap("AWSInstanceType2Arch", ref("InstanceType"), "Arch")),
			region: "us-east-1",
			params: map[string]string{"InstanceType": "t4g.nano"},
			want:   "ami-0a1",
		},
		{
			name:   "a list-valued leaf is rejoined in a scalar context",
			value:  findInMap("SecurityGroups", "Prod", "Ids"),
			region: "us-east-1",
			want:   "sg-prod1,sg-prod2",
		},
		{
			name:         "an unknown mapping name fails rather than yielding a literal",
			value:        findInMap("NoSuchMap", "us-east-1", "HVM64"),
			region:       "us-east-1",
			want:         "",
			wantFailures: 1,
		},
		{
			name:         "an unknown top-level key fails",
			value:        findInMap("AWSRegionArch2AMI", "ap-south-1", "HVM64"),
			region:       "us-east-1",
			want:         "",
			wantFailures: 1,
		},
		{
			name:         "an unknown second-level key fails",
			value:        findInMap("AWSRegionArch2AMI", "us-east-1", "RISCV"),
			region:       "us-east-1",
			want:         "",
			wantFailures: 1,
		},
		{
			name: "DefaultValue covers a missing top-level key",
			value: findInMap("AWSRegionArch2AMI", "ap-south-1", "HVM64",
				map[string]interface{}{"DefaultValue": "ami-fallback"}),
			region: "us-east-1",
			want:   "ami-fallback",
		},
		{
			name: "DefaultValue covers a missing second-level key",
			value: findInMap("AWSRegionArch2AMI", "us-east-1", "RISCV",
				map[string]interface{}{"DefaultValue": "ami-fallback"}),
			region: "us-east-1",
			want:   "ami-fallback",
		},
		{
			name: "DefaultValue is not consulted when the key resolves",
			value: findInMap("AWSRegionArch2AMI", "us-east-1", "HVM64",
				map[string]interface{}{"DefaultValue": "ami-fallback"}),
			region: "us-east-1",
			want:   "ami-0e1",
		},
		{
			name: "a misspelled fourth argument is not a default",
			// The fourth parameter "must be a map with the key DefaultValue", so
			// a template that writes Default instead gets the lookup failure
			// CloudFormation would give it rather than a silently different
			// value.
			value: findInMap("AWSRegionArch2AMI", "ap-south-1", "HVM64",
				map[string]interface{}{"Default": "ami-fallback"}),
			region:       "us-east-1",
			want:         "",
			wantFailures: 1,
		},
		{
			name:         "too few arguments fails",
			value:        findInMap("AWSRegionArch2AMI", "us-east-1"),
			region:       "us-east-1",
			want:         "",
			wantFailures: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, failures := emulator.CFNResolveWithMappingsForTest(
				tt.value, regionAMIMappings, tt.params, tt.region)
			assert.Equal(t, tt.want, got)
			assert.Len(t, failures, tt.wantFailures,
				"failures recorded: %v", failures)
			for _, f := range failures {
				assert.Contains(t, f, "Fn::FindInMap",
					"a reason reaches DescribeStackResources, so it must name the intrinsic")
			}
		})
	}
}

// TestCFN_FindInMap_ListLeafKeepsElements pins that a list-valued mapping leaf
// contributes its members in a list-valued context rather than one rejoined
// string — the `SecurityGroupIds: !FindInMap [...]` form.
func TestCFN_FindInMap_ListLeafKeepsElements(t *testing.T) {
	t.Parallel()

	got, failures := emulator.CFNResolveListWithMappingsForTest(
		map[string]interface{}{"Fn::FindInMap": []interface{}{"SecurityGroups", "Prod", "Ids"}},
		regionAMIMappings, nil, "us-east-1")
	assert.Empty(t, failures)
	assert.Equal(t, []string{"sg-prod1", "sg-prod2"}, got)

	// A failed lookup contributes nothing rather than one empty member, so a
	// property built from it is absent instead of holding "".
	got, failures = emulator.CFNResolveListWithMappingsForTest(
		map[string]interface{}{"Fn::FindInMap": []interface{}{"SecurityGroups", "Staging", "Ids"}},
		regionAMIMappings, nil, "us-east-1")
	assert.Empty(t, got)
	assert.Len(t, failures, 1)
}

// TestCFN_GetAZs_AgreesWithDescribeAvailabilityZones is the assertion the issue
// asks for: Fn::GetAZs and EC2's own DescribeAvailabilityZones must name the same
// zones, because a subnet placed in a zone EC2 does not report is not something a
// caller can then query.
func TestCFN_GetAZs_AgreesWithDescribeAvailabilityZones(t *testing.T) {
	// ec2Request pins the Host to ec2.us-east-1.amazonaws.com, so that is the
	// region both answers are read for.
	const region = "us-east-1"

	ts := newEC2TestServer(t)
	resp := ec2Request(t, ts, map[string]string{"Action": "DescribeAvailabilityZones"})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Read the zone names off the wire rather than out of a Go-side list: the point
	// of the assertion is that the two answers a caller can obtain agree.
	var described struct {
		AZs []struct {
			ZoneName string `xml:"zoneName"`
		} `xml:"availabilityZoneInfo>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&described))
	wire := make([]string, 0, len(described.AZs))
	for _, az := range described.AZs {
		wire = append(wire, az.ZoneName)
	}
	require.NotEmpty(t, wire)

	azs, failures := emulator.CFNResolveListWithMappingsForTest(
		map[string]interface{}{"Fn::GetAZs": ""}, nil, nil, region)
	assert.Empty(t, failures)
	// Equal, not Subset, in both directions at once: a resolver that invented a
	// fourth zone and one that reported only the first are both wrong, and a
	// containment assertion catches only one of them.
	assert.Equal(t, wire, azs,
		"Fn::GetAZs and DescribeAvailabilityZones must name the same zones")
}

// TestCFN_GetAZs_Conventions pins the region argument's three documented forms.
func TestCFN_GetAZs_Conventions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args interface{}
		want []string
	}{
		{
			// "Specifying an empty string is equivalent to specifying
			// AWS::Region."
			name: "an empty string means the caller's region",
			args: "",
			want: emulator.CFNSeededAZsForTest("us-west-2"),
		},
		{
			name: "Ref AWS::Region, the documented long form",
			args: map[string]interface{}{"Ref": "AWS::Region"},
			want: emulator.CFNSeededAZsForTest("us-west-2"),
		},
		{
			name: "an explicit region is not the caller's",
			args: "ap-northeast-1",
			want: emulator.CFNSeededAZsForTest("ap-northeast-1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, failures := emulator.CFNResolveListWithMappingsForTest(
				map[string]interface{}{"Fn::GetAZs": tt.args}, nil, nil, "us-west-2")
			assert.Empty(t, failures)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestCFN_Cidr_DocumentedExamples pins Fn::Cidr against the reference's own
// examples, and against the requests it must refuse.
//
// Refusing matters as much as answering: a template doing
// `!Select [3, !Cidr [...]]` over a list that came back short would read an empty
// string out of it, which is the shape of defect #522 is about.
func TestCFN_Cidr_DocumentedExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		block   string
		count   int
		bits    int
		want    []string
		wantErr string
	}{
		{
			// "This example creates 6 CIDRs with a subnet mask "/27" inside from
			// a CIDR with a mask of "/24"."
			name:  "the reference's basic example",
			block: "192.168.0.0/24",
			count: 6,
			bits:  5,
			want: []string{
				"192.168.0.0/27", "192.168.0.32/27", "192.168.0.64/27",
				"192.168.0.96/27", "192.168.0.128/27", "192.168.0.160/27",
			},
		},
		{
			// The IPv6-enabled-VPC example: cidrBits 8 against the VPC's /16.
			name:  "cidrBits 8 gives a /24",
			block: "10.0.0.0/16",
			count: 4,
			bits:  8,
			want:  []string{"10.0.0.0/24", "10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"},
		},
		{
			// The same example's Ipv6CidrBlock: cidrBits 64 against a /56. The
			// mask is 128 − 64, so the address family has to come from the block
			// rather than being assumed to be IPv4.
			name:  "IPv6 takes its width from the address",
			block: "2001:db8::/56",
			count: 2,
			bits:  64,
			want:  []string{"2001:db8::/64", "2001:db8:0:1::/64"},
		},
		{
			name:  "an unaligned block is masked first",
			block: "10.0.0.17/16",
			count: 1,
			bits:  8,
			want:  []string{"10.0.0.0/24"},
		},
		{
			name:    "a count the block cannot hold is refused",
			block:   "10.0.0.0/24",
			count:   3,
			bits:    7,
			wantErr: "holds only 2",
		},
		{
			name:    "cidrBits wider than the block is refused",
			block:   "10.0.0.0/24",
			count:   1,
			bits:    16,
			wantErr: "would widen",
		},
		{
			name:    "a malformed ipBlock is refused",
			block:   "10.0.0.0",
			count:   1,
			bits:    8,
			wantErr: "is not a CIDR block",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := emulator.CFNCidrBlocksForTest(tt.block, tt.count, tt.bits)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Empty(t, got, "a refused request must not return a partial list")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestCFN_Cidr_ThroughResolver pins the count/cidrBits validation the resolver
// applies before the split, and that a bad request records a reason.
func TestCFN_Cidr_ThroughResolver(t *testing.T) {
	t.Parallel()

	cidr := func(args ...interface{}) map[string]interface{} {
		return map[string]interface{}{"Fn::Cidr": args}
	}

	tests := []struct {
		name         string
		value        interface{}
		want         []string
		wantFailures int
	}{
		{
			name:  "numeric strings, as the JSON example writes them",
			value: cidr("192.168.0.0/24", "2", "5"),
			want:  []string{"192.168.0.0/27", "192.168.0.32/27"},
		},
		{
			name:  "YAML numbers, as the short form writes them",
			value: cidr("192.168.0.0/24", float64(2), float64(5)),
			want:  []string{"192.168.0.0/27", "192.168.0.32/27"},
		},
		{
			// "count — The number of CIDRs to generate. Valid range is between
			// 1 and 256."
			name:         "count above 256 is refused",
			value:        cidr("10.0.0.0/8", "257", "8"),
			wantFailures: 1,
		},
		{
			name:         "count of zero is refused",
			value:        cidr("10.0.0.0/8", "0", "8"),
			wantFailures: 1,
		},
		{
			name:         "too few arguments is refused",
			value:        cidr("10.0.0.0/8", "2"),
			wantFailures: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, failures := emulator.CFNResolveListWithMappingsForTest(tt.value, nil, nil, "us-east-1")
			assert.Len(t, failures, tt.wantFailures, "failures: %v", failures)
			if tt.wantFailures == 0 {
				assert.Equal(t, tt.want, got)
				return
			}
			assert.Empty(t, got)
			for _, f := range failures {
				assert.Contains(t, f, "Fn::Cidr")
			}
		})
	}
}

// TestCFN_PseudoParameters pins the four #522 adds, including the two whose value
// depends on the caller's partition.
func TestCFN_PseudoParameters(t *testing.T) {
	t.Parallel()

	t.Run("partition and URL suffix follow the region", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			region        string
			wantPartition string
			wantSuffix    string
		}{
			{"us-east-1", "aws", "amazonaws.com"},
			{"eu-west-1", "aws", "amazonaws.com"},
			// "the partition for resources in the China (Beijing and Ningxia)
			// Regions is aws-cn"
			{"cn-north-1", "aws-cn", "amazonaws.com.cn"},
			{"cn-northwest-1", "aws-cn", "amazonaws.com.cn"},
			// "the partition for resources in the AWS GovCloud (US-West) Region
			// is aws-us-gov"
			{"us-gov-west-1", "aws-us-gov", "amazonaws.com"},
			{"us-gov-east-1", "aws-us-gov", "amazonaws.com"},
		}
		for _, tt := range tests {
			assert.Equal(t, tt.wantPartition, emulator.CFNPartitionForTest(tt.region), tt.region)
			assert.Equal(t, tt.wantSuffix, emulator.CFNURLSuffixForTest(tt.region), tt.region)
		}
	})

	t.Run("resolved through Ref and Fn::Sub alike", func(t *testing.T) {
		t.Parallel()
		// Both spellings reach resolveRef, which is the invariant worth pinning:
		// a template writes `!Sub 'arn:${AWS::Partition}:s3:::b'` far more often
		// than it writes a bare Ref.
		got, failures := emulator.CFNResolveWithMappingsForTest(
			map[string]interface{}{"Ref": "AWS::Partition"}, nil, nil, "cn-north-1")
		assert.Empty(t, failures)
		assert.Equal(t, "aws-cn", got)

		got, failures = emulator.CFNResolveWithMappingsForTest(
			map[string]interface{}{"Fn::Sub": "arn:${AWS::Partition}:s3:::${AWS::URLSuffix}"},
			nil, nil, "cn-north-1")
		assert.Empty(t, failures)
		assert.Equal(t, "arn:aws-cn:s3:::amazonaws.com.cn", got)
	})

	t.Run("StackId is the stack ARN", func(t *testing.T) {
		t.Parallel()
		got, failures := emulator.CFNResolveWithMappingsForTest(
			map[string]interface{}{"Ref": "AWS::StackId"}, nil, nil, "us-east-1")
		assert.Empty(t, failures)
		// "arn:aws:cloudformation:us-west-2:123456789012:stack/teststack/51af…"
		assert.Regexp(t,
			`^arn:aws:cloudformation:us-east-1:123456789012:stack/teststack/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
			got)
		// Deterministic, which is the property a replay depends on.
		again, _ := emulator.CFNResolveWithMappingsForTest(
			map[string]interface{}{"Ref": "AWS::StackId"}, nil, nil, "us-east-1")
		assert.Equal(t, got, again)
	})

	t.Run("NotificationARNs is an empty list", func(t *testing.T) {
		t.Parallel()
		// "Unlike other pseudo parameters […] returns a list of ARNs." Substrate
		// has no notification model, so the list is empty — and empty is the
		// accurate answer for a stack created without any, not a placeholder.
		got, failures := emulator.CFNResolveListWithMappingsForTest(
			map[string]interface{}{"Ref": "AWS::NotificationARNs"}, nil, nil, "us-east-1")
		assert.Empty(t, failures)
		assert.Empty(t, got)

		// The documented `!Select ['0', !Ref 'AWS::NotificationARNs']` therefore
		// resolves to the empty string rather than to the reference string, which
		// is what would have reached a TopicARN property before (#522).
		scalar, failures := emulator.CFNResolveWithMappingsForTest(
			map[string]interface{}{"Fn::Select": []interface{}{
				"0", map[string]interface{}{"Ref": "AWS::NotificationARNs"},
			}}, nil, nil, "us-east-1")
		assert.Empty(t, failures)
		assert.Empty(t, scalar)
	})
}

// TestCFN_FindInMap_UnknownKeyFailsTheResource is the end-to-end gate: the
// template the issue describes — a region→AMI lookup that misses — must make the
// instance CREATE_FAILED rather than launching with the intrinsic's JSON encoding
// as its ImageId.
func TestCFN_FindInMap_UnknownKeyFailsTheResource(t *testing.T) {
	t.Parallel()

	deployer, _ := newFullTestDeployerWithRegistry(t)

	const template = `
AWSTemplateFormatVersion: '2010-09-09'
Mappings:
  RegionMap:
    eu-west-1:
      AMI: ami-0eu1
Resources:
  Good:
    Type: AWS::EC2::Instance
    Properties:
      ImageId: !FindInMap [RegionMap, eu-west-1, AMI]
      InstanceType: t3.micro
  Bad:
    Type: AWS::EC2::Instance
    Properties:
      ImageId: !FindInMap [RegionMap, ap-south-1, AMI]
      InstanceType: t3.micro
`
	result, err := deployer.Deploy(context.Background(), template, "findinmap-stack", nil)
	require.NoError(t, err, "one unresolvable property must not abort the stack")

	byLogicalID := map[string]emulator.DeployedResource{}
	for _, r := range result.Resources {
		byLogicalID[r.LogicalID] = r
	}

	good, ok := byLogicalID["Good"]
	require.True(t, ok)
	assert.Empty(t, good.Error, "a resolvable lookup must deploy")

	bad, ok := byLogicalID["Bad"]
	require.True(t, ok)
	require.NotEmpty(t, bad.Error,
		"an unknown mapping key must fail the resource, not launch it with a JSON literal")
	assert.Contains(t, bad.Error, "Fn::FindInMap")
	assert.Contains(t, bad.Error, "ap-south-1")

	// The reason reaches DescribeStackResources as CREATE_FAILED, which is the
	// observable a consumer polls — see the refused-resource behavior #519
	// established.
	assert.NotContains(t, good.Error, "Fn::FindInMap",
		"the failure must be attributed to the resource that caused it, not carried forward")
}

// TestCFN_MappingsIntrinsics_ThroughDeploy pins that the three new intrinsics
// reach a real deploy path with their values resolved, since a resolver that is
// right in isolation is still wrong if no template can reach it.
func TestCFN_MappingsIntrinsics_ThroughDeploy(t *testing.T) {
	t.Parallel()

	deployer, registry := newFullTestDeployerWithRegistry(t)

	const template = `
AWSTemplateFormatVersion: '2010-09-09'
Mappings:
  EnvMap:
    Prod:
      Cidr: 10.0.0.0/16
Resources:
  VPC:
    Type: AWS::EC2::VPC
    Properties:
      CidrBlock: !FindInMap [EnvMap, Prod, Cidr]
  Subnet:
    Type: AWS::EC2::Subnet
    Properties:
      VpcId: !Ref VPC
      CidrBlock: !Select
        - 0
        - !Cidr
          - !FindInMap [EnvMap, Prod, Cidr]
          - 4
          - 8
      AvailabilityZone: !Select
        - 1
        - !GetAZs ''
`
	result, err := deployer.Deploy(context.Background(), template, "intrinsics-stack", nil)
	require.NoError(t, err)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "%s failed: %s", r.LogicalID, r.Error)
	}
	subnet := findResource(t, result, "Subnet")
	require.NotEmpty(t, subnet.PhysicalID)

	// Read the subnet back through EC2 rather than asserting on DeployResult: the
	// deploy result reports what the deployer intended, and #527's symptom was a
	// correct-looking result over a wrong stored value.
	body := routeQuery(t, registry, "ec2", map[string]string{
		"Action":     "DescribeSubnets",
		"SubnetId.1": subnet.PhysicalID,
	})

	// The first /24 of the mapped /16, so Fn::Cidr ran over Fn::FindInMap's result
	// rather than over the intrinsic's JSON encoding.
	assert.Contains(t, body, "<cidrBlock>10.0.0.0/24</cidrBlock>")

	// The second zone the same emulator reports for this region — index 1, so a
	// resolver that truncated Fn::GetAZs to one element would fail here.
	zones := emulator.CFNSeededAZsForTest("us-east-1")
	require.GreaterOrEqual(t, len(zones), 2)
	assert.Contains(t, body, "<availabilityZone>"+zones[1]+"</availabilityZone>")
}

// TestCFNPlugin_FindInMapFailureReachesTheWire is the observable a consumer
// actually polls: DescribeStackResources over the wire, not a DeployResult a Go
// caller reads.
//
// A resolution failure recorded on the resource is only useful if it survives to
// the XML, and the derivation that carries it — DeployedResource.Error →
// CREATE_FAILED plus ResourceStatusReason — is the one #519 established.
func TestCFNPlugin_FindInMapFailureReachesTheWire(t *testing.T) {
	ts := newCFNTestServer(t)

	const template = `{"Mappings":{"RegionMap":{"eu-west-1":{"Bucket":"eu-data"}}},` +
		`"Resources":{"Data":{"Type":"AWS::S3::Bucket",` +
		`"Properties":{"BucketName":{"Fn::FindInMap":["RegionMap","ap-south-1","Bucket"]}}}}}`

	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "findinmap-wire",
		"TemplateBody": template,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	code, body = cfnAction(t, ts, "DescribeStackResources", map[string]string{
		"StackName": "findinmap-wire",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, "<ResourceStatus>CREATE_FAILED</ResourceStatus>")
	assert.Contains(t, body, "Fn::FindInMap")
	assert.Contains(t, body, "ap-south-1")
}

// TestCFNPlugin_StackIDPseudoParameterMatchesCreateStack pins that the stack ARN a
// template can read about itself is the one CreateStack reported.
//
// Two builders would have been the easy implementation and the wrong one: a
// consumer that captures StackId from CreateStack and then compares it against a
// resource property built from AWS::StackId would find two different ARNs for one
// stack, which is the divergence #517 fixed for the caller's account.
func TestCFNPlugin_StackIDPseudoParameterMatchesCreateStack(t *testing.T) {
	ts := newCFNTestServer(t)

	const template = `{"Resources":{},"Outputs":{` +
		`"SelfID":{"Value":{"Ref":"AWS::StackId"}},` +
		`"Partition":{"Value":{"Ref":"AWS::Partition"}},` +
		`"Suffix":{"Value":{"Ref":"AWS::URLSuffix"}}}}`

	code, body := cfnAction(t, ts, "CreateStack", map[string]string{
		"StackName":    "selfaware",
		"TemplateBody": template,
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	var created struct {
		StackID string `xml:"CreateStackResult>StackId"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &created))
	require.NotEmpty(t, created.StackID)

	code, body = cfnAction(t, ts, "DescribeStacks", map[string]string{"StackName": "selfaware"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	var described struct {
		Outputs []struct {
			Key   string `xml:"OutputKey"`
			Value string `xml:"OutputValue"`
		} `xml:"DescribeStacksResult>Stacks>member>Outputs>member"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &described))

	outputs := map[string]string{}
	for _, o := range described.Outputs {
		outputs[o.Key] = o.Value
	}
	assert.Equal(t, created.StackID, outputs["SelfID"],
		"AWS::StackId and CreateStack's StackId must describe the same stack")
	assert.Equal(t, "aws", outputs["Partition"])
	assert.Equal(t, "amazonaws.com", outputs["Suffix"])
}

// TestCFN_ListValuedShapeInsideAStructuredProperty pins the *shape* an intrinsic
// resolves to inside a structured property, which no scalar assertion can reach.
//
// A scalar context rejoins a list on commas, so a resolver that had lost the list
// and one that kept it produce the same string there. Inside a property the two are
// different JSON — an array against a string — and the array is what a typed plugin
// unmarshals into a `[]string` member.
func TestCFN_ListValuedShapeInsideAStructuredProperty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value interface{}
		want  interface{}
	}{
		{
			name:  "a list-valued mapping leaf becomes an array",
			value: map[string]interface{}{"Fn::FindInMap": []interface{}{"SecurityGroups", "Prod", "Ids"}},
			want:  []interface{}{"sg-prod1", "sg-prod2"},
		},
		{
			// One-element, not a bare string: the leaf's declared shape decides,
			// not how many members it happens to hold.
			name:  "a single-member list leaf is still an array",
			value: map[string]interface{}{"Fn::FindInMap": []interface{}{"SecurityGroups", "Dev", "Ids"}},
			want:  []interface{}{"sg-dev1"},
		},
		{
			// A string leaf must *not* become a one-element array, or every
			// scalar property built from a mapping would arrive as a list.
			name:  "a string leaf stays a string",
			value: map[string]interface{}{"Fn::FindInMap": []interface{}{"AWSRegionArch2AMI", "us-east-1", "HVM64"}},
			want:  "ami-0e1",
		},
		{
			name:  "Fn::GetAZs is an array",
			value: map[string]interface{}{"Fn::GetAZs": ""},
			want:  []interface{}{"us-east-1a", "us-east-1b", "us-east-1c"},
		},
		{
			name: "Fn::Cidr is an array",
			value: map[string]interface{}{"Fn::Cidr": []interface{}{
				"10.0.0.0/16", "2", "8",
			}},
			want: []interface{}{"10.0.0.0/24", "10.0.1.0/24"},
		},
		{
			// The whole reason AWS::NotificationARNs is modeled at all: a
			// property that takes a list of ARNs must receive an empty list, not
			// the one-element list holding "" that a scalar Ref would give.
			name:  "AWS::NotificationARNs is an empty array, not a one-element one",
			value: map[string]interface{}{"Ref": "AWS::NotificationARNs"},
			want:  []interface{}{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Wrapped in a property map, since resolveNested's list-splicing and
			// shape-preserving arms are reached through a member rather than at
			// the top level.
			got, failures := emulator.CFNResolveNestedWithMappingsForTest(
				map[string]interface{}{"Member": tt.value},
				regionAMIMappings, nil, "us-east-1")
			assert.Empty(t, failures)
			assert.Equal(t, map[string]interface{}{"Member": tt.want}, got)
		})
	}
}

// TestCFN_ResolutionFailureIsNotInheritedByALaterResource pins that a reason is
// attributed to the resource that caused it.
//
// The failure channel is state on the context, shared across every resource in the
// stack, so the drain is what keeps it per-resource. Getting that wrong is worse
// than not reporting at all: a stack whose first resource has a bad mapping key
// would report every *later* resource CREATE_FAILED too, and a consumer would
// chase a defect in the wrong resource.
func TestCFN_ResolutionFailureIsNotInheritedByALaterResource(t *testing.T) {
	t.Parallel()

	deployer := newFullTestDeployer(t)

	// Bad sorts before Later alphabetically and both are the same type, so the
	// deploy order puts the failing resource first whichever way ties are broken.
	const template = `
AWSTemplateFormatVersion: '2010-09-09'
Mappings:
  RegionMap:
    eu-west-1:
      Name: eu-bucket
Resources:
  Bad:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !FindInMap [RegionMap, ap-south-1, Name]
  Later:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: later-bucket
  Third:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: third-bucket
`
	result, err := deployer.Deploy(context.Background(), template, "inherit-stack", nil)
	require.NoError(t, err)

	bad := findResource(t, result, "Bad")
	require.NotEmpty(t, bad.Error, "the failing resource must carry the reason")

	for _, id := range []string{"Later", "Third"} {
		r := findResource(t, result, id)
		assert.Empty(t, r.Error,
			"%s did not fail; a reason recorded against Bad must not be reported against it", id)
		assert.NotEmpty(t, r.PhysicalID, "%s must still have been created", id)
	}
}

// TestCFN_ConditionResolutionFailureIsNotBlamedOnAResource covers the one place a
// failure is recorded with no resource to attribute it to.
//
// Conditions are evaluated once, before any resource deploys, and a condition may
// contain a `Fn::FindInMap` — `!Not [!Equals [!FindInMap [M, K1, K2], ”]]` is the
// conventional shape. A lookup that misses there records a reason against a context
// that has not begun deploying anything, so without a drain before the first
// dispatch the first resource in the stack inherits it and reports CREATE_FAILED for
// a property it never had.
//
// Dropping the reason rather than reporting it elsewhere is deliberate: a condition
// is not a resource, `DescribeStackResources` has no row for one, and blaming an
// unrelated resource is worse than the failure being visible only in the log.
func TestCFN_ConditionResolutionFailureIsNotBlamedOnAResource(t *testing.T) {
	t.Parallel()

	deployer := newFullTestDeployer(t)

	const template = `
AWSTemplateFormatVersion: '2010-09-09'
Mappings:
  RegionMap:
    eu-west-1:
      Flag: 'yes'
Conditions:
  # ap-south-1 is absent from RegionMap, so this lookup fails during condition
  # evaluation — before any resource exists to carry the reason.
  Enabled: !Equals [!FindInMap [RegionMap, ap-south-1, Flag], 'yes']
Resources:
  First:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: first-bucket
`
	result, err := deployer.Deploy(context.Background(), template, "condfail-stack", nil)
	require.NoError(t, err)

	first := findResource(t, result, "First")
	assert.Empty(t, first.Error,
		"a condition's resolution failure must not be reported against the first resource")
	assert.NotEmpty(t, first.PhysicalID, "the resource must still have been created")
}
