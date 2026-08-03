package emulator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestCFN_FnSelect_OverFnSplit covers the idiom the CloudFormation Fn::Split
// reference leads with: split a string, then Fn::Select an element out of the
// resulting list. Before #521 this resolved to the empty string, because
// Fn::Select required its second argument to be a literal list and an intrinsic
// resolved to a single string.
func TestCFN_FnSelect_OverFnSplit(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"QueueArn": {"Type": "String", "Default": "arn:aws:sqs:us-east-1:123456789012:jobs"}
		},
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "select-over-split"}}
		},
		"Outputs": {
			"Service":   {"Value": {"Fn::Select": ["2", {"Fn::Split": [":", {"Ref": "QueueArn"}]}]}},
			"QueueName": {"Value": {"Fn::Select": ["5", {"Fn::Split": [":", {"Ref": "QueueArn"}]}]}}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "select-split-stack", nil)
	require.NoError(t, err)
	assert.Equal(t, "sqs", result.Outputs["Service"])
	assert.Equal(t, "jobs", result.Outputs["QueueName"])
}

// TestCFN_FnSplit_ScalarContextJoins pins the documented behavior of Fn::Split
// where the property is scalar and has nowhere to put a list: the elements are
// rejoined on the delimiter, reproducing the source string, rather than
// truncating to the first element.
func TestCFN_FnSplit_ScalarContextJoins(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "split-scalar"}}
		},
		"Outputs": {
			"Path": {"Value": {"Fn::Split": ["/", "a/b/c"]}}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "split-scalar-stack", nil)
	require.NoError(t, err)
	assert.Equal(t, "a/b/c", result.Outputs["Path"])
}

// newVPCWithGroups creates a VPC, one subnet in it, and n security groups, and
// returns the subnet ID and the group IDs.
//
// A list-valued SecurityGroupIds property is only observable end to end against
// groups that exist and belong to the launch subnet's VPC — RunInstances rejects
// both an unknown group and one from another VPC — so the IDs a test splits have
// to come from the API rather than be invented.
func newVPCWithGroups(t *testing.T, registry *emulator.PluginRegistry, n int) (string, []string) {
	t.Helper()
	vpcID := extractXMLTag(t, routeQuery(t, registry, "ec2", map[string]string{
		"Action":    "CreateVpc",
		"CidrBlock": "10.0.0.0/16",
	}), "vpcId")
	subnetID := extractXMLTag(t, routeQuery(t, registry, "ec2", map[string]string{
		"Action":    "CreateSubnet",
		"VpcId":     vpcID,
		"CidrBlock": "10.0.1.0/24",
	}), "subnetId")

	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		body := routeQuery(t, registry, "ec2", map[string]string{
			"Action":           "CreateSecurityGroup",
			"VpcId":            vpcID,
			"GroupName":        fmt.Sprintf("split-group-%d", i),
			"GroupDescription": "list-valued property fixture",
		})
		id := extractXMLTag(t, body, "groupId")
		require.NotEmpty(t, id)
		ids = append(ids, id)
	}
	return subnetID, ids
}

// extractXMLTag returns the text content of the first <name> element in body.
func extractXMLTag(t *testing.T, body, name string) string {
	t.Helper()
	openTag, closeTag := "<"+name+">", "</"+name+">"
	start := strings.Index(body, openTag)
	require.GreaterOrEqual(t, start, 0, "%s not found in %s", name, body)
	start += len(openTag)
	end := strings.Index(body[start:], closeTag)
	require.GreaterOrEqual(t, end, 0, "%s unterminated in %s", name, body)
	return body[start : start+end]
}

// TestCFN_FnSplit_ListValuedProperty verifies that a list-valued property fed by
// Fn::Split reaches the API with every element, observed through
// DescribeInstances rather than through the deploy result — a consumer reads the
// security groups off the instance, not off the template.
func TestCFN_FnSplit_ListValuedProperty(t *testing.T) {
	d, registry := newEC2IAMTestDeployer(t)
	subnetID, groups := newVPCWithGroups(t, registry, 3)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Groups": {"Type": "String", "Default": ""},
			"Subnet": {"Type": "String", "Default": ""}
		},
		"Resources": {
			"Node": {
				"Type": "AWS::EC2::Instance",
				"Properties": {
					"ImageId": "ami-0abc1234",
					"InstanceType": "c5.large",
					"SubnetId": {"Ref": "Subnet"},
					"SecurityGroupIds": {"Fn::Split": [",", {"Ref": "Groups"}]}
				}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "split-sg-stack", map[string]string{
		"Groups": strings.Join(groups, ","),
		"Subnet": subnetID,
	})
	require.NoError(t, err)
	node := findResource(t, result, "Node")
	assert.Empty(t, node.Error)

	body := routeQuery(t, registry, "ec2", map[string]string{"Action": "DescribeInstances"})
	for _, sg := range groups {
		assert.Contains(t, body, sg, "every element of the split list must reach RunInstances")
	}
}

// TestCFN_FnIf_ListBranch covers an Fn::If whose branches are lists — the form
// ecs_worker.yml uses for a container command:
// !If [HasCommand, !Split [',', !Ref Command], !Ref 'AWS::NoValue'].
func TestCFN_FnIf_ListBranch(t *testing.T) {
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Groups": {"Type": "String", "Default": ""},
			"Subnet": {"Type": "String", "Default": ""}
		},
		"Conditions": {
			"HasGroups": {"Fn::Not": [{"Fn::Equals": [{"Ref": "Groups"}, ""]}]}
		},
		"Resources": {
			"Node": {
				"Type": "AWS::EC2::Instance",
				"Properties": {
					"ImageId": "ami-0abc1234",
					"InstanceType": "c5.large",
					"SubnetId": {"Ref": "Subnet"},
					"SecurityGroupIds": {"Fn::If": [
						"HasGroups",
						{"Fn::Split": [",", {"Ref": "Groups"}]},
						{"Ref": "AWS::NoValue"}
					]}
				}
			}
		}
	}`

	t.Run("condition true splices the list", func(t *testing.T) {
		d, registry := newEC2IAMTestDeployer(t)
		subnetID, groups := newVPCWithGroups(t, registry, 2)
		result, err := d.Deploy(context.Background(), tmpl, "if-list-true", map[string]string{
			"Groups": strings.Join(groups, ","),
			"Subnet": subnetID,
		})
		require.NoError(t, err)
		assert.Empty(t, findResource(t, result, "Node").Error)

		body := routeQuery(t, registry, "ec2", map[string]string{"Action": "DescribeInstances"})
		for _, sg := range groups {
			assert.Contains(t, body, sg)
		}
	})

	t.Run("condition false contributes no group", func(t *testing.T) {
		d, registry := newEC2IAMTestDeployer(t)
		result, err := d.Deploy(context.Background(), tmpl, "if-list-false", nil)
		require.NoError(t, err)
		assert.Empty(t, findResource(t, result, "Node").Error)

		// AWS::NoValue in a list position yields no element, so the instance
		// lands in the default group rather than one named for the empty
		// string. EC2 assigns "default" itself, which is why the assertion is
		// on the group name and not on the absence of any group at all.
		body := routeQuery(t, registry, "ec2", map[string]string{"Action": "DescribeInstances"})
		assert.Contains(t, body, "<groupName>default</groupName>")
	})
}

// TestCFN_Ref_CommaDelimitedListParameter verifies that a Ref to a
// CommaDelimitedList parameter resolves to a list in a list context, with each
// member space-trimmed as the Parameters reference specifies, and that
// Fn::Select can pick one out of it — the second example in the Fn::Select
// reference.
func TestCFN_Ref_CommaDelimitedListParameter(t *testing.T) {
	d, registry := newEC2IAMTestDeployer(t)
	subnetID, groups := newVPCWithGroups(t, registry, 2)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Groups": {"Type": "CommaDelimitedList", "Default": ""},
			"Subnet": {"Type": "String", "Default": ""}
		},
		"Resources": {
			"Node": {
				"Type": "AWS::EC2::Instance",
				"Properties": {
					"ImageId": "ami-0abc1234",
					"InstanceType": "c5.large",
					"SubnetId": {"Ref": "Subnet"},
					"SecurityGroupIds": {"Ref": "Groups"}
				}
			}
		},
		"Outputs": {
			"Second": {"Value": {"Fn::Select": ["1", {"Ref": "Groups"}]}}
		}
	}`

	// A space after the comma, because the Parameters reference specifies that
	// "each member string is space trimmed" — an untrimmed member would be an
	// unknown security group.
	result, err := d.Deploy(context.Background(), tmpl, "cdl-stack", map[string]string{
		"Groups": strings.Join(groups, ", "),
		"Subnet": subnetID,
	})
	require.NoError(t, err)
	assert.Empty(t, findResource(t, result, "Node").Error)
	assert.Equal(t, groups[1], result.Outputs["Second"])

	body := routeQuery(t, registry, "ec2", map[string]string{"Action": "DescribeInstances"})
	for _, sg := range groups {
		assert.Contains(t, body, sg)
	}
}

// TestResolveValueList_Conventions pins resolveValueList's conventions at the
// seam. Each is a rule CloudFormation states, and each is invisible through the
// current call sites — resolveStringList drops empty members, since a query API
// numbers its list parameters and would otherwise send `Member.2=`.
func TestResolveValueList_Conventions(t *testing.T) {
	params := map[string]string{
		"Command":  "python,-m,worker",
		"Subnets":  "subnet-1, subnet-2 , subnet-3",
		"Empty":    "",
		"OneValue": "a,b",
	}
	listParams := map[string]bool{"Subnets": true, "Empty": true}
	conditions := map[string]bool{"Yes": true, "No": false}

	cases := []struct {
		name string
		in   interface{}
		want []string
	}{
		{
			name: "scalar is a one-element list",
			in:   "single",
			want: []string{"single"},
		},
		{
			name: "Fn::Split keeps every element",
			in:   map[string]interface{}{"Fn::Split": []interface{}{",", map[string]interface{}{"Ref": "Command"}}},
			want: []string{"python", "-m", "worker"},
		},
		{
			// "If you split a string with consecutive delimiters, the resulting
			// list will include an empty string": !Split ['|','a||c|'] is
			// ["a", "", "c", ""].
			name: "consecutive and trailing delimiters yield empty elements",
			in:   map[string]interface{}{"Fn::Split": []interface{}{"|", "a||c|"}},
			want: []string{"a", "", "c", ""},
		},
		{
			name: "Ref to a list parameter splits and trims each member",
			in:   map[string]interface{}{"Ref": "Subnets"},
			want: []string{"subnet-1", "subnet-2", "subnet-3"},
		},
		{
			// The declared type is what makes a Ref list-valued, not the comma.
			name: "Ref to a String parameter is one value even with a comma",
			in:   map[string]interface{}{"Ref": "OneValue"},
			want: []string{"a,b"},
		},
		{
			name: "Ref to an empty list parameter is no members",
			in:   map[string]interface{}{"Ref": "Empty"},
			want: nil,
		},
		{
			name: "Ref AWS::NoValue contributes no element",
			in:   map[string]interface{}{"Ref": "AWS::NoValue"},
			want: nil,
		},
		{
			name: "AWS::NoValue inside a literal list drops only itself",
			in: []interface{}{
				"keep",
				map[string]interface{}{"Ref": "AWS::NoValue"},
				"also-keep",
			},
			want: []string{"keep", "also-keep"},
		},
		{
			name: "Fn::If takes the true branch's list",
			in: map[string]interface{}{"Fn::If": []interface{}{
				"Yes",
				map[string]interface{}{"Fn::Split": []interface{}{",", map[string]interface{}{"Ref": "Command"}}},
				map[string]interface{}{"Ref": "AWS::NoValue"},
			}},
			want: []string{"python", "-m", "worker"},
		},
		{
			name: "Fn::If taking AWS::NoValue yields no element at all",
			in: map[string]interface{}{"Fn::If": []interface{}{
				"No",
				map[string]interface{}{"Fn::Split": []interface{}{",", map[string]interface{}{"Ref": "Command"}}},
				map[string]interface{}{"Ref": "AWS::NoValue"},
			}},
			want: nil,
		},
		{
			// A list of lists is not a shape any AWS API member has.
			name: "a nested list-valued intrinsic splices rather than nesting",
			in: []interface{}{
				"first",
				map[string]interface{}{"Fn::Split": []interface{}{",", "second,third"}},
			},
			want: []string{"first", "second", "third"},
		},
		{
			// "For the Fn::Split delimiter, you can't use any functions."
			name: "an intrinsic delimiter is not resolved",
			in: map[string]interface{}{"Fn::Split": []interface{}{
				map[string]interface{}{"Ref": "Command"}, "a,b",
			}},
			want: nil,
		},
		{
			// A single key is what makes a map an intrinsic. A map carrying
			// other keys is user data — an ECS container definition may have a
			// member literally named "Ref" — and resolving it would also make
			// the result depend on Go's map iteration order.
			name: "a multi-key map containing Ref is not an intrinsic",
			in: map[string]interface{}{
				"Ref":   "Command",
				"Other": "value",
			},
			want: []string{`{"Other":"value","Ref":"Command"}`},
		},
		{
			// An empty delimiter has no documented behavior; the source string
			// comes back as one element rather than one element per byte, which
			// is what strings.Split would give.
			name: "an empty delimiter yields the source string",
			in:   map[string]interface{}{"Fn::Split": []interface{}{"", "abc"}},
			want: []string{"abc"},
		},
		{
			name: "a one-argument Fn::Split is no members",
			in:   map[string]interface{}{"Fn::Split": []interface{}{","}},
			want: nil,
		},
		{
			name: "a two-argument Fn::If is no members",
			in:   map[string]interface{}{"Fn::If": []interface{}{"Yes", "only"}},
			want: nil,
		},
		{
			name: "a non-string Ref argument is no members",
			in:   map[string]interface{}{"Ref": []interface{}{"not", "a", "name"}},
			want: nil,
		},
		{
			name: "an unrecognized intrinsic falls through to its JSON encoding",
			in:   map[string]interface{}{"Fn::Unknown": "x"},
			want: []string{`{"Fn::Unknown":"x"}`},
		},
		{
			name: "nil is no members",
			in:   nil,
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := emulator.ResolveValueListForTest(tc.in, params, listParams, conditions)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestCFNListParameterType pins which declared parameter types make a Ref
// list-valued: CommaDelimitedList is the literal-string form and every other
// list type — Number and the AWS-specific ones — is spelled List<…>.
func TestCFNListParameterType(t *testing.T) {
	for _, listType := range []string{
		"CommaDelimitedList",
		"List<Number>",
		"List<AWS::EC2::Subnet::Id>",
		"List<AWS::EC2::SecurityGroup::Id>",
		"List<AWS::SSM::Parameter::Value<String>>",
	} {
		assert.True(t, emulator.CFNListParameterTypeForTest(listType), listType)
	}
	for _, scalarType := range []string{
		"String",
		"Number",
		"AWS::EC2::Subnet::Id",
		"AWS::SSM::Parameter::Value<String>",
		"",
	} {
		assert.False(t, emulator.CFNListParameterTypeForTest(scalarType), scalarType)
	}
}

// TestCFN_MultiKeyMapIsNotAnIntrinsic pins that a map carrying a recognized
// intrinsic key alongside others is left alone.
//
// Determinism is the reason and not only fidelity: the resolver walked the map
// and returned whichever recognized key Go's map iteration reached first, so the
// same template could resolve two ways across runs — the one outcome an emulator
// built on deterministic replay must never produce. The same rule covers user
// data that happens to hold a member named "Ref".
func TestCFN_MultiKeyMapIsNotAnIntrinsic(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {"P": {"Type": "String", "Default": "resolved"}},
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "multikey-bucket"}}
		},
		"Outputs": {
			"Both": {"Value": {"Ref": "P", "Fn::Sub": "also-${P}"}}
		}
	}`

	// Deployed repeatedly, because the defect is a race with map iteration
	// order: a single run could pass by luck.
	for i := 0; i < 20; i++ {
		result, err := d.Deploy(context.Background(), tmpl, fmt.Sprintf("multikey-%d", i), nil)
		require.NoError(t, err)
		assert.NotEqual(t, "resolved", result.Outputs["Both"],
			"a two-key map is not a Ref")
		assert.NotEqual(t, "also-resolved", result.Outputs["Both"],
			"a two-key map is not an Fn::Sub either")
	}
}

// TestCFN_FnSelect_OutOfRange pins that an out-of-range or malformed Fn::Select
// resolves to the empty string.
//
// Real CloudFormation fails the stack — "Fn::Select cannot select nonexistent
// value at index N" — which needs the typed deployer errors tracked in #502. Until
// then the resolver keeps its existing shape rather than acquiring a second,
// inconsistent failure mode, and this test says so out loud so the behavior is a
// decision rather than an accident.
func TestCFN_FnSelect_OutOfRange(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "select-range"}}
		},
		"Outputs": {
			"TooHigh":  {"Value": {"Fn::Select": ["9", {"Fn::Split": [",", "a,b"]}]}},
			"Negative": {"Value": {"Fn::Select": ["-1", {"Fn::Split": [",", "a,b"]}]}},
			"OneArg":   {"Value": {"Fn::Select": ["0"]}},
			"InRange":  {"Value": {"Fn::Select": ["1", {"Fn::Split": [",", "a,b"]}]}}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "select-range-stack", nil)
	require.NoError(t, err)
	assert.Empty(t, result.Outputs["TooHigh"])
	assert.Empty(t, result.Outputs["Negative"])
	assert.Empty(t, result.Outputs["OneArg"])
	assert.Equal(t, "b", result.Outputs["InRange"])
}

// TestCFN_FnSplit_MalformedArguments pins that a malformed Fn::Split in a scalar
// context resolves to the empty string rather than panicking — one argument, and
// an intrinsic where the delimiter must be a literal ("For the Fn::Split
// delimiter, you can't use any functions").
func TestCFN_FnSplit_MalformedArguments(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {"Sep": {"Type": "String", "Default": ","}},
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "split-malformed"}}
		},
		"Outputs": {
			"OneArg":         {"Value": {"Fn::Split": [","]}},
			"NotAList":       {"Value": {"Fn::Split": "a,b"}},
			"IntrinsicSep":   {"Value": {"Fn::Split": [{"Ref": "Sep"}, "a,b"]}},
			"EmptyDelimiter": {"Value": {"Fn::Split": ["", "abc"]}}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "split-malformed-stack", nil)
	require.NoError(t, err)
	assert.Empty(t, result.Outputs["OneArg"])
	assert.Empty(t, result.Outputs["NotAList"])
	assert.Empty(t, result.Outputs["IntrinsicSep"])
	assert.Equal(t, "abc", result.Outputs["EmptyDelimiter"])
}
