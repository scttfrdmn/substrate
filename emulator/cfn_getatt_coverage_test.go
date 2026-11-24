package emulator_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cfn_getatt_coverage_test.go covers #1591: every published Fn::GetAtt attribute of the VPC,
// Subnet, NatGateway, ELBv2 and Lambda types is either answered or refused with a reason, and
// list-valued attributes reach a list context as lists.

// cfnCoverageResources is one deployable resource of each type #1591 covers, and what it needs.
var cfnCoverageResources = map[string]interface{}{
	"Vpc": map[string]interface{}{"Type": "AWS::EC2::VPC", "Properties": map[string]interface{}{"CidrBlock": "10.40.0.0/16"}},
	"Subnet": map[string]interface{}{"Type": "AWS::EC2::Subnet", "Properties": map[string]interface{}{
		"VpcId": map[string]interface{}{"Ref": "Vpc"}, "CidrBlock": "10.40.1.0/24", "AvailabilityZone": "us-east-1a"}},
	"Subnet2": map[string]interface{}{"Type": "AWS::EC2::Subnet", "Properties": map[string]interface{}{
		"VpcId": map[string]interface{}{"Ref": "Vpc"}, "CidrBlock": "10.40.2.0/24", "AvailabilityZone": "us-east-1b"}},
	"Eip": map[string]interface{}{"Type": "AWS::EC2::EIP", "Properties": map[string]interface{}{"Domain": "vpc"}},
	"Nat": map[string]interface{}{"Type": "AWS::EC2::NatGateway", "Properties": map[string]interface{}{
		"SubnetId":     map[string]interface{}{"Ref": "Subnet"},
		"AllocationId": map[string]interface{}{"Fn::GetAtt": []interface{}{"Eip", "AllocationId"}}}},
	"Sg": map[string]interface{}{"Type": "AWS::EC2::SecurityGroup", "Properties": map[string]interface{}{
		"GroupDescription": "lb", "VpcId": map[string]interface{}{"Ref": "Vpc"}}},
	"Lb": map[string]interface{}{"Type": "AWS::ElasticLoadBalancingV2::LoadBalancer", "Properties": map[string]interface{}{
		"Name":           "coverage-lb",
		"Subnets":        []interface{}{map[string]interface{}{"Ref": "Subnet"}, map[string]interface{}{"Ref": "Subnet2"}},
		"SecurityGroups": []interface{}{map[string]interface{}{"Ref": "Sg"}}}},
	"Tg": map[string]interface{}{"Type": "AWS::ElasticLoadBalancingV2::TargetGroup", "Properties": map[string]interface{}{
		"Name": "coverage-tg", "Port": 80, "Protocol": "HTTP", "VpcId": map[string]interface{}{"Ref": "Vpc"}}},
	"Listener": map[string]interface{}{"Type": "AWS::ElasticLoadBalancingV2::Listener", "Properties": map[string]interface{}{
		"LoadBalancerArn": map[string]interface{}{"Ref": "Lb"}, "Port": 80, "Protocol": "HTTP",
		"DefaultActions": []interface{}{map[string]interface{}{"Type": "forward",
			"TargetGroupArn": map[string]interface{}{"Ref": "Tg"}}}}},
	"Fn": map[string]interface{}{"Type": "AWS::Lambda::Function", "Properties": map[string]interface{}{
		"FunctionName": "coverage-fn", "Runtime": "python3.12", "Handler": "index.handler",
		"Role": "arn:aws:iam::123456789012:role/coverage", "Code": map[string]interface{}{"ZipFile": "def handler(e, c): pass"}}},
}

// cfnCoverageTypes are the logical IDs of cfnCoverageResources whose type #1591 covers.
var cfnCoverageTypes = map[string]string{
	"Vpc": "AWS::EC2::VPC", "Subnet": "AWS::EC2::Subnet", "Nat": "AWS::EC2::NatGateway",
	"Lb": "AWS::ElasticLoadBalancingV2::LoadBalancer", "Tg": "AWS::ElasticLoadBalancingV2::TargetGroup",
	"Listener": "AWS::ElasticLoadBalancingV2::Listener", "Fn": "AWS::Lambda::Function",
}

func cfnCoverageTemplate(t *testing.T, outputs map[string]interface{}) string {
	t.Helper()
	body, err := json.Marshal(map[string]interface{}{"Resources": cfnCoverageResources, "Outputs": outputs})
	require.NoError(t, err)
	return string(body)
}

// TestCFN_GetAttCoverage asserts, for each type #1591 covers, that every attribute the vendored
// specification publishes either resolves to a value or is refused as one substrate holds no value
// for — never silently empty. The specification is the list, so an attribute AWS adds is caught
// on the next `make cfn-schema-fetch`.
//
// It covers the #1591 types rather than every emulated type: the others still answer some
// published attributes with an empty string, and listing each of those is follow-up work.
func TestCFN_GetAttCoverage(t *testing.T) {
	outputs := map[string]interface{}{}
	var unmodelled []string
	for logical, typ := range cfnCoverageTypes {
		attrs := emulator.CFNSchemaAttributesForTest(typ)
		require.NotEmpty(t, attrs, "%s publishes attributes", typ)
		for _, attr := range attrs {
			if emulator.CFNUnmodelledAttributeForTest(typ, attr) {
				unmodelled = append(unmodelled, typ+"."+attr)
				continue
			}
			key := strings.NewReplacer(".", "", ":", "").Replace(logical + attr)
			outputs[key] = map[string]interface{}{"Value": map[string]interface{}{"Fn::GetAtt": []interface{}{logical, attr}}}
		}
	}
	result, err := newStrictDeployer(t).Deploy(context.Background(), cfnCoverageTemplate(t, outputs), "coverage", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
	require.Equal(t, "CREATE_COMPLETE", result.Status, result.StatusReason)
	for key := range outputs {
		assert.NotEmpty(t, result.Outputs[key], "output %s resolved to nothing", key)
	}
	assert.Equal(t, "Z35SXDOTRQ7X7K", result.Outputs["LbCanonicalHostedZoneID"])
	assert.True(t, strings.HasPrefix(result.Outputs["LbSecurityGroups"], "sg-"), result.Outputs["LbSecurityGroups"])
	assert.Contains(t, unmodelled, "AWS::EC2::VPC.VpcEncryptionControl.State", "a family is refused member by member")
	assert.Contains(t, unmodelled, "AWS::Lambda::Function.SnapStartResponse")

	t.Run("an unmodelled attribute in an Output refuses the request", func(t *testing.T) {
		for _, attr := range []string{"DefaultNetworkAcl", "VpcEncryptionControl.State"} {
			_, err := newStrictDeployer(t).Deploy(context.Background(), cfnCoverageTemplate(t, map[string]interface{}{
				"X": map[string]interface{}{"Value": map[string]interface{}{"Fn::GetAtt": []interface{}{"Vpc", attr}}},
			}), "refused", nil)
			require.Error(t, err, attr)
			assert.ErrorIs(t, err, emulator.ErrCFNUnsupportedAttribute)
		}
	})
}

// TestCFN_GetAttListValuedAttributes pins that a list-valued attribute reaches a list context as a
// list: a list property, Fn::Select and Fn::Join each see the members, and a scalar context sees
// them joined.
func TestCFN_GetAttListValuedAttributes(t *testing.T) {
	sgs := map[string]interface{}{"Fn::GetAtt": []interface{}{"Lb", "SecurityGroups"}}
	resources := map[string]interface{}{}
	for k, v := range cfnCoverageResources {
		resources[k] = v
	}
	// A second security group ingress that names the load balancer's group by Fn::Select.
	resources["Ingress"] = map[string]interface{}{"Type": "AWS::EC2::SecurityGroupIngress", "Properties": map[string]interface{}{
		"GroupId":    map[string]interface{}{"Fn::Select": []interface{}{0, sgs}},
		"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80, "CidrIp": "0.0.0.0/0"}}
	body, err := json.Marshal(map[string]interface{}{"Resources": resources, "Outputs": map[string]interface{}{
		"First":  map[string]interface{}{"Value": map[string]interface{}{"Fn::Select": []interface{}{0, sgs}}},
		"Joined": map[string]interface{}{"Value": map[string]interface{}{"Fn::Join": []interface{}{"|", sgs}}},
		"Scalar": map[string]interface{}{"Value": sgs},
		"SgID":   map[string]interface{}{"Value": map[string]interface{}{"Ref": "Sg"}},
	}})
	require.NoError(t, err)
	result, err := newStrictDeployer(t).Deploy(context.Background(), string(body), "lists", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
	sg := result.Outputs["SgID"]
	require.True(t, strings.HasPrefix(sg, "sg-"), sg)
	assert.Equal(t, sg, result.Outputs["First"])
	assert.Equal(t, sg, result.Outputs["Joined"])
	assert.Equal(t, sg, result.Outputs["Scalar"])
}

// TestCFN_GetAttListSurvivesTheStackRecord pins that a list-valued attribute still answers a list
// once it has been through the stack record: an update keeps the unchanged load balancer from the
// previous deployment, whose metadata was decoded from JSON, and a new Output reads it.
func TestCFN_GetAttListSurvivesTheStackRecord(t *testing.T) {
	d := newStrictDeployer(t)
	ctx := context.Background()
	_, err := d.Deploy(ctx, cfnCoverageTemplate(t, map[string]interface{}{
		"SgID": map[string]interface{}{"Value": map[string]interface{}{"Ref": "Sg"}},
	}), "kept", nil)
	require.NoError(t, err)

	result, err := d.UpdateStack(ctx, cfnCoverageTemplate(t, map[string]interface{}{
		"SgID": map[string]interface{}{"Value": map[string]interface{}{"Ref": "Sg"}},
		"Joined": map[string]interface{}{"Value": map[string]interface{}{"Fn::Join": []interface{}{",",
			map[string]interface{}{"Fn::GetAtt": []interface{}{"Lb", "SecurityGroups"}}}}},
	}), "kept", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(result.Outputs["SgID"], "sg-"), result.Outputs["SgID"])
	assert.Equal(t, result.Outputs["SgID"], result.Outputs["Joined"])
}

// TestCFN_GetAttHostedZoneID pins the per-Region, per-type hosted zone table.
func TestCFN_GetAttHostedZoneID(t *testing.T) {
	assert.Equal(t, "Z35SXDOTRQ7X7K", emulator.CFNELBHostedZoneIDForTest("us-east-1", "application"))
	assert.Equal(t, "Z35SXDOTRQ7X7K", emulator.CFNELBHostedZoneIDForTest("us-east-1", ""))
	assert.Equal(t, "Z26RNL4JYFTOTI", emulator.CFNELBHostedZoneIDForTest("us-east-1", "network"))
	assert.Equal(t, "Z1H1FL5HABSF5", emulator.CFNELBHostedZoneIDForTest("us-west-2", "application"))
	assert.Empty(t, emulator.CFNELBHostedZoneIDForTest("us-east-1", "gateway"))
	assert.Empty(t, emulator.CFNELBHostedZoneIDForTest("xx-nowhere-1", "application"))

	t.Run("a type the table does not cover fails the reading resource", func(t *testing.T) {
		const tmpl = `{"Resources":{
			"Gw":{"Type":"AWS::ElasticLoadBalancingV2::LoadBalancer","Properties":{"Name":"gw","Type":"gateway"}},
			"P":{"Type":"AWS::SSM::Parameter","Properties":{"Type":"String","Value":{"Fn::GetAtt":["Gw","CanonicalHostedZoneID"]}}}}}`
		result, err := newStrictDeployer(t).Deploy(context.Background(), tmpl, "gw", nil)
		require.NoError(t, err)
		assert.Contains(t, findResource(t, result, "P").Error, "has no value in substrate")
	})

	t.Run("a network load balancer", func(t *testing.T) {
		const tmpl = `{"Resources":{"Nlb":{"Type":"AWS::ElasticLoadBalancingV2::LoadBalancer","Properties":{"Name":"nlb","Type":"network"}}},
			"Outputs":{"Zone":{"Value":{"Fn::GetAtt":["Nlb","CanonicalHostedZoneID"]}}}}`
		result, err := newStrictDeployer(t).Deploy(context.Background(), tmpl, "nlb", nil)
		require.NoError(t, err)
		assert.Equal(t, "Z26RNL4JYFTOTI", result.Outputs["Zone"])
	})
}

// TestCFN_GetAttNewAttributes tests GetAtt for new resource types to improve resolveFnGetAtt coverage.
func TestCFN_GetAttNewAttributes(t *testing.T) {
	d := newV016Deployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyKey": {
				"Type": "AWS::KMS::Key",
				"Properties": {}
			},
			"MyTopic": {
				"Type": "AWS::SNS::Topic",
				"Properties": {"TopicName": "getatt-test-topic"}
			},
			"MyParam": {
				"Type": "AWS::SSM::Parameter",
				"Properties": {
					"Name": "/getatt/test",
					"Value": "param-value",
					"Type": "String"
				}
			}
		},
		"Outputs": {
			"KeyArn":    {"Value": {"Fn::GetAtt": ["MyKey", "Arn"]}},
			"KeyArn2":   {"Value": {"Fn::GetAtt": ["MyKey", "KeyArn"]}},
			"TopicArn":  {"Value": {"Fn::GetAtt": ["MyTopic", "TopicArn"]}},
			"ParamVal":  {"Value": {"Fn::GetAtt": ["MyParam", "Value"]}},
			"ParamType": {"Value": {"Fn::GetAtt": ["MyParam", "Type"]}}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "getatt-stack", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Outputs["KeyArn"])
	assert.NotEmpty(t, result.Outputs["KeyArn2"])
	assert.NotEmpty(t, result.Outputs["TopicArn"])
	// "Value — Returns the value of the parameter." Not the parameter *name*, which is
	// its physical ID (#827): the value and the name are both plausible strings, so only
	// an exact comparison tells them apart.
	assert.Equal(t, "param-value", result.Outputs["ParamVal"])
	assert.Equal(t, "String", result.Outputs["ParamType"])
}
