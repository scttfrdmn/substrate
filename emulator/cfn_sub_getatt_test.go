package emulator_test

// Fn::Sub's resource-attribute form, "${LogicalId.Attribute}" (#1103).
//
// The Fn::Sub reference page publishes it in one sentence: "If you specify resource attributes,
// such as ${MyInstance.PublicIp}, CloudFormation returns the same values as if you used the
// Fn::GetAtt intrinsic function." substrate resolved a ${} body as a parameter or a logical ID
// only, so "${MyQueue.QueueName}" was stored as the literal string "MyQueue.QueueName". Each case
// here deploys the same attribute through both spellings and asserts they agree, which is the
// equivalence the page states, and also pins the value so agreement on a wrong answer fails.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCFN_FnSubResourceAttribute(t *testing.T) {
	const tmpl = `{
	"Resources": {
		"Queue": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": "sub-attr-queue"}},
		"Topic": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "sub-attr-topic"}},
		"Db": {"Type": "AWS::RDS::DBInstance", "Properties": {
			"DBInstanceIdentifier": "sub-attr-db", "Engine": "postgres",
			"DBInstanceClass": "db.t3.micro", "AllocatedStorage": "20",
			"MasterUsername": "admin", "MasterUserPassword": "secret123"}},
		"Elb": {"Type": "AWS::ElasticLoadBalancing::LoadBalancer", "Properties": {
			"LoadBalancerName": "sub-attr-elb", "AvailabilityZones": ["us-east-1a"],
			"Listeners": [{"LoadBalancerPort": "80", "InstancePort": "80", "Protocol": "HTTP"}]}}
	},
	"Outputs": {
		"QueueNameSub": {"Value": {"Fn::Sub": "${Queue.QueueName}"}},
		"QueueNameGetAtt": {"Value": {"Fn::GetAtt": ["Queue", "QueueName"]}},
		"QueueArnSub": {"Value": {"Fn::Sub": "arn=${Queue.Arn};"}},
		"QueueArnGetAtt": {"Value": {"Fn::GetAtt": ["Queue", "Arn"]}},
		"TopicNameSub": {"Value": {"Fn::Sub": "${Topic.TopicName}"}},
		"TopicNameGetAtt": {"Value": {"Fn::GetAtt": ["Topic", "TopicName"]}},
		"DbAddressSub": {"Value": {"Fn::Sub": "${Db.Endpoint.Address}"}},
		"DbAddressGetAtt": {"Value": {"Fn::GetAtt": ["Db", "Endpoint.Address"]}},
		"ElbOwnerSub": {"Value": {"Fn::Sub": "${Elb.SourceSecurityGroup.OwnerAlias}"}},
		"ElbOwnerGetAtt": {"Value": {"Fn::GetAtt": ["Elb", "SourceSecurityGroup.OwnerAlias"]}},
		"MapKeyWins": {"Value": {"Fn::Sub": ["${Queue.QueueName}", {"Queue.QueueName": "from-the-map"}]}},
		"Undeclared": {"Value": {"Fn::Sub": "${Missing.Arn}"}},
		"UndeclaredGetAtt": {"Value": {"Fn::GetAtt": ["Missing", "Arn"]}},
		"Escaped": {"Value": {"Fn::Sub": "${!Queue.QueueName}"}}
	}}`

	result, err := newRefDeployer(t).Deploy(context.Background(), tmpl, "sub-attr-stack", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
	out := result.Outputs

	tests := []struct {
		name, sub, getAtt, want string
		// unmodelled marks an attribute substrate's deploy does not record, so Fn::GetAtt
		// answers "" for it. The split is still asserted: a ${} body cut at the wrong dot would
		// answer the "<logicalID>.<attr>" fallback, or the literal, rather than agreeing.
		unmodelled bool
	}{
		// SQS's physical ID is the queue URL, so QueueName and Arn are both attributes that
		// differ from what ${Queue} would answer.
		{name: "SQS QueueName", sub: "QueueNameSub", getAtt: "QueueNameGetAtt",
			want: "sub-attr-queue"},
		{name: "SNS TopicName, where Ref is the ARN", sub: "TopicNameSub", getAtt: "TopicNameGetAtt",
			want: "sub-attr-topic"},
		// The attribute name holds a dot, so the ${} body is split on the first dot only — the
		// same shape as Fn::GetAtt's published "SourceSecurityGroup.OwnerAlias".
		{name: "a dotted attribute splits on the first dot", sub: "DbAddressSub",
			getAtt: "DbAddressGetAtt"},
		// Fn::GetAtt's page publishes exactly this attribute as its dotted-name example.
		{name: "Fn::GetAtt's published SourceSecurityGroup.OwnerAlias", sub: "ElbOwnerSub",
			getAtt: "ElbOwnerGetAtt", unmodelled: true},
		// resolveFnGetAtt answers "<logicalID>.<attr>" for an undeclared resource on purpose,
		// because it names the template's mistake; the ${} spelling agrees with it.
		{name: "an undeclared resource names the mistake", sub: "Undeclared",
			getAtt: "UndeclaredGetAtt", want: "Missing.Arn"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.unmodelled {
				require.NotEmpty(t, out[tc.getAtt], "Fn::GetAtt resolved to nothing")
			}
			assert.NotContains(t, out[tc.sub], "SourceSecurityGroup",
				"the ${} body was not resolved as an attribute")
			assert.Equal(t, out[tc.getAtt], out[tc.sub],
				"${LogicalId.Attribute} answers what Fn::GetAtt answers")
			if tc.want != "" {
				assert.Equal(t, tc.want, out[tc.sub])
			}
		})
	}

	assert.Equal(t, "arn="+out["QueueArnGetAtt"]+";", out["QueueArnSub"],
		"the attribute form interpolates into the middle of a longer string")
	assert.Contains(t, out["QueueArnGetAtt"], ":sqs:")
	assert.Equal(t, "from-the-map", out["MapKeyWins"],
		"a variable-map key containing a dot still wins over the template reference")
	assert.Equal(t, "${Queue.QueueName}", out["Escaped"], "${!Literal} is still a literal")
}
