package emulator_test

import (
	"context"
	"encoding/xml"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cfn_strict_test.go covers #1605 (strict mode, the template checks and ValidateTemplate) and
// #1548 (references to undeclared names).

// newStrictDeployer is a deployer over every default plugin, strict unless an option says
// otherwise.
func newStrictDeployer(t *testing.T, opts ...emulator.StackDeployerOption) *emulator.StackDeployer {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	require.NoError(t, emulator.RegisterDefaultPlugins(context.Background(), registry, state, tc, logger, store, nil))
	return emulator.NewStackDeployer(registry, store, state, tc, logger,
		emulator.NewCostController(emulator.CostConfig{Enabled: true}), opts...)
}

// cfnStrictCall posts a CloudFormation action to a test server, unsigned.
func cfnStrictCall(t *testing.T, ts *emulator.TestServer, action string, params map[string]string) (int, string) {
	t.Helper()
	all := map[string]string{"Action": action, "Version": "2010-05-15"}
	for k, v := range params {
		all[k] = v
	}
	return singleAccountQuery(t, ts, "cloudformation", "", all)
}

// cfnErrorMessage reads the Message of an ErrorResponse.
func cfnErrorMessage(t *testing.T, body string) string {
	t.Helper()
	var doc struct {
		Message string `xml:"Error>Message"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "body was %s", body)
	return doc.Message
}

const cfnQueueTemplate = `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"strict-q"}}}}`

// TestCFNStrict_RequestLevelRefusals walks every template check made at the request through the
// four operations that make it, over the wire.
func TestCFNStrict_RequestLevelRefusals(t *testing.T) {
	cases := []struct {
		name, tmpl, want string
		// validateAccepts is a check ValidateTemplate does not make, because it is substrate's
		// own rather than AWS's.
		validateAccepts bool
	}{
		{
			name: "a misspelled type is unrecognized",
			tmpl: `{"Resources":{"B":{"Type":"AWS::S3::Bukcet"},"Z":{"Type":"AWS::Nope::Thing"}}}`,
			want: "Template format error: Unrecognized resource types: [AWS::Nope::Thing, AWS::S3::Bukcet]",
		},
		{
			name: "a type substrate does not emulate is refused in strict mode",
			tmpl: `{"Resources":{"Cluster":{"Type":"AWS::EKS::Cluster","Properties":{"RoleArn":"arn:aws:iam::123456789012:role/eks"}},` +
				`"Hook":{"Type":"Custom::Hook","Properties":{"ServiceToken":"arn:aws:lambda:us-east-1:123456789012:function:f"}},` +
				`"Raw":{"Type":"AWS::CloudFormation::CustomResource","Properties":{"ServiceToken":"arn:aws:sns:us-east-1:123456789012:t"}}}}`,
			want: "Template declares resource types substrate does not emulate: [AWS::CloudFormation::CustomResource (Raw); " +
				"AWS::EKS::Cluster (Cluster); Custom::Hook (Hook)]. Substrate refuses them rather than report a stub that " +
				"creates nothing as CREATE_COMPLETE; to deploy them as stubs, set cloudformation.strict: false " +
				"(SUBSTRATE_CLOUDFORMATION_STRICT=false) or start the test server with WithLenientCloudFormation()",
			validateAccepts: true,
		},
		{
			name: "a Ref to an undeclared name",
			tmpl: `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":{"Ref":"QueueNameParam"}}}}}`,
			want: "Template format error: Unresolved resource dependencies [QueueNameParam] in the Resources block of the template",
		},
		{
			name: "an Fn::Sub name and a dotted Fn::Sub name, inside an untaken Fn::If branch",
			tmpl: `{"Conditions":{"Never":{"Fn::Equals":["a","b"]}},"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{` +
				`"QueueName":{"Fn::If":["Never",{"Fn::Sub":"${Missing}-${Gone.Arn}"},"strict-q"]}}}}}`,
			want: "Template format error: Unresolved resource dependencies [Gone, Missing] in the Resources block of the template",
		},
		{
			name: "an Fn::GetAtt to an undeclared resource",
			tmpl: `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":{"Fn::GetAtt":["Nowhere","QueueName"]}}}}}`,
			want: "Template error: instance of Fn::GetAtt references undefined resource Nowhere",
		},
		{
			name: "an Output's Ref and an Export name's Fn::Sub",
			tmpl: `{"Resources":{"Q":{"Type":"AWS::SQS::Queue"}},"Outputs":{` +
				`"A":{"Value":{"Ref":"Typo"}},"B":{"Value":"x","Export":{"Name":{"Fn::Sub":"${Prefix}-x"}}}}}`,
			want: "Unresolved resource dependencies [Prefix, Typo] in the Outputs block of the template",
		},
		{
			name: "a parameter without a Type",
			tmpl: `{"Parameters":{"P":{"Default":"x"}},"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`,
			want: "Template format error: Every Parameters object must contain a Type member.",
		},
		{
			name: "an unrecognized parameter type",
			tmpl: `{"Parameters":{"P":{"Type":"SecureString"}},"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`,
			want: "Template format error: Unrecognized parameter type: SecureString",
		},
		{
			name: "no resources",
			tmpl: `{"Resources":{}}`,
			want: "Template format error: At least one Resources member must be defined.",
		},
		{
			name: "a resource without a Type",
			tmpl: `{"Resources":{"Q":{"Properties":{}}}}`,
			want: "Template format error: Every Resources object must contain a Type member.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "s", "TemplateBody": tc.tmpl})
			require.Equal(t, http.StatusBadRequest, code, "CreateStack: %s", body)
			assert.Contains(t, body, "<Code>ValidationError</Code>")
			assert.Equal(t, tc.want, cfnErrorMessage(t, body), "CreateStack")
			code, body = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": "s"})
			assert.Equal(t, http.StatusBadRequest, code, "a refused create leaves no stack: %s", body)

			code, body = cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "s", "TemplateBody": cfnQueueTemplate})
			require.Equal(t, http.StatusOK, code, body)
			for _, op := range []string{"UpdateStack", "CreateChangeSet"} {
				params := map[string]string{"StackName": "s", "TemplateBody": tc.tmpl, "ChangeSetName": "cs"}
				code, body = cfnStrictCall(t, ts, op, params)
				require.Equal(t, http.StatusBadRequest, code, "%s: %s", op, body)
				assert.Equal(t, tc.want, cfnErrorMessage(t, body), op)
			}

			code, body = cfnStrictCall(t, ts, "ValidateTemplate", map[string]string{"TemplateBody": tc.tmpl})
			if tc.validateAccepts {
				assert.Equal(t, http.StatusOK, code, "ValidateTemplate answers what AWS would: %s", body)
				return
			}
			require.Equal(t, http.StatusBadRequest, code, "ValidateTemplate: %s", body)
			assert.Equal(t, tc.want, cfnErrorMessage(t, body), "ValidateTemplate")
		})
	}
}

// TestCFNStrict_AcceptedReferences pins what #1548's check must not refuse: pseudo-parameters,
// declared parameters, the two-argument Fn::Sub's own variables, escaped literals, and a Ref to a
// declared resource under a false Condition inside the branch that is not taken.
func TestCFNStrict_AcceptedReferences(t *testing.T) {
	tmpl := `{
		"Parameters": {"Env": {"Type": "String", "Default": "dev"}},
		"Conditions": {"Never": {"Fn::Equals": ["a", "b"]}},
		"Resources": {
			"Skipped": {"Type": "AWS::SQS::Queue", "Condition": "Never"},
			"Q": {"Type": "AWS::SQS::Queue", "Properties": {
				"QueueName": {"Fn::Sub": ["${Env}-${Suffix}-${AWS::Region}-${!Literal}", {"Suffix": {"Ref": "AWS::AccountId"}}]}}}
		},
		"Outputs": {
			"Maybe": {"Value": {"Fn::If": ["Never", {"Ref": "Skipped"}, {"Ref": "AWS::NoValue"}]}},
			"Stack": {"Value": {"Fn::Sub": "${AWS::StackName}/${AWS::Partition}/${AWS::URLSuffix}/${AWS::StackId}"}}
		}
	}`
	result, err := newStrictDeployer(t).Deploy(context.Background(), tmpl, "accepted", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
	assert.Equal(t, "dev-123456789012-us-east-1-${Literal}", findResource(t, result, "Q").PhysicalID)
}

// TestCFNStrict_LenientModeStubs pins the opt-out: each of its three forms deploys an unemulated
// type, and a custom resource, as the stub every template got before #1605, while the two
// refusals CloudFormation itself makes still apply.
func TestCFNStrict_LenientModeStubs(t *testing.T) {
	const tmpl = `{"Resources":{"Thing":{"Type":"AWS::IoT::Thing","Properties":{"ThingName":"t"}},` +
		`"Hook":{"Type":"Custom::Hook","Properties":{"ServiceToken":"arn:aws:sns:us-east-1:123456789012:t","Anything":"goes"}}}}`

	t.Run("WithLenientCloudFormation", func(t *testing.T) {
		ts := emulator.StartTestServer(t, emulator.WithLenientCloudFormation())
		code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "lenient", "TemplateBody": tmpl})
		require.Equal(t, http.StatusOK, code, body)
		_, body = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": "lenient"})
		status, _, _ := cfnStackStatus(t, body)
		assert.Equal(t, "CREATE_COMPLETE", status)

		code, body = cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "typo",
			"TemplateBody": `{"Resources":{"B":{"Type":"AWS::S3::Bukcet"}}}`})
		assert.Equal(t, http.StatusBadRequest, code, "an unrecognized type is refused in either mode: %s", body)
	})

	t.Run("WithDeployerLenient", func(t *testing.T) {
		result, err := newStrictDeployer(t, emulator.WithDeployerLenient(true)).Deploy(context.Background(), tmpl, "lenient", nil)
		require.NoError(t, err)
		requireNoResourceErrors(t, result)
		assert.Equal(t, "Thing", findResource(t, result, "Thing").PhysicalID)
	})

	t.Run("WithClientLenientCloudFormation", func(t *testing.T) {
		cfg := emulator.DefaultConfig()
		registry := emulator.NewPluginRegistry()
		state := emulator.NewMemoryStateManager()
		logger := emulator.NewDefaultLogger(slog.LevelError, false)
		tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
		store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
		require.NoError(t, emulator.RegisterDefaultPlugins(context.Background(), registry, state, tc, logger, store, nil))

		_, err := emulator.NewClient(registry, store, state, tc, logger).Deploy(context.Background(), tmpl, emulator.Intent{})
		require.Error(t, err, "Client.Deploy is strict by default")
		assert.ErrorIs(t, err, emulator.ErrCFNUnemulatedType)

		result, err := emulator.NewClient(registry, store, state, tc, logger, emulator.WithClientLenientCloudFormation()).
			Deploy(context.Background(), tmpl, emulator.Intent{})
		require.NoError(t, err)
		requireNoResourceErrors(t, result)
	})

	t.Run("a resource under a false Condition is not refused", func(t *testing.T) {
		const conditioned = `{"Conditions":{"Never":{"Fn::Equals":["a","b"]}},"Resources":{` +
			`"Q":{"Type":"AWS::SQS::Queue"},"Thing":{"Type":"AWS::IoT::Thing","Condition":"Never"}}}`
		_, err := newStrictDeployer(t).Deploy(context.Background(), conditioned, "conditioned", nil)
		assert.NoError(t, err, "it does not deploy, so no stub would stand in for it")
	})
}

// TestCFNStrict_ConfigDefaultsToStrict pins the configuration switch and its environment form.
func TestCFNStrict_ConfigDefaultsToStrict(t *testing.T) {
	assert.False(t, emulator.DefaultConfig().CloudFormation.Lenient)
	assert.False(t, emulator.Config{}.CloudFormation.Lenient, "the zero value is strict")

	dir := t.TempDir()
	plain := filepath.Join(dir, "plain.yaml")
	require.NoError(t, os.WriteFile(plain, []byte("server:\n  address: 127.0.0.1:0\n"), 0o600))
	cfg, err := emulator.LoadConfig(plain)
	require.NoError(t, err)
	assert.False(t, cfg.CloudFormation.Lenient, "a config that does not mention it is strict")

	off := filepath.Join(dir, "off.yaml")
	require.NoError(t, os.WriteFile(off, []byte("cloudformation:\n  strict: false\n"), 0o600))
	cfg, err = emulator.LoadConfig(off)
	require.NoError(t, err)
	assert.True(t, cfg.CloudFormation.Lenient, "cloudformation.strict: false turns strict mode off")

	on := filepath.Join(dir, "on.yaml")
	require.NoError(t, os.WriteFile(on, []byte("cloudformation:\n  lenient: true\n"), 0o600))
	cfg, err = emulator.LoadConfig(on)
	require.NoError(t, err)
	assert.True(t, cfg.CloudFormation.Lenient, "cloudformation.lenient: true turns strict mode off")

	t.Setenv("SUBSTRATE_CLOUDFORMATION_STRICT", "false")
	cfg, err = emulator.LoadConfig(plain)
	require.NoError(t, err)
	assert.True(t, cfg.CloudFormation.Lenient, "SUBSTRATE_CLOUDFORMATION_STRICT=false turns strict mode off")

	t.Setenv("SUBSTRATE_CLOUDFORMATION_STRICT", "")
	t.Setenv("SUBSTRATE_CLOUDFORMATION_LENIENT", "true")
	cfg, err = emulator.LoadConfig(plain)
	require.NoError(t, err)
	assert.True(t, cfg.CloudFormation.Lenient, "SUBSTRATE_CLOUDFORMATION_LENIENT=true turns strict mode off")
}

// TestCFNStrict_TransformDeclaresItsOwnTypes pins the SAM case: a type only a macro defines is not
// refused as unrecognized, because substrate cannot tell it from a typo without running the macro.
// Strict mode refuses it as unemulated instead, and lenient mode stubs it.
func TestCFNStrict_TransformDeclaresItsOwnTypes(t *testing.T) {
	const tmpl = `{"Transform":"AWS::Serverless-2016-10-31","Resources":{"Fn":{"Type":"AWS::Serverless::Function",` +
		`"Properties":{"Handler":"index.handler","Runtime":"python3.12","CodeUri":"s3://b/k"}}}}`
	_, err := newStrictDeployer(t).Deploy(context.Background(), tmpl, "sam", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, emulator.ErrCFNUnemulatedType)
	assert.Contains(t, err.Error(), "AWS::Serverless::Function (Fn)")

	result, err := newStrictDeployer(t, emulator.WithDeployerLenient(true)).Deploy(context.Background(), tmpl, "sam", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
}

// TestCFNStrict_ExtraneousPropertyFailsTheResource pins property validation: a top-level key the
// type does not declare fails the resource at provisioning, and the stack rolls back, rather than
// the request being refused.
func TestCFNStrict_ExtraneousPropertyFailsTheResource(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		code, body := cfnStrictCall(t, ts, "CreateStack", map[string]string{"StackName": "typo", "TemplateBody": `{"Resources":{` +
			`"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"typo-q","VisibilityTimeoutt":30,"Zzz":1}}}}`})
		require.Equal(t, http.StatusOK, code, "property validation is not a request-level refusal: %s", body)

		_, body = cfnStrictCall(t, ts, "DescribeStacks", map[string]string{"StackName": "typo"})
		status, _, _ := cfnStackStatus(t, body)
		assert.Equal(t, "ROLLBACK_COMPLETE", status)

		_, body = cfnStrictCall(t, ts, "DescribeStackEvents", map[string]string{"StackName": "typo"})
		assert.Contains(t, body, "Properties validation failed for resource Q with message: "+
			"#: extraneous key [VisibilityTimeoutt] is not permitted #: extraneous key [Zzz] is not permitted")
		assert.Contains(t, body, "<ResourceStatus>CREATE_FAILED</ResourceStatus>")
	})

	t.Run("update", func(t *testing.T) {
		d := newStrictDeployer(t)
		ctx := context.Background()
		_, err := d.Deploy(ctx, cfnQueueTemplate, "upd", nil)
		require.NoError(t, err)
		result, err := d.UpdateStack(ctx, `{"Resources":{"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"strict-q","Nope":"x"}}}}`,
			"upd", nil, emulator.CFNDeployOptions{})
		require.NoError(t, err)
		assert.Contains(t, result.StatusReason, "#: extraneous key [Nope] is not permitted")
		assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", result.Status)
	})

	t.Run("what is not extraneous", func(t *testing.T) {
		// A key whose value is AWS::NoValue is dropped before validation; AWS::CDK::Metadata and
		// custom resources declare no properties to check against; a declared property substrate
		// ignores stays accepted.
		const tmpl = `{"Resources":{
			"Q":{"Type":"AWS::SQS::Queue","Properties":{"QueueName":"ok-q","Bogus":{"Ref":"AWS::NoValue"},
				"RedriveAllowPolicy":{"redrivePermission":"allowAll"}}},
			"Meta":{"Type":"AWS::CDK::Metadata","Properties":{"Analytics":"v2:deflate64:abc"}}}}`
		result, err := newStrictDeployer(t).Deploy(context.Background(), tmpl, "fine", nil)
		require.NoError(t, err)
		requireNoResourceErrors(t, result)
	})
}

// TestCFNStrict_CDKMetadataIsAModeledNoOp pins AWS::CDK::Metadata: it deploys, creates nothing in
// any service, and deletes cleanly.
func TestCFNStrict_CDKMetadataIsAModeledNoOp(t *testing.T) {
	d := newStrictDeployer(t)
	ctx := context.Background()
	result, err := d.Deploy(ctx, `{"Resources":{"CDKMetadata":{"Type":"AWS::CDK::Metadata",`+
		`"Properties":{"Analytics":"v2:deflate64:H4sI"}}}}`, "cdk", nil)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", result.Status)
	meta := findResource(t, result, "CDKMetadata")
	assert.Equal(t, "CDKMetadata", meta.PhysicalID)
	assert.Empty(t, meta.ARN)

	for _, del := range d.DeleteStackResourcesForTest(ctx, "cdk") {
		assert.Equal(t, "DELETE_COMPLETE", del.Status, "%s: %s", del.LogicalID, del.Reason)
	}
}

// TestCFNStrict_ValidateTemplate pins ValidateTemplate's response.
func TestCFNStrict_ValidateTemplate(t *testing.T) {
	ts := emulator.StartTestServer(t)
	type param struct {
		Key          string  `xml:"ParameterKey"`
		DefaultValue *string `xml:"DefaultValue"`
		NoEcho       bool    `xml:"NoEcho"`
		Description  string  `xml:"Description"`
	}
	type result struct {
		Parameters         []param  `xml:"ValidateTemplateResult>Parameters>member"`
		Description        string   `xml:"ValidateTemplateResult>Description"`
		Capabilities       []string `xml:"ValidateTemplateResult>Capabilities>member"`
		CapabilitiesReason string   `xml:"ValidateTemplateResult>CapabilitiesReason"`
		DeclaredTransforms []string `xml:"ValidateTemplateResult>DeclaredTransforms>member"`
		RequestID          string   `xml:"ResponseMetadata>RequestId"`
	}
	var lastBody string
	validate := func(t *testing.T, params map[string]string) result {
		t.Helper()
		code, body := cfnStrictCall(t, ts, "ValidateTemplate", params)
		require.Equal(t, http.StatusOK, code, body)
		var r result
		require.NoError(t, xml.Unmarshal([]byte(body), &r), body)
		assert.NotEmpty(t, r.RequestID)
		lastBody = body
		return r
	}

	t.Run("parameters, description and an unnamed IAM resource", func(t *testing.T) {
		r := validate(t, map[string]string{"TemplateBody": `
Description: Example template
Parameters:
  Env: {Type: String, Default: dev, Description: The environment}
  Secret: {Type: String, NoEcho: 'true'}
  Hidden: {Type: Number, NoEcho: true, Default: 3}
Resources:
  Role:
    Type: AWS::IAM::Role
    Properties:
      AssumeRolePolicyDocument: {Version: '2012-10-17', Statement: []}
  Policy:
    Type: AWS::IAM::Policy
    Properties:
      PolicyName: inline
      PolicyDocument: {Version: '2012-10-17', Statement: []}
      Roles: [!Ref Role]
`})
		assert.Equal(t, "Example template", r.Description)
		require.Len(t, r.Parameters, 3)
		assert.Equal(t, "Env", r.Parameters[0].Key)
		require.NotNil(t, r.Parameters[0].DefaultValue)
		assert.Equal(t, "dev", *r.Parameters[0].DefaultValue)
		assert.Equal(t, "The environment", r.Parameters[0].Description)
		assert.False(t, r.Parameters[0].NoEcho)
		assert.Equal(t, "Hidden", r.Parameters[1].Key)
		assert.True(t, r.Parameters[1].NoEcho)
		assert.Equal(t, "Secret", r.Parameters[2].Key)
		assert.True(t, r.Parameters[2].NoEcho, "NoEcho written as the string 'true'")
		assert.Nil(t, r.Parameters[2].DefaultValue, "a parameter with no default reports none")
		assert.Equal(t, []string{"CAPABILITY_IAM"}, r.Capabilities)
		assert.Equal(t, "The following resource(s) require capabilities: [AWS::IAM::Policy, AWS::IAM::Role]", r.CapabilitiesReason)
		assert.Empty(t, r.DeclaredTransforms)
	})

	t.Run("a named IAM resource needs CAPABILITY_NAMED_IAM", func(t *testing.T) {
		r := validate(t, map[string]string{"TemplateBody": `{"Resources":{"P":{"Type":"AWS::IAM::ManagedPolicy",` +
			`"Properties":{"ManagedPolicyName":"named","PolicyDocument":{"Version":"2012-10-17","Statement":[]}}}}}`})
		assert.Equal(t, []string{"CAPABILITY_NAMED_IAM"}, r.Capabilities)
		assert.Equal(t, "The following resource(s) require capabilities: [AWS::IAM::ManagedPolicy]", r.CapabilitiesReason)
	})

	t.Run("no parameters, no capabilities, declared transforms", func(t *testing.T) {
		r := validate(t, map[string]string{
			"TemplateBody": `{"Transform":["AWS::LanguageExtensions","AWS::Serverless-2016-10-31"],` +
				`"Resources":{"Fn":{"Type":"AWS::Serverless::Function"}}}`,
			"TemplateURL": "https://ignored.example/t.json",
		})
		assert.Contains(t, lastBody, "<Parameters></Parameters>", "Parameters is present even when empty")
		assert.NotContains(t, lastBody, "<Capabilities>", "an empty list is omitted")
		assert.Empty(t, r.Parameters)
		assert.Empty(t, r.Capabilities)
		assert.Empty(t, r.CapabilitiesReason)
		assert.Equal(t, []string{"AWS::LanguageExtensions", "AWS::Serverless-2016-10-31"}, r.DeclaredTransforms)
	})

	t.Run("refusals", func(t *testing.T) {
		code, body := cfnStrictCall(t, ts, "ValidateTemplate", nil)
		assert.Equal(t, http.StatusBadRequest, code, body)
		assert.Contains(t, body, "MissingParameter")

		code, body = cfnStrictCall(t, ts, "ValidateTemplate", map[string]string{"TemplateURL": "https://b.s3.amazonaws.com/t.json"})
		assert.Equal(t, http.StatusBadRequest, code, body)

		code, body = cfnStrictCall(t, ts, "ValidateTemplate", map[string]string{"TemplateBody": `{"Resources": [nope`})
		assert.Equal(t, http.StatusBadRequest, code, body)
		assert.True(t, strings.HasPrefix(cfnErrorMessage(t, body), "Template format error:"), body)

		code, body = cfnStrictCall(t, ts, "ValidateTemplate", map[string]string{"TemplateBody": `{"Resources":{` +
			`"A":{"Type":"AWS::SQS::Queue","DependsOn":"B"},"B":{"Type":"AWS::SQS::Queue","DependsOn":"A"}}}`})
		assert.Equal(t, http.StatusBadRequest, code, body)
		assert.Contains(t, cfnErrorMessage(t, body), "Circular dependency between resources")
	})
}

// TestCFNStrict_ParameterTypes pins the parameter types the Type check recognizes.
func TestCFNStrict_ParameterTypes(t *testing.T) {
	for typ, want := range map[string]bool{
		"String": true, "Number": true, "List<Number>": true, "List<String>": true, "CommaDelimitedList": true,
		"AWS::EC2::VPC::Id": true, "List<AWS::EC2::Subnet::Id>": true, "AWS::SSM::Parameter::Name": true,
		"AWS::SSM::Parameter::Value<String>": true, "AWS::SSM::Parameter::Value<List<String>>": true,
		"AWS::SSM::Parameter::Value<CommaDelimitedList>": true, "AWS::SSM::Parameter::Value<AWS::EC2::Image::Id>": true,
		"AWS::SSM::Parameter::Value<List<AWS::EC2::SecurityGroup::Id>>": true,
		"string": false, "SecureString": false, "List<AWS::EC2::Nope::Id>": false,
		"AWS::SSM::Parameter::Value<SecureString>": false, "AWS::SSM::Parameter::Value<List<Number>>": false,
	} {
		assert.Equal(t, want, emulator.CFNParameterTypeRecognizedForTest(typ), typ)
	}
}

// TestCFNStrict_EmulatedTypesAreRecognized pins the one source of truth against the vendored
// specification: every type substrate emulates is one CloudFormation recognizes, so none can be
// both deployable and refused, apart from AWS::CDK::Metadata, which CloudFormation accepts and
// the specification does not list.
func TestCFNStrict_EmulatedTypesAreRecognized(t *testing.T) {
	types := emulator.CFNEmulatedTypesForTest()
	require.NotEmpty(t, types)
	assert.Contains(t, types, "AWS::CDK::Metadata")
	assert.NotContains(t, types, "AWS::ECR::LifecyclePolicy", "CloudFormation has no such type")
	for _, typ := range types {
		if typ == "AWS::CDK::Metadata" {
			assert.False(t, emulator.CFNSchemaHasTypeForTest(typ))
			continue
		}
		assert.True(t, emulator.CFNSchemaHasTypeForTest(typ), "%s is emulated but the specification does not list it", typ)
	}
}

// TestCFNStrict_SchemaLookup pins the generated table's reads.
func TestCFNStrict_SchemaLookup(t *testing.T) {
	assert.NotEmpty(t, emulator.CFNSchemaVersionForTest)
	assert.True(t, emulator.CFNSchemaHasTypeForTest("AWS::ACMPCA::Certificate"), "the first type")
	assert.False(t, emulator.CFNSchemaHasTypeForTest("AWS::AAA::Nothing"), "before the first")
	assert.False(t, emulator.CFNSchemaHasTypeForTest("ZZZ::Last::Nothing"), "after the last")
	assert.Nil(t, emulator.CFNSchemaAttributesForTest("AWS::Nope::Nope"))

	declared, required := emulator.CFNSchemaPropertyForTest("AWS::Lambda::Function", "Code")
	assert.True(t, declared)
	assert.True(t, required)
	declared, _ = emulator.CFNSchemaPropertyForTest("AWS::SQS::Queue", "VisibilityTimeoutt")
	assert.False(t, declared)
	declared, _ = emulator.CFNSchemaPropertyForTest("AWS::Nope::Nope", "X")
	assert.False(t, declared)

	assert.True(t, emulator.CFNAttributeIsListForTest("AWS::ElasticLoadBalancingV2::LoadBalancer", "SecurityGroups"))
	assert.False(t, emulator.CFNAttributeIsListForTest("AWS::ElasticLoadBalancingV2::LoadBalancer", "DNSName"))
	assert.False(t, emulator.CFNAttributeIsListForTest("AWS::Nope::Nope", "X"))
}
