package emulator_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CloudFormation's three AWS Config resource types, which stopped being stubs in
// this release (#580, the #388 class).
//
// Every assertion here observes the *service* after the deploy rather than only the
// DeployedResource the deployer returned, because the defect being fixed was
// precisely a deployer returning a well-formed resource for a service call that never
// happened. A test that asserted only on the returned PhysicalID and ARN would have
// passed against the stub, which is how the stub survived from v0.32.0.

// newConfigTestDeployer creates a StackDeployer with the Config and S3 plugins and
// returns the registry, so a test can observe deployed resources through the same API
// a consumer would call.
//
// S3 is here because PutDeliveryChannel reads real bucket state; the shared state
// manager is what lets it. Config's requests carry a JSON body rather than query
// parameters, so routeQuery does not fit and configOp below is used instead.
func newConfigTestDeployer(t *testing.T) (*emulator.StackDeployer, *emulator.PluginRegistry) {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	costs := emulator.NewCostController(emulator.CostConfig{Enabled: true})

	configPlugin := &emulator.ConfigServicePlugin{}
	require.NoError(t, configPlugin.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(configPlugin)

	s3Plugin := &emulator.S3Plugin{}
	require.NoError(t, s3Plugin.Initialize(context.Background(), emulator.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
		},
	}))
	registry.Register(s3Plugin)

	return emulator.NewStackDeployer(registry, store, state, tc, logger, costs), registry
}

// configOp sends one Config operation through the registry and decodes its JSON
// response into out. It requires success: a test asserting a refusal names the
// operation itself rather than going through here.
func configOp(t *testing.T, registry *emulator.PluginRegistry, operation string, input, out any) {
	t.Helper()
	body, err := json.Marshal(input)
	require.NoError(t, err)

	resp, err := registry.RouteRequest(&emulator.RequestContext{
		RequestID: "test-request",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}, &emulator.AWSRequest{
		Service:   "config",
		Operation: operation,
		Body:      body,
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	})
	require.NoError(t, err, "%s was refused", operation)
	require.NotNil(t, resp)
	if out != nil {
		require.NoError(t, json.Unmarshal(resp.Body, out))
	}
}

// cfnConfigRecorders returns what DescribeConfigurationRecorders reports.
func cfnConfigRecorders(t *testing.T, registry *emulator.PluginRegistry) []map[string]any {
	t.Helper()
	var out struct {
		ConfigurationRecorders []map[string]any `json:"ConfigurationRecorders"`
	}
	configOp(t, registry, "DescribeConfigurationRecorders", map[string]any{}, &out)
	return out.ConfigurationRecorders
}

// cfnConfigRecorderStatus returns what DescribeConfigurationRecorderStatus reports —
// the operation that answers whether the recorder is actually recording.
func cfnConfigRecorderStatus(t *testing.T, registry *emulator.PluginRegistry) []map[string]any {
	t.Helper()
	var out struct {
		ConfigurationRecordersStatus []map[string]any `json:"ConfigurationRecordersStatus"`
	}
	configOp(t, registry, "DescribeConfigurationRecorderStatus", map[string]any{}, &out)
	return out.ConfigurationRecordersStatus
}

// cfnConfigChannels returns what DescribeDeliveryChannels reports.
func cfnConfigChannels(t *testing.T, registry *emulator.PluginRegistry) []map[string]any {
	t.Helper()
	var out struct {
		DeliveryChannels []map[string]any `json:"DeliveryChannels"`
	}
	configOp(t, registry, "DescribeDeliveryChannels", map[string]any{}, &out)
	return out.DeliveryChannels
}

// cfnConfigRules returns what DescribeConfigRules reports.
func cfnConfigRules(t *testing.T, registry *emulator.PluginRegistry) []map[string]any {
	t.Helper()
	var out struct {
		ConfigRules []map[string]any `json:"ConfigRules"`
	}
	configOp(t, registry, "DescribeConfigRules", map[string]any{}, &out)
	return out.ConfigRules
}

// cfnConfigDeliveryBucket creates a delivery bucket carrying a policy that admits
// Config, through the S3 plugin, so PutDeliveryChannel's check reads real state.
//
// The bucket and the policy are set up outside the template rather than in it because
// substrate has no AWS::S3::BucketPolicy resource type: a template can create the
// bucket but cannot express the policy the check wants, so a stack carrying a channel
// needs the policy — or the delivery-policy seed — from somewhere else. That gap is
// stated in cfn_resources_v101.go's doc comment, and this helper is the fixture form
// of it.
func cfnConfigDeliveryBucket(t *testing.T, registry *emulator.PluginRegistry, bucket string) {
	t.Helper()
	reqCtx := &emulator.RequestContext{
		RequestID: "fixture-request",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	_, err := registry.RouteRequest(reqCtx, &emulator.AWSRequest{
		Service: "s3", Operation: "PUT", Path: "/" + bucket,
		Headers: map[string]string{}, Params: map[string]string{},
	})
	require.NoError(t, err)

	policy := `{"Version":"2012-10-17","Statement":[{` +
		`"Sid":"AWSConfigBucketDelivery","Effect":"Allow",` +
		`"Principal":{"Service":"config.amazonaws.com"},` +
		`"Action":"s3:PutObject","Resource":"arn:aws:s3:::` + bucket + `/*"}]}`
	_, err = registry.RouteRequest(reqCtx, &emulator.AWSRequest{
		Service: "s3", Operation: "PUT", Path: "/" + bucket,
		Body:    []byte(policy),
		Headers: map[string]string{}, Params: map[string]string{"policy": "1"},
	})
	require.NoError(t, err)
}

// TestCFN_ConfigConfigurationRecorder verifies a recorder declared in a template is
// afterwards visible to DescribeConfigurationRecorders — and, because the template
// carries no delivery channel, that it is **not recording**.
//
// That second assertion is the release's headline behavior seen from
// CloudFormation: the two states a consumer confuses are "the recorder exists" and
// "the recorder is recording", and a template with a recorder alone produces only the
// first. Real CloudFormation is explicit that the start waits on the channel.
func TestCFN_ConfigConfigurationRecorder(t *testing.T) {
	d, registry := newConfigTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRecorder": {
				"Type": "AWS::Config::ConfigurationRecorder",
				"Properties": {
					"Name": "default",
					"RoleARN": "arn:aws:iam::123456789012:role/config-role"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "config-recorder-stack", nil)
	require.NoError(t, err)

	r := findResource(t, result, "MyRecorder")
	require.Empty(t, r.Error)
	assert.Equal(t, "AWS::Config::ConfigurationRecorder", r.Type)
	// Ref returns the recorder name, "such as default".
	assert.Equal(t, "default", r.PhysicalID)
	// The stub's ARN was recorder/default. The Service Authorization Reference gives
	// configuration-recorder/${RecorderName}/${RecorderId}, and the ID has no member
	// in the API model, so the deployer reads the ARN back rather than rebuilding it.
	assert.Contains(t, r.ARN, "arn:aws:config:us-east-1:123456789012:configuration-recorder/default/")
	assert.NotContains(t, r.ARN, ":recorder/")

	recorders := cfnConfigRecorders(t, registry)
	require.Len(t, recorders, 1, "the recorder the stub never created")
	assert.Equal(t, "default", recorders[0]["name"])
	assert.Equal(t, "arn:aws:iam::123456789012:role/config-role", recorders[0]["roleARN"])

	status := cfnConfigRecorderStatus(t, registry)
	require.Len(t, status, 1)
	assert.Equal(t, false, status[0]["recording"],
		"with no delivery channel in the template there is nothing to start the recorder")
}

// TestCFN_ConfigRecorderRequiresRoleARN verifies a template omitting RoleARN is
// refused at the CloudFormation layer.
//
// The divergence is deliberate: the resource's page marks RoleARN Required: Yes while
// the API model marks roleARN optional. Refusing here rather than letting the plugin
// answer InvalidRoleException means the stack event names the property real
// CloudFormation would have named. The refusal must also be *recorded* rather than
// returned — a returned error aborts the whole stack.
func TestCFN_ConfigRecorderRequiresRoleARN(t *testing.T) {
	d, registry := newConfigTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRecorder": {
				"Type": "AWS::Config::ConfigurationRecorder",
				"Properties": { "Name": "default" }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "config-no-role-stack", nil)
	require.NoError(t, err, "the resource fails; the deploy call does not")

	r := findResource(t, result, "MyRecorder")
	assert.Contains(t, r.Error, "RoleARN is required")
	assert.Empty(t, cfnConfigRecorders(t, registry),
		"a refused resource dispatched nothing")
}

// TestCFN_ConfigRecordingGroupIsTranslated verifies the template's RecordingGroup
// reaches the service.
//
// This is the case a pass-through of the properties would fail *silently*:
// CloudFormation spells the nested members UpperCamel (RecordingGroup.ResourceTypes)
// and the API spells them lowerCamel (recordingGroup.resourceTypes), so forwarding
// the map verbatim decodes to an empty group. The deploy would still succeed, the
// recorder would record AWS's default group, and nothing would report that the
// template's group had been dropped — so the assertion is on the recorded group's
// contents, not on the deploy's success.
//
// Both booleans are asserted true in at least one case, which is the only way the
// assertion can fail when a member is dropped: allSupported and
// includeGlobalResourceTypes decode to false when absent, so a case that expects
// false cannot tell a translated member from a discarded one. That is not
// hypothetical — a mutant renaming both wire keys survived a version of this test
// that only exercised the false shape.
func TestCFN_ConfigRecordingGroupIsTranslated(t *testing.T) {
	tests := []struct {
		name   string
		group  string
		assert func(t *testing.T, group map[string]any)
	}{
		{
			// The inclusion shape: allSupported false, an explicit type list, and
			// INCLUSION_BY_RESOURCE_TYPES — the group a template most often writes.
			name: "inclusion by resource types",
			group: `{
				"AllSupported": false,
				"IncludeGlobalResourceTypes": false,
				"ResourceTypes": ["AWS::S3::Bucket", "AWS::EC2::Instance"],
				"RecordingStrategy": { "UseOnly": "INCLUSION_BY_RESOURCE_TYPES" }
			}`,
			assert: func(t *testing.T, group map[string]any) {
				assert.Equal(t, []any{"AWS::S3::Bucket", "AWS::EC2::Instance"}, group["resourceTypes"])
				assert.Equal(t, false, group["allSupported"])
				assert.Equal(t, false, group["includeGlobalResourceTypes"])
				strategy, ok := group["recordingStrategy"].(map[string]any)
				require.True(t, ok, "the nested strategy survived too: %v", group)
				assert.Equal(t, "INCLUSION_BY_RESOURCE_TYPES", strategy["useOnly"])
			},
		},
		{
			// Both booleans true. A dropped member reports false here, so this is the
			// case that distinguishes a translation from a pass-through. The strategy
			// must be ALL_SUPPORTED_RESOURCE_TYPES: allSupported paired with either a
			// type list or EXCLUSION_BY_RESOURCE_TYPES is an
			// InvalidRecordingGroupException case, so those combinations cannot be
			// used to make the point.
			name: "all supported with global types",
			group: `{
				"AllSupported": true,
				"IncludeGlobalResourceTypes": true,
				"RecordingStrategy": { "UseOnly": "ALL_SUPPORTED_RESOURCE_TYPES" }
			}`,
			assert: func(t *testing.T, group map[string]any) {
				assert.Equal(t, true, group["allSupported"])
				assert.Equal(t, true, group["includeGlobalResourceTypes"])
			},
		},
		{
			// The exclusion shape, whose resource-type list is nested one level deeper
			// than the inclusion one and so is renamed at two levels.
			name: "exclusion by resource types",
			group: `{
				"AllSupported": false,
				"IncludeGlobalResourceTypes": true,
				"ExclusionByResourceTypes": { "ResourceTypes": ["AWS::EC2::Instance"] },
				"RecordingStrategy": { "UseOnly": "EXCLUSION_BY_RESOURCE_TYPES" }
			}`,
			assert: func(t *testing.T, group map[string]any) {
				assert.Equal(t, true, group["includeGlobalResourceTypes"])
				exclusion, ok := group["exclusionByResourceTypes"].(map[string]any)
				require.True(t, ok, "the nested exclusion survived: %v", group)
				assert.Equal(t, []any{"AWS::EC2::Instance"}, exclusion["resourceTypes"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, registry := newConfigTestDeployer(t)
			tmpl := `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"MyRecorder": {
						"Type": "AWS::Config::ConfigurationRecorder",
						"Properties": {
							"RoleARN": "arn:aws:iam::123456789012:role/config-role",
							"RecordingGroup": ` + tt.group + `,
							"RecordingMode": { "RecordingFrequency": "DAILY" }
						}
					}
				}
			}`
			result, err := d.Deploy(context.Background(), tmpl, "config-group-stack", nil)
			require.NoError(t, err)
			require.Empty(t, findResource(t, result, "MyRecorder").Error)

			recorders := cfnConfigRecorders(t, registry)
			require.Len(t, recorders, 1)

			group, ok := recorders[0]["recordingGroup"].(map[string]any)
			require.True(t, ok, "the recording group survived the case translation: %v", recorders[0])
			tt.assert(t, group)

			mode, ok := recorders[0]["recordingMode"].(map[string]any)
			require.True(t, ok, "the recording mode survived: %v", recorders[0])
			assert.Equal(t, "DAILY", mode["recordingFrequency"])
		})
	}
}

// TestCFN_ConfigDeliveryChannelStartsTheRecorder verifies the behavior the resource's
// own page states: "AWS CloudFormation starts the recorder as soon as the delivery
// channel is available."
//
// It also asserts the ordering comes from typePriority alone. The template declares
// the channel before the recorder and uses no DependsOn — which substrate parses and
// does not act on — so if the priorities were wrong the channel's Put would meet
// NoAvailableConfigurationRecorderException.
func TestCFN_ConfigDeliveryChannelStartsTheRecorder(t *testing.T) {
	d, registry := newConfigTestDeployer(t)
	cfnConfigDeliveryBucket(t, registry, "cfg-logs")

	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyChannel": {
				"Type": "AWS::Config::DeliveryChannel",
				"Properties": {
					"Name": "default",
					"S3BucketName": "cfg-logs",
					"S3KeyPrefix": "config",
					"ConfigSnapshotDeliveryProperties": { "DeliveryFrequency": "TwentyFour_Hours" }
				}
			},
			"MyRecorder": {
				"Type": "AWS::Config::ConfigurationRecorder",
				"Properties": {
					"Name": "default",
					"RoleARN": "arn:aws:iam::123456789012:role/config-role"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "config-channel-stack", nil)
	require.NoError(t, err)

	channel := findResource(t, result, "MyChannel")
	require.Empty(t, channel.Error)
	// Ref returns the channel name, and there is deliberately no ARN: the Service
	// Authorization Reference defines no delivery-channel resource type at all.
	assert.Equal(t, "default", channel.PhysicalID)
	assert.Empty(t, channel.ARN)

	channels := cfnConfigChannels(t, registry)
	require.Len(t, channels, 1)
	assert.Equal(t, "cfg-logs", channels[0]["s3BucketName"])
	assert.Equal(t, "config", channels[0]["s3KeyPrefix"])
	snapshot, ok := channels[0]["configSnapshotDeliveryProperties"].(map[string]any)
	require.True(t, ok, "the snapshot properties survived the case translation: %v", channels[0])
	assert.Equal(t, "TwentyFour_Hours", snapshot["deliveryFrequency"])

	status := cfnConfigRecorderStatus(t, registry)
	require.Len(t, status, 1)
	assert.Equal(t, true, status[0]["recording"],
		"the channel's deploy started the stack's recorder")
}

// TestCFN_ConfigDeliveryChannelRequiresBucket verifies the second
// CloudFormation-only requirement: S3BucketName is Required: Yes on the page while
// the API model bounds it 0-63 and requires nothing.
func TestCFN_ConfigDeliveryChannelRequiresBucket(t *testing.T) {
	d, registry := newConfigTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRecorder": {
				"Type": "AWS::Config::ConfigurationRecorder",
				"Properties": {
					"RoleARN": "arn:aws:iam::123456789012:role/config-role"
				}
			},
			"MyChannel": {
				"Type": "AWS::Config::DeliveryChannel",
				"Properties": { "Name": "default" }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "config-no-bucket-stack", nil)
	require.NoError(t, err)

	assert.Contains(t, findResource(t, result, "MyChannel").Error, "S3BucketName is required")
	assert.Empty(t, cfnConfigChannels(t, registry))

	// The failed resource rolled the whole stack back, and the rollback's sweep took
	// the recorder with it — which is only true because the recorder's delete is a real
	// DeleteConfigurationRecorder now. Under the stub the rollback removed a state key
	// and left the recorder behind, so a consumer whose stack failed on the channel was
	// left with a recorder no stack owned and a second attempt meeting
	// MaxNumberOfConfigurationRecordersExceededException.
	assert.Equal(t, "ROLLBACK_COMPLETE", result.Status)
	assert.Empty(t, cfnConfigRecorders(t, registry),
		"the rollback deleted the recorder the failed stack had created")
}

// TestCFN_ConfigConfigRule verifies a rule declared in a template is afterwards
// visible to DescribeConfigRules, and that its three documented GetAtt attributes
// resolve.
func TestCFN_ConfigConfigRule(t *testing.T) {
	d, registry := newConfigTestDeployer(t)
	cfnConfigDeliveryBucket(t, registry, "cfg-logs")

	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRecorder": {
				"Type": "AWS::Config::ConfigurationRecorder",
				"Properties": {
					"RoleARN": "arn:aws:iam::123456789012:role/config-role"
				}
			},
			"MyChannel": {
				"Type": "AWS::Config::DeliveryChannel",
				"Properties": { "S3BucketName": "cfg-logs" }
			},
			"MyRule": {
				"Type": "AWS::Config::ConfigRule",
				"Properties": {
					"ConfigRuleName": "s3-encrypted",
					"Description": "buckets must be encrypted",
					"Source": {
						"Owner": "AWS",
						"SourceIdentifier": "S3_BUCKET_SERVER_SIDE_ENCRYPTION_ENABLED"
					},
					"Scope": { "ComplianceResourceTypes": ["AWS::S3::Bucket"] },
					"InputParameters": { "minimumCount": "1" }
				}
			}
		},
		"Outputs": {
			"RuleArn":        { "Value": { "Fn::GetAtt": ["MyRule", "Arn"] } },
			"RuleId":         { "Value": { "Fn::GetAtt": ["MyRule", "ConfigRuleId"] } },
			"RuleCompliance": { "Value": { "Fn::GetAtt": ["MyRule", "Compliance.Type"] } },
			"RuleRef":        { "Value": { "Ref": "MyRule" } }
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "config-rule-stack", nil)
	require.NoError(t, err)

	r := findResource(t, result, "MyRule")
	require.Empty(t, r.Error)
	assert.Equal(t, "s3-encrypted", r.PhysicalID)
	// The stub built the ARN's ID component from the *logical* ID. The real ID is
	// minted from the rule name, and the deployer reads it back rather than
	// recomputing it, so the ARN cannot disagree with DescribeConfigRules.
	assert.Contains(t, r.ARN, "arn:aws:config:us-east-1:123456789012:config-rule/config-rule-")
	assert.NotContains(t, r.ARN, "config-rule-MyRule")

	rules := cfnConfigRules(t, registry)
	require.Len(t, rules, 1, "the rule the stub never created")
	assert.Equal(t, "s3-encrypted", rules[0]["ConfigRuleName"])
	assert.Equal(t, "buckets must be encrypted", rules[0]["Description"])
	source, ok := rules[0]["Source"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "AWS", source["Owner"])
	// InputParameters is a JSON *string* in the API model; the resource's JSON example
	// supplies an object, so the deployer encodes one.
	assert.Equal(t, `{"minimumCount":"1"}`, rules[0]["InputParameters"])

	assert.Equal(t, r.ARN, result.Outputs["RuleArn"])
	assert.Equal(t, "s3-encrypted", result.Outputs["RuleRef"])
	assert.NotEmpty(t, result.Outputs["RuleId"], "ConfigRuleId resolves from Metadata")
	assert.NotEqual(t, "s3-encrypted", result.Outputs["RuleId"],
		"the ID is not the name; falling through to the physical ID would return the name")
	// Compliance is seed-only and nothing was seeded, so the value is what AWS reports
	// for a rule that has not evaluated — not a fabricated COMPLIANT.
	assert.Equal(t, "INSUFFICIENT_DATA", result.Outputs["RuleCompliance"])
}

// TestCFN_ConfigRuleRequiresSource verifies the one requirement the resource's page
// and the API model agree on. The template the stub test used carried no Source at
// all and passed, which is how invalid fixtures went green for nine releases.
func TestCFN_ConfigRuleRequiresSource(t *testing.T) {
	d, registry := newConfigTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRule": {
				"Type": "AWS::Config::ConfigRule",
				"Properties": { "ConfigRuleName": "my-config-rule" }
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "config-no-source-stack", nil)
	require.NoError(t, err)

	assert.Contains(t, findResource(t, result, "MyRule").Error, "Source is required")
	assert.Empty(t, cfnConfigRules(t, registry))
}

// TestCFN_ConfigRuleWithoutARecorder verifies the service's own precondition surfaces
// as a resource failure rather than as a false success. The resource's own page says
// "You must first create and start the AWS Config configuration recorder in order to
// create AWS Config managed rules with AWS CloudFormation".
func TestCFN_ConfigRuleWithoutARecorder(t *testing.T) {
	d, registry := newConfigTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRule": {
				"Type": "AWS::Config::ConfigRule",
				"Properties": {
					"ConfigRuleName": "s3-encrypted",
					"Source": { "Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED" }
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "config-orphan-rule-stack", nil)
	require.NoError(t, err)

	assert.Contains(t, findResource(t, result, "MyRule").Error,
		"NoAvailableConfigurationRecorderException")
	assert.Empty(t, cfnConfigRules(t, registry))
}

// TestCFN_ConfigRuleNameIsGenerated verifies a template omitting ConfigRuleName gets a
// generated stack-scoped name, the shape the page documents
// (mystack-MyConfigRule-12ABCFPXHV4OV), rather than the bare logical ID the stub used.
//
// The bare logical ID is not a harmless simplification: PutConfigRule is idempotent on
// the name, so two stacks each declaring "MyRule" would silently share one rule and
// the first stack's teardown would delete the second's (#560, the reason
// cfnGeneratedNameTypes exists).
func TestCFN_ConfigRuleNameIsGenerated(t *testing.T) {
	d, registry := newConfigTestDeployer(t)
	cfnConfigDeliveryBucket(t, registry, "cfg-logs")

	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRecorder": {
				"Type": "AWS::Config::ConfigurationRecorder",
				"Properties": { "RoleARN": "arn:aws:iam::123456789012:role/config-role" }
			},
			"MyChannel": {
				"Type": "AWS::Config::DeliveryChannel",
				"Properties": { "S3BucketName": "cfg-logs" }
			},
			"MyRule": {
				"Type": "AWS::Config::ConfigRule",
				"Properties": {
					"Source": { "Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED" }
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "cfgstack", nil)
	require.NoError(t, err)

	r := findResource(t, result, "MyRule")
	require.Empty(t, r.Error)
	assert.NotEqual(t, "MyRule", r.PhysicalID)
	assert.Regexp(t, `^cfgstack-MyRule-[0-9a-z]{12}$`, r.PhysicalID)
	assert.LessOrEqual(t, len(r.PhysicalID), 128, "ConfigRuleName is bounded at 128")

	rules := cfnConfigRules(t, registry)
	require.Len(t, rules, 1)
	assert.Equal(t, r.PhysicalID, rules[0]["ConfigRuleName"])
}

// TestCFN_ConfigTeardownDeletesTheRealResources verifies the sweep removes what the
// deploy created, in an order the service accepts.
//
// Until this release all three types were stub-deletes: the sweep dropped a
// cfnStubNamespace key and reported DELETE_COMPLETE while the recorder, channel and
// rule stayed behind. The rebuild at the end is the assertion that matters — a second
// deploy of the same template met
// MaxNumberOfConfigurationRecordersExceededException from a stack that had reported
// itself fully deleted.
//
// The ordering is doubly load-bearing here. The reverse sweep tears down by descending
// priority, so the rule goes first and the recorder last; but DeleteDeliveryChannel is
// refused while the recorder is recording, and the deploy started it. The channel's
// pre-step stops the recorder, and the recorder's own delete has no precondition at
// all.
func TestCFN_ConfigTeardownDeletesTheRealResources(t *testing.T) {
	d, registry := newConfigTestDeployer(t)
	cfnConfigDeliveryBucket(t, registry, "cfg-logs")

	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRecorder": {
				"Type": "AWS::Config::ConfigurationRecorder",
				"Properties": { "RoleARN": "arn:aws:iam::123456789012:role/config-role" }
			},
			"MyChannel": {
				"Type": "AWS::Config::DeliveryChannel",
				"Properties": { "S3BucketName": "cfg-logs" }
			},
			"MyRule": {
				"Type": "AWS::Config::ConfigRule",
				"Properties": {
					"ConfigRuleName": "s3-encrypted",
					"Source": { "Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED" }
				}
			}
		}
	}`
	_, err := d.Deploy(context.Background(), tmpl, "config-teardown-stack", nil)
	require.NoError(t, err)
	require.Len(t, cfnConfigRecorders(t, registry), 1)
	require.Len(t, cfnConfigChannels(t, registry), 1)
	require.Len(t, cfnConfigRules(t, registry), 1)
	require.Equal(t, true, cfnConfigRecorderStatus(t, registry)[0]["recording"])

	// DeleteStack reports an error naming every resource whose delete failed, so a
	// clean return is the assertion that all three deletes were accepted.
	require.NoError(t, d.DeleteStack(context.Background(), "config-teardown-stack"))

	assert.Empty(t, cfnConfigRecorders(t, registry), "the recorder is really gone")
	assert.Empty(t, cfnConfigChannels(t, registry), "the channel is really gone")
	assert.Empty(t, cfnConfigRules(t, registry), "the rule is really gone")

	// The rebuild: the same template deploys clean a second time, which is what the
	// stub delete made impossible.
	rebuilt, err := d.Deploy(context.Background(), tmpl, "config-teardown-stack", nil)
	require.NoError(t, err)
	for _, res := range rebuilt.Resources {
		assert.Empty(t, res.Error, "%s (%s)", res.LogicalID, res.Type)
	}
	assert.Len(t, cfnConfigRecorders(t, registry), 1)
	assert.Equal(t, true, cfnConfigRecorderStatus(t, registry)[0]["recording"])
}

// TestCFN_ConfigTeardownToleratesAnAlreadyAbsentResource verifies a resource deleted
// out of band does not wedge the sweep.
//
// Config's not-found exceptions are HTTP 400 like every other Config exception, so
// nothing but the error code distinguishes an absence from a real failure — which is
// why the three NoSuch codes had to be added to cfnDeleteAbsentCodes. Without them a
// stack whose rule someone deleted by hand sits in DELETE_FAILED on a condition the
// caller cannot fix.
func TestCFN_ConfigTeardownToleratesAnAlreadyAbsentResource(t *testing.T) {
	d, registry := newConfigTestDeployer(t)
	cfnConfigDeliveryBucket(t, registry, "cfg-logs")

	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRecorder": {
				"Type": "AWS::Config::ConfigurationRecorder",
				"Properties": { "RoleARN": "arn:aws:iam::123456789012:role/config-role" }
			},
			"MyChannel": {
				"Type": "AWS::Config::DeliveryChannel",
				"Properties": { "S3BucketName": "cfg-logs" }
			},
			"MyRule": {
				"Type": "AWS::Config::ConfigRule",
				"Properties": {
					"ConfigRuleName": "s3-encrypted",
					"Source": { "Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED" }
				}
			}
		}
	}`
	_, err := d.Deploy(context.Background(), tmpl, "config-absent-stack", nil)
	require.NoError(t, err)

	// Out of band, as a consumer's console click would.
	configOp(t, registry, "DeleteConfigRule", map[string]any{"ConfigRuleName": "s3-encrypted"}, nil)

	// The sweep is run directly rather than through DeleteStack so the per-resource
	// statuses are visible: a tolerated absence must be a completion rather than a
	// skip, because the sweep's goal is that the resource not exist and it does not.
	deletions := d.DeleteStackResourcesForTest(context.Background(), "config-absent-stack")
	require.Len(t, deletions, 3)
	for _, res := range deletions {
		assert.Equal(t, "DELETE_COMPLETE", res.Status,
			"%s (%s): %s", res.LogicalID, res.Type, res.Reason)
	}

	require.NoError(t, d.DeleteStack(context.Background(), "config-absent-stack"),
		"and the sweep is idempotent: every resource is now absent")
}

// TestCFN_ConfigRecorderWireKeysAreLowerCamel asserts the exact keys the recorder
// translation emits.
//
// Every other test here observes the service, which is the right instinct and cannot
// make this point: substrate decodes a request with encoding/json, whose field
// matching is case-insensitive, so an UpperCamel body decodes into the same struct as
// a lowerCamel one and DescribeConfigurationRecorders reports the same group either
// way. Real Config is case-sensitive and would ignore the UpperCamel members, and the
// recorded request body is what an exported event log replays against AWS — so the
// emitted keys are asserted where they are produced. Mutants renaming
// allSupported/includeGlobalResourceTypes and nesting ResourceTypes under
// exclusionByResourceTypes both survived the service-level tests for exactly this
// reason.
func TestCFN_ConfigRecorderWireKeysAreLowerCamel(t *testing.T) {
	group := emulator.CFGSvcCFNRecordingGroupForTest(map[string]interface{}{
		"AllSupported":               true,
		"IncludeGlobalResourceTypes": true,
		"RecordingStrategy":          map[string]interface{}{"UseOnly": "ALL_SUPPORTED_RESOURCE_TYPES"},
	})
	assert.Equal(t, map[string]interface{}{
		"allSupported":               true,
		"includeGlobalResourceTypes": true,
		"recordingStrategy":          map[string]interface{}{"useOnly": "ALL_SUPPORTED_RESOURCE_TYPES"},
	}, group)

	exclusion := emulator.CFGSvcCFNRecordingGroupForTest(map[string]interface{}{
		"AllSupported": false,
		"ExclusionByResourceTypes": map[string]interface{}{
			"ResourceTypes": []interface{}{"AWS::EC2::Instance"},
		},
		"RecordingStrategy": map[string]interface{}{"UseOnly": "EXCLUSION_BY_RESOURCE_TYPES"},
	})
	assert.Equal(t, map[string]interface{}{
		"allSupported": false,
		"exclusionByResourceTypes": map[string]interface{}{
			"resourceTypes": []string{"AWS::EC2::Instance"},
		},
		"recordingStrategy": map[string]interface{}{"useOnly": "EXCLUSION_BY_RESOURCE_TYPES"},
	}, exclusion)

	inclusion := emulator.CFGSvcCFNRecordingGroupForTest(map[string]interface{}{
		"ResourceTypes":     []interface{}{"AWS::S3::Bucket"},
		"RecordingStrategy": map[string]interface{}{"UseOnly": "INCLUSION_BY_RESOURCE_TYPES"},
	})
	assert.Equal(t, map[string]interface{}{
		"resourceTypes":     []string{"AWS::S3::Bucket"},
		"recordingStrategy": map[string]interface{}{"useOnly": "INCLUSION_BY_RESOURCE_TYPES"},
	}, inclusion, "an unmentioned boolean is absent rather than sent as false")

	mode := emulator.CFGSvcCFNRecordingModeForTest(map[string]interface{}{
		"RecordingFrequency": "CONTINUOUS",
		"RecordingModeOverrides": []interface{}{
			map[string]interface{}{
				"ResourceTypes":      []interface{}{"AWS::S3::Bucket"},
				"RecordingFrequency": "DAILY",
				"Description":        "buckets daily",
			},
		},
	})
	assert.Equal(t, map[string]interface{}{
		"recordingFrequency": "CONTINUOUS",
		"recordingModeOverrides": []map[string]interface{}{{
			"resourceTypes":      []string{"AWS::S3::Bucket"},
			"recordingFrequency": "DAILY",
			"description":        "buckets daily",
		}},
	}, mode)

	assert.Nil(t, emulator.CFGSvcCFNRecordingGroupForTest(map[string]interface{}{}),
		"an empty group is nil, not a group with every parameter unset — which is an "+
			"InvalidRecordingGroupException case")
	assert.Nil(t, emulator.CFGSvcCFNRecordingModeForTest(map[string]interface{}{}),
		"a mode without the required RecordingFrequency is dropped rather than refused")
}
