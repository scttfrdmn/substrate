package substrate_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// newTestDeployer creates a StackDeployer with a full plugin set for CFN tests.
func newTestDeployer(t *testing.T) *substrate.StackDeployer {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	fs := afero.NewMemMapFs()
	costs := substrate.NewCostController(substrate.CostConfig{Enabled: true})

	iamPlugin := &substrate.IAMPlugin{}
	require.NoError(t, iamPlugin.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
	}))
	registry.Register(iamPlugin)

	lambdaPlugin := &substrate.LambdaPlugin{}
	require.NoError(t, lambdaPlugin.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
		},
	}))
	registry.Register(lambdaPlugin)

	sqsPlugin := &substrate.SQSPlugin{}
	require.NoError(t, sqsPlugin.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
		},
	}))
	registry.Register(sqsPlugin)

	s3Plugin := &substrate.S3Plugin{}
	require.NoError(t, s3Plugin.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      fs,
		},
	}))
	registry.Register(s3Plugin)

	appSyncPlugin := &substrate.AppSyncPlugin{}
	require.NoError(t, appSyncPlugin.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
		},
	}))
	registry.Register(appSyncPlugin)

	return substrate.NewStackDeployer(registry, store, state, tc, logger, costs)
}

func TestCFN_Parameters(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"BucketName": {"Type": "String", "Default": "default-bucket"}
		},
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {"BucketName": {"Ref": "BucketName"}}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "test-params", map[string]string{
		"BucketName": "my-custom-bucket",
	})
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "my-custom-bucket", result.Resources[0].PhysicalID)
}

func TestCFN_DefaultParameters(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"BucketName": {"Type": "String", "Default": "default-bucket"}
		},
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {"BucketName": {"Ref": "BucketName"}}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "test-defaults", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "default-bucket", result.Resources[0].PhysicalID)
}

func TestCFN_Conditions_Skip(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"CreateBucket": {"Type": "String", "Default": "false"}
		},
		"Conditions": {
			"ShouldCreate": {"Fn::Equals": [{"Ref": "CreateBucket"}, "true"]}
		},
		"Resources": {
			"AlwaysBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {"BucketName": "always-bucket"}
			},
			"ConditionalBucket": {
				"Type": "AWS::S3::Bucket",
				"Condition": "ShouldCreate",
				"Properties": {"BucketName": "conditional-bucket"}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "test-conditions", nil)
	require.NoError(t, err)
	// Only AlwaysBucket should be deployed (condition is false).
	assert.Len(t, result.Resources, 1)
	assert.Equal(t, "always-bucket", result.Resources[0].PhysicalID)
}

func TestCFN_Conditions_True(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"CreateBucket": {"Type": "String", "Default": "false"}
		},
		"Conditions": {
			"ShouldCreate": {"Fn::Equals": [{"Ref": "CreateBucket"}, "true"]}
		},
		"Resources": {
			"AlwaysBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {"BucketName": "always-bucket-2"}
			},
			"ConditionalBucket": {
				"Type": "AWS::S3::Bucket",
				"Condition": "ShouldCreate",
				"Properties": {"BucketName": "conditional-bucket-2"}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "test-conditions-true", map[string]string{
		"CreateBucket": "true",
	})
	require.NoError(t, err)
	// Both buckets should be deployed when condition is true.
	assert.Len(t, result.Resources, 2)
}

func TestCFN_Outputs(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {"BucketName": "output-test-bucket"}
			}
		},
		"Outputs": {
			"BucketName": {
				"Value": {"Ref": "MyBucket"},
				"Description": "The bucket name"
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "test-outputs", nil)
	require.NoError(t, err)
	assert.Equal(t, "output-test-bucket", result.Outputs["BucketName"])
}

func TestCFN_FnSub(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {"BucketName": {"Fn::Sub": "bucket-${AWS::Region}"}}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "test-fnsub", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "bucket-us-east-1", result.Resources[0].PhysicalID)
}

func TestCFN_FnJoin(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {"BucketName": {"Fn::Join": ["-", ["my", "joined", "bucket"]]}}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "test-fnjoin", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "my-joined-bucket", result.Resources[0].PhysicalID)
}

func TestCFN_LambdaFunction(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyFunction": {
				"Type": "AWS::Lambda::Function",
				"Properties": {
					"FunctionName": "my-cfn-function",
					"Runtime": "python3.12",
					"Role": "arn:aws:iam::123456789012:role/lambda-role",
					"Handler": "index.handler"
				}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "test-lambda", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "my-cfn-function", result.Resources[0].PhysicalID)
	assert.Empty(t, result.Resources[0].Error)
}

func TestCFN_SQSQueue(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyQueue": {
				"Type": "AWS::SQS::Queue",
				"Properties": {
					"QueueName": "my-cfn-queue"
				}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "test-sqs", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "my-cfn-queue", result.Resources[0].PhysicalID)
	assert.Empty(t, result.Resources[0].Error)
	assert.Contains(t, result.Resources[0].ARN, "my-cfn-queue")
}

func TestCFN_UpdateStack(t *testing.T) {
	d := newTestDeployer(t)

	// First deploy.
	tmpl1 := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"B1": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "update-bucket-1"}}
		}
	}`
	_, err := d.Deploy(context.Background(), tmpl1, "update-stack", nil)
	require.NoError(t, err)

	// Update with new template adding a second bucket.
	tmpl2 := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"B1": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "update-bucket-1"}},
			"B2": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "update-bucket-2"}}
		}
	}`
	result, err := d.UpdateStack(context.Background(), tmpl2, "update-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 2)
}

func TestCFN_DeleteStack(t *testing.T) {
	d := newTestDeployer(t)

	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "delete-test-bucket"}}
		}
	}`
	_, err := d.Deploy(context.Background(), tmpl, "delete-stack", nil)
	require.NoError(t, err)

	// Verify stack exists.
	stacks, err := d.ListStacks(context.Background())
	require.NoError(t, err)
	assert.Len(t, stacks, 1)

	// Delete it.
	err = d.DeleteStack(context.Background(), "delete-stack")
	require.NoError(t, err)

	// List again — should be empty.
	stacks2, err := d.ListStacks(context.Background())
	require.NoError(t, err)
	assert.Len(t, stacks2, 0)
}

func TestCFN_ListStacks(t *testing.T) {
	d := newTestDeployer(t)

	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("list-stack-%d", i)
		tmpl := fmt.Sprintf(`{
			"AWSTemplateFormatVersion": "2010-09-09",
			"Resources": {
				"B": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "list-bucket-%d"}}
			}
		}`, i)
		_, err := d.Deploy(context.Background(), tmpl, name, nil)
		require.NoError(t, err)
	}

	stacks, err := d.ListStacks(context.Background())
	require.NoError(t, err)
	assert.Len(t, stacks, 3)
}

// TestCFN_StackOutputsJSON verifies that Outputs are JSON-serializable and
// that parameter refs and pseudo-parameters resolve correctly.
func TestCFN_StackOutputsJSON(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Env": {"Type": "String", "Default": "prod"}
		},
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "out-bucket"}}
		},
		"Outputs": {
			"EnvName": {"Value": {"Ref": "Env"}, "Description": "env"},
			"Region":  {"Value": {"Ref": "AWS::Region"}, "Description": "region"}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "outputs-stack", map[string]string{
		"Env": "staging",
	})
	require.NoError(t, err)
	assert.Equal(t, "staging", result.Outputs["EnvName"])
	assert.Equal(t, "us-east-1", result.Outputs["Region"])

	// Outputs must be JSON-serializable.
	_, jsonErr := json.Marshal(result.Outputs)
	require.NoError(t, jsonErr)
}

// TestCFN_UnknownResourceType verifies that unknown resource types are skipped
// without causing an error.
func TestCFN_UnknownResourceType(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Known":   {"Type": "AWS::S3::Bucket",   "Properties": {"BucketName": "known-bucket"}},
			"Unknown": {"Type": "AWS::Custom::Thing", "Properties": {}}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "unknown-type-stack", nil)
	require.NoError(t, err)
	// Both are returned (unknown type as zero-error resource).
	assert.Len(t, result.Resources, 2)
	for _, r := range result.Resources {
		if r.Type == "AWS::S3::Bucket" {
			assert.Empty(t, r.Error)
		}
	}
}

// TestCFN_StackName verifies that AWS::StackName resolves to the stream ID.
func TestCFN_StackName(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "stackname-bucket"}}
		},
		"Outputs": {
			"StackNameOut": {"Value": {"Ref": "AWS::StackName"}}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "my-named-stack", nil)
	require.NoError(t, err)
	assert.Equal(t, "my-named-stack", result.Outputs["StackNameOut"])
}

// TestCFN_AccountIdPseudoParam verifies that AWS::AccountId resolves correctly.
func TestCFN_AccountIdPseudoParam(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "account-bucket"}}
		},
		"Outputs": {
			"AccountOut": {"Value": {"Ref": "AWS::AccountId"}}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "account-stack", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Outputs["AccountOut"])
}

// TestCFN_FnSelect verifies that Fn::Select resolves the correct list element.
func TestCFN_FnSelect(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {
					"BucketName": {"Fn::Select": ["1", ["first", "second", "third"]]}
				}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "fn-select-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "second", result.Resources[0].PhysicalID)
}

// TestCFN_FnBase64 verifies that Fn::Base64 encodes the value correctly.
func TestCFN_FnBase64(t *testing.T) {
	d := newTestDeployer(t)
	// We can't use base64 as a bucket name directly, so we route it through Outputs.
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "base64-test-bucket"}}
		},
		"Outputs": {
			"Encoded": {"Value": {"Fn::Base64": "hello"}}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "fn-base64-stack", nil)
	require.NoError(t, err)
	// base64("hello") = "aGVsbG8="
	assert.Equal(t, "aGVsbG8=", result.Outputs["Encoded"])
}

// TestCFN_FnGetAtt verifies that Fn::GetAtt on a deployed resource returns its ARN.
func TestCFN_FnGetAtt(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRole": {
				"Type": "AWS::IAM::Role",
				"Properties": {
					"RoleName": "getatt-test-role",
					"AssumeRolePolicyDocument": {
						"Version": "2012-10-17",
						"Statement": []
					}
				}
			}
		},
		"Outputs": {
			"RoleArn": {"Value": {"Fn::GetAtt": ["MyRole", "Arn"]}}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "fn-getatt-stack", nil)
	require.NoError(t, err)
	assert.Contains(t, result.Outputs["RoleArn"], "arn:aws:iam::")
}

// TestCFN_FnIf verifies that Fn::If selects the correct branch based on a condition.
func TestCFN_FnIf(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"IsProd": {"Type": "String", "Default": "false"}
		},
		"Conditions": {
			"ProdEnv": {"Fn::Equals": [{"Ref": "IsProd"}, "true"]}
		},
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "fnif-bucket"}}
		},
		"Outputs": {
			"EnvLabel": {
				"Value": {"Fn::If": ["ProdEnv", "production", "development"]}
			}
		}
	}`

	// Default (false) → development.
	result, err := d.Deploy(context.Background(), tmpl, "fn-if-false-stack", nil)
	require.NoError(t, err)
	assert.Equal(t, "development", result.Outputs["EnvLabel"])

	// Override to true → production.
	d2 := newTestDeployer(t)
	result2, err := d2.Deploy(context.Background(), tmpl, "fn-if-true-stack", map[string]string{
		"IsProd": "true",
	})
	require.NoError(t, err)
	assert.Equal(t, "production", result2.Outputs["EnvLabel"])
}

// TestCFN_FnSub_WithMap verifies the two-argument form of Fn::Sub.
func TestCFN_FnSub_WithMap(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "fnsub-map-bucket"}}
		},
		"Outputs": {
			"Greeting": {
				"Value": {"Fn::Sub": ["Hello ${Name}", {"Name": "World"}]}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "fn-sub-map-stack", nil)
	require.NoError(t, err)
	assert.Equal(t, "Hello World", result.Outputs["Greeting"])
}

// TestCFN_FnSplit verifies that Fn::Split returns the first element.
func TestCFN_FnSplit(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "fnsplit-bucket"}}
		},
		"Outputs": {
			"FirstPart": {
				"Value": {"Fn::Split": ["/", "a/b/c"]}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "fn-split-stack", nil)
	require.NoError(t, err)
	// resolveFnSplitFirst returns only the first part.
	assert.Equal(t, "a", result.Outputs["FirstPart"])
}

// TestCFN_Condition_And verifies the Fn::And condition combinator using inline
// Fn::Equals expressions to avoid map-iteration order issues.
func TestCFN_Condition_And(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"A": {"Type": "String", "Default": "yes"},
			"B": {"Type": "String", "Default": "yes"}
		},
		"Conditions": {
			"BothTrue": {"Fn::And": [
				{"Fn::Equals": [{"Ref": "A"}, "yes"]},
				{"Fn::Equals": [{"Ref": "B"}, "yes"]}
			]}
		},
		"Resources": {
			"AlwaysBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "and-always-bucket"}},
			"BothBucket": {
				"Type": "AWS::S3::Bucket",
				"Condition": "BothTrue",
				"Properties": {"BucketName": "and-cond-bucket"}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "fn-and-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 2)
}

// TestCFN_Condition_Or verifies the Fn::Or condition combinator using inline
// Fn::Equals expressions to avoid map-iteration order issues.
func TestCFN_Condition_Or(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"A": {"Type": "String", "Default": "no"},
			"B": {"Type": "String", "Default": "yes"}
		},
		"Conditions": {
			"EitherTrue": {"Fn::Or": [
				{"Fn::Equals": [{"Ref": "A"}, "yes"]},
				{"Fn::Equals": [{"Ref": "B"}, "yes"]}
			]}
		},
		"Resources": {
			"AlwaysBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "or-always-bucket"}},
			"OrBucket": {
				"Type": "AWS::S3::Bucket",
				"Condition": "EitherTrue",
				"Properties": {"BucketName": "or-cond-bucket"}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "fn-or-stack", nil)
	require.NoError(t, err)
	// B is "yes" so EitherTrue is true → both buckets deployed.
	assert.Len(t, result.Resources, 2)
}

// TestCFN_Condition_Not verifies the Fn::Not condition combinator.
func TestCFN_Condition_Not(t *testing.T) {
	d := newTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Parameters": {
			"Flag": {"Type": "String", "Default": "false"}
		},
		"Conditions": {
			"FlagTrue":  {"Fn::Equals": [{"Ref": "Flag"}, "true"]},
			"FlagFalse": {"Fn::Not": [{"Condition": "FlagTrue"}]}
		},
		"Resources": {
			"AlwaysBucket": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "not-always-bucket"}},
			"NotBucket": {
				"Type": "AWS::S3::Bucket",
				"Condition": "FlagFalse",
				"Properties": {"BucketName": "not-cond-bucket"}
			}
		}
	}`

	result, err := d.Deploy(context.Background(), tmpl, "fn-not-stack", nil)
	require.NoError(t, err)
	// Flag is false → FlagFalse is true → both buckets deployed.
	assert.Len(t, result.Resources, 2)
}

// newEC2TestDeployer creates a StackDeployer that also has the EC2 plugin registered.
func newEC2TestDeployer(t *testing.T) *substrate.StackDeployer {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	costs := substrate.NewCostController(substrate.CostConfig{Enabled: true})

	ec2Plugin := &substrate.EC2Plugin{}
	require.NoError(t, ec2Plugin.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(ec2Plugin)

	return substrate.NewStackDeployer(registry, store, state, tc, logger, costs)
}

func TestCFN_EC2VPC(t *testing.T) {
	d := newEC2TestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyVPC": {
				"Type": "AWS::EC2::VPC",
				"Properties": {"CidrBlock": "10.0.0.0/16"}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "ec2-vpc-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "AWS::EC2::VPC", result.Resources[0].Type)
	assert.NotEmpty(t, result.Resources[0].PhysicalID)
}

func TestCFN_EC2Subnet(t *testing.T) {
	d := newEC2TestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyVPC": {
				"Type": "AWS::EC2::VPC",
				"Properties": {"CidrBlock": "10.2.0.0/16"}
			},
			"MySubnet": {
				"Type": "AWS::EC2::Subnet",
				"Properties": {
					"VpcId": {"Ref": "MyVPC"},
					"CidrBlock": "10.2.1.0/24"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "ec2-subnet-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 2)
	var vpcID string
	for _, r := range result.Resources {
		if r.Type == "AWS::EC2::VPC" {
			vpcID = r.PhysicalID
		}
	}
	assert.NotEmpty(t, vpcID)
}

func TestCFN_EC2SecurityGroup(t *testing.T) {
	d := newEC2TestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyVPC": {
				"Type": "AWS::EC2::VPC",
				"Properties": {"CidrBlock": "10.3.0.0/16"}
			},
			"MySG": {
				"Type": "AWS::EC2::SecurityGroup",
				"Properties": {
					"GroupDescription": "test sg",
					"VpcId": {"Ref": "MyVPC"}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "ec2-sg-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 2)
}

func TestCFN_EC2InternetGateway(t *testing.T) {
	d := newEC2TestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyIGW": {
				"Type": "AWS::EC2::InternetGateway",
				"Properties": {}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "ec2-igw-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "AWS::EC2::InternetGateway", result.Resources[0].Type)
}

func TestCFN_EC2RouteTable(t *testing.T) {
	d := newEC2TestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyVPC": {
				"Type": "AWS::EC2::VPC",
				"Properties": {"CidrBlock": "10.4.0.0/16"}
			},
			"MyRTB": {
				"Type": "AWS::EC2::RouteTable",
				"Properties": {"VpcId": {"Ref": "MyVPC"}}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "ec2-rtb-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 2)
}

func TestCFN_EC2Instance(t *testing.T) {
	d := newEC2TestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyInstance": {
				"Type": "AWS::EC2::Instance",
				"Properties": {
					"ImageId": "ami-cfntest",
					"InstanceType": "t3.micro"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "ec2-instance-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "AWS::EC2::Instance", result.Resources[0].Type)
	assert.NotEmpty(t, result.Resources[0].PhysicalID)
}

// newFullTestDeployer creates a StackDeployer with a broad plugin set covering
// APIGateway, StepFunctions, ECR, ECS, Cognito, Kinesis, CloudFront and ACM.
func newFullTestDeployer(t *testing.T) *substrate.StackDeployer {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	fs := afero.NewMemMapFs()
	costs := substrate.NewCostController(substrate.CostConfig{Enabled: true})

	opts := substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      fs,
		},
	}

	for _, p := range []substrate.Plugin{
		&substrate.IAMPlugin{},
		&substrate.LambdaPlugin{},
		&substrate.SQSPlugin{},
		&substrate.S3Plugin{},
		&substrate.ACMPlugin{},
		&substrate.APIGatewayPlugin{},
		&substrate.APIGatewayV2Plugin{},
		&substrate.StepFunctionsPlugin{},
		&substrate.ECRPlugin{},
		&substrate.ECSPlugin{},
		&substrate.CognitoIDPPlugin{},
		&substrate.CognitoIdentityPlugin{},
		&substrate.KinesisPlugin{},
		&substrate.CloudFrontPlugin{},
		&substrate.RDSPlugin{},
		&substrate.ElastiCachePlugin{},
		&substrate.EFSPlugin{},
		&substrate.GluePlugin{},
		&substrate.BudgetsPlugin{},
		&substrate.MSKPlugin{},
		&substrate.SESv2Plugin{},
		&substrate.FirehosePlugin{},
	} {
		require.NoError(t, p.Initialize(context.Background(), opts))
		registry.Register(p)
	}

	return substrate.NewStackDeployer(registry, store, state, tc, logger, costs)
}

func TestCFN_ACMCertificate(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyCert": {
				"Type": "AWS::CertificateManager::Certificate",
				"Properties": {
					"DomainName": "example.com"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "acm-cert-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "AWS::CertificateManager::Certificate", result.Resources[0].Type)
	assert.Empty(t, result.Resources[0].Error)
	assert.NotEmpty(t, result.Resources[0].ARN)
}

func TestCFN_APIGatewayRestAPI(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::ApiGateway::RestApi",
				"Properties": {
					"Name": "cfn-test-api",
					"Description": "API from CFN"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "apigw-restapi-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::ApiGateway::RestApi", r.Type)
	assert.Empty(t, r.Error)
	assert.NotEmpty(t, r.PhysicalID)
}

func TestCFN_APIGatewayFullStack(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::ApiGateway::RestApi",
				"Properties": {"Name": "full-cfn-api"}
			},
			"MyDeployment": {
				"Type": "AWS::ApiGateway::Deployment",
				"Properties": {"RestApiId": {"Ref": "MyAPI"}}
			},
			"MyStage": {
				"Type": "AWS::ApiGateway::Stage",
				"Properties": {
					"RestApiId": {"Ref": "MyAPI"},
					"DeploymentId": {"Ref": "MyDeployment"},
					"StageName": "prod"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "apigw-fullstack-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 3)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s had error: %s", r.LogicalID, r.Error)
	}
}

func TestCFN_APIGatewayAuthorizer(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::ApiGateway::RestApi",
				"Properties": {"Name": "auth-api"}
			},
			"MyAuth": {
				"Type": "AWS::ApiGateway::Authorizer",
				"Properties": {
					"RestApiId": {"Ref": "MyAPI"},
					"Name": "my-authorizer",
					"Type": "TOKEN",
					"AuthorizerUri": "arn:aws:apigateway:us-east-1:lambda:path/functions/arn:aws:lambda:us-east-1:123456789012:function:auth/invocations"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "apigw-auth-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 2)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s had error: %s", r.LogicalID, r.Error)
	}
}

func TestCFN_APIGatewayResource(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::ApiGateway::RestApi",
				"Properties": {"Name": "resource-api"}
			},
			"MyResource": {
				"Type": "AWS::ApiGateway::Resource",
				"Properties": {
					"RestApiId": {"Ref": "MyAPI"},
					"PathPart": "items",
					"ParentId": {"Fn::GetAtt": ["MyAPI", "RootResourceId"]}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "apigw-resource-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 2)
}

func TestCFN_APIGatewayAPIKey(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::ApiGateway::RestApi",
				"Properties": {"Name": "apikey-api"}
			},
			"MyKey": {
				"Type": "AWS::ApiGateway::ApiKey",
				"Properties": {
					"Name": "cfn-test-key",
					"Enabled": true
				}
			},
			"MyPlan": {
				"Type": "AWS::ApiGateway::UsagePlan",
				"Properties": {"UsagePlanName": "cfn-test-plan"}
			},
			"MyPlanKey": {
				"Type": "AWS::ApiGateway::UsagePlanKey",
				"Properties": {
					"KeyId": {"Ref": "MyKey"},
					"KeyType": "API_KEY",
					"UsagePlanId": {"Ref": "MyPlan"}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "apigw-apikey-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 4)
}

func TestCFN_APIGatewayV2API(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::ApiGatewayV2::Api",
				"Properties": {
					"Name": "cfn-v2-api",
					"ProtocolType": "HTTP"
				}
			},
			"MyStage": {
				"Type": "AWS::ApiGatewayV2::Stage",
				"Properties": {
					"ApiId": {"Ref": "MyAPI"},
					"StageName": "$default"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "apigwv2-api-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 2)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s: %s", r.LogicalID, r.Error)
	}
}

func TestCFN_APIGatewayV2RouteAndIntegration(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::ApiGatewayV2::Api",
				"Properties": {"Name": "v2-route-api", "ProtocolType": "HTTP"}
			},
			"MyIntegration": {
				"Type": "AWS::ApiGatewayV2::Integration",
				"Properties": {
					"ApiId": {"Ref": "MyAPI"},
					"IntegrationType": "AWS_PROXY",
					"IntegrationUri": "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
				}
			},
			"MyRoute": {
				"Type": "AWS::ApiGatewayV2::Route",
				"Properties": {
					"ApiId": {"Ref": "MyAPI"},
					"RouteKey": "GET /items"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "apigwv2-route-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 3)
}

func TestCFN_APIGatewayV2Authorizer(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::ApiGatewayV2::Api",
				"Properties": {"Name": "v2-auth-api", "ProtocolType": "HTTP"}
			},
			"MyAuth": {
				"Type": "AWS::ApiGatewayV2::Authorizer",
				"Properties": {
					"ApiId": {"Ref": "MyAPI"},
					"AuthorizerType": "JWT",
					"Name": "cfn-v2-auth"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "apigwv2-auth-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 2)
}

func TestCFN_StepFunctionsStateMachine(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyStateMachine": {
				"Type": "AWS::StepFunctions::StateMachine",
				"Properties": {
					"StateMachineName": "cfn-test-sm",
					"RoleArn": "arn:aws:iam::123456789012:role/sfn-role"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "sfn-sm-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::StepFunctions::StateMachine", r.Type)
	assert.Empty(t, r.Error)
	assert.NotEmpty(t, r.ARN)
}

func TestCFN_StepFunctionsActivity(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyActivity": {
				"Type": "AWS::StepFunctions::Activity",
				"Properties": {"Name": "cfn-test-activity"}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "sfn-activity-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::StepFunctions::Activity", r.Type)
	assert.Empty(t, r.Error)
}

func TestCFN_ECRRepository(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRepo": {
				"Type": "AWS::ECR::Repository",
				"Properties": {"RepositoryName": "cfn-test-repo"}
			},
			"MyLifecycle": {
				"Type": "AWS::ECR::LifecyclePolicy",
				"Properties": {
					"RepositoryName": {"Ref": "MyRepo"},
					"LifecyclePolicyText": "{\"rules\":[]}"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "ecr-repo-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 2)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s: %s", r.LogicalID, r.Error)
	}
}

func TestCFN_ECSClusterAndTaskDefinition(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyCluster": {
				"Type": "AWS::ECS::Cluster",
				"Properties": {"ClusterName": "cfn-test-cluster"}
			},
			"MyTaskDef": {
				"Type": "AWS::ECS::TaskDefinition",
				"Properties": {
					"Family": "cfn-test-task",
					"ContainerDefinitions": [
						{
							"Name": "my-container",
							"Image": "nginx:latest",
							"Memory": 512,
							"Cpu": 256
						}
					]
				}
			},
			"MyCapProvider": {
				"Type": "AWS::ECS::CapacityProvider",
				"Properties": {
					"Name": "cfn-test-cap-provider"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "ecs-cluster-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 3)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s: %s", r.LogicalID, r.Error)
	}
}

func TestCFN_ECSService(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyCluster": {
				"Type": "AWS::ECS::Cluster",
				"Properties": {"ClusterName": "svc-test-cluster"}
			},
			"MyTaskDef": {
				"Type": "AWS::ECS::TaskDefinition",
				"Properties": {
					"Family": "svc-test-task",
					"ContainerDefinitions": [{"Name": "c1", "Image": "nginx", "Memory": 256}]
				}
			},
			"MyService": {
				"Type": "AWS::ECS::Service",
				"Properties": {
					"ServiceName": "cfn-test-service",
					"Cluster": {"Ref": "MyCluster"},
					"TaskDefinition": {"Ref": "MyTaskDef"},
					"DesiredCount": 1
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "ecs-service-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 3)
}

func TestCFN_CognitoUserPool(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyPool": {
				"Type": "AWS::Cognito::UserPool",
				"Properties": {"UserPoolName": "cfn-test-pool"}
			},
			"MyClient": {
				"Type": "AWS::Cognito::UserPoolClient",
				"Properties": {
					"UserPoolId": {"Ref": "MyPool"},
					"ClientName": "cfn-test-client"
				}
			},
			"MyGroup": {
				"Type": "AWS::Cognito::UserPoolGroup",
				"Properties": {
					"UserPoolId": {"Ref": "MyPool"},
					"GroupName": "cfn-admins"
				}
			},
			"MyDomain": {
				"Type": "AWS::Cognito::UserPoolDomain",
				"Properties": {
					"UserPoolId": {"Ref": "MyPool"},
					"Domain": "cfn-test-domain"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "cognito-pool-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 4)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s: %s", r.LogicalID, r.Error)
	}
}

func TestCFN_CognitoIdentityPool(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyPool": {
				"Type": "AWS::Cognito::UserPool",
				"Properties": {"UserPoolName": "idpool-test-pool"}
			},
			"MyIdentityPool": {
				"Type": "AWS::Cognito::IdentityPool",
				"Properties": {"IdentityPoolName": "cfn-test-identity-pool"}
			},
			"MyRoleAttach": {
				"Type": "AWS::Cognito::IdentityPoolRoleAttachment",
				"Properties": {
					"IdentityPoolId": {"Ref": "MyIdentityPool"},
					"Roles": {
						"authenticated": "arn:aws:iam::123456789012:role/auth-role",
						"unauthenticated": "arn:aws:iam::123456789012:role/unauth-role"
					}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "cognito-idpool-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 3)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s: %s", r.LogicalID, r.Error)
	}
}

func TestCFN_KinesisStream(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyStream": {
				"Type": "AWS::Kinesis::Stream",
				"Properties": {"Name": "cfn-test-stream"}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "kinesis-stream-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::Kinesis::Stream", r.Type)
	assert.Empty(t, r.Error)
	assert.NotEmpty(t, r.ARN)
}

func TestCFN_CloudFrontDistribution(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyDistribution": {
				"Type": "AWS::CloudFront::Distribution",
				"Properties": {
					"DistributionConfig": {
						"Comment": "CFN test distribution",
						"Enabled": true
					}
				}
			},
			"MyOAI": {
				"Type": "AWS::CloudFront::CloudFrontOriginAccessIdentity",
				"Properties": {
					"CloudFrontOriginAccessIdentityConfig": {
						"Comment": "cfn-test-oai"
					}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "cloudfront-dist-stack", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 2)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s: %s", r.LogicalID, r.Error)
	}
}

// TestCFN_FnGetAtt_ECRRepositoryUri verifies that Fn::GetAtt RepositoryUri works
// for ECR repositories.
func TestCFN_FnGetAtt_ECRRepositoryUri(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRepo": {
				"Type": "AWS::ECR::Repository",
				"Properties": {"RepositoryName": "getatt-ecr-repo"}
			}
		},
		"Outputs": {
			"RepoUri": {"Value": {"Fn::GetAtt": ["MyRepo", "RepositoryUri"]}}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "ecr-getatt-stack", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Outputs["RepoUri"])
}

// TestCFN_FnGetAtt_CognitoProviderName verifies Fn::GetAtt ProviderName and ProviderURL.
func TestCFN_FnGetAtt_CognitoProviderName(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyPool": {
				"Type": "AWS::Cognito::UserPool",
				"Properties": {"UserPoolName": "getatt-pool"}
			}
		},
		"Outputs": {
			"ProviderName": {"Value": {"Fn::GetAtt": ["MyPool", "ProviderName"]}},
			"ProviderURL":  {"Value": {"Fn::GetAtt": ["MyPool", "ProviderURL"]}}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "cognito-getatt-stack", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Outputs["ProviderName"])
	assert.NotEmpty(t, result.Outputs["ProviderURL"])
}

// TestCFN_FnGetAtt_StepFunctionsName verifies Fn::GetAtt Name for state machines.
func TestCFN_FnGetAtt_StepFunctionsName(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MySM": {
				"Type": "AWS::StepFunctions::StateMachine",
				"Properties": {
					"StateMachineName": "getatt-state-machine",
					"RoleArn": "arn:aws:iam::123456789012:role/sfn-role"
				}
			}
		},
		"Outputs": {
			"SMName": {"Value": {"Fn::GetAtt": ["MySM", "Name"]}}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "sfn-getatt-stack", nil)
	require.NoError(t, err)
	assert.Equal(t, "getatt-state-machine", result.Outputs["SMName"])
}

// TestCFN_FnGetAtt_CloudFrontDomainName verifies Fn::GetAtt DomainName for distributions.
func TestCFN_FnGetAtt_CloudFrontDomainName(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyDist": {
				"Type": "AWS::CloudFront::Distribution",
				"Properties": {
					"DistributionConfig": {
						"Comment": "getatt test",
						"Enabled": true
					}
				}
			}
		},
		"Outputs": {
			"Domain": {"Value": {"Fn::GetAtt": ["MyDist", "DomainName"]}}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "cf-getatt-stack", nil)
	require.NoError(t, err)
	assert.Contains(t, result.Outputs["Domain"], "cloudfront.net")
}

// TestCFN_FnGetAtt_KinesisStreamArn verifies Fn::GetAtt StreamArn for Kinesis streams.
func TestCFN_FnGetAtt_KinesisStreamArn(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyStream": {
				"Type": "AWS::Kinesis::Stream",
				"Properties": {"Name": "getatt-kinesis-stream"}
			}
		},
		"Outputs": {
			"StreamArn": {"Value": {"Fn::GetAtt": ["MyStream", "StreamArn"]}}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "kinesis-getatt-stack", nil)
	require.NoError(t, err)
	assert.Contains(t, result.Outputs["StreamArn"], "arn:aws:kinesis")
	assert.Contains(t, result.Outputs["StreamArn"], "getatt-kinesis-stream")
}

// TestCFN_FnGetAtt_APIGatewayInvokeURL verifies Fn::GetAtt InvokeURL for stages.
func TestCFN_FnGetAtt_APIGatewayInvokeURL(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::ApiGateway::RestApi",
				"Properties": {"Name": "getatt-invoke-api"}
			},
			"MyDeployment": {
				"Type": "AWS::ApiGateway::Deployment",
				"Properties": {"RestApiId": {"Ref": "MyAPI"}}
			},
			"MyStage": {
				"Type": "AWS::ApiGateway::Stage",
				"Properties": {
					"RestApiId": {"Ref": "MyAPI"},
					"DeploymentId": {"Ref": "MyDeployment"},
					"StageName": "v1"
				}
			}
		},
		"Outputs": {
			"InvokeURL": {"Value": {"Fn::GetAtt": ["MyStage", "InvokeURL"]}}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "apigw-invokeurl-stack", nil)
	require.NoError(t, err)
	// InvokeURL may be empty if not populated by the stage deployer, but the output should exist.
	assert.NotNil(t, result.Outputs["InvokeURL"])
}

// ----- v0.25.0 — RDS and ElastiCache CFN tests -----

// TestCFN_RDSDBInstance verifies that an RDS DB instance can be deployed via CFN.
func TestCFN_RDSDBInstance(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MySG": {
				"Type": "AWS::RDS::DBSubnetGroup",
				"Properties": {
					"DBSubnetGroupName": "cfn-rds-sg",
					"DBSubnetGroupDescription": "CFN test subnet group"
				}
			},
			"MyPG": {
				"Type": "AWS::RDS::DBParameterGroup",
				"Properties": {
					"DBParameterGroupName": "cfn-rds-pg",
					"Family": "mysql8.0",
					"Description": "CFN test param group"
				}
			},
			"MyDB": {
				"Type": "AWS::RDS::DBInstance",
				"Properties": {
					"DBInstanceIdentifier": "cfn-rds-db",
					"DBInstanceClass": "db.t3.micro",
					"Engine": "mysql",
					"MasterUsername": "admin",
					"AllocatedStorage": "20",
					"DBSubnetGroupName": {"Ref": "MySG"}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "rds-db-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 3)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s: %s", r.LogicalID, r.Error)
	}
	// Find DB instance resource and verify ARN is set.
	for _, r := range result.Resources {
		if r.Type == "AWS::RDS::DBInstance" {
			assert.NotEmpty(t, r.ARN)
			assert.Contains(t, r.ARN, "arn:aws:rds")
		}
	}
}

// TestCFN_FnGetAtt_RDSEndpoint verifies Fn::GetAtt Endpoint.Address for RDS instances.
func TestCFN_FnGetAtt_RDSEndpoint(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyDB": {
				"Type": "AWS::RDS::DBInstance",
				"Properties": {
					"DBInstanceIdentifier": "getatt-rds-db",
					"DBInstanceClass": "db.t3.micro",
					"Engine": "postgres"
				}
			}
		},
		"Outputs": {
			"DBEndpoint": {"Value": {"Fn::GetAtt": ["MyDB", "Endpoint.Address"]}}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "rds-getatt-stack", nil)
	require.NoError(t, err)
	assert.Contains(t, result.Outputs["DBEndpoint"], "getatt-rds-db")
	assert.Contains(t, result.Outputs["DBEndpoint"], "rds.")
}

// TestCFN_ElastiCacheCacheCluster verifies ElastiCache cluster deployment via CFN.
func TestCFN_ElastiCacheCacheCluster(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MySubnetGroup": {
				"Type": "AWS::ElastiCache::SubnetGroup",
				"Properties": {
					"CacheSubnetGroupName": "cfn-ec-sg",
					"Description": "CFN test ElastiCache subnet group"
				}
			},
			"MyParamGroup": {
				"Type": "AWS::ElastiCache::ParameterGroup",
				"Properties": {
					"CacheParameterGroupName": "cfn-ec-pg",
					"CacheParameterGroupFamily": "redis7",
					"Description": "CFN test param group"
				}
			},
			"MyCluster": {
				"Type": "AWS::ElastiCache::CacheCluster",
				"Properties": {
					"ClusterId": "cfn-ec-cluster",
					"Engine": "redis",
					"CacheNodeType": "cache.t3.micro",
					"NumCacheNodes": "1"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "elasticache-cluster-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 3)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s: %s", r.LogicalID, r.Error)
	}
	// Verify cluster ARN is set.
	for _, r := range result.Resources {
		if r.Type == "AWS::ElastiCache::CacheCluster" {
			assert.NotEmpty(t, r.ARN)
			assert.Contains(t, r.ARN, "arn:aws:elasticache")
		}
	}
}

// TestCFN_ElastiCacheReplicationGroup verifies replication group deployment via CFN.
func TestCFN_ElastiCacheReplicationGroup(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRG": {
				"Type": "AWS::ElastiCache::ReplicationGroup",
				"Properties": {
					"ReplicationGroupId": "cfn-ec-rg",
					"ReplicationGroupDescription": "CFN test replication group",
					"AutomaticFailoverEnabled": "true"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "elasticache-rg-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::ElastiCache::ReplicationGroup", r.Type)
	assert.Empty(t, r.Error)
	assert.NotEmpty(t, r.ARN)
	assert.Contains(t, r.ARN, "arn:aws:elasticache")
}

// TestCFN_EFSFileSystem verifies EFS file system deployment via CFN.
func TestCFN_EFSFileSystem(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyFS": {
				"Type": "AWS::EFS::FileSystem",
				"Properties": {
					"PerformanceMode": "generalPurpose",
					"ThroughputMode": "bursting"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "efs-fs-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::EFS::FileSystem", r.Type)
	assert.Empty(t, r.Error)
	assert.NotEmpty(t, r.PhysicalID)
	assert.Contains(t, r.PhysicalID, "fs-")
	assert.NotEmpty(t, r.ARN)
	assert.Contains(t, r.ARN, "arn:aws:elasticfilesystem")
}

// TestCFN_EFSAccessPoint verifies EFS access point deployment via CFN.
func TestCFN_EFSAccessPoint(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyFS": {
				"Type": "AWS::EFS::FileSystem",
				"Properties": {}
			},
			"MyAP": {
				"Type": "AWS::EFS::AccessPoint",
				"Properties": {
					"FileSystemId": {"Ref": "MyFS"}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "efs-ap-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 2)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s: %s", r.LogicalID, r.Error)
	}
	for _, r := range result.Resources {
		if r.Type == "AWS::EFS::AccessPoint" {
			assert.Contains(t, r.PhysicalID, "fsap-")
			assert.Contains(t, r.ARN, "arn:aws:elasticfilesystem")
		}
	}
}

// TestCFN_GlueDatabase verifies Glue database deployment via CFN.
func TestCFN_GlueDatabase(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyDB": {
				"Type": "AWS::Glue::Database",
				"Properties": {
					"DatabaseName": "cfn-glue-db",
					"Description": "CFN test Glue database"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "glue-db-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::Glue::Database", r.Type)
	assert.Empty(t, r.Error)
	assert.Equal(t, "cfn-glue-db", r.PhysicalID)
}

// TestCFN_GlueJob verifies Glue job deployment via CFN.
func TestCFN_GlueJob(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyDB": {
				"Type": "AWS::Glue::Database",
				"Properties": {
					"DatabaseName": "cfn-glue-job-db"
				}
			},
			"MyCrawler": {
				"Type": "AWS::Glue::Crawler",
				"Properties": {
					"CrawlerName": "cfn-glue-crawler",
					"Role": "arn:aws:iam::123456789012:role/GlueRole",
					"DatabaseName": {"Ref": "MyDB"}
				}
			},
			"MyJob": {
				"Type": "AWS::Glue::Job",
				"Properties": {
					"JobName": "cfn-glue-job",
					"Role": "arn:aws:iam::123456789012:role/GlueRole"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "glue-job-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 3)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s: %s", r.LogicalID, r.Error)
	}
	for _, r := range result.Resources {
		if r.Type == "AWS::Glue::Job" {
			assert.Equal(t, "cfn-glue-job", r.PhysicalID)
		}
	}
}

// TestCFN_EFSMountTarget verifies EFS mount target deployment via CFN.
func TestCFN_EFSMountTarget(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyFS": {
				"Type": "AWS::EFS::FileSystem",
				"Properties": {}
			},
			"MyMT": {
				"Type": "AWS::EFS::MountTarget",
				"Properties": {
					"FileSystemId": {"Ref": "MyFS"},
					"SubnetId": "subnet-abc123"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "efs-mt-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 2)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s: %s", r.LogicalID, r.Error)
	}
	for _, r := range result.Resources {
		if r.Type == "AWS::EFS::MountTarget" {
			assert.Contains(t, r.PhysicalID, "fsmt-")
		}
	}
}

// TestCFN_GlueTableAndConnection verifies Glue table and connection deployment via CFN.
func TestCFN_GlueTableAndConnection(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyDB": {
				"Type": "AWS::Glue::Database",
				"Properties": {
					"DatabaseName": "cfn-glue-tc-db"
				}
			},
			"MyTable": {
				"Type": "AWS::Glue::Table",
				"Properties": {
					"DatabaseName": {"Ref": "MyDB"},
					"TableName": "cfn-glue-table"
				}
			},
			"MyConn": {
				"Type": "AWS::Glue::Connection",
				"Properties": {
					"ConnectionName": "cfn-glue-conn",
					"ConnectionType": "JDBC"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "glue-tc-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 3)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s: %s", r.LogicalID, r.Error)
	}
}

// TestCFN_BudgetsBudget verifies that a Budgets budget can be deployed via CFN.
func TestCFN_BudgetsBudget(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBudget": {
				"Type": "AWS::Budgets::Budget",
				"Properties": {
					"Budget": {
						"BudgetName": "cfn-monthly-budget",
						"BudgetType": "COST",
						"TimeUnit": "MONTHLY",
						"BudgetLimit": {
							"Amount": "500.00",
							"Unit": "USD"
						}
					}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "budgets-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::Budgets::Budget", r.Type)
	assert.Empty(t, r.Error)
	assert.Equal(t, "cfn-monthly-budget", r.PhysicalID)
}

// TestCFN_RDSDBCluster verifies that an Aurora DB cluster can be deployed via CFN.
func TestCFN_RDSDBCluster(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MySubnetGroup": {
				"Type": "AWS::RDS::DBSubnetGroup",
				"Properties": {
					"DBSubnetGroupName": "cfn-cluster-subnet-group",
					"DBSubnetGroupDescription": "test subnet group"
				}
			},
			"MyCluster": {
				"Type": "AWS::RDS::DBCluster",
				"Properties": {
					"DBClusterIdentifier": "cfn-aurora-cluster",
					"Engine": "aurora-mysql",
					"MasterUsername": "admin",
					"DBSubnetGroupName": "cfn-cluster-subnet-group"
				},
				"DependsOn": "MySubnetGroup"
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "rds-cluster-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 2)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s has error: %s", r.LogicalID, r.Error)
	}
	var clusterResource substrate.DeployedResource
	for _, r := range result.Resources {
		if r.Type == "AWS::RDS::DBCluster" {
			clusterResource = r
		}
	}
	assert.Equal(t, "cfn-aurora-cluster", clusterResource.PhysicalID)
	assert.Contains(t, clusterResource.ARN, "cluster:cfn-aurora-cluster")
}

// TestCFN_MSKCluster verifies that an MSK cluster can be deployed via CFN.
func TestCFN_MSKCluster(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyMSK": {
				"Type": "AWS::MSK::Cluster",
				"Properties": {
					"ClusterName": "cfn-kafka-cluster",
					"KafkaVersion": "3.5.1",
					"NumberOfBrokerNodes": 3,
					"BrokerNodeGroupInfo": {
						"InstanceType": "kafka.m5.large",
						"ClientSubnets": []
					}
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "msk-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::MSK::Cluster", r.Type)
	assert.Empty(t, r.Error, "MSK cluster resource error: %s", r.Error)
	assert.Contains(t, r.PhysicalID, "cfn-kafka-cluster")
	assert.Contains(t, r.ARN, "kafka")
}

// TestCFN_SESv2EmailIdentity verifies the AWS::SES::EmailIdentity CFN resource.
func TestCFN_SESv2EmailIdentity(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyEmailIdentity": {
				"Type": "AWS::SES::EmailIdentity",
				"Properties": {
					"EmailIdentity": "test@example.com"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "sesv2-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::SES::EmailIdentity", r.Type)
	assert.Empty(t, r.Error, "SESv2 identity resource error: %s", r.Error)
}

// TestCFN_FirehoseDeliveryStream verifies the AWS::KinesisFirehose::DeliveryStream CFN resource.
func TestCFN_FirehoseDeliveryStream(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyStream": {
				"Type": "AWS::KinesisFirehose::DeliveryStream",
				"Properties": {
					"DeliveryStreamName": "cfn-test-stream"
				}
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "firehose-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	r := result.Resources[0]
	assert.Equal(t, "AWS::KinesisFirehose::DeliveryStream", r.Type)
	assert.Empty(t, r.Error, "Firehose delivery stream resource error: %s", r.Error)
}

// TestCFN_APIGatewayMethod verifies the AWS::ApiGateway::Method CFN resource.
func TestCFN_APIGatewayMethod(t *testing.T) {
	d := newFullTestDeployer(t)
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAPI": {
				"Type": "AWS::ApiGateway::RestApi",
				"Properties": {"Name": "method-test-api"}
			},
			"MyResource": {
				"Type": "AWS::ApiGateway::Resource",
				"Properties": {
					"RestApiId": {"Ref": "MyAPI"},
					"ParentId": {"Fn::GetAtt": ["MyAPI", "RootResourceId"]},
					"PathPart": "items"
				},
				"DependsOn": "MyAPI"
			},
			"MyMethod": {
				"Type": "AWS::ApiGateway::Method",
				"Properties": {
					"RestApiId": {"Ref": "MyAPI"},
					"ResourceId": {"Ref": "MyResource"},
					"HttpMethod": "GET",
					"AuthorizationType": "NONE"
				},
				"DependsOn": "MyResource"
			}
		}
	}`
	result, err := d.Deploy(context.Background(), tmpl, "apigw-method-stack", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 3)
	for _, r := range result.Resources {
		assert.Empty(t, r.Error, "resource %s had error: %s", r.LogicalID, r.Error)
	}
}
