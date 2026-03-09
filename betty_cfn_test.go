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
