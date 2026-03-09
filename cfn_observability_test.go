package substrate_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// newObsDeployer creates a StackDeployer with CloudWatchLogs, EventBridge, and
// CloudWatch plugins for observability CFN tests.
func newObsDeployer(t *testing.T) *substrate.StackDeployer {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Now())
	costs := substrate.NewCostController(substrate.CostConfig{Enabled: true})

	cwLogsPlugin := &substrate.CloudWatchLogsPlugin{}
	require.NoError(t, cwLogsPlugin.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(cwLogsPlugin)

	ebPlugin := &substrate.EventBridgePlugin{}
	require.NoError(t, ebPlugin.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(ebPlugin)

	cwPlugin := &substrate.CloudWatchPlugin{}
	require.NoError(t, cwPlugin.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(cwPlugin)

	return substrate.NewStackDeployer(registry, store, state, tc, logger, costs)
}

func TestCFN_LogGroupDeployment(t *testing.T) {
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyLogGroup": {
				"Type": "AWS::Logs::LogGroup",
				"Properties": {
					"LogGroupName": "/test/cfn-group",
					"RetentionInDays": 7
				}
			}
		}
	}`

	d := newObsDeployer(t)
	result, err := d.Deploy(context.Background(), tmpl, "test-lg", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "/test/cfn-group", result.Resources[0].PhysicalID)
	assert.Contains(t, result.Resources[0].ARN, "log-group")
}

func TestCFN_LogGroupDefaultName(t *testing.T) {
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"LogGroupResource": {
				"Type": "AWS::Logs::LogGroup",
				"Properties": {}
			}
		}
	}`

	d := newObsDeployer(t)
	result, err := d.Deploy(context.Background(), tmpl, "test-lg-default", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	// Falls back to logicalID when LogGroupName not specified.
	assert.Equal(t, "LogGroupResource", result.Resources[0].PhysicalID)
}

func TestCFN_EventsRuleDeployment(t *testing.T) {
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRule": {
				"Type": "AWS::Events::Rule",
				"Properties": {
					"Name": "my-cfn-rule",
					"EventPattern": "{\"source\":[\"aws.ec2\"]}",
					"State": "ENABLED"
				}
			}
		}
	}`

	d := newObsDeployer(t)
	result, err := d.Deploy(context.Background(), tmpl, "test-rule", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "my-cfn-rule", result.Resources[0].PhysicalID)
	assert.Contains(t, result.Resources[0].ARN, "my-cfn-rule")
}

func TestCFN_EventsRuleSchedule(t *testing.T) {
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"ScheduledRule": {
				"Type": "AWS::Events::Rule",
				"Properties": {
					"Name": "hourly-rule",
					"ScheduleExpression": "rate(1 hour)",
					"State": "ENABLED",
					"Description": "Fires every hour"
				}
			}
		}
	}`

	d := newObsDeployer(t)
	result, err := d.Deploy(context.Background(), tmpl, "test-sched-rule", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "hourly-rule", result.Resources[0].PhysicalID)
}

func TestCFN_CloudWatchAlarmDeployment(t *testing.T) {
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyAlarm": {
				"Type": "AWS::CloudWatch::Alarm",
				"Properties": {
					"AlarmName": "cfn-alarm",
					"AlarmDescription": "CPU too high",
					"MetricName": "CPUUtilization",
					"Namespace": "AWS/EC2",
					"Statistic": "Average",
					"ComparisonOperator": "GreaterThanThreshold",
					"Threshold": "90",
					"EvaluationPeriods": "2",
					"Period": "300"
				}
			}
		}
	}`

	d := newObsDeployer(t)
	result, err := d.Deploy(context.Background(), tmpl, "test-alarm", nil)
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)
	assert.Equal(t, "cfn-alarm", result.Resources[0].PhysicalID)
	assert.Contains(t, result.Resources[0].ARN, "cfn-alarm")
}

func TestCFN_LogStreamDeployment(t *testing.T) {
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyLogGroup": {
				"Type": "AWS::Logs::LogGroup",
				"Properties": {
					"LogGroupName": "/test/with-stream"
				}
			},
			"MyLogStream": {
				"Type": "AWS::Logs::LogStream",
				"Properties": {
					"LogGroupName": "/test/with-stream",
					"LogStreamName": "my-stream"
				}
			}
		}
	}`

	d := newObsDeployer(t)
	result, err := d.Deploy(context.Background(), tmpl, "test-stream", nil)
	require.NoError(t, err)
	assert.Len(t, result.Resources, 2)
}

func TestCFN_ObsResourceRefs(t *testing.T) {
	// Verify that Ref and GetAtt work for observability resources.
	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"LG": {
				"Type": "AWS::Logs::LogGroup",
				"Properties": {"LogGroupName": "/ref/test"}
			}
		},
		"Outputs": {
			"LGName": {"Value": {"Ref": "LG"}},
			"LGArn":  {"Value": {"Fn::GetAtt": ["LG", "Arn"]}}
		}
	}`

	d := newObsDeployer(t)
	result, err := d.Deploy(context.Background(), tmpl, "test-refs", nil)
	require.NoError(t, err)
	assert.Equal(t, "/ref/test", result.Outputs["LGName"])
	assert.Contains(t, result.Outputs["LGArn"], "log-group")
}
