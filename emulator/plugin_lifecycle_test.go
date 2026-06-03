package emulator_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestPluginShutdown_NoOps verifies that all no-op plugin Shutdown methods
// return nil without panicking.  This brings the no-op one-liner Shutdown
// implementations into coverage.
func TestPluginShutdown_NoOps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Now())
	cfg := emulator.PluginConfig{State: state, Logger: logger}

	plugins := []emulator.Plugin{
		&emulator.ACMPlugin{},
		&emulator.APIGatewayPlugin{},
		&emulator.APIGatewayV2Plugin{},
		&emulator.AppSyncPlugin{},
		&emulator.BudgetsPlugin{},
		&emulator.CloudFrontPlugin{},
		&emulator.CognitoIdentityPlugin{},
		&emulator.CognitoIDPPlugin{},
		&emulator.EC2Plugin{},
		&emulator.ECRPlugin{},
		&emulator.ECSPlugin{},
		&emulator.EFSPlugin{},
		&emulator.ElastiCachePlugin{},
		&emulator.ELBPlugin{},
		&emulator.FirehosePlugin{},
		&emulator.GluePlugin{},
		&emulator.HealthPlugin{},
		&emulator.KinesisPlugin{},
		&emulator.KMSPlugin{},
		&emulator.MSKPlugin{},
		&emulator.OrganizationsPlugin{},
		&emulator.RDSPlugin{},
		&emulator.Route53Plugin{},
		&emulator.S3Plugin{},
		&emulator.SecretsManagerPlugin{},
		&emulator.ServiceQuotasPlugin{},
		&emulator.SESv2Plugin{},
		&emulator.SNSPlugin{},
		&emulator.SQSPlugin{},
		&emulator.SSMPlugin{},
		&emulator.StepFunctionsPlugin{},
		&emulator.TaggingPlugin{},
		&emulator.CEPlugin{},
		&emulator.APIGatewayProxyPlugin{},
	}

	for _, p := range plugins {
		p := p
		t.Run(p.Name(), func(t *testing.T) {
			t.Parallel()
			pluginCfg := cfg
			if p.Name() == "ec2" || p.Name() == "elb" {
				pluginCfg = emulator.PluginConfig{
					State:   state,
					Logger:  logger,
					Options: map[string]any{"time_controller": tc},
				}
			}
			if err := p.Initialize(ctx, pluginCfg); err != nil {
				// Some plugins may fail if dependencies are missing, but
				// Shutdown should still be safe to call.
				t.Logf("Initialize returned: %v — still calling Shutdown", err)
			}
			if err := p.Shutdown(ctx); err != nil {
				t.Errorf("%s.Shutdown() = %v; want nil", p.Name(), err)
			}
		})
	}
}
