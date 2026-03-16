package substrate_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

// TestPluginShutdown_NoOps verifies that all no-op plugin Shutdown methods
// return nil without panicking.  This brings the no-op one-liner Shutdown
// implementations into coverage.
func TestPluginShutdown_NoOps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	cfg := substrate.PluginConfig{State: state, Logger: logger}

	plugins := []substrate.Plugin{
		&substrate.ACMPlugin{},
		&substrate.APIGatewayPlugin{},
		&substrate.APIGatewayV2Plugin{},
		&substrate.AppSyncPlugin{},
		&substrate.BudgetsPlugin{},
		&substrate.CloudFrontPlugin{},
		&substrate.CognitoIdentityPlugin{},
		&substrate.CognitoIDPPlugin{},
		&substrate.EC2Plugin{},
		&substrate.ECRPlugin{},
		&substrate.ECSPlugin{},
		&substrate.EFSPlugin{},
		&substrate.ElastiCachePlugin{},
		&substrate.ELBPlugin{},
		&substrate.FirehosePlugin{},
		&substrate.GluePlugin{},
		&substrate.HealthPlugin{},
		&substrate.KinesisPlugin{},
		&substrate.KMSPlugin{},
		&substrate.MSKPlugin{},
		&substrate.OrganizationsPlugin{},
		&substrate.RDSPlugin{},
		&substrate.Route53Plugin{},
		&substrate.S3Plugin{},
		&substrate.SecretsManagerPlugin{},
		&substrate.ServiceQuotasPlugin{},
		&substrate.SESv2Plugin{},
		&substrate.SNSPlugin{},
		&substrate.SQSPlugin{},
		&substrate.SSMPlugin{},
		&substrate.StepFunctionsPlugin{},
		&substrate.TaggingPlugin{},
		&substrate.CEPlugin{},
		&substrate.APIGatewayProxyPlugin{},
	}

	for _, p := range plugins {
		p := p
		t.Run(p.Name(), func(t *testing.T) {
			t.Parallel()
			pluginCfg := cfg
			if p.Name() == "ec2" || p.Name() == "elb" {
				pluginCfg = substrate.PluginConfig{
					State:  state,
					Logger: logger,
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
