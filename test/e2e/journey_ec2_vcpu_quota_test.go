package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/servicequotas"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_EC2SpotVCPUHeadroom is #1498 at the SDK level: the vCPU preflight a Spot
// consumer runs before launching a fleet — read the Spot quota, read its usage metric, and
// compare — through the real Service Quotas, EC2 and CloudWatch clients.
//
// What only this level catches is routing and the wire. ListAWSDefaultServiceQuotas is new,
// so its X-Amz-Target reaching the plugin is unproven until a real client sends it; and
// aws-sdk-go-v2's CloudWatch client speaks RPC v2 CBOR, so the usage datapoint's timestamp
// has to survive as a CBOR tag-1 time rather than as the epoch number the JSON path sends.
func TestJourney_EC2SpotVCPUHeadroom(t *testing.T) {
	ts := emulator.StartTestServer(t)
	now := time.Date(2026, 3, 1, 0, 7, 30, 0, time.UTC)
	ts.FreezeTimeAt(now)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	ctx := context.Background()
	quotas := servicequotas.NewFromConfig(cfg, func(o *servicequotas.Options) { o.RetryMaxAttempts = 1 })
	compute := ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.RetryMaxAttempts = 1 })
	metrics := cloudwatch.NewFromConfig(cfg, func(o *cloudwatch.Options) { o.RetryMaxAttempts = 1 })

	// --- the quota, as the preflight reads it ---
	quota, err := quotas.GetServiceQuota(ctx, &servicequotas.GetServiceQuotaInput{
		ServiceCode: aws.String("ec2"), QuotaCode: aws.String("L-34B43A08"),
	})
	if err != nil {
		t.Fatalf("GetServiceQuota(ec2, L-34B43A08): %v", err)
	}
	if v := aws.ToFloat64(quota.Quota.Value); v != 5 {
		t.Errorf("the Standard Spot quota reads %v vCPU, want AWS's new-account default of 5", v)
	}
	usageMetric := quota.Quota.UsageMetric
	if usageMetric == nil || aws.ToString(usageMetric.MetricNamespace) != "AWS/Usage" ||
		usageMetric.MetricDimensions["Class"] != "Standard/Spot" {
		t.Fatalf("the quota's UsageMetric is %+v; want AWS/Usage, Class Standard/Spot", usageMetric)
	}

	// --- every default, through the SDK's own paginator ---
	pager := servicequotas.NewListAWSDefaultServiceQuotasPaginator(quotas,
		&servicequotas.ListAWSDefaultServiceQuotasInput{ServiceCode: aws.String("ec2"), MaxResults: aws.Int32(5)})
	defaults := map[string]float64{}
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatalf("ListAWSDefaultServiceQuotas: %v", err)
		}
		for _, q := range page.Quotas {
			defaults[aws.ToString(q.QuotaCode)] = aws.ToFloat64(q.Value)
		}
	}
	if len(defaults) != 12 {
		t.Errorf("ListAWSDefaultServiceQuotas(ec2) reported %d quotas, want 12", len(defaults))
	}
	if defaults["L-1216C47A"] != 5 || defaults["L-34B43A08"] != 5 {
		t.Errorf("Standard defaults are %v On-Demand and %v Spot, want 5 and 5",
			defaults["L-1216C47A"], defaults["L-34B43A08"])
	}

	// --- launch two m5.large Spot instances: 4 vCPU ---
	if _, err := compute.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:      aws.String(journeyImage),
		InstanceType: ec2types.InstanceTypeM5Large,
		MinCount:     aws.Int32(2),
		MaxCount:     aws.Int32(2),
		InstanceMarketOptions: &ec2types.InstanceMarketOptionsRequest{
			MarketType: ec2types.MarketTypeSpot,
		},
	}); err != nil {
		t.Fatalf("RunInstances: %v", err)
	}

	// --- the usage, as the preflight reads it ---
	dims := make([]cwtypes.Dimension, 0, len(usageMetric.MetricDimensions))
	for _, name := range []string{"Service", "Type", "Resource", "Class"} {
		dims = append(dims, cwtypes.Dimension{Name: aws.String(name), Value: aws.String(usageMetric.MetricDimensions[name])})
	}
	data, err := metrics.GetMetricData(ctx, &cloudwatch.GetMetricDataInput{
		StartTime: aws.Time(now.Add(-15 * time.Minute)),
		EndTime:   aws.Time(now),
		MetricDataQueries: []cwtypes.MetricDataQuery{{
			Id: aws.String("spot"),
			MetricStat: &cwtypes.MetricStat{
				Metric: &cwtypes.Metric{
					Namespace:  usageMetric.MetricNamespace,
					MetricName: usageMetric.MetricName,
					Dimensions: dims,
				},
				Period: aws.Int32(300),
				Stat:   usageMetric.MetricStatisticRecommendation,
			},
		}},
	})
	if err != nil {
		t.Fatalf("GetMetricData: %v", err)
	}
	if len(data.MetricDataResults) != 1 {
		t.Fatalf("GetMetricData returned %d results, want 1", len(data.MetricDataResults))
	}
	result := data.MetricDataResults[0]
	if len(result.Values) != 1 || result.Values[0] != 4 {
		t.Fatalf("Standard/Spot usage is %v, want [4] for two m5.large", result.Values)
	}
	if want := time.Date(2026, 3, 1, 0, 7, 0, 0, time.UTC); len(result.Timestamps) != 1 || !result.Timestamps[0].Equal(want) {
		t.Errorf("the datapoint is stamped %v, want %v — the start of the period holding the clock",
			result.Timestamps, want)
	}

	// The preflight's arithmetic: 5 - 4 leaves room for one more vCPU, so a third m5.large
	// would not fit.
	if headroom := aws.ToFloat64(quota.Quota.Value) - result.Values[0]; headroom != 1 {
		t.Errorf("headroom = %v vCPU, want 1", headroom)
	}
}
