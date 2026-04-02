package substrate

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// CostConfig holds configuration for the cost controller.
type CostConfig struct {
	// Enabled gates cost recording. When false, CostForRequest always returns
	// 0.0.
	Enabled bool

	// Overrides maps "service/operation" or "service" keys to USD per request,
	// replacing the built-in pricing table entries.
	Overrides map[string]float64
}

// CostController computes per-request estimated AWS cost in USD. It is
// stateless after initialisation: CostForRequest is a pure lookup with no
// side effects, making it fully replay-safe.
type CostController struct {
	mu    sync.RWMutex
	table map[string]float64
}

// NewCostController creates a CostController from cfg. When cfg.Enabled is
// false, all lookups return 0.0. Overrides in cfg take precedence over the
// built-in pricing table.
func NewCostController(cfg CostConfig) *CostController {
	if !cfg.Enabled {
		return &CostController{table: make(map[string]float64)}
	}

	table := defaultCostTable()
	for k, v := range cfg.Overrides {
		table[k] = v
	}

	return &CostController{table: table}
}

// CostForRequest returns the estimated USD cost for the given request. It
// first checks for an operation-specific key ("service/operation"), then falls
// back to a service-level key ("service"), and finally returns 0.0 when no
// entry matches.
func (c *CostController) CostForRequest(req *AWSRequest) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	opKey := strings.ToLower(req.Service) + "/" + req.Operation
	if cost, ok := c.table[opKey]; ok {
		return cost
	}
	svcKey := strings.ToLower(req.Service)
	if cost, ok := c.table[svcKey]; ok {
		return cost
	}
	return 0.0
}

// UpdateConfig rebuilds the pricing table from cfg. It is safe to call
// concurrently with CostForRequest.
func (c *CostController) UpdateConfig(cfg CostConfig) {
	var table map[string]float64
	if cfg.Enabled {
		table = defaultCostTable()
		for k, v := range cfg.Overrides {
			table[k] = v
		}
	} else {
		table = make(map[string]float64)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.table = table
}

// defaultCostTable returns the built-in per-request pricing table. IAM and
// STS are free. S3, DynamoDB, and Lambda values are pre-populated so those
// services get cost tracking as soon as their plugins land.
func defaultCostTable() map[string]float64 {
	return map[string]float64{
		"iam":                     0.0,
		"sts":                     0.0,
		"s3/PutObject":            0.000005,
		"s3/GetObject":            0.0000004,
		"dynamodb/GetItem":        0.00000025,
		"dynamodb/PutItem":        0.00000125,
		"dynamodb/UpdateItem":     0.00000125,
		"dynamodb/DeleteItem":     0.00000125,
		"dynamodb/BatchWriteItem": 0.00000125,
		"dynamodb/Query":          0.00000025,
		"dynamodb/Scan":           0.00000025,
		"dynamodb/BatchGetItem":   0.00000025,
		"lambda/Invoke":           0.0000002,
		"sqs/SendMessage":         0.00000040,
		"sqs/ReceiveMessage":      0.00000040,
		"elasticloadbalancing/CreateLoadBalancer": 0.000008,
		"elasticloadbalancing/RegisterTargets":    0.000001,
		"route53/CreateHostedZone":                0.50,
		"route53/ChangeResourceRecordSets":        0.000004,
		// SNS pricing: $0.50 per 1M notifications published.
		"sns/Publish": 0.0000005,
		// Secrets Manager: $0.05 per secret per month (prorated per-API call at ~$0.00000166).
		"secretsmanager/CreateSecret":   0.00000166,
		"secretsmanager/GetSecretValue": 0.00000166,
		"secretsmanager/PutSecretValue": 0.00000166,
		// SSM Parameter Store: standard parameters are free; advanced are $0.05/10k API calls.
		"ssm/GetParameter":        0.000000005,
		"ssm/GetParameters":       0.000000005,
		"ssm/GetParametersByPath": 0.000000005,
		"ssm/PutParameter":        0.000000005,
		// KMS: $0.03 per 10k API calls ≈ $0.000003 per call.
		"kms/CreateKey":       0.000003,
		"kms/Encrypt":         0.000003,
		"kms/Decrypt":         0.000003,
		"kms/GenerateDataKey": 0.000003,
		"kms/ReEncrypt":       0.000003,
		// CloudWatch Logs: ~$0.50/GB ingestion, approximate per-call estimate.
		"logs/PutLogEvents": 0.0000005,
		// EventBridge: $1.00 per million events.
		"eventbridge/PutEvents": 0.000001,
		// CloudWatch Alarms: $0.10 per alarm per month, charged on creation.
		"monitoring/PutMetricAlarm": 0.10,
		// API Gateway REST APIs: ~$3.50/M API calls.
		"apigateway/CreateDeployment": 0.0000035,
		// API Gateway HTTP APIs: cheaper than REST.
		"apigatewayv2/CreateApi": 0.000001,
		// ACM: public certificates are free.
		"acm/RequestCertificate": 0.0,
		// Step Functions: $0.025 per 1k state transitions (Standard Workflows).
		"states/StartExecution": 0.000025,
		// ECS Fargate: stub per-task charge.
		"ecs/RunTask": 0.000025,
		// ECR image storage stub.
		"ecr/PutImage": 0.000001,
		// Cognito: ~$0.0055/MAU, approximated per auth call.
		"cognito-idp/InitiateAuth": 0.000055,
		// Kinesis: $0.014/1M records.
		"kinesis/PutRecord":  0.000000014,
		"kinesis/PutRecords": 0.000000014,
		// RDS: per-instance-hour prorated per API call.
		"rds/CreateDBInstance": 0.0001,
		"rds/CreateDBSnapshot": 0.00002,
		"rds/ModifyDBInstance": 0.0001,
		// ElastiCache: per-node-hour prorated per API call.
		"elasticache/CreateCacheCluster":     0.0001,
		"elasticache/CreateReplicationGroup": 0.0001,
		// EFS: per-GB-month prorated per API call.
		"elasticfilesystem/CreateFileSystem":  0.00003,
		"elasticfilesystem/CreateAccessPoint": 0.00001,
		"elasticfilesystem/CreateMountTarget": 0.00001,
		// Glue: per-DPU-hour prorated per API call.
		"glue/CreateDatabase": 0.00002,
		"glue/CreateJob":      0.0001,
		"glue/StartJobRun":    0.0001,
		"glue/CreateCrawler":  0.0001,
		// Budgets: $0.02 per budget per month, approximated per-API call.
		"budgets/CreateBudget": 0.00001,
		// SES v2: $0.10 per 1,000 emails sent.
		"sesv2/SendEmail": 0.0000001,
		// Firehose: $0.029 per GB ingested, approximated per record.
		"firehose/PutRecord":      0.000000029,
		"firehose/PutRecordBatch": 0.000000029,
		// AppSync: $4.00 per million query/mutation operations.
		"appsync/ExecuteGraphQL":   0.000004,
		"appsync/CreateGraphqlApi": 0.000004,
		// RDS Aurora: per-cluster-hour prorated per API call.
		"rds/CreateDBCluster":                 0.0001,
		"rds/RestoreDBInstanceFromDBSnapshot": 0.0001,
		// MSK: per-broker-hour prorated per API call.
		"msk/CreateCluster":       0.0002,
		"msk/GetBootstrapBrokers": 0.000001,
		// EC2 Elastic IPs: $0.005/hr when unattached.
		"ec2/AllocateAddress": 0.005,
		// EC2 NAT Gateways: $0.045/hr base charge.
		"ec2/CreateNatGateway": 0.045,
		// EventBridge Scheduler: $0.10 per million invocations, approx per-schedule-create.
		"scheduler/CreateSchedule": 0.0000001,
		// FSx: per file system per hour, prorated per API call.
		"fsx/CreateFileSystem": 0.00013,
		// Batch: per job submitted.
		"batch/SubmitJob":    0.00001,
		"batch/DescribeJobs": 0.0,
		// SageMaker: per training job created; per Studio app created.
		"sagemaker/CreateTrainingJob": 0.001,
		"sagemaker/CreateApp":         0.0001,
		// EMR Serverless: per job run submitted; per application created.
		"emrserverless/StartJobRun":       0.0001,
		"emrserverless/CreateApplication": 0.00001,
		// HealthOmics: per workflow run started.
		"omics/StartRun": 0.001,
		// QuickSight: per data source and dataset created.
		"quicksight/CreateDataSource": 0.000025,
		"quicksight/CreateDataSet":    0.000025,
		// Bedrock Runtime: per guardrail evaluation unit.
		"bedrock-runtime/ApplyGuardrail": 0.000075,
		// Athena: per query execution ($5/TB scanned, stub approximation).
		"athena/StartQueryExecution": 0.000005,
		// S3 Select: per query request.
		"s3/SelectObjectContent": 0.0000004,
		// OpenSearch: per document indexed.
		"opensearch/IndexDocument": 0.0000001,
		"opensearch/Bulk":          0.0000001,
		// WAFv2: $5.00 per Web ACL per month, approximated per creation call.
		"wafv2/CreateWebACL":    5.00,
		"wafv2/AssociateWebACL": 0.000001,
		// CloudTrail: $0.10 per 100,000 management events, approximated per trail creation.
		"cloudtrail/CreateTrail": 0.000002,
		// CodeBuild: $0.005 per build minute (BUILD_GENERAL1_SMALL), approximated per build.
		"codebuild/StartBuild": 0.0001,
		// CodePipeline: $1/month per active pipeline, approximated per execution.
		"codepipeline/StartPipelineExecution": 0.000001,
		// CodeDeploy: free for EC2/on-premises, approximated per deployment.
		"codedeploy/CreateDeployment": 0.000001,
		// AWS Backup: pricing based on protected storage; approximated per plan.
		"backup/CreateBackupPlan": 0.000001,
		// Transfer Family: $0.30/protocol/hour, approximated per server creation.
		"transfer/CreateServer": 0.30,
		// Redshift: $0.25/node/hour for dc2.large, approximated per cluster creation.
		"redshift/CreateCluster": 0.0002,
		// Redshift snapshot storage approximated per snapshot.
		"redshift/CreateClusterSnapshot": 0.00002,
	}
}

// CostSummary holds aggregated cost information for a set of events.
type CostSummary struct {
	// AccountID is the AWS account this summary covers.
	AccountID string

	// TotalCost is the sum of all event costs in USD.
	TotalCost float64

	// ByService maps service name to total cost in USD.
	ByService map[string]float64

	// ByOperation maps "service/operation" to total cost in USD.
	ByOperation map[string]float64

	// RequestCount is the number of matching events.
	RequestCount int64

	// StartTime is the earliest event timestamp included (zero if unbounded).
	StartTime time.Time

	// EndTime is the latest event timestamp included (zero if unbounded).
	EndTime time.Time
}

// GetCostSummary returns aggregated cost data for accountID in the half-open
// interval [start, end). Zero values for start and end are treated as
// unbounded. Events are fetched from the store using [EventStore.GetEvents].
func (e *EventStore) GetCostSummary(ctx context.Context, accountID string, start, end time.Time) (*CostSummary, error) {
	filter := EventFilter{
		AccountID: accountID,
		StartTime: start,
		EndTime:   end,
	}

	events, err := e.GetEvents(ctx, filter)
	if err != nil {
		return nil, err
	}

	summary := &CostSummary{
		AccountID:   accountID,
		ByService:   make(map[string]float64),
		ByOperation: make(map[string]float64),
		StartTime:   start,
		EndTime:     end,
	}

	for _, ev := range events {
		summary.TotalCost += ev.Cost
		summary.RequestCount++
		summary.ByService[ev.Service] += ev.Cost
		opKey := ev.Service + "/" + ev.Operation
		summary.ByOperation[opKey] += ev.Cost
	}

	return summary, nil
}

// CostForecast holds a projected cost estimate for a given account and service
// over a future horizon, computed via linear regression on historical daily costs.
type CostForecast struct {
	// AccountID is the AWS account this forecast covers (empty = all accounts).
	AccountID string

	// Service is the AWS service name (empty = all services).
	Service string

	// WindowDays is the number of historical days used for the regression.
	WindowDays int

	// HorizonDays is the number of days projected forward.
	HorizonDays int

	// ProjectedCost is the estimated total cost over HorizonDays in USD.
	ProjectedCost float64

	// ConfidenceLow is the lower bound of the 95% confidence interval.
	ConfidenceLow float64

	// ConfidenceHigh is the upper bound of the 95% confidence interval.
	ConfidenceHigh float64

	// DailyCosts is the historical per-day cost series used for forecasting.
	DailyCosts []DailyCost

	// Anomalies lists services where the latest day cost is statistically unusual.
	Anomalies []CostAnomaly

	// ComputedAt is the time this forecast was generated.
	ComputedAt time.Time
}

// DailyCost records the total estimated cost for a single UTC calendar day.
type DailyCost struct {
	// Date is the start of the UTC day (truncated to midnight).
	Date time.Time

	// Cost is the total estimated cost in USD for that day.
	Cost float64
}

// CostAnomaly describes a service whose latest-day cost deviates significantly
// from its rolling mean.
type CostAnomaly struct {
	// Service is the AWS service name.
	Service string

	// LatestDayCost is the cost recorded for the most recent day.
	LatestDayCost float64

	// MeanCost is the rolling mean cost per day.
	MeanCost float64

	// SigmaCount is the number of standard deviations above the mean.
	SigmaCount float64
}

// GetCostForecast returns a projected cost estimate for the given accountID and
// service over the next horizonDays days, based on windowDays of history.
// An empty accountID or service matches all accounts/services respectively.
// anomalyThresholdSigma is the number of standard deviations above which a
// service cost is flagged as anomalous; use 2.0 for a typical 95% threshold.
func (e *EventStore) GetCostForecast(
	ctx context.Context,
	accountID, service string,
	windowDays, horizonDays int,
	anomalyThresholdSigma float64,
) (*CostForecast, error) {
	if windowDays <= 0 {
		windowDays = 30
	}
	if horizonDays <= 0 {
		horizonDays = 7
	}
	if anomalyThresholdSigma <= 0 {
		anomalyThresholdSigma = 2.0
	}

	end := e.now().UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -windowDays)

	filter := EventFilter{
		AccountID: accountID,
		StartTime: start,
		EndTime:   end,
	}
	if service != "" {
		filter.Service = service
	}

	events, err := e.GetEvents(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("get events for forecast: %w", err)
	}

	daily := bucketByDay(events)
	projected, low, high := forecastLinear(daily, horizonDays)
	anomalies := detectAnomalies(events, anomalyThresholdSigma)

	return &CostForecast{
		AccountID:      accountID,
		Service:        service,
		WindowDays:     windowDays,
		HorizonDays:    horizonDays,
		ProjectedCost:  projected,
		ConfidenceLow:  low,
		ConfidenceHigh: high,
		DailyCosts:     daily,
		Anomalies:      anomalies,
		ComputedAt:     e.now().UTC(),
	}, nil
}

// bucketByDay aggregates event costs into per-UTC-day totals.
func bucketByDay(events []*Event) []DailyCost {
	totals := make(map[time.Time]float64)
	for _, ev := range events {
		day := ev.Timestamp.UTC().Truncate(24 * time.Hour)
		totals[day] += ev.Cost
	}
	result := make([]DailyCost, 0, len(totals))
	for day, cost := range totals {
		result = append(result, DailyCost{Date: day, Cost: cost})
	}
	// Sort ascending by date using insertion sort.
	for i := 1; i < len(result); i++ {
		for j := i; j > 0 && result[j].Date.Before(result[j-1].Date); j-- {
			result[j], result[j-1] = result[j-1], result[j]
		}
	}
	return result
}

// forecastLinear projects total cost over horizonDays using linear regression
// on the daily series. When fewer than 3 data points are available, it falls
// back to multiplying the mean daily cost by horizonDays.
// Returns (projected, confidenceLow, confidenceHigh).
func forecastLinear(daily []DailyCost, horizonDays int) (float64, float64, float64) {
	n := len(daily)
	if n == 0 {
		return 0, 0, 0
	}

	ys := make([]float64, n)
	for i, d := range daily {
		ys[i] = d.Cost
	}

	if n < 3 {
		// Insufficient data: use mean.
		mean := meanFloat(ys)
		projected := mean * float64(horizonDays)
		return projected, projected, projected
	}

	xs := make([]float64, n)
	for i := range xs {
		xs[i] = float64(i)
	}

	slope, intercept := linearRegression(xs, ys)

	// Project: sum predicted values for next horizonDays.
	var projected float64
	for h := 0; h < horizonDays; h++ {
		x := float64(n + h)
		v := slope*x + intercept
		if v < 0 {
			v = 0
		}
		projected += v
	}

	// Residual standard deviation.
	var residSumSq float64
	for i, y := range ys {
		pred := slope*xs[i] + intercept
		diff := y - pred
		residSumSq += diff * diff
	}
	sigma := 0.0
	if n > 2 {
		sigma = math.Sqrt(residSumSq / float64(n-2))
	}

	// 95% CI: ±1.96σ scaled over horizonDays.
	ci := 1.96 * sigma * math.Sqrt(float64(horizonDays))
	return projected, projected - ci, projected + ci
}

// linearRegression computes the least-squares slope and intercept for
// paired (xs[i], ys[i]) observations.
func linearRegression(xs, ys []float64) (slope, intercept float64) {
	n := float64(len(xs))
	if n == 0 {
		return 0, 0
	}
	var sumX, sumY, sumXY, sumXX float64
	for i := range xs {
		sumX += xs[i]
		sumY += ys[i]
		sumXY += xs[i] * ys[i]
		sumXX += xs[i] * xs[i]
	}
	denom := n*sumXX - sumX*sumX
	if denom == 0 {
		return 0, sumY / n
	}
	slope = (n*sumXY - sumX*sumY) / denom
	intercept = (sumY - slope*sumX) / n
	return slope, intercept
}

// detectAnomalies flags services where the latest 24-hour cost exceeds
// thresholdSigma standard deviations above the daily mean.
func detectAnomalies(events []*Event, thresholdSigma float64) []CostAnomaly {
	bySvc := make(map[string]map[time.Time]float64)
	for _, ev := range events {
		if _, ok := bySvc[ev.Service]; !ok {
			bySvc[ev.Service] = make(map[time.Time]float64)
		}
		day := ev.Timestamp.UTC().Truncate(24 * time.Hour)
		bySvc[ev.Service][day] += ev.Cost
	}

	var anomalies []CostAnomaly
	for svc, days := range bySvc {
		if len(days) < 2 {
			continue
		}
		costs := make([]float64, 0, len(days))
		var latestDay time.Time
		var latestCost float64
		for day, cost := range days {
			costs = append(costs, cost)
			if day.After(latestDay) {
				latestDay = day
				latestCost = cost
			}
		}
		mean := meanFloat(costs)
		sigma := stddevFloat(costs, mean)
		if sigma == 0 {
			continue
		}
		sigmaCount := (latestCost - mean) / sigma
		if sigmaCount > thresholdSigma {
			anomalies = append(anomalies, CostAnomaly{
				Service:       svc,
				LatestDayCost: latestCost,
				MeanCost:      mean,
				SigmaCount:    sigmaCount,
			})
		}
	}
	return anomalies
}

// meanFloat returns the arithmetic mean of vals. Returns 0 for empty slice.
func meanFloat(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

// stddevFloat returns the sample standard deviation of vals.
func stddevFloat(vals []float64, mean float64) float64 {
	if len(vals) < 2 {
		return 0
	}
	var sumSq float64
	for _, v := range vals {
		diff := v - mean
		sumSq += diff * diff
	}
	return math.Sqrt(sumSq / float64(len(vals)-1))
}
