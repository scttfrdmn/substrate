package emulator

// servicequotasNamespace is the state namespace used by ServiceQuotasPlugin.
const servicequotasNamespace = "servicequotas"

// ServiceQuota represents an AWS service quota.
type ServiceQuota struct {
	// ServiceCode is the unique identifier for the AWS service.
	ServiceCode string `json:"ServiceCode"`

	// ServiceName is the human-readable name of the AWS service.
	ServiceName string `json:"ServiceName"`

	// QuotaCode is the unique identifier for the quota within the service.
	QuotaCode string `json:"QuotaCode"`

	// QuotaName is the human-readable name of the quota.
	QuotaName string `json:"QuotaName"`

	// Value is the current quota value.
	Value float64 `json:"Value"`

	// Adjustable indicates whether the quota can be increased via a support request.
	Adjustable bool `json:"Adjustable"`

	// GlobalQuota indicates whether the quota is applied globally across all regions.
	GlobalQuota bool `json:"GlobalQuota"`

	// Unit is the unit of measurement for the quota (e.g., "None", "Requests").
	Unit string `json:"Unit"`
}

// QuotaIncrease represents a requested service quota increase.
type QuotaIncrease struct {
	// ID is the unique identifier for the increase request.
	ID string `json:"Id"`

	// ServiceCode is the service for which the increase was requested.
	ServiceCode string `json:"ServiceCode"`

	// QuotaCode is the quota for which the increase was requested.
	QuotaCode string `json:"QuotaCode"`

	// DesiredValue is the requested quota value.
	DesiredValue float64 `json:"DesiredValue"`

	// Status is the current status of the request (PENDING, CASE_OPENED, APPROVED, DENIED, CLOSED).
	Status string `json:"Status"`

	// Created is the Unix epoch (seconds) when the request was created.
	// Service Quotas uses smithy timestamp encoding (float64) not ISO-8601.
	Created float64 `json:"Created"`
}

// defaultServiceQuotas is the built-in quota table for common AWS services.
// These reflect representative default quota values from the AWS documentation.
var defaultServiceQuotas = map[string][]ServiceQuota{
	"lambda": {
		{
			ServiceCode: "lambda",
			ServiceName: "AWS Lambda",
			QuotaCode:   "L-B99A9384",
			QuotaName:   "Concurrent executions",
			Value:       1000,
			Adjustable:  true,
			Unit:        "None",
		},
		{
			ServiceCode: "lambda",
			ServiceName: "AWS Lambda",
			QuotaCode:   "L-548AE339",
			QuotaName:   "Function and layer storage",
			Value:       75,
			Adjustable:  true,
			Unit:        "GB",
		},
		{
			ServiceCode: "lambda",
			ServiceName: "AWS Lambda",
			QuotaCode:   "L-9FEE3D26",
			QuotaName:   "Function count",
			Value:       1000,
			Adjustable:  true,
			Unit:        "None",
		},
	},
	"s3": {
		{
			ServiceCode: "s3",
			ServiceName: "Amazon S3",
			QuotaCode:   "L-DC2B2D3D",
			QuotaName:   "Buckets",
			Value:       100,
			Adjustable:  true,
			Unit:        "None",
		},
	},
	"dynamodb": {
		{
			ServiceCode: "dynamodb",
			ServiceName: "Amazon DynamoDB",
			QuotaCode:   "L-F98FE922",
			QuotaName:   "Table count",
			Value:       2500,
			Adjustable:  true,
			Unit:        "None",
		},
		{
			ServiceCode: "dynamodb",
			ServiceName: "Amazon DynamoDB",
			QuotaCode:   "L-1235C3D1",
			QuotaName:   "Read capacity units (RCU) per table",
			Value:       40000,
			Adjustable:  true,
			Unit:        "None",
		},
	},
	"sqs": {
		{
			ServiceCode: "sqs",
			ServiceName: "Amazon Simple Queue Service",
			QuotaCode:   "L-B80E4348",
			QuotaName:   "Messages In Flight per queue",
			Value:       120000,
			Adjustable:  false,
			Unit:        "None",
		},
		{
			ServiceCode: "sqs",
			ServiceName: "Amazon Simple Queue Service",
			QuotaCode:   "L-E7BB4F14",
			QuotaName:   "Queues",
			Value:       10000,
			Adjustable:  true,
			Unit:        "None",
		},
	},
	"ec2": {
		{
			ServiceCode: "ec2",
			ServiceName: "Amazon Elastic Compute Cloud",
			QuotaCode:   "L-1216C47A",
			QuotaName:   "Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances",
			Value:       32,
			Adjustable:  true,
			Unit:        "vCPU",
		},
	},
	"iam": {
		{
			ServiceCode: "iam",
			ServiceName: "AWS Identity and Access Management",
			QuotaCode:   "L-FE177D64",
			QuotaName:   "Roles",
			Value:       1000,
			Adjustable:  true,
			Unit:        "None",
		},
	},
	"kinesis": {
		{
			ServiceCode: "kinesis",
			ServiceName: "Amazon Kinesis Data Streams",
			QuotaCode:   "L-14FD7D9A",
			QuotaName:   "Shards per region",
			Value:       200,
			Adjustable:  true,
			Unit:        "None",
		},
	},
	"cloudwatch": {
		{
			ServiceCode: "cloudwatch",
			ServiceName: "Amazon CloudWatch",
			QuotaCode:   "L-1648CFA3",
			QuotaName:   "Alarms",
			Value:       5000,
			Adjustable:  true,
			Unit:        "None",
		},
	},
	"sns": {
		{
			ServiceCode: "sns",
			ServiceName: "Amazon Simple Notification Service",
			QuotaCode:   "L-61103206",
			QuotaName:   "Topics",
			Value:       100000,
			Adjustable:  true,
			Unit:        "None",
		},
	},
	"rds": {
		{
			ServiceCode: "rds",
			ServiceName: "Amazon Relational Database Service",
			QuotaCode:   "L-7B6409FD",
			QuotaName:   "DB instances",
			Value:       40,
			Adjustable:  true,
			Unit:        "None",
		},
	},
	// Organizations quotas. Each value is the one substrate's Organizations plugin
	// actually enforces, so a consumer that reads the ceiling here and a consumer
	// that runs into it get the same number — a quota table disagreeing with the
	// limit it describes is worse than a missing one, because the disagreement is
	// silent.
	//
	// GlobalQuota is true for all three: Organizations is a global service hosted in
	// us-east-1, so its quotas are not per-region.
	"organizations": {
		{
			ServiceCode: "organizations",
			ServiceName: "AWS Organizations",
			// The only one of the three whose code is confirmable from AWS docs. The
			// endpoints-and-quotas page publishes a quota code only for adjustable
			// quotas, and this is the adjustable one.
			QuotaCode:   "L-E619E033",
			QuotaName:   "Maximum number of accounts",
			Value:       10, // Matches orgMaxAccounts.
			Adjustable:  true,
			GlobalQuota: true,
			Unit:        "None",
		},
		{
			ServiceCode: "organizations",
			ServiceName: "AWS Organizations",
			QuotaCode:   "L-29A0C5DF",
			QuotaName:   "Service control policies in an organization",
			Value:       10000, // Matches orgMaxSCPsPerOrg.
			Adjustable:  false,
			GlobalQuota: true,
			Unit:        "None",
		},
		{
			ServiceCode: "organizations",
			ServiceName: "AWS Organizations",
			QuotaCode:   "L-0F0F51F4",
			QuotaName:   "Organizational units in an organization",
			Value:       2000, // Matches orgMaxOUsPerOrg.
			Adjustable:  false,
			GlobalQuota: true,
			Unit:        "None",
		},
	},
}

// defaultServiceList is the list of services returned by ListServices.
//
// It must name exactly the services keyed in defaultServiceQuotas: ListServices
// reads only this table and ListServiceQuotas only the other, so a service present
// in one and absent from the other is discoverable but has no quotas, or has quotas
// nothing can discover. TestServiceQuotas_TheTwoTablesAgree pins the two together.
var defaultServiceList = []map[string]string{
	{"ServiceCode": "lambda", "ServiceName": "AWS Lambda"},
	{"ServiceCode": "s3", "ServiceName": "Amazon S3"},
	{"ServiceCode": "dynamodb", "ServiceName": "Amazon DynamoDB"},
	{"ServiceCode": "sqs", "ServiceName": "Amazon Simple Queue Service"},
	{"ServiceCode": "ec2", "ServiceName": "Amazon Elastic Compute Cloud"},
	{"ServiceCode": "iam", "ServiceName": "AWS Identity and Access Management"},
	{"ServiceCode": "kinesis", "ServiceName": "Amazon Kinesis Data Streams"},
	{"ServiceCode": "cloudwatch", "ServiceName": "Amazon CloudWatch"},
	{"ServiceCode": "sns", "ServiceName": "Amazon Simple Notification Service"},
	{"ServiceCode": "rds", "ServiceName": "Amazon Relational Database Service"},
	{"ServiceCode": "organizations", "ServiceName": "AWS Organizations"},
}
