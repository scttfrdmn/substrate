package substrate

import "time"

// cloudtrailNamespace is the state namespace for AWS CloudTrail resources.
const cloudtrailNamespace = "cloudtrail"

// CloudTrailTrail represents an AWS CloudTrail trail.
type CloudTrailTrail struct {
	// Name is the name of the trail.
	Name string `json:"Name"`
	// S3BucketName is the Amazon S3 bucket for log file delivery.
	S3BucketName string `json:"S3BucketName"`
	// S3KeyPrefix is the optional prefix for the S3 object key.
	S3KeyPrefix string `json:"S3KeyPrefix,omitempty"`
	// IncludeGlobalServiceEvents specifies whether global service events are included.
	IncludeGlobalServiceEvents bool `json:"IncludeGlobalServiceEvents"`
	// IsMultiRegionTrail specifies whether the trail is multi-region.
	IsMultiRegionTrail bool `json:"IsMultiRegionTrail"`
	// EnableLogFileValidation specifies whether log file validation is enabled.
	EnableLogFileValidation bool `json:"EnableLogFileValidation"`
	// CloudWatchLogsLogGroupArn is the ARN of the CloudWatch Logs log group.
	CloudWatchLogsLogGroupArn string `json:"CloudWatchLogsLogGroupArn,omitempty"`
	// CloudWatchLogsRoleArn is the ARN of the IAM role for CloudWatch Logs delivery.
	CloudWatchLogsRoleArn string `json:"CloudWatchLogsRoleArn,omitempty"`
	// KMSKeyID is the ARN or alias of the KMS key used for encryption.
	KMSKeyID string `json:"KMSKeyId,omitempty"`
	// TrailARN is the Amazon Resource Name of the trail.
	TrailARN string `json:"TrailARN"`
	// HomeRegion is the region where the trail was originally created.
	HomeRegion string `json:"HomeRegion"`
	// HasCustomEventSelectors indicates whether the trail has custom event selectors.
	HasCustomEventSelectors bool `json:"HasCustomEventSelectors"`
	// IsLogging indicates whether logging is currently enabled for the trail.
	IsLogging bool `json:"IsLogging"`
	// CreatedAt is the time the trail was created.
	CreatedAt time.Time `json:"CreatedAt"`
	// AccountID is the AWS account that owns this trail.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the trail exists.
	Region string `json:"Region"`
}
