package emulator

// The wire is a different thing from the state, and the two types below exist to keep them apart.
//
// CloudTrailTrail (cloudtrail_types.go) is a persisted state record, and four sites handed it to the
// caller whole: createTrail and updateTrail as the entire body, getTrail under `Trail`, and
// describeTrails in `trailList`. Three of its fields are substrate's own — AccountID, Region and
// CreatedAt — and none is a member of any CloudTrail shape (#756). A fourth, IsLogging, is published
// only by GetTrailStatus: API_Trail has no such member, and neither do CreateTrail's and UpdateTrail's
// responses.
//
// The two writes and the two reads answer different shapes, which is why there are two types. Trail,
// the shape GetTrail and DescribeTrails answer, publishes HomeRegion and HasCustomEventSelectors.
// CreateTrailResponse and UpdateTrailResponse publish neither.
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves the
// next one to be remembered rather than prevented, and it changes the format of every recorded run,
// because MemoryStateManager snapshots those bytes and a replay reads them back.

// cloudtrailTrailOut is one Trail, as GetTrail and DescribeTrails answer it: eleven of API_Trail's
// members, the ones the record models (#1013's rule, #1199's gap).
type cloudtrailTrailOut struct {
	CloudWatchLogsLogGroupArn  string `json:"CloudWatchLogsLogGroupArn,omitempty"`
	CloudWatchLogsRoleArn      string `json:"CloudWatchLogsRoleArn,omitempty"`
	HasCustomEventSelectors    bool   `json:"HasCustomEventSelectors"`
	HomeRegion                 string `json:"HomeRegion"`
	IncludeGlobalServiceEvents bool   `json:"IncludeGlobalServiceEvents"`
	IsMultiRegionTrail         bool   `json:"IsMultiRegionTrail"`
	KMSKeyID                   string `json:"KmsKeyId,omitempty"`
	LogFileValidationEnabled   bool   `json:"LogFileValidationEnabled"`
	Name                       string `json:"Name"`
	S3BucketName               string `json:"S3BucketName"`
	S3KeyPrefix                string `json:"S3KeyPrefix,omitempty"`
	TrailARN                   string `json:"TrailARN"`
}

// cloudtrailTrailToWire projects a persisted trail onto API_Trail.
func cloudtrailTrailToWire(trail CloudTrailTrail) cloudtrailTrailOut {
	return cloudtrailTrailOut{
		CloudWatchLogsLogGroupArn:  trail.CloudWatchLogsLogGroupArn,
		CloudWatchLogsRoleArn:      trail.CloudWatchLogsRoleArn,
		HasCustomEventSelectors:    trail.HasCustomEventSelectors,
		HomeRegion:                 trail.HomeRegion,
		IncludeGlobalServiceEvents: trail.IncludeGlobalServiceEvents,
		IsMultiRegionTrail:         trail.IsMultiRegionTrail,
		KMSKeyID:                   trail.KMSKeyID,
		LogFileValidationEnabled:   trail.LogFileValidationEnabled,
		Name:                       trail.Name,
		S3BucketName:               trail.S3BucketName,
		S3KeyPrefix:                trail.S3KeyPrefix,
		TrailARN:                   trail.TrailARN,
	}
}

// cloudtrailTrailWriteOut is CreateTrail's and UpdateTrail's response body: the nine members those
// responses publish that the record models, without Trail's HomeRegion and HasCustomEventSelectors.
type cloudtrailTrailWriteOut struct {
	CloudWatchLogsLogGroupArn  string `json:"CloudWatchLogsLogGroupArn,omitempty"`
	CloudWatchLogsRoleArn      string `json:"CloudWatchLogsRoleArn,omitempty"`
	IncludeGlobalServiceEvents bool   `json:"IncludeGlobalServiceEvents"`
	IsMultiRegionTrail         bool   `json:"IsMultiRegionTrail"`
	KMSKeyID                   string `json:"KmsKeyId,omitempty"`
	LogFileValidationEnabled   bool   `json:"LogFileValidationEnabled"`
	Name                       string `json:"Name"`
	S3BucketName               string `json:"S3BucketName"`
	S3KeyPrefix                string `json:"S3KeyPrefix,omitempty"`
	TrailARN                   string `json:"TrailARN"`
}

// cloudtrailTrailStatusOut is GetTrailStatus's response.
//
// IsLogging is the persisted flag; until #1157 it was hardcoded true, so StopLogging was unobservable
// through any response. StartLoggingTime and StopLoggingTime are the trail's recorded transitions, as
// epoch seconds (CloudTrail speaks awsJson1_1, where a Timestamp is a number), and each is omitted
// until the trail has made that transition, so a trail that has never logged reports no
// StartLoggingTime.
//
// The page's other members are not answered. Substrate delivers no log, digest, notification or
// CloudWatch Logs record, so LatestDeliveryTime and its siblings have nothing true to report;
// answering the request's own time as LatestDeliveryTime, as substrate did, claimed a delivery that
// never happened. The six string members the page marks "no longer in use" (TimeLoggingStarted,
// LatestDeliveryAttemptTime and the rest) are omitted rather than answered as empty strings.
type cloudtrailTrailStatusOut struct {
	IsLogging        bool         `json:"IsLogging"`
	StartLoggingTime EpochSeconds `json:"StartLoggingTime,omitzero"`
	StopLoggingTime  EpochSeconds `json:"StopLoggingTime,omitzero"`
}

// cloudtrailTrailStatusToWire projects a persisted trail onto GetTrailStatus's response.
func cloudtrailTrailStatusToWire(trail CloudTrailTrail) cloudtrailTrailStatusOut {
	return cloudtrailTrailStatusOut{
		IsLogging:        trail.IsLogging,
		StartLoggingTime: EpochSeconds(trail.StartLoggingTime),
		StopLoggingTime:  EpochSeconds(trail.StopLoggingTime),
	}
}

// cloudtrailTrailWriteToWire projects a persisted trail onto CreateTrail's and UpdateTrail's response.
func cloudtrailTrailWriteToWire(trail CloudTrailTrail) cloudtrailTrailWriteOut {
	return cloudtrailTrailWriteOut{
		CloudWatchLogsLogGroupArn:  trail.CloudWatchLogsLogGroupArn,
		CloudWatchLogsRoleArn:      trail.CloudWatchLogsRoleArn,
		IncludeGlobalServiceEvents: trail.IncludeGlobalServiceEvents,
		IsMultiRegionTrail:         trail.IsMultiRegionTrail,
		KMSKeyID:                   trail.KMSKeyID,
		LogFileValidationEnabled:   trail.LogFileValidationEnabled,
		Name:                       trail.Name,
		S3BucketName:               trail.S3BucketName,
		S3KeyPrefix:                trail.S3KeyPrefix,
		TrailARN:                   trail.TrailARN,
	}
}
