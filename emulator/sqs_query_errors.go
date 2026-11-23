package emulator

// SQS's awsQueryCompatible error codes (#1519).
//
// SQS answers JSON, and its model carries aws.protocols#awsQueryCompatible: a client that sends
// X-Amzn-Query-Mode: true is owed the error's Query-protocol code in an x-amzn-query-error
// header, beside the modeled shape name in "__type". For most SQS errors the two spellings
// differ. [marshalAWSError] sent the shape name in the header as well, so a caller matching on the
// Query code never matched. The Terraform AWS provider is one such caller: it waits for a deleted
// queue by polling GetQueueAttributes until the error code is
// "AWS.SimpleQueueService.NonExistentQueue", and it gave up on every `terraform destroy` of a
// queue.
//
// Provenance: each entry is the shape's aws.protocols#awsQueryError code in the AWS-published
// Smithy model for SQS, as aws-sdk-go-v2 vendors it (service/sqs/schemas). Shapes whose Query code
// equals their name are left out, because the fallback already answers the name.

// sqsQueryErrorCodes maps an SQS error shape name, the code substrate's SQS plugin raises, to the
// Query-protocol code its model declares.
var sqsQueryErrorCodes = map[string]string{
	"BatchEntryIdsNotDistinct":     "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
	"BatchRequestTooLong":          "AWS.SimpleQueueService.BatchRequestTooLong",
	"EmptyBatchRequest":            "AWS.SimpleQueueService.EmptyBatchRequest",
	"InvalidBatchEntryId":          "AWS.SimpleQueueService.InvalidBatchEntryId",
	"MessageNotInflight":           "AWS.SimpleQueueService.MessageNotInflight",
	"PurgeQueueInProgress":         "AWS.SimpleQueueService.PurgeQueueInProgress",
	"QueueDeletedRecently":         "AWS.SimpleQueueService.QueueDeletedRecently",
	"QueueDoesNotExist":            "AWS.SimpleQueueService.NonExistentQueue",
	"QueueNameExists":              "QueueAlreadyExists",
	"TooManyEntriesInBatchRequest": "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
	"UnsupportedOperation":         "AWS.SimpleQueueService.UnsupportedOperation",
}

// queryErrorCodeFor returns the Query-protocol code an x-amzn-query-error header carries for an
// error service raised with code: the model's awsQueryError code where one differs, and code
// itself otherwise.
func queryErrorCodeFor(service, code string) string {
	if service == "sqs" {
		if q, ok := sqsQueryErrorCodes[code]; ok {
			return q
		}
	}
	return code
}
