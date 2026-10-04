package emulator

import "slices"

// Every published S3 sub-resource substrate does not route, named for the operation it is, so that a
// request for one is refused rather than reinterpreted (#1349).
//
// parseS3Operation tests a fixed list of routed sub-resource keys in each method arm and otherwise
// returns that arm's default: ListObjects, CreateBucket or DeleteBucket for a bucket, and GetObject,
// PutObject or DeleteObject for an object. A default reached by absence turns every sub-resource
// missing from the list into a different, routed operation, and on DELETE that operation is
// destructive: DeleteBucketEncryption, DeleteBucketCors, DeleteBucketWebsite,
// DeleteBucketOwnershipControls and DeleteBucketReplication each deleted the bucket. On PUT,
// PutObjectRetention and PutObjectLegalHold overwrote the object with their XML body. #446 and #656
// fixed one sub-resource each by adding it to the list; the class stayed live.
//
// These tables name the rest. parseS3Operation consults them just before each default, after every
// routed test, so no routed operation changes name. A name found here is one HandleRequest's switch
// does not route, and its default answers NotImplemented/501, which is the code S3 publishes for
// functionality a server does not implement. The name also reaches the pipeline through
// operationResolvers, so authorization, metering and the event log see DeleteBucketEncryption rather
// than s3:DeleteBucket.
//
// Every entry is the operation's own Request Syntax line in the S3 API reference: the method and the
// query key. Keys are tested for presence only, never for a value, for the reason parseS3Operation's
// own comment gives (#656): an SDK sends "?encryption=" where the reference writes "?encryption".

// s3BucketSubresource names one unrouted bucket-level operation. When listName is set, the operation
// is a List…Configurations without an `id` parameter and the named Get…Configuration with one, which
// is how the four id-keyed configuration families are distinguished.
type s3BucketSubresource struct {
	name     string
	listName string
}

// s3UnroutedBucketSubresources maps each HTTP method to the published bucket-level sub-resource keys
// substrate does not route, and the operation each one is.
var s3UnroutedBucketSubresources = map[string]map[string]s3BucketSubresource{
	"GET": {
		"accelerate":            {name: "GetBucketAccelerateConfiguration"},
		"analytics":             {name: "GetBucketAnalyticsConfiguration", listName: "ListBucketAnalyticsConfigurations"},
		"encryption":            {name: "GetBucketEncryption"},
		"intelligent-tiering":   {name: "GetBucketIntelligentTieringConfiguration", listName: "ListBucketIntelligentTieringConfigurations"},
		"inventory":             {name: "GetBucketInventoryConfiguration", listName: "ListBucketInventoryConfigurations"},
		"location":              {name: "GetBucketLocation"},
		"logging":               {name: "GetBucketLogging"},
		"metadataConfiguration": {name: "GetBucketMetadataConfiguration"},
		"metadataTable":         {name: "GetBucketMetadataTableConfiguration"},
		"metrics":               {name: "GetBucketMetricsConfiguration", listName: "ListBucketMetricsConfigurations"},
		"object-lock":           {name: "GetObjectLockConfiguration"},
		"ownershipControls":     {name: "GetBucketOwnershipControls"},
		"policyStatus":          {name: "GetBucketPolicyStatus"},
		"replication":           {name: "GetBucketReplication"},
		"requestPayment":        {name: "GetBucketRequestPayment"},
		"session":               {name: "CreateSession"},
		"website":               {name: "GetBucketWebsite"},
	},
	"PUT": {
		"accelerate":             {name: "PutBucketAccelerateConfiguration"},
		"analytics":              {name: "PutBucketAnalyticsConfiguration"},
		"encryption":             {name: "PutBucketEncryption"},
		"intelligent-tiering":    {name: "PutBucketIntelligentTieringConfiguration"},
		"inventory":              {name: "PutBucketInventoryConfiguration"},
		"logging":                {name: "PutBucketLogging"},
		"metadataInventoryTable": {name: "UpdateBucketMetadataInventoryTableConfiguration"},
		"metadataJournalTable":   {name: "UpdateBucketMetadataJournalTableConfiguration"},
		"metrics":                {name: "PutBucketMetricsConfiguration"},
		"object-lock":            {name: "PutObjectLockConfiguration"},
		"ownershipControls":      {name: "PutBucketOwnershipControls"},
		"replication":            {name: "PutBucketReplication"},
		"requestPayment":         {name: "PutBucketRequestPayment"},
		"website":                {name: "PutBucketWebsite"},
	},
	"DELETE": {
		"analytics":             {name: "DeleteBucketAnalyticsConfiguration"},
		"encryption":            {name: "DeleteBucketEncryption"},
		"intelligent-tiering":   {name: "DeleteBucketIntelligentTieringConfiguration"},
		"inventory":             {name: "DeleteBucketInventoryConfiguration"},
		"metadataConfiguration": {name: "DeleteBucketMetadataConfiguration"},
		"metadataTable":         {name: "DeleteBucketMetadataTableConfiguration"},
		"metrics":               {name: "DeleteBucketMetricsConfiguration"},
		"ownershipControls":     {name: "DeleteBucketOwnershipControls"},
		"replication":           {name: "DeleteBucketReplication"},
		"website":               {name: "DeleteBucketWebsite"},
	},
	"POST": {
		"metadataConfiguration": {name: "CreateBucketMetadataConfiguration"},
		"metadataTable":         {name: "CreateBucketMetadataTableConfiguration"},
	},
}

// s3UnroutedObjectSubresources maps each HTTP method to the published object-level sub-resource keys
// substrate does not route, and the operation each one is.
var s3UnroutedObjectSubresources = map[string]map[string]string{
	"GET": {
		"attributes": "GetObjectAttributes",
		"legal-hold": "GetObjectLegalHold",
		"retention":  "GetObjectRetention",
		"torrent":    "GetObjectTorrent",
	},
	"PUT": {
		"legal-hold": "PutObjectLegalHold",
		"retention":  "PutObjectRetention",
	},
	"POST": {
		"restore": "RestoreObject",
	},
}

// s3UnroutedBucketOperation returns the name of the unrouted bucket-level operation params names for
// method, or "" when params names none of them.
//
// A request naming two such keys is not one S3 publishes. The keys are tested in sorted order, so any
// such request resolves to the same name on every call: a map's iteration order would let one input
// answer differently twice, which is what deterministic replay cannot have.
func s3UnroutedBucketOperation(method string, params map[string]string) string {
	table := s3UnroutedBucketSubresources[method]
	for _, key := range s3SortedKeys(table) {
		if _, ok := params[key]; !ok {
			continue
		}
		entry := table[key]
		if entry.listName != "" {
			if _, hasID := params["id"]; !hasID {
				return entry.listName
			}
		}
		return entry.name
	}
	return ""
}

// s3UnroutedObjectOperation returns the name of the unrouted object-level operation params names for
// method, or "" when params names none of them.
func s3UnroutedObjectOperation(method string, params map[string]string) string {
	table := s3UnroutedObjectSubresources[method]
	for _, key := range s3SortedKeys(table) {
		if _, ok := params[key]; ok {
			return table[key]
		}
	}
	return ""
}

// s3SortedKeys returns m's keys in sorted order.
func s3SortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
