package emulator_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// cfnS3Get reads one bucket sub-resource through the deployer's registry. The deployer reports an
// S3 error response as an error beside the response itself, so an error is accepted only when the
// response is the refusal it describes; anything else fails the test.
func cfnS3Get(t *testing.T, d *emulator.StackDeployer, bucket, sub string) (int, string) {
	t.Helper()
	resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{Service: "s3", Operation: "GET",
		Path: "/" + bucket, Params: map[string]string{sub: ""}, Headers: map[string]string{}}, "probe")
	require.NotNil(t, resp, "GET ?%s: %v", sub, err)
	if err != nil {
		require.GreaterOrEqualf(t, resp.StatusCode, http.StatusBadRequest,
			"GET ?%s failed with %v but answered %d", sub, err, resp.StatusCode)
	}
	return resp.StatusCode, string(resp.Body)
}

// TestCFNS3Bucket_WebsiteLoggingAccelerateReplication deploys a bucket declaring the four
// configurations #1613 added, then updates it to drop them, and reads each through S3.
func TestCFNS3Bucket_WebsiteLoggingAccelerateReplication(t *testing.T) {
	t.Parallel()
	d, _, _, _ := newSweepDeployer(t)
	const before = `{"Resources":{
		"Replica":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-replica","VersioningConfiguration":{"Status":"Enabled"}}},
		"Logs":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-logs"}},
		"Site":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-site",
			"VersioningConfiguration":{"Status":"Enabled"},
			"WebsiteConfiguration":{"IndexDocument":"index.html","ErrorDocument":"error.html",
				"RoutingRules":[{"RoutingRuleCondition":{"KeyPrefixEquals":"docs/"},"RedirectRule":{"ReplaceKeyPrefixWith":"documents/"}}]},
			"LoggingConfiguration":{"DestinationBucketName":{"Ref":"Logs"},"LogFilePrefix":"site/",
				"TargetObjectKeyFormat":{"PartitionedPrefix":{"PartitionDateSource":"DeliveryTime"}}},
			"AccelerateConfiguration":{"AccelerationStatus":"Enabled"},
			"ReplicationConfiguration":{"Role":"arn:aws:iam::123456789012:role/rep","Rules":[{
				"Id":"all","Status":"Enabled","Priority":1,"Filter":{"And":{"Prefix":"p/","TagFilters":[{"Key":"k","Value":"v"}]}},
				"DeleteMarkerReplication":{"Status":"Disabled"},
				"SourceSelectionCriteria":{"SseKmsEncryptedObjects":{"Status":"Enabled"}},
				"Destination":{"Bucket":{"Fn::GetAtt":["Replica","Arn"]},"StorageClass":"STANDARD_IA",
					"EncryptionConfiguration":{"ReplicaKmsKeyID":"arn:aws:kms:us-east-1:123456789012:key/k"},
					"ReplicationTime":{"Status":"Enabled","Time":{"Minutes":15}},
					"Metrics":{"Status":"Enabled","EventThreshold":{"Minutes":15}}}}]}}}}}`
	const after = `{"Resources":{
		"Replica":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-replica","VersioningConfiguration":{"Status":"Enabled"}}},
		"Logs":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-logs"}},
		"Site":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-site",
			"VersioningConfiguration":{"Status":"Enabled"},
			"WebsiteConfiguration":{"RedirectAllRequestsTo":{"HostName":"example.com","Protocol":"https"}},
			"LoggingConfiguration":{}}}}}`

	ctx := context.Background()
	created, err := d.Deploy(ctx, before, "site", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, created)

	_, website := cfnS3Get(t, d, "cfn-site", "website")
	assert.Contains(t, website, `<IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>error.html</Key></ErrorDocument>`)
	assert.Contains(t, website, `<RoutingRule><Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition><Redirect><ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith></Redirect></RoutingRule>`)
	_, logging := cfnS3Get(t, d, "cfn-site", "logging")
	assert.Contains(t, logging, `<TargetBucket>cfn-logs</TargetBucket>`)
	assert.Contains(t, logging, `<PartitionDateSource>DeliveryTime</PartitionDateSource>`)
	assert.Contains(t, logging, `<TargetPrefix>site/</TargetPrefix>`)
	_, accel := cfnS3Get(t, d, "cfn-site", "accelerate")
	assert.Contains(t, accel, `<Status>Enabled</Status>`)
	_, repl := cfnS3Get(t, d, "cfn-site", "replication")
	for _, want := range []string{
		`<Role>arn:aws:iam::123456789012:role/rep</Role>`,
		`<ID>all</ID><Priority>1</Priority><Filter><And><Prefix>p/</Prefix><Tag><Key>k</Key><Value>v</Value></Tag></And></Filter>`,
		`<SseKmsEncryptedObjects><Status>Enabled</Status></SseKmsEncryptedObjects>`,
		`<Bucket>arn:aws:s3:::cfn-replica</Bucket>`,
		`<StorageClass>STANDARD_IA</StorageClass>`,
		`<ReplicaKmsKeyID>arn:aws:kms:us-east-1:123456789012:key/k</ReplicaKmsKeyID>`,
		`<ReplicationTime><Status>Enabled</Status><Time><Minutes>15</Minutes></Time></ReplicationTime>`,
		`<Metrics><Status>Enabled</Status><EventThreshold><Minutes>15</Minutes></EventThreshold></Metrics>`,
		`<DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>`,
	} {
		assert.Contains(t, repl, want)
	}

	updated, err := d.UpdateStack(ctx, after, "site", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)

	_, website = cfnS3Get(t, d, "cfn-site", "website")
	assert.Contains(t, website, `<RedirectAllRequestsTo><HostName>example.com</HostName><Protocol>https</Protocol></RedirectAllRequestsTo>`)
	assert.NotContains(t, website, "IndexDocument")
	// LoggingConfiguration without DestinationBucketName logs to the bucket itself.
	_, logging = cfnS3Get(t, d, "cfn-site", "logging")
	assert.Contains(t, logging, `<TargetBucket>cfn-site</TargetBucket>`)
	// A dropped AccelerateConfiguration is left as it is; a dropped ReplicationConfiguration is deleted.
	_, accel = cfnS3Get(t, d, "cfn-site", "accelerate")
	assert.Contains(t, accel, `<Status>Enabled</Status>`)
	code, _ := cfnS3Get(t, d, "cfn-site", "replication")
	assert.Equal(t, http.StatusNotFound, code)

	// Dropping the website and logging configurations deletes the one and disables the other.
	const bare = `{"Resources":{
		"Replica":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-replica","VersioningConfiguration":{"Status":"Enabled"}}},
		"Logs":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-logs"}},
		"Site":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-site","VersioningConfiguration":{"Status":"Enabled"}}}}}`
	updated, err = d.UpdateStack(ctx, bare, "site", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	code, _ = cfnS3Get(t, d, "cfn-site", "website")
	assert.Equal(t, http.StatusNotFound, code)
	_, logging = cfnS3Get(t, d, "cfn-site", "logging")
	assert.NotContains(t, logging, "LoggingEnabled")
}

// TestCFNS3Bucket_ObjectLockEnabled pins that ObjectLockEnabled reaches CreateBucket, so the bucket
// reads back as Object Lock enabled and versioned, and a bucket without it reads as having none.
func TestCFNS3Bucket_ObjectLockEnabled(t *testing.T) {
	t.Parallel()
	d, _, _, _ := newSweepDeployer(t)
	created, err := d.Deploy(context.Background(), `{"Resources":{
		"Locked":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-locked","ObjectLockEnabled":true}},
		"Open":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-open","ObjectLockEnabled":false}}}}`, "lock", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, created)

	code, body := cfnS3Get(t, d, "cfn-locked", "object-lock")
	assert.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, `<ObjectLockEnabled>Enabled</ObjectLockEnabled>`)
	_, versioning := cfnS3Get(t, d, "cfn-locked", "versioning")
	assert.Contains(t, versioning, `<Status>Enabled</Status>`)
	code, _ = cfnS3Get(t, d, "cfn-open", "object-lock")
	assert.Equal(t, http.StatusNotFound, code)

	// Turning it on in an update goes through PutObjectLockConfiguration on the versioned bucket;
	// turning it off is left as it is, since Object Lock cannot be disabled.
	updated, err := d.UpdateStack(context.Background(), `{"Resources":{
		"Locked":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-locked"}},
		"Open":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cfn-open",
			"VersioningConfiguration":{"Status":"Enabled"},"ObjectLockEnabled":true}}}}`, "lock", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	code, body = cfnS3Get(t, d, "cfn-open", "object-lock")
	assert.Equal(t, http.StatusOK, code, body)
	code, _ = cfnS3Get(t, d, "cfn-locked", "object-lock")
	assert.Equal(t, http.StatusOK, code)
}

// TestCFNS3Bucket_ReplicationRefusalFailsTheResource pins that S3's own refusal reaches the
// resource: replication on an unversioned bucket fails it rather than being dropped silently.
func TestCFNS3Bucket_ReplicationRefusalFailsTheResource(t *testing.T) {
	t.Parallel()
	d, _, _, _ := newSweepDeployer(t)
	result, err := d.Deploy(context.Background(), `{"Resources":{"B":{"Type":"AWS::S3::Bucket","Properties":{
		"BucketName":"cfn-unversioned","ReplicationConfiguration":{"Role":"arn:aws:iam::123456789012:role/r",
		"Rules":[{"Status":"Enabled","Prefix":"","Destination":{"Bucket":"arn:aws:s3:::elsewhere"}}]}}}}}`, "repl-bad", nil)
	require.NoError(t, err)
	assert.Contains(t, resourceByLogicalID(t, result, "B").Error, "InvalidRequest: Versioning must be")
}
