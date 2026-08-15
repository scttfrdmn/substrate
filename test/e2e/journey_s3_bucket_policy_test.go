package e2e_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3_BucketPolicyLifecycle is #656 at the tier that found it, and the only tier
// that could have.
//
// The subresource markers S3 routes on arrive from aws-sdk-go-v2 as "?policy=" — a key
// with an empty value — not as the bare "?policy" the AWS documentation writes.
// Substrate's router compared them against the sentinel its own parser assigns to bare
// keys, so every SDK bucket-policy call fell through its arm's default: PutBucketPolicy
// was handled as CreateBucket, GetBucketPolicy as an object listing, and
// DeleteBucketPolicy as **DeleteBucket** — clearing a policy destroyed the bucket and
// its contents.
//
// Every S3 unit test passed throughout, because each hand-built the sentinel shape no
// client sends. Only a real client produces the query string, which is why this belongs
// here; #446, #561, #610 and #636 were all the same lesson.
func TestS3_BucketPolicyLifecycle(t *testing.T) {
	ctx := context.Background()
	cfg := awsConfig(t)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://s3.amazonaws.com")
		// Retries off, so each assertion is about the first response.
		o.RetryMaxAttempts = 1
	})

	const bucket = "e2e-policy-bucket"
	const policy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"config.amazonaws.com"},"Action":"s3:PutObject",` +
		`"Resource":"arn:aws:s3:::e2e-policy-bucket/*"}]}`

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "CreateBucket")

	// An object, so the delete path has something to lose: a bucket removed by a
	// misrouted DeleteBucketPolicy takes its contents with it, and that is the part of
	// the defect no status code shows.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("keepme"),
		Body:   strings.NewReader("keep me"),
	})
	require.NoError(t, err, "PutObject")

	// Put. A BucketAlreadyExists here is the misroute to CreateBucket.
	_, err = client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: aws.String(policy),
	})
	require.NoError(t, err,
		"PutBucketPolicy: a BucketAlreadyExists means ?policy= was not routed (#656)")

	// Get: the stored document, not an object listing.
	got, err := client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err, "GetBucketPolicy")
	assert.Contains(t, aws.ToString(got.Policy), "config.amazonaws.com",
		"GetBucketPolicy must answer the stored policy, not an object listing")

	// Delete, and the bucket survives it.
	_, err = client.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err, "DeleteBucketPolicy")

	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "HeadBucket after DeleteBucketPolicy: the bucket must "+
		"survive — a NotFound here is #656's data-loss path, where clearing a policy "+
		"deleted the bucket")

	_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("keepme"),
	})
	require.NoError(t, err,
		"HeadObject after DeleteBucketPolicy: the bucket's contents must survive too")

	// And the policy really is gone rather than never having been stored.
	_, err = client.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
		Bucket: aws.String(bucket),
	})
	require.Error(t, err, "GetBucketPolicy after the delete must be refused")
	assert.Contains(t, err.Error(), "NoSuchBucketPolicy",
		"the refusal names the missing policy, not a missing bucket")

}

// TestS3_DeleteBucketPolicyLeavesAnEmptyBucketStanding is the silent form of #656, and
// it is a separate test because it has to be reached.
//
// In the lifecycle above the misrouted DeleteBucket met a bucket holding an object and
// was refused with BucketNotEmpty, so the consumer at least saw an error and the test
// stops there. With nothing in the bucket the misroute **succeeds** at 204 — byte for
// byte what a correct DeleteBucketPolicy answers — and only a later HeadBucket shows
// that the bucket is gone. That is the case a consumer would never notice, so it gets
// its own path to the assertion.
func TestS3_DeleteBucketPolicyLeavesAnEmptyBucketStanding(t *testing.T) {
	ctx := context.Background()
	cfg := awsConfig(t)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://s3.amazonaws.com")
		o.RetryMaxAttempts = 1
	})

	const bucket = "e2e-policy-empty-bucket"
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "CreateBucket")

	_, err = client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Principal":{"Service":"config.amazonaws.com"},"Action":"s3:PutObject",` +
			`"Resource":"arn:aws:s3:::e2e-policy-empty-bucket/*"}]}`),
	})
	require.NoError(t, err, "PutBucketPolicy")

	_, err = client.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err, "DeleteBucketPolicy")

	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "HeadBucket after DeleteBucketPolicy on an empty bucket: a "+
		"NotFound here means the delete was routed as DeleteBucket and silently "+
		"destroyed the bucket (#656)")
}

// TestS3_BucketACLLifecycle covers the other markers #656 broke: a misrouted
// PutBucketAcl reached CreateBucket and stored nothing, and a misrouted GetBucketAcl
// answered an object listing.
func TestS3_BucketACLLifecycle(t *testing.T) {
	ctx := context.Background()
	cfg := awsConfig(t)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://s3.amazonaws.com")
		o.RetryMaxAttempts = 1
	})

	const bucket = "e2e-acl-bucket"
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "CreateBucket")

	_, err = client.PutBucketAcl(ctx, &s3.PutBucketAclInput{
		Bucket: aws.String(bucket),
		ACL:    s3types.BucketCannedACLPublicRead,
	})
	require.NoError(t, err,
		"PutBucketAcl: a BucketAlreadyExists means ?acl= was not routed (#656)")

	acl, err := client.GetBucketAcl(ctx, &s3.GetBucketAclInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "GetBucketAcl")
	// An object listing deserialized in place of an AccessControlPolicy leaves both of
	// these empty rather than erroring, so the assertion has to be on the contents.
	require.NotNil(t, acl.Owner,
		"GetBucketAcl returned no Owner — an object listing deserializes to exactly this")
	assert.NotEmpty(t, aws.ToString(acl.Owner.ID), "the ACL's Owner.ID")
	assert.NotEmpty(t, acl.Grants, "GetBucketAcl returned no Grants")

	// The object-level pair, whose misroute is quieter: a PutObjectAcl handled as
	// PutObject replaces the object with the ACL request's empty body.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("payload"),
	})
	require.NoError(t, err, "PutObject")

	_, err = client.PutObjectAcl(ctx, &s3.PutObjectAclInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
		ACL:    s3types.ObjectCannedACLPrivate,
	})
	require.NoError(t, err, "PutObjectAcl")

	objACL, err := client.GetObjectAcl(ctx, &s3.GetObjectAclInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err, "GetObjectAcl")
	require.NotNil(t, objACL.Owner,
		"GetObjectAcl returned no Owner — the object's own bytes deserialize to exactly this")

	// And the object still holds its bytes: a misrouted PutObjectAcl would have
	// replaced them with the ACL request's body.
	obj, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err, "GetObject")
	defer obj.Body.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(obj.Body)
	require.NoError(t, err)
	assert.Equal(t, "payload", buf.String(), "the object's bytes must survive PutObjectAcl")
}
