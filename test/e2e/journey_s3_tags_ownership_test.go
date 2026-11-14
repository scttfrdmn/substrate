package e2e_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// s3TagMap folds an SDK tag set into a map for comparison.
func s3TagMap(tags []s3types.Tag) map[string]string {
	out := make(map[string]string, len(tags))
	for _, tg := range tags {
		out[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
	}
	return out
}

// TestS3_TagsAndOwnershipThroughTheSDK drives #1434, #1463 and #1445 through aws-sdk-go-v2, which
// is what chooses the wire spelling: the `Tagging` input becomes the x-amz-tagging header, a
// CreateBucketConfiguration with Tags becomes the request body, and the ownership-controls calls
// arrive as "?ownershipControls=" — the shape a hand-built unit test does not produce (#656).
func TestS3_TagsAndOwnershipThroughTheSDK(t *testing.T) {
	ctx := context.Background()
	client := s3.NewFromConfig(awsConfig(t), func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://s3.amazonaws.com")
		o.RetryMaxAttempts = 1
	})
	const bucket = "e2e-tags-ownership"

	// #1463: tagged atomically at creation.
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:          aws.String(bucket),
		ObjectOwnership: s3types.ObjectOwnershipObjectWriter,
		CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
			Tags: []s3types.Tag{{Key: aws.String("aegc:managed"), Value: aws.String("true")}},
		},
	})
	require.NoError(t, err, "CreateBucket")
	bt, err := client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "GetBucketTagging")
	assert.Equal(t, map[string]string{"aegc:managed": "true"}, s3TagMap(bt.TagSet))

	// #1445: the create's x-amz-object-ownership, then a Put/Get/Delete cycle.
	oc, err := client.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "GetBucketOwnershipControls")
	require.Len(t, oc.OwnershipControls.Rules, 1)
	assert.Equal(t, s3types.ObjectOwnershipObjectWriter, oc.OwnershipControls.Rules[0].ObjectOwnership)

	_, err = client.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{
		Bucket: aws.String(bucket),
		OwnershipControls: &s3types.OwnershipControls{
			Rules: []s3types.OwnershipControlsRule{{ObjectOwnership: s3types.ObjectOwnershipBucketOwnerEnforced}},
		},
	})
	require.NoError(t, err, "PutBucketOwnershipControls answered 501 before #1445")
	oc, err = client.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	assert.Equal(t, s3types.ObjectOwnershipBucketOwnerEnforced, oc.OwnershipControls.Rules[0].ObjectOwnership)

	_, err = client.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "DeleteBucketOwnershipControls")
	_, err = client.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{Bucket: aws.String(bucket)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OwnershipControlsNotFoundError")
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "the bucket survives DeleteBucketOwnershipControls")

	// #1434: PutObject's Tagging, then a multipart upload's.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("k"),
		Body: strings.NewReader("x"), Tagging: aws.String("A=1&B=2"),
	})
	require.NoError(t, err, "PutObject")
	ot, err := client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String("k")})
	require.NoError(t, err, "GetObjectTagging")
	assert.Equal(t, map[string]string{"A": "1", "B": "2"}, s3TagMap(ot.TagSet))
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("k")})
	require.NoError(t, err)
	assert.Equal(t, int32(2), aws.ToInt32(head.TagCount))

	mpu, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String("big"), Tagging: aws.String("team=infra"),
	})
	require.NoError(t, err, "CreateMultipartUpload")
	part, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String("big"), UploadId: mpu.UploadId,
		PartNumber: aws.Int32(1), Body: strings.NewReader("only part"),
	})
	require.NoError(t, err, "UploadPart")
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String("big"), UploadId: mpu.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: []s3types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}},
		},
	})
	require.NoError(t, err, "CompleteMultipartUpload")
	ot, err = client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{Bucket: aws.String(bucket), Key: aws.String("big")})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "infra"}, s3TagMap(ot.TagSet))

	// A set over the object limit is refused, and leaves no object.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("too-many"), Body: strings.NewReader("x"),
		Tagging: aws.String("a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8&i=9&j=10&k=11"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidTag")
}
