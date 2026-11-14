package e2e_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3_RangedGetObjectWithChecksumValidation is #1438 at the tier that found it.
//
// From aws-sdk-go-v2 service/s3 v1.114 the client sends x-amz-checksum-mode: ENABLED
// and validates any x-amz-checksum-* header against the bytes it received. Substrate
// answered a ranged GetObject with the whole object's checksum, so reading the body of
// every ranged response failed with "checksum did not match". Real S3 omits the header
// from a range that does not align with a single part. Only the real SDK performs that
// validation, which is why the test belongs here: a unit test asserting the header is
// absent cannot show that the client accepts what remains.
func TestS3_RangedGetObjectWithChecksumValidation(t *testing.T) {
	ctx := context.Background()
	client := s3.NewFromConfig(awsConfig(t), func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://s3.amazonaws.com")
		o.RetryMaxAttempts = 1
	})

	const bucket = "e2e-ranged-checksum"
	const body = "hello world, this is a probe of ranged reads"

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "CreateBucket")
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("k"),
		Body:              strings.NewReader(body),
		ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32,
	})
	require.NoError(t, err, "PutObject")

	cases := []struct {
		name     string
		rng      *string
		want     string
		wantCRC  bool
		modeSets bool
	}{
		{name: "whole object, mode enabled", want: body, wantCRC: true, modeSets: true},
		{name: "prefix range, mode enabled", rng: aws.String("bytes=0-4"), want: "hello", modeSets: true},
		{name: "prefix range, mode unset", rng: aws.String("bytes=0-4"), want: "hello"},
		{name: "tail range, mode enabled", rng: aws.String("bytes=-5"), want: "reads", modeSets: true},
		{name: "range covering the object, mode enabled", rng: aws.String("bytes=0-"), want: body, wantCRC: true, modeSets: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("k"), Range: tc.rng}
			if tc.modeSets {
				in.ChecksumMode = s3types.ChecksumModeEnabled
			}
			out, err := client.GetObject(ctx, in)
			require.NoError(t, err, "GetObject")
			defer func() { _ = out.Body.Close() }()

			got, err := io.ReadAll(out.Body)
			require.NoError(t, err, "reading the body validates any checksum header the response carried (#1438)")
			assert.Equal(t, tc.want, string(got))
			if tc.wantCRC {
				assert.NotEmpty(t, aws.ToString(out.ChecksumCRC32), "a whole-object read keeps the checksum")
			} else {
				assert.Empty(t, aws.ToString(out.ChecksumCRC32), "a partial range carries no whole-object checksum")
			}
		})
	}
}
