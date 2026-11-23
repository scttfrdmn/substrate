package emulator

import (
	"context"
)

// cfn_s3_bucket_policy.go models AWS::S3::BucketPolicy (#1606), which fell through to the generic
// stub until v0.131.0: the stack reported the resource CREATE_COMPLETE and the bucket had no
// policy. The CDK bootstrap template declares one — StagingBucketPolicy, denying non-TLS access to
// the asset bucket — so the stub was the first thing a bootstrapped environment got wrong.
//
// From the resource's page: Bucket is "the name of the Amazon S3 bucket to which the policy
// applies" (Update requires: Replacement), and PolicyDocument is the policy, given in JSON or YAML
// (Update requires: No interruption). "If the DeletionPolicy attribute is not specified or set to
// Delete, the bucket policy will be removed when the stack is deleted. If set to Retain, the bucket
// policy will be preserved even after the stack is deleted." Each of the three is a call S3 routes:
// the create and an in-place update are PutBucketPolicy, and the delete is DeleteBucketPolicy.
//
// The physical ID is the bucket's name. The page publishes no Ref value of its own, and the bucket
// name is the one identifier both later calls need; a replacement (a changed Bucket) therefore has a
// new physical ID, as a replacement must, and the superseded policy is deleted from the old bucket.

// deployS3BucketPolicy applies an AWS::S3::BucketPolicy through PutBucketPolicy.
//
// S3's own refusals reach the resource unchanged: a bucket that does not exist is NoSuchBucket, a
// document that is not JSON is MalformedPolicy, and a public policy on a bucket whose Block Public
// Access sets BlockPublicPolicy is AccessDenied — each a CREATE_FAILED, as on AWS.
func (d *StackDeployer) deployS3BucketPolicy(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	bucket := resolveStringProp(props, "Bucket", "", cctx)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::S3::BucketPolicy", PhysicalID: bucket}
	var cost float64
	if err := d.putS3BucketPolicy(ctx, bucket, props, streamID, cctx, &cost); err != nil {
		dr.Error = err.Error()
	}
	return dr, cost, nil
}

// updateS3BucketPolicy applies a changed PolicyDocument in place. A changed Bucket is Replacement
// (cfnRecreationProperties), so it never reaches here.
func (d *StackDeployer) updateS3BucketPolicy(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	var cost float64
	if err := d.putS3BucketPolicy(ctx, prior.PhysicalID, props, streamID, cctx, &cost); err != nil {
		dr.Error = err.Error()
	}
	return dr, cost, nil
}

// putS3BucketPolicy sends PutBucketPolicy with the resolved PolicyDocument.
func (d *StackDeployer) putS3BucketPolicy(
	ctx context.Context, bucket string, props map[string]interface{}, streamID string, cctx *cfnContext,
	cost *float64,
) error {
	return d.cfnUpdateDispatch(ctx, &AWSRequest{
		Service:   "s3",
		Operation: "PUT",
		Path:      "/" + bucket,
		Params:    map[string]string{"policy": ""},
		Headers:   map[string]string{"Content-Type": "application/json"},
		Body:      []byte(cfnJSONDocument(props["PolicyDocument"], cctx)),
	}, streamID, cost)
}

// cfnS3BucketPolicyDeleter removes the policy from the bucket it was applied to.
func cfnS3BucketPolicyDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	if dr.PhysicalID == "" {
		return nil
	}
	return &AWSRequest{Service: "s3", Operation: "DELETE", Path: "/" + dr.PhysicalID,
		Headers: map[string]string{}, Params: map[string]string{"policy": ""}}
}
