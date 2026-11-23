package emulator_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// cfnS3Sub reads one bucket subresource through the deployer's registry and returns the status
// and body.
func cfnS3Sub(t *testing.T, d *emulator.StackDeployer, bucket, sub string) (int, string) {
	t.Helper()
	resp, _ := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: "s3", Operation: "GET", Path: "/" + bucket,
		Headers: map[string]string{}, Params: map[string]string{sub: ""},
	}, "probe")
	require.NotNil(t, resp)
	return resp.StatusCode, string(resp.Body)
}

// cfnPolicyTemplate is a bucket and its policy, the policy's Sid varying so an update has
// something to change. The second statement is chosen by Fn::If, and the third is AWS::NoValue,
// which is how the CDK bootstrap template writes its optional statements.
func cfnPolicyTemplate(bucket, sid string) string {
	return `{"Conditions":{"Yes":{"Fn::Equals":["a","a"]},"No":{"Fn::Equals":["a","b"]}},"Resources":{
		"B":{"Type":"AWS::S3::Bucket","DeletionPolicy":"Retain","Properties":{"BucketName":"` + bucket + `"}},
		"P":{"Type":"AWS::S3::BucketPolicy","Properties":{"Bucket":{"Ref":"B"},"PolicyDocument":{
			"Version":"2012-10-17","Statement":[
				{"Sid":"` + sid + `","Effect":"Deny","Principal":"*","Action":"s3:*",
				 "Resource":[{"Fn::Sub":"${B.Arn}"},{"Fn::Sub":"${B.Arn}/*"}],
				 "Condition":{"Fn::If":["Yes",{"Bool":{"aws:SecureTransport":"false"}},{"Ref":"AWS::NoValue"}]}},
				{"Fn::If":["Yes",{"Sid":"Chosen","Effect":"Deny","Principal":"*","Action":"s3:DeleteBucket",
				 "Resource":{"Fn::Sub":"${B.Arn}"}},{"Ref":"AWS::NoValue"}]},
				{"Fn::If":["No",{"Sid":"NotChosen","Effect":"Deny","Principal":"*","Action":"s3:*",
				 "Resource":"*"},{"Ref":"AWS::NoValue"}]}]}}}}}`
}

// TestCFN_S3BucketPolicyLifecycle pins AWS::S3::BucketPolicy (#1606): the create applies the
// policy, a changed PolicyDocument is applied in place, a changed Bucket replaces it, and a stack
// delete removes it — from a bucket that itself survives under DeletionPolicy Retain.
func TestCFN_S3BucketPolicyLifecycle(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()

	created, err := d.Deploy(ctx, cfnPolicyTemplate("policy-a", "First"), "policied", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, created)
	assert.Equal(t, "policy-a", resourceByLogicalID(t, created, "P").PhysicalID)
	code, policy := cfnS3Sub(t, d, "policy-a", "policy")
	require.Equal(t, 200, code, policy)
	assert.Contains(t, policy, `"Sid":"First"`)
	assert.Contains(t, policy, `"Condition":{"Bool":{"aws:SecureTransport":"false"}}`,
		"an Fn::If choosing a structure is that structure, not its JSON text")
	assert.Contains(t, policy, `"Sid":"Chosen"`)
	assert.NotContains(t, policy, "NotChosen")
	assert.Contains(t, policy, `"arn:aws:s3:::policy-a/*"`)

	updated, err := d.UpdateStack(ctx, cfnPolicyTemplate("policy-a", "Second"), "policied", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	assert.Equal(t, "policy-a", resourceByLogicalID(t, updated, "P").PhysicalID, "a PolicyDocument change is in place")
	_, policy = cfnS3Sub(t, d, "policy-a", "policy")
	assert.Contains(t, policy, `"Sid":"Second"`)

	// A different Bucket is a replacement: the new bucket gets the policy and the old one loses it.
	// The old bucket itself is retained, as its DeletionPolicy says.
	replaced, err := d.UpdateStack(ctx, cfnPolicyTemplate("policy-b", "Third"), "policied", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", replaced.Status, replaced.StatusReason)
	assert.Equal(t, "policy-b", resourceByLogicalID(t, replaced, "P").PhysicalID)
	_, policy = cfnS3Sub(t, d, "policy-b", "policy")
	assert.Contains(t, policy, `"Sid":"Third"`)
	code, _ = cfnS3Sub(t, d, "policy-a", "policy")
	assert.Equal(t, 404, code, "the replaced policy is deleted from the bucket it was on")

	require.NoError(t, d.DeleteStack(ctx, "policied"))
	assert.Equal(t, 200, headBucket(t, d, "policy-b"), "the bucket is retained")
	code, _ = cfnS3Sub(t, d, "policy-b", "policy")
	assert.Equal(t, 404, code, "the stack's delete removes the policy")
}

// TestCFN_S3BucketPolicyRefusals pins that S3's own refusal of a policy fails the resource.
func TestCFN_S3BucketPolicyRefusals(t *testing.T) {
	tests := []struct {
		name     string
		template string
		want     string
	}{
		{name: "no such bucket", template: `{"Resources":{"P":{"Type":"AWS::S3::BucketPolicy","Properties":{
			"Bucket":"never-created","PolicyDocument":{"Version":"2012-10-17","Statement":[]}}}}}`, want: "NoSuchBucket"},
		{name: "a bucket named with capitals", template: `{"Resources":{
			"B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cased-bucket"}},
			"P":{"Type":"AWS::S3::BucketPolicy","DependsOn":"B","Properties":{"Bucket":"Cased-Bucket",
				"PolicyDocument":{"Version":"2012-10-17","Statement":[]}}}}}`, want: "NoSuchBucket"},
		{name: "a public policy under BlockPublicPolicy", template: `{"Resources":{
			"B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"blocked-bucket",
				"PublicAccessBlockConfiguration":{"BlockPublicPolicy":true}}},
			"P":{"Type":"AWS::S3::BucketPolicy","Properties":{"Bucket":{"Ref":"B"},"PolicyDocument":{
				"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject",
				"Resource":"arn:aws:s3:::blocked-bucket/*"}]}}}}}`, want: "AccessDenied"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			result, err := d.DeployWithOptions(context.Background(), tc.template, "refused", nil,
				emulator.CFNDeployOptions{OnFailure: emulator.CFNOnFailureDoNothing})
			require.NoError(t, err)
			assert.Equal(t, "CREATE_FAILED", result.Status)
			assert.Contains(t, resourceByLogicalID(t, result, "P").Error, tc.want)
		})
	}
}

// TestCFN_S3BucketLifecycleAndPublicAccessBlock pins the two bucket subresources the CDK bootstrap
// template declares (part of #1573): each is applied on create, changed in place on update, and
// deleted when the template drops it. A PublicAccessBlockConfiguration whose Fn::If chooses
// AWS::NoValue is not declared.
func TestCFN_S3BucketLifecycleAndPublicAccessBlock(t *testing.T) {
	const lifecycle = `"LifecycleConfiguration":{"Rules":[
		{"Id":"Expire","Status":"Enabled","Prefix":"logs/","ExpirationInDays":30,
		 "Transitions":[{"StorageClass":"GLACIER","TransitionInDays":7}],
		 "NoncurrentVersionTransitions":[{"StorageClass":"STANDARD_IA","TransitionInDays":30,"NewerNoncurrentVersions":2}]},
		{"Id":"Tagged","Status":"Disabled","TagFilters":[{"Key":"k","Value":"v"}],"ObjectSizeGreaterThan":"1024",
		 "ExpirationDate":"2027-01-01T00:00:00.000Z","Transition":{"StorageClass":"GLACIER","TransitionDate":"2026-12-01T00:00:00.000Z"},
		 "NoncurrentVersionExpirationInDays":9,"NoncurrentVersionTransition":{"StorageClass":"GLACIER","TransitionInDays":3}},
		{"Id":"OneTag","Status":"Enabled","TagFilters":[{"Key":"only","Value":"one"}],"ExpiredObjectDeleteMarker":true},
		{"Id":"Sized","Status":"Enabled","ObjectSizeLessThan":"4096","AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}`
	bucket := func(extra string) string {
		return `{"Parameters":{"Block":{"Type":"String","Default":"true"}},
			"Conditions":{"Blocks":{"Fn::Equals":[{"Ref":"Block"},"true"]}},
			"Resources":{"B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"lifecycle-bucket"` + extra + `}}}}`
	}
	const pab = `,"PublicAccessBlockConfiguration":{"Fn::If":["Blocks",
		{"BlockPublicAcls":true,"BlockPublicPolicy":"true","IgnorePublicAcls":true,"RestrictPublicBuckets":false},
		{"Ref":"AWS::NoValue"}]}`

	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	created, err := d.Deploy(ctx, bucket(","+lifecycle+pab), "subresources", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, created)

	code, lc := cfnS3Sub(t, d, "lifecycle-bucket", "lifecycle")
	require.Equal(t, 200, code, lc)
	for _, want := range []string{
		`<Rule><ID>Expire</ID><Filter><Prefix>logs/</Prefix></Filter><Status>Enabled</Status>`,
		`<Expiration><Days>30</Days></Expiration>`,
		`<Transition><Days>7</Days><StorageClass>GLACIER</StorageClass></Transition>`,
		`<NoncurrentVersionTransition><NoncurrentDays>30</NoncurrentDays><StorageClass>STANDARD_IA</StorageClass><NewerNoncurrentVersions>2</NewerNoncurrentVersions></NoncurrentVersionTransition>`,
		`<Filter><And><Tag><Key>k</Key><Value>v</Value></Tag><ObjectSizeGreaterThan>1024</ObjectSizeGreaterThan></And></Filter><Status>Disabled</Status>`,
		`<Expiration><Date>2027-01-01T00:00:00.000Z</Date></Expiration>`,
		`<Transition><Date>2026-12-01T00:00:00.000Z</Date><StorageClass>GLACIER</StorageClass></Transition>`,
		`<NoncurrentVersionTransition><NoncurrentDays>3</NoncurrentDays><StorageClass>GLACIER</StorageClass></NoncurrentVersionTransition>`,
		`<NoncurrentVersionExpiration><NoncurrentDays>9</NoncurrentDays></NoncurrentVersionExpiration>`,
		`<Filter><Tag><Key>only</Key><Value>one</Value></Tag></Filter>`,
		`<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>`,
		`<Filter><ObjectSizeLessThan>4096</ObjectSizeLessThan></Filter>`,
		`<AbortIncompleteMultipartUpload><DaysAfterInitiation>1</DaysAfterInitiation></AbortIncompleteMultipartUpload>`,
	} {
		assert.Contains(t, lc, want)
	}
	code, block := cfnS3Sub(t, d, "lifecycle-bucket", "publicAccessBlock")
	require.Equal(t, 200, code, block)
	assert.Contains(t, block, "<BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls>"+
		"<BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets>")

	// A changed rule is applied in place; the condition turning false drops the block.
	changed := strings.Replace(lifecycle, `"ExpirationInDays":30`, `"ExpirationInDays":60`, 1)
	updated, err := d.UpdateStack(ctx, bucket(","+changed+pab), "subresources",
		map[string]string{"Block": "false"}, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	_, lc = cfnS3Sub(t, d, "lifecycle-bucket", "lifecycle")
	assert.Contains(t, lc, `<Expiration><Days>60</Days></Expiration>`)
	code, _ = cfnS3Sub(t, d, "lifecycle-bucket", "publicAccessBlock")
	assert.Equal(t, 404, code, "a block whose Fn::If chooses AWS::NoValue is removed")

	// Dropping the lifecycle configuration deletes it.
	updated, err = d.UpdateStack(ctx, bucket(""), "subresources", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	code, _ = cfnS3Sub(t, d, "lifecycle-bucket", "lifecycle")
	assert.Equal(t, 404, code)

	// A fresh bucket whose block's condition is false gets none at all.
	fresh, err := d.Deploy(ctx, strings.Replace(bucket(pab), "lifecycle-bucket", "unblocked-bucket", 1),
		"unblocked", map[string]string{"Block": "false"})
	require.NoError(t, err)
	requireNoResourceErrors(t, fresh)
	code, _ = cfnS3Sub(t, d, "unblocked-bucket", "publicAccessBlock")
	assert.Equal(t, 404, code)
}
