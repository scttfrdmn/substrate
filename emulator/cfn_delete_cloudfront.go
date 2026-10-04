package emulator

import (
	"context"
	"fmt"
	"strings"
)

// cfnSequencedDeleters maps a resource type whose delete is several requests, each depending on
// the response before it, to the function that dispatches them in order. It is consulted before
// [cfnResourceDeleters], whose single pre-built request cannot carry a value only a response
// supplies. A nil result is success, as from [StackDeployer.dispatchResourceDelete].
var cfnSequencedDeleters = map[string]func(context.Context, *StackDeployer, DeployedResource, string) *CFNResourceDeletion{
	"AWS::CloudFront::Distribution": cfnDeleteCloudFrontDistribution,
}

// cfnDeleteCloudFrontDistribution deletes a distribution the way CloudFormation does and
// API_DeleteDistribution requires (#1271): read the configuration and its ETag, disable the
// distribution if it is enabled by sending that configuration back with Enabled false and the
// ETag as If-Match, then delete it with the ETag the disable answered.
//
// The configuration sent back is the one read, changed in one member, so the update passes the
// same checks a caller's read-modify-write does. A distribution already gone at any step is a
// completed delete, the same reading [cfnDeleteIsAbsent] gives every other type. The "wait for
// Deployed" step is not needed: a distribution's Status is Deployed throughout.
func cfnDeleteCloudFrontDistribution(ctx context.Context, d *StackDeployer, dr DeployedResource, streamID string) *CFNResourceDeletion {
	path := "/2020-05-31/distribution/" + dr.PhysicalID
	failed := func(err error) *CFNResourceDeletion {
		if cfnDeleteIsAbsent(err) {
			return nil
		}
		return &CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type, Status: cfnDeleteFailed, Reason: err.Error()}
	}
	request := func(method, p, etag string, body []byte) *AWSRequest {
		headers := map[string]string{}
		if etag != "" {
			headers["If-Match"] = etag
		}
		return &AWSRequest{Service: "cloudfront", Operation: method, Path: p, Body: body, Headers: headers, Params: map[string]string{}}
	}

	resp, _, err := d.dispatch(ctx, request("GET", path+"/config", "", nil), streamID)
	if err != nil {
		return failed(err)
	}
	etag := resp.Headers["ETag"]
	cfg, err := cfParseDistributionConfig(resp.Body)
	if err != nil {
		return failed(fmt.Errorf("read the configuration of distribution %s: %w", dr.PhysicalID, err))
	}
	if enabled := cfg.child("Enabled"); enabled != nil && strings.TrimSpace(enabled.Text) == "true" {
		cfg.setText("Enabled", "false")
		body, err := cfMarshalConfig(cfg)
		if err != nil {
			return failed(fmt.Errorf("disable distribution %s: %w", dr.PhysicalID, err))
		}
		resp, _, err = d.dispatch(ctx, request("PUT", path+"/config", etag, []byte(body)), streamID)
		if err != nil {
			return failed(err)
		}
		etag = resp.Headers["ETag"]
	}
	if _, _, err := d.dispatch(ctx, request("DELETE", path, etag, nil), streamID); err != nil {
		return failed(err)
	}
	return nil
}
