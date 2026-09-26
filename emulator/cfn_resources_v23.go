package emulator

// cfn_resources_v23.go holds the StackDeployer.deployResource helpers for
// CloudFront and Kinesis.
// The name records the substrate release that added them (v0.23.0) rather than the
// services, because several releases touched overlapping services; the helpers here
// follow the same pattern as those in cfn_deployer.go.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"strings"
)

// ----- v0.23.0 — Kinesis ---------------------------------------------------

// deployKinesisStream creates a Kinesis data stream for the given CFN resource.
func (d *StackDeployer) deployKinesisStream(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)

	body := map[string]interface{}{
		"StreamName": name,
		"ShardCount": 1,
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "kinesis",
		Operation: "CreateStream",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "Kinesis_20131202.CreateStream"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	arn := "arn:aws:kinesis:" + cctx.region + ":" + cctx.accountID + ":stream/" + name
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Kinesis::Stream",
		PhysicalID: name,
		ARN:        arn,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// ----- v0.23.0 — CloudFront ------------------------------------------------

// deployCloudFrontDistribution creates a CloudFront distribution stub.
func (d *StackDeployer) deployCloudFrontDistribution(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// DistributionConfig is a nested object; indexed flat this matched nothing and the stored
	// comment was always the logical ID (#877). The PhysicalID is recovered from the response Id
	// below, so this was the cosmetic one of the five — but a comment that reports a template's
	// logical ID rather than what the template said is still a value from somewhere other than
	// the request.
	comment := resolveNestedStringProp(props, "DistributionConfig", "Comment", logicalID, cctx)

	body := []byte(`<DistributionConfig><Comment>` + comment + `</Comment><Enabled>true</Enabled></DistributionConfig>`)

	req := &AWSRequest{
		Service:   "cloudfront",
		Operation: "POST",
		Path:      "/2020-05-31/distribution",
		Body:      body,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CloudFront::Distribution",
		PhysicalID: logicalID,
		Metadata:   make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		// Extract Id and DomainName from XML response.
		idVal := extractXMLField(resp.Body, "Id")
		domainVal := extractXMLField(resp.Body, "DomainName")
		if idVal != "" {
			dr.PhysicalID = idVal
			dr.ARN = "arn:aws:cloudfront::" + cctx.accountID + ":distribution/" + idVal
		}
		if domainVal != "" {
			dr.Metadata["DomainName"] = domainVal
		}
	}
	return dr, cost, nil
}

// cfnOAIIDChars are the characters an origin access identity's ID is built from.
//
// AWS publishes the format only by example, and the two examples agree: Ref returns "the origin
// access identity, such as E15MNIMTCFKK4C", and Fn::GetAtt Id returns "E74FTE3AJFJ256A". Both are
// E followed by thirteen uppercase alphanumerics, which is also the shape
// generateCloudFrontID's own doc comment states for a distribution ID. So the alphabet is
// substrate's reading of two agreeing samples rather than of a published grammar.
const cfnOAIIDChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// cfnOAIIDLen is the number of characters after the leading "E".
const cfnOAIIDLen = 13

// cfnOriginAccessIdentityID derives an origin access identity's ID from the resource's scope.
//
// Deterministic for a given (account, Region, stack, logical ID), for the reason
// cfnGeneratedName records for its own suffix: UpdateStack in substrate re-deploys the whole
// template, so an ID minted from crypto/rand would change on every update and leak the identity
// it replaced. It is derived rather than reused from generateCloudFrontID
// (cloudfront_plugin.go), which produces exactly this shape but draws from the *request's* mint:
// that makes a replayed CreateDistribution reproduce its ID (#856), and says nothing about an
// UpdateStack in the same run, which is a second request with a second request id and so a second
// mint. What this ID has to be stable across is redeployment, not replay.
//
// The two obvious deterministic helpers do not fit. cfnGeneratedName returns a hyphenated
// {stack}-{logical}-{suffix}, which is not this shape. cfnNameSuffix is pinned to twelve base-36
// characters by cfnGeneratedNameSuffixLen — thirteen in total, one short — and cannot be widened:
// its modulus loop is uint64 and 36**13 (about 1.71e20) overflows 2**64 (about 1.84e19), where
// 36**12 (about 4.74e18) fits. So the derivation is SHA-256 over the joined scope, following
// cfnDeterministicUUID, mapped onto the thirty-six-character alphabet.
//
// Each output character takes its own hash byte rather than reducing one big integer, which keeps
// the mapping independent of any word size. The modulus makes the distribution very slightly
// uneven (256 is not a multiple of 36); that is irrelevant here, because the property needed is
// stability plus distinctness between resources, not uniformity.
func cfnOriginAccessIdentityID(cctx *cfnContext, logicalID string) string {
	stackName, region, accountID := "", "", ""
	if cctx != nil {
		stackName, region, accountID = cctx.stackName, cctx.region, cctx.accountID
	}
	sum := sha256.Sum256([]byte(strings.Join(
		[]string{accountID, region, stackName, logicalID}, "/")))

	out := make([]byte, 0, cfnOAIIDLen+1)
	out = append(out, 'E')
	for i := 0; i < cfnOAIIDLen; i++ {
		out = append(out, cfnOAIIDChars[int(sum[i])%len(cfnOAIIDChars)])
	}
	return string(out)
}

// deployCloudFrontOAI records a CloudFront origin access identity.
//
// It dispatches nothing, and the type stays in cfnDeleteInertTypes: substrate keeps no
// CloudFront-side OAI record, so there is nothing to create and nothing to delete. What #859
// fixes is the identity, not the record — Ref returned the resource's own *logical ID*, which is
// not an identifier AWS would ever hand back, and every template passing !Ref to an S3 bucket
// policy or an origin-access configuration passed that along.
//
// The comment is read but no longer reported. Before #877 the flat index of
// "CloudFrontOriginAccessIdentityConfig.Comment" matched nothing, so PhysicalID was the logical
// ID by way of the fallback rather than by intent; now the comment reaches Metadata, where a
// reader can see what the template said, and PhysicalID is the minted ID.
func (d *StackDeployer) deployCloudFrontOAI(
	_ context.Context,
	logicalID string,
	props map[string]interface{},
	_ string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	comment := resolveNestedStringProp(props,
		"CloudFrontOriginAccessIdentityConfig", "Comment", "", cctx)

	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CloudFront::CloudFrontOriginAccessIdentity",
		PhysicalID: cfnOriginAccessIdentityID(cctx, logicalID),
		Metadata:   make(map[string]interface{}),
	}
	if comment != "" {
		dr.Metadata["Comment"] = comment
	}
	return dr, 0, nil
}
