package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// AWS::Route53::HealthCheck (#1613).
//
// The resource type's HealthCheckConfig carries the API's HealthCheckConfig members under the same
// names, so the deploy writes them into a CreateHealthCheck request as they are, and its
// HealthCheckTags through ChangeTagsForResource. The resource type publishes Ref as the health check's
// ID and one attribute, HealthCheckId. A stack delete sends DeleteHealthCheck by the physical ID
// ([cfnResourceDeleters]), and a health check already gone is NoSuchHealthCheck, which the sweep
// reads as deleted ([cfnDeleteAbsentCodes]).
//
// The resource type has no CallerReference property, so the deploy supplies one per create attempt
// ([cfnHealthCheckCallerReference]). Route 53 keeps a deleted health check's reference and answers a
// create that reuses it with HealthCheckAlreadyExists, so a reference derived from the stack and
// logical ID alone would refuse a stack deleted and deployed again, and would hand two stacks one
// health check.

// cfnHealthCheckScalars are the HealthCheckConfig members that are one value, in the order
// API_HealthCheckConfig gives them.
var cfnHealthCheckScalars = []string{ //nolint:gochecknoglobals // immutable table
	"IPAddress", "Port", "Type", "ResourcePath", "FullyQualifiedDomainName", "SearchString", "RequestInterval",
	"FailureThreshold", "MeasureLatency", "Inverted", "Disabled", "HealthThreshold",
}

// deployRoute53HealthCheck creates a Route 53 health check for the given CFN resource.
func (d *StackDeployer) deployRoute53HealthCheck(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	cfg, _ := props["HealthCheckConfig"].(map[string]interface{})
	var b strings.Builder
	b.WriteString(`<CreateHealthCheckRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><CallerReference>` +
		cfnXMLText(d.cfnHealthCheckCallerReference(cctx, logicalID)) + `</CallerReference><HealthCheckConfig>`)
	for _, k := range cfnHealthCheckScalars {
		if v := resolveStringProp(cfg, k, "", cctx); v != "" {
			b.WriteString(`<` + k + `>` + cfnXMLText(v) + `</` + k + `>`)
		}
	}
	cfnHealthCheckList(&b, "ChildHealthChecks", "ChildHealthCheck", resolveStringList(cfg["ChildHealthChecks"], cctx))
	if v := resolveStringProp(cfg, "EnableSNI", "", cctx); v != "" {
		b.WriteString(`<EnableSNI>` + cfnXMLText(v) + `</EnableSNI>`)
	}
	cfnHealthCheckList(&b, "Regions", "Region", resolveStringList(cfg["Regions"], cctx))
	if alarm, ok := cfg["AlarmIdentifier"].(map[string]interface{}); ok {
		b.WriteString(`<AlarmIdentifier><Region>` + cfnXMLText(resolveStringProp(alarm, "Region", "", cctx)) +
			`</Region><Name>` + cfnXMLText(resolveStringProp(alarm, "Name", "", cctx)) + `</Name></AlarmIdentifier>`)
	}
	for _, k := range []string{"InsufficientDataHealthStatus", "RoutingControlArn"} {
		if v := resolveStringProp(cfg, k, "", cctx); v != "" {
			b.WriteString(`<` + k + `>` + cfnXMLText(v) + `</` + k + `>`)
		}
	}
	b.WriteString(`</HealthCheckConfig></CreateHealthCheckRequest>`)

	resp, cost, routeErr := d.dispatch(ctx, &AWSRequest{
		Service: "route53", Operation: "POST", Path: "/2013-04-01/healthcheck", Body: []byte(b.String()),
		Headers: map[string]string{"Content-Type": "application/xml"}, Params: map[string]string{},
	}, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Route53::HealthCheck"}
	var id string
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		id = extractXMLField(resp.Body, "Id")
		dr.PhysicalID = id
		dr.ARN = "arn:aws:route53:::healthcheck/" + id
		cfnSetMetadata(&dr, "HealthCheckId", id)
	}

	tags := cfnTagMap(props["HealthCheckTags"], cctx)
	if len(tags) == 0 || id == "" {
		return dr, cost, nil
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var tb strings.Builder
	tb.WriteString(`<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><AddTags>`)
	for _, k := range keys {
		tb.WriteString(`<Tag><Key>` + cfnXMLText(k) + `</Key><Value>` + cfnXMLText(tags[k]) + `</Value></Tag>`)
	}
	tb.WriteString(`</AddTags></ChangeTagsForResourceRequest>`)
	_, tagCost, tagErr := d.dispatch(ctx, &AWSRequest{
		Service: "route53", Operation: "POST", Path: "/2013-04-01/tags/healthcheck/" + id, Body: []byte(tb.String()),
		Headers: map[string]string{"Content-Type": "application/xml"}, Params: map[string]string{},
	}, streamID)
	if tagErr != nil {
		dr.Error = tagErr.Error()
	}
	return dr, cost + tagCost, nil
}

// cfnHealthCheckCallerReference returns the CallerReference one create attempt of a stack's health
// check is sent with: "cfn-", 16 hex characters of SHA-256 over the stack's ARN and the logical ID,
// and 16 hex characters drawn from the deployer's mint, 37 characters in all, inside the 64
// CreateHealthCheck allows. The hash keeps two stacks, and two resources of one stack, apart; the
// draw keeps two attempts of one resource apart. The mint is the deployment's, so a replay of the
// deployment draws the same reference.
func (d *StackDeployer) cfnHealthCheckCallerReference(cctx *cfnContext, logicalID string) string {
	sum := sha256.Sum256([]byte(cfnStackARN(cctx.region, cctx.accountID, cctx.stackName) + "\x00" + logicalID))
	return "cfn-" + hex.EncodeToString(sum[:8]) + "-" + d.mint.Hex(8)
}

// cfnHealthCheckList writes a list member of HealthCheckConfig, or nothing for an empty list.
func cfnHealthCheckList(b *strings.Builder, wrapper, member string, values []string) {
	if len(values) == 0 {
		return
	}
	b.WriteString(`<` + wrapper + `>`)
	for _, v := range values {
		b.WriteString(`<` + member + `>` + cfnXMLText(v) + `</` + member + `>`)
	}
	b.WriteString(`</` + wrapper + `>`)
}
