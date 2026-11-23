package emulator

import (
	"net/http"
	"strconv"
)

// ListVersionsByFunction (#1519).
//
// The Terraform AWS provider reads a function's latest version on every refresh of an
// aws_lambda_function, with "GET /2015-03-31/functions/{FunctionName}/versions", and fails the
// resource when the call fails. Substrate routed nothing there, so a Lambda function could be
// created through Terraform and then not read back.
//
// Substrate publishes no versions (PublishVersion is not routed), so a function's only version is
// the unpublished one, $LATEST. The reference's Response Syntax is `{"NextMarker", "Versions"}`,
// each element a FunctionConfiguration, and its CLI example lists $LATEST with a qualified
// FunctionArn ending ":$LATEST" and "Version": "$LATEST". That is the one element answered here.

// lambdaLatestVersion is the version name of a function's unpublished code and configuration.
const lambdaLatestVersion = "$LATEST"

// The bounds ListVersionsByFunction's MaxItems publishes as its valid range: a minimum value of
// 1 and a maximum value of 10000.
const (
	lambdaVersionsMinMaxItems = 1
	lambdaVersionsMaxMaxItems = 10000
)

// listVersionsByFunction answers a function's versions: $LATEST alone, since nothing else is ever
// published.
//
// MaxItems is validated against its published range, and a value outside it is refused with the
// InvalidParameterValueException the page publishes for "one of the parameters in the request is
// not valid" (the message is substrate's own). A Marker is refused for the same reason: the single
// page never carries a NextMarker, so no Marker can be one substrate issued, and answering page one
// for it would hide a caller's paging defect (#915). Both checks come before the function is looked
// up, so the answer to a malformed request does not depend on what exists (#887).
func (p *LambdaPlugin) listVersionsByFunction(ctx *RequestContext, req *AWSRequest, name string) (*AWSResponse, error) {
	if raw := req.Params["MaxItems"]; raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < lambdaVersionsMinMaxItems || n > lambdaVersionsMaxMaxItems {
			return nil, lambdaInvalidParameterValue("MaxItems must be between 1 and 10000")
		}
	}
	if marker := req.Params["Marker"]; marker != "" {
		return nil, lambdaInvalidParameterValue("the Marker '" + marker + "' is invalid")
	}
	fn, err := p.loadFunction(ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	latest := buildFunctionConfig(fn)
	latest["FunctionArn"] = fn.FunctionArn + ":" + lambdaLatestVersion
	return lambdaJSONResponse(http.StatusOK, struct {
		Versions []map[string]interface{} `json:"Versions"`
	}{Versions: []map[string]interface{}{latest}})
}

// getFunctionCodeSigningConfig answers "GET /2020-06-30/functions/{FunctionName}/code-signing-config",
// which the Terraform AWS provider also reads on every refresh of a function (#1519).
//
// Substrate models no code-signing configurations (CreateCodeSigningConfig is not routed), so no
// function has one. The reference marks both response members required, and the provider reads
// an empty CodeSigningConfigArn as "none attached", so that is the answer for an existing
// function. An absent one is the ResourceNotFoundException the page publishes.
func (p *LambdaPlugin) getFunctionCodeSigningConfig(ctx *RequestContext, name string) (*AWSResponse, error) {
	fn, err := p.loadFunction(ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	return lambdaJSONResponse(http.StatusOK, struct {
		CodeSigningConfigArn string `json:"CodeSigningConfigArn"`
		FunctionName         string `json:"FunctionName"`
	}{FunctionName: fn.FunctionName})
}
