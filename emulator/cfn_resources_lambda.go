package emulator

// StackDeployer helpers for CloudFormation's Lambda resource types. The rules every in-place
// updater follows are stated with cfnUpdateUnmodelledProperties.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// ----- AWS::Lambda::Function ------------------------------------------------------------

// cfnLambdaConfigProperties are the AWS::Lambda::Function properties the deploy forwards as
// configuration, which UpdateFunctionConfiguration takes.
var cfnLambdaConfigProperties = []string{
	"Runtime", "Role", "Handler", "Description", "Timeout", "MemorySize", "Environment",
}

// The defaults AWS::Lambda::Function publishes for two of them: "The default is 3 seconds"
// (Timeout) and "The default value is 128 MB." (MemorySize).
const (
	cfnLambdaDefaultTimeout    = 3
	cfnLambdaDefaultMemorySize = 128
)

// cfnLambdaConfiguration renders a function's configuration members, the same for CreateFunction
// and UpdateFunctionConfiguration. Runtime and Handler take the defaults the create has always
// sent. Timeout and MemorySize are numeric in the Lambda API, so the resolved string is converted.
//
// Environment's Variables are rendered as strings: the Lambda API's map is string to string, and a
// YAML template's `PORT: 8080` decodes as a number.
func cfnLambdaConfiguration(props map[string]interface{}, cctx *cfnContext) map[string]interface{} {
	body := map[string]interface{}{
		"Runtime":     resolveStringProp(props, "Runtime", "python3.12", cctx),
		"Role":        resolveStringProp(props, "Role", "", cctx),
		"Handler":     resolveStringProp(props, "Handler", "index.handler", cctx),
		"Description": resolveStringProp(props, "Description", "", cctx),
	}
	if timeout := resolveStringProp(props, "Timeout", "", cctx); timeout != "" {
		if n, convErr := strconv.Atoi(timeout); convErr == nil {
			body["Timeout"] = n
		}
	}
	if memory := resolveStringProp(props, "MemorySize", "", cctx); memory != "" {
		if n, convErr := strconv.Atoi(memory); convErr == nil {
			body["MemorySize"] = n
		}
	}
	if env, ok := cfnResolveStructured(props["Environment"], cctx).(map[string]interface{}); ok {
		vars := map[string]string{}
		if declared, ok := env["Variables"].(map[string]interface{}); ok {
			for k, v := range declared {
				vars[k] = resolveValue(v, cctx)
			}
		}
		body["Environment"] = map[string]interface{}{"Variables": vars}
	}
	return body
}

// updateLambdaFunction applies a changed AWS::Lambda::Function through UpdateFunctionConfiguration
// and UpdateFunctionCode, in that order — the page: "When you update a AWS::Lambda::Function
// resource, CloudFormation calls the UpdateFunctionConfiguration and UpdateFunctionCodeLambda APIs".
// FunctionName, PackageType, DurableConfig and TenancyConfig are Replacement and never reach here.
//
// A removed Timeout or MemorySize resets to the page's default, and a removed Environment to no
// variables. A removed Description is left as it was: substrate's UpdateFunctionConfiguration
// treats an empty Description as not sent.
func (d *StackDeployer) updateLambdaFunction(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, changed []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	var cost float64
	base := "/2015-03-31/functions/" + prior.PhysicalID
	cfg := cfnLambdaConfiguration(props, cctx)
	if cfnChangedAny(changed, cfnLambdaConfigProperties...) {
		if _, ok := cfg["Timeout"]; !ok {
			cfg["Timeout"] = cfnLambdaDefaultTimeout
		}
		if _, ok := cfg["MemorySize"]; !ok {
			cfg["MemorySize"] = cfnLambdaDefaultMemorySize
		}
		if _, ok := cfg["Environment"]; !ok {
			cfg["Environment"] = map[string]interface{}{"Variables": map[string]string{}}
		}
		req, err := cfnJSONRequest("lambda", "PUT", cfg)
		if err != nil {
			return DeployedResource{}, 0, err
		}
		req.Path = base + "/configuration"
		if routeErr := d.cfnUpdateDispatch(ctx, req, streamID, &cost); routeErr != nil {
			dr.Error = routeErr.Error()
			return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
		}
	}
	if slices.Contains(changed, "Code") {
		body := map[string]interface{}{"Runtime": cfg["Runtime"]}
		cfnLambdaCode(body, props, cctx)
		code, _ := body["Code"].(map[string]interface{})
		if code == nil {
			code = map[string]interface{}{}
		}
		req, err := cfnJSONRequest("lambda", "PUT", code)
		if err != nil {
			return DeployedResource{}, 0, err
		}
		req.Path = base + "/code"
		if routeErr := d.cfnUpdateDispatch(ctx, req, streamID, &cost); routeErr != nil {
			dr.Error = routeErr.Error()
			return dr, cost, nil //nolint:nilerr // A refused update is reported on the resource.
		}
	}
	return dr, cost, nil
}

// ----- AWS::Lambda::Permission -------------------------------------------------------------------

// cfnLambdaFunctionName splits a FunctionName property — "the name of the Lambda function, version,
// or alias", which may be a name, a function ARN or a partial ARN, any of them with a version or
// alias suffix — into the name the API path takes and the qualifier AddPermission and
// RemovePermission take as their Qualifier parameter. A template usually passes the function's Arn
// attribute, which is how the CDK writes it, or an alias's.
func cfnLambdaFunctionName(v string) (name, qualifier string) {
	if i := strings.Index(v, ":function:"); i >= 0 {
		v = v[i+len(":function:"):]
	}
	name, qualifier, _ = strings.Cut(v, ":")
	return name, qualifier
}

// cfnLambdaQualifier is the query a permission request carries for a qualified function.
func cfnLambdaQualifier(qualifier string) map[string]string {
	if qualifier == "" {
		return map[string]string{}
	}
	return map[string]string{"Qualifier": qualifier}
}

// deployLambdaPermission adds a statement to the function's resource policy through AddPermission.
//
// CloudFormation names the statement, the type declaring no StatementId, so substrate generates one
// per stack as it does a name ({stack}-{logical}-{suffix}), and a replacement draws a new one — every
// property is "Update requires: Replacement", and the replacement is added before the old statement
// is removed, so the two must not collide. The physical ID and Fn::GetAtt Id are the statement ID;
// the function is recorded too, because RemovePermission needs both.
func (d *StackDeployer) deployLambdaPermission(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	sid := cfnGeneratedName(cctx, "AWS::Lambda::Permission", logicalID)
	function, qualifier := cfnLambdaFunctionName(resolveStringProp(props, "FunctionName", "", cctx))
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Lambda::Permission", PhysicalID: sid,
		Metadata: map[string]interface{}{"Id": sid, "FunctionName": function}}
	if qualifier != "" {
		dr.Metadata["Qualifier"] = qualifier
	}
	body := map[string]interface{}{"StatementId": sid}
	for _, name := range []string{"Action", "Principal", "SourceArn", "SourceAccount", "PrincipalOrgID",
		"EventSourceToken", "FunctionUrlAuthType"} {
		if v := resolveStringProp(props, name, "", cctx); v != "" {
			body[name] = v
		}
	}
	if b, ok := cfgsvcCFNBool(props, "InvokedViaFunctionUrl"); ok && b {
		body["InvokedViaFunctionUrl"] = true
	}
	data, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployLambdaPermission marshal: %w", err)
	}
	var cost float64
	if err := d.cfnUpdateDispatch(ctx, &AWSRequest{Service: "lambda", Operation: http.MethodPost,
		Path: "/2015-03-31/functions/" + function + "/policy", Body: data,
		Headers: map[string]string{}, Params: cfnLambdaQualifier(qualifier)}, streamID, &cost); err != nil {
		dr.Error = err.Error()
	}
	return dr, cost, nil
}

// cfnLambdaPermissionDeleter removes the statement from the function's policy.
func cfnLambdaPermissionDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	function := cfnRefMetadata(dr, "FunctionName")
	if function == "" || dr.PhysicalID == "" {
		return nil
	}
	return &AWSRequest{Service: "lambda", Operation: http.MethodDelete,
		Path:    "/2015-03-31/functions/" + function + "/policy/" + dr.PhysicalID,
		Headers: map[string]string{}, Params: cfnLambdaQualifier(cfnRefMetadata(dr, "Qualifier"))}
}
