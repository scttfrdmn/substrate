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

// ----- AWS::Lambda::Version ----------------------------------------------------------------------

// deployLambdaVersion publishes a version of the function through PublishVersion. "Ref returns the
// ARN of the version", which is the physical ID; Fn::GetAtt Version is the version number and
// FunctionArn the version's ARN, the type's primary identifier. Every property but
// FunctionScalingConfig is Update requires: Replacement, so a changed declaration publishes again.
//
// PublishVersion answers the newest version again when the function has not changed since it was
// published, and an AWS::Lambda::Version cannot adopt a version that already exists: CloudFormation
// fails the resource with "A version for this Lambda function exists ( N ). Modify the function to
// create a new version." That sentence is observed CloudFormation behavior, quoted in
// aws/aws-cdk#14428 and on the Serverless Framework's tracker (serverless/serverless#8392); no AWS
// page publishes it. Without the refusal a replaced Version would adopt the version its predecessor
// names, and the predecessor's cleanup would then delete it. A version is new exactly when its number
// exceeds every number the function had before the publish, so the newest is read first.
func (d *StackDeployer) deployLambdaVersion(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	function, _ := cfnLambdaFunctionName(resolveStringProp(props, "FunctionName", "", cctx))
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Lambda::Version",
		Metadata: map[string]interface{}{"FunctionName": function}}
	newest, cost, listErr := d.lambdaNewestVersion(ctx, function, streamID)
	if listErr != nil {
		dr.Error = listErr.Error()
		return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
	}
	body := map[string]interface{}{}
	for _, name := range []string{"Description", "CodeSha256"} {
		if v := resolveStringProp(props, name, "", cctx); v != "" {
			body[name] = v
		}
	}
	req, err := cfnJSONRequest("lambda", http.MethodPost, body)
	if err != nil {
		return DeployedResource{}, 0, err
	}
	req.Path = "/2015-03-31/functions/" + function + "/versions"
	resp, publishCost, routeErr := d.dispatch(ctx, req, streamID)
	cost += publishCost
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
	}
	var out struct {
		FunctionArn string `json:"FunctionArn"`
		Version     string `json:"Version"`
	}
	if resp != nil {
		if err := json.Unmarshal(resp.Body, &out); err != nil {
			return DeployedResource{}, cost, fmt.Errorf("cfn deployLambdaVersion decode: %w", err)
		}
	}
	if n, convErr := strconv.Atoi(out.Version); convErr == nil && n <= newest {
		// No physical ID, so a rollback deletes nothing: the version belongs to whatever named it.
		dr.Error = "A version for this Lambda function exists ( " + out.Version +
			" ). Modify the function to create a new version."
		return dr, cost, nil
	}
	dr.PhysicalID, dr.ARN = out.FunctionArn, out.FunctionArn
	dr.Metadata["Version"], dr.Metadata["FunctionArn"] = out.Version, out.FunctionArn
	return dr, cost, nil
}

// lambdaNewestVersion returns the highest version number function has published, 0 for none, by
// walking ListVersionsByFunction to its last page. A refused listing, an absent function among
// them, is returned as the error to report on the resource.
func (d *StackDeployer) lambdaNewestVersion(ctx context.Context, function, streamID string) (int, float64, error) {
	newest, marker := 0, ""
	var cost float64
	for {
		params := map[string]string{"MaxItems": "50"}
		if marker != "" {
			params["Marker"] = marker
		}
		resp, c, err := d.dispatch(ctx, &AWSRequest{Service: "lambda", Operation: http.MethodGet,
			Path: "/2015-03-31/functions/" + function + "/versions", Headers: map[string]string{}, Params: params}, streamID)
		cost += c
		if err != nil {
			return 0, cost, err
		}
		var page struct {
			Versions []struct {
				Version string `json:"Version"`
			} `json:"Versions"`
			NextMarker string `json:"NextMarker"`
		}
		if resp != nil {
			if err := json.Unmarshal(resp.Body, &page); err != nil {
				return 0, cost, fmt.Errorf("cfn lambdaNewestVersion decode: %w", err)
			}
		}
		for _, v := range page.Versions {
			if n, convErr := strconv.Atoi(v.Version); convErr == nil {
				newest = max(newest, n)
			}
		}
		if page.NextMarker == "" {
			return newest, cost, nil
		}
		marker = page.NextMarker
	}
}

// cfnLambdaVersionDeleter deletes the version through DeleteFunction's Qualifier.
func cfnLambdaVersionDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	function, version := cfnRefMetadata(dr, "FunctionName"), cfnRefMetadata(dr, "Version")
	if function == "" || version == "" {
		return nil
	}
	return &AWSRequest{Service: "lambda", Operation: http.MethodDelete,
		Path:    "/2015-03-31/functions/" + function,
		Headers: map[string]string{}, Params: map[string]string{"Qualifier": version}}
}

// ----- AWS::Lambda::Alias ------------------------------------------------------------------------

// cfnLambdaAliasBody renders the members CreateAlias and UpdateAlias share. The template's
// RoutingConfig is a list of {FunctionVersion, FunctionWeight}, and the API's is a map from version
// to weight; an absent one is an empty map, which UpdateAlias reads as no routing.
func cfnLambdaAliasBody(props map[string]interface{}, cctx *cfnContext) map[string]interface{} {
	weights := map[string]float64{}
	if rc, ok := cfnResolveStructured(props["RoutingConfig"], cctx).(map[string]interface{}); ok {
		entries, _ := rc["AdditionalVersionWeights"].([]interface{})
		for _, e := range entries {
			m, ok := e.(map[string]interface{})
			if !ok {
				continue
			}
			version := fmt.Sprint(m["FunctionVersion"])
			if w, err := strconv.ParseFloat(fmt.Sprint(m["FunctionWeight"]), 64); err == nil && version != "" {
				weights[version] = w
			}
		}
	}
	return map[string]interface{}{
		"FunctionVersion": resolveStringProp(props, "FunctionVersion", "", cctx),
		"Description":     resolveStringProp(props, "Description", "", cctx),
		"RoutingConfig":   map[string]interface{}{"AdditionalVersionWeights": weights},
	}
}

// deployLambdaAlias creates the alias through CreateAlias. "Ref returns the resource ARN", the
// alias ARN, which is the physical ID and Fn::GetAtt AliasArn.
func (d *StackDeployer) deployLambdaAlias(
	ctx context.Context, logicalID string, props map[string]interface{}, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	function, _ := cfnLambdaFunctionName(resolveStringProp(props, "FunctionName", "", cctx))
	name := resolveStringProp(props, "Name", "", cctx)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Lambda::Alias",
		Metadata: map[string]interface{}{"FunctionName": function, "Name": name}}
	body := cfnLambdaAliasBody(props, cctx)
	body["Name"] = name
	req, err := cfnJSONRequest("lambda", http.MethodPost, body)
	if err != nil {
		return DeployedResource{}, 0, err
	}
	req.Path = "/2015-03-31/functions/" + function + "/aliases"
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, cost, nil //nolint:nilerr // A refused call is reported on the resource.
	}
	var out struct {
		AliasArn string `json:"AliasArn"`
	}
	if resp != nil {
		if err := json.Unmarshal(resp.Body, &out); err != nil {
			return DeployedResource{}, cost, fmt.Errorf("cfn deployLambdaAlias decode: %w", err)
		}
	}
	dr.PhysicalID, dr.ARN = out.AliasArn, out.AliasArn
	dr.Metadata["AliasArn"] = out.AliasArn
	return dr, cost, nil
}

// updateLambdaAlias applies a changed AWS::Lambda::Alias through UpdateAlias: Description,
// FunctionVersion and RoutingConfig are No interruption. All three are sent whether or not they
// changed, so a removed Description or RoutingConfig is cleared. FunctionName and Name are
// Replacement and never reach here.
func (d *StackDeployer) updateLambdaAlias(
	ctx context.Context, logicalID string, prior DeployedResource,
	props map[string]interface{}, _ []string, streamID string, cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := prior
	dr.LogicalID = logicalID
	dr.Error = ""
	req, err := cfnJSONRequest("lambda", http.MethodPut, cfnLambdaAliasBody(props, cctx))
	if err != nil {
		return DeployedResource{}, 0, err
	}
	req.Path = "/2015-03-31/functions/" + cfnRefMetadata(prior, "FunctionName") + "/aliases/" + cfnRefMetadata(prior, "Name")
	var cost float64
	if routeErr := d.cfnUpdateDispatch(ctx, req, streamID, &cost); routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// cfnLambdaAliasDeleter deletes the alias through DeleteAlias.
func cfnLambdaAliasDeleter(_ *StackDeployer, dr DeployedResource, _ map[string]interface{}, _ *cfnContext) *AWSRequest {
	function, name := cfnRefMetadata(dr, "FunctionName"), cfnRefMetadata(dr, "Name")
	if function == "" || name == "" {
		return nil
	}
	return &AWSRequest{Service: "lambda", Operation: http.MethodDelete,
		Path:    "/2015-03-31/functions/" + function + "/aliases/" + name,
		Headers: map[string]string{}, Params: map[string]string{}}
}
