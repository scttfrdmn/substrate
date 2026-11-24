package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Function versions (#1613).
//
// PublishVersion "creates a version from the current code and configuration" of a function, a
// snapshot "that doesn't change". Substrate stores each one at lambdaVersionStateKey, numbered from
// a per-function counter that never reuses a number, and answers it wherever a request names the
// version: GetFunction, GetFunctionConfiguration, Invoke, the resource policy and DeleteFunction all
// take a Qualifier, or a FunctionName with the version appended, and resolve it through
// [LambdaPlugin.resolveFunction]. An alias resolves to the version it points at (lambda_aliases.go).
//
// Updates change only $LATEST: UpdateFunctionCode and UpdateFunctionConfiguration write the
// function record, and a published version keeps the configuration it was published with.
//
// "AWS Lambda doesn't publish a version if the function's configuration and code haven't changed
// since the last version." Substrate reads that as answering the last version again, which is what
// the Terraform provider and the CDK rely on when a deploy publishes on every apply: each version
// records a fingerprint of the $LATEST code and configuration it was published from, and a publish
// whose fingerprint matches the newest version's answers that version with no new number.

// lambdaLatestVersion is the version name of a function's unpublished code and configuration.
const lambdaLatestVersion = "$LATEST"

// The bounds the two Lambda listings' MaxItems publishes as their valid range: a minimum value of
// 1 and a maximum value of 10000.
const (
	lambdaListMinMaxItems = 1
	lambdaListMaxMaxItems = 10000
)

// lambdaVersionsPageCap is the most versions one ListVersionsByFunction response carries: "Lambda
// returns up to 50 versions per call", and MaxItems' own note, "returns a maximum of 50 items in
// each response, even if you set the number higher". An absent MaxItems is the same 50.
const lambdaVersionsPageCap = 50

// lambdaMarkerPage builds the page request of a Lambda listing from its `MaxItems` and `Marker`
// query parameters.
//
// MaxItems outside its published range, or not an integer, is the InvalidParameterValueException
// each listing publishes for "one of the parameters in the request is not valid", and so is a Marker
// this operation did not issue for these filter members. Both messages are substrate's own. The
// page size is capped at pageCap whatever MaxItems asks for, and an absent MaxItems is the cap.
func lambdaMarkerPage(req *AWSRequest, op string, filter map[string][]string, pageCap int) (pageRequest, *AWSError) {
	var maxItems *int
	if raw := req.Params["MaxItems"]; raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return pageRequest{}, lambdaInvalidParameterValue("MaxItems must be between 1 and 10000")
		}
		maxItems = &n
	}
	pr, awsErr := jsonNextToken(jsonPaging{
		op:          op,
		size:        pageSizeRange{member: "MaxItems", min: lambdaListMinMaxItems, max: lambdaListMaxMaxItems, def: pageCap},
		invalidSize: lambdaInvalidParameterValue,
		invalidToken: func() *AWSError {
			return lambdaInvalidParameterValue("the Marker '" + req.Params["Marker"] + "' is invalid")
		},
	}, filter, req.Params["Marker"], maxItems)
	if awsErr != nil {
		return pageRequest{}, awsErr
	}
	pr.Size = min(pr.Size, pageCap)
	return pr, nil
}

// lambdaFunctionScope reads a FunctionName in any of its published forms — `name`,
// `name:qualifier`, a partial ARN `account:function:name[:qualifier]` or a full ARN — into the
// account and Region that own the function, its name, and the version or alias the name carries.
// A reference to a function outside the caller's account and Region is refused by
// [lambdaCheckFunctionScope]; action names the IAM action, such as lambda:InvokeFunction.
func lambdaFunctionScope(ctx *RequestContext, ref, action string) (accountID, region, name, qualifier string, awsErr *AWSError) {
	accountID, region = lambdaRefScope(ctx, ref)
	name, qualifier = lambdaParseFunctionRef(ref)
	if awsErr := lambdaCheckFunctionScope(ctx, ref, action); awsErr != nil {
		return "", "", "", "", awsErr
	}
	return accountID, region, name, qualifier, nil
}

// lambdaRefScope returns the account and Region a function reference names: a full ARN's own, a
// partial ARN's account with the caller's Region, and the caller's for a bare name.
func lambdaRefScope(ctx *RequestContext, ref string) (accountID, region string) {
	if acct, reg, ok := lambdaARNAccountRegion(ref); ok {
		return acct, reg
	}
	if parts := strings.Split(ref, ":"); len(parts) >= 3 && parts[1] == "function" {
		return parts[0], ctx.Region
	}
	return ctx.AccountID, ctx.Region
}

// lambdaCheckFunctionScope refuses a function reference outside the caller's account and Region.
//
// Another account's function is AccessDeniedException/403, in the sentence AWS answers a caller no
// resource-based policy allows — the form recorded for another account's layer (#1272). Reaching
// the function would take its resource policy to grant the caller the action, and substrate does
// not evaluate a function's resource policy for a cross-account caller.
//
// A full ARN in another Region is ResourceNotFoundException/404, "Functions from '<arn region>' are
// not reachable in this region ('<request region>')". That is observed behavior: the sentence is
// quoted from Invoke and UpdateFunctionCode answers in Stack Overflow question 48250426 and
// aws/aws-sdk-cpp#332; no AWS page publishes it. A request is served by one Region's endpoint, and
// Lambda does not forward it to another.
func lambdaCheckFunctionScope(ctx *RequestContext, ref, action string) *AWSError {
	accountID, region := lambdaRefScope(ctx, ref)
	if accountID != ctx.AccountID {
		name, _ := lambdaParseFunctionRef(ref)
		return lambdaLayerDenied(ctx, action, lambdaFunctionARN(region, accountID, name))
	}
	if region != ctx.Region {
		return &AWSError{Code: "ResourceNotFoundException", HTTPStatus: http.StatusNotFound,
			Message: "Functions from '" + region + "' are not reachable in this region ('" + ctx.Region + "')"}
	}
	return nil
}

// lambdaUnqualifiedScope is [lambdaFunctionScope] for an operation whose FunctionName names a
// function and never one of its versions or aliases: PublishVersion, ListVersionsByFunction and the
// alias operations. A qualified FunctionName is InvalidParameterValueException; the message is
// substrate's own.
func lambdaUnqualifiedScope(ctx *RequestContext, ref, action string) (accountID, region, name string, awsErr *AWSError) {
	accountID, region, name, qualifier, awsErr := lambdaFunctionScope(ctx, ref, action)
	if awsErr != nil {
		return "", "", "", awsErr
	}
	if qualifier != "" {
		return "", "", "", lambdaInvalidParameterValue("FunctionName '" + ref +
			"' names a version or alias; " + strings.TrimPrefix(action, "lambda:") + " takes the function's name or ARN")
	}
	return accountID, region, name, nil
}

// lambdaInvokeTarget is the path and parameters of the Invoke request another service sends to the
// function it names by ref: a name, `name:qualifier`, or a function ARN with or without a version or
// alias. The reference without its version or alias goes in the path, and the version or alias in
// Qualifier, so an SNS subscription, an S3 notification, an API Gateway integration, an event source
// mapping or a Step Functions task naming an alias ARN invokes the alias. An ARN keeps its account
// and Region, so Invoke resolves the function it names, or refuses one outside the dispatching
// service's account and Region ([lambdaCheckFunctionScope]), rather than invoking a function of the
// same name in the caller's own account.
func lambdaInvokeTarget(ref string) (path string, params map[string]string) {
	_, qualifier := lambdaParseFunctionRef(ref)
	params = map[string]string{}
	if qualifier != "" {
		params["Qualifier"] = qualifier
		ref = strings.TrimSuffix(ref, ":"+qualifier)
	}
	return "/2015-03-31/functions/" + ref + "/invocations", params
}

// lambdaMergeQualifier combines the qualifier a FunctionName carries with a Qualifier parameter.
// The two must agree when both are given; the refusal is the one the Lambda developer guide's
// troubleshooting section shows for `{"FunctionName": "MyFunction:2", "Qualifier": "1"}`.
func lambdaMergeQualifier(derived, param string) (string, *AWSError) {
	switch {
	case derived != "" && param != "" && derived != param:
		return "", lambdaInvalidParameterValue(
			"The derived qualifier from the function name does not match the specified qualifier.")
	case param != "":
		return param, nil
	}
	return derived, nil
}

// lambdaIsVersionNumber reports whether a qualifier names a published version rather than an alias:
// a version is all digits, and an alias name may not be ("(?!^[0-9]+$)([a-zA-Z0-9-_]+)").
func lambdaIsVersionNumber(q string) bool {
	return q != "" && strings.Trim(q, "0123456789") == ""
}

// lambdaTarget is what a possibly-qualified function reference resolves to.
type lambdaTarget struct {
	// accountID and region own the function.
	accountID, region string
	// latest is the function's $LATEST record.
	latest LambdaFunction
	// qualifier is the version or alias the request named, empty when it named none.
	qualifier string
	// version is the version the reference resolves to: $LATEST or a published number. For an alias
	// it is the alias's FunctionVersion, before any weighted routing.
	version string
	// snapshot is the published version, nil when version is $LATEST.
	snapshot *LambdaFunctionVersion
	// alias is the alias the qualifier named, nil when it named none.
	alias *LambdaAlias
}

// function returns the code and configuration the target names.
func (t *lambdaTarget) function() LambdaFunction {
	if t.snapshot != nil {
		return t.snapshot.LambdaFunction
	}
	return t.latest
}

// configuration renders the FunctionConfiguration the target names. A qualified request answers
// the ARN it qualified — an alias's configuration carries the alias ARN and the version it points
// at — and an unqualified one the function's own ARN.
func (t *lambdaTarget) configuration() map[string]interface{} {
	var cfg map[string]interface{}
	if t.snapshot != nil {
		cfg = buildVersionConfig(*t.snapshot)
	} else {
		cfg = buildFunctionConfig(t.latest)
	}
	if t.qualifier != "" {
		cfg["FunctionArn"] = t.latest.FunctionArn + ":" + t.qualifier
	}
	return cfg
}

// lambdaQualifiedNotFound is the ResourceNotFoundException for a version or alias the function does
// not have. The text follows loadFunction's "Function not found: …", naming the qualified ARN.
func lambdaQualifiedNotFound(functionArn, qualifier string) *AWSError {
	return &AWSError{Code: "ResourceNotFoundException",
		Message: "Function not found: " + functionArn + ":" + qualifier, HTTPStatus: http.StatusNotFound}
}

// resolveFunction resolves a FunctionName from a request path, in any published form, together with
// the request's Qualifier parameter, to the function and the version it names. action is the IAM
// action a refusal of another account's function names.
func (p *LambdaPlugin) resolveFunction(ctx *RequestContext, ref, qualifierParam, action string) (*lambdaTarget, error) {
	acct, region, name, derived, awsErr := lambdaFunctionScope(ctx, ref, action)
	if awsErr != nil {
		return nil, awsErr
	}
	qualifier, awsErr := lambdaMergeQualifier(derived, qualifierParam)
	if awsErr != nil {
		return nil, awsErr
	}
	latest, err := p.loadFunction(acct, region, name)
	if err != nil {
		return nil, err
	}
	t := &lambdaTarget{accountID: acct, region: region, latest: latest, qualifier: qualifier, version: lambdaLatestVersion}
	switch {
	case qualifier == "" || qualifier == lambdaLatestVersion:
		return t, nil
	case lambdaIsVersionNumber(qualifier):
		v, err := p.loadVersion(acct, region, name, qualifier)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, lambdaQualifiedNotFound(latest.FunctionArn, qualifier)
		}
		t.version, t.snapshot = qualifier, v
		return t, nil
	}
	alias, err := p.loadAlias(acct, region, name, qualifier)
	if err != nil {
		return nil, err
	}
	if alias == nil {
		return nil, lambdaQualifiedNotFound(latest.FunctionArn, qualifier)
	}
	t.alias, t.version = alias, alias.FunctionVersion
	if alias.FunctionVersion != lambdaLatestVersion {
		v, err := p.loadVersion(acct, region, name, alias.FunctionVersion)
		if err != nil {
			return nil, err
		}
		if v == nil {
			return nil, lambdaQualifiedNotFound(latest.FunctionArn, alias.FunctionVersion)
		}
		t.snapshot = v
	}
	return t, nil
}

// loadVersion reads one published version, nil when the function has none of that number.
func (p *LambdaPlugin) loadVersion(accountID, region, name, version string) (*LambdaFunctionVersion, error) {
	data, err := p.state.Get(context.Background(), lambdaNamespace, lambdaVersionStateKey(accountID, region, name, version))
	if err != nil {
		return nil, fmt.Errorf("lambda loadVersion state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var v LambdaFunctionVersion
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("lambda loadVersion unmarshal: %w", err)
	}
	return &v, nil
}

// listVersions reads every published version of a function, in no particular order.
func (p *LambdaPlugin) listVersions(accountID, region, name string) ([]LambdaFunctionVersion, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, lambdaNamespace, lambdaVersionKeyPrefix(accountID, region, name))
	if err != nil {
		return nil, fmt.Errorf("lambda listVersions state.List: %w", err)
	}
	out := make([]LambdaFunctionVersion, 0, len(keys))
	for _, k := range keys {
		data, err := p.state.Get(goCtx, lambdaNamespace, k)
		if err != nil {
			return nil, fmt.Errorf("lambda listVersions state.Get: %w", err)
		}
		if data == nil {
			continue
		}
		var v LambdaFunctionVersion
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf("lambda listVersions unmarshal: %w", err)
		}
		out = append(out, v)
	}
	return out, nil
}

// lambdaVersionNumber reads a version's number; a record whose number does not parse sorts first.
func lambdaVersionNumber(v string) int {
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}

// lambdaFunctionFingerprint identifies a function's code and the configuration a version snapshots,
// so two publishes of the same $LATEST can be told apart from two of different ones. It leaves out
// what changes without the function changing: RevisionId, LastModified and tags.
func lambdaFunctionFingerprint(fn LambdaFunction) string {
	data, err := json.Marshal(struct {
		Runtime, Role, Handler, Description, PackageType, ImageURI, TracingMode, CodeSha256 string
		CodeS3Location                                                                      string
		Timeout, MemorySize                                                                 int
		CodeSize                                                                            int64
		Environment                                                                         map[string]string
		Architectures                                                                       []string
	}{fn.Runtime, fn.Role, fn.Handler, fn.Description, fn.PackageType, fn.ImageURI, fn.TracingMode, fn.CodeSha256,
		fn.CodeS3Location, fn.Timeout, fn.MemorySize, fn.CodeSize, fn.Environment, fn.Architectures})
	if err != nil {
		// A struct of strings, numbers, a string map and a string slice always marshals.
		panic(fmt.Sprintf("lambda fingerprint marshal: %v", err))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// lambdaCodeS3Location renders an S3 package's object as [LambdaFunction.CodeS3Location] records it.
func lambdaCodeS3Location(bucket, key, version string) string {
	return bucket + "/" + key + "/" + version
}

// publish snapshots fn, the function's $LATEST record, as a new version, or answers the newest
// version when nothing has changed since it was published. The caller holds p.versionMu.
//
// description overrides the function's description on the new version, as PublishVersion's
// Description does; it plays no part in deciding whether anything changed.
func (p *LambdaPlugin) publish(ctx *RequestContext, accountID, region string, fn LambdaFunction, description string) (LambdaFunctionVersion, error) {
	existing, err := p.listVersions(accountID, region, fn.FunctionName)
	if err != nil {
		return LambdaFunctionVersion{}, err
	}
	fingerprint := lambdaFunctionFingerprint(fn)
	newest := -1
	for i, v := range existing {
		if newest < 0 || lambdaVersionNumber(v.Version) > lambdaVersionNumber(existing[newest].Version) {
			newest = i
		}
	}
	if newest >= 0 && existing[newest].Fingerprint == fingerprint {
		return existing[newest], nil
	}

	goCtx := context.Background()
	seqKey := lambdaVersionSeqStateKey(accountID, region, fn.FunctionName)
	last := 0
	if data, err := p.state.Get(goCtx, lambdaNamespace, seqKey); err != nil {
		return LambdaFunctionVersion{}, fmt.Errorf("lambda publish seq state.Get: %w", err)
	} else if data != nil {
		if err := json.Unmarshal(data, &last); err != nil {
			return LambdaFunctionVersion{}, fmt.Errorf("lambda publish seq unmarshal: %w", err)
		}
	}
	if newest >= 0 {
		last = max(last, lambdaVersionNumber(existing[newest].Version))
	}
	next := last + 1

	v := LambdaFunctionVersion{LambdaFunction: fn, Version: strconv.Itoa(next), Fingerprint: fingerprint}
	v.FunctionArn = fn.FunctionArn + ":" + v.Version
	v.Tags, v.EverTagged = nil, false
	v.RevisionID = ctx.IDs.HexUUID()
	v.LastModified = p.tc.Now()
	if description != "" {
		v.Description = description
	}
	data, err := json.Marshal(v)
	if err != nil {
		return LambdaFunctionVersion{}, fmt.Errorf("lambda publish marshal: %w", err)
	}
	if err := p.state.Put(goCtx, lambdaNamespace, lambdaVersionStateKey(accountID, region, fn.FunctionName, v.Version), data); err != nil {
		return LambdaFunctionVersion{}, fmt.Errorf("lambda publish state.Put: %w", err)
	}
	seq, err := json.Marshal(next)
	if err != nil {
		return LambdaFunctionVersion{}, fmt.Errorf("lambda publish seq marshal: %w", err)
	}
	if err := p.state.Put(goCtx, lambdaNamespace, seqKey, seq); err != nil {
		return LambdaFunctionVersion{}, fmt.Errorf("lambda publish seq state.Put: %w", err)
	}
	return v, nil
}

// buildVersionConfig renders a published version as a FunctionConfiguration: the snapshot, with
// its own number and its qualified ARN.
func buildVersionConfig(v LambdaFunctionVersion) map[string]interface{} {
	cfg := buildFunctionConfig(v.LambdaFunction)
	cfg["Version"] = v.Version
	return cfg
}

// lambdaPreconditionFailed is the PreconditionFailedException a RevisionId that is not the current
// one answers. The message is the error's own published description.
func lambdaPreconditionFailed() *AWSError {
	return &AWSError{Code: "PreconditionFailedException",
		// API_PublishVersion and API_UpdateAlias, Errors: PreconditionFailedException.
		Message:    "The RevisionId provided does not match the latest RevisionId for the Lambda function or alias.",
		HTTPStatus: http.StatusPreconditionFailed}
}

// publishVersion answers PublishVersion: "POST /2015-03-31/functions/{FunctionName}/versions".
//
// RevisionId, when given, must be $LATEST's current one, and CodeSha256 must be its current digest:
// both exist "to avoid publishing a version if the function … has changed since you last updated
// it". A stale RevisionId is the PreconditionFailedException the page publishes for it. The page
// names no code for a stale CodeSha256, so it is InvalidParameterValueException, the page's code for
// a parameter that is not valid; that message is substrate's own.
func (p *LambdaPlugin) publishVersion(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	var body struct {
		CodeSha256  string `json:"CodeSha256"`
		Description string `json:"Description"`
		RevisionID  string `json:"RevisionId"`
		PublishTo   string `json:"PublishTo"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, lambdaInvalidBody()
		}
	}
	if body.PublishTo != "" && body.PublishTo != "LATEST_PUBLISHED" {
		return nil, lambdaInvalidParameterValue("PublishTo must be LATEST_PUBLISHED")
	}
	if len(body.Description) > 256 {
		return nil, lambdaInvalidParameterValue("Description must be at most 256 characters")
	}
	acct, region, name, awsErr := lambdaUnqualifiedScope(ctx, ref, "lambda:PublishVersion")
	if awsErr != nil {
		return nil, awsErr
	}

	p.versionMu.Lock()
	defer p.versionMu.Unlock()
	fn, err := p.loadFunction(acct, region, name)
	if err != nil {
		return nil, err
	}
	if body.RevisionID != "" && body.RevisionID != fn.RevisionID {
		return nil, lambdaPreconditionFailed()
	}
	if body.CodeSha256 != "" && body.CodeSha256 != fn.CodeSha256 {
		return nil, lambdaInvalidParameterValue("CodeSha256 " + body.CodeSha256 +
			" does not match the function's current CodeSha256 " + fn.CodeSha256)
	}
	v, err := p.publish(ctx, acct, region, fn, body.Description)
	if err != nil {
		return nil, err
	}
	return lambdaJSONResponse(http.StatusCreated, buildVersionConfig(v))
}

// lambdaVersionItem is one entry of a ListVersionsByFunction listing with its cursor position:
// $LATEST first, then the published versions in numeric order.
type lambdaVersionItem struct {
	key cursorKey
	cfg map[string]interface{}
}

// listVersionsByFunction answers a function's versions: $LATEST, whose qualified FunctionArn ends
// ":$LATEST" as the reference's CLI example shows, then each published version in numeric order —
// the order the Terraform AWS provider relies on when it takes the last element as the latest
// version. Pages are cut by [lambdaMarkerPage], whose checks come before the function is looked up,
// so the answer to a malformed request does not depend on what exists (#887).
func (p *LambdaPlugin) listVersionsByFunction(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	acct, region, name, awsErr := lambdaUnqualifiedScope(ctx, ref, "lambda:ListVersionsByFunction")
	if awsErr != nil {
		return nil, awsErr
	}
	pr, awsErr := lambdaMarkerPage(req, "ListVersionsByFunction",
		map[string][]string{"FunctionName": {acct + "/" + region + "/" + name}}, lambdaVersionsPageCap)
	if awsErr != nil {
		return nil, awsErr
	}
	fn, err := p.loadFunction(acct, region, name)
	if err != nil {
		return nil, err
	}
	versions, err := p.listVersions(acct, region, name)
	if err != nil {
		return nil, err
	}
	latest := buildFunctionConfig(fn)
	latest["FunctionArn"] = fn.FunctionArn + ":" + lambdaLatestVersion
	items := []lambdaVersionItem{{cfg: latest}}
	for _, v := range versions {
		// Zero-padded so the cursor's string order is the numeric order; $LATEST's empty key sorts
		// before every number.
		items = append(items, lambdaVersionItem{
			key: cursorKey{Sort: fmt.Sprintf("%020d", lambdaVersionNumber(v.Version))},
			cfg: buildVersionConfig(v),
		})
	}
	res, err := page(items, func(it lambdaVersionItem) cursorKey { return it.key }, pr)
	if err != nil {
		return nil, lambdaInvalidParameterValue("the Marker '" + req.Params["Marker"] + "' is invalid")
	}
	out := struct {
		Versions   []map[string]interface{} `json:"Versions"`
		NextMarker string                   `json:"NextMarker,omitempty"`
	}{Versions: make([]map[string]interface{}, 0, len(res.Items)), NextMarker: res.Next}
	for _, it := range res.Items {
		out.Versions = append(out.Versions, it.cfg)
	}
	return lambdaJSONResponse(http.StatusOK, out)
}

// getFunctionConfiguration answers "GET /2015-03-31/functions/{FunctionName}/configuration", "the
// version-specific settings of a Lambda function or version" (#1660): $LATEST's, or with a
// Qualifier, or a FunctionName carrying one, the version's or the alias's version's.
func (p *LambdaPlugin) getFunctionConfiguration(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	t, err := p.resolveFunction(ctx, ref, req.Params["Qualifier"], "lambda:GetFunctionConfiguration")
	if err != nil {
		return nil, err
	}
	return lambdaJSONResponse(http.StatusOK, t.configuration())
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

// deleteFunctionVersion deletes one published version and its resource policy. A version an alias
// still routes to is refused: "You can't delete a version that an alias references." The page names
// ResourceConflictException for a conflicting operation; the message is substrate's own.
func (p *LambdaPlugin) deleteFunctionVersion(accountID, region string, fn LambdaFunction, version string) (*AWSResponse, error) {
	p.versionMu.Lock()
	defer p.versionMu.Unlock()
	v, err := p.loadVersion(accountID, region, fn.FunctionName, version)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, lambdaQualifiedNotFound(fn.FunctionArn, version)
	}
	aliases, err := p.listAliases(accountID, region, fn.FunctionName)
	if err != nil {
		return nil, err
	}
	for _, a := range aliases {
		if _, weighted := a.AdditionalVersionWeights[version]; a.FunctionVersion == version || weighted {
			return nil, &AWSError{Code: "ResourceConflictException", HTTPStatus: http.StatusConflict,
				Message: "Version " + version + " of function " + fn.FunctionName + " is referenced by alias " + a.Name}
		}
	}
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, lambdaNamespace, lambdaVersionStateKey(accountID, region, fn.FunctionName, version)); err != nil {
		return nil, fmt.Errorf("lambda deleteFunctionVersion state.Delete: %w", err)
	}
	if err := p.state.Delete(goCtx, lambdaNamespace, lambdaQualifiedPolicyStateKey(accountID, region, fn.FunctionName, version)); err != nil {
		return nil, fmt.Errorf("lambda deleteFunctionVersion policy state.Delete: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}

// deleteFunctionChildren deletes everything a function holds besides its own record: every
// published version, every alias, every version's and alias's resource policy, and the version
// counter — DeleteFunction without a Qualifier deletes "all versions and aliases".
func (p *LambdaPlugin) deleteFunctionChildren(accountID, region, name string) error {
	goCtx := context.Background()
	for _, prefix := range []string{
		lambdaVersionKeyPrefix(accountID, region, name),
		lambdaAliasKeyPrefix(accountID, region, name),
		lambdaPolicyStateKey(accountID, region, name) + ":",
	} {
		keys, err := p.state.List(goCtx, lambdaNamespace, prefix)
		if err != nil {
			return fmt.Errorf("lambda deleteFunctionChildren state.List: %w", err)
		}
		for _, k := range keys {
			if err := p.state.Delete(goCtx, lambdaNamespace, k); err != nil {
				return fmt.Errorf("lambda deleteFunctionChildren state.Delete: %w", err)
			}
		}
	}
	if err := p.state.Delete(goCtx, lambdaNamespace, lambdaVersionSeqStateKey(accountID, region, name)); err != nil {
		return fmt.Errorf("lambda deleteFunctionChildren seq state.Delete: %w", err)
	}
	return nil
}
