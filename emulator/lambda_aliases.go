package emulator

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
)

// Function aliases (#1613).
//
// An alias is "a function identifier that you can update to invoke a different version". Substrate
// stores each one at lambdaAliasStateKey, and every request that takes a qualifier resolves an alias
// name to the version it points at ([LambdaPlugin.resolveFunction]).
//
// # Weighted routing
//
// RoutingConfig.AdditionalVersionWeights sends a share of an alias's invocations to a second
// version. Lambda "uses a simple probabilistic model to distribute the traffic between the two
// function versions"; substrate draws the same proportion from the invoking request's [IDMint]
// rather than from a random source ([lambdaRouteAlias]). The draw is a function of the request id,
// so a recorded invocation replays onto the version it ran on, and over many invocations the
// shares approach the configured weights. A weight of 0 never routes to the additional version
// and a weight of 1 always does, which is how a test pins one branch.
//
// The rules a weighted alias must satisfy come from the developer guide's weighted-alias page: "You
// can point an alias to a maximum of two Lambda function versions", "Both versions must have the
// same execution role", and "Both versions must be published. The alias cannot point to $LATEST."
// Each is refused with InvalidParameterValueException, whose messages are substrate's own. The
// dead-letter-queue rule is not checked, because substrate models no dead-letter configuration.

// lambdaAliasNamePattern is an alias name's published pattern, `(?!^[0-9]+$)([a-zA-Z0-9-_]+)`, with
// its length bound of 1 to 128. Go's regexp has no lookahead, so the all-digits exclusion is
// [lambdaIsVersionNumber].
var lambdaAliasNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

// lambdaAliasVersionPattern is FunctionVersion's pattern, `(\$LATEST|[0-9]+)`, as the response
// publishes it. Its 1024-character bound is beyond any version number a function can reach.
var lambdaAliasVersionPattern = regexp.MustCompile(`^(\$LATEST|[0-9]+)$`)

// lambdaAliasWeightKeyPattern is AdditionalVersionWeights' published key pattern, `[0-9]+`.
var lambdaAliasWeightKeyPattern = regexp.MustCompile(`^[0-9]+$`)

// lambdaAliasesPageCap is the most aliases one ListAliases response carries. AWS publishes no cap
// for ListAliases; substrate uses ListVersionsByFunction's 50, and an absent MaxItems is the same.
const lambdaAliasesPageCap = 50

// lambdaRoutingConfig is AliasRoutingConfiguration on the wire.
type lambdaRoutingConfig struct {
	AdditionalVersionWeights map[string]float64 `json:"AdditionalVersionWeights"`
}

// lambdaAliasWire is AliasConfiguration on the wire. RoutingConfig is omitted when the alias routes
// to one version, so a caller that never set one reads none back.
type lambdaAliasWire struct {
	AliasArn        string               `json:"AliasArn"`
	Description     string               `json:"Description"`
	FunctionVersion string               `json:"FunctionVersion"`
	Name            string               `json:"Name"`
	RevisionID      string               `json:"RevisionId"`
	RoutingConfig   *lambdaRoutingConfig `json:"RoutingConfig,omitempty"`
}

// wire renders the alias as AliasConfiguration.
func (a LambdaAlias) wire() lambdaAliasWire {
	w := lambdaAliasWire{AliasArn: a.AliasArn, Description: a.Description, FunctionVersion: a.FunctionVersion,
		Name: a.Name, RevisionID: a.RevisionID}
	if len(a.AdditionalVersionWeights) > 0 {
		w.RoutingConfig = &lambdaRoutingConfig{AdditionalVersionWeights: a.AdditionalVersionWeights}
	}
	return w
}

// lambdaCheckAliasName validates an alias name against its published pattern.
func lambdaCheckAliasName(name string) *AWSError {
	if !lambdaAliasNamePattern.MatchString(name) || lambdaIsVersionNumber(name) {
		return lambdaInvalidParameterValue("Name '" + name + "' must match (?!^[0-9]+$)([a-zA-Z0-9-_]+) and be 1 to 128 characters")
	}
	return nil
}

// lambdaCheckAliasShape validates what an alias request says on its own, before any state is read:
// the version's pattern, the description's length, and the routing configuration's shape.
func lambdaCheckAliasShape(version string, description *string, routing *lambdaRoutingConfig) *AWSError {
	if version != "" && !lambdaAliasVersionPattern.MatchString(version) {
		return lambdaInvalidParameterValue("FunctionVersion '" + version + "' must be $LATEST or a version number")
	}
	if description != nil && len(*description) > 256 {
		return lambdaInvalidParameterValue("Description must be at most 256 characters")
	}
	if routing == nil {
		return nil
	}
	if len(routing.AdditionalVersionWeights) > 1 {
		return lambdaInvalidParameterValue("an alias can route to at most one additional version")
	}
	for v, w := range routing.AdditionalVersionWeights {
		if !lambdaAliasWeightKeyPattern.MatchString(v) {
			return lambdaInvalidParameterValue("AdditionalVersionWeights key '" + v + "' must be a version number")
		}
		if w < 0 || w > 1 {
			return lambdaInvalidParameterValue(fmt.Sprintf("AdditionalVersionWeights[%s] must be between 0.0 and 1.0, was %v", v, w))
		}
	}
	return nil
}

// checkAliasTargets checks that the versions an alias would point at exist and satisfy the
// weighted-alias rules. A version the function has not published is the ResourceNotFoundException
// a qualified lookup answers.
func (p *LambdaPlugin) checkAliasTargets(accountID, region string, fn LambdaFunction, version string, weights map[string]float64) error {
	var primary *LambdaFunctionVersion
	if version != lambdaLatestVersion {
		v, err := p.loadVersion(accountID, region, fn.FunctionName, version)
		if err != nil {
			return err
		}
		if v == nil {
			return lambdaQualifiedNotFound(fn.FunctionArn, version)
		}
		primary = v
	}
	for extra := range weights {
		if primary == nil {
			return lambdaInvalidParameterValue("a weighted alias must point to a published version, not $LATEST")
		}
		if extra == version {
			// "A maximum of two Lambda function versions": the additional version is the second one.
			return lambdaInvalidParameterValue("AdditionalVersionWeights names version " + extra +
				", the alias's own FunctionVersion; the additional version must be a different one")
		}
		v, err := p.loadVersion(accountID, region, fn.FunctionName, extra)
		if err != nil {
			return err
		}
		if v == nil {
			return lambdaQualifiedNotFound(fn.FunctionArn, extra)
		}
		if v.Role != primary.Role {
			return lambdaInvalidParameterValue("versions " + version + " and " + extra +
				" of a weighted alias must have the same execution role")
		}
	}
	return nil
}

// loadAlias reads one alias, nil when the function has none of that name.
func (p *LambdaPlugin) loadAlias(accountID, region, name, alias string) (*LambdaAlias, error) {
	data, err := p.state.Get(context.Background(), lambdaNamespace, lambdaAliasStateKey(accountID, region, name, alias))
	if err != nil {
		return nil, fmt.Errorf("lambda loadAlias state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var a LambdaAlias
	if err := json.Unmarshal(data, &a); err != nil {
		return nil, fmt.Errorf("lambda loadAlias unmarshal: %w", err)
	}
	return &a, nil
}

// saveAlias writes one alias.
func (p *LambdaPlugin) saveAlias(accountID, region string, a LambdaAlias) error {
	data, err := json.Marshal(a)
	if err != nil {
		return fmt.Errorf("lambda saveAlias marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), lambdaNamespace, lambdaAliasStateKey(accountID, region, a.FunctionName, a.Name), data); err != nil {
		return fmt.Errorf("lambda saveAlias state.Put: %w", err)
	}
	return nil
}

// listAliases reads every alias of a function, in name order.
func (p *LambdaPlugin) listAliases(accountID, region, name string) ([]LambdaAlias, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, lambdaNamespace, lambdaAliasKeyPrefix(accountID, region, name))
	if err != nil {
		return nil, fmt.Errorf("lambda listAliases state.List: %w", err)
	}
	slices.Sort(keys)
	out := make([]LambdaAlias, 0, len(keys))
	for _, k := range keys {
		data, err := p.state.Get(goCtx, lambdaNamespace, k)
		if err != nil {
			return nil, fmt.Errorf("lambda listAliases state.Get: %w", err)
		}
		if data == nil {
			continue
		}
		var a LambdaAlias
		if err := json.Unmarshal(data, &a); err != nil {
			return nil, fmt.Errorf("lambda listAliases unmarshal: %w", err)
		}
		out = append(out, a)
	}
	return out, nil
}

// lambdaAliasNotFound is the ResourceNotFoundException for an alias the function does not have. The
// message is substrate's own.
func lambdaAliasNotFound(aliasArn string) *AWSError {
	return &AWSError{Code: "ResourceNotFoundException", Message: "Alias not found: " + aliasArn,
		HTTPStatus: http.StatusNotFound}
}

// createAlias answers CreateAlias: "POST /2015-03-31/functions/{FunctionName}/aliases".
//
// An alias that already exists is the ResourceConflictException the page publishes for "the
// resource already exists"; the message is substrate's own.
func (p *LambdaPlugin) createAlias(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	var body struct {
		Name            string               `json:"Name"`
		FunctionVersion string               `json:"FunctionVersion"`
		Description     string               `json:"Description"`
		RoutingConfig   *lambdaRoutingConfig `json:"RoutingConfig"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, lambdaInvalidBody()
	}
	if awsErr := lambdaCheckAliasName(body.Name); awsErr != nil {
		return nil, awsErr
	}
	if body.FunctionVersion == "" {
		return nil, lambdaInvalidParameterValue("FunctionVersion is required")
	}
	if awsErr := lambdaCheckAliasShape(body.FunctionVersion, &body.Description, body.RoutingConfig); awsErr != nil {
		return nil, awsErr
	}
	acct, region, name, awsErr := lambdaUnqualifiedScope(ctx, ref, "lambda:CreateAlias")
	if awsErr != nil {
		return nil, awsErr
	}

	p.versionMu.Lock()
	defer p.versionMu.Unlock()
	fn, err := p.loadFunction(acct, region, name)
	if err != nil {
		return nil, err
	}
	existing, err := p.loadAlias(acct, region, name, body.Name)
	if err != nil {
		return nil, err
	}
	aliasArn := fn.FunctionArn + ":" + body.Name
	if existing != nil {
		return nil, &AWSError{Code: "ResourceConflictException", Message: "Alias already exists: " + aliasArn,
			HTTPStatus: http.StatusConflict}
	}
	var weights map[string]float64
	if body.RoutingConfig != nil && len(body.RoutingConfig.AdditionalVersionWeights) > 0 {
		weights = body.RoutingConfig.AdditionalVersionWeights
	}
	if err := p.checkAliasTargets(acct, region, fn, body.FunctionVersion, weights); err != nil {
		return nil, err
	}
	a := LambdaAlias{Name: body.Name, FunctionName: name, AliasArn: aliasArn, FunctionVersion: body.FunctionVersion,
		Description: body.Description, AdditionalVersionWeights: weights, RevisionID: ctx.IDs.HexUUID()}
	if err := p.saveAlias(acct, region, a); err != nil {
		return nil, err
	}
	return lambdaJSONResponse(http.StatusCreated, a.wire())
}

// getAlias answers GetAlias: "GET /2015-03-31/functions/{FunctionName}/aliases/{Name}".
func (p *LambdaPlugin) getAlias(ctx *RequestContext, ref, aliasName string) (*AWSResponse, error) {
	acct, region, name, awsErr := lambdaUnqualifiedScope(ctx, ref, "lambda:GetAlias")
	if awsErr != nil {
		return nil, awsErr
	}
	fn, err := p.loadFunction(acct, region, name)
	if err != nil {
		return nil, err
	}
	a, err := p.loadAlias(acct, region, name, aliasName)
	if err != nil {
		return nil, err
	}
	if a == nil {
		return nil, lambdaAliasNotFound(fn.FunctionArn + ":" + aliasName)
	}
	return lambdaJSONResponse(http.StatusOK, a.wire())
}

// updateAlias answers UpdateAlias: "PUT /2015-03-31/functions/{FunctionName}/aliases/{Name}".
//
// Each member is optional and replaces only what it names: a RoutingConfig replaces the routing,
// and an empty AdditionalVersionWeights clears it, as the guide's `--routing-config
// AdditionalVersionWeights={}` example does. A RevisionId that is not the alias's current one is the
// PreconditionFailedException the page publishes.
func (p *LambdaPlugin) updateAlias(ctx *RequestContext, req *AWSRequest, ref, aliasName string) (*AWSResponse, error) {
	var body struct {
		FunctionVersion string               `json:"FunctionVersion"`
		Description     *string              `json:"Description"`
		RevisionID      string               `json:"RevisionId"`
		RoutingConfig   *lambdaRoutingConfig `json:"RoutingConfig"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, lambdaInvalidBody()
		}
	}
	if awsErr := lambdaCheckAliasShape(body.FunctionVersion, body.Description, body.RoutingConfig); awsErr != nil {
		return nil, awsErr
	}
	acct, region, name, awsErr := lambdaUnqualifiedScope(ctx, ref, "lambda:UpdateAlias")
	if awsErr != nil {
		return nil, awsErr
	}

	p.versionMu.Lock()
	defer p.versionMu.Unlock()
	fn, err := p.loadFunction(acct, region, name)
	if err != nil {
		return nil, err
	}
	a, err := p.loadAlias(acct, region, name, aliasName)
	if err != nil {
		return nil, err
	}
	if a == nil {
		return nil, lambdaAliasNotFound(fn.FunctionArn + ":" + aliasName)
	}
	if body.RevisionID != "" && body.RevisionID != a.RevisionID {
		return nil, lambdaPreconditionFailed()
	}
	if body.FunctionVersion != "" {
		a.FunctionVersion = body.FunctionVersion
	}
	if body.Description != nil {
		a.Description = *body.Description
	}
	if body.RoutingConfig != nil {
		a.AdditionalVersionWeights = nil
		if len(body.RoutingConfig.AdditionalVersionWeights) > 0 {
			a.AdditionalVersionWeights = body.RoutingConfig.AdditionalVersionWeights
		}
	}
	if err := p.checkAliasTargets(acct, region, fn, a.FunctionVersion, a.AdditionalVersionWeights); err != nil {
		return nil, err
	}
	a.RevisionID = ctx.IDs.HexUUID()
	if err := p.saveAlias(acct, region, *a); err != nil {
		return nil, err
	}
	return lambdaJSONResponse(http.StatusOK, a.wire())
}

// deleteAlias answers DeleteAlias: "DELETE /2015-03-31/functions/{FunctionName}/aliases/{Name}",
// and the alias's resource policy with it.
//
// The page's Errors section lists no ResourceNotFoundException, so deleting an alias the function
// does not have succeeds with the 204 a delete answers. The function itself must exist, as for
// every other alias operation.
func (p *LambdaPlugin) deleteAlias(ctx *RequestContext, ref, aliasName string) (*AWSResponse, error) {
	acct, region, name, awsErr := lambdaUnqualifiedScope(ctx, ref, "lambda:DeleteAlias")
	if awsErr != nil {
		return nil, awsErr
	}
	p.versionMu.Lock()
	defer p.versionMu.Unlock()
	if _, err := p.loadFunction(acct, region, name); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, lambdaNamespace, lambdaAliasStateKey(acct, region, name, aliasName)); err != nil {
		return nil, fmt.Errorf("lambda deleteAlias state.Delete: %w", err)
	}
	if err := p.state.Delete(goCtx, lambdaNamespace, lambdaQualifiedPolicyStateKey(acct, region, name, aliasName)); err != nil {
		return nil, fmt.Errorf("lambda deleteAlias policy state.Delete: %w", err)
	}
	if err := p.state.Delete(goCtx, lambdaNamespace, lambdaQualifiedInvokeConfigStateKey(acct, region, name, aliasName)); err != nil {
		return nil, fmt.Errorf("lambda deleteAlias invoke config state.Delete: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}

// listAliasesResponse answers ListAliases: "GET /2015-03-31/functions/{FunctionName}/aliases", in
// name order, optionally narrowed to the aliases that invoke one FunctionVersion. An alias invokes
// a version when it is the alias's FunctionVersion or its weighted additional version.
func (p *LambdaPlugin) listAliasesResponse(ctx *RequestContext, req *AWSRequest, ref string) (*AWSResponse, error) {
	acct, region, name, awsErr := lambdaUnqualifiedScope(ctx, ref, "lambda:ListAliases")
	if awsErr != nil {
		return nil, awsErr
	}
	version := req.Params["FunctionVersion"]
	if version != "" && !lambdaAliasVersionPattern.MatchString(version) {
		return nil, lambdaInvalidParameterValue("FunctionVersion '" + version + "' must be $LATEST or a version number")
	}
	pr, awsErr := lambdaMarkerPage(req, "ListAliases",
		map[string][]string{"FunctionName": {acct + "/" + region + "/" + name}, "FunctionVersion": {version}},
		lambdaAliasesPageCap)
	if awsErr != nil {
		return nil, awsErr
	}
	if _, err := p.loadFunction(acct, region, name); err != nil {
		return nil, err
	}
	aliases, err := p.listAliases(acct, region, name)
	if err != nil {
		return nil, err
	}
	if version != "" {
		aliases = slices.DeleteFunc(aliases, func(a LambdaAlias) bool {
			_, weighted := a.AdditionalVersionWeights[version]
			return a.FunctionVersion != version && !weighted
		})
	}
	res, err := page(aliases, func(a LambdaAlias) cursorKey { return cursorKey{Sort: a.Name} }, pr)
	if err != nil {
		return nil, lambdaInvalidParameterValue("the Marker '" + req.Params["Marker"] + "' is invalid")
	}
	out := struct {
		Aliases    []lambdaAliasWire `json:"Aliases"`
		NextMarker string            `json:"NextMarker,omitempty"`
	}{Aliases: make([]lambdaAliasWire, 0, len(res.Items)), NextMarker: res.Next}
	for _, a := range res.Items {
		out.Aliases = append(out.Aliases, a.wire())
	}
	return lambdaJSONResponse(http.StatusOK, out)
}

// lambdaRouteAlias picks the version one invocation through alias runs.
//
// With no routing configuration it is the alias's FunctionVersion. With one, a uniform value in
// [0, 1) is drawn from the request's [IDMint] — four mint bytes read as a big-endian integer over
// 2^32 — and the additional version runs when the value is below its weight. The mint derives from
// the request id, so the choice is reproducible: replaying the request draws the same value and
// routes to the same version.
func lambdaRouteAlias(ids *IDMint, alias *LambdaAlias) string {
	for extra, weight := range alias.AdditionalVersionWeights {
		raw, err := hex.DecodeString(ids.Hex(4))
		if err != nil || len(raw) != 4 {
			// Hex always renders decodable hex of the requested width.
			panic(fmt.Sprintf("lambda routing draw: %v", err))
		}
		if float64(binary.BigEndian.Uint32(raw))/(1<<32) < weight {
			return extra
		}
	}
	return alias.FunctionVersion
}
