package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// CloudFrontPlugin emulates the Amazon CloudFront REST/XML API.
// It handles distribution lifecycle, invalidation, and tagging operations.
// CloudFront is a global service — distributions are stored keyed by account
// ID only, without a region component.
type CloudFrontPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "cloudfront".
func (p *CloudFrontPlugin) Name() string { return "cloudfront" }

// Initialize sets up the CloudFrontPlugin with the provided configuration.
func (p *CloudFrontPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for CloudFrontPlugin.
func (p *CloudFrontPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a CloudFront REST/XML request to the appropriate handler.
// The operation is derived from the HTTP method and URL path.
func (p *CloudFrontPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, resourceID := parseCloudFrontOperation(requestMethod(req), req.Path, req.Params)
	// Handle GetInvalidation (op includes invID after colon).
	if strings.HasPrefix(op, "GetInvalidation:") {
		invID := strings.TrimPrefix(op, "GetInvalidation:")
		return p.getInvalidation(ctx, resourceID, invID)
	}
	switch op {
	case "CreateDistribution":
		return p.createDistribution(ctx, req)
	case "CreateDistributionWithTags":
		return p.createDistributionWithTags(ctx, req)
	case "GetDistribution":
		return p.getDistribution(ctx, req, resourceID)
	case "GetDistributionConfig":
		return p.getDistributionConfig(ctx, req, resourceID)
	case "UpdateDistribution":
		return p.updateDistribution(ctx, req, resourceID)
	case "DeleteDistribution":
		return p.deleteDistribution(ctx, req, resourceID)
	case "ListDistributions":
		return p.listDistributions(ctx, req)
	case "CreateInvalidation":
		return p.createInvalidation(ctx, req, resourceID)
	case "ListInvalidations":
		return p.listInvalidations(ctx, resourceID)
	case "CreateOriginAccessControl":
		return p.createOriginAccessControl(ctx, req)
	case "GetOriginAccessControl":
		return p.getOriginAccessControl(ctx, resourceID)
	case "ListOriginAccessControls":
		return p.listOriginAccessControls(ctx)
	case "DeleteOriginAccessControl":
		return p.deleteOriginAccessControl(ctx, req, resourceID)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	default:
		return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
	}
}

// parseCloudFrontOperation derives the CloudFront operation name and optional
// resource ID from the HTTP method, URL path, and query parameters.
//
// The second return is a distribution ID for every operation on /distribution and an origin
// access control ID for every operation on /origin-access-control; it is named resourceID rather
// than distID because the two path families now share it, and a name that says "distribution"
// would invite a caller to pass an OAC's ID into a distribution lookup.
func parseCloudFrontOperation(method, path string, params map[string]string) (op, resourceID string) {
	// Normalise path: strip trailing slash and leading "/2020-05-31".
	const apiVersion = "/2020-05-31"
	p2 := strings.TrimSuffix(path, "/")
	p2 = strings.TrimPrefix(p2, apiVersion)

	// Tagging is addressed by query string rather than by path shape. The CloudFront
	// API Reference publishes all three as the same "/tagging" path: ListTagsForResource
	// is "GET /2020-05-31/tagging?Resource={{Resource}}", TagResource is
	// "POST /2020-05-31/tagging?Operation=Tag" and UntagResource is
	// "POST /2020-05-31/tagging?Operation=Untag".
	//
	// A POST resolves on the Operation value and on nothing else. Mapping every
	// Resource-bearing POST to TagResource is what sent an Operation=Untag request into
	// [CloudFrontPlugin.tagResource], which read the <TagKeys> body as <Tags>, matched no
	// Tag element, discarded the decode error and wrote the distribution back
	// byte-identical — answering the tagging success while the tag a caller asked to
	// remove was still there (#883). An Operation value substrate does not recognize now
	// resolves to no operation at all, so the request keeps its verb, reaches
	// [CloudFrontPlugin.HandleRequest]'s default arm and is refused: a write is never the
	// fallback for a word this function cannot name.
	//
	// "/tags" is accepted alongside the documented "/tagging"; it predates #883 and is
	// substrate tolerance, not a path AWS publishes.
	if p2 == "/tagging" || p2 == "/tags" || params["Resource"] != "" {
		switch method {
		case http.MethodGet:
			return "ListTagsForResource", ""
		case http.MethodPost:
			switch {
			case strings.EqualFold(params["Operation"], "Tag"):
				return "TagResource", ""
			case strings.EqualFold(params["Operation"], "Untag"):
				return "UntagResource", ""
			}
			return "", ""
		}
	}

	// The origin access control family, whose four operations are addressed by path shape
	// alone: POST and GET on the collection, GET and DELETE on one member.
	const oacPath = "/origin-access-control"
	if p2 == oacPath || strings.HasPrefix(p2, oacPath+"/") {
		oacID := strings.TrimPrefix(strings.TrimPrefix(p2, oacPath), "/")
		switch {
		case oacID == "" && method == http.MethodPost:
			return "CreateOriginAccessControl", ""
		case oacID == "" && method == http.MethodGet:
			return "ListOriginAccessControls", ""
		case method == http.MethodGet:
			return "GetOriginAccessControl", oacID
		case method == http.MethodDelete:
			return "DeleteOriginAccessControl", oacID
		}
		// A method the family does not publish for this path — a PUT, which would be
		// UpdateOriginAccessControl, an operation substrate does not implement — resolves to
		// no operation and is refused by the default arm rather than falling through to the
		// distribution parsing below.
		return "", oacID
	}

	switch {
	// CreateDistributionWithTags is the same method and path as CreateDistribution,
	// distinguished only by the WithTags query parameter: the reference publishes it as
	// "POST /2020-05-31/distribution?WithTags HTTP/1.1". The key carries no value, so the test
	// is for its *presence* — the parser substitutes the sentinel "1" for a bare query key,
	// and testing for a particular value would route a request AWS accepts to the untagged
	// create, silently dropping the tags the caller sent in the same body.
	case p2 == "/distribution" && method == http.MethodPost && cfHasWithTags(params):
		return "CreateDistributionWithTags", ""
	case p2 == "/distribution" && method == http.MethodPost:
		return "CreateDistribution", ""
	case p2 == "/distribution" && method == http.MethodGet:
		return "ListDistributions", ""
	}

	// Paths of the form /distribution/{id}[/...]
	const distPrefix = "/distribution/"
	if !strings.HasPrefix(p2, distPrefix) {
		return "", ""
	}
	rest := p2[len(distPrefix):]

	// Extract the distribution ID (first segment).
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		// /distribution/{id}
		switch method {
		case http.MethodGet:
			return "GetDistribution", rest
		case http.MethodPut:
			return "UpdateDistribution", rest
		case http.MethodDelete:
			return "DeleteDistribution", rest
		}
		return "", rest
	}

	id := rest[:slash]
	suffix := rest[slash+1:]

	switch suffix {
	case "config":
		switch method {
		case http.MethodGet:
			return "GetDistributionConfig", id
		case http.MethodPut:
			return "UpdateDistribution", id
		}
	case "invalidation":
		if method == http.MethodPost {
			return "CreateInvalidation", id
		}
		if method == http.MethodGet {
			return "ListInvalidations", id
		}
	}

	// /distribution/{id}/invalidation/{invID}
	if strings.HasPrefix(suffix, "invalidation/") {
		invID := strings.TrimPrefix(suffix, "invalidation/")
		if method == http.MethodGet && invID != "" {
			return "GetInvalidation:" + invID, id
		}
	}
	return "", id
}

// cfHasWithTags reports whether a request's query string carries the WithTags key that selects
// CreateDistributionWithTags.
//
// Presence, not value: the key is published bare ("?WithTags"), and a bare key reaches a plugin as
// the sentinel value "1" (parser.go). The comparison folds case because the cost of the two
// directions is not symmetric — treating "withtags" as absent creates an untagged distribution from
// a body that asked for tags and reports 201, while treating it as present routes a request to a
// decoder that refuses anything but a DistributionConfigWithTags body.
func cfHasWithTags(params map[string]string) bool {
	for key := range params {
		if strings.EqualFold(key, "WithTags") {
			return true
		}
	}
	return false
}

// --- Distribution operations ------------------------------------------------

// cfDistributionConfigBody is the part of a DistributionConfig substrate records: the two members
// CreateDistribution has always decoded. docs/services.md's "A configuration is not a
// distribution" section is the standing note on the three required members that are not here and
// on what that costs; #1271 is where the rest of the configuration starts being recorded.
//
// It is a named type rather than an anonymous struct because CreateDistributionWithTags decodes
// the same document one level down, inside DistributionConfigWithTags, and the two must decode it
// identically — a member the tagged create reads from a different element name would make the two
// creates record different distributions from the same body.
type cfDistributionConfigBody struct {
	XMLName xml.Name `xml:"DistributionConfig"`
	Comment string   `xml:"Comment"`
	Enabled string   `xml:"Enabled"`
}

func (p *CloudFrontPlugin) createDistribution(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Parse optional comment and enabled flag from XML body.
	var xmlBody cfDistributionConfigBody
	if len(req.Body) > 0 {
		// Tolerate wrapper element names (CreateDistributionRequest, DistributionConfig).
		_ = xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlBody)
	}
	return p.createDistributionFrom(ctx, xmlBody, nil)
}

// createDistributionWithTags handles POST /2020-05-31/distribution?WithTags.
//
// The body is a DistributionConfigWithTags wrapping the same DistributionConfig CreateDistribution
// takes and a Tags document in the shape TagResource takes. Both children are documented
// Required: Yes, and the operation is documented as requiring both the CreateDistribution and the
// TagResource permission — so it is one call doing the work of two, and here it is one decode
// feeding the two halves [CloudFrontPlugin.createDistributionFrom] already writes.
//
// Unlike CreateDistribution, the decode error is *not* discarded: a caller reaching this operation
// has asked for tags, and a body substrate cannot read would otherwise create an untagged
// distribution and report success — the failure mode #883 closed on the tagging path. The refusal
// is InvalidArgument/400, which the operation publishes alongside InvalidTagging/400.
func (p *CloudFrontPlugin) createDistributionWithTags(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		XMLName xml.Name                 `xml:"DistributionConfigWithTags"`
		Config  cfDistributionConfigBody `xml:"DistributionConfig"`
		Tags    struct {
			Items []struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			} `xml:"Items>Tag"`
		} `xml:"Tags"`
	}
	if err := xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&body); err != nil {
		return nil, cfInvalidTagBody("DistributionConfigWithTags", err)
	}

	tags := make(map[string]string, len(body.Tags.Items))
	for _, tag := range body.Tags.Items {
		tags[tag.Key] = tag.Value
	}
	return p.createDistributionFrom(ctx, body.Config, tags)
}

// createDistributionFrom records a distribution and answers the Distribution document both creates
// return.
//
// tags is nil for CreateDistribution and the decoded tag set for CreateDistributionWithTags. It is
// applied here rather than by a second call into the tagging path because the tags arrive with the
// create: writing the record and then tagging it would make a distribution observable untagged
// between the two writes, and would answer the create's 201 with the tagging's own errors still
// ahead of it.
func (p *CloudFrontPlugin) createDistributionFrom(ctx *RequestContext, xmlBody cfDistributionConfigBody, tags map[string]string) (*AWSResponse, error) {
	distID := generateCloudFrontID(ctx.IDs)

	enabled := !strings.EqualFold(xmlBody.Enabled, "false")
	arn := fmt.Sprintf("arn:aws:cloudfront::%s:distribution/%s", ctx.AccountID, distID)
	domainName := distID + ".cloudfront.net"
	now := p.tc.Now()

	if tags == nil {
		tags = map[string]string{}
	}
	dist := CloudFrontDistribution{
		ID:         distID,
		ARN:        arn,
		Status:     "Deployed",
		DomainName: domainName,
		Comment:    xmlBody.Comment,
		Enabled:    enabled,
		Tags:       tags,
		// EverTagged is set from the create's own tags, so that a distribution created with
		// tags and then untagged to empty is still distinguishable from one that was never
		// tagged — which is the distinction [taggingEverTagged] exists to preserve and which
		// the Resource Groups Tagging API's GetResources reads.
		EverTagged:       taggingEverTagged(false, 0, len(tags)),
		CreatedTime:      now,
		LastModifiedTime: now,
		AccountID:        ctx.AccountID,
	}

	data, err := json.Marshal(dist)
	if err != nil {
		return nil, fmt.Errorf("cloudfront createDistribution marshal: %w", err)
	}

	goCtx := context.Background()
	stateKey := cfDistKey(ctx.AccountID, distID)
	if err := p.state.Put(goCtx, cloudfrontNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("cloudfront createDistribution state.Put: %w", err)
	}

	idxKey := cfDistIDsKey(ctx.AccountID)
	updateStringIndex(goCtx, p.state, cloudfrontNamespace, idxKey, distID)

	type xmlDist struct {
		XMLName    xml.Name `xml:"Distribution"`
		ID         string   `xml:"Id"`
		ARN        string   `xml:"ARN"`
		Status     string   `xml:"Status"`
		DomainName string   `xml:"DomainName"`
	}
	return cloudfrontXMLResponse(http.StatusCreated, xmlDist{
		ID:         distID,
		ARN:        arn,
		Status:     "Deployed",
		DomainName: domainName,
	})
}

func (p *CloudFrontPlugin) getDistribution(ctx *RequestContext, _ *AWSRequest, distID string) (*AWSResponse, error) {
	dist, err := p.loadDistribution(ctx, distID)
	if err != nil {
		return nil, err
	}
	return p.marshalDistributionXML(dist)
}

// getDistributionConfig answers the distribution's configuration.
//
// The response carries `DistributionConfig` members and nothing else. `Id` and `ARN` were
// rendered here until #1091 and are members of the enclosing `Distribution` type, which this
// operation does not return — a caller reading `Id` out of a configuration was reading a field
// AWS publishes one level up, and `GetDistributionConfig`'s own Response Syntax does not carry
// it (#1013).
//
// Two divergences from the published contract remain, recorded rather than papered over:
//
//   - `API_DistributionConfig` marks five members `Required: Yes` — `CallerReference`,
//     `Comment`, `DefaultCacheBehavior`, `Enabled` and `Origins` — and substrate can answer
//     two. `CreateDistribution` decodes only `Comment` and `Enabled` from its body, so there is
//     no recorded value for the other three; rendering them would mean inventing a shape (an
//     `Origins` needs `Items` and a `Quantity`, a `DefaultCacheBehavior` a whole subtree) and
//     neither this page nor `API_CreateDistribution` publishes an example of a configuration to
//     copy one from. Omitting a member substrate has no value for is the honest answer.
//   - `Id` is published as "The distribution's ID. If the ID is empty, an empty distribution
//     configuration is returned." An empty ID is reachable here — the path
//     `/2020-05-31/distribution//config` routes to this operation with distID "" — and
//     substrate answers `NoSuchDistribution`/404 instead, because the "empty distribution
//     configuration" AWS describes is the shape it publishes no example of and whose five
//     required members substrate would have to invent. The refusal is the published code at the
//     published status, so a caller is told something true; it is simply not what AWS says for
//     this one input. Pinned by TestCloudFront_AnEmptyDistributionIDIsRefusedNotAnswered.
func (p *CloudFrontPlugin) getDistributionConfig(ctx *RequestContext, _ *AWSRequest, distID string) (*AWSResponse, error) {
	dist, err := p.loadDistribution(ctx, distID)
	if err != nil {
		return nil, err
	}

	// Comment carries no omitempty: it is Required: Yes and the Response Syntax renders it
	// unconditionally, so a distribution created without one answers an empty element rather
	// than dropping a member a caller is entitled to find.
	type xmlConfig struct {
		XMLName xml.Name `xml:"DistributionConfig"`
		Comment string   `xml:"Comment"`
		Enabled bool     `xml:"Enabled"`
	}
	return cloudfrontXMLResponse(http.StatusOK, xmlConfig{
		Comment: dist.Comment,
		Enabled: dist.Enabled,
	})
}

func (p *CloudFrontPlugin) updateDistribution(ctx *RequestContext, req *AWSRequest, distID string) (*AWSResponse, error) {
	dist, err := p.loadDistribution(ctx, distID)
	if err != nil {
		return nil, err
	}

	var xmlBody struct {
		XMLName xml.Name `xml:"DistributionConfig"`
		Comment string   `xml:"Comment"`
		Enabled string   `xml:"Enabled"`
	}
	if len(req.Body) > 0 {
		_ = xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlBody)
	}
	if xmlBody.Comment != "" {
		dist.Comment = xmlBody.Comment
	}
	if xmlBody.Enabled != "" {
		dist.Enabled = !strings.EqualFold(xmlBody.Enabled, "false")
	}
	dist.LastModifiedTime = p.tc.Now()

	data, err := json.Marshal(dist)
	if err != nil {
		return nil, fmt.Errorf("cloudfront updateDistribution marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), cloudfrontNamespace, cfDistKey(ctx.AccountID, distID), data); err != nil {
		return nil, fmt.Errorf("cloudfront updateDistribution state.Put: %w", err)
	}

	return p.marshalDistributionXML(dist)
}

func (p *CloudFrontPlugin) deleteDistribution(ctx *RequestContext, _ *AWSRequest, distID string) (*AWSResponse, error) {
	if _, err := p.loadDistribution(ctx, distID); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	if err := p.state.Delete(goCtx, cloudfrontNamespace, cfDistKey(ctx.AccountID, distID)); err != nil {
		return nil, fmt.Errorf("cloudfront deleteDistribution state.Delete: %w", err)
	}

	idxKey := cfDistIDsKey(ctx.AccountID)
	removeFromStringIndex(goCtx, p.state, cloudfrontNamespace, idxKey, distID)

	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

func (p *CloudFrontPlugin) listDistributions(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	idxKey := cfDistIDsKey(ctx.AccountID)
	ids, err := loadStringIndex(goCtx, p.state, cloudfrontNamespace, idxKey)
	if err != nil {
		return nil, fmt.Errorf("cloudfront listDistributions loadIndex: %w", err)
	}

	type xmlSummary struct {
		XMLName    xml.Name `xml:"DistributionSummary"`
		ID         string   `xml:"Id"`
		ARN        string   `xml:"ARN"`
		Status     string   `xml:"Status"`
		DomainName string   `xml:"DomainName"`
		Comment    string   `xml:"Comment,omitempty"`
		Enabled    bool     `xml:"Enabled"`
	}

	summaries := make([]xmlSummary, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, cloudfrontNamespace, cfDistKey(ctx.AccountID, id))
		if getErr != nil || data == nil {
			continue
		}
		var dist CloudFrontDistribution
		if err := json.Unmarshal(data, &dist); err != nil {
			continue
		}
		summaries = append(summaries, xmlSummary{
			ID:         dist.ID,
			ARN:        dist.ARN,
			Status:     dist.Status,
			DomainName: dist.DomainName,
			Comment:    dist.Comment,
			Enabled:    dist.Enabled,
		})
	}

	type xmlList struct {
		XMLName     xml.Name     `xml:"DistributionList"`
		Items       []xmlSummary `xml:"Items>DistributionSummary"`
		Quantity    int          `xml:"Quantity"`
		IsTruncated bool         `xml:"IsTruncated"`
	}
	return cloudfrontXMLResponse(http.StatusOK, xmlList{
		Items:       summaries,
		Quantity:    len(summaries),
		IsTruncated: false,
	})
}

// --- Invalidation -----------------------------------------------------------

func (p *CloudFrontPlugin) createInvalidation(ctx *RequestContext, _ *AWSRequest, distID string) (*AWSResponse, error) {
	if _, err := p.loadDistribution(ctx, distID); err != nil {
		return nil, err
	}

	// Use I prefix for invalidation IDs per CloudFront API convention.
	invID := "I" + generateCloudFrontID(ctx.IDs)[1:]

	now := p.tc.Now().UTC()
	inv := CloudFrontInvalidation{
		ID:         invID,
		Status:     "Completed",
		CreateTime: now,
	}

	// Persist invalidation.
	goCtx := context.Background()
	invData, _ := json.Marshal(inv)
	invKey := cfInvalKey(ctx.AccountID, distID, invID)
	_ = p.state.Put(goCtx, cloudfrontNamespace, invKey, invData)
	updateStringIndex(goCtx, p.state, cloudfrontNamespace, cfInvalIDsKey(ctx.AccountID, distID), invID)

	type xmlInvalidation struct {
		XMLName    xml.Name `xml:"Invalidation"`
		ID         string   `xml:"Id"`
		Status     string   `xml:"Status"`
		CreateTime string   `xml:"CreateTime"`
	}
	return cloudfrontXMLResponse(http.StatusCreated, xmlInvalidation{
		ID:         inv.ID,
		Status:     inv.Status,
		CreateTime: now.Format(time.RFC3339),
	})
}

// getInvalidation answers one invalidation batch of one distribution.
//
// API_GetInvalidation publishes NoSuchDistribution/404 and NoSuchInvalidation/404, in that
// order, and they are not interchangeable: until #1091 an invalidation ID under a distribution
// that does not exist answered NoSuchInvalidation, telling a caller the batch was missing from
// a distribution substrate never had. The distribution is loaded first so each of the two
// published codes reports the thing that is actually absent.
func (p *CloudFrontPlugin) getInvalidation(ctx *RequestContext, distID, invID string) (*AWSResponse, error) {
	if _, err := p.loadDistribution(ctx, distID); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	data, err := p.state.Get(goCtx, cloudfrontNamespace, cfInvalKey(ctx.AccountID, distID, invID))
	if err != nil {
		return nil, fmt.Errorf("cloudfront getInvalidation state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{
			Code:       "NoSuchInvalidation",
			Message:    "The specified invalidation does not exist.",
			HTTPStatus: http.StatusNotFound,
		}
	}
	var inv CloudFrontInvalidation
	if err := json.Unmarshal(data, &inv); err != nil {
		return nil, fmt.Errorf("cloudfront getInvalidation unmarshal: %w", err)
	}
	type xmlInvalidation struct {
		XMLName    xml.Name `xml:"Invalidation"`
		ID         string   `xml:"Id"`
		Status     string   `xml:"Status"`
		CreateTime string   `xml:"CreateTime"`
	}
	return cloudfrontXMLResponse(http.StatusOK, xmlInvalidation{
		ID: inv.ID, Status: inv.Status, CreateTime: inv.CreateTime.Format(time.RFC3339),
	})
}

// listInvalidations answers a distribution's invalidation batches.
//
// API_ListInvalidations publishes NoSuchDistribution/404 and substrate had nowhere to answer it
// until #1091: the handler read the invalidation index straight out of state without ever
// looking at the distribution record, so any ID at all — including one that exists in no
// account — was answered 200 with an empty list. A caller could not tell "this distribution has
// never been invalidated" from "there is no such distribution", which is the same defect
// ECR's requireRepository fixed for the four ECR image operations (#1090).
func (p *CloudFrontPlugin) listInvalidations(ctx *RequestContext, distID string) (*AWSResponse, error) {
	if _, err := p.loadDistribution(ctx, distID); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	ids, _ := loadStringIndex(goCtx, p.state, cloudfrontNamespace, cfInvalIDsKey(ctx.AccountID, distID))

	type invSummary struct {
		ID         string `xml:"Id"`
		Status     string `xml:"Status"`
		CreateTime string `xml:"CreateTime"`
	}
	type xmlList struct {
		XMLName     xml.Name     `xml:"InvalidationList"`
		Items       []invSummary `xml:"Items>InvalidationSummary"`
		Quantity    int          `xml:"Quantity"`
		IsTruncated bool         `xml:"IsTruncated"`
	}
	var items []invSummary
	for _, id := range ids {
		data, err := p.state.Get(goCtx, cloudfrontNamespace, cfInvalKey(ctx.AccountID, distID, id))
		if err != nil || data == nil {
			continue
		}
		var inv CloudFrontInvalidation
		if json.Unmarshal(data, &inv) == nil {
			items = append(items, invSummary{ID: inv.ID, Status: inv.Status, CreateTime: inv.CreateTime.Format(time.RFC3339)})
		}
	}
	return cloudfrontXMLResponse(http.StatusOK, xmlList{Items: items, Quantity: len(items)})
}

// --- Tagging ----------------------------------------------------------------
//
// CloudFront publishes three tagging operations and substrate now implements all three.
// They are the only way a distribution's tags can be changed here: the Resource Groups
// Tagging API cannot reach a CloudFront ARN at all, because [TaggingPlugin.resolveARN] has
// no "cloudfront" arm and its default answers a FailedResourcesMap entry of
// InternalServiceException/500 — "the resource type in the request is not supported by the
// Resource Groups Tagging API". So `UntagResources` against a distribution has never had
// #883's silent-success shape: it refuses, in the same answer every armless service gets,
// and TaggingResolveARN's guard tests pin that. Giving CloudFront an arm there is #835's
// work — it needs a [mergeResourceTags] case as well as a resolver case, and it changes
// which resources GetResources enumerates — so it is deliberately not folded into #883.

// tagResource implements CloudFront's TagResource. It merges the tags in the request body
// into the resource the Resource query parameter names.
//
// The body is a <Tags> document, per the operation's published request syntax:
//
//	<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">
//	   <Items><Tag><Key>{{string}}</Key><Value>{{string}}</Value></Tag></Items>
//	</Tags>
//
// The decode error is returned rather than discarded, which is the other half of #883.
// Because the root element name is part of the struct, a <TagKeys> document — an untag
// request that reached here by the misroute that issue fixes — used to decode into an
// empty item list, add nothing, and answer 204. A body of the wrong shape is now refused
// instead of read as "no tags", so no shape of request can be answered with the tagging
// success having changed nothing.
func (p *CloudFrontPlugin) tagResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	dist, err := p.resolveTagTarget(req)
	if err != nil {
		return nil, err
	}

	// Parse XML tags from body.
	var xmlTags struct {
		XMLName xml.Name `xml:"Tags"`
		Items   []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"Items>Tag"`
	}
	if len(req.Body) > 0 {
		if decErr := xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlTags); decErr != nil {
			return nil, cfInvalidTagBody("Tags", decErr)
		}
	}

	dist.EverTagged = taggingEverTagged(dist.EverTagged, len(dist.Tags), len(xmlTags.Items))

	if dist.Tags == nil {
		dist.Tags = make(map[string]string)
	}
	for _, tag := range xmlTags.Items {
		dist.Tags[tag.Key] = tag.Value
	}

	if err := p.putDistribution(dist, "tagResource"); err != nil {
		return nil, err
	}

	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

// untagResource implements CloudFront's UntagResource. It removes the tag keys named in the
// request body from the resource the Resource query parameter names, and answers the
// documented "HTTP/1.1 204" with an empty body.
//
// The body is a <TagKeys> document, per the operation's published request syntax:
//
//	<TagKeys xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">
//	   <Items><Key>{{string}}</Key></Items>
//	</TagKeys>
//
// TagKeys is documented "Required: Yes", so a request with no body is refused rather than
// treated as naming no keys — an untag that answers 204 having removed nothing is the
// defect #883 exists to close, and an absent body is the one remaining way to ask for it.
// Items is documented "Required: No", so <TagKeys/> with no Items is a legal request that
// removes nothing.
//
// **Removing a key the resource does not carry succeeds, and that is substrate's reading
// rather than something AWS publishes.** The operation's Errors list — AccessDenied 403,
// InvalidArgument 400, InvalidTagging 400, NoSuchResource 404 — names nothing for an absent
// key, and the response section documents an unconditional 204 with an empty body, but the
// reference does not address the case either way. Substrate treats it as success because the
// alternative makes a consumer's teardown loop fail on its second run, and because a
// response shape carrying no per-key result has nowhere to report a partial removal.
func (p *CloudFrontPlugin) untagResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	dist, err := p.resolveTagTarget(req)
	if err != nil {
		return nil, err
	}

	if len(req.Body) == 0 {
		return nil, &AWSError{
			Code:       "InvalidArgument",
			Message:    "a TagKeys request body is required",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	var xmlKeys struct {
		XMLName xml.Name `xml:"TagKeys"`
		Items   []string `xml:"Items>Key"`
	}
	if decErr := xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlKeys); decErr != nil {
		return nil, cfInvalidTagBody("TagKeys", decErr)
	}

	dist.EverTagged = taggingEverTagged(dist.EverTagged, len(dist.Tags), 0)

	for _, key := range xmlKeys.Items {
		delete(dist.Tags, key)
	}

	if err := p.putDistribution(dist, "untagResource"); err != nil {
		return nil, err
	}

	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

func (p *CloudFrontPlugin) listTagsForResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	dist, err := p.resolveTagTarget(req)
	if err != nil {
		return nil, err
	}

	type xmlTag struct {
		XMLName xml.Name `xml:"Tag"`
		Key     string   `xml:"Key"`
		Value   string   `xml:"Value"`
	}
	type xmlTags struct {
		XMLName xml.Name `xml:"Tags"`
		Items   []xmlTag `xml:"Items>Tag"`
	}

	tags := make([]xmlTag, 0, len(dist.Tags))
	for k, v := range dist.Tags {
		tags = append(tags, xmlTag{Key: k, Value: v})
	}
	// Each <Tag> member is emitted in slice order, so ranging the map put Go's map order on the wire
	// and two identical calls could differ (#946). CloudFront's ListTagsForResource documents no
	// order, so sorted-by-key is substrate's reading, taken for the reason [sortTagsByKey] records.
	sortTagsByKey(tags, func(t xmlTag) string { return t.Key })

	return cloudfrontXMLResponse(http.StatusOK, xmlTags{Items: tags})
}

// --- Helpers ----------------------------------------------------------------

// resolveTagTarget resolves the Resource query parameter that CloudFront's three tagging
// operations share to the distribution it names.
//
// One function for all three so they cannot drift on which resource an ARN addresses. That
// they resolved it in three copies is what let #883's routing defect matter: a request the
// caller aimed at UntagResource ran the tag path against the same target and reported
// success. A removal pointed at a resource the caller did not name is the more damaging
// direction, so the three share the resolution rather than each repeating it.
//
// The parameter is required. The reference's "URI Request Parameters" section documents
// Resource for ListTagsForResource only — "An ARN of a CloudFront resource. Pattern:
// arn:aws(-cn)?:cloudfront::[0-9]+:.* Required: Yes" — and says "the request does not use
// any URI parameters" on both TagResource and UntagResource, which cannot be right for
// operations whose request syntax is a bare "/tagging" path with no other way to name a
// target. Substrate reads that as a documentation omission and requires the parameter on all
// three; the request syntax those two pages publish shows the same query string carrying
// Operation, and every SDK sends Resource alongside it.
//
// An ARN naming no distribution answers NoSuchDistribution/404 rather than the NoSuchResource
// the three tagging pages list, because that is the code this plugin's other arms and
// [CloudFrontPlugin.loadDistribution] already answer and one plugin should not report a
// missing distribution two ways. Aligning all three tagging arms on the published code is a
// separate change from #883, which is about a request being routed to the wrong operation. An
// ARN naming a resource type substrate does not model is a different case and does answer the
// published NoSuchResource/404 — see [cfParseDistributionARN].
//
// It takes no *RequestContext, because the account the ARN names is the account the record is
// read from and written to. Taking it from the caller's context is what #918 fixed: this
// function used to resolve the distribution *ID* out of the ARN and then key the load by
// ctx.AccountID, so an ARN naming another account's distribution addressed the caller's
// same-named one. Not having the context in scope is what keeps that from coming back.
func (p *CloudFrontPlugin) resolveTagTarget(req *AWSRequest) (CloudFrontDistribution, error) {
	resourceARN := req.Params["Resource"]
	if resourceARN == "" {
		return CloudFrontDistribution{}, &AWSError{
			Code:       "InvalidArgument",
			Message:    "Resource query parameter is required",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	target, arnErr := cfParseDistributionARN(resourceARN)
	if arnErr != nil {
		return CloudFrontDistribution{}, arnErr
	}
	return p.loadDistributionForAccount(target.AccountID, target.DistID)
}

// cfInvalidTagBody refuses a tagging body whose XML does not decode into the root element
// the operation publishes, naming that element so a caller can see which one was expected.
//
// InvalidArgument/400 is the closest of the four codes the tagging operations document
// (AccessDenied 403, InvalidArgument 400, InvalidTagging 400, NoSuchResource 404): the
// reference glosses it as "an argument is invalid", and the body is the argument. AWS does
// not publish which of InvalidArgument and InvalidTagging it answers for an unparsable body,
// so the choice between the two is substrate's.
func cfInvalidTagBody(root string, err error) *AWSError {
	return &AWSError{
		Code:       "InvalidArgument",
		Message:    fmt.Sprintf("the request body is not a valid <%s> document: %v", root, err),
		HTTPStatus: http.StatusBadRequest,
	}
}

// putDistribution persists a distribution record under the key its own account and ID name.
//
// op names the calling operation and reaches only the wrapped error's text. The tagging arms
// share this rather than each marshaling and keying by hand, so an untag cannot write to a
// different key than the tag it is undoing wrote to.
//
// The account comes from the record rather than from a *RequestContext, and the parameter is
// gone so it cannot come from anywhere else (#918). A tagging arm reads the distribution the
// ARN names and writes back what it read; taking the account from the caller instead let a
// read from one account be written to another, which is how UntagResource stripped a tag from
// a distribution the request did not name.
func (p *CloudFrontPlugin) putDistribution(dist CloudFrontDistribution, op string) error {
	// A record with no account cannot be keyed. Every writer sets it (createDistribution,
	// cloudfront_plugin.go), so this is unreachable through the API — but keying an empty
	// account would write to "cfdist:/{id}", a key no read looks at, and report success.
	if dist.AccountID == "" {
		return fmt.Errorf("cloudfront %s: distribution %s record carries no account", op, dist.ID)
	}
	data, err := json.Marshal(dist)
	if err != nil {
		return fmt.Errorf("cloudfront %s marshal: %w", op, err)
	}
	if err := p.state.Put(context.Background(), cloudfrontNamespace, cfDistKey(dist.AccountID, dist.ID), data); err != nil {
		return fmt.Errorf("cloudfront %s state.Put: %w", op, err)
	}
	return nil
}

// loadDistribution loads a distribution owned by the calling account from state, returning a
// NoSuchDistribution error if absent.
//
// This is the right resolution for the operations that name a distribution by bare ID in the
// request path — GetDistribution, UpdateDistribution, DeleteDistribution and their siblings —
// because there the caller's own account is the only account the ID can refer to. The tagging
// operations name a full ARN and must not use it; they go through
// [CloudFrontPlugin.resolveTagTarget].
func (p *CloudFrontPlugin) loadDistribution(ctx *RequestContext, distID string) (CloudFrontDistribution, error) {
	return p.loadDistributionForAccount(ctx.AccountID, distID)
}

// loadDistributionForAccount loads a distribution owned by accountID, returning a
// NoSuchDistribution error if absent.
func (p *CloudFrontPlugin) loadDistributionForAccount(accountID, distID string) (CloudFrontDistribution, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, cloudfrontNamespace, cfDistKey(accountID, distID))
	if err != nil {
		return CloudFrontDistribution{}, fmt.Errorf("cloudfront loadDistribution state.Get: %w", err)
	}
	if data == nil {
		// "The specified distribution does not exist." is the description
		// API_GetDistributionConfig publishes for NoSuchDistribution, and it names no
		// distribution. The message read "Distribution not found: " + distID until #1091, which
		// trailed a bare colon whenever distID was empty — reachable, because
		// /2020-05-31/distribution//config routes here with an empty ID.
		return CloudFrontDistribution{}, &AWSError{
			Code:       "NoSuchDistribution",
			Message:    "The specified distribution does not exist.",
			HTTPStatus: http.StatusNotFound,
		}
	}
	var dist CloudFrontDistribution
	if err := json.Unmarshal(data, &dist); err != nil {
		return CloudFrontDistribution{}, fmt.Errorf("cloudfront loadDistribution unmarshal: %w", err)
	}
	return dist, nil
}

// marshalDistributionXML serializes a distribution to a <Distribution> XML response.
func (p *CloudFrontPlugin) marshalDistributionXML(dist CloudFrontDistribution) (*AWSResponse, error) {
	type xmlDist struct {
		XMLName          xml.Name `xml:"Distribution"`
		ID               string   `xml:"Id"`
		ARN              string   `xml:"ARN"`
		Status           string   `xml:"Status"`
		DomainName       string   `xml:"DomainName"`
		Comment          string   `xml:"Comment,omitempty"`
		Enabled          bool     `xml:"Enabled"`
		LastModifiedTime string   `xml:"LastModifiedTime"`
	}
	return cloudfrontXMLResponse(http.StatusOK, xmlDist{
		ID:               dist.ID,
		ARN:              dist.ARN,
		Status:           dist.Status,
		DomainName:       dist.DomainName,
		Comment:          dist.Comment,
		Enabled:          dist.Enabled,
		LastModifiedTime: dist.LastModifiedTime.UTC().Format(time.RFC3339),
	})
}

// cfIDAlphabet is the alphabet every CloudFront identifier substrate mints draws from: the
// uppercase letters and the digits, as a CloudFront distribution ID is rendered.
//
// Named rather than written inline because three kinds of identifier draw from it — a
// distribution ID, an invalidation ID and an origin access control's ID and ETag — and #856's
// conversion turned the one function that held it into a mint call. A second copy would be a
// second alphabet the day someone corrected one of them. [cfnOAIIDChars] (cfn_resources_v23.go)
// is a deliberate third copy, derived for CloudFormation's own reasons and documented there.
const cfIDAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// cfOACLocation is the prefix of the Location header CreateOriginAccessControl answers: the
// service endpoint and the operation's own path, which the new control's ID is appended to.
const cfOACLocation = "https://cloudfront.amazonaws.com/2020-05-31/origin-access-control/"

// generateCloudFrontID mints a CloudFront-style identifier: E followed by 13 characters of
// [cfIDAlphabet].
//
// It draws from the request's [IDMint] rather than from crypto/rand, so replaying a recorded
// CreateDistribution, CreateInvalidation or CreateOriginAccessControl mints the identifier the
// recording minted (#856). A nil or seedless mint still falls back to crypto/rand inside the
// mint, which is why there is no error to return: the byte source cannot fail in a way a caller
// here could act on, and the mapping — alphabet[b%len(alphabet)] — is the same one this function
// performed before the conversion, so an ID recorded by an earlier substrate is still the shape
// this one mints.
func generateCloudFrontID(m *IDMint) string {
	return "E" + m.Chars(13, cfIDAlphabet)
}

// cfMintETag mints the version identifier an origin access control carries in its ETag.
//
// AWS publishes no shape for a CloudFront ETag — the reference says only that it identifies the
// current version of a resource — so the rendering is substrate's: the same E-prefixed form as an
// ID, from the same mint, so that a replayed create reproduces the version its recording handed
// out and a recorded delete's If-Match still matches.
func cfMintETag(m *IDMint) string {
	return generateCloudFrontID(m)
}

// cloudfrontXMLResponse serializes v to XML and returns an AWSResponse with
// Content-Type: application/xml and the given HTTP status code.
func cloudfrontXMLResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("cloudfront xml marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/xml"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}
