package emulator

import (
	"bytes"
	"context"
	"crypto/rand"
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
	op, distID := parseCloudFrontOperation(requestMethod(req), req.Path, req.Params)
	// Handle GetInvalidation (op includes invID after colon).
	if strings.HasPrefix(op, "GetInvalidation:") {
		invID := strings.TrimPrefix(op, "GetInvalidation:")
		return p.getInvalidation(ctx, distID, invID)
	}
	switch op {
	case "CreateDistribution":
		return p.createDistribution(ctx, req)
	case "GetDistribution":
		return p.getDistribution(ctx, req, distID)
	case "GetDistributionConfig":
		return p.getDistributionConfig(ctx, req, distID)
	case "UpdateDistribution":
		return p.updateDistribution(ctx, req, distID)
	case "DeleteDistribution":
		return p.deleteDistribution(ctx, req, distID)
	case "ListDistributions":
		return p.listDistributions(ctx, req)
	case "CreateInvalidation":
		return p.createInvalidation(ctx, req, distID)
	case "ListInvalidations":
		return p.listInvalidations(ctx, distID)
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
// distribution ID from the HTTP method, URL path, and query parameters.
func parseCloudFrontOperation(method, path string, params map[string]string) (op, distID string) {
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

	switch {
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

// --- Distribution operations ------------------------------------------------

func (p *CloudFrontPlugin) createDistribution(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Parse optional comment and enabled flag from XML body.
	var xmlBody struct {
		XMLName xml.Name `xml:"DistributionConfig"`
		Comment string   `xml:"Comment"`
		Enabled string   `xml:"Enabled"`
	}
	if len(req.Body) > 0 {
		// Tolerate wrapper element names (CreateDistributionRequest, DistributionConfig).
		_ = xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlBody)
	}

	distID, err := generateCloudFrontID()
	if err != nil {
		return nil, fmt.Errorf("cloudfront createDistribution generateID: %w", err)
	}

	enabled := !strings.EqualFold(xmlBody.Enabled, "false")
	arn := fmt.Sprintf("arn:aws:cloudfront::%s:distribution/%s", ctx.AccountID, distID)
	domainName := distID + ".cloudfront.net"
	now := p.tc.Now()

	dist := CloudFrontDistribution{
		ID:               distID,
		ARN:              arn,
		Status:           "Deployed",
		DomainName:       domainName,
		Comment:          xmlBody.Comment,
		Enabled:          enabled,
		Tags:             map[string]string{},
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

func (p *CloudFrontPlugin) getDistributionConfig(ctx *RequestContext, _ *AWSRequest, distID string) (*AWSResponse, error) {
	dist, err := p.loadDistribution(ctx, distID)
	if err != nil {
		return nil, err
	}

	type xmlConfig struct {
		XMLName xml.Name `xml:"DistributionConfig"`
		ID      string   `xml:"Id"`
		ARN     string   `xml:"ARN"`
		Comment string   `xml:"Comment,omitempty"`
		Enabled bool     `xml:"Enabled"`
	}
	return cloudfrontXMLResponse(http.StatusOK, xmlConfig{
		ID:      dist.ID,
		ARN:     dist.ARN,
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

	invID, err := generateCloudFrontID()
	if err != nil {
		return nil, fmt.Errorf("cloudfront createInvalidation generateID: %w", err)
	}
	// Use I prefix for invalidation IDs per CloudFront API convention.
	invID = "I" + invID[1:]

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

func (p *CloudFrontPlugin) getInvalidation(ctx *RequestContext, distID, invID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, cloudfrontNamespace, cfInvalKey(ctx.AccountID, distID, invID))
	if err != nil || data == nil {
		return nil, &AWSError{Code: "NoSuchInvalidation", Message: "invalidation not found: " + invID, HTTPStatus: http.StatusNotFound}
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

func (p *CloudFrontPlugin) listInvalidations(ctx *RequestContext, distID string) (*AWSResponse, error) {
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
		return CloudFrontDistribution{}, &AWSError{
			Code:       "NoSuchDistribution",
			Message:    "Distribution not found: " + distID,
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

// generateCloudFrontID generates a CloudFront-style distribution identifier
// of the form E followed by 13 uppercase alphanumeric characters.
func generateCloudFrontID() (string, error) {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 13)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generateCloudFrontID rand.Read: %w", err)
	}
	out := make([]byte, 13)
	for i, ch := range b {
		out[i] = chars[int(ch)%len(chars)]
	}
	return "E" + string(out), nil
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
