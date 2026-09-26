package emulator

// CloudFront origin access control: the family that lets a distribution read a private S3
// bucket, and the one whose absence stops such a deploy at the first call.
//
// An origin access control (OAC) is a standalone CloudFront resource — created, read, listed and
// deleted on its own path, then named by an origin through OriginAccessControlId. Substrate
// routed none of it before #1277, so a consumer keeping its bucket private and serving it through
// a SigV4-signed origin could not run against the emulator at all: the create fell through
// [CloudFrontPlugin.HandleRequest]'s default arm as an unknown route.
//
// The whole family lives in one file rather than beside the distribution operations, because the
// two share nothing but the plugin: an OAC has no ARN, is not taggable, carries no status and
// does not transition. What it does carry is a **version**, and that is the one thing here the
// distribution operations do not model — DeleteOriginAccessControl takes an If-Match and answers
// PreconditionFailed for a stale one, so the record stores an ETag and the delete compares it.
//
// Two members of the published contract are deliberately absent, both recorded here rather than
// left to be discovered:
//
//   - OriginAccessControlInUse/409, which DeleteOriginAccessControl publishes for an OAC a
//     distribution still names. Answering it means knowing a distribution's origins, and
//     substrate records none — that is the same gap docs/services.md describes under "A
//     configuration is not a distribution", and it is why GetDistributionConfig answers two of
//     its five required members. #1271 is where a distribution starts recording its
//     configuration; the check becomes answerable there and is a follow-up on #1277 rather than
//     a guess here.
//   - OriginAccessControlAlreadyExists/409, which CreateOriginAccessControl publishes for one
//     "with the specified parameters". Which parameters make two OACs the same is not published,
//     and an OAC carries no CallerReference to key it on the way a distribution does, so
//     substrate would be inventing the duplicate key. A create that repeats a name succeeds and
//     mints a second OAC; a consumer converging by name should list first, which is what the
//     operation exists for.

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
)

// CloudFront origin access control state-key prefixes: the record and the per-account index of
// its IDs. Declared beside the distribution and invalidation prefixes in cloudfront_tags.go for
// the reason that block records — one producer per key kind is what keeps [cfKeyIsTaggable]'s
// single positive test honest, and a prefix spelled inline here would be invisible to it.

// CloudFrontOriginAccessControl holds persisted state for a CloudFront origin access control.
//
// Every field is a member the API publishes, plus the ETag. There is no AccountID: the record is
// keyed by the calling account and read back the same way, and no operation here resolves an OAC
// from an ARN, so an account on the record would be a field nothing reads — and a wire-visible
// bookkeeping member of exactly the kind #756 is removing.
type CloudFrontOriginAccessControl struct {
	// ID is the unique identifier CloudFront assigns, in the same E-prefixed shape as a
	// distribution ID.
	ID string `json:"Id"`

	// Name identifies the origin access control. Required on create; up to 64 characters.
	Name string `json:"Name"`

	// Description is the optional human-readable description.
	Description string `json:"Description,omitempty"`

	// OriginType is OriginAccessControlOriginType: the kind of origin this control is for,
	// one of s3, mediastore, mediapackagev2 or lambda.
	OriginType string `json:"OriginAccessControlOriginType"`

	// SigningBehavior is which origin requests CloudFront signs: never, always or
	// no-override.
	SigningBehavior string `json:"SigningBehavior"`

	// SigningProtocol is the signing protocol; sigv4 is the only published value.
	SigningProtocol string `json:"SigningProtocol"`

	// ETag is the version a caller must echo in If-Match to delete this control.
	//
	// AWS publishes no shape for it — only that it identifies "the current version" — so the
	// E-prefixed form substrate mints is substrate's choice. What matters is that it is opaque
	// to the caller and that the delete refuses a value that is not the current one.
	ETag string `json:"ETag"`
}

// cfOACKey returns the state key an origin access control is stored at.
//
// No Region component, for the reason [cfDistKey] has none: CloudFront is global.
func cfOACKey(accountID, oacID string) string {
	return cfOACKeyPrefix + accountID + "/" + oacID
}

// cfOACIDsKey returns the state index key for all origin access control IDs in an account.
func cfOACIDsKey(accountID string) string {
	return cfOACIDsKeyPrefix + accountID
}

// cfOACConfigWire is the OriginAccessControlConfig document, which is both the create request's
// body and a member of every response.
//
// One struct for both directions so the two cannot drift on an element name — a create that
// decodes SigningBehavior from one spelling and renders it under another would round-trip
// through substrate and fail against AWS. The member order is the reference's.
//
// XMLName is declared rather than left to the field tag so that decoding a create body *enforces*
// the root element: a body whose root is something else is a refusal rather than a config with
// every member empty, which would then fail the required-member checks with the wrong reason.
type cfOACConfigWire struct {
	XMLName         xml.Name `xml:"OriginAccessControlConfig"`
	Description     string   `xml:"Description,omitempty"`
	Name            string   `xml:"Name"`
	OriginType      string   `xml:"OriginAccessControlOriginType"`
	SigningBehavior string   `xml:"SigningBehavior"`
	SigningProtocol string   `xml:"SigningProtocol"`
}

// cfOACWire is the OriginAccessControl document CreateOriginAccessControl and
// GetOriginAccessControl both answer.
type cfOACWire struct {
	XMLName xml.Name        `xml:"OriginAccessControl"`
	ID      string          `xml:"Id"`
	Config  cfOACConfigWire `xml:"OriginAccessControlConfig"`
}

// cfOACSummaryWire is one OriginAccessControlSummary in a ListOriginAccessControls response. It
// is the config's members plus the Id, flattened — the summary is not a nested config.
type cfOACSummaryWire struct {
	XMLName         xml.Name `xml:"OriginAccessControlSummary"`
	Description     string   `xml:"Description,omitempty"`
	ID              string   `xml:"Id"`
	Name            string   `xml:"Name"`
	OriginType      string   `xml:"OriginAccessControlOriginType"`
	SigningBehavior string   `xml:"SigningBehavior"`
	SigningProtocol string   `xml:"SigningProtocol"`
}

// cfOACListWire is the OriginAccessControlList document.
//
// Marker, MaxItems and NextMarker are published members and are not rendered, because substrate
// answers the list whole and so holds no value for them — the same reading [listDistributions]
// takes for DistributionList, and the same one docs/services.md states for the three
// DistributionConfig members substrate cannot answer. Inventing a MaxItems would assert a page
// size AWS publishes nowhere.
//
// Items is a pointer to a slice so that an account using no OACs answers **no Items element at
// all**, which is what the reference states: "If you're not using origin access controls for your
// AWS account, the ListOriginAccessControls operation doesn't return the Items element in the
// response." An empty slice would render the same absence by accident rather than by decision,
// and a later reader could "fix" it into an empty element; the pointer makes the absence
// deliberate and reviewable.
type cfOACListWire struct {
	XMLName     xml.Name            `xml:"OriginAccessControlList"`
	IsTruncated bool                `xml:"IsTruncated"`
	Items       *[]cfOACSummaryWire `xml:"Items>OriginAccessControlSummary,omitempty"`
	Quantity    int                 `xml:"Quantity"`
}

// createOriginAccessControl handles POST /2020-05-31/origin-access-control.
//
// The response carries the ETag and Location headers. Neither appears in the API Reference's
// Response Syntax block — which shows the XML body alone — but both are top-level members of the
// operation's output shape in the CLI and the SDKs, and the ETag is the value a caller must hold
// to delete the control later. Emitting them as headers is how a REST/XML output member that is
// not in the body reaches a caller.
func (p *CloudFrontPlugin) createOriginAccessControl(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var cfg cfOACConfigWire
	if err := xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&cfg); err != nil {
		return nil, cfInvalidOACArgument(fmt.Sprintf("the request body is not a valid <OriginAccessControlConfig> document: %v", err))
	}
	if err := cfValidateOACConfig(cfg); err != nil {
		return nil, err
	}

	oac := CloudFrontOriginAccessControl{
		ID:              generateCloudFrontID(ctx.IDs),
		Name:            cfg.Name,
		Description:     cfg.Description,
		OriginType:      cfg.OriginType,
		SigningBehavior: cfg.SigningBehavior,
		SigningProtocol: cfg.SigningProtocol,
		ETag:            cfMintETag(ctx.IDs),
	}

	if err := p.putOriginAccessControl(ctx.AccountID, oac); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	updateStringIndex(goCtx, p.state, cloudfrontNamespace, cfOACIDsKey(ctx.AccountID), oac.ID)

	resp, err := cfOACResponse(http.StatusCreated, oac)
	if err != nil {
		return nil, err
	}
	resp.Headers["Location"] = cfOACLocation + oac.ID
	return resp, nil
}

// getOriginAccessControl handles GET /2020-05-31/origin-access-control/{Id}.
func (p *CloudFrontPlugin) getOriginAccessControl(ctx *RequestContext, oacID string) (*AWSResponse, error) {
	oac, err := p.loadOriginAccessControl(ctx.AccountID, oacID)
	if err != nil {
		return nil, err
	}
	return cfOACResponse(http.StatusOK, oac)
}

// cfOACResponse renders one control as the OriginAccessControl document with its version in the
// ETag header.
//
// Both the create and the get answer through here, so neither the body nor the header rule can
// drift: a get that answered a version the create never handed out would fail a caller's delete
// against a control nothing had changed.
func cfOACResponse(status int, oac CloudFrontOriginAccessControl) (*AWSResponse, error) {
	resp, err := cloudfrontXMLResponse(status, cfOACWireFrom(oac))
	if err != nil {
		return nil, err
	}
	resp.Headers["ETag"] = oac.ETag
	return resp, nil
}

// listOriginAccessControls handles GET /2020-05-31/origin-access-control.
//
// An unreadable record is skipped rather than failing the list, following [listDistributions]:
// one corrupt record must not make the account's other controls unfindable.
func (p *CloudFrontPlugin) listOriginAccessControls(ctx *RequestContext) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, cloudfrontNamespace, cfOACIDsKey(ctx.AccountID))
	if err != nil {
		return nil, fmt.Errorf("cloudfront listOriginAccessControls loadIndex: %w", err)
	}

	summaries := make([]cfOACSummaryWire, 0, len(ids))
	for _, id := range ids {
		oac, loadErr := p.loadOriginAccessControl(ctx.AccountID, id)
		if loadErr != nil {
			continue
		}
		summaries = append(summaries, cfOACSummaryWire{
			Description:     oac.Description,
			ID:              oac.ID,
			Name:            oac.Name,
			OriginType:      oac.OriginType,
			SigningBehavior: oac.SigningBehavior,
			SigningProtocol: oac.SigningProtocol,
		})
	}

	list := cfOACListWire{Quantity: len(summaries)}
	if len(summaries) > 0 {
		list.Items = &summaries
	}
	return cloudfrontXMLResponse(http.StatusOK, list)
}

// deleteOriginAccessControl handles DELETE /2020-05-31/origin-access-control/{Id}.
//
// The three refusals are three different failures and the reference gives each its own code: an
// absent control is NoSuchOriginAccessControl/404, a missing If-Match is
// InvalidIfMatchVersion/400 ("The If-Match version is missing or not valid"), and a version that
// is present but not the current one is PreconditionFailed/412. Answering the precondition
// failure for a *missing* header would tell a caller its version was stale when it never sent
// one.
//
// The absence is checked before the header, so a caller deleting an already-deleted control is
// told that rather than being asked for a version of something that is gone.
func (p *CloudFrontPlugin) deleteOriginAccessControl(ctx *RequestContext, req *AWSRequest, oacID string) (*AWSResponse, error) {
	oac, err := p.loadOriginAccessControl(ctx.AccountID, oacID)
	if err != nil {
		return nil, err
	}

	// A quoted value is tolerated: HTTP ETags are conventionally quoted, CloudFront's are not,
	// and a caller that re-quotes the value substrate handed it means the version it was given.
	ifMatch := strings.Trim(strings.TrimSpace(headerValueFold(req.Headers, "If-Match")), `"`)
	if ifMatch == "" {
		return nil, &AWSError{
			Code:       "InvalidIfMatchVersion",
			Message:    "The If-Match version is missing or not valid for the resource.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if ifMatch != oac.ETag {
		return nil, &AWSError{
			Code:       "PreconditionFailed",
			Message:    "The precondition in one or more of the request fields evaluated to false.",
			HTTPStatus: http.StatusPreconditionFailed,
		}
	}

	goCtx := context.Background()
	if err := p.state.Delete(goCtx, cloudfrontNamespace, cfOACKey(ctx.AccountID, oacID)); err != nil {
		return nil, fmt.Errorf("cloudfront deleteOriginAccessControl state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, cloudfrontNamespace, cfOACIDsKey(ctx.AccountID), oacID)

	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

// cfOACWireFrom maps a stored control onto the OriginAccessControl document, for [cfOACResponse].
func cfOACWireFrom(oac CloudFrontOriginAccessControl) cfOACWire {
	return cfOACWire{
		ID: oac.ID,
		Config: cfOACConfigWire{
			Description:     oac.Description,
			Name:            oac.Name,
			OriginType:      oac.OriginType,
			SigningBehavior: oac.SigningBehavior,
			SigningProtocol: oac.SigningProtocol,
		},
	}
}

// cfValidateOACConfig refuses a config missing a required member or carrying a value outside a
// published enum.
//
// Four members are Required: Yes — Name, OriginAccessControlOriginType, SigningBehavior and
// SigningProtocol — and three of those four publish a closed set of valid values. InvalidArgument
// /400 is the code the operation publishes for both kinds of failure ("An argument is invalid");
// the messages are substrate's, since the reference publishes codes and not message text, and
// each names the member so a caller learns which one in one round trip rather than in four.
//
// The enum values are compared exactly. AWS renders them lowercase and the SDKs send them that
// way; accepting "S3" would let a request through substrate that AWS refuses, which is the
// direction that costs a consumer a live deploy to discover.
func cfValidateOACConfig(cfg cfOACConfigWire) *AWSError {
	if cfg.Name == "" {
		return cfInvalidOACArgument("the OriginAccessControlConfig member Name is required")
	}
	switch cfg.OriginType {
	case "s3", "mediastore", "mediapackagev2", "lambda":
	case "":
		return cfInvalidOACArgument("the OriginAccessControlConfig member OriginAccessControlOriginType is required")
	default:
		return cfInvalidOACArgument(fmt.Sprintf(
			"OriginAccessControlOriginType %q is not one of s3, mediastore, mediapackagev2, lambda", cfg.OriginType))
	}
	switch cfg.SigningBehavior {
	case "never", "always", "no-override":
	case "":
		return cfInvalidOACArgument("the OriginAccessControlConfig member SigningBehavior is required")
	default:
		return cfInvalidOACArgument(fmt.Sprintf(
			"SigningBehavior %q is not one of never, always, no-override", cfg.SigningBehavior))
	}
	switch cfg.SigningProtocol {
	case "sigv4":
	case "":
		return cfInvalidOACArgument("the OriginAccessControlConfig member SigningProtocol is required")
	default:
		return cfInvalidOACArgument(fmt.Sprintf("SigningProtocol %q is not sigv4", cfg.SigningProtocol))
	}
	return nil
}

// cfInvalidOACArgument reports that an origin access control request carries an invalid argument.
func cfInvalidOACArgument(reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidArgument",
		Message:    reason,
		HTTPStatus: http.StatusBadRequest,
	}
}

// putOriginAccessControl persists a control under the key the account and ID name.
func (p *CloudFrontPlugin) putOriginAccessControl(accountID string, oac CloudFrontOriginAccessControl) error {
	data, err := json.Marshal(oac)
	if err != nil {
		return fmt.Errorf("cloudfront putOriginAccessControl marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), cloudfrontNamespace, cfOACKey(accountID, oac.ID), data); err != nil {
		return fmt.Errorf("cloudfront putOriginAccessControl state.Put: %w", err)
	}
	return nil
}

// loadOriginAccessControl loads a control owned by accountID, answering
// NoSuchOriginAccessControl/404 if absent.
//
// The message is the description the reference publishes for the code, and it names no control —
// the same choice [CloudFrontPlugin.loadDistributionForAccount] records, for the same reason: an
// empty ID is reachable through the path /2020-05-31/origin-access-control/, so a message
// interpolating the ID would trail a bare colon.
func (p *CloudFrontPlugin) loadOriginAccessControl(accountID, oacID string) (CloudFrontOriginAccessControl, error) {
	data, err := p.state.Get(context.Background(), cloudfrontNamespace, cfOACKey(accountID, oacID))
	if err != nil {
		return CloudFrontOriginAccessControl{}, fmt.Errorf("cloudfront loadOriginAccessControl state.Get: %w", err)
	}
	if data == nil {
		return CloudFrontOriginAccessControl{}, &AWSError{
			Code:       "NoSuchOriginAccessControl",
			Message:    "The origin access control does not exist.",
			HTTPStatus: http.StatusNotFound,
		}
	}
	var oac CloudFrontOriginAccessControl
	if err := json.Unmarshal(data, &oac); err != nil {
		return CloudFrontOriginAccessControl{}, fmt.Errorf("cloudfront loadOriginAccessControl unmarshal: %w", err)
	}
	return oac, nil
}
