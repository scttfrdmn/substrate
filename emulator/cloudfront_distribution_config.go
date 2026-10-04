package emulator

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"
)

// A distribution keeps the configuration it was sent, and UpdateDistribution replaces it (#1271).
//
// API_UpdateDistribution: "The new configuration replaces the existing configuration. The values
// that you specify in an UpdateDistribution request are not merged into your existing
// configuration. Make sure to include all fields: the ones that you modified and also the ones that
// you didn't." The documented way to honor that is read-modify-write — GetDistributionConfig, change
// your own fields, UpdateDistribution with the ETag as If-Match — and it is only testable if
// GetDistributionConfig answers back what a caller could have written. Until #1271 substrate recorded
// two members of the configuration (Comment and Enabled) and merged an update into them, so a
// from-scratch update succeeded here and failed against CloudFront, one field per round trip.
//
// # Why a tree rather than a struct
//
// DistributionConfig has twenty-odd members and several deep subtrees (a cache behavior alone has
// twenty). Substrate interprets none of them beyond Comment and Enabled — it routes no traffic — so
// a typed model would be a large schema whose only use is to echo the document back, and every
// member it omitted would be silently dropped on the round trip. [cfConfigNode] keeps whatever
// element tree the caller sent, in order, and the handful of checks below navigate it by name.
//
// # What CreateDistribution defaults, and what UpdateDistribution then requires
//
// CloudFront fills in members a create omitted, and refuses an update that omits them. The three a
// real account answered, one per round trip, in this order (#1271, observed — the messages are not
// published anywhere):
//
//   - no Aliases → IllegalUpdate, "Aliases are missing for the resource";
//   - an origin with no CustomHeaders → IllegalUpdate, "The 'OriginCustomHeaders' field is missing";
//   - a cache behavior with no SmoothStreaming → InvalidArgument, "The parameter SmoothStreaming
//     flag is missing".
//
// So a create records the configuration with those three filled in (Aliases and CustomHeaders with
// Quantity 0, SmoothStreaming false), GetDistributionConfig answers them, and a read-modify-write
// round-trips. The issue's reporter stopped at three, and suspects more of the same family
// (OriginPath, TrustedSigners, Compress, connection timeouts…); none of those is enforced, because
// nothing but the three was observed and the reference marks each of them Required: No.
//
// # What is not enforced, and why
//
// API_DistributionConfig marks five members Required: Yes — CallerReference, Comment,
// DefaultCacheBehavior, Enabled and Origins. CreateDistribution does not enforce them (it never has;
// that is #1197's class), so a distribution substrate holds may lack Origins or DefaultCacheBehavior.
// Refusing those on update alone would make a distribution substrate itself created un-updatable,
// which is worse than either consistent answer. The update therefore enforces Comment and Enabled —
// the two scalars it must have to replace the record's own values, and which every caller sends —
// and not the other three. CallerReference is enforced in the one way the reference states for an
// update: it cannot change.

// cfConfigNode is one element of a distribution configuration, kept as a generic tree.
//
// Names are held without their namespace (see [cfNormalizeConfig]), so a document a caller sent
// with CloudFront's xmlns and one sent without it are the same tree, and the tree marshals back
// without a namespace declaration on every element.
type cfConfigNode struct {
	XMLName xml.Name
	Text    string         `xml:",chardata"`
	Nodes   []cfConfigNode `xml:",any"`
}

// child returns the first direct child named local, or nil.
func (n *cfConfigNode) child(local string) *cfConfigNode {
	for i := range n.Nodes {
		if n.Nodes[i].XMLName.Local == local {
			return &n.Nodes[i]
		}
	}
	return nil
}

// path returns the descendant reached by following locals one child at a time, or nil.
func (n *cfConfigNode) path(locals ...string) *cfConfigNode {
	cur := n
	for _, local := range locals {
		if cur = cur.child(local); cur == nil {
			return nil
		}
	}
	return cur
}

// children returns every direct child named local.
func (n *cfConfigNode) children(local string) []*cfConfigNode {
	var out []*cfConfigNode
	for i := range n.Nodes {
		if n.Nodes[i].XMLName.Local == local {
			out = append(out, &n.Nodes[i])
		}
	}
	return out
}

// setText sets the text of the direct child named local, appending the child if it is absent.
func (n *cfConfigNode) setText(local, text string) {
	if c := n.child(local); c != nil {
		c.Text = text
		c.Nodes = nil
		return
	}
	n.Nodes = append(n.Nodes, cfConfigNode{XMLName: xml.Name{Local: local}, Text: text})
}

// ensure appends def as a child when no child of its name is present.
func (n *cfConfigNode) ensure(def cfConfigNode) {
	if n.child(def.XMLName.Local) == nil {
		n.Nodes = append(n.Nodes, def)
	}
}

// cfQuantityZero is a list member with no items, the form CloudFront defaults an omitted list to.
func cfQuantityZero(local string) cfConfigNode {
	return cfConfigNode{XMLName: xml.Name{Local: local}, Nodes: []cfConfigNode{
		{XMLName: xml.Name{Local: "Quantity"}, Text: "0"},
	}}
}

// cfNormalizeConfig strips namespaces and the whitespace between elements, recursively.
func cfNormalizeConfig(n *cfConfigNode) {
	n.XMLName.Space = ""
	if len(n.Nodes) > 0 && strings.TrimSpace(n.Text) == "" {
		n.Text = ""
	}
	for i := range n.Nodes {
		cfNormalizeConfig(&n.Nodes[i])
	}
}

// errCFNoConfig reports a body holding no DistributionConfig element.
var errCFNoConfig = errors.New("no DistributionConfig element")

// cfParseDistributionConfig decodes the DistributionConfig in body.
//
// The document may be the configuration itself or wrap it one level down — a
// DistributionConfigWithTags, or the CreateDistributionRequest wrapper CreateDistribution has
// always tolerated — so the first element named DistributionConfig at the root or among its
// children is the one taken.
func cfParseDistributionConfig(body []byte) (cfConfigNode, error) {
	var root cfConfigNode
	if err := xml.NewDecoder(bytes.NewReader(body)).Decode(&root); err != nil {
		if errors.Is(err, io.EOF) {
			return cfConfigNode{}, errCFNoConfig
		}
		return cfConfigNode{}, err
	}
	cfNormalizeConfig(&root)
	if root.XMLName.Local == "DistributionConfig" {
		return root, nil
	}
	if c := root.child("DistributionConfig"); c != nil {
		return *c, nil
	}
	return cfConfigNode{}, errCFNoConfig
}

// cfDefaultCreatedConfig fills in what CreateDistribution defaults: Aliases, each origin's
// CustomHeaders and each cache behavior's SmoothStreaming, the three members UpdateDistribution
// was observed to require (see the file comment). Comment and Enabled are set from the values the
// record holds, so the configuration and the record cannot disagree about them.
func cfDefaultCreatedConfig(cfg *cfConfigNode, comment string, enabled bool) {
	cfg.XMLName = xml.Name{Local: "DistributionConfig"}
	cfg.ensure(cfQuantityZero("Aliases"))
	for _, origin := range cfConfigItems(cfg, "Origins", "Origin") {
		origin.ensure(cfQuantityZero("CustomHeaders"))
	}
	for _, behavior := range cfCacheBehaviors(cfg) {
		behavior.ensure(cfConfigNode{XMLName: xml.Name{Local: "SmoothStreaming"}, Text: "false"})
	}
	cfg.setText("Comment", comment)
	cfg.setText("Enabled", cfBoolText(enabled))
}

// cfConfigItems returns the items of a list member: cfg/{list}/Items/{item}.
func cfConfigItems(cfg *cfConfigNode, list, item string) []*cfConfigNode {
	items := cfg.path(list, "Items")
	if items == nil {
		return nil
	}
	return items.children(item)
}

// cfCacheBehaviors returns the default cache behavior, when present, and every other cache behavior.
func cfCacheBehaviors(cfg *cfConfigNode) []*cfConfigNode {
	var out []*cfConfigNode
	if def := cfg.child("DefaultCacheBehavior"); def != nil {
		out = append(out, def)
	}
	return append(out, cfConfigItems(cfg, "CacheBehaviors", "CacheBehavior")...)
}

// cfBoolText renders a boolean as CloudFront's XML does.
func cfBoolText(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// cfConfigError is a 400 refusal of an update's configuration with the given code.
func cfConfigError(code, message string) *AWSError {
	return &AWSError{Code: code, Message: message, HTTPStatus: http.StatusBadRequest}
}

// cfCheckUpdatedConfig refuses an UpdateDistribution configuration CloudFront refuses, one member
// per call, in the order a real account answered them (see the file comment), then the members the
// update needs to replace the record and the CallerReference rule. priorRef is the CallerReference
// the distribution was created with, or "" if it recorded none.
func cfCheckUpdatedConfig(cfg *cfConfigNode, priorRef string) error {
	if cfg.child("Aliases") == nil {
		return cfConfigError("IllegalUpdate", "Aliases are missing for the resource")
	}
	for _, origin := range cfConfigItems(cfg, "Origins", "Origin") {
		if origin.child("CustomHeaders") == nil {
			return cfConfigError("IllegalUpdate", "The 'OriginCustomHeaders' field is missing")
		}
	}
	for _, behavior := range cfCacheBehaviors(cfg) {
		if behavior.child("SmoothStreaming") == nil {
			return cfConfigError("InvalidArgument", "The parameter SmoothStreaming flag is missing")
		}
	}
	for _, member := range []string{"Comment", "Enabled"} {
		if cfg.child(member) == nil {
			return cfConfigError("InvalidArgument", "The parameter "+member+" is required.")
		}
	}
	if enabled := strings.TrimSpace(cfg.child("Enabled").Text); enabled != "true" && enabled != "false" {
		return cfConfigError("InvalidArgument", "The parameter Enabled must be true or false.")
	}
	ref := ""
	if c := cfg.child("CallerReference"); c != nil {
		ref = strings.TrimSpace(c.Text)
	}
	if priorRef != "" && ref != priorRef {
		return cfConfigError("IllegalUpdate", "The CallerReference of a distribution cannot be changed.")
	}
	return nil
}

// cfConfigCallerReference returns the configuration's CallerReference text, or "".
func cfConfigCallerReference(cfg *cfConfigNode) string {
	if c := cfg.child("CallerReference"); c != nil {
		return strings.TrimSpace(c.Text)
	}
	return ""
}

// cfMarshalConfig renders a configuration tree as the stored, and answered, document.
func cfMarshalConfig(cfg cfConfigNode) (string, error) {
	out, err := xml.Marshal(cfg)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// cfStoredConfig returns the distribution's configuration tree.
//
// A record written before #1271 holds no configuration; it answers the one a create of its Comment
// and Enabled would have recorded, so GetDistributionConfig and a read-modify-write work on it too.
// A stored document that no longer parses is an error, never an empty configuration: answering one
// would invite the caller to send it back and replace the real configuration with nothing.
func cfStoredConfig(dist CloudFrontDistribution) (cfConfigNode, error) {
	if dist.Config == "" {
		cfg := cfConfigNode{}
		cfDefaultCreatedConfig(&cfg, dist.Comment, dist.Enabled)
		return cfg, nil
	}
	return cfParseDistributionConfig([]byte(dist.Config))
}

// cfDistributionETag is the version a distribution's If-Match must echo.
//
// A record written before #1271 carries no ETag. Its version is its own ID — the same E-prefixed
// shape [cfMintETag] produces — so a distribution restored from such a snapshot can still be
// updated and deleted, and the first update gives it a minted version like any other.
func cfDistributionETag(dist CloudFrontDistribution) string {
	if dist.ETag != "" {
		return dist.ETag
	}
	return dist.ID
}

// cfCheckIfMatch refuses a request whose If-Match does not name the distribution's current
// version: InvalidIfMatchVersion/400 when it is missing or not a version substrate could have
// issued, PreconditionFailed/412 when it is a well-formed but stale one. These are the codes,
// statuses and sentences API_UpdateDistribution and API_DeleteDistribution publish, and the
// distinction is [CloudFrontPlugin.deleteOriginAccessControl]'s.
func cfCheckIfMatch(req *AWSRequest, dist CloudFrontDistribution) error {
	ifMatch := strings.Trim(strings.TrimSpace(headerValueFold(req.Headers, "If-Match")), `"`)
	if !cfIsMintedVersion(ifMatch) {
		return &AWSError{
			Code:       "InvalidIfMatchVersion",
			Message:    "The If-Match version is missing or not valid for the resource.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if ifMatch != cfDistributionETag(dist) {
		return &AWSError{
			Code:       "PreconditionFailed",
			Message:    "The precondition in one or more of the request fields evaluated to false.",
			HTTPStatus: http.StatusPreconditionFailed,
		}
	}
	return nil
}
