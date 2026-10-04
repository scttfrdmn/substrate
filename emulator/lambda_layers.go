package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Lambda layers, and what another account's public layer looks like from outside (#1272).
//
// Until #1272 substrate routed no layer operation at all, so a consumer attaching the AWS Lambda Web
// Adapter layer had nothing to resolve against. This file adds the nine operations of the family under
// the `2018-10-31` API version: PublishLayerVersion, GetLayerVersion, GetLayerVersionByArn,
// ListLayerVersions, ListLayers, DeleteLayerVersion, AddLayerVersionPermission, GetLayerVersionPolicy
// and RemoveLayerVersionPermission. Each request and response shape is the one its page publishes.
//
// # Another account's layer
//
// A layer belongs to one account in one Region. The account is the one in the layer ARN when the
// caller names the layer by ARN, and the caller's own when it names the layer by name. That is the
// whole of what substrate needs to know about principals here: the rule below is a resource-policy
// decision, independent of the caller's IAM policies, so it holds whether or not IAM enforcement is
// on.
//
// The layer's owner can do everything. Another account can do only what a statement in the layer
// version's resource policy grants it, and `API_AddLayerVersionPermission` constrains `Action` to the
// single value `lambda:GetLayerVersion`, so that is all any policy can grant. It follows that:
//
//   - `ListLayerVersions` on another account's layer is always refused, even for a public layer. This
//     is observed behavior (#1272): "User: … is not authorized to perform: lambda:ListLayerVersions on
//     resource: arn:aws:lambda:us-west-2:753240598075:layer:LambdaAdapterLayerArm64 because no
//     resource-based policy allows the lambda:ListLayerVersions action".
//   - `GetLayerVersion` and `GetLayerVersionByArn` succeed when the version's policy grants
//     `lambda:GetLayerVersion` to the caller's account, its root ARN, or `*`. Otherwise they are
//     refused, and a version that does not exist is refused **the same way**, as
//     `AccessDeniedException` rather than `ResourceNotFoundException`. That is also observed: a
//     version that was never published has no policy granting anything, so "absent" and "forbidden"
//     are one answer from outside, which is what lets a consumer probe for the newest version.
//   - The write operations and `GetLayerVersionPolicy` are refused for another account, since no
//     policy can grant them.
//
// Neither refusal is in the pages' Errors lists: `AccessDeniedException` is the authorization answer,
// with 403 and the JSON protocol's code, as [accessDeniedCodeFor] gives it for an identity-policy
// denial (#595). Both are recorded as observed provenance in docs/services.md.
//
// A statement scoped to an organization (`OrganizationId`) grants nothing here. Substrate does not
// resolve which organization a caller's account belongs to, so it cannot honor one, and refusing is
// the reading that cannot over-grant.
//
// # A seeded public layer
//
// A consumer's search loop needs another account's public layer to exist, with a known number of
// published versions, without substrate asserting facts about a real account that may age. So it is
// seeded: `POST /v1/lambda/layer-versions` with `{"layerArn", "publishedVersions"}` makes versions 1
// through N of that layer readable, each with a statement granting `lambda:GetLayerVersion` to `*`, the
// way a public layer is published. The seed is applied at read time, like every other seed, and a
// version the layer's own account published takes precedence over a seeded one of the same number.
// `DELETE /v1/lambda/layer-versions` removes one seed (`?layerArn=`) or all of them.

// lambdaLayersAPIVersion dates every layer operation.
const lambdaLayersAPIVersion = "2018-10-31"

// lambdaLayerAction is the one action a layer version's resource policy can grant:
// `API_AddLayerVersionPermission` publishes `Action` with the pattern `lambda:GetLayerVersion`.
const lambdaLayerAction = "lambda:GetLayerVersion"

// lambdaLayerCreatedDateLayout renders a layer version's CreatedDate. The pages publish the member as a
// string "in ISO-8601 format (YYYY-MM-DDThh:mm:ss.sTZD)", and Lambda renders the offset without a colon.
const lambdaLayerCreatedDateLayout = "2006-01-02T15:04:05.000-0700"

// The patterns the layer pages publish.
var (
	// lambdaLayerNamePattern is LayerName's pattern on every operation that takes one: an ARN of a
	// layer, or a bare name.
	lambdaLayerNamePattern = regexp.MustCompile(`^(?:arn:[a-zA-Z0-9-]+:lambda:[a-zA-Z0-9-]+:\d{12}:layer:[a-zA-Z0-9-_]+|[a-zA-Z0-9-_]+)$`)

	// lambdaLayerVersionARNPattern is GetLayerVersionByArn's Arn pattern.
	lambdaLayerVersionARNPattern = regexp.MustCompile(`^arn:[a-zA-Z0-9-]+:lambda:[a-zA-Z0-9-]+:\d{12}:layer:[a-zA-Z0-9-_]+:[0-9]+$`)

	// lambdaLayerPrincipalPattern is AddLayerVersionPermission's Principal pattern.
	lambdaLayerPrincipalPattern = regexp.MustCompile(`^(?:\d{12}|\*|arn:(aws[a-zA-Z-]*):iam::\d{12}:root)$`)

	// lambdaLayerStatementIDPattern is the StatementId pattern on both permission operations.
	lambdaLayerStatementIDPattern = regexp.MustCompile(`^[a-zA-Z0-9-_]+$`)

	// lambdaLayerOrganizationIDPattern is AddLayerVersionPermission's OrganizationId pattern.
	lambdaLayerOrganizationIDPattern = regexp.MustCompile(`^o-[a-z0-9]{10,32}$`)
)

// lambdaLayerArchitectures are the published CompatibleArchitectures values.
var lambdaLayerArchitectures = map[string]bool{"x86_64": true, "arm64": true}

// LambdaLayerVersion is a persisted version of a Lambda layer.
//
// The owning account and Region are carried by the state key, not by the record, so the record holds
// only what the pages publish plus the version's resource policy.
type LambdaLayerVersion struct {
	// LayerName is the layer's name.
	LayerName string `json:"LayerName"`

	// LayerArn is the layer's ARN, without a version.
	LayerArn string `json:"LayerArn"`

	// Version is the version number.
	Version int64 `json:"Version"`

	// Description is the version's description.
	Description string `json:"Description,omitempty"`

	// LicenseInfo is the layer's software license.
	LicenseInfo string `json:"LicenseInfo,omitempty"`

	// CompatibleRuntimes lists the runtimes the version declares.
	CompatibleRuntimes []string `json:"CompatibleRuntimes,omitempty"`

	// CompatibleArchitectures lists the instruction set architectures the version declares.
	CompatibleArchitectures []string `json:"CompatibleArchitectures,omitempty"`

	// CodeSha256 is the base64 SHA-256 of the archive.
	CodeSha256 string `json:"CodeSha256"`

	// CodeSize is the archive's size in bytes.
	CodeSize int64 `json:"CodeSize"`

	// CreatedDate is when the version was published.
	CreatedDate time.Time `json:"CreatedDate"`

	// Policy is the version's resource policy, or nil when no statement has been added.
	Policy *LambdaLayerVersionPolicy `json:"Policy,omitempty"`
}

// LambdaLayerVersionPolicy is a layer version's resource-based policy.
type LambdaLayerVersionPolicy struct {
	// RevisionID identifies the policy's current revision.
	RevisionID string `json:"RevisionId"`

	// Statements are the policy's statements, in the order they were added.
	Statements []LambdaLayerPermissionStatement `json:"Statements"`
}

// LambdaLayerPermissionStatement is one statement of a layer version's resource policy, as
// AddLayerVersionPermission's request gave it.
type LambdaLayerPermissionStatement struct {
	// Sid is the statement ID.
	Sid string `json:"Sid"`

	// Principal is the account ID, root ARN or `*` the statement grants.
	Principal string `json:"Principal"`

	// OrganizationID scopes a `*` grant to one organization.
	OrganizationID string `json:"OrganizationId,omitempty"`
}

// lambdaLayerMeta is a layer's own record: the highest version number it has issued. Version numbers
// are not reused after a delete, so the counter outlives the versions it numbered.
type lambdaLayerMeta struct {
	LatestVersion int64 `json:"LatestVersion"`
}

// lambdaLayerSeed is a seeded public layer: versions 1 through PublishedVersions of LayerArn are
// readable by any account.
type lambdaLayerSeed struct {
	// LayerArn is the seeded layer's ARN, without a version.
	LayerArn string `json:"layerArn"`

	// PublishedVersions is how many versions the layer has published.
	PublishedVersions int64 `json:"publishedVersions"`

	// CreatedDate is the CreatedDate every seeded version reports: the instant the seed was written,
	// on the simulated clock.
	CreatedDate time.Time `json:"createdDate"`
}

// lambdaLayerTarget is the layer a request addresses.
type lambdaLayerTarget struct {
	account, region, name string
}

// arn returns the layer's ARN, without a version.
func (t lambdaLayerTarget) arn() string {
	return "arn:aws:lambda:" + t.region + ":" + t.account + ":layer:" + t.name
}

// versionARN returns the ARN of one version of the layer.
func (t lambdaLayerTarget) versionARN(version int64) string {
	return t.arn() + ":" + strconv.FormatInt(version, 10)
}

// lambdaLayerMetaKey and lambdaLayerVersionKey are the state keys of a layer's record and of one of
// its versions, scoped by the owning account and Region.
func lambdaLayerMetaKey(t lambdaLayerTarget) string {
	return "layer:" + t.account + "/" + t.region + "/" + t.name
}

func lambdaLayerVersionKeyPrefix(t lambdaLayerTarget) string {
	return "layerversion:" + t.account + "/" + t.region + "/" + t.name + "/"
}

func lambdaLayerVersionKey(t lambdaLayerTarget, version int64) string {
	return lambdaLayerVersionKeyPrefix(t) + strconv.FormatInt(version, 10)
}

// lambdaLayerSeedKey is the control-plane key of a seeded layer.
func lambdaLayerSeedKey(layerArn string) string {
	return "layer-versions:" + layerArn
}

// lambdaLayerSeedKeyPrefix prefixes every seeded layer's key.
const lambdaLayerSeedKeyPrefix = "layer-versions:"

// lambdaResolveLayerName resolves a LayerName path parameter, a name or a layer ARN, to the layer it
// addresses. A name addresses the caller's own account and Region.
func lambdaResolveLayerName(ctx *RequestContext, layerName string) (lambdaLayerTarget, error) {
	if len(layerName) < 1 || len(layerName) > 140 || !lambdaLayerNamePattern.MatchString(layerName) {
		return lambdaLayerTarget{}, lambdaInvalidParameterValue("1 validation error detected: Value '" + layerName +
			"' at 'layerName' failed to satisfy constraint: Member must satisfy regular expression pattern: " +
			"(arn:[a-zA-Z0-9-]+:lambda:[a-zA-Z0-9-]+:\\d{12}:layer:[a-zA-Z0-9-_]+)|[a-zA-Z0-9-_]+")
	}
	if !strings.HasPrefix(layerName, "arn:") {
		return lambdaLayerTarget{account: ctx.AccountID, region: ctx.Region, name: layerName}, nil
	}
	parts := strings.Split(layerName, ":")
	return lambdaLayerTarget{region: parts[3], account: parts[4], name: parts[6]}, nil
}

// lambdaParseLayerVersionNumber parses a VersionNumber path parameter.
func lambdaParseLayerVersionNumber(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 1 {
		return 0, lambdaInvalidParameterValue("Layer version number " + strconv.Quote(s) + " is not a positive integer")
	}
	return n, nil
}

// lambdaLayerDenied is the refusal another account's caller receives for an action no policy on the
// layer grants it. The sentence is the observed one (#1272).
func lambdaLayerDenied(ctx *RequestContext, action, resource string) *AWSError {
	caller := "arn:aws:iam::" + ctx.AccountID + ":root"
	if ctx.Principal != nil && ctx.Principal.ARN != "" {
		caller = ctx.Principal.ARN
	}
	return &AWSError{
		Code: "AccessDeniedException",
		Message: fmt.Sprintf("User: %s is not authorized to perform: %s on resource: %s because no resource-based policy allows the %s action",
			caller, action, resource, action),
		HTTPStatus: http.StatusForbidden,
	}
}

// lambdaLayerVersionNotFound is the owner's answer for a layer version that does not exist.
func lambdaLayerVersionNotFound(t lambdaLayerTarget, version int64) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    "The resource you requested does not exist. (Layer version " + t.versionARN(version) + ")",
		HTTPStatus: http.StatusNotFound,
	}
}

// grants reports whether a statement grants lambda:GetLayerVersion to the account. A statement scoped to
// an organization grants nothing, because substrate does not resolve a caller's organization.
func (s LambdaLayerPermissionStatement) grants(account string) bool {
	if s.OrganizationID != "" {
		return false
	}
	switch s.Principal {
	case "*", account:
		return true
	}
	return strings.HasSuffix(s.Principal, ":iam::"+account+":root")
}

// grantsGet reports whether the version's policy lets the account read it.
func (v *LambdaLayerVersion) grantsGet(account string) bool {
	if v.Policy == nil {
		return false
	}
	for _, s := range v.Policy.Statements {
		if s.grants(account) {
			return true
		}
	}
	return false
}

// loadLayerMeta reads a layer's record, returning the zero record when it has none.
func (p *LambdaPlugin) loadLayerMeta(t lambdaLayerTarget) (lambdaLayerMeta, error) {
	var meta lambdaLayerMeta
	data, err := p.state.Get(context.Background(), lambdaNamespace, lambdaLayerMetaKey(t))
	if err != nil {
		return meta, fmt.Errorf("lambda loadLayerMeta state.Get: %w", err)
	}
	if data == nil {
		return meta, nil
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, fmt.Errorf("lambda loadLayerMeta unmarshal: %w", err)
	}
	return meta, nil
}

// loadLayerSeed reads the seed for a layer, returning nil when it has none.
func (p *LambdaPlugin) loadLayerSeed(t lambdaLayerTarget) (*lambdaLayerSeed, error) {
	data, err := p.state.Get(context.Background(), lambdaCtrlNamespace, lambdaLayerSeedKey(t.arn()))
	if err != nil {
		return nil, fmt.Errorf("lambda loadLayerSeed state.Get: %w", err)
	}
	if data == nil {
		return nil, nil //nolint:nilnil // (nil, nil) = "not seeded".
	}
	var seed lambdaLayerSeed
	if err := json.Unmarshal(data, &seed); err != nil {
		return nil, fmt.Errorf("lambda loadLayerSeed unmarshal: %w", err)
	}
	return &seed, nil
}

// seededVersion synthesizes version n of a seeded public layer.
func (s *lambdaLayerSeed) seededVersion(t lambdaLayerTarget, n int64) LambdaLayerVersion {
	sum := sha256.Sum256([]byte(t.versionARN(n)))
	return LambdaLayerVersion{
		LayerName:   t.name,
		LayerArn:    t.arn(),
		Version:     n,
		CodeSha256:  base64.StdEncoding.EncodeToString(sum[:]),
		CreatedDate: s.CreatedDate,
		Policy: &LambdaLayerVersionPolicy{
			RevisionID: hex.EncodeToString(sum[:16]),
			Statements: []LambdaLayerPermissionStatement{{Sid: "public", Principal: "*"}},
		},
	}
}

// loadLayerVersion reads one version of a layer, published or seeded, returning nil when neither has it.
// A published version takes precedence over a seeded one of the same number.
func (p *LambdaPlugin) loadLayerVersion(t lambdaLayerTarget, n int64) (*LambdaLayerVersion, error) {
	data, err := p.state.Get(context.Background(), lambdaNamespace, lambdaLayerVersionKey(t, n))
	if err != nil {
		return nil, fmt.Errorf("lambda loadLayerVersion state.Get: %w", err)
	}
	if data != nil {
		var v LambdaLayerVersion
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf("lambda loadLayerVersion unmarshal: %w", err)
		}
		return &v, nil
	}
	seed, err := p.loadLayerSeed(t)
	if err != nil {
		return nil, err
	}
	if seed != nil && n <= seed.PublishedVersions {
		v := seed.seededVersion(t, n)
		return &v, nil
	}
	return nil, nil //nolint:nilnil // (nil, nil) = "no such version".
}

// layerVersions returns every version of a layer, published and seeded, newest first.
func (p *LambdaPlugin) layerVersions(t lambdaLayerTarget) ([]LambdaLayerVersion, error) {
	keys, err := p.state.List(context.Background(), lambdaNamespace, lambdaLayerVersionKeyPrefix(t))
	if err != nil {
		return nil, fmt.Errorf("lambda layerVersions state.List: %w", err)
	}
	byVersion := map[int64]LambdaLayerVersion{}
	for _, key := range keys {
		data, getErr := p.state.Get(context.Background(), lambdaNamespace, key)
		if getErr != nil {
			return nil, fmt.Errorf("lambda layerVersions state.Get: %w", getErr)
		}
		if data == nil {
			continue
		}
		var v LambdaLayerVersion
		if err := json.Unmarshal(data, &v); err != nil {
			return nil, fmt.Errorf("lambda layerVersions unmarshal: %w", err)
		}
		byVersion[v.Version] = v
	}
	seed, err := p.loadLayerSeed(t)
	if err != nil {
		return nil, err
	}
	if seed != nil {
		for n := int64(1); n <= seed.PublishedVersions; n++ {
			if _, ok := byVersion[n]; !ok {
				byVersion[n] = seed.seededVersion(t, n)
			}
		}
	}
	out := make([]LambdaLayerVersion, 0, len(byVersion))
	for _, v := range byVersion {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version > out[j].Version })
	return out, nil
}

// putLayerVersion persists a layer version.
func (p *LambdaPlugin) putLayerVersion(t lambdaLayerTarget, v LambdaLayerVersion) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("lambda putLayerVersion marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), lambdaNamespace, lambdaLayerVersionKey(t, v.Version), data); err != nil {
		return fmt.Errorf("lambda putLayerVersion state.Put: %w", err)
	}
	return nil
}

// The wire shapes the layer pages publish.
type (
	lambdaLayerContentOut struct {
		CodeSha256 string `json:"CodeSha256"`
		CodeSize   int64  `json:"CodeSize"`
		Location   string `json:"Location"`
	}

	lambdaLayerVersionOut struct {
		CompatibleArchitectures []string              `json:"CompatibleArchitectures,omitempty"`
		CompatibleRuntimes      []string              `json:"CompatibleRuntimes,omitempty"`
		Content                 lambdaLayerContentOut `json:"Content"`
		CreatedDate             string                `json:"CreatedDate"`
		Description             string                `json:"Description,omitempty"`
		LayerArn                string                `json:"LayerArn"`
		LayerVersionArn         string                `json:"LayerVersionArn"`
		LicenseInfo             string                `json:"LicenseInfo,omitempty"`
		Version                 int64                 `json:"Version"`
	}

	lambdaLayerVersionsListItem struct {
		CompatibleArchitectures []string `json:"CompatibleArchitectures,omitempty"`
		CompatibleRuntimes      []string `json:"CompatibleRuntimes,omitempty"`
		CreatedDate             string   `json:"CreatedDate"`
		Description             string   `json:"Description,omitempty"`
		LayerVersionArn         string   `json:"LayerVersionArn"`
		LicenseInfo             string   `json:"LicenseInfo,omitempty"`
		Version                 int64    `json:"Version"`
	}

	lambdaLayersListItem struct {
		LatestMatchingVersion lambdaLayerVersionsListItem `json:"LatestMatchingVersion"`
		LayerArn              string                      `json:"LayerArn"`
		LayerName             string                      `json:"LayerName"`
	}
)

func (v LambdaLayerVersion) toWire(t lambdaLayerTarget) lambdaLayerVersionOut {
	return lambdaLayerVersionOut{
		CompatibleArchitectures: v.CompatibleArchitectures,
		CompatibleRuntimes:      v.CompatibleRuntimes,
		Content: lambdaLayerContentOut{
			CodeSha256: v.CodeSha256,
			CodeSize:   v.CodeSize,
			Location:   "https://lambda-stub.localhost/layers/" + t.account + "/" + t.name + "/" + strconv.FormatInt(v.Version, 10),
		},
		CreatedDate:     v.CreatedDate.UTC().Format(lambdaLayerCreatedDateLayout),
		Description:     v.Description,
		LayerArn:        t.arn(),
		LayerVersionArn: t.versionARN(v.Version),
		LicenseInfo:     v.LicenseInfo,
		Version:         v.Version,
	}
}

func (v LambdaLayerVersion) toListItem(t lambdaLayerTarget) lambdaLayerVersionsListItem {
	return lambdaLayerVersionsListItem{
		CompatibleArchitectures: v.CompatibleArchitectures,
		CompatibleRuntimes:      v.CompatibleRuntimes,
		CreatedDate:             v.CreatedDate.UTC().Format(lambdaLayerCreatedDateLayout),
		Description:             v.Description,
		LayerVersionArn:         t.versionARN(v.Version),
		LicenseInfo:             v.LicenseInfo,
		Version:                 v.Version,
	}
}

// matches reports whether the version declares the runtime and architecture a list filter names. An
// empty filter matches every version.
func (v LambdaLayerVersion) matches(runtime, architecture string) bool {
	contains := func(list []string, s string) bool {
		for _, x := range list {
			if x == s {
				return true
			}
		}
		return false
	}
	if runtime != "" && !contains(v.CompatibleRuntimes, runtime) {
		return false
	}
	return architecture == "" || contains(v.CompatibleArchitectures, architecture)
}

// lambdaLayerListParams reads the filters and pagination the two list operations share.
func lambdaLayerListParams(req *AWSRequest) (runtime, architecture string, maxItems int, err error) {
	runtime = req.Params["CompatibleRuntime"]
	architecture = req.Params["CompatibleArchitecture"]
	if architecture != "" && !lambdaLayerArchitectures[architecture] {
		return "", "", 0, lambdaInvalidParameterValue("Value '" + architecture +
			"' at 'compatibleArchitecture' failed to satisfy constraint: Member must satisfy enum value set: [x86_64, arm64]")
	}
	maxItems = 50
	if s := req.Params["MaxItems"]; s != "" {
		n, convErr := strconv.Atoi(s)
		if convErr != nil || n < 1 || n > 50 {
			return "", "", 0, lambdaInvalidParameterValue("Value '" + s +
				"' at 'maxItems' failed to satisfy constraint: Member must have value between 1 and 50")
		}
		maxItems = n
	}
	return runtime, architecture, maxItems, nil
}

// publishLayerVersion handles PublishLayerVersion.
func (p *LambdaPlugin) publishLayerVersion(ctx *RequestContext, req *AWSRequest, layerName string) (*AWSResponse, error) {
	t, err := lambdaResolveLayerName(ctx, layerName)
	if err != nil {
		return nil, err
	}
	if t.account != ctx.AccountID {
		return nil, lambdaLayerDenied(ctx, "lambda:PublishLayerVersion", t.arn())
	}
	var body struct {
		CompatibleArchitectures []string `json:"CompatibleArchitectures"`
		CompatibleRuntimes      []string `json:"CompatibleRuntimes"`
		Content                 *struct {
			S3Bucket        string `json:"S3Bucket"`
			S3Key           string `json:"S3Key"`
			S3ObjectVersion string `json:"S3ObjectVersion"`
			ZipFile         string `json:"ZipFile"`
		} `json:"Content"`
		Description string `json:"Description"`
		LicenseInfo string `json:"LicenseInfo"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, lambdaInvalidBody()
	}
	switch {
	case body.Content == nil:
		return nil, lambdaInvalidParameterValue("Content is required")
	case len(body.CompatibleArchitectures) > 2:
		return nil, lambdaInvalidParameterValue("CompatibleArchitectures must have at most 2 items")
	case len(body.CompatibleRuntimes) > 15:
		return nil, lambdaInvalidParameterValue("CompatibleRuntimes must have at most 15 items")
	case len(body.Description) > 256:
		return nil, lambdaInvalidParameterValue("Description must be at most 256 characters")
	case len(body.LicenseInfo) > 512:
		return nil, lambdaInvalidParameterValue("LicenseInfo must be at most 512 characters")
	}
	for _, a := range body.CompatibleArchitectures {
		if !lambdaLayerArchitectures[a] {
			return nil, lambdaInvalidParameterValue("Value '" + a + "' in CompatibleArchitectures is not one of [x86_64, arm64]")
		}
	}

	var codeSize int64
	var codeSha string
	switch {
	case body.Content.ZipFile != "":
		decoded, decErr := base64.StdEncoding.DecodeString(body.Content.ZipFile)
		if decErr != nil {
			return nil, lambdaInvalidParameterValue("Content.ZipFile is not valid base64")
		}
		codeSize, codeSha = int64(len(decoded)), lambdaCodeSha256(decoded)
	case body.Content.S3Bucket != "" && body.Content.S3Key != "":
		codeSize, codeSha = p.sizeS3Package(body.Content.S3Bucket, body.Content.S3Key, body.Content.S3ObjectVersion)
	default:
		return nil, lambdaInvalidParameterValue("Content must name a ZipFile, or an S3Bucket and S3Key")
	}

	meta, err := p.loadLayerMeta(t)
	if err != nil {
		return nil, err
	}
	seed, err := p.loadLayerSeed(t)
	if err != nil {
		return nil, err
	}
	next := meta.LatestVersion
	if seed != nil && seed.PublishedVersions > next {
		next = seed.PublishedVersions
	}
	next++

	v := LambdaLayerVersion{
		LayerName:               t.name,
		LayerArn:                t.arn(),
		Version:                 next,
		Description:             body.Description,
		LicenseInfo:             body.LicenseInfo,
		CompatibleRuntimes:      body.CompatibleRuntimes,
		CompatibleArchitectures: body.CompatibleArchitectures,
		CodeSha256:              codeSha,
		CodeSize:                codeSize,
		CreatedDate:             p.tc.Now().UTC(),
	}
	if err := p.putLayerVersion(t, v); err != nil {
		return nil, err
	}
	metaData, err := json.Marshal(lambdaLayerMeta{LatestVersion: next})
	if err != nil {
		return nil, fmt.Errorf("lambda publishLayerVersion marshal meta: %w", err)
	}
	if err := p.state.Put(context.Background(), lambdaNamespace, lambdaLayerMetaKey(t), metaData); err != nil {
		return nil, fmt.Errorf("lambda publishLayerVersion state.Put meta: %w", err)
	}
	return lambdaJSONResponse(http.StatusCreated, v.toWire(t))
}

// getLayerVersion handles GetLayerVersion and, through getLayerVersionByArn, GetLayerVersionByArn.
func (p *LambdaPlugin) getLayerVersion(ctx *RequestContext, t lambdaLayerTarget, n int64) (*AWSResponse, error) {
	v, err := p.loadLayerVersion(t, n)
	if err != nil {
		return nil, err
	}
	if t.account != ctx.AccountID {
		// Absent and ungranted are one answer from outside (#1272).
		if v == nil || !v.grantsGet(ctx.AccountID) {
			return nil, lambdaLayerDenied(ctx, lambdaLayerAction, t.versionARN(n))
		}
	} else if v == nil {
		return nil, lambdaLayerVersionNotFound(t, n)
	}
	return lambdaJSONResponse(http.StatusOK, v.toWire(t))
}

// getLayerVersionByArn handles GetLayerVersionByArn: `GET /2018-10-31/layers?find=LayerVersion&Arn=…`.
func (p *LambdaPlugin) getLayerVersionByArn(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arn := req.Params["Arn"]
	if len(arn) > 140 || !lambdaLayerVersionARNPattern.MatchString(arn) {
		return nil, lambdaInvalidParameterValue("1 validation error detected: Value '" + arn +
			"' at 'arn' failed to satisfy constraint: Member must satisfy regular expression pattern: " +
			"arn:[a-zA-Z0-9-]+:lambda:[a-zA-Z0-9-]+:\\d{12}:layer:[a-zA-Z0-9-_]+:[0-9]+")
	}
	cut := strings.LastIndex(arn, ":")
	t, err := lambdaResolveLayerName(ctx, arn[:cut])
	if err != nil {
		return nil, err
	}
	n, err := lambdaParseLayerVersionNumber(arn[cut+1:])
	if err != nil {
		return nil, err
	}
	return p.getLayerVersion(ctx, t, n)
}

// listLayerVersions handles ListLayerVersions.
func (p *LambdaPlugin) listLayerVersions(ctx *RequestContext, req *AWSRequest, t lambdaLayerTarget) (*AWSResponse, error) {
	if t.account != ctx.AccountID {
		// No layer policy can grant ListLayerVersions, so another account is always refused (#1272).
		return nil, lambdaLayerDenied(ctx, "lambda:ListLayerVersions", t.arn())
	}
	runtime, architecture, maxItems, err := lambdaLayerListParams(req)
	if err != nil {
		return nil, err
	}
	versions, err := p.layerVersions(t)
	if err != nil {
		return nil, err
	}
	matching := make([]LambdaLayerVersion, 0, len(versions))
	for _, v := range versions {
		if v.matches(runtime, architecture) {
			matching = append(matching, v)
		}
	}
	start := 0
	if marker := req.Params["Marker"]; marker != "" {
		start = -1
		for i, v := range matching {
			if strconv.FormatInt(v.Version, 10) == marker {
				start = i
				break
			}
		}
		if start < 0 {
			return nil, lambdaInvalidParameterValue("Marker " + strconv.Quote(marker) + " was not issued by ListLayerVersions")
		}
	}
	end := min(start+maxItems, len(matching))
	items := make([]lambdaLayerVersionsListItem, 0, end-start)
	for _, v := range matching[start:end] {
		items = append(items, v.toListItem(t))
	}
	out := map[string]any{"LayerVersions": items}
	if end < len(matching) {
		out["NextMarker"] = strconv.FormatInt(matching[end].Version, 10)
	}
	return lambdaJSONResponse(http.StatusOK, out)
}

// listLayers handles ListLayers: the caller's own layers in its Region, each with its latest version
// matching the filters. A layer with no matching version is not listed.
func (p *LambdaPlugin) listLayers(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	runtime, architecture, maxItems, err := lambdaLayerListParams(req)
	if err != nil {
		return nil, err
	}
	scope := lambdaLayerTarget{account: ctx.AccountID, region: ctx.Region}
	names := map[string]bool{}
	keys, err := p.state.List(context.Background(), lambdaNamespace, "layer:"+scope.account+"/"+scope.region+"/")
	if err != nil {
		return nil, fmt.Errorf("lambda listLayers state.List: %w", err)
	}
	for _, key := range keys {
		names[key[strings.LastIndex(key, "/")+1:]] = true
	}
	seedKeys, err := p.state.List(context.Background(), lambdaCtrlNamespace, lambdaLayerSeedKeyPrefix)
	if err != nil {
		return nil, fmt.Errorf("lambda listLayers seeds state.List: %w", err)
	}
	ownPrefix := lambdaLayerSeedKeyPrefix + scope.arn()
	for _, key := range seedKeys {
		if strings.HasPrefix(key, ownPrefix) {
			names[key[len(ownPrefix):]] = true
		}
	}
	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)

	var layers []lambdaLayersListItem
	for _, name := range sorted {
		t := lambdaLayerTarget{account: scope.account, region: scope.region, name: name}
		versions, verr := p.layerVersions(t)
		if verr != nil {
			return nil, verr
		}
		for _, v := range versions {
			if v.matches(runtime, architecture) {
				layers = append(layers, lambdaLayersListItem{LatestMatchingVersion: v.toListItem(t), LayerArn: t.arn(), LayerName: name})
				break
			}
		}
	}
	start := 0
	if marker := req.Params["Marker"]; marker != "" {
		start = -1
		for i, l := range layers {
			if l.LayerName == marker {
				start = i
				break
			}
		}
		if start < 0 {
			return nil, lambdaInvalidParameterValue("Marker " + strconv.Quote(marker) + " was not issued by ListLayers")
		}
	}
	end := min(start+maxItems, len(layers))
	page := make([]lambdaLayersListItem, 0, end-start)
	page = append(page, layers[start:end]...)
	out := map[string]any{"Layers": page}
	if end < len(layers) {
		out["NextMarker"] = layers[end].LayerName
	}
	return lambdaJSONResponse(http.StatusOK, out)
}

// deleteLayerVersion handles DeleteLayerVersion. Its page publishes no not-found refusal, so deleting a
// version that does not exist succeeds. The layer's version counter is kept, so a number is never
// reused.
func (p *LambdaPlugin) deleteLayerVersion(ctx *RequestContext, t lambdaLayerTarget, n int64) (*AWSResponse, error) {
	if t.account != ctx.AccountID {
		return nil, lambdaLayerDenied(ctx, "lambda:DeleteLayerVersion", t.versionARN(n))
	}
	if err := p.state.Delete(context.Background(), lambdaNamespace, lambdaLayerVersionKey(t, n)); err != nil {
		return nil, fmt.Errorf("lambda deleteLayerVersion state.Delete: %w", err)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}

// lambdaLayerStatementJSON renders one statement as the policy document carries it. A bare account ID
// is rendered as that account's root ARN.
func lambdaLayerStatementJSON(t lambdaLayerTarget, n int64, s LambdaLayerPermissionStatement) map[string]any {
	var principal any = "*"
	if s.Principal != "*" {
		arn := s.Principal
		if !strings.HasPrefix(arn, "arn:") {
			arn = "arn:aws:iam::" + arn + ":root"
		}
		principal = map[string]string{"AWS": arn}
	}
	stmt := map[string]any{
		"Sid":       s.Sid,
		"Effect":    "Allow",
		"Principal": principal,
		"Action":    lambdaLayerAction,
		"Resource":  t.versionARN(n),
	}
	if s.OrganizationID != "" {
		stmt["Condition"] = map[string]any{"StringEquals": map[string]string{"aws:PrincipalOrgID": s.OrganizationID}}
	}
	return stmt
}

// ownedLayerVersion loads a version of a layer the caller must own, refusing another account and
// answering ResourceNotFoundException for a version that does not exist.
func (p *LambdaPlugin) ownedLayerVersion(ctx *RequestContext, t lambdaLayerTarget, n int64, action string) (*LambdaLayerVersion, error) {
	if t.account != ctx.AccountID {
		return nil, lambdaLayerDenied(ctx, action, t.versionARN(n))
	}
	v, err := p.loadLayerVersion(t, n)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, lambdaLayerVersionNotFound(t, n)
	}
	return v, nil
}

// lambdaLayerRevisionMismatch is the refusal for a RevisionId that is not the policy's current one.
func lambdaLayerRevisionMismatch() *AWSError {
	return &AWSError{
		Code:       "PreconditionFailedException",
		Message:    "The RevisionId provided does not match the latest RevisionId for the Lambda function or alias. Call the GetLayerVersionPolicy API to retrieve the latest RevisionId for your resource.",
		HTTPStatus: http.StatusPreconditionFailed,
	}
}

// checkRevision refuses a RevisionId that is not the policy's current one. No RevisionId is no check.
func (v *LambdaLayerVersion) checkRevision(revisionID string) error {
	if revisionID == "" {
		return nil
	}
	if v.Policy == nil || v.Policy.RevisionID != revisionID {
		return lambdaLayerRevisionMismatch()
	}
	return nil
}

// addLayerVersionPermission handles AddLayerVersionPermission.
func (p *LambdaPlugin) addLayerVersionPermission(ctx *RequestContext, req *AWSRequest, t lambdaLayerTarget, n int64) (*AWSResponse, error) {
	var body struct {
		Action         string `json:"Action"`
		OrganizationID string `json:"OrganizationId"`
		Principal      string `json:"Principal"`
		StatementID    string `json:"StatementId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, lambdaInvalidBody()
	}
	switch {
	case body.Action != lambdaLayerAction:
		return nil, lambdaInvalidParameterValue("Action must be " + lambdaLayerAction)
	case !lambdaLayerPrincipalPattern.MatchString(body.Principal):
		return nil, lambdaInvalidParameterValue("Principal must be an account ID, an account root ARN, or *")
	case len(body.StatementID) < 1 || len(body.StatementID) > 100 || !lambdaLayerStatementIDPattern.MatchString(body.StatementID):
		return nil, lambdaInvalidParameterValue("StatementId must be 1 to 100 characters of [a-zA-Z0-9-_]")
	case body.OrganizationID != "" && !lambdaLayerOrganizationIDPattern.MatchString(body.OrganizationID):
		return nil, lambdaInvalidParameterValue("OrganizationId must match o-[a-z0-9]{10,32}")
	}

	v, err := p.ownedLayerVersion(ctx, t, n, "lambda:AddLayerVersionPermission")
	if err != nil {
		return nil, err
	}
	if err := v.checkRevision(req.Params["RevisionId"]); err != nil {
		return nil, err
	}
	if v.Policy == nil {
		v.Policy = &LambdaLayerVersionPolicy{}
	}
	for _, s := range v.Policy.Statements {
		if s.Sid == body.StatementID {
			return nil, &AWSError{
				Code:       "ResourceConflictException",
				Message:    "The statement id (" + body.StatementID + ") provided already exists. Please provide a new statement id, or remove the existing statement.",
				HTTPStatus: http.StatusConflict,
			}
		}
	}
	stmt := LambdaLayerPermissionStatement{Sid: body.StatementID, Principal: body.Principal, OrganizationID: body.OrganizationID}
	v.Policy.Statements = append(v.Policy.Statements, stmt)
	v.Policy.RevisionID = ctx.IDs.UUID()
	if err := p.putLayerVersion(t, *v); err != nil {
		return nil, err
	}
	stmtJSON, err := json.Marshal(lambdaLayerStatementJSON(t, n, stmt))
	if err != nil {
		return nil, fmt.Errorf("lambda addLayerVersionPermission marshal statement: %w", err)
	}
	return lambdaJSONResponse(http.StatusCreated, map[string]string{"Statement": string(stmtJSON), "RevisionId": v.Policy.RevisionID})
}

// getLayerVersionPolicy handles GetLayerVersionPolicy.
func (p *LambdaPlugin) getLayerVersionPolicy(ctx *RequestContext, t lambdaLayerTarget, n int64) (*AWSResponse, error) {
	v, err := p.ownedLayerVersion(ctx, t, n, "lambda:GetLayerVersionPolicy")
	if err != nil {
		return nil, err
	}
	if v.Policy == nil || len(v.Policy.Statements) == 0 {
		return nil, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    "No policy is associated with the given resource.",
			HTTPStatus: http.StatusNotFound,
		}
	}
	stmts := make([]map[string]any, 0, len(v.Policy.Statements))
	for _, s := range v.Policy.Statements {
		stmts = append(stmts, lambdaLayerStatementJSON(t, n, s))
	}
	doc, err := json.Marshal(map[string]any{"Version": "2012-10-17", "Id": "default", "Statement": stmts})
	if err != nil {
		return nil, fmt.Errorf("lambda getLayerVersionPolicy marshal: %w", err)
	}
	return lambdaJSONResponse(http.StatusOK, map[string]string{"Policy": string(doc), "RevisionId": v.Policy.RevisionID})
}

// removeLayerVersionPermission handles RemoveLayerVersionPermission.
func (p *LambdaPlugin) removeLayerVersionPermission(ctx *RequestContext, req *AWSRequest, t lambdaLayerTarget, n int64, statementID string) (*AWSResponse, error) {
	if len(statementID) < 1 || len(statementID) > 100 || !lambdaLayerStatementIDPattern.MatchString(statementID) {
		return nil, lambdaInvalidParameterValue("StatementId must be 1 to 100 characters of [a-zA-Z0-9-_]")
	}
	v, err := p.ownedLayerVersion(ctx, t, n, "lambda:RemoveLayerVersionPermission")
	if err != nil {
		return nil, err
	}
	if err := v.checkRevision(req.Params["RevisionId"]); err != nil {
		return nil, err
	}
	kept := []LambdaLayerPermissionStatement{}
	found := false
	if v.Policy != nil {
		for _, s := range v.Policy.Statements {
			if s.Sid == statementID {
				found = true
				continue
			}
			kept = append(kept, s)
		}
	}
	if !found {
		return nil, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    "Statement " + statementID + " is not found in resource policy.",
			HTTPStatus: http.StatusNotFound,
		}
	}
	v.Policy.Statements = kept
	v.Policy.RevisionID = ctx.IDs.UUID()
	if err := p.putLayerVersion(t, *v); err != nil {
		return nil, err
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}}, nil
}

// handleLayer dispatches the layer operations that address one layer, resolving the layer and, where
// the path carries one, the version.
func (p *LambdaPlugin) handleLayer(ctx *RequestContext, req *AWSRequest, op, layerName, sub string) (*AWSResponse, error) {
	if op == "PublishLayerVersion" {
		return p.publishLayerVersion(ctx, req, layerName)
	}
	t, err := lambdaResolveLayerName(ctx, layerName)
	if err != nil {
		return nil, err
	}
	if op == "ListLayerVersions" {
		return p.listLayerVersions(ctx, req, t)
	}
	versionPart, statementID, _ := strings.Cut(sub, "/")
	n, err := lambdaParseLayerVersionNumber(versionPart)
	if err != nil {
		return nil, err
	}
	switch op {
	case "GetLayerVersion":
		return p.getLayerVersion(ctx, t, n)
	case "DeleteLayerVersion":
		return p.deleteLayerVersion(ctx, t, n)
	case "AddLayerVersionPermission":
		return p.addLayerVersionPermission(ctx, req, t, n)
	case "GetLayerVersionPolicy":
		return p.getLayerVersionPolicy(ctx, t, n)
	case "RemoveLayerVersionPermission":
		return p.removeLayerVersionPermission(ctx, req, t, n, statementID)
	}
	return nil, unknownRouteError(p.Name(), requestMethod(req), req.Path)
}

// lambdaLayerOperation resolves the operations under `/2018-10-31/layers`. The layer name is the first
// segment; the sub-resource carries the version number and, for RemoveLayerVersionPermission, the
// statement ID as `{version}/{statementId}`.
func lambdaLayerOperation(method, p string) (op, name, subResource string) {
	if p == "/layers" || p == "/layers/" {
		if method == http.MethodGet {
			return "ListLayers", "", ""
		}
		return lambdaUnknownOperation, "", ""
	}
	segs := strings.Split(strings.TrimPrefix(p, "/layers/"), "/")
	if segs[0] == "" || len(segs) < 2 || segs[1] != "versions" {
		return lambdaUnknownOperation, "", ""
	}
	layer := segs[0]
	switch len(segs) {
	case 2:
		switch method {
		case http.MethodPost:
			return "PublishLayerVersion", layer, ""
		case http.MethodGet:
			return "ListLayerVersions", layer, ""
		}
	case 3:
		switch method {
		case http.MethodGet:
			return "GetLayerVersion", layer, segs[2]
		case http.MethodDelete:
			return "DeleteLayerVersion", layer, segs[2]
		}
	case 4:
		if segs[3] == "policy" {
			switch method {
			case http.MethodPost:
				return "AddLayerVersionPermission", layer, segs[2]
			case http.MethodGet:
				return "GetLayerVersionPolicy", layer, segs[2]
			}
		}
	case 5:
		if segs[3] == "policy" && method == http.MethodDelete {
			return "RemoveLayerVersionPermission", layer, segs[2] + "/" + segs[4]
		}
	}
	return lambdaUnknownOperation, "", ""
}

// lambdaRequestOperation is [parseLambdaOperation] with the one distinction a path cannot make:
// GetLayerVersionByArn shares ListLayers' path and is told apart by its `find=LayerVersion` query
// parameter.
func lambdaRequestOperation(req *AWSRequest) (op, name, subResource string) {
	op, name, subResource = parseLambdaOperation(requestMethod(req), req.Path)
	if op == "ListLayers" && req.Params["find"] == "LayerVersion" {
		return "GetLayerVersionByArn", "", ""
	}
	return op, name, subResource
}

// lambdaLayerAuthzResourceARN returns the resource ARN a layer request is authorized against: the
// layer, or the layer version when the path names one.
func lambdaLayerAuthzResourceARN(p, region, accountID string) string {
	segs := strings.Split(strings.TrimPrefix(p, "/layers/"), "/")
	if segs[0] == "" || strings.HasPrefix(p, "/layers?") || p == "/layers" {
		return "*"
	}
	arn := segs[0]
	if !strings.HasPrefix(arn, "arn:") {
		arn = "arn:aws:lambda:" + region + ":" + accountID + ":layer:" + arn
	}
	if len(segs) >= 3 && segs[1] == "versions" {
		arn += ":" + segs[2]
	}
	return arn
}

// handleLambdaSeedLayerVersions handles POST /v1/lambda/layer-versions. It seeds another account's
// public layer: versions 1 through publishedVersions of layerArn become readable by any account.
// Body: {"layerArn", "publishedVersions"}.
func (s *Server) handleLambdaSeedLayerVersions(w http.ResponseWriter, r *http.Request) {
	var seed lambdaLayerSeed
	if err := json.NewDecoder(r.Body).Decode(&seed); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}
	if !lambdaLayerNamePattern.MatchString(seed.LayerArn) || !strings.HasPrefix(seed.LayerArn, "arn:") {
		http.Error(w, `{"error":"layerArn must be a layer ARN, without a version"}`, http.StatusBadRequest)
		return
	}
	if seed.PublishedVersions < 0 {
		http.Error(w, `{"error":"publishedVersions must not be negative"}`, http.StatusBadRequest)
		return
	}
	seed.CreatedDate = s.tc.Now().UTC()
	data, err := json.Marshal(seed)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if err := s.state.Put(r.Context(), lambdaCtrlNamespace, lambdaLayerSeedKey(seed.LayerArn), data); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
		return
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true, "layerArn": seed.LayerArn, "publishedVersions": seed.PublishedVersions})
}

// handleLambdaClearLayerVersions handles DELETE /v1/lambda/layer-versions. With ?layerArn=… it removes
// that seed; without it removes every seeded layer.
func (s *Server) handleLambdaClearLayerVersions(w http.ResponseWriter, r *http.Request) {
	keys := []string{lambdaLayerSeedKey(r.URL.Query().Get("layerArn"))}
	if r.URL.Query().Get("layerArn") == "" {
		all, err := s.state.List(r.Context(), lambdaCtrlNamespace, lambdaLayerSeedKeyPrefix)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
		keys = all
	}
	for _, key := range keys {
		if err := s.state.Delete(r.Context(), lambdaCtrlNamespace, key); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusInternalServerError)
			return
		}
	}
	writeJSONDebug(w, s.logger, map[string]any{"ok": true})
}
