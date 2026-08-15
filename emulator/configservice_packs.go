package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"regexp"
	"slices"
	"strings"
	"time"
)

// The AWS Config conformance-pack cluster (#580).
//
// A conformance pack deploys asynchronously: PutConformancePack returns an ARN
// immediately and the pack sits in CREATE_IN_PROGRESS until the CloudFormation stack
// behind it finishes, so the consumer code worth testing here is a *waiter* — a loop
// polling DescribeConformancePackStatus until the state is terminal. Substrate
// resolves that transition on first observation and persists the result, following
// resolveCreateAccountStatus (organizations_account.go): a waiter converges in one
// poll with no dependence on wall-clock or simulated time, and a status that
// re-resolved — flipping back to CREATE_IN_PROGRESS, or moving its
// LastUpdateCompletedTime — would make a waiter comparing successive polls loop
// forever. Clock-driven transitions are the subject of #514; choosing a duration
// here would front-run that design.
//
// Substrate does not deploy the template. The rules a pack declares are therefore
// not created, and DescribeConformancePackCompliance reports the rules a *seed*
// names — because inferring them would mean parsing arbitrary CloudFormation to
// guess at a listing a consumer would then assert against. Compliance is seeded for
// the reason the rule cluster's is: evaluating a rule against resource state is
// workload-internal, not an API observation.
//
// Two enum asymmetries have to be respected here, and neither is guessable from the
// rule cluster:
//
//   - ConformancePackComplianceType has **no NOT_APPLICABLE**, so a seed carrying
//     one is refused rather than stored as a value no response shape can hold.
//   - the *filter* narrows further still — "The allowed values are COMPLIANT and
//     NON_COMPLIANT. INSUFFICIENT_DATA is not supported" — so INSUFFICIENT_DATA is a
//     verdict a pack can report but not a value a caller may filter on.

// --- limits, all from the vendored model's own bounds ---

const (
	// cfgsvcMaxConformancePacks is the conformance-pack ceiling. The Developer Guide's
	// Service Limits page gives 50 and marks it not adjustable.
	//
	// The page says "per account", not per account per Region, and substrate counts per
	// account *and* Region because every key in this service is regional. That is the
	// permissive direction — 50 packs in each of two Regions is accepted where AWS might
	// refuse the 51st overall — and permissive is the right way to be wrong: a wrong
	// refusal breaks working consumer code, while a wrong acceptance only fails to
	// reproduce a limit no test fixture approaches.
	cfgsvcMaxConformancePacks = 50

	// cfgsvcMaxPackInputParameters is ConformancePackInputParameters' max (0-60).
	cfgsvcMaxPackInputParameters = 60

	// cfgsvcMaxPackNamesFilter is ConformancePackNamesList's max (0-25), the optional
	// filter the two describe operations take.
	cfgsvcMaxPackNamesFilter = 25

	// cfgsvcMaxPackNamesToSummarize is ConformancePackNamesToSummarizeList's max. Its
	// min is 1, and the member is required — the summary is the one operation in this
	// cluster that cannot be asked for "every pack".
	cfgsvcMaxPackNamesToSummarize = 5

	// cfgsvcPageSizeLimit is PageSizeLimit's max (0-20), shared by
	// DescribeConformancePacks, DescribeConformancePackStatus and
	// GetConformancePackComplianceSummary.
	//
	// It is deliberately not the rule cluster's 100. The caps differ per operation in
	// the model, and collapsing them onto one number would let a fixture page in units
	// AWS would refuse.
	cfgsvcPageSizeLimit = 20

	// cfgsvcPackComplianceLimit is DescribeConformancePackComplianceLimit's max
	// (0-1000) — a third ceiling on the same service.
	cfgsvcPackComplianceLimit = 1000

	// cfgsvcMaxPackFilterRuleNames is ConformancePackConfigRuleNames' max (0-10).
	cfgsvcMaxPackFilterRuleNames = 10

	// cfgsvcMaxPackFilterRuleNameLen is the length ceiling on a *filtered* rule name,
	// which is StringWithCharLimit64 — 64, not the 128 that ConfigRuleName carries in
	// DescribeConfigRules and in ConformancePackRuleCompliance. The asymmetry is the
	// model's; a rule whose name is 65-128 characters long can be reported by this
	// operation but not filtered for by it.
	cfgsvcMaxPackFilterRuleNameLen = 64

	// cfgsvcMaxPackControls is ControlsList's max (0-20), each member 1-128 characters.
	// A control is "a process to prevent or detect problems while meeting objectives" —
	// substrate carries whatever a seed names and derives nothing from it.
	cfgsvcMaxPackControls = 20
)

// Conformance-pack deployment states, the ConformancePackState enum.
//
// There is no UPDATE_ state and no DELETE_COMPLETE, even though PutConformancePack
// is documented as "Creates or updates": an update re-enters CREATE_IN_PROGRESS, and
// a completed delete is reported by the pack's absence rather than by a state.
const (
	cfgsvcPackCreateInProgress = "CREATE_IN_PROGRESS"
	cfgsvcPackCreateComplete   = "CREATE_COMPLETE"
	cfgsvcPackCreateFailed     = "CREATE_FAILED"
	cfgsvcPackDeleteInProgress = "DELETE_IN_PROGRESS"
	cfgsvcPackDeleteFailed     = "DELETE_FAILED"
)

// cfgsvcPackStates is the ConformancePackState enum, which the status seed validates
// against so a fixture cannot pin a state no SDK enum member matches.
var cfgsvcPackStates = []string{
	cfgsvcPackCreateInProgress,
	cfgsvcPackCreateComplete,
	cfgsvcPackCreateFailed,
	cfgsvcPackDeleteInProgress,
	cfgsvcPackDeleteFailed,
}

// cfgsvcPackFailedStates are the two states that carry a status reason.
var cfgsvcPackFailedStates = []string{cfgsvcPackCreateFailed, cfgsvcPackDeleteFailed}

// cfgsvcPackComplianceTypes is the ConformancePackComplianceType enum, which has no
// NOT_APPLICABLE member; see this file's header comment.
var cfgsvcPackComplianceTypes = []string{
	cfgsvcCompliant,
	cfgsvcNonCompliant,
	cfgsvcInsufficientData,
}

// cfgsvcPackComplianceFilterTypes is the narrower subset ConformancePackComplianceFilters
// accepts, again per this file's header comment.
var cfgsvcPackComplianceFilterTypes = []string{cfgsvcCompliant, cfgsvcNonCompliant}

// cfgsvcPackNamePattern is ConformancePackName's pattern, [a-zA-Z][-a-zA-Z0-9]*: a
// letter, then letters, digits and hyphens. Note it admits neither an underscore nor
// a leading digit, unlike ConfigRuleName's near-anything .*\S.*.
var cfgsvcPackNamePattern = regexp.MustCompile(`^[a-zA-Z][-a-zA-Z0-9]*$`)

// cfgsvcSSMDocumentNamePattern is SSMDocumentName's pattern, which bounds the length
// itself.
var cfgsvcSSMDocumentNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_\-.:/]{3,200}$`)

// cfgsvcSSMDocumentVersionPattern is SSMDocumentVersion's pattern: $LATEST, $DEFAULT
// or a positive integer.
var cfgsvcSSMDocumentVersionPattern = regexp.MustCompile(`^([$]LATEST|[$]DEFAULT|[1-9][0-9]*)$`)

// --- shapes ---

// ConfigConformancePackInputParameter is a ConformancePackInputParameter: one
// key-value pair passed to the pack's template. Both members are required.
type ConfigConformancePackInputParameter struct {
	// ParameterName is one half of the pair, up to 255 characters.
	ParameterName string `json:"ParameterName"`

	// ParameterValue is the other half, up to 4096 characters.
	ParameterValue string `json:"ParameterValue"`
}

// ConfigTemplateSSMDocumentDetails names the SSM document a pack template lives in.
type ConfigTemplateSSMDocumentDetails struct {
	// DocumentName is the name or ARN of the SSM document. Required.
	DocumentName string `json:"DocumentName"`

	// DocumentVersion is the document version; AWS uses the latest when it is empty.
	DocumentVersion string `json:"DocumentVersion,omitempty"`
}

// ConfigConformancePack is a conformance pack as substrate stores it.
//
// It carries the union of ConformancePackDetail and ConformancePackStatusDetail,
// because both operations describe the same pack and splitting the record would let
// the two disagree about its ID or ARN. Which members each operation emits is
// decided when its response is built, not by what is stored.
type ConfigConformancePack struct {
	// ConformancePackName is the pack's unique name within an account and Region.
	ConformancePackName string `json:"ConformancePackName"`

	// ConformancePackArn is the ARN substrate mints, conformance-pack/<name>/<id>.
	ConformancePackArn string `json:"ConformancePackArn"`

	// ConformancePackId is the deterministically minted pack ID.
	ConformancePackId string `json:"ConformancePackId"` //nolint:revive // wire name.

	// DeliveryS3Bucket is the optional bucket Config stores pack templates in.
	DeliveryS3Bucket string `json:"DeliveryS3Bucket,omitempty"`

	// DeliveryS3KeyPrefix is the optional prefix within that bucket.
	DeliveryS3KeyPrefix string `json:"DeliveryS3KeyPrefix,omitempty"`

	// ConformancePackInputParameters are the template's parameters.
	ConformancePackInputParameters []ConfigConformancePackInputParameter `json:"ConformancePackInputParameters,omitempty"`

	// TemplateSSMDocumentDetails names the SSM document the pack was built from, when
	// it was built from one.
	TemplateSSMDocumentDetails *ConfigTemplateSSMDocumentDetails `json:"TemplateSSMDocumentDetails,omitempty"`

	// LastUpdateRequestedTime is when the create or update was requested. It is a
	// required member of the status shape, so it is always set.
	LastUpdateRequestedTime EpochSeconds `json:"LastUpdateRequestedTime,omitempty"`

	// LastUpdateCompletedTime is when the deployment completed, and is unset while the
	// pack is still in progress.
	LastUpdateCompletedTime EpochSeconds `json:"LastUpdateCompletedTime,omitempty"`

	// ConformancePackState is the stored deployment state. What an observation reports
	// may differ from it, because a seed is applied at read time — see
	// cfgsvcPackStateView.
	ConformancePackState string `json:"ConformancePackState"`

	// ConformancePackStatusReason explains a CREATE_FAILED or DELETE_FAILED state. It is
	// never stored, only filled in from a seed as a response is built.
	ConformancePackStatusReason string `json:"ConformancePackStatusReason,omitempty"`

	// StackArn is the ARN of the CloudFormation stack Config deploys the pack through,
	// synthesized because the status shape requires it.
	StackArn string `json:"StackArn"`

	// TemplateBody is the template the caller submitted, held as recorded intent:
	// substrate never deploys it, and no response in this cluster emits it — neither
	// ConformancePackDetail nor ConformancePackStatusDetail has a template member.
	TemplateBody string `json:"TemplateBody,omitempty"`

	// TemplateS3Uri is the S3 location a template was named at, likewise recorded and
	// never fetched.
	TemplateS3Uri string `json:"TemplateS3Uri,omitempty"`
}

// cfgsvcPutPackRequest is PutConformancePackRequest.
//
// It has no Tags member in the vendored model, though the API reference's
// idempotency note discusses what a second Put does to a pack's tags. Substrate
// models the vendored shape: there is no tag input here to preserve or ignore.
type cfgsvcPutPackRequest struct {
	ConformancePackName            string                                `json:"ConformancePackName"`
	TemplateS3Uri                  string                                `json:"TemplateS3Uri"`
	TemplateBody                   string                                `json:"TemplateBody"`
	DeliveryS3Bucket               string                                `json:"DeliveryS3Bucket"`
	DeliveryS3KeyPrefix            string                                `json:"DeliveryS3KeyPrefix"`
	ConformancePackInputParameters []ConfigConformancePackInputParameter `json:"ConformancePackInputParameters"`
	TemplateSSMDocumentDetails     *ConfigTemplateSSMDocumentDetails     `json:"TemplateSSMDocumentDetails"`
}

// cfgsvcDescribePacksRequest is shared by DescribeConformancePacks and
// DescribeConformancePackStatus, whose inputs are member-for-member identical.
type cfgsvcDescribePacksRequest struct {
	ConformancePackNames []string `json:"ConformancePackNames"`
	Limit                int      `json:"Limit"`
	NextToken            string   `json:"NextToken"`
}

// cfgsvcDeletePackRequest is DeleteConformancePackRequest.
type cfgsvcDeletePackRequest struct {
	ConformancePackName string `json:"ConformancePackName"`
}

// cfgsvcPackComplianceFilters is ConformancePackComplianceFilters.
type cfgsvcPackComplianceFilters struct {
	ConfigRuleNames []string `json:"ConfigRuleNames"`
	ComplianceType  string   `json:"ComplianceType"`
}

// cfgsvcPackComplianceRequest is DescribeConformancePackComplianceRequest.
type cfgsvcPackComplianceRequest struct {
	ConformancePackName string                       `json:"ConformancePackName"`
	Filters             *cfgsvcPackComplianceFilters `json:"Filters"`
	Limit               int                          `json:"Limit"`
	NextToken           string                       `json:"NextToken"`
}

// cfgsvcPackSummaryRequest is GetConformancePackComplianceSummaryRequest.
type cfgsvcPackSummaryRequest struct {
	ConformancePackNames []string `json:"ConformancePackNames"`
	Limit                int      `json:"Limit"`
	NextToken            string   `json:"NextToken"`
}

// --- state keys and ARNs ---

// cfgsvcPackKey holds one conformance pack, by account, Region and name.
func cfgsvcPackKey(accountID, region, name string) string {
	return "pack:" + accountID + "/" + region + "/" + name
}

// cfgsvcPackNamesKey holds the index of pack names for an account and Region.
func cfgsvcPackNamesKey(accountID, region string) string {
	return "pack_names:" + accountID + "/" + region
}

// cfgsvcPackARN builds a conformance pack's ARN, conformance-pack/<Name>/<Id>, per
// the Service Authorization Reference. Both components appear, which is why the ID
// alone does not identify a pack.
func cfgsvcPackARN(ctx *RequestContext, name, packID string) string {
	return cfgsvcARN(ctx, "conformance-pack", name+"/"+packID)
}

// cfgsvcMintPackID derives a pack's ConformancePackId deterministically, for the
// reason cfgsvcMintRuleID does: a random or clock-derived ID would make every replay
// of the same event stream mint different ARNs, and replay equality is the property
// the whole emulator rests on.
//
// AWS's form is conformance-pack-<opaque>, which this reproduces so a consumer that
// parses or asserts on the shape of an ID behaves the same way.
func cfgsvcMintPackID(accountID, region, name string) string {
	sum := sha256.Sum256([]byte("conformance-pack/" + accountID + "/" + region + "/" + name))
	return "conformance-pack-" + strings.ToLower(base64.RawURLEncoding.EncodeToString(sum[:6]))[:8]
}

// cfgsvcPackStackARN synthesizes the ARN of the CloudFormation stack Config deploys
// a pack through.
//
// StackArn is a *required* member of ConformancePackStatusDetail, so it cannot be
// omitted: a consumer decoding a required member into a non-pointer field would get
// an empty string rather than an error. Substrate creates no stack — no
// CloudFormation state backs this ARN — so it is a well-formed identifier for a
// stack that does not exist, which is the honest alternative to leaving a required
// member blank.
//
// The awsconfigconforms-<pack name>-<pack id> stack-name form is provenance-worthy:
// it appears only in the Developer Guide's sample CLI output, never in normative
// prose, and matches the AWSServiceRoleForConfigConforms service-linked role's
// naming. The trailing component is a stack ID, which AWS gives as a UUID; substrate
// derives a UUID-shaped value from the same hash so it is stable across replays.
func cfgsvcPackStackARN(ctx *RequestContext, name, packID string) string {
	stackName := "awsconfigconforms-" + name + "-" + strings.TrimPrefix(packID, "conformance-pack-")
	return "arn:aws:cloudformation:" + ctx.Region + ":" + ctx.AccountID + ":stack/" + stackName + "/" +
		cfgsvcMintStackID(ctx.AccountID, ctx.Region, name)
}

// cfgsvcMintStackID derives a UUID-shaped CloudFormation stack ID deterministically,
// so the synthesized StackArn has the shape a consumer's parsing expects.
func cfgsvcMintStackID(accountID, region, name string) string {
	sum := sha256.Sum256([]byte("awsconfigconforms/" + accountID + "/" + region + "/" + name))
	hexed := hex.EncodeToString(sum[:16])
	return hexed[0:8] + "-" + hexed[8:12] + "-" + hexed[12:16] + "-" + hexed[16:20] + "-" + hexed[20:32]
}

// --- errors ---

// cfgsvcNoSuchPack is NoSuchConformancePackException.
func cfgsvcNoSuchPack() *AWSError {
	return cfgsvcErr("NoSuchConformancePackException",
		"You specified one or more conformance packs that do not exist.")
}

// cfgsvcInvalidLimit is InvalidLimitException, which every paginated operation in
// this cluster declares for an out-of-range Limit.
//
// The rule cluster answers InvalidParameterValueException for the same complaint,
// because none of its operations declares InvalidLimitException while all four
// paginated pack operations do. A caller's error handling is written against the
// codes the operation it called declares, so borrowing one cluster's code for the
// other would send it down a branch it has not got.
func cfgsvcInvalidLimit() *AWSError {
	return cfgsvcErr("InvalidLimitException", "The specified limit is outside the allowable range.")
}

// cfgsvcPackTemplateValidation is ConformancePackTemplateValidationException, whose
// own message is "You have specified a template that is not valid or supported." The
// caller-facing detail is appended, because that sentence alone does not say which
// bound the template broke.
func cfgsvcPackTemplateValidation(detail string) *AWSError {
	return cfgsvcErr("ConformancePackTemplateValidationException",
		"You have specified a template that is not valid or supported. "+detail)
}

// cfgsvcPackResourceInUse is ResourceInUseException for a pack whose deployment has
// not reached a terminal state.
//
// The exception's own documentation is a bulleted list covering seven unrelated
// cases, so the message is the bullet belonging to the operation that raised it,
// verbatim. A caller reading the message gets the one sentence that applies rather
// than a list it has to filter.
func cfgsvcPackResourceInUse(operation string) *AWSError {
	message := "For PutConformancePack and PutOrganizationConformancePack, a conformance pack " +
		"creation, update, and deletion is in progress. Try your request again later."
	if operation == "DeleteConformancePack" {
		message = "For DeleteConformancePack, a conformance pack creation, update, and deletion " +
			"is in progress. Try your request again later."
	}
	return cfgsvcErr("ResourceInUseException", "You see this exception in the following cases: "+message)
}

// packOperation claims the conformance-pack operations.
func (p *ConfigServicePlugin) packOperation(op string) (cfgsvcHandler, bool) {
	switch op {
	case "PutConformancePack":
		return p.putConformancePack, true
	case "DescribeConformancePacks":
		return p.describeConformancePacks, true
	case "DescribeConformancePackStatus":
		return p.describeConformancePackStatus, true
	case "DescribeConformancePackCompliance":
		return p.describeConformancePackCompliance, true
	case "GetConformancePackComplianceSummary":
		return p.getConformancePackComplianceSummary, true
	case "DeleteConformancePack":
		return p.deleteConformancePack, true
	}
	return nil, false
}

// putConformancePack creates or updates a conformance pack.
//
// The response carries the ARN and nothing else, so a caller learns the pack's state
// only by polling DescribeConformancePackStatus — which is the waiter this cluster
// exists to make testable.
//
// A Put arriving before the pack's deployment reaches a terminal state is
// ResourceInUseException. That window is reachable in substrate precisely because
// the state resolves on *observation*: a pack created and immediately Put again has
// never been observed, so it is still CREATE_IN_PROGRESS and the second Put is
// refused, exactly as AWS refuses it.
func (p *ConfigServicePlugin) putConformancePack(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcPutPackRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if err := cfgsvcCheckPackName(in.ConformancePackName); err != nil {
		return nil, err
	}
	if err := cfgsvcCheckPackTemplate(&in); err != nil {
		return nil, err
	}
	if err := cfgsvcCheckPackDelivery(&in); err != nil {
		return nil, err
	}
	if err := cfgsvcCheckPackInputParameters(in.ConformancePackInputParameters); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	var existing ConfigConformancePack
	found, err := p.cfgsvcGetJSON(goCtx,
		cfgsvcPackKey(ctx.AccountID, ctx.Region, in.ConformancePackName), &existing)
	if err != nil {
		return nil, err
	}
	if found {
		state, _, stateErr := p.cfgsvcPackStateView(goCtx, ctx, &existing)
		if stateErr != nil {
			return nil, stateErr
		}
		if !cfgsvcPackIsTerminal(state) {
			return nil, cfgsvcPackResourceInUse(req.Operation)
		}
	}

	names, err := p.cfgsvcPackNames(goCtx, ctx)
	if err != nil {
		return nil, err
	}
	if !found && len(names) >= cfgsvcMaxConformancePacks {
		return nil, cfgsvcErr("MaxNumberOfConformancePacksExceededException",
			"You have reached the limit of the number of conformance packs you can create in an "+
				"account. For more information, see Service Limits in the Config Developer Guide.")
	}

	packID := cfgsvcMintPackID(ctx.AccountID, ctx.Region, in.ConformancePackName)
	pack := ConfigConformancePack{
		ConformancePackName:            in.ConformancePackName,
		ConformancePackId:              packID,
		ConformancePackArn:             cfgsvcPackARN(ctx, in.ConformancePackName, packID),
		DeliveryS3Bucket:               in.DeliveryS3Bucket,
		DeliveryS3KeyPrefix:            in.DeliveryS3KeyPrefix,
		ConformancePackInputParameters: in.ConformancePackInputParameters,
		TemplateSSMDocumentDetails:     in.TemplateSSMDocumentDetails,
		TemplateBody:                   in.TemplateBody,
		TemplateS3Uri:                  in.TemplateS3Uri,
		StackArn:                       cfgsvcPackStackARN(ctx, in.ConformancePackName, packID),
		// An update re-enters CREATE_IN_PROGRESS — there is no UPDATE_ state in the enum —
		// and its LastUpdateCompletedTime is left cleared. Carrying the previous completion
		// forward would let a waiter return before the update it just requested was
		// observed at all, which is the bug a waiter exists to prevent.
		ConformancePackState:    cfgsvcPackCreateInProgress,
		LastUpdateRequestedTime: EpochSeconds(p.tc.Now()),
	}
	if err := p.cfgsvcSavePack(goCtx, ctx, &pack); err != nil {
		return nil, err
	}
	return cfgsvcJSONResponse(map[string]interface{}{
		"ConformancePackArn": pack.ConformancePackArn,
	}, "PutConformancePack")
}

// cfgsvcCheckPackName validates ConformancePackName's bounds and pattern.
func cfgsvcCheckPackName(name string) error {
	if name == "" {
		return cfgsvcInvalidParameter("ConformancePackName is required.")
	}
	if len(name) > cfgsvcMaxNameLen {
		return cfgsvcInvalidParameter("The conformance pack name must be between 1 and 256 " +
			"characters long.")
	}
	if !cfgsvcPackNamePattern.MatchString(name) {
		return cfgsvcInvalidParameter("The conformance pack name must match the pattern " +
			"[a-zA-Z][-a-zA-Z0-9]*: it must begin with a letter and may contain only letters, " +
			"numbers and hyphens.")
	}
	return nil
}

// cfgsvcCheckPackTemplate enforces "You must specify only one of the follow
// parameters: TemplateS3Uri, TemplateBody or TemplateSSMDocumentDetails" — AWS's
// own sentence, its "the follow" typo included, so a consumer matching on message
// text matches.
//
// Two is refused because the reference says so. **Zero is also refused, and that
// part is substrate's reading rather than a documented rule**: all three members are
// individually optional and no sentence says one is required, but a pack with no
// template declares no rules, so accepting one would report CREATE_COMPLETE for a
// deployment that could not have happened.
//
// The template's *content* is never judged beyond its bounds. Substrate does not
// deploy it, and refusing a template AWS would accept would break working consumer
// code — the worse of the two failures.
func cfgsvcCheckPackTemplate(in *cfgsvcPutPackRequest) error {
	supplied := 0
	if in.TemplateS3Uri != "" {
		supplied++
	}
	if in.TemplateBody != "" {
		supplied++
	}
	if in.TemplateSSMDocumentDetails != nil {
		supplied++
	}
	if supplied != 1 {
		return cfgsvcInvalidParameter("You must specify only one of the follow parameters: " +
			"TemplateS3Uri, TemplateBody or TemplateSSMDocumentDetails.")
	}

	switch {
	case in.TemplateS3Uri != "":
		if len(in.TemplateS3Uri) > 1024 {
			return cfgsvcInvalidParameter("The TemplateS3Uri may be up to 1024 characters long.")
		}
		if !strings.HasPrefix(in.TemplateS3Uri, "s3://") {
			return cfgsvcPackTemplateValidation("The TemplateS3Uri must match the pattern s3://.*, " +
				"naming an Amazon S3 bucket and key.")
		}
	case in.TemplateBody != "":
		if len(in.TemplateBody) > 51200 {
			return cfgsvcPackTemplateValidation("The TemplateBody has a minimum length of 1 byte " +
				"and a maximum length of 51,200 bytes.")
		}
	case in.TemplateSSMDocumentDetails != nil:
		ssm := in.TemplateSSMDocumentDetails
		if !cfgsvcSSMDocumentNamePattern.MatchString(ssm.DocumentName) {
			return cfgsvcInvalidParameter("The DocumentName is required and must match the " +
				`pattern ^[a-zA-Z0-9_\-.:/]{3,200}$.`)
		}
		if ssm.DocumentVersion != "" && !cfgsvcSSMDocumentVersionPattern.MatchString(ssm.DocumentVersion) {
			return cfgsvcInvalidParameter("The DocumentVersion must be $LATEST, $DEFAULT or a " +
				"positive version number.")
		}
	}
	return nil
}

// cfgsvcCheckPackDelivery validates the optional delivery bucket and prefix against
// DeliveryS3Bucket (0-63) and DeliveryS3KeyPrefix (0-1024). Both have a minimum of
// zero, so an empty value is accepted rather than treated as a missing one.
func cfgsvcCheckPackDelivery(in *cfgsvcPutPackRequest) error {
	if len(in.DeliveryS3Bucket) > 63 {
		return cfgsvcInvalidParameter("The DeliveryS3Bucket may be up to 63 characters long.")
	}
	if len(in.DeliveryS3KeyPrefix) > 1024 {
		return cfgsvcInvalidParameter("The DeliveryS3KeyPrefix may be up to 1024 characters long.")
	}
	return nil
}

// cfgsvcCheckPackInputParameters validates the parameter list's length and each
// pair's bounds. Both members are required, so an empty ParameterName is refused
// rather than stored as a nameless parameter the template could never resolve.
func cfgsvcCheckPackInputParameters(params []ConfigConformancePackInputParameter) error {
	if len(params) > cfgsvcMaxPackInputParameters {
		return cfgsvcInvalidParameter("ConformancePackInputParameters accepts up to 60 parameters.")
	}
	for _, param := range params {
		if param.ParameterName == "" || len(param.ParameterName) > 255 {
			return cfgsvcInvalidParameter("The ParameterName is required and may be up to 255 " +
				"characters long.")
		}
		if len(param.ParameterValue) > 4096 {
			return cfgsvcInvalidParameter("The ParameterValue may be up to 4096 characters long.")
		}
	}
	return nil
}

// cfgsvcPackIsTerminal reports whether a pack's state is one a Put or a Delete may
// act on. The two in-progress states are not: "You cannot update a conformance pack
// while it is in this state".
func cfgsvcPackIsTerminal(state string) bool {
	return state != cfgsvcPackCreateInProgress && state != cfgsvcPackDeleteInProgress
}

// cfgsvcSavePack stores a pack and its name index through one function, so the index
// cannot exist on one side only — the v0.99.0 saveAccount lesson.
func (p *ConfigServicePlugin) cfgsvcSavePack(goCtx context.Context, ctx *RequestContext,
	pack *ConfigConformancePack) error {
	if err := p.cfgsvcPutJSON(goCtx,
		cfgsvcPackKey(ctx.AccountID, ctx.Region, pack.ConformancePackName), pack); err != nil {
		return err
	}
	names, err := p.cfgsvcPackNames(goCtx, ctx)
	if err != nil {
		return err
	}
	if slices.Contains(names, pack.ConformancePackName) {
		return nil
	}
	names = append(names, pack.ConformancePackName)
	return p.cfgsvcPutJSON(goCtx, cfgsvcPackNamesKey(ctx.AccountID, ctx.Region), names)
}

// cfgsvcDeletePackRecord removes a pack, its index entry and its tags, the counterpart
// to cfgsvcSavePack.
//
// The tags go with it because "if you delete a resource, any tags for the resource are
// also deleted". That matters here specifically because a pack's ARN is deterministic:
// a rebuilt pack of the same name has the same ARN, so tags left behind would be read
// back as tags on the new pack, which no AWS account does. PutConformancePack has no
// Tags member of its own — a pack can only be tagged through TagResource — so this is
// the only place a pack's tags are ever removed.
func (p *ConfigServicePlugin) cfgsvcDeletePackRecord(goCtx context.Context, ctx *RequestContext,
	pack *ConfigConformancePack) error {
	name := pack.ConformancePackName
	for _, key := range []string{
		cfgsvcPackKey(ctx.AccountID, ctx.Region, name),
		cfgsvcTagsKey(pack.ConformancePackArn),
	} {
		if err := p.cfgsvcDeleteKey(goCtx, key); err != nil {
			return err
		}
	}
	names, err := p.cfgsvcPackNames(goCtx, ctx)
	if err != nil {
		return err
	}
	remaining := make([]string, 0, len(names))
	for _, existing := range names {
		if existing != name {
			remaining = append(remaining, existing)
		}
	}
	return p.cfgsvcPutJSON(goCtx, cfgsvcPackNamesKey(ctx.AccountID, ctx.Region), remaining)
}

// cfgsvcPackNames reads the pack-name index for an account and Region.
func (p *ConfigServicePlugin) cfgsvcPackNames(goCtx context.Context, ctx *RequestContext) ([]string, error) {
	var names []string
	if _, err := p.cfgsvcGetJSON(goCtx, cfgsvcPackNamesKey(ctx.AccountID, ctx.Region), &names); err != nil {
		return nil, err
	}
	return names, nil
}

// describeConformancePacks reports the packs' configuration — not their deployment
// state, which is DescribeConformancePackStatus's job.
//
// The split matters for the same reason the recorder's does: a pack in this listing
// may have failed to deploy, and a consumer that reads presence as success has the
// bug this cluster makes visible.
func (p *ConfigServicePlugin) describeConformancePacks(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	packs, next, err := p.cfgsvcPagePacks(ctx, req, true)
	if err != nil {
		return nil, err
	}
	details := make([]map[string]interface{}, 0, len(packs))
	for i := range packs {
		details = append(details, cfgsvcPackDetailShape(&packs[i]))
	}
	body := map[string]interface{}{"ConformancePackDetails": details}
	if next != "" {
		body["NextToken"] = next
	}
	return cfgsvcJSONResponse(body, "DescribeConformancePacks")
}

// describeConformancePackStatus reports each pack's deployment state, resolving
// CREATE_IN_PROGRESS to its terminal state on this observation.
//
// "If there are no conformance packs then you will see an empty result", and this
// operation does not declare NoSuchConformancePackException at all, so a name
// matching nothing contributes nothing to the list rather than refusing the call.
// DescribeConformancePacks does declare it, and does refuse.
func (p *ConfigServicePlugin) describeConformancePackStatus(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	packs, next, err := p.cfgsvcPagePacks(ctx, req, false)
	if err != nil {
		return nil, err
	}
	details := make([]map[string]interface{}, 0, len(packs))
	for i := range packs {
		if err := p.cfgsvcResolvePackState(ctx, &packs[i]); err != nil {
			return nil, err
		}
		details = append(details, cfgsvcPackStatusShape(&packs[i]))
	}
	body := map[string]interface{}{"ConformancePackStatusDetails": details}
	if next != "" {
		body["NextToken"] = next
	}
	return cfgsvcJSONResponse(body, "DescribeConformancePackStatus")
}

// cfgsvcPagePacks decodes the input the two describe operations share, selects the
// named packs and paginates them at PageSizeLimit's ceiling of 20.
//
// refuseUnknown says whether a named pack that does not exist refuses the whole
// call, which differs between the two callers: only DescribeConformancePacks
// declares NoSuchConformancePackException.
func (p *ConfigServicePlugin) cfgsvcPagePacks(ctx *RequestContext, req *AWSRequest, refuseUnknown bool) (
	[]ConfigConformancePack, string, error) {
	var in cfgsvcDescribePacksRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, "", err
	}
	if len(in.ConformancePackNames) > cfgsvcMaxPackNamesFilter {
		return nil, "", cfgsvcInvalidParameter("ConformancePackNames accepts up to 25 pack names.")
	}
	if in.Limit < 0 || in.Limit > cfgsvcPageSizeLimit {
		return nil, "", cfgsvcInvalidLimit()
	}
	for _, name := range in.ConformancePackNames {
		if err := cfgsvcCheckPackName(name); err != nil {
			return nil, "", err
		}
	}

	goCtx := context.Background()
	names := in.ConformancePackNames
	named := len(names) > 0
	if !named {
		var err error
		names, err = p.cfgsvcPackNames(goCtx, ctx)
		if err != nil {
			return nil, "", err
		}
	}

	page, next, err := cfgsvcPaginate(names, in.NextToken, in.Limit, cfgsvcPageSizeLimit)
	if err != nil {
		return nil, "", err
	}
	packs := make([]ConfigConformancePack, 0, len(page))
	for _, name := range page {
		var pack ConfigConformancePack
		found, getErr := p.cfgsvcGetJSON(goCtx, cfgsvcPackKey(ctx.AccountID, ctx.Region, name), &pack)
		if getErr != nil {
			return nil, "", getErr
		}
		if !found {
			// Only a *named* pack can be missing here — an unnamed listing comes from the
			// index, so a gap would mean the index and the records had diverged.
			if named && refuseUnknown {
				return nil, "", cfgsvcNoSuchPack()
			}
			continue
		}
		packs = append(packs, pack)
	}
	return packs, next, nil
}

// cfgsvcPackStateView returns the state and status reason an observation reports,
// applying a seed at read time and never writing it.
//
// Reading rather than writing is what makes a seed reversible: clearing it restores
// the pack's real state instead of leaving the seeded value baked into the record.
// That is why Put and Delete consult this rather than the stored field — a pack
// seeded CREATE_FAILED is terminal and must be deletable, even though what is stored
// still says CREATE_IN_PROGRESS.
func (p *ConfigServicePlugin) cfgsvcPackStateView(goCtx context.Context, ctx *RequestContext,
	pack *ConfigConformancePack) (state, reason string, err error) {
	seed, seeded, err := p.seededPackStatus(goCtx, ctx.AccountID, ctx.Region, pack.ConformancePackName)
	if err != nil {
		return "", "", err
	}
	if seeded {
		return seed.State, seed.StatusReason, nil
	}
	return pack.ConformancePackState, "", nil
}

// cfgsvcResolvePackState settles a pack's reported state for one observation of
// DescribeConformancePackStatus, mutating the in-memory copy the response is built
// from.
//
// Advance-on-observation, following resolveCreateAccountStatus
// (organizations_account.go), and for its reasons: resolving on observation rather
// than after an interval of the simulated clock lets a waiter converge in one poll
// with no dependence on wall-clock or simulated time, and *persisting* the resolved
// state means every later observation reports the same state and the same
// LastUpdateCompletedTime. A status that re-resolved would move that timestamp under
// a waiter comparing successive polls, which would loop forever. Clock-driven
// transitions are #514's subject; picking a duration here would front-run it.
//
// A seeded state short-circuits the advance and is *not* persisted, per
// cfgsvcPackStateView. Its completion time is reported as the pack's requested time
// rather than the clock's current value: an unwritten seed cannot remember when it
// "completed", and drawing a fresh timestamp on every poll would move it under the
// same waiter this design protects.
func (p *ConfigServicePlugin) cfgsvcResolvePackState(ctx *RequestContext, pack *ConfigConformancePack) error {
	goCtx := context.Background()
	state, reason, err := p.cfgsvcPackStateView(goCtx, ctx, pack)
	if err != nil {
		return err
	}
	if state != pack.ConformancePackState || reason != "" {
		pack.ConformancePackState = state
		pack.ConformancePackStatusReason = reason
		if cfgsvcPackIsTerminal(state) && time.Time(pack.LastUpdateCompletedTime).IsZero() {
			pack.LastUpdateCompletedTime = pack.LastUpdateRequestedTime
		}
		return nil
	}
	if cfgsvcPackIsTerminal(pack.ConformancePackState) {
		return nil
	}
	// DELETE_IN_PROGRESS is unreachable for a stored pack: deleteConformancePack removes
	// the record within the call rather than leaving one behind to converge, so the only
	// unresolved state a stored pack can hold is CREATE_IN_PROGRESS.
	pack.ConformancePackState = cfgsvcPackCreateComplete
	pack.LastUpdateCompletedTime = EpochSeconds(p.tc.Now())
	return p.cfgsvcSavePack(goCtx, ctx, pack)
}

// cfgsvcPackDetailShape builds a ConformancePackDetail.
//
// No template member appears, because the shape has none: a caller cannot read back
// the template it submitted, and emitting one would be an invention.
//
// CreatedBy is likewise absent. It names the Amazon Web Services service that
// created a service-managed pack, and substrate models none, so reporting a value
// would claim a caller's pack was service-created.
func cfgsvcPackDetailShape(pack *ConfigConformancePack) map[string]interface{} {
	out := map[string]interface{}{
		"ConformancePackName": pack.ConformancePackName,
		"ConformancePackArn":  pack.ConformancePackArn,
		"ConformancePackId":   pack.ConformancePackId,
	}
	if pack.DeliveryS3Bucket != "" {
		out["DeliveryS3Bucket"] = pack.DeliveryS3Bucket
	}
	if pack.DeliveryS3KeyPrefix != "" {
		out["DeliveryS3KeyPrefix"] = pack.DeliveryS3KeyPrefix
	}
	if len(pack.ConformancePackInputParameters) > 0 {
		out["ConformancePackInputParameters"] = pack.ConformancePackInputParameters
	}
	if pack.TemplateSSMDocumentDetails != nil {
		out["TemplateSSMDocumentDetails"] = pack.TemplateSSMDocumentDetails
	}
	if !time.Time(pack.LastUpdateRequestedTime).IsZero() {
		out["LastUpdateRequestedTime"] = pack.LastUpdateRequestedTime
	}
	return out
}

// cfgsvcPackStatusShape builds a ConformancePackStatusDetail.
//
// All six required members are always present — ConformancePackName, Id, Arn, State,
// StackArn and LastUpdateRequestedTime — the synthesized StackArn included, because
// a consumer decoding a required member into a non-pointer field gets a zero value
// rather than an error when one is missing, and a silently empty field is worse than
// an obviously synthetic one.
func cfgsvcPackStatusShape(pack *ConfigConformancePack) map[string]interface{} {
	out := map[string]interface{}{
		"ConformancePackName":     pack.ConformancePackName,
		"ConformancePackId":       pack.ConformancePackId,
		"ConformancePackArn":      pack.ConformancePackArn,
		"ConformancePackState":    pack.ConformancePackState,
		"StackArn":                pack.StackArn,
		"LastUpdateRequestedTime": pack.LastUpdateRequestedTime,
	}
	if pack.ConformancePackStatusReason != "" {
		out["ConformancePackStatusReason"] = pack.ConformancePackStatusReason
	}
	if !time.Time(pack.LastUpdateCompletedTime).IsZero() {
		out["LastUpdateCompletedTime"] = pack.LastUpdateCompletedTime
	}
	return out
}

// describeConformancePackCompliance reports the per-rule verdicts within a pack.
//
// The rules are the pack's *seeded* rules. Substrate does not deploy a pack's
// template, so it does not know which rules that template declares — on real AWS
// CloudFormation creates them as service-linked rules — and inferring them would mean
// parsing arbitrary CloudFormation to produce a listing a consumer would assert
// against. Naming the rules in the seed is the honest version of the same fixture.
func (p *ConfigServicePlugin) describeConformancePackCompliance(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcPackComplianceRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if err := cfgsvcCheckPackName(in.ConformancePackName); err != nil {
		return nil, err
	}
	if in.Limit < 0 || in.Limit > cfgsvcPackComplianceLimit {
		return nil, cfgsvcInvalidLimit()
	}
	if err := cfgsvcCheckPackComplianceFilters(in.Filters); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	found, err := p.cfgsvcGetJSON(goCtx,
		cfgsvcPackKey(ctx.AccountID, ctx.Region, in.ConformancePackName), &ConfigConformancePack{})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, cfgsvcNoSuchPack()
	}

	rules, err := p.cfgsvcPackRuleCompliance(goCtx, ctx, in.ConformancePackName)
	if err != nil {
		return nil, err
	}

	// "You must provide exact rule names." A filter naming a rule the pack does not
	// report is NoSuchConfigRuleInConformancePackException rather than an empty list, so
	// a typo in a fixture's rule name is a refusal instead of a silent pass — which is
	// the whole value of that sentence.
	for _, name := range cfgsvcPackFilterNames(in.Filters) {
		if _, ok := rules[name]; !ok {
			return nil, cfgsvcErr("NoSuchConfigRuleInConformancePackException",
				"Config rule that you passed in the filter does not exist.")
		}
	}

	names := make([]string, 0, len(rules))
	for name, rule := range rules {
		if filters := in.Filters; filters != nil {
			if len(filters.ConfigRuleNames) > 0 && !slices.Contains(filters.ConfigRuleNames, name) {
				continue
			}
			if filters.ComplianceType != "" && rule.ComplianceType != filters.ComplianceType {
				continue
			}
		}
		names = append(names, name)
	}

	page, next, err := cfgsvcPaginate(names, in.NextToken, in.Limit, cfgsvcPackComplianceLimit)
	if err != nil {
		return nil, err
	}
	list := make([]map[string]interface{}, 0, len(page))
	for _, name := range page {
		entry := map[string]interface{}{
			"ConfigRuleName": name,
			"ComplianceType": rules[name].ComplianceType,
		}
		if controls := rules[name].Controls; len(controls) > 0 {
			entry["Controls"] = controls
		}
		list = append(list, entry)
	}
	body := map[string]interface{}{
		"ConformancePackName":               in.ConformancePackName,
		"ConformancePackRuleComplianceList": list,
	}
	if next != "" {
		body["NextToken"] = next
	}
	return cfgsvcJSONResponse(body, "DescribeConformancePackCompliance")
}

// cfgsvcPackFilterNames returns the rule names a compliance filter names, or none.
func cfgsvcPackFilterNames(filters *cfgsvcPackComplianceFilters) []string {
	if filters == nil {
		return nil
	}
	return filters.ConfigRuleNames
}

// cfgsvcPackRuleCompliance reads the seeded per-rule verdicts for a pack, keyed by
// rule name.
func (p *ConfigServicePlugin) cfgsvcPackRuleCompliance(goCtx context.Context, ctx *RequestContext,
	name string) (map[string]cfgsvcSeededPackRule, error) {
	seed, seeded, err := p.seededPackCompliance(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	rules := make(map[string]cfgsvcSeededPackRule, len(seed.Rules))
	if !seeded {
		return rules, nil
	}
	for _, rule := range seed.Rules {
		rules[rule.ConfigRuleName] = rule
	}
	return rules, nil
}

// cfgsvcCheckPackComplianceFilters validates ConformancePackComplianceFilters.
//
// The ComplianceType filter admits only COMPLIANT and NON_COMPLIANT —
// "INSUFFICIENT_DATA is not supported" — even though INSUFFICIENT_DATA is in the enum
// the shape declares and is a value the *response* carries. Accepting it as a filter
// would return an empty list where AWS returns an error, so a fixture would read "no
// rules match" instead of "you cannot ask that".
func cfgsvcCheckPackComplianceFilters(filters *cfgsvcPackComplianceFilters) error {
	if filters == nil {
		return nil
	}
	if len(filters.ConfigRuleNames) > cfgsvcMaxPackFilterRuleNames {
		return cfgsvcInvalidParameter("The ConfigRuleNames filter accepts up to 10 rule names.")
	}
	for _, name := range filters.ConfigRuleNames {
		if name == "" || len(name) > cfgsvcMaxPackFilterRuleNameLen {
			return cfgsvcInvalidParameter("A filtered Config rule name is required and may be up " +
				"to 64 characters long.")
		}
	}
	if filters.ComplianceType != "" &&
		!slices.Contains(cfgsvcPackComplianceFilterTypes, filters.ComplianceType) {
		return cfgsvcInvalidParameter("The ComplianceType filter allows only COMPLIANT and " +
			"NON_COMPLIANT. INSUFFICIENT_DATA is not supported.")
	}
	return nil
}

// getConformancePackComplianceSummary reports one cumulative verdict per pack.
//
// ConformancePackNames is *required* here and accepts 1-5 names, unlike the describe
// operations' optional 0-25.
//
// The cumulative verdict is derived from the seeded per-rule verdicts rather than
// seeded separately, so the summary and the per-rule listing cannot disagree: any
// NON_COMPLIANT rule makes the pack NON_COMPLIANT, all-COMPLIANT makes it COMPLIANT,
// and a pack with no seeded rules is INSUFFICIENT_DATA — the same "has not evaluated"
// default the rule cluster reports, for the same reason.
func (p *ConfigServicePlugin) getConformancePackComplianceSummary(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcPackSummaryRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	// This operation declares only InvalidLimitException, InvalidNextTokenException and
	// NoSuchConformancePackException — it does not declare InvalidParameterValueException,
	// which every other paginated operation here does. An SDK caller never reaches the
	// two refusals below, because a required member and an array's 1-5 bounds are checked
	// client-side; a raw caller does, and serving a request outside the model's own
	// bounds would be worse than answering with the code its sibling operations use.
	if len(in.ConformancePackNames) == 0 {
		return nil, cfgsvcInvalidParameter("ConformancePackNames is required and accepts between " +
			"1 and 5 conformance pack names.")
	}
	if len(in.ConformancePackNames) > cfgsvcMaxPackNamesToSummarize {
		return nil, cfgsvcInvalidParameter("ConformancePackNames accepts up to 5 conformance pack " +
			"names.")
	}
	if in.Limit < 0 || in.Limit > cfgsvcPageSizeLimit {
		return nil, cfgsvcInvalidLimit()
	}

	goCtx := context.Background()
	for _, name := range in.ConformancePackNames {
		if err := cfgsvcCheckPackName(name); err != nil {
			return nil, err
		}
		found, getErr := p.cfgsvcGetJSON(goCtx, cfgsvcPackKey(ctx.AccountID, ctx.Region, name),
			&ConfigConformancePack{})
		if getErr != nil {
			return nil, getErr
		}
		if !found {
			return nil, cfgsvcNoSuchPack()
		}
	}

	page, next, err := cfgsvcPaginate(in.ConformancePackNames, in.NextToken, in.Limit, cfgsvcPageSizeLimit)
	if err != nil {
		return nil, err
	}
	summaries := make([]map[string]interface{}, 0, len(page))
	for _, name := range page {
		verdict, verdictErr := p.cfgsvcPackVerdict(goCtx, ctx, name)
		if verdictErr != nil {
			return nil, verdictErr
		}
		summaries = append(summaries, map[string]interface{}{
			"ConformancePackName":             name,
			"ConformancePackComplianceStatus": verdict,
		})
	}
	body := map[string]interface{}{"ConformancePackComplianceSummaryList": summaries}
	if next != "" {
		body["NextToken"] = next
	}
	return cfgsvcJSONResponse(body, "GetConformancePackComplianceSummary")
}

// cfgsvcPackVerdict computes a pack's cumulative verdict from its seeded rules; see
// getConformancePackComplianceSummary for why it is derived rather than seeded.
func (p *ConfigServicePlugin) cfgsvcPackVerdict(goCtx context.Context, ctx *RequestContext,
	name string) (string, error) {
	rules, err := p.cfgsvcPackRuleCompliance(goCtx, ctx, name)
	if err != nil {
		return "", err
	}
	if len(rules) == 0 {
		return cfgsvcInsufficientData, nil
	}
	verdict := cfgsvcCompliant
	for _, rule := range rules {
		switch rule.ComplianceType {
		case cfgsvcNonCompliant:
			// One non-compliant rule makes the pack non-compliant, and it outranks
			// INSUFFICIENT_DATA: a pack with a known failure is not "unknown".
			return cfgsvcNonCompliant, nil
		case cfgsvcInsufficientData:
			verdict = cfgsvcInsufficientData
		}
	}
	return verdict, nil
}

// deleteConformancePack deletes a pack "and all the Config rules, remediation
// actions, and all evaluation results within that conformance pack".
//
// AWS sets the pack to DELETE_IN_PROGRESS and completes asynchronously. Substrate's
// delete completes within the call, so no stored pack is ever DELETE_IN_PROGRESS and
// a consumer polling until the pack disappears converges on its first poll. What is
// preserved is the refusal a real ordering bug produces: a delete arriving before the
// create was ever observed is ResourceInUseException, exactly as on AWS.
//
// The pack's status and compliance seeds go with it, so a rebuilt pack of the same
// name starts at CREATE_IN_PROGRESS and INSUFFICIENT_DATA rather than inheriting its
// predecessor's. The "*" wildcard seeds are left alone: those are fixture-wide
// defaults rather than one pack's state, and deleting a pack should not silently
// change what every other pack reports.
func (p *ConfigServicePlugin) deleteConformancePack(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in cfgsvcDeletePackRequest
	if err := cfgsvcUnmarshal(req.Body, &in); err != nil {
		return nil, err
	}
	if err := cfgsvcCheckPackName(in.ConformancePackName); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	var pack ConfigConformancePack
	found, err := p.cfgsvcGetJSON(goCtx,
		cfgsvcPackKey(ctx.AccountID, ctx.Region, in.ConformancePackName), &pack)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, cfgsvcNoSuchPack()
	}
	state, _, err := p.cfgsvcPackStateView(goCtx, ctx, &pack)
	if err != nil {
		return nil, err
	}
	if !cfgsvcPackIsTerminal(state) {
		return nil, cfgsvcPackResourceInUse(req.Operation)
	}

	if err := p.cfgsvcDeletePackRecord(goCtx, ctx, &pack); err != nil {
		return nil, err
	}
	if err := p.cfgsvcClearPackSeeds(goCtx, ctx, in.ConformancePackName); err != nil {
		return nil, err
	}
	// DeleteConformancePack has no output shape at all: "the service sends back an HTTP
	// 200 response with an empty HTTP body."
	return cfgsvcEmptyResponse(), nil
}
