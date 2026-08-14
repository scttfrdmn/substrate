package emulator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"
)

// accountNamespace is the state namespace used by AccountPlugin.
const accountNamespace = "account"

// Region opt statuses, the RegionOptStatus enum from the account/2021-02-01 API
// model. A Region is in exactly one of them, and only the middle three are ever
// stored: a default Region is always ENABLED_BY_DEFAULT and an opt-in Region
// substrate has never been asked about is DISABLED, so neither needs a record.
const (
	// accountRegionEnabled is an opt-in Region whose enable has completed.
	accountRegionEnabled = "ENABLED"

	// accountRegionEnabling is an opt-in Region with an enable in flight.
	accountRegionEnabling = "ENABLING"

	// accountRegionDisabling is an opt-in Region with a disable in flight.
	accountRegionDisabling = "DISABLING"

	// accountRegionDisabled is an opt-in Region that has not been enabled.
	accountRegionDisabled = "DISABLED"

	// accountRegionEnabledByDefault is a Region launched before 2019-03-20, which
	// can be neither enabled nor disabled.
	accountRegionEnabledByDefault = "ENABLED_BY_DEFAULT"
)

// accountRegionOptStatuses is the RegionOptStatus enum, used to validate a
// RegionOptStatusContains filter and a seeded status. A value outside it would
// filter every Region out of a listing, or pin a status no SDK enum member
// matches, and the caller would read an empty list rather than a refusal.
var accountRegionOptStatuses = []string{
	accountRegionEnabled,
	accountRegionEnabling,
	accountRegionDisabling,
	accountRegionDisabled,
	accountRegionEnabledByDefault,
}

// accountDefaultRegions are the Regions enabled by default — those launched
// before 2019-03-20 — which "cannot be enabled or disabled".
//
// The API model publishes the RegionOptStatus enum but no Region list, so this
// table and the opt-in one below come from the AWS Account Management Reference
// Guide's "Regional availability reference":
// https://docs.aws.amazon.com/accounts/latest/reference/manage-acct-regions.html
//
// This is deliberately not unified with ec2SeededRegions, which seeds three
// Regions for DescribeRegions and answers a different question — which Regions
// EC2 reports, not which ones an account has opted into. Merging them would make
// every EC2 fixture's Region list depend on this table.
var accountDefaultRegions = []string{
	"ap-northeast-1",
	"ap-northeast-2",
	"ap-northeast-3",
	"ap-south-1",
	"ap-southeast-1",
	"ap-southeast-2",
	"ca-central-1",
	"eu-central-1",
	"eu-north-1",
	"eu-west-1",
	"eu-west-2",
	"eu-west-3",
	"sa-east-1",
	"us-east-1",
	"us-east-2",
	"us-west-1",
	"us-west-2",
}

// accountOptInRegions are the Regions disabled by default — those launched after
// 2019-03-20 — which must be enabled before use. Same source as
// accountDefaultRegions.
var accountOptInRegions = []string{
	"af-south-1",
	"ap-east-1",
	"ap-east-2",
	"ap-south-2",
	"ap-southeast-3",
	"ap-southeast-4",
	"ap-southeast-5",
	"ap-southeast-6",
	"ap-southeast-7",
	"ca-west-1",
	"eu-central-2",
	"eu-south-1",
	"eu-south-2",
	"il-central-1",
	"me-central-1",
	"me-south-1",
	"mx-central-1",
}

// accountMaxResults is the ListRegions MaxResults ceiling, from the model's
// ListRegionsRequestMaxResultsInteger shape (min 1, max 50).
const accountMaxResults = 50

// accountMinResults is the ListRegions MaxResults floor, from the same shape.
const accountMinResults = 1

// AccountRegion is the Region structure ListRegions returns: a Region code and
// its opt status.
type AccountRegion struct {
	// RegionName is the Region code, e.g. "af-south-1".
	RegionName string `json:"RegionName"`

	// RegionOptStatus is the Region's opt status, a RegionOptStatus enum member.
	RegionOptStatus string `json:"RegionOptStatus"`
}

// accountRegionOpt is the stored opt record for one account and one opt-in
// Region. Only a Region an account has acted on has a record; the absence of one
// means DISABLED, which is what makes a fresh account's listing correct with no
// setup at all.
type accountRegionOpt struct {
	// Status is the RegionOptStatus the last operation moved the Region to.
	Status string `json:"status"`

	// UpdatedAt is the simulated instant the status was last written, recorded so
	// a state dump is readable. Nothing reads it back: the ENABLING → ENABLED
	// transition is advance-on-observation, not clock-driven.
	UpdatedAt time.Time `json:"updated_at"`
}

// AccountPlugin emulates the AWS Account Management API's Region opt-in
// operations: ListRegions, EnableRegion, DisableRegion and GetRegionOptStatus.
//
// The service is rest-json with no X-Amz-Target, so the operation lives in the
// URL and nothing else; parseAccountOperation is what recovers it, both here and
// for the pipeline via operationResolvers.
//
// The other eleven operations in the account/2021-02-01 model — alternate
// contacts, the primary contact, the account name and the primary email — are not
// emulated. #629 asked for the four Region operations, which are what a consumer
// baselining an account's enabled Regions calls.
type AccountPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "account".
func (p *AccountPlugin) Name() string { return accountNamespace }

// Initialize sets up the AccountPlugin with the provided configuration.
func (p *AccountPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for AccountPlugin.
func (p *AccountPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an Account Management rest-json request.
func (p *AccountPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := parseAccountOperation(requestMethod(req), req.Path)
	switch op {
	case "ListRegions":
		return p.listRegions(reqCtx, req)
	case "EnableRegion":
		return p.enableRegion(reqCtx, req)
	case "DisableRegion":
		return p.disableRegion(reqCtx, req)
	case "GetRegionOptStatus":
		return p.getRegionOptStatus(reqCtx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("AccountPlugin: unsupported operation %q", op),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// parseAccountOperation maps an HTTP method and path to an Account Management
// operation name, and returns "" for a path that is not one of the four Region
// routes.
//
// Every requestUri in the model is a single flat segment and every method is
// POST, so this is an exact match on the path rather than a prefix walk. It is
// idempotent: an already-resolved operation name is not a path, so it falls
// through to "" and resolveOperationName leaves req.Operation alone.
func parseAccountOperation(method, path string) string {
	if method != http.MethodPost {
		return ""
	}
	switch strings.TrimSuffix(path, "/") {
	case "/listRegions":
		return "ListRegions"
	case "/enableRegion":
		return "EnableRegion"
	case "/disableRegion":
		return "DisableRegion"
	case "/getRegionOptStatus":
		return "GetRegionOptStatus"
	default:
		return ""
	}
}

// --- errors ------------------------------------------------------------------

// accountValidation returns ValidationException with a reason from the model's
// ValidationExceptionReason enum, which has exactly two members:
// invalidRegionOptTarget and fieldValidationFailed.
//
// The reason is carried in the message as well as in the "reason" member, because
// substrate's rest-json error document is {message, Message, Code} plus the
// x-amzn-errortype header — there is no place for a modeled member, so a caller
// that needs to tell the two apart has only the message to read.
func accountValidation(reason, message string) *AWSError {
	return &AWSError{
		Code:       "ValidationException",
		Message:    fmt.Sprintf("%s: %s", reason, message),
		HTTPStatus: http.StatusBadRequest,
	}
}

// accountConflict returns ConflictException, which the model declares for
// EnableRegion and DisableRegion at HTTP 409. It is the answer to an opt whose
// opposite is still in flight — "this happens if you try to enable a Region that
// is currently being disabled (in a status of DISABLING)".
func accountConflict(message string) *AWSError {
	return &AWSError{
		Code:       "ConflictException",
		Message:    message,
		HTTPStatus: http.StatusConflict,
	}
}

// --- state -------------------------------------------------------------------

// accountRegionOptKey is the state key for one account's record of one Region.
func accountRegionOptKey(acct, region string) string {
	return "region_opt:" + acct + "/" + region
}

// loadRegionOpt returns the stored opt record for a Region, or nil when the
// account has never acted on it.
func (p *AccountPlugin) loadRegionOpt(ctx context.Context, acct, region string) (*accountRegionOpt, error) {
	data, err := p.state.Get(ctx, accountNamespace, accountRegionOptKey(acct, region))
	if err != nil {
		return nil, fmt.Errorf("account loadRegionOpt %s/%s: %w", acct, region, err)
	}
	if data == nil {
		return nil, nil //nolint:nilnil // (nil, nil) = "never acted on", which means DISABLED.
	}
	var opt accountRegionOpt
	if err := json.Unmarshal(data, &opt); err != nil {
		return nil, fmt.Errorf("account loadRegionOpt %s/%s unmarshal: %w", acct, region, err)
	}
	return &opt, nil
}

// saveRegionOpt writes the opt record for a Region.
func (p *AccountPlugin) saveRegionOpt(ctx context.Context, acct, region, status string) error {
	data, err := json.Marshal(accountRegionOpt{Status: status, UpdatedAt: p.tc.Now()})
	if err != nil {
		return fmt.Errorf("account saveRegionOpt marshal: %w", err)
	}
	if err := p.state.Put(ctx, accountNamespace, accountRegionOptKey(acct, region), data); err != nil {
		return fmt.Errorf("account saveRegionOpt %s/%s: %w", acct, region, err)
	}
	return nil
}

// --- Region classification ---------------------------------------------------

// accountRegionKind reports whether a Region code is a default Region, an opt-in
// Region, or neither.
func accountRegionKind(region string) (isDefault, isOptIn bool) {
	return slices.Contains(accountDefaultRegions, region), slices.Contains(accountOptInRegions, region)
}

// accountUnknownRegion is the refusal for a Region code in neither table. The
// model's only fitting reason is invalidRegionOptTarget — there is no
// "no such Region" error and no ResourceNotFoundException in this model at all.
func accountUnknownRegion(region string) *AWSError {
	return accountValidation("invalidRegionOptTarget",
		fmt.Sprintf("%q is not an AWS Region code substrate knows", region))
}

// regionStatus returns the opt status of a Region for an account, advancing an
// in-flight opt towards its terminal status as a side effect of the observation.
//
// Only ListRegions and GetRegionOptStatus call this. EnableRegion and
// DisableRegion read through peekRegionStatus instead, because an opt is not an
// observation: letting a redundant EnableRegion advance the record would make a
// consumer's "ensure" pass converge a Region a poller had not yet seen finish.
func (p *AccountPlugin) regionStatus(ctx context.Context, acct, region string) (status string, err error) {
	status, stored, err := p.peekRegionStatus(ctx, acct, region)
	if err != nil || !stored {
		return status, err
	}
	return p.resolveRegionOpt(ctx, acct, region, status)
}

// peekRegionStatus returns the opt status of a Region without advancing anything.
//
// stored reports whether the status came from the account's own record, which is
// the only case an observation may advance: a default Region's status is fixed, an
// unopted Region has no record to move, and a seeded status is pinned precisely so
// that it does not move.
func (p *AccountPlugin) peekRegionStatus(ctx context.Context, acct, region string) (status string, stored bool, err error) {
	if isDefault, isOptIn := accountRegionKind(region); isDefault {
		return accountRegionEnabledByDefault, false, nil
	} else if !isOptIn {
		return "", false, accountUnknownRegion(region)
	}

	if seeded, found, seedErr := p.seededRegionOptStatus(ctx, region); seedErr != nil {
		return "", false, seedErr
	} else if found {
		return seeded, false, nil
	}

	opt, err := p.loadRegionOpt(ctx, acct, region)
	if err != nil {
		return "", false, err
	}
	if opt == nil {
		return accountRegionDisabled, false, nil
	}
	return opt.Status, true, nil
}

// resolveRegionOpt reports an in-flight opt's current status and advances the
// stored record to its terminal one, so the observation after this one reports
// ENABLED or DISABLED and every observation after that reports the same thing.
//
// Advancing on observation rather than after an interval of the simulated clock
// follows resolveCreateAccountStatus, and for the same reason: a waiter converges
// in a bounded number of polls with no dependence on wall-clock or simulated
// time, and a terminal status that flipped back to in-flight would make a waiter
// comparing successive polls loop forever. Enabling a Region takes "a few minutes
// to several hours" in AWS, which is exactly the wait a test must not have to
// actually spend. Clock-driven transitions are #514's subject; choosing a duration
// here would front-run that design.
//
// It differs from resolveCreateAccountStatus in one way that matters: the *first*
// observation reports the in-flight status, and the second reports the terminal
// one. CreateAccount hands back an IN_PROGRESS CreateAccountStatus in its own
// response, so a consumer sees the in-flight state without polling at all;
// EnableRegion has no output shape, so a poll is the only place ENABLING is ever
// observable. Resolving before the first report would make ENABLING and DISABLING
// unreachable, and a waiter's in-flight branch — the branch that exists precisely
// because AWS takes hours here — would never execute in a test.
func (p *AccountPlugin) resolveRegionOpt(ctx context.Context, acct, region, status string) (string, error) {
	var terminal string
	switch status {
	case accountRegionEnabling:
		terminal = accountRegionEnabled
	case accountRegionDisabling:
		terminal = accountRegionDisabled
	default:
		return status, nil
	}
	if err := p.saveRegionOpt(ctx, acct, region, terminal); err != nil {
		return "", err
	}
	return status, nil
}

// --- Operations --------------------------------------------------------------

// listRegions reports every Region substrate knows with its opt status for the
// caller's account: the 17 default Regions as ENABLED_BY_DEFAULT and the 17
// opt-in Regions as their stored status, defaulting to DISABLED.
//
// An in-flight opt resolves here as well as in GetRegionOptStatus. If it did not,
// a caller polling through the listing would read ENABLING forever while a
// GetRegionOptStatus of the same Region reported ENABLED — two observations of
// one Region contradicting each other.
func (p *AccountPlugin) listRegions(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		AccountID               string   `json:"AccountId"`
		MaxResults              *int     `json:"MaxResults"`
		NextToken               string   `json:"NextToken"`
		RegionOptStatusContains []string `json:"RegionOptStatusContains"`
	}
	if err := accountUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	for _, status := range input.RegionOptStatusContains {
		if !slices.Contains(accountRegionOptStatuses, status) {
			return nil, accountValidation("fieldValidationFailed",
				fmt.Sprintf("RegionOptStatusContains member %q is not a RegionOptStatus", status))
		}
	}
	if input.MaxResults != nil && (*input.MaxResults < accountMinResults || *input.MaxResults > accountMaxResults) {
		return nil, accountValidation("fieldValidationFailed",
			fmt.Sprintf("MaxResults must be between %d and %d, got %d",
				accountMinResults, accountMaxResults, *input.MaxResults))
	}

	goCtx := context.Background()
	target, err := p.targetAccount(reqCtx, input.AccountID)
	if err != nil {
		return nil, err
	}

	all := make([]string, 0, len(accountDefaultRegions)+len(accountOptInRegions))
	all = append(all, accountDefaultRegions...)
	all = append(all, accountOptInRegions...)
	sort.Strings(all)

	matched := make([]AccountRegion, 0, len(all))
	for _, region := range all {
		status, statusErr := p.regionStatus(goCtx, target, region)
		if statusErr != nil {
			return nil, statusErr
		}
		if len(input.RegionOptStatusContains) > 0 &&
			!slices.Contains(input.RegionOptStatusContains, status) {
			continue
		}
		matched = append(matched, AccountRegion{RegionName: region, RegionOptStatus: status})
	}

	limit := accountMaxResults
	if input.MaxResults != nil {
		limit = *input.MaxResults
	}
	page, next, err := accountPaginate(matched, input.NextToken, limit)
	if err != nil {
		return nil, err
	}

	out := map[string]any{"Regions": page}
	if next != "" {
		out["NextToken"] = next
	}
	return accountJSONResponse(out, "listRegions")
}

// getRegionOptStatus is the poll target for an enable or a disable. It reports
// the Region code back alongside the status, which is what lets a caller confirm
// it is reading the Region it asked about.
func (p *AccountPlugin) getRegionOptStatus(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	input, err := p.regionInput(req)
	if err != nil {
		return nil, err
	}
	target, err := p.targetAccount(reqCtx, input.AccountID)
	if err != nil {
		return nil, err
	}
	status, err := p.regionStatus(context.Background(), target, input.RegionName)
	if err != nil {
		return nil, err
	}
	return accountJSONResponse(map[string]any{
		"RegionName":      input.RegionName,
		"RegionOptStatus": status,
	}, "getRegionOptStatus")
}

// enableRegion opts an account into a Region. It answers an empty 200: the model
// gives EnableRegion no output shape, and the status is observed through
// GetRegionOptStatus.
//
// It is idempotent against the target state rather than the call: enabling a
// Region already ENABLED or ENABLING succeeds silently and changes nothing. That
// is what makes an "ensure these Regions are on" routine safe to re-run, which is
// the shape of the consumer #629 came from.
func (p *AccountPlugin) enableRegion(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.opt(reqCtx, req, accountRegionEnabling)
}

// disableRegion opts an account out of a Region, with the same empty 200 and the
// same idempotence as enableRegion.
//
// A default Region is refused: "Default Regions cannot be enabled or disabled".
// The refusal is ValidationException with reason invalidRegionOptTarget, not the
// ConstraintViolationException #629 named — the account/2021-02-01 model declares
// no such error for this operation, and invalidRegionOptTarget is the enum member
// that exists for a target that cannot be opted.
func (p *AccountPlugin) disableRegion(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.opt(reqCtx, req, accountRegionDisabling)
}

// opt is the shared body of enableRegion and disableRegion. want is the in-flight
// status the call moves the Region to.
func (p *AccountPlugin) opt(reqCtx *RequestContext, req *AWSRequest, want string) (*AWSResponse, error) {
	input, err := p.regionInput(req)
	if err != nil {
		return nil, err
	}
	target, err := p.targetAccount(reqCtx, input.AccountID)
	if err != nil {
		return nil, err
	}

	isDefault, isOptIn := accountRegionKind(input.RegionName)
	switch {
	case isDefault:
		return nil, accountValidation("invalidRegionOptTarget",
			fmt.Sprintf("%s is enabled by default and can be neither enabled nor disabled", input.RegionName))
	case !isOptIn:
		return nil, accountUnknownRegion(input.RegionName)
	}

	goCtx := context.Background()
	// peekRegionStatus, not regionStatus: an opt is not an observation. If this read
	// advanced the record, an "ensure these Regions are on" pass would converge a
	// Region the consumer's own poller had not yet seen reach ENABLED, and the
	// ConflictException below would then be unreachable even with a seed in place.
	current, _, err := p.peekRegionStatus(goCtx, target, input.RegionName)
	if err != nil {
		return nil, err
	}

	// The opposite opt is still in flight. AWS refuses rather than queueing, and
	// the model declares ConflictException for exactly this: "you cannot use the
	// Region until this process is complete … you cannot disable the Region until
	// the enabling process is fully completed".
	opposite := accountRegionDisabling
	if want == accountRegionDisabling {
		opposite = accountRegionEnabling
	}
	if current == opposite {
		return nil, accountConflict(fmt.Sprintf(
			"%s is in status %s for account %s; wait for it to complete before opting the other way",
			input.RegionName, current, target))
	}

	// Already where the call wants it, or already on the way there. Writing
	// ENABLING over ENABLED would be worse than a no-op: a waiter that had already
	// observed ENABLED would see the Region go backwards.
	settled := accountRegionEnabled
	if want == accountRegionDisabling {
		settled = accountRegionDisabled
	}
	if current == want || current == settled {
		return accountEmptyResponse(), nil
	}

	if err := p.saveRegionOpt(goCtx, target, input.RegionName, want); err != nil {
		return nil, err
	}
	return accountEmptyResponse(), nil
}

// --- input -------------------------------------------------------------------

// accountRegionRequest is the input shape the three Region-named operations
// share: a required RegionName and an optional AccountId.
type accountRegionRequest struct {
	// AccountID is the member account the operation targets, or "" for the caller.
	AccountID string `json:"AccountId"`

	// RegionName is the Region code the operation acts on.
	RegionName string `json:"RegionName"`
}

// regionInput decodes and validates the shared input. RegionName is required by
// the model, and its shape bounds the length at 1-50 characters.
func (p *AccountPlugin) regionInput(req *AWSRequest) (*accountRegionRequest, error) {
	var input accountRegionRequest
	if err := accountUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	if input.RegionName == "" {
		return nil, accountValidation("fieldValidationFailed", "RegionName is required")
	}
	if len(input.RegionName) > 50 {
		return nil, accountValidation("fieldValidationFailed", "RegionName must be at most 50 characters")
	}
	return &input, nil
}

// targetAccount resolves which account an operation reads or writes: the caller's
// own when AccountId is absent, or the named member account when it is present.
//
// The model's rules are specific and all three refusals below are
// ValidationException, the only 400 these operations declare:
//
//   - "The management account can't specify its own AccountId. It must call the
//     operation in standalone context by not including the AccountId parameter."
//   - "The specified account ID must be a member account in the same
//     organization."
//   - The organization "must have all features enabled".
//
// Membership is read through the Organizations reverse index #623 added, which is
// the one place that knows who manages whom. Substrate does not model Account
// Management trusted access or a delegated administrator for it, so the caller
// must be the management account itself; a member naming another member is
// refused.
func (p *AccountPlugin) targetAccount(reqCtx *RequestContext, accountID string) (string, error) {
	caller := fallbackAccountID
	if reqCtx != nil && reqCtx.AccountID != "" {
		caller = reqCtx.AccountID
	}
	if accountID == "" {
		return caller, nil
	}
	if !isAccountIDPattern(accountID) {
		return "", accountValidation("fieldValidationFailed",
			fmt.Sprintf("AccountId %q must be 12 digits", accountID))
	}
	if accountID == caller {
		return "", accountValidation("fieldValidationFailed",
			"the calling account can't specify its own AccountId; omit the parameter to operate in standalone context")
	}

	owner, err := p.organizationOwner(context.Background(), accountID)
	if err != nil {
		return "", err
	}
	if owner != caller {
		return "", accountValidation("invalidRegionOptTarget",
			fmt.Sprintf("account %s is not a member account of the organization managed by %s", accountID, caller))
	}
	return accountID, nil
}

// organizationOwner reads the Organizations member→management index directly out
// of state, returning "" when the account is not a recorded member.
//
// Reading another plugin's namespace is deliberate and is what the API models:
// ListRegions with an AccountId is only valid "if the caller is an identity in
// the organization's management account", so the answer depends on Organizations
// state and nothing else. The alternative — a second copy of the membership index
// under this namespace — could disagree with Organizations', and the whole point
// of #623 was that two answers to "who manages this account" is one too many.
func (p *AccountPlugin) organizationOwner(ctx context.Context, acct string) (string, error) {
	data, err := p.state.Get(ctx, organizationsNamespace, orgMemberOwnerKey(acct))
	if err != nil {
		return "", fmt.Errorf("account organizationOwner %s: %w", acct, err)
	}
	if data == nil {
		return "", nil
	}
	var owner string
	if err := json.Unmarshal(data, &owner); err != nil {
		return "", fmt.Errorf("account organizationOwner %s unmarshal: %w", acct, err)
	}
	return owner, nil
}

// isAccountIDPattern reports whether s matches the model's AccountId pattern,
// ^\d{12}$.
func isAccountIDPattern(s string) bool {
	if len(s) != 12 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// --- helpers -----------------------------------------------------------------

// accountUnmarshal decodes a request body, treating an absent one as empty so a
// required-member check reports the missing member rather than a parse failure.
// A body that will not parse is ValidationException/fieldValidationFailed: it is
// the only 400 these operations declare, so any other code is one no caller's
// catch branch can match.
func accountUnmarshal(body []byte, out any) error {
	if len(body) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return accountValidation("fieldValidationFailed", "could not parse request body: "+err.Error())
	}
	return nil
}

// accountJSONResponse marshals an Account Management response body.
func accountJSONResponse(out any, op string) (*AWSResponse, error) {
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("account %s marshal: %w", op, err)
	}
	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}

// accountEmptyResponse is the answer to an operation the model gives no output
// shape. EnableRegion and DisableRegion both declare responseCode 200 and no
// output, and the AWS CLI documents it as "This command produces no output if
// it's successful".
func accountEmptyResponse() *AWSResponse {
	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       []byte(`{}`),
	}
}

// accountPaginate returns one page of Regions and the token for the next.
//
// The token encodes the last Region code returned rather than an offset, so a
// listing whose contents changed between pages cannot silently skip an entry. An
// unreadable token is a refusal rather than a silent restart from the beginning:
// a paginating caller that restarts sees duplicates instead of an error, which is
// the failure mode hardest to notice.
func accountPaginate(regions []AccountRegion, nextToken string, maxResults int) (page []AccountRegion, next string, err error) {
	start := 0
	if nextToken != "" {
		decoded, decodeErr := base64.StdEncoding.DecodeString(nextToken)
		if decodeErr != nil {
			return nil, "", accountValidation("fieldValidationFailed", "the NextToken value is not valid")
		}
		found := false
		for i, r := range regions {
			if r.RegionName == string(decoded) {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return nil, "", accountValidation("fieldValidationFailed", "the NextToken value is not valid")
		}
	}

	limit := maxResults
	if limit <= 0 || limit > accountMaxResults {
		limit = accountMaxResults
	}
	end := min(start+limit, len(regions))
	page = regions[start:end]
	if end < len(regions) {
		next = base64.StdEncoding.EncodeToString([]byte(regions[end-1].RegionName))
	}
	if page == nil {
		page = []AccountRegion{}
	}
	return page, next, nil
}
