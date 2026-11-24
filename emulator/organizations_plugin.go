package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// OrganizationsPlugin emulates the AWS Organizations service.
// It supports the organization, root, OU, policy, account and tagging
// operations using the Organizations JSON-target protocol
// (X-Amz-Target: Organizations_20161128.{Op}).
//
// The organization, its root, and its management account are auto-created on
// first observation, and all three keep one identity for the life of the state
// store — a caller can attach a policy to the root, re-read it, and find the
// same root (#577).
//
// Asynchronous CreateAccount requests resolve on first observation rather than
// after an interval of the simulated clock: DescribeCreateAccountStatus reports
// IN_PROGRESS at most once and then a terminal state, so a waiter converges in
// one poll with no dependence on wall-clock or simulated time. Transitions
// driven by the simulated clock are the subject of #514, which is still open;
// picking a shape for them here would front-run that design.
type OrganizationsPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// orgCaller is the request context an Organizations handler runs against: the
// caller's own account, and the organization that account belongs to.
//
// The embedded *RequestContext is a copy whose AccountID has been resolved to the
// **management account** of the caller's organization, because that is the key
// every Organizations record is stored under — the organization, its root, its
// member list, its policies, its resource policy. A handler that keyed state by
// the account that signed the request would answer a member account with a
// private organization of its own, which is #623.
//
// For a management account, or for an account substrate has never seen, the
// resolved account and the signing account are the same, so nothing changes for a
// single-caller test. Only callerAccount tells them apart, and only the handlers
// that model a management-versus-member asymmetry read it.
type orgCaller struct {
	*RequestContext

	// callerAccount is the account that signed the request, which is not
	// necessarily the one AccountID now names.
	callerAccount string
}

// isMember reports whether the caller is a member of the organization rather than
// its management account.
func (c *orgCaller) isMember() bool { return c.callerAccount != c.AccountID }

// orgHandler handles one Organizations operation.
type orgHandler func(*orgCaller, *AWSRequest) (*AWSResponse, error)

// Name returns the service name "organizations".
func (p *OrganizationsPlugin) Name() string { return organizationsNamespace }

// Initialize configures the OrganizationsPlugin with the provided configuration.
func (p *OrganizationsPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for OrganizationsPlugin.
func (p *OrganizationsPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an Organizations JSON-target request to the first
// operation cluster that claims it. The clusters live in separate files
// (organizations_ou.go, organizations_policy.go, and so on) and each owns its own
// claim function, so adding an operation touches one file rather than a shared
// switch.
//
// The caller's organization is resolved once, here, rather than in each handler:
// resolving per read would let two reads in one request disagree about which
// organization they are about, and every handler would have to remember to do it.
func (p *OrganizationsPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	for _, claim := range []func(string) (orgHandler, bool){
		p.coreOperation,
		p.ouOperation,
		p.policyOperation,
		p.accountOperation,
		p.tagOperation,
		p.resourcePolicyOperation,
		p.lifecycleOperation,
		p.serviceAccessOperation,
	} {
		if h, ok := claim(req.Operation); ok {
			caller, err := p.resolveOrgCaller(context.Background(), ctx)
			if err != nil {
				return nil, fmt.Errorf("organizations resolve caller: %w", err)
			}
			if req.Operation != "CreateOrganization" {
				if err := p.requireInUse(context.Background(), caller.AccountID); err != nil {
					return nil, err
				}
			}
			resp, err := h(caller, req)
			return resp, orgUnwrapNotInUse(err)
		}
	}
	return nil, unknownActionError(p.Name(), req.Operation)
}

// resolveOrgCaller builds the orgCaller for a request, resolving the signing
// account to the management account of the organization it belongs to.
//
// An account the reverse index does not know resolves to itself, which is what
// keeps the no-setup auto-create working: the first caller of a fresh emulator is
// in no organization yet, and it must still get one rather than a refusal.
func (p *OrganizationsPlugin) resolveOrgCaller(ctx context.Context, reqCtx *RequestContext) (*orgCaller, error) {
	owner, err := p.organizationOwner(ctx, reqCtx.AccountID)
	if err != nil {
		return nil, err
	}
	resolved := *reqCtx
	if owner != "" {
		resolved.AccountID = owner
	}
	return &orgCaller{RequestContext: &resolved, callerAccount: reqCtx.AccountID}, nil
}

// coreOperation claims the organization- and root-level reads.
func (p *OrganizationsPlugin) coreOperation(op string) (orgHandler, bool) {
	switch op {
	case "DescribeOrganization":
		return p.describeOrganization, true
	case "ListAccounts":
		return p.listAccounts, true
	case "DescribeAccount":
		return p.describeAccount, true
	case "ListRoots":
		return p.listRoots, true
	default:
		return nil, false
	}
}

// --- operations ---

func (p *OrganizationsPlugin) describeOrganization(reqCtx *orgCaller, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := reqCtx.context()
	org, err := p.ensureOrganization(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("describeOrganization: %w", err)
	}
	if err := p.renderOrganization(goCtx, reqCtx.AccountID, org); err != nil {
		return nil, err
	}
	return orgJSONResponse(map[string]interface{}{"Organization": org}, "describeOrganization")
}

// renderOrganization sets the members DescribeOrganization and CreateOrganization report from more
// than the record. The feature set is read through the control plane so a seed set after the
// organization was created still governs what the caller observes.
func (p *OrganizationsPlugin) renderOrganization(ctx context.Context, acct string, org *Organization) error {
	featureSet, err := p.effectiveFeatureSet(ctx, acct)
	if err != nil {
		return fmt.Errorf("organization feature set: %w", err)
	}
	org.FeatureSet = featureSet
	if featureSet == orgFeatureSetAll {
		org.AvailablePolicyTypes = []OrgPolicyTypeSummary{{Type: orgPolicyTypeSCP, Status: "ENABLED"}}
	}
	return nil
}

func (p *OrganizationsPlugin) listAccounts(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := reqCtx.context()
	// Ensure org exists so the management account is always present.
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("listAccounts ensure org: %w", err)
	}

	var input struct {
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}

	ids, err := p.loadAccountIDs(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("listAccounts load ids: %w", err)
	}
	page, next, err := orgPaginate(ids, input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}

	// Each account is read the way DescribeAccount reads it, so a caller polling
	// through the listing sees a closure converge rather than watching
	// PENDING_CLOSURE forever while a Describe of the same account reports SUSPENDED.
	accounts := make([]OrgAccount, 0, len(page))
	for _, id := range page {
		a, loadErr := p.observeAccount(goCtx, id)
		if loadErr != nil {
			return nil, fmt.Errorf("listAccounts load account: %w", loadErr)
		}
		if a == nil {
			continue
		}
		accounts = append(accounts, *a)
	}

	out := map[string]interface{}{"Accounts": accounts}
	if next != "" {
		out["NextToken"] = next
	}
	return orgJSONResponse(out, "listAccounts")
}

func (p *OrganizationsPlugin) describeAccount(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := reqCtx.context()
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("describeAccount ensure org: %w", err)
	}

	var input struct {
		AccountID string `json:"AccountId"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}

	// observeAccount rather than loadAccount: DescribeAccount is the operation AWS
	// documents as the way to watch a closure finish, so it is one of the reads that
	// advances PENDING_CLOSURE to SUSPENDED.
	a, err := p.observeAccount(goCtx, input.AccountID)
	if err != nil {
		return nil, fmt.Errorf("describeAccount load: %w", err)
	}
	if a == nil {
		return nil, orgErr("AccountNotFoundException",
			"We can't find an Amazon Web Services account with the AccountId "+input.AccountID)
	}
	return orgJSONResponse(map[string]interface{}{"Account": a}, "describeAccount")
}

func (p *OrganizationsPlugin) listRoots(reqCtx *orgCaller, req *AWSRequest) (*AWSResponse, error) {
	goCtx := reqCtx.context()
	var input struct {
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}
	// The token is validated even though an organization has exactly one root, so
	// a caller passing a stale token learns it rather than silently restarting.
	if _, _, err := orgPaginate([]string{"root"}, input.NextToken, input.MaxResults); err != nil {
		return nil, err
	}

	root, err := p.loadRoot(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("listRoots: %w", err)
	}
	if root.PolicyTypes == nil {
		root.PolicyTypes = []OrgPolicyTypeSummary{}
	}
	return orgJSONResponse(map[string]interface{}{"Roots": []OrgRoot{*root}}, "listRoots")
}

// orgJSONResponse marshals an Organizations response body.
func orgJSONResponse(out interface{}, op string) (*AWSResponse, error) {
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("%s marshal: %w", op, err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

// orgEmptyResponse is the body of an operation the API model gives no output
// shape — AttachPolicy, MoveAccount, TagResource and the like answer 200 with an
// empty JSON object.
func orgEmptyResponse() *AWSResponse {
	return &AWSResponse{Body: []byte(`{}`), StatusCode: http.StatusOK}
}

// --- ID generation helpers ---

// orgLowerAlphanum is the alphabet of the random part of every Organizations
// identifier except the root's: the API reference's patterns for an organization
// (^o-[a-z0-9]{10,32}$), an OU, a policy, a resource policy and a CreateAccount
// request all admit lowercase letters and digits.
const orgLowerAlphanum = "abcdefghijklmnopqrstuvwxyz0123456789"

// generateOrganizationAccountID mints the 12-digit ID of a vended account.
//
// Minted from the request's mint rather than the auto-seeded math/rand it was drawn
// from until #1600, so a replay of the CreateAccount vends the account it vended
// live, and two deterministic runs vend the same one.
func generateOrganizationAccountID(ids *IDMint) string {
	return ids.Digits(12)
}

// orgMintKey is the [context.Context] key under which an Organizations handler
// carries its request's [IDMint].
type orgMintKey struct{}

// context returns the context an Organizations handler threads through its
// helpers, carrying the request's mint.
//
// The organization and its root are created on first observation by
// [OrganizationsPlugin.ensureOrganization], which is reached from every handler and
// from helpers several calls deep (loadRoot, resolveOrgTarget, requireOU), none of
// which otherwise sees the request. Carrying the mint on the context reaches all of
// them without widening a dozen signatures; a direct caller with a bare context gets
// a nil mint, which draws from crypto/rand as every hand-built RequestContext does.
func (c *orgCaller) context() context.Context {
	return context.WithValue(context.Background(), orgMintKey{}, c.IDs)
}

// orgMintFrom returns the mint [orgCaller.context] put on ctx, or nil.
func orgMintFrom(ctx context.Context) *IDMint {
	m, _ := ctx.Value(orgMintKey{}).(*IDMint)
	return m
}
