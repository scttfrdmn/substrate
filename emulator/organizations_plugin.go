package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand" // nosemgrep
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

// orgHandler handles one Organizations operation.
type orgHandler func(*RequestContext, *AWSRequest) (*AWSResponse, error)

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
func (p *OrganizationsPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	for _, claim := range []func(string) (orgHandler, bool){
		p.coreOperation,
		p.ouOperation,
		p.policyOperation,
		p.accountOperation,
		p.tagOperation,
		p.resourcePolicyOperation,
	} {
		if h, ok := claim(req.Operation); ok {
			return h(ctx, req)
		}
	}
	return nil, orgInvalidAction(req.Operation)
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

func (p *OrganizationsPlugin) describeOrganization(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	org, err := p.ensureOrganization(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("describeOrganization: %w", err)
	}
	// The feature set is read through the control plane so a seed set after the
	// organization was created still governs what the caller observes.
	featureSet, err := p.effectiveFeatureSet(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("describeOrganization feature set: %w", err)
	}
	org.FeatureSet = featureSet
	if featureSet == orgFeatureSetAll {
		org.AvailablePolicyTypes = []OrgPolicyTypeSummary{{Type: orgPolicyTypeSCP, Status: "ENABLED"}}
	}
	return orgJSONResponse(map[string]interface{}{"Organization": org}, "describeOrganization")
}

func (p *OrganizationsPlugin) listAccounts(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
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

	accounts := make([]OrgAccount, 0, len(page))
	for _, id := range page {
		a, loadErr := p.loadAccount(goCtx, id)
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

func (p *OrganizationsPlugin) describeAccount(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	if _, err := p.ensureOrganization(goCtx, reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("describeAccount ensure org: %w", err)
	}

	var input struct {
		AccountID string `json:"AccountId"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}

	a, err := p.loadAccount(goCtx, input.AccountID)
	if err != nil {
		return nil, fmt.Errorf("describeAccount load: %w", err)
	}
	if a == nil {
		return nil, orgErr("AccountNotFoundException",
			"We can't find an Amazon Web Services account with the AccountId "+input.AccountID)
	}
	return orgJSONResponse(map[string]interface{}{"Account": a}, "describeAccount")
}

func (p *OrganizationsPlugin) listRoots(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
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

// randomLowerAlphanum returns n random lowercase alphanumeric characters.
func randomLowerAlphanum(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))] //nolint:gosec
	}
	return string(b)
}

// randomLowerHex returns n random lowercase hex characters.
func randomLowerHex(n int) string {
	const chars = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))] //nolint:gosec
	}
	return string(b)
}

// generateOrganizationAccountID generates a 12-digit numeric AWS account ID.
func generateOrganizationAccountID() string {
	const digits = "0123456789"
	b := make([]byte, 12)
	for i := range b {
		b[i] = digits[rand.Intn(len(digits))] //nolint:gosec
	}
	return string(b)
}
