package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand" // nosemgrep
	"net/http"
	"sort"
	"time"
)

// OrganizationsPlugin emulates the AWS Organizations service.
// It supports CRUD operations on organizations and accounts using the
// Organizations JSON-target protocol (X-Amz-Target: Organizations_20161128.{Op}).
type OrganizationsPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

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

// HandleRequest dispatches an Organizations JSON-target request to the appropriate handler.
func (p *OrganizationsPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "DescribeOrganization":
		return p.describeOrganization(ctx, req)
	case "ListAccounts":
		return p.listAccounts(ctx, req)
	case "DescribeAccount":
		return p.describeAccount(ctx, req)
	case "ListRoots":
		return p.listRoots(ctx, req)
	case "CreateAccount":
		return p.createAccount(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "OrganizationsPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- state keys ---

func orgKey(acct string) string           { return "org:" + acct }
func orgAccountKey(id string) string      { return "account:" + id }
func orgAccountIDsKey(acct string) string { return "account_ids:" + acct }

// --- auto-create helpers ---

// ensureOrganization returns the organization for acct, creating it on first call.
func (p *OrganizationsPlugin) ensureOrganization(ctx context.Context, acct string) (*Organization, error) {
	data, err := p.state.Get(ctx, organizationsNamespace, orgKey(acct))
	if err != nil {
		return nil, fmt.Errorf("get org: %w", err)
	}
	if data != nil {
		var org Organization
		if unmarshalErr := json.Unmarshal(data, &org); unmarshalErr != nil {
			return nil, fmt.Errorf("unmarshal org: %w", unmarshalErr)
		}
		return &org, nil
	}

	// Auto-create.
	orgID := "o-" + randomLowerAlphanum(10)
	org := Organization{
		ID:                 orgID,
		Arn:                fmt.Sprintf("arn:aws:organizations::%s:organization/%s", acct, orgID),
		FeatureSet:         "ALL",
		MasterAccountID:    acct,
		MasterAccountArn:   fmt.Sprintf("arn:aws:organizations::%s:account/%s/%s", acct, orgID, acct),
		MasterAccountEmail: "master@example.com",
	}
	if saveErr := p.saveOrg(ctx, acct, org); saveErr != nil {
		return nil, saveErr
	}

	// Auto-create the master account entry.
	masterAccount := OrgAccount{
		ID:       acct,
		Arn:      org.MasterAccountArn,
		Name:     "master",
		Email:    "master@example.com",
		Status:   "ACTIVE",
		JoinedAt: p.tc.Now(),
	}
	if saveErr := p.saveAccount(ctx, acct, masterAccount); saveErr != nil {
		return nil, saveErr
	}

	return &org, nil
}

func (p *OrganizationsPlugin) saveOrg(ctx context.Context, acct string, org Organization) error {
	data, err := json.Marshal(org)
	if err != nil {
		return fmt.Errorf("marshal org: %w", err)
	}
	return p.state.Put(ctx, organizationsNamespace, orgKey(acct), data)
}

func (p *OrganizationsPlugin) saveAccount(ctx context.Context, masterAcct string, a OrgAccount) error {
	data, err := json.Marshal(a)
	if err != nil {
		return fmt.Errorf("marshal account: %w", err)
	}
	if err := p.state.Put(ctx, organizationsNamespace, orgAccountKey(a.ID), data); err != nil {
		return fmt.Errorf("put account: %w", err)
	}
	// Update account IDs index.
	ids, _ := p.loadAccountIDs(ctx, masterAcct)
	for _, id := range ids {
		if id == a.ID {
			return nil
		}
	}
	ids = append(ids, a.ID)
	sort.Strings(ids)
	return p.saveAccountIDs(ctx, masterAcct, ids)
}

func (p *OrganizationsPlugin) loadAccount(ctx context.Context, accountID string) (*OrgAccount, error) {
	data, err := p.state.Get(ctx, organizationsNamespace, orgAccountKey(accountID))
	if err != nil {
		return nil, fmt.Errorf("get account: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var a OrgAccount
	if err := json.Unmarshal(data, &a); err != nil {
		return nil, fmt.Errorf("unmarshal account: %w", err)
	}
	return &a, nil
}

func (p *OrganizationsPlugin) loadAccountIDs(ctx context.Context, masterAcct string) ([]string, error) {
	data, err := p.state.Get(ctx, organizationsNamespace, orgAccountIDsKey(masterAcct))
	if err != nil {
		return nil, fmt.Errorf("get account ids: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, fmt.Errorf("unmarshal account ids: %w", err)
	}
	return ids, nil
}

func (p *OrganizationsPlugin) saveAccountIDs(ctx context.Context, masterAcct string, ids []string) error {
	data, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("marshal account ids: %w", err)
	}
	return p.state.Put(ctx, organizationsNamespace, orgAccountIDsKey(masterAcct), data)
}

// --- operations ---

func (p *OrganizationsPlugin) describeOrganization(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	org, err := p.ensureOrganization(context.Background(), reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("describeOrganization: %w", err)
	}
	out := map[string]interface{}{"Organization": org}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("describeOrganization marshal: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *OrganizationsPlugin) listAccounts(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	// Ensure org exists so master account is always present.
	if _, err := p.ensureOrganization(context.Background(), reqCtx.AccountID); err != nil {
		return nil, fmt.Errorf("listAccounts ensure org: %w", err)
	}

	ids, err := p.loadAccountIDs(context.Background(), reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("listAccounts load ids: %w", err)
	}

	accounts := make([]OrgAccount, 0, len(ids))
	for _, id := range ids {
		a, loadErr := p.loadAccount(context.Background(), id)
		if loadErr != nil || a == nil {
			continue
		}
		accounts = append(accounts, *a)
	}

	out := map[string]interface{}{
		"Accounts":  accounts,
		"NextToken": "",
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("listAccounts marshal: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *OrganizationsPlugin) describeAccount(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		AccountID string `json:"AccountId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	a, err := p.loadAccount(context.Background(), input.AccountID)
	if err != nil {
		return nil, fmt.Errorf("describeAccount load: %w", err)
	}
	if a == nil {
		return nil, &AWSError{
			Code:       "AccountNotFoundException",
			Message:    "Account not found: " + input.AccountID,
			HTTPStatus: http.StatusNotFound,
		}
	}

	out := map[string]interface{}{"Account": a}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("describeAccount marshal: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *OrganizationsPlugin) listRoots(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	// Ensure org is initialized.
	org, err := p.ensureOrganization(context.Background(), reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("listRoots ensure org: %w", err)
	}

	rootID := "r-" + randomLowerHex(4)
	root := OrgRoot{
		ID:   rootID,
		Arn:  fmt.Sprintf("arn:aws:organizations::%s:root/%s/%s", reqCtx.AccountID, org.ID, rootID),
		Name: "Root",
		PolicyTypes: []OrgPolicyTypeSummary{
			{Type: "SERVICE_CONTROL_POLICY", Status: "ENABLED"},
		},
	}

	out := map[string]interface{}{
		"Roots":     []OrgRoot{root},
		"NextToken": "",
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("listRoots marshal: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *OrganizationsPlugin) createAccount(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		AccountName string `json:"AccountName"`
		Email       string `json:"Email"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	// Ensure org exists.
	org, err := p.ensureOrganization(context.Background(), reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("createAccount ensure org: %w", err)
	}

	newAcctID := generateOrganizationAccountID()
	a := OrgAccount{
		ID:       newAcctID,
		Arn:      fmt.Sprintf("arn:aws:organizations::%s:account/%s/%s", reqCtx.AccountID, org.ID, newAcctID),
		Name:     input.AccountName,
		Email:    input.Email,
		Status:   "ACTIVE",
		JoinedAt: p.tc.Now(),
	}
	if saveErr := p.saveAccount(context.Background(), reqCtx.AccountID, a); saveErr != nil {
		return nil, fmt.Errorf("createAccount save: %w", saveErr)
	}

	out := map[string]interface{}{
		"CreateAccountStatus": map[string]interface{}{
			"Id":          "car-" + randomLowerAlphanum(8),
			"AccountName": a.Name,
			"AccountId":   a.ID,
			"State":       "SUCCEEDED",
		},
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("createAccount marshal: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
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
