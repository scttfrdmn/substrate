package emulator

import (
	"context"
	"fmt"
)

// accountOperation claims the account vending and placement operations.
func (p *OrganizationsPlugin) accountOperation(op string) (orgHandler, bool) {
	switch op {
	case "CreateAccount":
		return p.createAccount, true
	default:
		return nil, false
	}
}

func (p *OrganizationsPlugin) createAccount(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var input struct {
		AccountName string `json:"AccountName"`
		Email       string `json:"Email"`
	}
	if err := orgUnmarshal(req.Body, &input); err != nil {
		return nil, err
	}

	org, err := p.ensureOrganization(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("createAccount ensure org: %w", err)
	}
	root, err := p.loadRoot(goCtx, reqCtx.AccountID)
	if err != nil {
		return nil, fmt.Errorf("createAccount load root: %w", err)
	}

	newAcctID := generateOrganizationAccountID()
	a := OrgAccount{
		ID:           newAcctID,
		Arn:          fmt.Sprintf("arn:aws:organizations::%s:account/%s/%s", reqCtx.AccountID, org.ID, newAcctID),
		Name:         input.AccountName,
		Email:        input.Email,
		Status:       "ACTIVE",
		JoinedMethod: "CREATED",
		JoinedAt:     p.tc.Now(),
	}
	if err := p.saveAccount(goCtx, reqCtx.AccountID, a); err != nil {
		return nil, fmt.Errorf("createAccount save: %w", err)
	}
	// A new account lands in the root; MoveAccount is the only way into an OU.
	if err := p.placeChild(goCtx, root.ID, a.ID); err != nil {
		return nil, fmt.Errorf("createAccount place: %w", err)
	}
	if err := p.attachFullAWSAccess(goCtx, reqCtx.AccountID, a.ID); err != nil {
		return nil, fmt.Errorf("createAccount attach FullAWSAccess: %w", err)
	}

	status := OrgCreateAccountStatus{
		ID:                 "car-" + randomLowerAlphanum(8),
		AccountName:        a.Name,
		State:              "SUCCEEDED",
		RequestedTimestamp: p.tc.Now(),
		AccountID:          a.ID,
	}
	completed := p.tc.Now()
	status.CompletedTimestamp = &completed
	if err := p.saveCreateAccountStatus(goCtx, reqCtx.AccountID, status); err != nil {
		return nil, fmt.Errorf("createAccount save status: %w", err)
	}

	return orgJSONResponse(map[string]interface{}{"CreateAccountStatus": status}, "createAccount")
}
