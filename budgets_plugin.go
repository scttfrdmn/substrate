package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"
)

// BudgetsPlugin emulates the AWS Budgets service.
// It supports CRUD operations on budgets using the Budgets JSON-target protocol
// (X-Amz-Target: AmazonBudgetServiceGateway.{Op}).
type BudgetsPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "budgets".
func (p *BudgetsPlugin) Name() string { return budgetsNamespace }

// Initialize configures the BudgetsPlugin with the provided configuration.
func (p *BudgetsPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for BudgetsPlugin.
func (p *BudgetsPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Budgets JSON-target request to the appropriate handler.
func (p *BudgetsPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateBudget":
		return p.createBudget(ctx, req)
	case "DescribeBudgets":
		return p.describeBudgets(ctx, req)
	case "DescribeBudget":
		return p.describeBudget(ctx, req)
	case "UpdateBudget":
		return p.updateBudget(ctx, req)
	case "DeleteBudget":
		return p.deleteBudget(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "BudgetsPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- state helpers ---

func budgetKey(acct, name string) string { return "budget:" + acct + "/" + name }
func budgetNamesKey(acct string) string  { return "budget_names:" + acct }

func (p *BudgetsPlugin) saveBudget(ctx context.Context, b Budget) error {
	data, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("marshal budget: %w", err)
	}
	if err := p.state.Put(ctx, budgetsNamespace, budgetKey(b.AccountID, b.BudgetName), data); err != nil {
		return fmt.Errorf("put budget: %w", err)
	}
	// update names index
	names, _ := p.loadBudgetNames(ctx, b.AccountID)
	for _, n := range names {
		if n == b.BudgetName {
			return nil
		}
	}
	names = append(names, b.BudgetName)
	sort.Strings(names)
	return p.saveBudgetNames(ctx, b.AccountID, names)
}

func (p *BudgetsPlugin) loadBudget(ctx context.Context, acct, name string) (*Budget, error) {
	data, err := p.state.Get(ctx, budgetsNamespace, budgetKey(acct, name))
	if err != nil {
		return nil, fmt.Errorf("get budget: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var b Budget
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("unmarshal budget: %w", err)
	}
	return &b, nil
}

func (p *BudgetsPlugin) deleteBudgetFromState(ctx context.Context, acct, name string) error {
	if err := p.state.Delete(ctx, budgetsNamespace, budgetKey(acct, name)); err != nil {
		return fmt.Errorf("delete budget: %w", err)
	}
	names, _ := p.loadBudgetNames(ctx, acct)
	filtered := names[:0]
	for _, n := range names {
		if n != name {
			filtered = append(filtered, n)
		}
	}
	return p.saveBudgetNames(ctx, acct, filtered)
}

func (p *BudgetsPlugin) loadBudgetNames(ctx context.Context, acct string) ([]string, error) {
	data, err := p.state.Get(ctx, budgetsNamespace, budgetNamesKey(acct))
	if err != nil {
		return nil, fmt.Errorf("get budget names: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("unmarshal budget names: %w", err)
	}
	return names, nil
}

func (p *BudgetsPlugin) saveBudgetNames(ctx context.Context, acct string, names []string) error {
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("marshal budget names: %w", err)
	}
	return p.state.Put(ctx, budgetsNamespace, budgetNamesKey(acct), data)
}

// --- operations ---

func (p *BudgetsPlugin) createBudget(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		AccountID string `json:"AccountId"`
		Budget    Budget `json:"Budget"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	acct := input.AccountID
	if acct == "" {
		acct = reqCtx.AccountID
	}

	existing, err := p.loadBudget(context.Background(), acct, input.Budget.BudgetName)
	if err != nil {
		return nil, fmt.Errorf("createBudget load: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{
			Code:       "DuplicateRecordException",
			Message:    "Budget already exists: " + input.Budget.BudgetName,
			HTTPStatus: http.StatusConflict,
		}
	}

	b := input.Budget
	b.AccountID = acct
	b.CreatedAt = p.tc.Now()
	b.Region = reqCtx.Region
	if b.BudgetType == "" {
		b.BudgetType = "COST"
	}
	if b.TimeUnit == "" {
		b.TimeUnit = "MONTHLY"
	}
	if b.BudgetLimit.Unit == "" {
		b.BudgetLimit.Unit = "USD"
	}

	if err := p.saveBudget(context.Background(), b); err != nil {
		return nil, fmt.Errorf("createBudget save: %w", err)
	}

	body, err := json.Marshal(map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("createBudget marshal: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *BudgetsPlugin) describeBudgets(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		AccountID string `json:"AccountId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	acct := input.AccountID
	if acct == "" {
		acct = reqCtx.AccountID
	}

	names, err := p.loadBudgetNames(context.Background(), acct)
	if err != nil {
		return nil, fmt.Errorf("describeBudgets names: %w", err)
	}

	budgets := make([]Budget, 0, len(names))
	for _, name := range names {
		b, loadErr := p.loadBudget(context.Background(), acct, name)
		if loadErr != nil || b == nil {
			continue
		}
		budgets = append(budgets, *b)
	}

	out := map[string]interface{}{
		"Budgets": budgets,
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("describeBudgets marshal: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *BudgetsPlugin) describeBudget(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		AccountID  string `json:"AccountId"`
		BudgetName string `json:"BudgetName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	acct := input.AccountID
	if acct == "" {
		acct = reqCtx.AccountID
	}

	b, err := p.loadBudget(context.Background(), acct, input.BudgetName)
	if err != nil {
		return nil, fmt.Errorf("describeBudget load: %w", err)
	}
	if b == nil {
		return nil, &AWSError{
			Code:       "NotFoundException",
			Message:    "Budget not found: " + input.BudgetName,
			HTTPStatus: http.StatusNotFound,
		}
	}

	out := map[string]interface{}{"Budget": b}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("describeBudget marshal: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *BudgetsPlugin) updateBudget(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		AccountID string `json:"AccountId"`
		NewBudget Budget `json:"NewBudget"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	acct := input.AccountID
	if acct == "" {
		acct = reqCtx.AccountID
	}

	existing, err := p.loadBudget(context.Background(), acct, input.NewBudget.BudgetName)
	if err != nil {
		return nil, fmt.Errorf("updateBudget load: %w", err)
	}
	if existing == nil {
		return nil, &AWSError{
			Code:       "NotFoundException",
			Message:    "Budget not found: " + input.NewBudget.BudgetName,
			HTTPStatus: http.StatusNotFound,
		}
	}

	b := input.NewBudget
	b.AccountID = acct
	b.CreatedAt = existing.CreatedAt
	b.Region = reqCtx.Region

	if err := p.saveBudget(context.Background(), b); err != nil {
		return nil, fmt.Errorf("updateBudget save: %w", err)
	}

	body, err := json.Marshal(map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("updateBudget marshal: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}

func (p *BudgetsPlugin) deleteBudget(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		AccountID  string `json:"AccountId"`
		BudgetName string `json:"BudgetName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	acct := input.AccountID
	if acct == "" {
		acct = reqCtx.AccountID
	}

	existing, err := p.loadBudget(context.Background(), acct, input.BudgetName)
	if err != nil {
		return nil, fmt.Errorf("deleteBudget load: %w", err)
	}
	if existing == nil {
		return nil, &AWSError{
			Code:       "NotFoundException",
			Message:    "Budget not found: " + input.BudgetName,
			HTTPStatus: http.StatusNotFound,
		}
	}

	if err := p.deleteBudgetFromState(context.Background(), acct, input.BudgetName); err != nil {
		return nil, fmt.Errorf("deleteBudget remove: %w", err)
	}

	body, err := json.Marshal(map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("deleteBudget marshal: %w", err)
	}
	return &AWSResponse{Body: body, StatusCode: http.StatusOK}, nil
}
