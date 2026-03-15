package substrate

import "time"

// budgetsNamespace is the service name used by BudgetsPlugin.
const budgetsNamespace = "budgets"

// Budget represents an AWS Budgets budget.
type Budget struct {
	// AccountID is the AWS account that owns this budget.
	AccountID string `json:"AccountId"`

	// BudgetName is the unique name of the budget within the account.
	BudgetName string `json:"BudgetName"`

	// BudgetType indicates what is being tracked (e.g. "COST", "USAGE").
	BudgetType string `json:"BudgetType"`

	// BudgetLimit is the threshold for the budget.
	BudgetLimit BudgetLimit `json:"BudgetLimit"`

	// TimeUnit is the granularity of the budget period (e.g. "MONTHLY").
	TimeUnit string `json:"TimeUnit"`

	// CostFilters optionally restricts which costs count toward the budget.
	CostFilters map[string][]string `json:"CostFilters,omitempty"`

	// CreatedAt is the time this budget was created.
	CreatedAt time.Time `json:"CreatedAt"`

	// Region is the AWS region associated with this budget record.
	Region string `json:"Region"`
}

// BudgetLimit specifies a budget threshold amount and unit.
type BudgetLimit struct {
	// Amount is the budget threshold as a decimal string.
	Amount string `json:"Amount"`

	// Unit is the currency unit (e.g. "USD").
	Unit string `json:"Unit"`
}
