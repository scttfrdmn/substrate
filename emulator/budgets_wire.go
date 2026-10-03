package emulator

// The wire is a different thing from the state, and the type below exists to keep them apart.
//
// Budget (budgets_types.go) is a persisted state record, and describeBudget and describeBudgets handed
// it straight to the caller, under `Budget` and in the `Budgets` list. Three of its fields are
// substrate's own: AccountId, which the request already names, plus Region and CreatedAt. None is a
// member of API_budgets_Budget (#756).
//
// Do not "fix" that by adding `json:"-"` to a state field. That works for the one field and leaves the
// next one to be remembered rather than prevented, and it changes the format of every recorded run,
// because MemoryStateManager snapshots those bytes and a replay reads them back.

// budgetOut is one budget as DescribeBudget and DescribeBudgets answer it.
//
// Five of API_budgets_Budget's members — the ones the record models. The rest (TimePeriod,
// CalculatedSpend, LastUpdatedTime and the others) are absent rather than present and empty (#1013's
// rule, #1199's gap).
type budgetOut struct {
	BudgetLimit BudgetLimit         `json:"BudgetLimit"`
	BudgetName  string              `json:"BudgetName"`
	BudgetType  string              `json:"BudgetType"`
	CostFilters map[string][]string `json:"CostFilters,omitempty"`
	TimeUnit    string              `json:"TimeUnit"`
}

// budgetToWire projects a persisted budget onto the published shape.
func budgetToWire(b Budget) budgetOut {
	return budgetOut{
		BudgetLimit: b.BudgetLimit,
		BudgetName:  b.BudgetName,
		BudgetType:  b.BudgetType,
		CostFilters: b.CostFilters,
		TimeUnit:    b.TimeUnit,
	}
}
