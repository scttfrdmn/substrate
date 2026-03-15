package substrate

import (
	"context"
	"encoding/json"
	"fmt"
)

// ----- v0.27.0 — Budgets -----------------------------------------------------

// deployBudgetsBudget creates an AWS Budgets budget for the given CFN resource.
func (d *StackDeployer) deployBudgetsBudget(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// CloudFormation AWS::Budgets::Budget nests everything under a "Budget" key.
	budgetName := logicalID
	if budget, ok := props["Budget"].(map[string]interface{}); ok {
		if name, ok := budget["BudgetName"].(string); ok && name != "" {
			budgetName = name
		}
	}

	limitAmount := "100.00"
	limitUnit := "USD"
	if budget, ok := props["Budget"].(map[string]interface{}); ok {
		if bl, ok := budget["BudgetLimit"].(map[string]interface{}); ok {
			if amt, ok := bl["Amount"].(string); ok && amt != "" {
				limitAmount = amt
			}
			if unit, ok := bl["Unit"].(string); ok && unit != "" {
				limitUnit = unit
			}
		}
	}

	body := map[string]interface{}{
		"AccountId": cctx.accountID,
		"Budget": map[string]interface{}{
			"BudgetName":  budgetName,
			"BudgetType":  "COST",
			"TimeUnit":    "MONTHLY",
			"BudgetLimit": map[string]string{"Amount": limitAmount, "Unit": limitUnit},
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal budgets budget body: %w", err)
	}

	req := &AWSRequest{
		Service:   "budgets",
		Operation: "CreateBudget",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AmazonBudgetServiceGateway.CreateBudget"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Budgets::Budget",
		PhysicalID: budgetName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}
