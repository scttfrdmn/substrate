package substrate_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func newBudgetsTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: false})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.BudgetsPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize budgets plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func budgetsRequest(t *testing.T, ts *httptest.Server, op string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal budgets request: %v", err)
	}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonBudgetServiceGateway."+op)
	req.Host = "budgets.amazonaws.com"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("budgets request %s: %v", op, err)
	}
	return resp
}

func TestBudgets_CRUD(t *testing.T) {
	ts := newBudgetsTestServer(t)

	acct := "123456789012"
	budgetName := "MonthlyCostBudget"

	// CreateBudget
	createResp := budgetsRequest(t, ts, "CreateBudget", map[string]interface{}{
		"AccountId": acct,
		"Budget": map[string]interface{}{
			"BudgetName":  budgetName,
			"BudgetType":  "COST",
			"TimeUnit":    "MONTHLY",
			"BudgetLimit": map[string]string{"Amount": "100.00", "Unit": "USD"},
		},
	})
	createResp.Body.Close() //nolint:errcheck
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateBudget: expected 200, got %d", createResp.StatusCode)
	}

	// DescribeBudgets
	descResp := budgetsRequest(t, ts, "DescribeBudgets", map[string]interface{}{
		"AccountId": acct,
	})
	defer descResp.Body.Close() //nolint:errcheck
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeBudgets: expected 200, got %d", descResp.StatusCode)
	}
	var descOut map[string]interface{}
	if err := json.NewDecoder(descResp.Body).Decode(&descOut); err != nil {
		t.Fatalf("DescribeBudgets decode: %v", err)
	}
	budgets, ok := descOut["Budgets"].([]interface{})
	if !ok || len(budgets) != 1 {
		t.Fatalf("expected 1 budget, got %v", descOut["Budgets"])
	}

	// DescribeBudget (single)
	singleResp := budgetsRequest(t, ts, "DescribeBudget", map[string]interface{}{
		"AccountId":  acct,
		"BudgetName": budgetName,
	})
	defer singleResp.Body.Close() //nolint:errcheck
	if singleResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeBudget: expected 200, got %d", singleResp.StatusCode)
	}

	// UpdateBudget
	updateResp := budgetsRequest(t, ts, "UpdateBudget", map[string]interface{}{
		"AccountId": acct,
		"NewBudget": map[string]interface{}{
			"BudgetName":  budgetName,
			"BudgetType":  "COST",
			"TimeUnit":    "MONTHLY",
			"BudgetLimit": map[string]string{"Amount": "200.00", "Unit": "USD"},
		},
	})
	updateResp.Body.Close() //nolint:errcheck
	if updateResp.StatusCode != http.StatusOK {
		t.Fatalf("UpdateBudget: expected 200, got %d", updateResp.StatusCode)
	}

	// DeleteBudget
	delResp := budgetsRequest(t, ts, "DeleteBudget", map[string]interface{}{
		"AccountId":  acct,
		"BudgetName": budgetName,
	})
	delResp.Body.Close() //nolint:errcheck
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteBudget: expected 200, got %d", delResp.StatusCode)
	}

	// NotFoundException after delete.
	notFoundResp := budgetsRequest(t, ts, "DescribeBudget", map[string]interface{}{
		"AccountId":  acct,
		"BudgetName": budgetName,
	})
	notFoundResp.Body.Close() //nolint:errcheck
	if notFoundResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", notFoundResp.StatusCode)
	}
}

func TestBudgets_DuplicateCreate(t *testing.T) {
	ts := newBudgetsTestServer(t)

	acct := "123456789012"
	body := map[string]interface{}{
		"AccountId": acct,
		"Budget": map[string]interface{}{
			"BudgetName":  "DupBudget",
			"BudgetType":  "COST",
			"TimeUnit":    "MONTHLY",
			"BudgetLimit": map[string]string{"Amount": "50.00", "Unit": "USD"},
		},
	}

	resp1 := budgetsRequest(t, ts, "CreateBudget", body)
	resp1.Body.Close() //nolint:errcheck
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first create: expected 200, got %d", resp1.StatusCode)
	}

	resp2 := budgetsRequest(t, ts, "CreateBudget", body)
	resp2.Body.Close() //nolint:errcheck
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate create: expected 409, got %d", resp2.StatusCode)
	}
}

func TestBudgets_UpdateNotFound(t *testing.T) {
	ts := newBudgetsTestServer(t)

	resp := budgetsRequest(t, ts, "UpdateBudget", map[string]interface{}{
		"AccountId": "123456789012",
		"NewBudget": map[string]interface{}{
			"BudgetName":  "NonExistent",
			"BudgetType":  "COST",
			"TimeUnit":    "MONTHLY",
			"BudgetLimit": map[string]string{"Amount": "50.00", "Unit": "USD"},
		},
	})
	resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}
