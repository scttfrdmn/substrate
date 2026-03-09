package substrate_test

import (
	"context"
	"strings"
	"testing"

	substrate "github.com/scttfrdmn/substrate"
)

const tfPlanFixture = `{
  "planned_values": {
    "root_module": {
      "resources": [
        {
          "address": "aws_s3_bucket.example",
          "type": "aws_s3_bucket",
          "values": {"bucket": "my-bucket"}
        },
        {
          "address": "aws_dynamodb_table.users",
          "type": "aws_dynamodb_table",
          "values": {"name": "users"}
        }
      ]
    }
  },
  "resource_changes": [
    {
      "address": "aws_s3_bucket.example",
      "type": "aws_s3_bucket",
      "change": {"actions": ["create"], "after": {"bucket": "my-bucket"}}
    },
    {
      "address": "aws_dynamodb_table.users",
      "type": "aws_dynamodb_table",
      "change": {"actions": ["create"], "after": {"name": "users"}}
    },
    {
      "address": "aws_lambda_function.handler",
      "type": "aws_lambda_function",
      "change": {"actions": ["create"], "after": {}}
    },
    {
      "address": "aws_instance.web",
      "type": "aws_instance",
      "change": {"actions": ["create"], "after": {"instance_type": "t3.micro"}}
    }
  ]
}`

func TestParseTerraformPlan_Valid(t *testing.T) {
	plan, err := substrate.ParseTerraformPlan(strings.NewReader(tfPlanFixture))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plan.PlannedValues.RootModule.Resources) != 2 {
		t.Errorf("expected 2 planned resources, got %d", len(plan.PlannedValues.RootModule.Resources))
	}
	if len(plan.ResourceChanges) != 4 {
		t.Errorf("expected 4 resource changes, got %d", len(plan.ResourceChanges))
	}
}

func TestParseTerraformPlan_Invalid(t *testing.T) {
	_, err := substrate.ParseTerraformPlan(strings.NewReader("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON input")
	}
}

func TestValidateTerraformPlan_CostEstimate(t *testing.T) {
	plan, err := substrate.ParseTerraformPlan(strings.NewReader(tfPlanFixture))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	costs := substrate.NewCostController(substrate.CostConfig{Enabled: true})
	result, err := substrate.ValidateTerraformPlan(context.Background(), plan, costs)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if result.ResourceCount != 4 {
		t.Errorf("expected 4 resource changes, got %d", result.ResourceCount)
	}
	if len(result.CreatedResources) != 4 {
		t.Errorf("expected 4 created resources, got %d", len(result.CreatedResources))
	}
	// t3.micro at $0.0104/hr × 730 hr/month ≈ $7.59
	if result.EstimatedMonthlyCostUSD <= 0 {
		t.Errorf("expected positive estimated monthly cost, got %f", result.EstimatedMonthlyCostUSD)
	}
}

func TestValidateTerraformPlan_EmptyPlan(t *testing.T) {
	emptyPlan := `{"planned_values":{"root_module":{"resources":[]}},"resource_changes":[]}`
	plan, err := substrate.ParseTerraformPlan(strings.NewReader(emptyPlan))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	result, err := substrate.ValidateTerraformPlan(context.Background(), plan, nil)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if result.ResourceCount != 0 {
		t.Errorf("expected 0 resources, got %d", result.ResourceCount)
	}
	if result.EstimatedMonthlyCostUSD != 0 {
		t.Errorf("expected zero cost for empty plan, got %f", result.EstimatedMonthlyCostUSD)
	}
}

func TestValidateTerraformPlan_DeletedResources(t *testing.T) {
	planJSON := `{
		"planned_values": {"root_module": {"resources": []}},
		"resource_changes": [
			{
				"address": "aws_s3_bucket.old",
				"type": "aws_s3_bucket",
				"change": {"actions": ["delete"], "after": null}
			}
		]
	}`
	plan, err := substrate.ParseTerraformPlan(strings.NewReader(planJSON))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	result, err := substrate.ValidateTerraformPlan(context.Background(), plan, nil)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(result.DeletedResources) != 1 || result.DeletedResources[0] != "aws_s3_bucket.old" {
		t.Errorf("expected deleted aws_s3_bucket.old, got %v", result.DeletedResources)
	}
}
