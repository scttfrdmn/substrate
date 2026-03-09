package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// TerraformPlan is the subset of terraform show -json output used for cost
// estimation and policy validation.
type TerraformPlan struct {
	// PlannedValues holds the post-apply resource values.
	PlannedValues TFPlannedValues `json:"planned_values"`

	// ResourceChanges describes each resource change in the plan.
	ResourceChanges []TFResourceChange `json:"resource_changes"`
}

// TFPlannedValues holds the root module's planned resource values.
type TFPlannedValues struct {
	// RootModule contains the planned resources.
	RootModule TFModule `json:"root_module"`
}

// TFModule holds the planned resources for a Terraform module.
type TFModule struct {
	// Resources is the list of resources in this module.
	Resources []TFResource `json:"resources"`
}

// TFResource describes a single Terraform resource and its planned values.
type TFResource struct {
	// Address is the resource address (e.g. "aws_s3_bucket.example").
	Address string `json:"address"`

	// Type is the Terraform resource type (e.g. "aws_s3_bucket").
	Type string `json:"type"`

	// Values holds the planned attribute values for the resource.
	Values map[string]interface{} `json:"values"`
}

// TFResourceChange describes a planned change to a single resource.
type TFResourceChange struct {
	// Address is the resource address.
	Address string `json:"address"`

	// Type is the Terraform resource type.
	Type string `json:"type"`

	// Change describes the actions and before/after values.
	Change TFChange `json:"change"`
}

// TFChange holds the planned actions and post-apply values for a resource.
type TFChange struct {
	// Actions is the list of planned actions: "create", "read", "update",
	// "delete", "no-op", or "replace".
	Actions []string `json:"actions"`

	// After holds the planned post-apply attribute values.
	After map[string]interface{} `json:"after"`
}

// TerraformValidation holds the result of validating a Terraform plan.
type TerraformValidation struct {
	// EstimatedMonthlyCostUSD is the estimated monthly AWS cost in USD.
	EstimatedMonthlyCostUSD float64

	// ResourceCount is the total number of resource changes in the plan.
	ResourceCount int

	// CreatedResources lists the addresses of resources being created.
	CreatedResources []string

	// DeletedResources lists the addresses of resources being deleted.
	DeletedResources []string

	// Warnings is a list of non-blocking advisory messages.
	Warnings []string

	// Errors is a list of blocking validation errors.
	Errors []string
}

// ParseTerraformPlan decodes a Terraform JSON plan from r. It returns an
// error when the input is not valid JSON or does not conform to the expected
// shape.
func ParseTerraformPlan(r io.Reader) (*TerraformPlan, error) {
	var plan TerraformPlan
	if err := json.NewDecoder(r).Decode(&plan); err != nil {
		return nil, fmt.Errorf("parse terraform plan: %w", err)
	}
	return &plan, nil
}

// ValidateTerraformPlan estimates costs and flags policy concerns for plan. It
// does not modify any emulator state — this is a pure read-only analysis.
// When costs is nil, cost estimation is skipped.
func ValidateTerraformPlan(_ context.Context, plan *TerraformPlan, costs *CostController) (*TerraformValidation, error) {
	result := &TerraformValidation{}

	for i := range plan.ResourceChanges {
		rc := &plan.ResourceChanges[i]
		result.ResourceCount++

		for _, action := range rc.Change.Actions {
			switch action {
			case "create":
				result.CreatedResources = append(result.CreatedResources, rc.Address)
			case "delete":
				result.DeletedResources = append(result.DeletedResources, rc.Address)
			}
		}

		// Cost estimation per resource type.
		if costs != nil {
			monthlyCost := estimateTFResourceCost(rc, costs)
			result.EstimatedMonthlyCostUSD += monthlyCost
		}

		// Policy warnings.
		warnings := validateTFResource(rc)
		result.Warnings = append(result.Warnings, warnings...)
	}

	return result, nil
}

// estimateTFResourceCost returns the estimated monthly USD cost for one
// Terraform resource change. It maps resource types to known AWS operations
// and multiplies by an average monthly usage factor.
func estimateTFResourceCost(rc *TFResourceChange, costs *CostController) float64 {
	const hoursPerMonth = 730.0

	// Check if this is a create action.
	isCreate := false
	for _, a := range rc.Change.Actions {
		if a == "create" || a == "update" {
			isCreate = true
			break
		}
	}
	if !isCreate {
		return 0
	}

	switch rc.Type {
	case "aws_s3_bucket":
		// Bucket creation is free; ongoing storage costs are usage-based.
		return 0
	case "aws_s3_object":
		// Per-PUT cost.
		req := &AWSRequest{Service: "s3", Operation: "PutObject"}
		return costs.CostForRequest(req) * 1000 // assume 1000 PUTs/month
	case "aws_lambda_function":
		// Free to create; invocation costs are usage-based.
		return 0
	case "aws_lambda_invocation":
		req := &AWSRequest{Service: "lambda", Operation: "Invoke"}
		return costs.CostForRequest(req) * 1_000_000 // assume 1M invocations/month
	case "aws_dynamodb_table":
		// Free to create; read/write costs are usage-based.
		return 0
	case "aws_sqs_queue":
		return 0
	case "aws_iam_role", "aws_iam_policy", "aws_iam_user":
		return 0
	case "aws_instance":
		// Stub: $0.0104/hr for t3.micro equivalent.
		instanceType := ""
		if rc.Change.After != nil {
			if it, ok := rc.Change.After["instance_type"].(string); ok {
				instanceType = it
			}
		}
		return ec2InstanceHourlyRate(instanceType) * hoursPerMonth
	case "aws_vpc", "aws_subnet", "aws_security_group",
		"aws_internet_gateway", "aws_route_table", "aws_route":
		return 0
	default:
		return 0
	}
}

// ec2InstanceHourlyRate returns the approximate on-demand hourly rate in USD
// for common instance types.
func ec2InstanceHourlyRate(instanceType string) float64 {
	rates := map[string]float64{
		"t3.nano":    0.0052,
		"t3.micro":   0.0104,
		"t3.small":   0.0208,
		"t3.medium":  0.0416,
		"t3.large":   0.0832,
		"t3.xlarge":  0.1664,
		"t3.2xlarge": 0.3328,
		"m5.large":   0.096,
		"m5.xlarge":  0.192,
		"c5.large":   0.085,
		"c5.xlarge":  0.17,
	}
	if r, ok := rates[instanceType]; ok {
		return r
	}
	return 0.01 // default stub
}

// validateTFResource returns advisory warnings for a single resource change.
func validateTFResource(rc *TFResourceChange) []string {
	var warnings []string

	isCreate := false
	for _, a := range rc.Change.Actions {
		if a == "create" {
			isCreate = true
			break
		}
	}
	if !isCreate || rc.Change.After == nil {
		return nil
	}

	switch rc.Type {
	case "aws_s3_object":
		// Warn if the object body is large.
		if content, ok := rc.Change.After["content"].(string); ok && len(content) > 1<<20 {
			warnings = append(warnings, fmt.Sprintf(
				"%s: body size %dMB may exceed quota",
				rc.Address, len(content)/(1<<20),
			))
		}
	case "aws_iam_role":
		// Warn if the role name contains a wildcard.
		if name, ok := rc.Change.After["name"].(string); ok && strings.Contains(name, "*") {
			warnings = append(warnings, fmt.Sprintf("%s: role name contains wildcard", rc.Address))
		}
	}

	return warnings
}
