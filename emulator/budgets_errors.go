package emulator

import "net/http"

// Budgets' refusal for a request body substrate could not decode.
//
// #950's inventory did not list Budgets, and it should have: five sites in budgets_plugin.go answered
// MalformedData, the same invented code the audit found in EFS, Firehose and SESv2. It appears nowhere
// in the AWS Cost Management reference.
//
// InvalidParameterException/400 is in the Errors section of all five guarded operations —
// CreateBudget, DescribeBudgets, DescribeBudget, UpdateBudget and DeleteBudget — and its gloss is this
// condition in AWS's own words: "An error on the client occurred. Typically, the cause is an invalid
// input value." Every error Budgets publishes carries HTTP 400, which is what the five sites already
// answered, so the status is unchanged.
//
// All five also read Message: "invalid JSON: " + err.Error(), handing the caller encoding/json's own
// text, which is why this constructor takes no argument.

// budgetsInvalidBody reports that a request body would not decode.
func budgetsInvalidBody() *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}
