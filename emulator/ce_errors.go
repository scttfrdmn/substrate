package emulator

import "net/http"

// Cost Explorer's refusal for a request body substrate could not decode.
//
// #950's inventory did not list Cost Explorer either; three sites in ce_plugin.go answered
// MalformedData, which appears nowhere in the AWS Cost Management reference.
//
// Unlike Budgets — which shares that reference and publishes InvalidParameterException on every
// operation — Cost Explorer publishes **no** input-validation code at all. The three guarded
// operations' Errors sections are data-availability and pagination only:
//
//	GetCostAndUsage     BillExpirationException, BillingViewHealthStatusException, DataUnavailableException,
//	                    InvalidNextTokenException, LimitExceededException, RequestChangedException,
//	                    ResourceNotFoundException — all 400
//	GetCostForecast     BillingViewHealthStatusException, DataUnavailableException, LimitExceededException,
//	                    ResourceNotFoundException — all 400
//	GetDimensionValues  as GetCostAndUsage
//
// So the code comes from the common-errors page the two services share — the fifteen-entry boilerplate
// — as it does for Step Functions, Systems Manager, EventBridge, SageMaker, Firehose and ACM:
// ValidationError, HTTP 400, "The input doesn't meet the required format or constraints. Check that
// all required parameters are included and that values are valid."
//
// Two services under one reference answering two different codes for one condition looks like the
// inconsistency #950 exists to remove, and is not: the rule the audit settled on is to prefer a
// per-operation code wherever one covers every guarded site, and to fall back to the common page only
// where none does. Budgets has one; Cost Explorer does not.
//
// All three sites also read Message: "invalid JSON: " + err.Error(), which is why this constructor
// takes no argument.

// ceInvalidBody reports that a request body would not decode.
func ceInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}
