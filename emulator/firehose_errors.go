package emulator

import "net/http"

// Firehose's refusals for a request substrate could not use.
//
// #950 found six sites in firehose_plugin.go: three body-decode guards answering MalformedData, a
// code that appears nowhere in Firehose's documentation, and three missing-member complaints
// answering InvalidArgumentException.
//
// **Firehose is the service that vindicated checking every guarded operation rather than a
// representative one.** InvalidArgumentException/400 is published for CreateDeliveryStream — the
// operation anyone would check first — and for neither of the other two. DescribeDeliveryStream
// publishes exactly one error, ResourceNotFoundException, and DeleteDeliveryStream two,
// ResourceInUseException and ResourceNotFoundException. Neither publishes any input-validation code at
// all. Applying the obvious replacement from the representative page would have been wrong at two of
// the three sites, which is #950's own defect relocated.
//
// So both refusals take ValidationError/400 from Firehose's common-errors page — the fifteen-entry
// boilerplate, "The input doesn't meet the required format or constraints. Check that all required
// parameters are included and that values are valid." That is the only code published for all three
// operations. It replaces InvalidArgumentException even at the one site where that code is published,
// because a missing DeliveryStreamName is one condition and answering it with two different codes
// depending on which operation received it is the failure mode the uniform choice exists to avoid.
//
// The status is unchanged: all six already answered 400.
//
// The three decode guards also read Message: "invalid JSON body: " + err.Error(), handing the caller
// encoding/json's own text. That is why [firehoseInvalidBody] takes no argument.

// firehoseInvalidBody reports that a request body would not decode.
func firehoseInvalidBody() *AWSError {
	return firehoseValidationError("the request body is not valid JSON")
}

// firehoseValidationError reports that a request does not satisfy the constraints its operation
// states.
func firehoseValidationError(message string) *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
