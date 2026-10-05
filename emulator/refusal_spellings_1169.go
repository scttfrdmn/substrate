package emulator

import "net/http"

// The two refusal codes #1169 corrected. Each was a code invented from the member's name rather than
// read off the page, and each was a near-miss of a code the service does publish, so a caller catching
// the published exception class never caught it.

// quicksightInvalidParameterValue is the refusal QuickSight publishes for a parameter whose value is
// not valid.
//
// API_CreateDataSource and API_CreateDataSet both list InvalidParameterValueException at 400. The
// suffixless InvalidParameterValue the two creates used to answer is on no QuickSight page, and boto3
// raised it as a generic ClientError rather than the modeled exception. It serves both a body that
// does not parse and an absent identifier, each with its own message.
func quicksightInvalidParameterValue(message string) *AWSError {
	return &AWSError{Code: "InvalidParameterValueException", Message: message, HTTPStatus: http.StatusBadRequest}
}

// ramMissingParameter is the refusal for a member a RAM operation marks Required: Yes that the request
// did not send.
//
// MissingRequiredParameter, which these sites used to answer, is published nowhere in RAM. Of the two
// codes RAM does publish for bad input, ValidationError is the one for this condition: RAM's Common
// Errors page glosses it "The input doesn't meet the required format or constraints. Check that all
// required parameters are included and that values are valid". InvalidParameterException, which
// API_CreateResourceShare lists, describes a parameter sent with a value that is not valid, and an
// absent member was not sent at all. One code serves every share operation, because loadShare is their
// one lookup and Common Errors applies to all of them. It is the code ramInvalidBody already answers.
func ramMissingParameter(message string) *AWSError {
	return &AWSError{Code: "ValidationError", Message: message, HTTPStatus: http.StatusBadRequest}
}
