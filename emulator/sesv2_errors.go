package emulator

import "net/http"

// SESv2's refusals for a request substrate could not use.
//
// #950 found one body-decode guard answering MalformedData — a code that appears nowhere in SESv2's
// documentation — and three missing-member complaints answering a bare BadRequest, which is not a
// SESv2 code either. The published code is BadRequestException, with the Exception suffix, and it is
// in the Errors section of **every** SESv2 operation substrate implements: CreateEmailIdentity,
// GetEmailIdentity, DeleteEmailIdentity, ListEmailIdentities and SendEmail, at HTTP 400 on all five,
// glossed "The input you provided is invalid."
//
// The status is unchanged: all four sites already answered 400.
//
// The decode guard also read Message: "invalid JSON body: " + err.Error(), handing the caller
// encoding/json's own text. That is why [sesv2InvalidBody] takes no argument.

// sesv2InvalidBody reports that a request body would not decode.
func sesv2InvalidBody() *AWSError {
	return sesv2BadRequest("the request body is not valid JSON")
}

// sesv2BadRequest reports that a request cannot be used as given.
func sesv2BadRequest(message string) *AWSError {
	return &AWSError{
		Code:       "BadRequestException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
