package emulator

import (
	"fmt"
	"net/http"
)

// Cognito's refusals for a request substrate could not use.
//
// InvalidParameterException at 400 is the code both services publish for a parameter the
// caller got wrong, and it is what [cognitoIdentityInvalidBody] and [cognitoIDPInvalidBody]
// already answered — so #1062's required-member refusals introduce no new code.
//
// The two glosses are not the same sentence, and the difference decides how much of the
// reading is substrate's. Amazon Cognito Identity publishes "Thrown for missing or bad input
// parameter(s)." — which names the missing case outright, so ListIdentityPools' refusal rests
// on the page. Amazon Cognito user pools publishes "This exception is thrown when the Amazon
// Cognito service encounters an invalid parameter." — which says invalid, not missing. For
// ListUserPools the refusal is therefore **substrate's reading**: a required parameter that is
// absent is that parameter being invalid, and InvalidParameterException is the only
// input-validation code on the page. The JSON common list's ValidationError was declined for
// the reason #950 settled — a code on the operation's own page takes precedence — and because
// answering it would split the plugin across two codes for one class of caller error.
//
// Why an absent MaxResults is refused at all, which reads as strict until the page is
// checked: API_ListIdentityPools and API_ListUserPools both mark MaxResults **Required: Yes**
// with a Valid Range of 1 to 60 and publish no default. Substrate rewrote an absent or
// non-positive value to 60, so a request AWS refuses outright was answered 200 with a page
// size the caller never asked for, and a value above 60 was honored. One range check now
// covers all of it: absent, zero, negative and above-maximum are each outside 1–60.
//
// Absent and an explicit `"MaxResults": 0` are indistinguishable after decoding into an int,
// and deliberately not distinguished — zero is itself below the published minimum, so both
// answer the same refusal and the ambiguity cannot produce a wrong answer.
//
// API_ListUserPoolClients is the near miss worth recording: it marks MaxResults
// **Required: No** over the same 1–60 range, so its silent rewrite is a page-size defect
// rather than a required-member one and is left to that issue. Its required member is
// UserPoolId, which nothing checked.

// cognitoIdentityInvalidParameter reports that an Amazon Cognito Identity request names a
// parameter substrate cannot use as given.
func cognitoIdentityInvalidParameter(message string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}

// cognitoIDPInvalidParameter reports that an Amazon Cognito user-pools request names a
// parameter substrate cannot use as given. It is a separate constructor from
// [cognitoIdentityInvalidParameter] for the same reason the two invalid-body constructors are
// separate: the services are two APIs that happen to share a code, and a future correction to
// one must not silently move the other.
func cognitoIDPInvalidParameter(message string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}

// cognitoMaxResultsMin and cognitoMaxResultsMax are the Valid Range both list pages publish
// for MaxResults.
const (
	cognitoMaxResultsMin = 1
	cognitoMaxResultsMax = 60
)

// cognitoMaxResultsOutOfRange builds the message for a MaxResults outside the published
// range, shared so the two services quote the same bounds.
func cognitoMaxResultsOutOfRange(value int) string {
	return fmt.Sprintf("MaxResults must be between %d and %d, was %d",
		cognitoMaxResultsMin, cognitoMaxResultsMax, value)
}
