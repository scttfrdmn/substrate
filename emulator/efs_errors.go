package emulator

import "net/http"

// EFS's refusals for a request substrate could not use.
//
// #950 found four sites in efs_plugin.go answering MalformedData for a body that would not decode.
// That code is not EFS's — the string appears nowhere in EFS's documentation.
//
// #950 also recorded that EFS had no common-errors page to fall back to, because the user guide's
// api-errors.html redirects to the guide's index. **That is no longer true, and #1064 corrected it:**
// https://docs.aws.amazon.com/efs/latest/APIReference/CommonErrors.html now publishes the same
// fifteen-entry boilerplate sixteen other services do, ValidationError/400 included. AWS's
// consolidation did not only change what a page says — it created pages that did not exist, which is
// the one direction the #1064 sweep was not looking in.
//
// The answer below is unchanged all the same, and the reason is the rule rather than luck: step 1 of
// #950's procedure is the operation's own Errors section and step 2 is the common page, so a common
// page appearing underneath a satisfied step 1 changes nothing. BadRequest/400 is in the Errors
// section of all four guarded operations —
// CreateFileSystem, CreateAccessPoint, CreateMountTarget and TagResource — glossed, on each of them,
// "Returned if the request is malformed or contains an error such as an invalid parameter value or a
// missing required parameter." That describes this condition in AWS's own words, and it is the code
// the rest of the plugin already answers for a missing member, so the fix makes EFS internally
// consistent rather than merely correct.
//
// The status is unchanged: all four already answered 400, which is what the pages give BadRequest.
//
// The second half of the fix is the message. All four read
//
//	Message: "invalid JSON body: " + err.Error()
//
// which handed the caller encoding/json's own text — which Go struct field failed to unmarshal — from
// an endpoint whose whole purpose is to look like AWS. That is why [efsInvalidBody] takes no argument.

// efsInvalidBody reports that a request body would not decode.
func efsInvalidBody() *AWSError {
	return efsBadRequest("the request body is not valid JSON")
}

// efsBadRequest reports that a request is malformed or cannot be used as given.
//
// One constructor for every such refusal in the plugin, so the code and the status cannot come to
// differ between the operation that creates a file system and the one that tags it.
func efsBadRequest(message string) *AWSError {
	return &AWSError{
		Code:       "BadRequest",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
