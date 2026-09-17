package emulator

import "net/http"

// MSK's refusals for a request substrate could not use, and the one service in #950's
// inventory whose code could not be verified against anything.
//
// Eleven sites in msk_plugin.go answered a bare BadRequest — two body-decode guards
// and nine complaints about a member or an ARN. #950 kept the code and fixed only the
// message, because **MSK publishes nothing to check it against**, and the three places
// a code would normally come from are each absent:
//
//   - There is no common-errors page. The CommonErrors link MSK's reference prints
//     does not resolve to the fifteen-entry boilerplate other services publish, so
//     the ValidationError fallback that settled EventBridge, SageMaker, Firehose,
//     ACM and Cost Explorer is not available here.
//   - The operation pages carry **no Errors section**. clusters.html documents
//     response codes only, and the Error schema it names has members {message,
//     invalidParameter} — no Code member at all — so nothing on the page states a
//     code string a caller could match on.
//   - CreateClusterV2, one of the two operations guarded here, **has no documentation
//     page**.
//
// So BadRequest stays, recorded as substrate's reading rather than presented as
// modeled — the same treatment CloudWatch's SerializationException gets in
// [cwSerializationError], for the same reason: the alternative is to invent a code
// on no evidence, which is worse than keeping one that has at least been shipped.
// HTTP 400 is not in doubt; it is the status every site already answered and the one
// the response codes on clusters.html give a client error.
//
// The message half needed no citation. Both decode guards read
//
//	Message: "invalid JSON body: " + err.Error()
//
// which handed the caller encoding/json's own text — which Go struct field failed to
// unmarshal — from an endpoint whose whole purpose is to look like AWS. That is why
// [mskInvalidBody] takes no argument.
//
// If MSK ever publishes an Errors section, this is the one file in the #950 set that
// should be revisited; every other service's code is quoted from a page.

// mskInvalidBody reports that a request body would not decode.
func mskInvalidBody() *AWSError {
	return mskBadRequest("the request body is not valid JSON")
}

// mskBadRequest reports that a request cannot be used as given.
//
// One constructor for all eleven call sites, so the code and the status cannot come to
// differ between the operation that creates a cluster and the one that describes it.
func mskBadRequest(message string) *AWSError {
	return &AWSError{
		Code:       "BadRequest",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
