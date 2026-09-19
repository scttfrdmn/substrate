package emulator

import "net/http"

// This file holds the body-parse refusals for the services that had none.
//
// #1007 found 95 sites spelled `_ = json.Unmarshal(req.Body, &x)`: a request body
// decoded, the error thrown away, and the handler continuing on a zero-valued
// struct. That is worse than a wrong error code, because a wrong code is at least
// an error — a discarded one produces a plausible success from a body the caller
// meant to be read, or a refusal about something else entirely. The sweep landed in
// three slices; the first two went into services that already had a constructor to
// call. These twenty-two are the tail: twenty-three files that answered a
// body-parse failure with an inline `&AWSError{…}` literal at whichever site
// happened to check, so there was no single place to state what the service
// publishes and no way for the next handler to inherit it.
//
// They are in one file rather than scattered across twenty-three plugin files
// because the thing under review is one decision repeated twenty-two times — which
// code does *this* service publish for a body that will not parse — and a reviewer
// reading them side by side can see a borrowed code that a reviewer reading one
// plugin cannot. That is how the two wrong codes below were found.
//
// Two rules the whole file follows, both from #950:
//
//   - The message never carries `err.Error()`. Passing encoding/json's own text
//     through hands the caller a Go struct field name from an endpoint whose whole
//     purpose is to look like AWS. #950 removed twenty-two such leaks; nine of the
//     twenty-three files in this slice were still leaking from their *existing*
//     checked guards, and those sites now call these constructors too.
//   - A code is used only where the service publishes it, for the condition it
//     publishes it for. Where a service's operation pages publish only codes that
//     name a *parameter*, the refusal uses the JSON-protocol common-errors
//     `ValidationError`, because a body that will not parse has no parameters to
//     be invalid — see [ramInvalidBody] and [cloudtrailInvalidBody], the two
//     places where that distinction changed an answer.

// ssoInvalidBody reports that an IAM Identity Center request body would not decode.
//
// ValidationException at 400 is what the file's own checked guards already
// answered, and AWS publishes it on the Identity Store and SSO Admin operation
// pages. The nine sites that reach this were all list operations behind a
// `len(req.Body) > 0` check, so an absent body still lists everything.
func ssoInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationException",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}

// apigwInvalidBody reports that an API Gateway v1 request body would not decode.
//
// BadRequestException at 400 is the code API Gateway publishes for a malformed
// request and the one this file's checked guards already answered; the REST API
// reference publishes it on every operation that takes a body.
func apigwInvalidBody() *AWSError {
	return &AWSError{
		Code:       "BadRequestException",
		Message:    "invalid request body",
		HTTPStatus: http.StatusBadRequest,
	}
}

// apigwv2InvalidBody reports that an API Gateway v2 request body would not decode.
//
// Separate from [apigwInvalidBody] because the two are separate APIs with separate
// references, even though both publish BadRequestException at 400 — collapsing them
// would make a v2 refusal cite a v1 page.
func apigwv2InvalidBody() *AWSError {
	return &AWSError{
		Code:       "BadRequestException",
		Message:    "invalid request body",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ramInvalidBody reports that a Resource Access Manager request body would not decode.
//
// This is one of the two refusals in this file that changed an answer. RAM's five
// sites answered MalformedQueryString at 400, and that is wrong twice over:
// https://docs.aws.amazon.com/ram/latest/APIReference/CommonErrors.html does not
// publish the code at all, and where AWS does publish it — the Query-protocol
// common-errors list, which RAM is not — it carries HTTP **404** and is glossed
// "The query string contains a syntax error." That is the URL's query string, not
// the body.
//
// RAM's own operation pages publish InvalidParameterException at 400, "The
// operation failed because a parameter you specified isn't valid." It is the more
// service-idiomatic code but the worse fit: a body that will not parse yields no
// parameters at all, so naming one sends the caller to inspect a value that was
// never read. RAM's common-errors page publishes ValidationError at 400, "The
// input doesn't meet the required format or constraints", which is a statement
// about the input as a whole. Operation pages normally outrank the common list;
// they do not outrank it when the code they publish describes a different
// condition, which is the rule #950 established.
func ramInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}

// cloudtrailInvalidBody reports that a CloudTrail request body would not decode.
//
// The other refusal in this file that changed an answer. The site answered
// InvalidParameterCombinationException at 400, a real CloudTrail code published on
// its operation pages — but glossed "This exception is thrown when the combination
// of parameters provided is not valid", which is two parameters that cannot be used
// together, not a parse failure. It is also not published on LookupEvents at all,
// whose complete Errors list is seven codes with no parse or serialization code
// among them.
//
// CloudTrail publishes neither InvalidRequestException nor ValidationException on
// any operation page checked, so the landing place is the common-errors
// ValidationError at 400, for the same reason given on [ramInvalidBody].
func cloudtrailInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecsInvalidBody reports that an Amazon ECS request body would not decode.
//
// InvalidParameterException at 400 is published on every ECS operation page and is
// what this file's own checked guards answered. ECS is one of the services whose
// generic-input code is the only 400 it publishes for caller error, so there is no
// narrower choice to make here.
func ecsInvalidBody() *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "invalid request body",
		HTTPStatus: http.StatusBadRequest,
	}
}

// cwlInvalidBody reports that a CloudWatch Logs request body would not decode.
//
// Named cwl rather than cw because the `cw` prefix belongs to the CloudWatch
// metrics plugin, which has its own [cwSerializationError] for the same condition —
// two services, two references, two codes, and a shared prefix would hide that.
// InvalidParameterException at 400 is what CloudWatch Logs publishes and what this
// file's checked guards already answered.
func cwlInvalidBody() *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "invalid request body",
		HTTPStatus: http.StatusBadRequest,
	}
}

// timestreamInvalidBody reports that a Timestream request body would not decode.
//
// ValidationException at 400 is published on the Timestream Query and Write
// operation pages and is the code the file's required-member checks already use.
func timestreamInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationException",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}

// redshiftDataInvalidBody reports that a Redshift Data API request body would not decode.
//
// ValidationException at 400 is published on every Redshift Data API operation and
// is what the file's checked guard answered — minus the err.Error() it was leaking.
func redshiftDataInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationException",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}

// cognitoIdentityInvalidBody reports that a Cognito Identity request body would not decode.
//
// Spelled out rather than sharing a `cognito` prefix with [cognitoIDPInvalidBody]:
// Cognito Identity (identity pools) and Cognito Identity Provider (user pools) are
// separate APIs with separate references, and both publish InvalidParameterException
// at 400 for their own reasons.
func cognitoIdentityInvalidBody() *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "invalid request body",
		HTTPStatus: http.StatusBadRequest,
	}
}

// cognitoIDPInvalidBody reports that a Cognito user-pools request body would not decode.
// See [cognitoIdentityInvalidBody] for why the two are not one constructor.
func cognitoIDPInvalidBody() *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "invalid request body",
		HTTPStatus: http.StatusBadRequest,
	}
}

// backupInvalidBody reports that an AWS Backup request body would not decode.
//
// InvalidRequestException at 400 is published on the Backup operation pages,
// glossed as indicating input that is not valid — the closest published fit for a
// body that cannot be read at all. The file's checked guard already answered it and
// was leaking err.Error(); this does not.
func backupInvalidBody() *AWSError {
	return &AWSError{
		Code:       "InvalidRequestException",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}

// athenaInvalidBody reports that an Athena request body would not decode.
//
// InvalidRequestException at 400 is the code Athena publishes on every operation
// page for a request it cannot honor, and the one the file's own required-member
// refusals use.
func athenaInvalidBody() *AWSError {
	return &AWSError{
		Code:       "InvalidRequestException",
		Message:    "invalid request body",
		HTTPStatus: http.StatusBadRequest,
	}
}

// smInvalidBody reports that a Secrets Manager request body would not decode.
//
// InvalidRequestException at 400 is what the file's own checked guard answers, and
// Secrets Manager publishes it on every operation page. The site this replaces
// carried `//nolint:errcheck // optional body` — true about the body's optionality,
// and beside the point about the error, which is the pattern #1007's second slice
// found fourteen times.
func smInvalidBody() *AWSError {
	return &AWSError{
		Code:       "InvalidRequestException",
		Message:    "invalid JSON body",
		HTTPStatus: http.StatusBadRequest,
	}
}

// emrInvalidBody reports that an EMR Serverless request body would not decode.
//
// ValidationException at 400 is published on every EMR Serverless operation page
// and is what the file's checked guard already answered.
func emrInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationException",
		Message:    "invalid request body",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecrInvalidBody reports that an Amazon ECR request body would not decode.
//
// InvalidParameterException at 400 is published on the ECR operation pages and is
// what the file's checked guards answered.
func ecrInvalidBody() *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "invalid request body",
		HTTPStatus: http.StatusBadRequest,
	}
}

// appsyncInvalidBody reports that an AWS AppSync request body would not decode.
//
// BadRequestException at 400 is published on the AppSync operation pages and is
// what the file's checked guards answered — three of them leaking err.Error(),
// which this does not.
func appsyncInvalidBody() *AWSError {
	return &AWSError{
		Code:       "BadRequestException",
		Message:    "invalid request body",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ddbInvalidBody reports that an Amazon DynamoDB request body would not decode.
//
// SerializationException at 400 is what DynamoDB puts on the wire, and it is the
// one code in this file that no AWS reference publishes: it is absent from the
// Common Errors page, from the developer guide's error list, from
// Programming.LowLevelAPI.html, and from DynamoDB's own model. The name appears in
// AWS-published source only as a Lambda client-side helper and a Smithy Kotlin
// serde class. On the wire it arrives under the Coral namespace
// (com.amazon.coral.service#SerializationException) rather than DynamoDB's own
// com.amazonaws.dynamodb.v20120810#, because the protocol layer rejects the body
// before the request reaches the service at all — which is exactly why the service
// never documents it.
//
// It is kept because wire fidelity is the point: an SDK's retry classifier reads
// the code it actually receives, not the one the page omits. The provenance is
// observed behavior rather than the API model, so it belongs in the release's
// ## Provenance section. The twenty in-tree guards already answer it.
func ddbInvalidBody() *AWSError {
	return &AWSError{
		Code:       "SerializationException",
		Message:    "Failed to parse request payload",
		HTTPStatus: http.StatusBadRequest,
	}
}

// wafv2InvalidBody reports that an AWS WAFv2 request body would not decode.
//
// ValidationError at 400, not the WAFInvalidParameterException the file's other
// guards answer, because substrate has already drawn this distinction and tested
// it: wafv2_createipset_validation_test.go records, from #755, that an omitted
// required member is ValidationError from WAFv2's Common Error Types ("Check that
// all required parameters are included and that values are valid") while a
// present-but-invalid value is WAFInvalidParameterException, which the operation
// pages list.
//
// A body that will not parse yields no members at all, so it sits on the first
// side of that line. WAFInvalidParameterException is published at 400 and is not
// wrong the way the codes this slice corrected in RAM and CloudTrail were — but
// AWS glosses it "AWS WAF didn't recognize a parameter in the request", all four
// published examples concern a value that was read, and it carries Field,
// Parameter and Reason members substrate cannot truthfully fill for a request
// that never deserialized. (WAFInvalidRequestException does not exist.)
func wafv2InvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "invalid request body",
		HTTPStatus: http.StatusBadRequest,
	}
}

// glueInvalidBody reports that an AWS Glue request body would not decode.
//
// InvalidInputException at 400, "The input provided was not valid.", published on
// every Glue operation page — not the InvalidParameterValueException the plugin's
// member-and-ARN guards answered, which Glue does not publish anywhere: it is
// absent from the Common Errors page, from API_CreateDatabase, API_GetTables and
// API_StartJobRun, and from all thirty-six AWSGlueException subclasses.
//
// #950 deferred those guards and this comment put their number at twenty-six.
// #1063 counted them: there were **four**, and they now call [glueInvalidInput],
// which is where the code and the status are decided for the whole plugin. See
// glue_errors.go for why an ARN that will not parse is not EntityNotFoundException.
func glueInvalidBody() *AWSError {
	return glueInvalidInput("invalid request body")
}

// fsxInvalidBody reports that an Amazon FSx request body would not decode.
//
// BadRequest at 400, "A generic error indicating a failure with a client
// request.", published on the FSx operation pages — not the bare InvalidRequest
// the plugin's one member guard answered, which FSx does not publish: it is absent
// from its Common Errors page, from API_DescribeFileSystems, from
// API_DeleteFileSystem, and from all thirty-five AmazonFSxException subclasses.
// InvalidRequest is an Amazon S3 code, the likely provenance of the mistake.
//
// The stem is what was wrong, not the suffix: FSx's Java class is
// BadRequestException but the wire code carries no suffix, so the file's
// no-suffix instinct was right.
//
// #1063 corrected that one member guard, which is in deleteFileSystem and not in
// describeFileSystems as the issue and this comment's citation both had it; it now
// calls [fsxBadRequest]. See fsx_errors.go for why the difference decides whether
// the guard should exist at all.
func fsxInvalidBody() *AWSError {
	return fsxBadRequest("invalid request body")
}
