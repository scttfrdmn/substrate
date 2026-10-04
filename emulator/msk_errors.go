package emulator

import "net/http"

// MSK's refusals, and why every code string here is substrate's reading rather than a citation.
//
// MSK's operation pages publish a status table and one error model, Error, whose members are
// {message, invalidParameter} — **no code member**, and no code string on any page (#950, #1198,
// #1211). The pages do name the error *shapes* their statuses map to in the service model —
// BadRequestException (400), NotFoundException (404), ConflictException (409), and so on — and a
// shape name is what an SDK matches an error on: aws-sdk-go-v2's REST-JSON deserializer reads the
// x-amzn-ErrorType header and maps BadRequestException to types.BadRequestException. So the shape
// name is the closest published string, and it is what each constructor below answers. Until #1211
// substrate answered "BadRequest", which no shape is called, so a typed SDK fell through to a
// generic error.
//
// # invalidParameter
//
// The Error model's gloss is "The parameter that caused the error." It is the only machine-readable
// field an MSK error carries, so every refusal that has an offending member names it, in the
// published lowerCamel spelling of the request member or query/path parameter: clusterName,
// kafkaVersion, numberOfBrokerNodes, brokerNodeGroupInfo, clientSubnets, instanceType, provisioned,
// serverless, vpcConfigs, clientAuthentication, maxResults, nextToken, clusterTypeFilter,
// clusterArn. A body that does not decode has no member to blame, so [mskInvalidBody] leaves it
// empty; the member is then omitted rather than sent as "". The value travels in
// [AWSError.Members], which the REST-JSON error writer renders beside message.
//
// HTTP statuses are not in doubt: each is the one the operation's own status table gives the case.

// mskInvalidBody reports that a request body would not decode. No member caused it, so
// invalidParameter is omitted.
func mskInvalidBody() *AWSError {
	return mskBadRequest("", "the request body is not valid JSON")
}

// mskBadRequest reports that a request cannot be used as given: BadRequestException/400, the shape
// every MSK page maps 400 to. member is the request member that caused it, reported as
// invalidParameter; empty when no single member did.
func mskBadRequest(member, message string) *AWSError {
	return mskError("BadRequestException", http.StatusBadRequest, member, message)
}

// mskNotFound reports that the resource a request names does not exist: NotFoundException/404.
func mskNotFound(member, message string) *AWSError {
	return mskError("NotFoundException", http.StatusNotFound, member, message)
}

// mskConflict reports that a create names a cluster that already exists: ConflictException/409, the
// status both CreateCluster's and CreateClusterV2's tables give "This cluster name already exists".
func mskConflict(member, message string) *AWSError {
	return mskError("ConflictException", http.StatusConflict, member, message)
}

// mskError builds one MSK refusal. One constructor for every site, so the code, the status and the
// invalidParameter rendering cannot drift between operations.
func mskError(code string, status int, member, message string) *AWSError {
	e := &AWSError{Code: code, Message: message, HTTPStatus: status}
	if member != "" {
		e.Members = map[string]string{"invalidParameter": member}
	}
	return e
}
