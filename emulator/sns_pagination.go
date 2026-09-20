package emulator

import "net/http"

// SNS pages three listings by an array offset, and a token it never issued is refused (#1086).
//
// ListTopics, ListSubscriptions and ListSubscriptionsByTopic all decoded their NextToken with the
// pre-#915 idiom — base64.StdEncoding.DecodeString then strconv.Atoi, both errors discarded — so a
// token substrate could not have issued left the offset at zero and the operation answered a
// well-formed **page one**. That is the one wrong answer a paginating caller cannot detect: a loop
// that runs until the token comes back empty is handed the first page again, so it either spins or
// processes the same records twice, and nothing in the response says so. The three now share
// [decodeOffsetPaginationToken], which defines issuability as a round trip through the encoder, and
// [pageByOffsetToken], which was already the cut all three had written out by hand.
//
// # The code is published, the condition is substrate's reading
//
// All three pages publish `InvalidParameter`/400 in their **own** Errors sections, glossed *"Indicates
// that a request parameter does not comply with the associated constraints."* — so the code is this
// operation's own vocabulary and not one borrowed from a sibling, which is what
// [#671]'s scope decision requires. What none of the three publishes is a code AWS attributes to a
// pagination token: each describes NextToken in a single sentence — *"Token returned by the previous
// ListTopics request."* — naming no constraint and no error, and SNS's common-errors page carries
// nothing about a token either. So the *condition* is substrate's reading of that gloss: a token no
// previous call returned is a parameter that does not comply, and the one constraint the page states
// about NextToken is the sentence the message below echoes back.
//
// The message names the parameter and the operation rather than restating the gloss, because a caller
// handed "does not comply with the associated constraints" cannot tell which of its parameters AWS
// means, and NextToken is the only one of the three operations' parameters this refusal can be about.
//
// This is not an expiry refusal. SNS publishes no lifetime for a NextToken and substrate's tokens do
// not expire; what is refused is the other way a token can be one this service will not resume from —
// one it could not have minted. (EventBridge is the case where AWS does publish an expiry code, and
// [ebInvalidToken] records why the two conditions are one observation for a caller.)
//
// # Ordering
//
// The token is decoded before any state is read, which is #887's criterion: a refusal must not depend
// on how much state happens to exist, and a store failure must not be reported as a 500 for a request
// that was already refusable. ListSubscriptionsByTopic is the one exception, and deliberately: it
// resolves the topic first, so the NotFound/404 that #926 added for an absent topic keeps its
// precedence over a token refusal. AWS publishes nothing about which of those two wins; the topic is
// the resource the request addresses, which is the same reading S3's ListObjectsV2 already records for
// its bucket. The token is still decoded before the subscription index is read.
//
// [#671]: https://github.com/scttfrdmn/substrate/issues/671

// snsListPageSize is how many records each of the three listings returns at most.
//
// All three pages say the same thing in their own prose — *"Each call returns a limited list of
// topics, up to 100"* — and none of the three publishes a request parameter that can change it, so
// this is a constant rather than a default a caller can override.
const snsListPageSize = 100

// snsInvalidPaginationToken refuses a NextToken that no previous call to operation returned.
//
// The operation name is in the message because it is in AWS's own description of the member, and
// because a token from a *different* SNS listing is one of the ways a caller arrives here: all three
// encode an offset the same way, so ListTopics' token decodes cleanly and would otherwise index into
// the wrong listing.
func snsInvalidPaginationToken(operation string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameter",
		Message:    "NextToken is not a token returned by a previous " + operation + " request",
		HTTPStatus: http.StatusBadRequest,
	}
}
