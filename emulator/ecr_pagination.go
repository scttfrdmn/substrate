package emulator

import "net/http"

// Amazon ECR's three paginated listings, and the three different shapes their pages publish
// for the same two members (#1090).
//
// DescribeRepositories, DescribeImages and ListImages each publish `maxResults`
// (Valid Range 1–1000, "If this parameter is not used, then … returns up to 100 results")
// and an opaque `nextToken`. Substrate decoded neither on any of the three, so every request
// answered the whole listing in one page and a caller written against the published contract —
// send maxResults, follow nextToken until it is absent — silently got everything at once. That
// is the pagination failure a consumer's loop cannot detect, because one full page is a
// well-formed answer.
//
// The three pages were read one at a time rather than the rule being taken from the first
// (#671), and they do not agree — which is the finding, and the reason this is not one shared
// guard:
//
//   - **API_DescribeRepositories** publishes, on **both** members, "This option cannot be used
//     when you specify repositories with `repositoryNames`."
//   - **API_DescribeImages** publishes, on **both** members, "This option cannot be used when
//     you specify images with `imageIds`."
//   - **API_ListImages** publishes **no exclusion sentence at all**. Its `filter` member is a
//     filter, not an enumeration of members, so a filtered listing still pages.
//
// So the exclusion is a property of the operation, declared at each call site, and only the
// decode and the range are shared.
//
// The cursor is [decodeOffsetPaginationToken]'s base64 offset, so a token substrate never
// issued is refused rather than silently answering page one (#915). AWS documents `nextToken`
// as "an opaque identifier that is only used to retrieve the next items in a list and not for
// other programmatic purposes", so the encoding is substrate's to choose; what it may not do is
// accept a string it could not have produced.
//
// An offset is only meaningful over a stable order, and none of the three listings had one:
// DescribeRepositories walked its names index in insertion order, and both image operations
// built their result by ranging over the tag map, whose iteration order Go randomizes. Two
// identical ListImages calls could therefore answer the same images in a different order, which
// is a determinism defect in its own right — substrate's whole claim is that the same inputs
// answer the same bytes. Each call site now sorts before it cuts, and says on what.

// ecrMaxResultsDefault is the page size all three ECR listings apply when maxResults is absent.
//
// "If this parameter is not used, then DescribeRepositories returns up to 100 results and a
// nextToken value, if applicable" — published verbatim, with the operation's own name
// substituted, on all three pages.
const ecrMaxResultsDefault = 100

// ecrMaxResultsCeiling is the published upper bound on maxResults.
//
// "Valid Range: Minimum value of 1. Maximum value of 1000." on all three pages. A published
// range that is neither enforced nor refused is the defect class this release is named for, so
// substrate refuses rather than clamping: a caller asking for 5000 results has misread the
// contract, and a page of 1000 does not tell them so.
const ecrMaxResultsCeiling = 1000

// ecrInvalidNextToken reports a nextToken substrate could not have issued.
//
// InvalidParameterException/400 is the only parameter-fault code published on any of the three
// pages — there is no ECR equivalent of KMS's InvalidMarkerException — so it is the code for
// both an unissued token and an out-of-range maxResults. The message is substrate's own.
func ecrInvalidNextToken() *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "The nextToken value is not valid.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecrInvalidMaxResults reports a maxResults outside the published 1–1000 range.
func ecrInvalidMaxResults() *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    "The maxResults value must be between 1 and 1000.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecrPageExcluded reports that a pagination member was combined with the member its page says
// it cannot be used with.
//
// The excluded member's name is the argument because the two pages that publish an exclusion
// name different members — `repositoryNames` on DescribeRepositories, `imageIds` on
// DescribeImages — and ListImages publishes none, so no single message serves all three.
func ecrPageExcluded(member, excludedBy string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    member + " cannot be used when you specify " + excludedBy + ".",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ecrPageRequest is the decoded pagination half of a listing request.
type ecrPageRequest struct {
	offset   int
	pageSize int
}

// ecrDecodePage validates maxResults and nextToken and reports the offset and page size to cut
// with.
//
// A zero maxResults is the member being absent, which the pages define as the 100 default; any
// other value outside 1–1000 is refused. The exclusion is not checked here, because the three
// operations exclude different members and one excludes nothing — see the file comment.
func ecrDecodePage(maxResults int, nextToken string) (ecrPageRequest, error) {
	if maxResults < 0 || maxResults > ecrMaxResultsCeiling {
		return ecrPageRequest{}, ecrInvalidMaxResults()
	}
	pageSize := maxResults
	if pageSize == 0 {
		pageSize = ecrMaxResultsDefault
	}
	offset, ok := decodeOffsetPaginationToken(nextToken)
	if !ok {
		return ecrPageRequest{}, ecrInvalidNextToken()
	}
	return ecrPageRequest{offset: offset, pageSize: pageSize}, nil
}
