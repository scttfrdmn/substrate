package emulator

import (
	"net/http"
	"strconv"
)

// This file holds the pagination pair every paginated API Gateway v1 collection publishes, and the
// single reading of it that all of them share.
//
// Seven v1 collections publish "limit" and "position" on their URI, in the same two sentences on
// every one of the seven pages: "The maximum number of returned results per page. The default value
// is 25 and the maximum value is 500." and "The current pagination position in the paged result
// set." GetBasePathMappings was made to read them under #917; the other six read neither and
// answered the whole collection with no cursor, so a paging consumer's loop terminated on its first
// response here and first ran for real against an account holding more elements than one page
// (#1025).
//
// The reading lives in one place rather than six because the pages are identical in every respect
// that decides it: the same default, the same maximum, the same absent minimum, the same
// BadRequestException/400 to refuse with — "the submitted request is not valid, for example, the
// input is incomplete or incorrect" — and no published order for the elements. What differs between
// the collections is only what they list and the order they are listed in, and each handler states
// that for itself.

// apigwDefaultLimit, apigwMaxLimit and apigwMinLimit are the bounds a v1 collection's "limit"
// publishes, which it does inside the parameter's own description rather than on a Valid Range line.
//
// So unlike every EC2 describe, these operations have a published *default*: a request naming no
// limit still pages, at 25. No minimum is published; refusing a limit below one is substrate's
// reading, and it is the bound a paginated listing forces, since a page of zero elements describes a
// walk that answers nothing and hands back a position forever.
const (
	apigwDefaultLimit = 25
	apigwMaxLimit     = 500
	apigwMinLimit     = 1
)

// apigwPageParams reads the "limit" and "position" pair off a v1 collection request and returns the
// number of elements one page may carry together with the offset into the collection to start at.
//
// Both are refused before any state is read, per #887: what a malformed request is answered with
// must not depend on how many elements happen to exist. "limit" is checked first because its refusal
// names a published range, where "position"'s names a value the caller cannot have got from
// substrate; naming the range first tells a caller sending two bad parameters the thing it can fix
// from the page.
//
// A position one past the end of the collection is not refused here — [pageByOffsetToken] clamps it
// to an empty final page, because that is a token substrate did issue over a collection that has
// since shrunk.
func apigwPageParams(req *AWSRequest) (pageSize, offset int, awsErr *AWSError) {
	pageSize, awsErr = apigwPageLimit(req.Params["limit"])
	if awsErr != nil {
		return 0, 0, awsErr
	}

	position := req.Params["position"]
	var tokenOK bool
	offset, tokenOK = decodeOffsetPaginationToken(position)
	if !tokenOK {
		return 0, 0, &AWSError{
			Code:       "BadRequestException",
			Message:    "Invalid position: " + position,
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return pageSize, offset, nil
}

// apigwPageLimit reads a v1 collection's "limit" and returns the number of elements one page may
// carry.
//
// A value outside 1..500 is refused rather than clamped, with the BadRequestException the page
// publishes, because a caller asking for 1000 elements per page asked for something the operation
// cannot do, and silently answering 500 hides it. A non-integer is refused for the same reason. An
// absent limit is the published default of 25.
func apigwPageLimit(raw string) (int, *AWSError) {
	if raw == "" {
		return apigwDefaultLimit, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < apigwMinLimit || n > apigwMaxLimit {
		return 0, &AWSError{
			Code: "BadRequestException",
			Message: "limit must be between " + strconv.Itoa(apigwMinLimit) +
				" and " + strconv.Itoa(apigwMaxLimit),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return n, nil
}
