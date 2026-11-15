package emulator

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// The three AWS Backup listings read their query parameters through one rule (#1184).
//
// ListBackupVaults, ListBackupPlans and ListBackupSelections each publish `maxResults` with "Valid
// Range: Minimum value of 1. Maximum value of 1000." and a `nextToken`, and each wrote the same
// validation out by hand. They now share [backupListMaxResults], the [pageSizeRange] every JSON list
// in this release uses, and [backupListParams]. A value outside the range, one that is not an integer,
// or a token substrate did not issue is InvalidParameterValueException/400, the one validation code the
// pages publish ("Indicates that something is wrong with a parameter's value. For example, the value is
// out of range."). With no `maxResults` the page size is the published maximum, so a request with no
// pagination parameters answers what it always did.
//
// A boolean query parameter (`shared` on ListBackupVaults, `includeDeleted` on ListBackupPlans) must be
// `true` or `false`, in any case. The pages type each as "A Boolean value" — ListBackupPlans spells the
// values `TRUE` and `FALSE` in its prose — and a restJson1 query boolean is serialized as one of those
// two words. strconv.ParseBool also accepts `1`, `0`, `t` and `f`, which is Go's reading rather than
// the protocol's, so those are refused rather than guessed at.
//
// Each handler cuts the page from its sorted ID index **before** loading records, so a listing loads
// only the records it returns. An index entry whose record is absent is passed over, which can leave a
// page shorter than `maxResults` while a token is still emitted; the offset counts index entries, so the
// walk stays coherent.

// backupListMaxResults is the maxResults constraint all three Backup listings publish.
var backupListMaxResults = pageSizeRange{member: "maxResults", min: 1, max: 1000, def: 1000}

// backupInvalidParameterValue is the refusal the Backup list pages publish for a bad parameter value.
func backupInvalidParameterValue(msg string) error {
	return &AWSError{Code: "InvalidParameterValueException", Message: msg, HTTPStatus: http.StatusBadRequest}
}

// backupListParams resolves a listing's maxResults and nextToken from the query string.
func backupListParams(params map[string]string) (size, offset int, err error) {
	var requested *int
	if raw, ok := params["maxResults"]; ok {
		n, convErr := strconv.Atoi(raw)
		if convErr != nil {
			return 0, 0, backupInvalidParameterValue(backupMaxResultsMessage())
		}
		requested = &n
	}
	size, ok := backupListMaxResults.resolve(requested)
	if !ok {
		return 0, 0, backupInvalidParameterValue(backupMaxResultsMessage())
	}
	offset, ok = decodeOffsetPaginationToken(params["nextToken"])
	if !ok {
		return 0, 0, backupInvalidParameterValue("The nextToken is not valid.")
	}
	return size, offset, nil
}

// backupMaxResultsMessage is the message an unusable maxResults answers; it is substrate's own, and the
// one the three listings already answered.
func backupMaxResultsMessage() string {
	return fmt.Sprintf("maxResults must be an integer from %d to %d.", backupListMaxResults.min, backupListMaxResults.max)
}

// backupQueryBool reads an optional boolean query parameter, which must be `true` or `false`.
func backupQueryBool(params map[string]string, name string) (value, present bool, err error) {
	raw, ok := params[name]
	if !ok {
		return false, false, nil
	}
	switch {
	case strings.EqualFold(raw, "true"):
		return true, true, nil
	case strings.EqualFold(raw, "false"):
		return false, true, nil
	default:
		return false, true, backupInvalidParameterValue(name + " must be true or false.")
	}
}
