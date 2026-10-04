package emulator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

// Timestream pagination, the issued-query record, and CancelQuery (#1195, #1197, #1206).
//
// # The two lists
//
// ListDatabases and ListTables publish `MaxResults` ("Valid Range: Minimum value of 1. Maximum value
// of 20.") and a `NextToken` on both request and response, "returned when the response is truncated".
// Both are read now. A page size outside 1–20 is ValidationException/400, the code both pages publish
// for "An invalid or malformed request". Neither page publishes a dedicated bad-token code, so a token
// substrate did not issue is the same ValidationException rather than a code borrowed from another
// service (#671). An absent MaxResults answers the whole listing, as before: neither page states a
// default page size, and choosing one would silently truncate a caller that never paginated. The token
// is [encodeOffsetPaginationToken]'s, over names in lexicographic order, and is omitted on the last
// page.
//
// # Query's MaxRows and NextToken
//
// API_query_Query publishes `MaxRows` (1–1000) and `NextToken` (1–2048 characters), and says what a
// first call with MaxRows does: it "will return the result set of the query in two cases: The size of
// the result is less than 1MB. The number of rows in the result set is less than the value of maxRows.
// Otherwise, the initial invocation of Query only returns a NextToken". Substrate models that sentence
// literally on the row count. A first call whose result has at least MaxRows rows answers no rows,
// its ColumnInfo, and a NextToken; each call with the token answers the next MaxRows rows; the last
// page carries no token. The 1 MB limit is not modeled, because substrate's rows are not sized like
// Timestream's. Without MaxRows the whole result is one page, as before.
//
// Every page answers the same QueryId, and a token resumes the result **as it was when the query
// ran**: the result is snapshotted into the issued-query record, so a WriteRecords between pages does
// not shift what the next page holds. A token is refused, with ValidationException "Invalid pagination
// token" (the error the page names for a token the reader may not use), when substrate did not issue
// it, when it names a query this account and Region did not run, or when the request's QueryString is
// not the query's. The page's five-invocation and one-hour token lifetimes are not modeled; a token is
// reusable and answers the same page each time, which the page also states ("Using the same NextToken
// will return the same set of records").
//
// `ClientToken` is read for its published 32–128 length only. Its idempotency window is not modeled:
// substrate's Query is deterministic, so a repeated query already answers the same result.
//
// # CancelQuery
//
// The page publishes `QueryId` "Required: Yes", 1–64 characters matching `[a-zA-Z0-9]+`, and a
// response of `{"CancellationMessage": "string"}`. Its Errors list is AccessDeniedException,
// InternalServerException, InvalidEndpointException, ThrottlingException and ValidationException; it
// publishes **no** ResourceNotFoundException, so a QueryId this account and Region never issued is
// ValidationException, not the not-found code #1206's criterion names, which would be borrowed from
// another page.
//
// "Cancellation is provided only if the query has not completed running", and "subsequent
// cancellation requests will return a CancellationMessage, indicating that the query has already been
// canceled". Substrate's queries complete synchronously, so the one query that has "not completed" is
// a paginated one with pages still unread. A cancellation marks it cancelled, and its next page is
// ConflictException/400, "Unable to poll results for a cancelled query", which API_query_Query
// publishes. The two message values are the ones #1206 quotes; neither API_query_CancelQuery nor the
// boto3 reference prints them, so they are recorded here as substrate's reading:
//
//   - a query with pages unread, on its first cancellation: "Query cancelled successfully";
//   - a query already cancelled, or one that completed: "Cancellation message is posted".

// Published bounds, from the operation pages named above.
const (
	timestreamListMaxResults = 20
	timestreamQueryMaxRows   = 1000
	timestreamTokenMaxLen    = 2048
	timestreamQueryStringMax = 262144
	timestreamQueryIDMax     = 64
	timestreamRecordsMax     = 100

	timestreamCancelledNow   = "Query cancelled successfully"
	timestreamCancelledAgain = "Cancellation message is posted"
)

// timestreamQueryIDPattern is API_query_CancelQuery's published QueryId pattern.
var timestreamQueryIDPattern = regexp.MustCompile(`^[a-zA-Z0-9]+$`)

// timestreamValidation is the ValidationException/400 every Timestream page publishes for "An invalid
// or malformed request".
func timestreamValidation(msg string) *AWSError {
	return &AWSError{Code: "ValidationException", Message: msg, HTTPStatus: http.StatusBadRequest}
}

// timestreamListPage decodes a list operation's MaxResults and NextToken, refusing a page size outside
// the published 1–20 and a token substrate did not issue. A nil maxResults means no limit.
func timestreamListPage(maxResults *int, nextToken string) (offset, pageSize int, err error) {
	if maxResults != nil && (*maxResults < 1 || *maxResults > timestreamListMaxResults) {
		return 0, 0, timestreamValidation(fmt.Sprintf("MaxResults must be between 1 and %d, got %d", timestreamListMaxResults, *maxResults))
	}
	offset, ok := decodeOffsetPaginationToken(nextToken)
	if !ok {
		return 0, 0, timestreamValidation("The NextToken is not one this operation issued")
	}
	if maxResults != nil {
		pageSize = *maxResults
	}
	return offset, pageSize, nil
}

// timestreamIssuedQuery is the record of one Query this account and Region ran, which CancelQuery and
// a NextToken read back. Rows holds the snapshotted result only while pages remain unread.
type timestreamIssuedQuery struct {
	QueryID     string                 `json:"QueryId"`
	QueryString string                 `json:"QueryString"`
	ColumnInfo  []TimestreamColumnInfo `json:"ColumnInfo,omitempty"`
	Rows        []TimestreamRow        `json:"Rows,omitempty"`
	Status      *TimestreamQueryStatus `json:"QueryStatus,omitempty"`
	PageSize    int                    `json:"PageSize,omitempty"`
	Completed   bool                   `json:"Completed"`
	Cancelled   bool                   `json:"Cancelled"`
}

func timestreamQueryKey(acct, region, id string) string {
	return fmt.Sprintf("query:%s/%s/%s", acct, region, id)
}

func (p *TimestreamPlugin) loadIssuedQuery(reqCtx *RequestContext, id string) (*timestreamIssuedQuery, error) {
	raw, err := p.state.Get(context.Background(), timestreamNamespace, timestreamQueryKey(reqCtx.AccountID, reqCtx.Region, id))
	if err != nil {
		return nil, fmt.Errorf("timestream load query: %w", err)
	}
	if raw == nil {
		return nil, nil
	}
	var q timestreamIssuedQuery
	if err := json.Unmarshal(raw, &q); err != nil {
		return nil, fmt.Errorf("timestream decode query: %w", err)
	}
	return &q, nil
}

func (p *TimestreamPlugin) saveIssuedQuery(reqCtx *RequestContext, q *timestreamIssuedQuery) error {
	data, err := json.Marshal(q)
	if err != nil {
		return fmt.Errorf("timestream marshal query: %w", err)
	}
	if err := p.state.Put(context.Background(), timestreamNamespace, timestreamQueryKey(reqCtx.AccountID, reqCtx.Region, q.QueryID), data); err != nil {
		return fmt.Errorf("timestream put query: %w", err)
	}
	return nil
}

// encodeTimestreamQueryToken renders the token that resumes query id at row offset.
func encodeTimestreamQueryToken(id string, offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(id + ":" + strconv.Itoa(offset)))
}

// decodeTimestreamQueryToken reverses [encodeTimestreamQueryToken], accepting only what it would emit.
func decodeTimestreamQueryToken(token string) (id string, offset int, ok bool) {
	raw, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", 0, false
	}
	id, n, found := strings.Cut(string(raw), ":")
	if !found || !timestreamQueryIDPattern.MatchString(id) {
		return "", 0, false
	}
	offset, err = strconv.Atoi(n)
	if err != nil || offset < 0 || strconv.Itoa(offset) != n {
		return "", 0, false
	}
	return id, offset, true
}

// timestreamQueryInput is Query's request.
type timestreamQueryInput struct {
	QueryString string `json:"QueryString"`
	MaxRows     *int   `json:"MaxRows"`
	NextToken   string `json:"NextToken"`
	ClientToken string `json:"ClientToken"`
}

// validate checks the members API_query_Query constrains.
func (in timestreamQueryInput) validate() error {
	switch {
	case in.QueryString == "":
		return timestreamValidation("QueryString is required")
	case len(in.QueryString) > timestreamQueryStringMax:
		return timestreamValidation(fmt.Sprintf("QueryString must be at most %d characters", timestreamQueryStringMax))
	case in.MaxRows != nil && (*in.MaxRows < 1 || *in.MaxRows > timestreamQueryMaxRows):
		return timestreamValidation(fmt.Sprintf("MaxRows must be between 1 and %d, got %d", timestreamQueryMaxRows, *in.MaxRows))
	case len(in.NextToken) > timestreamTokenMaxLen:
		return timestreamValidation(fmt.Sprintf("NextToken must be at most %d characters", timestreamTokenMaxLen))
	case in.ClientToken != "" && (len(in.ClientToken) < 32 || len(in.ClientToken) > 128):
		return timestreamValidation("ClientToken must be between 32 and 128 characters")
	}
	return nil
}

// queryPage answers one page of an issued query from offset, advancing the record. pageSize of zero
// answers the remainder.
func (p *TimestreamPlugin) queryPage(reqCtx *RequestContext, q *timestreamIssuedQuery, offset int) (*AWSResponse, error) {
	if offset > len(q.Rows) {
		offset = len(q.Rows)
	}
	rows := q.Rows[offset:]
	next := ""
	if q.PageSize > 0 && len(rows) > q.PageSize {
		rows = rows[:q.PageSize]
		next = encodeTimestreamQueryToken(q.QueryID, offset+q.PageSize)
	}
	page := append([]TimestreamRow{}, rows...)
	cols := q.ColumnInfo
	if next == "" {
		// The last page is read: the query has completed, and the snapshot is no longer needed.
		q.Completed, q.Rows, q.ColumnInfo = true, nil, nil
	}
	return p.queryResponse(reqCtx, q, page, cols, next)
}

// queryResponse writes q back and answers a page.
func (p *TimestreamPlugin) queryResponse(reqCtx *RequestContext, q *timestreamIssuedQuery, rows []TimestreamRow, cols []TimestreamColumnInfo, next string) (*AWSResponse, error) {
	if err := p.saveIssuedQuery(reqCtx, q); err != nil {
		return nil, err
	}
	if cols == nil {
		cols = []TimestreamColumnInfo{}
	}
	status := q.Status
	if status == nil {
		derived, err := timestreamQueryStatus(rows)
		if err != nil {
			return nil, err
		}
		status = &derived
	}
	body := map[string]any{
		"QueryId":     q.QueryID,
		"Rows":        rows,
		"ColumnInfo":  cols,
		"QueryStatus": status,
	}
	if next != "" {
		body["NextToken"] = next
	}
	return timestreamJSONResponse(http.StatusOK, body)
}

// cancelQuery handles CancelQuery; see the file comment.
func (p *TimestreamPlugin) cancelQuery(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		QueryID string `json:"QueryId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, timestreamInvalidBody()
		}
	}
	switch {
	case input.QueryID == "":
		return nil, timestreamValidation("QueryId is required")
	case len(input.QueryID) > timestreamQueryIDMax || !timestreamQueryIDPattern.MatchString(input.QueryID):
		return nil, timestreamValidation("QueryId must be 1 to 64 characters matching [a-zA-Z0-9]+")
	}
	q, err := p.loadIssuedQuery(reqCtx, input.QueryID)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, timestreamValidation("QueryId " + input.QueryID + " names no query this account ran in this Region")
	}
	message := timestreamCancelledAgain
	if !q.Completed && !q.Cancelled {
		q.Cancelled, q.Rows, q.ColumnInfo = true, nil, nil
		if err := p.saveIssuedQuery(reqCtx, q); err != nil {
			return nil, err
		}
		message = timestreamCancelledNow
	}
	return timestreamJSONResponse(http.StatusOK, map[string]any{"CancellationMessage": message})
}
