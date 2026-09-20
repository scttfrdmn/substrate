package emulator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// Batch's ListJobs reads its request (#1236).
//
// The handler took its request as `_ *AWSRequest`, so **not one of the seven published request
// members was read**: `jobQueue`, `jobStatus`, `arrayJobId`, `multiNodeJobId`, `filters`,
// `maxResults` and `nextToken`. It answered every job in the account and Region, in insertion
// order, with no cursor. Three of those omissions are each worse than the pagination one the
// operation was originally filed under:
//
//   - **The published default is `RUNNING` only.** `jobStatus` says *"If you don't specify a
//     status, only `RUNNING` jobs are returned."* Substrate answered every status alike, so a
//     `SUCCEEDED` job appeared in a list the caller reads as "still running" — the default path,
//     with no parameter to blame it on, and both of the page's own examples take it.
//   - **`jobQueue` selects a queue.** A request naming one queue was answered with every queue's
//     jobs, which reads as a passing assertion about the wrong queue.
//   - **`filters` carries a published sort order.** *"The results are sorted by the `createdAt`
//     field, with the most recent jobs being first."*
//
// # What the page publishes, and what is substrate's reading
//
// Quoted from [API_ListJobs], read 2026-09-19 and re-read 2026-09-20:
//
//   - *"You must specify only one of the following items: A job queue ID … A multi-node parallel
//     job ID … An array job ID"* — all three are `Required: No` individually, so the rule lives in
//     that prose rather than in a member's flag. More than one is refused. **None** of the three is
//     still the account-wide listing substrate has always answered, because the sentence forbids
//     naming two, not naming none, and the page publishes no error for an empty request. Requiring
//     exactly one would be substrate's reading, not the page's.
//   - *"If the `filters` parameter is specified, the `jobStatus` parameter is ignored and jobs with
//     any status are returned. The exception is the `SHARE_IDENTIFIER` filter and `jobStatus` can
//     be used together."* — so a filter other than `SHARE_IDENTIFIER` switches status selection
//     off entirely, rather than leaving the `RUNNING` default in place.
//   - *"Only one filter can be used at a time"* — more than one is refused.
//   - `jobStatus` publishes `Valid Values: SUBMITTED | PENDING | RUNNABLE | STARTING | RUNNING |
//     SUCCEEDED | FAILED`. A value outside that list is refused. The list is the page's own; that
//     a value outside it is one of the *"identifier[s] that's not valid"* the `ClientException`
//     gloss covers is substrate's reading, on the same footing #1086 used for the token refusal —
//     the page publishes the constraint and no error for violating it.
//   - `maxResults` publishes *"The minimum value is 1"* and three caps: 1000 with `--job-status`,
//     100 with `--filters`, 1000 with neither. Substrate **clamps** to the applicable cap and
//     applies it to an absent, zero or negative value as well, which is the same reading
//     [batchPage] records for the three describes: the page publishes a ceiling and no refusal for
//     exceeding it.
//   - `nextToken` publishes the describes' sentence verbatim — *"The `nextToken` value returned
//     from a previous paginated `ListJobs` request where `maxResults` was used"* — so this is the
//     fourth Batch paginator and [batchDecodeNextToken] refuses a token no previous `ListJobs`
//     returned rather than answering page one. See batch_pagination.go for that argument in full.
//   - Errors are exactly `ClientException`/400 and `ServerException`/500, and Batch publishes no
//     common-errors page, so [batchClientError] is the whole refusal vocabulary and nothing is
//     borrowed from a sibling operation (#671).
//
// # Filters, and what a substrate record can match
//
// All five published names are implemented, each by its own published rule: `JOB_NAME` is a
// case-insensitive match, or a case-insensitive prefix when the value ends in `*`; `JOB_DEFINITION`
// is case-sensitive, matches every revision when given a bare name, supports the same `*` suffix on
// a name but *"Asterisk (*) isn't supported when the ARN is used"*; `BEFORE_CREATED_AT` and
// `AFTER_CREATED_AT` take *"a string representation of the number of milliseconds since 00:00:00
// UTC (midnight) on January 1, 1970"*, and a value that is not one is refused rather than silently
// matching nothing.
//
// `SHARE_IDENTIFIER` is implemented and **matches nothing**, because `SubmitJob` records no share
// identifier — [BatchJob] has no such member and the fairshare scheduling it comes from is a
// scheduler internal rather than an API observation. That is a real empty answer rather than an
// ignored filter: a caller filtering on a share identifier is told that no recorded job carries
// one.
//
// Two published properties of the filter path are *not* modeled, and are recorded here rather than
// left unstated. *"When the `JOB_NAME` filter is used, the results are grouped by the job name and
// version"* — substrate returns the matching jobs in the published `createdAt`-descending order
// without grouping, because a group is a property of the listing's shape rather than of any one
// job, and the page does not say what the grouping does to the order it publishes in the same
// paragraph. And *"The filter doesn't apply to child jobs in an array or multi-node parallel (MNP)
// jobs"* is vacuous here, for the reason in the next paragraph.
//
// # Array children and MNP nodes are an empty list, not a listing
//
// `arrayJobId` and `multiNodeJobId` are read, checked against the exclusivity rule, and then
// answered with an **empty** `jobSummaryList`. `SubmitJob` records neither `arrayProperties` nor
// `nodeProperties`, so substrate mints no child jobs and no nodes: there is nothing for either
// listing to contain. Answering the account-wide listing instead — which is what ignoring the
// member did — reports a parent's children as though they existed, which is the worse of the two
// wrong answers because it is not empty.
//
// The `createdAt`-descending sort is applied on the filter path only, which is where the page
// publishes it. The unfiltered path keeps insertion order, because the page states no order for it
// and inventing one would be substrate's reading of an unpublished property.
//
// # The default listing is empty in substrate today, and that is a different bug
//
// `SubmitJob` records a job `SUCCEEDED` at submission, so no substrate job is ever `RUNNING` and
// the published default listing is **always empty**. Implementing the default faithfully is what
// makes that visible; it is tracked as [#1248] rather than papered over here by keeping the
// every-status listing, because a listing that answers a question AWS would not have answered is
// not a substitute for a job that reaches `RUNNING`.
//
// [API_ListJobs]: https://docs.aws.amazon.com/batch/latest/APIReference/API_ListJobs.html
// [#1248]: https://github.com/scttfrdmn/substrate/issues/1248

// batchKeyValuesPair is the published KeyValuesPair a ListJobs filter arrives as.
type batchKeyValuesPair struct {
	// Name is one of the five published filter names.
	Name string `json:"name"`

	// Values are the values to match; a job matching any of them is kept.
	Values []string `json:"values"`
}

// batchListJobsRequest is ListJobs' published request body, all seven members.
type batchListJobsRequest struct {
	ArrayJobID     string               `json:"arrayJobId"`
	Filters        []batchKeyValuesPair `json:"filters"`
	JobQueue       string               `json:"jobQueue"`
	JobStatus      string               `json:"jobStatus"`
	MaxResults     int                  `json:"maxResults"`
	MultiNodeJobID string               `json:"multiNodeJobId"`
	NextToken      string               `json:"nextToken"`
}

// batchJobSummary is the JobSummary shape ListJobs reports.
//
// `JobSummary` publishes eighteen members. These seven are the ones substrate has a record behind:
// `jobArn` is derived from the job ID the way `SubmitJob` derives the one it returns, and the other
// six are members of [BatchJob]. The eleven declined are `arrayProperties`, `capacityUsage`,
// `container`, `isCancelled`, `isTerminated`, `nodeProperties`, `scheduledAt`, `shareIdentifier`,
// `startedAt`, `stoppedAt` and `statusSummary` — each of which would have to be invented, and most
// of which describe the workload running inside the job rather than an API observation of it. A
// half-filled member is worse than an absent one, because a caller cannot tell a zero
// `container.exitCode` from a job that exited 0.
//
// The page's own two sample responses emit only `jobId` and `jobName`, so a reader of the examples
// alone would conclude nothing was missing; the Response Elements section is the authority.
type batchJobSummary struct {
	JobARN        string `json:"jobArn"`
	JobID         string `json:"jobId"`
	JobName       string `json:"jobName"`
	JobDefinition string `json:"jobDefinition,omitempty"`
	CreatedAt     int64  `json:"createdAt,omitempty"`
	Status        string `json:"status"`
	StatusReason  string `json:"statusReason,omitempty"`
}

// listJobs reports the jobs matching the request, refusing every violation of the page's published
// rules before it reads any state.
//
// The refusal order is the one the three describes use (#887): an unreadable body first, because a
// request whose JSON does not parse cannot have a member read out of it, then each request-only
// rule, then the token. A refusal must not depend on how many jobs happen to exist.
func (p *BatchPlugin) listJobs(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	body, awsErr := batchDecodeListJobsRequest(req.Body)
	if awsErr != nil {
		return nil, awsErr
	}

	named := 0
	for _, id := range []string{body.JobQueue, body.ArrayJobID, body.MultiNodeJobID} {
		if id != "" {
			named++
		}
	}
	if named > 1 {
		return nil, batchClientError("you must specify only one of jobQueue, arrayJobId or multiNodeJobId")
	}

	if body.JobStatus != "" && !batchValidJobStatus(body.JobStatus) {
		return nil, batchClientError("jobStatus " + body.JobStatus +
			" is not one of SUBMITTED, PENDING, RUNNABLE, STARTING, RUNNING, SUCCEEDED or FAILED")
	}

	filter, awsErr := batchListJobsFilter(body.Filters)
	if awsErr != nil {
		return nil, awsErr
	}

	offset := 0
	if body.NextToken != "" {
		if offset, awsErr = batchDecodeNextToken("ListJobs", body.NextToken); awsErr != nil {
			return nil, awsErr
		}
	}

	// A child or node listing over a record that has neither: empty, and empty before the index is
	// read, because no state can change the answer.
	if body.ArrayJobID != "" || body.MultiNodeJobID != "" {
		return batchJSONResponse(http.StatusOK, map[string]interface{}{
			"jobSummaryList": []batchJobSummary{},
		})
	}

	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, batchNamespace, "job_ids:"+ctx.AccountID+"/"+ctx.Region)
	if err != nil {
		return nil, fmt.Errorf("listJobs load job index: %w", err)
	}

	summaries := make([]batchJobSummary, 0, len(ids))
	for _, id := range ids {
		data, err := p.state.Get(goCtx, batchNamespace, "job:"+ctx.AccountID+"/"+ctx.Region+"/"+id)
		if err != nil {
			return nil, fmt.Errorf("listJobs get job %s: %w", id, err)
		}
		if data == nil {
			continue
		}
		var job BatchJob
		if err := json.Unmarshal(data, &job); err != nil {
			return nil, fmt.Errorf("listJobs unmarshal job %s: %w", id, err)
		}
		if !batchListJobsMatches(job, body, filter) {
			continue
		}
		summaries = append(summaries, batchJobSummary{
			JobARN:        batchARN(ctx, "job", job.JobID),
			JobID:         job.JobID,
			JobName:       job.JobName,
			JobDefinition: job.JobDefinition,
			CreatedAt:     job.CreatedAt,
			Status:        job.Status,
			StatusReason:  job.StatusReason,
		})
	}

	if filter != nil {
		// "The results are sorted by the createdAt field, with the most recent jobs being first."
		// Stable, so two jobs the simulated clock stamped in the same millisecond keep insertion
		// order rather than an order that depends on the sort's internals.
		sort.SliceStable(summaries, func(i, j int) bool {
			return summaries[i].CreatedAt > summaries[j].CreatedAt
		})
	}

	page, nextToken := pageByOffsetToken(summaries, offset, batchListJobsPageSize(body.MaxResults, filter != nil))
	result := map[string]interface{}{"jobSummaryList": page}
	if nextToken != "" {
		// "This value is null when there are no more results to return", and a caller's paginator
		// stops on its absence.
		result["nextToken"] = nextToken
	}
	return batchJSONResponse(http.StatusOK, result)
}

// batchDecodeListJobsRequest reads the request body, refusing one that is not JSON.
//
// An **empty** body is not an unreadable one: every member is `Required: No`, so a request that
// sends none of them is the whole listing, and the legacy `GET /v1/jobs` route carries no body at
// all. Refusing it would refuse the request the page's absent-parameter prose describes.
func batchDecodeListJobsRequest(body []byte) (*batchListJobsRequest, *AWSError) {
	var out batchListJobsRequest
	if len(bytes.TrimSpace(body)) == 0 {
		return &out, nil
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, batchClientError("the request body is not valid JSON")
	}
	return &out, nil
}

// batchValidJobStatus reports whether status is one of jobStatus' published Valid Values.
func batchValidJobStatus(status string) bool {
	switch status {
	case "SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "SUCCEEDED", "FAILED":
		return true
	}
	return false
}

// batchListJobsFilter resolves the filters member to the single filter the page allows, refusing
// more than one, an unpublished name, and a created-at bound that is not a number of milliseconds.
//
// The bound is validated here rather than where it is matched, so a request carrying a value the
// page does not describe is refused rather than answered with the empty list an unparsable bound
// would otherwise produce — the false negative this whole class of work exists to remove.
func batchListJobsFilter(filters []batchKeyValuesPair) (*batchKeyValuesPair, *AWSError) {
	if len(filters) == 0 {
		return nil, nil
	}
	if len(filters) > 1 {
		return nil, batchClientError("only one filter can be used at a time")
	}
	filter := filters[0]
	switch filter.Name {
	case "JOB_NAME", "JOB_DEFINITION", "SHARE_IDENTIFIER":
	case "BEFORE_CREATED_AT", "AFTER_CREATED_AT":
		for _, value := range filter.Values {
			if _, err := strconv.ParseInt(value, 10, 64); err != nil {
				return nil, batchClientError(filter.Name + " takes the number of milliseconds since" +
					" 00:00:00 UTC on January 1, 1970, and " + value + " is not one")
			}
		}
	default:
		return nil, batchClientError("filter name " + filter.Name + " is not one of JOB_NAME," +
			" JOB_DEFINITION, BEFORE_CREATED_AT, AFTER_CREATED_AT or SHARE_IDENTIFIER")
	}
	return &filter, nil
}

// batchListJobsPageSize is the number of summaries one page carries.
//
// The page publishes 100 with `--filters` and 1000 otherwise, and a minimum of 1, but no error for
// a value outside that — so the cap is applied by clamping, and an absent, zero or negative value
// takes it too. That is substrate's reading, and the same one [batchPage] records for the three
// describes.
func batchListJobsPageSize(maxResults int, usesFilter bool) int {
	limit := 1000
	if usesFilter {
		limit = 100
	}
	if maxResults < 1 || maxResults > limit {
		return limit
	}
	return maxResults
}

// batchListJobsMatches reports whether one job belongs in the listing the request describes.
//
// Status selection is skipped when a filter other than `SHARE_IDENTIFIER` is used, because the page
// says so: *"If the `filters` parameter is specified, the `jobStatus` parameter is ignored and jobs
// with any status are returned."* Otherwise an absent `jobStatus` is `RUNNING`.
func batchListJobsMatches(job BatchJob, body *batchListJobsRequest, filter *batchKeyValuesPair) bool {
	if body.JobQueue != "" && batchQueueName(job.JobQueue) != batchQueueName(body.JobQueue) {
		return false
	}

	if filter == nil || filter.Name == "SHARE_IDENTIFIER" {
		wanted := body.JobStatus
		if wanted == "" {
			wanted = "RUNNING"
		}
		if job.Status != wanted {
			return false
		}
	}

	return filter == nil || batchFilterMatchesJob(job, *filter)
}

// batchQueueName reduces a job queue name or full ARN to the name, so that a request naming the
// queue reaches a job submitted to the ARN and the other way round.
//
// `jobQueue` is documented as *"The name or full Amazon Resource Name (ARN) of the job queue"*, and
// `SubmitJob` stores whichever form its caller sent, so the two have to be compared on the part
// they share. Queue names are matched case-sensitively, as AWS resource names are.
func batchQueueName(queue string) string {
	if !strings.HasPrefix(queue, "arn:") {
		return queue
	}
	if slash := strings.LastIndex(queue, "/"); slash >= 0 {
		return queue[slash+1:]
	}
	return queue
}

// batchFilterMatchesJob applies the one filter to one job, keeping it if any value matches.
func batchFilterMatchesJob(job BatchJob, filter batchKeyValuesPair) bool {
	for _, value := range filter.Values {
		switch filter.Name {
		case "JOB_NAME":
			// "The value of the filter is a case-insensitive match for the job name. If the value
			// ends with an asterisk (*), the filter matches any job name that begins with the
			// string before the '*'."
			if batchWildcardMatch(job.JobName, value, false) {
				return true
			}
		case "JOB_DEFINITION":
			if batchJobDefinitionMatches(job.JobDefinition, value) {
				return true
			}
		case "BEFORE_CREATED_AT":
			if ms, err := strconv.ParseInt(value, 10, 64); err == nil && job.CreatedAt <= ms {
				return true
			}
		case "AFTER_CREATED_AT":
			if ms, err := strconv.ParseInt(value, 10, 64); err == nil && job.CreatedAt >= ms {
				return true
			}
		case "SHARE_IDENTIFIER":
			// No recorded job carries a share identifier, so nothing matches. See the file comment.
		}
	}
	return false
}

// batchJobDefinitionMatches applies the JOB_DEFINITION rules to one job's definition.
//
// The value is *"the name or Amazon Resource Name (ARN) of the job definition … The value is case
// sensitive."* A bare name matches *"all the jobs that used any revision of that job definition
// name"*, so the revision is stripped from both sides before comparing; an ARN *"include[s] jobs
// that used the specified revision"*, so it is compared whole and *"Asterisk (*) isn't supported
// when the ARN is used"*.
//
// A `SubmitJob` that recorded a bare name is therefore not reached by an ARN filter, and one that
// recorded an ARN is reached by a name filter, because the name is the part they share. That
// asymmetry is the page's, not substrate's: it is what comparing an ARN whole means.
func batchJobDefinitionMatches(definition, value string) bool {
	if strings.HasPrefix(value, "arn:") {
		return definition == value
	}
	return batchWildcardMatch(batchJobDefinitionName(definition), value, true)
}

// batchJobDefinitionName reduces `${name}:${revision}` or a full ARN to the definition name.
func batchJobDefinitionName(definition string) string {
	if strings.HasPrefix(definition, "arn:") {
		if slash := strings.LastIndex(definition, "/"); slash >= 0 {
			definition = definition[slash+1:]
		}
	}
	if colon := strings.LastIndex(definition, ":"); colon >= 0 {
		return definition[:colon]
	}
	return definition
}

// batchWildcardMatch matches a filter value against a job's value, honoring the trailing-asterisk
// prefix form both JOB_NAME and JOB_DEFINITION publish.
//
// Only a trailing asterisk is a wildcard, because that is the only form either member documents:
// *"If the value ends with an asterisk (*), the filter matches any job name that begins with the
// string before the '*'."* An asterisk anywhere else is an ordinary character.
func batchWildcardMatch(value, pattern string, caseSensitive bool) bool {
	if !caseSensitive {
		value = strings.ToLower(value)
		pattern = strings.ToLower(pattern)
	}
	if prefix, found := strings.CutSuffix(pattern, "*"); found {
		return strings.HasPrefix(value, prefix)
	}
	return value == pattern
}
