package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// Batch's ListJobs reads its request — #1236.
//
// The handler took its request as `_ *AWSRequest`, so every one of the seven published members was
// dropped: the published `RUNNING` default, the queue selector, the exclusivity rule, all five
// filters and both cursor members. Each case here asserts one of those against
// [API_ListJobs](https://docs.aws.amazon.com/batch/latest/APIReference/API_ListJobs.html).
//
// Requests go to `POST /v1/listjobs`, the published path, which the operation did not have a route
// for: its members live in a body and the legacy `GET /v1/jobs` route carries none.

// batchListJobsClock is the simulated instant the jobs in these tests are stamped from. Fixed, so a
// created-at bound is a number the test chose rather than one the wall clock supplied.
var batchListJobsClock = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// newBatchListJobsServer is [newBatchTestServer] with the clock handed back, so a test can stamp two
// jobs with created-at values far enough apart to order and to bound.
func newBatchListJobsServer(t *testing.T) (*httptest.Server, *emulator.TimeController) {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(batchListJobsClock)
	logger := emulator.NewDefaultLogger(0, false)

	p := &emulator.BatchPlugin{}
	if err := p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize batch plugin: %v", err)
	}
	registry.Register(p)

	cfg := emulator.DefaultConfig()
	ts := httptest.NewServer(emulator.NewServer(*cfg, registry, store, state, tc, logger))
	t.Cleanup(ts.Close)
	return ts, tc
}

// batchMillis renders an instant the way the two created-at filters take it: *"a string
// representation of the number of milliseconds since 00:00:00 UTC (midnight) on January 1, 1970"*.
func batchMillis(ms int64) string { return strconv.FormatInt(ms, 10) }

// batchListJobsRaw posts a body substrate has to read as bytes, so a body that is not JSON can be
// sent at all — [batchRequest] marshals its argument and would refuse to build the request.
func batchListJobsRaw(t *testing.T, ts *httptest.Server, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/listjobs", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Host = "batch.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post raw body: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(data)
}

// batchListJobsResult is ListJobs' published response shape, with the members substrate reports.
type batchListJobsResult struct {
	JobSummaryList []struct {
		JobARN        string `json:"jobArn"`
		JobID         string `json:"jobId"`
		JobName       string `json:"jobName"`
		JobDefinition string `json:"jobDefinition"`
		CreatedAt     int64  `json:"createdAt"`
		Status        string `json:"status"`
		StatusReason  string `json:"statusReason"`
	} `json:"jobSummaryList"`
	NextToken string `json:"nextToken"`
}

// listJobs issues one ListJobs and requires a 200.
func listJobs(t *testing.T, ts *httptest.Server, body interface{}) batchListJobsResult {
	t.Helper()
	resp := batchRequest(t, ts, http.MethodPost, "/v1/listjobs", body)
	data := batchBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("listJobs: expected 200, got %d: %s", resp.StatusCode, data)
	}
	var out batchListJobsResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("decode listJobs: %v (%s)", err, data)
	}
	return out
}

// listJobsRefused issues one ListJobs and requires the published ClientException / 400.
func listJobsRefused(t *testing.T, ts *httptest.Server, body interface{}) string {
	t.Helper()
	resp := batchRequest(t, ts, http.MethodPost, "/v1/listjobs", body)
	data := batchBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, data)
	}
	var out struct {
		Message string `json:"message"`
		Type    string `json:"__type"`
		Code    string `json:"code"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("decode refusal: %v (%s)", err, data)
	}
	if out.Type != "ClientException" && out.Code != "ClientException" {
		t.Errorf("expected ClientException, got %s", data)
	}
	return out.Message
}

// submitBatchJob submits one job and returns its ID.
func submitBatchJob(t *testing.T, ts *httptest.Server, name, queue, definition string) string {
	t.Helper()
	resp := batchRequest(t, ts, http.MethodPost, "/v1/submitjob", map[string]string{
		"jobName":       name,
		"jobQueue":      queue,
		"jobDefinition": definition,
	})
	data := batchBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("submitJob: expected 200, got %d: %s", resp.StatusCode, data)
	}
	var out struct {
		JobID string `json:"jobId"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("decode submitJob: %v (%s)", err, data)
	}
	return out.JobID
}

// TestBatchListJobs_DefaultListingIsRunningOnly is the sharpest of #1236's four divergences,
// because it is the default path and both of the page's own examples take it.
//
// *"If you don't specify a status, only `RUNNING` jobs are returned."* Substrate answered every
// status, so a `SUCCEEDED` job appeared in a list a consumer reads as "still running" — the call's
// meaning inverted, with no parameter to blame.
//
// The default listing is **empty** here, not merely short, because `SubmitJob` records a job
// `SUCCEEDED` at submission and so no substrate job is ever `RUNNING`. That is #1248, and it is
// asserted rather than worked around: the every-status listing is not a substitute for a job that
// reaches `RUNNING`, and a caller told "nothing is running" is told something true of the records.
func TestBatchListJobs_DefaultListingIsRunningOnly(t *testing.T) {
	ts, _ := newBatchListJobsServer(t)
	jobID := submitBatchJob(t, ts, "job-a", "q1", "jd1")

	if got := listJobs(t, ts, map[string]string{}); len(got.JobSummaryList) != 0 {
		t.Errorf("default listing returned %d jobs, want none RUNNING: %+v", len(got.JobSummaryList), got.JobSummaryList)
	}
	// The same request with no body at all, which is what the legacy GET route carries.
	if got := listJobs(t, ts, nil); len(got.JobSummaryList) != 0 {
		t.Errorf("bodyless listing returned %d jobs, want none RUNNING", len(got.JobSummaryList))
	}

	// Named, the job is there — so the empty default is a filter, not a broken listing.
	got := listJobs(t, ts, map[string]string{"jobStatus": "SUCCEEDED"})
	if len(got.JobSummaryList) != 1 || got.JobSummaryList[0].JobID != jobID {
		t.Fatalf("jobStatus SUCCEEDED returned %+v, want the submitted job", got.JobSummaryList)
	}

	// Every other published status is a listing of its own, and empty.
	for _, status := range []string{"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "FAILED"} {
		if got := listJobs(t, ts, map[string]string{"jobStatus": status}); len(got.JobSummaryList) != 0 {
			t.Errorf("jobStatus %s returned %d jobs, want 0", status, len(got.JobSummaryList))
		}
	}
}

// TestBatchListJobs_StatusSelectsAcrossTwoStates pins the status filter as a difference rather than
// a constant a single-status store would satisfy.
//
// `TerminateJob` reports a job `FAILED`, which is the one way a substrate job leaves `SUCCEEDED`, so
// two statuses exist in one account and each listing has to contain exactly its own.
func TestBatchListJobs_StatusSelectsAcrossTwoStates(t *testing.T) {
	ts, _ := newBatchListJobsServer(t)
	succeeded := submitBatchJob(t, ts, "job-succeeded", "q1", "jd1")
	failed := submitBatchJob(t, ts, "job-failed", "q1", "jd1")

	resp := batchRequest(t, ts, http.MethodPost, "/v1/terminatejob", map[string]string{
		"jobId": failed, "reason": "asked to stop",
	})
	if data := batchBody(t, resp); resp.StatusCode != http.StatusOK {
		t.Fatalf("terminateJob: expected 200, got %d: %s", resp.StatusCode, data)
	}

	for status, want := range map[string]string{"SUCCEEDED": succeeded, "FAILED": failed} {
		got := listJobs(t, ts, map[string]string{"jobStatus": status})
		if len(got.JobSummaryList) != 1 || got.JobSummaryList[0].JobID != want {
			t.Errorf("jobStatus %s returned %+v, want only %s", status, got.JobSummaryList, want)
		}
	}

	// The terminated job carries the reason it was given, which is one of the summary members the
	// three-member shape could not report.
	got := listJobs(t, ts, map[string]string{"jobStatus": "FAILED"})
	if len(got.JobSummaryList) != 1 || got.JobSummaryList[0].StatusReason != "asked to stop" {
		t.Errorf("statusReason = %+v, want \"asked to stop\"", got.JobSummaryList)
	}
}

// TestBatchListJobs_JobQueueScopesTheListing covers `jobQueue`, which was read as nothing at all, so
// a request naming one queue was answered with every queue's jobs.
//
// The member is *"The name or full Amazon Resource Name (ARN) of the job queue"*, and `SubmitJob`
// stores whichever form its caller sent, so both forms have to reach the same job.
func TestBatchListJobs_JobQueueScopesTheListing(t *testing.T) {
	ts, _ := newBatchListJobsServer(t)
	inQ1 := submitBatchJob(t, ts, "job-q1", "q1", "jd1")
	inQ2 := submitBatchJob(t, ts, "job-q2", "arn:aws:batch:us-east-1:000000000000:job-queue/q2", "jd1")

	for queue, want := range map[string]string{
		"q1": inQ1,
		"q2": inQ2,
		"arn:aws:batch:us-east-1:000000000000:job-queue/q1": inQ1,
		"arn:aws:batch:us-east-1:000000000000:job-queue/q2": inQ2,
	} {
		got := listJobs(t, ts, map[string]string{"jobQueue": queue, "jobStatus": "SUCCEEDED"})
		if len(got.JobSummaryList) != 1 || got.JobSummaryList[0].JobID != want {
			t.Errorf("jobQueue %q returned %+v, want only %s", queue, got.JobSummaryList, want)
		}
	}

	// Both jobs without the selector, so the scoping above narrowed rather than the store holding
	// one job.
	if got := listJobs(t, ts, map[string]string{"jobStatus": "SUCCEEDED"}); len(got.JobSummaryList) != 2 {
		t.Errorf("unscoped listing returned %d jobs, want 2", len(got.JobSummaryList))
	}

	// A queue nobody submitted to is empty rather than everything.
	if got := listJobs(t, ts, map[string]string{"jobQueue": "q3", "jobStatus": "SUCCEEDED"}); len(got.JobSummaryList) != 0 {
		t.Errorf("unknown queue returned %+v, want empty", got.JobSummaryList)
	}
}

// TestBatchListJobs_OnlyOneOfTheThreeSelectorsIsAccepted is the page's opening sentence: *"You must
// specify only one of the following items: A job queue ID … A multi-node parallel job ID … An array
// job ID"*.
//
// All three are `Required: No` individually, so the rule lives in that prose. Naming **none** stays
// the account-wide listing, because the sentence forbids naming two rather than naming none, and the
// page publishes no error for an empty request — requiring exactly one would be substrate's reading.
func TestBatchListJobs_OnlyOneOfTheThreeSelectorsIsAccepted(t *testing.T) {
	ts, _ := newBatchListJobsServer(t)
	submitBatchJob(t, ts, "job-a", "q1", "jd1")

	for _, body := range []map[string]string{
		{"jobQueue": "q1", "arrayJobId": "a1"},
		{"jobQueue": "q1", "multiNodeJobId": "m1"},
		{"arrayJobId": "a1", "multiNodeJobId": "m1"},
		{"jobQueue": "q1", "arrayJobId": "a1", "multiNodeJobId": "m1"},
	} {
		if msg := listJobsRefused(t, ts, body); msg == "" {
			t.Errorf("%v: refused with an empty message", body)
		}
	}

	// None of the three is not a refusal.
	listJobs(t, ts, map[string]string{"jobStatus": "SUCCEEDED"})
}

// TestBatchListJobs_ChildAndNodeListingsAreEmpty covers `arrayJobId` and `multiNodeJobId`, which
// list *"all child jobs from within the specified array"* and *"all nodes that are associated with
// the specified job"*.
//
// `SubmitJob` records neither `arrayProperties` nor `nodeProperties`, so substrate mints no children
// and no nodes and both listings are empty. Ignoring the member — which is what the handler did —
// answered the account-wide listing instead, reporting a parent's children as though they existed.
// That is the worse of the two wrong answers, because it is not empty.
func TestBatchListJobs_ChildAndNodeListingsAreEmpty(t *testing.T) {
	ts, _ := newBatchListJobsServer(t)
	parent := submitBatchJob(t, ts, "job-parent", "q1", "jd1")

	for _, member := range []string{"arrayJobId", "multiNodeJobId"} {
		got := listJobs(t, ts, map[string]string{member: parent})
		if len(got.JobSummaryList) != 0 {
			t.Errorf("%s returned %+v, want empty", member, got.JobSummaryList)
		}
		if got.NextToken != "" {
			t.Errorf("%s returned nextToken %q over an empty listing", member, got.NextToken)
		}
	}
}

// TestBatchListJobs_JobStatusOutsideThePublishedValuesIsRefused covers the `Valid Values` list.
//
// The list is the page's own; that a value outside it is one of the *"identifier[s] that's not
// valid"* the `ClientException` gloss covers is substrate's reading, on the footing #1086 used for
// the token refusal. The alternative — matching nothing — answers an empty list for a typo, which is
// the false negative this class of work exists to remove.
func TestBatchListJobs_JobStatusOutsideThePublishedValuesIsRefused(t *testing.T) {
	ts, _ := newBatchListJobsServer(t)
	submitBatchJob(t, ts, "job-a", "q1", "jd1")

	for _, status := range []string{"succeeded", "RUNNING_JOBS", "COMPLETED", "Canceled"} {
		if msg := listJobsRefused(t, ts, map[string]string{"jobStatus": status}); msg == "" {
			t.Errorf("jobStatus %q: refused with an empty message", status)
		}
	}
}

// TestBatchListJobs_FilterRulesAreRefusedNotIgnored covers `filters`' published constraints.
//
// *"Only one filter can be used at a time"*, the five published names, and the created-at bounds'
// *"string representation of the number of milliseconds since 00:00:00 UTC (midnight) on January 1,
// 1970"*. A bound that is not a number would otherwise match nothing, which reads as "no job was
// created before then" rather than as "that is not a time".
func TestBatchListJobs_FilterRulesAreRefusedNotIgnored(t *testing.T) {
	ts, _ := newBatchListJobsServer(t)
	submitBatchJob(t, ts, "job-a", "q1", "jd1")

	twoFilters := map[string]interface{}{"filters": []map[string]interface{}{
		{"name": "JOB_NAME", "values": []string{"job-a"}},
		{"name": "JOB_DEFINITION", "values": []string{"jd1"}},
	}}
	if msg := listJobsRefused(t, ts, twoFilters); msg == "" {
		t.Error("two filters: refused with an empty message")
	}

	for _, name := range []string{"JOB_ID", "job_name", "STATUS", ""} {
		body := map[string]interface{}{"filters": []map[string]interface{}{
			{"name": name, "values": []string{"x"}},
		}}
		if msg := listJobsRefused(t, ts, body); msg == "" {
			t.Errorf("filter name %q: refused with an empty message", name)
		}
	}

	for _, bound := range []string{"BEFORE_CREATED_AT", "AFTER_CREATED_AT"} {
		body := map[string]interface{}{"filters": []map[string]interface{}{
			{"name": bound, "values": []string{"2026-03-01T12:00:00Z"}},
		}}
		if msg := listJobsRefused(t, ts, body); msg == "" {
			t.Errorf("%s with an RFC3339 value: refused with an empty message", bound)
		}
	}
}

// TestBatchListJobs_FiltersMatchByTheirPublishedRules covers each of the five names' own rule.
//
// `JOB_NAME` is *"a case-insensitive match for the job name"* with a trailing-asterisk prefix form;
// `JOB_DEFINITION` is *"case sensitive"*, matches every revision of a bare name and supports the
// same asterisk; the two created-at bounds are milliseconds; `SHARE_IDENTIFIER` matches nothing,
// because no recorded job carries a share identifier.
//
// A filter also switches status selection off — *"If the `filters` parameter is specified, the
// `jobStatus` parameter is ignored and jobs with any status are returned"* — which is why these
// requests name no status and still see `SUCCEEDED` jobs.
func TestBatchListJobs_FiltersMatchByTheirPublishedRules(t *testing.T) {
	ts, tc := newBatchListJobsServer(t)

	tc.SetTime(batchListJobsClock)
	first := submitBatchJob(t, ts, "Test1", "q1", "jd1")
	tc.SetTime(batchListJobsClock.Add(2 * time.Hour))
	second := submitBatchJob(t, ts, "test10", "q1", "jd1A:3")

	firstMS := batchListJobsClock.UnixMilli()
	betweenMS := batchListJobsClock.Add(time.Hour).UnixMilli()

	cases := []struct {
		name   string
		filter map[string]interface{}
		want   []string
	}{
		{"JOB_NAME is case-insensitive", map[string]interface{}{"name": "JOB_NAME", "values": []string{"test1"}}, []string{first}},
		{"JOB_NAME prefix", map[string]interface{}{"name": "JOB_NAME", "values": []string{"test1*"}}, []string{second, first}},
		{"JOB_NAME misses", map[string]interface{}{"name": "JOB_NAME", "values": []string{"test2"}}, nil},
		{"JOB_NAME takes any value", map[string]interface{}{"name": "JOB_NAME", "values": []string{"nope", "test10"}}, []string{second}},
		{"JOB_DEFINITION any revision", map[string]interface{}{"name": "JOB_DEFINITION", "values": []string{"jd1A"}}, []string{second}},
		{"JOB_DEFINITION prefix", map[string]interface{}{"name": "JOB_DEFINITION", "values": []string{"jd1*"}}, []string{second, first}},
		{"JOB_DEFINITION is case-sensitive", map[string]interface{}{"name": "JOB_DEFINITION", "values": []string{"JD1"}}, nil},
		{"BEFORE_CREATED_AT", map[string]interface{}{"name": "BEFORE_CREATED_AT", "values": []string{batchMillis(betweenMS)}}, []string{first}},
		{"AFTER_CREATED_AT", map[string]interface{}{"name": "AFTER_CREATED_AT", "values": []string{batchMillis(betweenMS)}}, []string{second}},
		{"AFTER_CREATED_AT includes both", map[string]interface{}{"name": "AFTER_CREATED_AT", "values": []string{batchMillis(firstMS)}}, []string{second, first}},
		{"SHARE_IDENTIFIER matches nothing", map[string]interface{}{"name": "SHARE_IDENTIFIER", "values": []string{"lowpri"}}, nil},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := listJobs(t, ts, map[string]interface{}{
				"filters": []map[string]interface{}{tt.filter},
			})
			ids := make([]string, 0, len(got.JobSummaryList))
			for _, summary := range got.JobSummaryList {
				ids = append(ids, summary.JobID)
			}
			if len(ids) != len(tt.want) {
				t.Fatalf("got %v, want %v", ids, tt.want)
			}
			// Compared in order: the filter path publishes createdAt-descending, so the
			// two-job cases assert the sort as well as the match.
			for i := range ids {
				if ids[i] != tt.want[i] {
					t.Fatalf("got %v, want %v", ids, tt.want)
				}
			}
		})
	}
}

// TestBatchListJobs_FiltersIgnoreJobStatusExceptWithShareIdentifier is the interaction the page
// spells out twice.
//
// *"When the filter is used, `jobStatus` is ignored with the exception that `SHARE_IDENTIFIER` and
// `jobStatus` can be used together."* So a `JOB_NAME` filter carrying an impossible status still
// returns the job, and the exception is visible as its opposite: with `SHARE_IDENTIFIER` the status
// is applied — and that listing is empty either way, because no record carries a share identifier.
func TestBatchListJobs_FiltersIgnoreJobStatusExceptWithShareIdentifier(t *testing.T) {
	ts, _ := newBatchListJobsServer(t)
	jobID := submitBatchJob(t, ts, "job-a", "q1", "jd1")

	got := listJobs(t, ts, map[string]interface{}{
		"jobStatus": "RUNNING",
		"filters":   []map[string]interface{}{{"name": "JOB_NAME", "values": []string{"job-a"}}},
	})
	if len(got.JobSummaryList) != 1 || got.JobSummaryList[0].JobID != jobID {
		t.Errorf("filtered listing returned %+v, want the job despite jobStatus RUNNING", got.JobSummaryList)
	}

	// Without the filter the same status is empty, so the "ignored" above is a difference.
	if plain := listJobs(t, ts, map[string]string{"jobStatus": "RUNNING"}); len(plain.JobSummaryList) != 0 {
		t.Errorf("jobStatus RUNNING returned %+v, want empty", plain.JobSummaryList)
	}
}

// TestBatchListJobs_PaginatesAndRefusesATokenItDidNotIssue makes ListJobs the fourth Batch
// paginator.
//
// `nextToken` publishes the three describes' sentence verbatim — *"The `nextToken` value returned
// from a previous paginated `ListJobs` request where `maxResults` was used"* — which is the footing
// #1086 used: a token no previous call returned is refused rather than answered with a well-formed
// page one, the one wrong answer a paging loop cannot detect.
func TestBatchListJobs_PaginatesAndRefusesATokenItDidNotIssue(t *testing.T) {
	ts, _ := newBatchListJobsServer(t)
	for _, name := range []string{"job-a", "job-b", "job-c"} {
		submitBatchJob(t, ts, name, "q1", "jd1")
	}

	first := listJobs(t, ts, map[string]interface{}{"jobStatus": "SUCCEEDED", "maxResults": 2})
	if len(first.JobSummaryList) != 2 || first.NextToken == "" {
		t.Fatalf("first page: got %d jobs and token %q, want 2 and a token", len(first.JobSummaryList), first.NextToken)
	}

	second := listJobs(t, ts, map[string]interface{}{
		"jobStatus": "SUCCEEDED", "maxResults": 2, "nextToken": first.NextToken,
	})
	if len(second.JobSummaryList) != 1 {
		t.Fatalf("second page: got %d jobs, want 1", len(second.JobSummaryList))
	}
	if second.NextToken != "" {
		t.Errorf("last page carried nextToken %q; it is omitted when there are no more results", second.NextToken)
	}
	if second.JobSummaryList[0].JobID == first.JobSummaryList[0].JobID {
		t.Error("the second page repeated the first page's first job")
	}

	// A token no ListJobs issued is refused, not answered with page one.
	if msg := listJobsRefused(t, ts, map[string]interface{}{"nextToken": "not-a-token"}); msg == "" {
		t.Error("an unissuable token: refused with an empty message")
	}

	// An over-cap maxResults clamps rather than refusing: the page publishes a ceiling and no
	// error for exceeding it. Substrate's reading, recorded in batchListJobsPageSize.
	clamped := listJobs(t, ts, map[string]interface{}{"jobStatus": "SUCCEEDED", "maxResults": 5000})
	if len(clamped.JobSummaryList) != 3 || clamped.NextToken != "" {
		t.Errorf("maxResults 5000 returned %d jobs and token %q, want all 3 and none",
			len(clamped.JobSummaryList), clamped.NextToken)
	}
}

// TestBatchListJobs_SummaryCarriesEveryMemberSubstrateHasARecordFor covers the response shape.
//
// The old summary emitted three of `JobSummary`'s eighteen members. These seven are the ones
// substrate has a record behind: `jobArn` derived as `SubmitJob` derives the one it returns, and six
// members of [emulator.BatchJob]. The other eleven are declined in batch_list_jobs.go rather than
// half-filled, because a caller cannot tell a zero `container.exitCode` from a job that exited 0.
func TestBatchListJobs_SummaryCarriesEveryMemberSubstrateHasARecordFor(t *testing.T) {
	ts, _ := newBatchListJobsServer(t)

	// Submitted inline, so the ARN asserted below is the one SubmitJob itself returned rather than
	// one the test rebuilt from a hardcoded account.
	submitted := batchRequest(t, ts, http.MethodPost, "/v1/submitjob", map[string]string{
		"jobName": "job-a", "jobQueue": "q1", "jobDefinition": "jd1:2",
	})
	var submit struct {
		JobARN string `json:"jobArn"`
		JobID  string `json:"jobId"`
	}
	if err := json.Unmarshal(batchBody(t, submitted), &submit); err != nil {
		t.Fatalf("decode submitJob: %v", err)
	}
	jobID := submit.JobID

	got := listJobs(t, ts, map[string]string{"jobStatus": "SUCCEEDED"})
	if len(got.JobSummaryList) != 1 {
		t.Fatalf("got %d jobs, want 1", len(got.JobSummaryList))
	}
	summary := got.JobSummaryList[0]

	if summary.JobID != jobID || summary.JobName != "job-a" || summary.Status != "SUCCEEDED" {
		t.Errorf("summary = %+v, want the submitted job", summary)
	}
	if summary.JobDefinition != "jd1:2" {
		t.Errorf("jobDefinition = %q, want the definition the job was submitted with", summary.JobDefinition)
	}
	// Bounded rather than equal, because the controller advances a scale-1.0 clock from the instant
	// it was set: the assertion is that the stamp came from the *simulated* clock, which the wall
	// clock is months away from, not that a request took zero milliseconds.
	base := batchListJobsClock.UnixMilli()
	if summary.CreatedAt < base || summary.CreatedAt > base+time.Minute.Milliseconds() {
		t.Errorf("createdAt = %d, want the simulated clock's %d", summary.CreatedAt, base)
	}
	// The ARN SubmitJob returned for the same job, so a caller can join the two. `jobArn` is a
	// published JobSummary member that the three-member shape omitted entirely.
	if summary.JobARN != submit.JobARN {
		t.Errorf("jobArn = %q, want SubmitJob's %q", summary.JobARN, submit.JobARN)
	}
	if !strings.HasSuffix(summary.JobARN, ":job/"+jobID) {
		t.Errorf("jobArn = %q, want it to name the job ID", summary.JobARN)
	}
}

// TestBatchListJobs_AnUnreadableBodyIsRefusedBeforeAnyStateRead is #887's ordering criterion: a
// refusal must not depend on how many jobs happen to exist.
//
// An **empty** body is not an unreadable one — every member is `Required: No`, and the legacy
// `GET /v1/jobs` route carries no body at all — which the default-listing test asserts.
func TestBatchListJobs_AnUnreadableBodyIsRefusedBeforeAnyStateRead(t *testing.T) {
	ts, _ := newBatchListJobsServer(t)

	// Refused with no job in the store, and again with one: the same refusal either way, which is
	// what "before any state read" means when the state is what the operation reports.
	for _, phase := range []string{"empty store", "one job"} {
		code, body := batchListJobsRaw(t, ts, `{"jobQueue":`)
		if code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d: %s", phase, code, body)
		}
		if !strings.Contains(body, "ClientException") {
			t.Errorf("%s: expected ClientException, got %s", phase, body)
		}
		submitBatchJob(t, ts, "job-a", "q1", "jd1")
	}
}
