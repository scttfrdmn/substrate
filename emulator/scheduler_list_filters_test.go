package emulator_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schedListPage is one ListSchedules answer, decoded to the members these tests assert on.
type schedListPage struct {
	Schedules []struct {
		Name  string `json:"Name"`
		State string `json:"State"`
	} `json:"Schedules"`
	NextToken string `json:"NextToken"`
}

// schedListNames returns the schedule names on one page, in the order the page carries them, together
// with the page's NextToken. The order matters: the offset token is only meaningful against a stable
// listing order, so a resumption assertion has to compare names rather than counts.
func schedListNames(t *testing.T, ts *httptest.Server, query string) ([]string, string) {
	t.Helper()
	resp := schedulerRequest(t, ts, http.MethodGet, "/schedules"+query, "")
	require.Equal(t, http.StatusOK, resp.StatusCode, query)
	var page schedListPage
	require.NoError(t, json.Unmarshal(readSchedulerBody(t, resp), &page), query)
	names := make([]string, 0, len(page.Schedules))
	for _, s := range page.Schedules {
		names = append(names, s.Name)
	}
	return names, page.NextToken
}

// createSchedInState creates one schedule through the published CreateSchedule route, so the record
// under test is the one a caller would have made rather than one written into the store by the test
// (#765).
func createSchedInState(t *testing.T, ts *httptest.Server, name, state string) {
	t.Helper()
	body := fmt.Sprintf(`{
		"ScheduleExpression": "rate(1 hour)",
		"State": %q,
		"Target": {"Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn", "RoleArn": "arn:aws:iam::123456789012:role/role"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`, state)
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/"+name, body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create %s", name)
	require.NoError(t, resp.Body.Close())
}

// TestScheduler_ListSchedulesStateFilterIsAppliedBeforeTheCut pins the fix for #1229: State is a
// filter on the listing, so it selects what the page is cut from rather than thinning a page that was
// already chosen.
//
// The group interleaves ENABLED and DISABLED so that every prefix of the unfiltered listing contains
// both. That is what makes the assertions non-vacuous: with the filter applied after the cut, a
// MaxResults=4 page of the twelve below carried 2 schedules, and the third page carried 0 with a
// NextToken still set.
func TestScheduler_ListSchedulesStateFilterIsAppliedBeforeTheCut(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// Twelve schedules, alternating state, named so that the index's ASCII order is the creation
	// order: six ENABLED (even) and six DISABLED (odd).
	const total = 12
	for i := range total {
		state := "ENABLED"
		if i%2 == 1 {
			state = "DISABLED"
		}
		createSchedInState(t, ts, fmt.Sprintf("mixed-%02d", i), state)
	}

	t.Run("a filtered page is full", func(t *testing.T) {
		names, token := schedListNames(t, ts, "?State=ENABLED&MaxResults=4")
		assert.Equal(t, []string{"mixed-00", "mixed-02", "mixed-04", "mixed-06"}, names,
			"four enabled schedules, not the enabled half of the first four names")
		assert.NotEmpty(t, token, "two enabled schedules remain, so the cursor is set")
	})

	t.Run("resumption walks the filtered listing", func(t *testing.T) {
		// Page size 4 over six matches: a full page, then the remaining two with no cursor. Before the
		// fix the second page was the *unfiltered* offset 4-7, which is two enabled schedules and a
		// token, and the walk took three pages to reach the same six names.
		first, token := schedListNames(t, ts, "?State=ENABLED&MaxResults=4")
		require.NotEmpty(t, token)
		second, token2 := schedListNames(t, ts, "?State=ENABLED&MaxResults=4&NextToken="+token)
		assert.Equal(t, []string{"mixed-08", "mixed-10"}, second)
		assert.Empty(t, token2, "the filtered listing is exhausted, so there is no cursor")
		assert.Len(t, append(first, second...), 6, "every ENABLED schedule, once")
	})

	t.Run("no page is empty while matches remain", func(t *testing.T) {
		// The shape #1229 calls the worse consequence: an empty Schedules array together with a
		// non-empty NextToken, which a caller that stops at an empty list reads as "nothing matches".
		// Page size 1 is the sharpest case, since before the fix every other page was empty.
		seen := make([]string, 0, total/2)
		query := "?State=DISABLED&MaxResults=1"
		for range total + 1 {
			names, token := schedListNames(t, ts, query)
			if token != "" {
				require.NotEmpty(t, names, "a page carrying a cursor must carry a match: %s", query)
			}
			seen = append(seen, names...)
			if token == "" {
				break
			}
			query = "?State=DISABLED&MaxResults=1&NextToken=" + token
		}
		assert.Equal(t, []string{"mixed-01", "mixed-03", "mixed-05", "mixed-07", "mixed-09", "mixed-11"}, seen,
			"six single-schedule pages, each one a match")
	})

	t.Run("a state no schedule is in lists nothing", func(t *testing.T) {
		// Not a refusal: State's published Valid Values are ENABLED | DISABLED, and a value outside
		// them is its own class (see scheduler_pagination.go on MaxResults). What is asserted here is
		// that an empty listing carries no cursor, which is the same rule as the page above.
		names, token := schedListNames(t, ts, "?State=ENABLED&NamePrefix=nothing-matches-this")
		assert.Empty(t, names)
		assert.Empty(t, token, "an empty listing is exhausted, so there is nothing to resume")
	})

	t.Run("the unfiltered listing is unchanged", func(t *testing.T) {
		names, token := schedListNames(t, ts, "?MaxResults=4")
		assert.Equal(t, []string{"mixed-00", "mixed-01", "mixed-02", "mixed-03"}, names,
			"without State the page is the first four names in index order")
		assert.NotEmpty(t, token)
	})

	t.Run("State composes with NamePrefix", func(t *testing.T) {
		// Both filters run ahead of the cut now, so the page is the intersection rather than one
		// filter's page thinned by the other.
		createSchedInState(t, ts, "other-a", "ENABLED")
		createSchedInState(t, ts, "other-b", "DISABLED")
		names, token := schedListNames(t, ts, "?NamePrefix=other-&State=ENABLED")
		assert.Equal(t, []string{"other-a"}, names)
		assert.Empty(t, token)
	})
}

// TestScheduler_ListSchedulesStateFilterScopesToItsGroup keeps the group selector honest alongside the
// moved filter: ScheduleGroup still chooses which index is loaded, so a state-filtered listing of one
// group cannot answer another group's schedules.
func TestScheduler_ListSchedulesStateFilterScopesToItsGroup(t *testing.T) {
	srv := newSchedulerTestServer(t)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	createSchedInState(t, ts, "in-default", "ENABLED")

	// A schedule in a second group, created through the published lowerCamel groupName key that
	// CreateSchedule reads — which is not the PascalCase ScheduleGroup that ListSchedules reads
	// (#1226).
	body := `{
		"ScheduleExpression": "rate(1 hour)",
		"State": "ENABLED",
		"GroupName": "team-b",
		"Target": {"Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn", "RoleArn": "arn:aws:iam::123456789012:role/role"},
		"FlexibleTimeWindow": {"Mode": "OFF"}
	}`
	resp := schedulerRequest(t, ts, http.MethodPost, "/schedules/in-team-b?groupName=team-b", body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	names, _ := schedListNames(t, ts, "?State=ENABLED")
	assert.Equal(t, []string{"in-default"}, names, "the default group only")

	names, _ = schedListNames(t, ts, "?State=ENABLED&ScheduleGroup=team-b")
	assert.Equal(t, []string{"in-team-b"}, names, "the named group only")
}
