package emulator_test

// Athena's two listings publish two different MaxResults ranges, and each is now enforced on its own
// terms (#1185): 0–50 for ListQueryExecutions, 1–50 for ListWorkGroups, InvalidRequestException/400
// outside them. An explicit empty NextToken is refused for the published minimum length of 1.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestAthenaPageSize_EachListingHasItsOwnRange pins both ranges at both ends.
func TestAthenaPageSize_EachListingHasItsOwnRange(t *testing.T) {
	cases := []struct {
		op       string
		accepted []int
		refused  []int
	}{
		{op: "ListQueryExecutions", accepted: []int{0, 1, 50}, refused: []int{-1, 51}},
		{op: "ListWorkGroups", accepted: []int{1, 50}, refused: []int{-1, 0, 51}},
	}
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.AthenaPlugin{})
			status, body, _ := tokenRefusalAthenaCall(t, srv, tc.op, `{}`)
			assert.Equal(t, http.StatusOK, status, "an absent MaxResults keeps the default: %s", body)
			for _, v := range tc.accepted {
				status, body, _ := tokenRefusalAthenaCall(t, srv, tc.op, fmt.Sprintf(`{"MaxResults":%d}`, v))
				assert.Equal(t, http.StatusOK, status, "MaxResults=%d: %s", v, body)
			}
			for _, v := range tc.refused {
				status, body, code := tokenRefusalAthenaCall(t, srv, tc.op, fmt.Sprintf(`{"MaxResults":%d}`, v))
				assert.Equal(t, http.StatusBadRequest, status, "MaxResults=%d: %s", v, body)
				assert.Equal(t, "InvalidRequestException", code, "MaxResults=%d: %s", v, body)
			}
			status, body, code := tokenRefusalAthenaCall(t, srv, tc.op, `{"NextToken":""}`)
			assert.Equal(t, http.StatusBadRequest, status, "an empty NextToken is below the minimum length: %s", body)
			assert.Equal(t, "InvalidRequestException", code, body)
		})
	}
}

// TestAthenaPageSize_ZeroIsAFinalEmptyPage asserts what a legal MaxResults of 0 answers on
// ListQueryExecutions: no IDs and no token, so a paginator stops rather than following a cursor that
// never advances, and not the widest page substrate used to rewrite it to.
func TestAthenaPageSize_ZeroIsAFinalEmptyPage(t *testing.T) {
	srv := tokenRefusalServer(t, emulator.NewMemoryStateManager(), &emulator.AthenaPlugin{})
	tokenRefusalAthenaStartQuery(t, srv, "SELECT 1")
	second := tokenRefusalAthenaStartQuery(t, srv, "SELECT 2")

	type page struct {
		QueryExecutionIds []string `json:"QueryExecutionIds"`
		NextToken         *string  `json:"NextToken"`
	}
	list := func(body string) page {
		status, raw, _ := tokenRefusalAthenaCall(t, srv, "ListQueryExecutions", body)
		require.Equal(t, http.StatusOK, status, raw)
		var p page
		require.NoError(t, json.Unmarshal([]byte(raw), &p))
		return p
	}

	zero := list(`{"MaxResults":0}`)
	assert.Empty(t, zero.QueryExecutionIds, "MaxResults=0 asks for at most zero items")
	assert.Nil(t, zero.NextToken, "a zero page ends the walk even though records remain")

	// Mid-walk, a zero page is also final, and a non-zero walk still resumes and ends normally.
	one := list(`{"MaxResults":1}`)
	require.NotNil(t, one.NextToken)
	mid := list(fmt.Sprintf(`{"MaxResults":0,"NextToken":%q}`, *one.NextToken))
	assert.Empty(t, mid.QueryExecutionIds)
	assert.Nil(t, mid.NextToken)
	tail := list(fmt.Sprintf(`{"MaxResults":1,"NextToken":%q}`, *one.NextToken))
	assert.Equal(t, []string{second}, tail.QueryExecutionIds)
	assert.Nil(t, tail.NextToken)
}
