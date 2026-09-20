package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// One ELB plugin answered one published `PageSize` range two ways — #1150.
//
// ELBv2 `DescribeAccountLimits` and classic `DescribeLoadBalancers` publish the same constraint
// ("Valid Range: Minimum value of 1. Maximum value of 400", and "a number from 1 to 400. The default
// is 400"), and substrate answered an out-of-range value with a 200 and the whole set at the first
// and a `ValidationError` at the second. The consumer-visible failure is the one a harness cannot
// see: asking for a page size real AWS refuses, reading a 200, and concluding the value is legal.
//
// So the assertion here is not "each operation refuses" — that could be satisfied by two tables
// drifting apart again — but **that the two answers are equal**, case by case, read off the wire.
// Each subtest sends the same `PageSize` to both generations and compares code to code and status to
// status before comparing either against what the page publishes. A future operation joining the rule
// ([elbPageSize] is the one place it is written) gets a row here; an operation answering its own way
// fails on the comparison rather than on a constant this file chose.
//
// The four cases are the four ways a value can be unusable: above the maximum, at zero, negative, and
// not a number. Zero and negative are the pair the old fallback was worst at — asking for the smallest
// page was answered with the largest — and they are not covered by the "indistinguishable from a
// clamp" argument that made the high end look harmless.

// elbPageSizeOperation is one generation's paginated describe, as a request this file can send.
type elbPageSizeOperation struct {
	// name is the generation, for the subtest and the failure message.
	name string

	// params returns the request for one `PageSize`, including whatever routes the generation.
	params func(pageSize string) map[string]string
}

// elbPageSizeOperations are the plugin's two operations that read `PageSize`.
//
// The other four ELBv2 describes publish the member and read neither it nor `Marker` (#1244), so they
// have no answer to compare yet; when they gain the cursor they join this table. ELBv2 `DescribeTags`
// publishes no pagination member at all.
var elbPageSizeOperations = []elbPageSizeOperation{
	{
		name: "ELBv2 DescribeAccountLimits",
		params: func(pageSize string) map[string]string {
			return map[string]string{"Action": "DescribeAccountLimits", "PageSize": pageSize}
		},
	},
	{
		name: "classic DescribeLoadBalancers",
		params: func(pageSize string) map[string]string {
			return map[string]string{
				"Action": "DescribeLoadBalancers", "Version": elbClassicVersion, "PageSize": pageSize,
			}
		},
	},
}

// elbPageSizeAnswer is what one operation answered one `PageSize`: the status, and the error's code
// and message when it refused.
//
// A success leaves the code and the message empty rather than failing the decode, which is what lets
// two operations be *compared*: the defect being guarded against is precisely one of them answering
// 200 where the other refuses, and a helper that required an `ErrorResponse` would fail on the
// symptom before the comparison could name it.
type elbPageSizeAnswer struct {
	status  int
	code    string
	message string
}

// elbSendForPageSizeAnswer sends one request and reads its answer.
func elbSendForPageSizeAnswer(t *testing.T, baseURL string, params map[string]string) elbPageSizeAnswer {
	t.Helper()
	resp := elbRequest(t, baseURL, params)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// No `XMLName`, so this decodes a refusal's `ErrorResponse` and a success's own root alike.
	var decoded struct {
		Code    string `xml:"Error>Code"`
		Message string `xml:"Error>Message"`
	}
	require.NoError(t, xml.Unmarshal(body, &decoded), "%s: %s", params["Action"], body)
	return elbPageSizeAnswer{status: resp.StatusCode, code: decoded.Code, message: decoded.Message}
}

// TestELBPageSize_BothGenerationsRefuseTheSameValueTheSameWay is #1150's rule on the wire.
func TestELBPageSize_BothGenerationsRefuseTheSameValueTheSameWay(t *testing.T) {
	t.Parallel()
	require.Len(t, elbPageSizeOperations, 2, "both generations are compared, or nothing is")

	for _, tc := range []struct {
		name     string
		pageSize string
	}{
		{"above the published maximum", "401"},
		{"zero", "0"},
		{"negative", "-5"},
		{"not a number", "not-a-number"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newELBTestServer(t)

			// A load balancer in each generation, so neither refusal can be an empty-store artifact:
			// the request has something to page over and is refused anyway.
			elbCreateLB(t, ts.URL, "page-size-alb", nil)
			elbClassicCreate(t, ts.URL, "page-size-classic", nil)

			answers := make(map[string]elbPageSizeAnswer, len(elbPageSizeOperations))
			for _, op := range elbPageSizeOperations {
				answers[op.name] = elbSendForPageSizeAnswer(t, ts.URL, op.params(tc.pageSize))
			}

			first, second := elbPageSizeOperations[0].name, elbPageSizeOperations[1].name
			assert.Equal(t, answers[first].code, answers[second].code,
				"%s and %s answer one published range with two codes", first, second)
			assert.Equal(t, answers[first].status, answers[second].status,
				"%s and %s answer one published range with two statuses", first, second)

			for name, got := range answers {
				// `ValidationError`/400 is the code the consolidated Query Common Errors page both
				// operations link as their own publishes, which is why refusing invents nothing (#1064).
				assert.Equal(t, "ValidationError", got.code, name)
				assert.Equal(t, http.StatusBadRequest, got.status, name)

				// The message names the published range, so a caller reading it learns what would be
				// legal rather than only that this was not.
				assert.Contains(t, got.message, "PageSize", name)
				if tc.pageSize != "not-a-number" {
					assert.Contains(t, got.message, "from 1 to 400", name)
				}
			}
		})
	}
}

// TestELBPageSize_BothGenerationsAcceptTheEndsOfTheRange is the other side of the same rule: a
// refusal that narrowed the published range would be substrate inventing a contract.
//
// The minimum and the maximum are both accepted, and an absent member is the published default of 400
// rather than a refusal — the case AWS publishes a default for. The page each one answers is asserted
// per operation in `elb_account_limits_test.go` and `elb_classic_test.go`; what is asserted here is
// only that neither end and no absence is refused, in both generations, from one table.
func TestELBPageSize_BothGenerationsAcceptTheEndsOfTheRange(t *testing.T) {
	t.Parallel()

	for _, pageSize := range []string{"", "1", "400"} {
		name := "PageSize=" + pageSize
		if pageSize == "" {
			name = "PageSize absent"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := newELBTestServer(t)
			elbCreateLB(t, ts.URL, "page-size-ok-alb", nil)
			elbClassicCreate(t, ts.URL, "page-size-ok-classic", nil)

			for _, op := range elbPageSizeOperations {
				params := op.params(pageSize)
				if pageSize == "" {
					delete(params, "PageSize")
				}
				resp := elbRequest(t, ts.URL, params)
				assert.Equal(t, http.StatusOK, resp.StatusCode, op.name)
				require.NoError(t, resp.Body.Close())
			}
		})
	}
}
