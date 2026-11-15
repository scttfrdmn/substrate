package emulator_test

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// One `Marker` rule for every paginated ELB operation — #1245, #1244.
//
// Classic `DescribeLoadBalancers` refused a marker it never issued with `ValidationError`/400, while
// ELBv2 `DescribeAccountLimits` restarted the walk at page one, and the four other ELBv2 describes
// read no marker at all. The assertion here is that all six answer the same unusable cursor with the
// same code and status, read off the wire, and that an offset past the end is an empty last page at
// every one of them.

// elbCursorOperation is one paginated describe, as a request this file can send.
type elbCursorOperation struct {
	name string

	// action is the operation name, which also names the result wrapper.
	action string

	// params is the routing for the operation; the cursor members are added per request.
	params map[string]string
}

// elbCursorOperations are the six paginated operations of both generations.
func elbCursorOperations() []elbCursorOperation {
	return []elbCursorOperation{
		{"classic DescribeLoadBalancers", "DescribeLoadBalancers",
			map[string]string{"Action": "DescribeLoadBalancers", "Version": elbClassicVersion}},
		{"ELBv2 DescribeAccountLimits", "DescribeAccountLimits",
			map[string]string{"Action": "DescribeAccountLimits"}},
		{"ELBv2 DescribeLoadBalancers", "DescribeLoadBalancers",
			map[string]string{"Action": "DescribeLoadBalancers"}},
		{"ELBv2 DescribeTargetGroups", "DescribeTargetGroups",
			map[string]string{"Action": "DescribeTargetGroups"}},
		{"ELBv2 DescribeListeners", "DescribeListeners",
			map[string]string{"Action": "DescribeListeners"}},
		{"ELBv2 DescribeRules", "DescribeRules",
			map[string]string{"Action": "DescribeRules"}},
	}
}

// elbCursorParams returns op's request with the given cursor members set; an empty value is absent.
func elbCursorParams(op elbCursorOperation, marker, pageSize string) map[string]string {
	params := make(map[string]string, len(op.params)+2)
	for k, v := range op.params {
		params[k] = v
	}
	if marker != "" {
		params["Marker"] = marker
	}
	if pageSize != "" {
		params["PageSize"] = pageSize
	}
	return params
}

// TestELBPageCursor_EveryOperationRefusesTheSameUnissuedMarker is #1245's rule on the wire, at all
// six operations once #1244 gave the four ELBv2 describes the cursor.
func TestELBPageCursor_EveryOperationRefusesTheSameUnissuedMarker(t *testing.T) {
	t.Parallel()
	enc := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

	for _, tc := range []struct {
		name   string
		marker string
	}{
		{"never issued", "page-two"},
		{"a bare negative", "-3"},
		{"a bare decimal, the cursor DescribeAccountLimits used to issue", "3"},
		{"base64 of a negative offset", enc("-3")},
		{"base64 of text", enc("abc")},
		{"base64 of a form the encoder never emits", enc("05")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := newELBTestServer(t)
			lbARN := elbCreateLB(t, ts.URL, "cursor-alb", nil)
			tgARN := elbCreateTG(t, ts.URL, "cursor-tg", nil)
			elbCreateListener(t, ts.URL, lbARN, tgARN, nil)
			elbClassicCreate(t, ts.URL, "cursor-classic", nil)

			ops := elbCursorOperations()
			first := elbSendForPageSizeAnswer(t, ts.URL, elbCursorParams(ops[0], tc.marker, ""))
			for _, op := range ops {
				got := elbSendForPageSizeAnswer(t, ts.URL, elbCursorParams(op, tc.marker, ""))
				assert.Equal(t, first.code, got.code, "%s and %s answer one cursor with two codes",
					ops[0].name, op.name)
				assert.Equal(t, first.status, got.status, "%s and %s answer one cursor with two statuses",
					ops[0].name, op.name)
				assert.Equal(t, "ValidationError", got.code, op.name)
				assert.Equal(t, http.StatusBadRequest, got.status, op.name)
				assert.Contains(t, got.message, "Marker", op.name)
			}
		})
	}
}

// TestELBPageCursor_PastTheEndIsAnEmptyLastPage covers the cursor that is not refused: an offset
// substrate did issue over a listing that has since shrunk. Every operation answers 200, an empty
// page, the result wrapper, and no `NextMarker`.
func TestELBPageCursor_PastTheEndIsAnEmptyLastPage(t *testing.T) {
	t.Parallel()
	ts := newELBTestServer(t)
	elbCreateLB(t, ts.URL, "past-end-alb", nil)
	elbClassicCreate(t, ts.URL, "past-end-classic", nil)
	marker := base64.StdEncoding.EncodeToString([]byte("9999"))

	for _, op := range elbCursorOperations() {
		resp := elbRequest(t, ts.URL, elbCursorParams(op, marker, ""))
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op.name, body)
		assert.Contains(t, string(body), "<"+op.action+"Result>", op.name)
		assert.NotContains(t, string(body), "<NextMarker>", op.name)
		assert.NotContains(t, string(body), "<member>", "%s: an empty page lists nothing", op.name)
	}
}

// elbCursorPage is one decoded page of an ELBv2 describe: the identifying member of each record,
// and the marker.
type elbCursorPage struct {
	ids  []string
	next string
}

// elbDescribePage sends one paged ELBv2 describe and decodes the identifiers of what it listed.
func elbDescribePage(t *testing.T, ts *httptest.Server, params map[string]string,
	listMember, idMember string) elbCursorPage {
	t.Helper()
	resp := elbRequest(t, ts.URL, params)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)

	// A generic decode: the operation's result and list wrappers are renamed to the names the struct
	// reads, and the identifying child of each record is pulled out by name, so one helper reads all
	// four operations.
	var decoded struct {
		Result struct {
			Members []struct {
				Fields []struct {
					XMLName xml.Name
					Value   string `xml:",chardata"`
				} `xml:",any"`
			} `xml:"List>member"`
			NextMarker *string `xml:"NextMarker"`
		} `xml:"Result"`
	}
	result := params["Action"] + "Result"
	text := strings.NewReplacer(
		"<"+result+">", "<Result>", "</"+result+">", "</Result>",
		"<"+listMember+">", "<List>", "</"+listMember+">", "</List>",
	).Replace(string(body))
	require.NoError(t, xml.Unmarshal([]byte(text), &decoded), "%s", body)

	page := elbCursorPage{}
	for _, m := range decoded.Result.Members {
		for _, f := range m.Fields {
			if f.XMLName.Local == idMember {
				page.ids = append(page.ids, f.Value)
			}
		}
	}
	if decoded.Result.NextMarker != nil {
		require.NotEmpty(t, *decoded.Result.NextMarker, "a NextMarker is absent when exhausted, never empty")
		page.next = *decoded.Result.NextMarker
	}
	return page
}

// TestELBPageCursor_EachELBv2DescribeWalksEveryRecordOnce walks each of the four describes #1244
// paginated a page at a time and asserts every record is returned exactly once, in the order the
// unpaged call returns them, with a `NextMarker` on every page but the last.
func TestELBPageCursor_EachELBv2DescribeWalksEveryRecordOnce(t *testing.T) {
	t.Parallel()
	const records = 5
	ts := newELBTestServer(t)

	lbARN := elbCreateLB(t, ts.URL, "walk-alb-0", nil)
	for i := 1; i < records; i++ {
		elbCreateLB(t, ts.URL, fmt.Sprintf("walk-alb-%d", i), nil)
	}
	tgARN := elbCreateTG(t, ts.URL, "walk-tg-0", nil)
	for i := 1; i < records; i++ {
		elbCreateTG(t, ts.URL, fmt.Sprintf("walk-tg-%d", i), nil)
	}
	listenerARN := elbCreateListener(t, ts.URL, lbARN, tgARN, nil)
	for i := 1; i < records; i++ {
		elbCreateListener(t, ts.URL, lbARN, tgARN, map[string]string{"Port": fmt.Sprint(8080 + i)})
	}
	for i := 0; i < records; i++ {
		elbCreateRule(t, ts.URL, listenerARN, tgARN, map[string]string{"Priority": fmt.Sprint(10 + i)})
	}

	for _, tc := range []struct {
		action, listMember, idMember string
		extra                        map[string]string
	}{
		{"DescribeLoadBalancers", "LoadBalancers", "LoadBalancerArn", nil},
		{"DescribeTargetGroups", "TargetGroups", "TargetGroupArn", nil},
		{"DescribeListeners", "Listeners", "ListenerArn", map[string]string{"LoadBalancerArn": lbARN}},
		{"DescribeRules", "Rules", "RuleArn", map[string]string{"ListenerArn": listenerARN}},
	} {
		t.Run(tc.action, func(t *testing.T) {
			params := func(marker, pageSize string) map[string]string {
				p := map[string]string{"Action": tc.action}
				for k, v := range tc.extra {
					p[k] = v
				}
				if marker != "" {
					p["Marker"] = marker
				}
				if pageSize != "" {
					p["PageSize"] = pageSize
				}
				return p
			}
			whole := elbDescribePage(t, ts, params("", ""), tc.listMember, tc.idMember)
			require.GreaterOrEqual(t, len(whole.ids), records, "the fixture has a record per page and more")
			assert.Empty(t, whole.next, "the default page size of 400 holds the whole set")

			var walked []string
			marker := ""
			for pages := 0; ; pages++ {
				require.LessOrEqual(t, pages, len(whole.ids), "the walk did not terminate")
				page := elbDescribePage(t, ts, params(marker, "2"), tc.listMember, tc.idMember)
				require.LessOrEqual(t, len(page.ids), 2, "a page must not exceed its PageSize")
				walked = append(walked, page.ids...)
				if page.next == "" {
					break
				}
				marker = page.next
			}
			assert.Equal(t, whole.ids, walked, "a paged walk returns every record once, in order")
		})
	}
}
