package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ListPipelines pages on maxResults/nextToken (API_ListPipelines), over the wire.

// cpListPage is one decoded ListPipelines page.
type cpListPage struct {
	NextToken *string `json:"nextToken"`
	Pipelines []struct {
		Name string `json:"name"`
	} `json:"pipelines"`
}

// names is the page's pipeline names, in order.
func (p cpListPage) names() []string {
	out := []string{}
	for _, pl := range p.Pipelines {
		out = append(out, pl.Name)
	}
	return out
}

// cpPipeline is a minimal CreatePipeline body.
func cpPipeline(name string) map[string]any {
	return map[string]any{"pipeline": map[string]any{
		"name": name, "roleArn": "arn:aws:iam::123456789012:role/cp",
		"stages": []map[string]any{{"name": "Source", "actions": []any{}}, {"name": "Deploy", "actions": []any{}}},
	}}
}

// cpCall sends one CodePipeline operation through ts.
func cpCall(t *testing.T, ts *emulator.TestServer, op string, body any) (int, []byte) {
	t.Helper()
	return cicdCall(t, ts, cicdPipelineHost, cicdPipelineTgt, op, body)
}

// cpList reads one ListPipelines page, failing on anything but 200.
func cpList(t *testing.T, ts *emulator.TestServer, body map[string]any) cpListPage {
	t.Helper()
	status, raw := cpCall(t, ts, "ListPipelines", body)
	require.Equal(t, http.StatusOK, status, "ListPipelines: %s", raw)
	var out cpListPage
	require.NoError(t, json.Unmarshal(raw, &out), "%s", raw)
	if out.NextToken != nil {
		require.NotEmpty(t, *out.NextToken, "a nextToken that is present is not empty")
	}
	return out
}

func TestCodePipelineListPipelines_PagesAndSurvivesAMidWalkChange(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for i := 1; i <= 5; i++ {
		status, raw := cpCall(t, ts, "CreatePipeline", cpPipeline(fmt.Sprintf("cp-%d", i)))
		require.Equal(t, http.StatusOK, status, "%s", raw)
	}

	whole := cpList(t, ts, map[string]any{})
	assert.Equal(t, []string{"cp-1", "cp-2", "cp-3", "cp-4", "cp-5"}, whole.names(), "an absent maxResults lists everything, in name order")
	assert.Nil(t, whole.NextToken)

	first := cpList(t, ts, map[string]any{"maxResults": 2})
	require.Equal(t, []string{"cp-1", "cp-2"}, first.names())
	require.NotNil(t, first.NextToken)

	// Between pages: the cursor record and the one after it are deleted, one sorting before the
	// cursor and one after every record are created.
	for _, name := range []string{"cp-2", "cp-3"} {
		status, raw := cpCall(t, ts, "DeletePipeline", map[string]any{"name": name})
		require.Equal(t, http.StatusOK, status, "%s", raw)
	}
	for _, name := range []string{"cp-0", "cp-9"} {
		status, raw := cpCall(t, ts, "CreatePipeline", cpPipeline(name))
		require.Equal(t, http.StatusOK, status, "%s", raw)
	}

	walked := first.names()
	token := *first.NextToken
	for pages := 0; token != ""; pages++ {
		require.Less(t, pages, 10)
		pg := cpList(t, ts, map[string]any{"maxResults": 2, "nextToken": token})
		walked = append(walked, pg.names()...)
		token = ""
		if pg.NextToken != nil {
			token = *pg.NextToken
		}
	}
	assert.Equal(t, []string{"cp-1", "cp-2", "cp-4", "cp-5", "cp-9"}, walked,
		"every surviving pipeline once, the deleted cursor resumed in place, nothing repeated")
}

func TestCodePipelineListPipelines_RefusesWhatItCannotPage(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for _, name := range []string{"cp-a", "cp-b"} {
		status, raw := cpCall(t, ts, "CreatePipeline", cpPipeline(name))
		require.Equal(t, http.StatusOK, status, "%s", raw)
	}
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	for _, tc := range []struct {
		name string
		body map[string]any
		code string
	}{
		{"maxResults 0", map[string]any{"maxResults": 0}, "ValidationException"},
		{"maxResults 1001", map[string]any{"maxResults": 1001}, "ValidationException"},
		{"maxResults -1", map[string]any{"maxResults": -1}, "ValidationException"},
		{"a token that is not base64", map[string]any{"nextToken": "not a token!"}, "InvalidNextTokenException"},
		{"base64 of an offset", map[string]any{"nextToken": b64("1")}, "InvalidNextTokenException"},
		{"base64 of a name", map[string]any{"nextToken": b64("cp-a")}, "InvalidNextTokenException"},
		{"another operation's token", map[string]any{"nextToken": b64(`{"o":"ListResources","h":"cbf29ce484222325","s":"cp-a","t":""}`)}, "InvalidNextTokenException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, raw := cpCall(t, ts, "ListPipelines", tc.body)
			assert.Equal(t, http.StatusBadRequest, status, "%s", raw)
			assert.Equal(t, tc.code, cicdCode(t, raw), "%s", raw)
		})
	}
	// The bounds themselves are legal.
	for _, n := range []int{1, 1000} {
		pg := cpList(t, ts, map[string]any{"maxResults": n})
		assert.NotEmpty(t, pg.names(), "maxResults %d", n)
	}
}

func TestCodePipelineListPipelines_AnUnreadableRecordFailsAndADanglingEntryIsSkipped(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupCodePipelineWirePlugin(t)
	for _, name := range []string{"cp-a", "cp-b", "cp-c"} {
		codepipelineWire(t, p, ctx, "CreatePipeline", codepipelineWirePipeline(name))
	}
	key := func(name string) string {
		return "pipeline:" + codepipelineWireAccount + "/" + codepipelineWireRegion + "/" + name
	}

	// An index entry whose record is gone is passed over.
	require.NoError(t, state.Delete(t.Context(), "codepipeline", key("cp-b")))
	var listed cpListPage
	require.NoError(t, json.Unmarshal(codepipelineWire(t, p, ctx, "ListPipelines", nil), &listed))
	assert.Equal(t, []string{"cp-a", "cp-c"}, listed.names())

	// A record that does not decode fails the listing rather than shortening it.
	require.NoError(t, state.Put(t.Context(), "codepipeline", key("cp-c"), []byte("{not json")))
	_, err := p.HandleRequest(ctx, codepipelineRequest(t, "ListPipelines", map[string]any{}))
	require.Error(t, err)
}
