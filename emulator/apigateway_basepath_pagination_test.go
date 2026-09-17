package emulator_test

// GetBasePathMappings pages (#917).
//
// AWS publishes both pagination parameters on the operation's URI — "GET
// /domainnames/{domain_name}/basepathmappings?domainNameId={domainNameId}&limit={limit}&
// position={position}" — and substrate read neither, answering every mapping with no position. A
// paging consumer's loop terminated on its first response here and first ran for real against a
// domain holding more mappings than one page.
//
// This collection is the one v1 collection that pages, so it is also the one that can carry the
// "position" member at all; the other seven answer everything in one page and leave it unset,
// because a caller must not be handed a token for a page that does not exist. Six of those seven
// publish both parameters and so are the same defect this test closes here (#1025); the seventh,
// GetStages, publishes neither and has no "position" response member, so its single page is right.
//
// Unlike every EC2 describe, "limit" publishes a **default**: "The maximum number of returned
// results per page. The default value is 25 and the maximum value is 500." So the absent-parameter
// case is a paging case rather than an everything case, and it is asserted as one below. Every
// mapping is created through CreateBasePathMapping rather than written to state, per #765.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// apigwPagingDomain is the domain name every mapping below is created under.
const apigwPagingDomain = "api.example.com"

// apigwPagingServer builds an API Gateway server over a caller-supplied state manager, which the
// package's other API Gateway helpers do not allow and the sealed-store assertion needs.
func apigwPagingServer(t *testing.T, state emulator.StateManager) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))

	plugin := &emulator.APIGatewayPlugin{}
	require.NoError(t, plugin.Initialize(context.Background(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)

	return emulator.NewServer(*cfg, registry, store, state, tc, logger)
}

// apigwPagingCall drives one API Gateway v1 request through the server and returns the status and
// the raw body.
func apigwPagingCall(t *testing.T, srv *emulator.Server, method, path string, body any) (int, string) {
	t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		require.NoError(t, err)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(payload))
	r.Host = "apigateway.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	resp := w.Result()
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

// apigwPagingCreateDomain creates the domain name the mappings hang off.
func apigwPagingCreateDomain(t *testing.T, srv *emulator.Server) {
	t.Helper()
	status, raw := apigwPagingCall(t, srv, http.MethodPost, "/domainnames", map[string]any{
		"domainName":     apigwPagingDomain,
		"certificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/abc",
	})
	require.Equal(t, http.StatusCreated, status, raw)
}

// apigwPagingCreateMapping creates one base path mapping.
func apigwPagingCreateMapping(t *testing.T, srv *emulator.Server, basePath string) {
	t.Helper()
	status, raw := apigwPagingCall(t, srv, http.MethodPost,
		"/domainnames/"+apigwPagingDomain+"/basepathmappings", map[string]any{
			"basePath":  basePath,
			"restApiId": "abcde12345",
			"stage":     "prod",
		})
	require.Equal(t, http.StatusCreated, status, raw)
}

// apigwPagingSeed creates the domain and n mappings, whose base paths are zero-padded so that the
// lexicographic order the collection is walked in is also their creation order — which makes an
// assertion on a page's contents readable rather than only on its length.
func apigwPagingSeed(t *testing.T, srv *emulator.Server, n int) []string {
	t.Helper()
	apigwPagingCreateDomain(t, srv)
	paths := make([]string, 0, n)
	for i := 0; i < n; i++ {
		basePath := fmt.Sprintf("bp-%03d", i)
		apigwPagingCreateMapping(t, srv, basePath)
		paths = append(paths, basePath)
	}
	return paths
}

// apigwPagingPage is one GetBasePathMappings response, decoded. The element member is "item",
// singular, which is the locationName the model spells for every v1 collection.
type apigwPagingPage struct {
	Item []struct {
		BasePath  string `json:"basePath"`
		RestAPIID string `json:"restApiId"`
		Stage     string `json:"stage"`
	} `json:"item"`
	Position string `json:"position"`
}

// basePaths returns the page's base paths in the order the response carries them.
func (p apigwPagingPage) basePaths() []string {
	out := make([]string, 0, len(p.Item))
	for _, m := range p.Item {
		out = append(out, m.BasePath)
	}
	return out
}

// apigwPagingListRaw calls GetBasePathMappings and returns the status and the raw body.
//
// The query is built with url.Values so a position containing "+" or "=" survives the round trip
// rather than arriving corrupted, which would make the test assert against its own damage.
func apigwPagingListRaw(t *testing.T, srv *emulator.Server, pairs ...string) (int, string) {
	t.Helper()
	query := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] != "" {
			query.Set(pairs[i], pairs[i+1])
		}
	}
	path := "/domainnames/" + apigwPagingDomain + "/basepathmappings"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return apigwPagingCall(t, srv, http.MethodGet, path, nil)
}

// apigwPagingList calls GetBasePathMappings, requires a 200 and returns the decoded page.
func apigwPagingList(t *testing.T, srv *emulator.Server, pairs ...string) apigwPagingPage {
	t.Helper()
	status, raw := apigwPagingListRaw(t, srv, pairs...)
	require.Equal(t, http.StatusOK, status, raw)
	var page apigwPagingPage
	require.NoError(t, json.Unmarshal([]byte(raw), &page), raw)
	return page
}

// apigwPagingErrorCode reads the error code out of a REST-JSON error body.
func apigwPagingErrorCode(t *testing.T, raw string) string {
	t.Helper()
	var shape struct {
		Code string `json:"Code"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &shape), raw)
	return shape.Code
}

// TestAPIGatewayBasePathMappings_AbsentLimitPagesAtThePublishedDefault asserts the bound that
// distinguishes this operation from every EC2 describe: a request naming no limit still pages, at
// the published default of 25, where substrate used to answer the whole collection.
func TestAPIGatewayBasePathMappings_AbsentLimitPagesAtThePublishedDefault(t *testing.T) {
	srv := apigwPagingServer(t, emulator.NewMemoryStateManager())
	const total = 26
	created := apigwPagingSeed(t, srv, total)

	page1 := apigwPagingList(t, srv)
	require.Len(t, page1.Item, 25)
	assert.Equal(t, created[:25], page1.basePaths())
	require.NotEmpty(t, page1.Position, "a collection longer than the default must carry a position")

	page2 := apigwPagingList(t, srv, "position", page1.Position)
	assert.Equal(t, created[25:], page2.basePaths())
	assert.Empty(t, page2.Position, "the last page carries no position")
}

// TestAPIGatewayBasePathMappings_WalkReportsEveryMappingExactlyOnce is the assertion the cursor
// exists to satisfy: a caller looping until position comes back empty sees the whole collection, in
// order, with no repeat and no omission.
func TestAPIGatewayBasePathMappings_WalkReportsEveryMappingExactlyOnce(t *testing.T) {
	srv := apigwPagingServer(t, emulator.NewMemoryStateManager())
	const total = 7
	created := apigwPagingSeed(t, srv, total)

	var walked []string
	position := ""
	for pages := 0; ; pages++ {
		require.Less(t, pages, total+2, "the walk did not terminate")
		page := apigwPagingList(t, srv, "limit", "3", "position", position)
		assert.LessOrEqual(t, len(page.Item), 3)
		walked = append(walked, page.basePaths()...)
		if page.Position == "" {
			break
		}
		position = page.Position
	}
	assert.Equal(t, created, walked)

	// The same collection read in one page, for the comparison the walk is checked against.
	whole := apigwPagingList(t, srv, "limit", "500")
	assert.Equal(t, created, whole.basePaths())
	assert.Empty(t, whole.Position)
}

// TestAPIGatewayBasePathMappings_FullFinalPageCarriesNoPosition covers the exact-multiple case: the
// position names where the next page starts, so a full final page must not carry one.
func TestAPIGatewayBasePathMappings_FullFinalPageCarriesNoPosition(t *testing.T) {
	srv := apigwPagingServer(t, emulator.NewMemoryStateManager())
	created := apigwPagingSeed(t, srv, 6)

	page1 := apigwPagingList(t, srv, "limit", "3")
	require.Equal(t, created[:3], page1.basePaths())
	require.NotEmpty(t, page1.Position)

	page2 := apigwPagingList(t, srv, "limit", "3", "position", page1.Position)
	assert.Equal(t, created[3:], page2.basePaths())
	assert.Empty(t, page2.Position, "a full final page must not carry a token")
}

// TestAPIGatewayBasePathMappings_LimitOutsideThePublishedRangeIsRefused asserts the published
// maximum of 500 and the floor of one, which is substrate's reading — AWS publishes no minimum, and
// a page of zero elements describes a walk that answers nothing and hands back a position forever.
func TestAPIGatewayBasePathMappings_LimitOutsideThePublishedRangeIsRefused(t *testing.T) {
	srv := apigwPagingServer(t, emulator.NewMemoryStateManager())
	apigwPagingSeed(t, srv, 3)

	for _, tc := range []struct {
		name  string
		limit string
	}{
		{"zero", "0"},
		{"negative", "-1"},
		{"not an integer", "many"},
		{"one above the published maximum", "501"},
	} {
		t.Run("refused: "+tc.name, func(t *testing.T) {
			status, raw := apigwPagingListRaw(t, srv, "limit", tc.limit)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Equal(t, "BadRequestException", apigwPagingErrorCode(t, raw), raw)
			assert.NotContains(t, raw, "bp-000", "a refused limit must not be answered with a page")
		})
	}

	t.Run("one is accepted", func(t *testing.T) {
		page := apigwPagingList(t, srv, "limit", "1")
		assert.Len(t, page.Item, 1)
		assert.NotEmpty(t, page.Position)
	})

	t.Run("the published maximum is accepted", func(t *testing.T) {
		page := apigwPagingList(t, srv, "limit", "500")
		assert.Len(t, page.Item, 3)
		assert.Empty(t, page.Position)
	})
}

// TestAPIGatewayBasePathMappings_RefusesAPositionItDidNotIssue is #915 at this operation: a token
// substrate could not have issued is refused rather than answered with a well-formed page one.
func TestAPIGatewayBasePathMappings_RefusesAPositionItDidNotIssue(t *testing.T) {
	srv := apigwPagingServer(t, emulator.NewMemoryStateManager())
	created := apigwPagingSeed(t, srv, 3)

	// A position the operation issued still resumes the walk. Asserted first, so a refusal that
	// swallowed every token could not pass the rest of this test.
	page1 := apigwPagingList(t, srv, "limit", "1")
	require.Equal(t, created[:1], page1.basePaths())
	issued := page1.Position
	require.NotEmpty(t, issued)

	page2 := apigwPagingList(t, srv, "limit", "1", "position", issued)
	assert.Equal(t, created[1:2], page2.basePaths(), "an issued position must resume after the first page")

	for _, tc := range tokenRefusalBadTokens {
		t.Run(tc.name, func(t *testing.T) {
			status, raw := apigwPagingListRaw(t, srv, "limit", "1", "position", tc.token)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Equal(t, "BadRequestException", apigwPagingErrorCode(t, raw), raw)
			assert.NotContains(t, raw, created[0], "a refused position must not be answered with page one")
		})
	}

	// A position past the end is one substrate did issue, over a collection that has since shrunk,
	// so it clamps to an empty final page rather than being refused.
	t.Run("a position past the end clamps to an empty page", func(t *testing.T) {
		beyond := base64.StdEncoding.EncodeToString([]byte("99"))
		page := apigwPagingList(t, srv, "limit", "1", "position", beyond)
		assert.Empty(t, page.Item)
		assert.Empty(t, page.Position)
	})

	// Both refusals precede the read of any state, per #887: the answer to a malformed request must
	// not depend on how many mappings exist. Sealed against reads, the handler still refuses rather
	// than reporting the store's failure as a 500.
	for _, tc := range []struct {
		name  string
		pairs []string
	}{
		{"position", []string{"position", "!!not-base64!!"}},
		{"limit", []string{"limit", "0"}},
	} {
		t.Run(tc.name+" is refused before any state is read", func(t *testing.T) {
			sealed := &tokenRefusalSealedState{
				inner:     emulator.NewMemoryStateManager(),
				sealGets:  true,
				sealLists: true,
			}
			sealedSrv := apigwPagingServer(t, sealed)
			status, raw := apigwPagingListRaw(t, sealedSrv, tc.pairs...)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.Equal(t, "BadRequestException", apigwPagingErrorCode(t, raw), raw)
		})
	}
}
