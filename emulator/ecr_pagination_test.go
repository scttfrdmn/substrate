package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ECR's three paginated listings, and the three different shapes their pages publish (#1090).
//
// DescribeRepositories, DescribeImages and ListImages each publish maxResults (1–1000, default
// 100) and an opaque nextToken, and substrate decoded neither on any of the three: every request
// answered the whole listing, which a caller following the published contract cannot detect,
// because one full page is a well-formed answer.
//
// The three pages disagree about the exclusion, which is why each is asserted separately rather
// than through one shared case: DescribeRepositories excludes both members when repositoryNames
// is given, DescribeImages excludes both when imageIds is given, and ListImages publishes no
// exclusion at all.
//
// The assertions decode the wire body, because nextToken and the page cut are the things under
// test and HandleRequest's caller never sees the JSON.

// ecrDecode unmarshals a successful ECR response body.
func ecrDecode(t *testing.T, raw string) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &out), "body: %s", raw)
	return out
}

// ecrRepoNames reads the repository names out of a DescribeRepositories body, in the order the
// response carries them.
func ecrRepoNames(t *testing.T, raw string) []string {
	t.Helper()
	body := ecrDecode(t, raw)
	list, ok := body["repositories"].([]any)
	require.True(t, ok, "no repositories member: %s", raw)
	names := make([]string, 0, len(list))
	for _, entry := range list {
		repo, ok := entry.(map[string]any)
		require.True(t, ok)
		names = append(names, repo["repositoryName"].(string))
	}
	return names
}

// ecrNextToken reads nextToken, which is absent on a final page.
func ecrNextToken(t *testing.T, raw string) string {
	t.Helper()
	token, ok := ecrDecode(t, raw)["nextToken"]
	if !ok {
		return ""
	}
	return token.(string)
}

// TestECR_DescribeRepositoriesPagesTheRegistry walks the registry-wide listing to exhaustion.
//
// The union of the pages must be every repository exactly once — the failure a caller cannot see
// is a cursor that repeats or skips — and the final page must carry no nextToken, because AWS
// publishes it as "null when there are no more results to return", so a token on a full final
// page would describe a page that does not exist.
func TestECR_DescribeRepositoriesPagesTheRegistry(t *testing.T) {
	ts := newECRTestServer(t)

	want := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	// Created out of order, so the sorted page order is the operation's choice and not the
	// creation order the names index happens to hold.
	for _, name := range []string{"charlie", "alpha", "echo", "bravo", "delta"} {
		ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": name})
	}

	var seen []string
	token := ""
	for page := 0; ; page++ {
		require.Less(t, page, 10, "the cursor never terminated")
		req := map[string]any{"maxResults": 2}
		if token != "" {
			req["nextToken"] = token
		}
		raw := ecrCallOK(t, ts, "DescribeRepositories", req)
		names := ecrRepoNames(t, raw)
		assert.LessOrEqual(t, len(names), 2, "a page larger than maxResults")
		seen = append(seen, names...)
		token = ecrNextToken(t, raw)
		if token == "" {
			assert.Equal(t, 2, page, "five repositories at two per page ends on the third page")
			break
		}
	}
	assert.Equal(t, want, seen, "the pages are the whole registry, in order, once each")

	// The default is 100, so an unpaginated request still answers everything — the shape every
	// existing caller depends on.
	assert.Len(t, ecrRepoNames(t, ecrCallOK(t, ts, "DescribeRepositories", map[string]any{})), 5)
	assert.Empty(t, ecrNextToken(t, ecrCallOK(t, ts, "DescribeRepositories", map[string]any{})),
		"a listing that fits in one page carries no token")
}

// TestECR_ListImagesPagesAndAnswersOneEntryPerTag covers the third exclusion shape and the
// listing's membership.
//
// AWS's own published sample for ListImages answers two entries carrying the same digest and
// different tags, and the page says a TAGGED filter lists "all of the tags in your repository" —
// so the listing is over image IDs, not over images. Substrate de-duplicated by digest and kept
// whichever tag Go's map iteration yielded first, so an image with two tags was reported under a
// randomly chosen one of them.
func TestECR_ListImagesPagesAndAnswersOneEntryPerTag(t *testing.T) {
	ts := newECRTestServer(t)
	ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "images"})

	const shared = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	for _, tag := range []string{"latest", "v1"} {
		ecrCallOK(t, ts, "PutImage", map[string]any{
			"repositoryName": "images",
			"imageTag":       tag,
			"imageDigest":    shared,
			"imageManifest":  `{"schemaVersion":2}`,
		})
	}

	raw := ecrCallOK(t, ts, "ListImages", map[string]any{"repositoryName": "images"})
	ids, ok := ecrDecode(t, raw)["imageIds"].([]any)
	require.True(t, ok, "no imageIds member: %s", raw)
	require.Len(t, ids, 2, "one tag was dropped: %s", raw)
	assert.Equal(t, "latest", ids[0].(map[string]any)["imageTag"])
	assert.Equal(t, "v1", ids[1].(map[string]any)["imageTag"])

	// Repeated identical calls answer identical bytes. The old build ranged over the tag map, so
	// both the entry order and which tag survived de-duplication were Go's map order — a
	// determinism defect in an emulator whose whole claim is that the same inputs answer the same
	// bytes.
	for i := 0; i < 8; i++ {
		assert.Equal(t, raw, ecrCallOK(t, ts, "ListImages", map[string]any{"repositoryName": "images"}),
			"ListImages answered a different order on call %d", i+2)
	}

	// ListImages publishes no exclusion sentence, so its pagination members stand alone.
	first := ecrCallOK(t, ts, "ListImages", map[string]any{"repositoryName": "images", "maxResults": 1})
	ids, _ = ecrDecode(t, first)["imageIds"].([]any)
	assert.Len(t, ids, 1)
	token := ecrNextToken(t, first)
	require.NotEmpty(t, token, "a token is due while an entry remains")

	second := ecrCallOK(t, ts, "ListImages",
		map[string]any{"repositoryName": "images", "maxResults": 1, "nextToken": token})
	ids, _ = ecrDecode(t, second)["imageIds"].([]any)
	require.Len(t, ids, 1)
	assert.Equal(t, "v1", ids[0].(map[string]any)["imageTag"], "the second page resumes, it does not restart")
	assert.Empty(t, ecrNextToken(t, second))
}

// TestECR_DescribeImagesPagesAndRefusesAnImageItCannotAnswerFor covers the second exclusion
// shape and ImageNotFoundException.
//
// API_DescribeImages publishes ImageNotFoundException/400 and substrate had no site for it: an
// imageIds entry naming an unknown tag was dropped from the request and one naming an unknown
// digest was dropped from the answer, so a caller naming one real and one imaginary image got 200
// and a short list — the same shape as the DescribeRepositories defect.
func TestECR_DescribeImagesPagesAndRefusesAnImageItCannotAnswerFor(t *testing.T) {
	ts := newECRTestServer(t)
	ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "described"})
	for i := 0; i < 3; i++ {
		ecrCallOK(t, ts, "PutImage", map[string]any{
			"repositoryName": "described",
			"imageTag":       fmt.Sprintf("v%d", i),
			"imageDigest":    fmt.Sprintf("sha256:%064d", i),
			"imageManifest":  `{"schemaVersion":2}`,
		})
	}

	raw := ecrCallOK(t, ts, "DescribeImages", map[string]any{"repositoryName": "described", "maxResults": 2})
	details, ok := ecrDecode(t, raw)["imageDetails"].([]any)
	require.True(t, ok, "no imageDetails member: %s", raw)
	assert.Len(t, details, 2)
	token := ecrNextToken(t, raw)
	require.NotEmpty(t, token)

	raw = ecrCallOK(t, ts, "DescribeImages",
		map[string]any{"repositoryName": "described", "maxResults": 2, "nextToken": token})
	details, _ = ecrDecode(t, raw)["imageDetails"].([]any)
	assert.Len(t, details, 1)
	assert.Empty(t, ecrNextToken(t, raw), "three images at two per page is two pages")

	// The enumerated form: a real image answers, an unknown tag and an unknown digest are refused.
	ecrCallOK(t, ts, "DescribeImages", map[string]any{
		"repositoryName": "described",
		"imageIds":       []map[string]string{{"imageTag": "v1"}},
	})
	for _, tc := range []struct {
		name string
		id   map[string]string
	}{
		{name: "an unknown tag", id: map[string]string{"imageTag": "nope"}},
		{name: "an unknown digest", id: map[string]string{"imageDigest": "sha256:" + fmt.Sprintf("%064d", 9)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code := ecrErrorStatus(t, ts, "DescribeImages", map[string]any{
				"repositoryName": "described",
				"imageIds":       []map[string]string{{"imageTag": "v1"}, tc.id},
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "ImageNotFoundException", code,
				"a short list is not an answer to a request naming two images")
		})
	}
}

// TestECR_EachPageExcludesWhatItsOwnPagePublishes is the three shapes read back as cases.
//
// The point is that they differ. A single shared guard would either refuse a ListImages request
// AWS accepts or accept a DescribeImages request AWS refuses, and nothing in either answer would
// say so.
func TestECR_EachPageExcludesWhatItsOwnPagePublishes(t *testing.T) {
	ts := newECRTestServer(t)
	ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "excluded"})
	ecrCallOK(t, ts, "PutImage", map[string]any{
		"repositoryName": "excluded",
		"imageTag":       "latest",
		"imageManifest":  `{"schemaVersion":2}`,
	})
	issued := base64.StdEncoding.EncodeToString([]byte("1"))

	for _, tc := range []struct {
		name      string
		operation string
		body      map[string]any
	}{
		{
			name:      "DescribeRepositories maxResults with repositoryNames",
			operation: "DescribeRepositories",
			body:      map[string]any{"repositoryNames": []string{"excluded"}, "maxResults": 1},
		},
		{
			name:      "DescribeRepositories nextToken with repositoryNames",
			operation: "DescribeRepositories",
			body:      map[string]any{"repositoryNames": []string{"excluded"}, "nextToken": issued},
		},
		{
			name:      "DescribeImages maxResults with imageIds",
			operation: "DescribeImages",
			body: map[string]any{
				"repositoryName": "excluded",
				"imageIds":       []map[string]string{{"imageTag": "latest"}},
				"maxResults":     1,
			},
		},
		{
			name:      "DescribeImages nextToken with imageIds",
			operation: "DescribeImages",
			body: map[string]any{
				"repositoryName": "excluded",
				"imageIds":       []map[string]string{{"imageTag": "latest"}},
				"nextToken":      issued,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code := ecrErrorStatus(t, ts, tc.operation, tc.body)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidParameterException", code,
				"InvalidParameterException is the only parameter-fault code these pages publish")
		})
	}

	// And the shape that is not excluded: ListImages publishes no such sentence, so it must still
	// answer. Asserting the negative is the half that stops the guard being copied to all three.
	ecrCallOK(t, ts, "ListImages", map[string]any{"repositoryName": "excluded", "maxResults": 1})
	ecrCallOK(t, ts, "ListImages",
		map[string]any{"repositoryName": "excluded", "maxResults": 1, "nextToken": issued})
}

// TestECR_APageRequestOutsideThePublishedContractIsRefused covers the range and the cursor.
//
// "Valid Range: Minimum value of 1. Maximum value of 1000" on all three pages. Substrate refuses
// rather than clamping, because a page of 1000 does not tell a caller who asked for 5000 that they
// misread the contract. A token substrate never issued is refused for #915's reason: accepting it
// answers page one, and a caller looping until the token is empty then loops forever.
func TestECR_APageRequestOutsideThePublishedContractIsRefused(t *testing.T) {
	ts := newECRTestServer(t)
	ecrCallOK(t, ts, "CreateRepository", map[string]any{"repositoryName": "bounded"})
	ecrCallOK(t, ts, "PutImage", map[string]any{
		"repositoryName": "bounded",
		"imageTag":       "latest",
		"imageManifest":  `{"schemaVersion":2}`,
	})

	// Every operation that publishes the range, against every value outside it. The three
	// operations are walked rather than one, because each decodes its own body.
	for _, op := range []struct {
		name string
		body map[string]any
	}{
		{name: "DescribeRepositories", body: map[string]any{}},
		{name: "DescribeImages", body: map[string]any{"repositoryName": "bounded"}},
		{name: "ListImages", body: map[string]any{"repositoryName": "bounded"}},
	} {
		t.Run(op.name, func(t *testing.T) {
			for _, bad := range []any{-1, 1001, 5000} {
				body := map[string]any{"maxResults": bad}
				for k, v := range op.body {
					body[k] = v
				}
				status, code := ecrErrorStatus(t, ts, op.name, body)
				assert.Equal(t, http.StatusBadRequest, status, "maxResults %v", bad)
				assert.Equal(t, "InvalidParameterException", code, "maxResults %v", bad)
			}

			// The bounds themselves are inside the range and must be answered.
			for _, good := range []any{1, 1000} {
				body := map[string]any{"maxResults": good}
				for k, v := range op.body {
					body[k] = v
				}
				ecrCallOK(t, ts, op.name, body)
			}

			// And a token substrate could not have issued. The forms are
			// decodeOffsetPaginationToken's round-trip rule read back — not base64, base64 of
			// non-digits, and a negative offset.
			for _, token := range []string{
				"not-base64!",
				base64.StdEncoding.EncodeToString([]byte("page-two")),
				base64.StdEncoding.EncodeToString([]byte("-1")),
			} {
				body := map[string]any{"nextToken": token}
				for k, v := range op.body {
					body[k] = v
				}
				status, code := ecrErrorStatus(t, ts, op.name, body)
				assert.Equal(t, http.StatusBadRequest, status, "token %q", token)
				assert.Equal(t, "InvalidParameterException", code, "token %q", token)
			}

			// A token naming an offset past the end is one substrate did issue over a listing that
			// has since shrunk; clamping it to a final empty page is the honest answer, and
			// refusing it would break a walk whose records were deleted mid-loop.
			body := map[string]any{"nextToken": base64.StdEncoding.EncodeToString([]byte("500"))}
			for k, v := range op.body {
				body[k] = v
			}
			assert.Empty(t, ecrNextToken(t, ecrCallOK(t, ts, op.name, body)))
		})
	}
}
