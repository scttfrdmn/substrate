package emulator_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// seedConflict arms a conditional-conflict seed over the control plane. An empty
// bucket and key seed the wildcard.
func seedConflict(t *testing.T, srv *emulator.Server, body map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	w := ctrlRequest(t, srv, http.MethodPost, "/v1/s3/conditional-conflict", raw)
	require.Equal(t, http.StatusOK, w.Code, "seed conflict: %s", w.Body)
}

// ctrlRequest issues a control-plane request and returns the recorder. The S3
// helpers all address s3.amazonaws.com, which the control plane is not served
// under, so this uses a plain localhost target.
func ctrlRequest(t *testing.T, srv *emulator.Server, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body != nil {
		r = httptest.NewRequest(method, "http://localhost"+path, bytes.NewReader(body))
	} else {
		r = httptest.NewRequest(method, "http://localhost"+path, nil)
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w
}

// TestS3_SeededConditionalConflict_PutObject is the acceptance test for #540's
// PutObject arm: a seeded conflict makes a conditional write that would otherwise
// succeed report 409 ConditionalRequestConflict, and the budget is spent, so the
// caller's documented recovery — retry the PUT as-is — then works.
func TestS3_SeededConditionalConflict_PutObject(t *testing.T) {
	const bucket, key = "conflict-put", "obj"
	srv := condFixture(t, bucket)
	seedConflict(t, srv, map[string]any{"bucket": bucket, "key": key, "putConflicts": 1})

	w := s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("first"),
		map[string]string{"If-None-Match": "*"})
	require.Equal(t, http.StatusConflict, w.Code, "body: %s", w.Body)
	assert.Equal(t, "ConditionalRequestConflict", parseS3Error(t, w.Body.Bytes()).Code)

	// The refused write is a no-op: the key must still be absent, or a consumer
	// retrying would find state the 409 said was not applied.
	assert.Equal(t, http.StatusNotFound,
		s3Request(t, srv, http.MethodGet, "/"+bucket+"/"+key, nil, nil).Code,
		"a 409 must leave the key untouched")

	// The budget was one, so the retry AWS documents for this arm succeeds.
	w = s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("first"),
		map[string]string{"If-None-Match": "*"})
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body)
	assert.Equal(t, "first", getCondBody(t, srv, bucket, key))
}

// TestS3_SeededConditionalConflict_IfMatchLeavesObjectIntact covers the other
// header on the same arm. AWS documents 409 for If-Match as well as If-None-Match
// — the recovery differs (fetch the ETag first) but the outcome is the same — and
// the existing object must survive a refused overwrite.
func TestS3_SeededConditionalConflict_IfMatchLeavesObjectIntact(t *testing.T) {
	const bucket, key = "conflict-ifmatch", "obj"
	srv := condFixture(t, bucket)
	etag := putCondObject(t, srv, bucket, key, "original")
	seedConflict(t, srv, map[string]any{"bucket": bucket, "key": key, "putConflicts": 1})

	w := s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("overwrite"),
		map[string]string{"If-Match": etag})
	require.Equal(t, http.StatusConflict, w.Code, "body: %s", w.Body)
	assert.Equal(t, "ConditionalRequestConflict", parseS3Error(t, w.Body.Bytes()).Code)
	assert.Equal(t, "original", getCondBody(t, srv, bucket, key),
		"a 409 must leave the stored object byte-identical")
}

// TestS3_SeededConditionalConflict_UnconditionalWriteUnaffected is the gate on the
// seed's scope. AWS documents ConditionalRequestConflict only on the If-Match and
// If-None-Match members, so a plain PUT to a seeded key must be untouched — and
// must not spend the budget the test armed for a conditional write.
func TestS3_SeededConditionalConflict_UnconditionalWriteUnaffected(t *testing.T) {
	const bucket, key = "conflict-uncond", "obj"
	srv := condFixture(t, bucket)
	seedConflict(t, srv, map[string]any{"bucket": bucket, "key": key, "putConflicts": 1})

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("plain"), nil).Code,
		"an unconditional write must ignore a seeded conflict")

	// The budget is intact, so the next conditional write — one whose precondition
	// holds, so the seed is the only reason it can fail — still gets its 409.
	etag := s3Request(t, srv, http.MethodHead, "/"+bucket+"/"+key, nil, nil).Header().Get("ETag")
	require.NotEmpty(t, etag)
	w := s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("cas"),
		map[string]string{"If-Match": etag})
	assert.Equal(t, http.StatusConflict, w.Code,
		"an unconditional write must not consume the budget: %s", w.Body)
}

// TestS3_SeededConditionalConflict_FailedPreconditionKeepsBudget is the ordering
// gate. A 412 is a determinate observation of the destination's current state and
// must be reported as itself rather than replaced by a seeded race — and must not
// spend the budget, or the conflict the test armed would silently never fire.
func TestS3_SeededConditionalConflict_FailedPreconditionKeepsBudget(t *testing.T) {
	const bucket, key = "conflict-order", "obj"
	srv := condFixture(t, bucket)
	putCondObject(t, srv, bucket, key, "original")
	seedConflict(t, srv, map[string]any{"bucket": bucket, "key": key, "putConflicts": 1})

	// If-None-Match: * against a key that exists is a 412 on its own terms.
	w := s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("overwrite"),
		map[string]string{"If-None-Match": "*"})
	require.Equal(t, http.StatusPreconditionFailed, w.Code, "body: %s", w.Body)
	assert.Equal(t, "PreconditionFailed", parseS3Error(t, w.Body.Bytes()).Code)

	// An If-Match against an absent key is the 404 arm, likewise unmasked.
	w = s3Request(t, srv, http.MethodPut, "/"+bucket+"/absent", []byte("x"),
		map[string]string{"If-Match": `"whatever"`})
	require.Equal(t, http.StatusNotFound, w.Code, "body: %s", w.Body)
	assert.Equal(t, "NoSuchKey", parseS3Error(t, w.Body.Bytes()).Code)

	// Neither spent the budget, so a write whose preconditions do hold gets the 409.
	w = s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("cas"),
		map[string]string{"If-Match": `"nomatch"`})
	require.Equal(t, http.StatusPreconditionFailed, w.Code,
		"sanity: a stale ETag is still a 412")

	etag := s3Request(t, srv, http.MethodHead, "/"+bucket+"/"+key, nil, nil).Header().Get("ETag")
	w = s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("cas"),
		map[string]string{"If-Match": etag})
	assert.Equal(t, http.StatusConflict, w.Code,
		"the budget must survive a request that failed its preconditions: %s", w.Body)
}

// TestS3_SeededConditionalConflict_CompleteMultipartUpload is the headline gate for
// #540. AWS documents the recovery from a 409 on Complete as re-initiating with
// CreateMultipartUpload and re-uploading every part, which is only meaningful if
// the old upload ID can no longer complete. A CAS loop that instead re-sends
// Complete with the same ID spins until it gives up, and that bug is invisible
// unless the ID really dies.
//
// This is also the case a FaultController rule cannot express: faults are
// evaluated before a request reaches its plugin, so a faulted Complete writes no
// state and its upload ID stays completable.
func TestS3_SeededConditionalConflict_CompleteMultipartUpload(t *testing.T) {
	const bucket, key = "conflict-mpu", "obj.bin"
	srv, uploadID := multipartFixture(t, bucket, key)
	etag := uploadPart(t, srv, bucket, key, uploadID, 1, []byte("payload"))
	seedConflict(t, srv, map[string]any{"bucket": bucket, "key": key, "completeConflicts": 1})

	w := s3Request(t, srv, http.MethodPost, "/"+bucket+"/"+key+"?uploadId="+uploadID,
		completeBody(etag), map[string]string{"If-None-Match": "*"})
	require.Equal(t, http.StatusConflict, w.Code, "body: %s", w.Body)
	assert.Equal(t, "ConditionalRequestConflict", parseS3Error(t, w.Body.Bytes()).Code)

	// The object was not written: a 409 says the write did not happen.
	assert.Equal(t, http.StatusNotFound,
		s3Request(t, srv, http.MethodGet, "/"+bucket+"/"+key, nil, nil).Code)

	// The upload ID is dead. This is the assertion the issue exists for.
	w = s3Request(t, srv, http.MethodPost, "/"+bucket+"/"+key+"?uploadId="+uploadID,
		completeBody(etag), map[string]string{"If-None-Match": "*"})
	require.Equal(t, http.StatusNotFound, w.Code,
		"retrying Complete with the same upload ID must not succeed: %s", w.Body)
	assert.Equal(t, "NoSuchUpload", parseS3Error(t, w.Body.Bytes()).Code)

	assert.Empty(t, openUploadIDs(t, srv, bucket),
		"a conflicted upload must be gone from the listing")
	assert.Equal(t, http.StatusNotFound,
		s3Request(t, srv, http.MethodGet, "/"+bucket+"/"+key+"?uploadId="+uploadID, nil, nil).Code,
		"ListParts on a conflicted upload must be NoSuchUpload")

	// And the documented recovery works: a fresh upload completes.
	iw := s3Request(t, srv, http.MethodPost, "/"+bucket+"/"+key+"?uploads", nil, nil)
	require.Equal(t, http.StatusOK, iw.Code)
	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))
	fresh := uploadPart(t, srv, bucket, key, ir.UploadID, 1, []byte("payload"))
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPost,
		"/"+bucket+"/"+key+"?uploadId="+ir.UploadID, completeBody(fresh),
		map[string]string{"If-None-Match": "*"}).Code)
}

// TestS3_SeededConditionalConflict_MPU412LeavesUploadOpen is the control for the
// test above: the 412 arm keeps its documented behavior, so the difference
// between the two really is the seeded conflict and not multipart Complete having
// become fragile.
func TestS3_SeededConditionalConflict_MPU412LeavesUploadOpen(t *testing.T) {
	const bucket, key = "conflict-mpu-412", "obj.bin"
	srv, uploadID := multipartFixture(t, bucket, key)
	etag := uploadPart(t, srv, bucket, key, uploadID, 1, []byte("payload"))
	putCondObject(t, srv, bucket, key, "existing")

	w := s3Request(t, srv, http.MethodPost, "/"+bucket+"/"+key+"?uploadId="+uploadID,
		completeBody(etag), map[string]string{"If-None-Match": "*"})
	require.Equal(t, http.StatusPreconditionFailed, w.Code, "body: %s", w.Body)

	assert.Equal(t, []string{uploadID}, openUploadIDs(t, srv, bucket),
		"a 412'd Complete must leave the upload open, unlike a 409")
}

// TestS3_SeededConditionalConflict_CopyObject covers the third arm. CopyObject's
// preconditions are evaluated against the destination, so the seed is keyed by the
// destination too.
func TestS3_SeededConditionalConflict_CopyObject(t *testing.T) {
	const bucket = "conflict-copy"
	srv := condFixture(t, bucket)
	putCondObject(t, srv, bucket, "src", "payload")
	seedConflict(t, srv, map[string]any{"bucket": bucket, "key": "dst", "copyConflicts": 1})

	headers := map[string]string{
		"x-amz-copy-source": "/" + bucket + "/src",
		"If-None-Match":     "*",
	}
	w := s3Request(t, srv, http.MethodPut, "/"+bucket+"/dst", nil, headers)
	require.Equal(t, http.StatusConflict, w.Code, "body: %s", w.Body)
	assert.Equal(t, "ConditionalRequestConflict", parseS3Error(t, w.Body.Bytes()).Code)
	assert.Equal(t, http.StatusNotFound,
		s3Request(t, srv, http.MethodGet, "/"+bucket+"/dst", nil, nil).Code,
		"a refused copy must not create the destination")

	w = s3Request(t, srv, http.MethodPut, "/"+bucket+"/dst", nil, headers)
	require.Equal(t, http.StatusOK, w.Code, "the retry must succeed: %s", w.Body)
	assert.Equal(t, "payload", getCondBody(t, srv, bucket, "dst"))
}

// TestS3_SeededConditionalConflict_CountersAreIndependent asserts the three
// counters do not share a budget: a fixture seeding the multipart arm — the one
// with the expensive recovery — must be able to do so without perturbing plain
// writes to the same key.
func TestS3_SeededConditionalConflict_CountersAreIndependent(t *testing.T) {
	const bucket, key = "conflict-independent", "obj.bin"
	srv, uploadID := multipartFixture(t, bucket, key)
	seedConflict(t, srv, map[string]any{"bucket": bucket, "key": key, "completeConflicts": 1})

	// A conditional PutObject has no budget of its own and is unaffected.
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("plain"),
			map[string]string{"If-None-Match": "*"}).Code,
		"a Put must not consume the Complete counter")

	// The Complete budget is intact. Delete the object first so the precondition
	// holds and the seed is the reason for the failure.
	require.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/"+bucket+"/"+key, nil, nil).Code)
	etag := uploadPart(t, srv, bucket, key, uploadID, 1, []byte("payload"))
	w := s3Request(t, srv, http.MethodPost, "/"+bucket+"/"+key+"?uploadId="+uploadID,
		completeBody(etag), map[string]string{"If-None-Match": "*"})
	assert.Equal(t, http.StatusConflict, w.Code, "body: %s", w.Body)
}

// TestS3_SeededConditionalConflict_WildcardAndPrecedence covers the two ordering
// rules the seed shares with every other seeded window: a key-scoped seed is
// consulted before the wildcard, and an **exhausted** key-scoped seed falls
// through rather than masking a wildcard that still has budget.
func TestS3_SeededConditionalConflict_WildcardAndPrecedence(t *testing.T) {
	const bucket = "conflict-wildcard"
	srv := condFixture(t, bucket)
	seedConflict(t, srv, map[string]any{"putConflicts": 2})
	seedConflict(t, srv, map[string]any{"bucket": bucket, "key": "scoped", "putConflicts": 1})

	put := func(key string) int {
		return s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("v"),
			map[string]string{"If-None-Match": "*"}).Code
	}

	// An unseeded key draws on the wildcard.
	assert.Equal(t, http.StatusConflict, put("other"), "wildcard applies to any key")

	// The key-scoped seed is consulted first and spends its own budget.
	assert.Equal(t, http.StatusConflict, put("scoped"), "key-scoped seed fires")

	// Now exhausted, it falls through to the wildcard's remaining budget rather
	// than masking it.
	assert.Equal(t, http.StatusConflict, put("scoped"),
		"an exhausted key-scoped seed must fall through to a funded wildcard")

	// Both are spent.
	assert.Equal(t, http.StatusOK, put("third"), "no budget left anywhere")
}

// TestS3_SeededConditionalConflict_ClearAndReseed covers the DELETE arm: clearing
// one seed leaves the others, and a bare DELETE clears everything including the
// wildcard.
func TestS3_SeededConditionalConflict_ClearAndReseed(t *testing.T) {
	const bucket = "conflict-clear"
	srv := condFixture(t, bucket)
	seedConflict(t, srv, map[string]any{"bucket": bucket, "key": "a", "putConflicts": 1})
	seedConflict(t, srv, map[string]any{"bucket": bucket, "key": "b", "putConflicts": 1})
	seedConflict(t, srv, map[string]any{"putConflicts": 1})

	put := func(key string) int {
		return s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("v"),
			map[string]string{"If-None-Match": "*"}).Code
	}

	w := ctrlRequest(t, srv, http.MethodDelete,
		fmt.Sprintf("/v1/s3/conditional-conflict?bucket=%s&key=a", bucket), nil)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body)

	// "a" was cleared, so it falls through to the wildcard rather than being
	// unseeded outright — which is the behavior that would hide a clear that did
	// nothing, so "b" is the assertion that the clear was targeted.
	assert.Equal(t, http.StatusConflict, put("a"), "falls through to the wildcard")
	assert.Equal(t, http.StatusConflict, put("b"), "an untargeted seed survives")

	require.Equal(t, http.StatusOK,
		ctrlRequest(t, srv, http.MethodDelete, "/v1/s3/conditional-conflict", nil).Code)
	assert.Equal(t, http.StatusOK, put("c"), "a bare DELETE clears the wildcard too")
}

// TestS3_SeededConditionalConflict_Endpoint_Rejects covers the request validation.
// The bucket-without-key case matters most: such a seed would be stored under a
// key no write can match, so it would look armed and never fire, and the fixture
// would pass vacuously.
func TestS3_SeededConditionalConflict_Endpoint_Rejects(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed json", body: `{`},
		{name: "negative put count", body: `{"bucket":"b","key":"k","putConflicts":-1}`},
		{name: "negative copy count", body: `{"bucket":"b","key":"k","copyConflicts":-1}`},
		{name: "negative complete count", body: `{"bucket":"b","key":"k","completeConflicts":-1}`},
		{name: "bucket without key", body: `{"bucket":"b","putConflicts":1}`},
		{name: "key without bucket", body: `{"key":"k","putConflicts":1}`},
	}

	srv := condFixture(t, "conflict-reject")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := ctrlRequest(t, srv, http.MethodPost, "/v1/s3/conditional-conflict", []byte(tt.body))
			assert.Equal(t, http.StatusBadRequest, w.Code, "body: %s", w.Body)
		})
	}

	// The DELETE arm rejects a half-specified target for the same reason: it would
	// silently clear nothing.
	w := ctrlRequest(t, srv, http.MethodDelete, "/v1/s3/conditional-conflict?bucket=b", nil)
	assert.Equal(t, http.StatusBadRequest, w.Code, "body: %s", w.Body)
}

// TestS3_SeededConditionalConflict_ErrorDocumentShape asserts the wire form. An
// SDK dispatches on Error.Code, and S3's document is a bare <Error> rather than
// the wrapped <ErrorResponse> the Query protocol uses — a wrapped body would
// arrive at the client as a bare "Conflict" from the HTTP status, and a consumer
// matching on ConditionalRequestConflict would never see their own seed.
func TestS3_SeededConditionalConflict_ErrorDocumentShape(t *testing.T) {
	const bucket, key = "conflict-shape", "obj"
	srv := condFixture(t, bucket)
	seedConflict(t, srv, map[string]any{"bucket": bucket, "key": key, "putConflicts": 1})

	w := s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("v"),
		map[string]string{"If-None-Match": "*"})
	require.Equal(t, http.StatusConflict, w.Code)

	body := w.Body.String()
	assert.True(t, strings.Contains(body, "<Error>"), "want a bare <Error> document: %s", body)
	assert.False(t, strings.Contains(body, "<ErrorResponse"),
		"the Query wrapper would hide the code from an SDK: %s", body)

	parsed := parseS3Error(t, w.Body.Bytes())
	assert.Equal(t, "ConditionalRequestConflict", parsed.Code)
	assert.NotEmpty(t, parsed.Message)
}
