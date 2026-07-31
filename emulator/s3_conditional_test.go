package emulator_test

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// condFixture creates a bucket and returns the server. The bucket is empty; each
// test PUTs whatever it needs, so no test inherits another's object state.
func condFixture(t *testing.T, bucket string) *emulator.Server {
	t.Helper()
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code, "create bucket")
	return srv
}

// putCondObject PUTs an object unconditionally and returns its ETag.
func putCondObject(t *testing.T, srv *emulator.Server, bucket, key, body string) string {
	t.Helper()
	w := s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte(body), nil)
	require.Equal(t, http.StatusOK, w.Code, "seed PUT: %s", w.Body)
	etag := w.Header().Get("ETag")
	require.NotEmpty(t, etag)
	return etag
}

// getCondBody returns an object's body, requiring the GET to succeed.
func getCondBody(t *testing.T, srv *emulator.Server, bucket, key string) string {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"/"+key, nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	return w.Body.String()
}

// TestS3_ConditionalWrite is the acceptance table for #397's write rows: the five
// PutObject outcomes S3 documents for If-None-Match and If-Match.
//
// Every case asserts the stored body afterwards, not just the status code. A 412
// that still overwrote the object would satisfy a status-only assertion while
// destroying exactly the data the header was sent to protect.
func TestS3_ConditionalWrite(t *testing.T) {
	const (
		bucket   = "cond-write"
		existing = "original"
		attempt  = "overwrite"
	)

	tests := []struct {
		name string
		// seed is written first; "" leaves the key absent.
		seed string
		// header returns the precondition header, given the seeded object's ETag
		// ("" when nothing was seeded).
		header     func(etag string) map[string]string
		wantStatus int
		wantCode   string
		// wantBody is the body expected after the attempt. "" means the key must
		// still not exist.
		wantBody string
	}{
		{
			name: "If-None-Match star, key absent",
			seed: "",
			header: func(string) map[string]string {
				return map[string]string{"If-None-Match": "*"}
			},
			wantStatus: http.StatusOK,
			wantBody:   attempt,
		},
		{
			name: "If-None-Match star, key present",
			seed: existing,
			header: func(string) map[string]string {
				return map[string]string{"If-None-Match": "*"}
			},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
			wantBody:   existing,
		},
		{
			name: "If-Match matching etag",
			seed: existing,
			header: func(etag string) map[string]string {
				return map[string]string{"If-Match": etag}
			},
			wantStatus: http.StatusOK,
			wantBody:   attempt,
		},
		{
			name: "If-Match differing etag",
			seed: existing,
			header: func(string) map[string]string {
				return map[string]string{"If-Match": `"00000000000000000000000000000000"`}
			},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
			wantBody:   existing,
		},
		{
			// "If there's no current object version with the same name … the
			// operation fails with a 404 Not Found error." Not a 412: there is
			// nothing to compare the ETag against.
			name: "If-Match, key absent",
			seed: "",
			header: func(string) map[string]string {
				return map[string]string{"If-Match": `"00000000000000000000000000000000"`}
			},
			wantStatus: http.StatusNotFound,
			wantCode:   "NoSuchKey",
			wantBody:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := condFixture(t, bucket)

			var etag string
			if tt.seed != "" {
				etag = putCondObject(t, srv, bucket, "obj.txt", tt.seed)
			}

			w := s3Request(t, srv, http.MethodPut, "/"+bucket+"/obj.txt",
				[]byte(attempt), tt.header(etag))
			require.Equal(t, tt.wantStatus, w.Code, "body: %s", w.Body)

			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, parseS3Error(t, w.Body.Bytes()).Code)
			}

			gw := s3Request(t, srv, http.MethodGet, "/"+bucket+"/obj.txt", nil, nil)
			if tt.wantBody == "" {
				assert.Equal(t, http.StatusNotFound, gw.Code,
					"a rejected conditional write must not create the object")
				return
			}
			require.Equal(t, http.StatusOK, gw.Code)
			assert.Equal(t, tt.wantBody, gw.Body.String(),
				"stored body after a %d", tt.wantStatus)
		})
	}
}

// TestS3_ConditionalWrite_412LeavesObjectByteIdentical is the assertion the issue
// names explicitly. It is stronger than the table's body check: every observable
// field of the object — ETag, size, Last-Modified, user metadata — must survive the
// rejected write untouched, since a consumer that retries on 412 will compare them.
func TestS3_ConditionalWrite_412LeavesObjectByteIdentical(t *testing.T) {
	srv := condFixture(t, "cond-identical")

	seedHeaders := map[string]string{
		"Content-Type":      "text/plain",
		"x-amz-meta-origin": "seed",
	}
	pw := s3Request(t, srv, http.MethodPut, "/cond-identical/obj.txt",
		[]byte("original contents"), seedHeaders)
	require.Equal(t, http.StatusOK, pw.Code)

	before := s3Request(t, srv, http.MethodGet, "/cond-identical/obj.txt", nil, nil)
	require.Equal(t, http.StatusOK, before.Code)

	w := s3Request(t, srv, http.MethodPut, "/cond-identical/obj.txt",
		[]byte("a completely different body of another length"), map[string]string{
			"If-None-Match":     "*",
			"Content-Type":      "application/json",
			"x-amz-meta-origin": "clobber",
		})
	require.Equal(t, http.StatusPreconditionFailed, w.Code)

	after := s3Request(t, srv, http.MethodGet, "/cond-identical/obj.txt", nil, nil)
	require.Equal(t, http.StatusOK, after.Code)

	assert.Equal(t, before.Body.String(), after.Body.String(), "body")
	for _, h := range []string{
		"ETag", "Content-Length", "Content-Type", "Last-Modified", "X-Amz-Meta-Origin",
	} {
		assert.Equal(t, before.Header().Get(h), after.Header().Get(h), "header %s", h)
	}
	assert.Equal(t, "seed", after.Header().Get("X-Amz-Meta-Origin"),
		"the rejected write's metadata must not have been applied")
}

// TestS3_ConditionalWrite_ExactlyOneWinner is the concurrency assertion from the
// issue: N parallel `If-None-Match: *` PUTs of distinct bodies must produce exactly
// one 200 and N-1 412s, and the object must hold the winner's body.
//
// This is the property [emulator.StateManager] cannot provide on its own — it has
// no compare-and-swap, and MemoryStateManager is last-write-wins — so without the
// plugin's per-key lock held across check→Put every writer would observe the key as
// absent and all N would succeed. Run under -race.
func TestS3_ConditionalWrite_ExactlyOneWinner(t *testing.T) {
	const writers = 32

	srv := condFixture(t, "cond-race")

	type outcome struct {
		status int
		body   string
	}
	results := make([]outcome, writers)

	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup

	for i := range writers {
		done.Add(1)
		go func() {
			defer done.Done()
			body := fmt.Sprintf("writer-%02d", i)
			start.Wait() // release all writers at once to maximize overlap
			w := s3Request(t, srv, http.MethodPut, "/cond-race/obj.txt",
				[]byte(body), map[string]string{"If-None-Match": "*"})
			results[i] = outcome{status: w.Code, body: body}
		}()
	}
	start.Done()
	done.Wait()

	var winners []string
	for _, r := range results {
		switch r.status {
		case http.StatusOK:
			winners = append(winners, r.body)
		case http.StatusPreconditionFailed:
		default:
			t.Errorf("unexpected status %d from a concurrent conditional PUT", r.status)
		}
	}

	require.Len(t, winners, 1,
		"exactly one of %d concurrent If-None-Match: * PUTs may succeed", writers)
	assert.Equal(t, winners[0], getCondBody(t, srv, "cond-race", "obj.txt"),
		"the stored object must be the winner's body")
}

// TestS3_ConditionalWrite_IfMatchExactlyOneWinner is the compare-and-swap form of
// the same race: every writer asserts the ETag it read, so only the first may land.
// A consumer implementing optimistic locking depends on precisely this.
func TestS3_ConditionalWrite_IfMatchExactlyOneWinner(t *testing.T) {
	const writers = 32

	srv := condFixture(t, "cond-cas")
	etag := putCondObject(t, srv, "cond-cas", "obj.txt", "generation-0")

	statuses := make([]int, writers)

	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup

	for i := range writers {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			w := s3Request(t, srv, http.MethodPut, "/cond-cas/obj.txt",
				[]byte(fmt.Sprintf("generation-1-by-%02d", i)),
				map[string]string{"If-Match": etag})
			statuses[i] = w.Code
		}()
	}
	start.Done()
	done.Wait()

	ok := 0
	for _, s := range statuses {
		switch s {
		case http.StatusOK:
			ok++
		case http.StatusPreconditionFailed:
		default:
			t.Errorf("unexpected status %d from a concurrent If-Match PUT", s)
		}
	}
	assert.Equal(t, 1, ok, "exactly one If-Match writer may win the ETag it read")
}

// TestS3_ConditionalWrite_DeleteMarkerCountsAsAbsent covers the versioned rule:
// "if the current object version is a delete marker, the write operation succeeds"
// for If-None-Match, and fails with 404 for If-Match. A delete marker is not an
// object, so a key that once held data behaves as a free key again.
func TestS3_ConditionalWrite_DeleteMarkerCountsAsAbsent(t *testing.T) {
	tests := []struct {
		name       string
		header     map[string]string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "If-None-Match star succeeds",
			header:     map[string]string{"If-None-Match": "*"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "If-Match is a 404",
			header:     map[string]string{"If-Match": `"00000000000000000000000000000000"`},
			wantStatus: http.StatusNotFound,
			wantCode:   "NoSuchKey",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := condFixture(t, "cond-marker")
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
				"/cond-marker?versioning",
				[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`),
				map[string]string{"Content-Type": "application/xml"}).Code)

			putCondObject(t, srv, "cond-marker", "obj.txt", "v1")
			require.Equal(t, http.StatusNoContent,
				s3Request(t, srv, http.MethodDelete, "/cond-marker/obj.txt", nil, nil).Code,
				"delete must insert a delete marker")

			w := s3Request(t, srv, http.MethodPut, "/cond-marker/obj.txt",
				[]byte("v2"), tt.header)
			require.Equal(t, tt.wantStatus, w.Code, "body: %s", w.Body)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, parseS3Error(t, w.Body.Bytes()).Code)
			}
		})
	}
}

// TestS3_ConditionalWrite_IfNoneMatchNonStar asserts that a non-"*" If-None-Match
// on a write is rejected rather than ignored. S3 documents the header as expecting
// "*"; treating an unsupported value as "no condition" would let a header sent to
// prevent an overwrite silently permit one, which is the failure mode this whole
// issue is about.
func TestS3_ConditionalWrite_IfNoneMatchNonStar(t *testing.T) {
	srv := condFixture(t, "cond-nonstar")
	etag := putCondObject(t, srv, "cond-nonstar", "obj.txt", "original")

	for _, value := range []string{etag, `"00000000000000000000000000000000"`, ""} {
		t.Run("value="+value, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodPut, "/cond-nonstar/obj.txt",
				[]byte("overwrite"), map[string]string{"If-None-Match": value})
			assert.Equal(t, http.StatusPreconditionFailed, w.Code, "body: %s", w.Body)
			assert.Equal(t, "original", getCondBody(t, srv, "cond-nonstar", "obj.txt"))
		})
	}
}

// TestS3_ConditionalRead is the acceptance table for #397's read rows, run against
// both GET and HEAD since S3 evaluates preconditions identically for the two.
func TestS3_ConditionalRead(t *testing.T) {
	const bucket = "cond-read"

	tests := []struct {
		name       string
		header     func(etag string) map[string]string
		wantStatus int
		wantCode   string
	}{
		{
			name: "If-None-Match matching etag",
			header: func(etag string) map[string]string {
				return map[string]string{"If-None-Match": etag}
			},
			wantStatus: http.StatusNotModified,
		},
		{
			name: "If-None-Match star on an existing object",
			header: func(string) map[string]string {
				return map[string]string{"If-None-Match": "*"}
			},
			wantStatus: http.StatusNotModified,
		},
		{
			name: "If-None-Match differing etag",
			header: func(string) map[string]string {
				return map[string]string{"If-None-Match": `"00000000000000000000000000000000"`}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "If-Match differing etag",
			header: func(string) map[string]string {
				return map[string]string{"If-Match": `"00000000000000000000000000000000"`}
			},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
		{
			name: "If-Match matching etag",
			header: func(etag string) map[string]string {
				return map[string]string{"If-Match": etag}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "If-Match star on an existing object",
			header: func(string) map[string]string {
				return map[string]string{"If-Match": "*"}
			},
			wantStatus: http.StatusOK,
		},
		{
			// The object's Last-Modified is the pinned test clock, well after 1990.
			name: "If-Modified-Since in the past",
			header: func(string) map[string]string {
				return map[string]string{"If-Modified-Since": "Mon, 01 Jan 1990 00:00:00 GMT"}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "If-Modified-Since in the future",
			header: func(string) map[string]string {
				return map[string]string{"If-Modified-Since": "Fri, 01 Jan 2100 00:00:00 GMT"}
			},
			wantStatus: http.StatusNotModified,
		},
		{
			name: "If-Unmodified-Since in the future",
			header: func(string) map[string]string {
				return map[string]string{"If-Unmodified-Since": "Fri, 01 Jan 2100 00:00:00 GMT"}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "If-Unmodified-Since in the past",
			header: func(string) map[string]string {
				return map[string]string{"If-Unmodified-Since": "Mon, 01 Jan 1990 00:00:00 GMT"}
			},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
		{
			// RFC 9110: an unparseable date makes the condition inapplicable, not
			// failed. A spurious 412 here would break a client with a broken clock
			// formatter in a way that looks like a server bug.
			name: "unparseable date is ignored",
			header: func(string) map[string]string {
				return map[string]string{"If-Unmodified-Since": "not a date at all"}
			},
			wantStatus: http.StatusOK,
		},
		{
			// An empty value is a condition that cannot be met, distinct from an
			// absent header. Treating it as absent would serve the object to a
			// client whose ETag-building code produced nothing.
			name: "empty If-Match",
			header: func(string) map[string]string {
				return map[string]string{"If-Match": ""}
			},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
		{
			name: "empty If-None-Match",
			header: func(string) map[string]string {
				return map[string]string{"If-None-Match": ""}
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					srv := condFixture(t, bucket)
					etag := putCondObject(t, srv, bucket, "obj.txt", "contents")

					w := s3Request(t, srv, method, "/"+bucket+"/obj.txt", nil, tt.header(etag))
					require.Equal(t, tt.wantStatus, w.Code, "body: %s", w.Body)

					switch tt.wantStatus {
					case http.StatusNotModified:
						// 304 carries no body and must echo the ETag so a client can
						// confirm which entity it was told it already holds.
						assert.Zero(t, w.Body.Len(), "304 must carry no body")
						assert.Equal(t, etag, w.Header().Get("ETag"))
					case http.StatusPreconditionFailed:
						assert.Equal(t, tt.wantCode, parseS3Error(t, w.Body.Bytes()).Code)
					case http.StatusOK:
						assert.Equal(t, etag, w.Header().Get("ETag"))
					}
				})
			}
		})
	}
}

// TestS3_ConditionalRead_CombinationRules covers the two combinations S3 documents
// explicitly, both of which exist to stop a coarse date condition from overriding an
// exact entity assertion. Getting either backwards silently inverts a cache
// revalidation, so they are asserted directly rather than inferred from the table.
func TestS3_ConditionalRead_CombinationRules(t *testing.T) {
	const (
		past   = "Mon, 01 Jan 1990 00:00:00 GMT"
		future = "Fri, 01 Jan 2100 00:00:00 GMT"
	)

	tests := []struct {
		name       string
		header     func(etag string) map[string]string
		wantStatus int
		reason     string
	}{
		{
			// "If-Match condition evaluates to true, and If-Unmodified-Since
			// condition evaluates to false; then, S3 returns 200 OK and the data
			// requested."
			name: "If-Match true and If-Unmodified-Since false is 200",
			header: func(etag string) map[string]string {
				return map[string]string{"If-Match": etag, "If-Unmodified-Since": past}
			},
			wantStatus: http.StatusOK,
			reason:     "an exact ETag match outranks a failing If-Unmodified-Since",
		},
		{
			// "If-None-Match condition evaluates to false, and If-Modified-Since
			// condition evaluates to true; then, S3 returns 304 Not Modified."
			name: "If-None-Match false and If-Modified-Since true is 304",
			header: func(etag string) map[string]string {
				return map[string]string{"If-None-Match": etag, "If-Modified-Since": past}
			},
			wantStatus: http.StatusNotModified,
			reason:     "the caller already holds this entity regardless of dates",
		},
		{
			name: "If-Match false still fails despite a satisfied If-Unmodified-Since",
			header: func(string) map[string]string {
				return map[string]string{
					"If-Match":            `"00000000000000000000000000000000"`,
					"If-Unmodified-Since": future,
				}
			},
			wantStatus: http.StatusPreconditionFailed,
			reason:     "a failed ETag assertion is not rescued by a date condition",
		},
		{
			name: "If-None-Match true suppresses If-Modified-Since",
			header: func(string) map[string]string {
				return map[string]string{
					"If-None-Match":     `"00000000000000000000000000000000"`,
					"If-Modified-Since": future,
				}
			},
			wantStatus: http.StatusOK,
			reason:     "the caller does not hold this entity, so it must be sent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := condFixture(t, "cond-combo")
			etag := putCondObject(t, srv, "cond-combo", "obj.txt", "contents")

			w := s3Request(t, srv, http.MethodGet, "/cond-combo/obj.txt", nil, tt.header(etag))
			assert.Equal(t, tt.wantStatus, w.Code, "%s (body: %s)", tt.reason, w.Body)
		})
	}
}

// TestS3_ConditionalRead_PrecedesRange asserts the ordering that made #397 depend on
// #396: a failed precondition supersedes a Range, so a 412 or 304 is returned rather
// than a 206 of a stale entity. A client that got a partial body here would splice
// bytes from the wrong generation of the object into its cache.
func TestS3_ConditionalRead_PrecedesRange(t *testing.T) {
	srv := condFixture(t, "cond-range")
	etag := putCondObject(t, srv, "cond-range", "obj.txt", strings.Repeat("x", 1000))

	t.Run("failed If-Match beats a valid range", func(t *testing.T) {
		w := s3Request(t, srv, http.MethodGet, "/cond-range/obj.txt", nil, map[string]string{
			"If-Match": `"00000000000000000000000000000000"`,
			"Range":    "bytes=0-99",
		})
		require.Equal(t, http.StatusPreconditionFailed, w.Code)
		assert.Empty(t, w.Header().Get("Content-Range"), "no partial response may be described")
	})

	t.Run("If-None-Match match beats a valid range", func(t *testing.T) {
		w := s3Request(t, srv, http.MethodGet, "/cond-range/obj.txt", nil, map[string]string{
			"If-None-Match": etag,
			"Range":         "bytes=0-99",
		})
		require.Equal(t, http.StatusNotModified, w.Code)
		assert.Zero(t, w.Body.Len())
	})

	t.Run("failed If-Match beats an unsatisfiable range", func(t *testing.T) {
		// Both conditions would produce an error; the precondition is reported.
		w := s3Request(t, srv, http.MethodGet, "/cond-range/obj.txt", nil, map[string]string{
			"If-Match": `"00000000000000000000000000000000"`,
			"Range":    "bytes=5000-5099",
		})
		require.Equal(t, http.StatusPreconditionFailed, w.Code)
		assert.Equal(t, "PreconditionFailed", parseS3Error(t, w.Body.Bytes()).Code)
	})

	t.Run("satisfied precondition still serves the range", func(t *testing.T) {
		w := s3Request(t, srv, http.MethodGet, "/cond-range/obj.txt", nil, map[string]string{
			"If-Match": etag,
			"Range":    "bytes=0-99",
		})
		require.Equal(t, http.StatusPartialContent, w.Code)
		assert.Equal(t, "bytes 0-99/1000", w.Header().Get("Content-Range"))
		assert.Equal(t, 100, w.Body.Len())
	})
}

// TestS3_ConditionalRead_AbsentKeyIsNoSuchKey asserts a precondition does not turn a
// missing object into a 412 or 304: there is no ETag to compare, so the caller's
// real problem is the missing key.
func TestS3_ConditionalRead_AbsentKeyIsNoSuchKey(t *testing.T) {
	srv := condFixture(t, "cond-absent")

	tests := []struct {
		name   string
		header map[string]string
	}{
		{name: "If-Match", header: map[string]string{"If-Match": `"00000000000000000000000000000000"`}},
		{name: "If-None-Match", header: map[string]string{"If-None-Match": "*"}},
		{
			name:   "If-Modified-Since",
			header: map[string]string{"If-Modified-Since": "Mon, 01 Jan 1990 00:00:00 GMT"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodGet, "/cond-absent/missing.txt", nil, tt.header)
			require.Equal(t, http.StatusNotFound, w.Code)
			assert.Equal(t, "NoSuchKey", parseS3Error(t, w.Body.Bytes()).Code)
		})
	}
}

// TestS3_ConditionalRead_ETagFormsAndLists asserts the ETag comparison is
// quoting-, case- and list-insensitive on reads. Clients differ on whether they
// echo back the quotes S3 sent, and RFC 7232 allows a list of validators.
func TestS3_ConditionalRead_ETagFormsAndLists(t *testing.T) {
	srv := condFixture(t, "cond-etag")
	etag := putCondObject(t, srv, "cond-etag", "obj.txt", "contents")
	bare := strings.Trim(etag, `"`)

	tests := []struct {
		name  string
		value string
	}{
		{name: "as returned", value: etag},
		{name: "unquoted", value: bare},
		{name: "uppercase hex", value: strings.ToUpper(etag)},
		{name: "weak validator", value: `W/` + etag},
		{name: "list, first member", value: etag + `,"00000000000000000000000000000000"`},
		{name: "list, later member", value: `"00000000000000000000000000000000", ` + etag},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A matching If-None-Match is a 304; the same value as If-Match is a 200.
			// Asserting both proves the value was recognized as a match, rather than
			// having produced the right status for the wrong reason.
			nm := s3Request(t, srv, http.MethodGet, "/cond-etag/obj.txt", nil,
				map[string]string{"If-None-Match": tt.value})
			assert.Equal(t, http.StatusNotModified, nm.Code, "If-None-Match: %s", tt.value)

			m := s3Request(t, srv, http.MethodGet, "/cond-etag/obj.txt", nil,
				map[string]string{"If-Match": tt.value})
			assert.Equal(t, http.StatusOK, m.Code, "If-Match: %s (body: %s)", tt.value, m.Body)
		})
	}
}

// TestS3_ConditionalRead_DateFormats asserts the three date formats RFC 9110
// requires a recipient to accept. An SDK sending RFC 850 must not get a spurious
// 200 where a 304 was correct — that would defeat its cache silently.
func TestS3_ConditionalRead_DateFormats(t *testing.T) {
	srv := condFixture(t, "cond-dates")
	putCondObject(t, srv, "cond-dates", "obj.txt", "contents")

	// All three are after the pinned test clock of 2026-01-01, so If-Modified-Since
	// evaluates false and each must yield a 304. RFC 850's two-digit year cannot
	// express 2100, hence 2027 for that row.
	tests := []struct {
		name  string
		value string
	}{
		{name: "IMF-fixdate", value: "Fri, 01 Jan 2100 00:00:00 GMT"},
		{name: "RFC 850", value: "Friday, 01-Jan-27 00:00:00 GMT"},
		{name: "ANSI C asctime", value: "Fri Jan  1 00:00:00 2100"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodGet, "/cond-dates/obj.txt", nil,
				map[string]string{"If-Modified-Since": tt.value})
			assert.Equal(t, http.StatusNotModified, w.Code, "value %q: %s", tt.value, w.Body)
		})
	}
}

// TestS3_ConditionalRead_HeaderCaseInsensitive asserts the headers are matched
// case-insensitively. Tests elsewhere construct AWSRequest literals directly and so
// bypass net/http's canonicalization; a plugin reading req.Headers["If-Match"] would
// pass those and still miss a real client sending "if-match".
func TestS3_ConditionalRead_HeaderCaseInsensitive(t *testing.T) {
	srv := condFixture(t, "cond-case")
	etag := putCondObject(t, srv, "cond-case", "obj.txt", "contents")

	for _, name := range []string{"if-none-match", "IF-NONE-MATCH", "If-None-Match"} {
		t.Run(name, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodGet, "/cond-case/obj.txt", nil,
				map[string]string{name: etag})
			assert.Equal(t, http.StatusNotModified, w.Code)
		})
	}
}

// TestS3_ConditionalCopy_Destination asserts CopyObject honors the destination
// preconditions, which is what makes a copy usable as an atomic publish step.
func TestS3_ConditionalCopy_Destination(t *testing.T) {
	tests := []struct {
		name string
		// seedDest is written at the destination first; "" leaves it absent.
		seedDest   string
		header     func(destETag string) map[string]string
		wantStatus int
		wantCode   string
	}{
		{
			name:     "If-None-Match star, destination absent",
			seedDest: "",
			header: func(string) map[string]string {
				return map[string]string{"If-None-Match": "*"}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:     "If-None-Match star, destination present",
			seedDest: "existing",
			header: func(string) map[string]string {
				return map[string]string{"If-None-Match": "*"}
			},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
		{
			name:     "If-Match on the destination etag",
			seedDest: "existing",
			header: func(destETag string) map[string]string {
				return map[string]string{"If-Match": destETag}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:     "If-Match differing from the destination etag",
			seedDest: "existing",
			header: func(string) map[string]string {
				return map[string]string{"If-Match": `"00000000000000000000000000000000"`}
			},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := condFixture(t, "cond-copy")
			putCondObject(t, srv, "cond-copy", "src.txt", "source payload")

			var destETag string
			if tt.seedDest != "" {
				destETag = putCondObject(t, srv, "cond-copy", "dst.txt", tt.seedDest)
			}

			headers := tt.header(destETag)
			headers["X-Amz-Copy-Source"] = "/cond-copy/src.txt"
			w := s3Request(t, srv, http.MethodPut, "/cond-copy/dst.txt", nil, headers)
			require.Equal(t, tt.wantStatus, w.Code, "body: %s", w.Body)

			if tt.wantStatus == http.StatusOK {
				assert.Equal(t, "source payload", getCondBody(t, srv, "cond-copy", "dst.txt"))
				return
			}
			assert.Equal(t, tt.wantCode, parseS3Error(t, w.Body.Bytes()).Code)
			if tt.seedDest != "" {
				assert.Equal(t, tt.seedDest, getCondBody(t, srv, "cond-copy", "dst.txt"),
					"a rejected copy must leave the destination untouched")
			}
		})
	}
}

// TestS3_ConditionalCopy_Source asserts the x-amz-copy-source-if-* family, which
// gates reading the source rather than overwriting the destination. Every failure is
// a 412: CopyObject documents no 304, because there is no cached entity for a
// server-side copy to revalidate against.
func TestS3_ConditionalCopy_Source(t *testing.T) {
	tests := []struct {
		name       string
		header     func(srcETag string) map[string]string
		wantStatus int
	}{
		{
			name: "copy-source-if-match matching",
			header: func(etag string) map[string]string {
				return map[string]string{"x-amz-copy-source-if-match": etag}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "copy-source-if-match differing",
			header: func(string) map[string]string {
				return map[string]string{
					"x-amz-copy-source-if-match": `"00000000000000000000000000000000"`,
				}
			},
			wantStatus: http.StatusPreconditionFailed,
		},
		{
			name: "copy-source-if-none-match differing",
			header: func(string) map[string]string {
				return map[string]string{
					"x-amz-copy-source-if-none-match": `"00000000000000000000000000000000"`,
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			// The equivalent GET would be a 304. A copy is a 412.
			name: "copy-source-if-none-match matching",
			header: func(etag string) map[string]string {
				return map[string]string{"x-amz-copy-source-if-none-match": etag}
			},
			wantStatus: http.StatusPreconditionFailed,
		},
		{
			name: "copy-source-if-unmodified-since in the future",
			header: func(string) map[string]string {
				return map[string]string{
					"x-amz-copy-source-if-unmodified-since": "Fri, 01 Jan 2100 00:00:00 GMT",
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "copy-source-if-unmodified-since in the past",
			header: func(string) map[string]string {
				return map[string]string{
					"x-amz-copy-source-if-unmodified-since": "Mon, 01 Jan 1990 00:00:00 GMT",
				}
			},
			wantStatus: http.StatusPreconditionFailed,
		},
		{
			name: "copy-source-if-modified-since in the past",
			header: func(string) map[string]string {
				return map[string]string{
					"x-amz-copy-source-if-modified-since": "Mon, 01 Jan 1990 00:00:00 GMT",
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "copy-source-if-modified-since in the future",
			header: func(string) map[string]string {
				return map[string]string{
					"x-amz-copy-source-if-modified-since": "Fri, 01 Jan 2100 00:00:00 GMT",
				}
			},
			wantStatus: http.StatusPreconditionFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := condFixture(t, "cond-copysrc")
			srcETag := putCondObject(t, srv, "cond-copysrc", "src.txt", "source payload")

			headers := tt.header(srcETag)
			headers["X-Amz-Copy-Source"] = "/cond-copysrc/src.txt"
			w := s3Request(t, srv, http.MethodPut, "/cond-copysrc/dst.txt", nil, headers)
			require.Equal(t, tt.wantStatus, w.Code, "body: %s", w.Body)

			if tt.wantStatus == http.StatusOK {
				assert.Equal(t, "source payload", getCondBody(t, srv, "cond-copysrc", "dst.txt"))
				return
			}
			assert.Equal(t, "PreconditionFailed", parseS3Error(t, w.Body.Bytes()).Code)
			assert.Equal(t, http.StatusNotFound,
				s3Request(t, srv, http.MethodHead, "/cond-copysrc/dst.txt", nil, nil).Code,
				"a rejected copy must not create the destination")
		})
	}
}

// TestS3_ConditionalCopy_SourceAndDestinationBoth asserts both families can be sent
// on one request, and that the source is evaluated even when the destination
// condition would pass.
func TestS3_ConditionalCopy_SourceAndDestinationBoth(t *testing.T) {
	srv := condFixture(t, "cond-copyboth")
	srcETag := putCondObject(t, srv, "cond-copyboth", "src.txt", "source payload")

	w := s3Request(t, srv, http.MethodPut, "/cond-copyboth/dst.txt", nil, map[string]string{
		"X-Amz-Copy-Source":          "/cond-copyboth/src.txt",
		"x-amz-copy-source-if-match": `"00000000000000000000000000000000"`,
		"If-None-Match":              "*", // would pass: the destination is absent
	})
	require.Equal(t, http.StatusPreconditionFailed, w.Code, "body: %s", w.Body)
	assert.Equal(t, http.StatusNotFound,
		s3Request(t, srv, http.MethodHead, "/cond-copyboth/dst.txt", nil, nil).Code)

	ok := s3Request(t, srv, http.MethodPut, "/cond-copyboth/dst.txt", nil, map[string]string{
		"X-Amz-Copy-Source":          "/cond-copyboth/src.txt",
		"x-amz-copy-source-if-match": srcETag,
		"If-None-Match":              "*",
	})
	require.Equal(t, http.StatusOK, ok.Code, "body: %s", ok.Body)
	assert.Equal(t, "source payload", getCondBody(t, srv, "cond-copyboth", "dst.txt"))
}

// TestS3_ConditionalCompleteMultipartUpload asserts Complete honors the same
// conditional write headers as PutObject, per the conditional-writes documentation
// listing it alongside PutObject and CopyObject. A rejected Complete must leave the
// upload open, so a caller can abort it after losing the race.
func TestS3_ConditionalCompleteMultipartUpload(t *testing.T) {
	tests := []struct {
		name string
		// seed is written at the key before Complete; "" leaves it absent.
		seed       string
		header     func(seedETag string) map[string]string
		wantStatus int
		wantCode   string
	}{
		{
			name: "If-None-Match star, key absent",
			seed: "",
			header: func(string) map[string]string {
				return map[string]string{"If-None-Match": "*"}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "If-None-Match star, key written mid-upload",
			seed: "raced in by another writer",
			header: func(string) map[string]string {
				return map[string]string{"If-None-Match": "*"}
			},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
		{
			name: "If-Match matching",
			seed: "existing",
			header: func(etag string) map[string]string {
				return map[string]string{"If-Match": etag}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "If-Match differing",
			seed: "existing",
			header: func(string) map[string]string {
				return map[string]string{"If-Match": `"00000000000000000000000000000000"`}
			},
			wantStatus: http.StatusPreconditionFailed,
			wantCode:   "PreconditionFailed",
		},
		{
			name: "If-Match, key absent",
			seed: "",
			header: func(string) map[string]string {
				return map[string]string{"If-Match": `"00000000000000000000000000000000"`}
			},
			wantStatus: http.StatusNotFound,
			wantCode:   "NoSuchKey",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, uploadID := multipartFixture(t, "cond-mpu", "obj.bin")
			partETag := uploadPart(t, srv, "cond-mpu", "obj.bin", uploadID, 1, []byte("assembled"))

			var seedETag string
			if tt.seed != "" {
				seedETag = putCondObject(t, srv, "cond-mpu", "obj.bin", tt.seed)
			}

			w := s3Request(t, srv, http.MethodPost, "/cond-mpu/obj.bin?uploadId="+uploadID,
				completeBody(partETag), tt.header(seedETag))
			require.Equal(t, tt.wantStatus, w.Code, "body: %s", w.Body)

			if tt.wantStatus == http.StatusOK {
				assert.Equal(t, "assembled", getCondBody(t, srv, "cond-mpu", "obj.bin"))
				assert.Empty(t, openUploadIDs(t, srv, "cond-mpu"),
					"a successful Complete closes the upload")
				return
			}

			assert.Equal(t, tt.wantCode, parseS3Error(t, w.Body.Bytes()).Code)
			assert.Equal(t, []string{uploadID}, openUploadIDs(t, srv, "cond-mpu"),
				"a rejected Complete must leave the upload open to be aborted")
			if tt.seed != "" {
				assert.Equal(t, tt.seed, getCondBody(t, srv, "cond-mpu", "obj.bin"),
					"a rejected Complete must not overwrite the raced-in object")
			}
		})
	}
}

// TestS3_ConditionalCompleteMultipartUpload_MalformedBeatsPrecondition asserts a
// malformed parts list is reported as malformed even when the precondition would
// also fail. A caller that saw PreconditionFailed here would retry the upload
// forever instead of fixing its request.
func TestS3_ConditionalCompleteMultipartUpload_MalformedBeatsPrecondition(t *testing.T) {
	srv, uploadID := multipartFixture(t, "cond-mpu-xml", "obj.bin")
	uploadPart(t, srv, "cond-mpu-xml", "obj.bin", uploadID, 1, []byte("assembled"))
	putCondObject(t, srv, "cond-mpu-xml", "obj.bin", "already here")

	w := s3Request(t, srv, http.MethodPost, "/cond-mpu-xml/obj.bin?uploadId="+uploadID,
		[]byte(`<CompleteMultipartUpload></CompleteMultipartUpload>`),
		map[string]string{"If-None-Match": "*"})
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "MalformedXML", parseS3Error(t, w.Body.Bytes()).Code)
}

// TestS3_ConditionalWrite_UnconditionalPathUnaffected is the regression guard: an
// ordinary PUT/GET carrying no precondition must be untouched by all of the above.
func TestS3_ConditionalWrite_UnconditionalPathUnaffected(t *testing.T) {
	srv := condFixture(t, "cond-plain")

	first := putCondObject(t, srv, "cond-plain", "obj.txt", "first")
	second := putCondObject(t, srv, "cond-plain", "obj.txt", "second")
	assert.NotEqual(t, first, second, "an unconditional PUT still overwrites")
	assert.Equal(t, "second", getCondBody(t, srv, "cond-plain", "obj.txt"))

	w := s3Request(t, srv, http.MethodGet, "/cond-plain/obj.txt", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, second, w.Header().Get("ETag"))
	// Last-Modified is the pinned clock, proving the object was really rewritten
	// rather than served from the first generation.
	assert.NotEmpty(t, w.Header().Get("Last-Modified"))
	_, parseErr := time.Parse(http.TimeFormat, w.Header().Get("Last-Modified"))
	assert.NoError(t, parseErr, "Last-Modified must be an HTTP date")
}
