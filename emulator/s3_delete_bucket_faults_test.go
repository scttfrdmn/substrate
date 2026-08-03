package emulator_test

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Fault-injection tests for #508's propagation rule: every store or filesystem
// failure along DeleteBucket's clear, and along the current-version promotion, must
// surface as a 500 and leave the bucket present rather than being swallowed into a
// 204 that reports a teardown which did not happen. A partial clear is precisely
// the bug being fixed, so a discarded error here would hide a recurrence.
//
// [pabFaultState] in s3_publicaccessblock_test.go targets a single exact key and
// passes List straight through. These paths scan by prefix and delete keys whose
// names are generated at request time, so the store here matches on a prefix and
// can fail List too.

// s3PrefixFaultState fails one operation for every key under a prefix once armed,
// leaving every other key working. Arming is deferred for the same reason
// [pabFaultState] defers it: setting the bucket up touches the keys under test, so
// a store that fails from construction never gets a bucket to delete.
type s3PrefixFaultState struct {
	inner    emulator.StateManager
	prefix   string
	err      error
	armed    bool
	onGet    bool
	onPut    bool
	onDelete bool
	onList   bool
}

func (m *s3PrefixFaultState) fails(key string, op bool) bool {
	return m.armed && op && strings.HasPrefix(key, m.prefix)
}

func (m *s3PrefixFaultState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.fails(key, m.onGet) {
		return nil, m.err
	}
	return m.inner.Get(ctx, namespace, key)
}

func (m *s3PrefixFaultState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if m.fails(key, m.onPut) {
		return m.err
	}
	return m.inner.Put(ctx, namespace, key, value)
}

func (m *s3PrefixFaultState) Delete(ctx context.Context, namespace, key string) error {
	if m.fails(key, m.onDelete) {
		return m.err
	}
	return m.inner.Delete(ctx, namespace, key)
}

func (m *s3PrefixFaultState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if m.fails(prefix, m.onList) {
		return nil, m.err
	}
	return m.inner.List(ctx, namespace, prefix)
}

// s3CorruptState returns a value that is not valid JSON for keys under a prefix,
// which is how a record written by a different schema reads back. The scan over
// multipart: unmarshals every record it finds, so this reaches an unmarshal failure
// that a consistent store cannot produce.
type s3CorruptState struct {
	emulator.StateManager
	prefix string
	armed  bool
}

func (m *s3CorruptState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.armed && strings.HasPrefix(key, m.prefix) {
		return []byte("{not json"), nil
	}
	return m.StateManager.Get(ctx, namespace, key)
}

// s3VanishingState reports keys from List but nil from Get for a prefix, the shape a
// key removed between the two calls has.
type s3VanishingState struct {
	emulator.StateManager
	prefix string
	armed  bool
}

func (m *s3VanishingState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.armed && strings.HasPrefix(key, m.prefix) {
		return nil, nil
	}
	return m.StateManager.Get(ctx, namespace, key)
}

// s3FaultFs fails one filesystem operation once armed. The object mirror is the
// other half of the state these paths clear, and a failure there leaks exactly what
// a failure in the store leaks — an upload's part bodies, or an object body left
// describing a version that was deleted — so it has to propagate the same way.
//
// One operation is selected per test rather than all writes at once because the
// promotion performs MkdirAll and then WriteFile: failing both would only ever
// exercise the first.
type s3FaultFs struct {
	afero.Fs
	err           error
	armed         bool
	failOpen      bool
	failOpenFile  bool
	failMkdirAll  bool
	failRemove    bool
	failRemoveAll bool
}

func (f *s3FaultFs) Open(name string) (afero.File, error) {
	if f.armed && f.failOpen {
		return nil, f.err
	}
	return f.Fs.Open(name)
}

func (f *s3FaultFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	if f.armed && f.failOpenFile {
		return nil, f.err
	}
	return f.Fs.OpenFile(name, flag, perm)
}

func (f *s3FaultFs) MkdirAll(path string, perm os.FileMode) error {
	if f.armed && f.failMkdirAll {
		return f.err
	}
	return f.Fs.MkdirAll(path, perm)
}

func (f *s3FaultFs) Remove(name string) error {
	if f.armed && f.failRemove {
		return f.err
	}
	return f.Fs.Remove(name)
}

func (f *s3FaultFs) RemoveAll(path string) error {
	if f.armed && f.failRemoveAll {
		return f.err
	}
	return f.Fs.RemoveAll(path)
}

// TestS3_DeleteBucket_StateFaultsPropagate walks each store operation DeleteBucket's
// clear performs and requires a 500 with the bucket left in place.
//
// The bucket is asserted to survive in every case, not merely reported as failed: a
// caller that retries must find something to retry against, and HeadBucket must
// agree with the error rather than contradict it.
func TestS3_DeleteBucket_StateFaultsPropagate(t *testing.T) {
	const bucket = "faultbucket"

	tests := []struct {
		name string
		// prefix is the state-key prefix whose operations fail.
		prefix   string
		onGet    bool
		onDelete bool
		onList   bool
		// seed leaves the state the fault needs something to act on. Any state it
		// creates is in place before the store is armed.
		seed func(t *testing.T, srv *emulator.Server)
	}{
		{
			// The lookup that precedes the clear: a store failure reading the
			// bucket record must not be reported as this bucket's own NoSuchBucket,
			// which would tell a caller the teardown was already done.
			name:   "reading the bucket record fails",
			prefix: "bucket:" + bucket,
			onGet:  true,
		},
		{
			name:   "listing live objects fails",
			prefix: "object:" + bucket + "/",
			onList: true,
		},
		{
			name:   "listing object versions fails",
			prefix: "object_version:" + bucket + "/",
			onList: true,
		},
		{
			name:   "listing object ACLs fails",
			prefix: "object_acl:" + bucket + "/",
			onList: true,
		},
		{
			name:   "listing version indexes fails",
			prefix: "object_versions:" + bucket + "/",
			onList: true,
		},
		{
			name:     "deleting a stranded object ACL fails",
			prefix:   "object_acl:" + bucket + "/",
			onDelete: true,
			seed:     seedS3StrandedObjectACL(bucket),
		},
		{
			name:   "listing multipart uploads fails",
			prefix: "multipart:",
			onList: true,
		},
		{
			name:   "reading a multipart upload fails",
			prefix: "multipart:",
			onGet:  true,
			seed:   seedS3MultipartUpload(bucket, false),
		},
		{
			name:     "deleting a multipart upload record fails",
			prefix:   "multipart:",
			onDelete: true,
			seed:     seedS3MultipartUpload(bucket, false),
		},
		{
			name:   "listing an upload's parts fails",
			prefix: "part:",
			onList: true,
			seed:   seedS3MultipartUpload(bucket, false),
		},
		{
			name:     "deleting an upload's part record fails",
			prefix:   "part:",
			onDelete: true,
			seed:     seedS3MultipartUpload(bucket, true),
		},
		{
			name:     "deleting the bucket record itself fails",
			prefix:   "bucket:" + bucket,
			onDelete: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &s3PrefixFaultState{
				inner:    emulator.NewMemoryStateManager(),
				prefix:   tt.prefix,
				err:      errors.New("state store unavailable"),
				onGet:    tt.onGet,
				onDelete: tt.onDelete,
				onList:   tt.onList,
			}
			srv := newS3TestServerWithState(t, state)

			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)
			if tt.seed != nil {
				tt.seed(t, srv)
			}

			state.armed = true
			w := s3Request(t, srv, http.MethodDelete, "/"+bucket, nil, nil)
			assert.Equal(t, http.StatusInternalServerError, w.Code,
				"a store failure must be reported, not swallowed: %s", w.Body.String())

			state.armed = false
			assert.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodHead, "/"+bucket, nil, nil).Code,
				"a failed DeleteBucket must leave the bucket present")
		})
	}
}

// TestS3_DeleteBucket_AbsentBucketIsNotFound pins the other outcome the lookup can
// have. It is the boundary the fault test above sits against: a bucket that is
// genuinely absent is a 404, and only a store that cannot answer is a 500 — so the
// clear never runs against a name that was never a bucket.
func TestS3_DeleteBucket_AbsentBucketIsNotFound(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	w := s3Request(t, srv, http.MethodDelete, "/neverexisted", nil, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Contains(t, w.Body.String(), "NoSuchBucket")
}

// TestS3_DeleteBucket_FilesystemFaultPropagates covers the mirror half of the
// abort: a failure removing an upload's part bodies is a 500. Reporting success
// would leave the bodies for the next bucket of this name to inherit, which is the
// same leak in the filesystem rather than in the store.
func TestS3_DeleteBucket_FilesystemFaultPropagates(t *testing.T) {
	t.Parallel()
	fs := &s3FaultFs{
		Fs:            afero.NewMemMapFs(),
		err:           errors.New("filesystem unavailable"),
		failRemoveAll: true,
	}
	srv := newS3TestServerWithFaultFS(t, emulator.NewMemoryStateManager(), fs)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/fsfault", nil, nil).Code)
	seedS3MultipartUpload("fsfault", true)(t, srv)

	fs.armed = true
	w := s3Request(t, srv, http.MethodDelete, "/fsfault", nil, nil)
	assert.Equal(t, http.StatusInternalServerError, w.Code,
		"a failure removing part bodies must be reported: %s", w.Body.String())

	fs.armed = false
	assert.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodHead, "/fsfault", nil, nil).Code,
		"a failed DeleteBucket must leave the bucket present")
}

// TestS3_AbortMultipartUpload_StateFaultPropagates asserts the explicit abort
// reports a failed clear too. AbortMultipartUpload and DeleteBucket share one
// helper so the two paths cannot drift, and this is the half that is reachable
// directly: an abort that reports 204 while leaving the part records behind is the
// same false claim of cleanliness, one upload wide instead of one bucket wide.
func TestS3_AbortMultipartUpload_StateFaultPropagates(t *testing.T) {
	t.Parallel()
	state := &s3PrefixFaultState{
		inner:    emulator.NewMemoryStateManager(),
		prefix:   "part:",
		err:      errors.New("state store unavailable"),
		onDelete: true,
	}
	srv := newS3TestServerWithState(t, state)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/abortfault", nil, nil).Code)
	create := s3Request(t, srv, http.MethodPost, "/abortfault/big?uploads", nil, nil)
	require.Equal(t, http.StatusOK, create.Code)
	var initiated struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(create.Body.Bytes(), &initiated))
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
		"/abortfault/big?partNumber=1&uploadId="+initiated.UploadID,
		[]byte("part"), nil).Code)

	state.armed = true
	w := s3Request(t, srv, http.MethodDelete,
		"/abortfault/big?uploadId="+initiated.UploadID, nil, nil)
	assert.Equal(t, http.StatusInternalServerError, w.Code,
		"a failed abort must be reported, not swallowed: %s", w.Body.String())
}

// TestS3_DeleteBucket_CorruptUploadRecordPropagates covers the unmarshal branch of
// the multipart scan: a record that is not valid JSON is a 500, not an upload
// quietly skipped. Skipping it would leak the upload under a reusable bucket name
// while reporting a clean delete.
func TestS3_DeleteBucket_CorruptUploadRecordPropagates(t *testing.T) {
	t.Parallel()
	state := &s3CorruptState{
		StateManager: emulator.NewMemoryStateManager(),
		prefix:       "multipart:",
	}
	srv := newS3TestServerWithState(t, state)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/corruptmpu", nil, nil).Code)
	seedS3MultipartUpload("corruptmpu", false)(t, srv)

	state.armed = true
	w := s3Request(t, srv, http.MethodDelete, "/corruptmpu", nil, nil)
	assert.Equal(t, http.StatusInternalServerError, w.Code,
		"an unreadable upload record must be reported: %s", w.Body.String())
}

// TestS3_DeleteBucket_MissingUploadRecordIsSkipped is the companion to the test
// above: a key the List returned whose value is no longer readable is not an error.
// There is nothing left to abort, which is the state a successful abort produces —
// so the delete completes rather than wedging on a record that is already gone.
func TestS3_DeleteBucket_MissingUploadRecordIsSkipped(t *testing.T) {
	t.Parallel()
	state := &s3VanishingState{
		StateManager: emulator.NewMemoryStateManager(),
		prefix:       "multipart:",
	}
	srv := newS3TestServerWithState(t, state)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/vanishmpu", nil, nil).Code)
	seedS3MultipartUpload("vanishmpu", false)(t, srv)

	state.armed = true
	assert.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/vanishmpu", nil, nil).Code,
		"an upload record that is already gone is nothing to abort")
}

// seedS3StrandedObjectACL writes an object with an ACL and then deletes the object,
// which strands the object_acl: key — the leftover clearBucketState scans for. The
// bucket is left empty, so the delete under test reaches the clear.
func seedS3StrandedObjectACL(bucket string) func(t *testing.T, srv *emulator.Server) {
	return func(t *testing.T, srv *emulator.Server) {
		t.Helper()
		require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
			"/"+bucket+"/obj", []byte("x"),
			map[string]string{"x-amz-acl": "public-read"}).Code)
		require.Equal(t, http.StatusNoContent,
			s3Request(t, srv, http.MethodDelete, "/"+bucket+"/obj", nil, nil).Code)
	}
}

// seedS3MultipartUpload begins a multipart upload in bucket, optionally uploading
// one part so the part: scan and the mirror cleanup have something to act on.
func seedS3MultipartUpload(bucket string, withPart bool) func(t *testing.T, srv *emulator.Server) {
	return func(t *testing.T, srv *emulator.Server) {
		t.Helper()
		create := s3Request(t, srv, http.MethodPost, "/"+bucket+"/big?uploads", nil, nil)
		require.Equal(t, http.StatusOK, create.Code)
		if !withPart {
			return
		}
		var initiated struct {
			UploadID string `xml:"UploadId"`
		}
		require.NoError(t, xml.Unmarshal(create.Body.Bytes(), &initiated))
		require.NotEmpty(t, initiated.UploadID)
		require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
			"/"+bucket+"/big?partNumber=1&uploadId="+initiated.UploadID,
			[]byte("part"), nil).Code)
	}
}

// TestS3_VersionDelete_StateFaultsPropagate applies the same rule to the
// current-version promotion. A swallowed failure here leaves object: pointing at a
// version that no longer exists, so a later unversioned GET serves a deleted
// version's bytes — reported as a successful delete.
func TestS3_VersionDelete_StateFaultsPropagate(t *testing.T) {
	const bucket = "promofault"

	tests := []struct {
		name     string
		prefix   string
		onGet    bool
		onPut    bool
		onDelete bool
		// versions is how many versions to write before deleting the newest, which
		// selects the promote branch (two) or the no-survivor branch (one).
		versions int
	}{
		{
			name:     "reading the surviving version fails",
			prefix:   "object_version:" + bucket + "/",
			onGet:    true,
			versions: 2,
		},
		{
			name:     "promoting the surviving version fails",
			prefix:   "object:" + bucket + "/",
			onPut:    true,
			versions: 2,
		},
		{
			name:     "clearing the current pointer fails",
			prefix:   "object:" + bucket + "/",
			onDelete: true,
			versions: 1,
		},
		{
			name:     "clearing the version index fails",
			prefix:   "object_versions:" + bucket + "/",
			onDelete: true,
			versions: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &s3PrefixFaultState{
				inner:    emulator.NewMemoryStateManager(),
				prefix:   tt.prefix,
				err:      errors.New("state store unavailable"),
				onGet:    tt.onGet,
				onPut:    tt.onPut,
				onDelete: tt.onDelete,
			}
			srv := newS3TestServerWithState(t, state)
			newest := seedS3Versions(t, srv, bucket, tt.versions)

			state.armed = true
			w := s3Request(t, srv, http.MethodDelete,
				"/"+bucket+"/obj?versionId="+newest, nil, nil)
			assert.Equal(t, http.StatusInternalServerError, w.Code,
				"a store failure must be reported, not swallowed: %s", w.Body.String())
		})
	}
}

// TestS3_VersionDelete_FilesystemFaultsPropagate is the mirror half of the
// promotion: the promoted version's body has to move with its record, so a
// filesystem failure while moving it must be reported rather than leaving the
// record promoted and the body still the deleted version's.
func TestS3_VersionDelete_FilesystemFaultsPropagate(t *testing.T) {
	const bucket = "promofsfault"

	tests := []struct {
		name string
		// arm selects the operation that fails; each corresponds to one step of
		// moving a body, which is why they are exercised one at a time.
		arm func(fs *s3FaultFs)
		// versions selects the promote branch (two) or the no-survivor branch (one).
		versions int
	}{
		{
			name:     "reading the promoted version's body fails",
			arm:      func(fs *s3FaultFs) { fs.failOpen = true },
			versions: 2,
		},
		{
			name:     "creating the promoted body's directory fails",
			arm:      func(fs *s3FaultFs) { fs.failMkdirAll = true },
			versions: 2,
		},
		{
			name:     "writing the promoted body fails",
			arm:      func(fs *s3FaultFs) { fs.failOpenFile = true },
			versions: 2,
		},
		{
			name:     "removing the last version's body fails",
			arm:      func(fs *s3FaultFs) { fs.failRemove = true },
			versions: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fs := &s3FaultFs{
				Fs:  afero.NewMemMapFs(),
				err: errors.New("filesystem unavailable"),
			}
			tt.arm(fs)
			srv := newS3TestServerWithFaultFS(t, emulator.NewMemoryStateManager(), fs)
			newest := seedS3Versions(t, srv, bucket, tt.versions)

			// Armed only here, so the versions above are written normally and there
			// is a real body to promote.
			fs.armed = true
			w := s3Request(t, srv, http.MethodDelete,
				"/"+bucket+"/obj?versionId="+newest, nil, nil)
			assert.Equal(t, http.StatusInternalServerError, w.Code,
				"a filesystem failure must be reported, not swallowed: %s", w.Body.String())
		})
	}
}

// TestS3_VersionDelete_UnreadableSurvivorLeavesNoPointer covers the remaining
// branch of the promotion: a version ID still in the index whose record cannot be
// read is treated as no version at all. The alternative is worse than an error —
// object: would be left pointing at something unreadable, and a GET would report a
// 500 for a key the caller can neither read nor list.
func TestS3_VersionDelete_UnreadableSurvivorLeavesNoPointer(t *testing.T) {
	t.Parallel()
	state := &s3VanishingState{
		StateManager: emulator.NewMemoryStateManager(),
		prefix:       "object_version:",
	}
	srv := newS3TestServerWithState(t, state)
	newest := seedS3Versions(t, srv, "gonesurvivor", 2)

	state.armed = true
	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete,
		"/gonesurvivor/obj?versionId="+newest, nil, nil).Code)

	state.armed = false
	assert.Equal(t, http.StatusNotFound,
		s3Request(t, srv, http.MethodGet, "/gonesurvivor/obj", nil, nil).Code,
		"with no readable version left, the key must not exist")
}

// seedS3Versions creates bucket with versioning enabled and writes count versions
// of the key "obj", returning the newest version ID.
func seedS3Versions(t *testing.T, srv *emulator.Server, bucket string, count int) string {
	t.Helper()
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
		"/"+bucket+"?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status>`+
			`</VersioningConfiguration>`), nil).Code)

	newest := ""
	for i := 0; i < count; i++ {
		w := s3Request(t, srv, http.MethodPut, "/"+bucket+"/obj", []byte("body"), nil)
		require.Equal(t, http.StatusOK, w.Code)
		newest = w.Header().Get("x-amz-version-id")
	}
	require.NotEmpty(t, newest)
	return newest
}
