package emulator_test

import (
	"encoding/xml"
	"net/http"
	"os"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Regression tests for #534: deleting a directory-marker object ("prefix/")
// removed the afero directory node that holds the keys beneath it, so the next
// delete of a child panicked inside MemMapFs and surfaced as a 500.
//
// The invariant these assert is that the afero mirror holds object *bodies*, and
// a marker has none: no marker key may reach the filesystem on any path.

// TestS3_DirectoryMarker_DeleteDoesNotStrandChildren walks the reproduction from
// the issue — create a marker, create a key beneath it, delete the marker, then
// delete the child.
func TestS3_DirectoryMarker_DeleteDoesNotStrandChildren(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/marker-del", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/marker-del/dir/", []byte{}, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/marker-del/dir/a.txt", []byte("a"), nil).Code)

	assert.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/marker-del/dir/", nil, nil).Code,
		"deleting the marker must succeed")

	assert.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/marker-del/dir/a.txt", nil, nil).Code,
		"deleting the child after its marker must not fail: the marker's delete "+
			"must not have removed the directory node the child lives in")

	assert.Equal(t, http.StatusNotFound,
		s3Request(t, srv, http.MethodGet, "/marker-del/dir/a.txt", nil, nil).Code)
}

// TestS3_DirectoryMarker_DeleteObjectsDoesNotStrandChildren asserts the same
// through the multi-object delete, which routes per key through deleteObject and
// so inherited the identical failure. Marker first in the batch, so the child's
// delete follows it within one request.
func TestS3_DirectoryMarker_DeleteObjectsDoesNotStrandChildren(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	s3Request(t, srv, http.MethodPut, "/marker-batch", nil, nil)
	s3Request(t, srv, http.MethodPut, "/marker-batch/dir/", []byte{}, nil)
	s3Request(t, srv, http.MethodPut, "/marker-batch/dir/a.txt", []byte("a"), nil)
	s3Request(t, srv, http.MethodPut, "/marker-batch/dir/b.txt", []byte("b"), nil)

	deleteXML := []byte(`<Delete>` +
		`<Object><Key>dir/</Key></Object>` +
		`<Object><Key>dir/a.txt</Key></Object>` +
		`<Object><Key>dir/b.txt</Key></Object>` +
		`</Delete>`)
	w := s3Request(t, srv, http.MethodPost, "/marker-batch?delete", deleteXML,
		map[string]string{"Content-Type": "application/xml"})
	require.Equal(t, http.StatusOK, w.Code)

	var result struct {
		Deleted []struct {
			Key string `xml:"Key"`
		} `xml:"Deleted"`
		Errors []struct {
			Key  string `xml:"Key"`
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &result))
	assert.Empty(t, result.Errors, "no key in the batch may report an error")
	assert.Len(t, result.Deleted, 3)
}

// TestS3_DirectoryMarker_DeleteThenReadChild asserts the child is still readable
// after its marker is deleted. Deleting a marker deletes one zero-byte object; it
// says nothing about the keys that share its prefix.
func TestS3_DirectoryMarker_DeleteThenReadChild(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	s3Request(t, srv, http.MethodPut, "/marker-read", nil, nil)
	s3Request(t, srv, http.MethodPut, "/marker-read/dir/", []byte{}, nil)
	s3Request(t, srv, http.MethodPut, "/marker-read/dir/a.txt", []byte("payload"), nil)

	require.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/marker-read/dir/", nil, nil).Code)

	w := s3Request(t, srv, http.MethodGet, "/marker-read/dir/a.txt", nil, nil)
	assert.Equal(t, http.StatusOK, w.Code, "the child outlives its marker")
	assert.Equal(t, "payload", w.Body.String())

	assert.Equal(t, http.StatusNotFound,
		s3Request(t, srv, http.MethodGet, "/marker-read/dir/", nil, nil).Code,
		"the marker itself is gone")
}

// TestS3_DirectoryMarker_DeleteLeavesFilesystemIntact inspects the afero mirror
// directly: the marker was never written to it, so its delete must leave the
// directory node MkdirAll created for the child untouched.
func TestS3_DirectoryMarker_DeleteLeavesFilesystemIntact(t *testing.T) {
	t.Parallel()
	srv, fs := newS3TestServer(t)

	s3Request(t, srv, http.MethodPut, "/marker-fs", nil, nil)
	s3Request(t, srv, http.MethodPut, "/marker-fs/dir/", []byte{}, nil)
	s3Request(t, srv, http.MethodPut, "/marker-fs/dir/a.txt", []byte("a"), nil)

	require.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/marker-fs/dir/", nil, nil).Code)

	exists, err := afero.DirExists(fs, "/marker-fs/dir")
	require.NoError(t, err)
	assert.True(t, exists, "the directory node holding the child must survive the marker's delete")

	body, readErr := afero.ReadFile(fs, "/marker-fs/dir/a.txt")
	require.NoError(t, readErr)
	assert.Equal(t, "a", string(body))
}

// TestS3_DirectoryMarker_VersionedDelete covers the versioned branches: a marker
// written and permanently deleted by version ID must not touch the mirror either,
// since its versioned path collides the same way.
func TestS3_DirectoryMarker_VersionedDelete(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	s3Request(t, srv, http.MethodPut, "/marker-ver", nil, nil)
	s3Request(t, srv, http.MethodPut, "/marker-ver?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)

	w := s3Request(t, srv, http.MethodPut, "/marker-ver/dir/", []byte{}, nil)
	require.Equal(t, http.StatusOK, w.Code)
	versionID := w.Header().Get("x-amz-version-id")
	require.NotEmpty(t, versionID)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/marker-ver/dir/a.txt", []byte("a"), nil).Code)

	assert.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/marker-ver/dir/?versionId="+versionID, nil, nil).Code,
		"permanently deleting the marker version must succeed")

	w = s3Request(t, srv, http.MethodGet, "/marker-ver/dir/a.txt", nil, nil)
	assert.Equal(t, http.StatusOK, w.Code, "the child must remain readable")
	assert.Equal(t, "a", w.Body.String())
}

// TestS3_DirectoryMarker_WritesNoFileToTheMirror asserts the invariant the whole
// fix rests on, directly rather than through a symptom: a marker's PUT writes no
// file to the afero mirror. Both the unversioned and versioned write paths are
// covered, since each builds its own path from the key.
func TestS3_DirectoryMarker_WritesNoFileToTheMirror(t *testing.T) {
	t.Parallel()

	for _, versioned := range []bool{false, true} {
		name := "unversioned"
		bucket := "mirror-unver"
		if versioned {
			name = "versioned"
			bucket = "mirror-ver"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv, fs := newS3TestServer(t)

			s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil)
			if versioned {
				s3Request(t, srv, http.MethodPut, "/"+bucket+"?versioning",
					[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)
			}

			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/"+bucket+"/dir/", []byte{}, nil).Code)

			// Walk from the root: a marker-only bucket need not have a directory
			// node at all, and asserting on "/"+bucket would fail for the right
			// reason in a confusing way.
			var files []string
			walkErr := afero.Walk(fs, "/", func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !info.IsDir() {
					files = append(files, path)
				}
				return nil
			})
			require.NoError(t, walkErr)
			assert.Empty(t, files,
				"a directory marker has no body, so it must put no file in the mirror")
		})
	}
}

// TestS3_DirectoryMarker_VersionedDeleteDoesNotStrandChildVersions pins the
// versioned mirror's own instance of the collision. A marker's versioned path is
// "<bucket>/.versions/<key>/<versionID>", which for key "dir/" normalizes to
// ".../dir/<versionID>" — exactly the directory node holding the versions of a
// child key literally named "dir/<versionID>". The version ID is not secret: a
// client reads it off the marker's PUT, so this sequence is reachable rather than
// theoretical.
func TestS3_DirectoryMarker_VersionedDeleteDoesNotStrandChildVersions(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	s3Request(t, srv, http.MethodPut, "/marker-vercollide", nil, nil)
	s3Request(t, srv, http.MethodPut, "/marker-vercollide?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)

	w := s3Request(t, srv, http.MethodPut, "/marker-vercollide/dir/", []byte{}, nil)
	require.Equal(t, http.StatusOK, w.Code)
	markerVersion := w.Header().Get("x-amz-version-id")
	require.NotEmpty(t, markerVersion)

	// A child key named after the marker's version ID. Legal S3, and its versioned
	// mirror path is the directory the marker's version path names.
	child := "dir/" + markerVersion
	cw := s3Request(t, srv, http.MethodPut, "/marker-vercollide/"+child, []byte("child"), nil)
	require.Equal(t, http.StatusOK, cw.Code)
	childVersion := cw.Header().Get("x-amz-version-id")
	require.NotEmpty(t, childVersion)

	assert.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete,
			"/marker-vercollide/dir/?versionId="+markerVersion, nil, nil).Code)

	// Reading the child by explicit version ID goes to the mirror path under the
	// directory node the marker's delete named.
	got := s3Request(t, srv, http.MethodGet,
		"/marker-vercollide/"+child+"?versionId="+childVersion, nil, nil)
	assert.Equal(t, http.StatusOK, got.Code, "the child's version must survive")
	assert.Equal(t, "child", got.Body.String())

	// And permanently deleting that version must still work: this is the call that
	// removes the child's own mirror entry, so it is where a stranded node bites.
	assert.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete,
			"/marker-vercollide/"+child+"?versionId="+childVersion, nil, nil).Code,
		"the child's version must still be permanently deletable")
}

// TestS3_DirectoryMarker_SiblingKeyIsNotClobbered pins the collision from the
// other side. "dir" and "dir/" are two distinct S3 keys, but filepath.Clean maps
// both to the same mirror path — so a marker that reaches the filesystem truncates
// or deletes the body of the object one character away from it. Each step is a
// separate operation on the marker; the object at "dir" must be unaffected by all
// of them.
func TestS3_DirectoryMarker_SiblingKeyIsNotClobbered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// act performs one marker operation against bucket "sibling".
		act func(t *testing.T, srv *emulator.Server)
	}{
		{
			name: "put the marker",
			act: func(t *testing.T, srv *emulator.Server) {
				t.Helper()
				assert.Equal(t, http.StatusOK,
					s3Request(t, srv, http.MethodPut, "/sibling/dir/", []byte{}, nil).Code)
			},
		},
		{
			name: "put then delete the marker",
			act: func(t *testing.T, srv *emulator.Server) {
				t.Helper()
				s3Request(t, srv, http.MethodPut, "/sibling/dir/", []byte{}, nil)
				assert.Equal(t, http.StatusNoContent,
					s3Request(t, srv, http.MethodDelete, "/sibling/dir/", nil, nil).Code)
			},
		},
		{
			name: "delete a marker that was never written",
			act: func(t *testing.T, srv *emulator.Server) {
				t.Helper()
				// S3's DELETE is idempotent, so this is a 204 either way — the
				// point is that it must not reach the mirror.
				assert.Equal(t, http.StatusNoContent,
					s3Request(t, srv, http.MethodDelete, "/sibling/dir/", nil, nil).Code)
			},
		},
		{
			name: "copy onto the marker",
			act: func(t *testing.T, srv *emulator.Server) {
				t.Helper()
				s3Request(t, srv, http.MethodPut, "/sibling/other.txt", []byte("other"), nil)
				assert.Equal(t, http.StatusOK,
					s3Request(t, srv, http.MethodPut, "/sibling/dir/", nil,
						map[string]string{"x-amz-copy-source": "/sibling/other.txt"}).Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)

			s3Request(t, srv, http.MethodPut, "/sibling", nil, nil)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sibling/dir", []byte("real body"), nil).Code)

			tt.act(t, srv)

			w := s3Request(t, srv, http.MethodGet, "/sibling/dir", nil, nil)
			assert.Equal(t, http.StatusOK, w.Code, `the object at key "dir" must survive`)
			assert.Equal(t, "real body", w.Body.String(),
				`the object at key "dir" must keep its body: "dir/" is a different key`)
		})
	}
}

// TestS3_DirectoryMarker_Copy asserts CopyObject over marker keys. A marker has no
// body in the mirror, so both directions had to be guarded independently: reading
// one as a source used to fail on ReadFile, and writing one as a destination would
// corrupt the directory node for the keys beneath it.
func TestS3_DirectoryMarker_Copy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		srcKey  string
		dstKey  string
		srcBody []byte
		// wantBody is what a GET of the destination must return. A marker
		// destination is an empty object regardless of the source.
		wantBody string
	}{
		{
			name:     "marker to marker",
			srcKey:   "dir/",
			dstKey:   "copied/",
			srcBody:  []byte{},
			wantBody: "",
		},
		{
			name:     "marker to regular key",
			srcKey:   "dir/",
			dstKey:   "flattened.txt",
			srcBody:  []byte{},
			wantBody: "",
		},
		{
			name:     "regular key to marker",
			srcKey:   "src.txt",
			dstKey:   "dir2/",
			srcBody:  []byte("content"),
			wantBody: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)

			s3Request(t, srv, http.MethodPut, "/marker-copy", nil, nil)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/marker-copy/"+tt.srcKey, tt.srcBody, nil).Code)
			// A key beneath the destination prefix, so a mishandled destination
			// write would corrupt a directory node that is actually in use.
			s3Request(t, srv, http.MethodPut, "/marker-copy/dir2/child.txt", []byte("c"), nil)

			w := s3Request(t, srv, http.MethodPut, "/marker-copy/"+tt.dstKey, nil,
				map[string]string{"x-amz-copy-source": "/marker-copy/" + tt.srcKey})
			assert.Equal(t, http.StatusOK, w.Code, "CopyObject must succeed")

			got := s3Request(t, srv, http.MethodGet, "/marker-copy/"+tt.dstKey, nil, nil)
			assert.Equal(t, http.StatusOK, got.Code)
			assert.Equal(t, tt.wantBody, got.Body.String())

			// The unrelated key beneath dir2/ is untouched either way.
			child := s3Request(t, srv, http.MethodGet, "/marker-copy/dir2/child.txt", nil, nil)
			assert.Equal(t, http.StatusOK, child.Code)
			assert.Equal(t, "c", child.Body.String())

			// And it is still deletable. This is the assertion that makes a bad
			// destination write observable: writing a marker key into the mirror
			// replaces the directory node dir2/child.txt lives in with a file, and
			// the corruption only surfaces when something removes the child.
			assert.Equal(t, http.StatusNoContent,
				s3Request(t, srv, http.MethodDelete, "/marker-copy/dir2/child.txt", nil, nil).Code)
		})
	}
}
