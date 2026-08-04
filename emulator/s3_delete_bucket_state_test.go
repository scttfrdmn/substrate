package emulator_test

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Regression tests for #508: DeleteBucket removed the bucket: record and left
// every key scoped to that bucket behind, so a bucket created with the same name
// inherited the deleted bucket's configuration.
//
// The bucket name is the entire key for a bucket-scoped configuration, which is why
// the leak is observable rather than merely untidy.

// s3BucketSubresource describes one bucket-scoped configuration: how to set it, and
// what an *unconfigured* bucket reports for it. The recreated bucket must report the
// unconfigured value.
type s3BucketSubresource struct {
	name string
	// configure applies the configuration to bucket "recycled".
	configure func(t *testing.T, srv *emulator.Server)
	// stateKey is the state key the configuration is stored under. Asserting on it
	// directly is what proves DeleteBucket cleared *this* key: bucket_acl: is also
	// cleared by CreateBucket (s3CreateBucketACL), so a read after the recreate
	// cannot distinguish the delete having cleared it from the create having done so.
	stateKey string
	// readPath is the sub-resource query appended to the bucket path.
	readPath string
	// wantStatus is what a GET reports when nothing is configured.
	wantStatus int
	// wantBodyExcludes is a fragment that must be absent from an unconfigured
	// read — the marker of the configuration having survived.
	wantBodyExcludes string
}

func s3DeleteBucketSubresources() []s3BucketSubresource {
	return []s3BucketSubresource{
		{
			name: "bucket ACL",
			configure: func(t *testing.T, srv *emulator.Server) {
				t.Helper()
				require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
					"/recycled?acl", nil,
					map[string]string{"x-amz-acl": "public-read"}).Code)
			},
			stateKey:   "bucket_acl:recycled",
			readPath:   "?acl",
			wantStatus: http.StatusOK,
			// The public-read grant a public ACL carries.
			wantBodyExcludes: "AllUsers",
		},
		{
			name: "bucket policy",
			configure: func(t *testing.T, srv *emulator.Server) {
				t.Helper()
				policy := `{"Version":"2012-10-17","Statement":[{"Sid":"LeakMarker",` +
					`"Effect":"Allow","Principal":"*","Action":"s3:GetObject",` +
					`"Resource":"arn:aws:s3:::recycled/*"}]}`
				require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodPut,
					"/recycled?policy", []byte(policy), nil).Code)
			},
			stateKey: "bucket_policy:recycled",
			readPath: "?policy",
			// A bucket with no policy is NoSuchBucketPolicy, not an empty policy.
			wantStatus:       http.StatusNotFound,
			wantBodyExcludes: "LeakMarker",
		},
		{
			name: "bucket lifecycle",
			configure: func(t *testing.T, srv *emulator.Server) {
				t.Helper()
				cfg := `<LifecycleConfiguration><Rule><ID>LeakMarker</ID>` +
					`<Status>Enabled</Status><Filter><Prefix></Prefix></Filter>` +
					`<Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`
				code := s3Request(t, srv, http.MethodPut, "/recycled?lifecycle",
					[]byte(cfg), nil).Code
				require.Contains(t, []int{http.StatusOK, http.StatusNoContent}, code)
			},
			stateKey:         "bucket_lifecycle:recycled",
			readPath:         "?lifecycle",
			wantStatus:       http.StatusNotFound,
			wantBodyExcludes: "LeakMarker",
		},
		{
			name: "bucket versioning",
			configure: func(t *testing.T, srv *emulator.Server) {
				t.Helper()
				code := s3Request(t, srv, http.MethodPut, "/recycled?versioning",
					[]byte(`<VersioningConfiguration><Status>Enabled</Status>`+
						`</VersioningConfiguration>`), nil).Code
				require.Contains(t, []int{http.StatusOK, http.StatusNoContent}, code)
			},
			stateKey: "bucket_versioning:recycled",
			readPath: "?versioning",
			// An unversioned bucket reports an empty configuration, 200.
			wantStatus:       http.StatusOK,
			wantBodyExcludes: "Enabled",
		},
		{
			name: "bucket notification configuration",
			configure: func(t *testing.T, srv *emulator.Server) {
				t.Helper()
				// Real S3's element names, singular and with the destination named
				// after the service. Until #542 substrate matched this body by Go
				// field name, so written this way it parsed to an empty
				// configuration and stored nothing — which made this sub-test pass
				// whether or not the delete cleared the key.
				cfg := `<NotificationConfiguration><QueueConfiguration>` +
					`<Queue>arn:aws:sqs:us-east-1:123456789012:leak-marker</Queue>` +
					`<Event>s3:ObjectCreated:*</Event>` +
					`</QueueConfiguration></NotificationConfiguration>`
				code := s3Request(t, srv, http.MethodPut, "/recycled?notification",
					[]byte(cfg), nil).Code
				require.Contains(t, []int{http.StatusOK, http.StatusNoContent}, code)

				// Pin that the configuration really was stored, so the assertion
				// after the recreate is about inheritance.
				read := s3Request(t, srv, http.MethodGet, "/recycled?notification", nil, nil)
				require.Contains(t, read.Body.String(), "leak-marker",
					"the notification configuration must be stored before the delete")
			},
			stateKey:         "notification:recycled",
			readPath:         "?notification",
			wantStatus:       http.StatusOK,
			wantBodyExcludes: "leak-marker",
		},
		{
			name: "block public access",
			configure: func(t *testing.T, srv *emulator.Server) {
				t.Helper()
				cfg := `<PublicAccessBlockConfiguration>` +
					`<BlockPublicAcls>true</BlockPublicAcls>` +
					`<IgnorePublicAcls>true</IgnorePublicAcls>` +
					`<BlockPublicPolicy>true</BlockPublicPolicy>` +
					`<RestrictPublicBuckets>true</RestrictPublicBuckets>` +
					`</PublicAccessBlockConfiguration>`
				code := s3Request(t, srv, http.MethodPut, "/recycled?publicAccessBlock",
					[]byte(cfg), nil).Code
				require.Contains(t, []int{http.StatusOK, http.StatusNoContent}, code)
			},
			stateKey: "bucket_public_access_block:recycled",
			readPath: "?publicAccessBlock",
			// No configuration is NoSuchPublicAccessBlockConfiguration, which is
			// distinct from a configuration with all four settings false.
			wantStatus:       http.StatusNotFound,
			wantBodyExcludes: "<BlockPublicAcls>true</BlockPublicAcls>",
		},
	}
}

// TestS3_DeleteBucket_ClearsBucketScopedConfiguration is the core assertion: for
// each bucket-scoped configuration, configure it, delete the bucket, create a
// bucket of the same name, and require the recreated bucket to report the
// *unconfigured* value.
//
// One sub-test per configuration rather than one test setting all six: a single
// combined test passes as soon as any one key is cleared, so it cannot show that
// each is covered.
//
// Each sub-test asserts twice: on the state key immediately after the delete, and on
// the recreated bucket's read. The state assertion is the one that attributes the
// clear to DeleteBucket — CreateBucket clears bucket_acl: itself, so for that key the
// read alone would pass with no fix at all.
func TestS3_DeleteBucket_ClearsBucketScopedConfiguration(t *testing.T) {
	t.Parallel()

	for _, sub := range s3DeleteBucketSubresources() {
		t.Run(sub.name, func(t *testing.T) {
			t.Parallel()
			state := emulator.NewMemoryStateManager()
			srv := newS3TestServerWithState(t, state)

			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/recycled", nil, nil).Code)
			sub.configure(t, srv)

			stored, err := state.Get(context.Background(), "s3", sub.stateKey)
			require.NoError(t, err)
			require.NotNil(t, stored,
				"%s must hold the configuration before the delete", sub.stateKey)

			require.Equal(t, http.StatusNoContent,
				s3Request(t, srv, http.MethodDelete, "/recycled", nil, nil).Code)

			cleared, err := state.Get(context.Background(), "s3", sub.stateKey)
			require.NoError(t, err)
			assert.Nil(t, cleared, "DeleteBucket must clear %s", sub.stateKey)

			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/recycled", nil, nil).Code)

			w := s3Request(t, srv, http.MethodGet, "/recycled"+sub.readPath, nil, nil)
			assert.Equal(t, sub.wantStatus, w.Code,
				"a recreated bucket must report the unconfigured value")
			assert.NotContains(t, w.Body.String(), sub.wantBodyExcludes,
				"the deleted bucket's configuration must not survive its bucket")
		})
	}
}

// TestS3_DeleteBucket_RecreatedBucketDoesNotInheritBPA asserts the Block Public
// Access leak through *behavior* rather than through a read of the configuration,
// which is what the issue asks for: an inherited BPA silently refuses a write the
// caller's fixtures expect to succeed.
func TestS3_DeleteBucket_RecreatedBucketDoesNotInheritBPA(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	s3Request(t, srv, http.MethodPut, "/bpa-recycled", nil, nil)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
		"/bpa-recycled?publicAccessBlock",
		[]byte(`<PublicAccessBlockConfiguration>`+
			`<BlockPublicAcls>true</BlockPublicAcls>`+
			`</PublicAccessBlockConfiguration>`), nil).Code)

	// With BPA in force, a public-ACL write is refused. This pins that the
	// configuration was really in effect, so the assertion after the recreate is
	// about inheritance rather than about BPA not working at all.
	blocked := s3Request(t, srv, http.MethodPut, "/bpa-recycled/obj", []byte("x"),
		map[string]string{"x-amz-acl": "public-read"})
	require.Equal(t, http.StatusForbidden, blocked.Code,
		"BlockPublicAcls must refuse a public-ACL write while configured")

	require.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/bpa-recycled/obj", nil, nil).Code)
	require.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/bpa-recycled", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/bpa-recycled", nil, nil).Code)

	w := s3Request(t, srv, http.MethodPut, "/bpa-recycled/obj", []byte("x"),
		map[string]string{"x-amz-acl": "public-read"})
	assert.Equal(t, http.StatusOK, w.Code,
		"a fresh bucket has no Block Public Access configuration, so the write succeeds")
}

// TestS3_DeleteBucket_RecreatedBucketDoesNotInheritVersioning asserts the
// versioning leak through behavior: an inherited Enabled status makes every write
// versioned, which a consumer sees as an unexpected x-amz-version-id.
func TestS3_DeleteBucket_RecreatedBucketDoesNotInheritVersioning(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	s3Request(t, srv, http.MethodPut, "/ver-recycled", nil, nil)
	s3Request(t, srv, http.MethodPut, "/ver-recycled?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)

	versioned := s3Request(t, srv, http.MethodPut, "/ver-recycled/obj", []byte("x"), nil)
	require.Equal(t, http.StatusOK, versioned.Code)
	require.NotEmpty(t, versioned.Header().Get("x-amz-version-id"),
		"versioning must be in effect before the delete, or the test proves nothing")

	// Emptying a versioned bucket means removing the versions, not just the keys:
	// a plain DELETE inserts a delete marker, which still counts as content.
	s3Request(t, srv, http.MethodDelete,
		"/ver-recycled/obj?versionId="+versioned.Header().Get("x-amz-version-id"), nil, nil)
	require.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/ver-recycled", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/ver-recycled", nil, nil).Code)

	w := s3Request(t, srv, http.MethodPut, "/ver-recycled/obj", []byte("x"), nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("x-amz-version-id"),
		"a fresh bucket is unversioned, so a write reports no version ID")
}

// TestS3_DeleteBucket_RefusesNoncurrentVersions asserts the widened emptiness
// check. "All objects (including all object versions and delete markers) in the
// bucket must be deleted before the bucket itself can be deleted" — a bucket whose
// only remaining content is a deleted object's history is not empty.
func TestS3_DeleteBucket_RefusesNoncurrentVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// leave puts bucket "notempty" into the state under test, having already
		// enabled versioning and written one object.
		leave func(t *testing.T, srv *emulator.Server, versionID string)
	}{
		{
			name: "a delete marker over the only object",
			leave: func(t *testing.T, srv *emulator.Server, _ string) {
				t.Helper()
				// A plain DELETE on a versioned bucket inserts a delete marker: the
				// key stops being listed but its history remains.
				require.Equal(t, http.StatusNoContent,
					s3Request(t, srv, http.MethodDelete, "/notempty/obj", nil, nil).Code)
			},
		},
		{
			// The case that distinguishes the widened check from the old one: with
			// versioning suspended, substrate's DELETE removes the object: pointer
			// outright, so the bucket looks empty to a check that reads only
			// object: while the version it wrote is still there. Real S3 keeps that
			// version too — "when you suspend versioning, existing objects in your
			// bucket do not change" — so the bucket is not empty either way.
			name: "a version left by a delete under suspended versioning",
			leave: func(t *testing.T, srv *emulator.Server, _ string) {
				t.Helper()
				require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
					"/notempty?versioning",
					[]byte(`<VersioningConfiguration><Status>Suspended</Status>`+
						`</VersioningConfiguration>`), nil).Code)
				require.Equal(t, http.StatusNoContent,
					s3Request(t, srv, http.MethodDelete, "/notempty/obj", nil, nil).Code)
			},
		},
		{
			name: "a noncurrent version under a newer one",
			leave: func(t *testing.T, srv *emulator.Server, versionID string) {
				t.Helper()
				// Overwrite, then permanently remove only the newest version.
				w := s3Request(t, srv, http.MethodPut, "/notempty/obj", []byte("v2"), nil)
				require.Equal(t, http.StatusOK, w.Code)
				newest := w.Header().Get("x-amz-version-id")
				require.NotEqual(t, versionID, newest)
				require.Equal(t, http.StatusNoContent, s3Request(t, srv,
					http.MethodDelete, "/notempty/obj?versionId="+newest, nil, nil).Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)

			s3Request(t, srv, http.MethodPut, "/notempty", nil, nil)
			s3Request(t, srv, http.MethodPut, "/notempty?versioning",
				[]byte(`<VersioningConfiguration><Status>Enabled</Status>`+
					`</VersioningConfiguration>`), nil)
			w := s3Request(t, srv, http.MethodPut, "/notempty/obj", []byte("v1"), nil)
			require.Equal(t, http.StatusOK, w.Code)

			tt.leave(t, srv, w.Header().Get("x-amz-version-id"))

			// The key is not listed as live...
			list := s3Request(t, srv, http.MethodGet, "/notempty?list-type=2", nil, nil)
			require.Equal(t, http.StatusOK, list.Code)

			// ...but the bucket is not empty.
			del := s3Request(t, srv, http.MethodDelete, "/notempty", nil, nil)
			assert.Equal(t, http.StatusConflict, del.Code)
			assert.Contains(t, del.Body.String(), "BucketNotEmpty")
		})
	}
}

// TestS3_DeleteBucket_AllVersionsRemovedIsEmpty is the companion to the test
// above: once every version is permanently gone the bucket really is empty, so the
// widened check must not have made DeleteBucket impossible to satisfy.
func TestS3_DeleteBucket_AllVersionsRemovedIsEmpty(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	s3Request(t, srv, http.MethodPut, "/emptied", nil, nil)
	s3Request(t, srv, http.MethodPut, "/emptied?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)

	w := s3Request(t, srv, http.MethodPut, "/emptied/obj", []byte("v1"), nil)
	require.Equal(t, http.StatusOK, w.Code)
	first := w.Header().Get("x-amz-version-id")

	// Delete it the way a caller does: a marker, then both versions by ID.
	marker := s3Request(t, srv, http.MethodDelete, "/emptied/obj", nil, nil)
	require.Equal(t, http.StatusNoContent, marker.Code)
	markerVersion := marker.Header().Get("x-amz-version-id")
	require.NotEmpty(t, markerVersion)

	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete,
		"/emptied/obj?versionId="+markerVersion, nil, nil).Code)
	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete,
		"/emptied/obj?versionId="+first, nil, nil).Code)

	assert.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/emptied", nil, nil).Code,
		"with every version gone the bucket is empty and deletes")
}

// TestS3_DeleteBucket_AbortsInFlightMultipartUploads asserts the multipart
// treatment. For a general purpose bucket the reference only *recommends* removing
// incomplete uploads ("While emptying your bucket, we recommend that you also
// remove all incomplete multipart uploads"); the refusal is documented for
// directory buckets, which substrate does not model. So the delete succeeds and the
// upload is aborted — an upload whose destination bucket is gone can never
// complete, and left behind it is inherited by the next bucket of that name.
func TestS3_DeleteBucket_AbortsInFlightMultipartUploads(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	srv, fs := newS3TestServerWithFS(t, state)

	s3Request(t, srv, http.MethodPut, "/mpu-recycled", nil, nil)

	create := s3Request(t, srv, http.MethodPost, "/mpu-recycled/big?uploads", nil, nil)
	require.Equal(t, http.StatusOK, create.Code)
	var initiated struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(create.Body.Bytes(), &initiated))
	require.NotEmpty(t, initiated.UploadID)

	// A part large enough to be a legal non-final part.
	part := []byte(strings.Repeat("p", 5*1024*1024))
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
		"/mpu-recycled/big?partNumber=1&uploadId="+initiated.UploadID, part, nil).Code)

	// The upload is in progress and holds a part body in the mirror.
	uploads := s3Request(t, srv, http.MethodGet, "/mpu-recycled?uploads", nil, nil)
	require.Equal(t, http.StatusOK, uploads.Code)
	require.Contains(t, uploads.Body.String(), initiated.UploadID)

	// The part records are asserted on the state store rather than through an API
	// read: once the upload is aborted there is no operation that reports its parts,
	// so a stranded part: key is invisible from outside and would leak silently.
	partKeys, err := state.List(context.Background(), "s3", "part:"+initiated.UploadID+"/")
	require.NoError(t, err)
	require.NotEmpty(t, partKeys, "the uploaded part must be recorded before the delete")

	assert.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/mpu-recycled", nil, nil).Code,
		"an in-flight upload does not refuse a general purpose bucket delete")

	// Recreate: the upload must be gone, not inherited.
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/mpu-recycled", nil, nil).Code)
	after := s3Request(t, srv, http.MethodGet, "/mpu-recycled?uploads", nil, nil)
	assert.Equal(t, http.StatusOK, after.Code)
	assert.NotContains(t, after.Body.String(), initiated.UploadID,
		"a recreated bucket must not inherit the deleted bucket's uploads")

	// Uploading a further part to the aborted upload must fail rather than resume.
	resume := s3Request(t, srv, http.MethodPut,
		"/mpu-recycled/big?partNumber=2&uploadId="+initiated.UploadID, part, nil)
	assert.Equal(t, http.StatusNotFound, resume.Code)

	// And neither the part records nor the part bodies are left behind.
	strandedParts, err := state.List(context.Background(), "s3", "part:"+initiated.UploadID+"/")
	require.NoError(t, err)
	assert.Empty(t, strandedParts, "an aborted upload's part records must not be left behind")

	exists, err := afero.DirExists(fs, "/.multipart/"+initiated.UploadID)
	require.NoError(t, err)
	assert.False(t, exists, "an aborted upload's part bodies must not be left behind")
}

// TestS3_DeleteBucket_AbortsOnlyThisBucketsUploads is the other half of the abort:
// a multipart upload is keyed by upload ID alone, so the scan sees every bucket's
// uploads and must abort only the ones whose Bucket field names the bucket being
// deleted. Without that comparison, deleting one bucket silently destroys an
// unrelated bucket's in-flight upload.
func TestS3_DeleteBucket_AbortsOnlyThisBucketsUploads(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	begin := func(bucket string) string {
		t.Helper()
		require.Equal(t, http.StatusOK,
			s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)
		create := s3Request(t, srv, http.MethodPost, "/"+bucket+"/big?uploads", nil, nil)
		require.Equal(t, http.StatusOK, create.Code)
		var initiated struct {
			UploadID string `xml:"UploadId"`
		}
		require.NoError(t, xml.Unmarshal(create.Body.Bytes(), &initiated))
		require.NotEmpty(t, initiated.UploadID)
		return initiated.UploadID
	}

	doomed := begin("mpu-doomed")
	survivor := begin("mpu-survivor")
	require.NotEqual(t, doomed, survivor)

	require.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/mpu-doomed", nil, nil).Code)

	uploads := s3Request(t, srv, http.MethodGet, "/mpu-survivor?uploads", nil, nil)
	require.Equal(t, http.StatusOK, uploads.Code)
	assert.Contains(t, uploads.Body.String(), survivor,
		"deleting one bucket must not abort another bucket's upload")

	// And it is still usable, not merely still listed.
	part := []byte(strings.Repeat("p", 5*1024*1024))
	assert.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
		"/mpu-survivor/big?partNumber=1&uploadId="+survivor, part, nil).Code)
}

// TestS3_DeleteBucket_VersionDeletePromotesPredecessor pins the promotion half of
// [S3Plugin.promoteCurrentVersion]: "if you delete the current object version, ...
// the version that is next in the version stack becomes the current version". A
// pointer left stale reports the deleted version's body; a pointer merely cleared
// loses an object that still has versions.
func TestS3_DeleteBucket_VersionDeletePromotesPredecessor(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/promoted", nil, nil).Code)
	s3Request(t, srv, http.MethodPut, "/promoted?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)

	first := s3Request(t, srv, http.MethodPut, "/promoted/obj", []byte("older"), nil)
	require.Equal(t, http.StatusOK, first.Code)
	second := s3Request(t, srv, http.MethodPut, "/promoted/obj", []byte("newer"), nil)
	require.Equal(t, http.StatusOK, second.Code)
	newest := second.Header().Get("x-amz-version-id")
	require.NotEmpty(t, newest)

	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete,
		"/promoted/obj?versionId="+newest, nil, nil).Code)

	// An unversioned GET reads the current pointer, so this is what a caller sees.
	get := s3Request(t, srv, http.MethodGet, "/promoted/obj", nil, nil)
	require.Equal(t, http.StatusOK, get.Code,
		"the key still has a version, so it must still be readable")
	assert.Equal(t, "older", get.Body.String(),
		"deleting the current version promotes the next one in the version stack")
	assert.Equal(t, first.Header().Get("ETag"), get.Header().Get("ETag"),
		"the promoted body and its recorded ETag must describe the same bytes")

	// And the key is still listed: a cleared pointer would drop it entirely.
	list := s3Request(t, srv, http.MethodGet, "/promoted?list-type=2", nil, nil)
	require.Equal(t, http.StatusOK, list.Code)
	assert.Contains(t, list.Body.String(), "<Key>obj</Key>")
}

// TestS3_DeleteBucket_VersionDeletePromotesDeleteMarker is the promotion case with
// no body to promote: a delete marker is a version in the stack, so deleting the
// newer object version above it makes the marker current again and the key stops
// being readable. A marker has no stored body, so treating an absent versioned body
// as a failure would turn this ordinary sequence into a 500.
func TestS3_DeleteBucket_VersionDeletePromotesDeleteMarker(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/markerpromo", nil, nil).Code)
	s3Request(t, srv, http.MethodPut, "/markerpromo?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/markerpromo/obj", []byte("v1"), nil).Code)

	// A plain delete stacks a marker on top...
	marker := s3Request(t, srv, http.MethodDelete, "/markerpromo/obj", nil, nil)
	require.Equal(t, http.StatusNoContent, marker.Code)
	require.NotEmpty(t, marker.Header().Get("x-amz-version-id"))

	// ...a write stacks a new version above the marker...
	second := s3Request(t, srv, http.MethodPut, "/markerpromo/obj", []byte("v2"), nil)
	require.Equal(t, http.StatusOK, second.Code)

	// ...and removing that version makes the marker current again.
	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete,
		"/markerpromo/obj?versionId="+second.Header().Get("x-amz-version-id"), nil, nil).Code)

	get := s3Request(t, srv, http.MethodGet, "/markerpromo/obj", nil, nil)
	assert.Equal(t, http.StatusNotFound, get.Code,
		"a key whose current version is a delete marker is a 404, not a 500")
	assert.Equal(t, "true", get.Header().Get("x-amz-delete-marker"))
}

// TestS3_DeleteBucket_PromotingADirectoryMarkerTouchesNoBody extends #534 to the
// promotion path: promoting a version of a key ending in "/" must not write to the
// object mirror, whose path for such a key is the directory node holding its
// children. Nothing warns of the collision — afero's ReadFile of a directory returns
// an empty body and no error, so an unguarded promote writes an empty file over the
// node and the next child delete panics.
func TestS3_DeleteBucket_PromotingADirectoryMarkerTouchesNoBody(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/promomarker", nil, nil).Code)
	s3Request(t, srv, http.MethodPut, "/promomarker?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)

	// Two versions of the directory marker, and a child beneath it.
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/promomarker/dir/", nil, nil).Code)
	newer := s3Request(t, srv, http.MethodPut, "/promomarker/dir/", nil, nil)
	require.Equal(t, http.StatusOK, newer.Code)
	require.NotEmpty(t, newer.Header().Get("x-amz-version-id"))
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
		"/promomarker/dir/child.txt", []byte("child body"), nil).Code)

	// Removing the newer marker version promotes the older one.
	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete,
		"/promomarker/dir/?versionId="+newer.Header().Get("x-amz-version-id"), nil, nil).Code)

	// The child is intact and still deletable — the delete is the call that reaches
	// afero's Remove and panics when the directory node has been overwritten.
	child := s3Request(t, srv, http.MethodGet, "/promomarker/dir/child.txt", nil, nil)
	assert.Equal(t, http.StatusOK, child.Code)
	assert.Equal(t, "child body", child.Body.String())
	assert.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete,
		"/promomarker/dir/child.txt?versionId="+
			childVersionID(t, srv, "/promomarker", "dir/child.txt"), nil, nil).Code)
}

// TestS3_DeleteBucket_PromotingAMarkerDoesNotClobberItsSibling is the destructive
// half of the case above, and needs no children: "dir" and "dir/" are distinct S3
// keys whose mirror paths coincide, so promoting a version of the marker "dir/" must
// not write to the body of the object at "dir".
//
// It is reachable because the marker's versioned source path collides too:
// .versions/dir//<versionID> normalizes onto .versions/dir/<versionID>, which is the
// directory holding the versions of a child key named dir/<versionID>. afero reads
// that directory as an empty body with no error, so the unguarded promote truncates
// the sibling to zero bytes and reports success.
func TestS3_DeleteBucket_PromotingAMarkerDoesNotClobberItsSibling(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sibpromo", nil, nil).Code)
	s3Request(t, srv, http.MethodPut, "/sibpromo?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
		"/sibpromo/dir", []byte("real body"), nil).Code)

	// Two versions of the marker, so removing one promotes the other.
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sibpromo/dir/", nil, nil).Code)
	newer := s3Request(t, srv, http.MethodPut, "/sibpromo/dir/", nil, nil)
	require.Equal(t, http.StatusOK, newer.Code)

	// A child key whose name begins with the promoted version's ID is what makes
	// the collided source path a directory rather than absent.
	older := childVersionID(t, srv, "/sibpromo", "dir/")
	require.NotEqual(t, newer.Header().Get("x-amz-version-id"), older)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut,
		"/sibpromo/dir/"+older, []byte("collider"), nil).Code)

	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete,
		"/sibpromo/dir/?versionId="+newer.Header().Get("x-amz-version-id"), nil, nil).Code)

	sibling := s3Request(t, srv, http.MethodGet, "/sibpromo/dir", nil, nil)
	assert.Equal(t, http.StatusOK, sibling.Code)
	assert.Equal(t, "real body", sibling.Body.String(),
		`promoting a version of "dir/" must not touch the object at "dir"`)
}

// childVersionID returns the oldest version ID of key in bucket, read back from
// ListObjectVersions the way a client would.
func childVersionID(t *testing.T, srv *emulator.Server, bucketPath, key string) string {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, bucketPath+"?versions", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	var listing struct {
		Versions []struct {
			Key       string `xml:"Key"`
			VersionID string `xml:"VersionId"`
		} `xml:"Version"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &listing))
	// The listing is newest-first, so the last match is the oldest version.
	id := ""
	for _, v := range listing.Versions {
		if v.Key == key {
			id = v.VersionID
		}
	}
	if id == "" {
		t.Fatalf("no version of %q in %s", key, w.Body.String())
	}
	return id
}

// TestS3_DeleteBucket_ClearsObjectScopedLeftovers covers the keys that outlive the
// object they describe. An object ACL is stored under its own key, so a plain
// DeleteObject leaves it behind; the bucket then satisfies the emptiness check and
// its delete used to strand the ACL under a name a later CreateBucket reuses.
//
// The assertion is over the *whole* s3 namespace rather than a read of the recreated
// bucket, for two reasons. A fresh write clears its own object_acl: entry
// ([S3Plugin.s3StoreObjectACL]), so a behavioral read cannot see the leftover at
// all; and the invariant being fixed is that no key scoped to the deleted bucket
// survives it, which is a statement about the namespace, not about one read.
func TestS3_DeleteBucket_ClearsObjectScopedLeftovers(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	srv := newS3TestServerWithState(t, state)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/leftovers", nil, nil).Code)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/leftovers/obj",
		[]byte("x"), map[string]string{"x-amz-acl": "public-read"}).Code)

	// A plain delete removes the object but not its ACL: that is the leftover.
	require.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/leftovers/obj", nil, nil).Code)
	stranded, err := state.List(context.Background(), "s3", "object_acl:leftovers/")
	require.NoError(t, err)
	require.NotEmpty(t, stranded,
		"the object ACL must outlive its object, or the test proves nothing")

	require.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/leftovers", nil, nil).Code)

	// object_versions: is in the clear list for the same reason even though the
	// version-delete path now tidies it up itself ([S3Plugin.promoteCurrentVersion]):
	// the bucket's namespace is cleared as a whole, so a future key that outlives its
	// object cannot reintroduce this leak.
	for _, prefix := range []string{
		"object:", "object_version:", "object_versions:", "object_acl:",
	} {
		keys, listErr := state.List(context.Background(), "s3", prefix+"leftovers/")
		require.NoError(t, listErr)
		assert.Empty(t, keys, "no %s key may outlive its bucket", prefix)
	}
}

// TestS3_DeleteBucket_ErrorsPropagate asserts the judgement call the issue leaves
// open: a failure to clear a bucket-scoped key is reported rather than swallowed,
// and the bucket record survives it. A silent partial clear is the bug being fixed,
// so a discarded error here would hide a recurrence.
func TestS3_DeleteBucket_ErrorsPropagate(t *testing.T) {
	// Not parallel: pabFaultState is armed after setup, and the shared bucket name
	// keeps each case's keys distinct anyway.
	for _, key := range []string{
		"bucket_acl:failbucket",
		"bucket_policy:failbucket",
		"bucket_lifecycle:failbucket",
		"bucket_versioning:failbucket",
		"notification:failbucket",
		"bucket_public_access_block:failbucket",
	} {
		t.Run(key, func(t *testing.T) {
			state := &pabFaultState{
				inner:   emulator.NewMemoryStateManager(),
				key:     key,
				err:     errors.New("state store unavailable"),
				onDelet: true,
			}
			srv := newS3TestServerWithState(t, state)

			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/failbucket", nil, nil).Code)

			state.armed = true

			w := s3Request(t, srv, http.MethodDelete, "/failbucket", nil, nil)
			assert.Equal(t, http.StatusInternalServerError, w.Code,
				"a failed clear must be reported, not swallowed: %s", w.Body.String())

			// The bucket is still there: the delete failed, so it must not have
			// half happened. A caller can retry, and HeadBucket agrees with the
			// error rather than contradicting it.
			state.armed = false
			assert.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodHead, "/failbucket", nil, nil).Code,
				"a failed DeleteBucket must leave the bucket present")
		})
	}
}
