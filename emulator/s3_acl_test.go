package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The predefined LogDelivery group, spelled out here for the same reason the two
// public groups are in s3_publicaccess_test.go: a change to the package's constant
// has to be reflected deliberately.
const testGroupLogDelivery = "http://acs.amazonaws.com/groups/s3/LogDelivery"

// ownerGrant renders the owner's FULL_CONTROL grant for a bucket, which every ACL
// substrate stores carries: "When Amazon S3 receives a request with a canned ACL in
// the request, it adds the predefined grants to the ACL of the resource", and a new
// bucket or object starts with one granting its owner FULL_CONTROL.
func ownerGrant(bucket string) string {
	return bucket + "-owner/FULL_CONTROL"
}

// putObjectWithACL writes an object with the supplied headers, requiring a 200. It
// exists so a test asserting on the resulting ACL cannot accidentally pass because
// the write itself was refused.
func putObjectWithACL(t *testing.T, srv *emulator.Server, bucket, key string, headers map[string]string) {
	t.Helper()
	w := s3Request(t, srv, http.MethodPut, "/"+bucket+"/"+key, []byte("payload"), headers)
	require.Equal(t, http.StatusOK, w.Code, "PutObject: %s", w.Body.String())
}

// createBucketWithACL creates a bucket with the supplied headers, requiring a 200.
func createBucketWithACL(t *testing.T, srv *emulator.Server, bucket string, headers map[string]string) {
	t.Helper()
	w := s3Request(t, srv, http.MethodPut, "/"+bucket, nil, headers)
	require.Equal(t, http.StatusOK, w.Code, "CreateBucket: %s", w.Body.String())
}

// storedACLAbsent reports whether no ACL is stored for a key at all, which is a
// different observation from a stored owner-only ACL: the two report identically
// through GetObjectAcl, and only one of them records that the caller named an ACL.
func storedACLAbsent(t *testing.T, state emulator.StateManager, stateKey string) bool {
	t.Helper()
	raw, err := state.Get(t.Context(), "s3", stateKey)
	require.NoError(t, err)
	return raw == nil
}

// cannedACLCase is one canned ACL name and the grants it resolves to on each kind
// of resource, from the user guide's Canned ACL table.
type cannedACLCase struct {
	name string
	// bucketExtra and objectExtra are the grants beyond the owner's, which differ
	// between the two kinds for three of the seven names.
	bucketExtra []string
	objectExtra []string
}

// cannedACLCases is the Canned ACL table, reduced to cases. Every entry names its
// documented outcome, including the four that resolve to owner-only — each for its
// own reason rather than by falling through, which is why they are listed rather
// than assumed.
func cannedACLCases() []cannedACLCase {
	return []cannedACLCase{
		{
			// "Owner gets FULL_CONTROL. No one else has access rights (default)."
			name: "private",
		},
		{
			// "The AllUsers group … gets READ access."
			name:        "public-read",
			bucketExtra: []string{testGroupAllUsers + "/READ"},
			objectExtra: []string{testGroupAllUsers + "/READ"},
		},
		{
			// "AllUsers group gets READ and WRITE access."
			name:        "public-read-write",
			bucketExtra: []string{testGroupAllUsers + "/READ", testGroupAllUsers + "/WRITE"},
			objectExtra: []string{testGroupAllUsers + "/READ", testGroupAllUsers + "/WRITE"},
		},
		{
			// "The AuthenticatedUsers group gets READ access." Public by Block Public
			// Access's own definition, which is what makes this row load-bearing rather
			// than decorative — see TestS3_ACL_AuthenticatedReadIsPublic.
			name:        "authenticated-read",
			bucketExtra: []string{testGroupAuthenticatedUsers + "/READ"},
			objectExtra: []string{testGroupAuthenticatedUsers + "/READ"},
		},
		{
			// "Applies to: Bucket" — "The LogDelivery group gets WRITE and READ_ACP
			// permissions on the bucket." The table gives it no object meaning, so on an
			// object it resolves to owner-only.
			name: "log-delivery-write",
			bucketExtra: []string{
				testGroupLogDelivery + "/WRITE",
				testGroupLogDelivery + "/READ_ACP",
			},
		},
		{
			// Grants Amazon EC2 READ, whose canonical user ID AWS does not publish, so
			// substrate cannot name the grantee and records no second grant.
			name: "aws-exec-read",
		},
		{
			// "Applies to: Object", and substrate has one owner identity per bucket, so
			// the object owner and the bucket owner are the same principal and the grants
			// collapse. On a bucket, "Amazon S3 ignores it" — the same answer.
			name: "bucket-owner-read",
		},
		{
			name: "bucket-owner-full-control",
		},
	}
}

// TestS3_ACL_CannedOnPutObject asserts a canned ACL named on PutObject is stored and
// reported by GetObjectAcl.
//
// This is #470's first consequence, independent of Block Public Access: putObject did
// not read x-amz-acl at all, so the ACL GetObjectAcl reported was never the one the
// write set. A consumer asserting "my upload is publicly readable" saw the default
// owner-only ACL and had no way to tell the header had been discarded.
func TestS3_ACL_CannedOnPutObject(t *testing.T) {
	t.Parallel()

	for _, tc := range cannedACLCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			createBucketWithACL(t, srv, "canned-obj", nil)
			putObjectWithACL(t, srv, "canned-obj", "k.txt", map[string]string{"x-amz-acl": tc.name})

			want := append([]string{ownerGrant("canned-obj")}, tc.objectExtra...)
			assert.Equal(t, want, getObjectACLGrants(t, srv, "canned-obj", "k.txt"))
		})
	}
}

// TestS3_ACL_CannedOnCreateBucket is the bucket-level counterpart. createBucket read
// no ACL header either, so a bucket created public reported itself private.
func TestS3_ACL_CannedOnCreateBucket(t *testing.T) {
	t.Parallel()

	for _, tc := range cannedACLCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			createBucketWithACL(t, srv, "canned-bkt", map[string]string{"x-amz-acl": tc.name})

			want := append([]string{ownerGrant("canned-bkt")}, tc.bucketExtra...)
			assert.Equal(t, want, getBucketACLGrants(t, srv, "canned-bkt"))
		})
	}
}

// TestS3_ACL_LogDeliveryWriteIsBucketOnly pins the resource-kind distinction on its
// own, because it is the one a kind-blind resolver gets wrong in both directions: it
// would either grant LogDelivery WRITE on an object, which the table gives no
// meaning, or drop it on a bucket, where the table is explicit.
func TestS3_ACL_LogDeliveryWriteIsBucketOnly(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	createBucketWithACL(t, srv, "logs", map[string]string{"x-amz-acl": "log-delivery-write"})
	putObjectWithACL(t, srv, "logs", "k.txt", map[string]string{"x-amz-acl": "log-delivery-write"})

	assert.Equal(t, []string{
		ownerGrant("logs"),
		testGroupLogDelivery + "/WRITE",
		testGroupLogDelivery + "/READ_ACP",
	}, getBucketACLGrants(t, srv, "logs"), "the table says Applies to: Bucket")

	assert.Equal(t, []string{ownerGrant("logs")}, getObjectACLGrants(t, srv, "logs", "k.txt"),
		"the table gives log-delivery-write no object meaning")
}

// TestS3_ACL_UnrecognizedCannedValueIsOwnerOnly asserts a canned name substrate does
// not know is accepted and resolves to owner-only rather than being refused.
//
// The per-operation Valid Values lists differ — CreateBucket and PutBucketAcl document
// four names, PutObject and PutObjectAcl seven — and no error code is documented for a
// name outside them, so refusing would be substrate rejecting a request real S3 may
// accept. That includes the three object-only names on a bucket, which the table says
// S3 "ignores".
func TestS3_ACL_UnrecognizedCannedValueIsOwnerOnly(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	createBucketWithACL(t, srv, "unknown-canned", map[string]string{"x-amz-acl": "not-a-canned-acl"})
	putObjectWithACL(t, srv, "unknown-canned", "k.txt", map[string]string{"x-amz-acl": "also-not-one"})

	assert.Equal(t, []string{ownerGrant("unknown-canned")}, getBucketACLGrants(t, srv, "unknown-canned"))
	assert.Equal(t, []string{ownerGrant("unknown-canned")}, getObjectACLGrants(t, srv, "unknown-canned", "k.txt"))
}

// grantHeaderCase is one x-amz-grant-* header and the permission it grants.
type grantHeaderCase struct {
	header     string
	permission string
}

// grantHeaderCases are the five headers, each naming grantees for one permission.
//
// x-amz-grant-write is included for PutObject even though its request syntax omits it
// — the permissions table gives WRITE no object meaning ("Not applicable"). Substrate
// records it rather than inventing a rejection no error code is documented for, and
// this table pins that choice: refusing a request real S3 may accept is the defect
// this family of fixes exists to remove.
func grantHeaderCases() []grantHeaderCase {
	return []grantHeaderCase{
		{header: "x-amz-grant-full-control", permission: "FULL_CONTROL"},
		{header: "x-amz-grant-read", permission: "READ"},
		{header: "x-amz-grant-read-acp", permission: "READ_ACP"},
		{header: "x-amz-grant-write", permission: "WRITE"},
		{header: "x-amz-grant-write-acp", permission: "WRITE_ACP"},
	}
}

// TestS3_ACL_GrantHeadersStored asserts every x-amz-grant-* header is stored on every
// operation that accepts the family.
//
// An ACL set through grant headers was stored by **no** operation before #470 —
// including PutBucketAcl and PutObjectAcl, whose whole purpose is to set one. That is
// wider than #470's title suggests: the headers were parsed only to decide whether a
// Block Public Access check should refuse, so a non-public grant was silently
// discarded and a public one was refused without ever being storable.
func TestS3_ACL_GrantHeadersStored(t *testing.T) {
	t.Parallel()

	for _, gh := range grantHeaderCases() {
		t.Run(gh.header, func(t *testing.T) {
			t.Parallel()
			headers := map[string]string{gh.header: `id="grantee-1"`}
			want := []string{ownerGrant("grants"), "grantee-1/" + gh.permission}

			// CreateBucket and PutObject, the two operations #470 names.
			srv, _ := newS3TestServer(t)
			createBucketWithACL(t, srv, "grants", headers)
			putObjectWithACL(t, srv, "grants", "k.txt", headers)
			assert.Equal(t, want, getBucketACLGrants(t, srv, "grants"), "CreateBucket")
			assert.Equal(t, want, getObjectACLGrants(t, srv, "grants", "k.txt"), "PutObject")

			// PutBucketAcl and PutObjectAcl, which were equally affected. A fresh
			// server, so these cannot pass on what the writes above stored.
			srv2, _ := newS3TestServer(t)
			createBucketWithACL(t, srv2, "grants", nil)
			putObjectWithACL(t, srv2, "grants", "k.txt", nil)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv2, http.MethodPut, "/grants?acl", nil, headers).Code)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv2, http.MethodPut, "/grants/k.txt?acl", nil, headers).Code)
			assert.Equal(t, want, getBucketACLGrants(t, srv2, "grants"), "PutBucketAcl")
			assert.Equal(t, want, getObjectACLGrants(t, srv2, "grants", "k.txt"), "PutObjectAcl")
		})
	}
}

// TestS3_ACL_GrantHeaderGranteeForms covers the grantee spellings one header value can
// carry: the two supported types, a quoted and an unquoted value, and a multi-grantee
// list.
func TestS3_ACL_GrantHeaderGranteeForms(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		value string
		want  []string
	}{
		{name: "quoted id", value: `id="abc123"`, want: []string{"abc123/READ"}},
		{name: "unquoted id", value: "id=abc123", want: []string{"abc123/READ"}},
		{name: "uri", value: `uri="` + testGroupLogDelivery + `"`, want: []string{testGroupLogDelivery + "/READ"}},
		{
			name:  "list of id and uri",
			value: `id="abc123", uri="` + testGroupLogDelivery + `"`,
			want:  []string{"abc123/READ", testGroupLogDelivery + "/READ"},
		},
		{
			// The grantee type is matched case-insensitively, because the documented
			// spelling of one of the three is mixed-case (emailAddress).
			name:  "mixed-case type",
			value: `ID="abc123"`,
			want:  []string{"abc123/READ"},
		},
		{
			// A value that is not type=value produces no grantee rather than a
			// malformed one. No error code is documented for a malformed grant header.
			name:  "no equals sign",
			value: "abc123",
			want:  nil,
		},
		{
			// An empty value is skipped and the rest of the list still parsed. A
			// parser that stopped at the first unusable entry would silently drop
			// every grantee after it, which is the direction that loses a grant the
			// caller asked for.
			name:  "empty value before a good one",
			value: `id=, uri="` + testGroupLogDelivery + `"`,
			want:  []string{testGroupLogDelivery + "/READ"},
		},
		{
			name:  "empty value after a good one",
			value: `id="abc123", uri=`,
			want:  []string{"abc123/READ"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			createBucketWithACL(t, srv, "forms", map[string]string{"x-amz-grant-read": tc.value})

			assert.Equal(t, append([]string{ownerGrant("forms")}, tc.want...),
				getBucketACLGrants(t, srv, "forms"))
		})
	}
}

// TestS3_ACL_EmailGranteeSkipped pins the scope decision behind #507: an emailAddress
// grantee is skipped, not stored and not refused.
//
// S3 has ended support for email grantees — "As of October 1, 2025 … the request will
// receive an HTTP 405 (Method Not Allowed) error" — and substrate's clock is past that
// date, but returning the 405 is Region-conditional and applies to the XML body form
// too, so it is filed rather than guessed at. This test exists so that when #507 lands
// it changes this behavior deliberately.
func TestS3_ACL_EmailGranteeSkipped(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	createBucketWithACL(t, srv, "email-grantee", map[string]string{
		"x-amz-grant-read": `emailAddress="someone@example.com"`,
	})

	assert.Equal(t, []string{ownerGrant("email-grantee")}, getBucketACLGrants(t, srv, "email-grantee"),
		"an email grantee is skipped today; #507 makes it a 405")
}

// TestS3_ACL_GrantHeadersAugmentTheOwnerGrant asserts a grant header adds to the
// default ACL rather than replacing it.
//
// "you specify explicit access permissions and grantees … These permissions are then
// added to the ACL on the object. By default, all objects are private. Only the owner
// has full access control." A resolver that built the ACL from the headers alone would
// silently drop the owner's FULL_CONTROL, which is a lockout rather than a grant.
func TestS3_ACL_GrantHeadersAugmentTheOwnerGrant(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	createBucketWithACL(t, srv, "augment", map[string]string{
		"x-amz-grant-read":         `id="reader"`,
		"x-amz-grant-full-control": `id="admin"`,
	})

	grants := getBucketACLGrants(t, srv, "augment")
	assert.Equal(t, ownerGrant("augment"), grants[0], "the owner keeps FULL_CONTROL")
	assert.ElementsMatch(t, []string{ownerGrant("augment"), "reader/READ", "admin/FULL_CONTROL"}, grants)
}

// TestS3_ACL_GrantHeadersWinOverCannedHeader pins the precedence between the two
// forms.
//
// They are documented mutually exclusive — "If you use these ACL-specific headers, you
// cannot use the x-amz-acl header to set a canned ACL" — but no error code is
// documented for sending both, so substrate resolves rather than refuses and the grant
// headers win, being the more specific expression. Which one wins cannot open a Block
// Public Access hole either way, since the check reads the resolved ACL and the raw
// headers both.
func TestS3_ACL_GrantHeadersWinOverCannedHeader(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	createBucketWithACL(t, srv, "both-forms", map[string]string{
		"x-amz-acl":        "public-read",
		"x-amz-grant-read": `id="reader"`,
	})

	assert.Equal(t, []string{ownerGrant("both-forms"), "reader/READ"},
		getBucketACLGrants(t, srv, "both-forms"),
		"the grant headers are the more specific expression")
}

// TestS3_ACL_NoHeaderStoresNothing asserts a write naming no ACL stores no ACL, and
// still reports the owner-only default.
//
// The stored-state half is the point: a stored owner-only ACL and no stored ACL report
// identically through GetObjectAcl, and only one of them records that the caller asked
// for anything. Keeping the distinction is what lets an overwrite clear a previous ACL
// without a parallel "was one named" flag threaded through every write path.
func TestS3_ACL_NoHeaderStoresNothing(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	srv, _ := newS3TestServerWithFS(t, state)
	createBucketWithACL(t, srv, "bare", nil)
	putObjectWithACL(t, srv, "bare", "k.txt", nil)

	assert.Equal(t, []string{ownerGrant("bare")}, getBucketACLGrants(t, srv, "bare"))
	assert.Equal(t, []string{ownerGrant("bare")}, getObjectACLGrants(t, srv, "bare", "k.txt"))

	assert.True(t, storedACLAbsent(t, state, "bucket_acl:bare"),
		"CreateBucket named no ACL, so nothing should be stored")
	assert.True(t, storedACLAbsent(t, state, "object_acl:bare/k.txt"),
		"PutObject named no ACL, so nothing should be stored")
}

// TestS3_ACL_ExplicitPrivateStoresAnACL is the converse of
// TestS3_ACL_NoHeaderStoresNothing, and the assertion that keeps "named none" from
// collapsing into "named the default".
//
// An x-amz-acl of private resolves to the same owner-only grants as the default, so the
// two are indistinguishable through GetObjectAcl. The stored state is where they differ,
// and the difference is load-bearing: only a request that named nothing may inherit
// whatever the resolver later decides a default is, and only a stored ACL survives as a
// record that the caller asked for one.
func TestS3_ACL_ExplicitPrivateStoresAnACL(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	srv, _ := newS3TestServerWithFS(t, state)
	createBucketWithACL(t, srv, "explicit", map[string]string{"x-amz-acl": "private"})
	putObjectWithACL(t, srv, "explicit", "k.txt", map[string]string{"x-amz-acl": "private"})

	assert.Equal(t, []string{ownerGrant("explicit")}, getBucketACLGrants(t, srv, "explicit"))
	assert.Equal(t, []string{ownerGrant("explicit")}, getObjectACLGrants(t, srv, "explicit", "k.txt"))

	assert.False(t, storedACLAbsent(t, state, "bucket_acl:explicit"),
		"CreateBucket named private, so an ACL should be stored")
	assert.False(t, storedACLAbsent(t, state, "object_acl:explicit/k.txt"),
		"PutObject named private, so an ACL should be stored")
}

// TestS3_ACL_OverwriteWithNoHeaderClearsStoredACL is the one a convenient
// implementation gets wrong.
//
// A PUT is a whole-object replacement — "You cannot use PutObject to only update a
// single piece of metadata for an existing object. You must put the entire object with
// updated metadata" — so an overwrite naming no ACL must report owner-only afterwards
// even if the key previously carried a public grant. Skipping the clear would silently
// preserve a public grant across an overwrite that asked for none, which is the
// direction that leaves data exposed.
func TestS3_ACL_OverwriteWithNoHeaderClearsStoredACL(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	srv, _ := newS3TestServerWithFS(t, state)
	createBucketWithACL(t, srv, "overwrite", nil)

	putObjectWithACL(t, srv, "overwrite", "k.txt", map[string]string{"x-amz-acl": "public-read"})
	require.Equal(t, []string{ownerGrant("overwrite"), testGroupAllUsers + "/READ"},
		getObjectACLGrants(t, srv, "overwrite", "k.txt"))

	putObjectWithACL(t, srv, "overwrite", "k.txt", nil)
	assert.Equal(t, []string{ownerGrant("overwrite")}, getObjectACLGrants(t, srv, "overwrite", "k.txt"),
		"a PUT replaces the whole object, its ACL included")
	assert.True(t, storedACLAbsent(t, state, "object_acl:overwrite/k.txt"))
}

// TestS3_ACL_OverwriteClearsAnACLSetByPutObjectAcl is the same rule against the other
// way an ACL arrives: an ACL set by a separate PutObjectAcl call does not survive a
// later PutObject either, because the PUT replaced the object it was attached to.
func TestS3_ACL_OverwriteClearsAnACLSetByPutObjectAcl(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	createBucketWithACL(t, srv, "overwrite2", nil)
	putObjectWithACL(t, srv, "overwrite2", "k.txt", nil)

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/overwrite2/k.txt?acl", nil,
		map[string]string{"x-amz-acl": "public-read"}).Code)
	require.Contains(t, getObjectACLGrants(t, srv, "overwrite2", "k.txt"), testGroupAllUsers+"/READ")

	putObjectWithACL(t, srv, "overwrite2", "k.txt", nil)
	assert.Equal(t, []string{ownerGrant("overwrite2")}, getObjectACLGrants(t, srv, "overwrite2", "k.txt"))
}

// TestS3_ACL_CopyObjectDoesNotInherit asserts a copy takes its ACL from the request
// and never from the source.
//
// "When you copy an object, the ACL metadata is not preserved and is set to private by
// default. Only the owner has full access control. To override the default ACL setting,
// specify a new ACL when you generate a copy request." So a copy of a public object is
// private unless the copy request says otherwise — the opposite of the metadata
// families, where COPY is the default directive.
func TestS3_ACL_CopyObjectDoesNotInherit(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	createBucketWithACL(t, srv, "copy-acl", nil)
	putObjectWithACL(t, srv, "copy-acl", "src", map[string]string{"x-amz-acl": "public-read"})
	require.Contains(t, getObjectACLGrants(t, srv, "copy-acl", "src"), testGroupAllUsers+"/READ")

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/copy-acl/plain", nil,
		map[string]string{"X-Amz-Copy-Source": "/copy-acl/src"}).Code)
	assert.Equal(t, []string{ownerGrant("copy-acl")}, getObjectACLGrants(t, srv, "copy-acl", "plain"),
		"the source's public grant must not cross the copy")

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/copy-acl/granted", nil,
		map[string]string{
			"X-Amz-Copy-Source": "/copy-acl/src",
			"x-amz-grant-read":  `id="reader"`,
		}).Code)
	assert.Equal(t, []string{ownerGrant("copy-acl"), "reader/READ"},
		getObjectACLGrants(t, srv, "copy-acl", "granted"),
		"the copy request's own ACL is honored")
}

// TestS3_ACL_CopyOverAPublicObjectClearsItsACL is the copy path's version of the
// overwrite rule: a copy onto a key that already carries a public ACL replaces the
// whole object, so the ACL goes with it.
func TestS3_ACL_CopyOverAPublicObjectClearsItsACL(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	createBucketWithACL(t, srv, "copy-over", nil)
	putObjectWithACL(t, srv, "copy-over", "src", nil)
	putObjectWithACL(t, srv, "copy-over", "dst", map[string]string{"x-amz-acl": "public-read"})
	require.Contains(t, getObjectACLGrants(t, srv, "copy-over", "dst"), testGroupAllUsers+"/READ")

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/copy-over/dst", nil,
		map[string]string{"X-Amz-Copy-Source": "/copy-over/src"}).Code)
	assert.Equal(t, []string{ownerGrant("copy-over")}, getObjectACLGrants(t, srv, "copy-over", "dst"))
}

// completeUpload runs a whole one-part multipart upload against an existing bucket and
// returns the upload ID, so an ACL assertion can be made against the assembled object.
func completeUpload(t *testing.T, srv *emulator.Server, bucket, key string, headers map[string]string) string {
	t.Helper()
	iw := s3Request(t, srv, http.MethodPost, "/"+bucket+"/"+key+"?uploads", nil, headers)
	require.Equal(t, http.StatusOK, iw.Code, "CreateMultipartUpload: %s", iw.Body.String())

	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))

	etag := uploadPart(t, srv, bucket, key, ir.UploadID, 1, []byte("part one"))
	cw := s3Request(t, srv, http.MethodPost,
		fmt.Sprintf("/%s/%s?uploadId=%s", bucket, key, ir.UploadID), completeBody(etag), nil)
	require.Equal(t, http.StatusOK, cw.Code, "CompleteMultipartUpload: %s", cw.Body.String())
	return ir.UploadID
}

// TestS3_ACL_MultipartCarriesTheCreateACL asserts an ACL named at
// CreateMultipartUpload reaches the object CompleteMultipartUpload assembles.
//
// Create is the only place it can be supplied — Complete's request accepts no ACL
// header, exactly as it accepts no encryption header — so an ACL not carried on the
// upload record is lost for good. That is the same shape #492 established for
// server-side encryption, which is why the field sits beside it.
func TestS3_ACL_MultipartCarriesTheCreateACL(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	srv, _ := newS3TestServerWithFS(t, state)
	createBucketWithACL(t, srv, "mpu-acl", nil)

	uploadID := completeUpload(t, srv, "mpu-acl", "big.bin", map[string]string{
		"x-amz-acl": "public-read",
	})

	assert.Equal(t, []string{ownerGrant("mpu-acl"), testGroupAllUsers + "/READ"},
		getObjectACLGrants(t, srv, "mpu-acl", "big.bin"))

	// The upload record is deleted by Complete, so the ACL is asserted on a second,
	// still-open upload — the storage half of the round-trip, which a header echo
	// could not prove.
	iw := s3Request(t, srv, http.MethodPost, "/mpu-acl/other.bin?uploads", nil,
		map[string]string{"x-amz-grant-read": `id="reader"`})
	require.Equal(t, http.StatusOK, iw.Code)
	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))
	require.NotEqual(t, uploadID, ir.UploadID)

	raw, err := state.Get(t.Context(), "s3", "multipart:"+ir.UploadID)
	require.NoError(t, err)
	var upload emulator.S3MultipartUpload
	require.NoError(t, json.Unmarshal(raw, &upload))
	require.NotNil(t, upload.ACL, "the create's ACL is recorded on the upload")
	assert.Equal(t, "reader", upload.ACL.Grants[1].Grantee.ID)
}

// TestS3_ACL_MultipartWithNoACLClearsAPreviousOne is Complete's version of the
// overwrite rule. Complete replaces the object at the key wholesale, so an upload that
// named no ACL must clear one a previous object at that key carried.
func TestS3_ACL_MultipartWithNoACLClearsAPreviousOne(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	createBucketWithACL(t, srv, "mpu-clear", nil)
	putObjectWithACL(t, srv, "mpu-clear", "big.bin", map[string]string{"x-amz-acl": "public-read"})
	require.Contains(t, getObjectACLGrants(t, srv, "mpu-clear", "big.bin"), testGroupAllUsers+"/READ")

	completeUpload(t, srv, "mpu-clear", "big.bin", nil)
	assert.Equal(t, []string{ownerGrant("mpu-clear")}, getObjectACLGrants(t, srv, "mpu-clear", "big.bin"))
}

// TestS3_ACL_CreateBucketClearsAStaleACL covers the recreate-after-delete case.
//
// DeleteBucket removes the bucket: entry and leaves its sub-resources behind, so
// without a clear on create, a bucket created public, deleted and created again would
// report the deleted bucket's ACL. The create is authoritative either way — the same
// whole-resource-replacement rule the object path follows. The other bucket
// sub-resources still leak, which is #508 rather than this fix.
func TestS3_ACL_CreateBucketClearsAStaleACL(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	createBucketWithACL(t, srv, "recreated", map[string]string{"x-amz-acl": "public-read"})
	require.Contains(t, getBucketACLGrants(t, srv, "recreated"), testGroupAllUsers+"/READ")

	require.Equal(t, http.StatusNoContent,
		s3Request(t, srv, http.MethodDelete, "/recreated", nil, nil).Code)
	createBucketWithACL(t, srv, "recreated", nil)

	assert.Equal(t, []string{ownerGrant("recreated")}, getBucketACLGrants(t, srv, "recreated"),
		"a recreated bucket must not inherit the deleted bucket's ACL")
}

// TestS3_ACL_NonCanonicalHeaderNames asserts every ACL header is read
// case-insensitively.
//
// Requests reaching a plugin have not always been through net/http's canonicalization
// — substrate builds them in-process too — so the plugin is exercised directly here.
// Sending lowercase names through the server would prove nothing, because Go would
// canonicalize them on the way in.
func TestS3_ACL_NonCanonicalHeaderNames(t *testing.T) {
	p := newS3PluginDirect(t)
	rctx := &emulator.RequestContext{AccountID: "000000000000", Region: "us-east-1"}

	// Operation carries the HTTP verb on entry; the plugin resolves the semantic
	// operation from it and the path, exactly as the server pipeline does.
	_, err := p.HandleRequest(rctx, &emulator.AWSRequest{
		Service: "s3", Operation: http.MethodPut, Path: "/acl-fold",
		Params:  map[string]string{},
		Headers: map[string]string{"X-AMZ-ACL": "public-read"}, // canonical is X-Amz-Acl
	})
	require.NoError(t, err)

	_, err = p.HandleRequest(rctx, &emulator.AWSRequest{
		Service: "s3", Operation: http.MethodPut, Path: "/acl-fold/k.txt", Body: []byte("x"),
		Params:  map[string]string{},
		Headers: map[string]string{"x-amz-grant-READ": `uri="` + testGroupLogDelivery + `"`},
	})
	require.NoError(t, err)

	for _, tc := range []struct{ path, want string }{
		{"/acl-fold", testGroupAllUsers + "/READ"},
		{"/acl-fold/k.txt", testGroupLogDelivery + "/READ"},
	} {
		resp, aclErr := p.HandleRequest(rctx, &emulator.AWSRequest{
			Service: "s3", Operation: http.MethodGet, Path: tc.path,
			Headers: map[string]string{}, Params: map[string]string{"acl": "1"},
		})
		require.NoError(t, aclErr)
		require.Equal(t, http.StatusOK, resp.StatusCode, tc.path)

		assert.Equal(t, []string{ownerGrant("acl-fold"), tc.want}, aclGrantPairs(t, resp.Body), tc.path)
	}
}
