package emulator_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Object Ownership enforcement (#1474) and email-address grantees (#507).

// s3OwnerOnlyXML is an AccessControlPolicy granting FULL_CONTROL to bucket's owner and nobody else,
// the "equivalent form expressed in XML" of bucket-owner-full-control.
func s3OwnerOnlyXML(bucket string) string {
	return fmt.Sprintf(`<AccessControlPolicy><Owner><ID>%[1]s-owner</ID></Owner><AccessControlList>`+
		`<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">`+
		`<ID>%[1]s-owner</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant>`+
		`</AccessControlList></AccessControlPolicy>`, bucket)
}

// s3EmailGranteeXML is an AccessControlPolicy granting READ to an AmazonCustomerByEmail grantee.
const s3EmailGranteeXML = `<AccessControlPolicy><Owner><ID>o</ID></Owner><AccessControlList>` +
	`<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AmazonCustomerByEmail">` +
	`<EmailAddress>someone@example.com</EmailAddress></Grantee><Permission>READ</Permission></Grant>` +
	`</AccessControlList></AccessControlPolicy>`

// s3RegionalCall issues a path-style S3 request against a Region-bearing host with a body and
// headers.
func s3RegionalCall(t *testing.T, srv *emulator.Server, method, region, path string, body []byte,
	headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	host := "s3." + region + ".amazonaws.com"
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	r := httptest.NewRequest(method, "http://"+host+path, reader)
	r.Host = host
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w
}

// s3ACLWrite is one of the ACL-bearing writes, as a request against bucket.
type s3ACLWrite struct {
	name string
	// setup prepares what the write needs besides the bucket, such as a copy source.
	setup func(t *testing.T, srv *emulator.Server, region, bucket string)
	// send issues the write with the ACL headers or body given.
	send func(t *testing.T, srv *emulator.Server, region, bucket string, body []byte,
		headers map[string]string) *httptest.ResponseRecorder
	// left reports whether the write left anything behind, for asserting a refusal wrote nothing.
	left func(t *testing.T, srv *emulator.Server, region, bucket string) bool
	// xml says the write also accepts an XML ACL body.
	xml bool
}

// s3ACLWrites are the five writes on an existing bucket that can carry an ACL.
func s3ACLWrites() []s3ACLWrite {
	putObj := func(t *testing.T, srv *emulator.Server, region, bucket string) {
		t.Helper()
		require.Equal(t, http.StatusOK, s3RegionalCall(t, srv, http.MethodPut, region, "/"+bucket+"/obj",
			[]byte("x"), nil).Code)
	}
	objectExists := func(key string) func(*testing.T, *emulator.Server, string, string) bool {
		return func(t *testing.T, srv *emulator.Server, region, bucket string) bool {
			t.Helper()
			return s3RegionalCall(t, srv, http.MethodHead, region, "/"+bucket+"/"+key, nil, nil).Code == http.StatusOK
		}
	}
	return []s3ACLWrite{
		{
			name: "PutObject",
			send: func(t *testing.T, srv *emulator.Server, region, bucket string, _ []byte, h map[string]string) *httptest.ResponseRecorder {
				return s3RegionalCall(t, srv, http.MethodPut, region, "/"+bucket+"/new", []byte("x"), h)
			},
			left: objectExists("new"),
		},
		{
			name:  "CopyObject",
			setup: putObj,
			send: func(t *testing.T, srv *emulator.Server, region, bucket string, _ []byte, h map[string]string) *httptest.ResponseRecorder {
				headers := map[string]string{"x-amz-copy-source": "/" + bucket + "/obj"}
				for k, v := range h {
					headers[k] = v
				}
				return s3RegionalCall(t, srv, http.MethodPut, region, "/"+bucket+"/copy", nil, headers)
			},
			left: objectExists("copy"),
		},
		{
			name: "CreateMultipartUpload",
			send: func(t *testing.T, srv *emulator.Server, region, bucket string, _ []byte, h map[string]string) *httptest.ResponseRecorder {
				return s3RegionalCall(t, srv, http.MethodPost, region, "/"+bucket+"/mpu?uploads", nil, h)
			},
			left: func(t *testing.T, srv *emulator.Server, region, bucket string) bool {
				t.Helper()
				w := s3RegionalCall(t, srv, http.MethodGet, region, "/"+bucket+"?uploads", nil, nil)
				require.Equal(t, http.StatusOK, w.Code)
				return bytes.Contains(w.Body.Bytes(), []byte("<Upload>"))
			},
		},
		{
			name: "PutBucketAcl",
			xml:  true,
			send: func(t *testing.T, srv *emulator.Server, region, bucket string, body []byte, h map[string]string) *httptest.ResponseRecorder {
				return s3RegionalCall(t, srv, http.MethodPut, region, "/"+bucket+"?acl", body, h)
			},
			left: func(t *testing.T, srv *emulator.Server, region, bucket string) bool {
				t.Helper()
				return len(getBucketACLGrants(t, srv, bucket)) != 1
			},
		},
		{
			name:  "PutObjectAcl",
			xml:   true,
			setup: putObj,
			send: func(t *testing.T, srv *emulator.Server, region, bucket string, body []byte, h map[string]string) *httptest.ResponseRecorder {
				return s3RegionalCall(t, srv, http.MethodPut, region, "/"+bucket+"/obj?acl", body, h)
			},
			left: func(t *testing.T, srv *emulator.Server, region, bucket string) bool {
				t.Helper()
				return len(getObjectACLGrants(t, srv, bucket, "obj")) != 1
			},
		},
	}
}

// TestS3ObjectOwnership_EnforcedRefusesACLWrites is #1474: on a bucket created with the default
// BucketOwnerEnforced, every ACL write that is not bucket owner full control is refused with
// 400 AccessControlListNotSupported and leaves nothing behind.
func TestS3ObjectOwnership_EnforcedRefusesACLWrites(t *testing.T) {
	t.Parallel()
	refused := []struct {
		name    string
		headers map[string]string
		body    string
	}{
		{name: "canned public-read", headers: map[string]string{"x-amz-acl": "public-read"}},
		{name: "canned private", headers: map[string]string{"x-amz-acl": "private"}},
		{name: "a grant to another account", headers: map[string]string{"x-amz-grant-read": `id="someone-else"`}},
		{name: "a grant to a group", headers: map[string]string{"x-amz-grant-read": `uri="` + testGroupAllUsers + `"`}},
		{name: "an email grantee", headers: map[string]string{"x-amz-grant-read": `emailAddress="a@example.com"`}},
		{name: "XML granting a group", body: aclXML(testGroupAllUsers, "READ")},
	}
	for _, op := range s3ACLWrites() {
		for _, tc := range refused {
			if tc.body != "" && !op.xml {
				continue
			}
			t.Run(op.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				srv, _ := newS3TestServer(t)
				bucket := "enforced"
				require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)
				if op.setup != nil {
					op.setup(t, srv, "us-east-1", bucket)
				}
				w := op.send(t, srv, "us-east-1", bucket, nilIfEmptyBody(tc.body), tc.headers)
				require.Equal(t, http.StatusBadRequest, w.Code, "%s", w.Body.String())
				assert.Equal(t, "AccessControlListNotSupported", s3ErrorCode(t, w.Body.Bytes()))
				assert.Equal(t, "The bucket does not allow ACLs", s3ErrorMessage(t, w.Body.Bytes()))
				assert.False(t, op.left(t, srv, "us-east-1", bucket), "a refused write leaves nothing behind")
			})
		}
	}
}

// TestS3ObjectOwnership_EnforcedAcceptsOwnerFullControl covers the forms a bucket with ACLs disabled
// accepts: no ACL at all, the bucket-owner-full-control canned ACL, and FULL_CONTROL to the owner
// alone through a grant header or an XML body.
func TestS3ObjectOwnership_EnforcedAcceptsOwnerFullControl(t *testing.T) {
	t.Parallel()
	const bucket = "enforced-ok"
	accepted := []struct {
		name    string
		headers map[string]string
		body    string
	}{
		{name: "no ACL"},
		{name: "canned bucket-owner-full-control", headers: map[string]string{"x-amz-acl": "bucket-owner-full-control"}},
		{name: "full control to the owner", headers: map[string]string{"x-amz-grant-full-control": `id="` + bucket + `-owner"`}},
		{name: "XML full control to the owner", body: s3OwnerOnlyXML(bucket)},
	}
	for _, op := range s3ACLWrites() {
		for _, tc := range accepted {
			if tc.body != "" && !op.xml {
				continue
			}
			if tc.name == "no ACL" && op.xml {
				// PutBucketAcl and PutObjectAcl always set an ACL; with nothing named it resolves to
				// private, which is not bucket owner full control.
				continue
			}
			t.Run(op.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				srv, _ := newS3TestServer(t)
				require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)
				if op.setup != nil {
					op.setup(t, srv, "us-east-1", bucket)
				}
				w := op.send(t, srv, "us-east-1", bucket, nilIfEmptyBody(tc.body), tc.headers)
				assert.Equal(t, http.StatusOK, w.Code, "%s", w.Body.String())
			})
		}
	}
}

// TestS3ObjectOwnership_ReadsReportOwnerFullControl asserts GetBucketAcl and GetObjectAcl on a
// bucket with ACLs disabled report full control for the owner whatever is stored, and that the
// stored ACL is restored when ACLs are re-enabled.
func TestS3ObjectOwnership_ReadsReportOwnerFullControl(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	const bucket = "switched"
	createBucketWithACL(t, srv, bucket, nil)
	putObjectWithACL(t, srv, bucket, "obj", map[string]string{"x-amz-acl": "public-read"})
	public := []string{ownerGrant(bucket), testGroupAllUsers + "/READ"}
	require.Equal(t, public, getObjectACLGrants(t, srv, bucket, "obj"))

	setOwnership := func(v string) int {
		return s3Request(t, srv, http.MethodPut, "/"+bucket+"?ownershipControls", []byte(
			`<OwnershipControls><Rule><ObjectOwnership>`+v+`</ObjectOwnership></Rule></OwnershipControls>`), nil).Code
	}
	require.Equal(t, http.StatusOK, setOwnership("BucketOwnerEnforced"),
		"an object ACL does not block the setting; only the bucket's does")
	assert.Equal(t, []string{ownerGrant(bucket)}, getObjectACLGrants(t, srv, bucket, "obj"))
	assert.Equal(t, []string{ownerGrant(bucket)}, getBucketACLGrants(t, srv, bucket))

	require.Equal(t, http.StatusOK, setOwnership("ObjectWriter"))
	assert.Equal(t, public, getObjectACLGrants(t, srv, bucket, "obj"), "re-enabling ACLs restores the stored one")
}

// TestS3ObjectOwnership_InvalidBucketAclWithObjectOwnership covers the two ways a bucket would end
// up BucketOwnerEnforced with an ACL granting others: CreateBucket, and PutBucketOwnershipControls.
func TestS3ObjectOwnership_InvalidBucketAclWithObjectOwnership(t *testing.T) {
	t.Parallel()
	const code = "InvalidBucketAclWithObjectOwnership"
	const message = "Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting"

	t.Run("CreateBucket", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name    string
			headers map[string]string
			want    int
		}{
			{"default ownership, public-read", map[string]string{"x-amz-acl": "public-read"}, http.StatusBadRequest},
			{"explicit enforced, a grant to another account", map[string]string{
				"x-amz-object-ownership": "BucketOwnerEnforced", "x-amz-grant-read": `id="someone-else"`}, http.StatusBadRequest},
			{"default ownership, private", map[string]string{"x-amz-acl": "private"}, http.StatusOK},
			{"ObjectWriter, public-read", map[string]string{
				"x-amz-object-ownership": "ObjectWriter", "x-amz-acl": "public-read"}, http.StatusOK},
			{"BucketOwnerPreferred, public-read", map[string]string{
				"x-amz-object-ownership": "BucketOwnerPreferred", "x-amz-acl": "public-read"}, http.StatusOK},
		} {
			t.Run(tc.name, func(t *testing.T) {
				srv, _ := newS3TestServer(t)
				w := s3Request(t, srv, http.MethodPut, "/created", nil, tc.headers)
				require.Equal(t, tc.want, w.Code, "%s", w.Body.String())
				if tc.want == http.StatusOK {
					return
				}
				assert.Equal(t, code, s3ErrorCode(t, w.Body.Bytes()))
				assert.Equal(t, message, s3ErrorMessage(t, w.Body.Bytes()))
				assert.Equal(t, http.StatusNotFound, s3Request(t, srv, http.MethodHead, "/created", nil, nil).Code,
					"a refused create leaves no bucket")
			})
		}
	})

	t.Run("PutBucketOwnershipControls", func(t *testing.T) {
		t.Parallel()
		srv, _ := newS3TestServer(t)
		createBucketWithACL(t, srv, "granting", map[string]string{"x-amz-acl": "public-read"})
		w := s3Request(t, srv, http.MethodPut, "/granting?ownershipControls", []byte(
			`<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>`), nil)
		require.Equal(t, http.StatusBadRequest, w.Code, "%s", w.Body.String())
		assert.Equal(t, code, s3ErrorCode(t, w.Body.Bytes()))
		assert.Equal(t, message, s3ErrorMessage(t, w.Body.Bytes()))

		got := s3Request(t, srv, http.MethodGet, "/granting?ownershipControls", nil, nil)
		assert.Contains(t, got.Body.String(), "ObjectWriter", "the refused setting is not stored")
	})
}

// TestS3ObjectOwnership_ACLsEnabledSettingsKeepACLs asserts BucketOwnerPreferred, ObjectWriter and a
// bucket with no ownership controls all keep accepting ACLs.
func TestS3ObjectOwnership_ACLsEnabledSettingsKeepACLs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		create map[string]string
		delete bool
	}{
		{name: "BucketOwnerPreferred", create: map[string]string{"x-amz-object-ownership": "BucketOwnerPreferred"}},
		{name: "ObjectWriter", create: map[string]string{"x-amz-object-ownership": "ObjectWriter"}},
		{name: "no ownership controls", delete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/enabled", nil, tc.create).Code)
			if tc.delete {
				require.Equal(t, http.StatusNoContent,
					s3Request(t, srv, http.MethodDelete, "/enabled?ownershipControls", nil, nil).Code)
			}
			putObjectWithACL(t, srv, "enabled", "obj", map[string]string{"x-amz-acl": "public-read"})
			assert.Equal(t, []string{ownerGrant("enabled"), testGroupAllUsers + "/READ"},
				getObjectACLGrants(t, srv, "enabled", "obj"))
		})
	}
}

// TestS3ObjectOwnership_CheckedBeforeBlockPublicAccess asserts the order: a bucket that disables
// ACLs refuses a public ACL with its own 400, not Block Public Access's 403.
func TestS3ObjectOwnership_CheckedBeforeBlockPublicAccess(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/both", nil, nil).Code)
	require.Equal(t, http.StatusOK, putPAB(t, srv, "both", pabAllTrue).Code)
	w := s3Request(t, srv, http.MethodPut, "/both/k", []byte("x"), map[string]string{"x-amz-acl": "public-read"})
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, "AccessControlListNotSupported", s3ErrorCode(t, w.Body.Bytes()))
}

// TestS3_EmailGranteeIsRefused is #507: on a bucket that allows ACLs, an email-address grantee — in
// an x-amz-grant-* header or an AmazonCustomerByEmail XML grantee — is refused, with the
// end-of-support notice's 405 in the eight Regions it names and a 400 elsewhere, and the write
// leaves nothing behind.
func TestS3_EmailGranteeIsRefused(t *testing.T) {
	t.Parallel()
	forms := []struct {
		name    string
		headers map[string]string
		body    string
	}{
		{name: "header", headers: map[string]string{"x-amz-grant-read": `id="x", emailAddress="a@example.com"`}},
		{name: "XML", body: s3EmailGranteeXML},
	}
	regions := []struct {
		region string
		status int
		code   string
	}{
		{"us-east-1", http.StatusMethodNotAllowed, "MethodNotAllowed"},
		{"eu-west-1", http.StatusMethodNotAllowed, "MethodNotAllowed"},
		{"sa-east-1", http.StatusMethodNotAllowed, "MethodNotAllowed"},
		{"eu-central-1", http.StatusBadRequest, "InvalidArgument"},
	}
	for _, rg := range regions {
		for _, op := range s3ACLWrites() {
			for _, form := range forms {
				if form.body != "" && !op.xml {
					continue
				}
				t.Run(rg.region+"/"+op.name+"/"+form.name, func(t *testing.T) {
					t.Parallel()
					srv, _ := newS3TestServer(t)
					const bucket = "email"
					require.Equal(t, http.StatusOK, s3RegionalCall(t, srv, http.MethodPut, rg.region, "/"+bucket, nil,
						s3ACLsEnabled(nil)).Code)
					if op.setup != nil {
						op.setup(t, srv, rg.region, bucket)
					}
					w := op.send(t, srv, rg.region, bucket, nilIfEmptyBody(form.body), form.headers)
					require.Equal(t, rg.status, w.Code, "%s", w.Body.String())
					assert.Equal(t, rg.code, s3ErrorCode(t, w.Body.Bytes()))
					assert.False(t, op.left(t, srv, rg.region, bucket), "a refused write leaves nothing behind")
				})
			}
		}

		t.Run(rg.region+"/CreateBucket", func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			w := s3RegionalCall(t, srv, http.MethodPut, rg.region, "/email-create", nil,
				s3ACLsEnabled(map[string]string{"x-amz-grant-read": `emailAddress="a@example.com"`}))
			require.Equal(t, rg.status, w.Code, "%s", w.Body.String())
			assert.Equal(t, rg.code, s3ErrorCode(t, w.Body.Bytes()))
			assert.Equal(t, http.StatusNotFound,
				s3RegionalCall(t, srv, http.MethodHead, rg.region, "/email-create", nil, nil).Code)
		})
	}
}
