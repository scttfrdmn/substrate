package emulator_test

import (
	"context"
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The bucket configurations substrate records and answers back without acting on — website,
// logging, Transfer Acceleration, request payment and replication — and the Object Lock read that
// always answers "none" (#1613, #1658). Assertions are on the raw XML, so a member dropped or
// renamed on the way through is visible.

// s3CreateBucketT creates bucket on srv and fails the test if it is refused.
func s3CreateBucketT(t *testing.T, srv *emulator.Server, bucket string) {
	t.Helper()
	w := s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "CreateBucket %s: %s", bucket, w.Body.String())
}

// s3EnableVersioningT turns versioning on for bucket.
func s3EnableVersioningT(t *testing.T, srv *emulator.Server, bucket string) {
	t.Helper()
	w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)
	require.Equal(t, http.StatusOK, w.Code, "PutBucketVersioning %s: %s", bucket, w.Body.String())
}

// s3RequireError requires that w is the S3 error code at status.
func s3RequireError(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	require.Equal(t, status, w.Code, "%s", w.Body.String())
	require.Equal(t, code, parseS3Error(t, w.Body.Bytes()).Code, "%s", w.Body.String())
}

// TestS3BucketConfig_UnconfiguredAnswers pins what each read answers for a bucket that was never
// configured, which is what the AWS provider for Terraform reads on every aws_s3_bucket refresh.
func TestS3BucketConfig_UnconfiguredAnswers(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	const bucket = "never-configured"
	s3CreateBucketT(t, srv, bucket)

	for _, tc := range []struct {
		sub        string
		wantStatus int
		wantCode   string
		wantBody   string
	}{
		{sub: "website", wantStatus: http.StatusNotFound, wantCode: "NoSuchWebsiteConfiguration"},
		{sub: "replication", wantStatus: http.StatusNotFound, wantCode: "ReplicationConfigurationNotFoundError"},
		{sub: "object-lock", wantStatus: http.StatusNotFound, wantCode: "ObjectLockConfigurationNotFoundError"},
		{sub: "logging", wantStatus: http.StatusOK,
			wantBody: `<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></BucketLoggingStatus>`},
		{sub: "accelerate", wantStatus: http.StatusOK,
			wantBody: `<AccelerateConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></AccelerateConfiguration>`},
		{sub: "requestPayment", wantStatus: http.StatusOK,
			wantBody: `<RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Payer>BucketOwner</Payer></RequestPaymentConfiguration>`},
	} {
		t.Run(tc.sub, func(t *testing.T) {
			// "?sub=" is how an SDK spells the marker, "?sub" how the reference does.
			for _, marker := range []string{"?" + tc.sub, "?" + tc.sub + "="} {
				w := s3Request(t, srv, http.MethodGet, "/"+bucket+marker, nil, nil)
				require.Equal(t, tc.wantStatus, w.Code, "%s: %s", marker, w.Body.String())
				if tc.wantCode != "" {
					require.Equal(t, tc.wantCode, parseS3Error(t, w.Body.Bytes()).Code)
					continue
				}
				require.Contains(t, w.Body.String(), tc.wantBody)
			}
			w := s3Request(t, srv, http.MethodGet, "/no-such-bucket-here?"+tc.sub, nil, nil)
			s3RequireError(t, w, http.StatusNotFound, "NoSuchBucket")
		})
	}
}

// TestS3BucketConfig_UnconfiguredMessages pins the observed AWS texts of the three not-found codes.
func TestS3BucketConfig_UnconfiguredMessages(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	s3CreateBucketT(t, srv, "messages")
	for sub, want := range map[string]string{
		"website":     "The specified bucket does not have a website configuration",
		"replication": "The replication configuration was not found",
		"object-lock": "Object Lock configuration does not exist for this bucket",
	} {
		w := s3Request(t, srv, http.MethodGet, "/messages?"+sub, nil, nil)
		assert.Equal(t, want, parseS3Error(t, w.Body.Bytes()).Message, sub)
	}
}

func TestS3Website_PutGetDelete(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	const bucket = "website-roundtrip"
	s3CreateBucketT(t, srv, bucket)

	body := `<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IndexDocument><Suffix>index.html</Suffix></IndexDocument>
  <ErrorDocument><Key>error.html</Key></ErrorDocument>
  <RoutingRules>
    <RoutingRule>
      <Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition>
      <Redirect><ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith></Redirect>
    </RoutingRule>
    <RoutingRule>
      <Condition><HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals></Condition>
      <Redirect><HostName>example.com</HostName><HttpRedirectCode>301</HttpRedirectCode><Protocol>https</Protocol><ReplaceKeyWith>404.html</ReplaceKeyWith></Redirect>
    </RoutingRule>
  </RoutingRules>
</WebsiteConfiguration>`
	w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?website", []byte(body), nil)
	require.Equal(t, http.StatusOK, w.Code, "PutBucketWebsite: %s", w.Body.String())
	require.Empty(t, w.Body.String())

	w = s3Request(t, srv, http.MethodGet, "/"+bucket+"?website", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "GetBucketWebsite: %s", w.Body.String())
	require.Contains(t, w.Body.String(), `<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`+
		`<IndexDocument><Suffix>index.html</Suffix></IndexDocument>`+
		`<ErrorDocument><Key>error.html</Key></ErrorDocument>`+
		`<RoutingRules>`+
		`<RoutingRule><Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition><Redirect><ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith></Redirect></RoutingRule>`+
		`<RoutingRule><Condition><HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals></Condition>`+
		`<Redirect><HostName>example.com</HostName><HttpRedirectCode>301</HttpRedirectCode><Protocol>https</Protocol><ReplaceKeyWith>404.html</ReplaceKeyWith></Redirect></RoutingRule>`+
		`</RoutingRules></WebsiteConfiguration>`)

	// A put replaces: a redirect-all configuration leaves no index document behind.
	redirect := `<WebsiteConfiguration><RedirectAllRequestsTo><HostName>example.com</HostName><Protocol>https</Protocol></RedirectAllRequestsTo></WebsiteConfiguration>`
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket+"?website", []byte(redirect), nil).Code)
	w = s3Request(t, srv, http.MethodGet, "/"+bucket+"?website", nil, nil)
	require.Contains(t, w.Body.String(), `<RedirectAllRequestsTo><HostName>example.com</HostName><Protocol>https</Protocol></RedirectAllRequestsTo></WebsiteConfiguration>`)
	require.NotContains(t, w.Body.String(), "IndexDocument")

	for i := range 2 {
		w = s3Request(t, srv, http.MethodDelete, "/"+bucket+"?website", nil, nil)
		require.Equalf(t, http.StatusNoContent, w.Code, "DeleteBucketWebsite #%d: %s", i+1, w.Body.String())
	}
	w = s3Request(t, srv, http.MethodGet, "/"+bucket+"?website", nil, nil)
	s3RequireError(t, w, http.StatusNotFound, "NoSuchWebsiteConfiguration")
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodHead, "/"+bucket, nil, nil).Code,
		"DeleteBucketWebsite must not delete the bucket")

	w = s3Request(t, srv, http.MethodDelete, "/no-such-website-bucket?website", nil, nil)
	s3RequireError(t, w, http.StatusNotFound, "NoSuchBucket")
}

func TestS3Website_Refusals(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	const bucket = "website-refusals"
	s3CreateBucketT(t, srv, bucket)

	for _, tc := range []struct {
		name, body, code string
	}{
		{"not XML", `<WebsiteConfiguration>`, "MalformedXML"},
		{"empty body", ``, "MalformedXML"},
		{"redirect without host", `<WebsiteConfiguration><RedirectAllRequestsTo><Protocol>https</Protocol></RedirectAllRequestsTo></WebsiteConfiguration>`, "MalformedXML"},
		{"redirect beside an index", `<WebsiteConfiguration><RedirectAllRequestsTo><HostName>a.example</HostName></RedirectAllRequestsTo><IndexDocument><Suffix>i.html</Suffix></IndexDocument></WebsiteConfiguration>`, "MalformedXML"},
		{"redirect protocol", `<WebsiteConfiguration><RedirectAllRequestsTo><HostName>a.example</HostName><Protocol>ftp</Protocol></RedirectAllRequestsTo></WebsiteConfiguration>`, "MalformedXML"},
		{"error document without key", `<WebsiteConfiguration><IndexDocument><Suffix>i.html</Suffix></IndexDocument><ErrorDocument></ErrorDocument></WebsiteConfiguration>`, "MalformedXML"},
		{"routing rule without redirect", `<WebsiteConfiguration><IndexDocument><Suffix>i.html</Suffix></IndexDocument><RoutingRules><RoutingRule><Condition><KeyPrefixEquals>a/</KeyPrefixEquals></Condition></RoutingRule></RoutingRules></WebsiteConfiguration>`, "MalformedXML"},
		{"routing rule protocol", `<WebsiteConfiguration><IndexDocument><Suffix>i.html</Suffix></IndexDocument><RoutingRules><RoutingRule><Redirect><Protocol>gopher</Protocol></Redirect></RoutingRule></RoutingRules></WebsiteConfiguration>`, "MalformedXML"},
		{"empty configuration", `<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`, "InvalidArgument"},
		{"empty suffix", `<WebsiteConfiguration><IndexDocument><Suffix></Suffix></IndexDocument></WebsiteConfiguration>`, "InvalidArgument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?website", []byte(tc.body), nil)
			s3RequireError(t, w, http.StatusBadRequest, tc.code)
		})
	}

	// The InvalidArgument carries the argument it names, as S3's does.
	w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?website", []byte(`<WebsiteConfiguration/>`), nil)
	var detail struct {
		Message       string `xml:"Message"`
		ArgumentName  string `xml:"ArgumentName"`
		ArgumentValue string `xml:"ArgumentValue"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &detail))
	assert.Equal(t, "A value for IndexDocument Suffix must be provided if RedirectAllRequestsTo is empty", detail.Message)
	assert.Equal(t, "IndexDocument", detail.ArgumentName)
	assert.Equal(t, "null", detail.ArgumentValue)

	w = s3Request(t, srv, http.MethodGet, "/"+bucket+"?website", nil, nil)
	s3RequireError(t, w, http.StatusNotFound, "NoSuchWebsiteConfiguration")

	w = s3Request(t, srv, http.MethodPut, "/no-such-website-bucket?website",
		[]byte(`<WebsiteConfiguration><IndexDocument><Suffix>i.html</Suffix></IndexDocument></WebsiteConfiguration>`), nil)
	s3RequireError(t, w, http.StatusNotFound, "NoSuchBucket")
}

// TestS3ObjectLock_CreateBucketEnablesIt pins the Object Lock state a bucket's own requests
// observe: CreateBucket's header enables it and versioning, GetObjectLockConfiguration reports it
// without a Rule, versioning cannot then be suspended, and DeleteBucket clears it.
func TestS3ObjectLock_CreateBucketEnablesIt(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	w := s3Request(t, srv, http.MethodPut, "/locked", nil, map[string]string{"x-amz-bucket-object-lock-enabled": "True"})
	require.Equal(t, http.StatusOK, w.Code, "%s", w.Body.String())
	w = s3Request(t, srv, http.MethodPut, "/unlocked", nil, map[string]string{"x-amz-bucket-object-lock-enabled": "false"})
	require.Equal(t, http.StatusOK, w.Code, "%s", w.Body.String())

	w = s3Request(t, srv, http.MethodGet, "/locked?object-lock", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "%s", w.Body.String())
	assert.Contains(t, w.Body.String(), `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`+
		`<ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`)
	assert.NotContains(t, w.Body.String(), "Rule")
	assert.Contains(t, s3Request(t, srv, http.MethodGet, "/locked?versioning", nil, nil).Body.String(),
		"<Status>Enabled</Status>", "enabling Object Lock enables versioning")

	w = s3Request(t, srv, http.MethodGet, "/unlocked?object-lock", nil, nil)
	s3RequireError(t, w, http.StatusNotFound, "ObjectLockConfigurationNotFoundError")

	suspend := []byte(`<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`)
	w = s3Request(t, srv, http.MethodPut, "/locked?versioning", suspend, nil)
	s3RequireError(t, w, http.StatusConflict, "InvalidBucketState")
	assert.Equal(t, "An Object Lock configuration is present on this bucket, so the versioning state cannot be changed.",
		parseS3Error(t, w.Body.Bytes()).Message)
	assert.Contains(t, s3Request(t, srv, http.MethodGet, "/locked?versioning", nil, nil).Body.String(), "<Status>Enabled</Status>")
	s3EnableVersioningT(t, srv, "locked")
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/unlocked?versioning", suspend, nil).Code)

	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, "/locked", nil, nil).Code)
	s3CreateBucketT(t, srv, "locked")
	w = s3Request(t, srv, http.MethodGet, "/locked?object-lock", nil, nil)
	s3RequireError(t, w, http.StatusNotFound, "ObjectLockConfigurationNotFoundError")
}

// TestS3ObjectLock_PutEnablesItOnAVersionedBucket covers PutObjectLockConfiguration's one modeled
// form, ObjectLockEnabled without a Rule, and its refusals.
func TestS3ObjectLock_PutEnablesItOnAVersionedBucket(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	const enable = `<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`
	s3CreateBucketT(t, srv, "lock-later")

	w := s3Request(t, srv, http.MethodPut, "/lock-later?object-lock", []byte(enable), nil)
	s3RequireError(t, w, http.StatusConflict, "InvalidBucketState")
	assert.Equal(t, "Versioning must be 'Enabled' on the bucket to apply a Object Lock configuration",
		parseS3Error(t, w.Body.Bytes()).Message)
	w = s3Request(t, srv, http.MethodGet, "/lock-later?object-lock", nil, nil)
	s3RequireError(t, w, http.StatusNotFound, "ObjectLockConfigurationNotFoundError")

	s3EnableVersioningT(t, srv, "lock-later")
	for i := range 2 {
		w = s3Request(t, srv, http.MethodPut, "/lock-later?object-lock", []byte(enable), nil)
		require.Equalf(t, http.StatusOK, w.Code, "PutObjectLockConfiguration #%d: %s", i+1, w.Body.String())
	}
	assert.Contains(t, s3Request(t, srv, http.MethodGet, "/lock-later?object-lock", nil, nil).Body.String(),
		"<ObjectLockEnabled>Enabled</ObjectLockEnabled>")

	for _, tc := range []struct {
		name, body, code string
		status           int
	}{
		{"not XML", `<ObjectLockConfiguration>`, "MalformedXML", http.StatusBadRequest},
		{"no ObjectLockEnabled", `<ObjectLockConfiguration/>`, "MalformedXML", http.StatusBadRequest},
		{"ObjectLockEnabled outside its enum", `<ObjectLockConfiguration><ObjectLockEnabled>Disabled</ObjectLockEnabled></ObjectLockConfiguration>`, "MalformedXML", http.StatusBadRequest},
		{"a default retention rule", `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>` +
			`<Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>`,
			"NotImplemented", http.StatusNotImplemented},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodPut, "/lock-later?object-lock", []byte(tc.body), nil)
			s3RequireError(t, w, tc.status, tc.code)
		})
	}
	w = s3Request(t, srv, http.MethodPut, "/no-such-lock-bucket?object-lock", []byte(enable), nil)
	s3RequireError(t, w, http.StatusNotFound, "NoSuchBucket")
}

// TestS3Versioning_SuspendRefusedWhileReplicated pins "If you attempt to disable versioning on the
// source bucket, Amazon S3 returns an error. You must remove the replication configuration before
// you can disable versioning".
func TestS3Versioning_SuspendRefusedWhileReplicated(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	s3CreateBucketT(t, srv, "repl-src")
	s3CreateBucketT(t, srv, "repl-dst")
	s3EnableVersioningT(t, srv, "repl-src")
	s3EnableVersioningT(t, srv, "repl-dst")
	body := `<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/r</Role><Rule><Prefix></Prefix><Status>Enabled</Status>` +
		`<Destination><Bucket>arn:aws:s3:::repl-dst</Bucket></Destination></Rule></ReplicationConfiguration>`
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/repl-src?replication", []byte(body), nil).Code)

	suspend := []byte(`<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`)
	w := s3Request(t, srv, http.MethodPut, "/repl-src?versioning", suspend, nil)
	s3RequireError(t, w, http.StatusConflict, "InvalidBucketState")
	assert.Equal(t, "The request is not valid with the current state of the bucket.", parseS3Error(t, w.Body.Bytes()).Message)
	assert.Contains(t, s3Request(t, srv, http.MethodGet, "/repl-src?versioning", nil, nil).Body.String(), "<Status>Enabled</Status>")

	// The destination is not protected; nor is the source once its configuration is removed.
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/repl-dst?versioning", suspend, nil).Code)
	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, "/repl-src?replication", nil, nil).Code)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/repl-src?versioning", suspend, nil).Code)
}

// TestS3Grantee_WritesXSIType is the raw-bytes check of a Grantee on the wire: the xsi namespace
// is declared and the type is xsi:type, in both GetBucketAcl and GetBucketLogging, so a client that
// reads the qualified attribute sees the grantee's type.
func TestS3Grantee_WritesXSIType(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	s3CreateBucketT(t, srv, "grantee-acl")
	w := s3Request(t, srv, http.MethodGet, "/grantee-acl?acl", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "%s", w.Body.String())
	assert.Contains(t, w.Body.String(), `<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>`)
	assert.NotContains(t, w.Body.String(), ` type="`)

	s3CreateBucketT(t, srv, "grantee-logs")
	body := `<BucketLoggingStatus><LoggingEnabled><TargetBucket>grantee-logs</TargetBucket><TargetPrefix>l/</TargetPrefix>` +
		`<TargetGrants><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">` +
		`<URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI></Grantee><Permission>READ</Permission></Grant>` +
		`</TargetGrants></LoggingEnabled></BucketLoggingStatus>`
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/grantee-logs?logging", []byte(body), nil).Code)
	w = s3Request(t, srv, http.MethodGet, "/grantee-logs?logging", nil, nil)
	assert.Contains(t, w.Body.String(), `<TargetGrants><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">`+
		`<URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI></Grantee><Permission>READ</Permission></Grant></TargetGrants>`)
}

func TestS3Logging_PutGetDisable(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	s3CreateBucketT(t, srv, "logged")
	s3CreateBucketT(t, srv, "log-target")

	body := `<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <LoggingEnabled>
    <TargetBucket>log-target</TargetBucket>
    <TargetPrefix>logged/</TargetPrefix>
    <TargetObjectKeyFormat><PartitionedPrefix><PartitionDateSource>EventTime</PartitionDateSource></PartitionedPrefix></TargetObjectKeyFormat>
  </LoggingEnabled>
</BucketLoggingStatus>`
	w := s3Request(t, srv, http.MethodPut, "/logged?logging", []byte(body), nil)
	require.Equal(t, http.StatusOK, w.Code, "PutBucketLogging: %s", w.Body.String())
	w = s3Request(t, srv, http.MethodGet, "/logged?logging", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `<LoggingEnabled><TargetBucket>log-target</TargetBucket>`+
		`<TargetObjectKeyFormat><PartitionedPrefix><PartitionDateSource>EventTime</PartitionDateSource></PartitionedPrefix></TargetObjectKeyFormat>`+
		`<TargetPrefix>logged/</TargetPrefix></LoggingEnabled>`)

	// A bucket may log to itself, and an empty TargetPrefix is a value, not an absence.
	self := `<BucketLoggingStatus><LoggingEnabled><TargetBucket>logged</TargetBucket><TargetPrefix></TargetPrefix>` +
		`<TargetObjectKeyFormat><SimplePrefix/></TargetObjectKeyFormat></LoggingEnabled></BucketLoggingStatus>`
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/logged?logging", []byte(self), nil).Code)
	w = s3Request(t, srv, http.MethodGet, "/logged?logging", nil, nil)
	require.Contains(t, w.Body.String(), `<TargetBucket>logged</TargetBucket><TargetObjectKeyFormat><SimplePrefix></SimplePrefix></TargetObjectKeyFormat><TargetPrefix></TargetPrefix>`)

	// An empty BucketLoggingStatus disables logging.
	w = s3Request(t, srv, http.MethodPut, "/logged?logging", []byte(`<BucketLoggingStatus xmlns="http://doc.s3.amazonaws.com/2006-03-01" />`), nil)
	require.Equal(t, http.StatusOK, w.Code, "%s", w.Body.String())
	w = s3Request(t, srv, http.MethodGet, "/logged?logging", nil, nil)
	require.NotContains(t, w.Body.String(), "LoggingEnabled")
}

func TestS3Logging_Refusals(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	s3CreateBucketT(t, srv, "logged-refusals")

	for _, tc := range []struct {
		name, body, code string
		status           int
	}{
		{"not XML", `<BucketLoggingStatus><LoggingEnabled>`, "MalformedXML", http.StatusBadRequest},
		{"no target bucket", `<BucketLoggingStatus><LoggingEnabled><TargetPrefix>p/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>`, "MalformedXML", http.StatusBadRequest},
		{"no target prefix", `<BucketLoggingStatus><LoggingEnabled><TargetBucket>logged-refusals</TargetBucket></LoggingEnabled></BucketLoggingStatus>`, "MalformedXML", http.StatusBadRequest},
		{"missing target", `<BucketLoggingStatus><LoggingEnabled><TargetBucket>no-such-log-target</TargetBucket><TargetPrefix>p/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>`, "InvalidTargetBucketForLogging", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodPut, "/logged-refusals?logging", []byte(tc.body), nil)
			s3RequireError(t, w, tc.status, tc.code)
		})
	}
	w := s3Request(t, srv, http.MethodPut, "/logged-refusals?logging",
		[]byte(`<BucketLoggingStatus><LoggingEnabled><TargetBucket>no-such-log-target</TargetBucket><TargetPrefix>p/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>`), nil)
	assert.Equal(t, "The target bucket for logging does not exist", parseS3Error(t, w.Body.Bytes()).Message)

	w = s3Request(t, srv, http.MethodPut, "/no-such-logged?logging", []byte(`<BucketLoggingStatus/>`), nil)
	s3RequireError(t, w, http.StatusNotFound, "NoSuchBucket")
}

// s3DirectPlugin is an S3 plugin driven in process, so a test can create buckets as another
// account or in another Region, which the shared HTTP test server's unsigned requests cannot.
type s3DirectPlugin struct {
	p *emulator.S3Plugin
}

// newS3DirectPlugin returns an initialized S3 plugin over fresh state.
func newS3DirectPlugin(t *testing.T) s3DirectPlugin {
	t.Helper()
	p := &emulator.S3Plugin{}
	require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{
		State:  emulator.NewMemoryStateManager(),
		Logger: emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{
			"time_controller": emulator.NewTimeController(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
			"filesystem":      afero.NewMemMapFs(),
		},
	}))
	return s3DirectPlugin{p: p}
}

// do sends method path?sub as account in region and returns the response.
func (d s3DirectPlugin) do(t *testing.T, account, region, method, path, sub, body string) *emulator.AWSResponse {
	t.Helper()
	params := map[string]string{}
	if sub != "" {
		params[sub] = ""
	}
	req := &emulator.AWSRequest{Service: "s3", Operation: method, Path: path, Params: params,
		Headers: map[string]string{}, Body: []byte(body)}
	resp, err := d.p.HandleRequest(&emulator.RequestContext{AccountID: account, Region: region, RequestID: "r"}, req)
	require.NoError(t, err)
	return resp
}

func TestS3Logging_TargetOwnershipAndRegion(t *testing.T) {
	t.Parallel()
	d := newS3DirectPlugin(t)
	const owner, other = "111111111111", "222222222222"
	require.Equal(t, http.StatusOK, d.do(t, owner, "us-west-2", "PUT", "/source", "", "").StatusCode)
	require.Equal(t, http.StatusOK, d.do(t, other, "us-west-2", "PUT", "/others-target", "", "").StatusCode)
	require.Equal(t, http.StatusOK, d.do(t, owner, "eu-west-1", "PUT", "/far-target", "", "").StatusCode)
	require.Equal(t, http.StatusOK, d.do(t, owner, "us-west-2", "PUT", "/near-target", "", "").StatusCode)

	put := func(target string) *emulator.AWSResponse {
		return d.do(t, owner, "us-west-2", "PUT", "/source", "logging",
			`<BucketLoggingStatus><LoggingEnabled><TargetBucket>`+target+`</TargetBucket><TargetPrefix>p/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>`)
	}
	resp := put("others-target")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s", resp.Body)
	assert.Equal(t, "InvalidTargetBucketForLogging", parseS3Error(t, resp.Body).Code)

	resp = put("far-target")
	require.Equal(t, http.StatusForbidden, resp.StatusCode, "%s", resp.Body)
	assert.Equal(t, "CrossLocationLoggingProhibited", parseS3Error(t, resp.Body).Code)

	resp = put("near-target")
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", resp.Body)
}

func TestS3Accelerate_PutGet(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	const bucket = "accelerated"
	s3CreateBucketT(t, srv, bucket)

	for _, status := range []string{"Enabled", "Suspended"} {
		w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?accelerate",
			[]byte(`<AccelerateConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>`+status+`</Status></AccelerateConfiguration>`), nil)
		require.Equal(t, http.StatusOK, w.Code, "%s: %s", status, w.Body.String())
		w = s3Request(t, srv, http.MethodGet, "/"+bucket+"?accelerate", nil, nil)
		require.Contains(t, w.Body.String(), `<AccelerateConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>`+status+`</Status></AccelerateConfiguration>`)
	}

	// No Status leaves the state as it was: Status is Required: No.
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket+"?accelerate", []byte(`<AccelerateConfiguration/>`), nil).Code)
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?accelerate", nil, nil)
	require.Contains(t, w.Body.String(), `<Status>Suspended</Status>`)

	for _, body := range []string{
		`<AccelerateConfiguration><Status>On</Status></AccelerateConfiguration>`,
		`<AccelerateConfiguration>`,
	} {
		w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?accelerate", []byte(body), nil)
		s3RequireError(t, w, http.StatusBadRequest, "MalformedXML")
	}

	s3CreateBucketT(t, srv, "dotted.bucket")
	w = s3Request(t, srv, http.MethodPut, "/dotted.bucket?accelerate",
		[]byte(`<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>`), nil)
	s3RequireError(t, w, http.StatusBadRequest, "InvalidRequest")
	assert.Equal(t, "Amazon S3 Transfer Acceleration is not supported for buckets with periods (.) in their names.",
		parseS3Error(t, w.Body.Bytes()).Message)

	w = s3Request(t, srv, http.MethodPut, "/no-such-accelerated?accelerate",
		[]byte(`<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>`), nil)
	s3RequireError(t, w, http.StatusNotFound, "NoSuchBucket")
}

func TestS3RequestPayment_PutGet(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	const bucket = "requester-pays"
	s3CreateBucketT(t, srv, bucket)

	for _, payer := range []string{"Requester", "BucketOwner"} {
		w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?requestPayment",
			[]byte(`<RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Payer>`+payer+`</Payer></RequestPaymentConfiguration>`), nil)
		require.Equal(t, http.StatusOK, w.Code, "%s: %s", payer, w.Body.String())
		w = s3Request(t, srv, http.MethodGet, "/"+bucket+"?requestPayment", nil, nil)
		require.Contains(t, w.Body.String(), `<Payer>`+payer+`</Payer>`)
	}
	for _, body := range []string{
		`<RequestPaymentConfiguration/>`,
		`<RequestPaymentConfiguration><Payer>Everyone</Payer></RequestPaymentConfiguration>`,
		`not xml`,
	} {
		w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?requestPayment", []byte(body), nil)
		s3RequireError(t, w, http.StatusBadRequest, "MalformedXML")
	}
	w := s3Request(t, srv, http.MethodPut, "/no-such-payer?requestPayment",
		[]byte(`<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>`), nil)
	s3RequireError(t, w, http.StatusNotFound, "NoSuchBucket")
}

// s3ReplicationRuleV2 is a rule in the current schema: Filter, Priority and DeleteMarkerReplication.
const s3ReplicationRuleV2 = `<Rule><ID>rule1</ID><Priority>1</Priority><Filter><And><Prefix>TaxDocs</Prefix>` +
	`<Tag><Key>k1</Key><Value>v1</Value></Tag><Tag><Key>k2</Key><Value>v2</Value></Tag></And></Filter>` +
	`<Status>Enabled</Status>` +
	`<SourceSelectionCriteria><SseKmsEncryptedObjects><Status>Enabled</Status></SseKmsEncryptedObjects></SourceSelectionCriteria>` +
	`<Destination><Bucket>arn:aws:s3:::replica</Bucket><Account>123456789012</Account><StorageClass>STANDARD_IA</StorageClass>` +
	`<AccessControlTranslation><Owner>Destination</Owner></AccessControlTranslation>` +
	`<EncryptionConfiguration><ReplicaKmsKeyID>arn:aws:kms:us-east-1:123456789012:key/k</ReplicaKmsKeyID></EncryptionConfiguration>` +
	`<ReplicationTime><Status>Enabled</Status><Time><Minutes>15</Minutes></Time></ReplicationTime>` +
	`<Metrics><Status>Enabled</Status><EventThreshold><Minutes>15</Minutes></EventThreshold></Metrics></Destination>` +
	`<DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication></Rule>`

func TestS3Replication_PutGetDelete(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	s3CreateBucketT(t, srv, "source")
	s3CreateBucketT(t, srv, "replica")
	s3EnableVersioningT(t, srv, "source")
	s3EnableVersioningT(t, srv, "replica")

	body := `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::123456789012:role/r</Role>` +
		s3ReplicationRuleV2 + `</ReplicationConfiguration>`
	w := s3Request(t, srv, http.MethodPut, "/source?replication", []byte(body), nil)
	require.Equal(t, http.StatusOK, w.Code, "PutBucketReplication: %s", w.Body.String())

	w = s3Request(t, srv, http.MethodGet, "/source?replication", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "GetBucketReplication: %s", w.Body.String())
	require.Contains(t, w.Body.String(), `<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Role>arn:aws:iam::123456789012:role/r</Role>`+
		s3ReplicationRuleV2+`</ReplicationConfiguration>`, "every member must round-trip")

	// The deprecated rule-level Prefix schema, without Priority or DeleteMarkerReplication.
	v1 := `<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/r</Role><Rule><Prefix></Prefix><Status>Disabled</Status>` +
		`<Destination><Bucket>arn:aws:s3:::replica</Bucket></Destination></Rule></ReplicationConfiguration>`
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/source?replication", []byte(v1), nil).Code)
	w = s3Request(t, srv, http.MethodGet, "/source?replication", nil, nil)
	require.Contains(t, w.Body.String(), `<Rule><Prefix></Prefix><Status>Disabled</Status><Destination><Bucket>arn:aws:s3:::replica</Bucket></Destination></Rule>`)

	for i := range 2 {
		w = s3Request(t, srv, http.MethodDelete, "/source?replication", nil, nil)
		require.Equalf(t, http.StatusNoContent, w.Code, "DeleteBucketReplication #%d: %s", i+1, w.Body.String())
	}
	w = s3Request(t, srv, http.MethodGet, "/source?replication", nil, nil)
	s3RequireError(t, w, http.StatusNotFound, "ReplicationConfigurationNotFoundError")
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodHead, "/source", nil, nil).Code,
		"DeleteBucketReplication must not delete the bucket")
}

func TestS3Replication_Refusals(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	s3CreateBucketT(t, srv, "unversioned")
	s3CreateBucketT(t, srv, "versioned")
	s3CreateBucketT(t, srv, "plain-replica")
	s3CreateBucketT(t, srv, "good-replica")
	s3EnableVersioningT(t, srv, "versioned")
	s3EnableVersioningT(t, srv, "good-replica")

	const role = `<Role>arn:aws:iam::123456789012:role/r</Role>`
	rule := func(inner string) string {
		return `<ReplicationConfiguration>` + role + `<Rule>` + inner + `</Rule></ReplicationConfiguration>`
	}
	const dest = `<Destination><Bucket>arn:aws:s3:::good-replica</Bucket></Destination>`
	const filter = `<Filter><Prefix>a/</Prefix></Filter>`
	const dmr = `<DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication>`

	for _, tc := range []struct {
		name, bucket, body, code, message string
	}{
		{"not XML", "versioned", `<ReplicationConfiguration>`, "MalformedXML", ""},
		{"no role", "versioned", `<ReplicationConfiguration><Rule><Priority>1</Priority>` + filter + `<Status>Enabled</Status>` + dest + dmr + `</Rule></ReplicationConfiguration>`, "MalformedXML", ""},
		{"no rule", "versioned", `<ReplicationConfiguration>` + role + `</ReplicationConfiguration>`, "MalformedXML", ""},
		{"no status", "versioned", rule(`<Priority>1</Priority>` + filter + dest + dmr), "MalformedXML", ""},
		{"bad status", "versioned", rule(`<Priority>1</Priority>` + filter + `<Status>On</Status>` + dest + dmr), "MalformedXML", ""},
		{"no destination", "versioned", rule(`<Priority>1</Priority>` + filter + `<Status>Enabled</Status>` + dmr), "MalformedXML", ""},
		{"neither prefix nor filter", "versioned", rule(`<Status>Enabled</Status>` + dest), "MalformedXML", ""},
		{"both prefix and filter", "versioned", rule(`<Prefix>a/</Prefix>` + filter + `<Status>Enabled</Status>` + dest), "MalformedXML", ""},
		{"filter with two criteria", "versioned", rule(`<Priority>1</Priority><Filter><Prefix>a/</Prefix><Tag><Key>k</Key><Value>v</Value></Tag></Filter><Status>Enabled</Status>` + dest + dmr), "MalformedXML", ""},
		{"bad delete-marker status", "versioned", rule(`<Priority>1</Priority>` + filter + `<Status>Enabled</Status>` + dest + `<DeleteMarkerReplication><Status>On</Status></DeleteMarkerReplication>`), "MalformedXML", ""},
		{"source not versioned", "unversioned", rule(`<Priority>1</Priority>` + filter + `<Status>Enabled</Status>` + dest + dmr), "InvalidRequest",
			"Versioning must be 'Enabled' on the bucket to apply a replication configuration"},
		{"destination missing", "versioned", rule(`<Priority>1</Priority>` + filter + `<Status>Enabled</Status><Destination><Bucket>arn:aws:s3:::no-such-replica</Bucket></Destination>` + dmr), "InvalidRequest",
			"Destination bucket must exist."},
		{"destination not versioned", "versioned", rule(`<Priority>1</Priority>` + filter + `<Status>Enabled</Status><Destination><Bucket>arn:aws:s3:::plain-replica</Bucket></Destination>` + dmr), "InvalidRequest",
			"Destination bucket must have versioning enabled."},
		{"filter without priority", "versioned", rule(filter + `<Status>Enabled</Status>` + dest + dmr), "InvalidRequest",
			"Priority must be specified for this version of Cross Region Replication configuration schema. Please refer to S3 Developer Guide for more information."},
		{"filter without delete-marker replication", "versioned", rule(`<Priority>1</Priority>` + filter + `<Status>Enabled</Status>` + dest), "InvalidRequest",
			"DeleteMarkerReplication must be specified for this version of Cross Region Replication configuration schema. Please refer to S3 Developer Guide for more information."},
		{"prefix with priority", "versioned", rule(`<Priority>1</Priority><Prefix>a/</Prefix><Status>Enabled</Status>` + dest), "InvalidRequest",
			"Priority cannot be used for this version of Cross Region Replication configuration schema. Please refer to S3 Developer Guide for more information."},
		{"prefix with delete-marker replication", "versioned", rule(`<Prefix>a/</Prefix><Status>Enabled</Status>` + dest + dmr), "InvalidRequest",
			"DeleteMarkerReplication cannot be used for this version of Cross Region Replication configuration schema. Please refer to S3 Developer Guide for more information."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := s3Request(t, srv, http.MethodPut, "/"+tc.bucket+"?replication", []byte(tc.body), nil)
			s3RequireError(t, w, http.StatusBadRequest, tc.code)
			if tc.message != "" {
				assert.Equal(t, tc.message, parseS3Error(t, w.Body.Bytes()).Message)
			}
		})
	}
	w := s3Request(t, srv, http.MethodGet, "/versioned?replication", nil, nil)
	s3RequireError(t, w, http.StatusNotFound, "ReplicationConfigurationNotFoundError")
}

// TestS3BucketConfig_DeleteBucketClearsEveryConfiguration is the #508 rule for the new prefixes: a
// bucket re-created under a deleted bucket's name inherits none of its configuration.
func TestS3BucketConfig_DeleteBucketClearsEveryConfiguration(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	const bucket = "reborn"
	s3CreateBucketT(t, srv, bucket)
	s3CreateBucketT(t, srv, "reborn-replica")
	s3EnableVersioningT(t, srv, bucket)
	s3EnableVersioningT(t, srv, "reborn-replica")

	for sub, body := range map[string]string{
		"website":        `<WebsiteConfiguration><IndexDocument><Suffix>i.html</Suffix></IndexDocument></WebsiteConfiguration>`,
		"logging":        `<BucketLoggingStatus><LoggingEnabled><TargetBucket>reborn</TargetBucket><TargetPrefix>l/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>`,
		"accelerate":     `<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>`,
		"requestPayment": `<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>`,
		"replication": `<ReplicationConfiguration><Role>arn:aws:iam::123456789012:role/r</Role><Rule><Prefix></Prefix><Status>Enabled</Status>` +
			`<Destination><Bucket>arn:aws:s3:::reborn-replica</Bucket></Destination></Rule></ReplicationConfiguration>`,
	} {
		w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?"+sub, []byte(body), nil)
		require.Equal(t, http.StatusOK, w.Code, "%s: %s", sub, w.Body.String())
	}
	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, "/"+bucket, nil, nil).Code)
	s3CreateBucketT(t, srv, bucket)

	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?website", nil, nil)
	s3RequireError(t, w, http.StatusNotFound, "NoSuchWebsiteConfiguration")
	w = s3Request(t, srv, http.MethodGet, "/"+bucket+"?replication", nil, nil)
	s3RequireError(t, w, http.StatusNotFound, "ReplicationConfigurationNotFoundError")
	assert.NotContains(t, s3Request(t, srv, http.MethodGet, "/"+bucket+"?logging", nil, nil).Body.String(), "LoggingEnabled")
	assert.NotContains(t, s3Request(t, srv, http.MethodGet, "/"+bucket+"?accelerate", nil, nil).Body.String(), "Status")
	assert.Contains(t, s3Request(t, srv, http.MethodGet, "/"+bucket+"?requestPayment", nil, nil).Body.String(), "<Payer>BucketOwner</Payer>")
}

// TestS3BucketConfig_AStoreFaultIsAnError pins that a store fault at a read or write these handlers
// make is an error, never answered as a configuration, an absence or a success.
func TestS3BucketConfig_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	const website = `<WebsiteConfiguration><IndexDocument><Suffix>i.html</Suffix></IndexDocument></WebsiteConfiguration>`
	const logging = `<BucketLoggingStatus><LoggingEnabled><TargetBucket>fault-target</TargetBucket><TargetPrefix>p/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>`
	for _, tc := range []struct {
		name, method, sub, body string
		arm                     func(*cfFaultStateManager)
	}{
		{"PutBucketWebsite, bucket read", http.MethodPut, "website", website, func(m *cfFaultStateManager) { m.failGet = "bucket:fault-cfg" }},
		{"PutBucketWebsite, write", http.MethodPut, "website", website, func(m *cfFaultStateManager) { m.failPut = "bucket_website:" }},
		{"GetBucketWebsite, read", http.MethodGet, "website", "", func(m *cfFaultStateManager) { m.failGet = "bucket_website:" }},
		{"GetBucketWebsite, corrupt record", http.MethodGet, "website", "", func(m *cfFaultStateManager) { m.corruptGet = "bucket_website:" }},
		{"DeleteBucketWebsite, delete", http.MethodDelete, "website", "", func(m *cfFaultStateManager) { m.failDelete = "bucket_website:" }},
		{"PutBucketLogging, source read", http.MethodPut, "logging", logging, func(m *cfFaultStateManager) { m.failGet = "bucket:fault-cfg" }},
		{"PutBucketLogging, corrupt source", http.MethodPut, "logging", logging, func(m *cfFaultStateManager) { m.corruptGet = "bucket:fault-cfg" }},
		{"PutBucketLogging, target read", http.MethodPut, "logging", logging, func(m *cfFaultStateManager) { m.failGet = "bucket:fault-target" }},
		{"PutBucketLogging, disable", http.MethodPut, "logging", `<BucketLoggingStatus/>`, func(m *cfFaultStateManager) { m.failDelete = "bucket_logging:" }},
		{"GetBucketLogging, read", http.MethodGet, "logging", "", func(m *cfFaultStateManager) { m.failGet = "bucket_logging:" }},
		{"PutBucketAccelerateConfiguration, write", http.MethodPut, "accelerate", `<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>`,
			func(m *cfFaultStateManager) { m.failPut = "bucket_accelerate:" }},
		{"GetBucketRequestPayment, corrupt record", http.MethodGet, "requestPayment", "", func(m *cfFaultStateManager) { m.corruptGet = "bucket_request_payment:" }},
		{"GetBucketReplication, read", http.MethodGet, "replication", "", func(m *cfFaultStateManager) { m.failGet = "bucket_replication:" }},
		{"GetObjectLockConfiguration, bucket read", http.MethodGet, "object-lock", "", func(m *cfFaultStateManager) { m.failGet = "bucket:fault-cfg" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			srv := newS3TestServerWithState(t, fault)
			for _, b := range []string{"fault-cfg", "fault-target"} {
				require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+b, nil, nil).Code)
			}
			for sub, body := range map[string]string{
				"website":        website,
				"logging":        logging,
				"requestPayment": `<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>`,
			} {
				require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/fault-cfg?"+sub, []byte(body), nil).Code, sub)
			}
			tc.arm(fault)
			var body []byte
			if tc.body != "" {
				body = []byte(tc.body)
			}
			w := s3Request(t, srv, tc.method, "/fault-cfg?"+tc.sub, body, nil)
			require.GreaterOrEqualf(t, w.Code, http.StatusInternalServerError, "%s must fail on a store fault: %s", tc.name, w.Body.String())
		})
	}
}
