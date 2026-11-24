package emulator_test

// The request-validation interpreter's protocol adapters (#1612), exercised on operations the
// generated tables hold whether or not their service has validation turned on, through
// WireFailurePathsForTest, which walks with enum and pattern checks on.

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/scttfrdmn/substrate/emulator"
)

func TestWireValidation_EachProtocolAdapterReadsItsMembers(t *testing.T) {
	jsonHeaders := func(target string) map[string]string {
		return map[string]string{"Content-Type": "application/x-amz-json-1.1", "X-Amz-Target": target}
	}
	for _, tc := range []struct {
		name string
		req  *emulator.AWSRequest
		want []string
	}{
		// JSON.
		{"json: every member present", &emulator.AWSRequest{Service: "acm", Operation: "RequestCertificate",
			Headers: jsonHeaders("CertificateManager.RequestCertificate"), Body: []byte(`{"DomainName":"a.example"}`)},
			[]string{}},
		{"json: an empty body is an empty structure", &emulator.AWSRequest{Service: "acm", Operation: "RequestCertificate",
			Headers: jsonHeaders("CertificateManager.RequestCertificate")}, []string{"domainName"}},
		{"json: a body that is not an object is left to the plugin", &emulator.AWSRequest{Service: "acm",
			Operation: "RequestCertificate", Headers: jsonHeaders("CertificateManager.RequestCertificate"),
			Body: []byte(`[1]`)}, nil},
		{"json: a body that does not parse is left to the plugin", &emulator.AWSRequest{Service: "acm",
			Operation: "RequestCertificate", Headers: jsonHeaders("CertificateManager.RequestCertificate"),
			Body: []byte(`{`)}, nil},
		{"json: a blob's decoded length", &emulator.AWSRequest{Service: "kms", Operation: "Encrypt",
			Headers: jsonHeaders("TrentService.Encrypt"), Body: []byte(`{"KeyId":"k","Plaintext":""}`)},
			[]string{"plaintext"}},
		{"an operation the tables do not hold", &emulator.AWSRequest{Service: "acm", Operation: "NoSuchOperation"}, nil},

		// Query.
		{"query: a required list", &emulator.AWSRequest{Service: "elasticache",
			Operation: "AddTagsToResource", Params: map[string]string{"ResourceName": "arn:x"}},
			[]string{"tags"}},
		{"query: a list under its own member name", &emulator.AWSRequest{Service: "elasticache",
			Operation: "AddTagsToResource", Params: map[string]string{"ResourceName": "arn:x", "Tags.Tag.1.Key": "k"}},
			[]string{}},
		{"query: a structure member under a list element", &emulator.AWSRequest{Service: "sns",
			Operation: "TagResource", Params: map[string]string{"ResourceArn": "arn:x", "Tags.member.1.Key": "",
				"Tags.member.1.Value": "v"}}, []string{"tags.1.member.key"}},
		{"query: an empty list", &emulator.AWSRequest{Service: "sns", Operation: "TagResource",
			Params: map[string]string{"ResourceArn": "arn:x", "Tags": ""}}, []string{}},

		// EC2.
		{"ec2: a missing member", &emulator.AWSRequest{Service: "ec2", Operation: "CreateKeyPair",
			Params: map[string]string{}}, []string{"keyName"}},
		{"ec2: a member present", &emulator.AWSRequest{Service: "ec2", Operation: "CreateKeyPair",
			Params: map[string]string{"KeyName": "k"}}, []string{}},
		{"ec2: an enum", &emulator.AWSRequest{Service: "ec2", Operation: "CreateKeyPair",
			Params: map[string]string{"KeyName": "k", "KeyType": "dsa"}}, []string{"keyType"}},
		{"ec2: a list element's member, by its capitalized locationName", &emulator.AWSRequest{Service: "ec2",
			Operation: "CreateKeyPair", Params: map[string]string{"KeyName": "k",
				"TagSpecification.1.ResourceType": "not-a-type"}}, []string{"tagSpecifications.1.member.resourceType"}},
		{"ec2: a lowerCamel locationName is raised", &emulator.AWSRequest{Service: "ec2",
			Operation: "DeletePlacementGroup", Params: map[string]string{"GroupName": "g"}}, []string{}},
		{"ec2: a blob", &emulator.AWSRequest{Service: "ec2", Operation: "ImportKeyPair",
			Params: map[string]string{"KeyName": "k"}}, []string{"publicKeyMaterial"}},

		// REST-JSON.
		{"rest-json: a body member", &emulator.AWSRequest{Service: "sesv2", Operation: "CreateEmailIdentity",
			HTTPMethod: "POST", Path: "/v2/email/identities", Body: []byte(`{}`)}, []string{"emailIdentity"}},
		{"rest-json: a URI label", &emulator.AWSRequest{Service: "sesv2", Operation: "GetEmailIdentity",
			HTTPMethod: "GET", Path: "/v2/email/identities/a.example"}, []string{}},
		{"rest-json: a path the template does not match", &emulator.AWSRequest{Service: "sesv2",
			Operation: "GetEmailIdentity", HTTPMethod: "GET", Path: "/v2/email/other/a.example"}, nil},

		{"rest-json: a map under its minimum", &emulator.AWSRequest{Service: "appsync", Operation: "CreateGraphqlApi",
			HTTPMethod: "POST", Path: "/v1/apis", Body: []byte(`{"name":"a","authenticationType":"API_KEY","tags":{}}`)},
			[]string{"tags"}},
		{"rest-json: a header enum and a URI label", &emulator.AWSRequest{Service: "lambda", Operation: "Invoke",
			HTTPMethod: "POST", Path: "/2015-03-31/functions/f/invocations",
			Headers: map[string]string{"X-Amz-Invocation-Type": "Sometimes"}, Body: []byte(`{"any":"payload"}`)},
			[]string{"invocationType"}},
		{"rest-json: a blob payload is the body", &emulator.AWSRequest{Service: "lambda", Operation: "Invoke",
			HTTPMethod: "POST", Path: "/2015-03-31/functions/f/invocations", Body: []byte(`not json at all`)}, []string{}},
		{"rest-xml: a greedy label and a required query parameter", &emulator.AWSRequest{Service: "s3",
			Operation: "AbortMultipartUpload", HTTPMethod: "DELETE", Path: "/bucket/a/b/c.txt"},
			[]string{"uploadId"}},
		{"rest-xml: a greedy label with nothing to match", &emulator.AWSRequest{Service: "s3",
			Operation: "AbortMultipartUpload", HTTPMethod: "DELETE", Path: "/bucket"}, nil},
		{"rest-xml: a header enum", &emulator.AWSRequest{Service: "s3", Operation: "AbortMultipartUpload",
			HTTPMethod: "DELETE", Path: "/bucket/key", Params: map[string]string{"uploadId": "u"},
			Headers: map[string]string{"x-amz-request-payer": "someone"}}, []string{"requestPayer"}},
		{"rest-json: a repeated query-string list", &emulator.AWSRequest{Service: "sesv2", Operation: "GetEmailIdentity",
			HTTPMethod: "GET", Path: "/v2/email/identities/a.example",
			MultiValueParams: map[string][]string{"unused": {"a", "b"}}}, []string{}},
		{"rest-xml: a structure payload", &emulator.AWSRequest{Service: "cloudfront", Operation: "CreateInvalidation",
			HTTPMethod: "POST", Path: "/2020-05-31/distribution/E1/invalidation",
			Body: []byte(`<InvalidationBatch><Paths><Quantity>1</Quantity><Items><Path>/a</Path></Items></Paths>` +
				`</InvalidationBatch>`)}, []string{"invalidationBatch.callerReference"}},
		{"rest-xml: a structure payload absent", &emulator.AWSRequest{Service: "cloudfront", Operation: "CreateInvalidation",
			HTTPMethod: "POST", Path: "/2020-05-31/distribution/E1/invalidation"}, []string{"invalidationBatch"}},
		{"rest-xml: a flattened list in a payload", &emulator.AWSRequest{Service: "s3", Operation: "DeleteObjects",
			HTTPMethod: "POST", Path: "/bucket",
			Body: []byte(`<Delete><Object><Key>a</Key></Object><Object><VersionId>1</VersionId></Object></Delete>`)},
			[]string{"delete.objects.2.member.key"}},
		{"rest-xml: a flattened list absent", &emulator.AWSRequest{Service: "s3", Operation: "DeleteObjects",
			HTTPMethod: "POST", Path: "/bucket", Body: []byte(`<Delete><Quiet>true</Quiet></Delete>`)},
			[]string{"delete.objects"}},
		{"cbor: an integer under its range", &emulator.AWSRequest{Service: "monitoring", Operation: "DescribeAlarms",
			Headers: map[string]string{"Smithy-Protocol": "rpc-v2-cbor"},
			Body:    append(append([]byte{0xa1, 0x6a}, "MaxRecords"...), 0x00)}, []string{"maxRecords"}},
		{"cbor: a double over its range", &emulator.AWSRequest{Service: "monitoring", Operation: "DescribeAlarms",
			Headers: map[string]string{"Smithy-Protocol": "rpc-v2-cbor"},
			Body: append(append([]byte{0xa1, 0x6a}, "MaxRecords"...),
				0xfb, 0x40, 0x8f, 0x40, 0, 0, 0, 0, 0)}, []string{"maxRecords"}},
		{"ec2: a base64 blob present", &emulator.AWSRequest{Service: "ec2", Operation: "ImportKeyPair",
			Params: map[string]string{"KeyName": "k", "PublicKeyMaterial": "c3NoLXJzYQ=="}}, []string{}},
		{"json: a string where a structure belongs is left to the plugin", &emulator.AWSRequest{Service: "acm",
			Operation: "AddTagsToCertificate", Headers: jsonHeaders("CertificateManager.AddTagsToCertificate"),
			Body: []byte(`{"CertificateArn":"arn:aws:acm:us-east-1:123456789012:certificate/x","Tags":"nope"}`)}, nil},
		{"json: a number where a string belongs is left to the plugin", &emulator.AWSRequest{Service: "acm",
			Operation: "RequestCertificate", Headers: jsonHeaders("CertificateManager.RequestCertificate"),
			Body: []byte(`{"DomainName":5}`)}, nil},
		{"json: an object where a list element structure belongs", &emulator.AWSRequest{Service: "acm",
			Operation: "AddTagsToCertificate", Headers: jsonHeaders("CertificateManager.AddTagsToCertificate"),
			Body: []byte(`{"CertificateArn":"arn:aws:acm:us-east-1:123456789012:certificate/x","Tags":["k"]}`)}, nil},
		{"json: a list of scalars holding an object", &emulator.AWSRequest{Service: "acm", Operation: "ListCertificates",
			Headers: jsonHeaders("CertificateManager.ListCertificates"), Body: []byte(`{"CertificateStatuses":[{}]}`)}, nil},
		{"query: a model serving CBOR, JSON and Query is read in Query", &emulator.AWSRequest{Service: "monitoring",
			Operation: "DescribeAlarms", Params: map[string]string{"Action": "DescribeAlarms", "MaxRecords": "0"}},
			[]string{"maxRecords"}},
		{"json: a model whose primary protocol is CBOR is read in JSON", &emulator.AWSRequest{Service: "monitoring",
			Operation: "DescribeAlarms", Headers: jsonHeaders("GraniteServiceVersion20100801.DescribeAlarms"),
			Body: []byte(`{"MaxRecords":101}`)}, []string{"maxRecords"}},
		{"cbor: a model that does not serve CBOR is not validated", &emulator.AWSRequest{Service: "acm",
			Operation: "RequestCertificate", Headers: map[string]string{"Smithy-Protocol": "rpc-v2-cbor"},
			Body: []byte{0xa0}}, nil},
		{"json: a query-compatible request without a target is not validated", &emulator.AWSRequest{Service: "sqs",
			Operation: "TagQueue", Params: map[string]string{"Action": "TagQueue"}}, nil},

		// REST-XML.
		{"rest-xml: a URI label over its length", &emulator.AWSRequest{Service: "route53", Operation: "GetHostedZone",
			HTTPMethod: "GET", Path: "/2013-04-01/hostedzone/ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"}, []string{"id"}},
		{"rest-xml: body members", &emulator.AWSRequest{Service: "route53", Operation: "CreateHostedZone",
			HTTPMethod: "POST", Path: "/2013-04-01/hostedzone",
			Body: []byte(`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">` +
				`<Name>a.example</Name></CreateHostedZoneRequest>`)}, []string{"callerReference"}},
		{"rest-xml: a nested list under its minimum", &emulator.AWSRequest{Service: "route53",
			Operation: "ChangeResourceRecordSets", HTTPMethod: "POST", Path: "/2013-04-01/hostedzone/Z1/rrset/",
			Body: []byte(`<ChangeResourceRecordSetsRequest><ChangeBatch><Changes></Changes></ChangeBatch>` +
				`</ChangeResourceRecordSetsRequest>`)}, []string{"changeBatch.changes"}},
		{"rest-xml: a list element's member", &emulator.AWSRequest{Service: "route53",
			Operation: "ChangeResourceRecordSets", HTTPMethod: "POST", Path: "/2013-04-01/hostedzone/Z1/rrset/",
			Body: []byte(`<ChangeResourceRecordSetsRequest><ChangeBatch><Changes><Change><Action>CREATE</Action>` +
				`</Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`)},
			[]string{"changeBatch.changes.1.member.resourceRecordSet"}},
		{"rest-xml: a query-string enum", &emulator.AWSRequest{Service: "route53", Operation: "ListHostedZones",
			HTTPMethod: "GET", Path: "/2013-04-01/hostedzone", Params: map[string]string{"hostedzonetype": "Nope"}},
			[]string{"hostedZoneType"}},
		{"rest-xml: a body that does not parse is left to the plugin", &emulator.AWSRequest{Service: "route53",
			Operation: "CreateHostedZone", HTTPMethod: "POST", Path: "/2013-04-01/hostedzone", Body: []byte(`<a>`)}, nil},

		// CBOR, read through the same view as JSON once decoded.
		{"cbor: a missing member", &emulator.AWSRequest{Service: "monitoring", Operation: "PutMetricData",
			Headers: map[string]string{"Smithy-Protocol": "rpc-v2-cbor"}, Body: []byte{0xa0}}, []string{"namespace"}},
		{"cbor: a string under its minimum length and outside its pattern", &emulator.AWSRequest{Service: "monitoring", Operation: "PutMetricData",
			Headers: map[string]string{"Smithy-Protocol": "rpc-v2-cbor"},
			Body:    append([]byte{0xa1, 0x69}, append([]byte("Namespace"), 0x60)...)}, []string{"namespace", "namespace"}},
		{"cbor: a body that does not decode is left to the plugin", &emulator.AWSRequest{Service: "monitoring",
			Operation: "PutMetricData", Headers: map[string]string{"Smithy-Protocol": "rpc-v2-cbor"},
			Body: []byte{0xa1}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, emulator.WireFailurePathsForTest(tc.req))
		})
	}
}

// TestWireValidation_MapAndBlobReads pins the map and blob reads of the Query, XML and CBOR
// adapters, which no routed operation's constraints reach today.
func TestWireValidation_MapAndBlobReads(t *testing.T) {
	qe, qb, xe, xb, cb := emulator.WireViewReadsForTest(
		map[string]string{"M.entry.1.key": "a", "M.entry.1.value": "x", "M.entry.2.key": "b", "B": "aGk="},
		[]byte(`<R><M><entry><key>a</key></entry><entry><key>b</key></entry><entry><key>c</key></entry></M><B> aGVsbG8= </B></R>`),
		[]byte{1, 2, 3, 4})
	assert.Equal(t, 2, qe, "query map entries")
	assert.Equal(t, 2, qb, "query blob bytes")
	assert.Equal(t, 3, xe, "xml map entries")
	assert.Equal(t, 5, xb, "xml blob bytes")
	assert.Equal(t, 4, cb, "cbor blob bytes")

	qe, qb, xe, xb, cb = emulator.WireViewReadsForTest(map[string]string{"B": "not base64!"}, []byte(`<R/>`), nil)
	assert.Zero(t, qe+qb+xe+xb+cb, "absent maps and unreadable blobs read as nothing")
}

// TestWireValidation_RESTBindingReads pins the REST adapter's reads outside the body: a list
// bound to a repeated query-string key or to a single one, a header, and a prefixed header set.
func TestWireValidation_RESTBindingReads(t *testing.T) {
	list, header, blob, prefixed := emulator.WireRESTReadsForTest(&emulator.AWSRequest{
		MultiValueParams: map[string][]string{"q": {"a", "b", "c"}},
		Headers:          map[string]string{"h": "aGk=", "X-Meta-One": "1", "x-meta-two": "2", "Other": "3"},
	})
	assert.Equal(t, 3, list)
	assert.Equal(t, "aGk=", header, "a header is matched whatever its case")
	assert.Equal(t, 2, blob)
	assert.Equal(t, 2, prefixed)

	list, header, blob, prefixed = emulator.WireRESTReadsForTest(&emulator.AWSRequest{
		Params: map[string]string{"q": "one"}, Headers: map[string]string{"H": "not base64!"},
	})
	assert.Equal(t, 1, list, "a list sent once arrives in Params")
	assert.Equal(t, "not base64!", header)
	assert.Equal(t, -1, blob)
	assert.Equal(t, -1, prefixed)

	list, header, _, _ = emulator.WireRESTReadsForTest(&emulator.AWSRequest{})
	assert.Equal(t, -1, list)
	assert.Empty(t, header)
}

// TestWireValidation_TheQuotedStyleRendersTheValue pins the message form most JSON services
// answer with, which ACM does not use: the failing value is quoted, a list of scalars renders as
// [a, b], and a structure renders no value.
func TestWireValidation_TheQuotedStyleRendersTheValue(t *testing.T) {
	jsonReq := func(target, body string) *emulator.AWSRequest {
		return &emulator.AWSRequest{Service: "acm", Operation: target,
			Headers: map[string]string{"X-Amz-Target": "CertificateManager." + target}, Body: []byte(body)}
	}
	assert.Equal(t, "1 validation error detected: Value '0' at 'maxItems' failed to satisfy constraint: "+
		"Member must have value greater than or equal to 1",
		emulator.WireFailureMessageForTest(jsonReq("ListCertificates", `{"MaxItems":0}`)))
	assert.Equal(t, "1 validation error detected: Value '[ISSUED, NOPE]' at 'certificateStatuses' failed to "+
		"satisfy constraint: Member must satisfy constraint: [Member must satisfy enum value set: "+
		"[PENDING_VALIDATION, ISSUED, INACTIVE, EXPIRED, VALIDATION_TIMED_OUT, REVOKED, FAILED]]",
		emulator.WireFailureMessageForTest(jsonReq("ListCertificates", `{"CertificateStatuses":["ISSUED","NOPE"]}`)))
	assert.Equal(t, "1 validation error detected: Value at 'tags' failed to satisfy constraint: "+
		"Member must have length greater than or equal to 1",
		emulator.WireFailureMessageForTest(jsonReq("AddTagsToCertificate",
			`{"CertificateArn":"arn:aws:acm:us-east-1:123456789012:certificate/x","Tags":[]}`)))
	assert.Empty(t, emulator.WireFailureMessageForTest(jsonReq("ListCertificates", `{}`)))
}

// TestWireValidation_TablesCoverTheValidatedServices pins that every service validation is on
// for has its operations in the generated tables, so a listed service cannot silently validate
// nothing, and that the snapshot version names botocore.
func TestWireValidation_TablesCoverTheValidatedServices(t *testing.T) {
	for _, svc := range emulator.WireValidatedServicesForTest() {
		ops := emulator.RoutedOperations(svc)
		assert.NotEmpty(t, ops, svc)
		for _, op := range ops {
			assert.True(t, emulator.WireOperationCoveredForTest(svc, op), "%s %s has no table", svc, op)
		}
	}
	assert.Regexp(t, `^botocore/\d+\.\d+\.\d+$`, emulator.WireModelVersionForTest())
}
