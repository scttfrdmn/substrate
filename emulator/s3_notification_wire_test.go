package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// These are #542's gates. Every body here is written the way real S3 writes it —
// singular configuration elements, the destination named after the service, a
// repeated bare Event — and every assertion is against the raw wire response.
//
// A round-trip through substrate's own struct is what hid this: a type
// marshaled and unmarshaled by its own definition agrees with itself whatever
// its tags say. The pre-existing round-trip test passed throughout, and so did a
// delete-bucket test whose "stored configuration" was never stored at all.

// notificationXML is the request body a caller writes to configure one SQS
// destination, in real S3's element names.
func notificationXML(queueARN string) []byte {
	return []byte(`<NotificationConfiguration>` +
		`<QueueConfiguration>` +
		`<Id>toSQS</Id>` +
		`<Queue>` + queueARN + `</Queue>` +
		`<Event>s3:ObjectCreated:*</Event>` +
		`</QueueConfiguration>` +
		`</NotificationConfiguration>`)
}

// putNotification configures a bucket's notifications from a raw XML body.
func putNotification(t *testing.T, srv *emulator.Server, bucket string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	return s3Request(t, srv, http.MethodPut, "/"+bucket+"?notification", body,
		map[string]string{"Content-Type": "application/xml"})
}

// getNotification reads a bucket's notification configuration and returns the raw body.
func getNotification(t *testing.T, srv *emulator.Server, bucket string) (int, string) {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?notification", nil, nil)
	return w.Code, w.Body.String()
}

// TestS3Notification_RealS3BodyRoundTrips is the read-back half of #542: a body
// in the API's element names comes back as the same configuration. Before the fix
// every element of this body matched no field, xml.Unmarshal reported no error,
// and an empty configuration was persisted with a 200.
func TestS3Notification_RealS3BodyRoundTrips(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/notif", nil, nil).Code)

	const queueARN = "arn:aws:sqs:us-east-1:123456789012:my-queue"
	require.Equal(t, http.StatusOK, putNotification(t, srv, "notif", notificationXML(queueARN)).Code)

	code, body := getNotification(t, srv, "notif")
	require.Equal(t, http.StatusOK, code, "body: %s", body)

	// The wire names, not substrate's field names. Asserting on the raw body is
	// the point: a projection through the state struct would agree with itself.
	assert.Contains(t, body, "<QueueConfiguration>",
		"the configuration element is singular on the wire")
	assert.NotContains(t, body, "<QueueConfigurations>",
		"a plural element is substrate's field name leaking onto the wire")
	assert.Contains(t, body, "<Queue>"+queueARN+"</Queue>",
		"the destination element is Queue, not QueueArn")
	assert.NotContains(t, body, "QueueArn")
	assert.Contains(t, body, "<Event>s3:ObjectCreated:*</Event>",
		"events repeat as bare Event elements")
	assert.NotContains(t, body, "<Events>")
	assert.Contains(t, body, "<Id>toSQS</Id>")
}

// TestS3Notification_AllThreeDestinations covers each configuration element
// separately, so a single passing assertion cannot stand in for all three. Each
// destination has its own element name and its own differently-named target.
func TestS3Notification_AllThreeDestinations(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantsXML []string
	}{
		{
			name: "topic",
			body: `<NotificationConfiguration><TopicConfiguration>` +
				`<Id>t1</Id>` +
				`<Topic>arn:aws:sns:us-east-1:123456789012:topic-a</Topic>` +
				`<Event>s3:ObjectCreated:Put</Event>` +
				`</TopicConfiguration></NotificationConfiguration>`,
			wantsXML: []string{
				"<TopicConfiguration>",
				"<Topic>arn:aws:sns:us-east-1:123456789012:topic-a</Topic>",
				"<Event>s3:ObjectCreated:Put</Event>",
			},
		},
		{
			name: "queue",
			body: `<NotificationConfiguration><QueueConfiguration>` +
				`<Id>q1</Id>` +
				`<Queue>arn:aws:sqs:us-east-1:123456789012:queue-a</Queue>` +
				`<Event>s3:ObjectRemoved:*</Event>` +
				`</QueueConfiguration></NotificationConfiguration>`,
			wantsXML: []string{
				"<QueueConfiguration>",
				"<Queue>arn:aws:sqs:us-east-1:123456789012:queue-a</Queue>",
				"<Event>s3:ObjectRemoved:*</Event>",
			},
		},
		{
			// The SDKs call this member LambdaFunctionConfigurations; the XML kept
			// the original CloudFunctionConfiguration/CloudFunction names, so
			// matching on the SDK's spelling finds nothing.
			name: "lambda",
			body: `<NotificationConfiguration><CloudFunctionConfiguration>` +
				`<Id>l1</Id>` +
				`<CloudFunction>arn:aws:lambda:us-east-1:123456789012:function:f</CloudFunction>` +
				`<Event>s3:ObjectCreated:*</Event>` +
				`</CloudFunctionConfiguration></NotificationConfiguration>`,
			wantsXML: []string{
				"<CloudFunctionConfiguration>",
				"<CloudFunction>arn:aws:lambda:us-east-1:123456789012:function:f</CloudFunction>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/dests", nil, nil).Code)
			require.Equal(t, http.StatusOK, putNotification(t, srv, "dests", []byte(tt.body)).Code)

			code, body := getNotification(t, srv, "dests")
			require.Equal(t, http.StatusOK, code, "body: %s", body)
			for _, want := range tt.wantsXML {
				assert.Contains(t, body, want)
			}
		})
	}
}

// TestS3Notification_KeyFilterRoundTrips covers the Filter/S3Key/FilterRule
// nesting, whose inner element is S3Key on the wire and Key in state, and whose
// rules repeat as FilterRule rather than nesting under FilterRules.
func TestS3Notification_KeyFilterRoundTrips(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/filt", nil, nil).Code)

	body := `<NotificationConfiguration><QueueConfiguration>` +
		`<Queue>arn:aws:sqs:us-east-1:123456789012:filtered</Queue>` +
		`<Event>s3:ObjectCreated:Put</Event>` +
		`<Filter><S3Key>` +
		`<FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule>` +
		`<FilterRule><Name>suffix</Name><Value>.jpg</Value></FilterRule>` +
		`</S3Key></Filter>` +
		`</QueueConfiguration></NotificationConfiguration>`
	require.Equal(t, http.StatusOK, putNotification(t, srv, "filt", []byte(body)).Code)

	code, out := getNotification(t, srv, "filt")
	require.Equal(t, http.StatusOK, code, "body: %s", out)
	assert.Contains(t, out, "<S3Key>", "the inner filter element is S3Key on the wire, not Key")
	assert.NotContains(t, out, "<Key>")
	assert.Contains(t, out, "<FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule>")
	assert.Contains(t, out, "<FilterRule><Name>suffix</Name><Value>.jpg</Value></FilterRule>")
	assert.NotContains(t, out, "<FilterRules>")
}

// TestS3Notification_EmptyConfigurationDisables covers the documented way to turn
// notifications off, and is the case a MalformedXML refusal must not swallow.
func TestS3Notification_EmptyConfigurationDisables(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/off", nil, nil).Code)

	const queueARN = "arn:aws:sqs:us-east-1:123456789012:disable-me"
	require.Equal(t, http.StatusOK, putNotification(t, srv, "off", notificationXML(queueARN)).Code)
	_, configured := getNotification(t, srv, "off")
	require.Contains(t, configured, queueARN, "the configuration must be stored first")

	require.Equal(t, http.StatusOK,
		putNotification(t, srv, "off", []byte(`<NotificationConfiguration></NotificationConfiguration>`)).Code,
		"an empty configuration is the documented way to disable notifications")

	code, body := getNotification(t, srv, "off")
	require.Equal(t, http.StatusOK, code)
	assert.NotContains(t, body, queueARN, "the destination must be gone")
	assert.Contains(t, body, "NotificationConfiguration",
		"a bucket with no configuration still returns an empty NotificationConfiguration")
}

// TestS3Notification_UnrecognizedBodyIsMalformedXML covers the refusal that keeps
// #542 from recurring silently. xml.Unmarshal reports no error for a body none of
// whose elements it recognizes, so without this check a body full of wrong
// element names is indistinguishable from a deliberate disable — which is exactly
// how a real-S3 body used to turn every notification on the bucket off.
func TestS3Notification_UnrecognizedBodyIsMalformedXML(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/bad", nil, nil).Code)

	tests := []struct {
		name string
		body string
	}{
		{
			// The shape substrate used to require. It is no longer accepted, and
			// accepting it quietly is what the bug was.
			name: "substrate's old field names",
			body: `<NotificationConfiguration><QueueConfigurations>` +
				`<QueueArn>arn:aws:sqs:us-east-1:123456789012:q</QueueArn>` +
				`<Events>s3:ObjectCreated:*</Events>` +
				`</QueueConfigurations></NotificationConfiguration>`,
		},
		{
			name: "an element from no schema at all",
			body: `<NotificationConfiguration><Nonsense>x</Nonsense></NotificationConfiguration>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := putNotification(t, srv, "bad", []byte(tt.body))
			require.Equal(t, http.StatusBadRequest, w.Code, "body: %s", w.Body.Bytes())
			assert.Equal(t, "MalformedXML", parseS3Error(t, w.Body.Bytes()).Code)
		})
	}

	// The refusal must not have stored anything.
	code, body := getNotification(t, srv, "bad")
	require.Equal(t, http.StatusOK, code)
	assert.NotContains(t, body, "QueueConfiguration",
		"a refused body must leave the bucket unconfigured")
}

// TestS3Notification_EventBridgeConfiguration covers the element that carries no
// members, so "was it present" is the only thing to record.
func TestS3Notification_EventBridgeConfiguration(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/eventbridge", nil, nil).Code)

	require.Equal(t, http.StatusOK, putNotification(t, srv, "eventbridge",
		[]byte(`<NotificationConfiguration><EventBridgeConfiguration></EventBridgeConfiguration></NotificationConfiguration>`)).Code,
		"an EventBridge-only configuration names a destination, so it is not an empty body")

	code, body := getNotification(t, srv, "eventbridge")
	require.Equal(t, http.StatusOK, code, "body: %s", body)
	assert.Contains(t, body, "EventBridgeConfiguration")
}

// TestS3Notification_FiresSQSFromRealS3Body is #542's substantive half. The
// read-back being wrong was the filed symptom; fireNotifications reads the same
// record, so a configuration submitted the AWS way meant no notification was ever
// dispatched. A test asserting a message arrives failed far from its assertion,
// and one asserting none arrives passed vacuously.
func TestS3Notification_FiresSQSFromRealS3Body(t *testing.T) {
	srv := newS3SQSTestServer(t)

	sqsFormRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "wire-notify-queue",
	})
	const queueARN = "arn:aws:sqs:us-east-1:123456789012:wire-notify-queue"
	const queueURL = "http://sqs.us-east-1.localhost/123456789012/wire-notify-queue"

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/fire-wire", nil, nil).Code)
	require.Equal(t, http.StatusOK, putNotification(t, srv, "fire-wire", notificationXML(queueARN)).Code)

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/fire-wire/k.txt",
		[]byte("hello"), map[string]string{"Content-Type": "text/plain"}).Code)

	w := sqsFormRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
		"VisibilityTimeout":   "0",
	})
	body := w.Body.String()
	require.Contains(t, body, "s3:ObjectCreated",
		"a configuration written the AWS way must actually dispatch: %s", body)
	assert.Contains(t, body, "fire-wire", "the event names the bucket")
	assert.Contains(t, body, "k.txt", "the event names the key")
}

// TestS3Notification_KeyFilterAppliesOnDispatch proves the filter survives the
// round trip into state where fireNotifications reads it — the wire's S3Key
// nesting reaching s3KeyFilterMatches, not just reaching the read-back.
func TestS3Notification_KeyFilterAppliesOnDispatch(t *testing.T) {
	srv := newS3SQSTestServer(t)

	sqsFormRequest(t, srv, map[string]string{
		"Action":    "CreateQueue",
		"QueueName": "filtered-queue",
	})
	const queueARN = "arn:aws:sqs:us-east-1:123456789012:filtered-queue"
	const queueURL = "http://sqs.us-east-1.localhost/123456789012/filtered-queue"

	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/filter-fire", nil, nil).Code)
	cfg := `<NotificationConfiguration><QueueConfiguration>` +
		`<Queue>` + queueARN + `</Queue>` +
		`<Event>s3:ObjectCreated:*</Event>` +
		`<Filter><S3Key><FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule></S3Key></Filter>` +
		`</QueueConfiguration></NotificationConfiguration>`
	require.Equal(t, http.StatusOK, putNotification(t, srv, "filter-fire", []byte(cfg)).Code)

	// A key outside the prefix must not dispatch.
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/filter-fire/docs/a.txt", []byte("x"), nil).Code)
	w := sqsFormRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
		"VisibilityTimeout":   "0",
	})
	assert.NotContains(t, w.Body.String(), "s3:ObjectCreated",
		"a key outside the filter's prefix must not dispatch")

	// A key inside it must.
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/filter-fire/images/a.jpg", []byte("x"), nil).Code)
	w2 := sqsFormRequest(t, srv, map[string]string{
		"Action":              "ReceiveMessage",
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": "1",
		"VisibilityTimeout":   "0",
	})
	assert.Contains(t, w2.Body.String(), "s3:ObjectCreated",
		"a key inside the filter's prefix must dispatch: %s", w2.Body.String())
}

// TestS3Notification_JSONBodyStillAccepted pins substrate's own convenience path,
// which predates the XML one and is keyed on the state shape's json tags. It is
// what the existing FireSQS test uses, so the XML fix must not break it.
func TestS3Notification_JSONBodyStillAccepted(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/jsonb", nil, nil).Code)

	const queueARN = "arn:aws:sqs:us-east-1:123456789012:json-queue"
	body := `{"QueueConfigurations":[{"Id":"j1","QueueArn":"` + queueARN +
		`","Events":["s3:ObjectCreated:*"]}]}`
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/jsonb?notification", []byte(body),
			map[string]string{"Content-Type": "application/json"}).Code)

	code, out := getNotification(t, srv, "jsonb")
	require.Equal(t, http.StatusOK, code, "body: %s", out)
	// Read back on the wire, in the API's names, whatever shape went in.
	assert.Contains(t, out, "<Queue>"+queueARN+"</Queue>")
}

// TestS3Notification_UnconfiguredBucketReturnsEmptyElement pins the shape of the
// read for a bucket that has never been configured, which must parse as a
// NotificationConfiguration rather than 404 or an empty body.
func TestS3Notification_UnconfiguredBucketReturnsEmptyElement(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/fresh", nil, nil).Code)

	code, body := getNotification(t, srv, "fresh")
	require.Equal(t, http.StatusOK, code, "body: %s", body)

	var parsed struct {
		XMLName xml.Name `xml:"NotificationConfiguration"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &parsed), "body: %s", body)
	for _, dest := range []string{"TopicConfiguration", "QueueConfiguration",
		"CloudFunctionConfiguration", "EventBridgeConfiguration"} {
		assert.NotContains(t, body, "<"+dest+">", "no destination is configured")
	}
}
