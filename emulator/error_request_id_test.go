package emulator_test

// No XML error response carried the request's id (#1241).
//
// The Query document was one struct literal with no <RequestId> at all, and the EC2
// document carried the fixed "SUBSTRATE" — so a consumer correlating a call by its
// request id could do so when the call succeeded and not when it failed, which is the
// half where the id is actually used. The documents are now the ones the protocol
// specifications publish (smithy awsQuery/restXml: <RequestId> a sibling of <Error>;
// smithy ec2Query and the EC2 API reference: <RequestID> beside <Errors>), carrying
// reqCtx.RequestID — the id [emulator.Event.RequestID] records and a replay is
// dispatched with.

import (
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestMarshalAWSError_RequestIDPerFamily pins every protocol family's document byte for
// byte, given a request id. The two XML families render it in their own element; S3
// keeps the fixed id its plugin-raised errors carry, so a pipeline error stays
// byte-identical to a plugin one (#480); the JSON and CBOR families carry none in the
// body, as before.
func TestMarshalAWSError_RequestIDPerFamily(t *testing.T) {
	t.Parallel()
	const id = "req-1241"
	for _, tc := range []struct {
		proto string
		want  string
	}{
		{emulator.ErrProtoQueryXMLForTest,
			`<ErrorResponse><Error><Type>Sender</Type><Code>NotFound</Code><Message>gone</Message></Error><RequestId>req-1241</RequestId></ErrorResponse>`},
		{emulator.ErrProtoEC2XMLForTest,
			`<Response><Errors><Error><Code>NotFound</Code><Message>gone</Message></Error></Errors><RequestID>req-1241</RequestID></Response>`},
	} {
		t.Run(tc.proto, func(t *testing.T) {
			t.Parallel()
			body, _, _ := emulator.MarshalAWSErrorWithRequestIDForTest("NotFound", "gone", tc.proto, "", "", id, false, http.StatusNotFound)
			assert.Equal(t, tc.want, string(body))
		})
	}

	t.Run("s3 keeps the fixed id", func(t *testing.T) {
		t.Parallel()
		body, _, _ := emulator.MarshalAWSErrorWithRequestIDForTest("NoSuchKey", "gone", emulator.ErrProtoS3XMLForTest, "", "s3", id, false, http.StatusNotFound)
		assert.Contains(t, string(body), "<RequestId>SUBSTRATE</RequestId>")
		assert.NotContains(t, string(body), id)
	})

	for _, proto := range []string{emulator.ErrProtoJSONRPCForTest, emulator.ErrProtoRESTJSONForTest, emulator.ErrProtoRPCV2CBORForTest} {
		t.Run(proto+" carries none", func(t *testing.T) {
			t.Parallel()
			body, _, headers := emulator.MarshalAWSErrorWithRequestIDForTest("NotFound", "gone", proto, "", "", id, false, http.StatusNotFound)
			assert.NotContains(t, string(body), id)
			for k, v := range headers {
				assert.NotContains(t, v, id, "header %s", k)
			}
		})
	}

	// The negative control: with no request in hand the fixed fallback is rendered,
	// never a freshly minted id.
	t.Run("no request renders the fallback", func(t *testing.T) {
		t.Parallel()
		body, _, _ := emulator.MarshalAWSErrorForTest("NotFound", "gone", emulator.ErrProtoQueryXMLForTest, "", http.StatusNotFound)
		assert.Equal(t,
			`<ErrorResponse><Error><Type>Sender</Type><Code>NotFound</Code><Message>gone</Message></Error><RequestId>SUBSTRATE</RequestId></ErrorResponse>`,
			string(body))
	})
}

// errorRequestIDCall sends one Query-form request to host and returns its status and body.
func errorRequestIDCall(t *testing.T, ts *emulator.TestServer, host string, form url.Values) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

// TestXMLErrorResponse_CarriesTheRecordedRequestID asserts the element over the wire,
// for a refusal from each XML error family and from more than one plugin sharing the
// Query envelope — SNS, RDS and ELB through the server's shared writer, IAM through
// the document its own handlers build — and that the value is the id the refusal's
// event recorded. It then replays the stream and asserts no difference anywhere,
// which includes the request-id path of the IAM refusal, whose body is recorded and
// compared.
func TestXMLErrorResponse_CarriesTheRecordedRequestID(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	type ec2Doc struct {
		XMLName   xml.Name `xml:"Response"`
		Code      string   `xml:"Errors>Error>Code"`
		RequestID string   `xml:"RequestID"`
	}
	type queryDoc struct {
		XMLName   xml.Name `xml:"ErrorResponse"`
		Code      string   `xml:"Error>Code"`
		RequestID string   `xml:"RequestId"`
	}

	for _, tc := range []struct {
		name     string
		host     string
		form     url.Values
		ec2      bool
		wantCode string
	}{
		{"sns", "sns.us-east-1.amazonaws.com", url.Values{
			"Action": {"GetTopicAttributes"}, "TopicArn": {"arn:aws:sns:us-east-1:123456789012:absent"},
		}, false, "NotFound"},
		{"rds", "rds.us-east-1.amazonaws.com", url.Values{
			"Action": {"DescribeDBInstances"}, "DBInstanceIdentifier": {"absent"},
		}, false, "DBInstanceNotFound"},
		{"elb", "elasticloadbalancing.us-east-1.amazonaws.com", url.Values{
			"Action": {"AddTags"}, "Version": {"2012-06-01"}, "LoadBalancerNames.member.1": {"absent"},
			"Tags.member.1.Key": {"env"}, "Tags.member.1.Value": {"prod"},
		}, false, "LoadBalancerNotFound"},
		{"iam, a handler-built document", "iam.amazonaws.com", url.Values{
			"Action": {"GetPolicy"}, "PolicyArn": {"arn:aws:iam::123456789012:policy/absent"},
		}, false, "NoSuchEntity"},
		{"ec2", "ec2.us-east-1.amazonaws.com", url.Values{
			"Action": {"DescribeInstances"}, "InstanceId.1": {"i-0123456789abcdef0"},
		}, true, "InvalidInstanceID.NotFound"},
	} {
		status, body := errorRequestIDCall(t, ts, tc.host, tc.form)
		require.GreaterOrEqual(t, status, http.StatusBadRequest, "%s must be refused: %s", tc.name, body)

		events, err := ts.Store().GetStream(t.Context(), "default")
		require.NoError(t, err)
		require.NotEmpty(t, events)
		recorded := events[len(events)-1].RequestID
		require.NotEmpty(t, recorded, "%s: the event must carry the id the request was served under", tc.name)

		var code, got, element string
		if tc.ec2 {
			var doc ec2Doc
			require.NoError(t, xml.Unmarshal(body, &doc), "%s body was %s", tc.name, body)
			code, got, element = doc.Code, doc.RequestID, "<RequestID>"+recorded+"</RequestID></Response>"
		} else {
			var doc queryDoc
			require.NoError(t, xml.Unmarshal(body, &doc), "%s body was %s", tc.name, body)
			code, got, element = doc.Code, doc.RequestID, "</Error><RequestId>"+recorded+"</RequestId></ErrorResponse>"
		}
		assert.Equal(t, tc.wantCode, code, "%s", tc.name)
		assert.Equal(t, recorded, got, "%s: the error must carry the recorded request id", tc.name)
		// The element's position, not only its value: a sibling of <Error> (or of
		// <Errors>), closing the document, as the protocol specifications publish it.
		assert.True(t, strings.HasSuffix(string(body), element), "%s body was %s", tc.name, body)
	}

	engine := emulator.NewReplayEngine(ts.Store(), ts.StateManager(), ts.TimeController(), ts.Registry(),
		emulator.ReplayConfig{}, emulator.NewDefaultLogger(slog.LevelError, false))
	results, err := engine.Replay(t.Context(), "default")
	require.NoError(t, err)
	require.Zero(t, results.SkippedEvents, "every refusal must be re-executed, not skipped")
	assert.Empty(t, results.Differences, "a replayed refusal reproduces its recording: %s", replayDifferenceSummary(results))
}

// TestQueryError_TypeNamesTheFault pins the Query error document's <Type> per status
// (#1413). The smithy awsQuery specification defines it as "One of 'Sender' or
// 'Receiver'; whomever is at fault from the service perspective". Until #1413 both the
// server's shared writer and IAM's handler-built document answered "Sender" for a 5xx
// as well.
func TestQueryError_TypeNamesTheFault(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		status int
		want   string
	}{
		{"a 400 is the sender's", http.StatusBadRequest, "<Type>Sender</Type>"},
		{"a 404 is the sender's", http.StatusNotFound, "<Type>Sender</Type>"},
		{"a 500 is the receiver's", http.StatusInternalServerError, "<Type>Receiver</Type>"},
		{"a 503 is the receiver's", http.StatusServiceUnavailable, "<Type>Receiver</Type>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body, _, _ := emulator.MarshalAWSErrorWithRequestIDForTest("Code", "msg", emulator.ErrProtoQueryXMLForTest, "", "", "req", false, tc.status)
			assert.Contains(t, string(body), "<Error>"+tc.want, "shared Query writer: %s", body)
			assert.Contains(t, string(emulator.IAMErrorResponseForTest("Code", "msg", tc.status)), "<Error>"+tc.want, "IAM document")
		})
	}
}

// TestIAMSuccessResponse_CarriesTheRecordedRequestID asserts that IAM success documents,
// with a result and without one, carry the request's own id in ResponseMetadata
// (#1413). Until then both builders rendered the literal "stub-request-id", although
// #1149 had made ELB, Redshift and the other Query plugins render the request's id.
// The stream is then replayed, and the bodies must be byte-identical.
func TestIAMSuccessResponse_CarriesTheRecordedRequestID(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	type successDoc struct {
		RequestID string `xml:"ResponseMetadata>RequestId"`
	}
	for _, tc := range []struct {
		name string
		form url.Values
	}{
		{"a result-bearing create", url.Values{"Action": {"CreateUser"}, "UserName": {"rid-user"}}},
		{"a result-bearing read", url.Values{"Action": {"GetUser"}, "UserName": {"rid-user"}}},
		{"a memberless response", url.Values{"Action": {"DeleteUser"}, "UserName": {"rid-user"}}},
	} {
		status, body := errorRequestIDCall(t, ts, "iam.amazonaws.com", tc.form)
		require.Equal(t, http.StatusOK, status, "%s: %s", tc.name, body)

		events, err := ts.Store().GetStream(t.Context(), "default")
		require.NoError(t, err)
		require.NotEmpty(t, events)
		recorded := events[len(events)-1].RequestID
		require.NotEmpty(t, recorded, "%s: the event must carry the id the request was served under", tc.name)

		var doc successDoc
		require.NoError(t, xml.Unmarshal(body, &doc), "%s body was %s", tc.name, body)
		assert.Equal(t, recorded, doc.RequestID, "%s: the response must carry the recorded request id", tc.name)
		assert.NotContains(t, string(body), "stub-request-id", "%s", tc.name)
		assert.True(t, strings.HasSuffix(string(body),
			"<ResponseMetadata><RequestId>"+recorded+"</RequestId></ResponseMetadata></"+tc.form.Get("Action")+"Response>"),
			"%s body was %s", tc.name, body)
	}

	engine := emulator.NewReplayEngine(ts.Store(), ts.StateManager(), ts.TimeController(), ts.Registry(),
		emulator.ReplayConfig{}, emulator.NewDefaultLogger(slog.LevelError, false))
	results, err := engine.Replay(t.Context(), "default")
	require.NoError(t, err)
	require.Zero(t, results.SkippedEvents, "every request must be re-executed, not skipped")
	assert.Empty(t, results.Differences, "a replayed success reproduces its recording: %s", replayDifferenceSummary(results))
}
