package emulator_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CloudWatch's three wire protocols (#785).
//
// The tests below are split deliberately. The flattening and rendering tests pin the
// translation itself — the query keys a JSON or CBOR body produces, and the bytes each
// renderer emits — because those are the units where a mistake is silent. The
// end-to-end tests drive the real HTTP server the way each of the three real clients
// does, because the defect #785 reports was invisible to every test substrate had: they
// all posted a query form and read XML directly, so a plugin answering XML to a CBOR
// client looked green.

// --- Request flattening -----------------------------------------------------

func TestCWNormalizeInput_JSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want map[string]string
	}{
		{
			name: "scalars keep their member names",
			body: `{"AlarmName":"cpu-high","Threshold":80.5,"ActionsEnabled":true}`,
			want: map[string]string{
				"AlarmName":      "cpu-high",
				"Threshold":      "80.5",
				"ActionsEnabled": "true",
			},
		},
		{
			// The whole reason the flattening works: CloudWatch declares no
			// xmlFlattened list, so every list is wrapped in "member".
			name: "a list of strings becomes one-based members",
			body: `{"AlarmNames":["a","b"]}`,
			want: map[string]string{
				"AlarmNames.member.1": "a",
				"AlarmNames.member.2": "b",
			},
		},
		{
			name: "a list of structures indexes then names",
			body: `{"MetricData":[{"MetricName":"RequestCount","Value":42},` +
				`{"MetricName":"ErrorRate","Value":0.1}]}`,
			want: map[string]string{
				"MetricData.member.1.MetricName": "RequestCount",
				"MetricData.member.1.Value":      "42",
				"MetricData.member.2.MetricName": "ErrorRate",
				"MetricData.member.2.Value":      "0.1",
			},
		},
		{
			// The deepest input shape CloudWatch has, and the one GetMetricData's
			// existing handler already reads in this spelling.
			name: "nested structures are dotted",
			body: `{"MetricDataQueries":[{"Id":"size_0","MetricStat":{"Metric":` +
				`{"Namespace":"AWS/S3","MetricName":"BucketSizeBytes"},"Period":86400,` +
				`"Stat":"Average"}}]}`,
			want: map[string]string{
				"MetricDataQueries.member.1.Id":                           "size_0",
				"MetricDataQueries.member.1.MetricStat.Metric.Namespace":  "AWS/S3",
				"MetricDataQueries.member.1.MetricStat.Metric.MetricName": "BucketSizeBytes",
				"MetricDataQueries.member.1.MetricStat.Period":            "86400",
				"MetricDataQueries.member.1.MetricStat.Stat":              "Average",
			},
		},
		{
			// JSON has no integer type, so every number decodes as a float64. A
			// Period that came out as "86400.000000" would fail strconv.Atoi and the
			// handler would silently see zero.
			name: "a whole number is written without a fraction or an exponent",
			body: `{"Period":86400,"EvaluationPeriods":1,"Value":1e15}`,
			want: map[string]string{
				"Period":            "86400",
				"EvaluationPeriods": "1",
				"Value":             "1000000000000000",
			},
		},
		{
			// Past int64 the integer form is unavailable, so 'g' takes over. Nothing
			// reads such a value with strconv.Atoi — every integral CloudWatch member
			// is a 32-bit period, count or limit — and ParseFloat takes the exponent.
			name: "a number too large for an int64 keeps its exponent",
			body: `{"Value":1e21}`,
			want: map[string]string{"Value": "1e+21"},
		},
		{
			name: "an explicit null is dropped, as an absent member would be",
			body: `{"AlarmName":"x","AlarmDescription":null}`,
			want: map[string]string{"AlarmName": "x"},
		},
		{
			name: "an empty list contributes nothing",
			body: `{"AlarmNames":[]}`,
			want: map[string]string{},
		},
		{
			name: "an empty body is not an error",
			body: `{}`,
			want: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := emulator.CWNormalizeInputForTest(emulator.WireJSONRPC, []byte(tt.body))
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCWNormalizeInput_CBOR(t *testing.T) {
	t.Parallel()

	t.Run("a CBOR document flattens to the same keys as its JSON twin", func(t *testing.T) {
		t.Parallel()
		body, err := emulator.CBOREncodeForTest(emulator.CBORMapForTest(
			emulator.CBORPairForTest{Key: "Namespace", Value: "CargoShip/Test"},
			emulator.CBORPairForTest{Key: "MetricData", Value: []any{
				emulator.CBORMapForTest(
					emulator.CBORPairForTest{Key: "MetricName", Value: "RequestCount"},
					emulator.CBORPairForTest{Key: "Value", Value: 42.0},
					emulator.CBORPairForTest{Key: "Unit", Value: "Count"},
				),
			}},
		))
		require.NoError(t, err)

		got, err := emulator.CWNormalizeInputForTest(emulator.WireRPCV2CBOR, body)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			"Namespace":                      "CargoShip/Test",
			"MetricData.member.1.MetricName": "RequestCount",
			"MetricData.member.1.Value":      "42",
			"MetricData.member.1.Unit":       "Count",
		}, got)
	})

	t.Run("a timestamp arrives as tag 1 and leaves as ISO 8601", func(t *testing.T) {
		t.Parallel()
		when := time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC)
		body, err := emulator.CBOREncodeForTest(emulator.CBORMapForTest(
			emulator.CBORPairForTest{Key: "StartTime", Value: when},
		))
		require.NoError(t, err)

		got, err := emulator.CWNormalizeInputForTest(emulator.WireRPCV2CBOR, body)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"StartTime": "2024-01-01T12:30:00.000Z"}, got)
	})

	t.Run("an indefinite-length structure is accepted", func(t *testing.T) {
		t.Parallel()
		// 0xbf … 0xff is the form the protocol's own test vectors use, and the one
		// substrate's writer never emits, so the reader has to take it.
		body := []byte{0xbf, 0x69, 'A', 'l', 'a', 'r', 'm', 'N', 'a', 'm', 'e', 0x61, 'x', 0xff}
		got, err := emulator.CWNormalizeInputForTest(emulator.WireRPCV2CBOR, body)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"AlarmName": "x"}, got)
	})
}

func TestCWNormalizeInput_Malformed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		protocol emulator.WireProtocol
		body     []byte
		wantIn   string
	}{
		{
			name:     "truncated CBOR",
			protocol: emulator.WireRPCV2CBOR,
			body:     []byte{0xa1, 0x69, 'A'},
			wantIn:   "not well-formed CBOR",
		},
		{
			name:     "a CBOR body that is not a map",
			protocol: emulator.WireRPCV2CBOR,
			body:     []byte{0x63, 'a', 'b', 'c'},
			wantIn:   "must be a CBOR map",
		},
		{
			name:     "malformed JSON",
			protocol: emulator.WireJSONRPC,
			body:     []byte(`{"AlarmName":`),
			wantIn:   "not well-formed JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := emulator.CWNormalizeInputForTest(tt.protocol, tt.body)
			require.Error(t, err)
			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			// Substrate's choice: neither the rpcv2Cbor specification nor CloudWatch's
			// model names a shape for an undeserializable body.
			assert.Equal(t, "SerializationException", awsErr.Code)
			assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
			assert.Contains(t, awsErr.Message, tt.wantIn)
		})
	}
}

func TestCWNormalizeInput_QueryIsUntouched(t *testing.T) {
	t.Parallel()
	// A query request has already been form-parsed, so a body that happens to look
	// like JSON must not be re-read: the protocol, not the body, decides.
	got, err := emulator.CWNormalizeInputForTest(emulator.WireQuery, []byte(`{"AlarmName":"x"}`))
	require.NoError(t, err)
	assert.Nil(t, got)
}

// --- Response rendering -----------------------------------------------------

// cwTestAlarm is the alarm the rendering tests render. Statistic and the two action
// lists are set so that the "present" branch of every conditional member is covered;
// AlarmDescription, StateReason and StateReasonData are left unset so the absent branch
// is too.
func cwTestAlarm() emulator.CWAlarm {
	return emulator.CWAlarm{
		AlarmName:          "cpu-high",
		AlarmARN:           "arn:aws:cloudwatch:us-east-1:000000000000:alarm:cpu-high",
		MetricName:         "CPUUtilization",
		Namespace:          "AWS/EC2",
		Statistic:          "Average",
		ComparisonOperator: "GreaterThanThreshold",
		Threshold:          80,
		EvaluationPeriods:  2,
		Period:             60,
		StateValue:         "INSUFFICIENT_DATA",
		ActionsEnabled:     true,
		AlarmActions:       []string{"arn:aws:sns:us-east-1:000000000000:alerts"},
	}
}

func TestCWRender_QueryXML(t *testing.T) {
	t.Parallel()

	status, body, headers, err := emulator.CWRespondForTest(
		emulator.WireQuery, "DescribeAlarms", "req-1", []emulator.CWAlarm{cwTestAlarm()})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "text/xml; charset=UTF-8", headers["Content-Type"])

	// Asserted whole rather than by substring: the element names, the wrapper, the
	// member order and the number formats are all part of what a query client parses,
	// and this is the byte sequence substrate served before #785 for the members it
	// already had.
	assert.Equal(t, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+
		`<DescribeAlarmsResponse xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/">`+
		`<DescribeAlarmsResult><MetricAlarms><member>`+
		`<AlarmName>cpu-high</AlarmName>`+
		`<AlarmArn>arn:aws:cloudwatch:us-east-1:000000000000:alarm:cpu-high</AlarmArn>`+
		`<MetricName>CPUUtilization</MetricName>`+
		`<Namespace>AWS/EC2</Namespace>`+
		`<Statistic>Average</Statistic>`+
		`<ComparisonOperator>GreaterThanThreshold</ComparisonOperator>`+
		`<Threshold>80</Threshold>`+
		`<EvaluationPeriods>2</EvaluationPeriods>`+
		`<Period>60</Period>`+
		`<StateValue>INSUFFICIENT_DATA</StateValue>`+
		`<ActionsEnabled>true</ActionsEnabled>`+
		`<AlarmActions><member>arn:aws:sns:us-east-1:000000000000:alerts</member></AlarmActions>`+
		`</member></MetricAlarms></DescribeAlarmsResult>`+
		`<ResponseMetadata><RequestId>req-1</RequestId></ResponseMetadata>`+
		`</DescribeAlarmsResponse>`, string(body))

	// AlarmDescription, StateReason, StateReasonData, OKActions and
	// InsufficientDataActions were unset, so they are absent — not empty elements.
	for _, absent := range []string{
		"AlarmDescription", "StateReason", "StateReasonData", "OKActions", "InsufficientDataActions",
	} {
		assert.NotContains(t, string(body), absent)
	}
}

func TestCWRender_QueryXMLEscapesText(t *testing.T) {
	t.Parallel()
	alarm := cwTestAlarm()
	alarm.AlarmName = `a<b&c"d`
	_, body, _, err := emulator.CWRespondForTest(
		emulator.WireQuery, "DescribeAlarms", "req-1", []emulator.CWAlarm{alarm})
	require.NoError(t, err)
	assert.Contains(t, string(body), `<AlarmName>a&lt;b&amp;c&#34;d</AlarmName>`)
}

func TestCWRender_JSON(t *testing.T) {
	t.Parallel()

	status, body, headers, err := emulator.CWRespondForTest(
		emulator.WireJSONRPC, "DescribeAlarms", "req-1", []emulator.CWAlarm{cwTestAlarm()})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	// The protocol version is in the media type, which is how botocore picks its parser.
	assert.Equal(t, "application/x-amz-json-1.0", headers["Content-Type"])

	// Byte-exact, because member order is the property the hand-written writer exists to
	// guarantee: json.Marshal over a map would reorder this on every run.
	assert.JSONEq(t, `{"MetricAlarms":[{`+
		`"AlarmName":"cpu-high",`+
		`"AlarmArn":"arn:aws:cloudwatch:us-east-1:000000000000:alarm:cpu-high",`+
		`"MetricName":"CPUUtilization","Namespace":"AWS/EC2","Statistic":"Average",`+
		`"ComparisonOperator":"GreaterThanThreshold","Threshold":80,`+
		`"EvaluationPeriods":2,"Period":60,"StateValue":"INSUFFICIENT_DATA",`+
		`"ActionsEnabled":true,`+
		`"AlarmActions":["arn:aws:sns:us-east-1:000000000000:alerts"]}]}`, string(body))
	assert.True(t, strings.HasPrefix(string(body), `{"MetricAlarms":[{"AlarmName":`),
		"members must be emitted in the document's order, not a map's")

	// No XML wrapper leaks into the JSON rendering: the query protocol's
	// ResponseMetadata has no counterpart in awsJson1_0.
	assert.NotContains(t, string(body), "ResponseMetadata")
	assert.NotContains(t, string(body), "req-1")
}

func TestCWRender_CBOR(t *testing.T) {
	t.Parallel()

	status, body, headers, err := emulator.CWRespondForTest(
		emulator.WireRPCV2CBOR, "DescribeAlarms", "req-1", []emulator.CWAlarm{cwTestAlarm()})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "application/cbor", headers["Content-Type"])
	// Required on every RPC v2 CBOR response, and a client must reject a mismatch.
	assert.Equal(t, "rpc-v2-cbor", headers["Smithy-Protocol"])

	decoded, err := emulator.CBORDecodeForTest(body)
	require.NoError(t, err)
	doc, ok := decoded.(map[string]any)
	require.True(t, ok, "a Smithy structure encodes as a CBOR map, got %T", decoded)

	alarms, ok := doc["MetricAlarms"].([]any)
	require.True(t, ok, "MetricAlarms is a list, got %T", doc["MetricAlarms"])
	require.Len(t, alarms, 1)
	alarm, ok := alarms[0].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "cpu-high", alarm["AlarmName"])
	assert.Equal(t, "GreaterThanThreshold", alarm["ComparisonOperator"])
	assert.Equal(t, true, alarm["ActionsEnabled"])
	// Modeled as a double, so encoded as one: 0xfb, not an integer, even though the
	// value is whole. A typed client reads it into a *float64.
	assert.Equal(t, 80.0, alarm["Threshold"])
	// Modeled as integers, so encoded as major 0 — the distinction the old hardcoded
	// empty map could not express at all.
	assert.Equal(t, int64(2), alarm["EvaluationPeriods"])
	assert.Equal(t, int64(60), alarm["Period"])
	assert.Equal(t, []any{"arn:aws:sns:us-east-1:000000000000:alerts"}, alarm["AlarmActions"])

	// Unset optional members are omitted, not present as null: RPC v2 CBOR reserves
	// null for a @sparse collection's element.
	for _, absent := range []string{"AlarmDescription", "StateReason", "OKActions"} {
		_, present := alarm[absent]
		assert.False(t, present, "%s must be omitted when unset", absent)
	}
}

func TestCWRender_EmptyListIsPresentNotAbsent(t *testing.T) {
	t.Parallel()

	// DescribeAlarms with no matches: the member is there and empty. A typed client
	// distinguishes that from an omitted member, and CloudWatch itself answers <.../>.
	_, xmlBody, _, err := emulator.CWRespondForTest(
		emulator.WireQuery, "DescribeAlarms", "req-1", nil)
	require.NoError(t, err)
	assert.Contains(t, string(xmlBody), "<MetricAlarms></MetricAlarms>")

	_, jsonBody, _, err := emulator.CWRespondForTest(
		emulator.WireJSONRPC, "DescribeAlarms", "req-1", nil)
	require.NoError(t, err)
	assert.Equal(t, `{"MetricAlarms":[]}`, string(jsonBody))

	_, cborBody, _, err := emulator.CWRespondForTest(
		emulator.WireRPCV2CBOR, "DescribeAlarms", "req-1", nil)
	require.NoError(t, err)
	// a1 (map, 1 pair) 6c "MetricAlarms" 80 (array, 0 elements).
	assert.Equal(t, append([]byte{0xa1, 0x6c}, append([]byte("MetricAlarms"), 0x80)...), cborBody)
}

func TestCWRender_UnitOutput(t *testing.T) {
	t.Parallel()

	t.Run("CBOR sends no body and must not set Content-Type", func(t *testing.T) {
		t.Parallel()
		status, body, headers, err := emulator.CWUnitResponseForTest(
			emulator.WireRPCV2CBOR, "PutMetricData", "req-1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, status)
		assert.Empty(t, body)
		// smithy-protocol-tests/model/rpcv2Cbor/empty-input-output.smithy, test
		// `no_output`: headers are {smithy-protocol: rpc-v2-cbor}, the body is empty,
		// and forbidHeaders lists Content-Type. The 0xa0 substrate used to send is
		// merely tolerated by NoOutputClientAllowsEmptyCbor, not conformant.
		assert.Equal(t, "rpc-v2-cbor", headers[http.CanonicalHeaderKey("Smithy-Protocol")])
		_, present := headers["Content-Type"]
		assert.False(t, present, "Content-Type is forbidden on a Unit-output response")
	})

	t.Run("JSON sends an empty object", func(t *testing.T) {
		t.Parallel()
		_, body, headers, err := emulator.CWUnitResponseForTest(
			emulator.WireJSONRPC, "PutMetricData", "req-1")
		require.NoError(t, err)
		// Not an empty body: awsJson1_0 has no forbidden-Content-Type rule, and
		// botocore reads a zero-length JSON body as a parse failure.
		assert.Equal(t, "{}", string(body))
		assert.Equal(t, "application/x-amz-json-1.0", headers["Content-Type"])
	})

	t.Run("Query sends a response wrapper with no result element", func(t *testing.T) {
		t.Parallel()
		_, body, _, err := emulator.CWUnitResponseForTest(
			emulator.WireQuery, "PutMetricData", "req-1")
		require.NoError(t, err)
		assert.Equal(t, `<?xml version="1.0" encoding="UTF-8"?>`+"\n"+
			`<PutMetricDataResponse xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/">`+
			`<ResponseMetadata><RequestId>req-1</RequestId></ResponseMetadata>`+
			`</PutMetricDataResponse>`, string(body))
		assert.NotContains(t, string(body), "PutMetricDataResult")
	})
}

func TestCWRender_ScalarText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
		want  string
	}{
		// 'g' with the shortest round-tripping precision is what encoding/xml itself
		// used, so a Query response substrate already served does not change.
		{name: "a whole double keeps no fraction", value: 80.0, want: "80"},
		{name: "a fractional double round-trips", value: 0.1, want: "0.1"},
		{name: "an integer", value: 60, want: "60"},
		{name: "a boolean", value: true, want: "true"},
		// The query protocol's own timestamp spelling: ISO 8601, three fractional
		// digits, UTC. The JSON and CBOR renderings use epoch seconds instead.
		{
			name:  "a timestamp is ISO 8601 at millisecond resolution",
			value: time.Date(2024, 1, 1, 0, 0, 0, 123000000, time.UTC),
			want:  "2024-01-01T00:00:00.123Z",
		},
		{
			name:  "a timestamp is converted to UTC",
			value: time.Date(2024, 1, 1, 0, 0, 0, 0, time.FixedZone("x", 3600)),
			want:  "2023-12-31T23:00:00.000Z",
		},
		{name: "a blob is base64", value: []byte{0x01, 0x02}, want: "AQI="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := emulator.CWXMLTextForTest(tt.value)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("a type no CloudWatch member is modeled as is refused", func(t *testing.T) {
		t.Parallel()
		_, err := emulator.CWXMLTextForTest(struct{}{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot render")

		_, err = emulator.CWParamTextForTest(struct{}{})
		require.Error(t, err)
	})
}

// --- End to end, one test per real client -----------------------------------

// cwReadBytes reads a response body, which the CBOR assertions need unmodified rather
// than as the string [cwReadBody] returns.
func cwReadBytes(t *testing.T, r *http.Response) []byte {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	return body
}

// cwPost drives the emulator over HTTP the way a given client would, returning the
// response for inspection.
func cwPost(t *testing.T, srv http.Handler, path, contentType, body string, headers map[string]string) *http.Response {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	r.Host = "localhost:4566"
	r.Header.Set("Content-Type", contentType)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/"+
		"us-east-1/monitoring/aws4_request, SignedHeaders=host, Signature=fake")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

// TestCW_EndToEnd_RPCV2CBOR drives PutMetricData then ListMetrics the way
// aws-sdk-go-v2's cloudwatch client does: a POST to
// /service/{serviceShape}/operation/{Op} with a CBOR body, no X-Amz-Target, and
// X-Amzn-Query-Mode set because the service is awsQueryCompatible.
//
// Before #785 the first call answered XML and the SDK reported "deserialization failed,
// unexpected minor value 28" — the '<' of "<PutMetricDataResponse" read as a CBOR head.
func TestCW_EndToEnd_RPCV2CBOR(t *testing.T) {
	t.Parallel()
	srv := newCWAlarmTestServer(t)

	put, err := emulator.CBOREncodeForTest(emulator.CBORMapForTest(
		emulator.CBORPairForTest{Key: "Namespace", Value: "CargoShip/CBOR"},
		emulator.CBORPairForTest{Key: "MetricData", Value: []any{
			emulator.CBORMapForTest(
				emulator.CBORPairForTest{Key: "MetricName", Value: "RequestCount"},
				emulator.CBORPairForTest{Key: "Value", Value: 42.0},
				emulator.CBORPairForTest{Key: "Unit", Value: "Count"},
			),
		}},
	))
	require.NoError(t, err)

	cborHeaders := map[string]string{
		"Smithy-Protocol":   "rpc-v2-cbor",
		"Accept":            "application/cbor",
		"X-Amzn-Query-Mode": "true",
	}

	resp := cwPost(t, srv,
		"/service/GraniteServiceVersion20100801/operation/PutMetricData",
		"application/cbor", string(put), cborHeaders)
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "rpc-v2-cbor", resp.Header.Get("Smithy-Protocol"))
	// PutMetricData's output is Unit: no body, and Content-Type forbidden.
	assert.Empty(t, resp.Header.Get("Content-Type"))
	putBody := cwReadBytes(t, resp)
	assert.Empty(t, putBody)

	list := cwPost(t, srv,
		"/service/GraniteServiceVersion20100801/operation/ListMetrics",
		"application/cbor", "\xa0", cborHeaders)
	defer list.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, list.StatusCode)
	assert.Equal(t, "application/cbor", list.Header.Get("Content-Type"))

	decoded, err := emulator.CBORDecodeForTest(cwReadBytes(t, list))
	require.NoError(t, err)
	doc, ok := decoded.(map[string]any)
	require.True(t, ok, "got %T", decoded)
	metrics, ok := doc["Metrics"].([]any)
	require.True(t, ok, "Metrics is a list, got %T", doc["Metrics"])
	require.Len(t, metrics, 1, "the metric published over CBOR must be listed over CBOR")
	metric, ok := metrics[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "RequestCount", metric["MetricName"])
	assert.Equal(t, "CargoShip/CBOR", metric["Namespace"])
	assert.Equal(t, []any{}, metric["Dimensions"])
}

// TestCW_EndToEnd_JSONRPC drives the same journey the way the AWS CLI and boto3 do:
// X-Amz-Target with the service shape's name and an application/x-amz-json-1.0 body.
//
// Before #785 both calls answered XML with HTTP 200, and the CLI printed nothing at all.
func TestCW_EndToEnd_JSONRPC(t *testing.T) {
	t.Parallel()
	srv := newCWAlarmTestServer(t)

	target := func(op string) map[string]string {
		return map[string]string{"X-Amz-Target": "GraniteServiceVersion20100801." + op}
	}

	resp := cwPost(t, srv, "/", "application/x-amz-json-1.0",
		`{"Namespace":"CargoShip/JSON","MetricData":[{"MetricName":"ErrorRate","Value":0.1}]}`,
		target("PutMetricData"))
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.0", resp.Header.Get("Content-Type"))
	assert.Equal(t, "{}", string(cwReadBytes(t, resp)))

	list := cwPost(t, srv, "/", "application/x-amz-json-1.0",
		`{"Namespace":"CargoShip/JSON"}`, target("ListMetrics"))
	defer list.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, list.StatusCode)

	var out struct {
		Metrics []struct {
			MetricName string   `json:"MetricName"`
			Namespace  string   `json:"Namespace"`
			Dimensions []string `json:"Dimensions"`
		} `json:"Metrics"`
	}
	require.NoError(t, json.Unmarshal(cwReadBytes(t, list), &out))
	require.Len(t, out.Metrics, 1)
	assert.Equal(t, "ErrorRate", out.Metrics[0].MetricName)
	assert.Equal(t, "CargoShip/JSON", out.Metrics[0].Namespace)
	assert.Empty(t, out.Metrics[0].Dimensions)
}

// TestCW_EndToEnd_AlarmsAcrossProtocols writes an alarm over CBOR and reads it back over
// each of the three protocols, which is the property that makes the neutral document
// worth having: one stored alarm, three renderings, no divergence.
func TestCW_EndToEnd_AlarmsAcrossProtocols(t *testing.T) {
	t.Parallel()
	srv := newCWAlarmTestServer(t)

	put, err := emulator.CBOREncodeForTest(emulator.CBORMapForTest(
		emulator.CBORPairForTest{Key: "AlarmName", Value: "cpu-high"},
		emulator.CBORPairForTest{Key: "MetricName", Value: "CPUUtilization"},
		emulator.CBORPairForTest{Key: "Namespace", Value: "AWS/EC2"},
		emulator.CBORPairForTest{Key: "Statistic", Value: "Average"},
		emulator.CBORPairForTest{Key: "ComparisonOperator", Value: "GreaterThanThreshold"},
		emulator.CBORPairForTest{Key: "Threshold", Value: 80.0},
		emulator.CBORPairForTest{Key: "EvaluationPeriods", Value: 2},
		emulator.CBORPairForTest{Key: "Period", Value: 60},
		emulator.CBORPairForTest{Key: "AlarmActions", Value: []string{
			"arn:aws:sns:us-east-1:000000000000:alerts",
		}},
	))
	require.NoError(t, err)

	created := cwPost(t, srv,
		"/service/GraniteServiceVersion20100801/operation/PutMetricAlarm",
		"application/cbor", string(put),
		map[string]string{"Smithy-Protocol": "rpc-v2-cbor"})
	defer created.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, created.StatusCode)

	t.Run("read back over CBOR", func(t *testing.T) {
		resp := cwPost(t, srv,
			"/service/GraniteServiceVersion20100801/operation/DescribeAlarms",
			"application/cbor", "\xa0",
			map[string]string{"Smithy-Protocol": "rpc-v2-cbor"})
		defer resp.Body.Close() //nolint:errcheck
		decoded, err := emulator.CBORDecodeForTest(cwReadBytes(t, resp))
		require.NoError(t, err)
		alarms := decoded.(map[string]any)["MetricAlarms"].([]any) //nolint:errcheck,forcetypeassert
		require.Len(t, alarms, 1)
		alarm := alarms[0].(map[string]any) //nolint:errcheck,forcetypeassert
		assert.Equal(t, "cpu-high", alarm["AlarmName"])
		// The flattening preserved a double through the parameter map and the handler
		// parsed it back, so the alarm's threshold survived the round trip.
		assert.Equal(t, 80.0, alarm["Threshold"])
		assert.Equal(t, int64(2), alarm["EvaluationPeriods"])
		assert.Equal(t, int64(60), alarm["Period"])
		assert.Equal(t, "INSUFFICIENT_DATA", alarm["StateValue"])
		assert.Equal(t, []any{"arn:aws:sns:us-east-1:000000000000:alerts"}, alarm["AlarmActions"])
	})

	t.Run("read back over JSON", func(t *testing.T) {
		resp := cwPost(t, srv, "/", "application/x-amz-json-1.0", `{}`,
			map[string]string{"X-Amz-Target": "GraniteServiceVersion20100801.DescribeAlarms"})
		defer resp.Body.Close() //nolint:errcheck
		var out struct {
			MetricAlarms []struct {
				AlarmName string  `json:"AlarmName"`
				Threshold float64 `json:"Threshold"`
				Period    int     `json:"Period"`
			} `json:"MetricAlarms"`
		}
		require.NoError(t, json.Unmarshal(cwReadBytes(t, resp), &out))
		require.Len(t, out.MetricAlarms, 1)
		assert.Equal(t, "cpu-high", out.MetricAlarms[0].AlarmName)
		assert.Equal(t, 80.0, out.MetricAlarms[0].Threshold)
		assert.Equal(t, 60, out.MetricAlarms[0].Period)
	})

	t.Run("read back over Query", func(t *testing.T) {
		resp := cwRequest(t, srv, map[string]string{"Action": "DescribeAlarms"})
		defer resp.Body.Close() //nolint:errcheck
		body := string(cwReadBytes(t, resp))
		assert.Equal(t, "text/xml; charset=UTF-8", resp.Header.Get("Content-Type"))
		assert.Contains(t, body, "<AlarmName>cpu-high</AlarmName>")
		assert.Contains(t, body, "<Threshold>80</Threshold>")
		assert.Contains(t, body, "<Period>60</Period>")
	})
}

// TestCW_EndToEnd_DescribeAlarmsForMetricWrapper pins the operation's own element names.
// Substrate previously answered a DescribeAlarmsResponse/DescribeAlarmsResult wrapper
// here, borrowed from DescribeAlarms, which the query protocol does not permit.
func TestCW_EndToEnd_DescribeAlarmsForMetricWrapper(t *testing.T) {
	t.Parallel()
	srv := newCWAlarmTestServer(t)

	resp := cwRequest(t, srv, map[string]string{
		"Action":     "DescribeAlarmsForMetric",
		"MetricName": "CPUUtilization",
		"Namespace":  "AWS/EC2",
	})
	defer resp.Body.Close() //nolint:errcheck
	body := string(cwReadBytes(t, resp))
	assert.Contains(t, body, "<DescribeAlarmsForMetricResponse")
	assert.Contains(t, body, "<DescribeAlarmsForMetricResult>")
	assert.NotContains(t, body, "<DescribeAlarmsResult>")
	// The output shape has one member and does not paginate.
	assert.NotContains(t, body, "NextToken")
}

// TestCW_EndToEnd_MalformedCBORIsRefused checks that a body substrate cannot deserialize
// produces a shaped error rather than an operation acting on no parameters at all.
func TestCW_EndToEnd_MalformedCBORIsRefused(t *testing.T) {
	t.Parallel()
	srv := newCWAlarmTestServer(t)

	resp := cwPost(t, srv,
		"/service/GraniteServiceVersion20100801/operation/PutMetricAlarm",
		"application/cbor", "\xa1\x69A",
		map[string]string{"Smithy-Protocol": "rpc-v2-cbor"})
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	// The error follows the caller's protocol too (#757), so it comes back as CBOR.
	assert.Equal(t, "application/cbor", resp.Header.Get("Content-Type"))
	decoded, err := emulator.CBORDecodeForTest(cwReadBytes(t, resp))
	require.NoError(t, err)
	doc, ok := decoded.(map[string]any)
	require.True(t, ok, "got %T", decoded)
	assert.Equal(t, "SerializationException", doc["__type"])
}
