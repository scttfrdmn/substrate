package e2e_test

import (
	"context"
	"errors"
	"net/http"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// cwExchange is one request/response pair as it went over the wire.
type cwExchange struct {
	// Operation is the last path segment of a Smithy RPC v2 CBOR request URI.
	Operation string

	// RequestProtocol is the request's Smithy-Protocol header, so a journey can prove
	// which wire form it actually exercised rather than assuming.
	RequestProtocol string

	// Status, ResponseContentType, ContentLength and ResponseProtocol are what came
	// back. ContentLength is -1 when the server did not state one.
	Status              int
	ResponseContentType string
	ContentLength       int64
	ResponseProtocol    string
}

// cwRecorder is an http.RoundTripper that records each exchange.
//
// The journey needs the raw headers, not just the deserialized output: the
// smithy.api#Unit rule is that such a response carries no body and MUST NOT set
// Content-Type, and a typed client cannot report the difference between a conformant
// empty response and a one-byte CBOR map it tolerates.
type cwRecorder struct {
	next http.RoundTripper

	mu        sync.Mutex
	exchanges []cwExchange
}

func (r *cwRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.next.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	r.mu.Lock()
	r.exchanges = append(r.exchanges, cwExchange{
		// A Smithy RPC v2 CBOR request URI ends in /operation/{OperationName}.
		Operation:           path.Base(req.URL.Path),
		RequestProtocol:     req.Header.Get("Smithy-Protocol"),
		Status:              resp.StatusCode,
		ResponseContentType: resp.Header.Get("Content-Type"),
		ContentLength:       resp.ContentLength,
		ResponseProtocol:    resp.Header.Get("Smithy-Protocol"),
	})
	r.mu.Unlock()
	return resp, nil
}

// find returns the recorded exchange for an operation, or false when the operation
// never went over the wire.
func (r *cwRecorder) find(operation string) (cwExchange, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.exchanges {
		if e.Operation == operation {
			return e, true
		}
	}
	return cwExchange{}, false
}

// TestJourney_CloudWatchProtocols is #785 at the SDK level: the whole CloudWatch API
// driven by the real aws-sdk-go-v2 client, which speaks Smithy RPC v2 CBOR.
//
// This is the tier whose absence let the defect ship. Every CloudWatch unit test posts
// a form-encoded body and reads the XML back as a string, so all of them stayed green
// while the plugin answered `text/xml` to a client that had asked for
// `application/cbor` — the SDK reported "deserialization failed, expected map for
// struct, got major type 1" (0x3C, the leading `<`, read as a CBOR major type) and the
// AWS CLI printed nothing at all. Nothing short of a real client catches that.
//
// The journey therefore asserts three things a string-matching test cannot:
//
//   - the request genuinely went out as rpc-v2-cbor, so the coverage is real;
//   - the six smithy.api#Unit operations answer with no body and no Content-Type, per
//     the `no_output` protocol test, which lists Content-Type in its forbidHeaders;
//   - members arrive with their modeled types — Threshold a double, EvaluationPeriods
//     and Period 32-bit integers — because a codec that writes every number one way
//     round-trips against itself and still fails against a generated deserializer.
func TestJourney_CloudWatchProtocols(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	rec := &cwRecorder{next: http.DefaultTransport}
	cfg.HTTPClient = &http.Client{Transport: rec}

	ctx := context.Background()
	// Retries off, so every assertion is about the first response rather than whatever
	// the retry loop settled on.
	cw := cloudwatch.NewFromConfig(cfg, func(o *cloudwatch.Options) { o.RetryMaxAttempts = 1 })

	// --- a Unit output: PutMetricData ---
	if _, err := cw.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String("Substrate/Journey"),
		MetricData: []cwtypes.MetricDatum{{
			MetricName: aws.String("Requests"),
			Value:      aws.Float64(3),
			Unit:       cwtypes.StandardUnitCount,
			// A timestamp exercises tag 1 on the request side: the SDK writes epoch
			// seconds, and the plugin has to read it back without a @timestampFormat.
			Timestamp: aws.Time(time.Unix(1_700_000_000, 0)),
		}},
	}); err != nil {
		t.Fatalf("PutMetricData: %v — a CBOR request body the plugin cannot read is #785 exactly", err)
	}

	put, ok := rec.find("PutMetricData")
	if !ok {
		t.Fatal("PutMetricData never reached the wire")
	}
	if put.RequestProtocol != "rpc-v2-cbor" {
		t.Fatalf("PutMetricData went out as Smithy-Protocol %q; this journey only proves something if the SDK speaks CBOR",
			put.RequestProtocol)
	}
	if put.ResponseContentType != "" {
		t.Errorf("PutMetricData answered Content-Type %q; a Unit output MUST NOT set one (`no_output` forbidHeaders)",
			put.ResponseContentType)
	}
	if put.ContentLength != 0 {
		t.Errorf("PutMetricData answered a %d-byte body; a Unit output carries none", put.ContentLength)
	}
	if put.ResponseProtocol != "rpc-v2-cbor" {
		t.Errorf("PutMetricData answered Smithy-Protocol %q; every rpc-v2-cbor response carries the header, body or not",
			put.ResponseProtocol)
	}

	// --- a modeled output: ListMetrics ---
	metrics, err := cw.ListMetrics(ctx, &cloudwatch.ListMetricsInput{
		Namespace: aws.String("Substrate/Journey"),
	})
	if err != nil {
		t.Fatalf("ListMetrics: %v", err)
	}
	if len(metrics.Metrics) != 1 {
		t.Fatalf("ListMetrics returned %d metrics, want the one PutMetricData published", len(metrics.Metrics))
	}
	got := metrics.Metrics[0]
	if aws.ToString(got.MetricName) != "Requests" || aws.ToString(got.Namespace) != "Substrate/Journey" {
		t.Errorf("ListMetrics returned %s/%s, want Substrate/Journey/Requests",
			aws.ToString(got.Namespace), aws.ToString(got.MetricName))
	}
	// Present-and-empty, not absent: substrate records a metric by name and namespace,
	// and a caller that ranges over Dimensions must see a zero-length slice rather than
	// have to tell nil from empty.
	if got.Dimensions == nil {
		t.Error("ListMetrics omitted Dimensions; it is present and empty for a metric published without any")
	}
	if len(got.Dimensions) != 0 {
		t.Errorf("ListMetrics returned %d dimensions, want none", len(got.Dimensions))
	}

	// --- alarms: the members that have to keep their modeled types ---
	if _, err := cw.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{
		AlarmName:          aws.String("journey-alarm"),
		AlarmDescription:   aws.String("set by the CloudWatch journey"),
		MetricName:         aws.String("Requests"),
		Namespace:          aws.String("Substrate/Journey"),
		Statistic:          cwtypes.StatisticAverage,
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		// A fractional threshold: a codec that wrote every number as an integer would
		// deserialize this as 80 and no round-trip test would notice.
		Threshold:         aws.Float64(80.5),
		EvaluationPeriods: aws.Int32(2),
		Period:            aws.Int32(300),
		AlarmActions:      []string{"arn:aws:sns:us-east-1:123456789012:journey"},
	}); err != nil {
		t.Fatalf("PutMetricAlarm: %v", err)
	}

	alarm := journeyOneAlarm(t, cw)
	if v := aws.ToFloat64(alarm.Threshold); v != 80.5 {
		t.Errorf("Threshold reads %v, want 80.5 — it is a double, not an integer", v)
	}
	if n := aws.ToInt32(alarm.EvaluationPeriods); n != 2 {
		t.Errorf("EvaluationPeriods reads %d, want 2", n)
	}
	if n := aws.ToInt32(alarm.Period); n != 300 {
		t.Errorf("Period reads %d, want 300", n)
	}
	if !aws.ToBool(alarm.ActionsEnabled) {
		t.Error("ActionsEnabled reads false; the alarm was created with actions enabled")
	}
	if alarm.Statistic != cwtypes.StatisticAverage {
		t.Errorf("Statistic reads %q, want Average", alarm.Statistic)
	}
	if alarm.StateValue != cwtypes.StateValueInsufficientData {
		t.Errorf("a new alarm reads StateValue %q, want INSUFFICIENT_DATA", alarm.StateValue)
	}
	if d := aws.ToString(alarm.AlarmDescription); d != "set by the CloudWatch journey" {
		t.Errorf("AlarmDescription reads %q", d)
	}
	if len(alarm.AlarmActions) != 1 || alarm.AlarmActions[0] != "arn:aws:sns:us-east-1:123456789012:journey" {
		t.Errorf("AlarmActions reads %v, want the one ARN it was created with", alarm.AlarmActions)
	}
	// An unset list is absent rather than empty, which is how a caller can tell "no OK
	// actions were configured" from "an empty list was configured".
	if alarm.OKActions != nil {
		t.Errorf("OKActions reads %v; none were set, so the member is absent", alarm.OKActions)
	}

	// --- state, including StateReasonData, which was stored but never rendered ---
	if _, err := cw.SetAlarmState(ctx, &cloudwatch.SetAlarmStateInput{
		AlarmName:       aws.String("journey-alarm"),
		StateValue:      cwtypes.StateValueAlarm,
		StateReason:     aws.String("the journey said so"),
		StateReasonData: aws.String(`{"journey":true}`),
	}); err != nil {
		t.Fatalf("SetAlarmState: %v", err)
	}
	if set, found := rec.find("SetAlarmState"); !found {
		t.Error("SetAlarmState never reached the wire")
	} else if set.ResponseContentType != "" || set.ContentLength != 0 {
		t.Errorf("SetAlarmState answered Content-Type %q and %d bytes; it is a Unit output",
			set.ResponseContentType, set.ContentLength)
	}

	alarm = journeyOneAlarm(t, cw)
	if alarm.StateValue != cwtypes.StateValueAlarm {
		t.Errorf("StateValue reads %q after SetAlarmState, want ALARM", alarm.StateValue)
	}
	if r := aws.ToString(alarm.StateReason); r != "the journey said so" {
		t.Errorf("StateReason reads %q", r)
	}
	if d := aws.ToString(alarm.StateReasonData); d != `{"journey":true}` {
		t.Errorf("StateReasonData reads %q; it was stored but never returned before #785", d)
	}

	// --- DescribeAlarmsForMetric: its own output shape, not DescribeAlarms' ---
	//
	// The operation used to answer a DescribeAlarmsResponse/DescribeAlarmsResult wrapper
	// borrowed from DescribeAlarms. Over CBOR the wrapper does not exist, so what this
	// asserts is the narrower one: the output has MetricAlarms and no NextToken, and a
	// generated deserializer reads it.
	forMetric, err := cw.DescribeAlarmsForMetric(ctx, &cloudwatch.DescribeAlarmsForMetricInput{
		MetricName: aws.String("Requests"),
		Namespace:  aws.String("Substrate/Journey"),
	})
	if err != nil {
		t.Fatalf("DescribeAlarmsForMetric: %v", err)
	}
	if len(forMetric.MetricAlarms) != 1 {
		t.Fatalf("DescribeAlarmsForMetric returned %d alarms, want 1", len(forMetric.MetricAlarms))
	}
	if n := aws.ToString(forMetric.MetricAlarms[0].AlarmName); n != "journey-alarm" {
		t.Errorf("DescribeAlarmsForMetric returned alarm %q", n)
	}
	// A metric with no alarm answers an empty list, not the whole set.
	none, err := cw.DescribeAlarmsForMetric(ctx, &cloudwatch.DescribeAlarmsForMetricInput{
		MetricName: aws.String("NoSuchMetric"),
		Namespace:  aws.String("Substrate/Journey"),
	})
	if err != nil {
		t.Fatalf("DescribeAlarmsForMetric(NoSuchMetric): %v", err)
	}
	if len(none.MetricAlarms) != 0 {
		t.Errorf("DescribeAlarmsForMetric(NoSuchMetric) returned %d alarms; the metric filter is not applied",
			len(none.MetricAlarms))
	}

	// --- GetMetricData: no data points, and that is a modeled answer ---
	//
	// Substrate records a metric's identity, not its time series — running the workload
	// behind the API is out of scope — so the honest answer is an empty result list, and
	// a caller must be able to tell that from "the operation returned nothing". Both
	// lists are present and empty; before #785 this was a hardcoded one-byte CBOR map
	// that said nothing about either member.
	data, err := cw.GetMetricData(ctx, &cloudwatch.GetMetricDataInput{
		StartTime: aws.Time(time.Unix(1_700_000_000, 0)),
		EndTime:   aws.Time(time.Unix(1_700_003_600, 0)),
		MetricDataQueries: []cwtypes.MetricDataQuery{{
			Id: aws.String("journey0"),
			MetricStat: &cwtypes.MetricStat{
				Metric: &cwtypes.Metric{
					Namespace:  aws.String("Substrate/Journey"),
					MetricName: aws.String("Requests"),
				},
				Period: aws.Int32(300),
				Stat:   aws.String("Average"),
			},
		}},
	})
	if err != nil {
		t.Fatalf("GetMetricData: %v", err)
	}
	if data.MetricDataResults == nil {
		t.Error("GetMetricData omitted MetricDataResults; it is present and empty")
	}
	if len(data.MetricDataResults) != 0 {
		t.Errorf("GetMetricData returned %d results; substrate does not model a time series",
			len(data.MetricDataResults))
	}
	if data.Messages == nil {
		t.Error("GetMetricData omitted Messages; it is present and empty")
	}

	// --- the remaining Unit outputs, and the deletion they end at ---
	if _, err := cw.DisableAlarmActions(ctx, &cloudwatch.DisableAlarmActionsInput{
		AlarmNames: []string{"journey-alarm"},
	}); err != nil {
		t.Fatalf("DisableAlarmActions: %v", err)
	}
	if alarm = journeyOneAlarm(t, cw); aws.ToBool(alarm.ActionsEnabled) {
		t.Error("ActionsEnabled still reads true after DisableAlarmActions")
	}
	if _, err := cw.EnableAlarmActions(ctx, &cloudwatch.EnableAlarmActionsInput{
		AlarmNames: []string{"journey-alarm"},
	}); err != nil {
		t.Fatalf("EnableAlarmActions: %v", err)
	}
	if alarm = journeyOneAlarm(t, cw); !aws.ToBool(alarm.ActionsEnabled) {
		t.Error("ActionsEnabled still reads false after EnableAlarmActions")
	}
	if _, err := cw.DeleteAlarms(ctx, &cloudwatch.DeleteAlarmsInput{
		AlarmNames: []string{"journey-alarm"},
	}); err != nil {
		t.Fatalf("DeleteAlarms: %v", err)
	}

	// Every Unit operation this journey drove must have answered the same way.
	for _, op := range []string{
		"PutMetricData", "PutMetricAlarm", "SetAlarmState",
		"EnableAlarmActions", "DisableAlarmActions", "DeleteAlarms",
	} {
		e, found := rec.find(op)
		if !found {
			t.Errorf("%s never reached the wire", op)
			continue
		}
		if e.ResponseContentType != "" || e.ContentLength != 0 || e.Status != http.StatusOK {
			t.Errorf("%s answered %d with Content-Type %q and %d bytes; a Unit output is 200, no body, no Content-Type",
				op, e.Status, e.ResponseContentType, e.ContentLength)
		}
	}

	emptied, err := cw.DescribeAlarms(ctx, &cloudwatch.DescribeAlarmsInput{})
	if err != nil {
		t.Fatalf("DescribeAlarms after DeleteAlarms: %v", err)
	}
	// Present and empty: a caller polling until its alarm is gone reads a zero-length
	// list, not a nil it has to special-case.
	if emptied.MetricAlarms == nil {
		t.Error("DescribeAlarms omitted MetricAlarms once empty; the member is always present")
	}
	if len(emptied.MetricAlarms) != 0 {
		t.Errorf("DescribeAlarms returned %d alarms after DeleteAlarms", len(emptied.MetricAlarms))
	}

	// --- the refusal, through the typed exception a consumer branches on ---
	var notFound *cwtypes.ResourceNotFoundException
	_, err = cw.SetAlarmState(ctx, &cloudwatch.SetAlarmStateInput{
		AlarmName:   aws.String("journey-alarm"),
		StateValue:  cwtypes.StateValueOk,
		StateReason: aws.String("gone"),
	})
	if err == nil {
		t.Fatal("SetAlarmState on a deleted alarm succeeded")
	}
	if !errors.As(err, &notFound) {
		t.Fatalf("expected *ResourceNotFoundException, got %T: %v — the CBOR error's __type is the absolute shape ID",
			err, err)
	}
	if e, found := rec.find("SetAlarmState"); found && e.ResponseProtocol != "rpc-v2-cbor" {
		t.Errorf("the error response carried Smithy-Protocol %q", e.ResponseProtocol)
	}
}

// journeyOneAlarm reads the single alarm the journey maintains, failing if there is not
// exactly one. It keeps each assertion above about one member rather than about
// re-reading the list.
func journeyOneAlarm(t *testing.T, cw *cloudwatch.Client) cwtypes.MetricAlarm {
	t.Helper()
	out, err := cw.DescribeAlarms(context.Background(), &cloudwatch.DescribeAlarmsInput{
		AlarmNames: []string{"journey-alarm"},
	})
	if err != nil {
		t.Fatalf("DescribeAlarms: %v", err)
	}
	if len(out.MetricAlarms) != 1 {
		t.Fatalf("DescribeAlarms returned %d alarms, want the journey's 1", len(out.MetricAlarms))
	}
	return out.MetricAlarms[0]
}
