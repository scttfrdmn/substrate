package emulator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// CloudWatchPlugin emulates the Amazon CloudWatch Alarms and Metrics API. It
// handles PutMetricAlarm, DeleteAlarms, DescribeAlarms, DescribeAlarmsForMetric,
// SetAlarmState, EnableAlarmActions, DisableAlarmActions, PutMetricData,
// ListMetrics and GetMetricData.
//
// All three wire protocols CloudWatch's service shape declares are served —
// aws.protocols#awsQuery, aws.protocols#awsJson1_0 and
// smithy.protocols#rpcv2Cbor — so a hand-rolled query client, the AWS CLI and
// aws-sdk-go-v2 each get a response they can deserialize (#785). The handlers
// themselves are protocol-agnostic: cloudwatch_input.go normalizes the request
// into req.Params before dispatch, and cloudwatch_render.go renders the reply
// from one neutral document.
type CloudWatchPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "monitoring".
func (p *CloudWatchPlugin) Name() string { return "monitoring" }

// Initialize sets up the CloudWatchPlugin with the provided configuration.
func (p *CloudWatchPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for CloudWatchPlugin.
func (p *CloudWatchPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a CloudWatch request to the appropriate handler.
//
// A JSON or CBOR body is flattened into req.Params first, so that a handler reads one
// input representation whichever protocol the caller used.
func (p *CloudWatchPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if err := cwNormalizeInput(req); err != nil {
		return nil, err
	}
	switch req.Operation {
	case "PutMetricAlarm":
		return p.putMetricAlarm(ctx, req)
	case "DeleteAlarms":
		return p.deleteAlarms(ctx, req)
	case "DescribeAlarms":
		return p.describeAlarms(ctx, req)
	case "DescribeAlarmsForMetric":
		return p.describeAlarmsForMetric(ctx, req)
	case "SetAlarmState":
		return p.setAlarmState(ctx, req)
	case "EnableAlarmActions":
		return p.enableAlarmActions(ctx, req)
	case "DisableAlarmActions":
		return p.disableAlarmActions(ctx, req)
	case "GetMetricData":
		return p.getMetricData(ctx, req)
	case "PutMetricData":
		return p.putMetricData(ctx, req)
	case "ListMetrics":
		return p.listMetrics(ctx, req)
	default:
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// --- Alarm CRUD operations --------------------------------------------------

func (p *CloudWatchPlugin) putMetricAlarm(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["AlarmName"]
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "AlarmName is required", HTTPStatus: http.StatusBadRequest}
	}

	threshold, _ := strconv.ParseFloat(req.Params["Threshold"], 64)
	evalPeriods, _ := strconv.Atoi(req.Params["EvaluationPeriods"])
	period, _ := strconv.Atoi(req.Params["Period"])

	alarm := CWAlarm{
		AlarmName:               name,
		AlarmARN:                cwAlarmARN(ctx.Region, ctx.AccountID, name),
		AlarmDescription:        req.Params["AlarmDescription"],
		MetricName:              req.Params["MetricName"],
		Namespace:               req.Params["Namespace"],
		Statistic:               req.Params["Statistic"],
		ComparisonOperator:      req.Params["ComparisonOperator"],
		Threshold:               threshold,
		EvaluationPeriods:       evalPeriods,
		Period:                  period,
		StateValue:              "INSUFFICIENT_DATA",
		ActionsEnabled:          req.Params["ActionsEnabled"] != "false",
		AlarmActions:            parseMemberList(req.Params, "AlarmActions"),
		OKActions:               parseMemberList(req.Params, "OKActions"),
		InsufficientDataActions: parseMemberList(req.Params, "InsufficientDataActions"),
	}

	// Preserve existing state if the alarm already exists.
	goCtx := context.Background()
	existingData, _ := p.state.Get(goCtx, monitoringNamespace, cwAlarmStateKey(ctx.AccountID, ctx.Region, name))
	if existingData != nil {
		var existing CWAlarm
		if json.Unmarshal(existingData, &existing) == nil {
			alarm.StateValue = existing.StateValue
			alarm.StateReason = existing.StateReason
			alarm.StateReasonData = existing.StateReasonData
		}
	}

	data, err := json.Marshal(alarm)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch putMetricAlarm marshal: %w", err)
	}
	if err := p.state.Put(goCtx, monitoringNamespace, cwAlarmStateKey(ctx.AccountID, ctx.Region, name), data); err != nil {
		return nil, fmt.Errorf("cloudwatch putMetricAlarm state.Put: %w", err)
	}

	idxKey := cwAlarmNamesKey(ctx.AccountID, ctx.Region)
	updateStringIndex(goCtx, p.state, monitoringNamespace, idxKey, name)

	return cwUnitResponse(req, "PutMetricAlarm", ctx.RequestID)
}

func (p *CloudWatchPlugin) deleteAlarms(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	names := parseMemberList(req.Params, "AlarmNames")

	goCtx := context.Background()
	for _, name := range names {
		_ = p.state.Delete(goCtx, monitoringNamespace, cwAlarmStateKey(ctx.AccountID, ctx.Region, name))
		idxKey := cwAlarmNamesKey(ctx.AccountID, ctx.Region)
		removeFromStringIndex(goCtx, p.state, monitoringNamespace, idxKey, name)
	}

	return cwUnitResponse(req, "DeleteAlarms", ctx.RequestID)
}

func (p *CloudWatchPlugin) describeAlarms(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	filterNames := parseMemberList(req.Params, "AlarmNames")
	stateFilter := req.Params["StateValue"]
	maxRecords, _ := strconv.Atoi(req.Params["MaxRecords"])
	if maxRecords <= 0 {
		maxRecords = 100
	}
	nextToken := req.Params["NextToken"]

	goCtx := context.Background()
	idxKey := cwAlarmNamesKey(ctx.AccountID, ctx.Region)
	allNames, err := loadStringIndex(goCtx, p.state, monitoringNamespace, idxKey)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch describeAlarms loadIndex: %w", err)
	}

	// Filter by specific alarm names if provided.
	names := allNames
	if len(filterNames) > 0 {
		nameSet := make(map[string]bool, len(filterNames))
		for _, n := range filterNames {
			nameSet[n] = true
		}
		filtered := make([]string, 0, len(filterNames))
		for _, n := range allNames {
			if nameSet[n] {
				filtered = append(filtered, n)
			}
		}
		names = filtered
	}

	// Pagination.
	offset := 0
	if nextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(nextToken); decErr == nil {
			if n, atoiErr := strconv.Atoi(string(decoded)); atoiErr == nil && n > 0 {
				offset = n
			}
		}
	}
	if offset > len(names) {
		offset = len(names)
	}

	end := offset + maxRecords
	var outNextToken string
	if end < len(names) {
		outNextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	} else {
		end = len(names)
	}

	alarms := make([]CWAlarm, 0, end-offset)
	for _, name := range names[offset:end] {
		data, getErr := p.state.Get(goCtx, monitoringNamespace, cwAlarmStateKey(ctx.AccountID, ctx.Region, name))
		if getErr != nil || data == nil {
			continue
		}
		var alarm CWAlarm
		if unmarshalErr := json.Unmarshal(data, &alarm); unmarshalErr != nil {
			continue
		}
		if stateFilter != "" && alarm.StateValue != stateFilter {
			continue
		}
		alarms = append(alarms, alarm)
	}

	result := cwDoc{}.with("MetricAlarms", cwAlarmList(alarms)).withNonEmpty("NextToken", outNextToken)
	return cwRespond(req, "DescribeAlarms", ctx.RequestID, result)
}

func (p *CloudWatchPlugin) describeAlarmsForMetric(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	metricName := req.Params["MetricName"]
	namespace := req.Params["Namespace"]

	goCtx := context.Background()
	idxKey := cwAlarmNamesKey(ctx.AccountID, ctx.Region)
	allNames, err := loadStringIndex(goCtx, p.state, monitoringNamespace, idxKey)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch describeAlarmsForMetric loadIndex: %w", err)
	}

	var alarms []CWAlarm
	for _, name := range allNames {
		data, getErr := p.state.Get(goCtx, monitoringNamespace, cwAlarmStateKey(ctx.AccountID, ctx.Region, name))
		if getErr != nil || data == nil {
			continue
		}
		var alarm CWAlarm
		if unmarshalErr := json.Unmarshal(data, &alarm); unmarshalErr != nil {
			continue
		}
		if metricName != "" && alarm.MetricName != metricName {
			continue
		}
		if namespace != "" && alarm.Namespace != namespace {
			continue
		}
		alarms = append(alarms, alarm)
	}

	// DescribeAlarmsForMetricOutput has one member, MetricAlarms — no NextToken, since
	// the operation does not paginate. Substrate previously answered a
	// DescribeAlarmsResponse/DescribeAlarmsResult wrapper here, borrowed from
	// DescribeAlarms; the query protocol names both elements after the operation.
	result := cwDoc{}.with("MetricAlarms", cwAlarmList(alarms))
	return cwRespond(req, "DescribeAlarmsForMetric", ctx.RequestID, result)
}

func (p *CloudWatchPlugin) setAlarmState(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["AlarmName"]
	stateValue := req.Params["StateValue"]
	stateReason := req.Params["StateReason"]
	stateReasonData := req.Params["StateReasonData"]

	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "AlarmName is required", HTTPStatus: http.StatusBadRequest}
	}
	if stateValue != "OK" && stateValue != "ALARM" && stateValue != "INSUFFICIENT_DATA" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "StateValue must be OK, ALARM, or INSUFFICIENT_DATA", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := cwAlarmStateKey(ctx.AccountID, ctx.Region, name)
	data, err := p.state.Get(goCtx, monitoringNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch setAlarmState state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Alarm not found: " + name, HTTPStatus: http.StatusNotFound}
	}

	var alarm CWAlarm
	if err := json.Unmarshal(data, &alarm); err != nil {
		return nil, fmt.Errorf("cloudwatch setAlarmState unmarshal: %w", err)
	}

	alarm.StateValue = stateValue
	alarm.StateReason = stateReason
	alarm.StateReasonData = stateReasonData

	updated, err := json.Marshal(alarm)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch setAlarmState marshal: %w", err)
	}
	if err := p.state.Put(goCtx, monitoringNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("cloudwatch setAlarmState state.Put: %w", err)
	}

	return cwUnitResponse(req, "SetAlarmState", ctx.RequestID)
}

func (p *CloudWatchPlugin) enableAlarmActions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.setActionsEnabled(ctx, req, true)
}

func (p *CloudWatchPlugin) disableAlarmActions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.setActionsEnabled(ctx, req, false)
}

func (p *CloudWatchPlugin) setActionsEnabled(ctx *RequestContext, req *AWSRequest, enabled bool) (*AWSResponse, error) {
	names := parseMemberList(req.Params, "AlarmNames")

	goCtx := context.Background()
	for _, name := range names {
		stateKey := cwAlarmStateKey(ctx.AccountID, ctx.Region, name)
		data, getErr := p.state.Get(goCtx, monitoringNamespace, stateKey)
		if getErr != nil || data == nil {
			continue
		}
		var alarm CWAlarm
		if json.Unmarshal(data, &alarm) != nil {
			continue
		}
		alarm.ActionsEnabled = enabled
		updated, _ := json.Marshal(alarm)
		_ = p.state.Put(goCtx, monitoringNamespace, stateKey, updated)
	}

	operation := "EnableAlarmActions"
	if !enabled {
		operation = "DisableAlarmActions"
	}
	return cwUnitResponse(req, operation, ctx.RequestID)
}

// --- Response documents -----------------------------------------------------

// cwAlarmList renders alarms as a MetricAlarms list.
//
// The list is always returned, even when empty, so that a DescribeAlarms with no
// matches answers a present-but-empty MetricAlarms — which is what CloudWatch does and
// what a typed client expects — rather than omitting the member.
func cwAlarmList(alarms []CWAlarm) cwList {
	list := make(cwList, 0, len(alarms))
	for _, a := range alarms {
		list = append(list, cwAlarmDoc(a))
	}
	return list
}

// cwAlarmDoc renders one alarm as a MetricAlarm structure.
//
// Member order is the order substrate's XML struct used, not the model's, so that the
// Query responses substrate already served are byte-for-byte unchanged; see the note in
// cloudwatch_render.go on why order is free to be either.
//
// The members present are the ones [CWAlarm] stores. Modeled members substrate does not
// track — the three timestamps, Dimensions, Unit, ExtendedStatistic, DatapointsToAlarm,
// TreatMissingData and the rest — are absent rather than zero, which is the honest
// answer: a typed client reads them as nil and can tell they were never set.
func cwAlarmDoc(a CWAlarm) cwDoc {
	doc := cwDoc{}.
		with("AlarmName", a.AlarmName).
		with("AlarmArn", a.AlarmARN).
		withNonEmpty("AlarmDescription", a.AlarmDescription).
		with("MetricName", a.MetricName).
		with("Namespace", a.Namespace).
		withNonEmpty("Statistic", a.Statistic).
		with("ComparisonOperator", a.ComparisonOperator).
		with("Threshold", a.Threshold).
		with("EvaluationPeriods", a.EvaluationPeriods).
		with("Period", a.Period).
		with("StateValue", a.StateValue).
		withNonEmpty("StateReason", a.StateReason).
		withNonEmpty("StateReasonData", a.StateReasonData).
		with("ActionsEnabled", a.ActionsEnabled)
	return doc.
		withStrings("AlarmActions", a.AlarmActions).
		withStrings("OKActions", a.OKActions).
		withStrings("InsufficientDataActions", a.InsufficientDataActions)
}

// --- State key helpers -------------------------------------------------------

func cwAlarmStateKey(accountID, region, alarmName string) string {
	return "alarm:" + accountID + "/" + region + "/" + alarmName
}

func cwAlarmNamesKey(accountID, region string) string {
	return "alarm_names:" + accountID + "/" + region
}

// parseMemberList extracts "Param.member.N" style values from a params map.
// It returns a nil slice when no members are present.
func parseMemberList(params map[string]string, prefix string) []string {
	var result []string
	for i := 1; ; i++ {
		key := prefix + ".member." + strconv.Itoa(i)
		val, ok := params[key]
		if !ok || val == "" {
			break
		}
		result = append(result, val)
	}
	return result
}

// --- GetMetricData -----------------------------------------------------------

// getMetricData handles the GetMetricData operation.
//
// Substrate records a metric's name and namespace but not its time series — running the
// workload that produces data points is outside what an API observation can be — so the
// answer is a present-but-empty MetricDataResults list and an empty Messages list. A
// caller that degrades gracefully on zero values (displaying "0 bytes", say) works
// against this; one that needs data points needs a seeded time series, which is not
// modeled.
//
// Before #785 this was where substrate's only CBOR lived: a hardcoded one-byte 0xa0 for
// any caller whose Content-Type mentioned CBOR, with the request body never parsed. The
// empty map is *tolerated* by the protocol's NoOutputClientAllowsEmptyCbor test but says
// nothing about the members, so a client could not distinguish "no data" from "not
// implemented"; the two empty lists do.
func (p *CloudWatchPlugin) getMetricData(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	result := cwDoc{}.
		with("MetricDataResults", cwList{}).
		with("Messages", cwList{})
	return cwRespond(req, "GetMetricData", ctx.RequestID, result)
}

// --- PutMetricData -----------------------------------------------------------

// cwMetricNamesKey returns the state key for the metric name index.
func cwMetricNamesKey(accountID, region, namespace string) string {
	return "metric_names:" + accountID + "/" + region + "/" + namespace
}

// putMetricData handles the PutMetricData operation. Substrate records the
// metric name and namespace so that ListMetrics can return them; actual
// data-point values are discarded (GetMetricData returns empty results).
// Closes #221.
func (p *CloudWatchPlugin) putMetricData(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	namespace := req.Params["Namespace"]

	for i := 1; ; i++ {
		prefix := "MetricData.member." + strconv.Itoa(i) + "."
		name := req.Params[prefix+"MetricName"]
		if name == "" {
			break
		}
		idxKey := cwMetricNamesKey(ctx.AccountID, ctx.Region, namespace)
		updateStringIndex(goCtx, p.state, monitoringNamespace, idxKey, name)
	}

	return cwUnitResponse(req, "PutMetricData", ctx.RequestID)
}

// --- ListMetrics -------------------------------------------------------------

// listMetrics handles the ListMetrics operation. It returns the metric names
// that were previously published via PutMetricData for the given namespace.
// Closes #221.
func (p *CloudWatchPlugin) listMetrics(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	namespace := req.Params["Namespace"]
	filterName := req.Params["MetricName"]

	metrics := cwList{}

	var namespaces []string
	if namespace != "" {
		namespaces = []string{namespace}
	} else {
		prefix := "metric_names:" + ctx.AccountID + "/" + ctx.Region + "/"
		all, err := p.state.List(goCtx, monitoringNamespace, prefix)
		if err == nil {
			for _, k := range all {
				ns := strings.TrimPrefix(k, prefix)
				if ns != "" {
					namespaces = append(namespaces, ns)
				}
			}
		}
	}

	for _, ns := range namespaces {
		idxKey := cwMetricNamesKey(ctx.AccountID, ctx.Region, ns)
		names, err := loadStringIndex(goCtx, p.state, monitoringNamespace, idxKey)
		if err != nil {
			continue
		}
		for _, name := range names {
			if filterName != "" && name != filterName {
				continue
			}
			// Dimensions is present and empty rather than absent: substrate records a
			// metric by name and namespace only, and an empty dimension list is what
			// CloudWatch reports for a metric published without any.
			metrics = append(metrics, cwDoc{}.
				with("MetricName", name).
				with("Namespace", ns).
				with("Dimensions", cwList{}))
		}
	}

	result := cwDoc{}.with("Metrics", metrics)
	return cwRespond(req, "ListMetrics", ctx.RequestID, result)
}
