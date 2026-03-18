package substrate

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// CloudWatchPlugin emulates the Amazon CloudWatch query-protocol Alarms API.
// It handles PutMetricAlarm, DeleteAlarms, DescribeAlarms,
// DescribeAlarmsForMetric, SetAlarmState, EnableAlarmActions, and
// DisableAlarmActions.
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

// HandleRequest dispatches a CloudWatch query-protocol request to the
// appropriate handler.
func (p *CloudWatchPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
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
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("CloudWatchPlugin: unknown operation %q", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
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

	type response struct {
		XMLName  xml.Name `xml:"PutMetricAlarmResponse"`
		XMLNS    string   `xml:"xmlns,attr"`
		Metadata struct {
			RequestID string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	return cwXMLResponse(http.StatusOK, response{
		XMLNS: cloudwatchXMLNS,
		Metadata: struct {
			RequestID string `xml:"RequestId"`
		}{RequestID: ctx.RequestID},
	})
}

func (p *CloudWatchPlugin) deleteAlarms(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	names := parseMemberList(req.Params, "AlarmNames")

	goCtx := context.Background()
	for _, name := range names {
		_ = p.state.Delete(goCtx, monitoringNamespace, cwAlarmStateKey(ctx.AccountID, ctx.Region, name))
		idxKey := cwAlarmNamesKey(ctx.AccountID, ctx.Region)
		removeFromStringIndex(goCtx, p.state, monitoringNamespace, idxKey, name)
	}

	type response struct {
		XMLName  xml.Name `xml:"DeleteAlarmsResponse"`
		XMLNS    string   `xml:"xmlns,attr"`
		Metadata struct {
			RequestID string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	return cwXMLResponse(http.StatusOK, response{
		XMLNS: cloudwatchXMLNS,
		Metadata: struct {
			RequestID string `xml:"RequestId"`
		}{RequestID: ctx.RequestID},
	})
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

	return cwXMLResponse(http.StatusOK, buildDescribeAlarmsResponse(alarms, outNextToken, ctx.RequestID))
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

	return cwXMLResponse(http.StatusOK, buildDescribeAlarmsResponse(alarms, "", ctx.RequestID))
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

	type response struct {
		XMLName  xml.Name `xml:"SetAlarmStateResponse"`
		XMLNS    string   `xml:"xmlns,attr"`
		Metadata struct {
			RequestID string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	return cwXMLResponse(http.StatusOK, response{
		XMLNS: cloudwatchXMLNS,
		Metadata: struct {
			RequestID string `xml:"RequestId"`
		}{RequestID: ctx.RequestID},
	})
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

	opName := "EnableAlarmActionsResponse"
	if !enabled {
		opName = "DisableAlarmActionsResponse"
	}
	type response struct {
		XMLName  xml.Name `xml:"placeholder"`
		XMLNS    string   `xml:"xmlns,attr"`
		Metadata struct {
			RequestID string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	resp := response{
		XMLNS: cloudwatchXMLNS,
		Metadata: struct {
			RequestID string `xml:"RequestId"`
		}{RequestID: ctx.RequestID},
	}
	body, err := xml.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch setActionsEnabled xml.Marshal: %w", err)
	}
	// Replace placeholder element name.
	bodyStr := strings.ReplaceAll(string(body), "<placeholder>", "<"+opName+">")
	bodyStr = strings.ReplaceAll(bodyStr, "</placeholder>", "</"+opName+">")
	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), []byte(bodyStr)...),
	}, nil
}

// --- XML response builders --------------------------------------------------

// cwAlarmXML is an XML-serialisable representation of a CloudWatch alarm.
type cwAlarmXML struct {
	AlarmName               string   `xml:"AlarmName"`
	AlarmArn                string   `xml:"AlarmArn"`
	AlarmDescription        string   `xml:"AlarmDescription,omitempty"`
	MetricName              string   `xml:"MetricName"`
	Namespace               string   `xml:"Namespace"`
	Statistic               string   `xml:"Statistic,omitempty"`
	ComparisonOperator      string   `xml:"ComparisonOperator"`
	Threshold               float64  `xml:"Threshold"`
	EvaluationPeriods       int      `xml:"EvaluationPeriods"`
	Period                  int      `xml:"Period"`
	StateValue              string   `xml:"StateValue"`
	StateReason             string   `xml:"StateReason,omitempty"`
	ActionsEnabled          bool     `xml:"ActionsEnabled"`
	AlarmActions            []string `xml:"AlarmActions>member,omitempty"`
	OKActions               []string `xml:"OKActions>member,omitempty"`
	InsufficientDataActions []string `xml:"InsufficientDataActions>member,omitempty"`
}

// describeAlarmsXML is the XML response for DescribeAlarms and
// DescribeAlarmsForMetric.
type describeAlarmsXML struct {
	XMLName xml.Name `xml:"DescribeAlarmsResponse"`
	XMLNS   string   `xml:"xmlns,attr"`
	Result  struct {
		MetricAlarms []cwAlarmXML `xml:"MetricAlarms>member,omitempty"`
		NextToken    string       `xml:"NextToken,omitempty"`
	} `xml:"DescribeAlarmsResult"`
	Metadata struct {
		RequestID string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

func buildDescribeAlarmsResponse(alarms []CWAlarm, nextToken, requestID string) describeAlarmsXML {
	resp := describeAlarmsXML{XMLNS: cloudwatchXMLNS}
	resp.Metadata.RequestID = requestID
	resp.Result.NextToken = nextToken
	for _, a := range alarms {
		resp.Result.MetricAlarms = append(resp.Result.MetricAlarms, cwAlarmXML{
			AlarmName:               a.AlarmName,
			AlarmArn:                a.AlarmARN,
			AlarmDescription:        a.AlarmDescription,
			MetricName:              a.MetricName,
			Namespace:               a.Namespace,
			Statistic:               a.Statistic,
			ComparisonOperator:      a.ComparisonOperator,
			Threshold:               a.Threshold,
			EvaluationPeriods:       a.EvaluationPeriods,
			Period:                  a.Period,
			StateValue:              a.StateValue,
			StateReason:             a.StateReason,
			ActionsEnabled:          a.ActionsEnabled,
			AlarmActions:            a.AlarmActions,
			OKActions:               a.OKActions,
			InsufficientDataActions: a.InsufficientDataActions,
		})
	}
	return resp
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

// --- Response helper ---------------------------------------------------------

func cwXMLResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch xml.Marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}

// --- GetMetricData -----------------------------------------------------------

// getMetricData handles the GetMetricData operation.  Substrate does not
// store real metric time-series data, so it returns an empty
// MetricDataResults list.  Callers that degrade gracefully on zero values
// (e.g. display "0 bytes") work correctly with this response.
func (p *CloudWatchPlugin) getMetricData(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	type metricDataResult struct {
		ID         string   `xml:"Id"`
		Label      string   `xml:"Label"`
		StatusCode string   `xml:"StatusCode"`
		Timestamps []string `xml:"Timestamps>member"`
		Values     []string `xml:"Values>member"`
	}
	type response struct {
		XMLName  xml.Name `xml:"GetMetricDataResponse"`
		XMLNS    string   `xml:"xmlns,attr"`
		Result   struct {
			MetricDataResults []metricDataResult `xml:"MetricDataResults>member"`
			NextToken         string             `xml:"NextToken,omitempty"`
		} `xml:"GetMetricDataResult"`
		Metadata struct {
			RequestID string `xml:"RequestId"`
		} `xml:"ResponseMetadata"`
	}
	return cwXMLResponse(http.StatusOK, response{
		XMLNS: cloudwatchXMLNS,
		Metadata: struct {
			RequestID string `xml:"RequestId"`
		}{RequestID: ctx.RequestID},
	})
}
