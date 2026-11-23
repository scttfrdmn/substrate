package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// CloudWatchPlugin emulates the Amazon CloudWatch Alarms and Metrics API. It
// handles PutMetricAlarm, DeleteAlarms, DescribeAlarms, DescribeAlarmsForMetric,
// SetAlarmState, EnableAlarmActions, DisableAlarmActions, PutMetricData,
// ListMetrics, GetMetricData, GetMetricStatistics, TagResource, UntagResource and
// ListTagsForResource.
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

	// maxPoints is the number of datapoints a metric series keeps; see
	// cloudwatch_metric_store.go.
	maxPoints int

	// mu serializes the read-modify-write of a series or an alarm, so two concurrent writes to
	// one record cannot lose either.
	mu sync.Mutex
}

// cwMaxDatapointsOption is the plugin option that sets the per-series datapoint cap; see
// [CloudWatchCfg].
const cwMaxDatapointsOption = "max_datapoints_per_series"

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
	p.maxPoints = cwDefaultMaxDatapointsPerSeries
	if n, ok := cfg.Options[cwMaxDatapointsOption].(int); ok && n > 0 {
		p.maxPoints = n
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
	case "GetMetricStatistics":
		return p.getMetricStatistics(ctx, req)
	case "PutMetricData":
		return p.putMetricData(ctx, req)
	case "ListMetrics":
		return p.listMetrics(ctx, req)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	default:
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// --- Alarm CRUD operations --------------------------------------------------

// putMetricAlarm creates or updates an alarm; see cloudwatch_alarms.go for what it stores and
// the update rule.
func (p *CloudWatchPlugin) putMetricAlarm(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["AlarmName"]
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "AlarmName is required", HTTPStatus: http.StatusBadRequest}
	}
	alarm, awsErr := cwParseAlarm(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	requestTags, _ := cwParseTagList(req.Params, "Tags")

	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.tc.Now().UTC()
	alarm.AlarmARN = cwAlarmARN(ctx.Region, ctx.AccountID, name)
	alarm.AlarmConfigurationUpdatedTimestamp = now

	goCtx := context.Background()
	stateKey := cwAlarmStateKey(ctx.AccountID, ctx.Region, name)
	existing, found, err := p.loadAlarm(goCtx, stateKey)
	if err != nil {
		return nil, err
	}
	if found {
		// An update leaves the state, and the tags, as they were.
		alarm.StateValue = existing.StateValue
		alarm.StateReason = existing.StateReason
		alarm.StateReasonData = existing.StateReasonData
		alarm.StateUpdatedTimestamp = existing.StateUpdatedTimestamp
		alarm.StateTransitionedTimestamp = existing.StateTransitionedTimestamp
		alarm.Tags = existing.Tags
	} else {
		// The tags are checked only here: an update ignores them, so it cannot be refused for
		// them either.
		if awsErr := cwCheckTags(requestTags); awsErr != nil {
			return nil, awsErr
		}
		// "When this operation creates an alarm, the alarm state is immediately set to
		// INSUFFICIENT_DATA."
		alarm.StateValue = "INSUFFICIENT_DATA"
		alarm.StateUpdatedTimestamp = now
		alarm.StateTransitionedTimestamp = now
		alarm.Tags = requestTags
	}

	if err := p.storeAlarm(goCtx, stateKey, alarm); err != nil {
		return nil, err
	}
	idxKey := cwAlarmNamesKey(ctx.AccountID, ctx.Region)
	if err := updateStringIndex(goCtx, p.state, monitoringNamespace, idxKey, name); err != nil {
		return nil, fmt.Errorf("cloudwatch putMetricAlarm index: %w", err)
	}
	return cwUnitResponse(req, "PutMetricAlarm", ctx.RequestID)
}

// loadAlarm reads the alarm stored at stateKey, reporting false when there is none.
func (p *CloudWatchPlugin) loadAlarm(ctx context.Context, stateKey string) (CWAlarm, bool, error) {
	data, err := p.state.Get(ctx, monitoringNamespace, stateKey)
	if err != nil {
		return CWAlarm{}, false, fmt.Errorf("cloudwatch load alarm: %w", err)
	}
	if data == nil {
		return CWAlarm{}, false, nil
	}
	var alarm CWAlarm
	if err := json.Unmarshal(data, &alarm); err != nil {
		return CWAlarm{}, false, fmt.Errorf("cloudwatch decode alarm: %w", err)
	}
	return alarm, true, nil
}

// storeAlarm writes alarm at stateKey.
func (p *CloudWatchPlugin) storeAlarm(ctx context.Context, stateKey string, alarm CWAlarm) error {
	data, err := json.Marshal(alarm)
	if err != nil {
		return fmt.Errorf("cloudwatch marshal alarm: %w", err)
	}
	if err := p.state.Put(ctx, monitoringNamespace, stateKey, data); err != nil {
		return fmt.Errorf("cloudwatch store alarm: %w", err)
	}
	return nil
}

func (p *CloudWatchPlugin) deleteAlarms(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	names := parseMemberList(req.Params, "AlarmNames")

	p.mu.Lock()
	defer p.mu.Unlock()
	goCtx := context.Background()
	for _, name := range names {
		stateKey := cwAlarmStateKey(ctx.AccountID, ctx.Region, name)
		if err := p.state.Delete(goCtx, monitoringNamespace, stateKey); err != nil {
			return nil, fmt.Errorf("cloudwatch deleteAlarms: %w", err)
		}
		// The tag history goes with the alarm, so an alarm re-created under the name starts
		// never-tagged, as a new resource does.
		if err := p.state.Delete(goCtx, monitoringNamespace, cwAlarmTaggedKey(stateKey)); err != nil {
			return nil, fmt.Errorf("cloudwatch deleteAlarms tag history: %w", err)
		}
		idxKey := cwAlarmNamesKey(ctx.AccountID, ctx.Region)
		if err := removeFromStringIndex(goCtx, p.state, monitoringNamespace, idxKey, name); err != nil {
			return nil, fmt.Errorf("cloudwatch deleteAlarms index: %w", err)
		}
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
	offset, tokenOK := decodeOffsetPaginationToken(req.Params["NextToken"])
	if !tokenOK {
		return nil, cwInvalidNextToken()
	}

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

	// Pagination. The token was validated above, before the alarm index was read, so a
	// token substrate could not have issued is refused rather than answered with page one
	// (#915). An offset past the end is a token substrate did issue over a listing that
	// has since shrunk, and clamps to a final empty page.
	if offset > len(names) {
		offset = len(names)
	}

	end := offset + maxRecords
	var outNextToken string
	if end < len(names) {
		outNextToken = encodeOffsetPaginationToken(end)
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
		if !cwAlarmMatchesMetric(alarm, req.Params) {
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

	p.mu.Lock()
	defer p.mu.Unlock()
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

	now := p.tc.Now().UTC()
	if alarm.StateValue != stateValue {
		alarm.StateTransitionedTimestamp = now
	}
	alarm.StateUpdatedTimestamp = now
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

	p.mu.Lock()
	defer p.mu.Unlock()
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
		updated, err := json.Marshal(alarm)
		if err != nil {
			return nil, fmt.Errorf("cloudwatch setActionsEnabled marshal: %w", err)
		}
		if err := p.state.Put(goCtx, monitoringNamespace, stateKey, updated); err != nil {
			return nil, fmt.Errorf("cloudwatch setActionsEnabled state.Put: %w", err)
		}
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
