package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CloudWatchLogsPlugin emulates the Amazon CloudWatch Logs JSON-protocol API.
// It handles CreateLogGroup, DeleteLogGroup, DescribeLogGroups,
// PutRetentionPolicy, DeleteRetentionPolicy, CreateLogStream, DeleteLogStream,
// DescribeLogStreams, PutLogEvents, GetLogEvents, and FilterLogEvents.
type CloudWatchLogsPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "logs".
func (p *CloudWatchLogsPlugin) Name() string { return "logs" }

// Initialize sets up the CloudWatchLogsPlugin with the provided configuration.
func (p *CloudWatchLogsPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for CloudWatchLogsPlugin.
func (p *CloudWatchLogsPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a CloudWatch Logs JSON-protocol request to the
// appropriate handler.
func (p *CloudWatchLogsPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateLogGroup":
		return p.createLogGroup(ctx, req)
	case "DeleteLogGroup":
		return p.deleteLogGroup(ctx, req)
	case "DescribeLogGroups":
		return p.describeLogGroups(ctx, req)
	case "PutRetentionPolicy":
		return p.putRetentionPolicy(ctx, req)
	case "DeleteRetentionPolicy":
		return p.deleteRetentionPolicy(ctx, req)
	case "CreateLogStream":
		return p.createLogStream(ctx, req)
	case "DeleteLogStream":
		return p.deleteLogStream(ctx, req)
	case "DescribeLogStreams":
		return p.describeLogStreams(ctx, req)
	case "PutLogEvents":
		return p.putLogEvents(ctx, req)
	case "GetLogEvents":
		return p.getLogEvents(ctx, req)
	case "FilterLogEvents":
		return p.filterLogEvents(ctx, req)
	default:
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// --- Log group operations ---------------------------------------------------

func (p *CloudWatchLogsPlugin) createLogGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupName    string            `json:"logGroupName"`
		RetentionInDays int               `json:"retentionInDays"`
		Tags            map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.LogGroupName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := cwLogGroupKey(ctx.AccountID, ctx.Region, body.LogGroupName)
	existing, err := p.state.Get(goCtx, cloudwatchLogsNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("logs createLogGroup state.Get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "ResourceAlreadyExistsException", Message: "Log group already exists: " + body.LogGroupName, HTTPStatus: http.StatusConflict}
	}

	lg := CWLogGroup{
		LogGroupName:    body.LogGroupName,
		ARN:             cwLogGroupARN(ctx.Region, ctx.AccountID, body.LogGroupName),
		CreationTime:    p.tc.Now().UnixMilli(),
		RetentionInDays: body.RetentionInDays,
	}
	data, err := json.Marshal(lg)
	if err != nil {
		return nil, fmt.Errorf("logs createLogGroup marshal: %w", err)
	}
	if err := p.state.Put(goCtx, cloudwatchLogsNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("logs createLogGroup state.Put: %w", err)
	}

	idxKey := cwLogGroupNamesKey(ctx.AccountID, ctx.Region)
	updateStringIndex(goCtx, p.state, cloudwatchLogsNamespace, idxKey, body.LogGroupName)

	return cwLogsJSONResponse(http.StatusOK, struct{}{})
}

func (p *CloudWatchLogsPlugin) deleteLogGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupName string `json:"logGroupName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.LogGroupName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := cwLogGroupKey(ctx.AccountID, ctx.Region, body.LogGroupName)
	existing, err := p.state.Get(goCtx, cloudwatchLogsNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("logs deleteLogGroup state.Get: %w", err)
	}
	if existing == nil {
		// 400, not 404: API_DeleteLogGroup publishes ResourceNotFoundException at 400 like every other
		// page in this service, and the plugin answered two different statuses for one code until #1224.
		return nil, cwLogsGroupNotFound(body.LogGroupName)
	}

	// Delete log group.
	if err := p.state.Delete(goCtx, cloudwatchLogsNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("logs deleteLogGroup state.Delete: %w", err)
	}

	// Remove from index.
	idxKey := cwLogGroupNamesKey(ctx.AccountID, ctx.Region)
	removeFromStringIndex(goCtx, p.state, cloudwatchLogsNamespace, idxKey, body.LogGroupName)

	// Delete all streams for this group.
	streamsIdxKey := cwLogStreamNamesKey(ctx.AccountID, ctx.Region, body.LogGroupName)
	streamData, _ := p.state.Get(goCtx, cloudwatchLogsNamespace, streamsIdxKey)
	if streamData != nil {
		var streamNames []string
		if json.Unmarshal(streamData, &streamNames) == nil {
			for _, sn := range streamNames {
				_ = p.state.Delete(goCtx, cloudwatchLogsNamespace, cwLogStreamKey(ctx.AccountID, ctx.Region, body.LogGroupName, sn))
				_ = p.state.Delete(goCtx, cloudwatchLogsNamespace, cwLogEventsKey(ctx.AccountID, ctx.Region, body.LogGroupName, sn))
			}
		}
		_ = p.state.Delete(goCtx, cloudwatchLogsNamespace, streamsIdxKey)
	}

	return cwLogsJSONResponse(http.StatusOK, struct{}{})
}

func (p *CloudWatchLogsPlugin) describeLogGroups(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupNamePrefix string `json:"logGroupNamePrefix"`
		NextToken          string `json:"nextToken"`
		Limit              int    `json:"limit"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, cwlInvalidBody()
		}
	}

	// Before the log-group index is read, so the refusal does not depend on what the store holds — see
	// cloudwatchlogs_pagination.go for the code's provenance and the ordering argument (#1086).
	offset, tokenOK := decodeOffsetPaginationToken(body.NextToken)
	if !tokenOK {
		return nil, cwLogsInvalidPaginationToken("DescribeLogGroups")
	}

	goCtx := context.Background()
	idxKey := cwLogGroupNamesKey(ctx.AccountID, ctx.Region)
	allNames, err := loadStringIndex(goCtx, p.state, cloudwatchLogsNamespace, idxKey)
	if err != nil {
		return nil, fmt.Errorf("logs describeLogGroups loadIndex: %w", err)
	}

	// Filter by prefix.
	names := allNames
	if body.LogGroupNamePrefix != "" {
		filtered := make([]string, 0, len(allNames))
		for _, n := range allNames {
			if strings.HasPrefix(n, body.LogGroupNamePrefix) {
				filtered = append(filtered, n)
			}
		}
		names = filtered
	}

	// Pagination. The index is ASCII-sorted by log group name, which is the order the page publishes
	// for the results and is stable across calls — which is what [pageByOffsetToken]'s offset relies
	// on. The prefix filter above preserves that order, so the offset counts the groups this request
	// can see rather than every group in the account.
	limit := body.Limit
	if limit <= 0 {
		limit = cwLogsDescribeDefaultLimit
	}
	page, nextToken := pageByOffsetToken(names, offset, limit)

	groups := make([]cwLogGroupOut, 0, len(page))
	for _, name := range page {
		data, getErr := p.state.Get(goCtx, cloudwatchLogsNamespace, cwLogGroupKey(ctx.AccountID, ctx.Region, name))
		if getErr != nil || data == nil {
			continue
		}
		var lg CWLogGroup
		if unmarshalErr := json.Unmarshal(data, &lg); unmarshalErr != nil {
			continue
		}
		groups = append(groups, cwLogGroupWire(lg))
	}

	type response struct {
		LogGroups []cwLogGroupOut `json:"logGroups"`
		NextToken string          `json:"nextToken,omitempty"`
	}
	return cwLogsJSONResponse(http.StatusOK, response{LogGroups: groups, NextToken: nextToken})
}

// cwRetentionDays is the fixed set of values retentionInDays accepts. The API
// enumerates it rather than accepting a range, so anything else is an
// InvalidParameterException — including a plausible-looking 45 or 100.
var cwRetentionDays = map[int]bool{
	1: true, 3: true, 5: true, 7: true, 14: true, 30: true, 60: true, 90: true,
	120: true, 150: true, 180: true, 365: true, 400: true, 545: true, 731: true,
	1096: true, 1827: true, 2192: true, 2557: true, 2922: true, 3288: true,
	3653: true,
}

// putRetentionPolicy sets a log group's retention in days.
//
// Both of this operation's failure modes are documented at HTTP 400, including
// ResourceNotFoundException — the JSON-1.1 protocol carries the error code in the
// body's "__type", so the status is 400 for every modeled error rather than the
// 404 an HTTP-shaped API would use.
func (p *CloudWatchLogsPlugin) putRetentionPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupName    string `json:"logGroupName"`
		RetentionInDays *int   `json:"retentionInDays"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.LogGroupName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	// A pointer, not an int: retentionInDays is required, and 0 is not a valid
	// value, so an absent member and an explicit 0 are both errors — but only a
	// pointer can tell substrate which one the caller sent.
	if body.RetentionInDays == nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "retentionInDays is required", HTTPStatus: http.StatusBadRequest}
	}
	if !cwRetentionDays[*body.RetentionInDays] {
		return nil, &AWSError{
			Code: "InvalidParameterException",
			Message: fmt.Sprintf(
				"retentionInDays value of %d is not valid; possible values are: %s",
				*body.RetentionInDays, cwRetentionDaysList()),
			HTTPStatus: http.StatusBadRequest,
		}
	}

	goCtx := context.Background()
	stateKey := cwLogGroupKey(ctx.AccountID, ctx.Region, body.LogGroupName)
	lg, err := p.loadLogGroup(goCtx, stateKey, body.LogGroupName, "putRetentionPolicy")
	if err != nil {
		return nil, err
	}

	lg.RetentionInDays = *body.RetentionInDays
	if putErr := p.storeLogGroup(goCtx, stateKey, lg, "putRetentionPolicy"); putErr != nil {
		return nil, putErr
	}
	return cwLogsJSONResponse(http.StatusOK, struct{}{})
}

// deleteRetentionPolicy clears a log group's retention, so its events never
// expire. The API documents this as the way to make events permanent — there is
// no retentionInDays value meaning "never".
func (p *CloudWatchLogsPlugin) deleteRetentionPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupName string `json:"logGroupName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.LogGroupName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := cwLogGroupKey(ctx.AccountID, ctx.Region, body.LogGroupName)
	lg, err := p.loadLogGroup(goCtx, stateKey, body.LogGroupName, "deleteRetentionPolicy")
	if err != nil {
		return nil, err
	}

	// Deleting a policy that is not there is not an error — the operation is
	// idempotent and its documented errors do not include one for it.
	lg.RetentionInDays = 0
	if putErr := p.storeLogGroup(goCtx, stateKey, lg, "deleteRetentionPolicy"); putErr != nil {
		return nil, putErr
	}
	return cwLogsJSONResponse(http.StatusOK, struct{}{})
}

// loadLogGroup reads a log group, reporting ResourceNotFoundException when it is
// absent. op names the calling operation for the wrapped-error context.
func (p *CloudWatchLogsPlugin) loadLogGroup(ctx context.Context, stateKey, logGroupName, op string) (CWLogGroup, error) {
	data, err := p.state.Get(ctx, cloudwatchLogsNamespace, stateKey)
	if err != nil {
		return CWLogGroup{}, fmt.Errorf("logs %s state.Get: %w", op, err)
	}
	if data == nil {
		return CWLogGroup{}, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    "The specified log group does not exist: " + logGroupName,
			HTTPStatus: http.StatusBadRequest,
		}
	}
	var lg CWLogGroup
	if unmarshalErr := json.Unmarshal(data, &lg); unmarshalErr != nil {
		return CWLogGroup{}, fmt.Errorf("logs %s unmarshal: %w", op, unmarshalErr)
	}
	return lg, nil
}

// storeLogGroup writes a log group back to state.
func (p *CloudWatchLogsPlugin) storeLogGroup(ctx context.Context, stateKey string, lg CWLogGroup, op string) error {
	data, err := json.Marshal(lg)
	if err != nil {
		return fmt.Errorf("logs %s marshal: %w", op, err)
	}
	if putErr := p.state.Put(ctx, cloudwatchLogsNamespace, stateKey, data); putErr != nil {
		return fmt.Errorf("logs %s state.Put: %w", op, putErr)
	}
	return nil
}

// cwRetentionDaysList renders the valid retention values in ascending order for
// an error message. Sorted, not map order: a nondeterministic error message is
// one a consumer's test cannot assert on.
func cwRetentionDaysList() string {
	days := make([]int, 0, len(cwRetentionDays))
	for d := range cwRetentionDays {
		days = append(days, d)
	}
	sort.Ints(days)
	parts := make([]string, len(days))
	for i, d := range days {
		parts[i] = strconv.Itoa(d)
	}
	return strings.Join(parts, ", ")
}

// --- Log stream operations --------------------------------------------------

func (p *CloudWatchLogsPlugin) createLogStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupName  string `json:"logGroupName"`
		LogStreamName string `json:"logStreamName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.LogGroupName == "" || body.LogStreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName and logStreamName are required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	// Ensure the log group exists.
	groupKey := cwLogGroupKey(ctx.AccountID, ctx.Region, body.LogGroupName)
	groupData, err := p.state.Get(goCtx, cloudwatchLogsNamespace, groupKey)
	if err != nil {
		return nil, fmt.Errorf("logs createLogStream group.Get: %w", err)
	}
	if groupData == nil {
		return nil, cwLogsGroupNotFound(body.LogGroupName)
	}

	streamKey := cwLogStreamKey(ctx.AccountID, ctx.Region, body.LogGroupName, body.LogStreamName)
	existing, err := p.state.Get(goCtx, cloudwatchLogsNamespace, streamKey)
	if err != nil {
		return nil, fmt.Errorf("logs createLogStream state.Get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "ResourceAlreadyExistsException", Message: "Log stream already exists: " + body.LogStreamName, HTTPStatus: http.StatusConflict}
	}

	now := p.tc.Now().UnixMilli()
	ls := CWLogStream{
		LogStreamName:       body.LogStreamName,
		ARN:                 cwLogStreamARN(ctx.Region, ctx.AccountID, body.LogGroupName, body.LogStreamName),
		CreationTime:        now,
		UploadSequenceToken: generateLambdaRevisionID(ctx.IDs),
	}
	data, err := json.Marshal(ls)
	if err != nil {
		return nil, fmt.Errorf("logs createLogStream marshal: %w", err)
	}
	if err := p.state.Put(goCtx, cloudwatchLogsNamespace, streamKey, data); err != nil {
		return nil, fmt.Errorf("logs createLogStream state.Put: %w", err)
	}

	idxKey := cwLogStreamNamesKey(ctx.AccountID, ctx.Region, body.LogGroupName)
	updateStringIndex(goCtx, p.state, cloudwatchLogsNamespace, idxKey, body.LogStreamName)

	return cwLogsJSONResponse(http.StatusOK, struct{}{})
}

func (p *CloudWatchLogsPlugin) deleteLogStream(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupName  string `json:"logGroupName"`
		LogStreamName string `json:"logStreamName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.LogGroupName == "" || body.LogStreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName and logStreamName are required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	streamKey := cwLogStreamKey(ctx.AccountID, ctx.Region, body.LogGroupName, body.LogStreamName)
	existing, err := p.state.Get(goCtx, cloudwatchLogsNamespace, streamKey)
	if err != nil {
		return nil, fmt.Errorf("logs deleteLogStream state.Get: %w", err)
	}
	if existing == nil {
		return nil, cwLogsStreamNotFound(body.LogStreamName)
	}

	if err := p.state.Delete(goCtx, cloudwatchLogsNamespace, streamKey); err != nil {
		return nil, fmt.Errorf("logs deleteLogStream state.Delete: %w", err)
	}
	_ = p.state.Delete(goCtx, cloudwatchLogsNamespace, cwLogEventsKey(ctx.AccountID, ctx.Region, body.LogGroupName, body.LogStreamName))

	idxKey := cwLogStreamNamesKey(ctx.AccountID, ctx.Region, body.LogGroupName)
	removeFromStringIndex(goCtx, p.state, cloudwatchLogsNamespace, idxKey, body.LogStreamName)

	return cwLogsJSONResponse(http.StatusOK, struct{}{})
}

func (p *CloudWatchLogsPlugin) describeLogStreams(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupName        string `json:"logGroupName"`
		LogStreamNamePrefix string `json:"logStreamNamePrefix"`
		NextToken           string `json:"nextToken"`
		Limit               int    `json:"limit"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, cwlInvalidBody()
		}
	}
	if body.LogGroupName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	// Above the token decode, because the group is the resource the request addresses and a token is a
	// continuation of a listing over it — see cloudwatchlogs_not_found.go for the precedence argument
	// and the precedent it follows (#1224).
	if err := p.requireLogGroup(goCtx, ctx.AccountID, ctx.Region, body.LogGroupName, "describeLogStreams"); err != nil {
		return nil, err
	}

	// Below the required-member refusal and the group resolution, and above the stream index, for the
	// reason cloudwatchlogs_pagination.go states (#1086).
	offset, tokenOK := decodeOffsetPaginationToken(body.NextToken)
	if !tokenOK {
		return nil, cwLogsInvalidPaginationToken("DescribeLogStreams")
	}

	idxKey := cwLogStreamNamesKey(ctx.AccountID, ctx.Region, body.LogGroupName)
	allNames, err := loadStringIndex(goCtx, p.state, cloudwatchLogsNamespace, idxKey)
	if err != nil {
		return nil, fmt.Errorf("logs describeLogStreams loadIndex: %w", err)
	}

	names := allNames
	if body.LogStreamNamePrefix != "" {
		filtered := make([]string, 0, len(allNames))
		for _, n := range allNames {
			if strings.HasPrefix(n, body.LogStreamNamePrefix) {
				filtered = append(filtered, n)
			}
		}
		names = filtered
	}

	// The index is name-sorted and the prefix filter preserves that order, so the offset counts the
	// streams this request can see. A stream whose record no longer loads is skipped below, so a page
	// can be shorter than limit while a token is still emitted; the offset counts index entries rather
	// than rendered members, so the walk stays coherent.
	limit := body.Limit
	if limit <= 0 {
		limit = cwLogsDescribeDefaultLimit
	}
	page, nextToken := pageByOffsetToken(names, offset, limit)

	streams := make([]cwLogStreamOut, 0, len(page))
	for _, name := range page {
		data, getErr := p.state.Get(goCtx, cloudwatchLogsNamespace, cwLogStreamKey(ctx.AccountID, ctx.Region, body.LogGroupName, name))
		if getErr != nil || data == nil {
			continue
		}
		var ls CWLogStream
		if unmarshalErr := json.Unmarshal(data, &ls); unmarshalErr != nil {
			continue
		}
		streams = append(streams, cwLogStreamWire(ls))
	}

	type response struct {
		LogStreams []cwLogStreamOut `json:"logStreams"`
		NextToken  string           `json:"nextToken,omitempty"`
	}
	return cwLogsJSONResponse(http.StatusOK, response{LogStreams: streams, NextToken: nextToken})
}

// --- Log event operations ---------------------------------------------------

func (p *CloudWatchLogsPlugin) putLogEvents(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupName  string `json:"logGroupName"`
		LogStreamName string `json:"logStreamName"`
		LogEvents     []struct {
			Timestamp int64  `json:"timestamp"`
			Message   string `json:"message"`
		} `json:"logEvents"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.LogGroupName == "" || body.LogStreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName and logStreamName are required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	// Verify stream exists.
	streamKey := cwLogStreamKey(ctx.AccountID, ctx.Region, body.LogGroupName, body.LogStreamName)
	streamData, err := p.state.Get(goCtx, cloudwatchLogsNamespace, streamKey)
	if err != nil {
		return nil, fmt.Errorf("logs putLogEvents stream.Get: %w", err)
	}
	if streamData == nil {
		return nil, cwLogsStreamNotFound(body.LogStreamName)
	}

	now := p.tc.Now().UnixMilli()
	eventsKey := cwLogEventsKey(ctx.AccountID, ctx.Region, body.LogGroupName, body.LogStreamName)

	// Load existing events.
	var existing []CWLogEvent
	existingData, _ := p.state.Get(goCtx, cloudwatchLogsNamespace, eventsKey)
	if existingData != nil {
		_ = json.Unmarshal(existingData, &existing)
	}

	for _, le := range body.LogEvents {
		existing = append(existing, CWLogEvent{
			Timestamp:     le.Timestamp,
			Message:       le.Message,
			IngestionTime: now,
		})
	}

	eventsBytes, err := json.Marshal(existing)
	if err != nil {
		return nil, fmt.Errorf("logs putLogEvents marshal: %w", err)
	}
	if err := p.state.Put(goCtx, cloudwatchLogsNamespace, eventsKey, eventsBytes); err != nil {
		return nil, fmt.Errorf("logs putLogEvents state.Put: %w", err)
	}

	// Update stream metadata.
	var ls CWLogStream
	if json.Unmarshal(streamData, &ls) == nil {
		ls.LastIngestionTime = now
		ls.UploadSequenceToken = generateLambdaRevisionID(ctx.IDs)
		if updated, marshalErr := json.Marshal(ls); marshalErr == nil {
			_ = p.state.Put(goCtx, cloudwatchLogsNamespace, streamKey, updated)
		}
	}

	type response struct {
		NextSequenceToken string `json:"nextSequenceToken"`
	}
	return cwLogsJSONResponse(http.StatusOK, response{NextSequenceToken: ls.UploadSequenceToken})
}

func (p *CloudWatchLogsPlugin) getLogEvents(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupName  string `json:"logGroupName"`
		LogStreamName string `json:"logStreamName"`
		StartTime     int64  `json:"startTime"`
		EndTime       int64  `json:"endTime"`
		NextToken     string `json:"nextToken"`
		Limit         int    `json:"limit"`
		StartFromHead bool   `json:"startFromHead"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, cwlInvalidBody()
		}
	}
	if body.LogGroupName == "" || body.LogStreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName and logStreamName are required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	// Both resources this request addresses are resolved above the token decode, the group before the
	// stream so that a missing group is reported as a missing group — see cloudwatchlogs_not_found.go
	// (#1224). logStreamName is Required: Yes on this page, so the stream is as much the addressed
	// resource as the group is.
	if err := p.requireLogGroup(goCtx, ctx.AccountID, ctx.Region, body.LogGroupName, "getLogEvents"); err != nil {
		return nil, err
	}
	if err := p.requireLogStream(goCtx, ctx.AccountID, ctx.Region, body.LogGroupName, body.LogStreamName, "getLogEvents"); err != nil {
		return nil, err
	}

	// Below the required-member refusal and the two resolutions, and above the event read, for the
	// reason cloudwatchlogs_pagination.go states (#1086). The token carries its own direction, which is
	// what cloudwatchlogs_event_tokens.go is for (#1223) — so unlike the other three paginators here,
	// what a token decodes to is a cursor rather than a bare offset.
	cursor, tokenPresent, tokenOK := cwLogsDecodeEventToken(body.NextToken)
	if !tokenOK {
		return nil, cwLogsInvalidPaginationToken("GetLogEvents")
	}

	eventsKey := cwLogEventsKey(ctx.AccountID, ctx.Region, body.LogGroupName, body.LogStreamName)
	eventsData, err := p.state.Get(goCtx, cloudwatchLogsNamespace, eventsKey)
	if err != nil {
		return nil, fmt.Errorf("logs getLogEvents state.Get: %w", err)
	}

	var all []CWLogEvent
	if eventsData != nil {
		_ = json.Unmarshal(eventsData, &all)
	}

	// Filter by time range.
	filtered := make([]CWLogEvent, 0, len(all))
	for _, ev := range all {
		if body.StartTime > 0 && ev.Timestamp < body.StartTime {
			continue
		}
		if body.EndTime > 0 && ev.Timestamp >= body.EndTime {
			continue
		}
		filtered = append(filtered, ev)
	}

	// The events are stored in ingestion order and the time-range filter above preserves it, so the
	// offset counts the events this request can see rather than every event in the stream.
	limit := body.Limit
	if limit <= 0 {
		limit = cwLogsEventsDefaultLimit
	}
	start, end := cwLogsEventPageBounds(cursor, tokenPresent, body.StartFromHead, len(filtered), limit)
	page := filtered[start:end]

	events := make([]cwOutputLogEventOut, 0, len(page))
	for _, ev := range page {
		events = append(events, cwOutputLogEventWire(ev))
	}

	// Neither token is omitempty, because neither has an empty form: both publish Length Constraints of
	// minimum 1 and the overview says *"The returned tokens are never null"*. The forward token names the
	// position after this page and the backward token the position before it, which is what makes the
	// published termination rule — the returned token equalling the one passed in — hold at either end
	// without being special-cased (#1223).
	type response struct {
		Events            []cwOutputLogEventOut `json:"events"`
		NextForwardToken  string                `json:"nextForwardToken"`
		NextBackwardToken string                `json:"nextBackwardToken"`
	}
	return cwLogsJSONResponse(http.StatusOK, response{
		Events:            events,
		NextForwardToken:  cwLogsEncodeEventToken(cwLogsEventCursor{Offset: end}),
		NextBackwardToken: cwLogsEncodeEventToken(cwLogsEventCursor{Offset: start, Backward: true}),
	})
}

func (p *CloudWatchLogsPlugin) filterLogEvents(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		LogGroupName   string   `json:"logGroupName"`
		LogStreamNames []string `json:"logStreamNames"`
		StartTime      int64    `json:"startTime"`
		EndTime        int64    `json:"endTime"`
		FilterPattern  string   `json:"filterPattern"`
		NextToken      string   `json:"nextToken"`
		Limit          int      `json:"limit"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, cwlInvalidBody()
		}
	}
	if body.LogGroupName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	// Above the token decode, for the reason cloudwatchlogs_not_found.go states (#1224). Only the group
	// is resolved: logStreamNames is a filter on the search rather than the resource the request
	// addresses, and the page publishes nothing that makes naming an absent stream in it a refusal.
	if err := p.requireLogGroup(goCtx, ctx.AccountID, ctx.Region, body.LogGroupName, "filterLogEvents"); err != nil {
		return nil, err
	}

	// Below the required-member refusal and the group resolution, and above every listing read, for the
	// reason cloudwatchlogs_pagination.go states (#1086).
	offset, tokenOK := decodeOffsetPaginationToken(body.NextToken)
	if !tokenOK {
		return nil, cwLogsInvalidPaginationToken("FilterLogEvents")
	}

	// Determine which streams to search.
	streamNames := body.LogStreamNames
	if len(streamNames) == 0 {
		idxKey := cwLogStreamNamesKey(ctx.AccountID, ctx.Region, body.LogGroupName)
		var idxErr error
		streamNames, idxErr = loadStringIndex(goCtx, p.state, cloudwatchLogsNamespace, idxKey)
		if idxErr != nil {
			return nil, fmt.Errorf("logs filterLogEvents loadIndex: %w", idxErr)
		}
	}

	type filteredEvent struct {
		LogStreamName string `json:"logStreamName"`
		Timestamp     int64  `json:"timestamp"`
		Message       string `json:"message"`
		IngestionTime int64  `json:"ingestionTime"`
		EventID       string `json:"eventId"`
	}

	var allEvents []filteredEvent
	for _, streamName := range streamNames {
		eventsKey := cwLogEventsKey(ctx.AccountID, ctx.Region, body.LogGroupName, streamName)
		eventsData, getErr := p.state.Get(goCtx, cloudwatchLogsNamespace, eventsKey)
		if getErr != nil || eventsData == nil {
			continue
		}
		var events []CWLogEvent
		if unmarshalErr := json.Unmarshal(eventsData, &events); unmarshalErr != nil {
			continue
		}
		for i, ev := range events {
			if body.StartTime > 0 && ev.Timestamp < body.StartTime {
				continue
			}
			if body.EndTime > 0 && ev.Timestamp >= body.EndTime {
				continue
			}
			if body.FilterPattern != "" && !strings.Contains(ev.Message, body.FilterPattern) {
				continue
			}
			allEvents = append(allEvents, filteredEvent{
				LogStreamName: streamName,
				Timestamp:     ev.Timestamp,
				Message:       ev.Message,
				IngestionTime: ev.IngestionTime,
				EventID:       fmt.Sprintf("%s-%s-%d", ctx.AccountID, streamName, i),
			})
		}
	}

	// Sort by timestamp for deterministic output.
	sort.Slice(allEvents, func(i, j int) bool {
		return allEvents[i].Timestamp < allEvents[j].Timestamp
	})

	// The sort above is what makes the offset stable across calls: the streams are walked in index
	// order and their events concatenated, so without it the page boundary would depend on that walk
	// rather than on the order the caller sees.
	limit := body.Limit
	if limit <= 0 {
		limit = cwLogsEventsDefaultLimit
	}
	page, nextToken := pageByOffsetToken(allEvents, offset, limit)
	if page == nil {
		page = []filteredEvent{}
	}

	type response struct {
		Events             []filteredEvent `json:"events"`
		SearchedLogStreams []struct {
			LogStreamName      string `json:"logStreamName"`
			SearchedCompletely bool   `json:"searchedCompletely"`
		} `json:"searchedLogStreams,omitempty"`
		NextToken string `json:"nextToken,omitempty"`
	}
	searched := make([]struct {
		LogStreamName      string `json:"logStreamName"`
		SearchedCompletely bool   `json:"searchedCompletely"`
	}, len(streamNames))
	for i, n := range streamNames {
		searched[i] = struct {
			LogStreamName      string `json:"logStreamName"`
			SearchedCompletely bool   `json:"searchedCompletely"`
		}{LogStreamName: n, SearchedCompletely: true}
	}
	return cwLogsJSONResponse(http.StatusOK, response{
		Events:             page,
		SearchedLogStreams: searched,
		NextToken:          nextToken,
	})
}

// --- State key helpers -------------------------------------------------------

func cwLogGroupKey(accountID, region, logGroupName string) string {
	return "loggroup:" + accountID + "/" + region + "/" + logGroupName
}

func cwLogGroupNamesKey(accountID, region string) string {
	return "loggroup_names:" + accountID + "/" + region
}

func cwLogStreamKey(accountID, region, logGroupName, logStreamName string) string {
	return "logstream:" + accountID + "/" + region + "/" + logGroupName + "/" + logStreamName
}

func cwLogStreamNamesKey(accountID, region, logGroupName string) string {
	return "logstream_names:" + accountID + "/" + region + "/" + logGroupName
}

func cwLogEventsKey(accountID, region, logGroupName, logStreamName string) string {
	return "logevents:" + accountID + "/" + region + "/" + logGroupName + "/" + logStreamName
}

// --- Response helper ---------------------------------------------------------

func cwLogsJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("cwLogsJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}

// --- Package-level string index helpers used by all observability plugins ---

// updateStringIndex appends name to the sorted []string JSON index stored at
// namespace/key. It is a no-op if name is already present.
func updateStringIndex(ctx context.Context, state StateManager, ns, key, name string) {
	data, _ := state.Get(ctx, ns, key)
	var names []string
	if data != nil {
		_ = json.Unmarshal(data, &names)
	}
	for _, n := range names {
		if n == name {
			return
		}
	}
	names = append(names, name)
	sort.Strings(names)
	b, _ := json.Marshal(names)
	_ = state.Put(ctx, ns, key, b)
}

// removeFromStringIndex removes name from the []string JSON index stored at
// namespace/key. It is a no-op if name is not present.
func removeFromStringIndex(ctx context.Context, state StateManager, ns, key, name string) {
	data, _ := state.Get(ctx, ns, key)
	if data == nil {
		return
	}
	var names []string
	if json.Unmarshal(data, &names) != nil {
		return
	}
	filtered := make([]string, 0, len(names))
	for _, n := range names {
		if n != name {
			filtered = append(filtered, n)
		}
	}
	b, _ := json.Marshal(filtered)
	_ = state.Put(ctx, ns, key, b)
}

// loadStringIndex reads and deserialises the []string JSON index stored at
// namespace/key. Returns nil, nil when the key does not exist.
func loadStringIndex(ctx context.Context, state StateManager, ns, key string) ([]string, error) {
	data, err := state.Get(ctx, ns, key)
	if err != nil {
		return nil, fmt.Errorf("loadStringIndex state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("loadStringIndex unmarshal: %w", err)
	}
	return names, nil
}
