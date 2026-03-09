package substrate

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CloudWatchLogsPlugin emulates the Amazon CloudWatch Logs JSON-protocol API.
// It handles CreateLogGroup, DeleteLogGroup, DescribeLogGroups, CreateLogStream,
// DeleteLogStream, DescribeLogStreams, PutLogEvents, GetLogEvents, and
// FilterLogEvents.
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
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("CloudWatchLogsPlugin: unknown operation %q", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
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
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Log group not found: " + body.LogGroupName, HTTPStatus: http.StatusNotFound}
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
		_ = json.Unmarshal(req.Body, &body)
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

	// Pagination.
	limit := body.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := 0
	if body.NextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(body.NextToken); decErr == nil {
			if n, atoiErr := strconv.Atoi(string(decoded)); atoiErr == nil && n > 0 {
				offset = n
			}
		}
	}
	if offset > len(names) {
		offset = len(names)
	}

	end := offset + limit
	var nextToken string
	if end < len(names) {
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	} else {
		end = len(names)
	}

	groups := make([]CWLogGroup, 0, end-offset)
	for _, name := range names[offset:end] {
		data, getErr := p.state.Get(goCtx, cloudwatchLogsNamespace, cwLogGroupKey(ctx.AccountID, ctx.Region, name))
		if getErr != nil || data == nil {
			continue
		}
		var lg CWLogGroup
		if unmarshalErr := json.Unmarshal(data, &lg); unmarshalErr != nil {
			continue
		}
		groups = append(groups, lg)
	}

	type response struct {
		LogGroups []CWLogGroup `json:"logGroups"`
		NextToken string       `json:"nextToken,omitempty"`
	}
	return cwLogsJSONResponse(http.StatusOK, response{LogGroups: groups, NextToken: nextToken})
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
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Log group not found: " + body.LogGroupName, HTTPStatus: http.StatusNotFound}
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
		UploadSequenceToken: generateLambdaRevisionID(),
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
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Log stream not found: " + body.LogStreamName, HTTPStatus: http.StatusNotFound}
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
		_ = json.Unmarshal(req.Body, &body)
	}
	if body.LogGroupName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
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

	limit := body.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := 0
	if body.NextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(body.NextToken); decErr == nil {
			if n, atoiErr := strconv.Atoi(string(decoded)); atoiErr == nil && n > 0 {
				offset = n
			}
		}
	}
	if offset > len(names) {
		offset = len(names)
	}

	end := offset + limit
	var nextToken string
	if end < len(names) {
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	} else {
		end = len(names)
	}

	streams := make([]CWLogStream, 0, end-offset)
	for _, name := range names[offset:end] {
		data, getErr := p.state.Get(goCtx, cloudwatchLogsNamespace, cwLogStreamKey(ctx.AccountID, ctx.Region, body.LogGroupName, name))
		if getErr != nil || data == nil {
			continue
		}
		var ls CWLogStream
		if unmarshalErr := json.Unmarshal(data, &ls); unmarshalErr != nil {
			continue
		}
		streams = append(streams, ls)
	}

	type response struct {
		LogStreams []CWLogStream `json:"logStreams"`
		NextToken  string        `json:"nextToken,omitempty"`
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
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Log stream not found: " + body.LogStreamName, HTTPStatus: http.StatusNotFound}
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
		ls.UploadSequenceToken = generateLambdaRevisionID()
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
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}
	if body.LogGroupName == "" || body.LogStreamName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName and logStreamName are required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
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

	limit := body.Limit
	if limit <= 0 {
		limit = 10000
	}
	offset := 0
	if body.NextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(body.NextToken); decErr == nil {
			if n, atoiErr := strconv.Atoi(string(decoded)); atoiErr == nil && n > 0 {
				offset = n
			}
		}
	}
	if offset > len(filtered) {
		offset = len(filtered)
	}

	end := offset + limit
	var nextToken string
	if end < len(filtered) {
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	} else {
		end = len(filtered)
	}

	type response struct {
		Events            []CWLogEvent `json:"events"`
		NextForwardToken  string       `json:"nextForwardToken,omitempty"`
		NextBackwardToken string       `json:"nextBackwardToken,omitempty"`
	}
	return cwLogsJSONResponse(http.StatusOK, response{
		Events:           filtered[offset:end],
		NextForwardToken: nextToken,
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
		_ = json.Unmarshal(req.Body, &body)
	}
	if body.LogGroupName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "logGroupName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

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

	limit := body.Limit
	if limit <= 0 {
		limit = 10000
	}
	offset := 0
	if body.NextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(body.NextToken); decErr == nil {
			if n, atoiErr := strconv.Atoi(string(decoded)); atoiErr == nil && n > 0 {
				offset = n
			}
		}
	}
	if offset > len(allEvents) {
		offset = len(allEvents)
	}

	end := offset + limit
	var nextToken string
	if end < len(allEvents) {
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	} else {
		end = len(allEvents)
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
		Events:             allEvents[offset:end],
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
