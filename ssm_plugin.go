package substrate

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SSMPlugin emulates the AWS Systems Manager Parameter Store JSON-protocol API.
// It handles PutParameter, GetParameter, GetParameters, DeleteParameter,
// DeleteParameters, GetParametersByPath, DescribeParameters,
// GetParameterHistory, AddTagsToResource, RemoveTagsFromResource,
// ListTagsForResource, and LabelParameterVersion.
type SSMPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "ssm".
func (p *SSMPlugin) Name() string { return "ssm" }

// Initialize sets up the SSMPlugin with the provided configuration.
func (p *SSMPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for SSMPlugin.
func (p *SSMPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an SSM JSON-protocol request to the appropriate handler.
func (p *SSMPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "PutParameter":
		return p.putParameter(ctx, req)
	case "GetParameter":
		return p.getParameter(ctx, req)
	case "GetParameters":
		return p.getParameters(ctx, req)
	case "DeleteParameter":
		return p.deleteParameter(ctx, req)
	case "DeleteParameters":
		return p.deleteParameters(ctx, req)
	case "GetParametersByPath":
		return p.getParametersByPath(ctx, req)
	case "DescribeParameters":
		return p.describeParameters(ctx, req)
	case "GetParameterHistory":
		return p.getParameterHistory(ctx, req)
	case "AddTagsToResource":
		return p.addTagsToResource(ctx, req)
	case "RemoveTagsFromResource":
		return p.removeTagsFromResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	case "LabelParameterVersion":
		return p.labelParameterVersion(ctx, req)
	// Run Command operations
	case "SendCommand":
		return p.sendCommand(ctx, req)
	case "GetCommandInvocation":
		return p.getCommandInvocation(ctx, req)
	case "DescribeInstanceInformation":
		return p.describeInstanceInformation(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("SSMPlugin: unknown operation %q", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- State key helpers ---

func (p *SSMPlugin) paramKey(accountID, region, name string) string {
	return "parameter:" + accountID + "/" + region + "/" + name
}

func (p *SSMPlugin) paramPathsKey(accountID, region string) string {
	return "parameter_paths:" + accountID + "/" + region
}

// --- State helpers ---

func (p *SSMPlugin) loadParam(ctx context.Context, accountID, region, name string) (*SSMParameter, error) {
	data, err := p.state.Get(ctx, ssmNamespace, p.paramKey(accountID, region, name))
	if err != nil {
		return nil, fmt.Errorf("ssm loadParam state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var param SSMParameter
	if err := json.Unmarshal(data, &param); err != nil {
		return nil, fmt.Errorf("ssm loadParam unmarshal: %w", err)
	}
	return &param, nil
}

func (p *SSMPlugin) saveParam(ctx context.Context, param *SSMParameter) error {
	data, err := json.Marshal(param)
	if err != nil {
		return fmt.Errorf("ssm saveParam marshal: %w", err)
	}
	return p.state.Put(ctx, ssmNamespace, p.paramKey(param.AccountID, param.Region, param.Name), data)
}

func (p *SSMPlugin) loadPaths(ctx context.Context, accountID, region string) ([]string, error) {
	data, err := p.state.Get(ctx, ssmNamespace, p.paramPathsKey(accountID, region))
	if err != nil {
		return nil, fmt.Errorf("ssm loadPaths: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var paths []string
	if err := json.Unmarshal(data, &paths); err != nil {
		return nil, fmt.Errorf("ssm loadPaths unmarshal: %w", err)
	}
	return paths, nil
}

func (p *SSMPlugin) savePaths(ctx context.Context, accountID, region string, paths []string) error {
	sort.Strings(paths)
	data, err := json.Marshal(paths)
	if err != nil {
		return fmt.Errorf("ssm savePaths marshal: %w", err)
	}
	return p.state.Put(ctx, ssmNamespace, p.paramPathsKey(accountID, region), data)
}

// --- Operations ---

func (p *SSMPlugin) putParameter(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name        string   `json:"Name"`
		Value       string   `json:"Value"`
		Type        string   `json:"Type"`
		Description string   `json:"Description"`
		KeyID       string   `json:"KeyId"`
		Overwrite   bool     `json:"Overwrite"`
		Tags        []SSMTag `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	if input.Name == "" {
		return nil, &AWSError{Code: "ValidationException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	if input.Type == "" {
		input.Type = "String"
	}
	// Ensure name starts with /.
	if !strings.HasPrefix(input.Name, "/") {
		input.Name = "/" + input.Name
	}

	goCtx := context.Background()
	existing, err := p.loadParam(goCtx, ctx.AccountID, ctx.Region, input.Name)
	if err != nil {
		return nil, err
	}
	if existing != nil && !input.Overwrite {
		return nil, &AWSError{
			Code:       "ParameterAlreadyExists",
			Message:    fmt.Sprintf("Parameter %q already exists", input.Name),
			HTTPStatus: http.StatusConflict,
		}
	}

	version := int64(1)
	if existing != nil {
		version = existing.Version + 1
	}

	param := &SSMParameter{
		Name:             input.Name,
		Type:             input.Type,
		Value:            input.Value,
		Version:          version,
		Description:      input.Description,
		KeyID:            input.KeyID,
		LastModifiedDate: p.tc.Now(),
		Tags:             input.Tags,
		AccountID:        ctx.AccountID,
		Region:           ctx.Region,
		ARN:              ssmParameterARN(ctx.Region, ctx.AccountID, input.Name),
	}
	if existing != nil && len(param.Tags) == 0 {
		param.Tags = existing.Tags
	}

	if err := p.saveParam(goCtx, param); err != nil {
		return nil, fmt.Errorf("ssm putParameter saveParam: %w", err)
	}

	if existing == nil {
		paths, pathsErr := p.loadPaths(goCtx, ctx.AccountID, ctx.Region)
		if pathsErr != nil {
			return nil, pathsErr
		}
		paths = append(paths, input.Name)
		if err := p.savePaths(goCtx, ctx.AccountID, ctx.Region, paths); err != nil {
			return nil, fmt.Errorf("ssm putParameter savePaths: %w", err)
		}
	}

	out := map[string]interface{}{
		"Version": version,
		"Tier":    "Standard",
	}
	return ssmJSONResponse(http.StatusOK, out)
}

func (p *SSMPlugin) getParameter(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name           string `json:"Name"`
		WithDecryption bool   `json:"WithDecryption"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	name := input.Name
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}

	param, err := p.loadParam(context.Background(), ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if param == nil {
		return nil, &AWSError{
			Code:       "ParameterNotFound",
			Message:    fmt.Sprintf("Parameter %q not found", name),
			HTTPStatus: http.StatusNotFound,
		}
	}

	out := map[string]interface{}{
		"Parameter": map[string]interface{}{
			"Name":             param.Name,
			"Type":             param.Type,
			"Value":            param.Value,
			"Version":          param.Version,
			"ARN":              param.ARN,
			"LastModifiedDate": param.LastModifiedDate.Unix(),
		},
	}
	return ssmJSONResponse(http.StatusOK, out)
}

func (p *SSMPlugin) getParameters(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Names          []string `json:"Names"`
		WithDecryption bool     `json:"WithDecryption"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	type paramItem struct {
		Name    string `json:"Name"`
		Type    string `json:"Type"`
		Value   string `json:"Value"`
		Version int64  `json:"Version"`
		ARN     string `json:"ARN"`
	}
	var found []paramItem
	var invalid []string

	for _, rawName := range input.Names {
		name := rawName
		if !strings.HasPrefix(name, "/") {
			name = "/" + name
		}
		param, loadErr := p.loadParam(goCtx, ctx.AccountID, ctx.Region, name)
		if loadErr != nil || param == nil {
			invalid = append(invalid, rawName)
			continue
		}
		found = append(found, paramItem{
			Name:    param.Name,
			Type:    param.Type,
			Value:   param.Value,
			Version: param.Version,
			ARN:     param.ARN,
		})
	}

	out := map[string]interface{}{
		"Parameters":        found,
		"InvalidParameters": invalid,
	}
	return ssmJSONResponse(http.StatusOK, out)
}

func (p *SSMPlugin) deleteParameter(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	name := input.Name
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}

	goCtx := context.Background()
	existing, err := p.loadParam(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, &AWSError{Code: "ParameterNotFound", Message: "Parameter not found", HTTPStatus: http.StatusNotFound}
	}

	_ = p.state.Delete(goCtx, ssmNamespace, p.paramKey(ctx.AccountID, ctx.Region, name))

	paths, err := p.loadPaths(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	newPaths := make([]string, 0, len(paths))
	for _, p2 := range paths {
		if p2 != name {
			newPaths = append(newPaths, p2)
		}
	}
	if err := p.savePaths(goCtx, ctx.AccountID, ctx.Region, newPaths); err != nil {
		return nil, fmt.Errorf("ssm deleteParameter savePaths: %w", err)
	}

	return ssmJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SSMPlugin) deleteParameters(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Names []string `json:"Names"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	var deleted []string
	var invalid []string

	paths, err := p.loadPaths(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}

	for _, rawName := range input.Names {
		name := rawName
		if !strings.HasPrefix(name, "/") {
			name = "/" + name
		}
		existing, loadErr := p.loadParam(goCtx, ctx.AccountID, ctx.Region, name)
		if loadErr != nil || existing == nil {
			invalid = append(invalid, rawName)
			continue
		}
		_ = p.state.Delete(goCtx, ssmNamespace, p.paramKey(ctx.AccountID, ctx.Region, name))
		deleted = append(deleted, name)
		newPaths := paths[:0]
		for _, pth := range paths {
			if pth != name {
				newPaths = append(newPaths, pth)
			}
		}
		paths = newPaths
	}

	if err := p.savePaths(goCtx, ctx.AccountID, ctx.Region, paths); err != nil {
		return nil, fmt.Errorf("ssm deleteParameters savePaths: %w", err)
	}

	out := map[string]interface{}{
		"DeletedParameters": deleted,
		"InvalidParameters": invalid,
	}
	return ssmJSONResponse(http.StatusOK, out)
}

func (p *SSMPlugin) getParametersByPath(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Path           string `json:"Path"`
		Recursive      bool   `json:"Recursive"`
		WithDecryption bool   `json:"WithDecryption"`
		MaxResults     int    `json:"MaxResults"`
		NextToken      string `json:"NextToken"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	if input.MaxResults <= 0 {
		input.MaxResults = 10
	}
	path := input.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	goCtx := context.Background()
	paths, err := p.loadPaths(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}

	var matched []string
	for _, pth := range paths {
		if input.Recursive {
			if strings.HasPrefix(pth, path) {
				matched = append(matched, pth)
			}
		} else {
			// Non-recursive: only direct children (no additional slashes after prefix).
			if strings.HasPrefix(pth, path) {
				rest := pth[len(path):]
				if !strings.Contains(rest, "/") {
					matched = append(matched, pth)
				}
			}
		}
	}
	sort.Strings(matched)

	// Pagination.
	offset := 0
	if input.NextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(input.NextToken); decErr == nil {
			if n, parseErr := strconv.Atoi(string(decoded)); parseErr == nil && n >= 0 {
				offset = n
			}
		}
	}
	if offset > len(matched) {
		offset = len(matched)
	}
	page := matched[offset:]
	var nextToken string
	if len(page) > input.MaxResults {
		page = page[:input.MaxResults]
		nextOffset := offset + input.MaxResults
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(nextOffset)))
	}

	type paramItem struct {
		Name    string `json:"Name"`
		Type    string `json:"Type"`
		Value   string `json:"Value"`
		Version int64  `json:"Version"`
		ARN     string `json:"ARN"`
	}
	var results []paramItem
	for _, pth := range page {
		param, loadErr := p.loadParam(goCtx, ctx.AccountID, ctx.Region, pth)
		if loadErr != nil || param == nil {
			continue
		}
		results = append(results, paramItem{
			Name:    param.Name,
			Type:    param.Type,
			Value:   param.Value,
			Version: param.Version,
			ARN:     param.ARN,
		})
	}

	out := map[string]interface{}{
		"Parameters": results,
	}
	if nextToken != "" {
		out["NextToken"] = nextToken
	}
	return ssmJSONResponse(http.StatusOK, out)
}

func (p *SSMPlugin) describeParameters(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		MaxResults int    `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input) //nolint:errcheck // optional body
	}
	if input.MaxResults <= 0 {
		input.MaxResults = 10
	}

	goCtx := context.Background()
	paths, err := p.loadPaths(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)

	offset := 0
	if input.NextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(input.NextToken); decErr == nil {
			if n, parseErr := strconv.Atoi(string(decoded)); parseErr == nil && n >= 0 {
				offset = n
			}
		}
	}
	if offset > len(paths) {
		offset = len(paths)
	}
	page := paths[offset:]
	var nextToken string
	if len(page) > input.MaxResults {
		page = page[:input.MaxResults]
		nextOffset := offset + input.MaxResults
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(nextOffset)))
	}

	type paramMeta struct {
		Name        string `json:"Name"`
		Type        string `json:"Type"`
		Description string `json:"Description,omitempty"`
		Version     int64  `json:"Version"`
	}
	var metas []paramMeta
	for _, pth := range page {
		param, loadErr := p.loadParam(goCtx, ctx.AccountID, ctx.Region, pth)
		if loadErr != nil || param == nil {
			continue
		}
		metas = append(metas, paramMeta{
			Name:        param.Name,
			Type:        param.Type,
			Description: param.Description,
			Version:     param.Version,
		})
	}

	out := map[string]interface{}{
		"Parameters": metas,
	}
	if nextToken != "" {
		out["NextToken"] = nextToken
	}
	return ssmJSONResponse(http.StatusOK, out)
}

func (p *SSMPlugin) getParameterHistory(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	name := input.Name
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}

	param, err := p.loadParam(context.Background(), ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if param == nil {
		return nil, &AWSError{Code: "ParameterNotFound", Message: "Parameter not found", HTTPStatus: http.StatusNotFound}
	}

	// Stub: return single entry for current version.
	type historyEntry struct {
		Name    string `json:"Name"`
		Type    string `json:"Type"`
		Value   string `json:"Value"`
		Version int64  `json:"Version"`
	}
	out := map[string]interface{}{
		"Parameters": []historyEntry{{
			Name:    param.Name,
			Type:    param.Type,
			Value:   param.Value,
			Version: param.Version,
		}},
	}
	return ssmJSONResponse(http.StatusOK, out)
}

func (p *SSMPlugin) addTagsToResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceType string   `json:"ResourceType"`
		ResourceID   string   `json:"ResourceId"`
		Tags         []SSMTag `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	name := input.ResourceID
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}

	goCtx := context.Background()
	param, err := p.loadParam(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if param == nil {
		return nil, &AWSError{Code: "InvalidResourceId", Message: "Parameter not found", HTTPStatus: http.StatusNotFound}
	}

	tagMap := make(map[string]string, len(param.Tags))
	for _, t := range param.Tags {
		tagMap[t.Key] = t.Value
	}
	for _, t := range input.Tags {
		tagMap[t.Key] = t.Value
	}
	newTags := make([]SSMTag, 0, len(tagMap))
	for k, v := range tagMap {
		newTags = append(newTags, SSMTag{Key: k, Value: v})
	}
	param.Tags = newTags

	if err := p.saveParam(goCtx, param); err != nil {
		return nil, fmt.Errorf("ssm addTagsToResource saveParam: %w", err)
	}
	return ssmJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SSMPlugin) removeTagsFromResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceType string   `json:"ResourceType"`
		ResourceID   string   `json:"ResourceId"`
		TagKeys      []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	name := input.ResourceID
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}

	goCtx := context.Background()
	param, err := p.loadParam(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if param == nil {
		return nil, &AWSError{Code: "InvalidResourceId", Message: "Parameter not found", HTTPStatus: http.StatusNotFound}
	}

	removeSet := make(map[string]bool, len(input.TagKeys))
	for _, k := range input.TagKeys {
		removeSet[k] = true
	}
	newTags := make([]SSMTag, 0, len(param.Tags))
	for _, t := range param.Tags {
		if !removeSet[t.Key] {
			newTags = append(newTags, t)
		}
	}
	param.Tags = newTags

	if err := p.saveParam(goCtx, param); err != nil {
		return nil, fmt.Errorf("ssm removeTagsFromResource saveParam: %w", err)
	}
	return ssmJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SSMPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceType string `json:"ResourceType"`
		ResourceID   string `json:"ResourceId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	name := input.ResourceID
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}

	param, err := p.loadParam(context.Background(), ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if param == nil {
		return nil, &AWSError{Code: "InvalidResourceId", Message: "Parameter not found", HTTPStatus: http.StatusNotFound}
	}

	out := map[string]interface{}{
		"TagList": param.Tags,
	}
	return ssmJSONResponse(http.StatusOK, out)
}

func (p *SSMPlugin) labelParameterVersion(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	// Stub: labels are not tracked; succeed silently.
	return ssmJSONResponse(http.StatusOK, map[string]interface{}{
		"InvalidLabels":    []string{},
		"ParameterVersion": 1,
	})
}

// --- Response helper ---

// ssmJSONResponse builds an AWSResponse with a JSON body for SSM using
// Content-Type: application/x-amz-json-1.1.
func ssmJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("ssm marshal response: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}

// --- Run Command operations ---

// ssmCommandKey returns the state key for an SSM command record.
func ssmCommandKey(accountID, region, commandID string) string {
	return "command:" + accountID + "/" + region + "/" + commandID
}

// ssmInvocationKey returns the state key for a command invocation record.
func ssmInvocationKey(accountID, region, commandID, instanceID string) string {
	return "invocation:" + accountID + "/" + region + "/" + commandID + "/" + instanceID
}

// SSMCommand holds persisted state for an SSM Run Command submission.
type SSMCommand struct {
	// CommandID is the unique identifier for this command.
	CommandID string `json:"CommandId"`

	// DocumentName is the name of the SSM document (e.g. "AWS-RunShellScript").
	DocumentName string `json:"DocumentName"`

	// InstanceIDs holds the target instance identifiers.
	InstanceIDs []string `json:"InstanceIds"`

	// Parameters holds the document parameters.
	Parameters map[string][]string `json:"Parameters"`

	// Status is the overall command status.
	Status string `json:"Status"`

	// RequestedDateTime is the Unix epoch seconds when the command was submitted.
	// The Go SDK smithy deserializer expects float64 (not RFC3339) for DateTime fields.
	RequestedDateTime float64 `json:"RequestedDateTime"`
}

// SSMCommandInvocation holds the per-instance output of an SSM command.
type SSMCommandInvocation struct {
	// CommandID is the parent command identifier.
	CommandID string `json:"CommandId"`

	// InstanceID is the target instance.
	InstanceID string `json:"InstanceId"`

	// Status is the invocation status ("Success", "Failed", etc.).
	Status string `json:"Status"`

	// StandardOutputContent is the captured stdout.
	StandardOutputContent string `json:"StandardOutputContent"`

	// StandardErrorContent is the captured stderr.
	StandardErrorContent string `json:"StandardErrorContent"`

	// DocumentName is the SSM document that was executed.
	DocumentName string `json:"DocumentName"`
}

func (p *SSMPlugin) sendCommand(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DocumentName string              `json:"DocumentName"`
		InstanceIDs  []string            `json:"InstanceIds"`
		Parameters   map[string][]string `json:"Parameters"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if input.DocumentName == "" {
		return nil, &AWSError{Code: "InvalidDocument", Message: "DocumentName is required", HTTPStatus: http.StatusBadRequest}
	}

	commandID := generateSSMCommandID()
	nowUnix := float64(p.tc.Now().Unix())

	cmd := SSMCommand{
		CommandID:         commandID,
		DocumentName:      input.DocumentName,
		InstanceIDs:       input.InstanceIDs,
		Parameters:        input.Parameters,
		Status:            "Success",
		RequestedDateTime: nowUnix,
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("ssm sendCommand marshal: %w", err)
	}
	goCtx := context.Background()
	if err := p.state.Put(goCtx, ssmNamespace, ssmCommandKey(ctx.AccountID, ctx.Region, commandID), data); err != nil {
		return nil, fmt.Errorf("ssm sendCommand state.Put: %w", err)
	}

	// Create per-instance invocation records immediately (Success in test mode).
	for _, instID := range input.InstanceIDs {
		inv := SSMCommandInvocation{
			CommandID:             commandID,
			InstanceID:            instID,
			Status:                "Success",
			StandardOutputContent: "",
			StandardErrorContent:  "",
			DocumentName:          input.DocumentName,
		}
		invData, _ := json.Marshal(inv)
		_ = p.state.Put(goCtx, ssmNamespace, ssmInvocationKey(ctx.AccountID, ctx.Region, commandID, instID), invData)
	}

	return ssmJSONResponse(http.StatusOK, map[string]any{
		"Command": map[string]any{
			"CommandId":         commandID,
			"DocumentName":      cmd.DocumentName,
			"InstanceIds":       cmd.InstanceIDs,
			"Parameters":        cmd.Parameters,
			"Status":            cmd.Status,
			"RequestedDateTime": cmd.RequestedDateTime,
		},
	})
}

func (p *SSMPlugin) getCommandInvocation(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		CommandID  string `json:"CommandId"`
		InstanceID string `json:"InstanceId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "SerializationException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if input.CommandID == "" || input.InstanceID == "" {
		return nil, &AWSError{Code: "InvalidCommandId", Message: "CommandId and InstanceId are required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	data, err := p.state.Get(goCtx, ssmNamespace, ssmInvocationKey(ctx.AccountID, ctx.Region, input.CommandID, input.InstanceID))
	if err != nil {
		return nil, fmt.Errorf("ssm getCommandInvocation state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{
			Code:       "InvocationDoesNotExist",
			Message:    "The command " + input.CommandID + " has not been run on instance " + input.InstanceID,
			HTTPStatus: http.StatusBadRequest,
		}
	}
	var inv SSMCommandInvocation
	if err := json.Unmarshal(data, &inv); err != nil {
		return nil, fmt.Errorf("ssm getCommandInvocation unmarshal: %w", err)
	}
	return ssmJSONResponse(http.StatusOK, map[string]any{
		"CommandId":             inv.CommandID,
		"InstanceId":            inv.InstanceID,
		"Status":                inv.Status,
		"StandardOutputContent": inv.StandardOutputContent,
		"StandardErrorContent":  inv.StandardErrorContent,
		"DocumentName":          inv.DocumentName,
	})
}

// describeInstanceInformation returns all EC2 instances in the region as
// SSM-managed. In test mode every running instance is considered to have
// the SSM agent installed and active.
func (p *SSMPlugin) describeInstanceInformation(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	keys, err := p.state.List(goCtx, ec2Namespace, "instance:"+ctx.AccountID+"/"+ctx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ssm describeInstanceInformation list: %w", err)
	}

	type instanceInfo struct {
		InstanceID      string `json:"InstanceId"`
		PingStatus      string `json:"PingStatus"`
		AgentVersion    string `json:"AgentVersion"`
		PlatformType    string `json:"PlatformType"`
		PlatformName    string `json:"PlatformName"`
		PlatformVersion string `json:"PlatformVersion"`
		IsLatestVersion bool   `json:"IsLatestVersion"`
	}

	infos := make([]instanceInfo, 0, len(keys))
	for _, k := range keys {
		data, getErr := p.state.Get(goCtx, ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var inst EC2Instance
		if err := json.Unmarshal(data, &inst); err != nil {
			continue
		}
		if inst.State.Name != "running" {
			continue
		}
		infos = append(infos, instanceInfo{
			InstanceID:      inst.InstanceID,
			PingStatus:      "Online",
			AgentVersion:    "3.2.0.0",
			PlatformType:    "Linux",
			PlatformName:    "Amazon Linux",
			PlatformVersion: "2",
			IsLatestVersion: true,
		})
	}

	return ssmJSONResponse(http.StatusOK, map[string]any{
		"InstanceInformationList": infos,
	})
}

// generateSSMCommandID generates a UUID-format command ID.
func generateSSMCommandID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("ssm: rand.Read: %v", err))
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
