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

// SecretsManagerPlugin emulates the AWS Secrets Manager JSON-protocol API.
// It handles CreateSecret, GetSecretValue, PutSecretValue, DescribeSecret,
// UpdateSecret, DeleteSecret, ListSecrets, ListSecretVersionIds,
// TagResource, UntagResource, ListTagsForResource, and RotateSecret.
type SecretsManagerPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "secretsmanager".
func (p *SecretsManagerPlugin) Name() string { return "secretsmanager" }

// Initialize sets up the SecretsManagerPlugin with the provided configuration.
func (p *SecretsManagerPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for SecretsManagerPlugin.
func (p *SecretsManagerPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Secrets Manager JSON-protocol request to the appropriate handler.
func (p *SecretsManagerPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateSecret":
		return p.createSecret(ctx, req)
	case "GetSecretValue":
		return p.getSecretValue(ctx, req)
	case "PutSecretValue":
		return p.putSecretValue(ctx, req)
	case "DescribeSecret":
		return p.describeSecret(ctx, req)
	case "UpdateSecret":
		return p.updateSecret(ctx, req)
	case "DeleteSecret":
		return p.deleteSecret(ctx, req)
	case "ListSecrets":
		return p.listSecrets(ctx, req)
	case "ListSecretVersionIds":
		return p.listSecretVersionIDs(ctx, req)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	case "RotateSecret":
		return p.rotateSecret(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "UnrecognizedClientException",
			Message:    fmt.Sprintf("SecretsManagerPlugin: unknown operation %q", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- State key helpers ---

func (p *SecretsManagerPlugin) secretKey(accountID, region, name string) string {
	return "secret:" + accountID + "/" + region + "/" + name
}

func (p *SecretsManagerPlugin) secretNamesKey(accountID, region string) string {
	return "secret_names:" + accountID + "/" + region
}

func (p *SecretsManagerPlugin) versionKey(accountID, region, name, versionID string) string {
	return "secret_version:" + accountID + "/" + region + "/" + name + "/" + versionID
}

// --- State helpers ---

func (p *SecretsManagerPlugin) loadSecret(ctx context.Context, accountID, region, name string) (*SecretState, error) {
	data, err := p.state.Get(ctx, secretsManagerNamespace, p.secretKey(accountID, region, name))
	if err != nil {
		return nil, fmt.Errorf("sm loadSecret state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var s SecretState
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("sm loadSecret unmarshal: %w", err)
	}
	return &s, nil
}

func (p *SecretsManagerPlugin) saveSecret(ctx context.Context, s *SecretState) error {
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("sm saveSecret marshal: %w", err)
	}
	return p.state.Put(ctx, secretsManagerNamespace, p.secretKey(s.AccountID, s.Region, s.Name), data)
}

func (p *SecretsManagerPlugin) loadSecretNames(ctx context.Context, accountID, region string) ([]string, error) {
	data, err := p.state.Get(ctx, secretsManagerNamespace, p.secretNamesKey(accountID, region))
	if err != nil {
		return nil, fmt.Errorf("sm loadSecretNames: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("sm loadSecretNames unmarshal: %w", err)
	}
	return names, nil
}

func (p *SecretsManagerPlugin) saveSecretNames(ctx context.Context, accountID, region string, names []string) error {
	sort.Strings(names)
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("sm saveSecretNames marshal: %w", err)
	}
	return p.state.Put(ctx, secretsManagerNamespace, p.secretNamesKey(accountID, region), data)
}

// resolveSecretID resolves a SecretId (name or ARN) to the secret name.
func resolveSecretID(secretID string) string {
	// If it's an ARN, extract the name after the last ":".
	if strings.HasPrefix(secretID, "arn:") {
		parts := strings.Split(secretID, ":")
		if len(parts) >= 7 {
			// ARN: arn:aws:secretsmanager:{region}:{acct}:secret:{name}
			return parts[len(parts)-1]
		}
	}
	return secretID
}

// --- Operations ---

func (p *SecretsManagerPlugin) createSecret(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name         string  `json:"Name"`
		Description  string  `json:"Description"`
		KmsKeyID     string  `json:"KmsKeyId"`
		SecretString string  `json:"SecretString"`
		SecretBinary string  `json:"SecretBinary"`
		Tags         []SMTag `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	if input.Name == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	existing, err := p.loadSecret(goCtx, ctx.AccountID, ctx.Region, input.Name)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, &AWSError{
			Code:       "ResourceExistsException",
			Message:    fmt.Sprintf("A resource with the ID %q already exists", input.Name),
			HTTPStatus: http.StatusConflict,
		}
	}

	now := p.tc.Now()
	arn := generateSecretARN(ctx.Region, ctx.AccountID, input.Name)
	versionID := generateVersionID()

	secret := &SecretState{
		ARN:              arn,
		Name:             input.Name,
		Description:      input.Description,
		KMSKeyID:         input.KmsKeyID,
		Tags:             input.Tags,
		CurrentVersionID: versionID,
		AccountID:        ctx.AccountID,
		Region:           ctx.Region,
		CreatedDate:      now,
		LastChangedDate:  now,
	}

	if err := p.saveSecret(goCtx, secret); err != nil {
		return nil, fmt.Errorf("sm createSecret saveSecret: %w", err)
	}

	// Store secret value.
	value := input.SecretString
	if value == "" {
		value = input.SecretBinary
	}
	if value != "" {
		if err := p.state.Put(goCtx, secretsManagerNamespace, p.versionKey(ctx.AccountID, ctx.Region, input.Name, versionID), []byte(value)); err != nil {
			return nil, fmt.Errorf("sm createSecret store value: %w", err)
		}
	}

	names, err := p.loadSecretNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	names = append(names, input.Name)
	if err := p.saveSecretNames(goCtx, ctx.AccountID, ctx.Region, names); err != nil {
		return nil, fmt.Errorf("sm createSecret saveSecretNames: %w", err)
	}

	out := map[string]interface{}{
		"ARN":       arn,
		"Name":      input.Name,
		"VersionId": versionID,
	}
	return smJSONResponse(http.StatusOK, out)
}

func (p *SecretsManagerPlugin) getSecretValue(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		SecretID  string `json:"SecretId"`
		VersionID string `json:"VersionId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := resolveSecretID(input.SecretID)
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Secret not found", HTTPStatus: http.StatusNotFound}
	}

	versionID := input.VersionID
	if versionID == "" {
		versionID = secret.CurrentVersionID
	}

	valueData, err := p.state.Get(goCtx, secretsManagerNamespace, p.versionKey(ctx.AccountID, ctx.Region, name, versionID))
	if err != nil {
		return nil, fmt.Errorf("sm getSecretValue get value: %w", err)
	}

	out := map[string]interface{}{
		"ARN":          secret.ARN,
		"Name":         secret.Name,
		"VersionId":    versionID,
		"CreatedDate":  secret.CreatedDate.Unix(),
		"SecretString": "",
	}
	if valueData != nil {
		out["SecretString"] = string(valueData)
	}
	return smJSONResponse(http.StatusOK, out)
}

func (p *SecretsManagerPlugin) putSecretValue(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		SecretID     string `json:"SecretId"`
		SecretString string `json:"SecretString"`
		SecretBinary string `json:"SecretBinary"`
		VersionID    string `json:"ClientRequestToken"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := resolveSecretID(input.SecretID)
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Secret not found", HTTPStatus: http.StatusNotFound}
	}

	versionID := generateVersionID()
	value := input.SecretString
	if value == "" {
		value = input.SecretBinary
	}
	if err := p.state.Put(goCtx, secretsManagerNamespace, p.versionKey(ctx.AccountID, ctx.Region, name, versionID), []byte(value)); err != nil {
		return nil, fmt.Errorf("sm putSecretValue store value: %w", err)
	}

	secret.CurrentVersionID = versionID
	secret.LastChangedDate = p.tc.Now()
	if err := p.saveSecret(goCtx, secret); err != nil {
		return nil, fmt.Errorf("sm putSecretValue saveSecret: %w", err)
	}

	out := map[string]interface{}{
		"ARN":       secret.ARN,
		"Name":      secret.Name,
		"VersionId": versionID,
	}
	return smJSONResponse(http.StatusOK, out)
}

func (p *SecretsManagerPlugin) describeSecret(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		SecretID string `json:"SecretId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := resolveSecretID(input.SecretID)
	secret, err := p.loadSecret(context.Background(), ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Secret not found", HTTPStatus: http.StatusNotFound}
	}

	out := map[string]interface{}{
		"ARN":             secret.ARN,
		"Name":            secret.Name,
		"Description":     secret.Description,
		"KmsKeyId":        secret.KMSKeyID,
		"RotationEnabled": secret.RotationEnabled,
		"CreatedDate":     secret.CreatedDate.Unix(),
		"LastChangedDate": secret.LastChangedDate.Unix(),
		"Tags":            secret.Tags,
	}
	return smJSONResponse(http.StatusOK, out)
}

func (p *SecretsManagerPlugin) updateSecret(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		SecretID     string `json:"SecretId"`
		Description  string `json:"Description"`
		KmsKeyID     string `json:"KmsKeyId"`
		SecretString string `json:"SecretString"`
		SecretBinary string `json:"SecretBinary"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := resolveSecretID(input.SecretID)
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Secret not found", HTTPStatus: http.StatusNotFound}
	}

	if input.Description != "" {
		secret.Description = input.Description
	}
	if input.KmsKeyID != "" {
		secret.KMSKeyID = input.KmsKeyID
	}

	versionID := secret.CurrentVersionID
	value := input.SecretString
	if value == "" {
		value = input.SecretBinary
	}
	if value != "" {
		versionID = generateVersionID()
		if err := p.state.Put(goCtx, secretsManagerNamespace, p.versionKey(ctx.AccountID, ctx.Region, name, versionID), []byte(value)); err != nil {
			return nil, fmt.Errorf("sm updateSecret store value: %w", err)
		}
		secret.CurrentVersionID = versionID
	}

	secret.LastChangedDate = p.tc.Now()
	if err := p.saveSecret(goCtx, secret); err != nil {
		return nil, fmt.Errorf("sm updateSecret saveSecret: %w", err)
	}

	out := map[string]interface{}{
		"ARN":       secret.ARN,
		"Name":      secret.Name,
		"VersionId": versionID,
	}
	return smJSONResponse(http.StatusOK, out)
}

func (p *SecretsManagerPlugin) deleteSecret(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		SecretID string `json:"SecretId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := resolveSecretID(input.SecretID)
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Secret not found", HTTPStatus: http.StatusNotFound}
	}

	_ = p.state.Delete(goCtx, secretsManagerNamespace, p.secretKey(ctx.AccountID, ctx.Region, name))
	_ = p.state.Delete(goCtx, secretsManagerNamespace, p.versionKey(ctx.AccountID, ctx.Region, name, secret.CurrentVersionID))

	names, err := p.loadSecretNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != name {
			newNames = append(newNames, n)
		}
	}
	if err := p.saveSecretNames(goCtx, ctx.AccountID, ctx.Region, newNames); err != nil {
		return nil, fmt.Errorf("sm deleteSecret saveSecretNames: %w", err)
	}

	out := map[string]interface{}{
		"ARN":  secret.ARN,
		"Name": secret.Name,
	}
	return smJSONResponse(http.StatusOK, out)
}

func (p *SecretsManagerPlugin) listSecrets(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		MaxResults int    `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input) //nolint:errcheck // optional body
	}
	if input.MaxResults <= 0 {
		input.MaxResults = 100
	}

	goCtx := context.Background()
	names, err := p.loadSecretNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)

	offset := 0
	if input.NextToken != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(input.NextToken); decErr == nil {
			if n, parseErr := strconv.Atoi(string(decoded)); parseErr == nil && n >= 0 {
				offset = n
			}
		}
	}
	if offset > len(names) {
		offset = len(names)
	}
	page := names[offset:]
	var nextToken string
	if len(page) > input.MaxResults {
		page = page[:input.MaxResults]
		nextOffset := offset + input.MaxResults
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(nextOffset)))
	}

	type secretEntry struct {
		ARN  string `json:"ARN"`
		Name string `json:"Name"`
	}
	entries := make([]secretEntry, 0, len(page))
	for _, name := range page {
		s, loadErr := p.loadSecret(goCtx, ctx.AccountID, ctx.Region, name)
		if loadErr != nil || s == nil {
			continue
		}
		entries = append(entries, secretEntry{ARN: s.ARN, Name: s.Name})
	}

	out := map[string]interface{}{
		"SecretList": entries,
	}
	if nextToken != "" {
		out["NextToken"] = nextToken
	}
	return smJSONResponse(http.StatusOK, out)
}

func (p *SecretsManagerPlugin) listSecretVersionIDs(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		SecretID string `json:"SecretId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := resolveSecretID(input.SecretID)
	secret, err := p.loadSecret(context.Background(), ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Secret not found", HTTPStatus: http.StatusNotFound}
	}

	// Stub: return only the current version.
	type versionEntry struct {
		VersionID string   `json:"VersionId"`
		Stages    []string `json:"VersionStages"`
	}
	out := map[string]interface{}{
		"ARN":      secret.ARN,
		"Name":     secret.Name,
		"Versions": []versionEntry{{VersionID: secret.CurrentVersionID, Stages: []string{"AWSCURRENT"}}},
	}
	return smJSONResponse(http.StatusOK, out)
}

func (p *SecretsManagerPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		SecretID string  `json:"SecretId"`
		Tags     []SMTag `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := resolveSecretID(input.SecretID)
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Secret not found", HTTPStatus: http.StatusNotFound}
	}

	tagMap := make(map[string]string, len(secret.Tags))
	for _, t := range secret.Tags {
		tagMap[t.Key] = t.Value
	}
	for _, t := range input.Tags {
		tagMap[t.Key] = t.Value
	}
	newTags := make([]SMTag, 0, len(tagMap))
	for k, v := range tagMap {
		newTags = append(newTags, SMTag{Key: k, Value: v})
	}
	secret.Tags = newTags

	if err := p.saveSecret(goCtx, secret); err != nil {
		return nil, fmt.Errorf("sm tagResource saveSecret: %w", err)
	}
	return smJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SecretsManagerPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		SecretID string   `json:"SecretId"`
		TagKeys  []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := resolveSecretID(input.SecretID)
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Secret not found", HTTPStatus: http.StatusNotFound}
	}

	removeSet := make(map[string]bool, len(input.TagKeys))
	for _, k := range input.TagKeys {
		removeSet[k] = true
	}
	newTags := make([]SMTag, 0, len(secret.Tags))
	for _, t := range secret.Tags {
		if !removeSet[t.Key] {
			newTags = append(newTags, t)
		}
	}
	secret.Tags = newTags

	if err := p.saveSecret(goCtx, secret); err != nil {
		return nil, fmt.Errorf("sm untagResource saveSecret: %w", err)
	}
	return smJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SecretsManagerPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		SecretID string `json:"SecretId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := resolveSecretID(input.SecretID)
	secret, err := p.loadSecret(context.Background(), ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Secret not found", HTTPStatus: http.StatusNotFound}
	}

	out := map[string]interface{}{
		"ARN":  secret.ARN,
		"Name": secret.Name,
		"Tags": secret.Tags,
	}
	return smJSONResponse(http.StatusOK, out)
}

func (p *SecretsManagerPlugin) rotateSecret(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		SecretID string `json:"SecretId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	name := resolveSecretID(input.SecretID)
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Secret not found", HTTPStatus: http.StatusNotFound}
	}

	secret.RotationEnabled = true
	if err := p.saveSecret(goCtx, secret); err != nil {
		return nil, fmt.Errorf("sm rotateSecret saveSecret: %w", err)
	}

	out := map[string]interface{}{
		"ARN":  secret.ARN,
		"Name": secret.Name,
	}
	return smJSONResponse(http.StatusOK, out)
}

// --- Response helper ---

// smJSONResponse builds an AWSResponse with a JSON body for Secrets Manager
// using Content-Type: application/x-amz-json-1.1.
func smJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("sm marshal response: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
