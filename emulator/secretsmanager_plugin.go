package emulator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
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
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// --- State helpers ---

// The three state-key builders this plugin writes through are free functions in
// secretsmanager_tags.go — [smSecretStateKey], [smSecretNamesStateKey] and
// [smSecretVersionStateKey] — so the Resource Groups Tagging API's resolver reaches the same
// addresses without holding a plugin, and neither API can address a secret the other would not.

func (p *SecretsManagerPlugin) loadSecret(ctx context.Context, accountID, region, name string) (*SecretState, error) {
	data, err := p.state.Get(ctx, secretsManagerNamespace, smSecretStateKey(accountID, region, name))
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
	return p.state.Put(ctx, secretsManagerNamespace, smSecretStateKey(s.AccountID, s.Region, s.Name), data)
}

func (p *SecretsManagerPlugin) loadSecretNames(ctx context.Context, accountID, region string) ([]string, error) {
	data, err := p.state.Get(ctx, secretsManagerNamespace, smSecretNamesStateKey(accountID, region))
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
	return p.state.Put(ctx, secretsManagerNamespace, smSecretNamesStateKey(accountID, region), data)
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

	// Key-ordered on the way in, so a secret tagged at creation and a secret tagged by TagResource
	// report their tags in the same order.
	sortTagsByKey(input.Tags, func(t SMTag) string { return t.Key })

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
		if err := p.state.Put(goCtx, secretsManagerNamespace, smSecretVersionStateKey(ctx.AccountID, ctx.Region, input.Name, versionID), []byte(value)); err != nil {
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

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(input.SecretID)
	}

	versionID := input.VersionID
	if versionID == "" {
		versionID = secret.CurrentVersionID
	}

	valueData, err := p.state.Get(goCtx, secretsManagerNamespace, smSecretVersionStateKey(target.AccountID, target.Region, target.Name, versionID))
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

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(input.SecretID)
	}

	versionID := generateVersionID()
	value := input.SecretString
	if value == "" {
		value = input.SecretBinary
	}
	if err := p.state.Put(goCtx, secretsManagerNamespace, smSecretVersionStateKey(target.AccountID, target.Region, target.Name, versionID), []byte(value)); err != nil {
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

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	secret, err := p.loadSecret(context.Background(), target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(input.SecretID)
	}

	out := map[string]interface{}{
		"ARN":             secret.ARN,
		"Name":            secret.Name,
		"Description":     secret.Description,
		"KmsKeyId":        secret.KMSKeyID,
		"RotationEnabled": secret.RotationEnabled,
		"CreatedDate":     secret.CreatedDate.Unix(),
		"LastChangedDate": secret.LastChangedDate.Unix(),
	}
	// AWS states "Secrets Manager only returns fields that have a value in the response", and this is
	// the operation a caller reads a secret's tags back through — Secrets Manager publishes no
	// ListTagsForResource at all (#929). An untagged secret emitted "Tags": null, which is not a member
	// AWS sends; the other members here are left as they are and are tracked in #930, because Tags is
	// the one #928's tagging row is asserted through and the rest need their own decision.
	if len(secret.Tags) > 0 {
		out["Tags"] = secret.Tags
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

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(input.SecretID)
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
		if err := p.state.Put(goCtx, secretsManagerNamespace, smSecretVersionStateKey(target.AccountID, target.Region, target.Name, versionID), []byte(value)); err != nil {
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

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(input.SecretID)
	}

	_ = p.state.Delete(goCtx, secretsManagerNamespace, smSecretStateKey(target.AccountID, target.Region, target.Name))
	_ = p.state.Delete(goCtx, secretsManagerNamespace, smSecretVersionStateKey(target.AccountID, target.Region, target.Name, secret.CurrentVersionID))

	// The index entry removed is the one in the account and Region that owns the secret, which is
	// what the identifier named — not the caller's. Deleting the caller's left the owning account
	// still listing a secret whose record had just been removed.
	names, err := p.loadSecretNames(goCtx, target.AccountID, target.Region)
	if err != nil {
		return nil, err
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != target.Name {
			newNames = append(newNames, n)
		}
	}
	if err := p.saveSecretNames(goCtx, target.AccountID, target.Region, newNames); err != nil {
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

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	secret, err := p.loadSecret(context.Background(), target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(input.SecretID)
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

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(input.SecretID)
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
	// Sorted by key, per #862. The merged slice above is built by ranging a Go map, so without this
	// two identical TagResource calls in one run could store — and DescribeSecret report — the same
	// tags in a different order, and an assertion on that order could not replay from the event log.
	// This is the one arm of #835 where the map-range claim held; the SNS pass (#925) corrected the
	// record that it held there.
	sortTagsByKey(newTags, func(t SMTag) string { return t.Key })
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

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(input.SecretID)
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

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	secret, err := p.loadSecret(context.Background(), target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(input.SecretID)
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

	target, idErr := smResolveSecretID(input.SecretID, ctx.AccountID, ctx.Region)
	if idErr != nil {
		return nil, idErr
	}
	goCtx := context.Background()
	secret, err := p.loadSecret(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, smSecretNotFound(input.SecretID)
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
