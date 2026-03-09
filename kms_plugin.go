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

// KMSPlugin emulates the AWS Key Management Service (KMS) JSON-protocol API.
// It handles CreateKey, DescribeKey, ListKeys, EnableKey, DisableKey,
// ScheduleKeyDeletion, CancelKeyDeletion, GetKeyPolicy, PutKeyPolicy,
// GetKeyRotationStatus, EnableKeyRotation, DisableKeyRotation,
// TagResource, UntagResource, ListResourceTags, CreateAlias, DeleteAlias,
// UpdateAlias, ListAliases, Encrypt, Decrypt, GenerateDataKey,
// GenerateDataKeyWithoutPlaintext, and ReEncrypt.
type KMSPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "kms".
func (p *KMSPlugin) Name() string { return "kms" }

// Initialize sets up the KMSPlugin with the provided configuration.
func (p *KMSPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for KMSPlugin.
func (p *KMSPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a KMS JSON-protocol request to the appropriate handler.
func (p *KMSPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateKey":
		return p.createKey(ctx, req)
	case "DescribeKey":
		return p.describeKey(ctx, req)
	case "ListKeys":
		return p.listKeys(ctx, req)
	case "EnableKey":
		return p.enableKey(ctx, req)
	case "DisableKey":
		return p.disableKey(ctx, req)
	case "ScheduleKeyDeletion":
		return p.scheduleKeyDeletion(ctx, req)
	case "CancelKeyDeletion":
		return p.cancelKeyDeletion(ctx, req)
	case "GetKeyPolicy":
		return p.getKeyPolicy(ctx, req)
	case "PutKeyPolicy":
		return p.putKeyPolicy(ctx, req)
	case "GetKeyRotationStatus":
		return p.getKeyRotationStatus(ctx, req)
	case "EnableKeyRotation":
		return p.enableKeyRotation(ctx, req)
	case "DisableKeyRotation":
		return p.disableKeyRotation(ctx, req)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListResourceTags":
		return p.listResourceTags(ctx, req)
	case "CreateAlias":
		return p.createAlias(ctx, req)
	case "DeleteAlias":
		return p.deleteAlias(ctx, req)
	case "UpdateAlias":
		return p.updateAlias(ctx, req)
	case "ListAliases":
		return p.listAliases(ctx, req)
	case "Encrypt":
		return p.encrypt(ctx, req)
	case "Decrypt":
		return p.decrypt(ctx, req)
	case "GenerateDataKey":
		return p.generateDataKey(ctx, req)
	case "GenerateDataKeyWithoutPlaintext":
		return p.generateDataKeyWithoutPlaintext(ctx, req)
	case "ReEncrypt":
		return p.reEncrypt(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "UnsupportedOperationException",
			Message:    fmt.Sprintf("KMSPlugin: unknown operation %q", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- State key helpers ---

func (p *KMSPlugin) keyStateKey(accountID, region, keyID string) string {
	return "key:" + accountID + "/" + region + "/" + keyID
}

func (p *KMSPlugin) keyIDsKey(accountID, region string) string {
	return "key_ids:" + accountID + "/" + region
}

func (p *KMSPlugin) aliasKey(accountID, region, aliasName string) string {
	return "alias:" + accountID + "/" + region + "/" + aliasName
}

func (p *KMSPlugin) aliasNamesKey(accountID, region string) string {
	return "alias_names:" + accountID + "/" + region
}

func (p *KMSPlugin) policyKey(accountID, region, keyID string) string {
	return "key_policy:" + accountID + "/" + region + "/" + keyID
}

// --- State helpers ---

func (p *KMSPlugin) loadKey(ctx context.Context, accountID, region, keyID string) (*KMSKey, error) {
	data, err := p.state.Get(ctx, kmsNamespace, p.keyStateKey(accountID, region, keyID))
	if err != nil {
		return nil, fmt.Errorf("kms loadKey state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var k KMSKey
	if err := json.Unmarshal(data, &k); err != nil {
		return nil, fmt.Errorf("kms loadKey unmarshal: %w", err)
	}
	return &k, nil
}

func (p *KMSPlugin) saveKey(ctx context.Context, k *KMSKey) error {
	data, err := json.Marshal(k)
	if err != nil {
		return fmt.Errorf("kms saveKey marshal: %w", err)
	}
	return p.state.Put(ctx, kmsNamespace, p.keyStateKey(k.AccountID, k.Region, k.KeyID), data)
}

func (p *KMSPlugin) loadKeyIDs(ctx context.Context, accountID, region string) ([]string, error) {
	data, err := p.state.Get(ctx, kmsNamespace, p.keyIDsKey(accountID, region))
	if err != nil {
		return nil, fmt.Errorf("kms loadKeyIDs: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, fmt.Errorf("kms loadKeyIDs unmarshal: %w", err)
	}
	return ids, nil
}

func (p *KMSPlugin) saveKeyIDs(ctx context.Context, accountID, region string, ids []string) error {
	data, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("kms saveKeyIDs marshal: %w", err)
	}
	return p.state.Put(ctx, kmsNamespace, p.keyIDsKey(accountID, region), data)
}

func (p *KMSPlugin) loadAliasNames(ctx context.Context, accountID, region string) ([]string, error) {
	data, err := p.state.Get(ctx, kmsNamespace, p.aliasNamesKey(accountID, region))
	if err != nil {
		return nil, fmt.Errorf("kms loadAliasNames: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("kms loadAliasNames unmarshal: %w", err)
	}
	return names, nil
}

func (p *KMSPlugin) saveAliasNames(ctx context.Context, accountID, region string, names []string) error {
	sort.Strings(names)
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("kms saveAliasNames marshal: %w", err)
	}
	return p.state.Put(ctx, kmsNamespace, p.aliasNamesKey(accountID, region), data)
}

// resolveKeyID resolves a KeyId (raw ID, ARN, or alias/name) to the key ID.
func (p *KMSPlugin) resolveKeyID(ctx context.Context, accountID, region, keyIDOrAlias string) (string, error) {
	if strings.HasPrefix(keyIDOrAlias, "alias/") {
		data, err := p.state.Get(ctx, kmsNamespace, p.aliasKey(accountID, region, keyIDOrAlias))
		if err != nil {
			return "", fmt.Errorf("kms resolveKeyID alias lookup: %w", err)
		}
		if data == nil {
			return "", &AWSError{Code: "NotFoundException", Message: "alias not found: " + keyIDOrAlias, HTTPStatus: http.StatusNotFound}
		}
		return string(data), nil
	}
	if strings.HasPrefix(keyIDOrAlias, "arn:") {
		// ARN: arn:aws:kms:{region}:{acct}:key/{keyID}
		parts := strings.Split(keyIDOrAlias, "/")
		if len(parts) >= 2 {
			return parts[len(parts)-1], nil
		}
	}
	return keyIDOrAlias, nil
}

// --- Operations ---

func (p *KMSPlugin) createKey(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Description string   `json:"Description"`
		KeyUsage    string   `json:"KeyUsage"`
		KeySpec     string   `json:"KeySpec"`
		MultiRegion bool     `json:"MultiRegion"`
		Tags        []KMSTag `json:"Tags"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input) //nolint:errcheck // optional body
	}
	if input.KeyUsage == "" {
		input.KeyUsage = "ENCRYPT_DECRYPT"
	}
	if input.KeySpec == "" {
		input.KeySpec = "SYMMETRIC_DEFAULT"
	}

	keyID := generateKMSKeyID()
	arn := kmsKeyARN(ctx.Region, ctx.AccountID, keyID)
	key := &KMSKey{
		KeyID:        keyID,
		ARN:          arn,
		Description:  input.Description,
		KeyUsage:     input.KeyUsage,
		KeySpec:      input.KeySpec,
		KeyState:     "Enabled",
		Enabled:      true,
		MultiRegion:  input.MultiRegion,
		Tags:         input.Tags,
		AccountID:    ctx.AccountID,
		Region:       ctx.Region,
		CreationDate: p.tc.Now(),
	}

	goCtx := context.Background()
	if err := p.saveKey(goCtx, key); err != nil {
		return nil, fmt.Errorf("kms createKey saveKey: %w", err)
	}

	ids, err := p.loadKeyIDs(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	ids = append(ids, keyID)
	if err := p.saveKeyIDs(goCtx, ctx.AccountID, ctx.Region, ids); err != nil {
		return nil, fmt.Errorf("kms createKey saveKeyIDs: %w", err)
	}

	out := map[string]interface{}{
		"KeyMetadata": map[string]interface{}{
			"KeyId":        key.KeyID,
			"Arn":          key.ARN,
			"Description":  key.Description,
			"KeyUsage":     key.KeyUsage,
			"KeySpec":      key.KeySpec,
			"KeyState":     key.KeyState,
			"Enabled":      key.Enabled,
			"MultiRegion":  key.MultiRegion,
			"CreationDate": key.CreationDate.Unix(),
		},
	}
	return kmsJSONResponse(http.StatusOK, out)
}

func (p *KMSPlugin) describeKey(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID string `json:"KeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}

	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}

	out := map[string]interface{}{
		"KeyMetadata": map[string]interface{}{
			"KeyId":           key.KeyID,
			"Arn":             key.ARN,
			"Description":     key.Description,
			"KeyUsage":        key.KeyUsage,
			"KeySpec":         key.KeySpec,
			"KeyState":        key.KeyState,
			"Enabled":         key.Enabled,
			"MultiRegion":     key.MultiRegion,
			"RotationEnabled": key.RotationEnabled,
			"CreationDate":    key.CreationDate.Unix(),
		},
	}
	return kmsJSONResponse(http.StatusOK, out)
}

func (p *KMSPlugin) listKeys(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Limit  int    `json:"Limit"`
		Marker string `json:"Marker"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input) //nolint:errcheck // optional body
	}
	if input.Limit <= 0 {
		input.Limit = 100
	}

	goCtx := context.Background()
	ids, err := p.loadKeyIDs(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}

	// Pagination via base64 offset marker.
	offset := 0
	if input.Marker != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(input.Marker); decErr == nil {
			if n, parseErr := strconv.Atoi(string(decoded)); parseErr == nil && n >= 0 {
				offset = n
			}
		}
	}
	if offset > len(ids) {
		offset = len(ids)
	}
	page := ids[offset:]
	var nextMarker string
	if len(page) > input.Limit {
		page = page[:input.Limit]
		nextOffset := offset + input.Limit
		nextMarker = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(nextOffset)))
	}

	type keyEntry struct {
		KeyID  string `json:"KeyId"`
		KeyArn string `json:"KeyArn"`
	}
	entries := make([]keyEntry, 0, len(page))
	for _, id := range page {
		entries = append(entries, keyEntry{
			KeyID:  id,
			KeyArn: kmsKeyARN(ctx.Region, ctx.AccountID, id),
		})
	}

	out := map[string]interface{}{
		"Keys":      entries,
		"Truncated": nextMarker != "",
	}
	if nextMarker != "" {
		out["NextMarker"] = nextMarker
	}
	return kmsJSONResponse(http.StatusOK, out)
}

func (p *KMSPlugin) enableKey(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID string `json:"KeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	return p.setKeyState(ctx, input.KeyID, "Enabled", true)
}

func (p *KMSPlugin) disableKey(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID string `json:"KeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	return p.setKeyState(ctx, input.KeyID, "Disabled", false)
}

func (p *KMSPlugin) setKeyState(ctx *RequestContext, keyIDParam, state string, enabled bool) (*AWSResponse, error) {
	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, keyIDParam)
	if err != nil {
		return nil, err
	}
	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}
	key.KeyState = state
	key.Enabled = enabled
	if err := p.saveKey(goCtx, key); err != nil {
		return nil, fmt.Errorf("kms setKeyState saveKey: %w", err)
	}
	return kmsJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *KMSPlugin) scheduleKeyDeletion(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID               string `json:"KeyId"`
		PendingWindowInDays int    `json:"PendingWindowInDays"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}
	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}
	days := input.PendingWindowInDays
	if days <= 0 {
		days = 30
	}
	deletionDate := p.tc.Now().AddDate(0, 0, days)
	key.KeyState = "PendingDeletion"
	key.Enabled = false
	if err := p.saveKey(goCtx, key); err != nil {
		return nil, fmt.Errorf("kms scheduleKeyDeletion saveKey: %w", err)
	}
	out := map[string]interface{}{
		"KeyId":        key.KeyID,
		"DeletionDate": deletionDate.Unix(),
		"KeyState":     key.KeyState,
	}
	return kmsJSONResponse(http.StatusOK, out)
}

func (p *KMSPlugin) cancelKeyDeletion(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID string `json:"KeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	return p.setKeyState(ctx, input.KeyID, "Enabled", true)
}

func (p *KMSPlugin) getKeyPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID      string `json:"KeyId"`
		PolicyName string `json:"PolicyName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}

	data, err := p.state.Get(goCtx, kmsNamespace, p.policyKey(ctx.AccountID, ctx.Region, keyID))
	if err != nil {
		return nil, fmt.Errorf("kms getKeyPolicy state.Get: %w", err)
	}
	policy := `{"Version":"2012-10-17","Statement":[]}`
	if data != nil {
		policy = string(data)
	}
	out := map[string]interface{}{
		"Policy":     policy,
		"PolicyName": "default",
	}
	return kmsJSONResponse(http.StatusOK, out)
}

func (p *KMSPlugin) putKeyPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID      string `json:"KeyId"`
		PolicyName string `json:"PolicyName"`
		Policy     string `json:"Policy"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}
	if err := p.state.Put(goCtx, kmsNamespace, p.policyKey(ctx.AccountID, ctx.Region, keyID), []byte(input.Policy)); err != nil {
		return nil, fmt.Errorf("kms putKeyPolicy state.Put: %w", err)
	}
	return kmsJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *KMSPlugin) getKeyRotationStatus(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID string `json:"KeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}
	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}
	out := map[string]interface{}{
		"KeyRotationEnabled": key.RotationEnabled,
	}
	return kmsJSONResponse(http.StatusOK, out)
}

func (p *KMSPlugin) enableKeyRotation(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID string `json:"KeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}
	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}
	key.RotationEnabled = true
	if err := p.saveKey(goCtx, key); err != nil {
		return nil, fmt.Errorf("kms enableKeyRotation saveKey: %w", err)
	}
	return kmsJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *KMSPlugin) disableKeyRotation(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID string `json:"KeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}
	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}
	key.RotationEnabled = false
	if err := p.saveKey(goCtx, key); err != nil {
		return nil, fmt.Errorf("kms disableKeyRotation saveKey: %w", err)
	}
	return kmsJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *KMSPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID string   `json:"KeyId"`
		Tags  []KMSTag `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}
	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}
	tagMap := make(map[string]string, len(key.Tags))
	for _, t := range key.Tags {
		tagMap[t.TagKey] = t.TagValue
	}
	for _, t := range input.Tags {
		tagMap[t.TagKey] = t.TagValue
	}
	newTags := make([]KMSTag, 0, len(tagMap))
	for k, v := range tagMap {
		newTags = append(newTags, KMSTag{TagKey: k, TagValue: v})
	}
	key.Tags = newTags
	if err := p.saveKey(goCtx, key); err != nil {
		return nil, fmt.Errorf("kms tagResource saveKey: %w", err)
	}
	return kmsJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *KMSPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID   string   `json:"KeyId"`
		TagKeys []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}
	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}
	removeSet := make(map[string]bool, len(input.TagKeys))
	for _, k := range input.TagKeys {
		removeSet[k] = true
	}
	newTags := make([]KMSTag, 0, len(key.Tags))
	for _, t := range key.Tags {
		if !removeSet[t.TagKey] {
			newTags = append(newTags, t)
		}
	}
	key.Tags = newTags
	if err := p.saveKey(goCtx, key); err != nil {
		return nil, fmt.Errorf("kms untagResource saveKey: %w", err)
	}
	return kmsJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *KMSPlugin) listResourceTags(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID string `json:"KeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}
	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}
	out := map[string]interface{}{
		"Tags":      key.Tags,
		"Truncated": false,
	}
	return kmsJSONResponse(http.StatusOK, out)
}

func (p *KMSPlugin) createAlias(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		AliasName   string `json:"AliasName"`
		TargetKeyID string `json:"TargetKeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	if !strings.HasPrefix(input.AliasName, "alias/") {
		input.AliasName = "alias/" + input.AliasName
	}

	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.TargetKeyID)
	if err != nil {
		return nil, err
	}

	if err := p.state.Put(goCtx, kmsNamespace, p.aliasKey(ctx.AccountID, ctx.Region, input.AliasName), []byte(keyID)); err != nil {
		return nil, fmt.Errorf("kms createAlias state.Put: %w", err)
	}

	names, err := p.loadAliasNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	names = append(names, input.AliasName)
	if err := p.saveAliasNames(goCtx, ctx.AccountID, ctx.Region, names); err != nil {
		return nil, fmt.Errorf("kms createAlias saveAliasNames: %w", err)
	}
	return kmsJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *KMSPlugin) deleteAlias(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		AliasName string `json:"AliasName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	if !strings.HasPrefix(input.AliasName, "alias/") {
		input.AliasName = "alias/" + input.AliasName
	}

	goCtx := context.Background()
	_ = p.state.Delete(goCtx, kmsNamespace, p.aliasKey(ctx.AccountID, ctx.Region, input.AliasName))

	names, err := p.loadAliasNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != input.AliasName {
			newNames = append(newNames, n)
		}
	}
	if err := p.saveAliasNames(goCtx, ctx.AccountID, ctx.Region, newNames); err != nil {
		return nil, fmt.Errorf("kms deleteAlias saveAliasNames: %w", err)
	}
	return kmsJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *KMSPlugin) updateAlias(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		AliasName   string `json:"AliasName"`
		TargetKeyID string `json:"TargetKeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	if !strings.HasPrefix(input.AliasName, "alias/") {
		input.AliasName = "alias/" + input.AliasName
	}

	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.TargetKeyID)
	if err != nil {
		return nil, err
	}
	if err := p.state.Put(goCtx, kmsNamespace, p.aliasKey(ctx.AccountID, ctx.Region, input.AliasName), []byte(keyID)); err != nil {
		return nil, fmt.Errorf("kms updateAlias state.Put: %w", err)
	}
	return kmsJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *KMSPlugin) listAliases(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID  string `json:"KeyId"`
		Limit  int    `json:"Limit"`
		Marker string `json:"Marker"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input) //nolint:errcheck // optional body
	}
	if input.Limit <= 0 {
		input.Limit = 100
	}

	goCtx := context.Background()
	names, err := p.loadAliasNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}

	// Pagination.
	offset := 0
	if input.Marker != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(input.Marker); decErr == nil {
			if n, parseErr := strconv.Atoi(string(decoded)); parseErr == nil && n >= 0 {
				offset = n
			}
		}
	}
	if offset > len(names) {
		offset = len(names)
	}
	page := names[offset:]
	var nextMarker string
	if len(page) > input.Limit {
		page = page[:input.Limit]
		nextOffset := offset + input.Limit
		nextMarker = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(nextOffset)))
	}

	type aliasEntry struct {
		AliasName   string `json:"AliasName"`
		TargetKeyID string `json:"TargetKeyId"`
		AliasArn    string `json:"AliasArn"`
	}
	entries := make([]aliasEntry, 0, len(page))
	for _, name := range page {
		data, loadErr := p.state.Get(goCtx, kmsNamespace, p.aliasKey(ctx.AccountID, ctx.Region, name))
		if loadErr != nil || data == nil {
			continue
		}
		keyID := string(data)
		// Filter by KeyId if specified.
		if input.KeyID != "" && keyID != input.KeyID {
			continue
		}
		entries = append(entries, aliasEntry{
			AliasName:   name,
			TargetKeyID: keyID,
			AliasArn:    fmt.Sprintf("arn:aws:kms:%s:%s:%s", ctx.Region, ctx.AccountID, name),
		})
	}

	out := map[string]interface{}{
		"Aliases":   entries,
		"Truncated": nextMarker != "",
	}
	if nextMarker != "" {
		out["NextMarker"] = nextMarker
	}
	return kmsJSONResponse(http.StatusOK, out)
}

func (p *KMSPlugin) encrypt(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID     string `json:"KeyId"`
		Plaintext string `json:"Plaintext"` // base64-encoded
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}
	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}
	if !key.Enabled {
		return nil, &AWSError{Code: "DisabledException", Message: "Key is disabled", HTTPStatus: http.StatusConflict}
	}

	plaintext, err := base64.StdEncoding.DecodeString(input.Plaintext)
	if err != nil {
		return nil, &AWSError{Code: "InvalidCiphertextException", Message: "invalid base64 plaintext", HTTPStatus: http.StatusBadRequest}
	}

	ciphertext := kmsEncryptStub(keyID, plaintext)
	out := map[string]interface{}{
		"KeyId":          key.ARN,
		"CiphertextBlob": string(ciphertext),
	}
	return kmsJSONResponse(http.StatusOK, out)
}

func (p *KMSPlugin) decrypt(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		CiphertextBlob string `json:"CiphertextBlob"`
		KeyID          string `json:"KeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	keyID, plaintext, err := kmsDecryptStub([]byte(input.CiphertextBlob))
	if err != nil {
		return nil, &AWSError{Code: "InvalidCiphertextException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}
	if !key.Enabled {
		return nil, &AWSError{Code: "DisabledException", Message: "Key is disabled", HTTPStatus: http.StatusConflict}
	}

	out := map[string]interface{}{
		"KeyId":     key.ARN,
		"Plaintext": base64.StdEncoding.EncodeToString(plaintext),
	}
	return kmsJSONResponse(http.StatusOK, out)
}

func (p *KMSPlugin) generateDataKey(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID         string `json:"KeyId"`
		KeySpec       string `json:"KeySpec"`
		NumberOfBytes int    `json:"NumberOfBytes"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}
	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}
	if !key.Enabled {
		return nil, &AWSError{Code: "DisabledException", Message: "Key is disabled", HTTPStatus: http.StatusConflict}
	}

	// Generate a stub 32-byte data key.
	dataKeyHex := randomHex(16)
	dataKeyBytes, _ := base64.StdEncoding.DecodeString(base64.StdEncoding.EncodeToString([]byte(dataKeyHex)))
	plaintextB64 := base64.StdEncoding.EncodeToString([]byte(dataKeyHex))
	ciphertext := kmsEncryptStub(keyID, []byte(dataKeyHex))

	out := map[string]interface{}{
		"KeyId":          key.ARN,
		"Plaintext":      plaintextB64,
		"CiphertextBlob": string(ciphertext),
	}
	_ = dataKeyBytes
	return kmsJSONResponse(http.StatusOK, out)
}

func (p *KMSPlugin) generateDataKeyWithoutPlaintext(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID   string `json:"KeyId"`
		KeySpec string `json:"KeySpec"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	keyID, err := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.KeyID)
	if err != nil {
		return nil, err
	}
	key, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}

	dataKeyHex := randomHex(16)
	ciphertext := kmsEncryptStub(keyID, []byte(dataKeyHex))

	out := map[string]interface{}{
		"KeyId":          key.ARN,
		"CiphertextBlob": string(ciphertext),
	}
	return kmsJSONResponse(http.StatusOK, out)
}

func (p *KMSPlugin) reEncrypt(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		CiphertextBlob   string `json:"CiphertextBlob"`
		DestinationKeyID string `json:"DestinationKeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}

	_, plaintext, err := kmsDecryptStub([]byte(input.CiphertextBlob))
	if err != nil {
		return nil, &AWSError{Code: "InvalidCiphertextException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	destKeyID, resolveErr := p.resolveKeyID(goCtx, ctx.AccountID, ctx.Region, input.DestinationKeyID)
	if resolveErr != nil {
		return nil, resolveErr
	}
	destKey, loadErr := p.loadKey(goCtx, ctx.AccountID, ctx.Region, destKeyID)
	if loadErr != nil {
		return nil, loadErr
	}
	if destKey == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Destination key not found", HTTPStatus: http.StatusNotFound}
	}

	newCiphertext := kmsEncryptStub(destKeyID, plaintext)
	out := map[string]interface{}{
		"KeyId":          destKey.ARN,
		"CiphertextBlob": string(newCiphertext),
		"SourceKeyId":    input.CiphertextBlob,
	}
	return kmsJSONResponse(http.StatusOK, out)
}

// --- Response helper ---

// kmsJSONResponse builds an AWSResponse with a JSON body for KMS using
// Content-Type: application/x-amz-json-1.1.
func kmsJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("kms marshal response: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
