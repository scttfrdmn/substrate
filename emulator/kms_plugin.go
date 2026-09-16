package emulator

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
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// --- State key helpers ---

// The five key builders are free functions rather than methods because they use no receiver state
// and because the ARN resolver in kms_tags.go has to build the same key from an ARN alone. A
// resolver that re-derived the layout would be a second producer of it, which is the arrangement
// #826 found to have drifted for SQS and #918 for CloudFront. See kms_tags.go's preamble.

// kmsKeyStateKey returns the state key a KMS key record is stored at.
func kmsKeyStateKey(accountID, region, keyID string) string {
	return kmsKeyKeyPrefix + accountID + "/" + region + "/" + keyID
}

// kmsKeyIDsKey returns the state index key for all key IDs in an account and Region.
func kmsKeyIDsKey(accountID, region string) string {
	return kmsKeyIDsKeyPrefix + accountID + "/" + region
}

// kmsAliasKey returns the state key an alias-to-key-ID pointer is stored at.
func kmsAliasKey(accountID, region, aliasName string) string {
	return kmsAliasKeyPrefix + accountID + "/" + region + "/" + aliasName
}

// kmsAliasNamesKey returns the state index key for all alias names in an account and Region.
func kmsAliasNamesKey(accountID, region string) string {
	return kmsAliasNamesKeyPrefix + accountID + "/" + region
}

// kmsPolicyKey returns the state key a key policy document is stored at.
func kmsPolicyKey(accountID, region, keyID string) string {
	return kmsKeyPolicyKeyPrefix + accountID + "/" + region + "/" + keyID
}

// --- State helpers ---

func (p *KMSPlugin) loadKey(ctx context.Context, accountID, region, keyID string) (*KMSKey, error) {
	data, err := p.state.Get(ctx, kmsNamespace, kmsKeyStateKey(accountID, region, keyID))
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
	return p.state.Put(ctx, kmsNamespace, kmsKeyStateKey(k.AccountID, k.Region, k.KeyID), data)
}

func (p *KMSPlugin) loadKeyIDs(ctx context.Context, accountID, region string) ([]string, error) {
	data, err := p.state.Get(ctx, kmsNamespace, kmsKeyIDsKey(accountID, region))
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
	return p.state.Put(ctx, kmsNamespace, kmsKeyIDsKey(accountID, region), data)
}

func (p *KMSPlugin) loadAliasNames(ctx context.Context, accountID, region string) ([]string, error) {
	data, err := p.state.Get(ctx, kmsNamespace, kmsAliasNamesKey(accountID, region))
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
	return p.state.Put(ctx, kmsNamespace, kmsAliasNamesKey(accountID, region), data)
}

// resolveKeyTarget resolves a KeyId parameter to the account, Region and key ID it names.
//
// AWS documents four accepted forms and this handles all four: a key ID, a key ARN, an alias name
// and an alias ARN. The account and Region come from the **ARN** for the two ARN forms, and from the
// calling request only for the two that carry neither — which is the whole point of returning a
// target rather than a bare key ID. Every caller keyed its load and its store by ctx.AccountID and
// ctx.Region regardless of what the ARN said, so a key ARN naming another account or another Region
// addressed the caller's own same-named key. See kms_tags.go's preamble for the rule and its four
// precedents.
//
// The ARN forms delegate to [kmsParseARN], which takes no request context at all, so the rule is
// structural here rather than remembered at eighteen call sites.
func (p *KMSPlugin) resolveKeyTarget(ctx context.Context, reqCtx *RequestContext, keyID string) (kmsTagTarget, error) {
	if strings.HasPrefix(keyID, "arn:") {
		target, resType, arnErr := kmsParseARN(keyID)
		if arnErr != nil {
			return kmsTagTarget{}, arnErr
		}
		if resType == kmsKeyResourceType {
			return target, nil
		}
		if resType != kmsAliasResourceType {
			return kmsTagTarget{}, &AWSError{
				Code:       "NotFoundException",
				Message:    fmt.Sprintf("KMS resource type %q does not name a key: %s", resType, keyID),
				HTTPStatus: http.StatusBadRequest,
			}
		}
		// An alias ARN names an alias in the account and Region the ARN carries, so the pointer
		// is read from there — not from the caller's account, which is what let an alias ARN
		// reach the caller's own alias of the same name.
		aliasName := kmsAliasResourceType + "/" + target.KeyID
		resolved, err := p.followAlias(ctx, target.AccountID, target.Region, aliasName)
		if err != nil {
			return kmsTagTarget{}, err
		}
		return kmsTagTarget{AccountID: target.AccountID, Region: target.Region, KeyID: resolved}, nil
	}

	if strings.HasPrefix(keyID, kmsAliasResourceType+"/") {
		resolved, err := p.followAlias(ctx, reqCtx.AccountID, reqCtx.Region, keyID)
		if err != nil {
			return kmsTagTarget{}, err
		}
		return kmsTagTarget{AccountID: reqCtx.AccountID, Region: reqCtx.Region, KeyID: resolved}, nil
	}

	// A bare key ID carries no account or Region, so the caller's are the only ones available and
	// using them is not the defect this function fixes.
	return kmsTagTarget{AccountID: reqCtx.AccountID, Region: reqCtx.Region, KeyID: keyID}, nil
}

// resolveLocalKeyID resolves a KeyId that must name a key in the caller's own account and Region,
// for the two operations that write an alias.
//
// An alias lives in one account and one Region and can only point at a key in the same two: KMS
// publishes "Cross-account use: No" on CreateAlias and UpdateAlias, and an alias is itself a
// Region-scoped resource. Without this check a TargetKeyId naming another account resolves fine and
// the alias is written into the caller's account pointing at a key ID that account does not have —
// a dangling pointer that ListAliases reports and every later resolution of it fails on.
//
// The refusal is NotFoundException, because from the caller's side that is what a key in another
// account is: not found. AWS publishes the code on both operations but does not say it covers this
// case, so the mapping is substrate's reading.
func (p *KMSPlugin) resolveLocalKeyID(ctx context.Context, reqCtx *RequestContext, keyID string) (string, error) {
	target, err := p.resolveKeyTarget(ctx, reqCtx, keyID)
	if err != nil {
		return "", err
	}
	if localErr := kmsRequireLocal(reqCtx, target, "an alias"); localErr != nil {
		return "", localErr
	}
	return target.KeyID, nil
}

// followAlias reads the key ID an alias points at, in the account and Region given.
func (p *KMSPlugin) followAlias(ctx context.Context, accountID, region, aliasName string) (string, error) {
	data, err := p.state.Get(ctx, kmsNamespace, kmsAliasKey(accountID, region, aliasName))
	if err != nil {
		return "", fmt.Errorf("kms followAlias lookup: %w", err)
	}
	if data == nil {
		return "", &AWSError{Code: "NotFoundException", Message: "alias not found: " + aliasName, HTTPStatus: http.StatusNotFound}
	}
	return string(data), nil
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	keyID := target.KeyID

	key, err := p.loadKey(goCtx, target.AccountID, target.Region, keyID)
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
	target, err := p.resolveKeyTarget(goCtx, ctx, keyIDParam)
	if err != nil {
		return nil, err
	}
	keyID := target.KeyID
	key, err := p.loadKey(goCtx, target.AccountID, target.Region, keyID)
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	keyID := target.KeyID
	key, err := p.loadKey(goCtx, target.AccountID, target.Region, keyID)
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	keyID := target.KeyID

	data, err := p.state.Get(goCtx, kmsNamespace, kmsPolicyKey(target.AccountID, target.Region, keyID))
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	keyID := target.KeyID
	if err := p.state.Put(goCtx, kmsNamespace, kmsPolicyKey(target.AccountID, target.Region, keyID), []byte(input.Policy)); err != nil {
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	keyID := target.KeyID
	key, err := p.loadKey(goCtx, target.AccountID, target.Region, keyID)
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	keyID := target.KeyID
	key, err := p.loadKey(goCtx, target.AccountID, target.Region, keyID)
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	keyID := target.KeyID
	key, err := p.loadKey(goCtx, target.AccountID, target.Region, keyID)
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	if err := kmsRequireLocal(ctx, target, "TagResource"); err != nil {
		return nil, err
	}
	keyID := target.KeyID
	key, err := p.loadKey(goCtx, target.AccountID, target.Region, keyID)
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
	// "If you specify an existing tag key with a different tag value, AWS KMS replaces the current
	// tag value with the specified one" (API_TagResource), so the request wins on a collision.
	for _, t := range input.Tags {
		tagMap[t.TagKey] = t.TagValue
	}
	newTags := make([]KMSTag, 0, len(tagMap))
	for k, v := range tagMap {
		newTags = append(newTags, KMSTag{TagKey: k, TagValue: v})
	}
	// A merged slice built by ranging a Go map comes out in the map's hash order, so two identical
	// TagResource calls in one run could store — and ListResourceTags report — two different orders.
	// #862 settled this for the tagging plugin's own merge helpers; this is the same rule applied to
	// the handler that writes the record they merge into.
	sortTagsByKey(newTags, func(t KMSTag) string { return t.TagKey })
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	if err := kmsRequireLocal(ctx, target, "UntagResource"); err != nil {
		return nil, err
	}
	keyID := target.KeyID
	key, err := p.loadKey(goCtx, target.AccountID, target.Region, keyID)
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	if err := kmsRequireLocal(ctx, target, "ListResourceTags"); err != nil {
		return nil, err
	}
	keyID := target.KeyID
	key, err := p.loadKey(goCtx, target.AccountID, target.Region, keyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Key not found", HTTPStatus: http.StatusNotFound}
	}
	// Sorted on the way out as well as on the way in, because a record written before the merge
	// above started sorting still holds its tags in whatever order it was stored in.
	tags := make([]KMSTag, len(key.Tags))
	copy(tags, key.Tags)
	sortTagsByKey(tags, func(t KMSTag) string { return t.TagKey })
	out := map[string]interface{}{
		"Tags":      tags,
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
	keyID, err := p.resolveLocalKeyID(goCtx, ctx, input.TargetKeyID)
	if err != nil {
		return nil, err
	}

	if err := p.state.Put(goCtx, kmsNamespace, kmsAliasKey(ctx.AccountID, ctx.Region, input.AliasName), []byte(keyID)); err != nil {
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
	_ = p.state.Delete(goCtx, kmsNamespace, kmsAliasKey(ctx.AccountID, ctx.Region, input.AliasName))

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
	keyID, err := p.resolveLocalKeyID(goCtx, ctx, input.TargetKeyID)
	if err != nil {
		return nil, err
	}
	if err := p.state.Put(goCtx, kmsNamespace, kmsAliasKey(ctx.AccountID, ctx.Region, input.AliasName), []byte(keyID)); err != nil {
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
		data, loadErr := p.state.Get(goCtx, kmsNamespace, kmsAliasKey(ctx.AccountID, ctx.Region, name))
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	keyID := target.KeyID
	key, err := p.loadKey(goCtx, target.AccountID, target.Region, keyID)
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
	// The caller's account and Region are correct here and this is not a site the ARN rule reaches:
	// the key ID came out of the ciphertext, not out of a KeyId parameter, so there is no ARN to
	// take an account from. Decrypt's optional KeyId is a *constraint* on which key may be used,
	// not a selector.
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	keyID := target.KeyID
	key, err := p.loadKey(goCtx, target.AccountID, target.Region, keyID)
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
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	keyID := target.KeyID
	key, err := p.loadKey(goCtx, target.AccountID, target.Region, keyID)
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
	dest, resolveErr := p.resolveKeyTarget(goCtx, ctx, input.DestinationKeyID)
	if resolveErr != nil {
		return nil, resolveErr
	}
	destKeyID := dest.KeyID
	destKey, loadErr := p.loadKey(goCtx, dest.AccountID, dest.Region, destKeyID)
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
