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
			return kmsTagTarget{}, kmsNotFound(
				fmt.Sprintf("KMS resource type %q does not name a key: %s", resType, keyID))
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
		return "", kmsNotFound("alias not found: " + aliasName)
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
		return nil, kmsInvalidBody()
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
		return nil, kmsNotFound("Key not found")
	}

	// RotationEnabled is deliberately absent, and its absence is the point of #971. API_KeyMetadata
	// publishes 26 members and RotationEnabled is not one of them — the string does not appear on the page
	// at all. Substrate rendered it here until #971, which meant a caller could read rotation state off
	// DescribeKey against the emulator and get nothing back from AWS: a green test for code that cannot
	// work, which is #765's failure mode aimed at a response member.
	//
	// Rotation state is observable through GetKeyRotationStatus alone. That is why AWS has a separate
	// operation for a single boolean, and why API_EnableKeyRotation points a caller at
	// GetKeyRotationStatus rather than here. Restoring the member as a convenience would also hand a
	// caller a route to rotation state that bypasses the rules the real one carries, such as the
	// pending-deletion answer in #973.
	//
	// Sixteen published members are still missing from this map, five of which AWS would always send for a
	// key substrate can create — AWSAccountId, KeyManager, Origin, EncryptionAlgorithms and the deprecated
	// CustomerMasterKeySpec. That is the same shape failing to match the same page in the other direction,
	// and it is #974.
	metadata := map[string]interface{}{
		"KeyId":        key.KeyID,
		"Arn":          key.ARN,
		"Description":  key.Description,
		"KeyUsage":     key.KeyUsage,
		"KeySpec":      key.KeySpec,
		"KeyState":     key.KeyState,
		"Enabled":      key.Enabled,
		"MultiRegion":  key.MultiRegion,
		"CreationDate": key.CreationDate.Unix(),
	}
	// Emitted on the key state rather than on the field being non-zero, because that is the condition
	// API_KeyMetadata publishes: "this value is present only when the KMS key is scheduled for
	// deletion, that is, when its KeyState is PendingDeletion". Added by #963 — before it the deletion
	// date ScheduleKeyDeletion computed reached the caller once and was never stored, so DescribeKey
	// could not report when a pending key was due to go.
	//
	// PendingDeletionWindowInDays, the adjacent member, is deliberately absent: the page confines it to
	// KeyState PendingReplicaDeletion, which only a multi-Region primary that still has replicas
	// reaches and substrate never writes. Its range is 1-365, not ScheduleKeyDeletion's 7-30, so the
	// two are different members and reporting the waiting period under it would be wrong twice over.
	if key.KeyState == kmsKeyStatePendingDeletion {
		metadata["DeletionDate"] = key.DeletionDate.Unix()
	}
	out := map[string]interface{}{"KeyMetadata": metadata}
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
		return nil, kmsInvalidBody()
	}
	return p.setKeyState(ctx, input.KeyID, "Enabled", true)
}

func (p *KMSPlugin) disableKey(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID string `json:"KeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, kmsInvalidBody()
	}
	return p.setKeyState(ctx, input.KeyID, kmsKeyStateDisabled, false)
}

// setKeyState is the shared body of EnableKey and DisableKey, and the one refusal it makes is #968's.
//
// A key in PendingDeletion is refused with KMSInvalidStateException, footnote [3] of the developer
// guide's key-state table, which both operations' identical rows carry. Before #968 neither checked
// anything, so EnableKey against a pending key answered 200 and wrote Enabled — a one-call path from
// pending deletion to usable, which AWS does not have and which routes around the two-call recovery
// #963 established from API_CancelKeyDeletion's own first sentence. It also abandoned the deletion
// silently, because the clearing this function used to do left no record that one had been scheduled.
//
// The guard lives in the shared helper rather than in each handler because both callers refuse the
// same single state, and #963's counter-example is what makes that worth stating: CancelKeyDeletion
// was deliberately moved *off* this helper, since it requires the opposite state — PendingDeletion and
// nothing else — answers a different message for the absence of it, and returns a body. Two callers
// wanting one refusal belong together; a third wanting the inverse does not.
//
// The other four states the table refuses — PendingImport [5], Creating [14], Updating [15], and
// PendingReplicaDeletion alongside PendingDeletion in [3] — are unreachable: ScheduleKeyDeletion is
// substrate's only writer of a state other than Enabled or Disabled. Unavailable is the row that is
// neither, and it is recorded here so a later sweep does not read it as a refusal: footnote [12] makes
// it a *success* with a deferred effect, "the operation succeeds, but the key state of the KMS key does
// not change until it becomes available".
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
		return nil, kmsNotFound("Key not found")
	}
	// Before the write, so a refused call leaves the state, the enabled flag and the deletion date
	// exactly as they were — the ordering rule #949 established and #963 restated.
	if key.KeyState == kmsKeyStatePendingDeletion {
		return nil, kmsInvalidKeyState(keyID, key.KeyState)
	}
	key.KeyState = state
	key.Enabled = enabled
	// No deletion date is cleared here any more, and nothing is lost by that. This function used to
	// clear one, on the reasoning that a key reported Enabled must not still remember when it is due to
	// be deleted; with the refusal above, the only state that carries a date cannot reach this line, and
	// CancelKeyDeletion clears it on the one exit AWS documents. Clearing it anyway would be code no
	// request can run, which this package records in a comment rather than guards — the disposition the
	// four unreachable states above already have.
	if err := p.saveKey(goCtx, key); err != nil {
		return nil, fmt.Errorf("kms setKeyState saveKey: %w", err)
	}
	return kmsJSONResponse(http.StatusOK, map[string]interface{}{})
}

// scheduleKeyDeletion schedules a key for deletion after a waiting period.
//
// The three things #963 changed here, in the order the handler does them:
//
// The waiting period is checked against the published 7-30 range before the key is resolved. AWS does
// not publish which of a bad window and an absent key it reports first, so the precedence is
// substrate's reading: a value that is wrong on the face of the request is refused without a lookup,
// which is how kmsInvalidBody already behaves one line above. Before #963 the guard was
// "if days <= 0 { days = 30 }", which got the default right and the range not at all — 1, 365 and -5
// were all accepted and the last silently became 30.
//
// A key already in PendingDeletion is refused rather than re-stamped. Its row in the developer
// guide's key-state table is footnote [3] and the page carries the compatible-key-state sentence, so
// the previous behavior — recomputing the deletion date and saving it — silently moved a deadline the
// caller believed it had set. Updating, the table's other refused state, is unreachable: nothing in
// substrate writes it.
//
// The response gained PendingWindowInDays and its KeyId became the key ARN, both of which
// API_ScheduleKeyDeletion publishes and its own sample response carries.
func (p *KMSPlugin) scheduleKeyDeletion(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID               string `json:"KeyId"`
		PendingWindowInDays int    `json:"PendingWindowInDays"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, kmsInvalidBody()
	}
	days := input.PendingWindowInDays
	switch {
	case days == 0:
		// Absent, not zero: the field is an int with no pointer, so a body that omits it and a body
		// sending 0 are indistinguishable here. AWS refuses 0 and defaults an absent value, and
		// substrate cannot tell them apart without decoding into a *int — which is worth doing only
		// if a caller is ever shown to send an explicit 0, since defaulting is by far the commoner
		// intent. Recorded rather than papered over.
		days = kmsDefaultPendingWindowInDays
	case days < kmsMinPendingWindowInDays || days > kmsMaxPendingWindowInDays:
		return nil, kmsInvalidPendingWindow(days)
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
		return nil, kmsNotFound("Key not found")
	}
	if key.KeyState == kmsKeyStatePendingDeletion {
		return nil, kmsInvalidKeyState(key.KeyID, key.KeyState)
	}
	// Before the writes, so a refusal leaves the existing deletion date and key state alone rather
	// than half-applying the call it declined — the property #949 established and this asserts by
	// reading the date back through DescribeKey.
	key.KeyState = kmsKeyStatePendingDeletion
	key.Enabled = false
	key.DeletionDate = p.tc.Now().AddDate(0, 0, days)
	if err := p.saveKey(goCtx, key); err != nil {
		return nil, fmt.Errorf("kms scheduleKeyDeletion saveKey: %w", err)
	}
	out := map[string]interface{}{
		"KeyId":               key.ARN,
		"DeletionDate":        key.DeletionDate.Unix(),
		"KeyState":            key.KeyState,
		"PendingWindowInDays": days,
	}
	return kmsJSONResponse(http.StatusOK, out)
}

// cancelKeyDeletion cancels a scheduled deletion, leaving the key disabled.
//
// It no longer delegates to setKeyState, which is what made all three of #963's defects here one
// change: that helper writes whatever state it is handed, answers an empty body, and checks nothing.
// All three are wrong for this operation.
//
// The resulting state is Disabled, not Enabled. API_CancelKeyDeletion's first sentence is explicit —
// "when this operation succeeds, the key state of the KMS key is Disabled. To enable the KMS key, use
// EnableKey" — so recovery from a scheduled deletion is two calls. Answering Enabled collapsed it to
// one, and a consumer's recovery path written against substrate would have passed with its EnableKey
// step missing.
//
// A key that is not pending deletion is refused. CancelKeyDeletion is the only operation in the
// key-state table whose permitted set is a single state, every other row being footnote [4]. Without
// the check the operation enabled any key it was pointed at, which is EnableKey under another name and
// reachable by a caller authorized for one and not the other.
//
// The body is the key ARN under KeyId, the single response element the page publishes, where
// setKeyState answered {} and a consumer reading response["KeyId"] got nothing.
func (p *KMSPlugin) cancelKeyDeletion(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID string `json:"KeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, kmsInvalidBody()
	}
	goCtx := context.Background()
	target, err := p.resolveKeyTarget(goCtx, ctx, input.KeyID)
	if err != nil {
		return nil, err
	}
	key, err := p.loadKey(goCtx, target.AccountID, target.Region, target.KeyID)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, kmsNotFound("Key not found")
	}
	if key.KeyState != kmsKeyStatePendingDeletion {
		return nil, kmsKeyNotPendingDeletion(key.KeyID)
	}
	key.KeyState = kmsKeyStateDisabled
	key.Enabled = false
	key.DeletionDate = time.Time{}
	if err := p.saveKey(goCtx, key); err != nil {
		return nil, fmt.Errorf("kms cancelKeyDeletion saveKey: %w", err)
	}
	return kmsJSONResponse(http.StatusOK, map[string]interface{}{"KeyId": key.ARN})
}

func (p *KMSPlugin) getKeyPolicy(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID      string `json:"KeyId"`
		PolicyName string `json:"PolicyName"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, kmsInvalidBody()
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
		return nil, kmsInvalidBody()
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
		return nil, kmsInvalidBody()
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
		return nil, kmsNotFound("Key not found")
	}
	// No key-state guard, deliberately: see [kmsKeyStateError], which its seven callers use and this one
	// must not. Reading whether rotation is on is permitted in every state substrate can produce, and
	// API_GetKeyRotationStatus does not publish DisabledException at all.
	//
	// It *does* publish KMSInvalidStateException, which an earlier version of this comment denied; the
	// claim was wrong and is corrected here rather than left standing, since it was the stated reason for
	// not guarding. The decision it was defending survives the correction, on better grounds: the page
	// spells out what a pending-deletion key answers, and it is a value rather than a refusal — "While a
	// KMS key is pending deletion, its key rotation status is false and AWS KMS does not rotate the key
	// material." An operation that documents an answer for a state cannot also be refusing that state, so
	// the published KMSInvalidStateException belongs to the states substrate never writes (Creating,
	// Updating, Unavailable), and guarding here would invent a refusal AWS does not have.
	//
	// Substrate reports the stored setting in every state, so that documented false is *not* modeled yet
	// — a key pending deletion still reports true here. That is #973, along with the restore-on-cancel
	// behavior the same paragraph describes.
	//
	// KeyId is the bare key ID, not the ARN. The response element is glossed only "Identifies the
	// specified symmetric encryption KMS key", with none of the "Amazon Resource Name (key ARN)" wording
	// that API_ScheduleKeyDeletion and API_ReEncrypt use for their KeyId elements, and the page's sample
	// response renders "1234abcd-12ab-34cd-56ef-1234567890ab". Where AWS distinguishes the two forms this
	// precisely, echoing the caller's own input would be the wrong answer for an alias or an ARN.
	out := map[string]interface{}{
		"KeyId":              key.KeyID,
		"KeyRotationEnabled": key.RotationEnabled,
	}
	// Emitted only when rotation is on. AWS documents no answer for a key that has never rotated, and a
	// "number of days between each automatic rotation" is not a fact about a key with no rotations — so
	// this is substrate's reading, and the honest-empty one #827 established. The stored period is kept
	// across a DisableKeyRotation regardless (see [KMSKey.RotationPeriodInDays]); what changes is whether
	// it is reported, not whether it is remembered.
	//
	// NextRotationDate is the third member this could carry and does not: it needs the date rotation was
	// enabled, which nothing stores. #973.
	if key.RotationEnabled && key.RotationPeriodInDays > 0 {
		out["RotationPeriodInDays"] = key.RotationPeriodInDays
	}
	return kmsJSONResponse(http.StatusOK, out)
}

// kmsKeyStateError reports the refusal an operation owes a key whose key state does not permit it, or
// nil when the state permits the call.
//
// Seven operations call this: EnableKeyRotation and DisableKeyRotation, which #949 brought here, and
// the five cryptographic operations — Encrypt, Decrypt, GenerateDataKey,
// GenerateDataKeyWithoutPlaintext and ReEncrypt — which #961 added. All seven carry the sentence "the
// KMS key that you use for this operation must be in a compatible key state", and in the developer
// guide's "Key states of AWS KMS keys" table all seven refuse a Disabled key with footnote [1],
// "DisabledException: <key ARN> is disabled".
//
// The order below follows from that split rather than from convenience. ScheduleKeyDeletion writes
// KeyState and clears Enabled together (see [KMSPlugin.scheduleKeyDeletion]), so a lone !key.Enabled
// test would answer DisabledException for a key pending deletion, which points a caller at the wrong
// remedy: EnableKey alone recovers a Disabled key, while a key pending deletion needs
// CancelKeyDeletion and then EnableKey, which #963 made a genuinely two-step recovery.
//
// The pending-deletion code differs in provenance between the two groups, and the same value is
// correct for both:
//
//   - For the rotation pair the table gives footnote [3] alone, "KMSInvalidStateException: <key ARN> is
//     pending deletion (or pending replica deletion)". Answering anything else was simply wrong, which
//     is what #949 fixed.
//   - For the five cryptographic operations the cell reads "[2] or [3]", and footnote [2] is the *same
//     sentence under DisabledException*. So AWS admits both codes there and substrate's earlier
//     DisabledException was within what the table publishes. #961 chose KMSInvalidStateException
//     anyway, and that choice is **substrate's reading, not a match to a single published code**: it is
//     the only one of the two that lets a caller's error handler tell Disabled from PendingDeletion by
//     code, which is the whole reason an emulator models the distinction at all. It also gives one key
//     state one code across the plugin. The message names the state either way, so the two remain
//     distinguishable even for a caller that matches on the code alone.
//
// GetKeyRotationStatus deliberately does not call this. Its row in the same table permits Enabled,
// Disabled *and* pending deletion alike, and API_GetKeyRotationStatus publishes neither code — so a
// later sweep that guarded every key-state-sensitive operation "for consistency" would introduce a
// refusal AWS does not have. [TestKMSGetKeyRotationStatus_AnswersForADisabledAndAPendingDeletionKey]
// pins that.
//
// EnableKey and DisableKey must not call this either, and for a different reason: their rows permit a
// Disabled key, so the !key.Enabled arm here would refuse a call AWS accepts. They need the
// PendingDeletion arm alone, which is #968.
func kmsKeyStateError(key *KMSKey) *AWSError {
	if key.KeyState == kmsKeyStatePendingDeletion {
		return kmsInvalidKeyState(key.KeyID, key.KeyState)
	}
	if !key.Enabled {
		return kmsKeyDisabled(key.KeyID)
	}
	return nil
}

// enableKeyRotation turns on automatic rotation and records the period it will use.
//
// RotationPeriodInDays is a pointer so that an absent member and an explicit 0 are different requests.
// The distinction is load-bearing: absent means "use the default", which AWS puts at 365, while 0 is a
// value the caller chose and it is below the published minimum of 90, so it is refused. A plain int
// would collapse the two and silently accept 0 as 365 — the defect #964 describes, reintroduced one
// level down.
//
// The period is not validated against the *stored* one, because AWS documents the parameter as able to
// "modify the rotation period of a key that you previously enabled automatic key rotation on". A second
// call is a legitimate change rather than a conflict, and a second call that omits the member resets the
// period to the default; see [kmsDefaultRotationPeriodInDays] for why that reading is taken and pinned
// rather than the more intuitive "leave what was there".
func (p *KMSPlugin) enableKeyRotation(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		KeyID                string `json:"KeyId"`
		RotationPeriodInDays *int   `json:"RotationPeriodInDays"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, kmsInvalidBody()
	}
	// Validated before the key is even resolved, because a range violation does not depend on the key
	// and answering NotFoundException for a request that is malformed regardless would tell the caller
	// to fix the wrong thing.
	period := kmsDefaultRotationPeriodInDays
	if input.RotationPeriodInDays != nil {
		period = *input.RotationPeriodInDays
		if period < kmsMinRotationPeriodInDays || period > kmsMaxRotationPeriodInDays {
			return nil, kmsInvalidRotationPeriod(period)
		}
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
		return nil, kmsNotFound("Key not found")
	}
	// Before the write, so a refusal leaves RotationEnabled as it was rather than half-applying the
	// call it declined — the property #949 asks to be asserted by reading GetKeyRotationStatus back.
	if stateErr := kmsKeyStateError(key); stateErr != nil {
		return nil, stateErr
	}
	key.RotationEnabled = true
	key.RotationPeriodInDays = period
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
		return nil, kmsInvalidBody()
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
		return nil, kmsNotFound("Key not found")
	}
	// Refused on the same key states as its sibling: API_DisableKeyRotation publishes the identical
	// seven-error list, so "rotation is already off, so turning it off cannot hurt" is not a reading
	// AWS's table supports.
	if stateErr := kmsKeyStateError(key); stateErr != nil {
		return nil, stateErr
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
		return nil, kmsInvalidBody()
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
		return nil, kmsNotFound("Key not found")
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
		return nil, kmsInvalidBody()
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
		return nil, kmsNotFound("Key not found")
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
		return nil, kmsInvalidBody()
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
		return nil, kmsNotFound("Key not found")
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
		return nil, kmsInvalidBody()
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
		return nil, kmsInvalidBody()
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
		return nil, kmsInvalidBody()
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
		return nil, kmsInvalidBody()
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
		return nil, kmsNotFound("Key not found")
	}
	// [kmsKeyStateError] rather than a bare !key.Enabled test: the two states this can be in owe two
	// different codes, and until #961 both answered DisabledException.
	if stateErr := kmsKeyStateError(key); stateErr != nil {
		return nil, stateErr
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
		return nil, kmsInvalidBody()
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
		return nil, kmsNotFound("Key not found")
	}
	// [kmsKeyStateError] rather than a bare !key.Enabled test: the two states this can be in owe two
	// different codes, and until #961 both answered DisabledException.
	if stateErr := kmsKeyStateError(key); stateErr != nil {
		return nil, stateErr
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
		return nil, kmsInvalidBody()
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
		return nil, kmsNotFound("Key not found")
	}
	// [kmsKeyStateError] rather than a bare !key.Enabled test: the two states this can be in owe two
	// different codes, and until #961 both answered DisabledException.
	if stateErr := kmsKeyStateError(key); stateErr != nil {
		return nil, stateErr
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
		return nil, kmsInvalidBody()
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
		return nil, kmsNotFound("Key not found")
	}
	// Added by #961, where the other four cryptographic operations only had the wrong code for one of
	// two states: this one refused *nothing*, so a disabled key and a key pending deletion both minted a
	// data key and answered 200. Its row in the key-state table is identical to Encrypt's.
	if stateErr := kmsKeyStateError(key); stateErr != nil {
		return nil, stateErr
	}

	dataKeyHex := randomHex(16)
	ciphertext := kmsEncryptStub(keyID, []byte(dataKeyHex))

	out := map[string]interface{}{
		"KeyId":          key.ARN,
		"CiphertextBlob": string(ciphertext),
	}
	return kmsJSONResponse(http.StatusOK, out)
}

// reEncrypt decrypts a ciphertext under the key that produced it and re-encrypts the plaintext under a
// destination key.
//
// It is the one cryptographic operation with two keys, and #961 changed what that means here. Before
// it, only the destination key was loaded: the source key ID came out of the ciphertext and was
// discarded, so a source key that was disabled, pending deletion or deleted outright still
// re-encrypted, and the SourceKeyId in the response was **the ciphertext blob** rather than any key
// identifier at all.
//
// Both keys are now checked, and the table does not exempt either. API_ReEncrypt carries the same
// "must be in a compatible key state" sentence as the other four, publishes DisabledException and
// KMSInvalidStateException alike, and its "Required permissions" are split across the two keys —
// kms:ReEncryptFrom on the source and kms:ReEncryptTo on the destination — so AWS treats the source as
// a key the operation *uses*, not merely as a value inside the ciphertext. The one footnote that would
// have exempted a pending-deletion source, [10] ("if the source KMS key is pending deletion, the
// command succeeds"), belongs to UpdateAlias's row, not to this one; ReEncrypt's cell is a plain
// "[2] or [3]". Refusing both is therefore substrate's reading of an unsplit row rather than a
// published rule, and it is the conservative direction: it cannot let through a call AWS refuses.
//
// The source is checked first, following the operation's own description — "Decrypts ciphertext and
// then reencrypts it" — so a caller with two unusable keys is told about the half that fails first.
//
// The source key is loaded from the *caller's* account and Region for the reason [KMSPlugin.decrypt]
// records: the key ID came out of the ciphertext, so there is no ARN to take an account from. AWS
// permits a cross-account source here; substrate cannot address one until a ciphertext carries more
// than a bare key ID, which is the stub's shape (see [kmsEncryptStub]).
//
// Seven request members and four response members are still unmodelled, SourceKeyId's *request* form
// among them — so the IncorrectKeyException AWS publishes when it names the wrong key is unreachable.
// That is #969; this handler fixes SourceKeyId's value, not its enforcement.
func (p *KMSPlugin) reEncrypt(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		CiphertextBlob   string `json:"CiphertextBlob"`
		DestinationKeyID string `json:"DestinationKeyId"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, kmsInvalidBody()
	}

	sourceKeyID, plaintext, err := kmsDecryptStub([]byte(input.CiphertextBlob))
	if err != nil {
		return nil, &AWSError{Code: "InvalidCiphertextException", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	sourceKey, err := p.loadKey(goCtx, ctx.AccountID, ctx.Region, sourceKeyID)
	if err != nil {
		return nil, err
	}
	if sourceKey == nil {
		return nil, kmsNotFound("Source key not found")
	}
	if stateErr := kmsKeyStateError(sourceKey); stateErr != nil {
		return nil, stateErr
	}

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
		return nil, kmsNotFound("Destination key not found")
	}
	if stateErr := kmsKeyStateError(destKey); stateErr != nil {
		return nil, stateErr
	}

	newCiphertext := kmsEncryptStub(destKeyID, plaintext)
	// SourceKeyId is the source key's ARN, matching the sample response on API_ReEncrypt and the
	// element's own gloss, "unique identifier of the KMS key used to originally encrypt the data". It
	// held input.CiphertextBlob until #961 — a value that is not an identifier of anything, and one a
	// caller round-tripping it into a DescribeKey could only ever get NotFoundException from.
	out := map[string]interface{}{
		"KeyId":          destKey.ARN,
		"CiphertextBlob": string(newCiphertext),
		"SourceKeyId":    sourceKey.ARN,
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
