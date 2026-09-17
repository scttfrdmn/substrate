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
// UpdateSecret, DeleteSecret, RestoreSecret, ListSecrets, ListSecretVersionIds,
// TagResource, UntagResource, and RotateSecret.
//
// It deliberately does not handle ListTagsForResource, which the Secrets Manager API does not publish
// among its twenty-three operations; substrate answered one until #929. Tags are read through
// DescribeSecret.
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
	case "RestoreSecret":
		return p.restoreSecret(ctx, req)
	case "ListSecrets":
		return p.listSecrets(ctx, req)
	case "ListSecretVersionIds":
		return p.listSecretVersionIDs(ctx, req)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	// No ListTagsForResource arm: the Secrets Manager API does not publish that operation, and
	// substrate answered it until #929. A secret's tags are read through DescribeSecret's Tags member,
	// which is the read path #928's tagging row asserts through. The name now falls to
	// unknownActionError like any other operation AWS does not have, which is what a caller reaching
	// for it against real AWS gets.
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
	// "When a secret is scheduled for deletion, you cannot retrieve the secret value." The refusal is
	// InvalidRequestException, whose first published cause on this page is "The secret is scheduled for
	// deletion." — a different answer from the ResourceNotFoundException a secret that does not exist
	// gets, which is the distinction #953 exists to make observable.
	if !secret.DeletionDate.IsZero() {
		return nil, smSecretScheduledForDeletion(input.SecretID, secret.DeletionDate)
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

	return smJSONResponse(http.StatusOK, smDescribeSecretBody(secret))
}

// smDescribeSecretBody renders a secret as DescribeSecret's response, emitting only the members AWS
// emits for it.
//
// AWS's rule is one sentence on API_DescribeSecret — "Secrets Manager only returns fields that have a
// value in the response" — but the page does **not** apply it uniformly, and the difference is
// load-bearing because a caller can distinguish the two outcomes:
//
//   - Four members are documented "this field is omitted": DeletedDate, KmsKeyId ("If the secret is
//     encrypted with the AWS managed key aws/secretsmanager"), LastAccessedDate and RotationRules.
//     **That tier is AWS's**, stated per member.
//   - Three are documented "Secrets Manager returns null": LastRotatedDate, NextRotationDate and
//     RotationEnabled ("If the secret has never been configured for rotation"). A null member is
//     present, so these are emitted rather than omitted.
//   - The rest — ARN, CreatedDate, Description, LastChangedDate, Name, Tags and the others — carry no
//     per-member statement at all, so omitting an empty one rests on the blanket sentence alone.
//     **That tier is substrate's reading**, and #930's own criterion named Description as if AWS had
//     stated it, which the page does not.
//
// RotationEnabled needs no extra state to answer correctly: [SecretState.RotationEnabled] is set by
// RotateSecret and by nothing else, and substrate models no CancelRotateSecret, so false is exactly
// AWS's "never been configured for rotation" and true is exactly "configured". A false value would
// therefore be a claim AWS does not make, which is why it is emitted as null.
//
// DeletedDate is the one member of the omitted tier substrate does answer, since #953: it is emitted
// exactly when a recovery window is open, which is the observation that distinguishes a secret
// scheduled for deletion from a live one, and it is omitted otherwise — the omission AWS documents.
// Note the asymmetry in AWS's own naming, which substrate follows rather than tidies: DeleteSecret
// answers the stamp as DeletionDate and DescribeSecret reports the same stamp as DeletedDate.
//
// RotationLambdaARN and RotationRules are answered since #952, and they fall in different tiers, which
// is why they are emitted by different conditions. RotationRules is one of the four members AWS itself
// documents an omission for — "if the secret never had rotation turned on, this field is omitted" — so
// its absence is AWS's own contract and [SecretState.RotationRules] is a pointer to keep a
// never-configured schedule distinguishable from an empty one. RotationLambdaARN carries no per-member
// statement, so omitting an empty one rests on the blanket sentence, which puts it in substrate's tier.
//
// The members substrate does not model stay absent, which the same sentence makes correct rather than
// a gap: LastRotatedDate and NextRotationDate (no rotation function runs here, so nothing records a
// rotation time), LastAccessedDate, OwningService, PrimaryRegion, ReplicationStatus,
// VersionIdsToStages, and the three managed-external-secret members AWS publishes — Type,
// ExternalSecretRotationRoleArn and ExternalSecretRotationMetadata — which belong to a partner
// integration substrate models nothing of.
func smDescribeSecretBody(secret *SecretState) map[string]interface{} {
	out := map[string]interface{}{
		"ARN":  secret.ARN,
		"Name": secret.Name,
	}
	if secret.Description != "" {
		out["Description"] = secret.Description
	}
	if secret.KMSKeyID != "" {
		out["KmsKeyId"] = secret.KMSKeyID
	}
	if !secret.CreatedDate.IsZero() {
		out["CreatedDate"] = secret.CreatedDate.Unix()
	}
	if !secret.LastChangedDate.IsZero() {
		out["LastChangedDate"] = secret.LastChangedDate.Unix()
	}
	if !secret.DeletionDate.IsZero() {
		out["DeletedDate"] = secret.DeletionDate.Unix()
	}
	// Emitted either way, because AWS documents a null rather than an omission — see above. A nil
	// interface value is what renders the JSON null.
	if secret.RotationEnabled {
		out["RotationEnabled"] = true
	} else {
		out["RotationEnabled"] = nil
	}
	if secret.RotationLambdaARN != "" {
		out["RotationLambdaARN"] = secret.RotationLambdaARN
	}
	if secret.RotationRules != nil {
		out["RotationRules"] = secret.RotationRules
	}
	// This is the operation a caller reads a secret's tags back through, Secrets Manager publishing no
	// ListTagsForResource at all (#929). An untagged secret emitted "Tags": null before #928, which is
	// not a member AWS sends and not the null AWS documents for the three rotation members.
	if len(secret.Tags) > 0 {
		out["Tags"] = secret.Tags
	}
	return out
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

// DeleteSecret, RestoreSecret and the recovery window they share live in
// secretsmanager_deletion.go, whose file preamble carries the reasoning for scheduling rather than
// removing (#953) — including why the permanent deletion at the end of the window is deliberately
// unmodelled.

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

// rotateSecret lives in secretsmanager_rotation.go, beside the schedule it configures and the two
// InvalidRequestException causes it refuses — see that file's preamble (#952). It read one of its seven
// parameters here and enabled rotation on any secret it could load, including one with no rotation
// function at all.

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
