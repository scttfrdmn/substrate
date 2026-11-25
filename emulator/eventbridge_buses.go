package emulator

// EventBridge event buses: CreateEventBus, DescribeEventBus, UpdateEventBus, DeleteEventBus,
// ListEventBuses, and the bus resource policy PutPermission and RemovePermission edit (#1613).
//
// # The default bus
//
// Every account has a default bus in every Region, and no call creates or deletes it ("You can't
// use the name default for a custom event bus, as this name is already used for your account's
// default event bus", API_CreateEventBus). It
// has no record until a write gives it one — PutPermission, UpdateEventBus or a tag write — and is
// synthesized on every read until then ([ebLoadBusAt]), so ListEventBuses and DescribeEventBus
// answer it in an account substrate has never seen. It carries no CreationTime or LastModifiedTime:
// its creation predates anything substrate observed.
//
// # Partner buses
//
// A partner bus is matched to a partner event source, and substrate models no partner event
// sources, so CreateEventBus with an EventSourceName answers ResourceNotFoundException for the
// source. A custom bus name "can't contain the / character", so every stored bus name is free of
// it, which [ebRuleRef.key] relies on.
//
// # Codes this file does not produce
//
// CreateEventBus also publishes ConcurrentModificationException and InvalidStateException.
// Substrate serializes every bus and rule write under [EventBridgePlugin.ruleMu], so no two writes
// are concurrent, and InvalidStateException is "not a valid state for an event source", which needs
// the partner event sources substrate does not model.
//
// # Wording
//
// "Event bus X does not exist." and "Event bus X already exists." are AWS's text as observed
// (terraform-provider-aws#31634 for DescribeEventBus; Stack Overflow 76065443 for CreateEventBus),
// and so is "EventBus can't be deleted since it has rules." (terraform-provider-aws#22131,
// serverless/serverless#8479). Every other message here is substrate's own.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// ebBusKeyPrefix is the colon-terminated prefix every bus record is stored under; see [ebBusKey].
const ebBusKeyPrefix = "ebbus:"

// ebMaxEventBuses is the "Event buses" quota, "Maximum event buses per account", 100 per Region.
// Substrate counts the default bus among them, so an account can create 99 custom buses.
const ebMaxEventBuses = 100

// ebMaxPolicyLength is the "Event bus policy size" quota: "Maximum policy size, in characters".
const ebMaxPolicyLength = 10240

var (
	// ebBusNamePattern is CreateEventBus's Name pattern and length: `[/\.\-_A-Za-z0-9]+`, 1 to 256.
	ebBusNamePattern = regexp.MustCompile(`^[/.\-_A-Za-z0-9]{1,256}$`)

	// ebKMSKeyPattern is KmsKeyIdentifier's pattern: `^[a-zA-Z0-9_\-/:]*$`, at most 2048.
	ebKMSKeyPattern = regexp.MustCompile(`^[a-zA-Z0-9_\-/:]*$`)

	// ebStatementIDPattern is PutPermission's StatementId pattern: `[a-zA-Z0-9-_]+`, 1 to 64.
	ebStatementIDPattern = regexp.MustCompile(`^[a-zA-Z0-9\-_]{1,64}$`)

	// ebPermissionActionPattern is PutPermission's Action pattern: `events:[a-zA-Z]+`, 1 to 64.
	ebPermissionActionPattern = regexp.MustCompile(`^events:[a-zA-Z]{1,57}$`)

	// ebPrincipalPattern is PutPermission's Principal pattern: `(\d{12}|\*)`.
	ebPrincipalPattern = regexp.MustCompile(`^(\d{12}|\*)$`)
)

// ebBusKey is the state key of a bus record.
func ebBusKey(accountID, region, name string) string {
	return ebBusKeyPrefix + accountID + "/" + region + "/" + name
}

// ebBusNotFound reports that a bus does not exist; see this file's preamble for the wording.
func ebBusNotFound(name string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    "Event bus " + name + " does not exist.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ebLoadBusAt reads the bus stored at key, reporting false when there is none. The default bus is
// always found: an unwritten one is synthesized from the key's account and Region.
func ebLoadBusAt(ctx context.Context, state StateManager, key string) (*EBEventBus, bool, error) {
	raw, err := state.Get(ctx, eventbridgeNamespace, key)
	if err != nil {
		return nil, false, fmt.Errorf("eventbridge load bus %s: %w", key, err)
	}
	if raw != nil {
		var bus EBEventBus
		if err := json.Unmarshal(raw, &bus); err != nil {
			return nil, false, fmt.Errorf("eventbridge decode bus %s: %w", key, err)
		}
		return &bus, true, nil
	}
	parts := strings.SplitN(strings.TrimPrefix(key, ebBusKeyPrefix), "/", 3)
	if len(parts) == 3 && parts[2] == ebDefaultBus {
		return &EBEventBus{Name: ebDefaultBus, ARN: ebBusARN(parts[1], parts[0], ebDefaultBus)}, true, nil
	}
	return nil, false, nil
}

// loadBus reads the named bus in an account and Region; see [ebLoadBusAt].
func (p *EventBridgePlugin) loadBus(ctx context.Context, accountID, region, name string) (*EBEventBus, bool, error) {
	return ebLoadBusAt(ctx, p.state, ebBusKey(accountID, region, name))
}

// saveBus writes a bus record, stamping LastModifiedTime on a custom bus. The default bus keeps no
// timestamps; see this file's preamble.
func (p *EventBridgePlugin) saveBus(ctx context.Context, accountID, region string, bus *EBEventBus) error {
	if !bus.CreatedAt.IsZero() {
		bus.UpdatedAt = p.tc.Now().UTC()
	}
	data, err := json.Marshal(bus)
	if err != nil {
		return fmt.Errorf("eventbridge encode bus %s: %w", bus.Name, err)
	}
	if err := p.state.Put(ctx, eventbridgeNamespace, ebBusKey(accountID, region, bus.Name), data); err != nil {
		return fmt.Errorf("eventbridge store bus %s: %w", bus.Name, err)
	}
	return nil
}

// ebCheckBusConfig refuses a Description, KmsKeyIdentifier or LogConfig outside the constraints
// CreateEventBus and UpdateEventBus publish.
func ebCheckBusConfig(description, kmsKey string, logConfig *EBLogConfig) *AWSError {
	if len(description) > 512 {
		return ebValidationError("Description must be at most 512 characters")
	}
	if len(kmsKey) > 2048 || !ebKMSKeyPattern.MatchString(kmsKey) {
		return ebValidationError(fmt.Sprintf("KmsKeyIdentifier %q must be at most 2048 characters from [a-zA-Z0-9_-/:]", kmsKey))
	}
	if logConfig != nil {
		switch logConfig.IncludeDetail {
		case "", "FULL", "NONE":
		default:
			return ebValidationError(fmt.Sprintf("LogConfig.IncludeDetail %q must be FULL or NONE", logConfig.IncludeDetail))
		}
		switch logConfig.Level {
		case "", "OFF", "ERROR", "INFO", "TRACE":
		default:
			return ebValidationError(fmt.Sprintf("LogConfig.Level %q must be OFF, ERROR, INFO or TRACE", logConfig.Level))
		}
	}
	return nil
}

// ebNonEmptyDLQ returns cfg, or nil when it names no queue, which is how a caller clears it.
func ebNonEmptyDLQ(cfg *EBDeadLetterConfig) *EBDeadLetterConfig {
	if cfg == nil || cfg.Arn == "" {
		return nil
	}
	return cfg
}

// ebNonEmptyLogConfig returns cfg, or nil when it sets neither member, which is how a caller
// clears it.
func ebNonEmptyLogConfig(cfg *EBLogConfig) *EBLogConfig {
	if cfg == nil || (cfg.IncludeDetail == "" && cfg.Level == "") {
		return nil
	}
	return cfg
}

// ebBusConfigWire is the configuration CreateEventBus and UpdateEventBus answer with.
type ebBusConfigWire struct {
	Description      string              `json:"Description,omitempty"`
	KmsKeyIdentifier string              `json:"KmsKeyIdentifier,omitempty"`
	DeadLetterConfig *EBDeadLetterConfig `json:"DeadLetterConfig,omitempty"`
	LogConfig        *EBLogConfig        `json:"LogConfig,omitempty"`
}

// ebBusConfig projects a bus onto [ebBusConfigWire].
func ebBusConfig(b *EBEventBus) ebBusConfigWire {
	return ebBusConfigWire{
		Description: b.Description, KmsKeyIdentifier: b.KmsKeyIdentifier,
		DeadLetterConfig: b.DeadLetterConfig, LogConfig: b.LogConfig,
	}
}

// createEventBus handles CreateEventBus.
func (p *EventBridgePlugin) createEventBus(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name             string              `json:"Name"`
		Description      string              `json:"Description"`
		EventSourceName  string              `json:"EventSourceName"`
		KmsKeyIdentifier string              `json:"KmsKeyIdentifier"`
		DeadLetterConfig *EBDeadLetterConfig `json:"DeadLetterConfig"`
		LogConfig        *EBLogConfig        `json:"LogConfig"`
		Tags             []EBTag             `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	if body.Name == "" {
		return nil, ebValidationError("Name is required")
	}
	if !ebBusNamePattern.MatchString(body.Name) {
		return nil, ebValidationError(fmt.Sprintf("Name %q must be 1 to 256 characters from [/.-_A-Za-z0-9]", body.Name))
	}
	if body.EventSourceName != "" {
		return nil, &AWSError{
			Code:       "ResourceNotFoundException",
			Message:    fmt.Sprintf("partner event source %s does not exist; substrate models no partner event sources", body.EventSourceName),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if strings.Contains(body.Name, "/") {
		return nil, ebValidationError("Custom event bus names can't contain the / character.")
	}
	if cfgErr := ebCheckBusConfig(body.Description, body.KmsKeyIdentifier, body.LogConfig); cfgErr != nil {
		return nil, cfgErr
	}
	if tagErr := ebValidateTags(body.Tags); tagErr != nil {
		return nil, tagErr
	}
	tags := ebMergeTags(nil, body.Tags, nil)
	if quotaErr := ebCheckTagQuota(body.Name, len(tags)); quotaErr != nil {
		return nil, quotaErr
	}
	if len(tags) == 0 {
		tags = nil
	}

	goCtx := context.Background()
	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	if _, found, err := p.loadBus(goCtx, ctx.AccountID, ctx.Region, body.Name); err != nil {
		return nil, err
	} else if found {
		return nil, &AWSError{
			Code:       "ResourceAlreadyExistsException",
			Message:    "Event bus " + body.Name + " already exists.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	custom, err := p.state.List(goCtx, eventbridgeNamespace, ebBusKey(ctx.AccountID, ctx.Region, ""))
	if err != nil {
		return nil, fmt.Errorf("eventbridge createEventBus list: %w", err)
	}
	if ebCountCustomBuses(custom)+1 >= ebMaxEventBuses {
		return nil, &AWSError{
			Code:       "LimitExceededException",
			Message:    fmt.Sprintf("this account already has %d event buses in %s, the most it can have", ebMaxEventBuses, ctx.Region),
			HTTPStatus: http.StatusBadRequest,
		}
	}

	now := p.tc.Now().UTC()
	bus := &EBEventBus{
		Name:             body.Name,
		ARN:              ebBusARN(ctx.Region, ctx.AccountID, body.Name),
		Description:      body.Description,
		KmsKeyIdentifier: body.KmsKeyIdentifier,
		DeadLetterConfig: ebNonEmptyDLQ(body.DeadLetterConfig),
		LogConfig:        ebNonEmptyLogConfig(body.LogConfig),
		Tags:             tags,
		EverTagged:       len(tags) > 0,
		CreatedAt:        now,
	}
	if err := p.saveBus(goCtx, ctx.AccountID, ctx.Region, bus); err != nil {
		return nil, err
	}
	return ebJSONResponse(http.StatusOK, struct {
		EventBusArn string `json:"EventBusArn"`
		ebBusConfigWire
	}{EventBusArn: bus.ARN, ebBusConfigWire: ebBusConfig(bus)})
}

// ebCountCustomBuses counts the custom buses among a listing of bus keys: every key but the
// default bus's, which is stored once a write has given it a record.
func ebCountCustomBuses(keys []string) int {
	n := 0
	for _, k := range keys {
		if !strings.HasSuffix(k, "/"+ebDefaultBus) {
			n++
		}
	}
	return n
}

// describeEventBus handles DescribeEventBus. Name is the bus's name or ARN, and the default bus when
// omitted ("If you omit this, the default event bus is displayed").
func (p *EventBridgePlugin) describeEventBus(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name string `json:"Name"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, ebInvalidBody()
		}
	}
	name, busErr := ebRequestBus(ctx, body.Name)
	if busErr != nil {
		return nil, busErr
	}
	bus, found, err := p.loadBus(context.Background(), ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ebBusNotFound(name)
	}
	return ebJSONResponse(http.StatusOK, ebDescribeEventBusWire{
		ebEventBusWire:   ebBusToWire(bus),
		KmsKeyIdentifier: bus.KmsKeyIdentifier,
		DeadLetterConfig: bus.DeadLetterConfig,
		LogConfig:        bus.LogConfig,
	})
}

// updateEventBus handles UpdateEventBus. A member the request carries replaces the stored value
// and one it omits keeps it; an empty Description, KmsKeyIdentifier, DeadLetterConfig.Arn or
// LogConfig clears that member. That is the reading terraform-provider-aws's update relies on —
// "To unset the description, the only way is to explicitly set it to the empty string" — since the
// page states no rule of its own.
func (p *EventBridgePlugin) updateEventBus(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name             string              `json:"Name"`
		Description      *string             `json:"Description"`
		KmsKeyIdentifier *string             `json:"KmsKeyIdentifier"`
		DeadLetterConfig *EBDeadLetterConfig `json:"DeadLetterConfig"`
		LogConfig        *EBLogConfig        `json:"LogConfig"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	description, kmsKey := "", ""
	if body.Description != nil {
		description = *body.Description
	}
	if body.KmsKeyIdentifier != nil {
		kmsKey = *body.KmsKeyIdentifier
	}
	if cfgErr := ebCheckBusConfig(description, kmsKey, body.LogConfig); cfgErr != nil {
		return nil, cfgErr
	}

	goCtx := context.Background()
	name, busErr := ebRequestBus(ctx, body.Name)
	if busErr != nil {
		return nil, busErr
	}
	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	bus, found, err := p.loadBus(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ebBusNotFound(name)
	}
	if body.Description != nil {
		bus.Description = description
	}
	if body.KmsKeyIdentifier != nil {
		bus.KmsKeyIdentifier = kmsKey
	}
	if body.DeadLetterConfig != nil {
		bus.DeadLetterConfig = ebNonEmptyDLQ(body.DeadLetterConfig)
	}
	if body.LogConfig != nil {
		bus.LogConfig = ebNonEmptyLogConfig(body.LogConfig)
	}
	if err := p.saveBus(goCtx, ctx.AccountID, ctx.Region, bus); err != nil {
		return nil, err
	}
	return ebJSONResponse(http.StatusOK, struct {
		Arn  string `json:"Arn"`
		Name string `json:"Name"`
		ebBusConfigWire
	}{Arn: bus.ARN, Name: bus.Name, ebBusConfigWire: ebBusConfig(bus)})
}

// deleteEventBus handles DeleteEventBus. "All rules associated with this event bus need to be
// deleted. You can't delete your account's default event bus." Neither refusal has a code on the
// page; both answer ValidationException, the code AWS was observed answering for a bus with rules
// (see this file's preamble), and moto and Floci answer for the default bus. A bus that does not
// exist is deleted without complaint: the page publishes no ResourceNotFoundException, and
// terraform-provider-aws treats one as already gone.
func (p *EventBridgePlugin) deleteEventBus(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	if body.Name == "" {
		return nil, ebValidationError("Name is required")
	}
	if body.Name == ebDefaultBus {
		return nil, &AWSError{
			Code:       "ValidationException",
			Message:    "the default event bus cannot be deleted",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	goCtx := context.Background()
	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	names, err := loadStringIndex(goCtx, p.state, eventbridgeNamespace, ebRuleNamesKey(ctx.AccountID, ctx.Region, body.Name))
	if err != nil {
		return nil, fmt.Errorf("eventbridge deleteEventBus rule index: %w", err)
	}
	if len(names) > 0 {
		return nil, &AWSError{
			Code:       "ValidationException",
			Message:    "EventBus can't be deleted since it has rules.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if err := p.state.Delete(goCtx, eventbridgeNamespace, ebBusKey(ctx.AccountID, ctx.Region, body.Name)); err != nil {
		return nil, fmt.Errorf("eventbridge deleteEventBus: %w", err)
	}
	return ebJSONResponse(http.StatusOK, struct{}{})
}

// listEventBuses handles ListEventBuses: the default bus and every custom bus in the caller's
// account and Region whose name starts with NamePrefix, in name order, paged by [page].
func (p *EventBridgePlugin) listEventBuses(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		NamePrefix string `json:"NamePrefix"`
		NextToken  string `json:"NextToken"`
		Limit      int    `json:"Limit"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, ebInvalidBody()
		}
	}
	if body.Limit < 0 || body.Limit > 100 {
		return nil, ebValidationError("Limit must be between 1 and 100")
	}
	pr := pageRequest{
		Op:     "ListEventBuses",
		Filter: map[string][]string{"NamePrefix": {body.NamePrefix}},
		Token:  body.NextToken,
		Size:   body.Limit,
	}
	if pr.validate() != nil {
		return nil, ebInvalidToken()
	}

	goCtx := context.Background()
	keys, err := p.state.List(goCtx, eventbridgeNamespace, ebBusKey(ctx.AccountID, ctx.Region, ""))
	if err != nil {
		return nil, fmt.Errorf("eventbridge listEventBuses: %w", err)
	}
	defaultKey := ebBusKey(ctx.AccountID, ctx.Region, ebDefaultBus)
	keys = append(keys, defaultKey)
	seen := make(map[string]bool, len(keys))
	buses := make([]*EBEventBus, 0, len(keys))
	for _, k := range keys {
		if seen[k] {
			continue
		}
		seen[k] = true
		bus, found, err := ebLoadBusAt(goCtx, p.state, k)
		if err != nil {
			return nil, err
		}
		if found && strings.HasPrefix(bus.Name, body.NamePrefix) {
			buses = append(buses, bus)
		}
	}
	res, err := page(buses, func(b *EBEventBus) cursorKey { return cursorKey{Sort: b.Name} }, pr)
	if err != nil {
		return nil, ebInvalidToken()
	}
	out := struct {
		EventBuses []ebEventBusWire `json:"EventBuses"`
		NextToken  string           `json:"NextToken,omitempty"`
	}{EventBuses: make([]ebEventBusWire, 0, len(res.Items)), NextToken: res.Next}
	for _, b := range res.Items {
		out.EventBuses = append(out.EventBuses, ebBusToWire(b))
	}
	return ebJSONResponse(http.StatusOK, out)
}

// ----- Bus policy ----------------------------------------------------------

// ebPolicyDocument is a bus resource policy as PutPermission writes it. Statements are kept raw so
// a statement written through Policy keeps every member it was given.
type ebPolicyDocument struct {
	Version   string            `json:"Version"`
	ID        string            `json:"Id,omitempty"`
	Statement []json.RawMessage `json:"Statement"`
}

// ebPolicyStatement is the statement PutPermission builds from StatementId, Action, Principal and
// Condition: Principal "*" or {"AWS": "arn:aws:iam::{account}:root"}, Resource the bus ARN, and
// Condition {Type: {Key: Value}}. That is the shape terraform-provider-aws's
// aws_cloudwatch_event_permission reads back from DescribeEventBus.
type ebPolicyStatement struct {
	Sid       string                       `json:"Sid"`
	Effect    string                       `json:"Effect"`
	Principal any                          `json:"Principal"`
	Action    string                       `json:"Action"`
	Resource  string                       `json:"Resource"`
	Condition map[string]map[string]string `json:"Condition,omitempty"`
}

// ebParsePolicy decodes a bus policy, accepting a Statement given as one object as well as a list.
// An empty policy is an empty document. A policy that is not a JSON object, or a Statement that is
// neither an object nor a list of objects, is an error: PutPermission refuses one on the way in, so
// every later statement edit can decode what is stored.
func ebParsePolicy(policy string) (ebPolicyDocument, error) {
	doc := ebPolicyDocument{Version: "2012-10-17"}
	if policy == "" {
		return doc, nil
	}
	if !ebIsJSONObject([]byte(policy)) {
		return doc, fmt.Errorf("bus policy is not a JSON object")
	}
	var raw struct {
		Version   string          `json:"Version"`
		ID        string          `json:"Id"`
		Statement json.RawMessage `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(policy), &raw); err != nil {
		return doc, fmt.Errorf("decode bus policy: %w", err)
	}
	if raw.Version != "" {
		doc.Version = raw.Version
	}
	doc.ID = raw.ID
	trimmed := strings.TrimSpace(string(raw.Statement))
	switch {
	case trimmed == "" || trimmed == "null":
	case strings.HasPrefix(trimmed, "["):
		if err := json.Unmarshal(raw.Statement, &doc.Statement); err != nil {
			return doc, fmt.Errorf("decode bus policy statements: %w", err)
		}
	default:
		doc.Statement = []json.RawMessage{raw.Statement}
	}
	for _, s := range doc.Statement {
		if !ebIsJSONObject(s) {
			return doc, fmt.Errorf("bus policy statement %s is not a JSON object", s)
		}
	}
	return doc, nil
}

// ebIsJSONObject reports whether raw is a JSON object.
func ebIsJSONObject(raw []byte) bool {
	var m map[string]json.RawMessage
	return json.Unmarshal(raw, &m) == nil && m != nil
}

// ebStatementSid returns the Sid of a raw statement, empty when it has none.
func ebStatementSid(raw json.RawMessage) string {
	var s struct {
		Sid string `json:"Sid"`
	}
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s.Sid
}

// ebPolicyTooLong is PolicyLengthExceededException, "The event bus policy is too long", at 400.
func ebPolicyTooLong(n int) *AWSError {
	return &AWSError{
		Code:       "PolicyLengthExceededException",
		Message:    fmt.Sprintf("the event bus policy would be %d characters; it can be at most %d", n, ebMaxPolicyLength),
		HTTPStatus: http.StatusBadRequest,
	}
}

// putPermission handles PutPermission: it sets the bus policy to Policy, or adds (replacing one with
// the same Sid) the statement StatementId, Action, Principal and Condition describe. The page offers
// Policy "instead of" the other four, so substrate refuses a request carrying both.
func (p *EventBridgePlugin) putPermission(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Action       string `json:"Action"`
		EventBusName string `json:"EventBusName"`
		Policy       string `json:"Policy"`
		Principal    string `json:"Principal"`
		StatementID  string `json:"StatementId"`
		Condition    *struct {
			Key   string `json:"Key"`
			Type  string `json:"Type"`
			Value string `json:"Value"`
		} `json:"Condition"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	statement := body.StatementID != "" || body.Action != "" || body.Principal != "" || body.Condition != nil
	switch {
	case body.Policy != "" && statement:
		return nil, ebValidationError("Policy cannot be combined with StatementId, Action, Principal or Condition")
	case body.Policy != "":
		if _, err := ebParsePolicy(body.Policy); err != nil {
			return nil, ebValidationError("Policy must be a JSON object whose Statement is an object or a list of objects")
		}
		if len(body.Policy) > ebMaxPolicyLength {
			return nil, ebPolicyTooLong(len(body.Policy))
		}
	case body.StatementID == "" || body.Action == "" || body.Principal == "":
		return nil, ebValidationError("StatementId, Action and Principal are required when Policy is absent")
	case !ebStatementIDPattern.MatchString(body.StatementID):
		return nil, ebValidationError(fmt.Sprintf("StatementId %q must be 1 to 64 characters from [a-zA-Z0-9-_]", body.StatementID))
	case !ebPermissionActionPattern.MatchString(body.Action):
		return nil, ebValidationError(fmt.Sprintf("Action %q must match events:[a-zA-Z]+", body.Action))
	case !ebPrincipalPattern.MatchString(body.Principal):
		return nil, ebValidationError(fmt.Sprintf("Principal %q must be a 12-digit account ID or *", body.Principal))
	case body.Condition != nil && (body.Condition.Key == "" || body.Condition.Type == "" || body.Condition.Value == ""):
		return nil, ebValidationError("Condition must contain Type, Key and Value")
	}

	goCtx := context.Background()
	name, busErr := ebRequestBus(ctx, body.EventBusName)
	if busErr != nil {
		return nil, busErr
	}
	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	bus, found, err := p.loadBus(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ebBusNotFound(name)
	}

	if body.Policy != "" {
		bus.Policy = body.Policy
	} else {
		stmt := ebPolicyStatement{
			Sid: body.StatementID, Effect: "Allow", Action: body.Action, Resource: bus.ARN,
			Principal: map[string]string{"AWS": "arn:aws:iam::" + body.Principal + ":root"},
		}
		if body.Principal == "*" {
			stmt.Principal = "*"
		}
		if body.Condition != nil {
			stmt.Condition = map[string]map[string]string{body.Condition.Type: {body.Condition.Key: body.Condition.Value}}
		}
		policy, err := ebUpsertStatement(bus.Policy, stmt)
		if err != nil {
			return nil, err
		}
		if len(policy) > ebMaxPolicyLength {
			return nil, ebPolicyTooLong(len(policy))
		}
		bus.Policy = policy
	}
	if err := p.saveBus(goCtx, ctx.AccountID, ctx.Region, bus); err != nil {
		return nil, err
	}
	return ebJSONResponse(http.StatusOK, struct{}{})
}

// ebUpsertStatement returns policy with stmt added, replacing the statement with the same Sid.
func ebUpsertStatement(policy string, stmt ebPolicyStatement) (string, error) {
	doc, err := ebParsePolicy(policy)
	if err != nil {
		return "", fmt.Errorf("eventbridge putPermission: %w", err)
	}
	raw, err := json.Marshal(stmt)
	if err != nil {
		return "", fmt.Errorf("eventbridge putPermission encode statement: %w", err)
	}
	replaced := false
	for i, s := range doc.Statement {
		if ebStatementSid(s) == stmt.Sid {
			doc.Statement[i], replaced = raw, true
		}
	}
	if !replaced {
		doc.Statement = append(doc.Statement, raw)
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("eventbridge putPermission encode policy: %w", err)
	}
	return string(out), nil
}

// removePermission handles RemovePermission: it removes the statement StatementId names, or with
// RemoveAllPermissions the whole policy. A bus left with no statements has no policy, so
// DescribeEventBus answers no Policy member for it.
func (p *EventBridgePlugin) removePermission(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		EventBusName         string `json:"EventBusName"`
		RemoveAllPermissions bool   `json:"RemoveAllPermissions"`
		StatementID          string `json:"StatementId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, ebInvalidBody()
	}
	if !body.RemoveAllPermissions && body.StatementID == "" {
		return nil, ebValidationError("StatementId is required unless RemoveAllPermissions is true")
	}

	goCtx := context.Background()
	name, busErr := ebRequestBus(ctx, body.EventBusName)
	if busErr != nil {
		return nil, busErr
	}
	p.ruleMu.Lock()
	defer p.ruleMu.Unlock()
	bus, found, err := p.loadBus(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ebBusNotFound(name)
	}

	if body.RemoveAllPermissions {
		bus.Policy = ""
	} else {
		doc, err := ebParsePolicy(bus.Policy)
		if err != nil {
			return nil, fmt.Errorf("eventbridge removePermission: %w", err)
		}
		kept := make([]json.RawMessage, 0, len(doc.Statement))
		for _, s := range doc.Statement {
			if ebStatementSid(s) != body.StatementID {
				kept = append(kept, s)
			}
		}
		if len(kept) == len(doc.Statement) {
			return nil, &AWSError{
				Code:       "ResourceNotFoundException",
				Message:    fmt.Sprintf("the policy of event bus %s has no statement %s", name, body.StatementID),
				HTTPStatus: http.StatusBadRequest,
			}
		}
		bus.Policy = ""
		if len(kept) > 0 {
			doc.Statement = kept
			out, err := json.Marshal(doc)
			if err != nil {
				return nil, fmt.Errorf("eventbridge removePermission encode policy: %w", err)
			}
			bus.Policy = string(out)
		}
	}
	if err := p.saveBus(goCtx, ctx.AccountID, ctx.Region, bus); err != nil {
		return nil, err
	}
	return ebJSONResponse(http.StatusOK, struct{}{})
}
