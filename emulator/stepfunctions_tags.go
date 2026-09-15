package emulator

// Step Functions tagging: TagResource, UntagResource, ListTagsForResource, and the ARN resolver
// all three of them and the Resource Groups Tagging API key through.
//
// Split out of stepfunctions_plugin.go for the reason rds_tags.go was split out of
// rds_plugin.go: the resolver is the load-bearing part, and here there was no resolver at all.
// Each of the three operations took the resource *name* from the last colon-separated segment of
// the ARN and the account and Region from the caller's own request context, so an ARN naming
// another account's state machine reached the caller's same-named one — UntagResource being the
// damaging direction, since stripping a tag can turn an aws:ResourceTag Deny into an allow. That
// is the defect #826 closed for SQS and DynamoDB and #845 closed across the tagging resolver;
// these three operations were never audited against it (#910).
//
// The resource-type segments are AWS's. TagResource, UntagResource and ListTagsForResource each
// describe resourceArn as "the Amazon Resource Name (ARN) for the Step Functions state machine
// or activity", so those two are the whole taggable set:
//
//	State machine  arn:aws:states:{region}:{account}:stateMachine:{name}
//	Activity       arn:aws:states:{region}:{account}:activity:{name}
//
// An execution ARN — arn:aws:states:{region}:{account}:execution:{smName}:{execName} — is a
// well-formed states ARN naming a resource these operations do not accept, so it is refused with
// InvalidArn rather than ResourceNotFound: the resource may well exist, and it is the ARN that
// does not belong at this operation. It answered InvalidArn before this change too, but by
// falling off the end of a strings.Contains chain rather than by a decision.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// Step Functions state-key prefixes the tagging path resolves to. Each names one record shape in
// the states namespace; the namespace also holds an execution and three index keys, which no ARN
// addresses and which store no tags — hence [sfnKeyIsTaggable] in front of the merge.
const (
	sfnStateMachineKeyPrefix = "statemachine:"
	sfnActivityKeyPrefix     = "activity:"
)

// sfnTagsJSONMember is the JSON member both taggable Step Functions records store their tags in.
//
// StateMachineState and ActivityState both declare `json:"Tags,omitempty"`
// (stepfunctions_types.go), so one name serves both shapes. It is named rather than written
// inline because [mergeRecordStringMapTags] writes whichever member it is given, and a
// misspelling would add a second tags member while leaving the real one untouched — a tag call
// that answers 200 and stores nothing.
const sfnTagsJSONMember = "Tags"

// sfnStateMachineKey returns the state key a state machine is stored at.
//
// A package-level function rather than a method so [sfnResolveARN] can call it: there is one
// producer of a "statemachine:" key in the tree, and so nothing for the tagging path and the
// plugin's own operations to disagree about. That is the arrangement #826 established for ECS
// through ecsTagStateKey.
func sfnStateMachineKey(accountID, region, name string) string {
	return sfnStateMachineKeyPrefix + accountID + "/" + region + "/" + name
}

// sfnActivityKey returns the state key an activity is stored at. See [sfnStateMachineKey] for
// why it is a function rather than a method.
func sfnActivityKey(accountID, region, name string) string {
	return sfnActivityKeyPrefix + accountID + "/" + region + "/" + name
}

// sfnResolveARN parses a Step Functions ARN and returns the namespace and state key it addresses.
//
// The account and Region come from the ARN, never from the caller's request context — the rule
// #826 established — so an ARN naming another account's state machine cannot resolve the
// caller's own same-named one. The function takes no *RequestContext at all, which is what makes
// that structural rather than a thing each of the three call sites has to remember.
func sfnResolveARN(arn string) (ns, key string, err error) {
	// arn:aws:states:{region}:{acct}:{type}:{name}
	parts := strings.SplitN(arn, ":", 7)
	if len(parts) < 7 || parts[0] != "arn" || parts[2] != "states" {
		return "", "", fmt.Errorf("invalid Step Functions ARN: %q", arn)
	}
	region := parts[3]
	acct := parts[4]
	resType := parts[5]
	name := parts[6]
	if name == "" {
		return "", "", fmt.Errorf("states ARN names no resource: %q", arn)
	}

	// The type comparison is case-sensitive on purpose. AWS distinguishes the two taggable
	// resources by the literal segment alone — stateMachine with a capital M against activity —
	// so a case-folding match would let "statemachine:orders" address the same record as
	// "stateMachine:orders" while AWS refuses the first outright.
	var keyFn func(accountID, region, name string) string
	switch resType {
	case "stateMachine":
		keyFn = sfnStateMachineKey
	case "activity":
		keyFn = sfnActivityKey
	default:
		// "execution" lands here, deliberately: see this file's preamble.
		return "", "", fmt.Errorf("unsupported Step Functions ARN resource type: %q", resType)
	}

	// A state machine or activity name is letters, digits and a few punctuation characters —
	// AWS's Name pattern excludes ":" and "/". Without the check, an execution ARN's trailing
	// {smName}:{execName} pair or any other extra segment built a state key with a colon in the
	// name, which addresses nothing and so reported the resource absent rather than the ARN
	// malformed: the wrong error to hand a caller and the wrong one to see in a log. It is also
	// what made the previous strings.Contains(arn, ":stateMachine:") test unsound, since a name
	// carrying that substring satisfied it as readily as a type segment did.
	if strings.ContainsAny(name, "/:") {
		return "", "", fmt.Errorf("invalid Step Functions resource name: %q", name)
	}
	return statesNamespace, keyFn(acct, region, name), nil
}

// sfnKeyIsTaggable reports whether a states-namespace state key names a record substrate stores
// tags on — that is, one of the two shapes [sfnResolveARN] can produce.
//
// The namespace also holds executions and the plugin's own index keys
// (statemachine_names:, execution_ids:, activity_names:), none of which any ARN addresses.
// Naming the reachable set here keeps a caller that invented a key from writing a Tags member
// onto a record whose own service never reads one.
func sfnKeyIsTaggable(key string) bool {
	switch {
	case strings.HasPrefix(key, sfnStateMachineKeyPrefix):
		return true
	case strings.HasPrefix(key, sfnActivityKeyPrefix):
		return true
	default:
		return false
	}
}

// sfnInvalidArnError reports that an ARN is not one this operation accepts.
//
// TagResource, UntagResource and ListTagsForResource each publish InvalidArn — "The provided
// Amazon Resource Name (ARN) is not valid." — at HTTP 400.
func sfnInvalidArnError(arn string) *AWSError {
	return &AWSError{
		Code:       "InvalidArn",
		Message:    "The provided Amazon Resource Name (ARN) is not valid: " + arn,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnResourceNotFoundError reports that the resource a resolved ARN addresses does not exist.
//
// The status is 400, not 404. All three tagging operations publish ResourceNotFound — "Could not
// find the referenced resource." — with "HTTP Status Code: 400", which is unusual enough to be
// worth stating: this path answered 404 before, so a consumer branching on the status rather than
// the code saw something no AWS Step Functions endpoint returns.
func sfnResourceNotFoundError(arn string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFound",
		Message:    "Could not find the referenced resource: " + arn,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sfnTagList renders a tag map as AWS's array of Tag objects, ordered by key.
//
// AWS's Tag shape is {"key": …, "value": …} and both TagResource's request and
// ListTagsForResource's response carry an *array* of them, not an object. Substrate rendered an
// object here while CreateStateMachine and CreateActivity in the same plugin already decoded the
// array, so a tag set at create time could not be read back in a shape any SDK decodes — the
// plugin disagreed with itself about the wire shape of its own tags.
//
// The order is lexicographic by key because AWS documents none, and a member order that followed
// Go's map iteration would differ between two identical calls in one run and could not replay
// from the event log (#862).
func sfnTagList(tags map[string]string) []map[string]string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]map[string]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]string{"key": k, "value": tags[k]})
	}
	return out
}

// sfnTagsFromList collects AWS's array of Tag objects into a map.
//
// A tag whose "key" member is absent is skipped, matching what CreateStateMachine
// (stepfunctions_plugin.go) already does for the same shape.
func sfnTagsFromList(list []map[string]string) map[string]string {
	tags := make(map[string]string, len(list))
	for _, t := range list {
		if k, ok := t["key"]; ok {
			tags[k] = t["value"]
		}
	}
	return tags
}

// --- Tagging operations ---

// loadTaggedRecord returns the raw record a Step Functions ARN addresses, along with the key it
// resolved to.
//
// The three tagging operations share it so that a refusal and a not-found answer the same code at
// each of them: they resolved separately before, which is how the read side and the write side
// came to be able to disagree about which record an ARN named.
func (p *StepFunctionsPlugin) loadTaggedRecord(goCtx context.Context, arn string) (key string, raw []byte, err error) {
	ns, key, resolveErr := sfnResolveARN(arn)
	if resolveErr != nil {
		return "", nil, sfnInvalidArnError(arn)
	}
	raw, err = p.state.Get(goCtx, ns, key)
	if err != nil {
		return "", nil, fmt.Errorf("stepfunctions tagging state.Get: %w", err)
	}
	if raw == nil {
		return "", nil, sfnResourceNotFoundError(arn)
	}
	return key, raw, nil
}

// mergeTaggedRecord applies addTags and removeKeys to the record the ARN addresses.
//
// The merge goes through [mergeRecordStringMapTags] rather than a concrete type, for the reason
// the ECS and RDS arms of the tagging API already do: the states namespace holds two taggable
// shapes, and decoding one of them whatever the key named would round-trip an activity through a
// state machine's struct and drop every member the other does not carry (part of #835).
func (p *StepFunctionsPlugin) mergeTaggedRecord(goCtx context.Context, arn string, addTags map[string]string, removeKeys []string) error {
	key, raw, err := p.loadTaggedRecord(goCtx, arn)
	if err != nil {
		return err
	}
	updated, err := mergeRecordStringMapTags(raw, sfnTagsJSONMember, addTags, removeKeys)
	if err != nil {
		return fmt.Errorf("stepfunctions merge tags for %s: %w", arn, err)
	}
	if err := p.state.Put(goCtx, statesNamespace, key, updated); err != nil {
		return fmt.Errorf("stepfunctions tagging state.Put: %w", err)
	}
	return nil
}

func (p *StepFunctionsPlugin) tagResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string              `json:"resourceArn"`
		Tags        []map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	if err := p.mergeTaggedRecord(context.Background(), input.ResourceArn, sfnTagsFromList(input.Tags), nil); err != nil {
		return nil, err
	}
	return statesJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *StepFunctionsPlugin) untagResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	if err := p.mergeTaggedRecord(context.Background(), input.ResourceArn, nil, input.TagKeys); err != nil {
		return nil, err
	}
	return statesJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *StepFunctionsPlugin) listTagsForResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ResourceArn string `json:"resourceArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "InvalidRequest", Message: "invalid JSON body", HTTPStatus: http.StatusBadRequest}
	}
	_, raw, err := p.loadTaggedRecord(context.Background(), input.ResourceArn)
	if err != nil {
		return nil, err
	}
	// Only the tags member is read, so one decode serves both record shapes and neither can be
	// truncated by being read through the other's struct.
	var record struct {
		Tags map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, fmt.Errorf("stepfunctions listTagsForResource unmarshal record: %w", err)
	}
	return statesJSONResponse(http.StatusOK, map[string]interface{}{"tags": sfnTagList(record.Tags)})
}
