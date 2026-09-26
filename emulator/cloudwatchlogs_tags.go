package emulator

// CloudWatch Logs tagging: TagResource, UntagResource, ListTagsForResource, and the one ARN rule
// that decides which ARN a caller may name.
//
// The issue this closes (#1273) reports a validation refusal on TagResource, but the operation was
// **absent**: the plugin routed eleven operations and none of the three tagging ones, so a request
// was refused as an unknown action rather than accepted with the wrong ARN. The trio and the ARN
// rule therefore land together — there is no half of this worth shipping alone, since a rule on an
// unroutable operation is untestable and an operation without the rule is the fake that caused the
// report.
//
// # The ARN a caller reuses is the wrong one
//
// A log group has two published ARN forms and they differ only in a suffix, which
// cloudwatchlogs_types.go already models: `logGroupArn` is bare and `arn` carries a trailing `:*`.
// The suffixed form is what an IAM policy wants, because it matches the group's streams, and it is
// what DescribeLogGroups reports under `arn` — so substrate itself hands a caller the value these
// three operations refuse. That is the whole trap: reusing the policy ARN for the API is a natural
// mistake and the emulator has to make it, or a consumer's tagging path stays green offline and
// fails on every group of a live deploy.
//
// # Provenance of the refusal
//
// The code and message are **observed**, not published. None of the three pages lists a
// ValidationException at all — their Errors are InvalidParameterException/400,
// ResourceNotFoundException/400, ServiceUnavailableException/500, plus TooManyTagsException/400 on
// TagResource — and the reporter saw `ValidationException: Invalid resourceArn` from us-west-2.
//
// What *is* published supports refusing: `resourceArn` carries Pattern `[\w+=/:,.@-]*`, a character
// class that contains no `*`, so the suffixed form violates the request model before any resource
// is resolved. So the refusal is derivable from the model and only its code and message text come
// from observation, which is the split docs/services.md labels and a release note reports under
// Provenance.
//
// # The taggable set, and the type substrate cannot hold
//
// All three pages name the same two types: `log-group:{name}` and `destination:{name}`. Substrate
// models no destinations — the plugin routes neither PutDestination nor DescribeDestinations — so a
// well-formed destination ARN names a type AWS publishes as taggable and a resource that cannot
// exist here. It answers ResourceNotFoundException, whose published gloss ("The specified resource
// does not exist.") is exactly true of it, rather than the ARN refusal: the ARN is not the thing
// that is wrong.
//
// # tags is a map, not an array of Tag
//
// Both the TagResource request and the ListTagsForResource response carry `tags` as a
// string-to-string map. That is not the shape most services use — Step Functions, ECS and ELB all
// take an array of {key, value} objects — and getting it wrong is the #528 failure mode for this
// service: a JSON-1.1 member that does not match the model parses to nothing rather than erroring,
// so an SDK would report a group with no tags and an HTTP 200.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// cwlMaxTagsPerResource is the number of tags a CloudWatch Logs resource may carry.
//
// All three pages state it in prose — "You can associate as many as 50 tags with a CloudWatch Logs
// resource" — and TagResource publishes TooManyTagsException for exceeding it. It is enforced at
// TagResource only; see [CloudWatchLogsPlugin.tagResource] for why CreateLogGroup does not.
const cwlMaxTagsPerResource = 50

// cwlLogGroupARNType is the resource-type segment of a taggable log-group ARN.
const cwlLogGroupARNType = "log-group:"

// cwlDestinationARNType is the resource-type segment of a taggable destination ARN. Substrate
// stores no destination, so an ARN of this type resolves to nothing; see this file's preamble.
const cwlDestinationARNType = "destination:"

// tagResource handles TagResource: it merges tags onto a log group.
//
// An existing key is replaced and a new one appended, which is the page's own description of the
// operation applied to a resource that already has tags.
//
// The 50-tag ceiling is checked against the *merged* set rather than the request, because a request
// of ten tags against a group already holding forty-five exceeds it while the request alone does
// not. CreateLogGroup performs no equivalent check: its page publishes no error code for exceeding
// the `tags` map maximum, and inventing one would assert a refusal AWS documents nowhere (#671), so
// the ceiling lives at the one operation whose page publishes the code for it. The asymmetry is
// deliberate and recorded rather than smoothed over.
func (p *CloudWatchLogsPlugin) tagResource(req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceARN string            `json:"resourceArn"`
		Tags        map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, cwlInvalidBody()
	}
	// A nil map is an absent member and an empty one is `"tags": {}`. Only the first is a
	// violation of Required: Yes; the page publishes no minimum entry count, so an empty map is
	// a request that asks for nothing and gets it.
	if body.Tags == nil {
		return nil, cwlMissingMember("tags")
	}

	lg, stateKey, err := p.resolveTaggedLogGroup(body.ResourceARN)
	if err != nil {
		return nil, err
	}

	merged := mergeStringMap(lg.Tags, body.Tags, nil)
	if len(merged) > cwlMaxTagsPerResource {
		return nil, &AWSError{
			Code: "TooManyTagsException",
			Message: fmt.Sprintf("Resource %s would have %d tags; a resource can have no more than %d.",
				lg.LogGroupName, len(merged), cwlMaxTagsPerResource),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	lg.Tags = merged

	if putErr := p.storeLogGroup(context.Background(), stateKey, lg, "tagResource"); putErr != nil {
		return nil, putErr
	}
	return cwLogsJSONResponse(http.StatusOK, struct{}{})
}

// untagResource handles UntagResource: it removes the named keys from a log group.
//
// A key the group does not carry is not an error, and neither is an empty `tagKeys` — the member's
// Array Members constraint is "Minimum number of 0 items", so a list of none is a valid request,
// and the page publishes no code for a key that is absent.
func (p *CloudWatchLogsPlugin) untagResource(req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceARN string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, cwlInvalidBody()
	}
	if body.TagKeys == nil {
		return nil, cwlMissingMember("tagKeys")
	}

	lg, stateKey, err := p.resolveTaggedLogGroup(body.ResourceARN)
	if err != nil {
		return nil, err
	}

	lg.Tags = mergeStringMap(lg.Tags, nil, body.TagKeys)
	if putErr := p.storeLogGroup(context.Background(), stateKey, lg, "untagResource"); putErr != nil {
		return nil, putErr
	}
	return cwLogsJSONResponse(http.StatusOK, struct{}{})
}

// listTagsForResource handles ListTagsForResource.
//
// A group with no tags answers `{"tags":{}}` rather than omitting the member: the response's only
// element is documented without a condition, and a caller converging tags reads the map and
// compares it, which an absent member turns into a nil-versus-empty distinction AWS never makes.
func (p *CloudWatchLogsPlugin) listTagsForResource(req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceARN string `json:"resourceArn"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, cwlInvalidBody()
	}

	lg, _, err := p.resolveTaggedLogGroup(body.ResourceARN)
	if err != nil {
		return nil, err
	}

	tags := lg.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	return cwLogsJSONResponse(http.StatusOK, struct {
		Tags map[string]string `json:"tags"`
	}{Tags: tags})
}

// resolveTaggedLogGroup parses a resourceArn and loads the log group it names, returning the group
// and the state key it is stored at.
//
// All three operations resolve through here, so none of them can accept an ARN another refuses —
// the property the issue asks for, since a convergence path reads before it writes and a read that
// accepted the suffixed form would report a group untagged rather than telling the caller its ARN
// was wrong.
//
// This is also why the three handlers are the only ones in the plugin that take no
// [RequestContext]: everything they need to key a record comes from the ARN, so there is no
// caller's account in scope for a later edit to reach for. Step Functions records the same
// arrangement for the same reason (stepfunctions_tags.go).
func (p *CloudWatchLogsPlugin) resolveTaggedLogGroup(resourceARN string) (CWLogGroup, string, error) {
	accountID, region, name, arnErr := cwlParseLogGroupTagARN(resourceARN)
	if arnErr != nil {
		return CWLogGroup{}, "", arnErr
	}
	stateKey := cwLogGroupKey(accountID, region, name)
	lg, err := p.loadLogGroup(context.Background(), stateKey, name, "resolveTaggedLogGroup")
	if err != nil {
		return CWLogGroup{}, "", err
	}
	return lg, stateKey, nil
}

// cwlParseLogGroupTagARN parses a CloudWatch Logs resourceArn naming a log group.
//
// The account and Region come from the ARN and never from the caller's request context. That is the
// rule #826 established for SQS and DynamoDB and #845 applied across the tagging resolver: an ARN
// naming another account's log group must resolve that group or none, because resolving it against
// the caller's own account tags — or, in UntagResource's case, strips a tag from — a same-named
// group the caller never named. Taking it from the ARN makes the refusal fall out of the load: the
// key names a record that account does not have.
//
// The refusals, in the order they are decided:
//
//   - An absent resourceArn is InvalidParameterException, the shape every other operation in this
//     plugin uses for a missing required member. Its Length Constraints put the minimum at 1, so
//     the empty string is not a candidate ARN to be judged as malformed.
//   - Anything that is not a logs ARN of a taggable type — wrong service, too few segments, a
//     resource type these operations do not accept, or a log-group ARN with anything after the
//     name — is ValidationException / Invalid resourceArn. The trailing `:*` of the IAM-policy form
//     and the `:log-stream:{name}` of a stream ARN both land here, which is the point of the rule.
//   - A well-formed destination ARN resolves to no namespace substrate holds and is left to the
//     caller as a not-found; see this file's preamble.
func cwlParseLogGroupTagARN(resourceARN string) (accountID, region, name string, err *AWSError) {
	if resourceARN == "" {
		return "", "", "", cwlMissingMember("resourceArn")
	}

	// arn:aws:logs:{region}:{account}:{type}:{name} — six segments, the last of which keeps its
	// own colons so a group name may contain them.
	parts := strings.SplitN(resourceARN, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "logs" {
		return "", "", "", cwlInvalidResourceARN()
	}
	region, accountID = parts[3], parts[4]
	resource := parts[5]

	if strings.HasPrefix(resource, cwlDestinationARNType) {
		// A destination is taggable at AWS and unrepresentable here: report the resource as
		// absent rather than the ARN as invalid.
		return "", "", "", cwLogsDestinationNotFound(strings.TrimPrefix(resource, cwlDestinationARNType))
	}

	groupName, ok := strings.CutPrefix(resource, cwlLogGroupARNType)
	if !ok || groupName == "" {
		return "", "", "", cwlInvalidResourceARN()
	}
	// A log group name cannot contain a colon — the published pattern is [\.\-_/#A-Za-z0-9]+ —
	// so a remaining colon means the ARN carries a suffix past the name: the policy form's `:*`,
	// or a `:log-stream:{name}` naming something these operations do not accept.
	if strings.Contains(groupName, ":") {
		return "", "", "", cwlInvalidResourceARN()
	}
	return accountID, region, groupName, nil
}

// cwlInvalidResourceARN refuses a resourceArn that is not a taggable CloudWatch Logs ARN.
//
// The code and the message are both observed rather than published; see this file's preamble for
// the provenance and for the published pattern that makes the refusal derivable.
func cwlInvalidResourceARN() *AWSError {
	return &AWSError{
		Code:       "ValidationException",
		Message:    "Invalid resourceArn",
		HTTPStatus: http.StatusBadRequest,
	}
}

// cwLogsDestinationNotFound refuses a request naming a destination, which substrate does not model.
//
// It names the destination for the reason [cwLogsGroupNotFound] names its group: the published
// gloss says nothing about which resource, and these operations accept two types.
func cwLogsDestinationNotFound(destinationName string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    "The specified destination does not exist: " + destinationName,
		HTTPStatus: http.StatusBadRequest,
	}
}

// cwlMissingMember refuses a request that omits a required member, in the wording the plugin's
// other ten operations use for the same condition.
func cwlMissingMember(member string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    member + " is required",
		HTTPStatus: http.StatusBadRequest,
	}
}
