package emulator

import (
	"context"
	"fmt"
	"net/http"
)

// CloudWatch Logs resolves the log group its reads address, and every not-found refusal in the plugin
// answers the status its page publishes (#1224).
//
// Three of the four paginating reads — DescribeLogStreams, GetLogEvents and FilterLogEvents — never
// loaded the group or stream they addressed. A group that does not exist has no stream index and no
// events key, so `loadStringIndex` and `state.Get` answered nothing and the operation answered **HTTP
// 200 with an empty listing**. A caller could not tell "this group is empty" from "this group was
// never created", and a consumer whose error path exists for a typo'd or not-yet-created log group
// never reached it. `putLogEvents` did resolve both and had the refusal — at the wrong status — so the
// code existed in the plugin and was simply unreachable from the three read paths.
//
// # What the pages publish
//
// `API_DescribeLogStreams`, `API_GetLogEvents` and `API_FilterLogEvents` each publish, in their own
// Errors section:
//
//	ResourceNotFoundException
//	The specified resource does not exist.
//	HTTP Status Code: 400
//
// The status is **400, not 404**, and that is the page's own figure rather than a transcription slip:
// this is a JSON-1.1 service, so the code travels in the body's `__type` and the status line carries
// only the class of failure. `API_DescribeLogGroups` publishes no `ResourceNotFoundException` at all —
// its Errors are `InvalidParameterException`/400 and `ServiceUnavailableException`/500 — which is why
// this is a three-site change and not a four-site one: a `logGroupNamePrefix` matching nothing is a
// legitimately empty listing, and adding a refusal there would be inventing a code the page does not
// publish ([#671]).
//
// The four operations that already refused an absent resource did so at **404**: DeleteLogGroup,
// CreateLogStream, DeleteLogStream and PutLogEvents. Each of their pages publishes
// `ResourceNotFoundException` at 400 in the same words, so all four move here rather than being left
// as a split the plugin would then carry against itself — `PutRetentionPolicy` and
// `DeleteRetentionPolicy` already answered 400 through [CloudWatchLogsPlugin.loadLogGroup], and a
// consumer switching on the status would have found the same code at two different statuses depending
// on which operation it called.
//
// Not changed here, and recorded so the status sweep is not read as having covered it: `CreateLogGroup`
// and `CreateLogStream` answer `ResourceAlreadyExistsException` at **409**, where both pages publish
// 400. That is an already-exists code rather than a not-found one, it is a different refusal on a write
// path, and it needs the same survey across the plugin's other conflict answers that this change did
// for not-found. Filed as [#1251].
//
// # Precedence against the pagination-token refusal
//
// #1086 established that a token is decoded before any state is read, on #887's criterion: a refusal
// must not depend on how much state happens to exist. Adding a resource resolution puts a third
// refusal in the sequence — absent required member, unissuable token, absent resource — and AWS
// publishes nothing about which of the last two wins.
//
// The in-tree precedent is SNS `ListSubscriptionsByTopic` (#926, recorded in sns_pagination.go): the
// **resource the request addresses is resolved first**, so its not-found keeps precedence over a token
// refusal, because a token is a continuation of a listing *over that resource* and there is no listing
// to continue when the resource does not exist. S3's `ListObjectsV2` records the same reading for its
// bucket. The three reads follow it: the log group is resolved before the token is decoded, and
// GetLogEvents resolves its stream there too, since `logStreamName` is `Required: Yes` on that page and
// the stream is as much the addressed resource as the group is.
//
// #887's criterion survives the exception in the form that matters. The token is still decoded before
// the *listing* is read — the stream index at DescribeLogStreams and FilterLogEvents, the events key at
// GetLogEvents — so a store failure while reading a listing is still not reported for a request that
// was already refusable, and the resolution that now precedes the token is a single keyed `state.Get`
// that the refusal itself depends on.
//
// The absent-required-member refusal keeps its place at the front of all three. A request with no
// `logGroupName` names no resource to resolve, so there is nothing for the not-found to be about.
//
// [#671]: https://github.com/scttfrdmn/substrate/issues/671
// [#1251]: https://github.com/scttfrdmn/substrate/issues/1251

// cwLogsGroupNotFound refuses a request naming a log group that has no record.
//
// The message names the group, which is substrate's addition rather than AWS's wording: the published
// gloss is *"The specified resource does not exist."* and says nothing about which resource, and a
// caller that passed both a group and a stream cannot tell the two cases apart from the gloss alone.
// It is the shape [CloudWatchLogsPlugin.loadLogGroup] already used for the retention operations, so
// the plugin renders one message for one condition.
func cwLogsGroupNotFound(logGroupName string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    "The specified log group does not exist: " + logGroupName,
		HTTPStatus: http.StatusBadRequest,
	}
}

// cwLogsStreamNotFound refuses a request naming a log stream that has no record under its group.
//
// Distinct from [cwLogsGroupNotFound] for the reason that function's comment gives: GetLogEvents and
// PutLogEvents both name two resources, and one message for both conditions would leave a caller
// unable to tell which of the two it got wrong.
func cwLogsStreamNotFound(logStreamName string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    "The specified log stream does not exist: " + logStreamName,
		HTTPStatus: http.StatusBadRequest,
	}
}

// requireLogGroup answers [cwLogsGroupNotFound] unless the named log group has a record.
//
// op names the calling operation for the wrapped store error, matching
// [CloudWatchLogsPlugin.loadLogGroup]'s convention. It reads the record and discards it because the
// three read paths need only its existence; the retention operations, which need its contents, keep
// using loadLogGroup.
func (p *CloudWatchLogsPlugin) requireLogGroup(ctx context.Context, accountID, region, logGroupName, op string) error {
	data, err := p.state.Get(ctx, cloudwatchLogsNamespace, cwLogGroupKey(accountID, region, logGroupName))
	if err != nil {
		return fmt.Errorf("logs %s group.Get: %w", op, err)
	}
	if data == nil {
		return cwLogsGroupNotFound(logGroupName)
	}
	return nil
}

// requireLogStream answers [cwLogsStreamNotFound] unless the named log stream has a record under the
// named group.
//
// It does not resolve the group: every caller resolves that first, because a missing group has to be
// reported as a missing *group* rather than as a missing stream under it.
func (p *CloudWatchLogsPlugin) requireLogStream(ctx context.Context, accountID, region, logGroupName, logStreamName, op string) error {
	data, err := p.state.Get(ctx, cloudwatchLogsNamespace, cwLogStreamKey(accountID, region, logGroupName, logStreamName))
	if err != nil {
		return fmt.Errorf("logs %s stream.Get: %w", op, err)
	}
	if data == nil {
		return cwLogsStreamNotFound(logStreamName)
	}
	return nil
}
