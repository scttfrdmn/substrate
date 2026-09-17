package emulator

import "net/http"

// Kinesis's refusals for a request substrate could not read.
//
// #950 found nineteen sites in kinesis_plugin.go answering InvalidParameterException — sixteen
// body-decode guards and three missing-member complaints. That code is published by Kinesis
// **nowhere at all**: it appears in the Errors section of none of the sixteen guarded operations, and
// a grep of their reference pages finds it not even in prose. A caller matching on it matched
// something no SDK models.
//
// InvalidArgumentException is the answer, and it is stronger evidence than a common-errors fallback
// because it is published per-operation: it is in the Errors section of **all sixteen**, always at
// HTTP 400, glossed "A specified parameter exceeds its restrictions, is not supported, or can't be
// used. For more information, see the returned message." The status is unchanged — every one of the
// nineteen sites already answered 400 — so this is a code-string correction, which is why #923's
// status audit could not have found it.
//
// ValidationException is on four of the sixteen pages (CreateStream, UpdateShardCount, MergeShards,
// SplitShard) and must not be used: its gloss is capacity-mode-specific — "Specifies that you tried
// to invoke this API for a data stream with the on-demand capacity mode" — so it is not a generic
// validation code despite the name.
//
// [kinesisResolveStream] and [kinesisValidateTagMap] already answered InvalidArgumentException, and
// the comment at the ListTagsForStream limit check predicted this change by name. So after #950 every
// Kinesis refusal for an unusable request — decode, missing member, constraint violation and stream
// resolution alike — reports one code, which is the "one helper, one status" shape this issue asks
// for.

// kinesisInvalidBody reports that a request body would not decode.
//
// It takes no argument deliberately. Passing err.Error() through would tell a caller which Go struct
// field failed to unmarshal, by an endpoint whose whole purpose is to look like AWS — the second
// defect #950 found, at seventeen sites in six services.
func kinesisInvalidBody() *AWSError {
	return kinesisInvalidArgument("the request body is not valid JSON")
}

// kinesisInvalidArgument reports that a request parameter cannot be used as given.
func kinesisInvalidArgument(message string) *AWSError {
	return &AWSError{
		Code:       "InvalidArgumentException",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}
