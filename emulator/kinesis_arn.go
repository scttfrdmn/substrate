package emulator

// Kinesis stream addressing: the one place a StreamName and a StreamARN become an account, a Region
// and a stream name.
//
// Fifteen of the seventeen operations substrate models publish **both** members, each Required: No,
// under a Note that is byte-identical on every one of their pages:
//
//	When invoking this API, you must use either the StreamARN or the StreamName parameter, or both.
//	It is recommended that you use the StreamARN input parameter when you invoke this API.
//
// Substrate decoded only StreamName (#966). A caller sending only the ARN — which is what the AWS
// SDKs send when their client was handed one, and what a CloudFormation Ref, a Lambda event-source
// mapping and an IAM policy all carry — reached an empty-StreamName guard and was refused, with a
// code Kinesis does not publish. The recommended form was the one form that could not work.
//
// The two operations that publish no StreamARN are **CreateStream**, whose StreamName is the only
// operation-wide Required: Yes in the service and which is minting the ARN rather than resolving
// one, and **ListStreams**, which names no single stream at all. Both are left alone. **GetRecords**
// is the third irregular one: it publishes StreamARN but **no StreamName**, because its stream is
// implied by ShardIterator — and its page carries the boilerplate Note anyway, naming a parameter
// the same page does not document. That contradiction is AWS's, recorded here rather than resolved.
//
// The account and Region come from the ARN, by a parser that takes no *RequestContext at all, so an
// ARN naming another account or Region cannot address the caller's own same-named stream. That is
// what makes the rule structural rather than a thing each of fifteen call sites has to remember —
// the arrangement #826 established for SQS and DynamoDB, #845 for the tagging API's resolver, #918
// for CloudFront, #922 for KMS, #925 for SNS and #912 for Step Functions. A *RequestContext remains
// the right answer for the one path that legitimately has no account or Region of its own: a request
// naming the stream by name, which AWS resolves in the caller's own account and Region.
//
// Nothing here decodes **StreamId**, which all fifteen pages publish as "Not Implemented. Reserved
// for future use." A sweep that wired up every stream-shaped member would have taken it; it names
// nothing, and #966 asks for it to be left alone.

import (
	"fmt"
	"net/http"
	"strings"
)

// kinesisStreamARNResourcePrefix is the resource portion's prefix in a Kinesis stream ARN, from the
// published pattern arn:aws.*:kinesis:.*:\d{12}:stream/\S+.
const kinesisStreamARNResourcePrefix = "stream/"

// kinesisAccountIDLength is the number of digits the published ARN pattern's \d{12} requires of the
// account segment.
const kinesisAccountIDLength = 12

// kinesisStreamTarget is the stream a request names: the account and Region that own it, and its
// name.
//
// A target is what [kinesisStreamKey], [kinesisStreamNamesKey] and [kinesisRecordKey] are built
// from, so every key a request touches is derived from one resolution rather than from three
// independent readings of the request context.
type kinesisStreamTarget struct {
	// AccountID is the account that owns the stream: the ARN's account segment when the request
	// carried an ARN, and the caller's own only when it named the stream by name.
	AccountID string

	// Region is the Region the stream resides in, on the same terms as AccountID.
	Region string

	// Name is the stream's name.
	Name string
}

// kinesisStreamRef is the pair of members by which an operation names its stream.
//
// It is embedded in each handler's request struct rather than repeated, so a handler cannot decode
// one member and forget the other — which is the shape the defect took: fifteen structs each
// declaring StreamName alone.
type kinesisStreamRef struct {
	// StreamName is the stream's name, published Length 1–128, Pattern [a-zA-Z0-9_.-]+,
	// Required: No on every operation but CreateStream.
	StreamName string `json:"StreamName"`

	// StreamARN is the stream's ARN, published Length 1–2048,
	// Pattern arn:aws.*:kinesis:.*:\d{12}:stream/\S+, Required: No.
	StreamARN string `json:"StreamARN"`
}

// kinesisParseStreamARN parses a Kinesis stream ARN and returns the stream it names.
//
// The published pattern is arn:aws.*:kinesis:.*:\d{12}:stream/\S+, and what is enforced is that
// pattern rather than a stricter reading of it:
//
//   - The partition must begin with "aws" (the pattern's aws.*) and the service segment must be
//     exactly "kinesis".
//   - The account must be exactly twelve digits (\d{12}), so a shorter or non-numeric account is a
//     refusal rather than a lookup that cannot succeed.
//   - The **Region may be empty**, because the pattern's .* permits it. Such an ARN is not refused
//     for its shape — it resolves to a key nothing is written at and reports the stream absent,
//     which is the decision #912 recorded for an express execution ARN: refusing a shape the
//     published pattern admits would claim AWS rejects an ARN it does not.
//   - The resource portion must be "stream/" followed by at least one non-whitespace character
//     (\S+) that is not a "/". The slash rule is the in-tree one: a **consumer** ARN nests under the
//     same prefix as stream/{name}/consumer/{name}:{timestamp}, so a remaining "/" means the ARN
//     names a consumer rather than the stream, which substrate does not model. The tagging API's
//     kinesis arm has refused it on exactly that test since it was written, and now refuses it
//     through this function so the two cannot drift.
//
// What is deliberately **not** enforced is StreamName's own [a-zA-Z0-9_.-]+ character class. Nothing
// enforces it on CreateStream either, so applying it only here would make a stream substrate itself
// let a caller create unaddressable by its own ARN — a worse failure than accepting a name AWS
// would have refused at creation. Whether CreateStream should enforce the class is its own question.
//
// InvalidArgumentException at HTTP 400 is published on every operation that takes a StreamARN, and
// its description — "A specified parameter exceeds its restrictions, is not supported, or can't be
// used" — is this case exactly.
func kinesisParseStreamARN(arn string) (kinesisStreamTarget, *AWSError) {
	// arn:{partition}:kinesis:{region}:{account}:stream/{name}
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || !strings.HasPrefix(parts[1], "aws") || parts[2] != "kinesis" {
		return kinesisStreamTarget{}, kinesisInvalidStreamARN(arn)
	}
	region, account, resource := parts[3], parts[4], parts[5]

	if len(account) != kinesisAccountIDLength || strings.TrimLeft(account, "0123456789") != "" {
		return kinesisStreamTarget{}, kinesisInvalidStreamARN(arn)
	}

	name, ok := strings.CutPrefix(resource, kinesisStreamARNResourcePrefix)
	if !ok || name == "" || strings.Contains(name, "/") || strings.ContainsAny(name, " \t\r\n") {
		return kinesisStreamTarget{}, kinesisInvalidStreamARN(arn)
	}

	return kinesisStreamTarget{AccountID: account, Region: region, Name: name}, nil
}

// kinesisResolveStream resolves the StreamName/StreamARN pair an operation was given.
//
// The three cases AWS's Note names, and the fourth it does not:
//
//   - **ARN only** — the recommended form, and the one substrate refused outright. The account and
//     Region are the ARN's.
//   - **Name only** — resolved in the caller's own account and Region, which is the only reading
//     available: a name carries neither.
//   - **Both** — accepted, per "or both", when they name the same stream.
//   - **Neither** — refused. "You must use either the StreamARN or the StreamName parameter, or
//     both" makes a request carrying neither invalid, and this is the guard that replaces the
//     "StreamName is required" one. The code moves with it: InvalidArgumentException is published on
//     all fifteen operations, where the code the old guard answered is published by Kinesis nowhere
//     at all. #950 moved the rest of the plugin onto it too, so nothing is left answering the old
//     one; see [kinesisInvalidBody].
//
// **What happens when the two disagree is substrate's reading**, because no Kinesis page states it:
// the request is refused rather than one member silently winning. A caller that names two different
// streams in one request has a bug, and serving it either of them is how it stays hidden — the same
// judgement that made a well-formed ARN of the wrong resource type a refusal in #910 and #912. Note
// that only the **name** segments are compared: an ARN whose account or Region differ from the
// caller's is not a disagreement but the whole point of the change, and it wins.
func kinesisResolveStream(ctx *RequestContext, ref kinesisStreamRef) (kinesisStreamTarget, *AWSError) {
	if ref.StreamARN == "" {
		if ref.StreamName == "" {
			return kinesisStreamTarget{}, kinesisMissingStreamRef()
		}
		return kinesisStreamTarget{AccountID: ctx.AccountID, Region: ctx.Region, Name: ref.StreamName}, nil
	}

	target, err := kinesisParseStreamARN(ref.StreamARN)
	if err != nil {
		return kinesisStreamTarget{}, err
	}
	if ref.StreamName != "" && ref.StreamName != target.Name {
		return kinesisStreamTarget{}, &AWSError{
			Code: "InvalidArgumentException",
			Message: fmt.Sprintf("StreamName %q and StreamARN %q name different streams",
				ref.StreamName, ref.StreamARN),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return target, nil
}

// kinesisInvalidStreamARN reports that a StreamARN does not match the published pattern.
func kinesisInvalidStreamARN(arn string) *AWSError {
	return &AWSError{
		Code:       "InvalidArgumentException",
		Message:    "StreamARN is not a Kinesis stream ARN: " + arn,
		HTTPStatus: http.StatusBadRequest,
	}
}

// kinesisMissingStreamRef reports that a request named its stream neither way. See
// [kinesisResolveStream] for why the code is InvalidArgumentException.
func kinesisMissingStreamRef() *AWSError {
	return &AWSError{
		Code:       "InvalidArgumentException",
		Message:    "either StreamName or StreamARN is required",
		HTTPStatus: http.StatusBadRequest,
	}
}

// kinesisStreamNotFound reports that a resolved target names no stream.
//
// The code is ResourceNotFoundException — "The requested resource could not be found. The stream
// might not be specified correctly." — and the status is **400**, which is the part substrate had
// wrong: every error on every one of the seventeen Kinesis reference pages read for #966 is published
// at 400, including this one and ResourceInUseException, and substrate answered 404 and 409. The only
// 500 in the service is InternalFailureException, which substrate does not raise. #910 and #912
// established the same correction for Step Functions.
//
// The message names the ARN form of the target rather than the bare name it may have been given by,
// so a caller that sent a cross-account ARN can see which account was looked in.
func kinesisStreamNotFound(target kinesisStreamTarget) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    "Stream not found: " + kinesisStreamARN(target),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kinesisStreamARN renders a target as the ARN that names it.
func kinesisStreamARN(target kinesisStreamTarget) string {
	return "arn:aws:kinesis:" + target.Region + ":" + target.AccountID + ":" +
		kinesisStreamARNResourcePrefix + target.Name
}
