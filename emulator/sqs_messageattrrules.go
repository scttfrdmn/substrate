package emulator

import (
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"strings"
)

// The documented limits on a message's user-defined attributes, from the SQS
// developer guide's "Message metadata" page.
//
// The two length bounds are **exclusive**, which is not what the guide's prose says.
// The guide states a name and a type "can be up to 256 characters long", but the error
// AWS returns reads "must be shorter than 256 Bytes" and LocalStack — which snapshot-
// tests its SQS provider against the live service — rejects at exactly 256. The
// message is the more specific evidence of where the boundary sits, so a 256-byte name
// is refused here and the conflict is recorded rather than quietly resolved.
//
// "Bytes" is also the message's own word where the guide says "characters", so the
// length is measured in UTF-8 bytes — Go's len on a string — not in runes.
const (
	sqsMaxMessageAttributes     = 10
	sqsMaxAttributeNameLength   = 256
	sqsMaxAttributeTypeLength   = 256
	sqsAttributeNumberPrecision = 512
)

// sqsNumberAttributeMin and sqsNumberAttributeMax bound a Number attribute's value.
//
// The guide states a Number "can have up to 38 digits of precision, and it can be
// between 10^-128 and 10^+126". That phrasing is ambiguous between magnitude bounds
// and signed bounds; the signed reading is taken here because it is the one an
// independent implementation arrived at (elasticmq's numberAttributeValueLimit is
// -10^128..10^126) and because the same sentence describes the type as holding
// "positive or negative numerical values".
//
// They are [big.Float] rather than float64 because 10^126 is not exactly representable
// in float64, so a float64 comparison would land on the wrong side of the boundary for
// a value written out in full. The precision is set well above the ~419 bits 10^126
// needs so both bounds are exact.
//
// Computed once at package scope rather than per call, following
// [sqsMaxBatchPayloadSize]: the values are immutable, and rebuilding a 128-digit
// integer on every attribute check would be waste in the send path.
var (
	sqsNumberAttributeMin = new(big.Float).SetPrec(sqsAttributeNumberPrecision).
				Neg(sqsPowerOfTen(128))
	sqsNumberAttributeMax = sqsPowerOfTen(126)
)

// sqsPowerOfTen returns 10^exp as an exact [big.Float].
func sqsPowerOfTen(exp int64) *big.Float {
	i := new(big.Int).Exp(big.NewInt(10), big.NewInt(exp), nil)
	return new(big.Float).SetPrec(sqsAttributeNumberPrecision).SetInt(i)
}

// sqsCheckMessageAttributes validates a message's user-defined attributes, returning
// the error AWS raises for the first violation or nil.
//
// Every rule here is a documented constraint substrate previously ignored, which is the
// silent-success shape this tracker exists to close: a caller whose eleventh attribute
// is dropped by real SQS, or whose Number attribute holds "abc", saw substrate accept
// the send and had no reachable error branch to test (#472).
//
// Attributes are visited **in sorted name order** so a message violating two rules
// always reports the same one. Go map iteration is randomized, so visiting in map order
// would make the reported error differ between two identical requests — the precise
// nondeterminism this emulator exists to remove.
//
// The count is checked before any individual attribute, so an eleven-attribute message
// that also carries an illegal name reports the count. No capture settles that
// ordering; it is chosen because the count is a property of the set rather than of any
// one attribute, and because it is the cheap check.
//
// Provenance, by rule, strongest first. Substrate matches AWS on codes; message text is
// only as good as its source, and the tiers differ here:
//
//   - The count and the Number cast are **real-AWS captures**, quoted below.
//   - The empty-String-value message is a real-AWS capture, twice over
//     (open-telemetry/opentelemetry-js-contrib#1528, and a direct `aws sqs send-message`
//     against sqs.ap-northeast-1.amazonaws.com in softwaremill/elasticmq#373).
//   - The name and type messages come from LocalStack's check_attributes, which
//     snapshot-tests against the live service. No independent capture was found for any
//     of them; moto implements none of these rules and uses different wording for the
//     one name check it has, so LocalStack's strings stand alone.
//   - The Number **range** message is one reimplementation's own wording with no
//     capture and no snapshot testing behind it. It is the weakest claim in this file
//     and is labeled as such where it is raised.
//
// Two documented rules are deliberately not coded here. **Uniqueness within a message**
// is structural: attributes arrive as a Go map keyed by name, so a duplicate name
// cannot survive parsing to be rejected. And the **empty-value rule is applied to
// String only**, following the one source that specifies a message for it; a Binary
// attribute with an empty value is still accepted, which is what keeps the
// transport-byte case in TestSQS_MessageAttributes_BinaryIsIdentifiedByDataType
// reachable through the public API.
func sqsCheckMessageAttributes(attrs map[string]SQSMessageAttribute) *AWSError {
	if len(attrs) > sqsMaxMessageAttributes {
		return sqsTooManyMessageAttributes(len(attrs))
	}

	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if awsErr := sqsCheckAttributeName(name); awsErr != nil {
			return awsErr
		}
		if awsErr := sqsCheckAttributeValue(name, attrs[name]); awsErr != nil {
			return awsErr
		}
	}
	return nil
}

// sqsWarnStoredAttributes logs at WARN when a stored message's attributes violate a
// rule that [sqsCheckMessageAttributes] enforces on send, and does nothing else. The
// message is still returned, in full, exactly as stored (#491).
//
// Returning it is the decision, not an omission. Substrate's core property is that
// replaying an event log reproduces the same observations, and a message written by an
// older substrate was accepted by the substrate that recorded it — so withholding or
// dropping it now would make a recorded run unreplayable, which is the one outcome #491
// rules out. The warning gives an operator the signal that their fixture predates the
// rules without changing what any caller observes.
//
// The whole stored attribute set is checked rather than the subset the request asked
// for: the violation is a property of what is in state, and #461's selection means a
// request naming no attribute names would otherwise hide it entirely.
//
// It fires on every receive of the same message rather than once. A message redelivered
// after its visibility timeout is a fixture being exercised again, and remembering which
// messages have already warned would be per-process state that a replay could not
// reproduce.
//
// The attribute-free fast path is the common case — most messages carry no attributes at
// all — and skips a sort and an allocation per message in the receive loop.
func sqsWarnStoredAttributes(logger Logger, queueURL string, msg *SQSMessage) {
	if len(msg.MessageAttributes) == 0 {
		return
	}
	awsErr := sqsCheckMessageAttributes(msg.MessageAttributes)
	if awsErr == nil {
		return
	}
	logger.Warn("sqs receiveMessage: stored message attributes violate a current rule; "+
		"returning the message as stored so the run stays replayable",
		"queue", queueURL, "messageId", msg.MessageID,
		"code", awsErr.Code, "violation", awsErr.Message)
}

// sqsCheckAttributeName validates one attribute name against the guide's rules.
//
// The prefix rule is **case-INsensitive**, and that is the detail a reader arriving
// from EC2's reserved tag keys will get wrong. The guide says a name "can't start with
// AWS. or Amazon. (or any casing variations)", and LocalStack matches its
// prefix regex against name.lower(). EC2's aws: tag prefix is the opposite —
// case-*sensitive*, so AWS:foo is a legal tag key (see ec2CheckReservedTagKeys and
// #452). Both findings are correct; the two services document different rules, and
// unifying them would break one of the two.
//
// The character class follows the **developer guide** — alphanumerics, underscore,
// hyphen and period — rather than LocalStack's regex, which additionally admits the
// Latin-1 supplement range U+00C0..U+017F. The guide is the authoritative source and
// the wider class is unattributed, so the narrower rule is enforced; a name outside the
// guide's class is rejected here even though LocalStack would accept it.
//
// Sequential periods need their own check: ".." satisfies the character class, so the
// guide's "must not have periods in a sequence" is invisible to a class test.
func sqsCheckAttributeName(name string) *AWSError {
	if len(name) >= sqsMaxAttributeNameLength {
		return sqsInvalidAttributeParameter(
			"Message (user) attribute names must be shorter than 256 Bytes")
	}
	if !sqsAttributeNameCharsLegal(name) {
		// Reproduced verbatim from the observed text, trailing space and all: "upper
		// and lower score characters" is AWS's own odd phrasing, and the string ends
		// with a space. A tidied version is no longer the string a consumer asserting
		// on real SQS would see, so neither is corrected here.
		return sqsInvalidAttributeParameter("Message (user) attributes name can only " +
			"contain upper and lower score characters, digits, periods, hyphens and underscores. ")
	}
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon.") ||
		strings.HasPrefix(lower, ".") || strings.HasSuffix(lower, ".") {
		return sqsInvalidAttributeParameter("You can't use message attribute names " +
			"beginning with 'AWS.' or 'Amazon.'. These strings are reserved for internal use. " +
			"Additionally, they cannot start or end with '.'.")
	}
	if strings.Contains(name, "..") {
		// A separate message, from a third reimplementation (Inqnuam/local-aws-sqs),
		// because LocalStack has no sequential-period check at all and so supplies no
		// wording for it. Whether real AWS reports this under the message above or a
		// distinct one is unsettled — no capture of either was found.
		return sqsInvalidAttributeParameter(
			"Message (user) attribute name must not have successive '.' characters.")
	}
	return nil
}

// sqsAttributeNameCharsLegal reports whether every byte of a name is in the character
// class the developer guide allows: A-Z, a-z, 0-9, underscore, hyphen and period.
//
// Written as a loop rather than a regexp so the class is readable beside the guide's
// sentence and the package keeps no compiled-pattern globals. Byte-wise is sufficient
// because every legal character is ASCII, so any multi-byte rune fails on its first
// byte.
func sqsAttributeNameCharsLegal(name string) bool {
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '_', c == '-', c == '.':
		default:
			return false
		}
	}
	return true
}

// sqsCheckAttributeValue validates one attribute's DataType and value.
//
// The type must be prefixed with String, Binary or Number, with any trailing text
// forming a custom type — "Number.java.lang.Long" and "Binary.gif" are both documented
// and both legal. The prefix is matched case-sensitively: the guide states the type "is
// case-sensitive", so "string" is not a String.
func sqsCheckAttributeValue(name string, attr SQSMessageAttribute) *AWSError {
	if attr.DataType == "" {
		return sqsInvalidAttributeParameter("Missing required parameter DataType")
	}
	base, _, _ := strings.Cut(attr.DataType, ".")
	if base != "String" && base != "Number" && base != "Binary" {
		// LocalStack's own f-string joins "must be prefixed" to "with" without a
		// space, which is visibly a Python string-concatenation slip rather than
		// observed text; the grammatical form is emitted here. The literal
		// "MessageAttributes.Attribute_name.DataType" is left uninterpolated because
		// that is how the source has it and the "but was" clause already tells a
		// caller which value was refused.
		return sqsInvalidAttributeParameter(fmt.Sprintf("Type for parameter "+
			"MessageAttributes.Attribute_name.DataType must be prefixed with \"String\", "+
			"\"Binary\", or \"Number\", but was: %s", attr.DataType))
	}
	if len(attr.DataType) >= sqsMaxAttributeTypeLength {
		return sqsInvalidAttributeParameter(
			"Message (user) attribute types must be shorter than 256 Bytes")
	}

	// The exact type, not the base: a custom "String.json" carries no source for this
	// rejection, so only the plain String type is held to it.
	if attr.DataType == "String" && attr.StringValue == "" {
		return sqsInvalidAttributeParameter(fmt.Sprintf(
			"Message (user) attribute '%s' must contain a non-empty value of type 'String'.", name))
	}
	if base == "Number" {
		return sqsCheckNumberAttribute(name, attr.StringValue)
	}
	return nil
}

// sqsCheckNumberAttribute validates a Number attribute's value.
//
// A non-numeric value is the case #472 names, and it is the silent-success shape this
// tracker exists to close: a caller storing a counter or a timestamp in a Number
// attribute has substrate accept "abc" and their consumer's parse-failure branch
// unreachable.
//
// The rejection is a **real-AWS capture**, code and message together: boto3 calling
// SendMessage against live SQS with {'StringValue': 'Hello', 'DataType': 'Number'}
// reports `An error occurred (InvalidParameterValue) when calling the SendMessage
// operation: Can't cast the value of message (user) attribute 'Age' to a number.` — the
// offending attribute's name interpolated, as reproduced here. An independent
// reimplementation (Inqnuam/local-aws-sqs) emits the same sentence.
//
// The syntax is validated before the value is parsed, and that ordering is load-bearing
// rather than stylistic: [big.Float]'s SetString parses in base 0, which would also
// accept hexadecimal and underscore-separated forms real SQS has no reason to allow, so
// a decimal-only scan runs first and SetString only ever sees input it has already
// approved. It also bounds the work — a bare exponent like "1e999999999" is syntax that
// scans in constant time, and big.Float overflows it to +Inf, which the range check
// then refuses without materializing anything.
func sqsCheckNumberAttribute(name, value string) *AWSError {
	cantCast := sqsInvalidAttributeParameter(fmt.Sprintf(
		"Can't cast the value of message (user) attribute '%s' to a number.", name))
	if !sqsIsDecimalNumber(value) {
		return cantCast
	}
	f, _, err := big.ParseFloat(value, 10, sqsAttributeNumberPrecision, big.ToNearestEven)
	if err != nil {
		// Unreachable for input sqsIsDecimalNumber approved. Reported as the cast
		// failure rather than discarded so that a divergence between the scanner and
		// the parser surfaces as a rejection a test can see, not as an accepted value.
		return cantCast
	}
	if f.Cmp(sqsNumberAttributeMin) < 0 || f.Cmp(sqsNumberAttributeMax) > 0 {
		return sqsNumberAttributeOutOfRange(value)
	}
	return nil
}

// sqsIsDecimalNumber reports whether a string is a plain decimal numeral: an optional
// sign, digits with at most one point, and an optional decimal exponent.
//
// Scientific notation is accepted because the one implementation that bounds the range
// parses it (elasticmq, via BigDecimal) and because the guide describes the type by its
// value range rather than by a literal syntax. No capture settles whether real SQS
// accepts "1e5" as a Number, so this is the permissive reading of an unsettled detail —
// stated here rather than implied.
func sqsIsDecimalNumber(s string) bool {
	s = strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+")
	mantissa, exponent, hasExponent := s, "", false
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		mantissa, exponent, hasExponent = s[:i], s[i+1:], true
	}
	whole, frac, hasPoint := strings.Cut(mantissa, ".")
	if !isDigitsOnly(whole) || (hasPoint && !isDigitsOnly(frac)) {
		return false
	}
	// At least one digit somewhere in the mantissa: "." and "-" are not numbers, and
	// neither is the empty string a caller left in a Number attribute.
	if whole == "" && frac == "" {
		return false
	}
	if hasExponent {
		exponent = strings.TrimPrefix(strings.TrimPrefix(exponent, "-"), "+")
		if exponent == "" || !isDigitsOnly(exponent) {
			return false
		}
	}
	return true
}

// isDigitsOnly reports whether every byte of s is an ASCII digit. An empty string
// reports true, which is what lets a caller distinguish "12." from "." by checking the
// two halves separately.
func isDigitsOnly(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// sqsTooManyMessageAttributes returns the error SendMessage raises for a message
// carrying more than the documented ten attributes.
//
// The message is a **real-AWS capture**. A stack trace in
// open-telemetry/opentelemetry-java-instrumentation#13799 records the AWS SDK for Java
// receiving `SqsException: Number of message attributes [11] exceeds the allowed maximum
// [10]. (Service: Sqs, Status Code: 400, Request ID: f5704601-82c5-5752-a930-266672732710)`
// from live SQS — an AWS request ID and HTTP 400, reproduced here with both numbers
// interpolated as the capture shows them.
//
// The **code** is not in that trace, and comes instead from convergence: five
// independent reimplementations (elasticmq, jennyEckstein/sqslite, Inqnuam/local-aws-sqs,
// tyrchen/rustack, jacoelho/cloudish) all raise this exact sentence as
// InvalidParameterValue, which is also the code its sibling attribute rules carry. One
// of them hardcodes the count as `[12]` while checking for more than ten, which is the
// signature of a string transcribed from a response rather than composed — further
// evidence the wording is AWS's own.
//
// Neither moto nor LocalStack enforces the count at all, which is why substrate
// accepting an eleventh attribute went unnoticed until #461 made attributes observable.
func sqsTooManyMessageAttributes(count int) *AWSError {
	return &AWSError{
		Code: "InvalidParameterValue",
		Message: fmt.Sprintf("Number of message attributes [%d] exceeds the allowed maximum [%d].",
			count, sqsMaxMessageAttributes),
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqsNumberAttributeOutOfRange returns the error raised for a Number attribute whose
// value is numeric but outside the documented range.
//
// This carries the **weakest provenance in this file** and the label belongs on it: the
// range itself is documented ("between 10^-128 and 10^+126"), but the wording is
// elasticmq's own — one reimplementation, with no snapshot testing against the live
// service behind it and no capture of this rejection found anywhere. It is used in
// preference to inventing a sentence because a transcription attempt by an independent
// project is at worst no further from AWS's text than substrate's would be.
func sqsNumberAttributeOutOfRange(value string) *AWSError {
	return &AWSError{
		Code: "InvalidParameterValue",
		Message: fmt.Sprintf("Number attribute value %s should be in range (-10**128..10**126)",
			value),
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqsInvalidAttributeParameter wraps an attribute-rule message in the code and status
// every one of them carries: InvalidParameterValue, HTTP 400.
//
// The code is shared deliberately. SendMessage's Errors section names neither these
// rules nor any code for them, so the evidence is LocalStack reporting all of them as
// InvalidParameterValue plus the two captures above showing real SQS doing the same for
// two members of the family. A single constructor keeps that one claim in one place
// instead of repeating it at each call site.
func sqsInvalidAttributeParameter(message string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    message,
		HTTPStatus: http.StatusBadRequest,
	}
}

// sqsBatchResultErrorEntry is one failed entry in a batch operation's response.
//
// SendMessageBatch reports per-entry failures rather than failing the request: "The
// result of sending each message is reported individually in the response. Because the
// batch request can result in a combination of successful and unsuccessful actions, you
// should check for batch errors even when the call returns an HTTP status code of 200."
// Substrate had no type for this — both batch paths emitted an empty Failed list — so a
// consumer's per-entry error handling had nothing to exercise.
//
// One struct carries both wire shapes. The element name is BatchResultErrorEntry rather
// than the member name Failed, because the query-protocol model declares
// BatchResultErrorEntryList flattened with locationName "BatchResultErrorEntry" under
// resultWrapper SendMessageBatchResult — so an SDK parses repeated
// BatchResultErrorEntry elements, not a Failed wrapper. Field order follows the model's
// member order. Message is the only optional member of the four.
type sqsBatchResultErrorEntry struct {
	ID          string `xml:"Id" json:"Id"`
	SenderFault bool   `xml:"SenderFault" json:"SenderFault"`
	Code        string `xml:"Code" json:"Code"`
	Message     string `xml:"Message,omitempty" json:"Message,omitempty"`
}

// sqsBatchFailure converts an attribute-rule rejection into a batch entry failure.
//
// SenderFault is true: the model documents it as "whether the error happened due to the
// caller of the batch API action", and a malformed attribute is the caller's. Getting it
// backwards would tell a consumer's retry logic that a permanent request defect was a
// service-side fault worth retrying.
//
// This is deliberately different from the batch **size** failures a few lines away in
// sendMessageBatch, which fail the whole request with a 400. BatchRequestTooLong is a
// documented request-level error about the aggregate payload, and an oversized single
// entry is observed to be reported the same way; an attribute violation is per-entry.
// Both are faithful, and the distinction matters now that the two sit together.
func sqsBatchFailure(entryID string, awsErr *AWSError) sqsBatchResultErrorEntry {
	return sqsBatchResultErrorEntry{
		ID:          entryID,
		SenderFault: true,
		Code:        awsErr.Code,
		Message:     awsErr.Message,
	}
}
