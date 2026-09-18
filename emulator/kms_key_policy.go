package emulator

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// KMS key policies: the document AWS attaches when a caller supplies none, and the refusals the two
// policy operations publish (#983).
//
// A key policy is the only place a KMS key's own permissions live — an IAM policy cannot grant access
// to a key whose key policy does not delegate to IAM — so the document substrate reports for a key
// that has never had PutKeyPolicy called on it is not a placeholder. Until #983 it was
// `{"Version":"2012-10-17","Statement":[]}`: a syntactically valid document that grants nothing,
// which is the *opposite* of what AWS attaches. And CreateKey's Policy member was decoded by nobody,
// so a caller that attached a policy at creation time read back a document it had never sent.
//
// **The default document comes from API_GetKeyPolicy's own Example Response**, not from the developer
// guide's *Default key policy* page that describes it in prose. The page shows the whole thing,
// including an `Id` member — `"key-default-1"` — that the prose does not mention, which is why the
// reference page is the source here even though both agree about the statement.
//
// Three codes, each published on the page that answers it. A `Policy` outside its length range is
// LimitExceededException, on the strength of the member's own note (*"if the key policy exceeds the
// length constraint, AWS KMS returns a LimitExceededException"*) rather than an inference from the
// error's gloss. A document that is not a JSON object is MalformedPolicyDocumentException. A
// PolicyName other than `default` is NotFoundException, which is a one-step reading recorded at
// [kmsUnknownPolicyName]. All three are 400 on both API_CreateKey and API_PutKeyPolicy.
//
// **Four things AWS publishes here that substrate deliberately does not implement**, each recorded
// rather than half-built:
//
//   - *The key policy lockout safety check*, and therefore BypassPolicyLockoutSafetyCheck. AWS
//     requires that a supplied policy "allow the calling principal to make a subsequent PutKeyPolicy
//     request", which is a policy evaluation against the caller's principal — #671's machinery, not
//     this operation's. With no lockout check, bypassing it is unobservable, so the member stays
//     undecoded on both operations rather than becoming a value read by nobody: the defect class #984
//     closed for Origin.
//   - *Statement-level validation.* AWS's own Policy description says a statement missing `Action` or
//     `Resource` "has no effect" while "the CreateKey and PutKeyPolicy API requests succeed", so
//     "semantically correct" in MalformedPolicyDocumentException's gloss is narrower than it reads and
//     there is no published statement-level refusal to implement.
//   - *The Policy pattern*, which admits tab, line feed, carriage return and the printable range up
//     through U+00FF. Neither page publishes a code for a pattern violation, and inventing one would
//     refuse documents AWS accepts.
//   - *InvalidArnException*, glossed "a specified ARN, or an ARN in a key policy, is not valid".
//     Nothing walks the principals, so it stays unreachable.
//
// A fifth is unreachable for a stronger reason: both operations publish KMSInvalidStateException, and
// the developer guide's key-state table gives GetKeyPolicy and PutKeyPolicy a green checkmark in
// **all seven** state columns — Enabled, Disabled, Pending deletion, Pending import, Unavailable,
// Creating, Updating — with no footnote on any of them. So there is no key state that refuses either
// operation, and adding one would be a divergence rather than fidelity. Contrast TagResource, two
// rows below, which is refused at Pending deletion under footnote [3].

// kmsPolicyMinBytes and kmsPolicyMaxBytes are the published length range of a key policy document.
//
// API_CreateKey and API_PutKeyPolicy both state them on their Policy member as "Minimum length of 1.
// Maximum length of 32768", and CreateKey's description names the code for a violation directly,
// which is what makes [kmsPolicyTooLong] a transcription rather than a reading. PutKeyPolicy's own
// Policy note repeats it verbatim.
//
// The minimum is separately load-bearing on PutKeyPolicy, where Policy is Required: Yes: a member
// present and empty satisfies presence, so nothing but the range refuses `{"Policy": ""}`.
const (
	kmsPolicyMinBytes = 1
	kmsPolicyMaxBytes = 32768
)

// kmsDefaultPolicyName is the only key policy name AWS accepts, and the value it defaults to.
//
// Both API_GetKeyPolicy and API_PutKeyPolicy state it the same way on PolicyName — "If no policy name
// is specified, the default value is `default`. The only valid value is `default`" — and
// API_GetKeyPolicy's Example Response reports it in the PolicyName response member. Substrate answered
// that string unconditionally before #983; it now answers it because that is the policy it read.
const kmsDefaultPolicyName = "default"

// kmsDefaultKeyPolicy renders the key policy AWS attaches to a key created without one, for a key
// owned by the given account.
//
// Transcribed from API_GetKeyPolicy's Example Response, which carries the document in full:
// Version 2012-10-17, Id "key-default-1", and one statement — Sid "Enable IAM User Permissions",
// Effect Allow, Principal the account root, Action "kms:*", Resource "*". The example's account is
// AWS's documentation placeholder 111122223333; the account substituted here is the key's own, since
// the whole content of the statement is that this key's account has full control of it, and a
// document naming a foreign root would grant nothing to anyone who can reach the key.
//
// The ARN is composed with the `aws` partition, matching every other IAM ARN substrate renders
// ([iamAccountResourceARN], [buildCallerARN]). Substrate models no other partition, and the SM2 key
// spec — the one KMS feature that is China-Regions-only — is admitted everywhere for the same reason.
//
// Whitespace is not preserved from the example. AWS's response formats the document with spaces
// around the colons; a consumer parses the string as JSON, so the formatting is transport and the
// members are the observation. This is the same reading [kmsJSONResponse]'s callers make everywhere
// else in the plugin.
//
// The document is built and marshaled rather than fmt-composed so that the account cannot escape the
// string it lands in: an account ID is not caller-controlled today, but a document assembled by
// concatenation is one refactor away from being injectable, and the marshaled form is also what
// guarantees the member set is exactly the one named above.
func kmsDefaultKeyPolicy(accountID string) string {
	document := map[string]any{
		"Version": "2012-10-17",
		"Id":      "key-default-1",
		"Statement": []any{
			map[string]any{
				"Sid":       "Enable IAM User Permissions",
				"Effect":    "Allow",
				"Principal": map[string]any{"AWS": "arn:aws:iam::" + accountID + ":root"},
				"Action":    "kms:*",
				"Resource":  "*",
			},
		},
	}
	// The map's members are all string-keyed and JSON-representable, so this cannot fail; the error is
	// discarded rather than wrapped because there is no request-scoped failure to report and returning
	// one would give every caller a branch that cannot be taken. A malformed default would surface
	// immediately in the test that decodes this document member by member.
	encoded, err := json.Marshal(document)
	if err != nil {
		return ""
	}
	return string(encoded)
}

// kmsPolicyTooLong reports that a key policy document is outside its published length range.
//
// LimitExceededException at 400, published on both API_CreateKey and API_PutKeyPolicy and named for
// this condition by CreateKey's Policy member itself — "if the key policy exceeds the length
// constraint, AWS KMS returns a LimitExceededException" — which is why this is not
// MalformedPolicyDocumentException even though an oversize document is also usually a bad one. The
// error's own gloss agrees: "a length constraint or quota was exceeded".
//
// It is checked before the document is parsed, so a caller that sends 40 KB of valid JSON is told the
// size rather than being handed a parse of it, and a 40 KB document that is *also* malformed hears
// the reason AWS names for its size.
func kmsPolicyTooLong(size int) *AWSError {
	return &AWSError{
		Code: "LimitExceededException",
		Message: fmt.Sprintf("the key policy is %d bytes, which is outside the supported range of %d to %d",
			size, kmsPolicyMinBytes, kmsPolicyMaxBytes),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsMalformedPolicyDocument reports that a key policy is not a document KMS can read.
//
// MalformedPolicyDocumentException at 400, published on both API_CreateKey and API_PutKeyPolicy and
// glossed "the specified policy is not syntactically or semantically correct". Substrate answers it
// for exactly two conditions — the value is not JSON, and the value is JSON but not an object — and
// this file's preamble records why nothing stricter is defensible: AWS's own Policy description states
// that a statement missing Action or Resource still succeeds.
//
// The reason is passed through rather than composed here because the two conditions are actionable in
// different ways: a caller that sent a JSON array has a different bug from one whose string was
// truncated.
func kmsMalformedPolicyDocument(reason string) *AWSError {
	return &AWSError{
		Code:       "MalformedPolicyDocumentException",
		Message:    "the key policy is not a valid policy document: " + reason,
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsUnknownPolicyName reports that a PolicyName names no policy on the key.
//
// NotFoundException at 400. This is a one-step reading rather than a transcription: neither
// API_GetKeyPolicy nor API_PutKeyPolicy publishes a code *for* PolicyName, but both state that
// `default` is the only valid value and both publish NotFoundException, glossed "the specified entity
// or resource could not be found". A name that is not `default` names no policy, which is that gloss
// exactly — and it keeps the refusal among the codes the page publishes instead of reaching for
// CommonErrors' ValidationError, which is where substrate lands only when the page offers nothing.
//
// The message names the one valid value, because a caller that guessed a name has no way to discover
// from the refusal alone that the set has exactly one member.
func kmsUnknownPolicyName(policyName string) *AWSError {
	return &AWSError{
		Code: "NotFoundException",
		Message: "no key policy named " + policyName + " exists on this key; " +
			kmsDefaultPolicyName + " is the only valid policy name",
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsValidatePolicyName admits an absent or empty PolicyName and refuses any value but `default`.
//
// Absent and empty are the same request here for the reason the member's own documentation gives —
// PolicyName is Required: No on both operations and "if no policy name is specified, the default
// value is `default`" — so a caller that omits it and one that sends `""` are both asking for the only
// policy that exists. That is a different reading from Plaintext's in [kmsDecodePlaintext], where the
// empty value is refused; the difference is that this member has a documented default and that one has
// a documented minimum length.
func kmsValidatePolicyName(policyName string) *AWSError {
	if policyName == "" || policyName == kmsDefaultPolicyName {
		return nil
	}
	return kmsUnknownPolicyName(policyName)
}

// kmsValidateKeyPolicy refuses a key policy document that KMS would not accept, and reports nil for
// one it would.
//
// Length first, then parse, for the reason [kmsPolicyTooLong] records. The parse target is a
// string-keyed map rather than `any`, which is what makes "valid JSON but not an object" — `[]`,
// `"policy"`, `5`, `null` — a refusal: a policy document is a JSON object per the IAM JSON Policy
// Reference, and a caller that sent a bare array of statements has made a mistake substrate can name
// cheaply and AWS's console would report.
//
// It is called from both createKey and putKeyPolicy on the same value, so the two operations cannot
// disagree about what they will store — the divergence #961 found between GenerateDataKey and its
// sibling, avoided by construction rather than by a test.
func kmsValidateKeyPolicy(policy string) *AWSError {
	if len(policy) < kmsPolicyMinBytes || len(policy) > kmsPolicyMaxBytes {
		return kmsPolicyTooLong(len(policy))
	}
	if !json.Valid([]byte(policy)) {
		return kmsMalformedPolicyDocument("it is not valid JSON")
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal([]byte(policy), &document); err != nil {
		return kmsMalformedPolicyDocument("its top-level value is not a JSON object")
	}
	return nil
}
