package emulator

// KMS error construction: one helper per published code, so a code's HTTP status is decided once
// rather than at each of the thirty-nine call sites that answered it.
//
// The reason for the file is a finding that holds across every KMS operation substrate models, and
// it is worth stating before the helpers because it is what makes them one-liners: **KMS publishes
// no 404 for any resource.** Across the fifteen operation pages reached from this package, exactly
// two statuses appear — 500 for DependencyTimeoutException, KMSInternalException and
// KeyUnavailableException, and 400 for everything else, NotFoundException included. The only 404
// anywhere in KMS's documentation is UnknownOperationException on CommonErrors.html, which reports
// that the *action name* was not recognized. So a KMS status carries nothing a caller can branch on
// and the code is the whole signal — the shape #933 recorded for Systems Manager, arrived at from
// the other direction.
//
// Three codes were wrong before #923, each in a different way:
//
//   - NotFoundException answered HTTP 404 at fifteen sites. Every one of the fifteen belongs to an
//     operation whose own reference page gives it 400: "The request was rejected because the
//     specified entity or resource could not be found. HTTP Status Code: 400". That is the status
//     kms_tags.go's own refusals already used, so the two halves of the package disagreed.
//
//   - DisabledException answered HTTP 409 at three sites. API_Encrypt, API_Decrypt and
//     API_GenerateDataKey each publish it at 400: "The request was rejected because the specified
//     KMS key is not enabled".
//
//   - InvalidRequest was answered at twenty-one sites for an unparseable request body, and the
//     string appears nowhere in KMS's documentation — not on an operation page, not on
//     CommonErrors.html. A caller matching on it matched something no SDK models. The replacement
//     is ValidationError, published on CommonErrors.html at 400: "The input doesn't meet the
//     required format or constraints. Check that all required parameters are included and that
//     values are valid." Being a common error, it applies to every operation, which the
//     twenty-one sites need and TagException does not give: TagException is also 400 but is
//     glossed "one or more tags are not valid", which is the content of a Tags member, and
//     eighteen of the twenty-one operations take no tags at all.
//
// Three codes deliberately not reached for, recorded because each is a plausible guess that would
// repeat the defect above. MalformedHttpRequestException is on CommonErrors.html at 400 but its
// published scope is the transport layer — "This typically happens when the request body can't be
// decompressed using the specified content encoding algorithm" — and a body that decompressed and
// then failed to parse is not that. SerializationException and ValidationException appear nowhere
// in KMS's documentation at all; KMS spells it ValidationError with no "Exception" suffix, unlike
// most JSON-protocol services, so a caller matching ValidationException would match nothing.
//
// Messages are substrate's throughout. AWS publishes codes and statuses, not message text.

import (
	"fmt"
	"net/http"
	"strings"
)

// kmsNotFound reports that a KeyId, an alias or a destination key names nothing KMS holds.
//
// The code is NotFoundException and the status is 400, which every operation that can answer it
// publishes; see this file's preamble for why no KMS refusal is a 404. The detail is passed through
// rather than composed here because the callers name different things — a key, an alias, the
// destination key of a ReEncrypt — and a caller reading a log needs to know which.
func kmsNotFound(detail string) *AWSError {
	return &AWSError{
		Code:       "NotFoundException",
		Message:    detail,
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsKeyDisabled reports that an operation named a key that exists and is not enabled.
//
// The code is DisabledException at 400, published identically on API_Encrypt, API_Decrypt,
// API_GenerateDataKey, API_GenerateDataKeyWithoutPlaintext, API_ReEncrypt, API_EnableKeyRotation and
// API_DisableKeyRotation — the seven operations substrate refuses this way, every one of which gives a
// Disabled key footnote [1] in the developer guide's key-state table. The rotation pair was added by
// #949; before it they wrote RotationEnabled against a disabled key and answered 200, which #923
// recorded as a missing refusal rather than a wrong status. GenerateDataKeyWithoutPlaintext and
// ReEncrypt were added by #961, which found they refused a disabled key nowhere at all.
//
// Every caller reaches this through [kmsKeyStateError], which is what rules out PendingDeletion first —
// the key-state table gives that state a different code, and ScheduleKeyDeletion clears Enabled as it
// writes the state, so a bare !key.Enabled test at a call site would answer this for a key pending
// deletion. See [kmsInvalidKeyState].
func kmsKeyDisabled(keyID string) *AWSError {
	return &AWSError{
		Code:       "DisabledException",
		Message:    fmt.Sprintf("the KMS key %q is not enabled", keyID),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsInvalidKeyState reports that a key exists but is in a key state the operation does not permit.
//
// The code is KMSInvalidStateException at 400 — "the request was rejected because the state of the
// specified resource is not valid for this request" — published on every operation reached from this
// package that reads or writes a key, API_EnableKeyRotation and API_DisableKeyRotation included.
//
// It is a separate code from DisabledException rather than a synonym for it, and the developer
// guide's "Key states of AWS KMS keys" table is where the two divide: for both rotation operations a
// Disabled key is footnote [1], "DisabledException: <key ARN> is disabled", while a key pending
// deletion is footnote [3], "KMSInvalidStateException: <key ARN> is pending deletion". So a caller
// distinguishing "enable the key and retry" from "cancel the deletion and retry" reads the code, and
// answering DisabledException for both would collapse two different remedies into one. The state is
// named in the message for the same reason.
//
// For the five cryptographic operations #961 brought here the same table cell reads "[2] or [3]", and
// footnote [2] is that same sentence under **DisabledException** — so AWS admits either code there and
// the DisabledException substrate answered before #961 was not outside what the table publishes.
// Choosing this one anyway is substrate's reading, argued in [kmsKeyStateError]: it is the only
// choice under which one key state produces one code across the plugin, and the only one a caller can
// branch on to tell the two remedies apart.
//
// PendingDeletion is the only state substrate can be in here: ScheduleKeyDeletion is the sole writer
// of anything but Enabled or Disabled, so PendingImport, Unavailable, Creating and Updating — which
// the table also refuses — are unreachable and are recorded as such rather than guarded against.
//
// For the inverse refusal — an operation that requires PendingDeletion and did not find it — see
// [kmsKeyNotPendingDeletion], which carries the same code and a different message because AWS's
// footnote for it is a negation.
func kmsInvalidKeyState(keyID, state string) *AWSError {
	return &AWSError{
		Code:       "KMSInvalidStateException",
		Message:    fmt.Sprintf("the KMS key %q is in state %q, which this operation does not permit", keyID, state),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsKeyNotPendingDeletion reports that CancelKeyDeletion named a key that is not scheduled for
// deletion, so there is no deletion to cancel.
//
// The code is KMSInvalidStateException at 400, the same as [kmsInvalidKeyState], and this is a
// separate helper only because of the message. CancelKeyDeletion is the one operation in the
// developer guide's key-state table whose permitted set is a single state: every row but
// PendingDeletion is footnote [4], and that footnote is phrased as a negation —
// "KMSInvalidStateException: <key ARN> is not pending deletion" — where every other footnote names
// the offending state. Naming the state here would read as though some other state were the problem,
// when the problem is the absence of the one state the operation needs.
//
// Answering this at all is the point of #963: before it, CancelKeyDeletion on a key that had never
// been scheduled answered 200 and enabled the key, which is EnableKey reached through an operation a
// caller may hold no iam:EnableKey permission for.
func kmsKeyNotPendingDeletion(keyID string) *AWSError {
	return &AWSError{
		Code:       "KMSInvalidStateException",
		Message:    fmt.Sprintf("the KMS key %q is not pending deletion, so there is no deletion to cancel", keyID),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsInvalidPendingWindow reports a ScheduleKeyDeletion waiting period outside the published range.
//
// API_ScheduleKeyDeletion gives PendingWindowInDays a Valid Range of 7 to 30 and states it twice —
// "you can specify a waiting period of 7-30 days" in the prose and "if you include a value, it must
// be between 7 and 30, inclusive" on the parameter — but publishes no error code for violating it:
// its list is DependencyTimeoutException, InvalidArnException, KMSInternalException,
// KMSInvalidStateException and NotFoundException, none of which describes a bad parameter value. So
// the code here is substrate's reading, taken from CommonErrors.html for the same reason
// [kmsInvalidBody] takes it: a failure that belongs to no operation-specific code has to come from
// the common set, and ValidationError at 400 is the one that fits a value out of range.
//
// The bound is stated in the message rather than left implicit, because a caller that sent 1 or 365
// has no way to discover 7-30 from a bare refusal.
func kmsInvalidPendingWindow(days int) *AWSError {
	return &AWSError{
		Code: "ValidationError",
		Message: fmt.Sprintf(
			"PendingWindowInDays is %d, which is outside the valid range of %d to %d",
			days, kmsMinPendingWindowInDays, kmsMaxPendingWindowInDays),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsInvalidRotationPeriod reports an EnableKeyRotation rotation period outside the published range.
//
// The code is the same reading [kmsInvalidPendingWindow] records, reached the same way and deliberately
// reused rather than re-argued: API_EnableKeyRotation gives RotationPeriodInDays a Valid Range of 90 to
// 2560 and publishes no code for violating it — its seven errors are DependencyTimeoutException,
// DisabledException, InvalidArnException, KMSInternalException, KMSInvalidStateException,
// NotFoundException and UnsupportedOperationException — so ValidationError at 400, from
// CommonErrors.html, is where a bad parameter value has to land. Two range violations in one plugin
// answering two different codes would be the divergence #923 exists to prevent.
//
// UnsupportedOperationException is the near miss on that list and is not chosen. Its published gloss is
// "a specified parameter is not supported or a specified resource is not valid for this operation",
// which describes a parameter or resource that is inadmissible — an asymmetric key, say, which is #972 —
// not an admissible parameter carrying a number out of range.
//
// The bound is named in the message for [kmsInvalidPendingWindow]'s reason: a caller that sent 30,
// reasoning by analogy from the deletion window, cannot discover 90-2560 from a bare refusal.
func kmsInvalidRotationPeriod(days int) *AWSError {
	return &AWSError{
		Code: "ValidationError",
		Message: fmt.Sprintf(
			"RotationPeriodInDays is %d, which is outside the valid range of %d to %d",
			days, kmsMinRotationPeriodInDays, kmsMaxRotationPeriodInDays),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsRotationUnsupportedKeySpec reports that EnableKeyRotation or DisableKeyRotation named a key whose
// type cannot have automatic rotation.
//
// UnsupportedOperationException at 400, published on both pages and glossed "the request was rejected
// because a specified parameter is not supported or a specified resource is not valid for this
// operation". A key of the wrong type is the second half of that gloss — a specified resource not valid
// for this operation — and it is the case [kmsInvalidRotationPeriod] names when it declines the same code
// for a number out of range, so the two readings are one decision seen from both sides.
//
// The restriction is stated twice on API_EnableKeyRotation, once in the prose and once on the KeyId
// parameter: "automatic key rotation is supported only on symmetric encryption KMS keys. You cannot
// enable automatic rotation of asymmetric KMS keys, HMAC KMS keys, KMS keys with imported key material,
// or KMS keys in a custom key store." API_GetKeyRotationStatus repeats it verbatim, which is corroboration
// that the restriction is a property of the key rather than of the call.
//
// The discriminator is KeySpec, not KeyUsage. SYMMETRIC_DEFAULT is the only spec that rotates, so one
// equality test covers all three unsupported families at once — the asymmetric specs, the HMAC specs and
// the ML-DSA specs — where KeyUsage would separate the HMAC case from the asymmetric one and still need
// the spec to tell RSA from symmetric. That CreateKey once accepted a KeySpec and KeyUsage that cannot
// occur together was #977, closed by [kmsResolveKeySpecAndUsage]; this refusal reads the member AWS's own
// sentence is about, and is unchanged by it — a rotation call reaches a key that already exists, so it
// answers for the key's stored spec whatever CreateKey validated.
//
// The spec is named in the message so a caller can tell this refusal from a key-state one, which is the
// other reason an EnableKeyRotation gets a 400.
func kmsRotationUnsupportedKeySpec(key *KMSKey) *AWSError {
	return &AWSError{
		Code: "UnsupportedOperationException",
		Message: fmt.Sprintf(
			"the KMS key %q has key spec %q, and automatic rotation is supported only on %s keys",
			key.KeyID, key.KeySpec, kmsSymmetricDefaultKeySpec),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsUnknownKeySpec reports a CreateKey KeySpec outside the published set.
//
// ValidationError at 400, the reading [kmsInvalidPendingWindow] records and [kmsUnknownEncryptionAlgorithm]
// applies to the same class of failure — a member carrying a value its enum does not contain. It has to
// come from CommonErrors.html because API_CreateKey publishes thirteen errors and not one of them
// describes a malformed member: the four custom-key-store and external-key codes are about a store or an
// XKS key, InvalidArnException about an ARN, MalformedPolicyDocumentException about the Policy,
// TagException about the Tags, LimitExceededException about a quota, and the two 500s about KMS itself.
//
// UnsupportedOperationException is the near miss and is deliberately left to [kmsInadmissibleKeyUsage],
// which is the same decision [kmsUnknownEncryptionAlgorithm] made against InvalidKeyUsageException and for
// the same reason: that code presupposes a value AWS recognizes and says something about how it fits the
// key, while a misspelling fits no key and says nothing. Keeping the two apart is what lets a caller tell
// a typo from a misunderstanding.
//
// The set is listed for [kmsUnknownEncryptionAlgorithm]'s reason — a caller that sent RSA_2049 or a spec
// from an API version newer than its SDK can then see what it should have sent. Seventeen values is a long
// message, and the alternative is a caller reading the documentation to learn something the refusal
// already knows.
func kmsUnknownKeySpec(keySpec string) *AWSError {
	return &AWSError{
		Code: "ValidationError",
		Message: fmt.Sprintf(
			"KeySpec is %q, which is not one of %s",
			keySpec, strings.Join(kmsKeySpecs, ", ")),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsUnknownKeyUsage reports a CreateKey KeyUsage outside the published set.
//
// ValidationError at 400, for [kmsUnknownKeySpec]'s reasons exactly — the same operation, the same
// thirteen published errors, and the same class of failure. The two are separate helpers only so the
// message names the member the caller got wrong, which is the whole content of the refusal when a request
// carries one bad member and one good one.
//
// The four are listed rather than the ones this key spec admits, because the spec is not what is wrong: a
// caller that sent ENCRYPT_DECRYPTT wants to know the spelling, and one that sent a usage admissible
// nowhere is answered by [kmsInadmissibleKeyUsage] instead, which does name the spec's own set.
func kmsUnknownKeyUsage(keyUsage string) *AWSError {
	return &AWSError{
		Code: "ValidationError",
		Message: fmt.Sprintf(
			"KeyUsage is %q, which is not one of %s",
			keyUsage, strings.Join(kmsKeyUsages, ", ")),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsUnknownCustomerMasterKeySpec reports a CreateKey CustomerMasterKeySpec outside that member's own
// published set.
//
// ValidationError at 400, for [kmsUnknownKeySpec]'s reasons, and a separate helper from it because the two
// enums are not the same enum: this member publishes thirteen valid values where KeySpec publishes
// seventeen. Listing the thirteen is the whole point of the refusal — a caller that sent ML_DSA_44 under the
// deprecated name has a value that is a perfectly good key spec, and the message has to say that this
// member does not carry it rather than implying the spec does not exist. That is why the message names the
// member and quotes its own set instead of deferring to [kmsUnknownKeySpec]'s.
//
// The narrowing is AWS's, per [kmsCustomerMasterKeySpecs]; the refusal is substrate's reading, and it is the
// request-side half of the omission #974 chose when reporting the member. Accepting the value as a key spec
// would be the other reading, and it would let a request name a spec through a member AWS does not publish
// it under — the same shape of defect as reporting one.
func kmsUnknownCustomerMasterKeySpec(keySpec string) *AWSError {
	return &AWSError{
		Code: "ValidationError",
		Message: fmt.Sprintf(
			"CustomerMasterKeySpec is %q, which is not one of %s; use KeySpec for a newer key spec",
			keySpec, strings.Join(kmsCustomerMasterKeySpecs, ", ")),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsConflictingKeySpecMembers reports a CreateKey that sent KeySpec and CustomerMasterKeySpec with
// different values.
//
// ValidationError at 400, and both values are named because either could be the intended one — substrate
// cannot tell, which is precisely why it refuses instead of choosing. AWS documents no answer for the
// conflict; what it documents is that the two members "have the same value", so a request in which they
// disagree is not a request the page describes. See [kmsResolveRequestKeySpec] for why a precedence rule was
// rejected: it discards half of a contradictory request silently, which is the failure mode #985 exists to
// remove rather than relocate.
//
// Equal values do not reach here, so a caller that sets both to RSA_4096 — as an SDK migrating between the
// two names might — is not punished for redundancy.
func kmsConflictingKeySpecMembers(keySpec, deprecated string) *AWSError {
	return &AWSError{
		Code: "ValidationError",
		Message: fmt.Sprintf(
			"KeySpec is %q and CustomerMasterKeySpec is %q; the two members must carry the same value",
			keySpec, deprecated),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsKeyUsageRequired reports a CreateKey that omitted KeyUsage for a key spec that requires it.
//
// ValidationError at 400. A required parameter that is absent is a defect of the request in the same way a
// value outside an enum is, so it lands where [kmsUnknownKeySpec] lands and by the same argument about
// API_CreateKey's thirteen errors. It is not UnsupportedOperationException: nothing about the request is
// unsupported, and the caller has not asked for a pairing AWS declines — it has not said what it wants.
//
// AWS's rule is on the KeyUsage parameter, verbatim: "this parameter is optional when you are creating a
// symmetric encryption KMS key; otherwise, it is required". The reading that matters is what "symmetric
// encryption KMS key" excludes, and AWS settles it in the operation's own HMAC guidance rather than
// leaving it to inference: "you must set the key usage even though GENERATE_VERIFY_MAC is the only valid
// key usage value for HMAC KMS keys". An HMAC key is symmetric and is not a symmetric *encryption* key, so
// the default fires for SYMMETRIC_DEFAULT alone — see [kmsResolveKeySpecAndUsage].
//
// The admissible set is named because it is what the caller has to supply next, and for HMAC and ML-DSA it
// is a single value: the refusal then tells them the whole answer rather than that a member is missing.
func kmsKeyUsageRequired(keySpec string) *AWSError {
	return &AWSError{
		Code: "ValidationError",
		Message: fmt.Sprintf(
			"KeyUsage is required for a key with key spec %q, which supports %s",
			keySpec, kmsAdmissibleKeyUsageList(keySpec)),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsInadmissibleKeyUsage reports a CreateKey whose KeySpec and KeyUsage are each published and cannot be
// paired.
//
// UnsupportedOperationException at 400, published on API_CreateKey and glossed "the request was rejected
// because a specified parameter is not supported or a specified resource is not valid for this operation".
// A well-formed parameter that this operation will not accept is the first half of that gloss, and it is
// the reading [kmsRotationUnsupportedKeySpec] already took for the same code — so the two agree about what
// the code means, which is the consistency #923 exists to hold.
//
// It is not ValidationError, and the distinction is the point rather than a nicety. Both members satisfy
// their own constraints; what fails is the combination, which is a fact about KMS rather than about the
// request's format. A caller reading ValidationError looks for a typo and finds none. The two codes are
// therefore load-bearing together: this one says *that is a key spec, and not with that usage*, where
// [kmsUnknownKeySpec] says *that is not a key spec*.
//
// AWS publishes the pairing rules as seven bullets under KeyUsage and attaches no code to them, so the
// code is substrate's reading; [kmsKeySpecAdmitsKeyUsage] records why the rules are derived from the
// algorithm tables rather than transcribed. The admissible set is named for
// [kmsIncompatibleEncryptionAlgorithm]'s reason: a caller that sent GENERATE_VERIFY_MAC to an RSA spec
// needs to be told what that spec does support, not merely that this is not it.
func kmsInadmissibleKeyUsage(keySpec, keyUsage string) *AWSError {
	return &AWSError{
		Code: "UnsupportedOperationException",
		Message: fmt.Sprintf(
			"KeyUsage %q is not valid for a key with key spec %q, which supports %s",
			keyUsage, keySpec, kmsAdmissibleKeyUsageList(keySpec)),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsUnknownKeyOrigin reports a CreateKey Origin outside the published set.
//
// ValidationError at 400, for [kmsUnknownKeySpec]'s reasons exactly: no operation-specific code on
// API_CreateKey describes a malformed member, so it comes from CommonErrors.html, and a value that names
// no origin is a spelling problem rather than a statement about the key. It is deliberately not
// [kmsUnsupportedKeyOrigin]'s code — see kms_key_origin.go's preamble for why the two must stay apart.
//
// The four are listed because that is what a caller has to choose from next, and because three of them
// are then refused for a different reason: naming the set here and the boundary there is what makes the
// pair of refusals legible in sequence.
func kmsUnknownKeyOrigin(origin string) *AWSError {
	return &AWSError{
		Code: "ValidationError",
		Message: fmt.Sprintf(
			"Origin is %q, which is not one of %s",
			origin, strings.Join(kmsKeyOrigins, ", ")),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsXksKeyIDNotValidForOrigin reports an XksKeyId sent with an Origin that does not take one.
//
// ValidationError at 400, and this is the one refusal in #984's set that is not about substrate's scope:
// AWS states the condition itself — "this parameter is required for a KMS key with an Origin value of
// EXTERNAL_KEY_STORE. It is not valid for KMS keys with any other Origin value" — so a caller reading
// this has a request real KMS would also reject. AWS attaches no code to that sentence, so the code is
// substrate's reading, and it is the same one #977 took for every other malformed-member refusal on this
// operation: a member that does not belong in the request is a defect of the request.
//
// XksKeyInvalidConfigurationException is the near miss and is not chosen, for the reason
// kms_key_origin.go's preamble gives: that code is about an external key's configuration in a store,
// which presupposes the store this refusal exists because substrate does not have.
//
// The origin is named because it is the half of the pair the caller can change — sending the parameter is
// correct for exactly one origin, and the message has to say which request the caller actually made.
func kmsXksKeyIDNotValidForOrigin(origin string) *AWSError {
	return &AWSError{
		Code: "ValidationError",
		Message: fmt.Sprintf(
			"XksKeyId is not valid for a key with Origin %q; it is valid only for Origin %s",
			origin, kmsKeyOriginExternalKeyStore),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsUnsupportedKeyOrigin reports a published Origin substrate does not create a key for.
//
// UnsupportedOperationException at 400, published on API_CreateKey and glossed "the request was rejected
// because a specified parameter is not supported or a specified resource is not valid for this
// operation". A well-formed parameter this operation will not accept is the first half of that gloss,
// which is the reading [kmsInadmissibleKeyUsage] and [kmsRotationUnsupportedKeySpec] already took for the
// same code — so all three agree about what it means, the consistency #923 exists to hold.
//
// Unlike those two, the boundary here is substrate's rather than KMS's: real KMS honors every value in
// [kmsKeyOrigins]. That is stated in the message rather than left to the code, because a caller whose
// import workflow stops here needs to know it is testing against an emulator that models no imported key
// material, not that its request is wrong. Both facts are in the message for that reason: what substrate
// does support, and that this is a boundary rather than a rejection.
func kmsUnsupportedKeyOrigin(origin string) *AWSError {
	return &AWSError{
		Code: "UnsupportedOperationException",
		Message: fmt.Sprintf(
			"Origin %q is not supported: substrate creates key material itself and models neither imported "+
				"key material nor a custom key store, so %s is the only supported origin",
			origin, kmsKeyOriginAWSKMS),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsUnsupportedCustomKeyStore reports a CustomKeyStoreId substrate has no store to resolve.
//
// UnsupportedOperationException at 400, for [kmsUnsupportedKeyOrigin]'s reasons and reached only for an
// AWS_KMS origin, since every other origin is refused before this check.
//
// It is not CustomKeyStoreNotFoundException, although API_CreateKey publishes that code and it would
// describe the immediate fact. That code says *no store has this ID*, which invites a caller to create
// one — and CreateCustomKeyStore does not exist either, so the caller would loop. This one says the
// parameter is unsupported, which is the accurate and actionable answer. The distinction is the same one
// [kmsUnsupportedKeyOrigin] draws, and it is why neither of the two store-specific errors is constructed
// anywhere: each presupposes a modeled store.
//
// The ID is quoted because a caller reading its own value back is how they confirm the parameter reached
// substrate at all, which is precisely what was in doubt before #984.
func kmsUnsupportedCustomKeyStore(customKeyStoreID string) *AWSError {
	return &AWSError{
		Code: "UnsupportedOperationException",
		Message: fmt.Sprintf(
			"CustomKeyStoreId %q is not supported: substrate models no custom key store",
			customKeyStoreID),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsIncorrectKey reports that a caller named a key that is not the one which encrypted the ciphertext.
//
// IncorrectKeyException at 400, published on API_Decrypt and API_ReEncrypt and glossed identically on
// both: "the request was rejected because the specified KMS key cannot decrypt the data. The KeyId in a
// Decrypt request and the SourceKeyId in a ReEncrypt request must identify the same KMS key that was
// used to encrypt the ciphertext."
//
// It is the one refusal in #969's set that AWS attaches a code to, which is why that issue put it
// first: the two algorithm members add a rendered value, while this adds an outcome a caller's error
// path can be tested against. Both operations describe the member as a constraint rather than a
// selector — "if you identify a different KMS key, the operation throws an IncorrectKeyException" —
// and substrate decoded neither, so a caller that named the wrong key was silently given the right
// one, which is the opposite of what the member exists for.
//
// Both key ARNs are named because the whole content of the refusal is that two identifiers differ, and
// a caller holding a ciphertext it did not create has no other way to learn which key it needs.
func kmsIncorrectKey(namedARN, ciphertextARN string) *AWSError {
	return &AWSError{
		Code: "IncorrectKeyException",
		Message: fmt.Sprintf(
			"the KMS key %q cannot decrypt this ciphertext, which was encrypted under %q",
			namedARN, ciphertextARN),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsCiphertextAlgorithmMismatch reports a decrypt request naming an encryption algorithm other than the
// one that produced the ciphertext.
//
// InvalidCiphertextException at 400, published on API_Decrypt and API_ReEncrypt. The behavior is
// published on both pages — "specify the same algorithm that was used to encrypt the data. If you specify
// a different algorithm, the Decrypt operation fails" — and the *code* for it is not, so this is the
// published rule with substrate's reading of which code carries it. The gloss's "or otherwise invalid" is
// what it rests on: the two conditions it does name, a corrupted ciphertext and a mismatched encryption
// context, are both "this blob and this request disagree", and so is this.
//
// [kmsIncompatibleEncryptionAlgorithm] is the near miss and is wrong here. That one is about an algorithm
// the *key* does not admit, which is a fact about the key and knowable without any ciphertext; this is
// about an algorithm the key admits perfectly well but which did not encrypt these bytes.
// [kmsIncorrectKey] is the other near miss and is wrong for the mirrored reason — it is about the key the
// caller named, not about the algorithm.
//
// Both algorithms are named, following [kmsIncorrectKey]: the whole content of the refusal is that two
// values differ, and a caller handed a ciphertext it did not create has no other way to learn which
// algorithm it needs. Naming the recorded one leaks nothing — API_ReEncrypt tells a caller to record it
// alongside the key ID, so it is a value AWS expects the caller to hold already.
func kmsCiphertextAlgorithmMismatch(member, recorded, supplied string) *AWSError {
	return &AWSError{
		Code: "InvalidCiphertextException",
		Message: fmt.Sprintf(
			"%s is %q, but this ciphertext was encrypted with %q",
			member, supplied, recorded),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsCiphertextContextMismatch reports a decrypt request whose encryption context is not the exact
// case-sensitive match of the one the ciphertext was encrypted under.
//
// InvalidCiphertextException at 400, and unlike [kmsCiphertextAlgorithmMismatch] this one is published
// end to end. API_Encrypt states the consequence and names the code: "if you specify an EncryptionContext
// when encrypting data, you must specify the same encryption context (a case-sensitive exact match) when
// decrypting the data. Otherwise, the request to decrypt fails with an InvalidCiphertextException." The
// gloss then names the condition outright — "the specified ciphertext, or additional authenticated data
// incorporated into the ciphertext, such as the encryption context, is corrupted, missing, or otherwise
// invalid".
//
// Both contexts are rendered, which needs saying because an encryption context is caller data. AWS
// settles it: the member's own gloss is "an encryption context is a collection of non-secret key-value
// pairs", and API_GenerateDataKey adds "do not include confidential or sensitive information in this
// field. This field may be displayed in plaintext in CloudTrail logs and other output." A refusal message
// is exactly that other output, and a caller whose context differs in one character cannot act on a
// refusal that will not say which.
//
// The two absent cases render as "none" via [kmsFormatEncryptionContext] rather than as an empty string,
// so a message about a context that was never sent reads as a sentence rather than as a missing value.
func kmsCiphertextContextMismatch(member string, recorded, supplied map[string]string) *AWSError {
	return &AWSError{
		Code: "InvalidCiphertextException",
		Message: fmt.Sprintf(
			"%s is %s, but this ciphertext was encrypted with %s",
			member, kmsFormatEncryptionContext(supplied), kmsFormatEncryptionContext(recorded)),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsUnknownEncryptionAlgorithm reports an encryption algorithm outside the published set.
//
// ValidationError at 400, the same reading [kmsInvalidPendingWindow] records and reached the same way:
// a member carrying a value the enum does not contain is a malformed request, none of the four
// operation pages that publish these members gives a code for it, and CommonErrors.html is therefore
// where it has to land.
//
// InvalidKeyUsageException is the near miss and is not chosen here, though it is chosen by
// [kmsIncompatibleEncryptionAlgorithm] for the neighboring case. Its gloss is about an algorithm
// "incompatible with the type of key material in the KMS key (KeySpec)", which presupposes an algorithm
// AWS recognizes; a misspelling is incompatible with every key spec and says nothing about the key.
// The set is listed in the message so a caller can see what it should have sent.
func kmsUnknownEncryptionAlgorithm(member, algorithm string) *AWSError {
	return &AWSError{
		Code: "ValidationError",
		Message: fmt.Sprintf(
			"%s is %q, which is not one of %s",
			member, algorithm, strings.Join(kmsEncryptionAlgorithms, ", ")),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsIncompatibleEncryptionAlgorithm reports a published encryption algorithm that the named key's key
// spec does not admit.
//
// InvalidKeyUsageException at 400, published on API_Encrypt, API_Decrypt and API_ReEncrypt. This is the
// second of the two bullets in its gloss, verbatim: "the encryption algorithm or signing algorithm
// specified for the operation is incompatible with the type of key material in the KMS key (KeySpec)".
// The code is published rather than substrate's reading, and the mapping is exact.
//
// The first bullet — "the KeyUsage value of the KMS key is incompatible with the API operation" — is
// [kmsInvalidKeyUsageForOperation], added by #977. The two are one error code with two causes, so a caller
// matching on the code alone cannot tell them apart; the message names the key spec, which is the half this
// one is about, where the other names the key usage.
//
// The key spec and the admissible set are both in the message because the refusal is otherwise
// unactionable: a caller that sent SYMMETRIC_DEFAULT to an RSA key needs to be told the key is RSA.
//
// The empty set — "no encryption algorithm" — is now unreachable, and the branch is kept rather than
// removed. It was the answer for a key whose spec admits no encryption at all, an ECC key say, and #977
// made that key unreachable twice over: CreateKey will not pair such a spec with ENCRYPT_DECRYPT, and
// [kmsKeyUsageError] runs ahead of this check at all five call sites, so such a key is refused for its
// usage before its algorithm is considered. That ordering is deliberate — see [kmsKeyUsageError] for why
// one condition producing one message was worth choosing — and the branch stays because it is a claim
// about what an empty list means, which remains true whether or not a caller can provoke it.
func kmsIncompatibleEncryptionAlgorithm(key *KMSKey, member, algorithm string) *AWSError {
	permitted := "no encryption algorithm"
	if admissible := kmsEncryptionAlgorithmsByKeySpec[key.KeySpec]; len(admissible) > 0 {
		permitted = strings.Join(admissible, ", ")
	}
	return &AWSError{
		Code: "InvalidKeyUsageException",
		Message: fmt.Sprintf(
			"%s is %q, which the KMS key %q does not support: its key spec %q supports %s",
			member, algorithm, key.KeyID, key.KeySpec, permitted),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsInvalidKeyUsageForOperation reports that a cryptographic operation named a key whose KeyUsage is not
// ENCRYPT_DECRYPT.
//
// InvalidKeyUsageException at 400, published on all five operations that call it — API_Encrypt, API_Decrypt,
// API_ReEncrypt, API_GenerateDataKey and API_GenerateDataKeyWithoutPlaintext. This is the **first** bullet
// of its gloss, verbatim: "the KeyUsage value of the KMS key is incompatible with the API operation". The
// requirement the bullet is about is stated on each operation's own page — "for encrypting, decrypting,
// re-encrypting, and generating data keys, the KeyUsage must be ENCRYPT_DECRYPT" — so both the code and the
// rule are published, and only the message is substrate's.
//
// [kmsIncompatibleEncryptionAlgorithm] is the second bullet, and the two sharing one code is AWS's design
// rather than a collision: a caller cannot distinguish them by code, which is why one message leads with
// the key usage and the other with the key spec. #969 shipped the second and this is the first, and they
// are genuinely independent — RSA_2048 admits RSAES_OAEP_SHA_256 whatever the key's usage is, so an Encrypt
// against an RSA signing key satisfied every check substrate had before #977.
//
// The usage is named and the required one spelled out, because the remedy is not to retry: KeyUsage cannot
// be changed after creation — "you can't change the KeyUsage value after the KMS key is created" — so the
// caller needs a different key, and the message has to say what kind.
func kmsInvalidKeyUsageForOperation(key *KMSKey) *AWSError {
	return &AWSError{
		Code: "InvalidKeyUsageException",
		Message: fmt.Sprintf(
			"the KMS key %q has key usage %q, and this operation requires %s",
			key.KeyID, key.KeyUsage, kmsKeyUsageEncryptDecrypt),
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsInvalidBody reports that a request body could not be parsed as JSON.
//
// The code is ValidationError at 400, from CommonErrors.html, which is where a failure that belongs
// to no single operation has to come from — the twenty-one sites that call this span twenty-one
// operations. It takes no argument because the encoding/json error text describes the emulator's
// decoder rather than anything a caller can act on: the actionable fact is that the body was not
// JSON, which the message states.
func kmsInvalidBody() *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    "the request body is not valid JSON",
		HTTPStatus: http.StatusBadRequest,
	}
}

// kmsInvalidARN reports that an ARN is not one KMS accepts, naming the reason so a caller can tell
// a wrong service from a wrong resource type.
//
// InvalidArnException at 400, which API_TagResource, API_UntagResource and API_ListResourceTags
// each publish and gloss as "the request was rejected because a specified ARN, or an ARN in a key
// policy, is not valid". Moved here from kms_tags.go by #923 so that every KMS refusal is
// constructed in one file.
func kmsInvalidARN(arn, reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidArnException",
		Message:    fmt.Sprintf("the ARN %q is not valid: %s", arn, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}
