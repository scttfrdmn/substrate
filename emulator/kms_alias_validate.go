package emulator

// KMS alias validation: what CreateAlias and UpdateAlias check before they write an alias pointer, and
// which page publishes each check (#1085).
//
// `updateAlias` verified nothing at all. It decoded two members, prepended `alias/` if it was missing and
// wrote a state key — so an alias that did not exist was **created** by the operation whose first
// published sentence is *"Associates an **existing** AWS KMS alias with a different KMS key"*, a bare
// `TargetKeyId` naming no key produced a dangling pointer that every later resolution of the alias fails
// on, a key pending deletion took the alias, and a symmetric key's alias could be re-pointed at an
// RSA_2048 one — the last against a sentence AWS publishes twice and glosses itself: *"This restriction
// prevents errors in code that uses aliases."* `createAlias` checked the same nothing, so all four gaps
// existed twice in one plugin and are fixed together.
//
// # The three alias pages do not publish one AliasName rule
//
// This is why the checks are per-operation rather than one shared validator, and it is the finding that
// makes the issue's "make the three agree" framing unachievable:
//
//	Operation      Pattern                        Length   Codes published for a name
//	CreateAlias    ^alias/[a-zA-Z0-9/_-]+$        1-256    InvalidAliasNameException, LimitExceededException
//	UpdateAlias    ^alias/[a-zA-Z0-9/_-]+$        1-256    LimitExceededException
//	DeleteAlias    ^[a-zA-Z0-9:/_-]+$             1-256    (none)
//
// `DeleteAlias`'s pattern requires **no `alias/` prefix** and permits a colon, while its own prose still
// says the name "must begin with `alias/`" — the page contradicts itself and the machine-readable half is
// the permissive one. So substrate removes the prepend at the two operations whose pattern requires the
// prefix and keeps it at `deleteAlias`, whose pattern does not.
//
// `InvalidAliasNameException` is published on `CreateAlias` **alone**. That is not a gap on
// `UpdateAlias`, because of the order the checks run in: the alias must already exist, and no alias that
// fails `CreateAlias`'s pattern can ever have been created, so a malformed name there is answered by
// `NotFoundException` — which `UpdateAlias` does publish. Nothing is borrowed across pages (#671).
// Over-length is `LimitExceededException` at both, by its own gloss: *"a length constraint or quota was
// exceeded."*
//
// The `alias/aws/` prefix is reserved — *"The `alias/aws/` prefix is reserved for AWS managed keys"* — and
// that sentence is on `CreateAlias` only. Substrate mints no AWS-managed alias, so refusing it at create
// time is the whole of the rule: there is no such alias for `UpdateAlias` to find.
//
// # Only the new target's key state is checked, and the developer guide says why
//
// The operation pages say only *"The KMS key that you use for this operation must be in a compatible key
// state"*, which reads as one condition. The key-state table resolves it into two different ones:
//
//   - `CreateAlias` is footnote **[3]** for PendingDeletion — *"KMSInvalidStateException: <key ARN> is
//     pending deletion (or pending replica deletion)"* — and a checkmark for Disabled.
//   - `UpdateAlias` is footnote **[10]**: *"If the source KMS key is pending deletion, the command
//     succeeds. If the destination KMS key is pending deletion, the command fails."*
//
// So the alias's **current** key being pending deletion is explicitly fine — which is the case an author
// reaching for a single "check the key state" guard would get wrong — and only the **new** target is
// refused. Disabled is accepted at both, by the table and by the pages: neither publishes
// `DisabledException` at all. That is why the refusal is [kmsInvalidKeyState] rather than
// [kmsKeyStateError], which carries both codes.
//
// PendingDeletion is the only refusable state substrate can hold, for the reason [kmsInvalidKeyState]
// records: ScheduleKeyDeletion is the sole writer of anything but Enabled or Disabled.
//
// # The three key families are derived, not listed
//
// The published restriction names them — *"both symmetric or both asymmetric or both HMAC"* — and a
// fourth list of key specs here would be free to drift from the algorithm tables the rest of the plugin
// reads, which is the defect class #952 records and #974 hit inside this plugin. So
// [kmsAliasKeyFamily] asks [kmsIsSymmetricEncryptionKey] and [kmsMACAlgorithmsByKeySpec] instead, and
// everything neither claims is asymmetric.

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	// kmsAliasNamePrefix is the prefix an alias name must carry on the two operations that publish a
	// pattern requiring it — "This value must begin with alias/ followed by the alias name" is the
	// sentence, and the pattern is the machine-readable half of it.
	kmsAliasNamePrefix = kmsAliasResourceType + "/"

	// kmsAWSManagedAliasPrefix is the prefix CreateAlias reserves — "The alias name cannot begin with
	// alias/aws/. The alias/aws/ prefix is reserved for AWS managed keys" — and substrate mints none.
	kmsAWSManagedAliasPrefix = kmsAliasNamePrefix + "aws/"

	// kmsAliasNameMaxLength is the published maximum for AliasName on all three alias operations:
	// "Length Constraints: Minimum length of 1. Maximum length of 256". The minimum of 1 needs no
	// constant, because a name of length 0 fails the pattern for want of the prefix.
	kmsAliasNameMaxLength = 256
)

// kmsAliasNamePattern is the pattern API_CreateAlias and API_UpdateAlias publish for AliasName,
// verbatim: `^alias/[a-zA-Z0-9/_-]+$`.
//
// API_DeleteAlias publishes a different one and is deliberately not matched against this; see this
// file's preamble for the table and for which operation gets which.
var kmsAliasNamePattern = regexp.MustCompile(`^alias/[a-zA-Z0-9/_-]+$`)

// The three key families the alias type-match restriction is stated in terms of. They are substrate's
// spellings of AWS's parenthesis — "both symmetric or both asymmetric or both HMAC" — and appear in a
// refusal message rather than on the wire, so nothing branches on the strings themselves.
const (
	kmsAliasFamilySymmetric  = "symmetric"
	kmsAliasFamilyHMAC       = "HMAC"
	kmsAliasFamilyAsymmetric = "asymmetric"
)

// kmsValidateCreateAliasName reports an AliasName CreateAlias would refuse, or nil.
//
// Three rules, each with its own published code: the length bound answers `LimitExceededException`, and
// the pattern and the reserved prefix answer `InvalidAliasNameException`, which is published on this
// operation alone. Length is tested first because an over-long name that also breaks the pattern has two
// faults and only one of them has a bound to report.
func kmsValidateCreateAliasName(aliasName string) *AWSError {
	if lengthErr := kmsValidateAliasNameLength(aliasName); lengthErr != nil {
		return lengthErr
	}
	if !kmsAliasNamePattern.MatchString(aliasName) {
		return kmsInvalidAliasName(aliasName, fmt.Sprintf(
			"an alias name matches %s", kmsAliasNamePattern.String()))
	}
	if strings.HasPrefix(aliasName, kmsAWSManagedAliasPrefix) {
		return kmsInvalidAliasName(aliasName, "the "+kmsAWSManagedAliasPrefix+
			" prefix is reserved for AWS managed keys")
	}
	return nil
}

// kmsValidateAliasNameLength reports an AliasName longer than the published maximum, or nil.
//
// Shared by CreateAlias and UpdateAlias because both publish the bound *and* the code for it. It is the
// only name check UpdateAlias makes; see this file's preamble for why the pattern needs no separate
// refusal there.
func kmsValidateAliasNameLength(aliasName string) *AWSError {
	if n := utf8.RuneCountInString(aliasName); n > kmsAliasNameMaxLength {
		return kmsAliasNameTooLong(n)
	}
	return nil
}

// kmsAliasKeyFamily names which of AWS's three alias-compatible key families a key belongs to.
//
// Derived from the two structures that already decide the same thing elsewhere, for the reason this
// file's preamble gives. "Symmetric" here is symmetric *encryption*, which is what makes the HMAC arm
// necessary rather than redundant: an HMAC key is symmetric and encrypts nothing, and AWS lists it as its
// own family for exactly that reason. See [kmsIsSymmetricEncryptionKey], which records the same
// distinction from the response side.
func kmsAliasKeyFamily(key *KMSKey) string {
	switch {
	case kmsIsSymmetricEncryptionKey(key):
		return kmsAliasFamilySymmetric
	case len(kmsMACAlgorithmsByKeySpec[key.KeySpec]) > 0:
		return kmsAliasFamilyHMAC
	default:
		return kmsAliasFamilyAsymmetric
	}
}

// kmsCheckAliasTargetMatches reports whether UpdateAlias may move an alias from one key to another.
//
// The published restriction is two conditions in one sentence, and they are checked in the order it
// states them: *"The current and new KMS key must be the same type (both symmetric or both asymmetric or
// both HMAC), and they must have the same key usage."* Both are refused with `ValidationError`/400 and
// that is **substrate's reading**, not a citation: the operation's five published errors contain no
// type-mismatch entry, and `ValidationError` is where a malformed request lands for this plugin —
// [kmsUnknownKeySpec] and [kmsUnknownKeyUsage] are the precedents.
//
// Two keys of the same spec cannot differ in family, so the usage test is the one that fires for, say,
// two RSA_4096 keys created with different usages — which is the case the restriction's own justification
// is about, since an alias a caller signs with is not one it can encrypt with.
func kmsCheckAliasTargetMatches(aliasName string, current, next *KMSKey) *AWSError {
	if currentFamily, nextFamily := kmsAliasKeyFamily(current), kmsAliasKeyFamily(next); currentFamily != nextFamily {
		return kmsAliasTargetMismatch(aliasName, fmt.Sprintf(
			"the current key %s is %s and the new key %s is %s",
			current.KeyID, currentFamily, next.KeyID, nextFamily))
	}
	if current.KeyUsage != next.KeyUsage {
		return kmsAliasTargetMismatch(aliasName, fmt.Sprintf(
			"the current key %s has key usage %s and the new key %s has %s",
			current.KeyID, current.KeyUsage, next.KeyID, next.KeyUsage))
	}
	return nil
}
