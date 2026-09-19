package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #1085: the two KMS alias writers checked nothing, and every assertion here goes over the wire.
//
// The whole suite was green before the fix, which is the finding underneath this file: no test named an
// alias operation's refusal, so `UpdateAlias` creating an alias, pointing one at a key that does not
// exist, taking a key scheduled for deletion, and moving a symmetric alias to an RSA key were four
// behaviors nothing could see.
//
// Two traps the issue's own acceptance criteria walk into, both avoided here:
//
//   - **`ListAliases` cannot see what `UpdateAlias` used to do.** The handler never touched the
//     alias-names index, so an alias it fabricated was absent from `ListAliases` either way and the
//     criterion "ListAliases does not report the fabricated alias" passed before the fix as well as
//     after. Existence is therefore asserted through [kmsDescribeKeyID] — resolving the alias as a
//     `KeyId`, which reads the pointer the handler actually writes. `ListAliases` is asserted only where
//     the index itself is the subject, which is `CreateAlias`'s duplicate.
//   - **The key-state condition is asymmetric**, so "a key pending deletion is refused" is only half
//     true and a test asserting the half would pin a bug. See
//     TestKMSAlias_OnlyTheNewTargetIsStateChecked.
//
// See kms_alias_validate.go for which page publishes each refusal, and for why the three alias pages do
// not share one AliasName rule.

// kmsAliasNames reads the alias names ListAliases reports, in the order it reports them.
//
// Duplicates are preserved on purpose: the defect this catches is an index entry appended twice, which a
// set would hide, and `ListAliases` renders one response entry per index entry.
func kmsAliasNames(t *testing.T, ts *emulator.TestServer) []string {
	t.Helper()
	var out struct {
		Aliases []struct {
			AliasName   string `json:"AliasName"`
			TargetKeyID string `json:"TargetKeyId"`
		} `json:"Aliases"`
	}
	status, code := decodeAWSResponse(t,
		signedRequest(t, ts, kmsTarget, taggingTestAccount, "ListAliases", map[string]any{}), &out)
	require.Empty(t, code, "ListAliases")
	require.Equal(t, http.StatusOK, status, "ListAliases")

	names := make([]string, 0, len(out.Aliases))
	for _, alias := range out.Aliases {
		names = append(names, alias.AliasName)
	}
	return names
}

// kmsCountAliasName counts how many entries ListAliases reports for one name.
func kmsCountAliasName(t *testing.T, ts *emulator.TestServer, aliasName string) int {
	t.Helper()
	n := 0
	for _, name := range kmsAliasNames(t, ts) {
		if name == aliasName {
			n++
		}
	}
	return n
}

// kmsUpdateAlias re-points an alias and returns the status and error code.
func kmsUpdateAlias(t *testing.T, ts *emulator.TestServer, alias, targetKeyID string) (int, string) {
	t.Helper()
	var out map[string]any
	return decodeAWSResponse(t,
		signedRequest(t, ts, kmsTarget, taggingTestAccount, "UpdateAlias",
			map[string]any{"AliasName": alias, "TargetKeyId": targetKeyID}), &out)
}

// kmsScheduleKeyDeletion puts a key into PendingDeletion, which is the only refusable state substrate
// can hold — ScheduleKeyDeletion is the sole writer of anything but Enabled or Disabled.
func kmsScheduleKeyDeletion(t *testing.T, ts *emulator.TestServer, keyID string) {
	t.Helper()
	status, code := kmsCall(t, ts, "ScheduleKeyDeletion", map[string]any{"KeyId": keyID})
	require.Empty(t, code, "ScheduleKeyDeletion on %s", keyID)
	require.Equal(t, http.StatusOK, status, "ScheduleKeyDeletion on %s", keyID)
}

// TestKMSUpdateAlias_AnAbsentAliasIsRefusedAndCreatesNothing pins the first published sentence of
// API_UpdateAlias: "Associates an existing AWS KMS alias with a different KMS key."
//
// Before #1085 the handler wrote the pointer unconditionally, so this call succeeded and *created* the
// alias — doing CreateAlias's job without CreateAlias's name rules. Both halves are asserted, because the
// refusal alone would still pass against a handler that refused after writing.
func TestKMSUpdateAlias_AnAbsentAliasIsRefusedAndCreatesNothing(t *testing.T) {
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	status, code := kmsUpdateAlias(t, ts, "alias/never-created", keyID)
	assert.Equal(t, "NotFoundException", code, "UpdateAlias on an alias that does not exist")
	assert.Equal(t, http.StatusBadRequest, status, "UpdateAlias on an alias that does not exist")

	// The pointer, not the index: UpdateAlias never wrote the index, so ListAliases could not see the
	// alias it fabricated. DescribeKey resolves through the pointer the handler does write.
	resolved, describeCode := kmsDescribeKeyID(t, ts, kmsTarget, "alias/never-created")
	assert.Equal(t, "NotFoundException", describeCode, "DescribeKey through the refused alias")
	assert.Empty(t, resolved, "the refused alias resolves to no key")
	assert.NotContains(t, kmsAliasNames(t, ts), "alias/never-created", "ListAliases after the refusal")
}

// TestKMSUpdateAlias_ATargetNamingNoKeyIsRefused covers the dangling pointer, which was the gap with the
// longest reach: every later resolution of the alias failed, at a call that named neither the alias nor
// the operation that broke it.
//
// The alias is left pointing where it did, which is the half a refusal-only assertion would miss.
func TestKMSUpdateAlias_ATargetNamingNoKeyIsRefused(t *testing.T) {
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)
	require.Empty(t, mustCreateAlias(t, ts, "alias/app", keyID))

	for _, tc := range []struct {
		name   string
		target string
	}{
		{"a well-formed key ID no CreateKey minted", kmsAbsentKeyID},
		// "If you supply a null or empty string value, this operation returns an error." An empty
		// TargetKeyId reaches the same lookup as a bare key ID naming nothing, rather than a special case.
		{"an empty TargetKeyId", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code := kmsUpdateAlias(t, ts, "alias/app", tc.target)
			assert.Equal(t, "NotFoundException", code, "UpdateAlias to %s", tc.name)
			assert.Equal(t, http.StatusBadRequest, status, "UpdateAlias to %s", tc.name)

			resolved, describeCode := kmsDescribeKeyID(t, ts, kmsTarget, "alias/app")
			assert.Empty(t, describeCode, "DescribeKey through the alias after the refusal")
			assert.Equal(t, keyID, resolved, "the alias still points where it did")
		})
	}
}

// TestKMSCreateAlias_ATargetNamingNoKeyIsRefused is the same gap on the other writer, whose page says it
// outright: "A valid KMS key is required. You can't create an alias without a KMS key."
func TestKMSCreateAlias_ATargetNamingNoKeyIsRefused(t *testing.T) {
	ts := arnGuardServer(t)

	for _, tc := range []struct {
		name   string
		target string
	}{
		{"a well-formed key ID no CreateKey minted", kmsAbsentKeyID},
		{"an empty TargetKeyId", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code := kmsCreateAlias(t, ts, kmsTarget, "alias/"+strings.ReplaceAll(tc.name, " ", "-"), tc.target)
			assert.Equal(t, "NotFoundException", code, "CreateAlias to %s", tc.name)
			assert.Equal(t, http.StatusBadRequest, status, "CreateAlias to %s", tc.name)
			assert.Empty(t, kmsAliasNames(t, ts), "ListAliases records nothing for a refused create")
		})
	}
}

// TestKMSAlias_OnlyTheNewTargetIsStateChecked is the footnote the operation pages hide, asserted in both
// directions because one direction alone would pin the opposite behavior.
//
// The pages say only "The KMS key that you use for this operation must be in a compatible key state",
// which reads as one condition over "the key". The developer guide's key-state table resolves it into two:
// CreateAlias is footnote [3] and refuses a target pending deletion, while UpdateAlias is footnote [10] —
// "If the source KMS key is pending deletion, the command succeeds. If the destination KMS key is pending
// deletion, the command fails." So an alias whose *current* key is scheduled for deletion may still be
// re-pointed, which is exactly what a caller does to rescue it, and a single "check the key state" guard
// would break.
//
// Disabled is accepted at both: the table gives both operations a checkmark for it and neither page
// publishes DisabledException at all.
func TestKMSAlias_OnlyTheNewTargetIsStateChecked(t *testing.T) {
	t.Run("a new target pending deletion is refused at both writers", func(t *testing.T) {
		ts := arnGuardServer(t)
		_, live := createKMSKey(t, ts)
		_, doomed := createKMSKey(t, ts)
		kmsScheduleKeyDeletion(t, ts, doomed)

		status, code := kmsCreateAlias(t, ts, kmsTarget, "alias/fresh", doomed)
		assert.Equal(t, "KMSInvalidStateException", code, "CreateAlias onto a key pending deletion")
		assert.Equal(t, http.StatusBadRequest, status, "CreateAlias onto a key pending deletion")

		require.Empty(t, mustCreateAlias(t, ts, "alias/app", live))
		status, code = kmsUpdateAlias(t, ts, "alias/app", doomed)
		assert.Equal(t, "KMSInvalidStateException", code, "UpdateAlias onto a key pending deletion")
		assert.Equal(t, http.StatusBadRequest, status, "UpdateAlias onto a key pending deletion")

		resolved, describeCode := kmsDescribeKeyID(t, ts, kmsTarget, "alias/app")
		assert.Empty(t, describeCode, "DescribeKey through the alias after the refusal")
		assert.Equal(t, live, resolved, "the alias still points at the key it did")
	})

	t.Run("an alias whose current key is pending deletion can still be re-pointed", func(t *testing.T) {
		ts := arnGuardServer(t)
		_, doomed := createKMSKey(t, ts)
		_, rescue := createKMSKey(t, ts)
		require.Empty(t, mustCreateAlias(t, ts, "alias/app", doomed))
		kmsScheduleKeyDeletion(t, ts, doomed)

		status, code := kmsUpdateAlias(t, ts, "alias/app", rescue)
		assert.Empty(t, code, "UpdateAlias away from a key pending deletion")
		assert.Equal(t, http.StatusOK, status, "UpdateAlias away from a key pending deletion")

		resolved, describeCode := kmsDescribeKeyID(t, ts, kmsTarget, "alias/app")
		assert.Empty(t, describeCode, "DescribeKey through the re-pointed alias")
		assert.Equal(t, rescue, resolved, "the alias moved to the key that is not pending deletion")
	})

	t.Run("a disabled key is accepted at both writers", func(t *testing.T) {
		ts := arnGuardServer(t)
		_, enabled := createKMSKey(t, ts)
		_, disabled := createKMSKey(t, ts)
		status, code := kmsCall(t, ts, "DisableKey", map[string]any{"KeyId": disabled})
		require.Empty(t, code, "DisableKey")
		require.Equal(t, http.StatusOK, status, "DisableKey")

		status, code = kmsCreateAlias(t, ts, kmsTarget, "alias/onto-disabled", disabled)
		assert.Empty(t, code, "CreateAlias onto a disabled key")
		assert.Equal(t, http.StatusOK, status, "CreateAlias onto a disabled key")

		require.Empty(t, mustCreateAlias(t, ts, "alias/app", enabled))
		status, code = kmsUpdateAlias(t, ts, "alias/app", disabled)
		assert.Empty(t, code, "UpdateAlias onto a disabled key")
		assert.Equal(t, http.StatusOK, status, "UpdateAlias onto a disabled key")
	})
}

// TestKMSUpdateAlias_TheTypeMatchIsRefusedAndNamesItsArm pins the restriction AWS publishes twice
// verbatim and glosses itself: "The current and new KMS key must be the same type (both symmetric or both
// asymmetric or both HMAC), and they must have the same key usage. This restriction prevents errors in
// code that uses aliases."
//
// The message is asserted, not just the code, for the reason the restriction's own gloss gives: a caller
// whose deploy moved an alias between two key types needs to know *which* half it broke, and the code is
// substrate's reading rather than a published one, so it carries no distinction on its own.
//
// The three cases are the three families the sentence names, plus the usage arm — which needs two keys of
// the same spec, since two keys of one spec cannot differ in family.
func TestKMSUpdateAlias_TheTypeMatchIsRefusedAndNamesItsArm(t *testing.T) {
	for _, tc := range []struct {
		name            string
		currentSpec     string
		currentUsage    string
		nextSpec        string
		nextUsage       string
		messageContains []string
	}{
		{
			name:            "symmetric to asymmetric",
			currentSpec:     "SYMMETRIC_DEFAULT",
			currentUsage:    "ENCRYPT_DECRYPT",
			nextSpec:        "RSA_2048",
			nextUsage:       "ENCRYPT_DECRYPT",
			messageContains: []string{"is symmetric", "is asymmetric"},
		},
		{
			name:            "symmetric to HMAC",
			currentSpec:     "SYMMETRIC_DEFAULT",
			currentUsage:    "ENCRYPT_DECRYPT",
			nextSpec:        "HMAC_256",
			nextUsage:       "GENERATE_VERIFY_MAC",
			messageContains: []string{"is symmetric", "is HMAC"},
		},
		{
			// Same spec on both sides, so the family arm cannot fire and the usage arm is what is under
			// test. This is the case the restriction's justification is about: an alias a caller signs
			// with is not one it can encrypt with.
			name:            "one key usage to another at the same key spec",
			currentSpec:     "RSA_4096",
			currentUsage:    "ENCRYPT_DECRYPT",
			nextSpec:        "RSA_4096",
			nextUsage:       "SIGN_VERIFY",
			messageContains: []string{"has key usage ENCRYPT_DECRYPT", "has SIGN_VERIFY"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := arnGuardServer(t)
			_, current := createKMSKeySpecUsage(t, ts, tc.currentSpec, tc.currentUsage)
			_, next := createKMSKeySpecUsage(t, ts, tc.nextSpec, tc.nextUsage)
			require.Empty(t, mustCreateAlias(t, ts, "alias/app", current))

			status, code, message := kmsRefusal(t, ts, "UpdateAlias", map[string]any{
				"AliasName":   "alias/app",
				"TargetKeyId": next,
			})
			assert.Equal(t, "ValidationError", code, "UpdateAlias from %s", tc.name)
			assert.Equal(t, http.StatusBadRequest, status, "UpdateAlias from %s", tc.name)
			for _, want := range tc.messageContains {
				assert.Contains(t, message, want, "the refusal names which half of the restriction failed")
			}

			resolved, describeCode := kmsDescribeKeyID(t, ts, kmsTarget, "alias/app")
			assert.Empty(t, describeCode, "DescribeKey through the alias after the refusal")
			assert.Equal(t, current, resolved, "the alias still points at the key it did")
		})
	}
}

// TestKMSUpdateAlias_AMatchingTypeAndUsageIsAccepted is the guard against the refusal above being reached
// by something other than what it names.
//
// Without it, a handler refusing every UpdateAlias would satisfy all three cases. Two asymmetric keys of
// different specs are the sharper half: they are a different key spec and the same family and usage, which
// is what the restriction permits and a spec-equality check would wrongly refuse.
func TestKMSUpdateAlias_AMatchingTypeAndUsageIsAccepted(t *testing.T) {
	for _, tc := range []struct {
		name         string
		currentSpec  string
		currentUsage string
		nextSpec     string
		nextUsage    string
	}{
		{"two symmetric keys", "SYMMETRIC_DEFAULT", "ENCRYPT_DECRYPT", "SYMMETRIC_DEFAULT", "ENCRYPT_DECRYPT"},
		{"two asymmetric keys of different specs", "RSA_2048", "SIGN_VERIFY", "ECC_NIST_P256", "SIGN_VERIFY"},
		{"two HMAC keys of different specs", "HMAC_256", "GENERATE_VERIFY_MAC", "HMAC_512", "GENERATE_VERIFY_MAC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := arnGuardServer(t)
			_, current := createKMSKeySpecUsage(t, ts, tc.currentSpec, tc.currentUsage)
			_, next := createKMSKeySpecUsage(t, ts, tc.nextSpec, tc.nextUsage)
			require.Empty(t, mustCreateAlias(t, ts, "alias/app", current))

			status, code := kmsUpdateAlias(t, ts, "alias/app", next)
			assert.Empty(t, code, "UpdateAlias between %s", tc.name)
			assert.Equal(t, http.StatusOK, status, "UpdateAlias between %s", tc.name)

			resolved, describeCode := kmsDescribeKeyID(t, ts, kmsTarget, "alias/app")
			assert.Empty(t, describeCode, "DescribeKey through the re-pointed alias")
			assert.Equal(t, next, resolved, "the alias points at the new key")
		})
	}
}

// TestKMSCreateAlias_TheNameRulesAreTheOnesItsOwnPagePublishes covers the three name rules on the one
// alias operation that publishes a code for each, and the codes are not interchangeable: the length bound
// is LimitExceededException and the pattern and the reserved prefix are InvalidAliasNameException, which
// API_CreateAlias publishes alone among the three alias pages.
//
// The bare name is the case substrate used to *rewrite*: `alias/` was prepended, so "app" became
// "alias/app" and a request AWS refuses was accepted as a different one. UpdateAlias needs no row here —
// no alias failing this pattern can exist, so its existence check answers a malformed name with the
// NotFoundException its page does publish, and nothing is borrowed across pages (#671).
func TestKMSCreateAlias_TheNameRulesAreTheOnesItsOwnPagePublishes(t *testing.T) {
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	for _, tc := range []struct {
		name  string
		alias string
		code  string
	}{
		{"a bare name, which used to be rewritten", "app", "InvalidAliasNameException"},
		{"the prefix with nothing after it", "alias/", "InvalidAliasNameException"},
		{"an empty name", "", "InvalidAliasNameException"},
		{"a space, which the pattern excludes", "alias/my app", "InvalidAliasNameException"},
		{"a dot, which the pattern excludes", "alias/my.app", "InvalidAliasNameException"},
		{"a colon, which only DeleteAlias's pattern admits", "alias/my:app", "InvalidAliasNameException"},
		// "The alias name cannot begin with alias/aws/. The alias/aws/ prefix is reserved for AWS managed
		// keys." Substrate mints no AWS-managed alias, so refusing it here is the whole of the rule.
		{"the reserved AWS-managed prefix", "alias/aws/s3", "InvalidAliasNameException"},
		{"a name past the published maximum", "alias/" + strings.Repeat("a", 251), "LimitExceededException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code := kmsCreateAlias(t, ts, kmsTarget, tc.alias, keyID)
			assert.Equal(t, tc.code, code, "CreateAlias with %s", tc.name)
			assert.Equal(t, http.StatusBadRequest, status, "CreateAlias with %s", tc.name)
			assert.Empty(t, kmsAliasNames(t, ts), "ListAliases records nothing for a refused name")
		})
	}

	// The longest name the bound admits, to fix which side of it is inclusive.
	status, code := kmsCreateAlias(t, ts, kmsTarget, "alias/"+strings.Repeat("a", 250), keyID)
	assert.Empty(t, code, "CreateAlias with a name of exactly the published maximum")
	assert.Equal(t, http.StatusOK, status, "CreateAlias with a name of exactly the published maximum")
}

// TestKMSUpdateAlias_AnOverLongNameIsRefusedWithTheLengthCode is UpdateAlias's one published name code.
//
// It is separate from the table above because it is the only name rule the two operations share: the bound
// and LimitExceededException are published on both pages, while InvalidAliasNameException is not. A name
// this long cannot name an existing alias either, so the assertion is that the *length* code wins over the
// NotFoundException the lookup would otherwise give — the order the checks run in, made observable.
func TestKMSUpdateAlias_AnOverLongNameIsRefusedWithTheLengthCode(t *testing.T) {
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	status, code, message := kmsRefusal(t, ts, "UpdateAlias", map[string]any{
		"AliasName":   "alias/" + strings.Repeat("a", 251),
		"TargetKeyId": keyID,
	})
	assert.Equal(t, "LimitExceededException", code, "UpdateAlias with a name past the published maximum")
	assert.Equal(t, http.StatusBadRequest, status, "UpdateAlias with a name past the published maximum")
	assert.Contains(t, message, "at most 256 characters", "the refusal names the bound it enforced")
}

// TestKMSCreateAlias_ASecondCreateIsRefusedAndTheIndexHoldsOneEntry covers the adjacent defect #1085
// absorbed: the handler appended to the alias-names index with neither a dedup nor an existence check.
//
// Two things were wrong and only one is a code. "The alias must be unique in the account and Region" and
// AlreadyExistsException is published for it — and separately, ListAliases reported the name twice,
// because the index and the pointer are two state keys and only the pointer is idempotent. Both are
// asserted, and the second is the one only ListAliases can see.
func TestKMSCreateAlias_ASecondCreateIsRefusedAndTheIndexHoldsOneEntry(t *testing.T) {
	ts := arnGuardServer(t)
	_, first := createKMSKey(t, ts)
	_, second := createKMSKey(t, ts)
	require.Empty(t, mustCreateAlias(t, ts, "alias/app", first))

	for _, tc := range []struct {
		name   string
		target string
	}{
		{"the same key", first},
		{"a different key", second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code := kmsCreateAlias(t, ts, kmsTarget, "alias/app", tc.target)
			assert.Equal(t, "AlreadyExistsException", code, "a second CreateAlias naming %s", tc.name)
			assert.Equal(t, http.StatusBadRequest, status, "a second CreateAlias naming %s", tc.name)
		})
	}

	assert.Equal(t, 1, kmsCountAliasName(t, ts, "alias/app"), "ListAliases reports the alias once")
	resolved, describeCode := kmsDescribeKeyID(t, ts, kmsTarget, "alias/app")
	assert.Empty(t, describeCode, "DescribeKey through the alias")
	assert.Equal(t, first, resolved, "a refused create did not move the alias")

	// "you can have aliases with the same name in different Regions" — so the refusal is scoped to the
	// account and Region, not to the name.
	_, west := createKMSKeyIn(t, ts, kmsWest2Target)
	status, code := kmsCreateAlias(t, ts, kmsWest2Target, "alias/app", west)
	assert.Empty(t, code, "the same alias name in another Region")
	assert.Equal(t, http.StatusOK, status, "the same alias name in another Region")
}

// TestKMSDeleteAlias_StillAcceptsAName_WithoutThePrefix pins the split #1085 made between the writers,
// which is the one place the three pages' disagreement is visible over the wire.
//
// API_DeleteAlias publishes `^[a-zA-Z0-9:/_-]+$` — no required prefix, and a colon permitted — where
// CreateAlias and UpdateAlias publish `^alias/[a-zA-Z0-9/_-]+$`. Its prose still says the name "must begin
// with alias/", so the page contradicts itself; substrate takes the machine-readable half, which means the
// prepend stays here after being removed from the other two. Without this test the split reads as an
// oversight rather than a reading of the pages.
func TestKMSDeleteAlias_StillAcceptsAName_WithoutThePrefix(t *testing.T) {
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)
	require.Empty(t, mustCreateAlias(t, ts, "alias/app", keyID))

	status, code := kmsCall(t, ts, "DeleteAlias", map[string]any{"AliasName": "app"})
	assert.Empty(t, code, "DeleteAlias with the prefix omitted")
	assert.Equal(t, http.StatusOK, status, "DeleteAlias with the prefix omitted")

	resolved, describeCode := kmsDescribeKeyID(t, ts, kmsTarget, "alias/app")
	assert.Equal(t, "NotFoundException", describeCode, "DescribeKey through the deleted alias")
	assert.Empty(t, resolved, "the deleted alias resolves to no key")
	assert.Empty(t, kmsAliasNames(t, ts), "ListAliases after the delete")
}

// mustCreateAlias creates an alias a test needs in place before the behavior it is about, and returns the
// error code so the caller can require it empty.
//
// It returns rather than asserts because every caller wants the same require at the top of its own body,
// and a helper that failed the test itself would report the failure against this file's line.
func mustCreateAlias(t *testing.T, ts *emulator.TestServer, alias, targetKeyID string) string {
	t.Helper()
	status, code := kmsCreateAlias(t, ts, kmsTarget, alias, targetKeyID)
	if code == "" && status != http.StatusOK {
		return "CreateAlias answered " + http.StatusText(status)
	}
	return code
}
