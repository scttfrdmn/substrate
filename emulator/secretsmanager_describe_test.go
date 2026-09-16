package emulator_test

import (
	"encoding/json"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// DescribeSecret's response members, and the status every refusal that names an absent secret carries
// — #929 and #930.
//
// Two things are asserted here that no existing test could assert:
//
//  1. **A member's absence, on the raw bytes.** AWS's rule is one sentence on API_DescribeSecret —
//     "Secrets Manager only returns fields that have a value in the response" — and the difference
//     between an absent member, a null one and a zero-valued one is exactly what a decoded struct
//     destroys. Every assertion below therefore runs over map[string]json.RawMessage, in which an
//     absent member has no key and a null one has the four bytes "null".
//
//  2. **The status of ResourceNotFoundException, at all nine call sites at once.** Substrate answered
//     404, which Secrets Manager publishes nowhere: API_DescribeSecret's error list is three codes
//     long and gives it 400, and every other SecretId-taking operation publishes the same. That is the
//     defect ACM carried at one site (#921) and KMS at fifteen (#923), and the table below is what
//     stops a tenth operation from disagreeing with the reference page.
//
// The nine are enumerated rather than sampled because the fix is one helper — [smSecretNotFound] — and
// a table over every caller is the only thing that proves no handler builds its own refusal instead.

// smDescribeMembers reads DescribeSecret's response as its raw members, keyed by name.
//
// json.RawMessage rather than any: an absent member is a missing key and a null member is the literal
// "null", which is the distinction both AWS tiers on this operation turn on and the one a struct or an
// any-valued map cannot represent.
func smDescribeMembers(t *testing.T, ts *emulator.TestServer, secretID string) map[string]json.RawMessage {
	t.Helper()
	status, body, code := smRawCall(t, ts, smTarget, "DescribeSecret", map[string]any{"SecretId": secretID})
	require.Empty(t, code, "DescribeSecret %s", secretID)
	require.Equal(t, http.StatusOK, status, "DescribeSecret %s", secretID)
	out := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal([]byte(body), &out), "decode DescribeSecret %s", secretID)
	return out
}

// smMemberNames returns a member set's names, sorted, for a whole-set assertion.
func smMemberNames(members map[string]json.RawMessage) []string {
	names := make([]string, 0, len(members))
	for name := range members {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// smSecretIDOperations is every operation that takes a SecretId, with a body naming an absent secret.
//
// All nine resolve the identifier, load the record and refuse through [smSecretNotFound] before
// reading any other parameter, which is why each body carries only what its shape requires.
var smSecretIDOperations = []struct {
	name string
	body func(secretID string) map[string]any
}{
	{"GetSecretValue", func(id string) map[string]any { return map[string]any{"SecretId": id} }},
	{"PutSecretValue", func(id string) map[string]any {
		return map[string]any{"SecretId": id, "SecretString": "v"}
	}},
	{"DescribeSecret", func(id string) map[string]any { return map[string]any{"SecretId": id} }},
	{"UpdateSecret", func(id string) map[string]any {
		return map[string]any{"SecretId": id, "Description": "d"}
	}},
	{"DeleteSecret", func(id string) map[string]any { return map[string]any{"SecretId": id} }},
	{"ListSecretVersionIds", func(id string) map[string]any { return map[string]any{"SecretId": id} }},
	{"TagResource", func(id string) map[string]any {
		return map[string]any{"SecretId": id, "Tags": []map[string]string{{"Key": "env", "Value": "prod"}}}
	}},
	{"UntagResource", func(id string) map[string]any {
		return map[string]any{"SecretId": id, "TagKeys": []string{"env"}}
	}},
	{"RotateSecret", func(id string) map[string]any { return map[string]any{"SecretId": id} }},
}

func TestSMErrorStatus_AnAbsentSecretIsAlways400(t *testing.T) {
	ts := smTagServer(t)

	// A well-formed ARN in the caller's own account, so nothing here is refused by the parser: the
	// identifier is fine and the secret simply does not exist.
	absent := "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":secret:no-such-secret"

	for _, op := range smSecretIDOperations {
		t.Run(op.name, func(t *testing.T) {
			status, _, code := smRawCall(t, ts, smTarget, op.name, op.body(absent))
			assert.Equal(t, "ResourceNotFoundException", code,
				"%s reports the secret absent under the code AWS publishes", op.name)
			assert.Equal(t, http.StatusBadRequest, status,
				"%s answers 400, the status API_%s publishes for it; substrate answered 404 until #930",
				op.name, op.name)
		})
	}
}

func TestSMErrorStatus_ABareNameNamingNoSecretIsAlso400(t *testing.T) {
	ts := smTagServer(t)

	// The name path is the one identifier that legitimately reads the caller's own account, so it
	// reaches [smSecretNotFound] by a different route than an ARN does and is asserted separately.
	status, _, code := smRawCall(t, ts, smTarget, "DescribeSecret", map[string]any{"SecretId": "no-such-secret"})
	assert.Equal(t, "ResourceNotFoundException", code)
	assert.Equal(t, http.StatusBadRequest, status)
}

func TestSMDescribeMembers_ASecretWithNoDescriptionOrKeyOmitsBoth(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "bare", nil)

	members := smDescribeMembers(t, ts, arn)

	// KmsKeyId's omission is AWS's own statement — "If the secret is encrypted with the AWS managed key
	// aws/secretsmanager, this field is omitted" — and a secret substrate created without a CMK is
	// exactly that case.
	assert.NotContains(t, members, "KmsKeyId")
	// Description's omission rests on the blanket sentence alone; its own per-member text says nothing
	// about omission, so this is substrate's reading rather than AWS's, per the correction on #930.
	assert.NotContains(t, members, "Description")
	assert.NotContains(t, members, "Tags")
}

func TestSMDescribeMembers_ADescriptionAndKeyAreReportedWhenPresent(t *testing.T) {
	ts := smTagServer(t)

	// The other half of omission: a member with a value must still be emitted, or "omit what has no
	// value" would have been satisfied by emitting nothing at all.
	status, errCode := decodeAWSResponse(t, signedRequest(t, ts, smTarget, taggingTestAccount, "CreateSecret",
		map[string]any{
			"Name":         "furnished",
			"SecretString": "value",
			"Description":  "the production database password",
			"KmsKeyId":     "arn:aws:kms:us-east-1:" + taggingTestAccount + ":key/11111111-2222-3333-4444-555555555555",
		}), nil)
	require.Empty(t, errCode, "CreateSecret furnished")
	require.Equal(t, http.StatusOK, status)

	members := smDescribeMembers(t, ts, "furnished")
	assert.JSONEq(t, `"the production database password"`, string(members["Description"]))
	assert.JSONEq(t,
		`"arn:aws:kms:us-east-1:`+taggingTestAccount+`:key/11111111-2222-3333-4444-555555555555"`,
		string(members["KmsKeyId"]))
}

func TestSMDescribeMembers_RotationEnabledIsNullUntilRotationIsRequested(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "rotatable", nil)

	// AWS publishes a null here rather than an omission — "If the secret has never been configured for
	// rotation, Secrets Manager returns null" — which is a different treatment from KmsKeyId's on the
	// same page, and the reason this member is emitted unconditionally. The key must be present and its
	// value must be the four bytes "null"; false would be a claim AWS does not make.
	before := smDescribeMembers(t, ts, arn)
	require.Contains(t, before, "RotationEnabled", "the member is present, not omitted")
	assert.Equal(t, "null", string(before["RotationEnabled"]))

	status, _, code := smRawCall(t, ts, smTarget, "RotateSecret", map[string]any{"SecretId": arn})
	require.Empty(t, code, "RotateSecret")
	require.Equal(t, http.StatusOK, status)

	after := smDescribeMembers(t, ts, arn)
	assert.Equal(t, "true", string(after["RotationEnabled"]),
		"and rotation having been requested is the only thing that turns it true")
}

func TestSMDescribeMembers_TheReportedSetIsExactlyWhatSubstrateHasAValueFor(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "whole-set", nil)

	// A whole-set assertion rather than a handful of absences, because the defect being pinned is a
	// member reported with no value behind it — and only an exact set catches the next one added.
	// RotationEnabled is in the set as AWS's documented null; everything AWS documents as omitted, and
	// everything substrate models nothing of, is out of it.
	assert.Equal(t,
		[]string{"ARN", "CreatedDate", "LastChangedDate", "Name", "RotationEnabled"},
		smMemberNames(smDescribeMembers(t, ts, arn)))
}

func TestSMDescribeMembers_TheUnmodeledPublishedMembersStayAbsent(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "unmodeled", map[string]string{"env": "prod"})

	members := smDescribeMembers(t, ts, arn)

	// API_DescribeSecret's Response Syntax has twenty-one members. These are the ones substrate holds
	// no state for, and their absence is correct under the same sentence rather than a gap — asserted so
	// that modeling one later has to come with a decision about how it is reported. DeletedDate,
	// LastAccessedDate and RotationRules carry AWS's explicit "this field is omitted"; LastRotatedDate
	// and NextRotationDate carry its "returns null", so if either is ever modeled it belongs with
	// RotationEnabled rather than here.
	for _, member := range []string{
		"DeletedDate", "ExternalSecretRotationMetadata", "ExternalSecretRotationRoleArn",
		"LastAccessedDate", "LastRotatedDate", "NextRotationDate", "OwningService", "PrimaryRegion",
		"ReplicationStatus", "RotationLambdaARN", "RotationRules", "Type", "VersionIdsToStages",
	} {
		assert.NotContains(t, members, member,
			"%s is a published member substrate holds no value for, so it is not reported", member)
	}
}

func TestSMDescribeMembers_ListTagsForResourceIsNotAnOperation(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "tagged", map[string]string{"env": "prod"})

	// Secrets Manager publishes twenty-three operations and ListTagsForResource is not among them;
	// substrate answered one until #929. A caller reaching for it now gets what real AWS gives for a
	// name the service does not have, which is the whole point of removing it rather than leaving a
	// working operation nobody can call against AWS.
	status, _, code := smRawCall(t, ts, smTarget, "ListTagsForResource", map[string]any{"SecretId": arn})
	assert.Equal(t, "UnknownOperationException", code)
	assert.Equal(t, http.StatusNotFound, status)

	// And the tags are still readable, through the operation AWS does publish for it.
	assert.Equal(t, map[string]string{"env": "prod"}, smTagMap(t, ts, arn))
}
