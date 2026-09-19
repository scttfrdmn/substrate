package emulator_test

// What CreateStateMachine and CreateActivity refuse, and what they answer for a repeat (#1072).
//
// Three divergences, on both handlers:
//
//  1. An absent name answered InvalidParameterException, a code neither page publishes —
//     API_CreateStateMachine lists fifteen errors and API_CreateActivity seven, and it is among
//     neither. The published code is InvalidName, and the published constraint list it enforces (a
//     character list and a 1–80 bound) was not checked at all.
//  2. A name already taken answered 409, where both pages publish 400.
//  3. Both pages publish their operation as idempotent, and substrate refused unconditionally.
//
// Plus two members on CreateStateMachine that are published as constrained and were not checked:
// roleArn (Required: Yes, answering InvalidArn) and type (Valid Values STANDARD | EXPRESS, answering
// StateMachineTypeNotSupported).
//
// Every assertion here checks the status as well as the code, because the status is half of what was
// wrong: no test in the tree pinned it, which is why a 409 survived #910's and #912's sweeps.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

const (
	// sfnCreateRole is a well-formed role ARN, so a test asserting something other than roleArn is not
	// accidentally asserting roleArn.
	sfnCreateRole = "arn:aws:iam::" + taggingTestAccount + ":role/StepFunctionsRole"

	// sfnCreateNameMax is the published maximum name length, restated here because the constant it
	// mirrors is unexported.
	sfnCreateNameMax = 80
)

// sfnCreateSM posts one CreateStateMachine and returns its status, error code and raw body, filling
// in the members the test under way is not the subject of.
func sfnCreateSM(t *testing.T, ts *emulator.TestServer, body map[string]any) (int, string, string) {
	t.Helper()
	full := map[string]any{"definition": sfnArnPassDefinition, "roleArn": sfnCreateRole}
	for k, v := range body {
		full[k] = v
	}
	return sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, "CreateStateMachine", full)
}

// sfnCreateRefused asserts one create is refused with a published code at a published status.
func sfnCreateRefused(t *testing.T, ts *emulator.TestServer, op string, body map[string]any,
	wantCode string,
) {
	t.Helper()
	var status int
	var errCode, raw string
	if op == "CreateStateMachine" {
		status, errCode, raw = sfnCreateSM(t, ts, body)
	} else {
		status, errCode, raw = sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion, op, body)
	}
	assert.Equalf(t, wantCode, errCode, "%s: %s", op, raw)
	assert.Equalf(t, http.StatusBadRequest, status, "%s: %s", op, raw)
	// The code that was answered before #1072 is on neither create page's Errors list, so no path may
	// report it again.
	assert.NotContainsf(t, raw, "InvalidParameterException", "%s: %s", op, raw)
}

// TestSFNCreate_AnAbsentNameAnswersInvalidName covers the code both handlers had wrong.
//
// InvalidName is the only one of the two citable codes published on *both* pages: CreateStateMachine
// also publishes ValidationException and CreateActivity does not, so answering that would make one
// plugin report two codes for one failure and report an unpublished one on CreateActivity.
func TestSFNCreate_AnAbsentNameAnswersInvalidName(t *testing.T) {
	ts := sfnArnServer(t)

	sfnCreateRefused(t, ts, "CreateStateMachine", map[string]any{"name": ""}, "InvalidName")
	sfnCreateRefused(t, ts, "CreateActivity", map[string]any{"name": ""}, "InvalidName")
}

// TestSFNCreate_ThePublishedNameConstraintsAreEnforced walks the list both pages publish verbatim.
//
// One case per clause, because the clauses are separate sentences on the page and a validator that
// caught only the brackets would still pass a test that sent only a bracket. The two non-printing
// cases name their code point rather than carrying the byte, so the table stays readable and an
// editor cannot silently normalise the case away.
//
// The surrogate clause (`U+D800-DFFF`) has no case here, and that is a property of the wire rather
// than a gap: Go's JSON decoder substitutes U+FFFD for an unpaired surrogate escape, so there is no
// input a caller could send that would reach the check — see `sfnValidateName`.
func TestSFNCreate_ThePublishedNameConstraintsAreEnforced(t *testing.T) {
	for label, name := range map[string]string{
		"white space":           "two words",
		"an angle bracket":      "sm<name",
		"a curly bracket":       "sm{name}",
		"a square bracket":      "sm[0]",
		"a wildcard":            "sm*",
		"a question mark":       "sm?",
		"a special character":   "sm/name",
		"a colon":               "sm:name",
		"a control character":   "sm" + string(rune(0x01)) + "name",
		"a noncharacter":        "sm" + string(rune(0xFFFE)) + "name",
		"the 81st character":    strings.Repeat("a", sfnCreateNameMax+1),
		"an invalid code point": "sm\U0010ffff",
	} {
		t.Run(label, func(t *testing.T) {
			ts := sfnArnServer(t)
			sfnCreateRefused(t, ts, "CreateStateMachine", map[string]any{"name": name}, "InvalidName")
			sfnCreateRefused(t, ts, "CreateActivity", map[string]any{"name": name}, "InvalidName")
		})
	}
}

// TestSFNCreate_AnEightyCharacterNameWithTheLoggingSetIsAccepted is the other side of the bound, so
// the validator is known not to refuse what the page permits.
//
// Eighty is the published maximum rather than one under it, and `-` and `_` are the two punctuation
// characters the page's CloudWatch Logs sentence names as safe. That sentence is a condition on
// logging stated with "should", not a constraint on the name, so it must not become one — and a name
// built only from the characters it names has to pass either way.
func TestSFNCreate_AnEightyCharacterNameWithTheLoggingSetIsAccepted(t *testing.T) {
	ts := sfnArnServer(t)
	name := "sm-Name_9" + strings.Repeat("x", sfnCreateNameMax-len("sm-Name_9"))
	require.Len(t, name, sfnCreateNameMax)

	sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, name, "STANDARD", sfnArnPassDefinition)
	sfnArnCreateActivity(t, ts, taggingTestAccount, sfnArnEastRegion, name)
}

// TestSFNCreateStateMachine_ARoleArnIsRequiredAndIsAnArn covers the required member nothing checked.
//
// roleArn is Required: Yes with a 1–256 bound, and substrate stored whatever arrived — so a state
// machine could exist with no role at all. InvalidArn is published on this page. The over-256 case
// asserts the published bound; the "not an ARN" case is the narrowest reading of the code, since the
// page publishes no Pattern for the member.
func TestSFNCreateStateMachine_ARoleArnIsRequiredAndIsAnArn(t *testing.T) {
	for label, roleArn := range map[string]string{
		"absent":         "",
		"not an ARN":     "StepFunctionsRole",
		"over 256 chars": "arn:aws:iam::" + taggingTestAccount + ":role/" + strings.Repeat("r", 256),
	} {
		t.Run(label, func(t *testing.T) {
			ts := sfnArnServer(t)
			sfnCreateRefused(t, ts, "CreateStateMachine",
				map[string]any{"name": "sm-role", "roleArn": roleArn}, "InvalidArn")
		})
	}
}

// TestSFNCreateStateMachine_AnUnpublishedTypeIsNotSupported covers the third unchecked constraint.
//
// The page publishes `Valid Values: STANDARD | EXPRESS`, and substrate stored any string — leaving a
// record whose type DescribeStateMachine would report as a value AWS does not publish, and leaving
// the execution path branching on it undecided. StateMachineTypeNotSupported is published here for
// exactly this, and the lowercase case is the one a consumer is most likely to send.
func TestSFNCreateStateMachine_AnUnpublishedTypeIsNotSupported(t *testing.T) {
	ts := sfnArnServer(t)
	sfnCreateRefused(t, ts, "CreateStateMachine",
		map[string]any{"name": "sm-type", "type": "standard"}, "StateMachineTypeNotSupported")
	sfnCreateRefused(t, ts, "CreateStateMachine",
		map[string]any{"name": "sm-type", "type": "SYNC"}, "StateMachineTypeNotSupported")
}

// TestSFNCreateStateMachine_AnAbsentTypeIsTheDefaultRatherThanUnsupported keeps the default from
// being refused by the check that was just added.
//
// "Determines whether a Standard or Express state machine is created. The default is STANDARD" — so
// an absent type is applied before the Valid Values are consulted, and DescribeStateMachine reports
// the default rather than an empty string.
func TestSFNCreateStateMachine_AnAbsentTypeIsTheDefaultRatherThanUnsupported(t *testing.T) {
	ts := sfnArnServer(t)
	status, errCode, raw := sfnCreateSM(t, ts, map[string]any{"name": "sm-default-type"})
	require.Emptyf(t, errCode, "CreateStateMachine: %s", raw)
	require.Equal(t, http.StatusOK, status, raw)

	described := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": sfnArnMember(t, raw, "stateMachineArn")})
	assert.Equal(t, "STANDARD", sfnArnMember(t, described, "type"), described)
}

// TestSFNCreateStateMachine_ARepeatMatchingThePublishedCheckIsIdempotent is the published Note.
//
// "CreateStateMachine is an idempotent API. Subsequent requests won't create a duplicate resource if
// it was already created." The check is on name, definition, type and five members substrate does not
// model, so a repeat matching those three answers the first call's own response — the same ARN and
// the same creationDate, which is what shows nothing was re-created.
func TestSFNCreateStateMachine_ARepeatMatchingThePublishedCheckIsIdempotent(t *testing.T) {
	ts := sfnArnServer(t)

	first := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "CreateStateMachine", map[string]any{
		"name": "sm-idem", "definition": sfnArnPassDefinition, "roleArn": sfnCreateRole,
		"type": "STANDARD",
	})
	second := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "CreateStateMachine", map[string]any{
		"name": "sm-idem", "definition": sfnArnPassDefinition, "roleArn": sfnCreateRole,
		"type": "STANDARD",
	})
	assert.JSONEq(t, first, second, "an idempotent repeat answers the first call's own response")
}

// TestSFNCreateStateMachine_ARepeatDifferingOnlyInRoleArnIsStillIdempotent is where the page
// contradicts itself and the Note wins.
//
// StateMachineAlreadyExists is glossed "A state machine with the same name but a different definition
// **or role ARN** already exists", while the Note says a differing roleArn is ignored and "roleArn
// and tags will not be updated, even if they are different". The Note is the more specific statement
// — it enumerates the check's inputs and excludes roleArn twice — so this is a success, and
// DescribeStateMachine must still report the *first* role.
func TestSFNCreateStateMachine_ARepeatDifferingOnlyInRoleArnIsStillIdempotent(t *testing.T) {
	ts := sfnArnServer(t)
	smARN := sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "sm-idem-role", "STANDARD",
		sfnArnPassDefinition)

	repeat := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "CreateStateMachine", map[string]any{
		"name": "sm-idem-role", "definition": sfnArnPassDefinition, "type": "STANDARD",
		"roleArn": "arn:aws:iam::" + taggingTestAccount + ":role/SomeOtherRole",
	})
	assert.Equal(t, smARN, sfnArnMember(t, repeat, "stateMachineArn"), repeat)

	described := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "DescribeStateMachine",
		map[string]any{"stateMachineArn": smARN})
	assert.Equal(t, "arn:aws:iam::"+taggingTestAccount+":role/StepFunctionsRole",
		sfnArnMember(t, described, "roleArn"), "roleArn is not updated by an idempotent repeat")
}

// TestSFNCreateStateMachine_ARealCollisionAnswers400 is the status half of the fix.
//
// The page publishes StateMachineAlreadyExists at **HTTP 400** and substrate answered 409 — the
// #910/#912 class one step further along, missed by those sweeps because it was a 409 rather than a
// 404. A definition difference and a type difference are both genuine collisions under the published
// check.
func TestSFNCreateStateMachine_ARealCollisionAnswers400(t *testing.T) {
	for label, body := range map[string]map[string]any{
		"a different definition": {"definition": sfnArnDefinition("changed"), "type": "STANDARD"},
		"a different type":       {"definition": sfnArnPassDefinition, "type": "EXPRESS"},
	} {
		t.Run(label, func(t *testing.T) {
			ts := sfnArnServer(t)
			sfnArnCreateSM(t, ts, taggingTestAccount, sfnArnEastRegion, "sm-clash", "STANDARD",
				sfnArnPassDefinition)

			body["name"] = "sm-clash"
			body["roleArn"] = sfnCreateRole
			status, errCode, raw := sfnArnCall(t, ts, taggingTestAccount, sfnArnEastRegion,
				"CreateStateMachine", body)
			assert.Equal(t, "StateMachineAlreadyExists", errCode, raw)
			assert.Equal(t, http.StatusBadRequest, status, raw)
			assert.NotEqual(t, http.StatusConflict, status, "409 is not a status this page publishes")
			assert.Contains(t, raw,
				"A state machine with the same name but a different definition or role ARN already exists",
				raw)
		})
	}
}

// TestSFNCreateActivity_ARepeatAnswersTheExistingActivity covers the narrower published check.
//
// "CreateActivity's idempotency check is based on the activity name" — nothing else, since the only
// other member is tags, which the Note says are ignored. So every second create against an existing
// name is a success, and the first call's tags survive it.
//
// This is what makes ActivityAlreadyExists unreachable in substrate rather than merely restatused:
// the page's one published condition for it is "EncryptionConfiguration may not be updated", and
// encryptionConfiguration is a member this plugin does not decode.
func TestSFNCreateActivity_ARepeatAnswersTheExistingActivity(t *testing.T) {
	ts := sfnArnServer(t)

	first := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "CreateActivity", map[string]any{
		"name": "act-idem",
		"tags": []map[string]string{{"key": "env", "value": "first"}},
	})
	second := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "CreateActivity", map[string]any{
		"name": "act-idem",
		"tags": []map[string]string{{"key": "env", "value": "second"}},
	})
	assert.JSONEq(t, first, second, "an idempotent repeat answers the first call's own response")

	listed := sfnArnOK(t, ts, taggingTestAccount, sfnArnEastRegion, "ListTagsForResource",
		map[string]any{"resourceArn": sfnArnMember(t, first, "activityArn")})
	var tags struct {
		Tags []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"tags"`
	}
	require.NoError(t, json.Unmarshal([]byte(listed), &tags), listed)
	require.Len(t, tags.Tags, 1, listed)
	assert.Equal(t, "first", tags.Tags[0].Value, "tags are not updated by an idempotent repeat")
}
