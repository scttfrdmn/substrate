package emulator_test

// Systems Manager tagging: the resource type an identifier names, and the tagging API's eighth row of
// #835.
//
// Four defects compounded here, and the first is the one this file exists for. All three of Systems
// Manager's tag operations declared `ResourceType string` and never read it, so every tag call was
// answered as though it named a Parameter Store parameter. AWS publishes ResourceType as
// Required: Yes over a ten-value enum, so {"ResourceType": "Document", "ResourceId": "MyRunbook"}
// tagged the *parameter* named "/MyRunbook" — and ListTagsForResource with the same pair read the tag
// back, so the round trip confirmed that the wrong resource had been tagged. RemoveTagsFromResource is
// the damaging direction: it stripped tags from a parameter a caller was not addressing and answered
// 200. InvalidResourceType, which AWS publishes at all three operations, was unreachable.
//
// This is the resource-*type* counterpart of the account confusion #826 established for SQS and DynamoDB
// and #910, #918, #922, #925 and #928 carried through Step Functions, CloudFront, KMS, SNS and Secrets
// Manager. It is also the first of #835's rows where the owning service's own tag operations do not
// take an ARN at all — AWS documents a Parameter ResourceId as the parameter *name* — so the two APIs
// take different identifier forms and still have to agree about which record they address. What makes
// them agree is a shared key builder, not a shared parameter list, which is what the cross-readability
// assertions below check.
//
// Second, anything without a leading "/" was normalized into a name, so a full ARN became the *name*
// "/arn:aws:ssm:...". Third, AddTagsToResource merged through a Go map and appended without sorting,
// so two identical calls could store two orders — the defect #862 fixed in the tagging API's own
// helpers and missed here. Fourth, ListTagsForResource rendered a nil Tags slice as "TagList": null,
// which is not the array AWS publishes.
//
// Every assertion goes through signed wire calls against emulator.StartTestServer; nothing writes
// state directly, per #765's rule that a helper writing state cannot prove a value is readable through
// the owning service's own call. Tags are read back through ListTagsForResource, which — unlike
// Secrets Manager's invented one (#929) — is an operation Systems Manager genuinely publishes.

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Wire details a real SDK would send. Two Regions are needed because a parameter is Region-scoped and
// its ARN carries the Region, so a cross-Region assertion needs a parameter that genuinely exists
// somewhere else; the parser takes the Region off the Host, which is why these two differ only there.
var (
	ssmTagTarget      = signedRequestTarget{host: "ssm.us-east-1.amazonaws.com", target: "AmazonSSM", signingName: "ssm"}
	ssmTagWest2Target = signedRequestTarget{host: "ssm.us-west-2.amazonaws.com", target: "AmazonSSM", signingName: "ssm"}
)

// ssmTagServer starts a server callable as [taggingTestAccount].
func ssmTagServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
}

// ssmRawCall posts a Systems Manager operation and returns the status, the raw body and the bare error
// code a refusal carries.
//
// The raw body is returned because one assertion below is about the *shape* of an empty TagList —
// whether it is [] or null — and a decoded []SSMTag cannot tell those apart.
func ssmRawCall(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, op string, body map[string]any) (int, string, string) {
	t.Helper()
	resp := signedRequest(t, ts, tgt, taggingTestAccount, op, body)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read %s body", op)

	var errShape struct {
		Type string `json:"__type"`
	}
	code := ""
	if unmarshalErr := json.Unmarshal(raw, &errShape); unmarshalErr == nil {
		code = awsErrorCode(errShape.Type)
	}
	return resp.StatusCode, string(raw), code
}

// ssmPutParameterIn creates a parameter through one Region's endpoint.
func ssmPutParameterIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, name, value string, tags map[string]string) {
	t.Helper()
	body := map[string]any{"Name": name, "Value": value, "Type": "String"}
	if len(tags) > 0 {
		list := make([]map[string]string, 0, len(tags))
		for k, v := range tags {
			list = append(list, map[string]string{"Key": k, "Value": v})
		}
		body["Tags"] = list
	}
	status, _, code := ssmRawCall(t, ts, tgt, "PutParameter", body)
	require.Empty(t, code, "PutParameter %s on %s", name, tgt.host)
	require.Equal(t, http.StatusOK, status, "PutParameter %s on %s", name, tgt.host)
}

// ssmPutParameter creates a parameter in us-east-1 and returns the ARN Systems Manager itself minted,
// read back through GetParameter — rather than one built here, so no assertion below can pass by
// agreeing with an ARN this test happens to construct.
func ssmPutParameter(t *testing.T, ts *emulator.TestServer, name string, tags map[string]string) string {
	t.Helper()
	ssmPutParameterIn(t, ts, ssmTagTarget, name, "value-of-"+name, tags)
	return ssmParameterARNOf(t, ts, ssmTagTarget, name)
}

// ssmParameterARNOf reads a parameter's ARN through GetParameter.
func ssmParameterARNOf(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, name string) string {
	t.Helper()
	status, raw, code := ssmRawCall(t, ts, tgt, "GetParameter", map[string]any{"Name": name})
	require.Empty(t, code, "GetParameter %s", name)
	require.Equal(t, http.StatusOK, status, "GetParameter %s", name)
	var out struct {
		Parameter struct {
			ARN string `json:"ARN"`
		} `json:"Parameter"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &out), "decode GetParameter %s", name)
	require.NotEmpty(t, out.Parameter.ARN, "GetParameter %s reports an ARN", name)
	return out.Parameter.ARN
}

// ssmParameterRecord reads the members of a parameter that are not tags, so a tag merge can be shown
// not to have truncated the rest of the record.
func ssmParameterRecord(t *testing.T, ts *emulator.TestServer, name string) (value, paramType string, version int64) {
	t.Helper()
	status, raw, code := ssmRawCall(t, ts, ssmTagTarget, "GetParameter", map[string]any{"Name": name})
	require.Empty(t, code, "GetParameter %s", name)
	require.Equal(t, http.StatusOK, status, "GetParameter %s", name)
	var out struct {
		Parameter struct {
			Value   string `json:"Value"`
			Type    string `json:"Type"`
			Version int64  `json:"Version"`
		} `json:"Parameter"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &out), "decode GetParameter %s", name)
	return out.Parameter.Value, out.Parameter.Type, out.Parameter.Version
}

// ssmListTags calls Systems Manager's own ListTagsForResource and returns the status, the tags and the
// raw body.
func ssmListTags(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, resourceType, resourceID string) (int, []emulator.SSMTag, string, string) {
	t.Helper()
	status, raw, code := ssmRawCall(t, ts, tgt, "ListTagsForResource",
		map[string]any{"ResourceType": resourceType, "ResourceId": resourceID})
	var out struct {
		TagList []emulator.SSMTag `json:"TagList"`
	}
	if status == http.StatusOK {
		require.NoError(t, json.Unmarshal([]byte(raw), &out), "decode ListTagsForResource %s", resourceID)
	}
	return status, out.TagList, raw, code
}

// ssmTagMap reads a parameter's tags through ListTagsForResource as a map.
func ssmTagMap(t *testing.T, ts *emulator.TestServer, name string) map[string]string {
	t.Helper()
	status, tags, _, code := ssmListTags(t, ts, ssmTagTarget, "Parameter", name)
	require.Empty(t, code, "ListTagsForResource %s", name)
	require.Equal(t, http.StatusOK, status, "ListTagsForResource %s", name)
	out := make(map[string]string, len(tags))
	for _, tag := range tags {
		out[tag.Key] = tag.Value
	}
	return out
}

// ssmTagKeyOrder returns the tag keys in the order ListTagsForResource reported them, for the
// determinism assertion. A map would discard the one thing being checked.
func ssmTagKeyOrder(t *testing.T, ts *emulator.TestServer, name string) []string {
	t.Helper()
	status, tags, _, code := ssmListTags(t, ts, ssmTagTarget, "Parameter", name)
	require.Empty(t, code, "ListTagsForResource %s", name)
	require.Equal(t, http.StatusOK, status, "ListTagsForResource %s", name)
	keys := make([]string, 0, len(tags))
	for _, tag := range tags {
		keys = append(keys, tag.Key)
	}
	return keys
}

// ssmAddTags calls Systems Manager's own AddTagsToResource and returns the status and error code, so a
// test can assert either the success path or a refusal.
func ssmAddTags(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, resourceType, resourceID string, tags map[string]string) (int, string) {
	t.Helper()
	list := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]string{"Key": k, "Value": v})
	}
	body := map[string]any{"ResourceId": resourceID, "Tags": list}
	if resourceType != "" {
		body["ResourceType"] = resourceType
	}
	status, _, code := ssmRawCall(t, ts, tgt, "AddTagsToResource", body)
	return status, code
}

// ssmRemoveTags calls Systems Manager's own RemoveTagsFromResource. It is never left untested
// alongside AddTagsToResource: a removal aimed at the wrong resource is the more damaging direction.
func ssmRemoveTags(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, resourceType, resourceID string, keys ...string) (int, string) {
	t.Helper()
	body := map[string]any{"ResourceId": resourceID, "TagKeys": keys}
	if resourceType != "" {
		body["ResourceType"] = resourceType
	}
	status, _, code := ssmRawCall(t, ts, tgt, "RemoveTagsFromResource", body)
	return status, code
}

// ─── The resource type an identifier names ───────────────────────────────────

// TestSSMTagType_AResourceTypeOtherThanParameterDoesNotReachASameNamedParameter is the assertion this
// file exists for.
//
// Before #932 the ResourceType member was decoded and discarded, so a Document, an OpsItem or a
// maintenance window whose identifier matched a parameter name was answered as that parameter. All
// three operations are covered because they fail in three different ways: AddTagsToResource wrote onto
// the wrong record, RemoveTagsFromResource stripped from it, and ListTagsForResource read it back —
// which is what made the round trip *confirm* the wrong resource had been tagged.
func TestSSMTagType_AResourceTypeOtherThanParameterDoesNotReachASameNamedParameter(t *testing.T) {
	ts := ssmTagServer(t)
	ssmPutParameter(t, ts, "/MyRunbook", map[string]string{"env": "prod"})

	// The nine ResourceType values AWS publishes that substrate models no taggable resource for. The
	// type is real, so the refusal names the identifier rather than the type: it is the ID that names
	// nothing here.
	for _, resourceType := range []string{
		"Document", "ManagedInstance", "MaintenanceWindow", "PatchBaseline",
		"OpsItem", "OpsMetadata", "Automation", "Association", "CloudConnector",
	} {
		t.Run(resourceType, func(t *testing.T) {
			status, code := ssmAddTags(t, ts, ssmTagTarget, resourceType, "MyRunbook", map[string]string{"leaked": "yes"})
			assert.Equal(t, "InvalidResourceId", code, "AddTagsToResource as %s is refused", resourceType)
			assert.NotEqual(t, http.StatusOK, status, "AddTagsToResource as %s is not a success", resourceType)

			status, code = ssmRemoveTags(t, ts, ssmTagTarget, resourceType, "MyRunbook", "env")
			assert.Equal(t, "InvalidResourceId", code, "RemoveTagsFromResource as %s is refused", resourceType)
			assert.NotEqual(t, http.StatusOK, status, "RemoveTagsFromResource as %s is not a success", resourceType)

			status, _, _, code = ssmListTags(t, ts, ssmTagTarget, resourceType, "MyRunbook")
			assert.Equal(t, "InvalidResourceId", code, "ListTagsForResource as %s is refused", resourceType)
			assert.NotEqual(t, http.StatusOK, status, "ListTagsForResource as %s is not a success", resourceType)
		})
	}

	// The parameter is untouched by all twenty-seven refusals: neither the tag it was created with was
	// removed, nor the one the Document calls tried to add was written.
	assert.Equal(t, map[string]string{"env": "prod"}, ssmTagMap(t, ts, "/MyRunbook"),
		"the parameter's own tags are unchanged")
}

// TestSSMTagType_AResourceTypeOutsideTheEnumIsRefusedAsAType separates the two refusals, because they
// tell a caller two different things: a value AWS publishes names a resource that does not exist here,
// while a value AWS does not publish is not a resource type at all. Answering the same code for both
// would tell a caller who mistyped "Parameter" that their parameter is missing.
func TestSSMTagType_AResourceTypeOutsideTheEnumIsRefusedAsAType(t *testing.T) {
	ts := ssmTagServer(t)
	ssmPutParameter(t, ts, "/enum/guard", nil)

	for _, resourceType := range []string{
		"parameter",  // right value, wrong case — the enum is case-sensitive
		"Parameters", // plural
		"Banana",     // not a type at all
		"Secret",     // another service's word
	} {
		t.Run(resourceType, func(t *testing.T) {
			status, code := ssmAddTags(t, ts, ssmTagTarget, resourceType, "/enum/guard", map[string]string{"k": "v"})
			assert.Equal(t, "InvalidResourceType", code, "AddTagsToResource with %q", resourceType)
			assert.Equal(t, http.StatusBadRequest, status, "InvalidResourceType is a 400")

			status, code = ssmRemoveTags(t, ts, ssmTagTarget, resourceType, "/enum/guard", "k")
			assert.Equal(t, "InvalidResourceType", code, "RemoveTagsFromResource with %q", resourceType)
			assert.Equal(t, http.StatusBadRequest, status, "InvalidResourceType is a 400")
		})
	}

	assert.Empty(t, ssmTagMap(t, ts, "/enum/guard"), "no refused call wrote a tag")
}

// TestSSMTagType_AnAbsentResourceTypeIsRefused covers AWS publishing ResourceType as Required: Yes.
// Substrate answered the call as a Parameter, which is the same defect in its most silent form — a
// caller who omitted the member got the behavior of the one type substrate happens to model.
func TestSSMTagType_AnAbsentResourceTypeIsRefused(t *testing.T) {
	ts := ssmTagServer(t)
	ssmPutParameter(t, ts, "/required/type", nil)

	status, code := ssmAddTags(t, ts, ssmTagTarget, "", "/required/type", map[string]string{"k": "v"})
	assert.Equal(t, "ValidationException", code, "an absent ResourceType is refused")
	assert.Equal(t, http.StatusBadRequest, status, "and at 400")
	assert.Empty(t, ssmTagMap(t, ts, "/required/type"), "the refused call wrote nothing")
}

// TestSSMTagType_AnARNAsAParameterResourceIdIsRefused pins the second half of the resolution fix. AWS
// is explicit that "For the Document and Parameter values, use the name of the resource", so an ARN is
// not the identifier form here. This one is a guard rather than a proof: substrate used to prepend "/"
// to the ARN and look up the resulting name, which is refused too and under the same code — the
// difference is that the refusal is now deliberate and its message says what is wrong, rather than
// arriving after a normalization that makes the message nonsense. #928 settled the rule for Secrets
// Manager's SecretId: a caller who wrote an ARN prefix meant an ARN.
func TestSSMTagType_AnARNAsAParameterResourceIdIsRefused(t *testing.T) {
	ts := ssmTagServer(t)
	arn := ssmPutParameter(t, ts, "/db/password", map[string]string{"env": "prod"})

	status, code := ssmAddTags(t, ts, ssmTagTarget, "Parameter", arn, map[string]string{"viaARN": "yes"})
	assert.Equal(t, "InvalidResourceId", code, "an ARN is not a Parameter ResourceId")
	assert.NotEqual(t, http.StatusOK, status, "and is not a success")

	status, code = ssmRemoveTags(t, ts, ssmTagTarget, "Parameter", arn, "env")
	assert.Equal(t, "InvalidResourceId", code, "the removal direction is refused too")
	assert.NotEqual(t, http.StatusOK, status, "and is not a success")

	assert.Equal(t, map[string]string{"env": "prod"}, ssmTagMap(t, ts, "/db/password"),
		"the parameter's tags are unchanged")
}

// TestSSMTagType_ABareNameStillResolves is a regression guard rather than a fix. Substrate's own
// PutParameter normalizes Name to a leading "/", so a caller who created "MyParam" has a parameter
// called "/MyParam" and must be able to tag it as "MyParam". AWS documents that tolerance explicitly
// only for OpsMetadata, so for a Parameter it is substrate's reading — required by substrate's own
// normalization, which is why it is asserted from both directions.
func TestSSMTagType_ABareNameStillResolves(t *testing.T) {
	ts := ssmTagServer(t)
	ssmPutParameterIn(t, ts, ssmTagTarget, "MyParam", "v", nil)

	status, code := ssmAddTags(t, ts, ssmTagTarget, "Parameter", "MyParam", map[string]string{"env": "dev"})
	require.Empty(t, code, "AddTagsToResource with a bare name")
	require.Equal(t, http.StatusOK, status, "AddTagsToResource with a bare name")

	assert.Equal(t, map[string]string{"env": "dev"}, ssmTagMap(t, ts, "MyParam"),
		"read back under the bare name")
	assert.Equal(t, map[string]string{"env": "dev"}, ssmTagMap(t, ts, "/MyParam"),
		"and under the slash-prefixed name it was stored as")
}

// TestSSMTagType_AnAbsentParameterIsReportedInvalidResourceId is a regression guard: Systems Manager
// publishes no distinct not-found code for these three operations, so InvalidResourceId is how a
// nonexistent parameter is reported.
//
// The status is asserted alongside the code because that is the whole of #933: all three reference
// pages give InvalidResourceId HTTP 400 and publish no 404 at all, so substrate's 404 was a status no
// Systems Manager operation can answer. It is asserted on the response rather than through a decoded
// error struct, which carries the code and not the status.
func TestSSMTagType_AnAbsentParameterIsReportedInvalidResourceId(t *testing.T) {
	ts := ssmTagServer(t)

	status, code := ssmAddTags(t, ts, ssmTagTarget, "Parameter", "/nothing/here", map[string]string{"k": "v"})
	assert.Equal(t, "InvalidResourceId", code, "AddTagsToResource on an absent parameter")
	assert.Equal(t, http.StatusBadRequest, status, "AddTagsToResource on an absent parameter")

	status, code = ssmRemoveTags(t, ts, ssmTagTarget, "Parameter", "/nothing/here", "k")
	assert.Equal(t, "InvalidResourceId", code, "RemoveTagsFromResource on an absent parameter")
	assert.Equal(t, http.StatusBadRequest, status, "RemoveTagsFromResource on an absent parameter")

	status, _, _, code = ssmListTags(t, ts, ssmTagTarget, "Parameter", "/nothing/here")
	assert.Equal(t, "InvalidResourceId", code, "ListTagsForResource on an absent parameter")
	assert.Equal(t, http.StatusBadRequest, status, "ListTagsForResource on an absent parameter")
}

// TestSSMTagType_EveryRefusalOfAnIdentifierIsA400 covers the other five reasons
// [ssmInvalidResourceID] is reached, because the status lives in that one helper and a test that only
// exercised the absent-parameter path would leave the rest resting on nothing. AWS publishes no 404
// on these operations, so no input should produce one.
func TestSSMTagType_EveryRefusalOfAnIdentifierIsA400(t *testing.T) {
	ts := ssmTagServer(t)

	for _, tc := range []struct {
		name       string
		resourceID string
	}{
		{"an empty ResourceId", ""},
		{"an ARN where a name belongs", "arn:aws:ssm:us-east-1:123456789012:parameter/app"},
		{"an ARN of another service", "arn:aws:s3:::a-bucket"},
		{"a parameter that does not exist", "/nothing/here"},
		{"a bare name that does not exist", "nothing-here"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code := ssmAddTags(t, ts, ssmTagTarget, "Parameter", tc.resourceID, map[string]string{"k": "v"})
			assert.Equal(t, "InvalidResourceId", code, "AddTagsToResource with %s", tc.name)
			assert.Equal(t, http.StatusBadRequest, status, "AddTagsToResource with %s", tc.name)
		})
	}
}

// ─── Systems Manager's own tags ──────────────────────────────────────────────

// TestSSMTags_TagsSetAtCreationAreReadBackThroughListTagsForResource is the baseline the rest of the
// file rests on: the owning service's own read path reports what its own write path stored.
func TestSSMTags_TagsSetAtCreationAreReadBackThroughListTagsForResource(t *testing.T) {
	ts := ssmTagServer(t)
	ssmPutParameter(t, ts, "/app/db/url", map[string]string{"env": "prod", "team": "platform"})

	assert.Equal(t, map[string]string{"env": "prod", "team": "platform"}, ssmTagMap(t, ts, "/app/db/url"))
}

// TestSSMTags_AnUntaggedParameterReportsAnEmptyList asserts on raw bytes, because the distinction is
// between [] and null and a decoded []SSMTag cannot tell them apart. SSMParameter.Tags is
// `omitempty` and therefore nil on an untagged parameter, which used to render as "TagList": null —
// not the array AWS publishes.
func TestSSMTags_AnUntaggedParameterReportsAnEmptyList(t *testing.T) {
	ts := ssmTagServer(t)
	ssmPutParameter(t, ts, "/untagged", nil)

	status, tags, raw, code := ssmListTags(t, ts, ssmTagTarget, "Parameter", "/untagged")
	require.Empty(t, code, "ListTagsForResource /untagged")
	require.Equal(t, http.StatusOK, status, "ListTagsForResource /untagged")
	assert.Empty(t, tags, "no tags")
	assert.Contains(t, raw, `"TagList":[]`, "TagList is an empty array")
	assert.NotContains(t, raw, `"TagList":null`, "and not null")
}

// TestSSMTags_AddTagsToResourceAppendsRatherThanReplaces is a regression guard on the merge semantics,
// asserted because the sort added in #932 rebuilds the list and a rebuild is where a merge turns into
// a replace.
func TestSSMTags_AddTagsToResourceAppendsRatherThanReplaces(t *testing.T) {
	ts := ssmTagServer(t)
	ssmPutParameter(t, ts, "/append", map[string]string{"first": "1"})

	status, code := ssmAddTags(t, ts, ssmTagTarget, "Parameter", "/append", map[string]string{"second": "2"})
	require.Empty(t, code, "AddTagsToResource")
	require.Equal(t, http.StatusOK, status, "AddTagsToResource")

	assert.Equal(t, map[string]string{"first": "1", "second": "2"}, ssmTagMap(t, ts, "/append"))

	// An existing key is overwritten rather than duplicated.
	status, code = ssmAddTags(t, ts, ssmTagTarget, "Parameter", "/append", map[string]string{"first": "one"})
	require.Empty(t, code, "AddTagsToResource overwriting a key")
	require.Equal(t, http.StatusOK, status, "AddTagsToResource overwriting a key")
	assert.Equal(t, map[string]string{"first": "one", "second": "2"}, ssmTagMap(t, ts, "/append"))
	assert.Len(t, ssmTagKeyOrder(t, ts, "/append"), 2, "the overwritten key is not duplicated")
}

// TestSSMTags_RemoveTagsFromResourceRemovesOnlyTheNamedKeys is the removal direction, which is the one
// that loses a caller's data when it is aimed at the wrong record.
func TestSSMTags_RemoveTagsFromResourceRemovesOnlyTheNamedKeys(t *testing.T) {
	ts := ssmTagServer(t)
	ssmPutParameter(t, ts, "/remove", map[string]string{"keep": "yes", "drop": "no", "also": "yes"})

	status, code := ssmRemoveTags(t, ts, ssmTagTarget, "Parameter", "/remove", "drop")
	require.Empty(t, code, "RemoveTagsFromResource")
	require.Equal(t, http.StatusOK, status, "RemoveTagsFromResource")

	assert.Equal(t, map[string]string{"keep": "yes", "also": "yes"}, ssmTagMap(t, ts, "/remove"))

	// A key that is not there is not an error, and removes nothing.
	status, code = ssmRemoveTags(t, ts, ssmTagTarget, "Parameter", "/remove", "never-set")
	require.Empty(t, code, "removing an absent key")
	require.Equal(t, http.StatusOK, status, "removing an absent key")
	assert.Equal(t, map[string]string{"keep": "yes", "also": "yes"}, ssmTagMap(t, ts, "/remove"))
}

// TestSSMTags_TagOrderIsDeterministic pins the third defect. AddTagsToResource merged through a
// map[string]string and appended by ranging it, so the order it stored came from Go's map hash seed —
// the defect #862 fixed in all four of the tagging API's merge helpers and missed in Systems Manager's
// own path. Creation-time tagging had the same problem from the other side: PutParameter stored the
// request's order, so a parameter tagged at creation and one tagged afterwards reported different
// orders for the same tags.
func TestSSMTags_TagOrderIsDeterministic(t *testing.T) {
	const want = 3
	keys := []string{"alpha", "beta", "gamma"}

	t.Run("through AddTagsToResource", func(t *testing.T) {
		ts := ssmTagServer(t)
		ssmPutParameter(t, ts, "/order/added", nil)
		status, code := ssmAddTags(t, ts, ssmTagTarget, "Parameter", "/order/added",
			map[string]string{"gamma": "3", "alpha": "1", "beta": "2"})
		require.Empty(t, code, "AddTagsToResource")
		require.Equal(t, http.StatusOK, status, "AddTagsToResource")

		order := ssmTagKeyOrder(t, ts, "/order/added")
		require.Len(t, order, want)
		assert.Equal(t, keys, order, "sorted by key")
		assert.Equal(t, order, ssmTagKeyOrder(t, ts, "/order/added"), "and the same on a second read")
	})

	t.Run("at creation", func(t *testing.T) {
		ts := ssmTagServer(t)
		ssmPutParameterIn(t, ts, ssmTagTarget, "/order/created", "v",
			map[string]string{"gamma": "3", "alpha": "1", "beta": "2"})
		assert.Equal(t, keys, ssmTagKeyOrder(t, ts, "/order/created"),
			"a parameter tagged at creation reports the same order as one tagged afterwards")
	})

	t.Run("through the tagging API", func(t *testing.T) {
		ts := ssmTagServer(t)
		arn := ssmPutParameter(t, ts, "/order/tagged", nil)
		tagResourcesWith(t, ts, arn, map[string]string{"gamma": "3", "alpha": "1", "beta": "2"})
		assert.Equal(t, keys, ssmTagKeyOrder(t, ts, "/order/tagged"),
			"a tag written through the tagging API reports the same order too")
	})
}

// ─── The tagging API's eighth row of #835 ────────────────────────────────────

// TestTaggingSSM_AParameterTagIsReadBackThroughSystemsManager is #765's cross-readability criterion in
// the direction that proves the two APIs share one address: the tagging API writes and Systems
// Manager's own operation reads. Before #932 the tagging API had no ssm arm at all, so TagResources
// reported the ARN unsupported.
func TestTaggingSSM_AParameterTagIsReadBackThroughSystemsManager(t *testing.T) {
	ts := ssmTagServer(t)
	arn := ssmPutParameter(t, ts, "/app/db/url", map[string]string{"env": "prod"})

	tagResourcesWith(t, ts, arn, map[string]string{"owner": "platform"})

	assert.Equal(t, map[string]string{"env": "prod", "owner": "platform"}, ssmTagMap(t, ts, "/app/db/url"),
		"the tagging API's write is readable through ListTagsForResource")
}

// TestTaggingSSM_ATagWrittenThroughSystemsManagerIsReportedByGetResources is the other direction, and
// it is the one the scanner is for.
func TestTaggingSSM_ATagWrittenThroughSystemsManagerIsReportedByGetResources(t *testing.T) {
	ts := ssmTagServer(t)
	arn := ssmPutParameter(t, ts, "/app/api/key", nil)

	status, code := ssmAddTags(t, ts, ssmTagTarget, "Parameter", "/app/api/key", map[string]string{"env": "staging"})
	require.Empty(t, code, "AddTagsToResource")
	require.Equal(t, http.StatusOK, status, "AddTagsToResource")

	assert.Equal(t, map[string]string{"env": "staging"}, getResourcesTags(t, ts, arn),
		"GetResources reports the tag Systems Manager wrote")
}

// TestTaggingSSM_UntagResourcesRemovesOnlyTheNamedKeys covers the tagging API's removal direction.
func TestTaggingSSM_UntagResourcesRemovesOnlyTheNamedKeys(t *testing.T) {
	ts := ssmTagServer(t)
	arn := ssmPutParameter(t, ts, "/untag/me", map[string]string{"keep": "yes", "drop": "no"})

	untagResourcesWith(t, ts, arn, "drop")

	assert.Equal(t, map[string]string{"keep": "yes"}, ssmTagMap(t, ts, "/untag/me"),
		"only the named key is gone, read through the owning service")
}

// TestTaggingSSM_TheMergePreservesTheRestOfTheRecord is why the merge arm goes through
// mergeRecordTagListTags on the raw JSON rather than unmarshalling into an SSMParameter: a struct
// round trip drops every member the struct does not declare, and a tag call must not rewrite a
// parameter's value, type or version.
func TestTaggingSSM_TheMergePreservesTheRestOfTheRecord(t *testing.T) {
	ts := ssmTagServer(t)
	arn := ssmPutParameter(t, ts, "/preserve/me", nil)
	valueBefore, typeBefore, versionBefore := ssmParameterRecord(t, ts, "/preserve/me")
	require.NotEmpty(t, valueBefore, "the parameter has a value to lose")

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod"})

	valueAfter, typeAfter, versionAfter := ssmParameterRecord(t, ts, "/preserve/me")
	assert.Equal(t, valueBefore, valueAfter, "the value survives a tag merge")
	assert.Equal(t, typeBefore, typeAfter, "the type survives")
	assert.Equal(t, versionBefore, versionAfter, "and the version is not bumped by a tag call")
	assert.Equal(t, arn, ssmParameterARNOf(t, ts, ssmTagTarget, "/preserve/me"), "and the ARN survives")
}

// TestTaggingSSM_GetResourcesReportsTheParameterAndNothingElse asserts an exact set rather than
// membership. The scanner lists a key prefix, and "parameter" is a prefix of "parameter_paths" —
// whose value is a JSON array of names, not a parameter record. A membership assertion would pass
// while the scanner also reported that index key with an empty ARN.
func TestTaggingSSM_GetResourcesReportsTheParameterAndNothingElse(t *testing.T) {
	ts := ssmTagServer(t)
	first := ssmPutParameter(t, ts, "/one", map[string]string{"n": "1"})
	second := ssmPutParameter(t, ts, "/two/deep/name", map[string]string{"n": "2"})

	assert.ElementsMatch(t, []string{first, second}, getResourcesARNs(t, ts, "ssm"),
		"exactly the two parameters, and no index key")
}

// TestTaggingSSM_AHierarchicalNameRoundTripsThroughItsARN pins the ARN derivation. ssmParameterARN
// absorbs the name's own leading "/", so the resource portion of the ARN is "parameter/two/deep/name"
// — and taking the ARN's last "/"-separated component, which is how several of substrate's older
// resolvers worked, would truncate the name to "name".
func TestTaggingSSM_AHierarchicalNameRoundTripsThroughItsARN(t *testing.T) {
	ts := ssmTagServer(t)
	arn := ssmPutParameter(t, ts, "/two/deep/name", nil)
	require.Contains(t, arn, ":parameter/two/deep/name", "the ARN carries the whole hierarchy")

	// A decoy at the truncated name, so a resolver that took the last component would tag this one
	// instead and the assertion below would catch it.
	ssmPutParameterIn(t, ts, ssmTagTarget, "/name", "decoy", nil)

	tagResourcesWith(t, ts, arn, map[string]string{"depth": "three"})

	assert.Equal(t, map[string]string{"depth": "three"}, ssmTagMap(t, ts, "/two/deep/name"),
		"the deep parameter was tagged")
	assert.Empty(t, ssmTagMap(t, ts, "/name"), "and the decoy at the truncated name was not")
}

// TestTaggingSSM_GetResourcesDoesNotReportAParameterFromAnotherRegion pins the scanner's Region
// attribution. A parameter is Region-scoped, its ARN carries the Region, and the state key is
// Region-qualified — so a us-east-1 GetResources must not report a us-west-2 parameter.
func TestTaggingSSM_GetResourcesDoesNotReportAParameterFromAnotherRegion(t *testing.T) {
	ts := ssmTagServer(t)
	east := ssmPutParameter(t, ts, "/regional", map[string]string{"where": "east"})
	ssmPutParameterIn(t, ts, ssmTagWest2Target, "/regional", "west-value", map[string]string{"where": "west"})
	west := ssmParameterARNOf(t, ts, ssmTagWest2Target, "/regional")
	require.NotEqual(t, east, west, "the two ARNs differ in Region")

	assert.Equal(t, []string{east}, getResourcesARNs(t, ts, "ssm"),
		"only the caller's own Region's parameter")
	assert.Equal(t, []string{west}, getResourcesARNsIn(t, ts, taggingWest2Target),
		"and the other Region reports only its own")
}

// TestTaggingSSM_AParameterARNFromAnotherRegionDoesNotReachTheLocalParameter is the write direction of
// the same isolation, and it is the damaging one: before the ssm arm existed the ARN was refused
// outright, so this asserts that adding the arm did not add a cross-Region reach.
func TestTaggingSSM_AParameterARNFromAnotherRegionDoesNotReachTheLocalParameter(t *testing.T) {
	ts := ssmTagServer(t)
	ssmPutParameter(t, ts, "/regional", map[string]string{"where": "east"})
	ssmPutParameterIn(t, ts, ssmTagWest2Target, "/regional", "west-value", nil)
	west := ssmParameterARNOf(t, ts, ssmTagWest2Target, "/regional")

	// Tagging the us-west-2 ARN from us-east-1 reaches the us-west-2 parameter, because the key comes
	// from the ARN — and leaves the caller's own same-named parameter alone.
	tagResourcesWith(t, ts, west, map[string]string{"tagged": "remotely"})

	assert.Equal(t, map[string]string{"where": "east"}, ssmTagMap(t, ts, "/regional"),
		"the caller's own Region's parameter is untouched")

	status, tags, _, code := ssmListTags(t, ts, ssmTagWest2Target, "Parameter", "/regional")
	require.Empty(t, code, "ListTagsForResource in us-west-2")
	require.Equal(t, http.StatusOK, status, "ListTagsForResource in us-west-2")
	got := map[string]string{}
	for _, tag := range tags {
		got[tag.Key] = tag.Value
	}
	assert.Equal(t, map[string]string{"tagged": "remotely"}, got,
		"the ARN's own Region's parameter is the one that was tagged")
}

// TestTaggingSSM_AForeignAccountARNCannotBeTagged covers the account half of the same isolation. It is
// emergent rather than guarded: the state key carries the ARN's account, so an ARN naming another
// account builds a key nothing is stored at.
//
// It passed before #932 too, because every ssm ARN was refused by resolveARN's default arm. That is
// exactly why it is here: the point of the assertion is that adding the arm did not open the reach.
func TestTaggingSSM_AForeignAccountARNCannotBeTagged(t *testing.T) {
	ts := ssmTagServer(t)
	ssmPutParameter(t, ts, "/shared", map[string]string{"env": "prod"})

	foreign := "arn:aws:ssm:us-east-1:" + taggingForeignAccount + ":parameter/shared"
	failures := tagResourcesFailures(t, ts, "TagResources", foreign)
	assert.NotEmpty(t, failures[foreign].ErrorCode, "a foreign-account ARN is reported as a failure")

	assert.Equal(t, map[string]string{"env": "prod"}, ssmTagMap(t, ts, "/shared"),
		"and the caller's own same-named parameter is untouched")

	failures = tagResourcesFailures(t, ts, "UntagResources", foreign)
	assert.NotEmpty(t, failures[foreign].ErrorCode, "the removal direction is refused too")
	assert.Equal(t, map[string]string{"env": "prod"}, ssmTagMap(t, ts, "/shared"),
		"and it removed nothing")
}

// TestTaggingSSM_AnARNThatDoesNotNameAParameterIsNotTaggable pins the anchored-segment rule of #910.
// The ssm namespace addresses documents, service settings, OpsMetadata objects, maintenance windows,
// patch baselines and managed instances, none of which keep tags at a parameter's state key — and
// "parameter" must be the whole first segment of the resource portion, not merely start it.
func TestTaggingSSM_AnARNThatDoesNotNameAParameterIsNotTaggable(t *testing.T) {
	ts := ssmTagServer(t)
	ssmPutParameter(t, ts, "/real", nil)

	const prefix = "arn:aws:ssm:us-east-1:" + taggingTestAccount + ":"
	for name, arn := range map[string]string{
		"a document":            prefix + "document/MyRunbook",
		"a service setting":     prefix + "servicesetting/ssm/parameter-store/high-throughput-enabled",
		"an OpsMetadata object": prefix + "opsmetadata/aws/ssm/MyGroup/appmanager",
		"a maintenance window":  prefix + "maintenancewindow/mw-012345abcde",
		"a patch baseline":      prefix + "patchbaseline/pb-012345abcde",
		"a managed instance":    prefix + "managed-instance/mi-012345abcde",
		"a prefix of the type":  prefix + "parameters/real",
		"the bare type":         prefix + "parameter",
		"no resource portion":   "arn:aws:ssm:us-east-1:" + taggingTestAccount,
		"another service":       "arn:aws:s3:::my-bucket",
	} {
		t.Run(name, func(t *testing.T) {
			failures := tagResourcesFailures(t, ts, "TagResources", arn)
			assert.NotEmpty(t, failures[arn].ErrorCode, "%s is not taggable as a parameter", arn)
		})
	}
}
