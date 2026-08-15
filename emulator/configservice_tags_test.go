package emulator_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AWS Config resource tagging (#580).
//
// Two behaviors carry the weight here. The first is that TagResource *merges*: a tag
// the request did not mention survives, per "if existing tags on a resource are not
// specified in the request parameters, they are not changed". A replace-everything
// implementation passes a naive round-trip test and silently drops tags a consumer set
// earlier, so the merge is asserted in both directions — the new tag arrives and the
// old one is still there.
//
// The second is which ARNs name a taggable resource. Substrate models three of the ten
// types the Service Authorization Reference defines, and **a delivery channel is not one
// of the ten at all** — so an ARN naming one is refused rather than accepted into a
// namespace no operation would ever read back. The cases below also pin that an ARN
// pairing a real name with a wrong ID is refused, because a recorder's and a pack's ARN
// each carry two components and matching only the first would tag the real resource from
// a bogus ARN.

// configTagServer starts a server holding one of each taggable resource, and returns
// their ARNs read back from the API rather than assembled here.
func configTagServer(t *testing.T) (ts *emulator.TestServer, recorderARN, ruleARN, packARN string) {
	t.Helper()
	ts = emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutRule(t, ts, "s3-encrypted")
	packARN = configPutPack(t, ts, "ops")
	recorderARN = configRecorderARN(t, ts, "us-east-1")

	rules := configDescribeRules(t, ts, map[string]any{})
	require.Len(t, rules, 1)
	arn, ok := rules[0]["ConfigRuleArn"].(string)
	require.True(t, ok, "the rule reports a ConfigRuleArn")
	return ts, recorderARN, arn, packARN
}

// configTagResource sends a TagResource and returns the status and error code.
func configTagResource(t *testing.T, ts *emulator.TestServer, arn string,
	tags []map[string]any) (int, string, string) {
	t.Helper()
	resp := configRequest(t, ts, "TagResource", map[string]any{"ResourceArn": arn, "Tags": tags})
	return decodeConfigResponse(t, resp, nil)
}

// configTag builds one Tag member.
func configTag(key, value string) map[string]any {
	return map[string]any{"Key": key, "Value": value}
}

// configTagOK tags a resource and requires that it succeed.
func configTagOK(t *testing.T, ts *emulator.TestServer, arn string, tags ...map[string]any) {
	t.Helper()
	status, code, message := configTagResource(t, ts, arn, tags)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
}

// configUntagResource sends an UntagResource and returns the status and error code.
func configUntagResource(t *testing.T, ts *emulator.TestServer, arn string,
	keys []string) (int, string, string) {
	t.Helper()
	resp := configRequest(t, ts, "UntagResource", map[string]any{"ResourceArn": arn, "TagKeys": keys})
	return decodeConfigResponse(t, resp, nil)
}

// configListTagsRaw sends a ListTagsForResource and returns the whole response.
func configListTagsRaw(t *testing.T, ts *emulator.TestServer, body map[string]any) (
	tags []map[string]any, next string, status int, code, message string) {
	t.Helper()
	resp := configRequest(t, ts, "ListTagsForResource", body)
	var out struct {
		Tags      []map[string]any `json:"Tags"`
		NextToken string           `json:"NextToken"`
	}
	status, code, message = decodeConfigResponse(t, resp, &out)
	return out.Tags, out.NextToken, status, code, message
}

// configListTags reads a resource's tags as a map, requiring success.
func configListTags(t *testing.T, ts *emulator.TestServer, arn string) map[string]string {
	t.Helper()
	tags, next, status, code, message := configListTagsRaw(t, ts, map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	require.Empty(t, next, "a resource under the page size reports no NextToken")
	return configTagsToMap(t, tags)
}

// configTagsToMap folds a reported TagList into a map, asserting each member's shape on
// the way through.
func configTagsToMap(t *testing.T, tags []map[string]any) map[string]string {
	t.Helper()
	out := make(map[string]string, len(tags))
	for _, tag := range tags {
		key, ok := tag["Key"].(string)
		require.True(t, ok, "each tag reports a Key: %v", tag)
		value, ok := tag["Value"].(string)
		require.True(t, ok, "each tag reports a Value: %v", tag)
		out[key] = value
	}
	return out
}

func TestConfigTags_MergeRatherThanReplace(t *testing.T) {
	// The behavior the operation's own prose specifies: "If existing tags on a resource
	// are not specified in the request parameters, they are not changed. If existing
	// tags are specified, however, then their values will be updated."
	//
	// Both halves matter and they fail in opposite directions. An implementation that
	// replaced would drop `owner`, and one that refused to overwrite would leave `env`
	// at its first value — each passes a test that checks only the other.
	ts, _, ruleARN, _ := configTagServer(t)

	configTagOK(t, ts, ruleARN, configTag("env", "dev"), configTag("owner", "platform"))
	configTagOK(t, ts, ruleARN, configTag("env", "prod"))

	assert.Equal(t, map[string]string{"env": "prod", "owner": "platform"},
		configListTags(t, ts, ruleARN),
		"the named tag is updated and the unnamed one survives")
}

func TestConfigTags_EachTaggableTypeIsReachable(t *testing.T) {
	// The three types substrate models, each tagged and read back through its own ARN.
	// A recorder's and a pack's ARN carry two components while a rule's carries one, so
	// this is also where the three resolution paths are exercised at all.
	ts, recorderARN, ruleARN, packARN := configTagServer(t)

	for _, tc := range []struct{ name, arn string }{
		{"recorder", recorderARN},
		{"rule", ruleARN},
		{"pack", packARN},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configTagOK(t, ts, tc.arn, configTag("kind", tc.name))
			assert.Equal(t, map[string]string{"kind": tc.name}, configListTags(t, ts, tc.arn))
		})
	}
}

func TestConfigTags_AreNotSharedBetweenResources(t *testing.T) {
	// Tags are keyed by ARN, so tagging one resource must not be visible on another.
	// A key that dropped the ARN would make every Config resource in the account share
	// one tag set, which is the shape a policy conditioned on aws:ResourceTag would then
	// match far too widely.
	ts, recorderARN, ruleARN, packARN := configTagServer(t)

	configTagOK(t, ts, ruleARN, configTag("env", "prod"))

	assert.Empty(t, configListTags(t, ts, recorderARN))
	assert.Empty(t, configListTags(t, ts, packARN))
}

func TestConfigTags_CreationTimeTagsAreVisibleToTheTagAPI(t *testing.T) {
	// A Put's Tags list and ListTagsForResource must agree: the Put writes tags keyed by
	// the resource's stored ARN and the tag cluster resolves the caller's ARN back to
	// that same key. Two spellings of one resource's tags would each be invisible to the
	// other, and the Put's would be the copy no operation ever read.
	ts := emulator.StartTestServer(t)
	resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": configRecorderPayload("default"),
		"Tags":                  []map[string]any{configTag("team", "compliance")},
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	arn := configRecorderARN(t, ts, "us-east-1")
	assert.Equal(t, map[string]string{"team": "compliance"}, configListTags(t, ts, arn),
		"the tag API reads what the Put wrote")
	assert.Equal(t, configStoredTags(t, ts, arn), configListTags(t, ts, arn),
		"the wire response and the stored state agree")
}

func TestConfigTags_ATagAddedByTheAPIIsNotErasedByAnUpdatingPut(t *testing.T) {
	// A second Put does not touch tags — "the tags for a configuration recorder are
	// added at creation and are not updated with configuration recorder updates" — and
	// that has to hold for tags the *tag API* added too, not only for creation-time
	// ones. Otherwise an unrelated recording-group update would silently delete a
	// consumer's tags.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	arn := configRecorderARN(t, ts, "us-east-1")
	configTagOK(t, ts, arn, configTag("env", "prod"))

	configPutRecorder(t, ts, "default")

	assert.Equal(t, map[string]string{"env": "prod"}, configListTags(t, ts, arn))
}

func TestConfigTags_UntagRemovesOnlyTheNamedKeys(t *testing.T) {
	ts, _, ruleARN, _ := configTagServer(t)
	configTagOK(t, ts, ruleARN,
		configTag("env", "prod"), configTag("owner", "platform"), configTag("tier", "1"))

	status, code, message := configUntagResource(t, ts, ruleARN, []string{"env", "tier"})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	assert.Equal(t, map[string]string{"owner": "platform"}, configListTags(t, ts, ruleARN))
}

func TestConfigTags_UntaggingAnAbsentKeyIsANoOp(t *testing.T) {
	// Undocumented either way: the operation's prose does not say, and neither of the
	// two exceptions it declares (ValidationException, ResourceNotFoundException)
	// describes an absent key.
	//
	// Substrate answers 200. The no-op is what keeps a teardown idempotent — a fixture
	// that untags before deleting, run twice, must not fail the second time on a tag the
	// first run already removed — and refusing would make substrate stricter than
	// anything documented, which breaks working code.
	ts, _, ruleARN, _ := configTagServer(t)
	configTagOK(t, ts, ruleARN, configTag("env", "prod"))

	status, code, message := configUntagResource(t, ts, ruleARN, []string{"nosuchkey"})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	status, code, message = configUntagResource(t, ts, ruleARN, []string{"env"})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	status, code, message = configUntagResource(t, ts, ruleARN, []string{"env"})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	assert.Empty(t, configListTags(t, ts, ruleARN))
}

func TestConfigTags_AnUntaggedResourceReportsAnEmptyList(t *testing.T) {
	// TagList's model bound is min 1, which cannot hold for a resource with no tags, so
	// the member is emitted empty rather than omitted: a consumer taking len() of the
	// result gets 0 rather than following a nil path. Output bounds are not something an
	// SDK enforces on the way in.
	ts, recorderARN, _, _ := configTagServer(t)

	tags, next, status, code, message := configListTagsRaw(t, ts,
		map[string]any{"ResourceArn": recorderARN})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Empty(t, tags)
	assert.Empty(t, next)
}

func TestConfigTags_TheDeliveryChannelIsNotTaggable(t *testing.T) {
	// **There is no delivery-channel resource type.** The Service Authorization
	// Reference defines ten Config resource types and none is a delivery channel;
	// PutDeliveryChannel and its three siblings authorize against an empty resource
	// list, i.e. "*". TagResource's own ResourceArn documentation likewise does not list
	// a delivery channel among the taggable types, and the DeliveryChannel shape has no
	// arn member to be named by.
	//
	// So an ARN in the shape one might guess is refused. Accepting it would write tags
	// into a key no operation reads back, and a consumer's test asserting the tag "took"
	// would pass while nothing had happened.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")

	for _, arn := range []string{
		"arn:aws:config:us-east-1:123456789012:delivery-channel/default",
		"arn:aws:config:us-east-1:123456789012:deliverychannel/default",
	} {
		status, code, _ := configTagResource(t, ts, arn, []map[string]any{configTag("env", "prod")})
		assert.Equal(t, http.StatusBadRequest, status, arn)
		assert.Equal(t, "ResourceNotFoundException", code, arn)
	}
}

func TestConfigTags_ARefusedARNRefusesAllThreeOperations(t *testing.T) {
	// The three operations resolve the ARN through one function, so an ARN that names
	// nothing must be refused by all three. A read path that resolved more loosely than
	// the write path would report tags for a resource a tag could not be written to.
	ts, _, ruleARN, _ := configTagServer(t)

	for _, tc := range []struct{ name, arn string }{
		{"an empty ARN", ""},
		{"not an ARN at all", "s3-encrypted"},
		{"another service's ARN", "arn:aws:s3:::cfg-logs"},
		{"a resource type substrate does not model",
			"arn:aws:config:us-east-1:123456789012:config-aggregator/agg"},
		{"a resource type that does not exist",
			"arn:aws:config:us-east-1:123456789012:nosuchtype/thing"},
		{"a type with no identifier", "arn:aws:config:us-east-1:123456789012:config-rule/"},
		{"another account's rule",
			strings.Replace(ruleARN, "123456789012", "210987654321", 1)},
		{"another Region's rule", strings.Replace(ruleARN, "us-east-1", "eu-west-1", 1)},
		{"an unknown rule ID", "arn:aws:config:us-east-1:123456789012:config-rule/config-rule-nope00"},
		{"a recorder ARN with the wrong ID",
			"arn:aws:config:us-east-1:123456789012:configuration-recorder/default/deadbeef"},
		{"a recorder ARN with no ID component",
			"arn:aws:config:us-east-1:123456789012:configuration-recorder/default"},
		{"a pack ARN with the wrong ID",
			"arn:aws:config:us-east-1:123456789012:conformance-pack/ops/conformance-pack-xxxxxxxx"},
		{"an unknown pack", "arn:aws:config:us-east-1:123456789012:conformance-pack/ghost/x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// An empty ARN is a missing required member, which is ValidationException;
			// every other case names a resource that does not exist from the caller's
			// position, which is ResourceNotFoundException.
			want := "ResourceNotFoundException"
			if tc.arn == "" {
				want = "ValidationException"
			}

			status, code, _ := configTagResource(t, ts, tc.arn, []map[string]any{configTag("k", "v")})
			assert.Equal(t, http.StatusBadRequest, status, "TagResource")
			assert.Equal(t, want, code, "TagResource")

			status, code, _ = configUntagResource(t, ts, tc.arn, []string{"k"})
			assert.Equal(t, http.StatusBadRequest, status, "UntagResource")
			assert.Equal(t, want, code, "UntagResource")

			_, _, status, code, _ = configListTagsRaw(t, ts, map[string]any{"ResourceArn": tc.arn})
			assert.Equal(t, http.StatusBadRequest, status, "ListTagsForResource")
			assert.Equal(t, want, code, "ListTagsForResource")
		})
	}
}

func TestConfigTags_AreIsolatedByRegion(t *testing.T) {
	// Config is regional and every state key carries the Region, so a rule tagged in one
	// Region is not tagged in the other — and a rule of the same name in the other
	// Region has a different ARN, because the minted ID is Region-derived.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutRecorderIn(t, ts, "eu-west-1", "default")
	configPutRule(t, ts, "s3-encrypted")

	resp := configRequestIn(t, ts, "eu-west-1", "PutConfigRule", map[string]any{
		"ConfigRule": configRulePayload("s3-encrypted"),
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	east := configDescribeRules(t, ts, map[string]any{})
	require.Len(t, east, 1)
	eastARN, ok := east[0]["ConfigRuleArn"].(string)
	require.True(t, ok)

	resp = configRequestIn(t, ts, "eu-west-1", "DescribeConfigRules", map[string]any{})
	var out struct {
		ConfigRules []map[string]any `json:"ConfigRules"`
	}
	status, code, message = decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	require.Len(t, out.ConfigRules, 1)
	westARN, ok := out.ConfigRules[0]["ConfigRuleArn"].(string)
	require.True(t, ok)
	require.NotEqual(t, eastARN, westARN, "the same rule name in two Regions has two ARNs")

	configTagOK(t, ts, eastARN, configTag("env", "prod"))

	// The west ARN is refused in us-east-1 outright: it names a resource that does not
	// exist from this Region's position.
	_, _, status, code, _ = configListTagsRaw(t, ts, map[string]any{"ResourceArn": westARN})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceNotFoundException", code)

	// And read from its own Region, the west rule carries no tags.
	resp = configRequestIn(t, ts, "eu-west-1", "ListTagsForResource",
		map[string]any{"ResourceArn": westARN})
	var west struct {
		Tags []map[string]any `json:"Tags"`
	}
	status, code, message = decodeConfigResponse(t, resp, &west)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Empty(t, west.Tags, "the tag was applied in us-east-1 only")
}

func TestConfigTags_RefusesTagsAWSWouldRefuse(t *testing.T) {
	// The same restrictions the creation-time TagsList enforces, through the same
	// helper: a tag AWS would refuse must not enter through one door having been refused
	// at the other.
	ts, _, ruleARN, _ := configTagServer(t)

	for _, tc := range []struct {
		name string
		tags []map[string]any
		want string
	}{
		{"no Tags member at all", nil, "ValidationException"},
		{"an empty list", []map[string]any{}, "ValidationException"},
		{"an empty key", []map[string]any{configTag("", "v")}, "ValidationException"},
		{"a key over 128 characters",
			[]map[string]any{configTag(strings.Repeat("k", 129), "v")}, "ValidationException"},
		{"a value over 256 characters",
			[]map[string]any{configTag("k", strings.Repeat("v", 257))}, "ValidationException"},
		{"the reserved aws: prefix", []map[string]any{configTag("aws:x", "v")}, "ValidationException"},
		{"the reserved prefix in any case",
			[]map[string]any{configTag("AWS:x", "v")}, "ValidationException"},
		{"a character outside the documented set",
			[]map[string]any{configTag("k$", "v")}, "ValidationException"},
		{"more than 50 tags in one request", configTagsOfN(51), "ValidationException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, _ := configTagResource(t, ts, ruleARN, tc.tags)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.want, code)
		})
	}

	// The boundaries themselves are accepted, so the bounds are off-by-one-proof.
	configTagOK(t, ts, ruleARN,
		configTag(strings.Repeat("k", 128), strings.Repeat("v", 256)),
		configTag("empty-value-is-allowed", ""),
		configTag("unicode-and-symbols_.:/=+-@", "καλημέρα 1"))
}

func TestConfigTags_TheResourceCeilingIsFiftyAndIsTooManyTags(t *testing.T) {
	// Two size rules, two exceptions, and only one of them is reachable with a
	// well-formed request.
	//
	// A Tags *list* longer than 50 breaks TagList's own model bound, which is
	// ValidationException. A merge that would leave the *resource* holding more than 50
	// breaks the service limit, which is what TooManyTagsException exists for — and it
	// is reachable only across two calls, because no single request may carry 51 tags.
	// TagResource is the one of the three operations that declares it.
	ts, _, ruleARN, _ := configTagServer(t)

	configTagOK(t, ts, ruleARN, configTagsOfN(50)...)
	assert.Len(t, configListTags(t, ts, ruleARN), 50, "the fiftieth tag is accepted")

	// Re-tagging an existing key does not grow the resource, so it still succeeds at the
	// ceiling — otherwise a consumer could never correct a tag's value on a full
	// resource.
	configTagOK(t, ts, ruleARN, configTag("tag-00", "changed"))
	assert.Equal(t, "changed", configListTags(t, ts, ruleARN)["tag-00"])

	status, code, message := configTagResource(t, ts, ruleARN,
		[]map[string]any{configTag("one-too-many", "v")})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "TooManyTagsException", code)
	assert.Contains(t, message, "50", "the message names the limit the model's own does not")

	assert.Len(t, configListTags(t, ts, ruleARN), 50, "the refused tag was not stored")
}

func TestConfigTags_UntagRefusesKeysAWSWouldRefuse(t *testing.T) {
	// UntagResource declares ValidationException and ResourceNotFoundException and
	// **not** TooManyTagsException — only TagResource declares that — so an over-long
	// TagKeys list is answered here with the code this operation actually declares.
	ts, _, ruleARN, _ := configTagServer(t)

	tooMany := make([]string, 51)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("tag-%02d", i)
	}

	for _, tc := range []struct {
		name string
		keys []string
	}{
		{"no TagKeys member at all", nil},
		{"an empty list", []string{}},
		{"an empty key", []string{""}},
		{"a key over 128 characters", []string{strings.Repeat("k", 129)}},
		{"more than 50 keys", tooMany},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, _ := configUntagResource(t, ts, ruleARN, tc.keys)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "ValidationException", code)
		})
	}

	// Exactly 50 keys is the bound and is accepted.
	status, code, message := configUntagResource(t, ts, ruleARN, tooMany[:50])
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
}

func TestConfigTags_ListPaginatesAtTheModelsHundred(t *testing.T) {
	// **The Limit is documented two ways** — the member's prose says "The limit maximum
	// is 50. You cannot specify a number greater than 50", the shape it points at is
	// max 100 — and the contradiction is in both the rendered page and the vendored
	// model, so it is AWS's rather than a stale copy.
	//
	// Substrate takes 100, the model's bound, because an SDK generated from that model
	// will not client-side-reject a Limit of 60: refusing one would mean refusing a
	// request the SDK was built to send. So 60 is accepted here, which is exactly the
	// value that tells the two readings apart.
	ts, _, ruleARN, _ := configTagServer(t)
	configTagOK(t, ts, ruleARN, configTagsOfN(50)...)

	for _, limit := range []int{1, 50, 51, 60, 100} {
		tags, _, status, code, message := configListTagsRaw(t, ts,
			map[string]any{"ResourceArn": ruleARN, "Limit": limit})
		require.Equal(t, http.StatusOK, status, "Limit %d: %s: %s", limit, code, message)
		assert.LessOrEqual(t, len(tags), limit, "Limit %d bounds the page", limit)
	}

	for _, limit := range []int{-1, 101} {
		_, _, status, code, _ := configListTagsRaw(t, ts,
			map[string]any{"ResourceArn": ruleARN, "Limit": limit})
		assert.Equal(t, http.StatusBadRequest, status, "Limit %d", limit)
		assert.Equal(t, "InvalidLimitException", code, "Limit %d", limit)
	}
}

func TestConfigTags_PaginationWalksEveryTag(t *testing.T) {
	// Pagination walks the keys in sorted order, so a page boundary falls in the same
	// place on every replay. Every tag must appear exactly once across the pages: a
	// token that re-served a page would loop a consumer's walk, and one that skipped
	// would silently lose tags.
	ts, _, ruleARN, _ := configTagServer(t)
	configTagOK(t, ts, ruleARN, configTagsOfN(50)...)

	seen := map[string]string{}
	body := map[string]any{"ResourceArn": ruleARN, "Limit": 7}
	for pages := 0; ; pages++ {
		require.Less(t, pages, 20, "pagination terminates")
		tags, next, status, code, message := configListTagsRaw(t, ts, body)
		require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
		require.LessOrEqual(t, len(tags), 7)
		for key, value := range configTagsToMap(t, tags) {
			_, dup := seen[key]
			require.False(t, dup, "%s appears on two pages", key)
			seen[key] = value
		}
		if next == "" {
			break
		}
		body["NextToken"] = next
	}
	assert.Len(t, seen, 50, "every tag appeared exactly once")
}

func TestConfigTags_ABadNextTokenIsRefused(t *testing.T) {
	// ListTagsForResource declares **both** InvalidLimitException and
	// InvalidNextTokenException, which is unusual — most paginated Config operations
	// declare one or the other — so each complaint is answered with the code that
	// describes it rather than with whichever one the cluster happens to share.
	ts, _, ruleARN, _ := configTagServer(t)
	configTagOK(t, ts, ruleARN, configTag("env", "prod"))

	for _, token := range []string{"not-base64!!", "bm9zdWNoa2V5"} {
		_, _, status, code, _ := configListTagsRaw(t, ts,
			map[string]any{"ResourceArn": ruleARN, "NextToken": token})
		assert.Equal(t, http.StatusBadRequest, status, token)
		assert.Equal(t, "InvalidNextTokenException", code, token)
	}
}

func TestConfigTags_GoWithTheResourceOnDelete(t *testing.T) {
	// "If you delete a resource, any tags for the resource are also deleted." Leaving
	// them would make a rebuilt resource of the same name inherit its predecessor's
	// tags — which no AWS account does, and which a fixture that tears down and rebuilds
	// would see as a tag it never set.
	//
	// The ARN is deterministic, so a rebuilt resource has the same ARN and would read
	// the stale tags back if the delete had not removed them. That is what makes this
	// assertable at all.
	ts, _, ruleARN, packARN := configTagServer(t)
	configTagOK(t, ts, ruleARN, configTag("env", "prod"))
	configTagOK(t, ts, packARN, configTag("env", "prod"))

	status, code, message := decodeConfigResponse(t,
		configRequest(t, ts, "DeleteConfigRule", map[string]any{"ConfigRuleName": "s3-encrypted"}), nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	// A pack must be observed before it is deletable: its state advances to
	// CREATE_COMPLETE on the first observation, and a delete arriving while it is still
	// CREATE_IN_PROGRESS is ResourceInUseException.
	require.Equal(t, "CREATE_COMPLETE", configPackStateOf(t, ts, "ops")["ConformancePackState"])
	status, code, message = configDeletePack(t, ts, "ops")
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	configPutRule(t, ts, "s3-encrypted")
	rebuilt := configPutPack(t, ts, "ops")
	require.Equal(t, packARN, rebuilt, "the rebuilt pack has the same ARN")

	assert.Empty(t, configListTags(t, ts, ruleARN), "the rebuilt rule carries no stale tags")
	assert.Empty(t, configListTags(t, ts, packARN), "the rebuilt pack carries no stale tags")
}

func TestConfigTags_RecorderTagsGoWithTheRecorder(t *testing.T) {
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	arn := configRecorderARN(t, ts, "us-east-1")
	configTagOK(t, ts, arn, configTag("env", "prod"))

	status, code, message := decodeConfigResponse(t, configRequest(t, ts,
		"DeleteConfigurationRecorder", map[string]any{"ConfigurationRecorderName": "default"}), nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	configPutRecorder(t, ts, "default")
	require.Equal(t, arn, configRecorderARN(t, ts, "us-east-1"),
		"the rebuilt recorder has the same ARN")
	assert.Empty(t, configListTags(t, ts, arn))
}

func TestConfigTags_AreClearedRatherThanStoredEmpty(t *testing.T) {
	// Removing the last tag deletes the state key rather than storing "{}", so a state
	// dump of a fully-untagged resource carries no shadow of the tags it once had.
	ts, _, ruleARN, _ := configTagServer(t)
	configTagOK(t, ts, ruleARN, configTag("env", "prod"))

	status, code, message := configUntagResource(t, ts, ruleARN, []string{"env"})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	assert.Nil(t, configStoredTags(t, ts, ruleARN), "the key is gone, not an empty map")
}

func TestConfigTags_AnUnsupportedTagOperationIsRefusedByName(t *testing.T) {
	// AWS Config has 97 operations; substrate implements the detective-controls subset.
	// An unclaimed one names itself so a consumer discovers which call is missing.
	ts := emulator.StartTestServer(t)

	status, code, message := decodeConfigResponse(t,
		configRequest(t, ts, "ListResourceEvaluations", map[string]any{}), nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidAction", code)
	assert.Contains(t, message, "ListResourceEvaluations")
}

// configTagsOfN builds n distinct tags, named so their sort order is stable.
func configTagsOfN(n int) []map[string]any {
	tags := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		tags = append(tags, configTag(fmt.Sprintf("tag-%02d", i), fmt.Sprintf("value-%02d", i)))
	}
	return tags
}
