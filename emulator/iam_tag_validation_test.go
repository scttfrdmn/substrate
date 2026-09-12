package emulator_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// IAM tag validation (#806).
//
// Every one of the twelve tagging paths accepted anything at all before this: fifty-one tags, a
// key beginning with the reserved `aws:` prefix, an empty key, a character outside the set AWS
// publishes as a regex. And `Department` and `department` were two tags on a user, where AWS says
// they are one.
//
// The assertions run through the **form protocol** rather than the JSON body, because that is the
// wire every real client uses and the one the drift lived on: `iamMemberTags` decodes
// `Tags.member.N` and used to silently drop an empty member, so a JSON-only test could not have
// seen the empty key arrive at all.
//
// Which code answers which rule is substrate's mapping over what each operation's Errors list
// makes available — `ValidationError`/400 for a violated shape constraint (the only 400 the untag
// operations declare), `InvalidInput`/400 for the reserved prefix, `LimitExceeded`/409 for the
// total. See `iam_tag_validation.go` for the reasoning; these tests pin it.

// iamTagParams renders tags as the query-protocol members a tagging operation reads, so a case
// can name what it sends rather than spelling out Tags.member.N twice per tag.
func iamTagParams(tags ...[2]string) map[string]string {
	params := make(map[string]string, 2*len(tags))
	for i, t := range tags {
		n := strconv.Itoa(i + 1)
		params["Tags.member."+n+".Key"] = t[0]
		params["Tags.member."+n+".Value"] = t[1]
	}
	return params
}

// iamTagKeyParams renders keys as the query-protocol members an untag operation reads.
func iamTagKeyParams(keys ...string) map[string]string {
	params := make(map[string]string, len(keys))
	for i, k := range keys {
		params["TagKeys.member."+strconv.Itoa(i+1)] = k
	}
	return params
}

// iamGeneratedTags returns n distinct tags, keyed tag-00 upward so their sort order is their
// generation order.
func iamGeneratedTags(n int) [][2]string {
	tags := make([][2]string, 0, n)
	for i := range n {
		tags = append(tags, [2]string{fmt.Sprintf("tag-%02d", i), "v"})
	}
	return tags
}

func TestIAMTagValidation_ARefusalPerRule(t *testing.T) {
	// One case per rule AWS publishes, swept across all four taggable entity types — the rules
	// are properties of the `Tag` data type, so a rule enforced on a user and not on an instance
	// profile would be the same defect #806 reports.
	t.Parallel()

	rules := []struct {
		name     string
		tags     [][2]string
		wantCode string
		wantHTTP int
	}{
		{
			// "You cannot create an empty tag key" — and the model agrees twice over,
			// through the key's minimum length of 1 and its pattern's + quantifier.
			name:     "an empty key",
			tags:     [][2]string{{"", "prod"}},
			wantCode: "ValidationError", wantHTTP: http.StatusBadRequest,
		},
		{
			// Key — "Maximum length of 128".
			name:     "a key of 129 characters",
			tags:     [][2]string{{strings.Repeat("k", 129), "prod"}},
			wantCode: "ValidationError", wantHTTP: http.StatusBadRequest,
		},
		{
			// Value — "Maximum length of 256".
			name:     "a value of 257 characters",
			tags:     [][2]string{{"env", strings.Repeat("v", 257)}},
			wantCode: "ValidationError", wantHTTP: http.StatusBadRequest,
		},
		{
			// Key — Pattern: [\p{L}\p{Z}\p{N}_.:/=+\-@]+ , which has no ! in it.
			name:     "a key outside the published character set",
			tags:     [][2]string{{"env!", "prod"}},
			wantCode: "ValidationError", wantHTTP: http.StatusBadRequest,
		},
		{
			// Value — the same class, so the same character is refused there.
			name:     "a value outside the published character set",
			tags:     [][2]string{{"env", "prod!"}},
			wantCode: "ValidationError", wantHTTP: http.StatusBadRequest,
		},
		{
			// "You cannot create a tag key or value that begins with the text aws:."
			name:     "a key beginning with the reserved prefix",
			tags:     [][2]string{{"aws:owner", "platform"}},
			wantCode: "InvalidInput", wantHTTP: http.StatusBadRequest,
		},
		{
			// The same sentence covers the value, which is where IAM is stricter than EC2 —
			// EC2's equivalent restriction names keys only.
			name:     "a value beginning with the reserved prefix",
			tags:     [][2]string{{"owner", "aws:platform"}},
			wantCode: "InvalidInput", wantHTTP: http.StatusBadRequest,
		},
		{
			// "Array Members: Maximum number of 50 items", on Tags.member.N.
			name:     "fifty-one members in one request",
			tags:     iamGeneratedTags(51),
			wantCode: "ValidationError", wantHTTP: http.StatusBadRequest,
		},
	}

	for _, entity := range iamTaggedEntities {
		for _, rule := range rules {
			t.Run(entity.name+"/"+rule.name, func(t *testing.T) {
				t.Parallel()
				srv := newIAMTestServer(t)
				id := entity.seed(t, srv, nil)

				code := iamFormErrorCode(t, srv, entity.tag,
					iamMergeParams(id, iamTagParams(rule.tags...)), rule.wantHTTP)
				assert.Equal(t, rule.wantCode, code, "%s with %s", entity.tag, rule.name)

				// The refusal is total: nothing from the request reached the entity.
				assert.Empty(t, iamTagListing(t, srv, entity.list, id),
					"a refused %s must store nothing", entity.tag)
			})
		}
	}
}

func TestIAMTagValidation_TheCharacterSetIsUnicode(t *testing.T) {
	// The patterns are `\p{L}`, `\p{Z}` and `\p{N}` based, so this is not the ASCII whitelist the
	// User Guide's prose rendering ("letters, numbers, spaces, and _ . : / = + - @") reads like.
	// Compiling AWS's published regex rather than hand-rolling the prose is what makes these
	// accepted; a whitelist derived from the prose would refuse all three.
	t.Parallel()

	accepted := [][2]string{
		{"Abteilung", "Zürich"},       // \p{L} beyond ASCII, on the value.
		{"Kostenstelle-Zürich", "42"}, // ...and on the key.
		{"Cost Center", "12345"},      // \p{Z}: a space, which AWS's own TagRole sample sends.
		{"部門", "エンジニアリング"},            // \p{L} in another script entirely.
		{"weight", "١٢٣"},             // \p{N}: Arabic-Indic digits.
		{"phoneNumber", ""},           // "You can create a tag with an empty value."
		{"punctuation", "_.:/=+-@"},   // every literal the class names, together.
	}

	for _, entity := range iamTaggedEntities {
		t.Run(entity.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)
			id := entity.seed(t, srv, nil)

			iamFormOK(t, srv, entity.tag, iamMergeParams(id, iamTagParams(accepted...)))

			got := iamTagListing(t, srv, entity.list, id)
			for _, tag := range accepted {
				value, ok := got[tag[0]]
				require.True(t, ok, "%s should have stored key %q", entity.tag, tag[0])
				assert.Equal(t, tag[1], value, "value for key %q", tag[0])
			}
		})
	}
}

func TestIAMTagValidation_TheFiftyFirstKeyIsRefusedAndAnOverwriteIsNot(t *testing.T) {
	// The cap is counted over the **post-merge** key set, the way ec2CheckTagLimit counts it: a
	// request that rewrites a value on an entity already holding fifty tags adds no key and
	// succeeds, while one adding a fifty-first is refused with LimitExceeded — the 409 the
	// operations document, which is *not* EC2's 400 for the same refusal. Counting the request
	// against the stored total would refuse both.
	t.Parallel()

	for _, entity := range iamTaggedEntities {
		t.Run(entity.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)
			id := entity.seed(t, srv, nil)

			fifty := iamGeneratedTags(50)
			iamFormOK(t, srv, entity.tag, iamMergeParams(id, iamTagParams(fifty...)))
			require.Len(t, iamTagListing(t, srv, entity.list, id), 50)

			// An overwrite of a key already there consumes no room.
			iamFormOK(t, srv, entity.tag,
				iamMergeParams(id, iamTagParams([2]string{"tag-00", "rewritten"})))
			assert.Equal(t, "rewritten", iamTagListing(t, srv, entity.list, id)["tag-00"])

			// A fifty-first key does not fit.
			code := iamFormErrorCode(t, srv, entity.tag,
				iamMergeParams(id, iamTagParams([2]string{"one-too-many", "v"})),
				http.StatusConflict)
			assert.Equal(t, "LimitExceeded", code)

			after := iamTagListing(t, srv, entity.list, id)
			assert.Len(t, after, 50, "the refused tag must not have been stored")
			assert.NotContains(t, after, "one-too-many")
		})
	}
}

func TestIAMTagValidation_CaseSensitivityFollowsTheEntityType(t *testing.T) {
	// AWS's split, and the User Guide's own worked example: "If you have tagged a user with the
	// Department=finance tag and you add the department=hr tag, it replaces the first tag. A
	// second tag is not added." — against — "if you have tagged a customer managed policy with
	// the Costcenter = 1234 tag and you add the costcenter = 5678 tag, the policy will have both
	// the Costcenter and costcenter tag keys."
	//
	// The insensitive side also has to preserve the stored spelling ("but case is preserved"), so
	// the surviving key is `Department` and not `department`. A merge that took the incoming
	// spelling would silently rename a key a policy might be matching on.
	t.Parallel()

	for _, entity := range iamTaggedEntities {
		t.Run(entity.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)
			id := entity.seed(t, srv, nil)

			iamFormOK(t, srv, entity.tag,
				iamMergeParams(id, iamTagParams([2]string{"Department", "finance"})))
			iamFormOK(t, srv, entity.tag,
				iamMergeParams(id, iamTagParams([2]string{"department", "hr"})))

			got := iamTagListing(t, srv, entity.list, id)
			if entity.keysAreCaseSensitive {
				assert.Equal(t, map[string]string{"Department": "finance", "department": "hr"}, got)
				return
			}
			assert.Equal(t, map[string]string{"Department": "hr"}, got,
				"one tag, the stored spelling, the new value")
		})
	}
}

func TestIAMTagValidation_AnUntagFollowsTheEntityTypesCaseRule(t *testing.T) {
	// The case rule has to reach the removal as well as the merge, and the reason is not
	// symmetry: if `Department` and `department` cannot both exist on a user, then untagging
	// `DEPARTMENT` has to remove the user's `Department` — otherwise a caller is left holding a
	// tag no spelling it can send will delete. On a policy the same request correctly removes
	// nothing, because there the two keys are two tags.
	t.Parallel()

	for _, entity := range iamTaggedEntities {
		t.Run(entity.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)
			id := entity.seed(t, srv, nil)

			iamFormOK(t, srv, entity.tag,
				iamMergeParams(id, iamTagParams([2]string{"Department", "finance"})))
			iamFormOK(t, srv, entity.untag,
				iamMergeParams(id, iamTagKeyParams("DEPARTMENT")))

			got := iamTagListing(t, srv, entity.list, id)
			if entity.keysAreCaseSensitive {
				assert.Equal(t, map[string]string{"Department": "finance"}, got,
					"a differently-cased key names a different tag here")
				return
			}
			assert.Empty(t, got, "the differently-cased key names the same tag here")
		})
	}
}

func TestIAMTagValidation_UntagKeysAreValidatedToo(t *testing.T) {
	// `TagKeys.member.N` carries the same constraints as a tag key — "Array Members: Maximum
	// number of 50 items", "Minimum length of 1. Maximum length of 128", and the key pattern — so
	// the same checks apply. `InvalidInput` is deliberately absent from the untag operations'
	// Errors lists, which is why a shape violation answers `ValidationError` here and on the
	// tag-writing paths alike.
	t.Parallel()

	cases := []struct {
		name     string
		keys     []string
		wantCode string
	}{
		{"an empty key", []string{""}, "ValidationError"},
		{"a key of 129 characters", []string{strings.Repeat("k", 129)}, "ValidationError"},
		{"a key outside the character set", []string{"env!"}, "ValidationError"},
		{"fifty-one keys", func() []string {
			keys := make([]string, 0, 51)
			for _, t := range iamGeneratedTags(51) {
				keys = append(keys, t[0])
			}
			return keys
		}(), "ValidationError"},
		// A caller cannot create such a key, so no entity can hold one; naming it for removal is
		// refused rather than quietly matching nothing.
		{"a reserved key", []string{"aws:cloudformation:stack-name"}, "InvalidInput"},
	}

	for _, entity := range iamTaggedEntities {
		for _, tc := range cases {
			t.Run(entity.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				srv := newIAMTestServer(t)
				id := entity.seed(t, srv, map[string]string{
					"Tags.member.1.Key":   "keep",
					"Tags.member.1.Value": "me",
				})

				code := iamFormErrorCode(t, srv, entity.untag,
					iamMergeParams(id, iamTagKeyParams(tc.keys...)), http.StatusBadRequest)
				assert.Equal(t, tc.wantCode, code)
				assert.Equal(t, map[string]string{"keep": "me"},
					iamTagListing(t, srv, entity.list, id),
					"a refused %s must remove nothing", entity.untag)
			})
		}
	}
}

func TestIAMTagValidation_ACreateWithABadTagLeavesNoEntity(t *testing.T) {
	// `CreateUser`'s Tags.member.N, and the same note on the other three creates: "If any one of
	// the tags is invalid or if you exceed the allowed maximum number of tags, then the entire
	// request fails and the resource is not created." So the validation has to run before the
	// write, and the entity read afterwards has to answer NoSuchEntity.
	t.Parallel()

	bad := []struct {
		name     string
		tags     [][2]string
		wantCode string
		wantHTTP int
	}{
		{"a reserved key", [][2]string{{"aws:owner", "platform"}},
			"InvalidInput", http.StatusBadRequest},
		{"an empty key", [][2]string{{"", "prod"}},
			"ValidationError", http.StatusBadRequest},
		{"fifty-one tags", iamGeneratedTags(51),
			"ValidationError", http.StatusBadRequest},
	}

	for _, entity := range iamTaggedEntities {
		for _, tc := range bad {
			t.Run(entity.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				srv := newIAMTestServer(t)

				code := iamFormErrorCode(t, srv, entity.create,
					iamMergeParams(entity.createParams, iamTagParams(tc.tags...)), tc.wantHTTP)
				assert.Equal(t, tc.wantCode, code, entity.create)

				assert.Equal(t, "NoSuchEntity",
					iamFormErrorCode(t, srv, entity.get, entity.identify, http.StatusNotFound),
					"%s must not have created the entity", entity.create)
			})
		}
	}
}

func TestIAMTagValidation_ACreateCollapsesADuplicateKeyUnderTheTypesCaseRule(t *testing.T) {
	// A create has to apply the same case rule its Tag* operation does, or an entity can be born
	// holding two keys that no later TagUser could ever produce — the state would violate the
	// invariant the entity type documents. Merging the request against an empty set also sorts
	// it, which is the order the listing operations document.
	t.Parallel()

	for _, entity := range iamTaggedEntities {
		t.Run(entity.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)
			id := entity.seed(t, srv, iamTagParams(
				[2]string{"Department", "finance"},
				[2]string{"department", "hr"},
			))

			got := iamTagListing(t, srv, entity.list, id)
			if entity.keysAreCaseSensitive {
				assert.Equal(t, map[string]string{"Department": "finance", "department": "hr"}, got)
				return
			}
			assert.Equal(t, map[string]string{"Department": "hr"}, got)
		})
	}
}

func TestIAMTagValidation_AnUnauthorizedCallerLearnsNothingAboutItsPayload(t *testing.T) {
	// The order of the two gates is deliberate and is worth pinning: authorization first, then the
	// tags. A caller without iam:TagPolicy on the named policy that sends an illegal tag is told
	// AccessDenied — the same answer it gets for a legal one — rather than being told which of its
	// tags was malformed on a resource it cannot touch.
	//
	// Driven through the plugin-door harness, which is the only server in the suite where an
	// IAM handler's own gate reaches a verdict; see
	// [TestIAMTagging_TheNewOperationsAreGatedAtThePluginDoor].
	srv := newIAMPluginDoorServer(t)

	for _, name := range []string{"app", "other"} {
		require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "CreatePolicy", map[string]any{
			"PolicyName":     name,
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		}).StatusCode)
	}

	keyID := trustSetupCaller(t, srv, "alice")
	require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "PutUserPolicy", map[string]any{
		"UserName":   "alice",
		"PolicyName": "app-only",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Action":"iam:TagPolicy",` +
			`"Resource":"arn:aws:iam::123456789012:policy/app"}]}`,
	}).StatusCode)

	tagAs := func(t *testing.T, arn string, tags []map[string]string) int {
		t.Helper()
		raw, err := json.Marshal(map[string]any{"PolicyArn": arn, "Tags": tags})
		require.NoError(t, err)
		r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
		r.Host = "iam.amazonaws.com"
		r.Header.Set("X-Amz-Target", "AmazonIdentityManagementService.TagPolicy")
		r.Header.Set("Content-Type", "application/x-amz-json-1.1")
		r.Header.Set("Authorization", trustAuthHeader(keyID, "iam"))
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		resp := w.Result()
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode
	}

	reserved := []map[string]string{{"Key": "aws:owner", "Value": "platform"}}
	assert.Equal(t, http.StatusForbidden,
		tagAs(t, "arn:aws:iam::123456789012:policy/other", reserved),
		"the payload is judged only after the caller is allowed to send one")
	assert.Equal(t, http.StatusBadRequest,
		tagAs(t, "arn:aws:iam::123456789012:policy/app", reserved),
		"and is judged once the caller is allowed")
}
