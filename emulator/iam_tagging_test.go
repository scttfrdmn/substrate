package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The IAM tagging operations for policies and instance profiles (#796).
//
// Six operations reached the dispatcher's default arm and answered `InvalidAction` / HTTP 400:
// `TagPolicy`, `UntagPolicy`, `ListPolicyTags`, `TagInstanceProfile`, `UntagInstanceProfile`,
// `ListInstanceProfileTags`. A consumer could tag a user or a role and neither a policy nor an
// instance profile, which is the other half of the drift #796a fixed on the read side: the CDK's
// tag aspect tags every entity it creates.
//
// The listing shape is AWS's, and the same on all six: `Tags` is a *required* response member —
// "If no tags are attached to the specified resource, the response contains an empty list" — the
// list "is sorted by tag key", `MaxItems` defaults to 100, and `Marker` appears only when
// `IsTruncated` is true. That is the opposite of the entity shapes, where `Tags` is
// `Required: No` and an untagged entity omits the member; see iam_entity_tags_test.go.
//
// The user and role pairs are swept alongside the four new ones wherever the assertion is about
// the shared semantics rather than about one entity type, because all six listings now render
// through one helper — so a regression in the sort or the pagination would otherwise be reported
// for two of six.

// iamTagListing lists a resource's tags through operation, requiring a 200, and returns the
// keys and values it reported.
func iamTagListing(t *testing.T, srv *emulator.Server, operation string,
	params map[string]string) map[string]string {
	t.Helper()
	resp := iamFormRequest(t, srv, operation, params)
	require.Equal(t, http.StatusOK, resp.StatusCode, operation)
	return iamFormTagNames(t, resp)
}

// iamTagKeyOrder returns the tag keys a listing response reports, in the order it reported
// them. [iamFormTagNames] decodes into a map, which cannot answer "sorted by tag key".
func iamTagKeyOrder(body string) []string {
	keys := []string{}
	rest := body
	for {
		open := strings.Index(rest, "<Key>")
		if open < 0 {
			return keys
		}
		rest = rest[open+len("<Key>"):]
		end := strings.Index(rest, "</Key>")
		if end < 0 {
			return keys
		}
		keys = append(keys, rest[:end])
		rest = rest[end:]
	}
}

// iamFormErrorCode returns the error code a form-protocol IAM response reports, requiring the
// status the caller expected. An SDK maps the Code string onto a typed exception, so the code is
// the assertion and the status alone is not.
func iamFormErrorCode(t *testing.T, srv *emulator.Server, operation string,
	params map[string]string, wantHTTP int) string {
	t.Helper()
	resp := iamFormRequest(t, srv, operation, params)
	require.Equal(t, wantHTTP, resp.StatusCode, operation)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	code, _ := result["__type"].(string)
	return code
}

// iamSeedTaggablePolicy creates a customer-managed policy and returns its ARN.
func iamSeedTaggablePolicy(t *testing.T, srv *emulator.Server, name string) string {
	t.Helper()
	iamFormOK(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": name,
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Action":"s3:GetObject","Resource":"*"}]}`,
	})
	return "arn:aws:iam::123456789012:policy/" + name
}

func TestIAMTagging_APolicyTagRoundTrip(t *testing.T) {
	// The whole cycle for the pair that did not exist: list before any tag, tag, list, overwrite
	// a key, untag, list again — and finally the entity read, so the two halves of #796 are
	// asserted to agree about one record rather than each about its own.
	t.Parallel()
	srv := newIAMTestServer(t)
	arn := iamSeedTaggablePolicy(t, srv, "reader")

	assert.Empty(t, iamTagListing(t, srv, "ListPolicyTags", map[string]string{"PolicyArn": arn}),
		"a resource with no tags reports an empty list, not an error")

	iamFormOK(t, srv, "TagPolicy", map[string]string{
		"PolicyArn":           arn,
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
		"Tags.member.2.Key":   "team",
		"Tags.member.2.Value": "infra",
	})
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"},
		iamTagListing(t, srv, "ListPolicyTags", map[string]string{"PolicyArn": arn}))

	// "If a tag with the same key name already exists, then that tag is overwritten with the
	// new value" — a second TagPolicy merges rather than replacing the set.
	iamFormOK(t, srv, "TagPolicy", map[string]string{
		"PolicyArn":           arn,
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "staging",
		"Tags.member.2.Key":   "owner",
		"Tags.member.2.Value": "platform",
	})
	assert.Equal(t, map[string]string{"env": "staging", "team": "infra", "owner": "platform"},
		iamTagListing(t, srv, "ListPolicyTags", map[string]string{"PolicyArn": arn}))

	// TagKeys is a flat member list rather than a struct one, so it exercises the other
	// decoder (#639). Removing one of three proves the keys are read rather than the set
	// being dropped.
	iamFormOK(t, srv, "UntagPolicy", map[string]string{
		"PolicyArn":        arn,
		"TagKeys.member.1": "team",
	})
	assert.Equal(t, map[string]string{"env": "staging", "owner": "platform"},
		iamTagListing(t, srv, "ListPolicyTags", map[string]string{"PolicyArn": arn}))

	// Removing a key that is not there is not an error: AWS's untag operations declare none
	// for it, and a consumer removing a tag it is unsure of should not have to read first.
	iamFormOK(t, srv, "UntagPolicy", map[string]string{
		"PolicyArn":        arn,
		"TagKeys.member.1": "absent",
	})
	assert.Equal(t, map[string]string{"env": "staging", "owner": "platform"},
		iamTagListing(t, srv, "ListPolicyTags", map[string]string{"PolicyArn": arn}))

	// An empty TagKeys removes nothing rather than clearing the set, which is the one answer
	// that cannot lose a consumer's tags. AWS marks TagKeys `Required: Yes` and would refuse the
	// request with InvalidInput; substrate does not validate yet (#806), so what it must not do
	// is treat "no keys named" as "every key".
	iamFormOK(t, srv, "UntagPolicy", map[string]string{"PolicyArn": arn})
	assert.Equal(t, map[string]string{"env": "staging", "owner": "platform"},
		iamTagListing(t, srv, "ListPolicyTags", map[string]string{"PolicyArn": arn}))

	// And the entity read reports the same set, which is #796a's half of the issue.
	resp := iamFormRequest(t, srv, "GetPolicy", map[string]string{"PolicyArn": arn})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"env": "staging", "owner": "platform"},
		iamEntityTags(t, resp, "Policy"))
}

func TestIAMTagging_AnInstanceProfileTagRoundTrip(t *testing.T) {
	// The same cycle for the other new pair. It is a separate test rather than a table row
	// because the two load their records from different state keys, so a fix applied to one
	// would leave the other untouched.
	t.Parallel()
	srv := newIAMTestServer(t)
	iamFormOK(t, srv, "CreateInstanceProfile", map[string]string{"InstanceProfileName": "web"})

	assert.Empty(t, iamTagListing(t, srv, "ListInstanceProfileTags",
		map[string]string{"InstanceProfileName": "web"}))

	iamFormOK(t, srv, "TagInstanceProfile", map[string]string{
		"InstanceProfileName": "web",
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
		"Tags.member.2.Key":   "team",
		"Tags.member.2.Value": "infra",
	})
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"},
		iamTagListing(t, srv, "ListInstanceProfileTags",
			map[string]string{"InstanceProfileName": "web"}))

	iamFormOK(t, srv, "UntagInstanceProfile", map[string]string{
		"InstanceProfileName": "web",
		"TagKeys.member.1":    "env",
	})
	assert.Equal(t, map[string]string{"team": "infra"},
		iamTagListing(t, srv, "ListInstanceProfileTags",
			map[string]string{"InstanceProfileName": "web"}))

	resp := iamFormRequest(t, srv, "GetInstanceProfile",
		map[string]string{"InstanceProfileName": "web"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, map[string]string{"team": "infra"}, iamEntityTags(t, resp, "InstanceProfile"))
}

func TestIAMTagging_CreateTimeTagsAreListed(t *testing.T) {
	// The create path and the listing path have to agree about one record. `CreatePolicy` and
	// `CreateInstanceProfile` only started accepting `Tags.member.N` in #796a, and until this
	// PR there was no listing operation to read them back with at all.
	t.Parallel()
	srv := newIAMTestServer(t)

	iamFormOK(t, srv, "CreatePolicy", map[string]string{
		"PolicyName":          "reader",
		"PolicyDocument":      `{"Version":"2012-10-17","Statement":[]}`,
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
	})
	assert.Equal(t, map[string]string{"env": "prod"},
		iamTagListing(t, srv, "ListPolicyTags", map[string]string{
			"PolicyArn": "arn:aws:iam::123456789012:policy/reader",
		}))

	iamFormOK(t, srv, "CreateInstanceProfile", map[string]string{
		"InstanceProfileName": "web",
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
	})
	assert.Equal(t, map[string]string{"env": "prod"},
		iamTagListing(t, srv, "ListInstanceProfileTags",
			map[string]string{"InstanceProfileName": "web"}))
}

// iamTaggedEntity is one taggable IAM entity type, for the assertions that are about the
// semantics all six listings share rather than about one type.
type iamTaggedEntity struct {
	name string
	// seed creates the entity carrying the tags in extra, and returns the identifier its
	// tagging operations name it by.
	seed func(t *testing.T, srv *emulator.Server, extra map[string]string) map[string]string
	tag  string
	list string

	// The remaining fields let a case drive the type's create and untag paths directly rather
	// than only through seed, which requires a 200 — a create that must be *refused* cannot use
	// it, and neither can an untag whose keys are the thing under test (#806).
	untag string
	// create and createParams are the operation and the minimum parameters that create the
	// entity, without any tags.
	create       string
	createParams map[string]string
	// identify names the entity for its tagging operations and for get, and is what seed
	// returns. It is spelled out here so a case that never seeds still has it.
	identify map[string]string
	// get is the single-entity read, for asserting an entity was not created.
	get string
	// keysAreCaseSensitive is AWS's split, from *Tagging IAM resources*: "Tag key values for IAM
	// users and roles are not case sensitive, but case is preserved. […] For other IAM resource
	// types, tag key values are case sensitive."
	keysAreCaseSensitive bool
}

// iamTaggedEntities covers all four taggable types, so the shared listing helper is asserted
// for the user and role pairs that predate it as well as for the four operations #796 adds.
var iamTaggedEntities = []iamTaggedEntity{
	{
		name: "a user",
		tag:  "TagUser", list: "ListUserTags", untag: "UntagUser", get: "GetUser",
		create:       "CreateUser",
		createParams: map[string]string{"UserName": "jill"},
		identify:     map[string]string{"UserName": "jill"},
		seed: func(t *testing.T, srv *emulator.Server, extra map[string]string) map[string]string {
			t.Helper()
			id := map[string]string{"UserName": "jill"}
			iamFormOK(t, srv, "CreateUser", iamMergeParams(id, extra))
			return id
		},
	},
	{
		name: "a role",
		tag:  "TagRole", list: "ListRoleTags", untag: "UntagRole", get: "GetRole",
		create: "CreateRole",
		createParams: map[string]string{
			"RoleName":                 "worker",
			"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		},
		identify: map[string]string{"RoleName": "worker"},
		seed: func(t *testing.T, srv *emulator.Server, extra map[string]string) map[string]string {
			t.Helper()
			id := map[string]string{"RoleName": "worker"}
			iamFormOK(t, srv, "CreateRole", iamMergeParams(map[string]string{
				"RoleName":                 "worker",
				"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			}, extra))
			return id
		},
	},
	{
		name: "a customer-managed policy",
		tag:  "TagPolicy", list: "ListPolicyTags", untag: "UntagPolicy", get: "GetPolicy",
		create: "CreatePolicy",
		createParams: map[string]string{
			"PolicyName":     "reader",
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		},
		identify:             map[string]string{"PolicyArn": "arn:aws:iam::123456789012:policy/reader"},
		keysAreCaseSensitive: true,
		seed: func(t *testing.T, srv *emulator.Server, extra map[string]string) map[string]string {
			t.Helper()
			iamFormOK(t, srv, "CreatePolicy", iamMergeParams(map[string]string{
				"PolicyName":     "reader",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			}, extra))
			return map[string]string{"PolicyArn": "arn:aws:iam::123456789012:policy/reader"}
		},
	},
	{
		name: "an instance profile",
		tag:  "TagInstanceProfile", list: "ListInstanceProfileTags",
		untag: "UntagInstanceProfile", get: "GetInstanceProfile",
		create:               "CreateInstanceProfile",
		createParams:         map[string]string{"InstanceProfileName": "web"},
		identify:             map[string]string{"InstanceProfileName": "web"},
		keysAreCaseSensitive: true,
		seed: func(t *testing.T, srv *emulator.Server, extra map[string]string) map[string]string {
			t.Helper()
			id := map[string]string{"InstanceProfileName": "web"}
			iamFormOK(t, srv, "CreateInstanceProfile", iamMergeParams(id, extra))
			return id
		},
	},
}

// iamMergeParams returns base with extra merged over it, leaving base untouched — the seeds
// above are shared by subtests that run in parallel.
func iamMergeParams(base, extra map[string]string) map[string]string {
	merged := make(map[string]string, len(base)+len(extra))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range extra {
		merged[k] = v
	}
	return merged
}

func TestIAMTagging_AListingIsSortedByKey(t *testing.T) {
	// "The returned list of tags is sorted by tag key", on all six listing operations. This is
	// not cosmetic: the marker is a position in that order, so an unsorted underlying order
	// makes a second page arbitrary. Tags are supplied deliberately out of order, and every
	// type is swept — the user and role listings were unsorted before this PR routed all six
	// through one helper.
	t.Parallel()

	for _, entity := range iamTaggedEntities {
		t.Run(entity.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)
			id := entity.seed(t, srv, map[string]string{
				"Tags.member.1.Key":   "zone",
				"Tags.member.1.Value": "a",
				"Tags.member.2.Key":   "env",
				"Tags.member.2.Value": "prod",
				"Tags.member.3.Key":   "owner",
				"Tags.member.3.Value": "platform",
			})

			resp := iamFormRequest(t, srv, entity.list, id)
			body := iamResponseXML(t, resp)
			require.Equal(t, http.StatusOK, resp.StatusCode, body)
			assert.Equal(t, []string{"env", "owner", "zone"}, iamTagKeyOrder(body),
				"%s reports the tags sorted by key, whatever order they were stored in", entity.list)
		})
	}
}

func TestIAMTagging_AListingPaginates(t *testing.T) {
	// `MaxItems`, `IsTruncated` and `Marker`, on every listing. The response `Marker` "is only
	// present when there are more results", and a caller passes it back to get the next page;
	// three tags at MaxItems=2 makes the second page a genuine second page rather than the
	// whole set again.
	t.Parallel()

	for _, entity := range iamTaggedEntities {
		t.Run(entity.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)
			id := entity.seed(t, srv, map[string]string{
				"Tags.member.1.Key":   "env",
				"Tags.member.1.Value": "prod",
				"Tags.member.2.Key":   "owner",
				"Tags.member.2.Value": "platform",
				"Tags.member.3.Key":   "zone",
				"Tags.member.3.Value": "a",
			})

			page := func(marker string) map[string]any {
				params := iamMergeParams(id, map[string]string{"MaxItems": "2"})
				if marker != "" {
					params["Marker"] = marker
				}
				resp := iamFormRequest(t, srv, entity.list, params)
				require.Equal(t, http.StatusOK, resp.StatusCode)
				var result map[string]any
				decodeIAMXML(t, resp, &result)
				return result
			}

			first := page("")
			assert.Equal(t, true, first["IsTruncated"])
			assert.Equal(t, "zone", first["Marker"], "the marker names the next page's first key")
			require.Len(t, first["Tags"], 2)

			second := page("zone")
			assert.Equal(t, false, second["IsTruncated"])
			assert.NotContains(t, second, "Marker",
				"Marker is present only when IsTruncated is true")
			require.Len(t, second["Tags"], 1)

			// The default page is 100, so an unpaged call reports all three.
			assert.Len(t, iamTagListing(t, srv, entity.list, id), 3)
		})
	}
}

func TestIAMTagging_AnAbsentResourceIsNoSuchEntity(t *testing.T) {
	// `NoSuchEntity` / 404, which all six publish. The AWS managed policy rows are the
	// interesting ones: AWS documents these operations for an "IAM customer managed policy",
	// and a managed policy belongs to the `aws` account, so substrate's read-only catalog is
	// deliberately not resolved here.
	t.Parallel()

	const managed = "arn:aws:iam::aws:policy/ReadOnlyAccess"
	const absent = "arn:aws:iam::123456789012:policy/absent"

	for _, tc := range []struct {
		name      string
		operation string
		params    map[string]string
	}{
		{"TagPolicy on an absent policy", "TagPolicy", map[string]string{
			"PolicyArn": absent, "Tags.member.1.Key": "env", "Tags.member.1.Value": "prod",
		}},
		{"UntagPolicy on an absent policy", "UntagPolicy", map[string]string{
			"PolicyArn": absent, "TagKeys.member.1": "env",
		}},
		{"ListPolicyTags on an absent policy", "ListPolicyTags", map[string]string{
			"PolicyArn": absent,
		}},
		{"TagPolicy on an AWS managed policy", "TagPolicy", map[string]string{
			"PolicyArn": managed, "Tags.member.1.Key": "env", "Tags.member.1.Value": "prod",
		}},
		{"ListPolicyTags on an AWS managed policy", "ListPolicyTags", map[string]string{
			"PolicyArn": managed,
		}},
		{"TagInstanceProfile on an absent profile", "TagInstanceProfile", map[string]string{
			"InstanceProfileName": "absent",
			"Tags.member.1.Key":   "env", "Tags.member.1.Value": "prod",
		}},
		{"UntagInstanceProfile on an absent profile", "UntagInstanceProfile", map[string]string{
			"InstanceProfileName": "absent", "TagKeys.member.1": "env",
		}},
		{"ListInstanceProfileTags on an absent profile", "ListInstanceProfileTags",
			map[string]string{"InstanceProfileName": "absent"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newIAMTestServer(t)
			assert.Equal(t, "NoSuchEntity",
				iamFormErrorCode(t, srv, tc.operation, tc.params, http.StatusNotFound))
		})
	}

	t.Run("the managed rows are not vacuous", func(t *testing.T) {
		// GetPolicy resolves the same ARN from the catalog, so the 404s above are the tagging
		// operations declining a policy substrate holds rather than one it cannot find.
		t.Parallel()
		srv := newIAMTestServer(t)
		body := iamFormOK(t, srv, "GetPolicy", map[string]string{"PolicyArn": managed})
		assert.Contains(t, body, managed)
	})
}

func TestIAMTagging_AMissingIdentifierIsAValidationError(t *testing.T) {
	// Every one of the six requires its identifier, so a request without one is refused before
	// any record is read — the same `ValidationError` / 400 the rest of the plugin answers for
	// a missing required parameter.
	t.Parallel()
	srv := newIAMTestServer(t)

	for _, operation := range []string{
		"TagPolicy", "UntagPolicy", "ListPolicyTags",
		"TagInstanceProfile", "UntagInstanceProfile", "ListInstanceProfileTags",
	} {
		t.Run(operation, func(t *testing.T) {
			assert.Equal(t, "ValidationError",
				iamFormErrorCode(t, srv, operation, nil, http.StatusBadRequest))
		})
	}
}

func TestIAMTagging_AMalformedBodyIsAValidationError(t *testing.T) {
	// A body that is JSON but not an object — the shape a hand-rolled client or a mangled proxy
	// sends. All six refuse it as `ValidationError` / 400 before reading any record, which is
	// what the rest of the plugin answers for an unparseable body.
	t.Parallel()
	srv := newIAMTestServer(t)

	for _, operation := range []string{
		"TagPolicy", "UntagPolicy", "ListPolicyTags",
		"TagInstanceProfile", "UntagInstanceProfile", "ListInstanceProfileTags",
	} {
		t.Run(operation, func(t *testing.T) {
			resp := iamRequest(t, srv, operation, "not-an-object")
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, "ValidationError", result["__type"])
		})
	}
}

func TestIAMTagging_AStateFailureIsNotReportedAsNoSuchEntity(t *testing.T) {
	// A store failure and an absent record are opposite signals: `NoSuchEntity` tells a consumer
	// the resource is gone and to stop retrying, while a store failure is transient. Collapsing
	// the first into the second would send a consumer down a permanent-failure path over a blip,
	// which is why the loaders distinguish "no record" from "could not read" — the former is the
	// caller's 404, the latter is propagated and answered as a 500.
	t.Parallel()

	for _, tc := range iamTagStoreCases() {
		t.Run(tc.name+" via "+tc.operation, func(t *testing.T) {
			t.Parallel()
			state := &errAfterGetsStateManager{
				inner:  emulator.NewMemoryStateManager(),
				getErr: errors.New("state store unavailable"),
				allow:  math.MaxInt, // permissive until the record exists
			}
			srv := newIAMTestServerWithState(t, state)
			tc.seed(t, srv)

			// Every Get from here on fails, so the record cannot be read even though it was
			// written — the one case where an absent record and a broken store differ.
			state.allow = state.gets

			resp := iamRequest(t, srv, tc.operation, tc.body)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
				"a store failure is not the caller's NoSuchEntity")
		})
	}
}

// iamCorruptGetsStateManager is a StateManager that serves the first allow Get calls normally
// and then answers every one with a body that is not valid JSON. It reaches the one branch a
// failing store cannot: a record that reads back but does not decode.
type iamCorruptGetsStateManager struct {
	inner emulator.StateManager
	allow int
	gets  int
}

func (m *iamCorruptGetsStateManager) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	m.gets++
	if m.gets > m.allow {
		return []byte("{not json"), nil
	}
	return m.inner.Get(ctx, namespace, key)
}

func (m *iamCorruptGetsStateManager) Put(ctx context.Context, namespace, key string, value []byte) error {
	return m.inner.Put(ctx, namespace, key, value)
}

func (m *iamCorruptGetsStateManager) Delete(ctx context.Context, namespace, key string) error {
	return m.inner.Delete(ctx, namespace, key)
}

func (m *iamCorruptGetsStateManager) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	return m.inner.List(ctx, namespace, prefix)
}

// iamFailingPutStateManager is a StateManager whose writes start failing once armed, so a
// tagging operation's read-modify-*write* can be made to fail after the read succeeded.
type iamFailingPutStateManager struct {
	inner  emulator.StateManager
	fail   bool
	putErr error
}

func (m *iamFailingPutStateManager) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	return m.inner.Get(ctx, namespace, key)
}

func (m *iamFailingPutStateManager) Put(ctx context.Context, namespace, key string, value []byte) error {
	if m.fail {
		return m.putErr
	}
	return m.inner.Put(ctx, namespace, key, value)
}

func (m *iamFailingPutStateManager) Delete(ctx context.Context, namespace, key string) error {
	return m.inner.Delete(ctx, namespace, key)
}

func (m *iamFailingPutStateManager) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	return m.inner.List(ctx, namespace, prefix)
}

func TestIAMTagging_AnUndecodableRecordIsNotReportedAsNoSuchEntity(t *testing.T) {
	// The other half of the distinction above. A record that reads back but does not decode is
	// a corrupt store, not a missing resource, so it is propagated rather than answered 404 —
	// telling a consumer their policy is gone when the bytes are merely unreadable would send
	// them to delete-and-recreate over a store problem.
	t.Parallel()

	for _, tc := range iamTagStoreCases() {
		t.Run(tc.name+" via "+tc.operation, func(t *testing.T) {
			t.Parallel()
			state := &iamCorruptGetsStateManager{
				inner: emulator.NewMemoryStateManager(),
				allow: math.MaxInt,
			}
			srv := newIAMTestServerWithState(t, state)
			tc.seed(t, srv)
			state.allow = state.gets

			resp := iamRequest(t, srv, tc.operation, tc.body)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		})
	}
}

func TestIAMTagging_AFailedWriteIsReportedRatherThanSwallowed(t *testing.T) {
	// The write half: a tag operation whose read succeeded and whose store failed must not
	// answer 200. A consumer that got a success and no tags would have no way to tell the
	// difference from AWS accepting the call, which is the failure mode #639 was — TagUser
	// answered 200 and stored nothing.
	t.Parallel()

	for _, tc := range iamTagStoreCases() {
		if strings.HasPrefix(tc.operation, "List") {
			continue // a listing performs no write.
		}
		t.Run(tc.name+" via "+tc.operation, func(t *testing.T) {
			t.Parallel()
			state := &iamFailingPutStateManager{
				inner:  emulator.NewMemoryStateManager(),
				putErr: errors.New("state store unavailable"),
			}
			srv := newIAMTestServerWithState(t, state)
			tc.seed(t, srv)
			state.fail = true

			resp := iamRequest(t, srv, tc.operation, tc.body)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		})
	}
}

// iamTagStoreCase is a seeded resource and one tagging call against it, for the three
// store-failure sweeps above.
type iamTagStoreCase struct {
	name      string
	seed      func(t *testing.T, srv *emulator.Server)
	operation string
	body      any
}

// iamTagStoreCases covers every one of the six operations against a record that exists, so a
// store failure is asserted on each rather than only on the pair that happens to be listed.
func iamTagStoreCases() []iamTagStoreCase {
	const policyARN = "arn:aws:iam::123456789012:policy/reader"
	seedPolicy := func(t *testing.T, srv *emulator.Server) {
		t.Helper()
		require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreatePolicy", map[string]any{
			"PolicyName":     "reader",
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		}).StatusCode)
	}
	seedProfile := func(t *testing.T, srv *emulator.Server) {
		t.Helper()
		require.Equal(t, http.StatusOK, iamRequest(t, srv, "CreateInstanceProfile",
			map[string]any{"InstanceProfileName": "web"}).StatusCode)
	}
	tags := []map[string]string{{"Key": "env", "Value": "prod"}}

	return []iamTagStoreCase{
		{"a policy", seedPolicy, "ListPolicyTags", map[string]any{"PolicyArn": policyARN}},
		{"a policy", seedPolicy, "TagPolicy",
			map[string]any{"PolicyArn": policyARN, "Tags": tags}},
		{"a policy", seedPolicy, "UntagPolicy",
			map[string]any{"PolicyArn": policyARN, "TagKeys": []string{"env"}}},
		{"an instance profile", seedProfile, "ListInstanceProfileTags",
			map[string]any{"InstanceProfileName": "web"}},
		{"an instance profile", seedProfile, "TagInstanceProfile",
			map[string]any{"InstanceProfileName": "web", "Tags": tags}},
		{"an instance profile", seedProfile, "UntagInstanceProfile",
			map[string]any{"InstanceProfileName": "web", "TagKeys": []string{"env"}}},
	}
}

func TestIAMTagging_ThereIsNoGroupEquivalent(t *testing.T) {
	// A group is not a taggable IAM resource, so these three stay unrouted and keep answering
	// the Query protocol's unknown-action refusal. Four citations agree: the `Group` data type
	// documents no `Tags` member, the Actions index publishes no TagGroup/UntagGroup/
	// ListGroupTags, the vendored service-authorization snapshot has no `iam:*Group` tagging
	// action, and the IAM User Guide says it directly — "You can tag most IAM resources, but
	// not groups, assumed roles, access reports, or hardware-based MFA devices."
	//
	// Answering `InvalidAction` is the honest report that substrate models no such operation;
	// answering `NoSuchEntity` or 200 would tell a consumer the group was the problem.
	t.Parallel()
	srv := newIAMTestServer(t)

	// Not vacuous: the group exists, and the group operations substrate does model answer.
	iamFormOK(t, srv, "CreateGroup", map[string]string{"GroupName": "admins"})
	iamFormOK(t, srv, "GetGroup", map[string]string{"GroupName": "admins"})

	for _, operation := range []string{"TagGroup", "UntagGroup", "ListGroupTags"} {
		t.Run(operation, func(t *testing.T) {
			assert.Equal(t, "InvalidAction", iamFormErrorCode(t, srv, operation,
				map[string]string{"GroupName": "admins"}, http.StatusBadRequest))
		})
	}
}

// TestIAMTagging_TheNewOperationsAreGatedAtThePluginDoor is the plugin door's own verdict on the
// six operations, in the shape of [TestIAMPlugin_InstanceProfileOperationsAreGated].
//
// The server wires no [emulator.AuthController], so the generic gate does not run and every
// verdict below is the handler's own — which is the only way to assert that a *new* handler
// calls a gate at all. That both doors then name the same resource is asserted for free by
// TestIAMAuthzResource_BothDoorsAgree, which derives a case from every row of
// iamAuthzOperationResource including the six this PR adds.
//
// Tagging is a privilege-relevant operation rather than a bookkeeping one: an
// `aws:ResourceTag`-conditioned policy is decided on the resource's tags, so a caller able to
// retag an instance profile can move it in or out of the reach of every such statement.
func TestIAMTagging_TheNewOperationsAreGatedAtThePluginDoor(t *testing.T) {
	srv := newIAMPluginDoorServer(t)

	const namedPolicy = "arn:aws:iam::123456789012:policy/app"
	const otherPolicy = "arn:aws:iam::123456789012:policy/other"
	for _, name := range []string{"app", "other"} {
		require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "CreatePolicy", map[string]any{
			"PolicyName":     name,
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		}).StatusCode)
		require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "CreateInstanceProfile",
			map[string]any{"InstanceProfileName": name}).StatusCode)
	}

	keyID := trustSetupCaller(t, srv, "alice")
	require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "PutUserPolicy", map[string]any{
		"UserName":   "alice",
		"PolicyName": "app-only",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
			`"Action":["iam:TagPolicy","iam:UntagPolicy","iam:ListPolicyTags",` +
			`"iam:TagInstanceProfile","iam:UntagInstanceProfile","iam:ListInstanceProfileTags"],` +
			`"Resource":["arn:aws:iam::123456789012:policy/app",` +
			`"arn:aws:iam::123456789012:instance-profile/app"]}]}`,
	}).StatusCode)

	call := func(t *testing.T, operation string, body any) int {
		t.Helper()
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
		r.Host = "iam.amazonaws.com"
		r.Header.Set("X-Amz-Target", "AmazonIdentityManagementService."+operation)
		r.Header.Set("Content-Type", "application/x-amz-json-1.1")
		r.Header.Set("Authorization", trustAuthHeader(keyID, "iam"))
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		resp := w.Result()
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode
	}

	tags := []map[string]string{{"Key": "env", "Value": "prod"}}
	for _, tc := range []struct {
		name       string
		operation  string
		body       any
		wantStatus int
	}{
		{
			name: "TagPolicy on the named policy", operation: "TagPolicy",
			body:       map[string]any{"PolicyArn": namedPolicy, "Tags": tags},
			wantStatus: http.StatusOK,
		},
		{
			name: "TagPolicy on another policy", operation: "TagPolicy",
			body:       map[string]any{"PolicyArn": otherPolicy, "Tags": tags},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "ListPolicyTags on the named policy", operation: "ListPolicyTags",
			body:       map[string]any{"PolicyArn": namedPolicy},
			wantStatus: http.StatusOK,
		},
		{
			name: "ListPolicyTags on another policy", operation: "ListPolicyTags",
			body:       map[string]any{"PolicyArn": otherPolicy},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "UntagPolicy on the named policy", operation: "UntagPolicy",
			body:       map[string]any{"PolicyArn": namedPolicy, "TagKeys": []string{"env"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "UntagPolicy on another policy", operation: "UntagPolicy",
			body:       map[string]any{"PolicyArn": otherPolicy, "TagKeys": []string{"env"}},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "TagInstanceProfile on the named profile", operation: "TagInstanceProfile",
			body:       map[string]any{"InstanceProfileName": "app", "Tags": tags},
			wantStatus: http.StatusOK,
		},
		{
			name: "TagInstanceProfile on another profile", operation: "TagInstanceProfile",
			body:       map[string]any{"InstanceProfileName": "other", "Tags": tags},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "ListInstanceProfileTags on the named profile",
			operation:  "ListInstanceProfileTags",
			body:       map[string]any{"InstanceProfileName": "app"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "ListInstanceProfileTags on another profile",
			operation:  "ListInstanceProfileTags",
			body:       map[string]any{"InstanceProfileName": "other"},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "UntagInstanceProfile on the named profile", operation: "UntagInstanceProfile",
			body:       map[string]any{"InstanceProfileName": "app", "TagKeys": []string{"env"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "UntagInstanceProfile on another profile", operation: "UntagInstanceProfile",
			body:       map[string]any{"InstanceProfileName": "other", "TagKeys": []string{"env"}},
			wantStatus: http.StatusForbidden,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantStatus, call(t, tc.operation, tc.body))
		})
	}
}
