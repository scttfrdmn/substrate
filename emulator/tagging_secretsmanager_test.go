package emulator_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Secrets Manager had four defects that are one defect. resolveSecretID split an identifier on ":"
// and returned the last segment, checked neither the service nor the "secret" type keyword, and fell
// through to treating a malformed ARN as a bare *name*; and every one of the ten operations taking a
// SecretId then keyed its load and its store by the *caller's* account and Region. So a secret ARN
// naming another account or Region addressed the caller's own secret of that name — and
// UntagResource, the damaging direction, answered 200 while stripping its tags. That is the rule #826
// established for SQS, #845 carried into the tagging API's resolver, and #910, #918, #922 and #925
// applied to Step Functions, CloudFront, KMS and SNS.
//
// On top of that fix, the Secrets Manager row of #835: the Resource Groups Tagging API had no
// secretsmanager arm, so a secret ARN fell to resolveARN's default and came back as a
// FailedResourcesMap entry, and GetResources reported no secrets at all.
//
// The two ship together because they are the same change seen from two sides: both need one
// context-free key builder, and shipping them apart would leave two parsers that can disagree about
// which secret an ARN names.
//
// What makes this service different from the five rows before it is that SecretId legitimately
// accepts a *bare name* as well as an ARN — AWS documents it as "The ARN or name of the secret" — and
// a name carries no account, so the caller's own account is the correct source for that one case and
// only that one. The tests below assert both halves: an ARN never reads the caller's account, and a
// name still does.
//
// Every tag is read back through DescribeSecret, per #765. Secrets Manager publishes no
// ListTagsForResource at all — substrate answered one until #929 removed it — so DescribeSecret is
// the owning service's own read path, and the "Tags" member it reports is the only thing that tells a
// tag that landed on the right record from a tag reported as landed. Nothing here ever called the
// invented operation, which is why removing it did not touch this file.

// Wire details a real SDK would send. Two Regions are needed because a secret is Region-scoped and
// its ARN carries the Region, so a cross-Region assertion needs a secret that genuinely exists
// somewhere else; the parser takes the Region off the Host (parser.go:198-228), which is why these two
// differ only there.
var (
	smTarget      = signedRequestTarget{host: "secretsmanager.us-east-1.amazonaws.com", target: "secretsmanager", signingName: "secretsmanager"}
	smWest2Target = signedRequestTarget{host: "secretsmanager.us-west-2.amazonaws.com", target: "secretsmanager", signingName: "secretsmanager"}
)

// smTagServer starts a server callable as [taggingTestAccount].
func smTagServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
}

// smRawCall posts a Secrets Manager operation and returns the status, the raw body and the bare error
// code a refusal carries.
//
// The raw body is returned because one assertion below is about a member's *absence*, and a decoded
// struct cannot tell an absent member from an empty one — the distinction that let v0.115.0's
// boundary defect through a test written to catch it.
func smRawCall(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, op string, body map[string]any) (int, string, string) {
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

// smCreateSecretIn creates a secret through one Region's endpoint and returns the ARN Secrets Manager
// itself minted, rather than one built here — so no assertion below can pass by agreeing with an ARN
// this test happens to construct.
func smCreateSecretIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, name, value string, tags map[string]string) string {
	t.Helper()
	body := map[string]any{"Name": name, "SecretString": value}
	if len(tags) > 0 {
		list := make([]map[string]string, 0, len(tags))
		for k, v := range tags {
			list = append(list, map[string]string{"Key": k, "Value": v})
		}
		body["Tags"] = list
	}
	var out struct {
		ARN string `json:"ARN"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "CreateSecret", body), &out)
	require.Empty(t, errCode, "CreateSecret %s on %s", name, tgt.host)
	require.Equal(t, http.StatusOK, status, "CreateSecret %s on %s", name, tgt.host)
	require.NotEmpty(t, out.ARN, "CreateSecret %s reports an ARN", name)
	return out.ARN
}

// smCreateSecret creates a secret in us-east-1.
func smCreateSecret(t *testing.T, ts *emulator.TestServer, name string, tags map[string]string) string {
	t.Helper()
	return smCreateSecretIn(t, ts, smTarget, name, "value-of-"+name, tags)
}

// smDescribe reads a secret through DescribeSecret — the owning service's own read path for tags,
// since Secrets Manager publishes no ListTagsForResource.
func smDescribe(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, secretID string) (arn string, tags []emulator.SMTag, raw string) {
	t.Helper()
	status, body, errCode := smRawCall(t, ts, tgt, "DescribeSecret", map[string]any{"SecretId": secretID})
	require.Empty(t, errCode, "DescribeSecret %s", secretID)
	require.Equal(t, http.StatusOK, status, "DescribeSecret %s", secretID)
	var out struct {
		ARN  string           `json:"ARN"`
		Tags []emulator.SMTag `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out), "decode DescribeSecret %s", secretID)
	return out.ARN, out.Tags, body
}

// smTagMap reads a secret's tags through DescribeSecret as a map.
func smTagMap(t *testing.T, ts *emulator.TestServer, secretID string) map[string]string {
	t.Helper()
	_, tags, _ := smDescribe(t, ts, smTarget, secretID)
	out := make(map[string]string, len(tags))
	for _, tag := range tags {
		out[tag.Key] = tag.Value
	}
	return out
}

// smTagKeyOrder returns the tag keys in the order DescribeSecret reported them, for the determinism
// assertion. A map would discard the one thing being checked.
func smTagKeyOrder(t *testing.T, ts *emulator.TestServer, secretID string) []string {
	t.Helper()
	_, tags, _ := smDescribe(t, ts, smTarget, secretID)
	keys := make([]string, 0, len(tags))
	for _, tag := range tags {
		keys = append(keys, tag.Key)
	}
	return keys
}

// smTagResource calls Secrets Manager's own TagResource and returns the status and error code, so a
// test can assert either the success path or a refusal.
func smTagResource(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, secretID string, tags map[string]string) (int, string) {
	t.Helper()
	list := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]string{"Key": k, "Value": v})
	}
	status, _, code := smRawCall(t, ts, tgt, "TagResource", map[string]any{"SecretId": secretID, "Tags": list})
	return status, code
}

// smUntagResource calls Secrets Manager's own UntagResource. It is never left untested alongside
// TagResource: a removal aimed at the wrong resource is the more damaging direction.
func smUntagResource(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, secretID string, keys ...string) (int, string) {
	t.Helper()
	status, _, code := smRawCall(t, ts, tgt, "UntagResource", map[string]any{"SecretId": secretID, "TagKeys": keys})
	return status, code
}

// smListSecretNames returns the names ListSecrets reports through one Region's endpoint.
func smListSecretNames(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget) []string {
	t.Helper()
	var out struct {
		SecretList []struct {
			Name string `json:"Name"`
		} `json:"SecretList"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "ListSecrets", map[string]any{}), &out)
	require.Empty(t, errCode, "ListSecrets on %s", tgt.host)
	require.Equal(t, http.StatusOK, status, "ListSecrets on %s", tgt.host)
	names := make([]string, 0, len(out.SecretList))
	for _, s := range out.SecretList {
		names = append(names, s.Name)
	}
	return names
}

// smSecretValue reads a secret's value, which tells two same-named secrets in different Regions apart
// more sharply than a tag does — a tag could match by coincidence of the fixture.
func smSecretValue(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, secretID string) string {
	t.Helper()
	var out struct {
		SecretString string `json:"SecretString"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "GetSecretValue", map[string]any{"SecretId": secretID}), &out)
	require.Empty(t, errCode, "GetSecretValue %s", secretID)
	require.Equal(t, http.StatusOK, status, "GetSecretValue %s", secretID)
	return out.SecretString
}

// ----- The resolution fix -----

func TestSMResolution_ASecretARNFromAnotherRegionDoesNotReachTheLocalSecret(t *testing.T) {
	ts := smTagServer(t)

	eastARN := smCreateSecretIn(t, ts, smTarget, "shared", "east-value", map[string]string{"env": "east"})
	westARN := smCreateSecretIn(t, ts, smWest2Target, "shared", "west-value", map[string]string{"env": "west"})
	require.NotEqual(t, eastARN, westARN, "the two ARNs differ by Region")

	// The west ARN, presented to the east endpoint, must reach the west secret.
	gotARN, _, _ := smDescribe(t, ts, smTarget, westARN)
	assert.Equal(t, westARN, gotARN,
		"a west-Region ARN describes the west secret even when presented to the east endpoint")
	assert.Equal(t, "west-value", smSecretValue(t, ts, smTarget, westARN),
		"and reads the west secret's value, not the east secret's")

	// UntagResource is the damaging direction: removing "env" through the west ARN must not touch the
	// east secret, which is what keying by the caller's Region did.
	status, code := smUntagResource(t, ts, smTarget, westARN, "env")
	require.Empty(t, code, "UntagResource on the west ARN")
	require.Equal(t, http.StatusOK, status)

	assert.Equal(t, map[string]string{"env": "east"}, smTagMap(t, ts, eastARN),
		"the east secret keeps its tag after an untag aimed at the west one")
	assert.Empty(t, smTagMap(t, ts, westARN),
		"and the west secret is the one that lost it")
}

func TestSMResolution_AForeignAccountARNDoesNotReachTheCallersSecret(t *testing.T) {
	ts := smTagServer(t)

	ownARN := smCreateSecret(t, ts, "shared", map[string]string{"env": "mine"})
	foreignARN := strings.Replace(ownARN, taggingTestAccount, taggingForeignAccount, 1)
	require.NotEqual(t, ownARN, foreignARN, "the foreign ARN differs only by account")

	// Well-formed, so it is not refused by the parser — it simply names a secret in an account this
	// server holds nothing for, which is the isolation the account-qualified state key makes emergent.
	// The status is 400, which is what every SecretId-taking operation publishes for
	// ResourceNotFoundException; these read 404 until #930.
	for _, op := range []string{"DescribeSecret", "GetSecretValue", "DeleteSecret"} {
		t.Run(op, func(t *testing.T) {
			status, _, code := smRawCall(t, ts, smTarget, op, map[string]any{"SecretId": foreignARN})
			assert.Equal(t, "ResourceNotFoundException", code,
				"%s on a foreign-account ARN reports the secret absent rather than reaching the caller's", op)
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}
	t.Run("UntagResource", func(t *testing.T) {
		status, code := smUntagResource(t, ts, smTarget, foreignARN, "env")
		assert.Equal(t, "ResourceNotFoundException", code)
		assert.Equal(t, http.StatusBadRequest, status)
	})

	assert.Equal(t, map[string]string{"env": "mine"}, smTagMap(t, ts, ownARN),
		"and the caller's own same-named secret is untouched by any of them")
}

func TestSMResolution_AMalformedARNIsRefused(t *testing.T) {
	ts := smTagServer(t)
	smCreateSecret(t, ts, "real", nil)

	for _, tc := range []struct {
		name string
		id   string
	}{
		{"too few segments", "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":secret"},
		{"another service", "arn:aws:kms:us-east-1:" + taggingTestAccount + ":secret:real"},
		{"no type keyword", "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":other:real"},
		{"a keyword the type is a prefix of", "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":secretpolicy:real"},
		{"no Region", "arn:aws:secretsmanager::" + taggingTestAccount + ":secret:real"},
		{"no account", "arn:aws:secretsmanager:us-east-1::secret:real"},
		{"no name", "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":secret:"},
		{"an S3 ARN", "arn:aws:s3:::my-bucket"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Refused rather than looked up: the previous resolver fell through and treated each of
			// these as a bare *name*, so a mistyped ARN became a lookup — and, for the ones ending in
			// "real", a lookup that reached the caller's actual secret.
			status, _, code := smRawCall(t, ts, smTarget, "DescribeSecret", map[string]any{"SecretId": tc.id})
			assert.Equal(t, "InvalidParameterException", code, "DescribeSecret refuses %q", tc.id)
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}
}

func TestSMResolution_ABareNameStillResolvesInTheCallersAccount(t *testing.T) {
	ts := smTagServer(t)

	// SecretId is documented as "The ARN or name of the secret", so a bare name must keep working —
	// and it is the one identifier carrying no account, which makes the caller's the correct source
	// for it. Refusing everything that is not an ARN would have been the easy over-correction.
	arn := smCreateSecret(t, ts, "by-name", map[string]string{"env": "prod"})

	gotARN, _, _ := smDescribe(t, ts, smTarget, "by-name")
	assert.Equal(t, arn, gotARN, "a bare name resolves in the caller's own account and Region")
	assert.Equal(t, map[string]string{"env": "prod"}, smTagMap(t, ts, "by-name"))
}

func TestSMResolution_ABareNameIsScopedToTheCallersRegion(t *testing.T) {
	ts := smTagServer(t)

	smCreateSecretIn(t, ts, smTarget, "shared", "east-value", nil)
	smCreateSecretIn(t, ts, smWest2Target, "shared", "west-value", nil)

	// The name path reads the caller's Region, so which endpoint the name arrives at decides which
	// secret it names. This is the half of the resolver that is *meant* to read the request context.
	assert.Equal(t, "east-value", smSecretValue(t, ts, smTarget, "shared"))
	assert.Equal(t, "west-value", smSecretValue(t, ts, smWest2Target, "shared"))
}

func TestSMResolution_AHierarchicalNameRoundTripsThroughItsARN(t *testing.T) {
	ts := smTagServer(t)

	// A secret name may contain "/" and is hierarchical in practice. The name is the whole remainder
	// after the "secret" keyword, so it must survive being read back off its own ARN — a resolver that
	// split the resource portion further, or took its last component, would truncate it to "password".
	arn := smCreateSecret(t, ts, "prod/db/password", map[string]string{"tier": "prod"})
	require.True(t, strings.HasSuffix(arn, ":secret:prod/db/password"), "ARN is %q", arn)

	gotARN, _, _ := smDescribe(t, ts, smTarget, arn)
	assert.Equal(t, arn, gotARN, "a hierarchical name resolves from its own ARN")
	assert.Equal(t, map[string]string{"tier": "prod"}, smTagMap(t, ts, arn))
	assert.Equal(t, map[string]string{"tier": "prod"}, getResourcesTags(t, ts, arn),
		"and the tagging API's resolver derives the same name from the same ARN")
}

func TestSMResolution_DeleteSecretRemovesTheOwningRegionsIndexEntry(t *testing.T) {
	ts := smTagServer(t)

	smCreateSecretIn(t, ts, smTarget, "gone", "east-value", nil)
	westARN := smCreateSecretIn(t, ts, smWest2Target, "gone", "west-value", nil)

	// Forced, because #928's subject is which Region's index entry a *removal* takes out, and since #953
	// only a forced delete removes anything — a plain DeleteSecret opens a recovery window and leaves
	// both indexes as they were.
	status, _, code := smRawCall(t, ts, smTarget, "DeleteSecret", map[string]any{
		"SecretId":                   westARN,
		"ForceDeleteWithoutRecovery": true,
	})
	require.Empty(t, code, "DeleteSecret on the west ARN")
	require.Equal(t, http.StatusOK, status)

	assert.Contains(t, smListSecretNames(t, ts, smTarget), "gone",
		"the east Region still lists its own secret")
	assert.NotContains(t, smListSecretNames(t, ts, smWest2Target), "gone",
		"and the west Region's index entry is the one that was removed")
}

func TestSMResolution_AnAbsentSecretIsReportedNotFound(t *testing.T) {
	ts := smTagServer(t)
	absent := "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":secret:no-such-secret"

	// 400 rather than 404, per #930 — see [TestSMErrorStatus_AnAbsentSecretIsAlways400], which asserts
	// the same thing across all nine SecretId-taking operations.
	t.Run("TagResource", func(t *testing.T) {
		status, code := smTagResource(t, ts, smTarget, absent, map[string]string{"env": "prod"})
		assert.Equal(t, "ResourceNotFoundException", code)
		assert.Equal(t, http.StatusBadRequest, status)
	})
	t.Run("UntagResource", func(t *testing.T) {
		status, code := smUntagResource(t, ts, smTarget, absent, "env")
		assert.Equal(t, "ResourceNotFoundException", code)
		assert.Equal(t, http.StatusBadRequest, status)
	})
	t.Run("DescribeSecret", func(t *testing.T) {
		status, _, code := smRawCall(t, ts, smTarget, "DescribeSecret", map[string]any{"SecretId": absent})
		assert.Equal(t, "ResourceNotFoundException", code)
		assert.Equal(t, http.StatusBadRequest, status)
	})
}

// ----- Tags through Secrets Manager's own operations -----

func TestSMTags_CreateSecretTagsAreReadBackThroughDescribeSecret(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "tagged-at-creation", map[string]string{"env": "prod", "owner": "platform"})

	assert.Equal(t, map[string]string{"env": "prod", "owner": "platform"}, smTagMap(t, ts, arn))
}

func TestSMTags_AnUntaggedSecretOmitsTheTagsMember(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "untagged", nil)

	_, tags, raw := smDescribe(t, ts, smTarget, arn)
	assert.Empty(t, tags, "an untagged secret reports no tags")
	// Asserted on the raw body: AWS states "Secrets Manager only returns fields that have a value in
	// the response", and a decoded struct cannot tell an absent member from a null one.
	assert.NotContains(t, raw, `"Tags"`,
		"DescribeSecret omits the Tags member entirely rather than sending null")
}

func TestSMTags_TagResourceAppendsRatherThanReplaces(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "appending", map[string]string{"env": "prod"})

	// AWS: "This operation appends tags to the existing list of tags."
	status, code := smTagResource(t, ts, smTarget, arn, map[string]string{"owner": "platform"})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, map[string]string{"env": "prod", "owner": "platform"}, smTagMap(t, ts, arn))

	// A repeated key replaces that key's value and nothing else.
	status, code = smTagResource(t, ts, smTarget, arn, map[string]string{"env": "staging"})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, map[string]string{"env": "staging", "owner": "platform"}, smTagMap(t, ts, arn))
}

func TestSMTags_UntagResourceIsIdempotent(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "idempotent", map[string]string{"env": "prod"})

	// AWS: "This operation is idempotent. If a requested tag is not attached to the secret, no error
	// is returned and the secret metadata is unchanged."
	status, code := smUntagResource(t, ts, smTarget, arn, "never-set")
	assert.Empty(t, code, "removing an absent key is not an error")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, map[string]string{"env": "prod"}, smTagMap(t, ts, arn),
		"and the metadata is unchanged")
}

func TestSMTags_TagOrderIsDeterministic(t *testing.T) {
	ts := smTagServer(t)
	want := []string{"alpha", "beta", "delta", "epsilon", "gamma", "zeta"}
	// Written in descending order, one call at a time, so the stored slice cannot come out sorted by
	// accident of insertion order.
	descending := []string{"zeta", "gamma", "epsilon", "delta", "beta", "alpha"}

	t.Run("through Secrets Manager's own TagResource", func(t *testing.T) {
		arn := smCreateSecret(t, ts, "ordered-native", nil)
		// The merged list is built by ranging a Go map, so before #862's rule was applied here two
		// identical runs could report the same tags in a different order.
		for _, key := range descending {
			status, code := smTagResource(t, ts, smTarget, arn, map[string]string{key: "v"})
			require.Empty(t, code, "TagResource %s", key)
			require.Equal(t, http.StatusOK, status)
		}
		assert.Equal(t, want, smTagKeyOrder(t, ts, arn))
	})

	t.Run("through the tagging API", func(t *testing.T) {
		arn := smCreateSecret(t, ts, "ordered-tagging-api", nil)
		for _, key := range descending {
			tagResourcesWith(t, ts, arn, map[string]string{key: "v"})
		}
		assert.Equal(t, want, smTagKeyOrder(t, ts, arn),
			"both arms sort, so the two APIs agree on order as well as on content")
	})

	t.Run("at creation", func(t *testing.T) {
		tags := make(map[string]string, len(descending))
		for _, key := range descending {
			tags[key] = "v"
		}
		arn := smCreateSecret(t, ts, "ordered-at-creation", tags)
		assert.Equal(t, want, smTagKeyOrder(t, ts, arn))
	})
}

// ----- The #835 row -----

func TestTaggingSM_ASecretTagIsReadBackThroughSecretsManager(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "cross-readable", nil)

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod", "cost-center": "1234"})

	// #765: the tag has to be visible through the owning service's own read path, which for Secrets
	// Manager is DescribeSecret — there is no ListTagsForResource in this API.
	assert.Equal(t, map[string]string{"env": "prod", "cost-center": "1234"}, smTagMap(t, ts, arn),
		"a tag written through the tagging API is readable through DescribeSecret")
}

func TestTaggingSM_ATagWrittenThroughSecretsManagerIsReportedByGetResources(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "native-then-tagging-api", nil)

	status, code := smTagResource(t, ts, smTarget, arn, map[string]string{"env": "prod"})
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)

	assert.Equal(t, map[string]string{"env": "prod"}, getResourcesTags(t, ts, arn),
		"cross-readability in the other direction")
}

func TestTaggingSM_UntagResourcesRemovesOnlyTheNamedKeys(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "partial-untag", map[string]string{"env": "prod", "owner": "platform", "tier": "db"})

	untagResourcesWith(t, ts, arn, "owner")

	assert.Equal(t, map[string]string{"env": "prod", "tier": "db"}, smTagMap(t, ts, arn),
		"only the named key is removed, and the record still decodes")
}

func TestTaggingSM_TheMergePreservesTheRestOfTheRecord(t *testing.T) {
	ts := smTagServer(t)
	arn := smCreateSecret(t, ts, "preserved", nil)

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod"})

	// The merge rewrites the stored JSON, so one that decoded into SecretState and re-encoded would
	// silently drop any member that struct does not carry. The secret's value lives at a separate key
	// and the record holds the version ID addressing it, so reading the value back is what proves the
	// record survived rather than merely still parsing.
	assert.Equal(t, "value-of-preserved", smSecretValue(t, ts, smTarget, arn))
	_, _, raw := smDescribe(t, ts, smTarget, arn)
	assert.Contains(t, raw, `"Name":"preserved"`, "the record's other members survive the merge")
}

func TestTaggingSM_GetResourcesReportsTheSecretAndNothingElse(t *testing.T) {
	ts := smTagServer(t)
	first := smCreateSecret(t, ts, "alpha", map[string]string{"env": "prod"})
	// Tagged at creation, because GetResources reports what has been tagged and a secret that never was
	// is absent by rule (#938). It carries a different tag from the first so the two are not
	// interchangeable in a failure message.
	second := smCreateSecret(t, ts, "beta", map[string]string{"env": "dev"})

	// An exact-set match rather than a membership check, because that is the only form that catches a
	// scanner using a bare "secret" prefix: the namespace also holds "secret_names:{acct}/{region}"
	// and a "secret_version:" key per version, and neither has a wire-reachable ARN that could test
	// the colon-terminated-prefix rule directly. Listing the version key would be the worst of the
	// three — its value is a caller's secret, not JSON.
	assert.ElementsMatch(t, []string{first, second}, getResourcesARNs(t, ts, "secretsmanager"),
		"GetResources reports the two secret records and neither the index nor a version key")
}

func TestTaggingSM_GetResourcesDoesNotReportASecretFromAnotherRegion(t *testing.T) {
	ts := smTagServer(t)
	eastARN := smCreateSecretIn(t, ts, smTarget, "east-only", "v", map[string]string{"env": "east"})
	westARN := smCreateSecretIn(t, ts, smWest2Target, "west-only", "v", map[string]string{"env": "west"})

	eastReported := getResourcesARNsIn(t, ts, taggingTarget)
	assert.Contains(t, eastReported, eastARN)
	assert.NotContains(t, eastReported, westARN,
		"the scanner is scoped to the caller's Region, which the state-key prefix is what enforces")
}

func TestTaggingSM_AForeignAccountARNCannotBeTagged(t *testing.T) {
	ts := smTagServer(t)
	ownARN := smCreateSecret(t, ts, "shared", map[string]string{"env": "mine"})
	foreignARN := strings.Replace(ownARN, taggingTestAccount, taggingForeignAccount, 1)

	for _, op := range []string{"TagResources", "UntagResources"} {
		t.Run(op, func(t *testing.T) {
			failures := tagResourcesFailures(t, ts, op, foreignARN)
			require.Contains(t, failures, foreignARN,
				"%s on a foreign-account ARN reports a failure rather than reaching the caller's secret", op)
		})
	}

	assert.Equal(t, map[string]string{"env": "mine"}, smTagMap(t, ts, ownARN),
		"and the caller's own same-named secret is untouched")
}

func TestTaggingSM_AnARNThatDoesNotNameASecretIsNotTaggable(t *testing.T) {
	ts := smTagServer(t)

	for _, tc := range []struct {
		name string
		arn  string
	}{
		{"no type keyword", "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":other:real"},
		{"a keyword the type is a prefix of", "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":secretpolicy:real"},
		{"too few segments", "arn:aws:secretsmanager:us-east-1:" + taggingTestAccount + ":secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range []string{"TagResources", "UntagResources"} {
				failures := tagResourcesFailures(t, ts, op, tc.arn)
				require.Contains(t, failures, tc.arn,
					"%s refuses %q rather than resolving it", op, tc.arn)
			}
		})
	}
}
