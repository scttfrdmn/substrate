package emulator_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// KMS had two defects that are one defect: a key ARN was resolved by taking its last "/"-delimited
// component, and every one of the eighteen operations taking a KeyId then keyed its load and its
// store by the *caller's* account and Region. So a key ARN naming another account addressed the
// caller's own key of that ID, and an alias ARN — whose resource portion is "alias/{name}" —
// resolved to the alias name rather than to the key it points at. That is the rule #826 established
// for SQS, #845 carried into the tagging API's resolver, #910 applied to Step Functions and #918 to
// CloudFront.
//
// On top of that fix, the KMS key row of #835: the Resource Groups Tagging API had no kms arm, so a
// key ARN fell to resolveARN's default and answered an InternalServiceException FailedResourcesMap
// entry, and GetResources reported no keys at all.
//
// The two ship together because they are the same change seen from two sides. Both need one
// context-free key builder, and the ARN fix is what makes the cross-account case *reachable*: while
// the account came from the request, a foreign-account key ARN could only ever hit the caller's own
// record, so KMS's published "Cross-account use: No" had nothing to refuse. Fixing the resolution
// without adding the refusal would turn a wrong-record write into a genuine cross-account write.
//
// Every tag written through the tagging API is read back through KMS's own ListResourceTags, per
// #765 — the only assertion that distinguishes a tag on the right record from one on a phantom key
// that answered 200.

// kmsTarget and kmsWest2Target are the wire details a real SDK would send. KMS's X-Amz-Target
// prefix is "TrentService", which bears no resemblance to the endpoint name (parser_test.go:884).
// Two Regions are needed because a KMS key is Region-scoped and its ARN carries the Region, so a
// cross-Region assertion needs a key that genuinely exists somewhere else.
var (
	kmsTarget      = signedRequestTarget{host: "kms.us-east-1.amazonaws.com", target: "TrentService", signingName: "kms"}
	kmsWest2Target = signedRequestTarget{host: "kms.us-west-2.amazonaws.com", target: "TrentService", signingName: "kms"}
)

// createKMSKey creates a key and returns the ARN and key ID KMS itself minted, rather than ones
// built here — so no assertion below can pass by agreeing with a key substrate happens to build.
func createKMSKey(t *testing.T, ts *emulator.TestServer) (arn, keyID string) {
	t.Helper()
	return createKMSKeyIn(t, ts, kmsTarget)
}

// createKMSKeyIn creates a key through a specific Region's endpoint.
func createKMSKeyIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget) (arn, keyID string) {
	t.Helper()
	var out struct {
		KeyMetadata struct {
			KeyID string `json:"KeyId"`
			Arn   string `json:"Arn"`
		} `json:"KeyMetadata"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "CreateKey", map[string]any{}), &out)
	require.Empty(t, errCode, "CreateKey on %s", tgt.host)
	require.Equal(t, http.StatusOK, status, "CreateKey on %s", tgt.host)
	require.NotEmpty(t, out.KeyMetadata.Arn, "CreateKey reports an ARN")
	require.NotEmpty(t, out.KeyMetadata.KeyID, "CreateKey reports a key ID")
	return out.KeyMetadata.Arn, out.KeyMetadata.KeyID
}

// kmsResourceTags reads a key's tags through KMS's own ListResourceTags.
func kmsResourceTags(t *testing.T, ts *emulator.TestServer, keyID string) map[string]string {
	t.Helper()
	return kmsResourceTagsIn(t, ts, kmsTarget, keyID)
}

// kmsResourceTagsIn reads a key's tags through a specific Region's endpoint. KMS spells a tag's two
// fields TagKey and TagValue, not Key and Value — it is the only one of #835's remaining four that
// does, which is why mergeRecordTagListTags takes both field names.
func kmsResourceTagsIn(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, keyID string) map[string]string {
	t.Helper()
	var out struct {
		Tags []struct {
			TagKey   string `json:"TagKey"`
			TagValue string `json:"TagValue"`
		} `json:"Tags"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "ListResourceTags",
			map[string]any{"KeyId": keyID}), &out)
	require.Empty(t, errCode, "ListResourceTags %s", keyID)
	require.Equal(t, http.StatusOK, status, "ListResourceTags %s", keyID)
	tags := make(map[string]string, len(out.Tags))
	for _, tag := range out.Tags {
		tags[tag.TagKey] = tag.TagValue
	}
	return tags
}

// kmsTagKeyOrder returns the tag keys in the order ListResourceTags reported them, for the
// determinism assertion. A map would discard the one thing being checked.
func kmsTagKeyOrder(t *testing.T, ts *emulator.TestServer, keyID string) []string {
	t.Helper()
	var out struct {
		Tags []struct {
			TagKey string `json:"TagKey"`
		} `json:"Tags"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, kmsTarget, taggingTestAccount, "ListResourceTags",
			map[string]any{"KeyId": keyID}), &out)
	require.Empty(t, errCode, "ListResourceTags %s", keyID)
	require.Equal(t, http.StatusOK, status, "ListResourceTags %s", keyID)
	keys := make([]string, 0, len(out.Tags))
	for _, tag := range out.Tags {
		keys = append(keys, tag.TagKey)
	}
	return keys
}

// kmsTagResource calls KMS's own TagResource and returns the error code, so a test can assert
// either the success path or a refusal.
func kmsTagResource(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, keyID string, tags map[string]string) (int, string) {
	t.Helper()
	list := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		list = append(list, map[string]string{"TagKey": k, "TagValue": v})
	}
	var out map[string]any
	return decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "TagResource",
			map[string]any{"KeyId": keyID, "Tags": list}), &out)
}

// kmsCreateAlias points an alias at a key and returns the status and error code.
func kmsCreateAlias(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, alias, targetKeyID string) (int, string) {
	t.Helper()
	var out map[string]any
	return decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "CreateAlias",
			map[string]any{"AliasName": alias, "TargetKeyId": targetKeyID}), &out)
}

// kmsDescribeKeyID resolves a KeyId through DescribeKey and returns the key ID KMS reports, or the
// error code if it refused. DescribeKey is used rather than the state store because the whole
// question is what an identifier resolves to over the wire.
func kmsDescribeKeyID(t *testing.T, ts *emulator.TestServer, tgt signedRequestTarget, keyIDOrARN string) (string, string) {
	t.Helper()
	var out struct {
		KeyMetadata struct {
			KeyID string `json:"KeyId"`
		} `json:"KeyMetadata"`
	}
	_, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, tgt, taggingTestAccount, "DescribeKey",
			map[string]any{"KeyId": keyIDOrARN}), &out)
	return out.KeyMetadata.KeyID, errCode
}

// ----- The resolution fix --------------------------------------------------

// TestKMSResolution_AnAliasARNResolvesToTheKeyItPointsAt is the anchored-segment assertion. An
// alias ARN's resource portion is "alias/{name}", so the old last-"/"-component scan handed back
// the alias name as if it were a key ID — a different resource type addressing a record it does not
// name, which is the mistake #910 found in strings.Contains(arn, ":stateMachine:").
func TestKMSResolution_AnAliasARNResolvesToTheKeyItPointsAt(t *testing.T) {
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	status, errCode := kmsCreateAlias(t, ts, kmsTarget, "alias/resolves", keyID)
	require.Empty(t, errCode, "CreateAlias")
	require.Equal(t, http.StatusOK, status, "CreateAlias")

	aliasARN := "arn:aws:kms:us-east-1:" + taggingTestAccount + ":alias/resolves"
	got, errCode := kmsDescribeKeyID(t, ts, kmsTarget, aliasARN)
	assert.Empty(t, errCode, "DescribeKey on an alias ARN")
	assert.Equal(t, keyID, got, "an alias ARN resolves to the key it points at, not to %q", "resolves")
}

// TestKMSResolution_ASlashedAliasNameSurvivesTheARN is the case that makes the anchored cut
// load-bearing rather than tidy. AWS's own example of an AWS managed key's alias is alias/aws/s3,
// whose ARN is arn:aws:kms:{region}:{account}:alias/aws/s3 — so the identifier legitimately
// contains a "/", and both a last-component scan ("s3") and a refuse-any-slash parser get it wrong.
func TestKMSResolution_ASlashedAliasNameSurvivesTheARN(t *testing.T) {
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)

	status, errCode := kmsCreateAlias(t, ts, kmsTarget, "alias/team/prod/db", keyID)
	require.Empty(t, errCode, "CreateAlias with a slashed name")
	require.Equal(t, http.StatusOK, status, "CreateAlias with a slashed name")

	aliasARN := "arn:aws:kms:us-east-1:" + taggingTestAccount + ":alias/team/prod/db"
	got, errCode := kmsDescribeKeyID(t, ts, kmsTarget, aliasARN)
	assert.Empty(t, errCode, "DescribeKey on a slashed alias ARN")
	assert.Equal(t, keyID, got, "the whole alias name after the first slash is the alias name")
}

// TestKMSResolution_AKeyARNFromAnotherRegionDoesNotReachTheLocalKey is the substituted-record
// assertion. Two Regions each hold a key; the ARN of the us-west-2 key is presented to the
// us-east-1 endpoint. Keying by the caller's Region answered with the *local* key's metadata under
// the foreign key's ARN — a caller cannot tell one from the other, which is the whole defect.
func TestKMSResolution_AKeyARNFromAnotherRegionDoesNotReachTheLocalKey(t *testing.T) {
	ts := arnGuardServer(t)
	_, eastKeyID := createKMSKey(t, ts)
	westARN, westKeyID := createKMSKeyIn(t, ts, kmsWest2Target)
	require.NotEqual(t, eastKeyID, westKeyID, "the two keys are distinct")

	got, errCode := kmsDescribeKeyID(t, ts, kmsTarget, westARN)
	require.Empty(t, errCode, "DescribeKey on a us-west-2 key ARN")
	assert.Equal(t, westKeyID, got, "a key ARN resolves to the key the ARN names")
	assert.NotEqual(t, eastKeyID, got,
		"never to the caller's own us-east-1 key, which is what keying by the request Region did")
}

// TestKMSResolution_DescribeKeyStillAnswersForAForeignRegionARN records what the resolution fix does
// *not* change, so the residue is pinned rather than discovered later.
//
// The three tagging operations refuse a key outside the caller's Region, because AWS publishes
// "Cross-account use: No" on each of them. The other fifteen KeyId operations do not, so DescribeKey
// on a us-west-2 key ARN at the us-east-1 endpoint now answers with that key. Real KMS would not — a
// Regional endpoint serves only its own Region — but AWS publishes no per-operation statement to cite
// for the other fifteen, and inventing one across all of them is a wider change than an ARN
// resolution fix. Recorded on #922 rather than guessed at.
//
// This is still strictly better than before: the answer now describes the key the ARN names, instead
// of silently describing a different key that happened to share its ID in the caller's Region.
func TestKMSResolution_DescribeKeyStillAnswersForAForeignRegionARN(t *testing.T) {
	ts := arnGuardServer(t)
	westARN, westKeyID := createKMSKeyIn(t, ts, kmsWest2Target)

	got, errCode := kmsDescribeKeyID(t, ts, kmsTarget, westARN)
	assert.Empty(t, errCode, "DescribeKey does not yet enforce the Region")
	assert.Equal(t, westKeyID, got, "and it reports the key the ARN names")
}

// TestKMSResolution_AnAliasCannotTargetAKeyInAnotherRegion asserts the alias invariant. An alias is
// Region-scoped and can only point at a key in its own Region, so a TargetKeyId naming a key
// elsewhere must be refused rather than written — a written one is a dangling pointer that
// ListAliases reports and every later resolution fails on.
func TestKMSResolution_AnAliasCannotTargetAKeyInAnotherRegion(t *testing.T) {
	ts := arnGuardServer(t)
	westARN, _ := createKMSKeyIn(t, ts, kmsWest2Target)

	status, errCode := kmsCreateAlias(t, ts, kmsTarget, "alias/crossregion", westARN)
	assert.Equal(t, "NotFoundException", errCode,
		"an alias in us-east-1 cannot target a key in us-west-2")
	assert.Equal(t, http.StatusBadRequest, status,
		"KMS publishes NotFoundException at HTTP 400, not 404")
}

// TestKMSResolution_TagResourceRefusesAKeyInAnotherRegion is the refusal the resolution fix makes
// necessary. All three KMS tagging operations publish "Cross-account use: No", and the developer
// guide adds "You cannot tag ... KMS keys in other AWS accounts". Before the fix that prohibition
// had nothing to refuse, because a foreign ARN could only reach a local record.
func TestKMSResolution_TagResourceRefusesAKeyInAnotherRegion(t *testing.T) {
	ts := arnGuardServer(t)
	westARN, westKeyID := createKMSKeyIn(t, ts, kmsWest2Target)

	status, errCode := kmsTagResource(t, ts, kmsTarget, westARN, map[string]string{"env": "prod"})
	assert.Equal(t, "NotFoundException", errCode, "TagResource refuses a key outside the Region")
	assert.Equal(t, http.StatusBadRequest, status, "at HTTP 400, as all three operations publish")

	assert.Empty(t, kmsResourceTagsIn(t, ts, kmsWest2Target, westKeyID),
		"and the refusal wrote nothing to the key it named")
}

// TestKMSResolution_AnARNNamingNoKeyIsRefusedAsInvalid asserts the distinction a caller branches
// on: InvalidArnException says the ARN is wrong and retrying is pointless, NotFoundException says
// the ARN is fine and the resource is not there. AWS publishes both at 400 on all three tagging
// operations, so a caller cannot tell them apart by status alone.
func TestKMSResolution_AnARNNamingNoKeyIsRefusedAsInvalid(t *testing.T) {
	ts := arnGuardServer(t)

	for _, tc := range []struct {
		name string
		arn  string
		code string
	}{
		{"too few ARN segments", "arn:aws:kms", "InvalidArnException"},
		{"another service", "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":key/abc", "InvalidArnException"},
		{"no Region", "arn:aws:kms::" + taggingTestAccount + ":key/abc", "InvalidArnException"},
		{"no account", "arn:aws:kms:us-east-1::key/abc", "InvalidArnException"},
		{"no resource", "arn:aws:kms:us-east-1:" + taggingTestAccount + ":key", "InvalidArnException"},
		{"nested under a key", "arn:aws:kms:us-east-1:" + taggingTestAccount + ":key/abc/def", "InvalidArnException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, errCode := kmsTagResource(t, ts, kmsTarget, tc.arn, map[string]string{"env": "prod"})
			assert.Equal(t, tc.code, errCode, "%s is refused as a malformed ARN", tc.arn)
			assert.Equal(t, http.StatusBadRequest, status, "InvalidArnException is HTTP 400")
		})
	}
}

// TestKMSResolution_ANonARNIdentifierIsTreatedAsAKeyID is the other side of the InvalidArnException
// boundary. AWS's KeyId accepts a bare key ID, so an identifier that does not begin with "arn:" is
// not a malformed ARN — it is a key ID, and one that names nothing is NotFoundException. Answering
// InvalidArnException here would tell a caller its ARN was wrong when it sent no ARN.
//
// The status asserted is the 404 substrate answers today, not the 400 AWS publishes. All three KMS
// tagging operations publish NotFoundException at 400 and fifteen pre-existing KMS sites answer 404;
// that is a compatibility change filed as #923 rather than folded in here, the split #921 took for
// ACM. This assertion pins the current value so #923 has something to change.
func TestKMSResolution_ANonARNIdentifierIsTreatedAsAKeyID(t *testing.T) {
	ts := arnGuardServer(t)

	status, errCode := kmsTagResource(t, ts, kmsTarget, "no-such-key-id", map[string]string{"env": "prod"})
	assert.Equal(t, "NotFoundException", errCode, "a bare identifier is a key ID, not an ARN")
	assert.Equal(t, http.StatusNotFound, status,
		"404 today; AWS publishes 400 and the change is filed separately")
}

// TestKMSResolution_ABareKeyIDAndAliasNameStillResolve is the regression guard on the two forms
// that carry no account. AWS's KeyId parameter documents four accepted forms and only two are ARNs,
// so the fix must not have narrowed the other two to ARNs-only.
func TestKMSResolution_ABareKeyIDAndAliasNameStillResolve(t *testing.T) {
	ts := arnGuardServer(t)
	keyARN, keyID := createKMSKey(t, ts)

	status, errCode := kmsCreateAlias(t, ts, kmsTarget, "alias/bare", keyID)
	require.Empty(t, errCode, "CreateAlias")
	require.Equal(t, http.StatusOK, status, "CreateAlias")

	for _, tc := range []struct{ name, id string }{
		{"a bare key ID", keyID},
		{"a key ARN", keyARN},
		{"a bare alias name", "alias/bare"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, errCode := kmsDescribeKeyID(t, ts, kmsTarget, tc.id)
			assert.Empty(t, errCode, "%s resolves", tc.name)
			assert.Equal(t, keyID, got, "%s resolves to the key", tc.name)
		})
	}
}

// ----- The #835 row --------------------------------------------------------

// TestTaggingKMS_AKeyTagIsReadBackThroughKMS is the cross-readability assertion #765 requires of
// every row: the tagging API and the owning service must agree on where one resource's tags live.
// Both sides build the key through kmsKeyStateKey, which is what makes that true by construction
// rather than by two call sites happening to agree.
func TestTaggingKMS_AKeyTagIsReadBackThroughKMS(t *testing.T) {
	ts := arnGuardServer(t)
	arn, keyID := createKMSKey(t, ts)

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod", "owner": "platform"})

	assert.Equal(t, map[string]string{"env": "prod", "owner": "platform"}, kmsResourceTags(t, ts, keyID),
		"KMS reports the tags the tagging API wrote")
}

// TestTaggingKMS_UntagResourcesRemovesOnlyTheNamedKeys asserts the damaging direction. A removal
// that reaches the wrong record, or removes more than it was asked to, turns an aws:ResourceTag
// Deny into an allow.
func TestTaggingKMS_UntagResourcesRemovesOnlyTheNamedKeys(t *testing.T) {
	ts := arnGuardServer(t)
	arn, keyID := createKMSKey(t, ts)

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod", "owner": "platform"})
	untagResourcesWith(t, ts, arn, "env")

	assert.Equal(t, map[string]string{"owner": "platform"}, kmsResourceTags(t, ts, keyID),
		"only the named key is removed")
}

// TestTaggingKMS_ATagWrittenThroughKMSIsReportedByGetResources is the other direction of the same
// invariant: the scanner must read the member KMS's own TagResource writes. It is the assertion
// that catches a scanner reading "Key"/"Value" out of a record that stores "TagKey"/"TagValue".
func TestTaggingKMS_ATagWrittenThroughKMSIsReportedByGetResources(t *testing.T) {
	ts := arnGuardServer(t)
	arn, keyID := createKMSKey(t, ts)

	status, errCode := kmsTagResource(t, ts, kmsTarget, keyID, map[string]string{"team": "sre"})
	require.Empty(t, errCode, "TagResource")
	require.Equal(t, http.StatusOK, status, "TagResource")

	assert.Equal(t, map[string]string{"team": "sre"}, getResourcesTags(t, ts, arn),
		"GetResources reports the tag KMS's own TagResource wrote")
}

// TestTaggingKMS_GetResourcesReportsTheKey is the scanner half. A resolver arm alone leaves the
// resource writable and invisible — a caller can tag it and then cannot find it.
func TestTaggingKMS_GetResourcesReportsTheKey(t *testing.T) {
	ts := arnGuardServer(t)
	arn, _ := createKMSKey(t, ts)

	assert.Contains(t, getResourcesARNs(t, ts, "kms"), arn,
		"GetResources reports the key under a kms type filter")
}

// TestTaggingKMS_GetResourcesDoesNotReportAKeyFromAnotherRegion asserts the scanner's Region
// scoping. A KMS key is Region-scoped, and TagResources states "you can only tag resources that are
// located in the specified AWS Region for the AWS account" — so a us-west-2 key must not appear in
// a us-east-1 GetResources.
func TestTaggingKMS_GetResourcesDoesNotReportAKeyFromAnotherRegion(t *testing.T) {
	ts := arnGuardServer(t)
	westARN, _ := createKMSKeyIn(t, ts, kmsWest2Target)

	assert.NotContains(t, getResourcesARNs(t, ts, "kms"), westARN,
		"a us-west-2 key is not reported by a us-east-1 GetResources")
}

// TestTaggingKMS_AnAliasARNIsNotTaggableThroughTheTaggingAPI asserts the boundary the resolver
// draws. The developer guide states "You cannot tag aliases, custom key stores, AWS managed keys,
// AWS owned keys, or KMS keys in other AWS accounts", and an alias is the one of those five
// substrate stores in this namespace — so its ARN must be refused rather than resolved to the
// pointer record, which stores no tags at all.
func TestTaggingKMS_AnAliasARNIsNotTaggableThroughTheTaggingAPI(t *testing.T) {
	ts := arnGuardServer(t)
	_, keyID := createKMSKey(t, ts)
	status, errCode := kmsCreateAlias(t, ts, kmsTarget, "alias/nottaggable", keyID)
	require.Empty(t, errCode, "CreateAlias")
	require.Equal(t, http.StatusOK, status, "CreateAlias")

	aliasARN := "arn:aws:kms:us-east-1:" + taggingTestAccount + ":alias/nottaggable"
	failures := tagResourcesFailures(t, ts, "TagResources", aliasARN)
	require.Contains(t, failures, aliasARN, "an alias ARN is refused")

	assert.Empty(t, kmsResourceTags(t, ts, keyID),
		"and nothing was written to the key the alias points at")
}

// TestTaggingKMS_AForeignAccountARNCannotBeTagged is the account-isolation assertion. It is
// emergent rather than guarded in the tagging plugin: kmsResolveARN builds an account-qualified
// state key, so a foreign-account ARN builds a key nothing is stored at, the merge fails
// "resource not found" and the caller gets a FailedResourcesMap entry instead of a silent success.
func TestTaggingKMS_AForeignAccountARNCannotBeTagged(t *testing.T) {
	ts := arnGuardServer(t)
	arn, keyID := createKMSKey(t, ts)
	foreign := strings.Replace(arn, taggingTestAccount, taggingForeignAccount, 1)
	require.NotEqual(t, arn, foreign, "the foreign ARN differs only by account")

	failures := tagResourcesFailures(t, ts, "TagResources", foreign)
	require.Contains(t, failures, foreign, "a foreign-account key ARN is refused")

	assert.Empty(t, kmsResourceTags(t, ts, keyID),
		"and the caller's own key of that ID is untouched")
}

// ----- Determinism ---------------------------------------------------------

// TestTaggingKMS_TagOrderIsDeterministic is the #862 rule applied to KMS's own handler. Both merge
// paths build the stored slice from a Go map, so without a sort the order came from the map's hash
// seed and one recorded run would not replay byte-identically.
func TestTaggingKMS_TagOrderIsDeterministic(t *testing.T) {
	ts := arnGuardServer(t)
	arn, keyID := createKMSKey(t, ts)

	// Enough keys that an unsorted map range is overwhelmingly unlikely to come out ascending, and
	// inserted in descending order so the insertion order is not the answer either.
	tags := map[string]string{"zeta": "5", "epsilon": "4", "delta": "3", "gamma": "2", "beta": "1", "alpha": "0"}
	want := []string{"alpha", "beta", "delta", "epsilon", "gamma", "zeta"}

	t.Run("through KMS's own TagResource", func(t *testing.T) {
		status, errCode := kmsTagResource(t, ts, kmsTarget, keyID, tags)
		require.Empty(t, errCode, "TagResource")
		require.Equal(t, http.StatusOK, status, "TagResource")
		assert.Equal(t, want, kmsTagKeyOrder(t, ts, keyID), "KMS stores tags in key order")
	})

	t.Run("through the tagging API", func(t *testing.T) {
		untagResourcesWith(t, ts, arn, want...)
		tagResourcesWith(t, ts, arn, tags)
		assert.Equal(t, want, kmsTagKeyOrder(t, ts, keyID),
			"mergeRecordTagListTags stores tags in key order")
	})
}
