package emulator

// Secrets Manager resource resolution: the one place a SecretId or ResourceArn parameter becomes a
// state key.
//
// Split out of secretsmanager_plugin.go for the reason kms_tags.go and sns_tags.go were split out of
// their plugins: the resolver is the load-bearing part, and here it was eleven lines that got four
// things wrong for every one of the ten operations that called it.
//
// resolveSecretID split an ARN on ":" and returned the LAST segment, and its callers then keyed the
// load and the store by the caller's own account and Region. Four consequences compounded:
//
//  1. arn:aws:secretsmanager:eu-west-1:999988887777:secret:db-password presented by a us-east-1
//     caller in account 111122223333 addressed that caller's own secret named "db-password".
//     UntagResource is the damaging direction and answered 200 while stripping tags from it;
//     GetSecretValue read its value and DeleteSecret deleted it. This is the rule #826 established
//     for SQS and DynamoDB, #845 carried through the tagging API's resolver, and #910, #918, #922
//     and #925 applied to Step Functions, CloudFront, KMS and SNS: the account and Region come from
//     the ARN, never from the request context.
//
//  2. The type keyword was never checked. A Secrets Manager ARN is
//     arn:{partition}:secretsmanager:{region}:{account}:secret:{name}, whose fifth colon-delimited
//     segment is the literal "secret" — so this service *does* have a segment to anchor a type match
//     against, unlike SNS. Nothing anchored one, so any six-colon string whose tail looked like a
//     name resolved: arn:aws:secretsmanager:r:a:other:foo named a secret called "foo".
//
//  3. The service was never checked either, so an ARN belonging to another service resolved to its
//     own last segment as a secret name.
//
//  4. The length guard fell through to returning the argument, so a malformed ARN — "arn:aws:s3",
//     a URL, an ARN with too few segments — became a *name* and was looked up as one rather than
//     refused. That fall-through is not entirely wrong, because SecretId legitimately accepts a bare
//     name; it is wrong for anything that begins "arn:", which is a caller who meant an ARN and
//     mistyped it.
//
// The name/ARN split is what makes this service different from the five before it. AWS documents
// SecretId as "The ARN or name of the secret", so a bare name is a valid identifier and carries no
// account — which makes the caller's own account the *correct* source for it, and the only case in
// this file where the caller's account is read at all. [smParseSecretARN] therefore takes no account
// or Region parameter, so the ARN path structurally cannot reach for the caller's, and
// [smResolveSecretID] takes them explicitly for the name path alone.
//
// What is deliberately not changed: substrate mints an ARN with no random suffix
// ([generateSecretARN]), where AWS appends "-" plus six random characters to the name. AWS calls out
// the ambiguity that creates — a name may itself end in "-xxxxxx", so trimming at the last "-" cannot
// recover the name in general — and substrate not minting one is what makes ARN-to-name derivation
// exact here. Adding a suffix would be an #826-class change to a value CloudFormation records as a
// PhysicalID, and it would make this resolver ambiguous by construction; it is a separate decision.

import (
	"fmt"
	"net/http"
	"strings"
)

// Secrets Manager state-key prefixes. The namespace holds three kinds: a secret record, the
// per-account index of secret names, and a secret's version payload. Only the secret record stores
// tags, hence [smKeyIsTaggable] in front of the merge.
//
// Every prefix is tested colon-terminated, because "secret" is a prefix of both "secret_names" and
// "secret_version" — the fifth namespace to need that after cert/cert_arns and cfdist/cfdist_ids
// (#835), key/key_ids (#924) and topic/topic_names (#925). Four of the five namespaces reached so far
// have needed it, so it is the default rather than a per-service discovery.
const (
	smSecretKeyPrefix        = "secret:"
	smSecretNamesKeyPrefix   = "secret_names:"
	smSecretVersionKeyPrefix = "secret_version:"
)

// smARNTypeKeyword is the fifth colon-delimited segment of a Secrets Manager secret ARN.
//
// Matched as a whole segment rather than as a prefix, per #910: "secret" must be the segment, not
// merely start it, or "secretpolicy" and every other keyword AWS might add would resolve as a secret.
const smARNTypeKeyword = "secret"

// smTagsJSONMember is the JSON member a secret record stores its tags in.
//
// [SecretState] declares `json:"Tags,omitempty"`. It is named rather than written inline because
// [mergeRecordTagListTags] writes whichever member it is given, and a misspelling would add a second
// tags member while leaving the real one untouched — a tag call that answers 200 and stores nothing.
const smTagsJSONMember = "Tags"

// Secrets Manager spells a tag's two fields Key and Value, as [SMTag] and AWS's own Tag shape do.
// Passed to [mergeRecordTagListTags], which takes both field names as parameters because KMS alone
// among #835's rows spells them TagKey/TagValue.
const (
	smTagKeyField   = "Key"
	smTagValueField = "Value"
)

// smKeyIsTaggable reports whether a Secrets Manager state key names a record that stores tags.
//
// Only the secret record does. A single positive test rather than an enumeration of the two refusals,
// which is the safe direction: a key kind added later is refused by default, and refusing is the
// conservative outcome — merging tags into a record that does not model them writes a member nothing
// reads and reports success. The version payload is not even JSON, so a merge would corrupt it.
func smKeyIsTaggable(key string) bool {
	return strings.HasPrefix(key, smSecretKeyPrefix)
}

// smSecretTarget is the secret an identifier names: the account and Region that own it and its name.
type smSecretTarget struct {
	// AccountID is the account that owns the secret — the ARN's account segment when the identifier
	// was an ARN, and the caller's own only when it was a bare name.
	AccountID string

	// Region is the Region that owns the secret, on the same terms as AccountID.
	Region string

	// Name is the secret name, which is the whole resource portion after the "secret:" keyword.
	Name string
}

// smParseSecretARN parses a Secrets Manager secret ARN and returns the account, Region and name it
// identifies.
//
// It takes no account, Region or *RequestContext, which is what makes "the account and Region come
// from the ARN" structural rather than a thing each of ten operations has to remember — the
// arrangement [sfnResolveARN] settled on for Step Functions, [cfParseDistributionARN] for
// CloudFront, [kmsParseARN] for KMS and [snsParseTopicARN] for SNS.
//
// The shape is arn:{partition}:secretsmanager:{region}:{account}:secret:{name}. The fifth segment is
// matched against [smARNTypeKeyword] as a whole segment, per #910. A secret name may contain "/" and
// is hierarchical in practice, so the name is taken as the entire remainder after the keyword rather
// than split further; it may not contain ":", which is why the remainder is unambiguous.
//
// A malformed ARN is InvalidParameterException/400, which API_DescribeSecret, API_TagResource,
// API_UntagResource and API_GetSecretValue each publish. The message text is substrate's; the
// reference publishes codes and not messages.
func smParseSecretARN(arn string) (smSecretTarget, *AWSError) {
	// arn:aws:secretsmanager:{region}:{account}:secret:{name}
	parts := strings.SplitN(arn, ":", 7)
	if len(parts) < 7 || parts[0] != "arn" || parts[2] != secretsManagerNamespace {
		return smSecretTarget{}, smInvalidIdentifier(arn, "not a Secrets Manager ARN")
	}
	region, account, keyword, name := parts[3], parts[4], parts[5], parts[6]
	if keyword != smARNTypeKeyword {
		return smSecretTarget{}, smInvalidIdentifier(arn, "does not name a secret")
	}
	if region == "" {
		return smSecretTarget{}, smInvalidIdentifier(arn, "names no Region")
	}
	if account == "" {
		return smSecretTarget{}, smInvalidIdentifier(arn, "names no account")
	}
	if name == "" {
		return smSecretTarget{}, smInvalidIdentifier(arn, "names no secret")
	}
	return smSecretTarget{AccountID: account, Region: region, Name: name}, nil
}

// smResolveSecretID resolves a SecretId parameter, which AWS documents as "The ARN or name of the
// secret", into the secret it names.
//
// An ARN goes through [smParseSecretARN] and carries its own account and Region. A bare name carries
// neither, so the caller's are the correct source — and this is the only place in this file that
// reads them. Anything beginning "arn:" that does not parse is refused rather than treated as a name:
// a caller who wrote an ARN prefix meant an ARN, and silently looking up "arn:aws:s3:::bucket" as a
// secret name is how the previous resolver turned a typo into a 200 against the wrong resource, or
// into a phantom lookup that reported the secret absent for the wrong reason.
func smResolveSecretID(secretID, callerAccountID, callerRegion string) (smSecretTarget, *AWSError) {
	if secretID == "" {
		return smSecretTarget{}, smInvalidIdentifier(secretID, "SecretId is required")
	}
	if strings.HasPrefix(secretID, "arn:") {
		return smParseSecretARN(secretID)
	}
	return smSecretTarget{AccountID: callerAccountID, Region: callerRegion, Name: secretID}, nil
}

// smResolveARN parses a Secrets Manager secret ARN and returns the namespace and state key it
// addresses, for the Resource Groups Tagging API.
//
// A thin wrapper over [smParseSecretARN] rather than a second parser, so the tagging API and Secrets
// Manager's own tag operations cannot disagree about which secret an ARN names — the arrangement #826
// established through ecsTagStateKey and #910, #924 and #925 through their own resolvers. It exists
// at all because the two callers need different error *shapes*: Secrets Manager's own operations
// answer an [AWSError] carrying a published code, while [TaggingPlugin.resolveARN] returns a plain
// error that its caller renders into a FailedResourcesMap entry.
//
// It takes an ARN only, with no name fallback: the tagging API's parameter is ResourceARNList and a
// bare name is not an ARN, so there is nothing here for the caller's own account to supply.
//
// Account isolation at this arm is emergent rather than guarded, as ACM's and SNS's are: the state
// key carries the ARN's account and Region, so an ARN naming another account builds a key nothing is
// stored at and the merge reports the resource absent.
func smResolveARN(arn string) (ns, key string, err error) {
	target, arnErr := smParseSecretARN(arn)
	if arnErr != nil {
		return "", "", fmt.Errorf("%s: %s", arnErr.Code, arnErr.Message)
	}
	return secretsManagerNamespace, smSecretStateKey(target.AccountID, target.Region, target.Name), nil
}

// smSecretStateKey returns the state key a secret record is stored at.
//
// A free function rather than a method, so the tagging API's resolver reaches the same address
// Secrets Manager's own operations write to without holding a plugin. Account- and Region-qualified,
// which is what makes cross-account isolation emergent at both arms.
func smSecretStateKey(accountID, region, name string) string {
	return smSecretKeyPrefix + accountID + "/" + region + "/" + name
}

// smSecretNamesStateKey returns the state key the per-account index of secret names is stored at.
func smSecretNamesStateKey(accountID, region string) string {
	return smSecretNamesKeyPrefix + accountID + "/" + region
}

// smSecretVersionStateKey returns the state key a secret version's payload is stored at.
func smSecretVersionStateKey(accountID, region, name, versionID string) string {
	return smSecretVersionKeyPrefix + accountID + "/" + region + "/" + name + "/" + versionID
}

// smInvalidIdentifier reports that a string is not an identifier Secrets Manager accepts, naming the
// reason so a caller can tell a wrong service from a wrong resource type.
func smInvalidIdentifier(id, reason string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterException",
		Message:    fmt.Sprintf("the identifier %q is not valid: %s", id, reason),
		HTTPStatus: http.StatusBadRequest,
	}
}

// smSecretNotFound reports that a well-formed identifier names no secret.
//
// The code is ResourceNotFoundException, which every SecretId-taking operation publishes. The HTTP
// status substrate answers is unchanged by this pass and is tracked in #930: Secrets Manager publishes
// 400 for it, as ACM does (#921) and KMS did (#923), and correcting the status for all ten operations
// at once is a different change from correcting which secret an identifier names. One helper means one
// fix, which is why the status lives here rather than at each call site.
func smSecretNotFound(id string) *AWSError {
	return &AWSError{
		Code:       "ResourceNotFoundException",
		Message:    fmt.Sprintf("no secret found for the identifier %q", id),
		HTTPStatus: http.StatusNotFound,
	}
}
