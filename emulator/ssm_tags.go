package emulator

// Systems Manager resource resolution for the tag operations: the one place a (ResourceType,
// ResourceId) pair or a parameter ARN becomes a state key.
//
// Split out of ssm_plugin.go for the reason kms_tags.go, sns_tags.go and secretsmanager_tags.go were
// split out of theirs: the resolution is the load-bearing part, and here three handlers each got the
// same two things wrong.
//
//  1. ResourceType was decoded and never read. AddTagsToResource, RemoveTagsFromResource and
//     ListTagsForResource each declared `ResourceType string` and then went straight to loadParam, so
//     every SSM tag call was answered as though it named a Parameter Store parameter. AWS publishes
//     ResourceType as Required: Yes over a ten-value enum, so
//     {"ResourceType": "Document", "ResourceId": "MyRunbook"} tagged the *parameter* named
//     "/MyRunbook" — and ListTagsForResource with the same pair read the tag back, so the round trip
//     confirmed that the wrong resource had been tagged. A value outside the enum was accepted, and
//     InvalidResourceType — which AWS publishes at all three operations — was unreachable.
//
//     This is the resource-*type* counterpart of the account confusion #826 established for SQS and
//     DynamoDB and #910, #918, #922, #925 and #928 carried through Step Functions, CloudFront, KMS,
//     SNS and Secrets Manager: an identifier must resolve to the thing it names, and the one thing it
//     must never resolve to is a same-named resource of a different kind.
//
//  2. Anything without a leading "/" was normalized into a name. That is correct for a bare name and
//     wrong for an ARN: arn:aws:ssm:us-east-1:123456789012:parameter/db/password became the *name*
//     "/arn:aws:ssm:us-east-1:123456789012:parameter/db/password" and was looked up as one. It is
//     refused either way, but for the wrong reason and after a normalization that makes the message
//     nonsense — the fall-through #928 removed from Secrets Manager's SecretId, for the same reason: a
//     caller who wrote an ARN prefix meant an ARN.
//
// Systems Manager is the first of #835's rows where the owning service's own tag operations do not
// take an ARN at all. AWS is explicit — "For the Document and Parameter values, use the name of the
// resource" — so a parameter's identifier carries no account or Region, and the caller's own are the
// only possible source for them. That makes "the account comes from the ARN" (#826) inapplicable to
// this half and leaves nothing for it to guard; the confusion this file exists to prevent is between
// resource *types*, not between accounts. The tagging API's half does take an ARN, and
// [ssmParseParameterARN] therefore takes no account, Region or *RequestContext at all, so it
// structurally cannot reach for the caller's.
//
// The leading-slash tolerance is kept, and it is substrate's reading rather than AWS's: AWS documents
// that tolerance explicitly only for OpsMetadata, whose ResourceID may be given "either
// aws/ssm/MyGroup/appmanager or /aws/ssm/MyGroup/appmanager". For a Parameter it is *required* by
// substrate's own behavior — [SSMPlugin.putParameter] normalizes Name to a leading "/", so a caller
// who created "MyParam" would otherwise be unable to tag it.

import (
	"fmt"
	"net/http"
	"strings"
)

// Systems Manager state-key prefixes. The namespace holds a parameter record, the per-account index
// of parameter names, and Run Command's command and invocation records. Only the parameter record
// stores tags, hence [ssmKeyIsTaggable] in front of the merge.
//
// Both prefixes are tested colon-terminated, because "parameter" is a prefix of "parameter_paths" —
// the sixth namespace to need that after cert/cert_arns and cfdist/cfdist_ids (#835), key/key_ids
// (#924), topic/topic_names (#925) and secret/secret_names (#928). Five of the six namespaces reached
// so far have collided, so it is the default rather than a per-service discovery.
const (
	ssmParameterKeyPrefix      = "parameter:"
	ssmParameterPathsKeyPrefix = "parameter_paths:"
)

// ssmARNTypeSegment is the first "/"-delimited segment of the resource portion of a Systems Manager
// parameter ARN.
//
// Matched as a whole segment rather than as a prefix, per #910. The same namespace addresses
// document/, servicesetting/, opsmetadata/, automation-execution/, maintenancewindow/,
// patchbaseline/ and managed-instance/ resources, and a prefix match would grow wrong the moment AWS
// adds a type whose name begins "parameter".
const ssmARNTypeSegment = "parameter"

// ssmTagsJSONMember is the JSON member a parameter record stores its tags in.
//
// [SSMParameter] declares `json:"Tags,omitempty"`. It is named rather than written inline because
// [mergeRecordTagListTags] writes whichever member it is given, and a misspelling would add a second
// tags member while leaving the real one untouched — a tag call that answers 200 and stores nothing.
const ssmTagsJSONMember = "Tags"

// Systems Manager spells a tag's two fields Key and Value, as [SSMTag] and AWS's own Tag shape do.
// Passed to [mergeRecordTagListTags], which takes both field names as parameters because KMS alone
// among #835's rows spells them TagKey/TagValue.
const (
	ssmTagKeyField   = "Key"
	ssmTagValueField = "Value"
)

// ssmResourceTypeParameter is the one member of AWS's ResourceType enum that substrate models a
// taggable resource for.
const ssmResourceTypeParameter = "Parameter"

// ssmResourceTypes is AWS's ResourceType enum for the tag operations, in the order the reference
// publishes it.
//
// Substrate models a taggable resource for exactly one of the ten,
// [ssmResourceTypeParameter]. The other nine are listed so that a value AWS publishes can be told
// apart from a value AWS does not: the first is a resource that does not exist here
// (InvalidResourceId), the second is not a resource type at all (InvalidResourceType). Answering the
// same code for both would tell a caller who mistyped "Parameter" that their parameter is missing.
var ssmResourceTypes = []string{
	"Document",
	"ManagedInstance",
	"MaintenanceWindow",
	ssmResourceTypeParameter,
	"PatchBaseline",
	"OpsItem",
	"OpsMetadata",
	"Automation",
	"Association",
	"CloudConnector",
}

// ssmKeyIsTaggable reports whether a Systems Manager state key names a record that stores tags.
//
// Only the parameter record does. A single positive test rather than an enumeration of the refusals,
// which is the safe direction: a key kind added later is refused by default, and refusing is the
// conservative outcome — merging tags into a record that does not model them writes a member nothing
// reads and reports success. The colon is what keeps "parameter_paths:", whose value is a JSON array
// of names, out.
func ssmKeyIsTaggable(key string) bool {
	return strings.HasPrefix(key, ssmParameterKeyPrefix)
}

// ssmParameterStateKey returns the state key a parameter record is stored at.
//
// A free function rather than a method, so the tagging API's resolver reaches the same address
// Systems Manager's own operations write to without holding a plugin. Account- and Region-qualified,
// which is what makes cross-account and cross-Region isolation emergent at both arms.
//
// The name carries its own leading "/", so the key holds a doubled separator
// ("parameter:{acct}/{region}//db/password"). That is the shape already on disk and is left alone: it
// is unambiguous, and changing it would orphan every stored parameter.
func ssmParameterStateKey(accountID, region, name string) string {
	return ssmParameterKeyPrefix + accountID + "/" + region + "/" + name
}

// ssmParameterPathsStateKey returns the state key the per-account index of parameter names is stored
// at.
func ssmParameterPathsStateKey(accountID, region string) string {
	return ssmParameterPathsKeyPrefix + accountID + "/" + region
}

// ssmParameterTarget is the parameter an ARN names: the account and Region that own it and its name.
type ssmParameterTarget struct {
	// AccountID is the account that owns the parameter, taken from the ARN.
	AccountID string

	// Region is the Region that owns the parameter, taken from the ARN.
	Region string

	// Name is the parameter name, including its leading "/".
	Name string
}

// ssmParseParameterARN parses a Systems Manager parameter ARN and returns the account, Region and
// name it identifies.
//
// It takes no account, Region or *RequestContext, which is what makes "the account and Region come
// from the ARN" structural rather than a thing each caller has to remember — the arrangement
// [sfnResolveARN] settled on for Step Functions, [cfParseDistributionARN] for CloudFront,
// [kmsParseARN] for KMS, [snsParseTopicARN] for SNS and [smParseSecretARN] for Secrets Manager.
//
// The shape is arn:{partition}:ssm:{region}:{account}:parameter{name}, and the absence of a
// separator before the name is not a typo: [ssmParameterARN] absorbs the name's own leading "/", so
// a parameter named "/db/password" has the resource portion "parameter/db/password". The derivation
// back is therefore exact — the first "/"-delimited segment must be [ssmARNTypeSegment], and the
// name is the rest with the "/" restored. Taking the last "/"-separated component instead would
// truncate a hierarchical name to "password".
func ssmParseParameterARN(arn string) (ssmParameterTarget, *AWSError) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != ssmNamespace {
		return ssmParameterTarget{}, ssmInvalidResourceID(arn, "not a Systems Manager ARN")
	}
	region, account, resource := parts[3], parts[4], parts[5]
	segment, rest, ok := strings.Cut(resource, "/")
	if !ok || segment != ssmARNTypeSegment {
		return ssmParameterTarget{}, ssmInvalidResourceID(arn, "does not name a parameter")
	}
	if region == "" {
		return ssmParameterTarget{}, ssmInvalidResourceID(arn, "names no Region")
	}
	if account == "" {
		return ssmParameterTarget{}, ssmInvalidResourceID(arn, "names no account")
	}
	return ssmParameterTarget{AccountID: account, Region: region, Name: "/" + rest}, nil
}

// ssmResolveARN parses a Systems Manager parameter ARN and returns the namespace and state key it
// addresses, for the Resource Groups Tagging API.
//
// A thin wrapper over [ssmParseParameterARN] rather than a second parser, so the tagging API and
// Systems Manager's own tag operations cannot disagree about which parameter an identifier names —
// the arrangement #826 established through ecsTagStateKey and #910, #924, #925 and #928 through
// their own resolvers. It exists at all because the two callers need different error *shapes*:
// Systems Manager's own operations answer an [AWSError] carrying a published code, while
// [TaggingPlugin.resolveARN] returns a plain error that its caller renders into a
// FailedResourcesMap entry.
//
// It takes an ARN only. The tagging API's parameter is ResourceARNList, so unlike Systems Manager's
// own ResourceId there is no name form to accept here.
//
// Account and Region isolation at this arm is emergent rather than guarded, as ACM's, SNS's and
// Secrets Manager's are: the state key carries the ARN's account and Region, so an ARN naming
// another account builds a key nothing is stored at and the merge reports the resource absent.
func ssmResolveARN(arn string) (ns, key string, err error) {
	target, arnErr := ssmParseParameterARN(arn)
	if arnErr != nil {
		return "", "", fmt.Errorf("%s: %s", arnErr.Code, arnErr.Message)
	}
	return ssmNamespace, ssmParameterStateKey(target.AccountID, target.Region, target.Name), nil
}

// ssmResolveTagTarget validates a (ResourceType, ResourceId) pair from AddTagsToResource,
// RemoveTagsFromResource or ListTagsForResource and returns the parameter name it addresses.
//
// The three refusals are distinct on purpose, because they tell a caller three different things:
//
//   - A ResourceType that is not one of AWS's ten values is InvalidResourceType/400, because that
//     code's own description — "The resource type isn't valid" — is the true statement about it.
//   - A ResourceType AWS publishes but substrate models no taggable resource for is
//     InvalidResourceId/400: the type is real, and it is the identifier that names nothing. This is
//     the honest-empty behavior #827 established — substrate reports the resource absent rather than
//     inventing one, and does not claim AWS's own type is invalid.
//   - A ResourceId beginning "arn:" is InvalidResourceId/400 rather than being normalized into a
//     name, per #928.
//
// An absent ResourceType is ValidationException/400, matching [SSMPlugin.putParameter]'s treatment of
// an absent required member. AWS's own server-side code for it cannot be verified, because
// ResourceType is Required: Yes and every SDK refuses the call before it reaches the service.
func ssmResolveTagTarget(resourceType, resourceID string) (string, *AWSError) {
	if resourceType == "" {
		return "", &AWSError{
			Code:       "ValidationException",
			Message:    "ResourceType is required",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	known := false
	for _, t := range ssmResourceTypes {
		if t == resourceType {
			known = true
			break
		}
	}
	if !known {
		return "", ssmInvalidResourceType(resourceType)
	}
	if resourceType != ssmResourceTypeParameter {
		return "", ssmInvalidResourceID(resourceID, fmt.Sprintf("no %s exists", resourceType))
	}
	if resourceID == "" {
		return "", ssmInvalidResourceID(resourceID, "ResourceId is required")
	}
	if strings.HasPrefix(resourceID, "arn:") {
		return "", ssmInvalidResourceID(resourceID, "a Parameter ResourceId is the parameter name, not an ARN")
	}
	if !strings.HasPrefix(resourceID, "/") {
		return "/" + resourceID, nil
	}
	return resourceID, nil
}
