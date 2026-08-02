package emulator

import (
	"fmt"
	"net/http"
	"strings"
)

// ec2ReservedTagPrefix is the tag-key prefix EC2 reserves for its own use. A caller
// cannot create, edit, or delete a tag whose key begins with it.
//
// The match is case-sensitive, and deliberately so: the EC2 tagging documentation
// states that "tag keys and values are case-sensitive", and every observed rejection
// quotes the lowercase form. A case-folded check would reject "AWS:foo", which real
// EC2 accepts as an ordinary user tag — trading one infidelity for another.
//
// Source note: the CreateTags reference's Errors section is empty, so neither the
// error code nor its message is derivable from the API model. Both come from observed
// real-AWS responses — two independent captures giving byte-identical text, one of
// which records "Service: AmazonEC2; Status Code: 400; Error Code:
// InvalidParameterValue". Both captures are of RunInstances tag-on-create rather than
// CreateTags; the message is evidently shared across the tagging paths, but substrate
// has not observed it on CreateTags directly.
const ec2ReservedTagPrefix = "aws:"

// ec2ReservedTagKeyError returns the error EC2 raises for a reserved tag key.
//
// The observed message does not name the offending key, so neither does this. See
// [ec2ReservedTagPrefix] for where the code, status, and wording come from.
func ec2ReservedTagKeyError() *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    "Tag keys starting with 'aws:' are reserved for internal use",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2MaxTagsPerResource is the number of user tags EC2 allows on one resource.
//
// From the tagging documentation's restrictions: "Maximum number of tags per resource –
// 50". Tags whose keys carry [ec2ReservedTagPrefix] are excluded from the count, per the
// same list — "Tags with the aws: prefix do not count against your tags per resource
// limit" — so a resource can hold 50 user tags plus any number of reserved ones.
const ec2MaxTagsPerResource = 50

// ec2TagLimitExceededError returns the error EC2 raises when a resource would exceed
// [ec2MaxTagsPerResource].
//
// Provenance, which is split: the *code* is documented — EC2's client-error table lists
// TagLimitExceeded as "You've reached the limit on the number of tags that you can
// assign to the specified resource." The *message* is not published anywhere, so the
// wording here is moto's, from a reimplementation rather than a captured response. That
// is a weaker claim than the code's and is marked as such deliberately: SDKs dispatch on
// Error.Code, so the code is the part a consumer's error branch turns on.
func ec2TagLimitExceededError() *AWSError {
	return &AWSError{
		Code:       "TagLimitExceeded",
		Message:    "The maximum number of Tags for a resource has been reached.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2CheckTagLimit returns an error if merging incoming into existing would leave a
// resource with more than [ec2MaxTagsPerResource] user tags, or nil.
//
// The count is over the **post-merge key set**, excluding reserved keys, which is what
// gets both of the documented rules right in one expression:
//
//   - A key already on the resource adds nothing, so overwriting an existing tag on a
//     resource already at the limit succeeds. Written as len(existing)+len(incoming) it
//     would fail instead — real AWS permits it, and getmoto/moto#8151 reports exactly
//     that case.
//   - Reserved keys are excluded from both sides, so they neither count nor consume
//     room. This is load-bearing rather than pedantry: substrate stamps
//     [ec2FleetIDTagKey] on every fleet instance, so a counter that included it would
//     reject a 50-tag launch template on a fleet instance that real EC2 accepts.
//
// Callers on a tag-on-create path must check before the resource is created; see
// [ec2CheckReservedTagKeys] for the rollback rule both checks follow.
func ec2CheckTagLimit(existing, incoming []EC2Tag) *AWSError {
	keys := make(map[string]struct{}, len(existing)+len(incoming))
	for _, t := range existing {
		if !strings.HasPrefix(t.Key, ec2ReservedTagPrefix) {
			keys[t.Key] = struct{}{}
		}
	}
	for _, t := range incoming {
		if !strings.HasPrefix(t.Key, ec2ReservedTagPrefix) {
			keys[t.Key] = struct{}{}
		}
	}
	if len(keys) > ec2MaxTagsPerResource {
		return ec2TagLimitExceededError()
	}
	return nil
}

// ec2LaunchTagsForResource collects the TagSpecification.N tags scoped to
// resourceType, in the request's Tag.N order.
//
// This is the single parser for tag-on-create across every EC2 operation that
// accepts TagSpecification.N — RunInstances, CreateFleet, CreateImage and
// CreateNatGateway. It exists as one function because it is what every
// tag-on-create path must be checked at: four separate copies of this loop had
// drifted apart, and only the RunInstances one was reachable by
// [ec2CheckReservedTagKeys] (#468).
//
// A specification whose ResourceType names a different resource is skipped rather
// than ending the walk, so TagSpecification.1=volume followed by
// TagSpecification.2=instance still yields the instance's tags. An absent or empty
// ResourceType ends it, which is how the query protocol terminates an indexed list.
//
// Note that authz.go builds aws:RequestTag condition keys from the same params with
// its own walk, deliberately: it evaluates a policy against the request rather than
// producing resource state, and the two must not share a filter on resourceType.
func ec2LaunchTagsForResource(params map[string]string, resourceType string) []EC2Tag {
	var tags []EC2Tag
	for n := 1; ; n++ {
		rt, ok := params[fmt.Sprintf("TagSpecification.%d.ResourceType", n)]
		if !ok || rt == "" {
			break
		}
		if rt != resourceType {
			continue
		}
		for m := 1; ; m++ {
			key, keyOK := params[fmt.Sprintf("TagSpecification.%d.Tag.%d.Key", n, m)]
			if !keyOK || key == "" {
				break
			}
			tags = append(tags, EC2Tag{
				Key:   key,
				Value: params[fmt.Sprintf("TagSpecification.%d.Tag.%d.Value", n, m)],
			})
		}
	}
	return tags
}

// ec2CheckReservedTagKeys returns an error if any tag uses a reserved key, or nil.
//
// The tags are checked in slice order — extractEC2Tags and
// [ec2LaunchTagsForResource] both preserve the request's Tag.N numbering — so the
// request that is rejected is decided identically on every run. A map would make it
// iteration-order dependent, which is the opposite of what this emulator is for.
//
// Callers must check before applying anything. CreateTags accepts up to 1000 resource
// IDs, and rejecting partway through the loop would leave the earlier resources tagged
// and the rest not, a state real EC2 never produces.
//
// On a tag-on-create path the same rule means checking before the resource is created
// at all. The tagging documentation is explicit — "If tags cannot be applied during
// resource creation, we roll back the resource creation process. This ensures that
// resources are either created with tags or not created at all" — so a rejected
// request must leave nothing behind.
func ec2CheckReservedTagKeys(tags []EC2Tag) *AWSError {
	for _, t := range tags {
		if strings.HasPrefix(t.Key, ec2ReservedTagPrefix) {
			return ec2ReservedTagKeyError()
		}
	}
	return nil
}
