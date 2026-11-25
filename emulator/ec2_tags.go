package emulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"
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
		if !taggingReservedKey(t.Key) {
			keys[t.Key] = struct{}{}
		}
	}
	for _, t := range incoming {
		if !taggingReservedKey(t.Key) {
			keys[t.Key] = struct{}{}
		}
	}
	if len(keys) > ec2MaxTagsPerResource {
		return ec2TagLimitExceededError()
	}
	return nil
}

// ec2MaxTagKeyLength and ec2MaxTagValueLength are the tag length limits, from the
// tagging documentation's restrictions list: "Maximum key length – 128 Unicode
// characters in UTF-8" and "Maximum value length – 256 Unicode characters in UTF-8".
//
// The unit is **Unicode characters, not bytes**, which is why [ec2CheckTagLengths]
// counts runes. A 128-emoji key is 128 characters and 512 bytes; it is legal, and a
// byte-counting check would reject it. The two agree on ASCII, so an ASCII-only test
// suite passes under either and cannot tell them apart.
//
// There is no lower bound. The same documentation states "You can set the value of a
// tag to an empty string, but you can't set the value of a tag to null", so an empty
// value is legal and the check is upper-bound only. A key is required by the query
// encoding — [extractEC2Tags] ends its walk on an absent or empty Tag.N.Key — so an
// empty key is not expressible rather than rejected here.
const (
	ec2MaxTagKeyLength   = 128
	ec2MaxTagValueLength = 256
)

// ec2TagLengthError returns the error EC2 raises for a tag key or value over its
// length limit. what names the part that was too long ("key" or "value").
//
// Provenance, which is the weakest of any error in the EC2 tagging paths and is marked
// as such deliberately. The *code* is by analogy with [ec2ReservedTagKeyError]:
// InvalidParameterValue, HTTP 400, is what real AWS answers for the other
// tag-restriction violation on the same operations, and CreateTags' Errors section is
// empty so the model supplies nothing. The *message text is substrate's own* — no
// captured real-AWS length rejection was found, in moto or LocalStack either. It
// follows sqsMessageTooLong's precedent of interpolating the limit and the actual
// length, because the message is the only place a caller learns which of the two
// limits they hit and by how much.
func ec2TagLengthError(what string, limit, actual int) *AWSError {
	return &AWSError{
		Code: "InvalidParameterValue",
		Message: fmt.Sprintf(
			"Tag %s must be no more than %d Unicode characters in UTF-8; the supplied %s is %d",
			what, limit, what, actual),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2CheckTagLengths returns an error if any tag's key or value exceeds its length
// limit, or nil. Pass an empty value on a key-only path such as DeleteTags, which
// names keys and treats the value as optional.
//
// **Reserved keys are checked too**, which is deliberately narrower than
// [ec2CheckTagLimit]'s exemption sitting a few lines above, and the difference is worth
// stating because the adjacent function doing the opposite is what a reader will trip
// on. The exemption in the restrictions list is scoped to the count alone — "Tags with
// the aws: prefix do not count against your tags per resource limit" — and nothing in
// it exempts a reserved key from either length. Substrate's own [ec2FleetIDTagKey] is
// far under both limits, so checking them changes nothing it stamps.
//
// That choice is currently unobservable, and saying so is more useful than implying
// otherwise: [ec2CheckTagRules] runs [ec2CheckReservedTagKeys] first, so a caller's
// reserved key is refused for being reserved before its length is ever measured. The
// exemption question therefore decides no wire behavior today. Checking reserved keys is
// still the right code — it is correct for any future path that checks lengths alone, and
// it is the reading the documentation supports — but it is a clarity decision rather than
// a behavioral one.
//
// Tags are checked in slice order, so which tag a mixed request is rejected on is
// decided identically on every run — the same reason [ec2CheckReservedTagKeys] avoids a
// map. Callers on a tag-on-create path must check before the resource is created; see
// [ec2CheckReservedTagKeys] for the rollback rule all three checks follow.
func ec2CheckTagLengths(tags []EC2Tag) *AWSError {
	for _, t := range tags {
		if n := utf8.RuneCountInString(t.Key); n > ec2MaxTagKeyLength {
			return ec2TagLengthError("key", ec2MaxTagKeyLength, n)
		}
		if n := utf8.RuneCountInString(t.Value); n > ec2MaxTagValueLength {
			return ec2TagLengthError("value", ec2MaxTagValueLength, n)
		}
	}
	return nil
}

// ec2CheckTagRules runs every tag restriction substrate models over one tag list:
// reserved keys, then lengths. It is what a tag-on-create path calls, so a new
// restriction is added in one place rather than at each of the eleven call sites that
// had to be found by hand for #490.
//
// The per-resource count is *not* included, because it needs the resource's existing
// tags as well and its callers pass different things for them — see [ec2CheckTagLimit].
func ec2CheckTagRules(tags []EC2Tag) *AWSError {
	if awsErr := ec2CheckReservedTagKeys(tags); awsErr != nil {
		return awsErr
	}
	return ec2CheckTagLengths(tags)
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
// Note that authz.go builds aws:RequestTag condition keys from the same params with
// its own walk, deliberately: it evaluates a policy against the request rather than
// producing resource state, and the two must not share a filter on resourceType.
func ec2LaunchTagsForResource(params map[string]string, resourceType string) []EC2Tag {
	return ec2TagSpecificationTags(params, "", resourceType)
}

// ec2CheckTagSpecificationTypes returns an error if any TagSpecification.N names a
// ResourceType other than want, or nil.
//
// AWS states the rule on RegisterImage: "To tag the AMI, the value for ResourceType must be
// image. If you specify another value for ResourceType, the request fails." That is a
// refusal, and [ec2TagSpecificationTags] performs a silent skip instead — correct for an
// operation that accepts several scopes, since CreateImage tags an AMI and its snapshots and
// a snapshot-scoped specification is not a mistake there, and wrong for one that accepts a
// single scope, where the skip drops the caller's tags and reports success.
//
// The walk ends on the first absent or empty ResourceType, matching
// [ec2TagSpecificationTags] exactly: if the two disagreed about which specifications exist,
// this check could pass a list whose tags that one then read, or refuse one it would ignore.
//
// Provenance: the code is substrate's reading. RegisterImage's Errors section is empty and
// AWS says only that "the request fails", so nothing publishes one. InvalidParameterValue is
// EC2's gloss for "A value specified in a parameter is not valid, is unsupported, or cannot
// be used", which is what a resource type the operation does not accept is.
func ec2CheckTagSpecificationTypes(params map[string]string, want string) *AWSError {
	for n := 1; ; n++ {
		rt, ok := params[fmt.Sprintf("TagSpecification.%d.ResourceType", n)]
		if !ok || rt == "" {
			return nil
		}
		if rt != want {
			return &AWSError{
				Code: "InvalidParameterValue",
				Message: fmt.Sprintf(
					"'%s' is not a valid resource type for this request; the value for ResourceType must be %s",
					rt, want),
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
}

// ec2TagSpecificationTags collects the tags a TagSpecification.N list scopes to
// resourceType, under an optional param-name prefix.
//
// prefix serves the nested form a launch template uses:
// "LaunchTemplateData.TagSpecification.1.Tag.1.Key" is the same list one level in
// (#471). It is a parameter rather than a fifth copy of this loop because the whole
// point of [ec2LaunchTagsForResource] is that there is exactly one walk to check.
//
// A specification whose ResourceType names a different resource is skipped rather
// than ending the walk, so TagSpecification.1=volume followed by
// TagSpecification.2=instance still yields the instance's tags. An absent or empty
// ResourceType ends it, which is how the query protocol terminates an indexed list.
func ec2TagSpecificationTags(params map[string]string, prefix, resourceType string) []EC2Tag {
	var tags []EC2Tag
	for n := 1; ; n++ {
		rt, ok := params[fmt.Sprintf("%sTagSpecification.%d.ResourceType", prefix, n)]
		if !ok || rt == "" {
			break
		}
		if rt != resourceType {
			continue
		}
		for m := 1; ; m++ {
			key, keyOK := params[fmt.Sprintf("%sTagSpecification.%d.Tag.%d.Key", prefix, n, m)]
			if !keyOK || key == "" {
				break
			}
			tags = append(tags, EC2Tag{
				Key:   key,
				Value: params[fmt.Sprintf("%sTagSpecification.%d.Tag.%d.Value", prefix, n, m)],
			})
		}
	}
	return tags
}

// ec2CheckTemplateTags returns an error if a launch template's tags break any tag
// rule, or nil.
//
// Run when a template or a version of one is created, so a template carrying a
// reserved key, an over-long key or value, or more than [ec2MaxTagsPerResource] is
// refused up front rather than at every launch that names it. Real EC2 rejects at
// creation too: every rule is on the tag and the count, not on the operation.
//
// Every scope the template records is checked, and each is counted against the limit
// on its own: the limit is per resource, and the instance and its volumes are
// different resources (#670).
//
// The launch path checks again, deliberately — see the fallback in
// [EC2Plugin.runInstancesWithTags] for why a stored template can still carry one.
func ec2CheckTemplateTags(d EC2LaunchTemplateData) *AWSError {
	for _, scope := range [][]EC2Tag{d.TagSpecifications, d.VolumeTagSpecifications} {
		if awsErr := ec2CheckTagRules(scope); awsErr != nil {
			return awsErr
		}
		if awsErr := ec2CheckTagLimit(nil, scope); awsErr != nil {
			return awsErr
		}
	}
	return nil
}

// ec2HasUserTags reports whether tags holds any key that is not reserved.
//
// It is how the launch-template tag fallback decides whether the caller named tags
// of their own (#471). A plain len(tags) == 0 would answer "yes" for every fleet
// instance, because substrate has already appended [ec2FleetIDTagKey] by then — so a
// fleet launched from a tagging template would silently lose the template's tags,
// which is the very defect #471 exists to close.
func ec2HasUserTags(tags []EC2Tag) bool {
	for _, t := range tags {
		if !taggingReservedKey(t.Key) {
			return true
		}
	}
	return false
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
		if taggingReservedKey(t.Key) {
			return ec2ReservedTagKeyError()
		}
	}
	return nil
}

// ----- Tagging API provider ------------------------------------------------

// EC2 answers the Resource Groups Tagging API for its own resources (#1609): GetResources through
// [EC2Plugin.listTaggedResources], and TagResources, UntagResources and CloudFormation's stamps
// through [EC2Plugin.tagResourceByARN] and [EC2Plugin.tagRecord], under [EC2Plugin.tagMu], the lock
// CreateTags and DeleteTags hold. CloudFormation's ID-addressed stamps take the same lock through
// [ec2ApplyTagsThroughOwner]. The functions below take the state rather than the plugin, so the
// tagging plugin's direct path, used when no EC2 plugin is registered, runs the same code.

// taggingEC2ScannedType is one EC2 type the tagging API scans by state-key prefix.
type taggingEC2ScannedType struct {
	// arnType is the type segment of the resource's ARN.
	arnType string

	// keyPrefix is the type segment of the resource's state key, colon included.
	keyPrefix string
}

// taggingEC2ScannedTypes are the EC2 types #1523 added, whose records [ec2MergeTaggedRecord] merges
// on the raw record.
var taggingEC2ScannedTypes = []taggingEC2ScannedType{
	{arnType: "security-group", keyPrefix: "sg:"},
	{arnType: "launch-template", keyPrefix: "lt:"},
	{arnType: "volume", keyPrefix: "volume:"},
}

// taggingEC2RawMergedKey reports whether key belongs to one of [taggingEC2ScannedTypes].
func taggingEC2RawMergedKey(key string) bool {
	for _, t := range taggingEC2ScannedTypes {
		if strings.HasPrefix(key, t.keyPrefix) {
			return true
		}
	}
	return false
}

// taggingEC2ARN builds an EC2 resource ARN.
func taggingEC2ARN(region, accountID, arnType, id string) string {
	return "arn:aws:ec2:" + region + ":" + accountID + ":" + arnType + "/" + id
}

// ec2TagStateKey resolves the resource portion of an EC2 ARN to the state key of the record it
// names. The account and Region come from the ARN, never from the caller, so an ARN naming another
// account's resource resolves that resource or none.
func ec2TagStateKey(region, account, resource string) (string, error) {
	// arn:aws:ec2:{region}:{acct}:instance/{id}
	if strings.HasPrefix(resource, "instance/") {
		id := strings.TrimPrefix(resource, "instance/")
		return "instance:" + account + "/" + region + "/" + id, nil
	}
	// arn:aws:ec2:{region}:{acct}:elastic-ip/{allocationId} (#1191).
	if id, ok := strings.CutPrefix(resource, "elastic-ip/"); ok && id != "" {
		return "eip:" + account + "/" + region + "/" + id, nil
	}
	// The three types GetResources scans (#1523), each under the ARN the Service
	// Authorization Reference publishes for it.
	for _, t := range taggingEC2ScannedTypes {
		if id, ok := strings.CutPrefix(resource, t.arnType+"/"); ok && id != "" && !strings.Contains(id, "/") {
			return t.keyPrefix + account + "/" + region + "/" + id, nil
		}
	}
	return "", unsupportedTagResource("EC2 %q is not a taggable resource type", resource)
}

// ec2ScanTaggedInstances returns the caller's instances. A record that cannot be read or decoded is
// reported as an error alongside the ones that could be, through [taggingScanRecords], rather than
// skipped silently (#1523).
func ec2ScanTaggedInstances(ctx context.Context, state StateManager, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	err := taggingScanRecords(ctx, state, ec2Namespace, taggingScanPrefix("instance:", reqCtx), func(inst EC2Instance) {
		out = append(out, resourceTagMapping{
			ResourceARN: taggingEC2ARN(inst.Region, inst.AccountID, "instance", inst.InstanceID),
			Tags:        ec2TagsToTaggingTags(inst.Tags),
			everTagged:  inst.EverTagged,
		})
	})
	if err != nil {
		return out, fmt.Errorf("scan ec2 instances: %w", err)
	}
	return out, nil
}

// ec2ScanTaggedElasticIPs returns the caller's Elastic IP addresses (#1191), under the ARN the
// Service Authorization Reference publishes for the type,
// arn:${Partition}:ec2:${Region}:${Account}:elastic-ip/${AllocationId}.
//
// A record that cannot be read or decoded fails the scan, through [ec2ScanRecords], rather than
// being dropped from the listing; [TaggingPlugin.scanAllResources] logs the failure.
func ec2ScanTaggedElasticIPs(ctx context.Context, state StateManager, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	err := ec2ScanRecords(ctx, state, taggingScanPrefix("eip:", reqCtx), func(_ string, eip EC2ElasticIP) {
		out = append(out, resourceTagMapping{
			ResourceARN: "arn:aws:ec2:" + eip.Region + ":" + eip.AccountID + ":elastic-ip/" + eip.AllocationID,
			Tags:        ec2TagsToTaggingTags(eip.Tags),
			everTagged:  eip.EverTagged,
		})
	})
	if err != nil {
		return nil, fmt.Errorf("scan ec2 elastic IPs: %w", err)
	}
	return out, nil
}

// ec2ScanTaggedSecurityGroups returns the caller's security groups.
func ec2ScanTaggedSecurityGroups(ctx context.Context, state StateManager, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	err := taggingScanRecords(ctx, state, ec2Namespace, taggingScanPrefix("sg:", reqCtx), func(sg EC2SecurityGroup) {
		out = append(out, resourceTagMapping{
			ResourceARN: taggingEC2ARN(sg.Region, sg.AccountID, "security-group", sg.GroupID),
			Tags:        ec2TagsToTaggingTags(sg.Tags),
			everTagged:  sg.EverTagged,
		})
	})
	if err != nil {
		return out, fmt.Errorf("scan ec2 security groups: %w", err)
	}
	return out, nil
}

// ec2ScanTaggedLaunchTemplates returns the caller's launch templates.
func ec2ScanTaggedLaunchTemplates(ctx context.Context, state StateManager, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	err := taggingScanRecords(ctx, state, ec2Namespace, taggingScanPrefix("lt:", reqCtx), func(lt EC2LaunchTemplate) {
		out = append(out, resourceTagMapping{
			ResourceARN: taggingEC2ARN(lt.Region, lt.AccountID, "launch-template", lt.LaunchTemplateID),
			Tags:        ec2TagsToTaggingTags(lt.Tags),
			everTagged:  lt.EverTagged,
		})
	})
	if err != nil {
		return out, fmt.Errorf("scan ec2 launch templates: %w", err)
	}
	return out, nil
}

// ec2ScanTaggedVolumes returns the caller's EBS volumes, including those a launch created from a
// TagSpecifications entry with ResourceType volume.
func ec2ScanTaggedVolumes(ctx context.Context, state StateManager, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	err := taggingScanRecords(ctx, state, ec2Namespace, ec2VolumeStatePrefix(reqCtx.AccountID, reqCtx.Region), func(v EC2Volume) {
		out = append(out, resourceTagMapping{
			ResourceARN: taggingEC2ARN(v.Region, v.AccountID, "volume", v.VolumeID),
			Tags:        ec2TagsToTaggingTags(v.Tags),
			everTagged:  v.EverTagged,
		})
	})
	if err != nil {
		return out, fmt.Errorf("scan ec2 volumes: %w", err)
	}
	return out, nil
}

// ec2ListTaggedResources returns every EC2 resource the tagging API reports: instances, Elastic
// IPs, security groups, launch templates and volumes. Each type's failure is joined into the error
// and the others are still listed, so one unreadable type hides no other.
func ec2ListTaggedResources(ctx context.Context, state StateManager, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	var out []resourceTagMapping
	var errs []error
	for _, scan := range []func(context.Context, StateManager, *RequestContext) ([]resourceTagMapping, error){
		ec2ScanTaggedInstances,
		ec2ScanTaggedElasticIPs,
		ec2ScanTaggedSecurityGroups,
		ec2ScanTaggedLaunchTemplates,
		ec2ScanTaggedVolumes,
	} {
		resources, err := scan(ctx, state, reqCtx)
		out = append(out, resources...)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return out, errors.Join(errs...)
}

// ec2MergeTaggedRecord merges addTags and removeKeys into the EC2 record raw, stored at key.
func ec2MergeTaggedRecord(
	goCtx context.Context, state StateManager, key string, raw []byte,
	addTags map[string]string, removeKeys []string,
) error {
	// The namespace holds Elastic IPs as well as instances (#1191), and decoding one as
	// the other would rewrite it without its own members.
	if strings.HasPrefix(key, "eip:") {
		var eip EC2ElasticIP
		if err := json.Unmarshal(raw, &eip); err != nil {
			return fmt.Errorf("unmarshal EC2ElasticIP: %w", err)
		}
		eip.EverTagged = taggingEverTagged(eip.EverTagged, len(eip.Tags), len(addTags))
		eip.Tags = mergeEC2Tags(eip.Tags, addTags, removeKeys)
		updated, err := json.Marshal(eip)
		if err != nil {
			return fmt.Errorf("tagging mergeResourceTags marshal: %w", err)
		}
		return state.Put(goCtx, ec2Namespace, key, updated)
	}
	// Security groups, launch templates and volumes (#1523) are merged on the raw record,
	// through the writer EC2's own CreateTags and DeleteTags use, so every member those records
	// carry survives — including the ones a decode into a struct here would not know about.
	if taggingEC2RawMergedKey(key) {
		return mergeRecordEC2Tags(goCtx, state, key, raw, addTags, removeKeys)
	}
	var inst EC2Instance
	if err := json.Unmarshal(raw, &inst); err != nil {
		return fmt.Errorf("unmarshal EC2Instance: %w", err)
	}
	inst.EverTagged = taggingEverTagged(inst.EverTagged, len(inst.Tags), len(addTags))
	inst.Tags = mergeEC2Tags(inst.Tags, addTags, removeKeys)
	updated, err := json.Marshal(inst)
	if err != nil {
		return fmt.Errorf("tagging mergeResourceTags marshal: %w", err)
	}
	return state.Put(goCtx, ec2Namespace, key, updated)
}

// mergeRecordEC2Tags merges tags into the EC2 record at key through [mergeRecordTagListTags], which
// stamps the previously-tagged flag from the pre-merge count and keeps every other member of the
// record as stored.
func mergeRecordEC2Tags(
	goCtx context.Context, state StateManager, key string, raw []byte,
	addTags map[string]string, removeKeys []string,
) error {
	updated, err := mergeRecordTagListTags(raw, "tags", "key", "value", addTags, removeKeys)
	if err != nil {
		return fmt.Errorf("tagging merge %s: %w", key, err)
	}
	if err := state.Put(goCtx, ec2Namespace, key, updated); err != nil {
		return fmt.Errorf("tagging put %s: %w", key, err)
	}
	return nil
}

// listTaggedResources implements [taggableResourceLister].
func (p *EC2Plugin) listTaggedResources(ctx context.Context, reqCtx *RequestContext) ([]resourceTagMapping, error) {
	return ec2ListTaggedResources(ctx, p.state, reqCtx)
}

// tagResourceByARN implements [resourceTagger]. What it adds is held to CreateTags' rules and what
// it removes to DeleteTags' ([ec2CheckTagRules]), so a caller can neither write nor strip an
// `aws:` key.
func (p *EC2Plugin) tagResourceByARN(
	_ *RequestContext, arn string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	parts, ok := taggingSplitARN(arn)
	if !ok {
		return taggingResolveFailed(fmt.Errorf("invalid ARN: %q", arn))
	}
	key, err := ec2TagStateKey(parts[3], parts[4], parts[5])
	if err != nil {
		return taggingResolveFailed(err)
	}
	if awsErr := ec2TagKeyRefusal(add, remove); awsErr != nil {
		return taggingQuotaRefusal(awsErr)
	}
	return p.tagRecord(context.Background(), key, add, remove, quota)
}

// ec2TagKeyRefusal holds a tagging API write to EC2's rules ([ec2CheckTagRules]): what it adds to
// CreateTags' and what it removes to DeleteTags'. Both the EC2 plugin and the tagging plugin's
// direct path apply it, before looking for the resource, as CreateTags checks its tags before its
// resource IDs.
func ec2TagKeyRefusal(add map[string]string, remove []string) *AWSError {
	checked := make([]EC2Tag, 0, len(add)+len(remove))
	for _, k := range taggingSortedKeys(add) {
		checked = append(checked, EC2Tag{Key: k, Value: add[k]})
	}
	for _, k := range remove {
		checked = append(checked, EC2Tag{Key: k})
	}
	return ec2CheckTagRules(checked)
}

// applyTagsByID applies tags to the EC2 resource id through [ec2ApplyTagsToResource], under
// [EC2Plugin.tagMu]. It applies no key rules, because its callers are CloudFormation's stamp and
// stack-tag propagation, which write `aws:cloudformation:*`.
func (p *EC2Plugin) applyTagsByID(reqCtx *RequestContext, id string, tags []EC2Tag, remove bool) error {
	defer p.lockTagWrite([]string{id})()
	return ec2ApplyTagsToResource(p.state, reqCtx, id, tags, remove)
}

// ec2ApplyTagsThroughOwner applies tags to the EC2 resource id through the EC2 plugin registered in
// owners, under its tag lock, or on state directly when owners holds none.
//
// The direct path takes no lock, so it does not serialize with EC2 handlers: a security group
// record it rewrites can race an authorize, revoke or rule modify. It is reached only when no EC2
// plugin is registered, in which case no EC2 handler runs to race it.
func ec2ApplyTagsThroughOwner(
	owners *PluginRegistry, state StateManager, reqCtx *RequestContext, id string, tags []EC2Tag, remove bool,
) error {
	if owners != nil {
		if plugin, ok := owners.Plugin(ec2Namespace); ok {
			if ec2, ok := plugin.(*EC2Plugin); ok {
				return ec2.applyTagsByID(reqCtx, id, tags, remove)
			}
		}
	}
	return ec2ApplyTagsToResource(state, reqCtx, id, tags, remove)
}

// tagRecord implements [resourceTagger]. The quota is EC2's 50 tags per resource
// ([taggingEC2QuotaRefusal]), checked before anything is written.
func (p *EC2Plugin) tagRecord(
	ctx context.Context, key string, add map[string]string, remove []string, quota tagQuotaMode,
) error {
	// The key's last segment is the resource's ID for every type the tagging API reaches, so the
	// same locks as CreateTags' are taken (#1614).
	defer p.lockTagWrite([]string{ec2StateKeyTail(key)})()
	raw, err := loadTagRecord(ctx, p.state, ec2Namespace, key)
	if err != nil {
		return err
	}
	if quota == enforceTagQuota && len(add) > 0 {
		if refusal := taggingEC2QuotaRefusal(raw, add, remove); refusal != nil {
			return taggingQuotaRefusal(refusal)
		}
	}
	return ec2MergeTaggedRecord(ctx, p.state, key, raw, add, remove)
}

// taggingEC2QuotaRefusal applies [ec2CheckTagLimit] to a stored EC2 record.
//
// Every taggable EC2 record spells the member `tags` in lowercase and holds [EC2Tag], so one shape
// covers all fourteen of them.
func taggingEC2QuotaRefusal(raw []byte, addTags map[string]string, removeKeys []string) *AWSError {
	var record struct {
		Tags []EC2Tag `json:"tags"`
	}
	if !taggingDecodeForQuota(raw, &record) {
		return nil
	}
	return ec2CheckTagLimit(
		taggingDropRemovedTags(record.Tags, removeKeys, func(t EC2Tag) string { return t.Key }),
		taggingTagsAsEC2(addTags))
}
