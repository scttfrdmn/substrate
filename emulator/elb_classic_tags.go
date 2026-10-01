package emulator

import (
	"context"
	"fmt"
	"net/http"
)

// The classic (2012-06-01) tag trio: `AddTags`, `RemoveTags` and `DescribeTags`.
//
// These are #844's Tier 1b, and they are the second half of the same defect Tier 1a fixed. All three
// action names exist in **both** Elastic Load Balancing APIs, so before this they reached the ELBv2
// handlers unconditionally and a classic caller was refused for `ResourceArns is required` — a member
// name the 2012-06-01 API does not have, which is `Name is required` in a different costume.
//
// # What differs between the generations, which is the whole reason these are separate handlers
//
//   - **The resource is named, not ARN'd.** Classic takes `LoadBalancerNames.member.N`; ELBv2 takes
//     `ResourceArns.member.N`. So does the response: classic's `TagDescription` carries
//     `LoadBalancerName` where ELBv2's carries `ResourceArn`.
//   - **`AddTags` and `RemoveTags` take one load balancer.** "You can specify one load balancer only"
//     and "You can specify a maximum of one load balancer name" respectively. ELBv2's take a list
//     with no published bound. `DescribeTags` takes 1–20 names in both generations.
//   - **`RemoveTags` names the keys as `Tags.member.N` of [TagKeyOnly], not `TagKeys.member.N`.** A
//     caller sending ELBv2's spelling to the classic API removes nothing, which is why
//     elb_classic_tags_test.go asserts both spellings against each other rather than only the right
//     one.
//   - **The cap is 10, not 50**, and the `TooManyTags` wording is the classic page's. Both already
//     travel together as [elbClassicTagQuota] (#1148) and are resolved from the record's own state
//     key by [elbTagQuotaForStateKey], so one load balancer gets the same answer through its own API,
//     through its create and through the Resource Groups Tagging API.
//
// # AWS's `RemoveTags` page contradicts itself, and substrate follows the model
//
// `API_RemoveTags`' parameter table publishes `LoadBalancerNames.member.N`; its Sample Request shows
// singular `&LoadBalancerName=my-loadbalancer`. `AddTags` and `DescribeTags` publish *and* sample the
// indexed plural form, so the singular is a typo on one page rather than a second accepted spelling.
// The indexed form is what every SDK serializes, so it is the one read here — which is the same
// resolution [elbMaxTagKeyLength] records for the Tag-length contradiction. A caller who copies
// AWS's own sample curl is refused `ValidationError`, and docs/services.md says so.
//
// # What is shared rather than duplicated
//
// Every rule these handlers apply is a function elb_tags.go already holds: [extractELBTags],
// [elbCheckDuplicateTagKeys], [elbCheckTagRules], [elbCheckTagLimit], [elbMergeTags],
// [elbRemoveTagKeys], [ELBPlugin.elbWriteTags] and [elbTagItems]. Only the request decode, the
// resolution by name and the response shape are new. In particular the write goes through
// [elbTaggedResource.encode], which is what stamps `ever_tagged` from the pre-merge count (#938) —
// this is that method's first caller, which its own doc comment anticipated.

// elbClassicMaxTagOperationNames is how many load balancers one classic `AddTags` or `RemoveTags`
// may name.
//
// Published as prose rather than as an `Array Members` constraint — `AddTags` says "You can specify
// one load balancer only" and `RemoveTags` "You can specify a maximum of one load balancer name" —
// and neither page publishes a code for exceeding it. So the refusal is `ValidationError`/400 from
// the Query Common Errors page, which is what [elbDescribeTagsMaxResources]' refusal already answers
// for ELBv2's twenty.
const elbClassicMaxTagOperationNames = 1

// elbClassicNotFoundError is the refusal for a `LoadBalancerNames.member.N` entry that names nothing.
//
// The code comes from [elbNotFoundCodes] rather than being spelled here, so the classic API's own
// doors and the Resource Groups Tagging API's classic arm cannot come to answer different codes for
// one absent load balancer. All four of the operations that can reach it — `DescribeLoadBalancers`
// and the tag trio — publish `LoadBalancerNotFound` at HTTP 400.
//
// The message names the load balancer rather than an ARN, because the classic API addresses a load
// balancer by name and a caller who never supplied an ARN should not be shown one. The wording is
// AWS's observed text for this code; its published text is the one-line description of the code
// itself ("The specified load balancer does not exist."), which is not a response body.
func elbClassicNotFoundError(name string) *AWSError {
	return &AWSError{
		Code:       elbNotFoundCodes[elbKindClassicLB],
		Message:    fmt.Sprintf("There is no ACTIVE Load Balancer named '%s'", name),
		HTTPStatus: http.StatusBadRequest,
	}
}

// elbClassicTagNames reads `LoadBalancerNames.member.N` and checks it against the operation's
// published maximum.
//
// `Required: Yes` on all three operations, with a minimum of one item on `DescribeTags` and the
// one-load-balancer prose bound on the other two, so an absent list and an over-long one are both
// refusals rather than an empty answer.
//
// Both messages name `LoadBalancerNames` rather than ELBv2's `ResourceArns`, which is not cosmetic:
// a `ValidationError`/400 is what the v2 handler these requests used to reach already answered, so
// the member the message names is the only part of the refusal that tells a caller — or a test —
// which generation turned it away.
func elbClassicTagNames(req *AWSRequest, maxNames int) ([]string, *AWSError) {
	names := extractIndexedParams(req.Params, "LoadBalancerNames.member")
	if len(names) == 0 {
		return nil, elbValidationError("LoadBalancerNames is required")
	}
	if len(names) > maxNames {
		return nil, elbValidationError(
			"LoadBalancerNames names %d load balancers; this operation accepts at most %d",
			len(names), maxNames)
	}
	return names, nil
}

// elbClassicTagKeysOnly reads the `Tags.member.N` list of [TagKeyOnly] objects classic `RemoveTags`
// takes, returning the keys.
//
// The walk ends on an absent or empty `Key`, which is how the query protocol terminates an indexed
// list and what [extractELBTags] does for the `Tag` list beside it. `TagKeyOnly` has exactly one
// member, so there is nothing else to read — and that single member is why this cannot reuse
// [extractELBTags], whose walk yields pairs.
func elbClassicTagKeysOnly(params map[string]string) []string {
	var keys []string
	for i := 1; ; i++ {
		key := params[fmt.Sprintf("Tags.member.%d.Key", i)]
		if key == "" {
			break
		}
		keys = append(keys, key)
	}
	return keys
}

// elbClassicResolveTagged finds the classic record a `LoadBalancerNames.member.N` entry names, or
// the refusal for it.
//
// A direct [StateManager.Get] on [elbClassicStateKey] rather than the List-and-scan
// [elbResolveTaggedResource] runs, because the classic API addresses a load balancer by name and the
// key is built from the name — the same resolution [ELBPlugin.deleteClassicLoadBalancer] uses. The
// record is decoded through [elbClassicDecodeTaggedResource], so the tag write that follows goes
// through the same closure the tagging API's and CloudFormation's writes do.
//
// A read that genuinely fails is an error, not a NotFound: a broken backend is not an absent load
// balancer, and answering `LoadBalancerNotFound` for one would tell a consumer's retry loop the
// wrong thing. A record that is present but will not decode is reported as absent, which is what the
// generation-blind resolvers already do with one.
func (p *ELBPlugin) elbClassicResolveTagged(
	scope, name string,
) (*elbTaggedResource, *AWSError, error) {
	key := elbClassicStateKey(scope, name)
	data, err := p.state.Get(context.Background(), elbNamespace, key)
	if err != nil {
		return nil, nil, fmt.Errorf("elb classic resolve %s: %w", name, err)
	}
	if data == nil {
		return nil, elbClassicNotFoundError(name), nil
	}
	res := elbClassicDecodeTaggedResource(key, data)
	if res == nil {
		return nil, elbClassicNotFoundError(name), nil
	}
	return res, nil, nil
}

// elbClassicResolveTaggedAll resolves every name a classic tag call makes, refusing the whole
// request on the first that names nothing.
//
// Resolving all before writing any is [elbResolveAll]'s rule and it is here for the same reason: a
// request that is going to be refused must not have already changed half of what it named. It
// matters less for `AddTags` and `RemoveTags`, which take one name, and it is the whole of
// `DescribeTags`' behavior for twenty.
func (p *ELBPlugin) elbClassicResolveTaggedAll(
	scope string, names []string,
) ([]*elbTaggedResource, *AWSError, error) {
	out := make([]*elbTaggedResource, 0, len(names))
	for _, name := range names {
		res, awsErr, err := p.elbClassicResolveTagged(scope, name)
		if err != nil {
			return nil, nil, err
		}
		if awsErr != nil {
			return nil, awsErr, nil
		}
		out = append(out, res)
	}
	return out, nil, nil
}

// addClassicTags answers the 2012-06-01 `AddTags`.
//
// AWS: "Adds the specified tags to the specified load balancer. Each load balancer can have a
// maximum of 10 tags. Each tag consists of a key and an optional value. If a tag with the same key is
// already associated with the load balancer, AddTags updates its value."
//
// The three published errors are all answered and all at HTTP 400: `DuplicateTagKeys` before the
// record is resolved, `LoadBalancerNotFound` at the resolve, and `TooManyTags` before the write — so
// a request that cannot legally apply its tags leaves the stored set exactly as it was.
func (p *ELBPlugin) addClassicTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	names, awsErr := elbClassicTagNames(req, elbClassicMaxTagOperationNames)
	if awsErr != nil {
		return nil, awsErr
	}
	tags := extractELBTags(req.Params, "Tags.member")
	if len(tags) == 0 {
		return nil, elbValidationError("Tags is required")
	}
	if awsErr := elbCheckDuplicateTagKeys(tags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := elbCheckTagRules(tags); awsErr != nil {
		return nil, awsErr
	}

	scope := reqCtx.AccountID + "/" + reqCtx.Region
	resolved, awsErr, err := p.elbClassicResolveTaggedAll(scope, names)
	if err != nil {
		return nil, err
	}
	if awsErr != nil {
		return nil, awsErr
	}
	for _, res := range resolved {
		if limitErr := elbCheckTagLimit(res.tags, tags, elbTagQuotaForStateKey(res.stateKey)); limitErr != nil {
			return nil, limitErr
		}
	}
	for _, res := range resolved {
		if writeErr := p.elbWriteTags(res, elbMergeTags(res.tags, tags)); writeErr != nil {
			return nil, writeErr
		}
	}

	return elbClassicEmptyOKResponse(reqCtx, "AddTags")
}

// removeClassicTags answers the 2012-06-01 `RemoveTags`.
//
// The page publishes **one** error, `LoadBalancerNotFound`, so there is no `DuplicateTagKeys` and no
// cap on the key list here: naming the same key twice removes it once, which is the only other thing
// a removal can do, and it is the reading ELBv2's `RemoveTags` already records. Each key is still
// checked against `TagKeyOnly`'s own length and character constraints, which are the `Tag` key's and
// are the constraints an SDK validates before the request leaves the caller.
func (p *ELBPlugin) removeClassicTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	names, awsErr := elbClassicTagNames(req, elbClassicMaxTagOperationNames)
	if awsErr != nil {
		return nil, awsErr
	}
	keys := elbClassicTagKeysOnly(req.Params)
	if len(keys) == 0 {
		return nil, elbValidationError("Tags is required")
	}
	for _, k := range keys {
		if awsErr := elbCheckTagKey(k); awsErr != nil {
			return nil, awsErr
		}
	}

	scope := reqCtx.AccountID + "/" + reqCtx.Region
	resolved, awsErr, err := p.elbClassicResolveTaggedAll(scope, names)
	if err != nil {
		return nil, err
	}
	if awsErr != nil {
		return nil, awsErr
	}
	for _, res := range resolved {
		if writeErr := p.elbWriteTags(res, elbRemoveTagKeys(res.tags, keys)); writeErr != nil {
			return nil, writeErr
		}
	}

	return elbClassicEmptyOKResponse(reqCtx, "RemoveTags")
}

// describeClassicTags answers the 2012-06-01 `DescribeTags`.
//
// Up to [elbDescribeTagsMaxResources] names, which is the same twenty ELBv2 publishes, and a load
// balancer carrying no tags is still reported with an empty `Tags` list — omitting it would make "no
// tags" indistinguishable from "no such load balancer", which the operation has
// `LoadBalancerNotFound` to distinguish.
func (p *ELBPlugin) describeClassicTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	names, awsErr := elbClassicTagNames(req, elbDescribeTagsMaxResources)
	if awsErr != nil {
		return nil, awsErr
	}

	scope := reqCtx.AccountID + "/" + reqCtx.Region
	resolved, awsErr, err := p.elbClassicResolveTaggedAll(scope, names)
	if err != nil {
		return nil, err
	}
	if awsErr != nil {
		return nil, awsErr
	}

	type tagsResult struct {
		TagDescriptions []elbClassicTagDescriptionItem `xml:"TagDescriptions>member"`
	}
	var result tagsResult
	for i, res := range resolved {
		result.TagDescriptions = append(result.TagDescriptions, elbClassicTagDescriptionItem{
			Tags: elbTagItems(res.tags),
			// The name the request supplied rather than one read back off the record, so the
			// response pairs with the request element-for-element the way `DescribeTags` is read.
			// They are the same string: the record was resolved by this name.
			LoadBalancerName: names[i],
		})
	}
	return elbOKResponse(reqCtx, "DescribeTags", elbClassicXMLNS, result)
}

// elbClassicTagDescriptionItem is the XML representation of a classic `TagDescription`.
//
// It is not [elbTagDescriptionItem]: that one carries `ResourceArn`, which the 2012-06-01
// `TagDescription` does not publish, and this one carries `LoadBalancerName`, which ELBv2's does not.
// The member order is the published sample response's — `Tags` then `LoadBalancerName` — which costs
// nothing to match and is what a reader comparing the two sees.
type elbClassicTagDescriptionItem struct {
	Tags             []elbTagItem `xml:"Tags>member"`
	LoadBalancerName string       `xml:"LoadBalancerName"`
}
