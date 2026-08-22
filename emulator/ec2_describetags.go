package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// ec2MaxTagResults is the largest MaxResults DescribeTags accepts, and the page size a
// request naming none receives.
const ec2MaxTagResults = 1000

// ec2MinTagResults is the smallest MaxResults DescribeTags accepts.
//
// AWS documents this operation's range as "between 5 and 1000", where
// DescribeLaunchTemplateVersions' is 1 to 200 — the floor is per-operation, so it cannot be
// shared with [ec2MaxLaunchTemplateVersionResults]' check even though the two read alike.
const ec2MinTagResults = 5

// ec2StatePrefix is the key prefix every EC2 record of one namespace in one account and
// region shares.
//
// Every EC2 state key is "<namespace>:<account>/<region>/<id>", so one builder for the
// prefix half is what lets [ec2TagScanTargets] name a namespace once instead of
// concatenating it, for the reason [ec2VolumeStateKey]'s doc comment gives: a reader that
// spells a key differently from the writer sees no records at all. The scan is naturally
// regional because the scope is inside the prefix, which is also real EC2's scope for this
// operation.
func ec2StatePrefix(namespace, accountID, region string) string {
	return namespace + ":" + accountID + "/" + region + "/"
}

// ec2StateKeyTail returns the last path segment of an EC2 state key.
//
// For thirteen of the fifteen namespaces in [ec2TagScanTargets] that segment is the
// resource's own ID, which is both what a caller passes to CreateTags and what AWS's
// TagDescription reports. For the other two it is the resource's *name* — a placement group
// and a key pair are keyed by name — which is why [ec2TagScanTarget.idMember] exists and why
// this function is named for what it returns rather than for what it usually means.
func ec2StateKeyTail(key string) string {
	if i := strings.LastIndex(key, "/"); i >= 0 {
		return key[i+1:]
	}
	return key
}

// ec2TagScanTarget is one resource type DescribeTags reads: the namespace its records live
// under, and AWS's TagSpecification spelling of the type.
//
// The spelling matters twice over — it is the value the resource-type filter is documented
// against and the value the resourceType response member carries — which is why it is
// written here beside the namespace rather than derived from it: "sg" is "security-group"
// and "nat" is "natgateway", unhyphenated, and neither could be computed.
type ec2TagScanTarget struct {
	// namespace is the state-key namespace, the half before the colon.
	namespace string
	// resourceType is AWS's TagSpecification spelling of the resource type.
	resourceType string
	// idMember is the record's JSON member holding the resource's ID, for a namespace
	// keyed by name rather than by ID. Empty when [ec2StateKeyTail] is already the ID.
	//
	// AWS's TagDescription reports resourceId, so a name-keyed namespace has to read the
	// ID out of the record: reporting the tail would name a placement group by its group
	// name, which is not an ID a caller can pass back to CreateTags.
	idMember string
}

// ec2TagScanTargets is every resource type DescribeTags reads tags from.
//
// The list is no longer wider than [ec2TaggableResource]'s prefixes: #708 closed the gap,
// so every type here can also be tagged through CreateTags and every type CreateTags
// accepts is reported here. Keeping the two aligned is the point — until #708 an AMI,
// launch template, fleet, placement group or key pair could hold tags from its own create
// call's TagSpecification that CreateTags had no way to change, and a placement group or
// key pair held tags DescribeTags had no way to report.
//
// Order is alphabetical by type, which only sets the scan order — the response is sorted
// by [ec2SortTagDescriptions] regardless.
func ec2TagScanTargets() []ec2TagScanTarget {
	return []ec2TagScanTarget{
		{namespace: "eip", resourceType: "elastic-ip"},
		{namespace: "fleet", resourceType: "fleet"},
		{namespace: "image", resourceType: "image"},
		{namespace: "instance", resourceType: "instance"},
		{namespace: "igw", resourceType: "internet-gateway"},
		{namespace: "keypair", resourceType: "key-pair", idMember: "keyPairId"},
		{namespace: "lt", resourceType: "launch-template"},
		{namespace: "nat", resourceType: "natgateway"},
		{namespace: "placement_group", resourceType: "placement-group", idMember: "group_id"},
		{namespace: "rtb", resourceType: "route-table"},
		{namespace: "sg", resourceType: "security-group"},
		{namespace: "snapshot", resourceType: "snapshot"},
		{namespace: "subnet", resourceType: "subnet"},
		{namespace: "volume", resourceType: "volume"},
		{namespace: "vpc", resourceType: "vpc"},
	}
}

// ec2TagFilterSpec is DescribeTags' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeTags.html.
//
// Five names, all evaluated, and **no tag-key** — the one operation in EC2's describe family
// that documents none, which is worth stating rather than leaving to look like an omission
// here. Its `key` filter is that question: `key` matches a tag key whatever its value, which
// is what tag-key spells elsewhere.
func ec2TagFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated:      []string{"key", "resource-id", "resource-type", "value"},
	}
}

// ec2TagDescription renders one tag on one resource as AWS's TagDescription element.
//
// Its own type rather than [ec2TagItem]: a TagDescription carries the resource it is on,
// which a Tag does not, so the two are different shapes for different questions.
//
// No omitempty on value. AWS's Example 1 renders `<value/>` for a tag whose value is empty,
// and an SDK tells a present-but-empty element ("the tag's value is the empty string") from
// an absent one ("unknown") — the same reasoning CreateVolume's tagSet follows.
type ec2TagDescription struct {
	// ResourceID is the ID of the resource the tag is on.
	ResourceID string `xml:"resourceId"`

	// ResourceType is AWS's TagSpecification spelling of the resource's type.
	ResourceType string `xml:"resourceType"`

	// Key is the tag key.
	Key string `xml:"key"`

	// Value is the tag value, which may be empty.
	Value string `xml:"value"`
}

// describeTags handles the DescribeTags action, the operation whose whole job is finding
// resources by tag.
//
// Until #688 it reached the dispatcher's default arm and answered InvalidAction / HTTP 400,
// while four of [ListManagedPolicies]' bundles granted ec2:DescribeTags — AmazonVPCFullAccess
// and AmazonVPCReadOnlyAccess name it outright, AmazonEC2FullAccess and
// AmazonEC2ReadOnlyAccess reach it through ec2:* and ec2:Describe* — so a policy permitted an
// operation nothing served. Real EC2 offers three routes to a resource's tags: this
// operation, a tag:<key> filter on each describe, and Resource Groups Tagging. Substrate
// served the third in full and the second on five operations.
//
// The answer is assembled in four steps, in this order: refuse an undocumented filter name,
// validate MaxResults, scan and filter, then sort and page. Refusal comes first so that it
// never depends on how many resources happen to be tagged, which is the ordering
// [ec2FilterSpec.check] documents.
func (p *EC2Plugin) describeTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if awsErr := ec2TagFilterSpec().check(req.Params); awsErr != nil {
		return nil, awsErr
	}
	maxResults, awsErr := ec2TagMaxResults(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}

	items, err := p.scanTags(context.Background(), reqCtx, extractEC2Filters(req.Params))
	if err != nil {
		return nil, fmt.Errorf("ec2 describeTags: %w", err)
	}
	ec2SortTagDescriptions(items)

	page, nextToken, awsErr := ec2PageTagDescriptions(items, req.Params["NextToken"], maxResults)
	if awsErr != nil {
		return nil, awsErr
	}

	type response struct {
		XMLName   xml.Name            `xml:"DescribeTagsResponse"`
		XMLNS     string              `xml:"xmlns,attr"`
		Tags      []ec2TagDescription `xml:"tagSet>item"`
		NextToken string              `xml:"nextToken,omitempty"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:     "http://ec2.amazonaws.com/doc/2016-11-15/",
		Tags:      page,
		NextToken: nextToken,
	})
}

// scanTags reads every stored tag in the account and region, keeping the ones the filters
// accept.
//
// Each record is decoded into a member map, so a new resource type joins the scan by being
// listed in [ec2TagScanTargets] and needs no decoder of its own: "tags" is the
// type-agnostic shape [EC2Plugin.applyTagsToResource] writes and [ec2AuthzTagsFor] reads,
// and a name-keyed type's ID member is named by the target rather than by a Go type.
//
// A record that cannot be read or decoded is skipped rather than failing the request: a
// partial answer names the resources it could read, where an error names none of them, and
// a single unparseable record would otherwise make DescribeTags unusable. A failed List is
// different — that is the whole namespace, so it is returned as an error.
func (p *EC2Plugin) scanTags(ctx context.Context, reqCtx *RequestContext, filters map[string][]string) ([]ec2TagDescription, error) {
	var out []ec2TagDescription
	for _, target := range ec2TagScanTargets() {
		keys, err := p.state.List(ctx, ec2Namespace,
			ec2StatePrefix(target.namespace, reqCtx.AccountID, reqCtx.Region))
		if err != nil {
			return nil, fmt.Errorf("list %s: %w", target.namespace, err)
		}
		for _, key := range keys {
			data, getErr := p.state.Get(ctx, ec2Namespace, key)
			if getErr != nil || data == nil {
				continue
			}
			var record map[string]json.RawMessage
			if json.Unmarshal(data, &record) != nil {
				continue
			}
			var tags []EC2Tag
			if raw, ok := record["tags"]; ok && json.Unmarshal(raw, &tags) != nil {
				continue
			}
			resourceID := ec2StateKeyTail(key)
			if target.idMember != "" {
				// A record that carries no ID member is skipped rather than reported under
				// its name: a TagDescription naming a resource by something that is not its
				// resourceId is worse than an omission, because a caller cannot tell.
				if json.Unmarshal(record[target.idMember], &resourceID) != nil {
					continue
				}
			}
			for _, tag := range tags {
				item := ec2TagDescription{
					ResourceID:   resourceID,
					ResourceType: target.resourceType,
					Key:          tag.Key,
					Value:        tag.Value,
				}
				if ec2TagMatchesFilters(item, filters) {
					out = append(out, item)
				}
			}
		}
	}
	return out, nil
}

// ec2TagMatchesFilters reports whether one tag satisfies every supplied filter. Filters AND
// with each other and each one's values OR, AWS's documented rule.
func ec2TagMatchesFilters(item ec2TagDescription, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2TagMatchesFilter(item, name, values) {
			return false
		}
	}
	return true
}

// ec2TagMatchesFilter evaluates a single DescribeTags filter against one tag.
//
// Values are matched with [ec2FilterAccepts], so **wildcards work** — AWS's Example 4 says
// so for this operation in as many words: "You can use wildcards with filters, so you could
// specify the value as ?ebserver to find tags with the key webserver or Webserver." Since #697
// every EC2 describe matches its filter values that way, so this is no longer one of two
// operations that does; see [ec2FilterValueMatches] for the rules and for what AWS's own page
// contradicts itself about.
//
// An explicitly empty value is a value, not an absent one: AWS's Example 6 filters on
// `value` with `Filter.3.Value.1=` to find tags whose value is the empty string, and
// [extractEC2Filters] preserves that as a one-element list. A filter carrying *no* Value.N
// at all is a different request, and it now matches nothing, which is what every other EC2
// describe already answered — #696 converged the two permissive sites on the nine strict ones
// rather than the other way round. This operation still inherits the rule from
// [ec2FilterAccepts] rather than choosing it. Only names present in the filter map reach here,
// so an absent filter never arrives as an empty list.
//
// There is no default arm returning true. Every name that reaches here is one of the five
// AWS documents, because [ec2TagFilterSpec] refuses the rest before the scan and its
// evaluated list is the whole documented set — this operation is the only one in the family
// with nothing inert.
func ec2TagMatchesFilter(item ec2TagDescription, name string, values []string) bool {
	if tagKey, ok := strings.CutPrefix(name, "tag:"); ok {
		return item.Key == tagKey && ec2FilterAccepts(values, item.Value)
	}
	switch name {
	case "key":
		return ec2FilterAccepts(values, item.Key)
	case "resource-id":
		return ec2FilterAccepts(values, item.ResourceID)
	case "resource-type":
		return ec2FilterAccepts(values, item.ResourceType)
	case "value":
		return ec2FilterAccepts(values, item.Value)
	default:
		return false
	}
}

// ec2SortTagDescriptions orders the answer by resource ID, then type, then key.
//
// AWS says its own order "might vary" and that applications should not rely on it.
// Substrate is stricter deliberately: [StateManager.List] documents no ordering either, so
// an offset-based NextToken over an unordered list could skip or repeat a tag between
// pages, and two replays of one recorded request could answer differently. A deterministic
// emulator must not answer one request two ways.
//
// The key breaks the tie within a resource, and the type breaks it between two resources of
// different types sharing an ID — which substrate's generated IDs make impossible, but the
// comparison is total rather than nearly so.
func ec2SortTagDescriptions(items []ec2TagDescription) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].ResourceID != items[j].ResourceID {
			return items[i].ResourceID < items[j].ResourceID
		}
		if items[i].ResourceType != items[j].ResourceType {
			return items[i].ResourceType < items[j].ResourceType
		}
		return items[i].Key < items[j].Key
	})
}

// ec2TagMaxResults reads DescribeTags' MaxResults, defaulting to [ec2MaxTagResults].
//
// A value outside 5–1000 is refused rather than clamped, which is what
// DescribeLaunchTemplateVersions does with its own range: a caller who asked for 2000 items
// asked for something the operation cannot do, and silently giving 1000 hides that.
func ec2TagMaxResults(params map[string]string) (int, *AWSError) {
	raw := params["MaxResults"]
	if raw == "" {
		return ec2MaxTagResults, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < ec2MinTagResults || n > ec2MaxTagResults {
		return 0, &AWSError{
			Code: "InvalidParameterValue",
			Message: "MaxResults must be between " + strconv.Itoa(ec2MinTagResults) +
				" and " + strconv.Itoa(ec2MaxTagResults),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return n, nil
}

// ec2PageTagDescriptions returns the page of items starting at token, and the token for the
// page after it.
//
// The wire shape is DescribeLaunchTemplateVersions': a plain decimal offset, an
// InvalidParameterValue for a token that is not one, an out-of-range offset clamped to the
// end rather than refused — a caller resuming after a tag was deleted gets an empty last
// page, not an error — and an omitted nextToken on the last page, since AWS documents the
// member as null when there are no more items.
//
// The offset is only stable because [ec2SortTagDescriptions] ran first.
func ec2PageTagDescriptions(items []ec2TagDescription, token string, maxResults int) ([]ec2TagDescription, string, *AWSError) {
	start := 0
	if token != "" {
		n, err := strconv.Atoi(token)
		if err != nil || n < 0 {
			return nil, "", &AWSError{
				Code:       "InvalidParameterValue",
				Message:    "The token '" + token + "' is invalid",
				HTTPStatus: http.StatusBadRequest,
			}
		}
		start = n
	}
	if start > len(items) {
		start = len(items)
	}
	page := items[start:]
	if len(page) > maxResults {
		return page[:maxResults], strconv.Itoa(start + maxResults), nil
	}
	return page, "", nil
}
