package emulator

import (
	"fmt"
	"sort"
	"strconv"
)

// Tags: CreateOrUpdateTags, DeleteTags and DescribeTags.
//
// A group's tags are on its record, so a tag change is a group write. Changing a tag does
// not retag instances the group already launched — the tagging guide's lifecycle applies a
// group's tags "during resource creation" only — and a PropagateAtLaunch tag reaches every
// instance launched after the change.

// asgTagsByGroup groups request tags by the group each names, refusing a tag that names no
// group or a resource type other than auto-scaling-group ("The only supported value is
// auto-scaling-group"). Groups are returned in first-mention order.
func (p *AutoScalingPlugin) asgTagsByGroup(reqCtx *RequestContext, tags []asgRequestTag) ([]*AutoScalingGroup, map[string][]AutoScalingTag, error) {
	var order []*AutoScalingGroup
	byGroup := map[string][]AutoScalingTag{}
	for _, t := range tags {
		if t.resourceID == "" {
			return nil, nil, asgMissingParameter("Tags.member.ResourceId")
		}
		if t.resourceType != "" && t.resourceType != "auto-scaling-group" {
			return nil, nil, asgValidationError("ResourceType %s is not supported. The only supported value is auto-scaling-group.", t.resourceType)
		}
		if _, seen := byGroup[t.resourceID]; !seen {
			g, err := p.loadGroup(reqCtx, t.resourceID)
			if err != nil {
				return nil, nil, err
			}
			if g == nil {
				return nil, nil, asgGroupNotFound(t.resourceID)
			}
			order = append(order, g)
		}
		byGroup[t.resourceID] = append(byGroup[t.resourceID], t.AutoScalingTag)
	}
	return order, byGroup, nil
}

func (p *AutoScalingPlugin) createOrUpdateTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	tags := asgParseTags(req.Params)
	if len(tags) == 0 {
		return nil, asgMissingParameter("Tags")
	}
	if awsErr := asgCheckTagKeys(tags); awsErr != nil {
		return nil, awsErr
	}
	groups, byGroup, err := p.asgTagsByGroup(reqCtx, tags)
	if err != nil {
		return nil, err
	}
	// Every group is checked before any is written, so a request over the limit for one group
	// changes none.
	merged := make([][]AutoScalingTag, len(groups))
	for i, g := range groups {
		merged[i] = asgMergeTags(g.Tags, byGroup[g.Name])
		if len(merged[i]) > asgMaxTags {
			// LimitExceeded, HTTP 400, is on this operation's Errors list; "The maximum number
			// of tags per resource is 50."
			return nil, &AWSError{Code: "LimitExceeded", Message: fmt.Sprintf("The number of tags on %s would exceed the limit of %d.", g.Name, asgMaxTags), HTTPStatus: 400}
		}
	}
	for i, g := range groups {
		g.Tags = merged[i]
		if err := p.saveGroup(reqCtx, g); err != nil {
			return nil, err
		}
	}
	return asgNoResultResponse(reqCtx, "CreateOrUpdateTags")
}

func (p *AutoScalingPlugin) deleteTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	tags := asgParseTags(req.Params)
	if len(tags) == 0 {
		return nil, asgMissingParameter("Tags")
	}
	for _, t := range tags {
		if t.Key == "" {
			return nil, asgMissingParameter("Tags.member.Key")
		}
	}
	groups, byGroup, err := p.asgTagsByGroup(reqCtx, tags)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		drop := map[string]bool{}
		for _, t := range byGroup[g.Name] {
			drop[t.Key] = true
		}
		kept := g.Tags[:0:0]
		for _, t := range g.Tags {
			if !drop[t.Key] {
				kept = append(kept, t)
			}
		}
		g.Tags = kept
		if err := p.saveGroup(reqCtx, g); err != nil {
			return nil, err
		}
	}
	return asgNoResultResponse(reqCtx, "DeleteTags")
}

func (p *AutoScalingPlugin) describeTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	maxRecords, cursor, awsErr := asgPageParams(req.Params, 50, 100)
	if awsErr != nil {
		return nil, awsErr
	}
	filters := asgParseFilters(req.Params)
	for _, f := range filters {
		switch f.name {
		case "auto-scaling-group", "key", "value", "propagate-at-launch":
		default:
			return nil, asgValidationError("Filter type %s is not correct. Allowed Filter types are: auto-scaling-group key value propagate-at-launch", f.name)
		}
	}
	groups, err := p.listGroups(reqCtx)
	if err != nil {
		return nil, err
	}
	var items []asgTagXML
	for _, g := range groups {
		for _, t := range asgTagsXML(g) {
			if asgTagMatches(t, filters) {
				items = append(items, t)
			}
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].ResourceID != items[j].ResourceID {
			return items[i].ResourceID < items[j].ResourceID
		}
		return items[i].Key < items[j].Key
	})
	page, next, awsErr := asgPage(items, func(t asgTagXML) string { return t.ResourceID + "\x00" + t.Key }, cursor, maxRecords)
	if awsErr != nil {
		return nil, awsErr
	}
	type result struct {
		Tags      asgList[asgTagXML] `xml:"Tags"`
		NextToken string             `xml:"NextToken,omitempty"`
	}
	return asgOKResponse(reqCtx, "DescribeTags", result{Tags: asgListOf(page), NextToken: next})
}

// asgTagMatches applies DescribeTags' filters: "A tag must match at least one of the
// specified values for it to be included", and "The result includes information for a
// particular tag only if it matches all the filters" (API_DescribeTags).
func asgTagMatches(t asgTagXML, filters []asgRequestFilter) bool {
	for _, f := range filters {
		var v string
		switch f.name {
		case "auto-scaling-group":
			v = t.ResourceID
		case "key":
			v = t.Key
		case "value":
			v = t.Value
		case "propagate-at-launch":
			v = strconv.FormatBool(t.PropagateAtLaunch)
		}
		if !containsStr(f.values, v) {
			return false
		}
	}
	return true
}
