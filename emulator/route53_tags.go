package emulator

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// Route 53's own tag operations: ChangeTagsForResource, ListTagsForResource and
// ListTagsForResources, for the two resource types substrate models, hostedzone and healthcheck
// (#1613).
//
// The tags live on the resource's own record, Route53HostedZone.Tags and Route53HealthCheck.Tags, so
// they go with the resource when it is deleted. The Resource Groups Tagging API reads no Route 53
// state: GetResources does not report hosted zones or health checks, with or without tags, so the
// two views never disagree.
//
// API_ChangeTagsForResource publishes NoSuchHostedZone and NoSuchHealthCheck (404) for a resource
// the ID names nothing of, and NoSuchHealthCheckSource (404), "No health check source exists with
// the specified ID.", for the third ResourceType value. Substrate models no health check sources, so
// every healthchecksource ID is answered with it. AddTags holds "the tags that you want to add ...
// and/or the tags that you want to edit Value for", so a key the resource holds takes the new value,
// and RemoveTagKeys removes keys, an absent key included. Request validation bounds both lists to 10
// entries and the ResourceType to the enum. The page also says "You can add a maximum of 10 tags to
// a health check, a health check source, or a hosted zone", so a change that would leave a resource
// holding more is InvalidInput (400); that message is substrate's own.

// r53MaxTagsPerResource is the per-resource limit API_ChangeTagsForResource publishes.
const r53MaxTagsPerResource = 10

// r53XMLTag is a Tag as Route 53 renders it.
type r53XMLTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// r53XMLResourceTagSet is a ResourceTagSet.
type r53XMLResourceTagSet struct {
	ResourceType string      `xml:"ResourceType"`
	ResourceID   string      `xml:"ResourceId"`
	Tags         []r53XMLTag `xml:"Tags>Tag"`
}

// r53TagSet renders a resource's tags in key order.
func r53TagSet(resourceType, resourceID string, tags map[string]string) r53XMLResourceTagSet {
	set := r53XMLResourceTagSet{ResourceType: resourceType, ResourceID: resourceID}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		set.Tags = append(set.Tags, r53XMLTag{Key: k, Value: tags[k]})
	}
	return set
}

// r53TagTarget is one taggable resource, loaded: its tags and how to write them back.
type r53TagTarget struct {
	tags map[string]string
	save func(map[string]string) error
}

// loadTagTarget loads the resource of resourceType that resourceID names in the caller's account,
// refusing an absent one with the type's own not-found error.
func (p *Route53Plugin) loadTagTarget(reqCtx *RequestContext, resourceType, resourceID string) (r53TagTarget, error) {
	switch resourceType {
	case "hostedzone":
		zone, err := p.loadHostedZone(resourceID)
		if err != nil {
			return r53TagTarget{}, err
		}
		return r53TagTarget{tags: zone.Tags, save: func(tags map[string]string) error {
			zone.Tags = tags
			return p.putZone(r53ZoneSuffix(resourceID), zone)
		}}, nil
	case "healthcheck":
		hc, err := p.loadHealthCheck(reqCtx, resourceID)
		if err != nil {
			return r53TagTarget{}, err
		}
		return r53TagTarget{tags: hc.Tags, save: func(tags map[string]string) error {
			hc.Tags = tags
			return p.putHealthCheck(hc)
		}}, nil
	case "healthchecksource":
		return r53TagTarget{}, &AWSError{Code: "NoSuchHealthCheckSource",
			Message: "No health check source exists with the specified ID.", HTTPStatus: http.StatusNotFound}
	}
	// The gate refuses a ResourceType outside the enum; this answers a dispatch it did not see.
	return r53TagTarget{}, &AWSError{Code: "InvalidInput",
		Message:    fmt.Sprintf("%q is not a valid resource type; the valid values are healthcheck, hostedzone and healthchecksource.", resourceType),
		HTTPStatus: http.StatusBadRequest}
}

// r53SplitTagPath splits the "{ResourceType}/{ResourceId}" [parseRoute53Operation] returns.
func r53SplitTagPath(path string) (resourceType, resourceID string) {
	resourceType, resourceID, _ = strings.Cut(path, "/")
	return resourceType, resourceID
}

// changeTagsForResource handles ChangeTagsForResource.
func (p *Route53Plugin) changeTagsForResource(reqCtx *RequestContext, req *AWSRequest, path string) (*AWSResponse, error) {
	resourceType, resourceID := r53SplitTagPath(path)
	var xmlReq struct {
		XMLName       xml.Name    `xml:"ChangeTagsForResourceRequest"`
		AddTags       []r53XMLTag `xml:"AddTags>Tag"`
		RemoveTagKeys []string    `xml:"RemoveTagKeys>Key"`
	}
	if len(bytes.TrimSpace(req.Body)) > 0 {
		if err := xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlReq); err != nil {
			return nil, &AWSError{Code: "MalformedXML", Message: "could not parse request body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	target, err := p.loadTagTarget(reqCtx, resourceType, resourceID)
	if err != nil {
		return nil, err
	}
	tags := make(map[string]string, len(target.tags)+len(xmlReq.AddTags))
	for k, v := range target.tags {
		tags[k] = v
	}
	for _, t := range xmlReq.AddTags {
		tags[t.Key] = t.Value
	}
	for _, k := range xmlReq.RemoveTagKeys {
		delete(tags, k)
	}
	if len(tags) > r53MaxTagsPerResource {
		return nil, &AWSError{Code: "InvalidInput",
			Message:    fmt.Sprintf("The %s %s would hold %d tags; a resource can hold at most %d.", resourceType, resourceID, len(tags), r53MaxTagsPerResource),
			HTTPStatus: http.StatusBadRequest}
	}
	if len(tags) == 0 {
		tags = nil
	}
	if err := target.save(tags); err != nil {
		return nil, fmt.Errorf("route53 changeTagsForResource: %w", err)
	}
	return r53XMLResponse(http.StatusOK, struct {
		XMLName xml.Name `xml:"ChangeTagsForResourceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}{XMLNS: r53XMLNS})
}

// listTagsForResource handles ListTagsForResource.
func (p *Route53Plugin) listTagsForResource(reqCtx *RequestContext, _ *AWSRequest, path string) (*AWSResponse, error) {
	resourceType, resourceID := r53SplitTagPath(path)
	target, err := p.loadTagTarget(reqCtx, resourceType, resourceID)
	if err != nil {
		return nil, err
	}
	return r53XMLResponse(http.StatusOK, struct {
		XMLName        xml.Name             `xml:"ListTagsForResourceResponse"`
		XMLNS          string               `xml:"xmlns,attr"`
		ResourceTagSet r53XMLResourceTagSet `xml:"ResourceTagSet"`
	}{XMLNS: r53XMLNS, ResourceTagSet: r53TagSet(resourceType, resourceID, target.tags)})
}

// listTagsForResources handles ListTagsForResources: one ResourceTagSet per ID, in request order.
// An ID that names nothing fails the whole request with its type's not-found error, as
// ListTagsForResource does.
func (p *Route53Plugin) listTagsForResources(reqCtx *RequestContext, req *AWSRequest, resourceType string) (*AWSResponse, error) {
	var xmlReq struct {
		XMLName     xml.Name `xml:"ListTagsForResourcesRequest"`
		ResourceIDs []string `xml:"ResourceIds>ResourceId"`
	}
	if err := xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlReq); err != nil {
		return nil, &AWSError{Code: "MalformedXML", Message: "could not parse request body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if len(xmlReq.ResourceIDs) == 0 {
		return nil, wireRequireMember("route53", "ListTagsForResources", req, "ResourceIds",
			&AWSError{Code: "InvalidInput", Message: "ResourceIds is required", HTTPStatus: http.StatusBadRequest})
	}
	sets := make([]r53XMLResourceTagSet, 0, len(xmlReq.ResourceIDs))
	for _, id := range xmlReq.ResourceIDs {
		target, err := p.loadTagTarget(reqCtx, resourceType, id)
		if err != nil {
			return nil, err
		}
		sets = append(sets, r53TagSet(resourceType, id, target.tags))
	}
	return r53XMLResponse(http.StatusOK, struct {
		XMLName         xml.Name               `xml:"ListTagsForResourcesResponse"`
		XMLNS           string                 `xml:"xmlns,attr"`
		ResourceTagSets []r53XMLResourceTagSet `xml:"ResourceTagSets>ResourceTagSet"`
	}{XMLNS: r53XMLNS, ResourceTagSets: sets})
}
