package emulator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SNSPlugin emulates the Amazon Simple Notification Service (SNS) query-protocol API.
// It handles CreateTopic, DeleteTopic, GetTopicAttributes, SetTopicAttributes,
// ListTopics, Subscribe, Unsubscribe, ListSubscriptions, ListSubscriptionsByTopic,
// GetSubscriptionAttributes, SetSubscriptionAttributes, Publish, PublishBatch,
// AddPermission, RemovePermission, TagResource, UntagResource, and ListTagsForResource.
type SNSPlugin struct {
	state    StateManager
	logger   Logger
	tc       *TimeController
	registry *PluginRegistry
}

// Name returns the service name "sns".
func (p *SNSPlugin) Name() string { return "sns" }

// Initialize sets up the SNSPlugin with the provided configuration.
func (p *SNSPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	p.registry, _ = cfg.Options["registry"].(*PluginRegistry)
	return nil
}

// Shutdown is a no-op for SNSPlugin.
func (p *SNSPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an SNS query-protocol request to the appropriate handler.
func (p *SNSPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateTopic":
		return p.createTopic(ctx, req)
	case "DeleteTopic":
		return p.deleteTopic(ctx, req)
	case "GetTopicAttributes":
		return p.getTopicAttributes(ctx, req)
	case "SetTopicAttributes":
		return p.setTopicAttributes(ctx, req)
	case "ListTopics":
		return p.listTopics(ctx, req)
	case "Subscribe":
		return p.subscribe(ctx, req)
	case "Unsubscribe":
		return p.unsubscribe(ctx, req)
	case "ListSubscriptions":
		return p.listSubscriptions(ctx, req)
	case "ListSubscriptionsByTopic":
		return p.listSubscriptionsByTopic(ctx, req)
	case "GetSubscriptionAttributes":
		return p.getSubscriptionAttributes(ctx, req)
	case "SetSubscriptionAttributes":
		return p.setSubscriptionAttributes(ctx, req)
	case "Publish":
		return p.publish(ctx, req)
	case "PublishBatch":
		return p.publishBatch(ctx, req)
	case "AddPermission":
		return p.addPermission(ctx, req)
	case "RemovePermission":
		return p.removePermission(ctx, req)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	default:
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// --- State helpers ---

// The five SNS state-key builders. Free functions rather than methods on the plugin, so the Resource
// Groups Tagging API can build the same key from an ARN alone: a resolver that re-derived the layout
// would be a second producer of it, which is the drift #826 found for SQS and #918 for CloudFront.
// The prefixes live in sns_tags.go beside the taggability guard that tests them, so the guard cannot
// fall out of step with the keys actually written.
//
// Every key is account- and Region-qualified, and after #925 the account and Region a topic key
// carries come from the ARN rather than from the caller. That is what makes isolation emergent: an
// ARN naming another account builds a key nothing is stored at.

func snsTopicStateKey(accountID, region, name string) string {
	return snsTopicKeyPrefix + accountID + "/" + region + "/" + name
}

func snsTopicNamesStateKey(accountID, region string) string {
	return snsTopicNamesKeyPrefix + accountID + "/" + region
}

func snsSubStateKey(accountID, region, subID string) string {
	return snsSubKeyPrefix + accountID + "/" + region + "/" + subID
}

func snsSubAllIDsStateKey(accountID, region string) string {
	return snsSubAllIDsKeyPrefix + accountID + "/" + region
}

func snsSubTopicIDsStateKey(accountID, region, topicName string) string {
	return snsSubTopicIDsKeyPrefix + accountID + "/" + region + "/" + topicName
}

func (p *SNSPlugin) loadTopic(ctx context.Context, accountID, region, name string) (*SNSTopic, error) {
	data, err := p.state.Get(ctx, snsNamespace, snsTopicStateKey(accountID, region, name))
	if err != nil {
		return nil, fmt.Errorf("sns loadTopic state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var t SNSTopic
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("sns loadTopic unmarshal: %w", err)
	}
	return &t, nil
}

// requireTopic parses a topic ARN and loads the topic it names, refusing a well-formed ARN that names
// no topic.
//
// One helper for six operations, on the #961/#969 precedent: those issues each found one operation
// refusing nothing that its siblings refused, which is what a per-handler existence check invites.
// Before #926, Subscribe, Publish, PublishBatch and ListSubscriptionsByTopic read only the
// subscription index, so an absent topic was indistinguishable from a real topic with no subscribers
// and all four answered 200 — GetTopicAttributes and SetTopicAttributes were the two that checked, and
// they are routed through here so the six cannot drift apart again.
//
// The load is keyed by the ARN's own account and Region, never the caller's, which is the rule #925
// made structural: [snsParseTopicARN] takes no *RequestContext, so a cross-account or cross-Region ARN
// builds a state key nothing is stored at and the topic reads as absent. That is also why Subscribe
// and Publish now refuse a topic in another Region rather than serving a same-named local one, which
// API_Publish independently requires: "You can publish messages only to topics and endpoints in the
// same AWS Region."
//
// DeleteTopic is deliberately not a caller (#992): API_DeleteTopic's description states that "this
// action is idempotent, so deleting a topic that does not exist does not result in an error", so it
// keeps its own load and treats an absent topic as a no-op.
//
// The returned error is an [AWSError] for a malformed ARN or an absent topic, and a wrapped state
// error otherwise; every caller returns it unexamined.
func (p *SNSPlugin) requireTopic(ctx context.Context, arn string) (*SNSTopic, snsTopicTarget, error) {
	target, arnErr := snsParseTopicARN(arn)
	if arnErr != nil {
		return nil, snsTopicTarget{}, arnErr
	}
	t, err := p.loadTopic(ctx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, target, err
	}
	if t == nil {
		return nil, target, snsNoSuchTopic(arn)
	}
	return t, target, nil
}

func (p *SNSPlugin) saveTopic(ctx context.Context, t *SNSTopic) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("sns saveTopic marshal: %w", err)
	}
	return p.state.Put(ctx, snsNamespace, snsTopicStateKey(t.AccountID, t.Region, t.Name), data)
}

func (p *SNSPlugin) loadTopicNames(ctx context.Context, accountID, region string) ([]string, error) {
	data, err := p.state.Get(ctx, snsNamespace, snsTopicNamesStateKey(accountID, region))
	if err != nil {
		return nil, fmt.Errorf("sns loadTopicNames: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("sns loadTopicNames unmarshal: %w", err)
	}
	return names, nil
}

func (p *SNSPlugin) saveTopicNames(ctx context.Context, accountID, region string, names []string) error {
	sort.Strings(names)
	data, err := json.Marshal(names)
	if err != nil {
		return fmt.Errorf("sns saveTopicNames marshal: %w", err)
	}
	return p.state.Put(ctx, snsNamespace, snsTopicNamesStateKey(accountID, region), data)
}

func (p *SNSPlugin) loadSub(ctx context.Context, accountID, region, subID string) (*SNSSubscription, error) {
	data, err := p.state.Get(ctx, snsNamespace, snsSubStateKey(accountID, region, subID))
	if err != nil {
		return nil, fmt.Errorf("sns loadSub state.Get: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var s SNSSubscription
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("sns loadSub unmarshal: %w", err)
	}
	return &s, nil
}

func (p *SNSPlugin) saveSub(ctx context.Context, s *SNSSubscription) error {
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("sns saveSub marshal: %w", err)
	}
	return p.state.Put(ctx, snsNamespace, snsSubStateKey(s.AccountID, s.Region, s.ARN), data)
}

func (p *SNSPlugin) loadSubIDs(ctx context.Context, key string) ([]string, error) {
	data, err := p.state.Get(ctx, snsNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("sns loadSubIDs: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, fmt.Errorf("sns loadSubIDs unmarshal: %w", err)
	}
	return ids, nil
}

func (p *SNSPlugin) saveSubIDs(ctx context.Context, key string, ids []string) error {
	data, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("sns saveSubIDs marshal: %w", err)
	}
	return p.state.Put(ctx, snsNamespace, key, data)
}

// --- Operations ---

func (p *SNSPlugin) createTopic(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["Name"]
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameter", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	existing, err := p.loadTopic(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		// Idempotent — return existing ARN.
		type result struct {
			TopicArn string `xml:"TopicArn"`
		}
		type response struct {
			XMLName           xml.Name         `xml:"CreateTopicResponse"`
			Xmlns             string           `xml:"xmlns,attr"`
			CreateTopicResult result           `xml:"CreateTopicResult"`
			ResponseMetadata  responseMetadata `xml:"ResponseMetadata"`
		}
		return snsXMLResponse(http.StatusOK, response{
			Xmlns:             snsXMLNS,
			CreateTopicResult: result{TopicArn: existing.ARN},
			ResponseMetadata:  responseMetadata{RequestID: ctx.RequestID},
		})
	}

	arn := snsTopicARN(ctx.Region, ctx.AccountID, name)
	topic := &SNSTopic{
		ARN:       arn,
		Name:      name,
		AccountID: ctx.AccountID,
		Region:    ctx.Region,
	}
	// CreateTopic accepts initial attributes in real AWS; persist DisplayName so
	// it round-trips and is drift-checkable.
	if dn := req.Params["DisplayName"]; dn != "" {
		topic.Attributes = map[string]string{"DisplayName": dn}
	}
	// CreateTopic publishes a Tags parameter and substrate decoded none of it, so a topic created
	// with tags in one call reported none through ListTagsForResource or GetResources (#925). Ordered
	// by key for the same reason TagResource orders them.
	if tags := snsTagParams(req.Params); len(tags) > 0 {
		sortTagsByKey(tags, func(tag SNSTag) string { return tag.Key })
		topic.Tags = tags
	}
	if err := p.saveTopic(goCtx, topic); err != nil {
		return nil, fmt.Errorf("sns createTopic saveTopic: %w", err)
	}

	names, err := p.loadTopicNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	names = append(names, name)
	if err := p.saveTopicNames(goCtx, ctx.AccountID, ctx.Region, names); err != nil {
		return nil, fmt.Errorf("sns createTopic saveTopicNames: %w", err)
	}

	type result struct {
		TopicArn string `xml:"TopicArn"`
	}
	type response struct {
		XMLName           xml.Name         `xml:"CreateTopicResponse"`
		Xmlns             string           `xml:"xmlns,attr"`
		CreateTopicResult result           `xml:"CreateTopicResult"`
		ResponseMetadata  responseMetadata `xml:"ResponseMetadata"`
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:             snsXMLNS,
		CreateTopicResult: result{TopicArn: arn},
		ResponseMetadata:  responseMetadata{RequestID: ctx.RequestID},
	})
}

// deleteTopic removes a topic and its index entry, and succeeds when there is nothing to remove.
//
// The absent topic answers 200 rather than NotFound, because API_DeleteTopic's description says so in
// as many words (#992): "This action is idempotent, so deleting a topic that does not exist does not
// result in an error." The same page's Errors list *does* publish NotFound/404, alongside
// ConcurrentAccess, InvalidState, StaleTag and TagPolicy — codes plainly about the tag and
// event-source paths. Where a page contradicts itself the more specific statement governs: the
// sentence names this condition and this outcome, and the error-list entry names no condition at all.
// API_Unsubscribe corroborates by contrast — it publishes the same NotFound and carries no idempotence
// sentence, so AWS states the property where it holds rather than leaving it to be inferred.
//
// A malformed ARN is still InvalidParameter/400. The sentence licenses a topic that does not exist,
// not a string that is not an ARN.
func (p *SNSPlugin) deleteTopic(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	target, arnErr := snsParseTopicARN(req.Params["TopicArn"])
	if arnErr != nil {
		return nil, arnErr
	}

	goCtx := context.Background()
	t, err := p.loadTopic(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}

	// The record and its index entry are removed from the account and Region the ARN names, which
	// after #925 is where the load found it. Keying the delete by the caller instead removed an entry
	// from their own index while leaving the record the load had just read in place. Both are skipped
	// when the load found nothing, so the idempotent path writes no state at all rather than rewriting
	// the owning Region's index to itself.
	if t != nil {
		if delErr := p.state.Delete(goCtx, snsNamespace, snsTopicStateKey(target.AccountID, target.Region, target.Name)); delErr != nil {
			return nil, fmt.Errorf("sns deleteTopic state.Delete: %w", delErr)
		}

		names, loadErr := p.loadTopicNames(goCtx, target.AccountID, target.Region)
		if loadErr != nil {
			return nil, loadErr
		}
		newNames := make([]string, 0, len(names))
		for _, n := range names {
			if n != target.Name {
				newNames = append(newNames, n)
			}
		}
		if saveErr := p.saveTopicNames(goCtx, target.AccountID, target.Region, newNames); saveErr != nil {
			return nil, fmt.Errorf("sns deleteTopic saveTopicNames: %w", saveErr)
		}
	}

	return snsUnitResponse("DeleteTopic", ctx.RequestID)
}

func (p *SNSPlugin) getTopicAttributes(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	t, target, err := p.requireTopic(goCtx, req.Params["TopicArn"])
	if err != nil {
		return nil, err
	}

	// The stored attributes go in first and the derived ones over the top, which is the reverse of the
	// order this handler used before #993. setTopicAttributes used to write any AttributeName a caller
	// sent into this map unchecked, so merging it last let a stored TopicArn shadow the real one — and
	// would have let a stored SubscriptionsConfirmed shadow the derived count, making the emulator report
	// a subscription count of the caller's choosing. A derived member is a fact about the topic, so it
	// wins. Since #1067 setTopicAttributes refuses all four derived names, so this ordering is defense in
	// depth rather than the only guard — but it is still the only guard for a value that reached the map
	// another way, which is why it stays and why its test now seeds state directly.
	attrs := make(map[string]string, len(t.Attributes)+len(snsDerivedTopicAttributeNames))
	for k, v := range t.Attributes {
		attrs[k] = v
	}
	derived, err := p.derivedTopicAttributes(goCtx, ctx, t, target.Name)
	if err != nil {
		return nil, err
	}
	for k, v := range derived {
		attrs[k] = v
	}

	type attrEntry struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}
	type result struct {
		Attributes []attrEntry `xml:"Attributes>entry"`
	}
	type response struct {
		XMLName                  xml.Name         `xml:"GetTopicAttributesResponse"`
		Xmlns                    string           `xml:"xmlns,attr"`
		GetTopicAttributesResult result           `xml:"GetTopicAttributesResult"`
		ResponseMetadata         responseMetadata `xml:"ResponseMetadata"`
	}
	entries := make([]attrEntry, 0, len(attrs))
	for k, v := range attrs {
		entries = append(entries, attrEntry{Key: k, Value: v})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:                    snsXMLNS,
		GetTopicAttributesResult: result{Attributes: entries},
		ResponseMetadata:         responseMetadata{RequestID: ctx.RequestID},
	})
}

// setTopicAttributes stores one of the twenty-five attribute names API_SetTopicAttributes publishes.
//
// Until #1067 it stored whatever name arrived, so `AttributeName=Banana` was written into the topic
// record and GetTopicAttributes reported it back as though SNS carried it. The allowlist is
// [snsSettableTopicAttributeNames] and the argument for an allowlist over a denylist of the derived
// names is written there; the short version is that the Set and Get pages publish 25 and 17 names with
// only 9 in common, so no subtraction of one from the other describes what a caller may set.
//
// Both refusals are InvalidParameter/400, which the page publishes with the gloss "Indicates that a
// request parameter does not comply with the associated constraints" — AttributeName's constraint is its
// published value list, and it is marked Required: Yes, so both cases are that code. The page's only
// prose about InvalidParameter is about a MaximumMessageSize above 256 KiB on a topic that cannot carry
// it, so reading the code onto an unpublished name is substrate's reading of the gloss rather than a
// sentence AWS wrote; the two *messages* are substrate's own, because the page publishes none.
//
// The name is checked before the topic is resolved. The page publishes both InvalidParameter/400 and
// NotFound/404 without ordering them, so this is substrate's choice, made so that a refusal of an
// unpublished name does not depend on whether the topic happens to exist. AttributeValue is
// Required: No and is deliberately not checked for emptiness: CloudFormation's own AWS::SNS::TopicPolicy
// deleter clears a policy by setting it to the empty string (cfn_delete.go:87).
func (p *SNSPlugin) setTopicAttributes(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	attrName := req.Params["AttributeName"]
	attrValue := req.Params["AttributeValue"]

	if attrName == "" {
		return nil, &AWSError{
			Code:       "InvalidParameter",
			Message:    "AttributeName is required",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if !snsTopicAttributeIsSettable(attrName) {
		return nil, &AWSError{
			Code:       "InvalidParameter",
			Message:    "AttributeName " + attrName + " is not a settable topic attribute",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	goCtx := context.Background()
	t, _, err := p.requireTopic(goCtx, req.Params["TopicArn"])
	if err != nil {
		return nil, err
	}
	if t.Attributes == nil {
		t.Attributes = make(map[string]string)
	}
	t.Attributes[attrName] = attrValue
	if err := p.saveTopic(goCtx, t); err != nil {
		return nil, fmt.Errorf("sns setTopicAttributes saveTopic: %w", err)
	}

	return snsUnitResponse("SetTopicAttributes", ctx.RequestID)
}

func (p *SNSPlugin) listTopics(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := p.loadTopicNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)

	// Pagination.
	nextTokenParam := req.Params["NextToken"]
	offset := 0
	pageSize := 100
	if nextTokenParam != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(nextTokenParam); decErr == nil {
			if n, parseErr := strconv.Atoi(string(decoded)); parseErr == nil && n >= 0 {
				offset = n
			}
		}
	}
	if offset > len(names) {
		offset = len(names)
	}
	page := names[offset:]
	var nextToken string
	if len(page) > pageSize {
		page = page[:pageSize]
		nextOffset := offset + pageSize
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(nextOffset)))
	}

	type topicEntry struct {
		TopicArn string `xml:"TopicArn"`
	}
	type result struct {
		Topics    []topicEntry `xml:"Topics>member"`
		NextToken string       `xml:"NextToken,omitempty"`
	}
	type response struct {
		XMLName          xml.Name         `xml:"ListTopicsResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ListTopicsResult result           `xml:"ListTopicsResult"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}

	entries := make([]topicEntry, len(page))
	for i, name := range page {
		entries[i] = topicEntry{TopicArn: snsTopicARN(ctx.Region, ctx.AccountID, name)}
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns: snsXMLNS,
		ListTopicsResult: result{
			Topics:    entries,
			NextToken: nextToken,
		},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SNSPlugin) subscribe(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	topicARN := req.Params["TopicArn"]
	protocol := req.Params["Protocol"]
	endpoint := req.Params["Endpoint"]

	if topicARN == "" || protocol == "" {
		return nil, &AWSError{Code: "InvalidParameter", Message: "TopicArn and Protocol are required", HTTPStatus: http.StatusBadRequest}
	}

	// The topic has to exist before anything is written. Subscribe is the one of the four #926
	// operations that *writes*, so a missing check left a subscription record and two index entries
	// under a topic that was never created — reported by ListSubscriptions ever after, and delivered to
	// if a topic of that name was later created.
	goCtx := context.Background()
	_, target, err := p.requireTopic(goCtx, topicARN)
	if err != nil {
		return nil, err
	}
	topicName := target.Name
	subID := generateSNSSubID()
	// The subscription ARN is minted under the account and Region of the topic, because a
	// subscription ARN is the topic's ARN with the subscription's identifier appended — its account
	// segment is the topic's, not the subscriber's. The record and the two indexes below stay keyed by
	// the caller, per the note in sns_tags.go: a cross-account subscription is not modeled, and
	// moving them would leave Publish reading an index the subscriptions are not in.
	subARN := snsSubscriptionARN(target.Region, target.AccountID, topicName, subID)

	sub := &SNSSubscription{
		ARN:       subARN,
		TopicARN:  topicARN,
		Protocol:  protocol,
		Endpoint:  endpoint,
		AccountID: ctx.AccountID,
		Region:    ctx.Region,
	}

	if err := p.saveSub(goCtx, sub); err != nil {
		return nil, fmt.Errorf("sns subscribe saveSub: %w", err)
	}

	// Add to per-topic and global lists.
	allIDs, err := p.loadSubIDs(goCtx, snsSubAllIDsStateKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, err
	}
	allIDs = append(allIDs, subARN)
	if err := p.saveSubIDs(goCtx, snsSubAllIDsStateKey(ctx.AccountID, ctx.Region), allIDs); err != nil {
		return nil, fmt.Errorf("sns subscribe saveSubIDs all: %w", err)
	}

	topicIDs, err := p.loadSubIDs(goCtx, snsSubTopicIDsStateKey(ctx.AccountID, ctx.Region, topicName))
	if err != nil {
		return nil, err
	}
	topicIDs = append(topicIDs, subARN)
	if err := p.saveSubIDs(goCtx, snsSubTopicIDsStateKey(ctx.AccountID, ctx.Region, topicName), topicIDs); err != nil {
		return nil, fmt.Errorf("sns subscribe saveSubIDs topic: %w", err)
	}

	type result struct {
		SubscriptionArn string `xml:"SubscriptionArn"`
	}
	type response struct {
		XMLName          xml.Name         `xml:"SubscribeResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		SubscribeResult  result           `xml:"SubscribeResult"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:            snsXMLNS,
		SubscribeResult:  result{SubscriptionArn: subARN},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

// unsubscribe deletes a subscription, refusing an ARN that names none.
//
// The absent subscription answers NotFound/404 rather than succeeding silently, which is the half of
// #926 that is about a subscription rather than a topic. It had been an unsourced "idempotent" comment,
// and API_Unsubscribe does not support it: the page publishes NotFound/404 ("Indicates that the
// requested resource does not exist") and states no idempotence.
//
// The condition is not left to inference either. SubscriptionArn is the operation's only request
// parameter and the only resource it names — "The ARN of the subscription to be deleted" — so the
// resource a published NotFound can be about is the subscription and nothing else. The contrast with
// API_DeleteTopic (#992) is what makes the absence of an idempotence sentence here meaningful rather
// than an omission: AWS writes one where it means one.
func (p *SNSPlugin) unsubscribe(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	subARN := req.Params["SubscriptionArn"]

	goCtx := context.Background()
	sub, err := p.loadSub(goCtx, ctx.AccountID, ctx.Region, subARN)
	if err != nil {
		return nil, err
	}
	if sub == nil {
		return nil, &AWSError{
			Code:       "NotFound",
			Message:    fmt.Sprintf("no subscription found for the ARN %q", subARN),
			HTTPStatus: http.StatusNotFound,
		}
	}

	if err := p.state.Delete(goCtx, snsNamespace, snsSubStateKey(ctx.AccountID, ctx.Region, subARN)); err != nil {
		return nil, fmt.Errorf("sns unsubscribe state.Delete: %w", err)
	}

	// Remove from global list.
	allIDs, err := p.loadSubIDs(goCtx, snsSubAllIDsStateKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, err
	}
	newAll := make([]string, 0, len(allIDs))
	for _, id := range allIDs {
		if id != subARN {
			newAll = append(newAll, id)
		}
	}
	_ = p.saveSubIDs(goCtx, snsSubAllIDsStateKey(ctx.AccountID, ctx.Region), newAll)

	// Remove from per-topic list. The topic name comes from the stored subscription's own TopicArn,
	// which substrate minted, so the parse cannot fail on a caller's input — but it is parsed rather
	// than string-scanned so there is one producer of a topic name in this file (#925).
	topicTarget, arnErr := snsParseTopicARN(sub.TopicARN)
	if arnErr != nil {
		return nil, arnErr
	}
	topicName := topicTarget.Name
	topicIDs, err := p.loadSubIDs(goCtx, snsSubTopicIDsStateKey(ctx.AccountID, ctx.Region, topicName))
	if err != nil {
		return nil, err
	}
	newTopic := make([]string, 0, len(topicIDs))
	for _, id := range topicIDs {
		if id != subARN {
			newTopic = append(newTopic, id)
		}
	}
	_ = p.saveSubIDs(goCtx, snsSubTopicIDsStateKey(ctx.AccountID, ctx.Region, topicName), newTopic)

	return snsUnitResponse("Unsubscribe", ctx.RequestID)
}

func (p *SNSPlugin) listSubscriptions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	allIDs, err := p.loadSubIDs(goCtx, snsSubAllIDsStateKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, err
	}
	return p.buildSubscriptionListResponse(ctx, req, allIDs, "ListSubscriptions", "ListSubscriptionsResult")
}

func (p *SNSPlugin) listSubscriptionsByTopic(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// An empty list and an absent topic were the same answer here, and API_ListSubscriptionsByTopic
	// publishes NotFound/404 to distinguish them (#926).
	goCtx := context.Background()
	_, target, err := p.requireTopic(goCtx, req.Params["TopicArn"])
	if err != nil {
		return nil, err
	}

	topicIDs, err := p.loadSubIDs(goCtx, snsSubTopicIDsStateKey(ctx.AccountID, ctx.Region, target.Name))
	if err != nil {
		return nil, err
	}
	return p.buildSubscriptionListResponse(ctx, req, topicIDs, "ListSubscriptionsByTopic", "ListSubscriptionsByTopicResult")
}

func (p *SNSPlugin) buildSubscriptionListResponse(ctx *RequestContext, req *AWSRequest, subARNs []string, rootElem, resultElem string) (*AWSResponse, error) {
	// Pagination.
	nextTokenParam := req.Params["NextToken"]
	offset := 0
	pageSize := 100
	if nextTokenParam != "" {
		if decoded, decErr := base64.StdEncoding.DecodeString(nextTokenParam); decErr == nil {
			if n, parseErr := strconv.Atoi(string(decoded)); parseErr == nil && n >= 0 {
				offset = n
			}
		}
	}
	if offset > len(subARNs) {
		offset = len(subARNs)
	}
	page := subARNs[offset:]
	var nextToken string
	if len(page) > pageSize {
		page = page[:pageSize]
		nextOffset := offset + pageSize
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(nextOffset)))
	}

	type subEntry struct {
		SubscriptionArn string `xml:"SubscriptionArn"`
		TopicArn        string `xml:"TopicArn"`
		Protocol        string `xml:"Protocol"`
		Endpoint        string `xml:"Endpoint"`
		Owner           string `xml:"Owner"`
	}

	goCtx := context.Background()
	entries := make([]subEntry, 0, len(page))
	for _, subARN := range page {
		sub, loadErr := p.loadSub(goCtx, ctx.AccountID, ctx.Region, subARN)
		if loadErr != nil || sub == nil {
			continue
		}
		entries = append(entries, subEntry{
			SubscriptionArn: sub.ARN,
			TopicArn:        sub.TopicARN,
			Protocol:        sub.Protocol,
			Endpoint:        sub.Endpoint,
			Owner:           sub.AccountID,
		})
	}

	// Build response using generic XML wrapper.
	type result struct {
		Subscriptions []subEntry `xml:"Subscriptions>member"`
		NextToken     string     `xml:"NextToken,omitempty"`
	}
	res := result{Subscriptions: entries, NextToken: nextToken}
	resBody, err := xml.Marshal(res)
	if err != nil {
		return nil, fmt.Errorf("sns listSubscriptions marshal result: %w", err)
	}
	// Wrap in root element with xmlns.
	fullXML := xml.Header + `<` + rootElem + ` xmlns="` + snsXMLNS + `">` +
		`<` + resultElem + `>` + string(resBody[len("<result>"):len(resBody)-len("</result>")]) + `</` + resultElem + `>` +
		`<ResponseMetadata><RequestId>` + ctx.RequestID + `</RequestId></ResponseMetadata>` +
		`</` + rootElem + `>`
	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       []byte(fullXML),
	}, nil
}

func (p *SNSPlugin) getSubscriptionAttributes(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	subARN := req.Params["SubscriptionArn"]
	sub, err := p.loadSub(context.Background(), ctx.AccountID, ctx.Region, subARN)
	if err != nil {
		return nil, err
	}
	if sub == nil {
		return nil, &AWSError{Code: "NotFound", Message: "Subscription not found", HTTPStatus: http.StatusNotFound}
	}

	type attrEntry struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}
	type result struct {
		Attributes []attrEntry `xml:"Attributes>entry"`
	}
	type response struct {
		XMLName                         xml.Name         `xml:"GetSubscriptionAttributesResponse"`
		Xmlns                           string           `xml:"xmlns,attr"`
		GetSubscriptionAttributesResult result           `xml:"GetSubscriptionAttributesResult"`
		ResponseMetadata                responseMetadata `xml:"ResponseMetadata"`
	}
	attrs := []attrEntry{
		{Key: "SubscriptionArn", Value: sub.ARN},
		{Key: "TopicArn", Value: sub.TopicARN},
		{Key: "Protocol", Value: sub.Protocol},
		{Key: "Endpoint", Value: sub.Endpoint},
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:                           snsXMLNS,
		GetSubscriptionAttributesResult: result{Attributes: attrs},
		ResponseMetadata:                responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SNSPlugin) setSubscriptionAttributes(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Stub: accept any attribute set and succeed.
	_ = req.Params["SubscriptionArn"]
	return snsUnitResponse("SetSubscriptionAttributes", ctx.RequestID)
}

func (p *SNSPlugin) publish(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	message := req.Params["Message"]
	subject := req.Params["Subject"]

	// The topic has to exist before a MessageId is minted. Reading only the subscription index made a
	// deleted or misnamed topic indistinguishable from a live one with no subscribers, so a consumer's
	// error path for a torn-down topic was unreachable (#926).
	goCtx := context.Background()
	_, target, err := p.requireTopic(goCtx, req.Params["TopicArn"])
	if err != nil {
		return nil, err
	}
	topicName := target.Name

	// Parse message attributes from request params (MessageAttributes.entry.N.*).
	msgAttrs := parseSNSMessageAttributes(req.Params)

	// Fan out to subscriptions.
	subIDs, err := p.loadSubIDs(goCtx, snsSubTopicIDsStateKey(ctx.AccountID, ctx.Region, topicName))
	if err != nil {
		return nil, err
	}

	msgID := generateSNSSubID()

	for _, subARN := range subIDs {
		sub, loadErr := p.loadSub(goCtx, ctx.AccountID, ctx.Region, subARN)
		if loadErr != nil || sub == nil {
			continue
		}
		// Apply filter policy: skip if subscription has a policy and message doesn't match.
		if len(sub.FilterPolicy) > 0 && !matchesSNSFilterPolicy(sub.FilterPolicy, msgAttrs) {
			continue
		}
		p.dispatchToSubscriber(ctx, sub, message, subject)
	}

	type result struct {
		MessageId string `xml:"MessageId"` //nolint:revive // XML tag matches AWS protocol.
	}
	type response struct {
		XMLName          xml.Name         `xml:"PublishResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		PublishResult    result           `xml:"PublishResult"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:            snsXMLNS,
		PublishResult:    result{MessageId: msgID},
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

// dispatchToSubscriber delivers a message to a single subscriber endpoint.
func (p *SNSPlugin) dispatchToSubscriber(ctx *RequestContext, sub *SNSSubscription, message, subject string) {
	if p.registry == nil && sub.Protocol != "http" && sub.Protocol != "https" {
		return
	}
	switch sub.Protocol {
	case "sqs":
		envelope := p.buildSNSEnvelope(sub, message, subject)
		envelopeBytes, _ := json.Marshal(envelope)
		queueURL := sub.Endpoint
		_, err := p.registry.RouteRequest(ctx, &AWSRequest{
			Service:   "sqs",
			Operation: "SendMessage",
			Headers:   map[string]string{},
			Params: map[string]string{
				"Action":      "SendMessage",
				"QueueUrl":    queueURL,
				"MessageBody": string(envelopeBytes),
			},
		})
		if err != nil {
			p.logger.Warn("sns dispatch to sqs failed", "queue", queueURL, "err", err)
		}
	case "lambda":
		fnName := sub.Endpoint
		// Extract function name from ARN if necessary.
		if parts := strings.Split(fnName, ":"); len(parts) > 6 {
			fnName = parts[len(parts)-1]
		}
		payload, _ := json.Marshal(map[string]interface{}{
			"Records": []map[string]interface{}{
				{
					"EventSource":          "aws:sns",
					"EventVersion":         "1.0",
					"EventSubscriptionArn": sub.ARN,
					"Sns": map[string]interface{}{
						"TopicArn": sub.TopicARN,
						"Subject":  subject,
						"Message":  message,
					},
				},
			},
		})
		_, err := p.registry.RouteRequest(ctx, &AWSRequest{
			Service:   "lambda",
			Operation: "POST",
			Path:      "/2015-03-31/functions/" + fnName + "/invocations",
			Body:      payload,
			Headers:   map[string]string{},
			Params:    map[string]string{},
		})
		if err != nil {
			p.logger.Warn("sns dispatch to lambda failed", "function", fnName, "err", err)
		}
	case "http", "https":
		envelope := p.buildSNSEnvelope(sub, message, subject)
		envelopeBytes, _ := json.Marshal(envelope)
		httpReq, reqErr := http.NewRequest(http.MethodPost, sub.Endpoint, bytes.NewReader(envelopeBytes))
		if reqErr != nil {
			p.logger.Warn("sns dispatch to http: build request failed", "endpoint", sub.Endpoint, "err", reqErr)
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("x-amz-sns-message-type", "Notification")
		client := &http.Client{Timeout: 5 * time.Second}
		resp, doErr := client.Do(httpReq)
		if doErr != nil {
			p.logger.Warn("sns dispatch to http failed", "endpoint", sub.Endpoint, "err", doErr)
			return
		}
		_, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
	default:
		p.logger.Warn("sns dispatch: unsupported protocol (no-op)", "protocol", sub.Protocol, "endpoint", sub.Endpoint)
	}
}

// buildSNSEnvelope wraps a message in the standard SNS notification JSON envelope.
func (p *SNSPlugin) buildSNSEnvelope(sub *SNSSubscription, message, subject string) map[string]interface{} {
	return map[string]interface{}{
		"Type":             "Notification",
		"MessageId":        generateSNSSubID(),
		"TopicArn":         sub.TopicARN,
		"Subject":          subject,
		"Message":          message,
		"Timestamp":        p.tc.Now().UTC().Format(time.RFC3339),
		"SignatureVersion": "1",
		"Signature":        "stub",
		"SigningCertURL":   "https://sns.us-east-1.amazonaws.com/stub.pem",
		"UnsubscribeURL":   "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=" + sub.ARN,
	}
}

// parseSNSMessageAttributes extracts message attributes from Publish request params.
// AWS format: MessageAttributes.entry.N.Name / MessageAttributes.entry.N.Value.StringValue.
func parseSNSMessageAttributes(params map[string]string) map[string]string {
	attrs := make(map[string]string)
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("MessageAttributes.entry.%d.", i)
		name := params[prefix+"Name"]
		if name == "" {
			break
		}
		value := params[prefix+"Value.StringValue"]
		if value != "" {
			attrs[name] = value
		}
	}
	return attrs
}

// matchesSNSFilterPolicy evaluates a subscription filter policy against message
// attributes. All policy keys must have at least one matching value.
func matchesSNSFilterPolicy(policy map[string]interface{}, msgAttrs map[string]string) bool {
	for key, allowedRaw := range policy {
		attrVal, exists := msgAttrs[key]
		if !exists {
			return false
		}
		allowed, ok := allowedRaw.([]interface{})
		if !ok {
			continue
		}
		matched := false
		for _, v := range allowed {
			if fmt.Sprintf("%v", v) == attrVal {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// publishBatch publishes up to a batch of messages to one topic, refusing an ARN that names no topic.
//
// The absent topic is a **top-level** NotFound/404 rather than a per-entry BatchResultErrorEntry, which
// #926 flagged as needing sourcing because API_PublishBatch has both shapes. The page settles it by
// structure: TopicArn is a request-level parameter, one per call, so every entry in a batch addresses
// the same topic and a per-entry rendering would report the identical failure on all of them while
// still answering 200. The page's own framing agrees — "the result of publishing each message is
// reported individually in the response", and the batch codes it publishes (BatchEntryIdsNotDistinct,
// InvalidBatchEntryId, ParameterValueInvalid glossed "the parameter of an entry in a request") are
// about the individual messages. NotFound appears in the operation's top-level Errors list.
func (p *SNSPlugin) publishBatch(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	_, target, err := p.requireTopic(goCtx, req.Params["TopicArn"])
	if err != nil {
		return nil, err
	}

	subIDs, err := p.loadSubIDs(goCtx, snsSubTopicIDsStateKey(ctx.AccountID, ctx.Region, target.Name))
	if err != nil {
		return nil, err
	}

	type successEntry struct {
		ID        string `xml:"Id"`
		MessageId string `xml:"MessageId"` //nolint:revive // XML tag matches AWS protocol.
	}
	var successes []successEntry

	for i := 1; ; i++ {
		entryID := req.Params[fmt.Sprintf("PublishBatchRequestEntries.member.%d.Id", i)]
		if entryID == "" {
			break
		}
		message := req.Params[fmt.Sprintf("PublishBatchRequestEntries.member.%d.Message", i)]
		batchSubject := req.Params[fmt.Sprintf("PublishBatchRequestEntries.member.%d.Subject", i)]
		msgID := generateSNSSubID()
		for _, subARN := range subIDs {
			sub, loadErr := p.loadSub(goCtx, ctx.AccountID, ctx.Region, subARN)
			if loadErr != nil || sub == nil {
				continue
			}
			p.dispatchToSubscriber(ctx, sub, message, batchSubject)
		}
		successes = append(successes, successEntry{ID: entryID, MessageId: msgID})
	}

	type result struct {
		Successful []successEntry `xml:"Successful>member"`
	}
	type response struct {
		XMLName            xml.Name         `xml:"PublishBatchResponse"`
		Xmlns              string           `xml:"xmlns,attr"`
		PublishBatchResult result           `xml:"PublishBatchResult"`
		ResponseMetadata   responseMetadata `xml:"ResponseMetadata"`
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:              snsXMLNS,
		PublishBatchResult: result{Successful: successes},
		ResponseMetadata:   responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SNSPlugin) addPermission(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	// Stub — permissions are tracked by IAM plugin.
	return snsUnitResponse("AddPermission", ctx.RequestID)
}

func (p *SNSPlugin) removePermission(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	// Stub — permissions are tracked by IAM plugin.
	return snsUnitResponse("RemovePermission", ctx.RequestID)
}

func (p *SNSPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceArn"]
	target, arnErr := snsParseTopicARN(resourceARN)
	if arnErr != nil {
		return nil, arnErr
	}

	goCtx := context.Background()
	t, err := p.loadTopic(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, snsTopicNotFound(resourceARN)
	}

	added := snsTagParams(req.Params)
	t.EverTagged = taggingEverTagged(t.EverTagged, len(t.Tags), len(added))

	for _, tag := range added {
		// Replace an existing value rather than appending a second entry for the key. AWS states
		// "if the tag key already exists, the tag value is replaced".
		found := false
		for idx, existing := range t.Tags {
			if existing.Key == tag.Key {
				t.Tags[idx].Value = tag.Value
				found = true
				break
			}
		}
		if !found {
			t.Tags = append(t.Tags, tag)
		}
	}
	// Ordered by key, so a topic's tags read back the same however they were written. SNS's own merge
	// is already deterministic — it walks indexed parameters, not a Go map — but the Resource Groups
	// Tagging API's arm emits key-sorted order (#862), and without this one topic's tags came back in
	// two different orders depending on which API was asked.
	sortTagsByKey(t.Tags, func(tag SNSTag) string { return tag.Key })

	if err := p.saveTopic(goCtx, t); err != nil {
		return nil, fmt.Errorf("sns tagResource saveTopic: %w", err)
	}

	return snsEmptyResultResponse("TagResource", ctx.RequestID)
}

func (p *SNSPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceArn"]
	target, arnErr := snsParseTopicARN(resourceARN)
	if arnErr != nil {
		return nil, arnErr
	}

	goCtx := context.Background()
	t, err := p.loadTopic(goCtx, target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, snsTopicNotFound(resourceARN)
	}

	t.EverTagged = taggingEverTagged(t.EverTagged, len(t.Tags), 0)

	removeKeys := make(map[string]bool)
	for _, key := range snsTagKeyParams(req.Params) {
		removeKeys[key] = true
	}

	newTags := make([]SNSTag, 0, len(t.Tags))
	for _, tag := range t.Tags {
		if !removeKeys[tag.Key] {
			newTags = append(newTags, tag)
		}
	}
	t.Tags = newTags

	if err := p.saveTopic(goCtx, t); err != nil {
		return nil, fmt.Errorf("sns untagResource saveTopic: %w", err)
	}

	return snsEmptyResultResponse("UntagResource", ctx.RequestID)
}

func (p *SNSPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceArn"]
	target, arnErr := snsParseTopicARN(resourceARN)
	if arnErr != nil {
		return nil, arnErr
	}

	t, err := p.loadTopic(context.Background(), target.AccountID, target.Region, target.Name)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, snsTopicNotFound(resourceARN)
	}

	type tagEntry struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type result struct {
		Tags []tagEntry `xml:"Tags>member"`
	}
	type response struct {
		XMLName                   xml.Name         `xml:"ListTagsForResourceResponse"`
		Xmlns                     string           `xml:"xmlns,attr"`
		ListTagsForResourceResult result           `xml:"ListTagsForResourceResult"`
		ResponseMetadata          responseMetadata `xml:"ResponseMetadata"`
	}

	tags := make([]tagEntry, len(t.Tags))
	for i, tag := range t.Tags {
		tags[i] = tagEntry{Key: tag.Key, Value: tag.Value} //nolint:staticcheck
	}
	// Sorted on the way out as well as on the way in, so a record written by the Resource Groups
	// Tagging API's arm — or by an earlier substrate that did not sort — reads back in one order.
	sortTagsByKey(tags, func(tag tagEntry) string { return tag.Key })
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:                     snsXMLNS,
		ListTagsForResourceResult: result{Tags: tags},
		ResponseMetadata:          responseMetadata{RequestID: ctx.RequestID},
	})
}

// --- Response helpers ---

const snsXMLNS = "https://sns.amazonaws.com/doc/2010-03-31/"

// snsXMLResponse marshals v as XML and returns an AWSResponse with text/xml Content-Type.
func snsXMLResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("snsXMLResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}

// --- Utility ---

// The topic name no longer comes from a string scan. [snsParseTopicARN] in sns_tags.go is the one
// producer, and it takes no *RequestContext, so the account and Region come from the ARN at every one
// of the ten operations that used to take them from the caller (#925).
