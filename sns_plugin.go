package substrate

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
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("The action %s is not valid for this endpoint.", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- State helpers ---

func (p *SNSPlugin) topicKey(accountID, region, name string) string {
	return "topic:" + accountID + "/" + region + "/" + name
}

func (p *SNSPlugin) topicNamesKey(accountID, region string) string {
	return "topic_names:" + accountID + "/" + region
}

func (p *SNSPlugin) subKey(accountID, region, subID string) string {
	return "subscription:" + accountID + "/" + region + "/" + subID
}

func (p *SNSPlugin) subIDsKey(accountID, region string) string {
	return "sub_all_ids:" + accountID + "/" + region
}

func (p *SNSPlugin) subTopicIDsKey(accountID, region, topicName string) string {
	return "sub_ids:" + accountID + "/" + region + "/" + topicName
}

func (p *SNSPlugin) loadTopic(ctx context.Context, accountID, region, name string) (*SNSTopic, error) {
	data, err := p.state.Get(ctx, snsNamespace, p.topicKey(accountID, region, name))
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

func (p *SNSPlugin) saveTopic(ctx context.Context, t *SNSTopic) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("sns saveTopic marshal: %w", err)
	}
	return p.state.Put(ctx, snsNamespace, p.topicKey(t.AccountID, t.Region, t.Name), data)
}

func (p *SNSPlugin) loadTopicNames(ctx context.Context, accountID, region string) ([]string, error) {
	data, err := p.state.Get(ctx, snsNamespace, p.topicNamesKey(accountID, region))
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
	return p.state.Put(ctx, snsNamespace, p.topicNamesKey(accountID, region), data)
}

func (p *SNSPlugin) loadSub(ctx context.Context, accountID, region, subID string) (*SNSSubscription, error) {
	data, err := p.state.Get(ctx, snsNamespace, p.subKey(accountID, region, subID))
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
	return p.state.Put(ctx, snsNamespace, p.subKey(s.AccountID, s.Region, s.ARN), data)
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

func (p *SNSPlugin) deleteTopic(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	topicARN := req.Params["TopicArn"]
	name := snsNameFromARN(topicARN)

	goCtx := context.Background()
	t, err := p.loadTopic(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, &AWSError{Code: "NotFound", Message: "Topic not found", HTTPStatus: http.StatusNotFound}
	}

	_ = p.state.Delete(goCtx, snsNamespace, p.topicKey(ctx.AccountID, ctx.Region, name))

	names, err := p.loadTopicNames(goCtx, ctx.AccountID, ctx.Region)
	if err != nil {
		return nil, err
	}
	newNames := make([]string, 0, len(names))
	for _, n := range names {
		if n != name {
			newNames = append(newNames, n)
		}
	}
	if err := p.saveTopicNames(goCtx, ctx.AccountID, ctx.Region, newNames); err != nil {
		return nil, fmt.Errorf("sns deleteTopic saveTopicNames: %w", err)
	}

	type response struct {
		XMLName          xml.Name         `xml:"DeleteTopicResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:            snsXMLNS,
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SNSPlugin) getTopicAttributes(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	topicARN := req.Params["TopicArn"]
	name := snsNameFromARN(topicARN)

	t, err := p.loadTopic(context.Background(), ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, &AWSError{Code: "NotFound", Message: "Topic not found", HTTPStatus: http.StatusNotFound}
	}

	attrs := map[string]string{
		"TopicArn":           t.ARN,
		"SubscriptionsCount": "0",
	}
	for k, v := range t.Attributes {
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

func (p *SNSPlugin) setTopicAttributes(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	topicARN := req.Params["TopicArn"]
	name := snsNameFromARN(topicARN)
	attrName := req.Params["AttributeName"]
	attrValue := req.Params["AttributeValue"]

	goCtx := context.Background()
	t, err := p.loadTopic(goCtx, ctx.AccountID, ctx.Region, name)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, &AWSError{Code: "NotFound", Message: "Topic not found", HTTPStatus: http.StatusNotFound}
	}
	if t.Attributes == nil {
		t.Attributes = make(map[string]string)
	}
	t.Attributes[attrName] = attrValue
	if err := p.saveTopic(goCtx, t); err != nil {
		return nil, fmt.Errorf("sns setTopicAttributes saveTopic: %w", err)
	}

	type response struct {
		XMLName          xml.Name         `xml:"SetTopicAttributesResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:            snsXMLNS,
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
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

	topicName := snsNameFromARN(topicARN)
	subID := generateSNSSubID()
	subARN := snsSubscriptionARN(ctx.Region, ctx.AccountID, topicName, subID)

	sub := &SNSSubscription{
		ARN:       subARN,
		TopicARN:  topicARN,
		Protocol:  protocol,
		Endpoint:  endpoint,
		AccountID: ctx.AccountID,
		Region:    ctx.Region,
	}

	goCtx := context.Background()
	if err := p.saveSub(goCtx, sub); err != nil {
		return nil, fmt.Errorf("sns subscribe saveSub: %w", err)
	}

	// Add to per-topic and global lists.
	allIDs, err := p.loadSubIDs(goCtx, p.subIDsKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, err
	}
	allIDs = append(allIDs, subARN)
	if err := p.saveSubIDs(goCtx, p.subIDsKey(ctx.AccountID, ctx.Region), allIDs); err != nil {
		return nil, fmt.Errorf("sns subscribe saveSubIDs all: %w", err)
	}

	topicIDs, err := p.loadSubIDs(goCtx, p.subTopicIDsKey(ctx.AccountID, ctx.Region, topicName))
	if err != nil {
		return nil, err
	}
	topicIDs = append(topicIDs, subARN)
	if err := p.saveSubIDs(goCtx, p.subTopicIDsKey(ctx.AccountID, ctx.Region, topicName), topicIDs); err != nil {
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

func (p *SNSPlugin) unsubscribe(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	subARN := req.Params["SubscriptionArn"]

	goCtx := context.Background()
	sub, err := p.loadSub(goCtx, ctx.AccountID, ctx.Region, subARN)
	if err != nil {
		return nil, err
	}
	if sub == nil {
		// Idempotent — silently succeed.
		type response struct {
			XMLName          xml.Name         `xml:"UnsubscribeResponse"`
			Xmlns            string           `xml:"xmlns,attr"`
			ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
		}
		return snsXMLResponse(http.StatusOK, response{
			Xmlns:            snsXMLNS,
			ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
		})
	}

	_ = p.state.Delete(goCtx, snsNamespace, p.subKey(ctx.AccountID, ctx.Region, subARN))

	// Remove from global list.
	allIDs, err := p.loadSubIDs(goCtx, p.subIDsKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, err
	}
	newAll := make([]string, 0, len(allIDs))
	for _, id := range allIDs {
		if id != subARN {
			newAll = append(newAll, id)
		}
	}
	_ = p.saveSubIDs(goCtx, p.subIDsKey(ctx.AccountID, ctx.Region), newAll)

	// Remove from per-topic list.
	topicName := snsNameFromARN(sub.TopicARN)
	topicIDs, err := p.loadSubIDs(goCtx, p.subTopicIDsKey(ctx.AccountID, ctx.Region, topicName))
	if err != nil {
		return nil, err
	}
	newTopic := make([]string, 0, len(topicIDs))
	for _, id := range topicIDs {
		if id != subARN {
			newTopic = append(newTopic, id)
		}
	}
	_ = p.saveSubIDs(goCtx, p.subTopicIDsKey(ctx.AccountID, ctx.Region, topicName), newTopic)

	type response struct {
		XMLName          xml.Name         `xml:"UnsubscribeResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:            snsXMLNS,
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SNSPlugin) listSubscriptions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	allIDs, err := p.loadSubIDs(goCtx, p.subIDsKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, err
	}
	return p.buildSubscriptionListResponse(ctx, req, allIDs, "ListSubscriptions", "ListSubscriptionsResult")
}

func (p *SNSPlugin) listSubscriptionsByTopic(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	topicARN := req.Params["TopicArn"]
	topicName := snsNameFromARN(topicARN)

	goCtx := context.Background()
	topicIDs, err := p.loadSubIDs(goCtx, p.subTopicIDsKey(ctx.AccountID, ctx.Region, topicName))
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
	type response struct {
		XMLName          xml.Name         `xml:"SetSubscriptionAttributesResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:            snsXMLNS,
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SNSPlugin) publish(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	topicARN := req.Params["TopicArn"]
	message := req.Params["Message"]
	subject := req.Params["Subject"]

	topicName := snsNameFromARN(topicARN)

	// Parse message attributes from request params (MessageAttributes.entry.N.*).
	msgAttrs := parseSNSMessageAttributes(req.Params)

	// Fan out to subscriptions.
	goCtx := context.Background()
	subIDs, err := p.loadSubIDs(goCtx, p.subTopicIDsKey(ctx.AccountID, ctx.Region, topicName))
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

func (p *SNSPlugin) publishBatch(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	topicARN := req.Params["TopicArn"]
	topicName := snsNameFromARN(topicARN)

	goCtx := context.Background()
	subIDs, err := p.loadSubIDs(goCtx, p.subTopicIDsKey(ctx.AccountID, ctx.Region, topicName))
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
	type response struct {
		XMLName          xml.Name         `xml:"AddPermissionResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:            snsXMLNS,
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SNSPlugin) removePermission(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	// Stub — permissions are tracked by IAM plugin.
	type response struct {
		XMLName          xml.Name         `xml:"RemovePermissionResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:            snsXMLNS,
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SNSPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceArn"]
	topicName := snsNameFromARN(resourceARN)

	goCtx := context.Background()
	t, err := p.loadTopic(goCtx, ctx.AccountID, ctx.Region, topicName)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, &AWSError{Code: "NotFound", Message: "Resource not found", HTTPStatus: http.StatusNotFound}
	}

	for i := 1; ; i++ {
		k := req.Params[fmt.Sprintf("Tags.member.%d.Key", i)]
		v := req.Params[fmt.Sprintf("Tags.member.%d.Value", i)]
		if k == "" {
			break
		}
		// Merge tag.
		found := false
		for idx, existing := range t.Tags {
			if existing.Key == k {
				t.Tags[idx].Value = v
				found = true
				break
			}
		}
		if !found {
			t.Tags = append(t.Tags, SNSTag{Key: k, Value: v})
		}
	}

	if err := p.saveTopic(goCtx, t); err != nil {
		return nil, fmt.Errorf("sns tagResource saveTopic: %w", err)
	}

	type response struct {
		XMLName          xml.Name         `xml:"TagResourceResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:            snsXMLNS,
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SNSPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceArn"]
	topicName := snsNameFromARN(resourceARN)

	goCtx := context.Background()
	t, err := p.loadTopic(goCtx, ctx.AccountID, ctx.Region, topicName)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, &AWSError{Code: "NotFound", Message: "Resource not found", HTTPStatus: http.StatusNotFound}
	}

	removeKeys := make(map[string]bool)
	for i := 1; ; i++ {
		k := req.Params[fmt.Sprintf("TagKeys.member.%d", i)]
		if k == "" {
			break
		}
		removeKeys[k] = true
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

	type response struct {
		XMLName          xml.Name         `xml:"UntagResourceResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
	}
	return snsXMLResponse(http.StatusOK, response{
		Xmlns:            snsXMLNS,
		ResponseMetadata: responseMetadata{RequestID: ctx.RequestID},
	})
}

func (p *SNSPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceArn"]
	topicName := snsNameFromARN(resourceARN)

	t, err := p.loadTopic(context.Background(), ctx.AccountID, ctx.Region, topicName)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, &AWSError{Code: "NotFound", Message: "Resource not found", HTTPStatus: http.StatusNotFound}
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

// snsNameFromARN extracts the topic name from an SNS topic ARN.
// ARN format: arn:aws:sns:{region}:{account}:{name}.
func snsNameFromARN(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) >= 6 {
		return parts[len(parts)-1]
	}
	return arn
}
