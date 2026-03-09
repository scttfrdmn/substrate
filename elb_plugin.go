package substrate

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ELBPlugin emulates the AWS Elastic Load Balancing v2 (ALB/NLB) API.
// It supports Application and Network Load Balancers, listeners, target
// groups, and routing rules using the AWS Query protocol.
type ELBPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "elasticloadbalancing".
func (p *ELBPlugin) Name() string { return "elasticloadbalancing" }

// Initialize sets up the ELBPlugin with the provided configuration.
func (p *ELBPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for ELBPlugin.
func (p *ELBPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an ELBv2 query-protocol request to the appropriate handler.
func (p *ELBPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	action := req.Operation
	if action == "" {
		action = req.Params["Action"]
	}
	switch action {
	case "CreateLoadBalancer":
		return p.createLoadBalancer(ctx, req)
	case "DescribeLoadBalancers":
		return p.describeLoadBalancers(ctx, req)
	case "DeleteLoadBalancer":
		return p.deleteLoadBalancer(ctx, req)
	case "DescribeLoadBalancerAttributes":
		return p.describeLoadBalancerAttributes(ctx, req)
	case "ModifyLoadBalancerAttributes":
		return p.modifyLoadBalancerAttributes(ctx, req)
	case "CreateTargetGroup":
		return p.createTargetGroup(ctx, req)
	case "DescribeTargetGroups":
		return p.describeTargetGroups(ctx, req)
	case "DeleteTargetGroup":
		return p.deleteTargetGroup(ctx, req)
	case "ModifyTargetGroup":
		return p.modifyTargetGroup(ctx, req)
	case "RegisterTargets":
		return p.registerTargets(ctx, req)
	case "DeregisterTargets":
		return p.deregisterTargets(ctx, req)
	case "DescribeTargetHealth":
		return p.describeTargetHealth(ctx, req)
	case "CreateListener":
		return p.createListener(ctx, req)
	case "DescribeListeners":
		return p.describeListeners(ctx, req)
	case "DeleteListener":
		return p.deleteListener(ctx, req)
	case "ModifyListener":
		return p.modifyListener(ctx, req)
	case "CreateRule":
		return p.createRule(ctx, req)
	case "DescribeRules":
		return p.describeRules(ctx, req)
	case "DeleteRule":
		return p.deleteRule(ctx, req)
	case "SetRulePriorities":
		return p.setRulePriorities(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "ELBPlugin: unknown action " + action,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Load Balancer operations ---

func (p *ELBPlugin) createLoadBalancer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["Name"]
	if name == "" {
		return nil, &AWSError{Code: "ValidationError", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	lbType := req.Params["Type"]
	if lbType == "" {
		lbType = "application"
	}
	scheme := req.Params["Scheme"]
	if scheme == "" {
		scheme = "internet-facing"
	}
	vpcID := req.Params["VpcId"]
	suffix := generateELBSuffix()
	arn := elbLoadBalancerARN(reqCtx.Region, reqCtx.AccountID, lbType, name, suffix)
	dnsName := elbDNSName(name, suffix, reqCtx.Region)

	subnets := extractIndexedParams(req.Params, "Subnets.member")
	sgs := extractIndexedParams(req.Params, "SecurityGroups.member")

	azs := make([]string, len(subnets))
	for i := range subnets {
		azs[i] = reqCtx.Region + string(rune('a'+i))
	}
	if len(azs) == 0 {
		azs = []string{reqCtx.Region + "a"}
	}

	lb := ELBLoadBalancer{
		Name:              name,
		ARN:               arn,
		DNSName:           dnsName,
		Type:              lbType,
		Scheme:            scheme,
		VpcID:             vpcID,
		State:             ELBState{Code: "active"},
		AvailabilityZones: azs,
		SecurityGroups:    sgs,
		AccountID:         reqCtx.AccountID,
		Region:            reqCtx.Region,
		CreatedTime:       p.tc.Now(),
		Suffix:            suffix,
	}
	data, err := json.Marshal(lb)
	if err != nil {
		return nil, fmt.Errorf("elb createLoadBalancer marshal: %w", err)
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	if err := p.state.Put(context.Background(), elbNamespace, "lb:"+scope+"/"+name, data); err != nil {
		return nil, fmt.Errorf("elb createLoadBalancer state.Put: %w", err)
	}
	if err := p.appendToList(scope, "lb_names", name); err != nil {
		return nil, err
	}

	type lbResult struct {
		LoadBalancers []elbLBItem `xml:"LoadBalancers>member"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateLoadBalancerResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  lbResult `xml:"CreateLoadBalancerResult"`
	}
	return elbXMLResponse(http.StatusOK, response{
		XMLNS:  elbXMLNS,
		Result: lbResult{LoadBalancers: []elbLBItem{lbToItem(lb)}},
	})
}

func (p *ELBPlugin) describeLoadBalancers(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	names := extractIndexedParams(req.Params, "Names.member")
	arns := extractIndexedParams(req.Params, "LoadBalancerArns.member")

	allKeys, err := p.state.List(context.Background(), elbNamespace, "lb:"+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("elb describeLoadBalancers list: %w", err)
	}

	type lbResult struct {
		LoadBalancers []elbLBItem `xml:"LoadBalancers>member"`
	}
	type response struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  lbResult `xml:"DescribeLoadBalancersResult"`
	}
	resp := response{XMLNS: elbXMLNS}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), elbNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var lb ELBLoadBalancer
		if json.Unmarshal(data, &lb) != nil {
			continue
		}
		if len(names) > 0 && !containsStr(names, lb.Name) {
			continue
		}
		if len(arns) > 0 && !containsStr(arns, lb.ARN) {
			continue
		}
		resp.Result.LoadBalancers = append(resp.Result.LoadBalancers, lbToItem(lb))
	}
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *ELBPlugin) deleteLoadBalancer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arn := req.Params["LoadBalancerArn"]
	if arn == "" {
		return nil, &AWSError{Code: "ValidationError", Message: "LoadBalancerArn is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	allKeys, _ := p.state.List(context.Background(), elbNamespace, "lb:"+scope+"/")
	for _, k := range allKeys {
		data, err := p.state.Get(context.Background(), elbNamespace, k)
		if err != nil || data == nil {
			continue
		}
		var lb ELBLoadBalancer
		if json.Unmarshal(data, &lb) != nil || lb.ARN != arn {
			continue
		}
		if err := p.state.Delete(context.Background(), elbNamespace, k); err != nil {
			return nil, fmt.Errorf("elb deleteLoadBalancer delete: %w", err)
		}
		p.removeFromList(scope, "lb_names", lb.Name)
		break
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteLoadBalancerResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return elbXMLResponse(http.StatusOK, response{XMLNS: elbXMLNS})
}

func (p *ELBPlugin) describeLoadBalancerAttributes(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	type attr struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type attrResult struct {
		Attributes []attr `xml:"Attributes>member"`
	}
	type response struct {
		XMLName xml.Name   `xml:"DescribeLoadBalancerAttributesResponse"`
		XMLNS   string     `xml:"xmlns,attr"`
		Result  attrResult `xml:"DescribeLoadBalancerAttributesResult"`
	}
	return elbXMLResponse(http.StatusOK, response{XMLNS: elbXMLNS})
}

func (p *ELBPlugin) modifyLoadBalancerAttributes(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	type attrResult struct{}
	type response struct {
		XMLName xml.Name   `xml:"ModifyLoadBalancerAttributesResponse"`
		XMLNS   string     `xml:"xmlns,attr"`
		Result  attrResult `xml:"ModifyLoadBalancerAttributesResult"`
	}
	return elbXMLResponse(http.StatusOK, response{XMLNS: elbXMLNS})
}

// --- Target Group operations ---

func (p *ELBPlugin) createTargetGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["Name"]
	if name == "" {
		return nil, &AWSError{Code: "ValidationError", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	protocol := req.Params["Protocol"]
	if protocol == "" {
		protocol = "HTTP"
	}
	port, _ := strconv.Atoi(req.Params["Port"])
	vpcID := req.Params["VpcId"]
	targetType := req.Params["TargetType"]
	if targetType == "" {
		targetType = "instance"
	}
	suffix := generateELBSuffix()
	arn := elbTargetGroupARN(reqCtx.Region, reqCtx.AccountID, name, suffix)

	tg := ELBTargetGroup{
		ARN:                 arn,
		Name:                name,
		Protocol:            protocol,
		Port:                port,
		VpcID:               vpcID,
		TargetType:          targetType,
		HealthCheckPath:     req.Params["HealthCheckPath"],
		HealthCheckProtocol: req.Params["HealthCheckProtocol"],
		HealthCheckPort:     req.Params["HealthCheckPort"],
		AccountID:           reqCtx.AccountID,
		Region:              reqCtx.Region,
		Suffix:              suffix,
	}
	data, err := json.Marshal(tg)
	if err != nil {
		return nil, fmt.Errorf("elb createTargetGroup marshal: %w", err)
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	if err := p.state.Put(context.Background(), elbNamespace, "tg:"+scope+"/"+name, data); err != nil {
		return nil, fmt.Errorf("elb createTargetGroup state.Put: %w", err)
	}
	if err := p.appendToList(scope, "tg_names", name); err != nil {
		return nil, err
	}

	type tgResult struct {
		TargetGroups []elbTGItem `xml:"TargetGroups>member"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateTargetGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  tgResult `xml:"CreateTargetGroupResult"`
	}
	return elbXMLResponse(http.StatusOK, response{
		XMLNS:  elbXMLNS,
		Result: tgResult{TargetGroups: []elbTGItem{tgToItem(tg)}},
	})
}

func (p *ELBPlugin) describeTargetGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	names := extractIndexedParams(req.Params, "Names.member")
	arns := extractIndexedParams(req.Params, "TargetGroupArns.member")

	allKeys, err := p.state.List(context.Background(), elbNamespace, "tg:"+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("elb describeTargetGroups list: %w", err)
	}
	type tgResult struct {
		TargetGroups []elbTGItem `xml:"TargetGroups>member"`
	}
	type response struct {
		XMLName xml.Name `xml:"DescribeTargetGroupsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  tgResult `xml:"DescribeTargetGroupsResult"`
	}
	resp := response{XMLNS: elbXMLNS}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), elbNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var tg ELBTargetGroup
		if json.Unmarshal(data, &tg) != nil {
			continue
		}
		if len(names) > 0 && !containsStr(names, tg.Name) {
			continue
		}
		if len(arns) > 0 && !containsStr(arns, tg.ARN) {
			continue
		}
		resp.Result.TargetGroups = append(resp.Result.TargetGroups, tgToItem(tg))
	}
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *ELBPlugin) deleteTargetGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arn := req.Params["TargetGroupArn"]
	if arn == "" {
		return nil, &AWSError{Code: "ValidationError", Message: "TargetGroupArn is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	allKeys, _ := p.state.List(context.Background(), elbNamespace, "tg:"+scope+"/")
	for _, k := range allKeys {
		data, err := p.state.Get(context.Background(), elbNamespace, k)
		if err != nil || data == nil {
			continue
		}
		var tg ELBTargetGroup
		if json.Unmarshal(data, &tg) != nil || tg.ARN != arn {
			continue
		}
		if err := p.state.Delete(context.Background(), elbNamespace, k); err != nil {
			return nil, fmt.Errorf("elb deleteTargetGroup delete: %w", err)
		}
		p.removeFromList(scope, "tg_names", tg.Name)
		break
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteTargetGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return elbXMLResponse(http.StatusOK, response{XMLNS: elbXMLNS})
}

func (p *ELBPlugin) modifyTargetGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arn := req.Params["TargetGroupArn"]
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	allKeys, _ := p.state.List(context.Background(), elbNamespace, "tg:"+scope+"/")
	for _, k := range allKeys {
		data, err := p.state.Get(context.Background(), elbNamespace, k)
		if err != nil || data == nil {
			continue
		}
		var tg ELBTargetGroup
		if json.Unmarshal(data, &tg) != nil || tg.ARN != arn {
			continue
		}
		if v := req.Params["HealthCheckPath"]; v != "" {
			tg.HealthCheckPath = v
		}
		if v := req.Params["HealthCheckProtocol"]; v != "" {
			tg.HealthCheckProtocol = v
		}
		newData, _ := json.Marshal(tg)
		_ = p.state.Put(context.Background(), elbNamespace, k, newData)
		break
	}
	type tgResult struct {
		TargetGroups []elbTGItem `xml:"TargetGroups>member"`
	}
	type response struct {
		XMLName xml.Name `xml:"ModifyTargetGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  tgResult `xml:"ModifyTargetGroupResult"`
	}
	return elbXMLResponse(http.StatusOK, response{XMLNS: elbXMLNS})
}

// --- Target operations ---

func (p *ELBPlugin) registerTargets(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	tgARN := req.Params["TargetGroupArn"]
	scope := reqCtx.AccountID + "/" + reqCtx.Region

	var newTargets []ELBTarget
	for i := 1; ; i++ {
		id := req.Params[fmt.Sprintf("Targets.member.%d.Id", i)]
		if id == "" {
			break
		}
		port, _ := strconv.Atoi(req.Params[fmt.Sprintf("Targets.member.%d.Port", i)])
		newTargets = append(newTargets, ELBTarget{ID: id, Port: port})
	}

	allKeys, _ := p.state.List(context.Background(), elbNamespace, "tg:"+scope+"/")
	for _, k := range allKeys {
		data, err := p.state.Get(context.Background(), elbNamespace, k)
		if err != nil || data == nil {
			continue
		}
		var tg ELBTargetGroup
		if json.Unmarshal(data, &tg) != nil || tg.ARN != tgARN {
			continue
		}
		existing := make(map[string]bool)
		for _, t := range tg.Targets {
			existing[t.ID] = true
		}
		for _, nt := range newTargets {
			if !existing[nt.ID] {
				tg.Targets = append(tg.Targets, nt)
			}
		}
		newData, _ := json.Marshal(tg)
		_ = p.state.Put(context.Background(), elbNamespace, k, newData)
		break
	}

	type response struct {
		XMLName xml.Name `xml:"RegisterTargetsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return elbXMLResponse(http.StatusOK, response{XMLNS: elbXMLNS})
}

func (p *ELBPlugin) deregisterTargets(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	tgARN := req.Params["TargetGroupArn"]
	scope := reqCtx.AccountID + "/" + reqCtx.Region

	var removeIDs []string
	for i := 1; ; i++ {
		id := req.Params[fmt.Sprintf("Targets.member.%d.Id", i)]
		if id == "" {
			break
		}
		removeIDs = append(removeIDs, id)
	}

	allKeys, _ := p.state.List(context.Background(), elbNamespace, "tg:"+scope+"/")
	for _, k := range allKeys {
		data, err := p.state.Get(context.Background(), elbNamespace, k)
		if err != nil || data == nil {
			continue
		}
		var tg ELBTargetGroup
		if json.Unmarshal(data, &tg) != nil || tg.ARN != tgARN {
			continue
		}
		removeSet := make(map[string]bool)
		for _, id := range removeIDs {
			removeSet[id] = true
		}
		filtered := tg.Targets[:0]
		for _, t := range tg.Targets {
			if !removeSet[t.ID] {
				filtered = append(filtered, t)
			}
		}
		tg.Targets = filtered
		newData, _ := json.Marshal(tg)
		_ = p.state.Put(context.Background(), elbNamespace, k, newData)
		break
	}

	type response struct {
		XMLName xml.Name `xml:"DeregisterTargetsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return elbXMLResponse(http.StatusOK, response{XMLNS: elbXMLNS})
}

func (p *ELBPlugin) describeTargetHealth(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	tgARN := req.Params["TargetGroupArn"]
	scope := reqCtx.AccountID + "/" + reqCtx.Region

	type targetDesc struct {
		Target struct {
			ID   string `xml:"Id"`
			Port int    `xml:"Port"`
		} `xml:"Target"`
		TargetHealth struct {
			State string `xml:"State"`
		} `xml:"TargetHealth"`
	}
	type healthResult struct {
		Descriptions []targetDesc `xml:"TargetHealthDescriptions>member"`
	}
	type response struct {
		XMLName xml.Name     `xml:"DescribeTargetHealthResponse"`
		XMLNS   string       `xml:"xmlns,attr"`
		Result  healthResult `xml:"DescribeTargetHealthResult"`
	}
	resp := response{XMLNS: elbXMLNS}

	allKeys, _ := p.state.List(context.Background(), elbNamespace, "tg:"+scope+"/")
	for _, k := range allKeys {
		data, err := p.state.Get(context.Background(), elbNamespace, k)
		if err != nil || data == nil {
			continue
		}
		var tg ELBTargetGroup
		if json.Unmarshal(data, &tg) != nil || tg.ARN != tgARN {
			continue
		}
		for _, t := range tg.Targets {
			desc := targetDesc{}
			desc.Target.ID = t.ID
			desc.Target.Port = t.Port
			desc.TargetHealth.State = "healthy"
			resp.Result.Descriptions = append(resp.Result.Descriptions, desc)
		}
		break
	}
	return elbXMLResponse(http.StatusOK, resp)
}

// --- Listener operations ---

func (p *ELBPlugin) createListener(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	lbARN := req.Params["LoadBalancerArn"]
	if lbARN == "" {
		return nil, &AWSError{Code: "ValidationError", Message: "LoadBalancerArn is required", HTTPStatus: http.StatusBadRequest}
	}
	protocol := req.Params["Protocol"]
	port, _ := strconv.Atoi(req.Params["Port"])

	var actions []ELBAction
	for i := 1; ; i++ {
		actionType := req.Params[fmt.Sprintf("DefaultActions.member.%d.Type", i)]
		if actionType == "" {
			break
		}
		actions = append(actions, ELBAction{
			Type:           actionType,
			TargetGroupArn: req.Params[fmt.Sprintf("DefaultActions.member.%d.TargetGroupArn", i)],
		})
	}

	suffix := generateELBSuffix()
	arn := elbListenerARN(lbARN, suffix)
	listener := ELBListener{
		ARN:             arn,
		LoadBalancerARN: lbARN,
		Port:            port,
		Protocol:        protocol,
		DefaultActions:  actions,
		AccountID:       reqCtx.AccountID,
		Region:          reqCtx.Region,
		Suffix:          suffix,
	}
	data, err := json.Marshal(listener)
	if err != nil {
		return nil, fmt.Errorf("elb createListener marshal: %w", err)
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "listener:" + scope + "/" + suffix
	if err := p.state.Put(context.Background(), elbNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("elb createListener state.Put: %w", err)
	}
	lbName := extractELBNameFromARN(lbARN)
	if err := p.appendToList(scope+"/"+lbName, "listener_ids", suffix); err != nil {
		return nil, err
	}

	type listenerResult struct {
		Listeners []elbListenerItem `xml:"Listeners>member"`
	}
	type response struct {
		XMLName xml.Name       `xml:"CreateListenerResponse"`
		XMLNS   string         `xml:"xmlns,attr"`
		Result  listenerResult `xml:"CreateListenerResult"`
	}
	return elbXMLResponse(http.StatusOK, response{
		XMLNS:  elbXMLNS,
		Result: listenerResult{Listeners: []elbListenerItem{listenerToItem(listener)}},
	})
}

func (p *ELBPlugin) describeListeners(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	listenerARNs := extractIndexedParams(req.Params, "ListenerArns.member")
	lbARN := req.Params["LoadBalancerArn"]

	allKeys, err := p.state.List(context.Background(), elbNamespace, "listener:"+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("elb describeListeners list: %w", err)
	}
	type listenerResult struct {
		Listeners []elbListenerItem `xml:"Listeners>member"`
	}
	type response struct {
		XMLName xml.Name       `xml:"DescribeListenersResponse"`
		XMLNS   string         `xml:"xmlns,attr"`
		Result  listenerResult `xml:"DescribeListenersResult"`
	}
	resp := response{XMLNS: elbXMLNS}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), elbNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var l ELBListener
		if json.Unmarshal(data, &l) != nil {
			continue
		}
		if len(listenerARNs) > 0 && !containsStr(listenerARNs, l.ARN) {
			continue
		}
		if lbARN != "" && l.LoadBalancerARN != lbARN {
			continue
		}
		resp.Result.Listeners = append(resp.Result.Listeners, listenerToItem(l))
	}
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *ELBPlugin) deleteListener(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arn := req.Params["ListenerArn"]
	if arn == "" {
		return nil, &AWSError{Code: "ValidationError", Message: "ListenerArn is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	allKeys, _ := p.state.List(context.Background(), elbNamespace, "listener:"+scope+"/")
	for _, k := range allKeys {
		data, err := p.state.Get(context.Background(), elbNamespace, k)
		if err != nil || data == nil {
			continue
		}
		var l ELBListener
		if json.Unmarshal(data, &l) != nil || l.ARN != arn {
			continue
		}
		if err := p.state.Delete(context.Background(), elbNamespace, k); err != nil {
			return nil, fmt.Errorf("elb deleteListener delete: %w", err)
		}
		break
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteListenerResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return elbXMLResponse(http.StatusOK, response{XMLNS: elbXMLNS})
}

func (p *ELBPlugin) modifyListener(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arn := req.Params["ListenerArn"]
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	allKeys, _ := p.state.List(context.Background(), elbNamespace, "listener:"+scope+"/")
	for _, k := range allKeys {
		data, err := p.state.Get(context.Background(), elbNamespace, k)
		if err != nil || data == nil {
			continue
		}
		var l ELBListener
		if json.Unmarshal(data, &l) != nil || l.ARN != arn {
			continue
		}
		if v := req.Params["Protocol"]; v != "" {
			l.Protocol = v
		}
		if v := req.Params["Port"]; v != "" {
			l.Port, _ = strconv.Atoi(v)
		}
		newData, _ := json.Marshal(l)
		_ = p.state.Put(context.Background(), elbNamespace, k, newData)

		type listenerResult struct {
			Listeners []elbListenerItem `xml:"Listeners>member"`
		}
		type response struct {
			XMLName xml.Name       `xml:"ModifyListenerResponse"`
			XMLNS   string         `xml:"xmlns,attr"`
			Result  listenerResult `xml:"ModifyListenerResult"`
		}
		return elbXMLResponse(http.StatusOK, response{
			XMLNS:  elbXMLNS,
			Result: listenerResult{Listeners: []elbListenerItem{listenerToItem(l)}},
		})
	}
	type listenerResult struct {
		Listeners []elbListenerItem `xml:"Listeners>member"`
	}
	type response struct {
		XMLName xml.Name       `xml:"ModifyListenerResponse"`
		XMLNS   string         `xml:"xmlns,attr"`
		Result  listenerResult `xml:"ModifyListenerResult"`
	}
	return elbXMLResponse(http.StatusOK, response{XMLNS: elbXMLNS})
}

// --- Rule operations ---

func (p *ELBPlugin) createRule(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	listenerARN := req.Params["ListenerArn"]
	if listenerARN == "" {
		return nil, &AWSError{Code: "ValidationError", Message: "ListenerArn is required", HTTPStatus: http.StatusBadRequest}
	}
	priority := req.Params["Priority"]

	var conditions []ELBCondition
	for i := 1; ; i++ {
		field := req.Params[fmt.Sprintf("Conditions.member.%d.Field", i)]
		if field == "" {
			break
		}
		var vals []string
		for j := 1; ; j++ {
			v := req.Params[fmt.Sprintf("Conditions.member.%d.Values.member.%d", i, j)]
			if v == "" {
				break
			}
			vals = append(vals, v)
		}
		conditions = append(conditions, ELBCondition{Field: field, Values: vals})
	}

	var actions []ELBAction
	for i := 1; ; i++ {
		actionType := req.Params[fmt.Sprintf("Actions.member.%d.Type", i)]
		if actionType == "" {
			break
		}
		actions = append(actions, ELBAction{
			Type:           actionType,
			TargetGroupArn: req.Params[fmt.Sprintf("Actions.member.%d.TargetGroupArn", i)],
		})
	}

	suffix := generateELBSuffix()
	arn := elbRuleARN(listenerARN, suffix)
	rule := ELBRule{
		ARN:         arn,
		ListenerARN: listenerARN,
		Priority:    priority,
		Conditions:  conditions,
		Actions:     actions,
		IsDefault:   false,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
		Suffix:      suffix,
	}
	data, err := json.Marshal(rule)
	if err != nil {
		return nil, fmt.Errorf("elb createRule marshal: %w", err)
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "rule:" + scope + "/" + suffix
	if err := p.state.Put(context.Background(), elbNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("elb createRule state.Put: %w", err)
	}
	listenerSuffix := extractELBSuffixFromARN(listenerARN)
	if err := p.appendToList(scope+"/"+listenerSuffix, "rule_ids", suffix); err != nil {
		return nil, err
	}

	type ruleResult struct {
		Rules []elbRuleItem `xml:"Rules>member"`
	}
	type response struct {
		XMLName xml.Name   `xml:"CreateRuleResponse"`
		XMLNS   string     `xml:"xmlns,attr"`
		Result  ruleResult `xml:"CreateRuleResult"`
	}
	return elbXMLResponse(http.StatusOK, response{
		XMLNS:  elbXMLNS,
		Result: ruleResult{Rules: []elbRuleItem{ruleToItem(rule)}},
	})
}

func (p *ELBPlugin) describeRules(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	ruleARNs := extractIndexedParams(req.Params, "RuleArns.member")
	listenerARN := req.Params["ListenerArn"]

	allKeys, err := p.state.List(context.Background(), elbNamespace, "rule:"+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("elb describeRules list: %w", err)
	}
	type ruleResult struct {
		Rules []elbRuleItem `xml:"Rules>member"`
	}
	type response struct {
		XMLName xml.Name   `xml:"DescribeRulesResponse"`
		XMLNS   string     `xml:"xmlns,attr"`
		Result  ruleResult `xml:"DescribeRulesResult"`
	}
	resp := response{XMLNS: elbXMLNS}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), elbNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var r ELBRule
		if json.Unmarshal(data, &r) != nil {
			continue
		}
		if len(ruleARNs) > 0 && !containsStr(ruleARNs, r.ARN) {
			continue
		}
		if listenerARN != "" && r.ListenerARN != listenerARN {
			continue
		}
		resp.Result.Rules = append(resp.Result.Rules, ruleToItem(r))
	}
	return elbXMLResponse(http.StatusOK, resp)
}

func (p *ELBPlugin) deleteRule(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	arn := req.Params["RuleArn"]
	if arn == "" {
		return nil, &AWSError{Code: "ValidationError", Message: "RuleArn is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	allKeys, _ := p.state.List(context.Background(), elbNamespace, "rule:"+scope+"/")
	for _, k := range allKeys {
		data, err := p.state.Get(context.Background(), elbNamespace, k)
		if err != nil || data == nil {
			continue
		}
		var r ELBRule
		if json.Unmarshal(data, &r) != nil || r.ARN != arn {
			continue
		}
		if err := p.state.Delete(context.Background(), elbNamespace, k); err != nil {
			return nil, fmt.Errorf("elb deleteRule delete: %w", err)
		}
		break
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteRuleResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return elbXMLResponse(http.StatusOK, response{XMLNS: elbXMLNS})
}

func (p *ELBPlugin) setRulePriorities(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	for i := 1; ; i++ {
		ruleARN := req.Params[fmt.Sprintf("RulePriorities.member.%d.RuleArn", i)]
		if ruleARN == "" {
			break
		}
		priority := req.Params[fmt.Sprintf("RulePriorities.member.%d.Priority", i)]
		allKeys, _ := p.state.List(context.Background(), elbNamespace, "rule:"+scope+"/")
		for _, k := range allKeys {
			data, err := p.state.Get(context.Background(), elbNamespace, k)
			if err != nil || data == nil {
				continue
			}
			var r ELBRule
			if json.Unmarshal(data, &r) != nil || r.ARN != ruleARN {
				continue
			}
			r.Priority = priority
			newData, _ := json.Marshal(r)
			_ = p.state.Put(context.Background(), elbNamespace, k, newData)
			break
		}
	}
	type ruleResult struct {
		Rules []elbRuleItem `xml:"Rules>member"`
	}
	type response struct {
		XMLName xml.Name   `xml:"SetRulePrioritiesResponse"`
		XMLNS   string     `xml:"xmlns,attr"`
		Result  ruleResult `xml:"SetRulePrioritiesResult"`
	}
	return elbXMLResponse(http.StatusOK, response{XMLNS: elbXMLNS})
}

// --- Helpers ---

// elbXMLNS is the XML namespace for ELBv2 API responses.
const elbXMLNS = "https://elasticloadbalancing.amazonaws.com/doc/2015-12-01/"

// elbLBItem is the XML representation of an ELBv2 load balancer.
type elbLBItem struct {
	LoadBalancerArn  string       `xml:"LoadBalancerArn"`
	LoadBalancerName string       `xml:"LoadBalancerName"`
	DNSName          string       `xml:"DNSName"`
	Type             string       `xml:"Type"`
	Scheme           string       `xml:"Scheme"`
	VpcID            string       `xml:"VpcId"`
	State            elbStateItem `xml:"State"`
	CreatedTime      string       `xml:"CreatedTime"`
}

// elbStateItem is the XML representation of an ELBv2 load balancer state.
type elbStateItem struct {
	Code string `xml:"Code"`
}

func lbToItem(lb ELBLoadBalancer) elbLBItem {
	return elbLBItem{
		LoadBalancerArn:  lb.ARN,
		LoadBalancerName: lb.Name,
		DNSName:          lb.DNSName,
		Type:             lb.Type,
		Scheme:           lb.Scheme,
		VpcID:            lb.VpcID,
		State:            elbStateItem{Code: lb.State.Code},
		CreatedTime:      lb.CreatedTime.UTC().Format(time.RFC3339),
	}
}

// elbTGItem is the XML representation of an ELBv2 target group.
type elbTGItem struct {
	TargetGroupArn      string `xml:"TargetGroupArn"`
	TargetGroupName     string `xml:"TargetGroupName"`
	Protocol            string `xml:"Protocol"`
	Port                int    `xml:"Port"`
	VpcID               string `xml:"VpcId"`
	TargetType          string `xml:"TargetType"`
	HealthCheckPath     string `xml:"HealthCheckPath"`
	HealthCheckProtocol string `xml:"HealthCheckProtocol"`
	HealthCheckPort     string `xml:"HealthCheckPort"`
}

func tgToItem(tg ELBTargetGroup) elbTGItem {
	return elbTGItem{
		TargetGroupArn:      tg.ARN,
		TargetGroupName:     tg.Name,
		Protocol:            tg.Protocol,
		Port:                tg.Port,
		VpcID:               tg.VpcID,
		TargetType:          tg.TargetType,
		HealthCheckPath:     tg.HealthCheckPath,
		HealthCheckProtocol: tg.HealthCheckProtocol,
		HealthCheckPort:     tg.HealthCheckPort,
	}
}

// elbListenerItem is the XML representation of an ELBv2 listener.
type elbListenerItem struct {
	ListenerArn     string          `xml:"ListenerArn"`
	LoadBalancerArn string          `xml:"LoadBalancerArn"`
	Port            int             `xml:"Port"`
	Protocol        string          `xml:"Protocol"`
	DefaultActions  []elbActionItem `xml:"DefaultActions>member"`
}

// elbActionItem is the XML representation of an ELBv2 action.
type elbActionItem struct {
	Type           string `xml:"Type"`
	TargetGroupArn string `xml:"TargetGroupArn,omitempty"`
	Order          int    `xml:"Order,omitempty"`
}

func listenerToItem(l ELBListener) elbListenerItem {
	item := elbListenerItem{
		ListenerArn:     l.ARN,
		LoadBalancerArn: l.LoadBalancerARN,
		Port:            l.Port,
		Protocol:        l.Protocol,
	}
	for _, a := range l.DefaultActions {
		item.DefaultActions = append(item.DefaultActions, elbActionItem{ //nolint:staticcheck
			Type:           a.Type,
			TargetGroupArn: a.TargetGroupArn,
			Order:          a.Order,
		})
	}
	return item
}

// elbConditionItem is the XML representation of a rule condition.
type elbConditionItem struct {
	Field  string   `xml:"Field"`
	Values []string `xml:"Values>member"`
}

// elbRuleItem is the XML representation of an ELBv2 rule.
type elbRuleItem struct {
	RuleArn    string             `xml:"RuleArn"`
	Priority   string             `xml:"Priority"`
	IsDefault  bool               `xml:"IsDefault"`
	Conditions []elbConditionItem `xml:"Conditions>member"`
	Actions    []elbActionItem    `xml:"Actions>member"`
}

func ruleToItem(r ELBRule) elbRuleItem {
	item := elbRuleItem{
		RuleArn:   r.ARN,
		Priority:  r.Priority,
		IsDefault: r.IsDefault,
	}
	for _, c := range r.Conditions {
		item.Conditions = append(item.Conditions, elbConditionItem{Field: c.Field, Values: c.Values}) //nolint:staticcheck
	}
	for _, a := range r.Actions {
		item.Actions = append(item.Actions, elbActionItem{ //nolint:staticcheck
			Type:           a.Type,
			TargetGroupArn: a.TargetGroupArn,
			Order:          a.Order,
		})
	}
	return item
}

func (p *ELBPlugin) appendToList(scope, listName, id string) error {
	key := listName + ":" + scope
	data, err := p.state.Get(context.Background(), elbNamespace, key)
	if err != nil {
		return fmt.Errorf("elb appendToList get %s: %w", key, err)
	}
	var ids []string
	if data != nil {
		_ = json.Unmarshal(data, &ids)
	}
	ids = append(ids, id)
	newData, _ := json.Marshal(ids)
	return p.state.Put(context.Background(), elbNamespace, key, newData)
}

func (p *ELBPlugin) removeFromList(scope, listName, id string) {
	key := listName + ":" + scope
	data, err := p.state.Get(context.Background(), elbNamespace, key)
	if err != nil || data == nil {
		return
	}
	var ids []string
	if json.Unmarshal(data, &ids) != nil {
		return
	}
	filtered := ids[:0]
	for _, v := range ids {
		if v != id {
			filtered = append(filtered, v)
		}
	}
	newData, _ := json.Marshal(filtered)
	_ = p.state.Put(context.Background(), elbNamespace, key, newData)
}

// elbXMLResponse serialises v to XML and returns an AWSResponse.
func elbXMLResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("elb xml marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}

// extractELBNameFromARN extracts the load balancer name from an ELBv2 ARN.
// ARN format: arn:aws:elasticloadbalancing:region:account:loadbalancer/type/name/suffix.
func extractELBNameFromARN(arn string) string {
	parts := strings.Split(arn, "/")
	if len(parts) >= 3 {
		return parts[len(parts)-2]
	}
	return arn
}

// extractELBSuffixFromARN extracts the last path segment (suffix) from an ELBv2 ARN.
func extractELBSuffixFromARN(arn string) string {
	parts := strings.Split(arn, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return arn
}
