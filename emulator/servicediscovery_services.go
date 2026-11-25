package emulator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
)

// ----- Services ------------------------------------------------------------------------------------

// sdServiceNamePattern is the ServiceName shape's pattern.
var sdServiceNamePattern = regexp.MustCompile(`^((?:[a-zA-Z0-9_][a-zA-Z0-9\-_]{0,61}[a-zA-Z0-9_]|[a-zA-Z0-9])(?:\.(?:[a-zA-Z0-9_][a-zA-Z0-9\-_]{0,61}[a-zA-Z0-9_]|[a-zA-Z0-9]))*|\.)$`)

// sdRecordTypes and the other enum sets below are the model's DnsRecord.Type, RoutingPolicy and
// HealthCheckConfig.Type values.
var (
	sdRecordTypes     = []string{"SRV", "A", "AAAA", "CNAME"}
	sdRoutingPolicies = []string{"MULTIVALUE", "WEIGHTED"}
	sdHealthTypes     = []string{"HTTP", "HTTPS", "TCP"}
)

// sdCheckDNSRecords validates a service's DnsRecords: each Type is a DnsRecord type and TTL is
// present, the combination is one DnsRecord.Type lists ("A", "AAAA", "A and AAAA", "SRV", "CNAME"),
// and a CNAME is WEIGHTED and has no HealthCheckConfig ("You must specify WEIGHTED for the value of
// RoutingPolicy", "You can't specify both CNAME for Type and settings for HealthCheckConfig. If you
// do, the request will fail with an InvalidInput error"). The messages are substrate's own.
func sdCheckDNSRecords(records []sdDNSRecord, routing string, health *sdHealthCheckConfig) *AWSError {
	if len(records) == 0 {
		return sdInvalid("DnsConfig.DnsRecords must contain at least one record.")
	}
	types := make([]string, 0, len(records))
	for _, r := range records {
		if !slices.Contains(sdRecordTypes, r.Type) {
			return sdInvalid("Unsupported DNS record type: %s.", r.Type)
		}
		if r.TTL == nil || *r.TTL < 0 {
			return sdInvalid("DnsRecord %s needs a TTL of 0 or more.", r.Type)
		}
		types = append(types, r.Type)
	}
	slices.Sort(types)
	combo := strings.Join(types, "+")
	if !slices.Contains([]string{"A", "AAAA", "A+AAAA", "SRV", "CNAME"}, combo) {
		return sdInvalid("Unsupported combination of DNS record types: %s.", combo)
	}
	if routing != "" && !slices.Contains(sdRoutingPolicies, routing) {
		return sdInvalid("Unsupported routing policy: %s.", routing)
	}
	if combo == "CNAME" && routing != "WEIGHTED" {
		return sdInvalid("A CNAME record needs RoutingPolicy WEIGHTED.")
	}
	if combo == "CNAME" && health != nil {
		return sdInvalid("A CNAME record cannot be combined with HealthCheckConfig.")
	}
	return nil
}

// sdCheckHealth validates the two health check configurations: at most one of them ("you can
// specify either HealthCheckCustomConfig or HealthCheckConfig but not both"), a HealthCheckType, and
// FailureThreshold 1 to 10.
func sdCheckHealth(config *sdHealthCheckConfig, custom *sdHealthCheckCustomConfig) *AWSError {
	if config != nil && custom != nil {
		return sdInvalid("Specify HealthCheckConfig or HealthCheckCustomConfig, not both.")
	}
	if config != nil {
		if !slices.Contains(sdHealthTypes, config.Type) {
			return sdInvalid("Unsupported health check type: %s.", config.Type)
		}
		if t := config.FailureThreshold; t != nil && (*t < 1 || *t > 10) {
			return sdInvalid("FailureThreshold must be between 1 and 10.")
		}
	}
	if custom != nil {
		if t := custom.FailureThreshold; t != nil && (*t < 1 || *t > 10) {
			return sdInvalid("FailureThreshold must be between 1 and 10.")
		}
	}
	return nil
}

// createService handles CreateService. The namespace is NamespaceId, or the deprecated
// DnsConfig.NamespaceId; an HTTP namespace takes no DnsConfig. The service's Type is HTTP in an
// HTTP namespace or when the caller asks for it ("The only valid value is HTTP"), and DNS_HTTP in a
// DNS namespace otherwise. A name the namespace already holds is ServiceAlreadyExists — compared
// without case in a DNS namespace, where "you can't create multiple services with names that differ
// only by case".
func (p *ServiceDiscoveryPlugin) createService(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		Name                    string                     `json:"Name"`
		NamespaceID             string                     `json:"NamespaceId"`
		CreatorRequestID        string                     `json:"CreatorRequestId"`
		Description             string                     `json:"Description"`
		DNSConfig               *sdDNSConfig               `json:"DnsConfig"`
		HealthCheckConfig       *sdHealthCheckConfig       `json:"HealthCheckConfig"`
		HealthCheckCustomConfig *sdHealthCheckCustomConfig `json:"HealthCheckCustomConfig"`
		Tags                    []taggingTag               `json:"Tags"`
		Type                    string                     `json:"Type"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if in.Name == "" {
		return nil, sdInvalid("Name is required.")
	}
	if len(in.Name) > 127 || !sdServiceNamePattern.MatchString(in.Name) {
		return nil, sdInvalid("Service name %q is not valid.", in.Name)
	}
	if in.Type != "" && in.Type != "HTTP" {
		return nil, sdInvalid("Unsupported service type: %s.", in.Type)
	}
	if len(in.Description) > 1024 {
		return nil, sdInvalid("Description must have length less than or equal to 1024.")
	}
	ref := in.NamespaceID
	if ref == "" && in.DNSConfig != nil {
		ref = in.DNSConfig.NamespaceID
	}
	if ref == "" {
		return nil, sdInvalid("NamespaceId is required.")
	}
	if awsErr := sdCheckHealth(in.HealthCheckConfig, in.HealthCheckCustomConfig); awsErr != nil {
		return nil, awsErr
	}
	tags, awsErr := sdTagMap(in.Tags, nil)
	if awsErr != nil {
		return nil, awsErr
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	ns, err := p.loadNamespace(ctx, ref)
	if err != nil {
		return nil, err
	}
	typ := "HTTP"
	if ns.isDNS() && in.Type == "" {
		typ = "DNS_HTTP"
	}
	if typ == "HTTP" && in.DNSConfig != nil {
		return nil, sdInvalid("A service in an HTTP namespace, or of type HTTP, takes no DnsConfig.")
	}
	if typ == "DNS_HTTP" {
		if in.DNSConfig == nil {
			return nil, sdInvalid("DnsConfig is required for a service in a DNS namespace.")
		}
		if awsErr := sdCheckDNSRecords(in.DNSConfig.DNSRecords, in.DNSConfig.RoutingPolicy, in.HealthCheckConfig); awsErr != nil {
			return nil, awsErr
		}
		in.DNSConfig.NamespaceID = ns.ID
		if in.DNSConfig.RoutingPolicy == "" {
			in.DNSConfig.RoutingPolicy = "MULTIVALUE"
		}
	}
	all, err := p.services(ctx)
	if err != nil {
		return nil, err
	}
	for _, s := range all {
		if s.NamespaceID == ns.ID && (s.Name == in.Name || (ns.isDNS() && strings.EqualFold(s.Name, in.Name))) {
			return nil, sdError("ServiceAlreadyExists", http.StatusBadRequest, "Service %s already exists with ID %s.", in.Name, s.ID)
		}
	}
	id := "srv-" + ctx.IDs.Chars(16, sdIDAlphabet)
	svc := sdService{
		ID:                      id,
		ARN:                     fmt.Sprintf("arn:aws:servicediscovery:%s:%s:service/%s", ctx.Region, ctx.AccountID, id),
		Name:                    in.Name,
		NamespaceID:             ns.ID,
		Description:             in.Description,
		Type:                    typ,
		DNSConfig:               in.DNSConfig,
		HealthCheckConfig:       in.HealthCheckConfig,
		HealthCheckCustomConfig: in.HealthCheckCustomConfig,
		CreateDate:              p.tc.Now().UTC(),
		CreatorRequestID:        in.CreatorRequestID,
		Tags:                    tags,
	}
	if err := p.put(sdServiceKey(ctx, id), svc); err != nil {
		return nil, err
	}
	return sdJSON(struct {
		Service sdServiceOut `json:"Service"`
	}{sdServiceToWire(ctx, &svc, 0)})
}

// loadService reads the service ref names (an ID or ARN), answering ServiceNotFound when there is
// none.
func (p *ServiceDiscoveryPlugin) loadService(ctx *RequestContext, ref string) (*sdService, error) {
	if ref == "" {
		return nil, sdInvalid("ServiceId is required.")
	}
	id := sdResolveID(ref, "service")
	var svc sdService
	found, err := p.get(sdServiceKey(ctx, id), &svc)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sdServiceNotFound(ref)
	}
	return &svc, nil
}

// instances returns a service's instances in ID order.
func (p *ServiceDiscoveryPlugin) instances(ctx *RequestContext, serviceID string) ([]sdInstance, error) {
	var out []sdInstance
	err := p.list(sdInstancePrefix(ctx, serviceID), func(raw []byte) error {
		var inst sdInstance
		if err := json.Unmarshal(raw, &inst); err != nil {
			return fmt.Errorf("servicediscovery decode instance: %w", err)
		}
		out = append(out, inst)
		return nil
	})
	return out, err
}

// getService handles GetService.
func (p *ServiceDiscoveryPlugin) getService(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ID string `json:"Id"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	svc, err := p.loadService(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	insts, err := p.instances(ctx, svc.ID)
	if err != nil {
		return nil, err
	}
	return sdJSON(struct {
		Service sdServiceOut `json:"Service"`
	}{sdServiceToWire(ctx, svc, len(insts))})
}

// listServices handles ListServices, filtered on NAMESPACE_ID and RESOURCE_OWNER as the
// ServiceFilter shape lists, in name order, up to 100 to a page.
func (p *ServiceDiscoveryPlugin) listServices(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		NextToken  string     `json:"NextToken"`
		MaxResults *int32     `json:"MaxResults"`
		Filters    []sdFilter `json:"Filters"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if awsErr := sdCheckFilters(in.Filters, map[string][]string{"NAMESPACE_ID": {"EQ", "IN"}, "RESOURCE_OWNER": {"EQ"}}); awsErr != nil {
		return nil, awsErr
	}
	size, awsErr := sdPageSize(in.MaxResults, 100, 100)
	if awsErr != nil {
		return nil, awsErr
	}
	preq := pageRequest{Op: "ListServices", Token: in.NextToken, Size: size, Filter: sdFilterValues(in.Filters)}
	if err := preq.validate(); err != nil {
		return nil, sdInvalid("The NextToken is not valid for this request.")
	}
	all, err := p.services(ctx)
	if err != nil {
		return nil, err
	}
	var matched []sdService
	for _, s := range all {
		ok := true
		for _, f := range in.Filters {
			switch f.Name {
			case "NAMESPACE_ID":
				resolved := sdFilter{Name: f.Name, Condition: f.Condition}
				for _, v := range f.Values {
					resolved.Values = append(resolved.Values, sdResolveID(v, "namespace"))
				}
				ok = ok && resolved.matches(s.NamespaceID)
			case "RESOURCE_OWNER":
				ok = ok && f.matches("SELF")
			}
		}
		if ok {
			matched = append(matched, s)
		}
	}
	res, err := page(matched, func(s sdService) cursorKey { return cursorKey{Sort: s.Name, Tie: s.ID} }, preq)
	if err != nil {
		return nil, sdInvalid("The NextToken is not valid for this request.")
	}
	out := struct {
		Services  []sdServiceOut `json:"Services"`
		NextToken string         `json:"NextToken,omitempty"`
	}{Services: make([]sdServiceOut, 0, len(res.Items)), NextToken: res.Next}
	for i := range res.Items {
		insts, err := p.instances(ctx, res.Items[i].ID)
		if err != nil {
			return nil, err
		}
		summary := sdServiceToWire(ctx, &res.Items[i], len(insts))
		summary.NamespaceID, summary.CreatorRequestID = "", ""
		out.Services = append(out.Services, summary)
	}
	return sdJSON(out)
}

// updateService handles UpdateService. Description and DnsConfig's records replace the service's
// when sent; HealthCheckConfig replaces it, and "If you omit any existing DnsRecords or
// HealthCheckConfig configurations from an UpdateService request, the configurations are deleted
// from the service" (API_UpdateService), so an absent one deletes it.
func (p *ServiceDiscoveryPlugin) updateService(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ID      string `json:"Id"`
		Service *struct {
			Description *string `json:"Description"`
			DNSConfig   *struct {
				DNSRecords []sdDNSRecord `json:"DnsRecords"`
			} `json:"DnsConfig"`
			HealthCheckConfig *sdHealthCheckConfig `json:"HealthCheckConfig"`
		} `json:"Service"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if in.Service == nil {
		return nil, sdInvalid("Service is required.")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	svc, err := p.loadService(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	if in.Service.DNSConfig != nil {
		if svc.DNSConfig == nil {
			return nil, sdInvalid("Service %s has no DnsConfig to update.", svc.ID)
		}
		if awsErr := sdCheckDNSRecords(in.Service.DNSConfig.DNSRecords, svc.DNSConfig.RoutingPolicy, in.Service.HealthCheckConfig); awsErr != nil {
			return nil, awsErr
		}
	}
	if in.Service.HealthCheckConfig != nil {
		if awsErr := sdCheckHealth(in.Service.HealthCheckConfig, svc.HealthCheckCustomConfig); awsErr != nil {
			return nil, awsErr
		}
	}
	op, err := p.newOperation(ctx, "UPDATE_SERVICE", map[string]string{"SERVICE": svc.ID})
	if err != nil {
		return nil, err
	}
	if op.Status == sdOpSuccess {
		if in.Service.Description != nil {
			svc.Description = *in.Service.Description
		}
		if in.Service.DNSConfig != nil {
			svc.DNSConfig.DNSRecords = in.Service.DNSConfig.DNSRecords
		}
		svc.HealthCheckConfig = in.Service.HealthCheckConfig
		if err := p.put(sdServiceKey(ctx, svc.ID), svc); err != nil {
			return nil, err
		}
	}
	if err := p.saveOperation(ctx, &op); err != nil {
		return nil, err
	}
	return sdOperationAnswer(op)
}

// deleteService handles DeleteService: "If the service still contains one or more registered
// instances, the request fails", with ResourceInUse (409).
func (p *ServiceDiscoveryPlugin) deleteService(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ID string `json:"Id"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	svc, err := p.loadService(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	insts, err := p.instances(ctx, svc.ID)
	if err != nil {
		return nil, err
	}
	if len(insts) > 0 {
		return nil, sdError("ResourceInUse", http.StatusConflict, "Service %s still has %d registered instances.", svc.ID, len(insts))
	}
	if err := p.del(sdServiceKey(ctx, svc.ID)); err != nil {
		return nil, err
	}
	return sdJSON(struct{}{})
}

// ----- Instances -----------------------------------------------------------------------------------

// sdInstanceIDPattern is the InstanceId shape's pattern.
var sdInstanceIDPattern = regexp.MustCompile(`^[0-9a-zA-Z_/:.@-]{1,64}$`)

// sdReservedAttributes are the attribute keys RegisterInstance's Attributes member defines. Any
// other key is a custom attribute, of which "You can add up to 30".
var sdReservedAttributes = []string{
	"AWS_ALIAS_DNS_NAME", "AWS_EC2_INSTANCE_ID", "AWS_INIT_HEALTH_STATUS", "AWS_INSTANCE_CNAME",
	"AWS_INSTANCE_IPV4", "AWS_INSTANCE_IPV6", "AWS_INSTANCE_PORT",
}

// sdCheckAttributes validates an instance's Attributes against RegisterInstance's Attributes
// documentation: at most 30 custom attributes, keys up to 255 and values up to 1,024 characters, a
// total of at most 5,000, and the attributes the service's records and health check need. The
// messages are substrate's own.
func sdCheckAttributes(attrs map[string]string, svc *sdService, ns *sdNamespace) *AWSError {
	custom, total := 0, 0
	for k, v := range attrs {
		if k == "" || len(k) > 255 {
			return sdInvalid("Attribute keys must be between 1 and 255 characters long.")
		}
		if len(v) > 1024 {
			return sdInvalid("Attribute values must be no more than 1024 characters long.")
		}
		total += len(k) + len(v)
		if !slices.Contains(sdReservedAttributes, k) {
			custom++
		}
	}
	if custom > 30 {
		return sdInvalid("An instance can have at most 30 custom attributes.")
	}
	if total > 5000 {
		return sdInvalid("The attributes' keys and values must total no more than 5000 characters.")
	}
	if s, ok := attrs["AWS_INIT_HEALTH_STATUS"]; ok && s != "HEALTHY" && s != "UNHEALTHY" {
		return sdInvalid("AWS_INIT_HEALTH_STATUS must be HEALTHY or UNHEALTHY.")
	}
	if _, ok := attrs["AWS_EC2_INSTANCE_ID"]; ok && ns != nil && ns.isDNS() {
		return sdInvalid("AWS_EC2_INSTANCE_ID is supported for HTTP namespaces only.")
	}
	_, alias := attrs["AWS_ALIAS_DNS_NAME"]
	need := func(key string) *AWSError {
		if _, ok := attrs[key]; !ok {
			return sdInvalid("Service %s needs the %s attribute.", svc.ID, key)
		}
		return nil
	}
	if svc.DNSConfig != nil && !alias {
		for _, r := range svc.DNSConfig.DNSRecords {
			var awsErr *AWSError
			switch r.Type {
			case "A":
				awsErr = need("AWS_INSTANCE_IPV4")
			case "AAAA":
				awsErr = need("AWS_INSTANCE_IPV6")
			case "CNAME":
				awsErr = need("AWS_INSTANCE_CNAME")
			case "SRV":
				awsErr = need("AWS_INSTANCE_PORT")
				if _, v4 := attrs["AWS_INSTANCE_IPV4"]; awsErr == nil && !v4 {
					awsErr = need("AWS_INSTANCE_IPV6")
				}
			}
			if awsErr != nil {
				return awsErr
			}
		}
	}
	if svc.HealthCheckConfig != nil {
		return need("AWS_INSTANCE_PORT")
	}
	return nil
}

// registerInstance handles RegisterInstance, which creates the instance or updates the one the
// service already holds under the same ID. A service with a custom health check starts the
// instance HEALTHY unless AWS_INIT_HEALTH_STATUS says otherwise ("If you don't specify a value for
// AWS_INIT_HEALTH_STATUS, the initial status is HEALTHY"). The DNS records a DNS service's instance
// would get in the namespace's hosted zone are not created (#1732).
func (p *ServiceDiscoveryPlugin) registerInstance(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ServiceID        string            `json:"ServiceId"`
		InstanceID       string            `json:"InstanceId"`
		CreatorRequestID string            `json:"CreatorRequestId"`
		Attributes       map[string]string `json:"Attributes"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if !sdInstanceIDPattern.MatchString(in.InstanceID) {
		return nil, sdInvalid("InstanceId %q is not valid.", in.InstanceID)
	}
	if in.Attributes == nil {
		return nil, sdInvalid("Attributes is required.")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	svc, err := p.loadService(ctx, in.ServiceID)
	if err != nil {
		return nil, err
	}
	var ns sdNamespace
	nsFound, err := p.get(sdNamespaceKey(ctx, svc.NamespaceID), &ns)
	if err != nil {
		return nil, err
	}
	nsRef := &ns
	if !nsFound {
		nsRef = nil
	}
	if awsErr := sdCheckAttributes(in.Attributes, svc, nsRef); awsErr != nil {
		return nil, awsErr
	}
	op, err := p.newOperation(ctx, "REGISTER_INSTANCE", map[string]string{"INSTANCE": in.InstanceID, "SERVICE": svc.ID})
	if err != nil {
		return nil, err
	}
	if op.Status == sdOpSuccess {
		inst := sdInstance{ID: in.InstanceID, ServiceID: svc.ID, CreatorRequestID: in.CreatorRequestID, Attributes: in.Attributes}
		if svc.HealthCheckCustomConfig != nil {
			inst.CustomHealth = "HEALTHY"
			if s := in.Attributes["AWS_INIT_HEALTH_STATUS"]; s != "" {
				inst.CustomHealth = s
			}
		}
		if err := p.put(sdInstanceKey(ctx, svc.ID, inst.ID), inst); err != nil {
			return nil, err
		}
		svc.Revision++
		if err := p.put(sdServiceKey(ctx, svc.ID), svc); err != nil {
			return nil, err
		}
	}
	if err := p.saveOperation(ctx, &op); err != nil {
		return nil, err
	}
	return sdOperationAnswer(op)
}

// loadInstance reads one of a service's instances, answering InstanceNotFound when there is none.
func (p *ServiceDiscoveryPlugin) loadInstance(ctx *RequestContext, serviceID, instanceID string) (*sdInstance, error) {
	var inst sdInstance
	found, err := p.get(sdInstanceKey(ctx, serviceID, instanceID), &inst)
	if err != nil {
		return nil, err
	}
	if !found || instanceID == "" {
		return nil, sdInstanceNotFound(instanceID)
	}
	return &inst, nil
}

// deregisterInstance handles DeregisterInstance.
func (p *ServiceDiscoveryPlugin) deregisterInstance(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ServiceID  string `json:"ServiceId"`
		InstanceID string `json:"InstanceId"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	svc, err := p.loadService(ctx, in.ServiceID)
	if err != nil {
		return nil, err
	}
	if _, err := p.loadInstance(ctx, svc.ID, in.InstanceID); err != nil {
		return nil, err
	}
	op, err := p.newOperation(ctx, "DEREGISTER_INSTANCE", map[string]string{"INSTANCE": in.InstanceID, "SERVICE": svc.ID})
	if err != nil {
		return nil, err
	}
	if op.Status == sdOpSuccess {
		if err := p.del(sdInstanceKey(ctx, svc.ID, in.InstanceID)); err != nil {
			return nil, err
		}
		svc.Revision++
		if err := p.put(sdServiceKey(ctx, svc.ID), svc); err != nil {
			return nil, err
		}
	}
	if err := p.saveOperation(ctx, &op); err != nil {
		return nil, err
	}
	return sdOperationAnswer(op)
}

// getInstance handles GetInstance.
func (p *ServiceDiscoveryPlugin) getInstance(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ServiceID  string `json:"ServiceId"`
		InstanceID string `json:"InstanceId"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	svc, err := p.loadService(ctx, in.ServiceID)
	if err != nil {
		return nil, err
	}
	inst, err := p.loadInstance(ctx, svc.ID, in.InstanceID)
	if err != nil {
		return nil, err
	}
	return sdJSON(struct {
		ResourceOwner string        `json:"ResourceOwner"`
		Instance      sdInstanceOut `json:"Instance"`
	}{ctx.AccountID, sdInstanceOut{ID: inst.ID, CreatorRequestID: inst.CreatorRequestID, Attributes: inst.Attributes, CreatedByAccount: ctx.AccountID}})
}

// listInstances handles ListInstances: the service's instances in ID order, up to 100 to a page.
func (p *ServiceDiscoveryPlugin) listInstances(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ServiceID  string `json:"ServiceId"`
		NextToken  string `json:"NextToken"`
		MaxResults *int32 `json:"MaxResults"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	size, awsErr := sdPageSize(in.MaxResults, 100, 100)
	if awsErr != nil {
		return nil, awsErr
	}
	svc, err := p.loadService(ctx, in.ServiceID)
	if err != nil {
		return nil, err
	}
	preq := pageRequest{Op: "ListInstances", Token: in.NextToken, Size: size, Filter: map[string][]string{"ServiceId": {svc.ID}}}
	insts, err := p.instances(ctx, svc.ID)
	if err != nil {
		return nil, err
	}
	res, err := page(insts, func(i sdInstance) cursorKey { return cursorKey{Sort: i.ID} }, preq)
	if err != nil {
		return nil, sdInvalid("The NextToken is not valid for this request.")
	}
	out := struct {
		ResourceOwner string          `json:"ResourceOwner"`
		Instances     []sdInstanceOut `json:"Instances"`
		NextToken     string          `json:"NextToken,omitempty"`
	}{ResourceOwner: ctx.AccountID, Instances: make([]sdInstanceOut, 0, len(res.Items)), NextToken: res.Next}
	for _, i := range res.Items {
		out.Instances = append(out.Instances, sdInstanceOut{ID: i.ID, Attributes: i.Attributes, CreatedByAccount: ctx.AccountID})
	}
	return sdJSON(out)
}

// sdHealthOf returns the health status an instance reports: its custom health check's status, HEALTHY
// for a Route 53 health check, which substrate does not run, and UNKNOWN for a service with neither.
func sdHealthOf(svc *sdService, inst *sdInstance) string {
	switch {
	case svc.HealthCheckCustomConfig != nil:
		return inst.CustomHealth
	case svc.HealthCheckConfig != nil:
		return "HEALTHY"
	}
	return "UNKNOWN"
}

// getInstancesHealthStatus handles GetInstancesHealthStatus: the named instances, or every instance,
// each with the status [sdHealthOf] reports. A named instance the service does not hold is
// InstanceNotFound.
func (p *ServiceDiscoveryPlugin) getInstancesHealthStatus(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ServiceID  string   `json:"ServiceId"`
		Instances  []string `json:"Instances"`
		NextToken  string   `json:"NextToken"`
		MaxResults *int32   `json:"MaxResults"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	size, awsErr := sdPageSize(in.MaxResults, 100, 100)
	if awsErr != nil {
		return nil, awsErr
	}
	svc, err := p.loadService(ctx, in.ServiceID)
	if err != nil {
		return nil, err
	}
	insts, err := p.instances(ctx, svc.ID)
	if err != nil {
		return nil, err
	}
	if len(in.Instances) > 0 {
		var named []sdInstance
		for _, id := range in.Instances {
			idx := slices.IndexFunc(insts, func(i sdInstance) bool { return i.ID == id })
			if idx < 0 {
				return nil, sdInstanceNotFound(id)
			}
			named = append(named, insts[idx])
		}
		insts = named
	}
	preq := pageRequest{Op: "GetInstancesHealthStatus", Token: in.NextToken, Size: size,
		Filter: map[string][]string{"ServiceId": {svc.ID}, "Instances": in.Instances}}
	res, err := page(insts, func(i sdInstance) cursorKey { return cursorKey{Sort: i.ID} }, preq)
	if err != nil {
		return nil, sdInvalid("The NextToken is not valid for this request.")
	}
	status := make(map[string]string, len(res.Items))
	for i := range res.Items {
		status[res.Items[i].ID] = sdHealthOf(svc, &res.Items[i])
	}
	return sdJSON(struct {
		Status    map[string]string `json:"Status"`
		NextToken string            `json:"NextToken,omitempty"`
	}{status, res.Next})
}

// updateInstanceCustomHealthStatus handles UpdateInstanceCustomHealthStatus, which changes "the
// status only for custom health checks"; a service without one is CustomHealthNotFound (404). The
// new status applies at once: Cloud Map's 30-second wait is wall-clock time, which substrate does not
// model.
func (p *ServiceDiscoveryPlugin) updateInstanceCustomHealthStatus(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ServiceID  string `json:"ServiceId"`
		InstanceID string `json:"InstanceId"`
		Status     string `json:"Status"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if in.Status != "HEALTHY" && in.Status != "UNHEALTHY" {
		return nil, sdInvalid("Status must be HEALTHY or UNHEALTHY.")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	svc, err := p.loadService(ctx, in.ServiceID)
	if err != nil {
		return nil, err
	}
	inst, err := p.loadInstance(ctx, svc.ID, in.InstanceID)
	if err != nil {
		return nil, err
	}
	if svc.HealthCheckCustomConfig == nil {
		return nil, sdError("CustomHealthNotFound", http.StatusNotFound, "Service %s has no custom health check.", svc.ID)
	}
	inst.CustomHealth = in.Status
	if err := p.put(sdInstanceKey(ctx, svc.ID, inst.ID), inst); err != nil {
		return nil, err
	}
	return sdJSON(struct{}{})
}

// discoverInstances handles DiscoverInstances, on the data-servicediscovery host. It finds the
// service by the namespace's HttpName and the service's name, keeps the instances matching every
// QueryParameters pair, narrows them by OptionalParameters when any match those too, and applies
// HealthStatus — HEALTHY when absent, because "When you configure a health check, AWS Cloud Map
// returns healthy resources by default" (the developer guide's health check page) — only to a
// service with a health check: the parameter "is ignored for services that don't have a health check
// configured". The list is in instance ID order, not randomized, so a test reads the same order on
// every run.
func (p *ServiceDiscoveryPlugin) discoverInstances(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		NamespaceName      string            `json:"NamespaceName"`
		ServiceName        string            `json:"ServiceName"`
		MaxResults         *int32            `json:"MaxResults"`
		QueryParameters    map[string]string `json:"QueryParameters"`
		OptionalParameters map[string]string `json:"OptionalParameters"`
		HealthStatus       string            `json:"HealthStatus"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if in.NamespaceName == "" || in.ServiceName == "" {
		return nil, sdInvalid("NamespaceName and ServiceName are required.")
	}
	size, awsErr := sdPageSize(in.MaxResults, 100, 1000)
	if awsErr != nil {
		return nil, awsErr
	}
	if in.HealthStatus == "" {
		in.HealthStatus = "HEALTHY"
	}
	if !slices.Contains([]string{"HEALTHY", "UNHEALTHY", "ALL", "HEALTHY_OR_ELSE_ALL"}, in.HealthStatus) {
		return nil, sdInvalid("Unsupported health status filter: %s.", in.HealthStatus)
	}
	all, err := p.namespaces(ctx)
	if err != nil {
		return nil, err
	}
	idx := slices.IndexFunc(all, func(ns sdNamespace) bool { return ns.Name == in.NamespaceName })
	if idx < 0 {
		return nil, sdNamespaceNotFound(in.NamespaceName)
	}
	ns := all[idx]
	svcs, err := p.services(ctx)
	if err != nil {
		return nil, err
	}
	sIdx := slices.IndexFunc(svcs, func(s sdService) bool { return s.NamespaceID == ns.ID && s.Name == in.ServiceName })
	if sIdx < 0 {
		return nil, sdServiceNotFound(in.ServiceName)
	}
	svc := svcs[sIdx]
	insts, err := p.instances(ctx, svc.ID)
	if err != nil {
		return nil, err
	}
	insts = sdMatching(insts, in.QueryParameters)
	if optional := sdMatching(insts, in.OptionalParameters); len(in.OptionalParameters) > 0 && len(optional) > 0 {
		insts = optional
	}
	if svc.healthChecked() && in.HealthStatus != "ALL" {
		want := in.HealthStatus
		if want == "HEALTHY_OR_ELSE_ALL" {
			want = "HEALTHY"
		}
		kept := slices.DeleteFunc(slices.Clone(insts), func(i sdInstance) bool { return sdHealthOf(&svc, &i) != want })
		if len(kept) > 0 || in.HealthStatus != "HEALTHY_OR_ELSE_ALL" {
			insts = kept
		}
	}
	if len(insts) > size {
		insts = insts[:size]
	}
	out := struct {
		Instances         []sdHTTPInstanceOut `json:"Instances"`
		InstancesRevision int64               `json:"InstancesRevision"`
	}{Instances: make([]sdHTTPInstanceOut, 0, len(insts)), InstancesRevision: svc.Revision}
	for i := range insts {
		out.Instances = append(out.Instances, sdHTTPInstanceOut{
			InstanceID: insts[i].ID, NamespaceName: ns.Name, ServiceName: svc.Name,
			HealthStatus: sdHealthOf(&svc, &insts[i]), Attributes: insts[i].Attributes,
		})
	}
	return sdJSON(out)
}

// sdMatching returns the instances carrying every pair in want.
func sdMatching(insts []sdInstance, want map[string]string) []sdInstance {
	var out []sdInstance
	for _, i := range insts {
		ok := true
		for k, v := range want {
			if i.Attributes[k] != v {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, i)
		}
	}
	return out
}
