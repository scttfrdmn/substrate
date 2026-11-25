package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// ServiceDiscoveryPlugin emulates AWS Cloud Map, the JSON 1.1 API whose target prefix is
// Route53AutoNaming_v20170314. It records namespaces, services and instances; a DNS namespace
// creates its Route 53 hosted zone through the Route 53 plugin, and deleting the namespace deletes
// the zone. Namespace and instance changes are asynchronous operations (servicediscovery_operations.go).
type ServiceDiscoveryPlugin struct {
	state    StateManager
	logger   Logger
	tc       *TimeController
	registry *PluginRegistry

	// mu serializes every write to a namespace, service or instance record, the tagging API's too.
	mu sync.Mutex
	// seedMu serializes the operation-status countdown (see [progression.observe]).
	seedMu sync.Mutex
}

// Name returns the service name "servicediscovery".
func (p *ServiceDiscoveryPlugin) Name() string { return serviceDiscoveryNamespace }

// Initialize sets up the plugin with the provided configuration.
func (p *ServiceDiscoveryPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	if r, ok := cfg.Options["registry"].(*PluginRegistry); ok {
		p.registry = r
	}
	return nil
}

// Shutdown is a no-op.
func (p *ServiceDiscoveryPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Cloud Map request on the X-Amz-Target suffix.
func (p *ServiceDiscoveryPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := req.Operation
	if target := req.Headers["X-Amz-Target"]; target != "" {
		if dot := strings.LastIndexByte(target, '.'); dot >= 0 {
			op = target[dot+1:]
		}
	}
	switch op {
	case "CreateHttpNamespace":
		return p.createNamespace(ctx, req, sdTypeHTTP)
	case "CreatePrivateDnsNamespace":
		return p.createNamespace(ctx, req, sdTypeDNSPrivate)
	case "CreatePublicDnsNamespace":
		return p.createNamespace(ctx, req, sdTypeDNSPublic)
	case "GetNamespace":
		return p.getNamespace(ctx, req)
	case "ListNamespaces":
		return p.listNamespaces(ctx, req)
	case "DeleteNamespace":
		return p.deleteNamespace(ctx, req)
	case "UpdateHttpNamespace":
		return p.updateNamespace(ctx, req, sdTypeHTTP)
	case "UpdatePrivateDnsNamespace":
		return p.updateNamespace(ctx, req, sdTypeDNSPrivate)
	case "UpdatePublicDnsNamespace":
		return p.updateNamespace(ctx, req, sdTypeDNSPublic)
	case "CreateService":
		return p.createService(ctx, req)
	case "GetService":
		return p.getService(ctx, req)
	case "ListServices":
		return p.listServices(ctx, req)
	case "UpdateService":
		return p.updateService(ctx, req)
	case "DeleteService":
		return p.deleteService(ctx, req)
	case "RegisterInstance":
		return p.registerInstance(ctx, req)
	case "DeregisterInstance":
		return p.deregisterInstance(ctx, req)
	case "GetInstance":
		return p.getInstance(ctx, req)
	case "ListInstances":
		return p.listInstances(ctx, req)
	case "DiscoverInstances":
		return p.discoverInstances(ctx, req)
	case "GetInstancesHealthStatus":
		return p.getInstancesHealthStatus(ctx, req)
	case "UpdateInstanceCustomHealthStatus":
		return p.updateInstanceCustomHealthStatus(ctx, req)
	case "GetOperation":
		return p.getOperation(ctx, req)
	case "ListOperations":
		return p.listOperations(ctx, req)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "UntagResource":
		return p.untagResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	default:
		return nil, unknownActionError(p.Name(), op)
	}
}

// ----- State and wire helpers ----------------------------------------------------------------------

// sdDecode reads a JSON request body into dst; an empty body is the empty object.
func sdDecode(req *AWSRequest, dst any) error {
	body := req.Body
	if len(body) == 0 {
		body = []byte("{}")
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return &AWSError{
			Code: "SerializationException", Message: "The request body is not well-formed JSON.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return nil
}

// sdJSON renders v as an application/x-amz-json-1.1 response.
func sdJSON(v any) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("servicediscovery marshal response: %w", err)
	}
	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}

// get reads the record at key into dst, reporting false when there is none.
func (p *ServiceDiscoveryPlugin) get(key string, dst any) (bool, error) {
	raw, err := p.state.Get(context.Background(), serviceDiscoveryNamespace, key)
	if err != nil {
		return false, fmt.Errorf("servicediscovery get %s: %w", key, err)
	}
	if raw == nil {
		return false, nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return false, fmt.Errorf("servicediscovery decode %s: %w", key, err)
	}
	return true, nil
}

// put writes v at key.
func (p *ServiceDiscoveryPlugin) put(key string, v any) error {
	return sdPut(context.Background(), p.state, key, v)
}

// sdPut writes v at key in state.
func sdPut(ctx context.Context, state StateManager, key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("servicediscovery marshal %s: %w", key, err)
	}
	if err := state.Put(ctx, serviceDiscoveryNamespace, key, data); err != nil {
		return fmt.Errorf("servicediscovery put %s: %w", key, err)
	}
	return nil
}

// del deletes the record at key.
func (p *ServiceDiscoveryPlugin) del(key string) error {
	if err := p.state.Delete(context.Background(), serviceDiscoveryNamespace, key); err != nil {
		return fmt.Errorf("servicediscovery delete %s: %w", key, err)
	}
	return nil
}

// list calls each with every record whose key begins with prefix, in key order.
func (p *ServiceDiscoveryPlugin) list(prefix string, each func([]byte) error) error {
	return sdList(context.Background(), p.state, prefix, func(_ string, raw []byte) error { return each(raw) })
}

// sdList calls each with the key and bytes of every record in state whose key begins with prefix,
// in key order.
func sdList(ctx context.Context, state StateManager, prefix string, each func(string, []byte) error) error {
	keys, err := state.List(ctx, serviceDiscoveryNamespace, prefix)
	if err != nil {
		return fmt.Errorf("servicediscovery list %s: %w", prefix, err)
	}
	sort.Strings(keys)
	for _, k := range keys {
		raw, err := state.Get(ctx, serviceDiscoveryNamespace, k)
		if err != nil {
			return fmt.Errorf("servicediscovery get %s: %w", k, err)
		}
		if raw == nil {
			continue
		}
		if err := each(k, raw); err != nil {
			return err
		}
	}
	return nil
}

// namespaces returns every namespace of the caller's account and Region.
func (p *ServiceDiscoveryPlugin) namespaces(ctx *RequestContext) ([]sdNamespace, error) {
	var out []sdNamespace
	err := p.list(sdNamespaceKeyPrefix+sdScope(ctx), func(raw []byte) error {
		var ns sdNamespace
		if err := json.Unmarshal(raw, &ns); err != nil {
			return fmt.Errorf("servicediscovery decode namespace: %w", err)
		}
		out = append(out, ns)
		return nil
	})
	return out, err
}

// services returns every service of the caller's account and Region.
func (p *ServiceDiscoveryPlugin) services(ctx *RequestContext) ([]sdService, error) {
	var out []sdService
	err := p.list(sdServiceKeyPrefix+sdScope(ctx), func(raw []byte) error {
		var s sdService
		if err := json.Unmarshal(raw, &s); err != nil {
			return fmt.Errorf("servicediscovery decode service: %w", err)
		}
		out = append(out, s)
		return nil
	})
	return out, err
}

// loadNamespace reads the namespace ref names (an ID or ARN), answering NamespaceNotFound when
// there is none.
func (p *ServiceDiscoveryPlugin) loadNamespace(ctx *RequestContext, ref string) (*sdNamespace, error) {
	if ref == "" {
		return nil, sdInvalid("Id is required.")
	}
	id := sdResolveID(ref, "namespace")
	var ns sdNamespace
	found, err := p.get(sdNamespaceKey(ctx, id), &ns)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sdNamespaceNotFound(ref)
	}
	return &ns, nil
}

// ----- Namespaces ----------------------------------------------------------------------------------

// The NamespaceName patterns of the three create operations' Name members. The HTTP and private
// patterns are `^(?!arn:)[!-~]{1,1024}$` and `^(?!arn:)[!-~]{1,253}$`; Go's regexp has no lookahead
// and caps a repeat at 1000, so the prefix and the length are checked beside the character class.
var (
	sdPrintableNamePattern = regexp.MustCompile(`^[!-~]+$`)
	sdPublicNamePattern    = regexp.MustCompile(`^([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?$`)
)

// sdCheckNamespaceName validates a new namespace's Name against its create operation's pattern;
// the private and HTTP patterns also refuse a name beginning "arn:". The message is substrate's own.
func sdCheckNamespaceName(typ, name string) *AWSError {
	var ok bool
	switch typ {
	case sdTypeHTTP:
		ok = len(name) <= 1024 && sdPrintableNamePattern.MatchString(name) && !strings.HasPrefix(name, "arn:")
	case sdTypeDNSPrivate:
		ok = len(name) <= 253 && sdPrintableNamePattern.MatchString(name) && !strings.HasPrefix(name, "arn:")
	default:
		ok = len(name) <= 253 && sdPublicNamePattern.MatchString(name)
	}
	if !ok {
		return sdInvalid("Namespace name %q is not valid.", name)
	}
	return nil
}

// sdSOAInput is the SOA a DNS namespace's create or update carries.
type sdSOAInput struct {
	DNSProperties *struct {
		SOA *struct {
			TTL *int64 `json:"TTL"`
		} `json:"SOA"`
	} `json:"DnsProperties"`
}

// ttl returns the SOA TTL the properties name, or nil.
func (s *sdSOAInput) ttl() *int64 {
	if s == nil || s.DNSProperties == nil || s.DNSProperties.SOA == nil {
		return nil
	}
	return s.DNSProperties.SOA.TTL
}

// createNamespace handles CreateHttpNamespace, CreatePrivateDnsNamespace and
// CreatePublicDnsNamespace. A name the account already uses is NamespaceAlreadyExists, except a
// retry carrying the same CreatorRequestId, which answers the original operation. A DNS namespace
// creates its hosted zone, private and associated with Vpc for a private namespace; a zone Route 53
// refuses fails the operation with CANNOT_CREATE_HOSTED_ZONE, the ErrorCode the Terraform AWS
// provider quotes from a failed private namespace (internal/service/servicediscovery/operation.go).
func (p *ServiceDiscoveryPlugin) createNamespace(ctx *RequestContext, req *AWSRequest, typ string) (*AWSResponse, error) {
	var in struct {
		Name             string       `json:"Name"`
		CreatorRequestID string       `json:"CreatorRequestId"`
		Description      string       `json:"Description"`
		Vpc              string       `json:"Vpc"`
		Tags             []taggingTag `json:"Tags"`
		Properties       *sdSOAInput  `json:"Properties"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if in.Name == "" {
		return nil, sdInvalid("Name is required.")
	}
	if awsErr := sdCheckNamespaceName(typ, in.Name); awsErr != nil {
		return nil, awsErr
	}
	if typ == sdTypeDNSPrivate && in.Vpc == "" {
		return nil, sdInvalid("Vpc is required.")
	}
	if len(in.Description) > 1024 {
		return nil, sdInvalid("Description must have length less than or equal to 1024.")
	}
	tags, awsErr := sdTagMap(in.Tags, nil)
	if awsErr != nil {
		return nil, awsErr
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	all, err := p.namespaces(ctx)
	if err != nil {
		return nil, err
	}
	for _, ns := range all {
		// A namespace's name is unique within the account and Region, whatever its type or VPC:
		// every namespace's HttpProperties.HttpName is its name, and DiscoverInstances finds the
		// namespace by that name alone, which a second namespace of the same name would make
		// ambiguous. The reasoning is substrate's; no page states the rule.
		if ns.Name != in.Name {
			continue
		}
		if in.CreatorRequestID != "" && ns.CreatorRequestID == in.CreatorRequestID && ns.CreateOperationID != "" {
			return sdOperationAnswer(sdOperation{ID: ns.CreateOperationID})
		}
		return nil, sdError("NamespaceAlreadyExists", http.StatusBadRequest,
			"Namespace %s already exists with ID %s.", in.Name, ns.ID)
	}

	id := "ns-" + ctx.IDs.Chars(16, sdIDAlphabet)
	op, err := p.newOperation(ctx, "CREATE_NAMESPACE", map[string]string{"NAMESPACE": id})
	if err != nil {
		return nil, err
	}
	if op.Status == sdOpSuccess {
		// The operation is stored before the namespace, so a CreatorRequestId retry never answers an
		// OperationId GetOperation cannot find, and a later failure undoes what this request made.
		if err := p.saveOperation(ctx, &op); err != nil {
			return nil, err
		}
		ns := sdNamespace{
			ID:                id,
			ARN:               fmt.Sprintf("arn:aws:servicediscovery:%s:%s:namespace/%s", ctx.Region, ctx.AccountID, id),
			Name:              in.Name,
			Type:              typ,
			Description:       in.Description,
			CreatorRequestID:  in.CreatorRequestID,
			CreateDate:        op.CreateDate,
			Vpc:               in.Vpc,
			CreateOperationID: op.ID,
			Tags:              tags,
			SOATTL:            in.Properties.ttl(),
		}
		if typ != sdTypeHTTP {
			zoneID, zoneErr := p.createHostedZone(ctx, &ns, op.ID)
			var awsErr *AWSError
			switch {
			case errors.As(zoneErr, &awsErr):
				op.fail("CANNOT_CREATE_HOSTED_ZONE", awsErr.Message)
			case zoneErr != nil:
				p.undoHostedZone(ctx, zoneID)
				p.dropOperation(ctx, op)
				return nil, zoneErr
			}
			ns.HostedZoneID = zoneID
		}
		if op.Status == sdOpSuccess {
			if err := p.put(sdNamespaceKey(ctx, id), ns); err != nil {
				p.undoHostedZone(ctx, ns.HostedZoneID)
				p.dropOperation(ctx, op)
				return nil, err
			}
			return sdOperationAnswer(op)
		}
	}
	if err := p.saveOperation(ctx, &op); err != nil {
		// A SUCCESS stored before the effect failed must not outlive this request.
		p.dropOperation(ctx, op)
		return nil, err
	}
	return sdOperationAnswer(op)
}

// undoHostedZone deletes a hosted zone this request created, when a later step of the request
// failed. It is best effort: a failure is logged, because the request is already failing with the
// error that caused the undo.
func (p *ServiceDiscoveryPlugin) undoHostedZone(ctx *RequestContext, zoneID string) {
	if err := p.deleteHostedZone(ctx, zoneID); err != nil && p.logger != nil {
		p.logger.Warn("servicediscovery: undo hosted zone", "zone", zoneID, "error", err)
	}
}

// createHostedZone creates a DNS namespace's hosted zone with Route 53's CreateHostedZone and
// returns its ID. A private namespace's zone is private and names the namespace's VPC. The caller
// reference is the operation ID, which is unique per request. Without a registry or a Route 53
// plugin no zone is created and the namespace reports none. When the zone was created but its
// answer cannot be decoded, the ID is still returned, read from the answer's text, beside the
// error, so the caller can undo it.
func (p *ServiceDiscoveryPlugin) createHostedZone(ctx *RequestContext, ns *sdNamespace, callerRef string) (string, error) {
	if p.registry == nil {
		return "", nil
	}
	if _, ok := p.registry.Plugin("route53"); !ok {
		return "", nil
	}
	var b strings.Builder
	b.WriteString(`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>`)
	if err := xml.EscapeText(&b, []byte(ns.Name)); err != nil {
		return "", fmt.Errorf("servicediscovery escape zone name: %w", err)
	}
	b.WriteString(`</Name><CallerReference>` + callerRef + `</CallerReference>`)
	if ns.Type == sdTypeDNSPrivate {
		b.WriteString(`<HostedZoneConfig><PrivateZone>true</PrivateZone></HostedZoneConfig><VPC><VPCRegion>` +
			ctx.Region + `</VPCRegion><VPCId>`)
		if err := xml.EscapeText(&b, []byte(ns.Vpc)); err != nil {
			return "", fmt.Errorf("servicediscovery escape VPC: %w", err)
		}
		b.WriteString(`</VPCId></VPC>`)
	}
	b.WriteString(`</CreateHostedZoneRequest>`)
	resp, err := p.registry.RouteRequest(ctx, &AWSRequest{
		Service: "route53", Operation: http.MethodPost, Path: "/2013-04-01/hostedzone",
		Body: []byte(b.String()), Headers: map[string]string{"Content-Type": "application/xml"}, Params: map[string]string{},
	})
	if err != nil {
		return "", err
	}
	var out struct {
		ID string `xml:"HostedZone>Id"`
	}
	if err := xml.Unmarshal(resp.Body, &out); err != nil {
		zoneID := ""
		if m := sdZoneIDPattern.FindSubmatch(resp.Body); m != nil {
			zoneID = string(m[1])
		}
		return zoneID, fmt.Errorf("servicediscovery decode CreateHostedZone: %w", err)
	}
	return strings.TrimPrefix(out.ID, "/hostedzone/"), nil
}

// sdZoneIDPattern finds a hosted zone ID in a CreateHostedZone answer that does not decode.
var sdZoneIDPattern = regexp.MustCompile(`/hostedzone/([A-Za-z0-9]+)`)

// deleteHostedZone deletes a namespace's hosted zone with Route 53's DeleteHostedZone. A zone
// already gone is not an error.
func (p *ServiceDiscoveryPlugin) deleteHostedZone(ctx *RequestContext, zoneID string) error {
	if p.registry == nil || zoneID == "" {
		return nil
	}
	if _, ok := p.registry.Plugin("route53"); !ok {
		return nil
	}
	_, err := p.registry.RouteRequest(ctx, &AWSRequest{
		Service: "route53", Operation: http.MethodDelete, Path: "/2013-04-01/hostedzone/" + zoneID,
		Headers: map[string]string{}, Params: map[string]string{},
	})
	var awsErr *AWSError
	if errors.As(err, &awsErr) && awsErr.Code == "NoSuchHostedZone" {
		return nil
	}
	return err
}

// serviceCount returns how many services the namespace holds.
func (p *ServiceDiscoveryPlugin) serviceCount(ctx *RequestContext, namespaceID string) (int, error) {
	all, err := p.services(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, s := range all {
		if s.NamespaceID == namespaceID {
			n++
		}
	}
	return n, nil
}

// getNamespace handles GetNamespace.
func (p *ServiceDiscoveryPlugin) getNamespace(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ID string `json:"Id"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	ns, err := p.loadNamespace(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	count, err := p.serviceCount(ctx, ns.ID)
	if err != nil {
		return nil, err
	}
	return sdJSON(struct {
		Namespace sdNamespaceOut `json:"Namespace"`
	}{sdNamespaceToWire(ctx, ns, count, true)})
}

// listNamespaces handles ListNamespaces, filtered on TYPE, NAME, HTTP_NAME and RESOURCE_OWNER as the
// NamespaceFilter shape lists, in name order, up to 100 to a page.
func (p *ServiceDiscoveryPlugin) listNamespaces(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		NextToken  string     `json:"NextToken"`
		MaxResults *int32     `json:"MaxResults"`
		Filters    []sdFilter `json:"Filters"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if awsErr := sdCheckFilters(in.Filters, map[string][]string{
		"TYPE": {"EQ", "IN", "BEGINS_WITH"}, "NAME": {"EQ", "BEGINS_WITH"}, "HTTP_NAME": {"EQ", "BEGINS_WITH"},
		"RESOURCE_OWNER": {"EQ"},
	}); awsErr != nil {
		return nil, awsErr
	}
	size, awsErr := sdPageSize(in.MaxResults, 100, 100)
	if awsErr != nil {
		return nil, awsErr
	}
	preq := pageRequest{Op: "ListNamespaces", Token: in.NextToken, Size: size, Filter: sdFilterValues(in.Filters)}
	if err := preq.validate(); err != nil {
		return nil, sdInvalid("The NextToken is not valid for this request.")
	}
	all, err := p.namespaces(ctx)
	if err != nil {
		return nil, err
	}
	var matched []sdNamespace
	for _, ns := range all {
		ok := true
		for _, f := range in.Filters {
			switch f.Name {
			case "TYPE":
				ok = ok && f.matches(ns.Type)
			case "NAME", "HTTP_NAME":
				ok = ok && f.matches(ns.Name)
			case "RESOURCE_OWNER":
				// SELF names the caller's own namespaces, which are the only ones substrate holds.
				ok = ok && f.matches("SELF")
			}
		}
		if ok {
			matched = append(matched, ns)
		}
	}
	res, err := page(matched, func(ns sdNamespace) cursorKey { return cursorKey{Sort: ns.Name, Tie: ns.ID} }, preq)
	if err != nil {
		return nil, sdInvalid("The NextToken is not valid for this request.")
	}
	out := struct {
		Namespaces []sdNamespaceOut `json:"Namespaces"`
		NextToken  string           `json:"NextToken,omitempty"`
	}{Namespaces: make([]sdNamespaceOut, 0, len(res.Items)), NextToken: res.Next}
	for i := range res.Items {
		count, err := p.serviceCount(ctx, res.Items[i].ID)
		if err != nil {
			return nil, err
		}
		out.Namespaces = append(out.Namespaces, sdNamespaceToWire(ctx, &res.Items[i], count, false))
	}
	return sdJSON(out)
}

// deleteNamespace handles DeleteNamespace: "If the namespace still contains one or more services,
// the request fails", with ResourceInUse (409). A DNS namespace's hosted zone is deleted with it;
// a zone Route 53 refuses to delete fails the operation with Route 53's code and message.
func (p *ServiceDiscoveryPlugin) deleteNamespace(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var in struct {
		ID string `json:"Id"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	ns, err := p.loadNamespace(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	count, err := p.serviceCount(ctx, ns.ID)
	if err != nil {
		return nil, err
	}
	if count > 0 {
		return nil, sdError("ResourceInUse", http.StatusConflict, "Namespace %s still contains %d services.", ns.ID, count)
	}
	op, err := p.newOperation(ctx, "DELETE_NAMESPACE", map[string]string{"NAMESPACE": ns.ID})
	if err != nil {
		return nil, err
	}
	if op.Status == sdOpSuccess {
		// The operation is stored, then the namespace record removed, then the zone deleted, so a
		// store failure leaves the zone, and a zone Route 53 refuses to delete restores the record.
		if err := p.saveOperation(ctx, &op); err != nil {
			return nil, err
		}
		key := sdNamespaceKey(ctx, ns.ID)
		if err := p.del(key); err != nil {
			p.dropOperation(ctx, op)
			return nil, err
		}
		zoneErr := p.deleteHostedZone(ctx, ns.HostedZoneID)
		if zoneErr == nil {
			return sdOperationAnswer(op)
		}
		if err := p.put(key, ns); err != nil {
			p.dropOperation(ctx, op)
			return nil, fmt.Errorf("servicediscovery restore namespace %s after %v: %w", ns.ID, zoneErr, err)
		}
		var awsErr *AWSError
		if !errors.As(zoneErr, &awsErr) {
			p.dropOperation(ctx, op)
			return nil, zoneErr
		}
		op.fail(awsErr.Code, awsErr.Message)
	}
	if err := p.saveOperation(ctx, &op); err != nil {
		// A SUCCESS stored before the effect failed must not outlive this request.
		p.dropOperation(ctx, op)
		return nil, err
	}
	return sdOperationAnswer(op)
}

// updateNamespace handles UpdateHttpNamespace, UpdatePrivateDnsNamespace and
// UpdatePublicDnsNamespace: a new Description and, for a DNS namespace, a new SOA TTL. A namespace
// of another type is InvalidInput.
func (p *ServiceDiscoveryPlugin) updateNamespace(ctx *RequestContext, req *AWSRequest, typ string) (*AWSResponse, error) {
	var in struct {
		ID        string `json:"Id"`
		Namespace *struct {
			Description *string     `json:"Description"`
			Properties  *sdSOAInput `json:"Properties"`
		} `json:"Namespace"`
	}
	if err := sdDecode(req, &in); err != nil {
		return nil, err
	}
	if in.Namespace == nil {
		return nil, sdInvalid("Namespace is required.")
	}
	if typ == sdTypeHTTP && in.Namespace.Description == nil {
		return nil, sdInvalid("Namespace.Description is required.")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	ns, err := p.loadNamespace(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	if ns.Type != typ {
		return nil, sdInvalid("Namespace %s is a %s namespace.", ns.ID, ns.Type)
	}
	op, err := p.newOperation(ctx, "UPDATE_NAMESPACE", map[string]string{"NAMESPACE": ns.ID})
	if err != nil {
		return nil, err
	}
	if op.Status == sdOpSuccess {
		if in.Namespace.Description != nil {
			ns.Description = *in.Namespace.Description
		}
		if ttl := in.Namespace.Properties.ttl(); ttl != nil {
			ns.SOATTL = ttl
		}
		if err := p.put(sdNamespaceKey(ctx, ns.ID), ns); err != nil {
			return nil, err
		}
	}
	if err := p.saveOperation(ctx, &op); err != nil {
		return nil, err
	}
	return sdOperationAnswer(op)
}
