package emulator

// The Cloud Map responses are rendered from these types, not from the persisted records, so a
// record's bookkeeping (EverTagged), its tags, and the members substrate keeps for itself (a
// namespace's VPC and create operation, a service's revision) reach no response that does not
// publish them. Member names follow the model's Namespace, NamespaceSummary, Service,
// ServiceSummary, Instance, HttpInstanceSummary, Operation and OperationSummary shapes. Timestamps are
// EpochSeconds, because the API speaks application/x-amz-json-1.1.

// sdSOAOut is the SOA structure.
type sdSOAOut struct {
	TTL int64 `json:"TTL"`
}

// sdDNSPropertiesOut is the DnsProperties structure.
type sdDNSPropertiesOut struct {
	HostedZoneID string    `json:"HostedZoneId,omitempty"`
	SOA          *sdSOAOut `json:"SOA,omitempty"`
}

// sdHTTPPropertiesOut is the HttpProperties structure.
type sdHTTPPropertiesOut struct {
	HTTPName string `json:"HttpName"`
}

// sdNamespacePropertiesOut is the NamespaceProperties structure.
type sdNamespacePropertiesOut struct {
	DNSProperties  *sdDNSPropertiesOut  `json:"DnsProperties,omitempty"`
	HTTPProperties *sdHTTPPropertiesOut `json:"HttpProperties"`
}

// sdNamespaceOut is the Namespace structure, and without CreatorRequestId the NamespaceSummary.
type sdNamespaceOut struct {
	ID               string                   `json:"Id"`
	ARN              string                   `json:"Arn"`
	ResourceOwner    string                   `json:"ResourceOwner"`
	Name             string                   `json:"Name"`
	Type             string                   `json:"Type"`
	Description      string                   `json:"Description,omitempty"`
	ServiceCount     int                      `json:"ServiceCount"`
	Properties       sdNamespacePropertiesOut `json:"Properties"`
	CreateDate       EpochSeconds             `json:"CreateDate"`
	CreatorRequestID string                   `json:"CreatorRequestId,omitempty"`
}

// sdNamespaceToWire projects a namespace onto the Namespace structure, or with full false onto the
// NamespaceSummary, which has no CreatorRequestId. Every namespace reports HttpProperties.HttpName,
// its name, which is what DiscoverInstances' NamespaceName matches; a DNS namespace also reports its
// hosted zone and, when one was set, its SOA TTL.
func sdNamespaceToWire(ctx *RequestContext, ns *sdNamespace, serviceCount int, full bool) sdNamespaceOut {
	out := sdNamespaceOut{
		ID: ns.ID, ARN: ns.ARN, ResourceOwner: ctx.AccountID, Name: ns.Name, Type: ns.Type,
		Description: ns.Description, ServiceCount: serviceCount,
		Properties: sdNamespacePropertiesOut{HTTPProperties: &sdHTTPPropertiesOut{HTTPName: ns.Name}},
		CreateDate: EpochSeconds(ns.CreateDate),
	}
	if ns.isDNS() {
		dns := &sdDNSPropertiesOut{HostedZoneID: ns.HostedZoneID}
		if ns.SOATTL != nil {
			dns.SOA = &sdSOAOut{TTL: *ns.SOATTL}
		}
		out.Properties.DNSProperties = dns
	}
	if full {
		out.CreatorRequestID = ns.CreatorRequestID
	}
	return out
}

// sdServiceOut is the Service structure; ListServices clears the members ServiceSummary lacks.
type sdServiceOut struct {
	ID                      string                     `json:"Id"`
	ARN                     string                     `json:"Arn"`
	ResourceOwner           string                     `json:"ResourceOwner"`
	Name                    string                     `json:"Name"`
	NamespaceID             string                     `json:"NamespaceId,omitempty"`
	Description             string                     `json:"Description,omitempty"`
	InstanceCount           int                        `json:"InstanceCount"`
	DNSConfig               *sdDNSConfig               `json:"DnsConfig,omitempty"`
	Type                    string                     `json:"Type"`
	HealthCheckConfig       *sdHealthCheckConfig       `json:"HealthCheckConfig,omitempty"`
	HealthCheckCustomConfig *sdHealthCheckCustomConfig `json:"HealthCheckCustomConfig,omitempty"`
	CreateDate              EpochSeconds               `json:"CreateDate"`
	CreatorRequestID        string                     `json:"CreatorRequestId,omitempty"`
	CreatedByAccount        string                     `json:"CreatedByAccount"`
}

// sdServiceToWire projects a service onto the Service structure.
func sdServiceToWire(ctx *RequestContext, s *sdService, instanceCount int) sdServiceOut {
	return sdServiceOut{
		ID: s.ID, ARN: s.ARN, ResourceOwner: ctx.AccountID, Name: s.Name, NamespaceID: s.NamespaceID,
		Description: s.Description, InstanceCount: instanceCount, DNSConfig: s.DNSConfig, Type: s.Type,
		HealthCheckConfig: s.HealthCheckConfig, HealthCheckCustomConfig: s.HealthCheckCustomConfig,
		CreateDate: EpochSeconds(s.CreateDate), CreatorRequestID: s.CreatorRequestID, CreatedByAccount: ctx.AccountID,
	}
}

// sdInstanceOut is the Instance structure, and without CreatorRequestId the InstanceSummary.
type sdInstanceOut struct {
	ID               string            `json:"Id"`
	CreatorRequestID string            `json:"CreatorRequestId,omitempty"`
	Attributes       map[string]string `json:"Attributes"`
	CreatedByAccount string            `json:"CreatedByAccount"`
}

// sdHTTPInstanceOut is the HttpInstanceSummary structure.
type sdHTTPInstanceOut struct {
	InstanceID    string            `json:"InstanceId"`
	NamespaceName string            `json:"NamespaceName"`
	ServiceName   string            `json:"ServiceName"`
	HealthStatus  string            `json:"HealthStatus"`
	Attributes    map[string]string `json:"Attributes"`
}

// sdOperationOut is the Operation structure.
type sdOperationOut struct {
	ID           string            `json:"Id"`
	OwnerAccount string            `json:"OwnerAccount"`
	Type         string            `json:"Type"`
	Status       string            `json:"Status"`
	ErrorMessage string            `json:"ErrorMessage,omitempty"`
	ErrorCode    string            `json:"ErrorCode,omitempty"`
	CreateDate   EpochSeconds      `json:"CreateDate"`
	UpdateDate   EpochSeconds      `json:"UpdateDate"`
	Targets      map[string]string `json:"Targets"`
}

// sdOperationSummaryOut is the OperationSummary structure.
type sdOperationSummaryOut struct {
	ID     string `json:"Id"`
	Status string `json:"Status"`
}
