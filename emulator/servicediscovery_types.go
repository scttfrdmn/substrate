package emulator

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// serviceDiscoveryNamespace is the Cloud Map plugin's registry name and state namespace. It is the
// IAM action prefix and the ARN service segment of a namespace and a service, so the tagging API's
// owner lookup ([taggingARNOwner]) reaches the plugin by the ARN alone.
const serviceDiscoveryNamespace = "servicediscovery"

// The state key prefixes of the four Cloud Map record kinds. Every key continues with
// "{account}/{region}/".
const (
	sdNamespaceKeyPrefix = "namespace:"
	sdServiceKeyPrefix   = "service:"
	sdInstanceKeyPrefix  = "instance:"
	sdOperationKeyPrefix = "operation:"
)

// The three NamespaceType values.
const (
	sdTypeDNSPublic  = "DNS_PUBLIC"
	sdTypeDNSPrivate = "DNS_PRIVATE"
	sdTypeHTTP       = "HTTP"
)

// sdIDAlphabet is the alphabet substrate draws Cloud Map's namespace, service and operation IDs
// from: lowercase letters and digits, the characters the API reference's sample IDs use.
const sdIDAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

// sdNamespace is one namespace. It is persisted state; responses are rendered from the wire types
// in servicediscovery_wire.go, so EverTagged and Tags reach no response that does not publish them.
type sdNamespace struct {
	ID                string            `json:"Id"`
	ARN               string            `json:"Arn"`
	Name              string            `json:"Name"`
	Type              string            `json:"Type"`
	Description       string            `json:"Description,omitempty"`
	CreatorRequestID  string            `json:"CreatorRequestId,omitempty"`
	CreateDate        time.Time         `json:"CreateDate"`
	HostedZoneID      string            `json:"HostedZoneId,omitempty"`
	SOATTL            *int64            `json:"SOATTL,omitempty"`
	Vpc               string            `json:"Vpc,omitempty"`
	CreateOperationID string            `json:"CreateOperationId,omitempty"`
	Tags              map[string]string `json:"Tags,omitempty"`
	// EverTagged is the previously-tagged flag the tagging API's GetResources reads (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// isDNS reports whether the namespace is a DNS namespace, public or private.
func (n *sdNamespace) isDNS() bool { return n.Type == sdTypeDNSPublic || n.Type == sdTypeDNSPrivate }

// sdDNSRecord is the DnsRecord structure.
type sdDNSRecord struct {
	Type string `json:"Type"`
	TTL  *int64 `json:"TTL"`
}

// sdDNSConfig is the DnsConfig structure.
type sdDNSConfig struct {
	NamespaceID   string        `json:"NamespaceId,omitempty"`
	RoutingPolicy string        `json:"RoutingPolicy,omitempty"`
	DNSRecords    []sdDNSRecord `json:"DnsRecords"`
}

// sdHealthCheckConfig is the HealthCheckConfig structure, a Route 53 health check's settings.
type sdHealthCheckConfig struct {
	Type             string `json:"Type"`
	ResourcePath     string `json:"ResourcePath,omitempty"`
	FailureThreshold *int32 `json:"FailureThreshold,omitempty"`
}

// sdHealthCheckCustomConfig is the HealthCheckCustomConfig structure.
type sdHealthCheckCustomConfig struct {
	FailureThreshold *int32 `json:"FailureThreshold,omitempty"`
}

// sdService is one service. It is persisted state; see [sdNamespace].
type sdService struct {
	ID                      string                     `json:"Id"`
	ARN                     string                     `json:"Arn"`
	Name                    string                     `json:"Name"`
	NamespaceID             string                     `json:"NamespaceId"`
	Description             string                     `json:"Description,omitempty"`
	Type                    string                     `json:"Type"`
	DNSConfig               *sdDNSConfig               `json:"DnsConfig,omitempty"`
	HealthCheckConfig       *sdHealthCheckConfig       `json:"HealthCheckConfig,omitempty"`
	HealthCheckCustomConfig *sdHealthCheckCustomConfig `json:"HealthCheckCustomConfig,omitempty"`
	CreateDate              time.Time                  `json:"CreateDate"`
	CreatorRequestID        string                     `json:"CreatorRequestId,omitempty"`
	// Revision is the InstancesRevision DiscoverInstances answers: "If a new instance is registered
	// or deregistered, the InstancesRevision updates. The health status updates don't update
	// InstancesRevision" (API_DiscoverInstances).
	Revision int64             `json:"Revision"`
	Tags     map[string]string `json:"Tags,omitempty"`
	// EverTagged is the previously-tagged flag the tagging API's GetResources reads (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// healthChecked reports whether the service has a health check of either kind, which is what
// decides whether DiscoverInstances' HealthStatus filter applies.
func (s *sdService) healthChecked() bool {
	return s.HealthCheckConfig != nil || s.HealthCheckCustomConfig != nil
}

// sdInstance is one registered instance.
type sdInstance struct {
	ID               string            `json:"Id"`
	ServiceID        string            `json:"ServiceId"`
	CreatorRequestID string            `json:"CreatorRequestId,omitempty"`
	Attributes       map[string]string `json:"Attributes"`
	// CustomHealth is the status of a custom health check: the AWS_INIT_HEALTH_STATUS the instance
	// was registered with, or the last UpdateInstanceCustomHealthStatus.
	CustomHealth string `json:"CustomHealth,omitempty"`
}

// sdOperation is one asynchronous operation's record. Status is its settled status, fixed when the
// operation starts (see servicediscovery_operations.go); an observation may report a transient
// status before it.
type sdOperation struct {
	ID           string            `json:"Id"`
	Type         string            `json:"Type"`
	Status       string            `json:"Status"`
	ErrorCode    string            `json:"ErrorCode,omitempty"`
	ErrorMessage string            `json:"ErrorMessage,omitempty"`
	CreateDate   time.Time         `json:"CreateDate"`
	Targets      map[string]string `json:"Targets"`
	// Seq numbers the account and Region's operations in the order they started.
	Seq int64 `json:"Seq,omitempty"`
}

// sdScope is the "{account}/{region}/" every key of a caller's records continues with.
func sdScope(ctx *RequestContext) string { return ctx.AccountID + "/" + ctx.Region + "/" }

// sdNamespaceKey, sdServiceKey, sdInstanceKey and sdOperationKey are the four record kinds' keys.
func sdNamespaceKey(ctx *RequestContext, id string) string {
	return sdNamespaceKeyPrefix + sdScope(ctx) + id
}

// sdServiceKey is a service's state key.
func sdServiceKey(ctx *RequestContext, id string) string {
	return sdServiceKeyPrefix + sdScope(ctx) + id
}

// sdInstanceKey is an instance's state key; a service's instances share the prefix
// [sdInstancePrefix] returns.
func sdInstanceKey(ctx *RequestContext, serviceID, instanceID string) string {
	return sdInstancePrefix(ctx, serviceID) + instanceID
}

// sdInstancePrefix is the key prefix of every instance of one service.
func sdInstancePrefix(ctx *RequestContext, serviceID string) string {
	return sdInstanceKeyPrefix + sdScope(ctx) + serviceID + "/"
}

// sdOperationKey is an operation's state key.
func sdOperationKey(ctx *RequestContext, id string) string {
	return sdOperationKeyPrefix + sdScope(ctx) + id
}

// sdResolveID reduces a namespace or service reference to its ID. The Id, NamespaceId and ServiceId
// members take "the ID or Amazon Resource Name (ARN)" (API_CreateService's NamespaceId), so an ARN
// of the expected kind is reduced to the ID after its slash.
func sdResolveID(ref, kind string) string {
	if !strings.HasPrefix(ref, "arn:") {
		return ref
	}
	parts, ok := taggingSplitARN(ref)
	if !ok || parts[2] != serviceDiscoveryNamespace {
		return ref
	}
	id, found := strings.CutPrefix(parts[5], kind+"/")
	if !found {
		return ref
	}
	return id
}

// sdError builds a Cloud Map error. Every code and status is the model's: InvalidInput 400,
// NamespaceNotFound, ServiceNotFound, InstanceNotFound, OperationNotFound, CustomHealthNotFound and
// ResourceNotFoundException 404, ResourceInUse 409, NamespaceAlreadyExists, ServiceAlreadyExists and
// TooManyTagsException 400. The messages are substrate's own.
func sdError(code string, status int, format string, args ...any) *AWSError {
	return &AWSError{Code: code, Message: fmt.Sprintf(format, args...), HTTPStatus: status}
}

// sdInvalid builds an InvalidInput, "One or more specified values aren't valid" (API_GetNamespace).
func sdInvalid(format string, args ...any) *AWSError {
	return sdError("InvalidInput", http.StatusBadRequest, format, args...)
}

// sdNamespaceNotFound builds a NamespaceNotFound.
func sdNamespaceNotFound(id string) *AWSError {
	return sdError("NamespaceNotFound", http.StatusNotFound, "Namespace %s not found.", id)
}

// sdServiceNotFound builds a ServiceNotFound.
func sdServiceNotFound(id string) *AWSError {
	return sdError("ServiceNotFound", http.StatusNotFound, "Service %s not found.", id)
}

// sdInstanceNotFound builds an InstanceNotFound.
func sdInstanceNotFound(id string) *AWSError {
	return sdError("InstanceNotFound", http.StatusNotFound, "Instance %s not found.", id)
}
