package emulator

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// route53Namespace is the service name used in Route 53 state keys.
const route53Namespace = "route53"

// Route53ZoneConfig holds configuration for a Route 53 hosted zone.
type Route53ZoneConfig struct {
	// Comment is an optional comment for the hosted zone.
	Comment string `json:"Comment,omitempty"`

	// PrivateZone indicates whether this is a private hosted zone.
	PrivateZone bool `json:"PrivateZone"`
}

// Route53HostedZone represents an Amazon Route 53 hosted zone.
type Route53HostedZone struct {
	// ID is the hosted zone ID (e.g., "/hostedzone/Z...").
	ID string `json:"Id"`

	// Name is the DNS name of the hosted zone (e.g., "example.com.").
	Name string `json:"Name"`

	// Config holds zone configuration.
	Config Route53ZoneConfig `json:"Config"`

	// CallerReference is the caller-supplied unique identifier.
	CallerReference string `json:"CallerReference"`

	// ResourceRecordSetCount is the number of record sets in the zone.
	ResourceRecordSetCount int64 `json:"ResourceRecordSetCount"`

	// AccountID is the AWS account that owns the hosted zone.
	AccountID string `json:"AccountID"`

	// DefaultRecordSets records that CreateHostedZone stored the zone's apex SOA and NS (#1537). A
	// zone stored before #1537 has neither and no flag, and is treated as having no defaults; see
	// route53_defaults.go.
	DefaultRecordSets bool `json:"DefaultRecordSets,omitempty"`

	// Tags holds the zone's tags, written by ChangeTagsForResource.
	Tags map[string]string `json:"Tags,omitempty"`
}

// Route53ResourceRecord holds a single DNS resource record value.
type Route53ResourceRecord struct {
	// Value is the record value.
	Value string `json:"Value"`
}

// Route53AliasTarget holds alias target configuration for a record set.
type Route53AliasTarget struct {
	// HostedZoneID is the hosted zone ID of the alias target.
	HostedZoneID string `json:"HostedZoneId"`

	// DNSName is the DNS name of the alias target.
	DNSName string `json:"DNSName"`

	// EvaluateTargetHealth indicates whether to evaluate target health.
	EvaluateTargetHealth bool `json:"EvaluateTargetHealth"`
}

// Route53ResourceRecordSet represents a Route 53 resource record set.
type Route53ResourceRecordSet struct {
	// Name is the DNS name for the record set.
	Name string `json:"Name"`

	// Type is the DNS record type (A, AAAA, CNAME, MX, NS, SOA, TXT).
	Type string `json:"Type"`

	// TTL is the time-to-live in seconds.
	TTL int64 `json:"TTL,omitempty"`

	// ResourceRecords holds the record values.
	ResourceRecords []Route53ResourceRecord `json:"ResourceRecords,omitempty"`

	// AliasTarget holds alias configuration for alias records.
	AliasTarget *Route53AliasTarget `json:"AliasTarget,omitempty"`

	// ZoneID is the zone ID suffix this record belongs to.
	ZoneID string `json:"ZoneID"`

	// SetIdentifier tells apart the record sets of one name and type under a weighted, latency,
	// failover, geolocation or multivalue routing policy. It is part of the record set's identity
	// ([r53RRSetIdentityKey]).
	SetIdentifier string `json:"SetIdentifier,omitempty"`

	// Weight is a weighted record set's weight, 0 to 255; nil when the request sent none.
	Weight *int64 `json:"Weight,omitempty"`

	// Failover is PRIMARY or SECONDARY for a failover record set.
	Failover string `json:"Failover,omitempty"`

	// HealthCheckID is the health check the record set is associated with.
	HealthCheckID string `json:"HealthCheckId,omitempty"`

	// Region is a latency record set's Region.
	Region string `json:"Region,omitempty"`

	// RoutingRecorded marks a record set stored with its routing members. A record set stored
	// without it holds none, so its identity is its name and type alone; see
	// [r53LegacyRoutingMatch].
	RoutingRecorded bool `json:"RoutingRecorded,omitempty"`
}

// Route53ChangeInfo describes the result of a change to a hosted zone.
type Route53ChangeInfo struct {
	// ID is the change ID (e.g., "/change/C...").
	ID string `json:"Id"`

	// Status is the change's settled status, INSYNC. What an observation reports is governed by
	// the change-status seed (route53_change_progression.go).
	Status string `json:"Status"`

	// SubmittedAt is when the change was submitted.
	SubmittedAt time.Time `json:"SubmittedAt"`

	// Comment is an optional comment.
	Comment string `json:"Comment,omitempty"`
}

// generateHostedZoneID mints a hosted zone ID suffix from m (12 uppercase hex chars).
func generateHostedZoneID(m *IDMint) string {
	return strings.ToUpper(m.Hex(6))
}

// generateChangeID mints a Route 53 change ID from m.
func generateChangeID(m *IDMint) string {
	return "/change/C" + strings.ToUpper(m.Hex(8))
}

// parseRoute53Operation extracts the operation name and the resource ID its path names from the
// HTTP method and URL path for Route 53 REST API calls. The ID is the hosted zone ID, the change
// ID, the health check ID, or for the tag operations "{ResourceType}/{ResourceId}" (or
// "{ResourceType}" alone for ListTagsForResources).
func parseRoute53Operation(method, path string) (op, zoneID string) {
	const prefix = "/2013-04-01"
	if !strings.HasPrefix(path, prefix) {
		return "", ""
	}
	rest := path[len(prefix):]

	switch {
	case rest == "/hostedzone" && method == "POST":
		return "CreateHostedZone", ""
	case rest == "/hostedzone" && method == "GET":
		return "ListHostedZones", ""
	case rest == "/hostedzonesbyname" && method == "GET":
		return "ListHostedZonesByName", ""
	case strings.HasPrefix(rest, "/change/") && method == "GET":
		return "GetChange", rest[len("/change/"):]
	case rest == "/healthcheck" && method == "POST":
		return "CreateHealthCheck", ""
	case rest == "/healthcheck" && method == "GET":
		return "ListHealthChecks", ""
	case rest == "/healthcheckcount" && method == "GET":
		return "GetHealthCheckCount", ""
	case strings.HasPrefix(rest, "/healthcheck/"):
		return parseRoute53HealthCheckPath(method, rest[len("/healthcheck/"):])
	case strings.HasPrefix(rest, "/tags/"):
		return parseRoute53TagsPath(method, rest[len("/tags/"):])
	case strings.HasPrefix(rest, "/hostedzone/"):
		zonePart := rest[len("/hostedzone/"):]
		slash := strings.IndexByte(zonePart, '/')
		if slash < 0 {
			switch method {
			case "GET":
				return "GetHostedZone", zonePart
			case "DELETE":
				return "DeleteHostedZone", zonePart
			case "POST":
				return "UpdateHostedZoneComment", zonePart
			}
			return "", ""
		}
		zoneID = zonePart[:slash]
		// botocore's model publishes ChangeResourceRecordSets as .../rrset/ (trailing
		// slash) and ListResourceRecordSets as .../rrset; aws-sdk-go-v2 sends neither
		// with a slash. Both spellings name the same resource, so accept both (#1559).
		sub := strings.TrimSuffix(zonePart[slash:], "/")
		switch {
		case sub == "/rrset" && method == "POST":
			return "ChangeResourceRecordSets", zoneID
		case sub == "/rrset" && method == "GET":
			return "ListResourceRecordSets", zoneID
		}
	}
	return "", ""
}

// parseRoute53HealthCheckPath resolves the operations under /2013-04-01/healthcheck/{HealthCheckId}.
func parseRoute53HealthCheckPath(method, rest string) (op, id string) {
	if hc, ok := strings.CutSuffix(rest, "/status"); ok && method == "GET" && hc != "" && !strings.Contains(hc, "/") {
		return "GetHealthCheckStatus", hc
	}
	if rest == "" || strings.Contains(rest, "/") {
		return "", ""
	}
	switch method {
	case "GET":
		return "GetHealthCheck", rest
	case "POST":
		return "UpdateHealthCheck", rest
	case "DELETE":
		return "DeleteHealthCheck", rest
	}
	return "", ""
}

// parseRoute53TagsPath resolves the operations under /2013-04-01/tags/: POST and GET of
// {ResourceType}/{ResourceId} are ChangeTagsForResource and ListTagsForResource, and POST of
// {ResourceType} alone is ListTagsForResources.
func parseRoute53TagsPath(method, rest string) (op, id string) {
	resourceType, resourceID, hasID := strings.Cut(rest, "/")
	switch {
	case resourceType == "":
		return "", ""
	case !hasID && method == "POST":
		return "ListTagsForResources", rest
	case !hasID, resourceID == "", strings.Contains(resourceID, "/"):
		return "", ""
	case method == "POST":
		return "ChangeTagsForResource", rest
	case method == "GET":
		return "ListTagsForResource", rest
	}
	return "", ""
}

// r53ZoneSuffix returns the ID suffix from a zone path like "Z123ABC" or
// "/hostedzone/Z123ABC".
func r53ZoneSuffix(id string) string {
	id = strings.TrimPrefix(id, "/hostedzone/")
	return id
}

// r53RRSetKey returns the state key portion for a resource record set. The name is canonical
// ([r53CanonicalName]): API_CreateHostedZone says Route 53 treats a name with and without its
// trailing dot as identical, so "example.com" and "example.com." name one record set, which is what
// lets a caller's UPSERT of the apex NS replace the default one rather than sit beside it (#1537).
//
// The Type is upper-cased: the published values are upper case, and a lower-case Type is refused
// before anything is keyed ([r53InvalidType]), so only a key written before #1537 can differ.
func r53RRSetKey(rrType, name string) string {
	return fmt.Sprintf("%s:%s", strings.ToUpper(rrType), r53CanonicalName(name))
}

// r53RRSetIdentityKey returns the state key portion for the record set a name, type and
// SetIdentifier name. A record set with no SetIdentifier keeps the key [r53RRSetKey] gives it, so
// every record set stored without one is found under the key it was written with. One with a
// SetIdentifier, a weighted, latency, failover, geolocation or multivalue record set, has the
// identifier appended in hex after "|": API_ResourceRecordSet calls SetIdentifier "An identifier
// that differentiates among multiple resource record sets that have the same combination of name
// and type", so two weighted record sets of one name and type are two keys. Hex keeps the
// identifier's case, and its "|" and "." apart from the name's, through [r53EntryIdentity], which
// canonicalizes an index entry's name to compare it.
func r53RRSetIdentityKey(rrType, name, setID string) string {
	key := r53RRSetKey(rrType, name)
	if setID == "" {
		return key
	}
	return key + "|" + hex.EncodeToString([]byte(setID))
}
