package substrate

import (
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
}

// Route53ChangeInfo describes the result of a change to a hosted zone.
type Route53ChangeInfo struct {
	// ID is the change ID (e.g., "/change/C...").
	ID string `json:"Id"`

	// Status is the change status (always "INSYNC" in the emulator).
	Status string `json:"Status"`

	// SubmittedAt is when the change was submitted.
	SubmittedAt time.Time `json:"SubmittedAt"`

	// Comment is an optional comment.
	Comment string `json:"Comment,omitempty"`
}

// generateHostedZoneID generates a random hosted zone ID suffix (12 uppercase hex chars).
func generateHostedZoneID() string {
	return strings.ToUpper(randomHex(6))
}

// generateChangeID generates a random Route 53 change ID.
func generateChangeID() string {
	return "/change/C" + strings.ToUpper(randomHex(8))
}

// parseRoute53Operation extracts the operation name and zone ID from the HTTP
// method and URL path for Route 53 REST API calls.
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
	case strings.HasPrefix(rest, "/hostedzone/"):
		zonePart := rest[len("/hostedzone/"):]
		slash := strings.IndexByte(zonePart, '/')
		if slash < 0 {
			switch method {
			case "GET":
				return "GetHostedZone", zonePart
			case "DELETE":
				return "DeleteHostedZone", zonePart
			}
			return "", ""
		}
		zoneID = zonePart[:slash]
		sub := zonePart[slash:]
		switch {
		case sub == "/rrset" && method == "POST":
			return "ChangeResourceRecordSets", zoneID
		case sub == "/rrset" && method == "GET":
			return "ListResourceRecordSets", zoneID
		}
	}
	return "", ""
}

// r53ZoneSuffix returns the ID suffix from a zone path like "Z123ABC" or
// "/hostedzone/Z123ABC".
func r53ZoneSuffix(id string) string {
	id = strings.TrimPrefix(id, "/hostedzone/")
	return id
}

// r53RRSetKey returns the state key portion for a resource record set.
func r53RRSetKey(rrType, name string) string {
	return fmt.Sprintf("%s:%s", rrType, name)
}
