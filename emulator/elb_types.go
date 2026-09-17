package emulator

import (
	"fmt"
	"strings"
	"time"
)

// elbNamespace is the service name used in ELB state keys.
const elbNamespace = "elb"

// ELBState holds the state of an Elastic Load Balancer.
type ELBState struct {
	// Code is the state code (e.g., "active", "provisioning", "failed").
	Code string `json:"Code"`
}

// ELBTag is a key-value tag attached to an ELB resource.
type ELBTag struct {
	// Key is the tag key.
	Key string `json:"Key"`
	// Value is the tag value.
	Value string `json:"Value"`
}

// ELBLoadBalancer represents an AWS Elastic Load Balancer (v2).
type ELBLoadBalancer struct {
	// Name is the load balancer name.
	Name string `json:"Name"`

	// ARN is the load balancer ARN.
	ARN string `json:"LoadBalancerArn"`

	// DNSName is the DNS name assigned to the load balancer.
	DNSName string `json:"DNSName"`

	// Type is the load balancer type: "application" or "network".
	Type string `json:"Type"`

	// Scheme is "internet-facing" or "internal".
	Scheme string `json:"Scheme"`

	// VpcID is the VPC the load balancer is deployed in.
	VpcID string `json:"VpcId"`

	// State is the current state of the load balancer.
	State ELBState `json:"State"`

	// AvailabilityZones lists the availability zones.
	AvailabilityZones []string `json:"AvailabilityZones"`

	// SecurityGroups lists the security groups associated with the load balancer.
	SecurityGroups []string `json:"SecurityGroups"`

	// Tags holds key-value metadata.
	Tags []ELBTag `json:"Tags,omitempty"`

	// AccountID is the AWS account that owns the load balancer.
	AccountID string `json:"AccountID"`

	// Region is the AWS region in which the load balancer resides.
	Region string `json:"Region"`

	// CreatedTime is when the load balancer was created.
	CreatedTime time.Time `json:"CreatedTime"`

	// Suffix is the unique suffix used in the ARN and DNS name.
	Suffix string `json:"Suffix"`

	// EverTagged records that this load balancer has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// ELBAction represents an action taken by a listener or rule.
type ELBAction struct {
	// Type is the action type: "forward", "redirect", or "fixed-response".
	Type string `json:"Type"`

	// TargetGroupArn is the ARN of the target group for forward actions.
	TargetGroupArn string `json:"TargetGroupArn,omitempty"`

	// Order is the priority order for the action.
	Order int `json:"Order,omitempty"`
}

// ELBListener represents an ELBv2 listener.
type ELBListener struct {
	// ARN is the listener ARN.
	ARN string `json:"ListenerArn"`

	// LoadBalancerARN is the ARN of the associated load balancer.
	LoadBalancerARN string `json:"LoadBalancerArn"`

	// Port is the port the listener listens on.
	Port int `json:"Port"`

	// Protocol is the listener protocol: HTTP, HTTPS, TCP, TLS, UDP.
	Protocol string `json:"Protocol"`

	// DefaultActions is the list of default actions.
	DefaultActions []ELBAction `json:"DefaultActions"`

	// Tags holds key-value metadata.
	Tags []ELBTag `json:"Tags,omitempty"`

	// AccountID is the AWS account that owns the listener.
	AccountID string `json:"AccountID"`

	// Region is the AWS region in which the listener resides.
	Region string `json:"Region"`

	// Suffix is the unique suffix used in the ARN.
	Suffix string `json:"Suffix"`

	// EverTagged records that this listener has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// ELBTarget represents a registered target in a target group.
type ELBTarget struct {
	// ID is the target ID (instance ID or IP address).
	ID string `json:"Id"`

	// Port is the port for the target. Zero means use the target group default.
	Port int `json:"Port,omitempty"`
}

// ELBTargetGroup represents an ELBv2 target group.
type ELBTargetGroup struct {
	// ARN is the target group ARN.
	ARN string `json:"TargetGroupArn"`

	// Name is the target group name.
	Name string `json:"TargetGroupName"`

	// Protocol is the routing protocol: HTTP, HTTPS, TCP, TLS, UDP.
	Protocol string `json:"Protocol"`

	// Port is the port targets receive traffic on.
	Port int `json:"Port"`

	// VpcID is the VPC ID for the target group.
	VpcID string `json:"VpcId"`

	// TargetType is "instance", "ip", or "lambda".
	TargetType string `json:"TargetType"`

	// HealthCheckPath is the path for HTTP/HTTPS health checks.
	HealthCheckPath string `json:"HealthCheckPath"`

	// HealthCheckProtocol is the protocol for health checks.
	HealthCheckProtocol string `json:"HealthCheckProtocol"`

	// HealthCheckPort is the port for health checks.
	HealthCheckPort string `json:"HealthCheckPort"`

	// Targets holds the registered targets.
	Targets []ELBTarget `json:"Targets,omitempty"`

	// Tags holds key-value metadata.
	Tags []ELBTag `json:"Tags,omitempty"`

	// AccountID is the AWS account that owns the target group.
	AccountID string `json:"AccountID"`

	// Region is the AWS region in which the target group resides.
	Region string `json:"Region"`

	// Suffix is the unique suffix used in the ARN.
	Suffix string `json:"Suffix"`

	// EverTagged records that this target group has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// ELBCondition represents a rule condition for an ELBv2 listener rule.
type ELBCondition struct {
	// Field is the condition field (e.g., "path-pattern", "host-header").
	Field string `json:"Field"`

	// Values holds the condition values.
	Values []string `json:"Values"`
}

// ELBRule represents an ELBv2 listener rule.
type ELBRule struct {
	// ARN is the rule ARN.
	ARN string `json:"RuleArn"`

	// ListenerARN is the ARN of the associated listener.
	ListenerARN string `json:"ListenerArn"`

	// Priority is "1"-"50000" or "default".
	Priority string `json:"Priority"`

	// Conditions holds the rule conditions.
	Conditions []ELBCondition `json:"Conditions"`

	// Actions holds the rule actions.
	Actions []ELBAction `json:"Actions"`

	// IsDefault indicates whether this is the default rule.
	IsDefault bool `json:"IsDefault"`

	// Tags holds key-value metadata.
	Tags []ELBTag `json:"Tags,omitempty"`

	// AccountID is the AWS account that owns the rule.
	AccountID string `json:"AccountID"`

	// Region is the AWS region in which the rule resides.
	Region string `json:"Region"`

	// Suffix is the unique suffix used in the ARN.
	Suffix string `json:"Suffix"`

	// EverTagged records that this rule has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// generateELBSuffix generates a unique 17-character ELB resource suffix.
func generateELBSuffix() string {
	return "0" + randomHex(8)
}

// elbARNSubtypes maps a `Type` a caller sends to `CreateLoadBalancer` onto the abbreviation AWS's
// ARNs carry for it.
//
// The two vocabularies are genuinely different: `LoadBalancerTypeEnum` is
// `application | network | gateway`, and the ARN says `app`, `net`, `gwy`. The vendored Service
// Reference Information snapshot names all three abbreviations as resource types of their own
// (`loadbalancer/app/`, `listener/net`, `listener-rule/app`, …), so this is read off AWS's own
// published data rather than inferred; [TestELB_AnARNCarriesAWSsTypeAbbreviation] asserts the
// join.
//
// Substrate put the long form in the ARN before #774, which no `…:loadbalancer/app/*` — the
// wildcard AWS's own examples write — could match. It has to be fixed alongside the listener
// shape rather than after it, because a listener ARN repeats its load balancer's subtype: minting
// `listener/application/…` would leave `listener/app/*` matching nothing, which is the very defect
// #774 reports.
var elbARNSubtypes = map[string]string{
	"application": "app",
	"network":     "net",
	"gateway":     "gwy",
}

// elbARNSubtype returns the ARN abbreviation for a load balancer type.
//
// A type AWS does not publish is returned unchanged, which is what substrate did for every type
// before #774. Refusing it would be the accurate answer — `CreateLoadBalancer` documents
// `ValidationError` for a `Type` outside the enum — but validating the enum is a separate question
// from the ARN's shape, and inventing a refusal here would fail requests that pass today for a
// reason unrelated to this change.
func elbARNSubtype(lbType string) string {
	if short, ok := elbARNSubtypes[lbType]; ok {
		return short
	}
	return lbType
}

// elbLoadBalancerARN returns the ARN for an ELBv2 load balancer.
func elbLoadBalancerARN(region, accountID, lbType, name, suffix string) string {
	return fmt.Sprintf("arn:aws:elasticloadbalancing:%s:%s:loadbalancer/%s/%s/%s",
		region, accountID, elbARNSubtype(lbType), name, suffix)
}

// elbTargetGroupARN returns the ARN for an ELBv2 target group.
func elbTargetGroupARN(region, accountID, name, suffix string) string {
	return fmt.Sprintf("arn:aws:elasticloadbalancing:%s:%s:targetgroup/%s/%s",
		region, accountID, name, suffix)
}

// A listener and a listener rule are **siblings** of their load balancer's ARN, not children of
// it (#774).
//
// AWS publishes the two formats machine-readably, in its Service Reference Information document
// for elasticloadbalancing — vendored at `emulator/authzref/elasticloadbalancing.json` and
// asserted against by [TestAuthzReference_PublishesTheARNFormatSubstrateBuilds] rather than quoted
// in a comment that can go stale:
//
//	listener/app       arn:${Partition}:elasticloadbalancing:${Region}:${Account}:listener/app/${LoadBalancerName}/${LoadBalancerId}/${ListenerId}
//	listener-rule/app  arn:…:listener-rule/app/${LoadBalancerName}/${LoadBalancerId}/${ListenerId}/${ListenerRuleId}
//
// So the resource type is `listener/<subtype>`, and the load balancer's name and id are *repeated*
// inside the child's own ARN rather than the child being appended to the parent's. Substrate
// nested them before this — `arn:…:loadbalancer/app/web/<lbid>/listener/<id>` and that plus
// `/rule/<id>` — which no `…:listener/*` Resource can match. Every AWS policy example writes the
// wildcard that way, so a `Deny` scoped to `…:listener-rule/*` silently failed to deny, and an
// `Allow` scoped to `…:loadbalancer/*` reached listeners it should not have.
//
// The parts are read off the parent's ARN rather than passed in separately because that is what
// the caller holds: `CreateListener` takes a `LoadBalancerArn` and `CreateRule` a `ListenerArn`,
// and neither loads the parent's record. Both builders report whether the parent parsed, so a
// malformed ARN answers `ValidationError` instead of minting a malformed child from it.

// elbARNResource splits an ELBv2 ARN into everything through the fifth colon and the resource part
// that follows it, reporting whether the ARN had the six colon-separated fields an ARN has.
//
// The prefix is returned with its trailing colon so a caller can concatenate a new resource part
// onto it, which is the whole reason this exists: a child ARN carries its parent's partition,
// region and account unchanged, and re-deriving them from a [RequestContext] would let the two
// disagree for a cross-account or cross-region ARN a caller passed in.
func elbARNResource(arn string) (prefix, resource string, ok bool) {
	const arnFields = 6
	fields := strings.SplitN(arn, ":", arnFields)
	if len(fields) < arnFields || fields[0] != "arn" || fields[arnFields-1] == "" {
		return "", "", false
	}
	resource = fields[arnFields-1]
	return arn[:len(arn)-len(resource)], resource, true
}

// elbChildARN builds a child ARN from a parent's, replacing the parent's resource type with kind
// and appending childID.
//
// wantSegments is how many `/`-separated segments the parent's resource part must carry after its
// type — 3 for `loadbalancer/app/<name>/<id>`, 4 for `listener/app/<name>/<id>/<listenerid>` — and
// is checked rather than assumed so the classic-ELB `loadbalancer/<name>` form, which AWS also
// publishes, cannot be mistaken for an ELBv2 one and yield a child of the wrong arity.
func elbChildARN(parentARN, wantType, kind, childID string, wantSegments int) (string, bool) {
	prefix, resource, ok := elbARNResource(parentARN)
	if !ok {
		return "", false
	}
	gotType, rest, ok := strings.Cut(resource, "/")
	if !ok || gotType != wantType || strings.Count(resource, "/") != wantSegments {
		return "", false
	}
	return prefix + kind + "/" + rest + "/" + childID, true
}

// elbListenerARN returns the ARN for an ELBv2 listener, from its load balancer's ARN and the
// generated listener id, and reports whether the load balancer's ARN parsed.
func elbListenerARN(lbARN, listenerID string) (string, bool) {
	return elbChildARN(lbARN, elbKindLoadBalancer, elbKindListener, listenerID, 3)
}

// elbRuleARN returns the ARN for an ELBv2 listener rule, from its listener's ARN and the generated
// rule id, and reports whether the listener's ARN parsed.
//
// A listener ARN of the pre-#774 nested shape does not parse here, which is deliberate: it would
// yield a rule ARN nested two deep that neither `…:listener-rule/*` nor anything else could match.
// The nested shape stays *resolvable* for reads and tagging ([elbResourceKindFromARN]) because
// recorded state and exported fixtures carry it; it is not a shape new ARNs are minted from.
func elbRuleARN(listenerARN, ruleID string) (string, bool) {
	return elbChildARN(listenerARN, elbKindListener, elbKindRule, ruleID, 4)
}

// elbDNSName returns the DNS name for an ELBv2 load balancer.
func elbDNSName(name, suffix, region string) string {
	frag := suffix
	if len(frag) > 8 {
		frag = frag[:8]
	}
	return fmt.Sprintf("%s-%s.%s.elb.amazonaws.com", name, frag, region)
}
