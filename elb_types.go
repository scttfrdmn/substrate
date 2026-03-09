package substrate

import (
	"fmt"
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

	// AccountID is the AWS account that owns the listener.
	AccountID string `json:"AccountID"`

	// Region is the AWS region in which the listener resides.
	Region string `json:"Region"`

	// Suffix is the unique suffix used in the ARN.
	Suffix string `json:"Suffix"`
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

	// AccountID is the AWS account that owns the rule.
	AccountID string `json:"AccountID"`

	// Region is the AWS region in which the rule resides.
	Region string `json:"Region"`

	// Suffix is the unique suffix used in the ARN.
	Suffix string `json:"Suffix"`
}

// generateELBSuffix generates a unique 17-character ELB resource suffix.
func generateELBSuffix() string {
	return "0" + randomHex(8)
}

// elbLoadBalancerARN returns the ARN for an ELBv2 load balancer.
func elbLoadBalancerARN(region, accountID, lbType, name, suffix string) string {
	return fmt.Sprintf("arn:aws:elasticloadbalancing:%s:%s:loadbalancer/%s/%s/%s",
		region, accountID, lbType, name, suffix)
}

// elbTargetGroupARN returns the ARN for an ELBv2 target group.
func elbTargetGroupARN(region, accountID, name, suffix string) string {
	return fmt.Sprintf("arn:aws:elasticloadbalancing:%s:%s:targetgroup/%s/%s",
		region, accountID, name, suffix)
}

// elbListenerARN returns the ARN for an ELBv2 listener.
func elbListenerARN(lbARN, suffix string) string {
	return lbARN + "/listener/" + suffix
}

// elbRuleARN returns the ARN for an ELBv2 listener rule.
func elbRuleARN(listenerARN, suffix string) string {
	return listenerARN + "/rule/" + suffix
}

// elbDNSName returns the DNS name for an ELBv2 load balancer.
func elbDNSName(name, suffix, region string) string {
	frag := suffix
	if len(frag) > 8 {
		frag = frag[:8]
	}
	return fmt.Sprintf("%s-%s.%s.elb.amazonaws.com", name, frag, region)
}
