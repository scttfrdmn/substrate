package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Elastic Load Balancing publishes two APIs under one endpoint, and substrate routed one of them
// (#844).
//
// # The two generations, and what told them apart before this
//
// `elasticloadbalancing.<region>.amazonaws.com` serves both the 2012-06-01 API — Classic Load
// Balancers, the one `AWS::ElasticLoadBalancing::LoadBalancer` deploys — and the 2015-12-01 API —
// Application, Network and Gateway Load Balancers. They share an endpoint, a signing name, an IAM
// action prefix and, for three operations, an action *name*: `CreateLoadBalancer`,
// `DescribeLoadBalancers` and `DeleteLoadBalancer` exist in both with different request and
// response shapes. What separates them on the wire is the Query protocol's own `Version` member,
// which every SDK sends and which substrate read nowhere: the only `Params["Version"]` read in the
// tree was a parser test.
//
// So a classic call reached the ELBv2 handler of the same name:
//
//   - `CreateLoadBalancer` answered `ValidationError`/400 on `Name is required`, because ELBv2
//     spells the member `Name` where classic spells it `LoadBalancerName`.
//   - `DeleteLoadBalancer` answered `ValidationError`/400 on a missing `LoadBalancerArn`, where the
//     classic operation takes a name, publishes **no** errors at all, and documents that deleting
//     an absent load balancer succeeds.
//   - `DescribeLoadBalancers` answered **HTTP 200 with an ELBv2 body**. Both generations wrap the
//     result in `DescribeLoadBalancersResult`, so botocore finds the wrapper it is looking for and
//     decodes an **empty list** of `LoadBalancerDescriptions` — no exception, no missing key, just
//     a consumer told it has no classic load balancers. That is the worst of the three: the other
//     two fail loudly.
//
// # Why the version and not the parameter names
//
// Discriminating on which members a request carries would work for the two writes and not for
// `DescribeLoadBalancers`, whose classic form can be sent with no members at all — and a request
// with no members is exactly the shape that decoded to the silent empty list. The `Version` is what
// AWS itself dispatches on, it is mandatory in the Query protocol, and every SDK and the AWS CLI
// send it, so it is the discriminator that cannot be ambiguous.
//
// An absent `Version` resolves to ELBv2, which keeps every request substrate already answered
// answering the same way: substrate's own tests and its recorded event logs are full of
// hand-built ELBv2 Query bodies that carry no version, and a fixture replayed after this change
// must decode as it did before it. A `Version` substrate does not recognize also resolves to
// ELBv2 rather than being refused, because AWS publishes no error code for an unrecognized
// `Version` on any ELB page and no SDK can send one.
//
// # What is routed here, and what is not
//
// Tier 1a of #844 routes the three shared action names and nothing else. An action only the classic
// API publishes — `RegisterInstancesWithLoadBalancer`, `CreateLoadBalancerListeners`,
// `ConfigureHealthCheck` — is still an unrouted action and answers as one. The three tagging
// actions are shared *names* with different shapes (classic addresses load balancers by
// `LoadBalancerNames.member.N`, ELBv2 by `ResourceArns.member.N`), and they are deliberately not
// discriminated yet: their classic forms also carry a different removal member, which is one
// decision rather than three and belongs with the rest of Tier 1b. Their different tag cap is no
// longer part of that decision — #1148 resolved it from the record's own kind, so the classic 10 is
// enforced by the create above and by the Resource Groups Tagging API already, and routing the trio
// adds doors to a rule rather than the rule itself. See [elbTagQuota].
//
// A classic load balancer created here is therefore taggable through its create and through the
// Resource Groups Tagging API, and not through classic `AddTags`. See
// [elbResolveAnyGenerationTaggedResource] for why the tagging API reaches it while ELBv2's own tag
// operations still refuse its ARN.

// elbClassicAPIVersion is the Query protocol `Version` the Classic Load Balancer API publishes,
// and the value that routes a shared action name to this file.
const elbClassicAPIVersion = "2012-06-01"

// elbClassicXMLNS is the XML namespace classic responses carry.
//
// It is the 2012-06-01 document namespace and it is spelled `http://`, which is what AWS publishes
// in every sample response on every classic page. The sibling [elbXMLNS] carries the 2015-12-01
// one and is spelled the same way; it was `https://` until #1147, and the two differ only in the
// version segment.
const elbClassicXMLNS = "http://elasticloadbalancing.amazonaws.com/doc/2012-06-01/"

// elbKindClassicLB is the internal kind constant for a Classic Load Balancer.
//
// **It is not an ARN resource type.** Both generations write `loadbalancer` as the type segment —
// the vendored Service Authorization Reference (`authzref/elasticloadbalancing.json`) publishes
// `…:loadbalancer/${LoadBalancerName}` for classic and
// `…:loadbalancer/app/${LoadBalancerName}/${LoadBalancerId}` for ELBv2 — so the two are told apart
// by the arity of what follows the type, never by the type itself. See
// [elbResourceKindFromARN] for the full argument and
// [elbAnyGenerationKindFromARN] for the classifier that admits this kind.
//
// The value is a spelling no ARN carries, so a kind that leaked into an ARN would be visible rather
// than plausible.
const elbKindClassicLB = "classic-loadbalancer"

// elbClassicLBKeyPrefix is the state-key prefix a Classic Load Balancer record lives under.
//
// A prefix of its own rather than sharing [elbLBKeyPrefix], because the two generations can hold
// the same name at once — AWS scopes a classic name and an ELBv2 name to separate namespaces, and
// `CreateLoadBalancer` in either generation publishes its duplicate-name refusal against its own
// generation only. One prefix would make an ELBv2 load balancer named `web` collide with a classic
// one, and the collision would surface as a scan reporting one record under the other's ARN.
//
// The trailing colon is load-bearing for the reason the other four prefixes document: the
// namespace also holds this generation's `classic_lb_names:` index key, and a bare `classic_lb`
// prefix would list it.
const elbClassicLBKeyPrefix = "classic_lb:"

// elbClassicLBNamesList is the index key suffix holding the classic names in one account and
// Region, used by [ELBPlugin.appendToList] and [ELBPlugin.removeFromList].
const elbClassicLBNamesList = "classic_lb_names"

// elbClassicMaxNameLength is the published maximum length of a classic load balancer name.
//
// AWS, on `CreateLoadBalancer`'s `LoadBalancerName`: "This name must be unique within your set of
// load balancers for the region, must have a maximum of 32 characters, must contain only
// alphanumeric characters or hyphens, and cannot begin or end with a hyphen." The uniqueness half of
// that sentence is [ELBPlugin.createClassicLoadBalancer]'s; the rest is here and in
// [elbClassicNamePattern].
const elbClassicMaxNameLength = 32

// elbClassicNamePattern is the rest of that sentence: alphanumeric characters or hyphens, not
// beginning or ending with a hyphen.
//
// The two ends are in the pattern rather than checked separately so the rule is stated once; the
// length is checked separately so the refusal can name the length it got.
var elbClassicNamePattern = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$`)

// elbClassicSchemes are the two values `Scheme` publishes.
//
// AWS publishes `InvalidScheme`/400 for a value outside them, which is why this is a refusal rather
// than the silent default ELBv2's handler applies to its own `Scheme`.
var elbClassicSchemes = map[string]bool{"internet-facing": true, "internal": true}

// elbClassicProtocols are the four values a classic `Listener`'s `Protocol` and `InstanceProtocol`
// publish: "HTTP, HTTPS, TCP, or SSL".
//
// A value outside them answers `UnsupportedProtocol`/400, which `CreateLoadBalancer` publishes and
// whose description — "The specified protocol or signature version is not supported." — is the
// refusal for exactly this.
var elbClassicProtocols = map[string]bool{"HTTP": true, "HTTPS": true, "TCP": true, "SSL": true}

// elbClassicMaxPort and elbClassicMinPort are the published range of a classic listener's
// `InstancePort` — "Valid Range: Minimum value of 1. Maximum value of 65535." — and, per
// [elbClassicPort], of its `LoadBalancerPort` too.
const (
	elbClassicMinPort = 1
	elbClassicMaxPort = 65535
)

// elbClassicDefaultPageSize and elbClassicMaxPageSize are `DescribeLoadBalancers`' published
// `PageSize` bounds — "The maximum number of results to return with this call (a number from 1 to
// 400). The default is 400." — so the default and the maximum are one number.
const (
	elbClassicDefaultPageSize = 400
	elbClassicMaxPageSize     = 400
)

// ELBClassicListener is one listener on a Classic Load Balancer, as `CreateLoadBalancer` accepts it
// and `DescribeLoadBalancers` reports it.
//
// The members are the published `Listener` type's five, with AWS's own required/optional split:
// `Protocol`, `LoadBalancerPort` and `InstancePort` are `Required: Yes`, `InstanceProtocol` and
// `SSLCertificateId` are not.
type ELBClassicListener struct {
	// Protocol is the load balancer transport protocol: HTTP, HTTPS, TCP or SSL.
	Protocol string `json:"Protocol"`

	// LoadBalancerPort is the port the load balancer listens on.
	LoadBalancerPort int `json:"LoadBalancerPort"`

	// InstanceProtocol is the protocol used to route traffic to the instances.
	InstanceProtocol string `json:"InstanceProtocol,omitempty"`

	// InstancePort is the port on the instance traffic is routed to.
	InstancePort int `json:"InstancePort"`

	// SSLCertificateID is the ARN of the server certificate, for an HTTPS or SSL listener.
	SSLCertificateID string `json:"SSLCertificateId,omitempty"`
}

// ELBClassicLoadBalancer is a Classic Load Balancer record.
//
// It carries what a `CreateLoadBalancer` determined and nothing else. All sixteen members of the
// published `LoadBalancerDescription` are `Required: No`, so a subset is a legal response shape —
// and every member absent here is one no operation Tier 1a routes could set:
// `BackendServerDescriptions` and `Instances` need `RegisterInstancesWithLoadBalancer`,
// `HealthCheck` needs `ConfigureHealthCheck`, `Policies` and a listener's `PolicyNames` need the
// policy operations, `SourceSecurityGroup` needs the security-group model ELB's own plugin does not
// have, and `CanonicalHostedZoneName`/`CanonicalHostedZoneNameID` name a Route 53 zone substrate
// does not mint. Inventing any of them would put a value on the wire that no request produced,
// which is the #1013 failure.
//
// `VPCId` is stored and reported only when a create names subnets, because that is the only way a
// classic load balancer is in a VPC — and substrate does not resolve a subnet to its VPC here, so
// it stays empty and the member is omitted rather than guessed.
type ELBClassicLoadBalancer struct {
	// Name is the load balancer name, which is also its identity: classic operations address a
	// load balancer by name, never by ARN.
	Name string `json:"Name"`

	// ARN is the IAM resource ARN for the load balancer. It appears in no classic response —
	// `LoadBalancerDescription` publishes no ARN member — and exists so that IAM authorization
	// and the Resource Groups Tagging API have the identifier they are keyed on.
	ARN string `json:"ARN"`

	// DNSName is the load balancer's DNS name.
	DNSName string `json:"DNSName"`

	// Scheme is "internet-facing" or "internal".
	Scheme string `json:"Scheme"`

	// Listeners are the listeners the create declared.
	Listeners []ELBClassicListener `json:"Listeners,omitempty"`

	// AvailabilityZones are the Availability Zones the create named.
	AvailabilityZones []string `json:"AvailabilityZones,omitempty"`

	// Subnets are the subnets the create named.
	Subnets []string `json:"Subnets,omitempty"`

	// SecurityGroups are the security groups the create named.
	SecurityGroups []string `json:"SecurityGroups,omitempty"`

	// VPCId is the VPC the load balancer is in, spelled as `LoadBalancerDescription` spells it —
	// `VPCId`, where ELBv2's `LoadBalancer` spells the same thing `VpcId`.
	VPCId string `json:"VPCId,omitempty"`

	// Tags holds key-value metadata.
	Tags []ELBTag `json:"Tags,omitempty"`

	// AccountID is the AWS account that owns the load balancer.
	AccountID string `json:"AccountID"`

	// Region is the AWS Region the load balancer resides in.
	Region string `json:"Region"`

	// CreatedTime is when the load balancer was created.
	CreatedTime time.Time `json:"CreatedTime"`

	// EverTagged records that this load balancer has carried a tag; see [taggingEverTagged].
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// elbClassicLoadBalancerARN returns the IAM resource ARN for a Classic Load Balancer.
//
// One segment after the type, which is the whole of what distinguishes it from an ELBv2 load
// balancer's three. The format is the vendored Service Authorization Reference's own
// `arn:${Partition}:elasticloadbalancing:${Region}:${Account}:loadbalancer/${LoadBalancerName}`.
func elbClassicLoadBalancerARN(region, accountID, name string) string {
	return fmt.Sprintf("arn:aws:elasticloadbalancing:%s:%s:loadbalancer/%s", region, accountID, name)
}

// elbClassicDNSName returns the DNS name AWS publishes for a classic load balancer.
//
// AWS's samples are `my-loadbalancer-1234567890.us-east-1.elb.amazonaws.com` for an internet-facing
// one and `internal-my-internal-loadbalancer-1234567890.us-east-1.elb.amazonaws.com` for an
// internal one — the `internal-` prefix is published, not inferred, and a consumer that asserts a
// scheme from the host name reads it.
func elbClassicDNSName(name, suffix, region, scheme string) string {
	frag := suffix
	if len(frag) > 8 {
		frag = frag[:8]
	}
	host := fmt.Sprintf("%s-%s.%s.elb.amazonaws.com", name, frag, region)
	if scheme == "internal" {
		return "internal-" + host
	}
	return host
}

// elbClassicRequest reports whether a request names the Classic Load Balancer API.
//
// The `Version` member is the Query protocol's own, so this reads what AWS dispatches on. An absent
// or unrecognized version is ELBv2; the file comment says why.
func elbClassicRequest(req *AWSRequest) bool {
	return req.Params["Version"] == elbClassicAPIVersion
}

// elbClassicValidationError returns the refusal for a classic request member that is absent or
// outside its published constraints.
//
// `ValidationError`/400 rather than an operation-specific code, and it is published rather than
// borrowed: Elastic Load Balancing's Common Errors page — which the 2012-06-01 reference links as
// its own — is the consolidated Query protocol list, and it publishes `ValidationError` at 400
// ("The input fails to satisfy the constraints specified by an AWS service."). It is also the code
// ELBv2's own handlers in this plugin already answer for a missing member, so one generation does
// not answer a different code from the other for the same class of mistake.
func elbClassicValidationError(format string, args ...any) *AWSError {
	return &AWSError{
		Code:       "ValidationError",
		Message:    fmt.Sprintf(format, args...),
		HTTPStatus: http.StatusBadRequest,
	}
}

// elbClassicCheckName validates `LoadBalancerName` against the three constraints its parameter
// description publishes.
//
// Uniqueness is the fourth and is not here: it needs state, and it has its own published code
// ([ELBPlugin.createClassicLoadBalancer] answers `DuplicateLoadBalancerName`).
func elbClassicCheckName(name string) *AWSError {
	if name == "" {
		return elbClassicValidationError("LoadBalancerName is required")
	}
	if n := len(name); n > elbClassicMaxNameLength {
		return elbClassicValidationError(
			"LoadBalancerName must have a maximum of %d characters; the supplied name has %d",
			elbClassicMaxNameLength, n)
	}
	if !elbClassicNamePattern.MatchString(name) {
		return elbClassicValidationError(
			"LoadBalancerName '%s' must contain only alphanumeric characters or hyphens, "+
				"and cannot begin or end with a hyphen", name)
	}
	return nil
}

// elbClassicParseListeners reads the `Listeners.member.N` list a classic `CreateLoadBalancer`
// carries, or the refusal for it.
//
// The walk ends at the first index carrying no member at all, which is how the Query protocol
// terminates an indexed list. An index that carries *some* member is validated rather than treated
// as the terminator, so a caller who sent `Listeners.member.1.Protocol` and forgot the ports is
// told which member is missing instead of being told there are no listeners.
func elbClassicParseListeners(params map[string]string) ([]ELBClassicListener, *AWSError) {
	var out []ELBClassicListener
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("Listeners.member.%d.", i)
		protocol := params[prefix+"Protocol"]
		lbPort := params[prefix+"LoadBalancerPort"]
		instPort := params[prefix+"InstancePort"]
		instProtocol := params[prefix+"InstanceProtocol"]
		cert := params[prefix+"SSLCertificateId"]
		if protocol == "" && lbPort == "" && instPort == "" && instProtocol == "" && cert == "" {
			break
		}

		listener, awsErr := elbClassicListenerFrom(i, protocol, instProtocol, lbPort, instPort, cert)
		if awsErr != nil {
			return nil, awsErr
		}
		out = append(out, listener)
	}
	if len(out) == 0 {
		return nil, elbClassicValidationError("Listeners.member.1 is required")
	}
	return out, nil
}

// elbClassicListenerFrom validates one listener's members and returns it.
//
// `InstanceProtocol` is optional and is *not* defaulted to `Protocol`: AWS's published rule is that
// the two must be compatible and that an absent `InstanceProtocol` takes the listener's protocol,
// but the value substrate reports is the value the request set, and reporting one the caller did not
// send would put an invented value on the wire. It is validated when present and omitted when not.
func elbClassicListenerFrom(
	index int, protocol, instanceProtocol, lbPort, instancePort, certificate string,
) (ELBClassicListener, *AWSError) {
	prefix := fmt.Sprintf("Listeners.member.%d.", index)
	if protocol == "" {
		return ELBClassicListener{}, elbClassicValidationError("%sProtocol is required", prefix)
	}
	if !elbClassicProtocols[protocol] {
		return ELBClassicListener{}, &AWSError{
			Code: "UnsupportedProtocol",
			Message: fmt.Sprintf("Protocol '%s' is not supported; the supported protocols are "+
				"HTTP, HTTPS, TCP and SSL", protocol),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if instanceProtocol != "" && !elbClassicProtocols[instanceProtocol] {
		return ELBClassicListener{}, &AWSError{
			Code: "UnsupportedProtocol",
			Message: fmt.Sprintf("InstanceProtocol '%s' is not supported; the supported protocols "+
				"are HTTP, HTTPS, TCP and SSL", instanceProtocol),
			HTTPStatus: http.StatusBadRequest,
		}
	}

	parsedLBPort, awsErr := elbClassicPort(prefix+"LoadBalancerPort", lbPort)
	if awsErr != nil {
		return ELBClassicListener{}, awsErr
	}
	parsedInstancePort, awsErr := elbClassicPort(prefix+"InstancePort", instancePort)
	if awsErr != nil {
		return ELBClassicListener{}, awsErr
	}
	return ELBClassicListener{
		Protocol:         protocol,
		LoadBalancerPort: parsedLBPort,
		InstanceProtocol: instanceProtocol,
		InstancePort:     parsedInstancePort,
		SSLCertificateID: certificate,
	}, nil
}

// elbClassicPort parses one required port member and checks it against 1–65535.
//
// The range is the one `InstancePort` publishes. `LoadBalancerPort` publishes no range of its own on
// the API page — the permitted classic values are in the user guide rather than the model — so the
// same bound is applied to both, which is substrate's reading and is the narrower-is-wrong
// direction: no value inside 1–65535 is refused here that AWS's model publishes as valid.
func elbClassicPort(member, raw string) (int, *AWSError) {
	if raw == "" {
		return 0, elbClassicValidationError("%s is required", member)
	}
	port, err := strconv.Atoi(raw)
	if err != nil {
		return 0, elbClassicValidationError("%s '%s' is not a number", member, raw)
	}
	if port < elbClassicMinPort || port > elbClassicMaxPort {
		return 0, elbClassicValidationError("%s must be between %d and %d; the supplied value is %d",
			member, elbClassicMinPort, elbClassicMaxPort, port)
	}
	return port, nil
}

// elbClassicCheckDuplicatePorts refuses two listeners in one create that claim the same
// `LoadBalancerPort`.
//
// `InvalidConfigurationRequest` is the one non-400 among `CreateLoadBalancer`'s twelve published
// errors — **HTTP 409** — and its description is "The requested configuration change is not valid."
// Two listeners on one port is the configuration this create cannot satisfy, and it is the only one
// of the twelve that fits: `DuplicateListener` is published on `CreateLoadBalancerListeners` and on
// no page this operation has, so borrowing it would be the #671 violation. **The mapping is
// substrate's reading**; the code and its 409 are the page's.
func elbClassicCheckDuplicatePorts(listeners []ELBClassicListener) *AWSError {
	seen := make(map[int]bool, len(listeners))
	for _, l := range listeners {
		if seen[l.LoadBalancerPort] {
			return &AWSError{
				Code: "InvalidConfigurationRequest",
				Message: fmt.Sprintf(
					"Two listeners cannot share LoadBalancerPort %d", l.LoadBalancerPort),
				HTTPStatus: http.StatusConflict,
			}
		}
		seen[l.LoadBalancerPort] = true
	}
	return nil
}

// elbClassicStateKey returns the state key a classic record lives under, which is its name within
// an account and Region.
//
// The name is the identity of a classic load balancer — every classic operation addresses one by
// name — so unlike a listener's or a rule's, this key is derivable from the ARN, and
// [elbResolveAnyGenerationTaggedResource] still scans rather than deriving it so that one resolver
// serves both generations.
func elbClassicStateKey(scope, name string) string {
	return elbClassicLBKeyPrefix + scope + "/" + name
}

// createClassicLoadBalancer answers the 2012-06-01 `CreateLoadBalancer`.
//
// The response is `DNSName` and nothing else, which is the whole of the operation's published
// Response Elements — where ELBv2's returns the load balancer it made. A consumer of the classic
// API has to call `DescribeLoadBalancers` to see anything else, and before this it could not.
func (p *ELBPlugin) createClassicLoadBalancer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["LoadBalancerName"]
	if awsErr := elbClassicCheckName(name); awsErr != nil {
		return nil, awsErr
	}
	listeners, awsErr := elbClassicParseListeners(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	if awsErr := elbClassicCheckDuplicatePorts(listeners); awsErr != nil {
		return nil, awsErr
	}

	scheme := req.Params["Scheme"]
	if scheme == "" {
		scheme = "internet-facing"
	}
	if !elbClassicSchemes[scheme] {
		return nil, &AWSError{
			Code:       "InvalidScheme",
			Message:    fmt.Sprintf("Scheme '%s' is not valid; the valid values are internet-facing and internal", scheme),
			HTTPStatus: http.StatusBadRequest,
		}
	}

	// The tags are validated before the record is written, for the reason ELBv2's four creates
	// document: a create carrying a tag it cannot legally apply must leave no load balancer
	// behind. The classic operation publishes `DuplicateTagKeys` exactly as the ELBv2 one does,
	// which is why it also passes true, and it accepts the same indexed `Tags.member.N` shape.
	//
	// It passes its own kind, so the tag cap counted here is the 10 the classic `AddTags` page
	// publishes rather than ELBv2's 50 (#1148).
	tags, tagErr := elbTagsForCreate(req, true, elbKindClassicLB)
	if tagErr != nil {
		return nil, tagErr
	}

	goCtx := context.Background()
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	key := elbClassicStateKey(scope, name)
	existing, err := p.state.Get(goCtx, elbNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("elb createClassicLoadBalancer state.Get: %w", err)
	}
	if existing != nil {
		// Published at 400 on this operation, and the reason the classic records live under their
		// own key prefix: an ELBv2 load balancer of the same name is a different resource and must
		// not collide with this.
		return nil, &AWSError{
			Code:       "DuplicateLoadBalancerName",
			Message:    fmt.Sprintf("Load balancer named '%s' already exists", name),
			HTTPStatus: http.StatusBadRequest,
		}
	}

	subnets := extractIndexedParams(req.Params, "Subnets.member")
	lb := ELBClassicLoadBalancer{
		Name:              name,
		ARN:               elbClassicLoadBalancerARN(reqCtx.Region, reqCtx.AccountID, name),
		DNSName:           elbClassicDNSName(name, generateELBSuffix(), reqCtx.Region, scheme),
		Scheme:            scheme,
		Listeners:         listeners,
		AvailabilityZones: extractIndexedParams(req.Params, "AvailabilityZones.member"),
		Subnets:           subnets,
		SecurityGroups:    extractIndexedParams(req.Params, "SecurityGroups.member"),
		Tags:              tags,
		AccountID:         reqCtx.AccountID,
		Region:            reqCtx.Region,
		CreatedTime:       p.tc.Now(),
	}
	data, err := json.Marshal(lb)
	if err != nil {
		return nil, fmt.Errorf("elb createClassicLoadBalancer marshal: %w", err)
	}
	if err := p.state.Put(goCtx, elbNamespace, key, data); err != nil {
		return nil, fmt.Errorf("elb createClassicLoadBalancer state.Put: %w", err)
	}
	if err := p.appendToList(scope, elbClassicLBNamesList, name); err != nil {
		return nil, err
	}

	type createResult struct {
		DNSName string `xml:"DNSName"`
	}
	return elbOKResponse(reqCtx, "CreateLoadBalancer", elbClassicXMLNS,
		createResult{DNSName: lb.DNSName})
}

// describeClassicLoadBalancers answers the 2012-06-01 `DescribeLoadBalancers`.
//
// This is the operation whose ELBv2 answer was a silent empty list, so the shape is the point: the
// result member is `LoadBalancerDescriptions`, not ELBv2's `LoadBalancers`, and each member is a
// `LoadBalancerDescription` rather than a `LoadBalancer`.
func (p *ELBPlugin) describeClassicLoadBalancers(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	pageSize, awsErr := elbClassicPageSize(req.Params["PageSize"])
	if awsErr != nil {
		return nil, awsErr
	}
	offset, ok := decodeOffsetPaginationToken(req.Params["Marker"])
	if !ok {
		// No token code is published on this operation — its Errors section publishes
		// `DependencyThrottle` and `LoadBalancerNotFound` and nothing else — so the code comes from
		// the Common Errors page that covers it. **Refusing rather than clamping is substrate's
		// reading**: an unissued marker silently restarting the listing at page one is a wrong
		// answer a consumer's paging loop cannot see, which is the defect #915 records for the three
		// operations this decoder was hoisted out of.
		return nil, elbClassicValidationError("Marker '%s' is not a marker this service issued",
			req.Params["Marker"])
	}

	names := extractIndexedParams(req.Params, "LoadBalancerNames.member")
	all, err := p.loadClassicLoadBalancers(reqCtx)
	if err != nil {
		return nil, err
	}

	// A name naming nothing is refused rather than skipped, because this operation publishes
	// `LoadBalancerNotFound` for it — unlike ELBv2's `DescribeLoadBalancers`, which publishes the
	// code for its own `Names` and whose handler here still filters silently.
	byName := make(map[string]ELBClassicLoadBalancer, len(all))
	for _, lb := range all {
		byName[lb.Name] = lb
	}
	selected := all
	if len(names) > 0 {
		selected = make([]ELBClassicLoadBalancer, 0, len(names))
		for _, name := range names {
			lb, found := byName[name]
			if !found {
				return nil, &AWSError{
					Code:       "LoadBalancerNotFound",
					Message:    fmt.Sprintf("There is no ACTIVE Load Balancer named '%s'", name),
					HTTPStatus: http.StatusBadRequest,
				}
			}
			selected = append(selected, lb)
		}
	}

	page, next := pageByOffsetToken(selected, offset, pageSize)
	items := make([]elbClassicLBItem, 0, len(page))
	for _, lb := range page {
		items = append(items, classicLBToItem(lb))
	}

	type describeResult struct {
		LoadBalancerDescriptions []elbClassicLBItem `xml:"LoadBalancerDescriptions>member"`
		NextMarker               string             `xml:"NextMarker,omitempty"`
	}
	return elbOKResponse(reqCtx, "DescribeLoadBalancers", elbClassicXMLNS,
		describeResult{LoadBalancerDescriptions: items, NextMarker: next})
}

// elbClassicPageSize resolves the `PageSize` member against its published range.
//
// Absent is the published default of 400. A value outside 1–400 is refused rather than clamped:
// the range is published on the parameter ("a number from 1 to 400"), and honoring a larger one
// would let a test pass against substrate that AWS refuses.
func elbClassicPageSize(raw string) (int, *AWSError) {
	if raw == "" {
		return elbClassicDefaultPageSize, nil
	}
	size, err := strconv.Atoi(raw)
	if err != nil {
		return 0, elbClassicValidationError("PageSize '%s' is not a number", raw)
	}
	if size < 1 || size > elbClassicMaxPageSize {
		return 0, elbClassicValidationError("PageSize must be a number from 1 to %d; the supplied value is %d",
			elbClassicMaxPageSize, size)
	}
	return size, nil
}

// loadClassicLoadBalancers reads every classic record in the caller's account and Region, in the
// lexicographic key order [StateManager.List] guarantees (#865) — which is name order, and is what
// makes the offset cursor above mean the same thing on every call.
func (p *ELBPlugin) loadClassicLoadBalancers(reqCtx *RequestContext) ([]ELBClassicLoadBalancer, error) {
	goCtx := context.Background()
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	keys, err := p.state.List(goCtx, elbNamespace, elbClassicLBKeyPrefix+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("elb describeClassicLoadBalancers list: %w", err)
	}
	out := make([]ELBClassicLoadBalancer, 0, len(keys))
	for _, k := range keys {
		data, getErr := p.state.Get(goCtx, elbNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var lb ELBClassicLoadBalancer
		if json.Unmarshal(data, &lb) != nil {
			continue
		}
		out = append(out, lb)
	}
	return out, nil
}

// deleteClassicLoadBalancer answers the 2012-06-01 `DeleteLoadBalancer`.
//
// Two things the ELBv2 handler of the same name gets wrong for a classic caller. The load balancer
// is named, not ARN'd — so a missing `LoadBalancerName` is the only refusal here. And an absent
// load balancer is a **success**: AWS publishes no operation-specific errors at all for this
// operation and states it outright — "If the load balancer does not exist or has already been
// deleted, the call to DeleteLoadBalancer still succeeds." A consumer's delete-then-delete cleanup
// path depends on that, and it met `ValidationError` instead.
func (p *ELBPlugin) deleteClassicLoadBalancer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["LoadBalancerName"]
	if name == "" {
		return nil, elbClassicValidationError("LoadBalancerName is required")
	}
	goCtx := context.Background()
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	key := elbClassicStateKey(scope, name)
	data, err := p.state.Get(goCtx, elbNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("elb deleteClassicLoadBalancer state.Get: %w", err)
	}
	if data != nil {
		if err := p.state.Delete(goCtx, elbNamespace, key); err != nil {
			return nil, fmt.Errorf("elb deleteClassicLoadBalancer delete: %w", err)
		}
		p.removeFromList(scope, elbClassicLBNamesList, name)
	}
	return elbClassicEmptyOKResponse(reqCtx, "DeleteLoadBalancer")
}

// elbClassicListenerItem is the XML representation of a classic `Listener`.
type elbClassicListenerItem struct {
	Protocol         string `xml:"Protocol"`
	LoadBalancerPort int    `xml:"LoadBalancerPort"`
	InstanceProtocol string `xml:"InstanceProtocol,omitempty"`
	InstancePort     int    `xml:"InstancePort"`
	SSLCertificateID string `xml:"SSLCertificateId,omitempty"`
}

// elbClassicPolicyNames is the `PolicyNames` list on a `ListenerDescription`.
//
// It is a struct rather than a `PolicyNames>member` slice so that the element is emitted even when
// the list is empty, which is what AWS publishes: "The policies. If there are no policies enabled,
// the list is empty." A nil slice under the flattened tag would emit no element at all, and a
// decoder would report the member as absent rather than as an empty list.
type elbClassicPolicyNames struct {
	Member []string `xml:"member"`
}

// elbClassicListenerDescriptionItem is the XML representation of a `ListenerDescription`, the
// two-member wrapper classic responses carry each listener inside.
type elbClassicListenerDescriptionItem struct {
	Listener    elbClassicListenerItem `xml:"Listener"`
	PolicyNames elbClassicPolicyNames  `xml:"PolicyNames"`
}

// elbClassicLBItem is the XML representation of a `LoadBalancerDescription`.
//
// The members are those [ELBClassicLoadBalancer] holds; its doc comment says which of the published
// sixteen are absent and why each needs an operation Tier 1a does not route.
type elbClassicLBItem struct {
	LoadBalancerName     string                              `xml:"LoadBalancerName"`
	DNSName              string                              `xml:"DNSName"`
	Scheme               string                              `xml:"Scheme"`
	VPCId                string                              `xml:"VPCId,omitempty"`
	CreatedTime          string                              `xml:"CreatedTime"`
	ListenerDescriptions []elbClassicListenerDescriptionItem `xml:"ListenerDescriptions>member"`
	AvailabilityZones    []string                            `xml:"AvailabilityZones>member,omitempty"`
	Subnets              []string                            `xml:"Subnets>member,omitempty"`
	SecurityGroups       []string                            `xml:"SecurityGroups>member,omitempty"`
}

// classicLBToItem renders one classic record as its published wire shape.
//
// The record's own `ARN`, `AccountID`, `Region`, `Tags` and `EverTagged` do not appear:
// `LoadBalancerDescription` publishes none of them, and the first three are substrate's own
// bookkeeping (#756). That is also why the listener is *converted* rather than marshaled directly:
// [ELBClassicListener] and [elbClassicListenerItem] carry the same five members in the same order
// and differ only in their tags — one is what the record stores, the other is what the wire
// publishes — and keeping the two types distinct is what stops a member added to the record from
// reaching a response nobody decided to put it in. The conversion is the safe way to spell that: if
// the two ever diverge it stops compiling here, rather than quietly carrying the new member onto the
// wire the way marshaling one struct for both purposes would.
func classicLBToItem(lb ELBClassicLoadBalancer) elbClassicLBItem {
	descriptions := make([]elbClassicListenerDescriptionItem, 0, len(lb.Listeners))
	for _, l := range lb.Listeners {
		descriptions = append(descriptions, elbClassicListenerDescriptionItem{
			Listener: elbClassicListenerItem(l),
		})
	}
	return elbClassicLBItem{
		LoadBalancerName:     lb.Name,
		DNSName:              lb.DNSName,
		Scheme:               lb.Scheme,
		VPCId:                lb.VPCId,
		CreatedTime:          lb.CreatedTime.UTC().Format(time.RFC3339),
		ListenerDescriptions: descriptions,
		AvailabilityZones:    lb.AvailabilityZones,
		Subnets:              lb.Subnets,
		SecurityGroups:       lb.SecurityGroups,
	}
}

// elbClassicDecodeTaggedResource decodes a classic record for the tagging resolvers.
//
// It is [elbDecodeTaggedResource]'s fifth arm, living here rather than beside the other four so that
// every classic shape is in one file. The closure is the same shape as theirs, which is what lets
// one scanner ([TaggingPlugin.scanELBKind]) and one resolver
// ([elbResolveAnyGenerationTaggedResource]) serve both generations without either learning what a
// classic record looks like.
//
// The closure has no caller yet, and that is worth saying rather than leaving to be discovered:
// [elbTaggedResource.encode] is called only by CloudFormation's two tag writers, and
// [cfnELBStampableTypes] lists the four ELBv2 types alone, so nothing reaches it until the classic
// type has a deploy helper (Tier 2 of #844). The tagging API's writes go through
// [mergeResourceTags] on the raw JSON instead. It is written now because the alternative is a fifth
// arm shaped differently from the other four, and a nil closure there is a panic waiting for the
// first caller rather than a decision anyone made.
func elbClassicDecodeTaggedResource(stateKey string, data []byte) *elbTaggedResource {
	var lb ELBClassicLoadBalancer
	if json.Unmarshal(data, &lb) != nil {
		return nil
	}
	return &elbTaggedResource{
		arn:        lb.ARN,
		stateKey:   stateKey,
		tags:       lb.Tags,
		everTagged: lb.EverTagged,
		withTags: func(tags []ELBTag, everTagged bool) ([]byte, error) {
			lb.Tags = tags
			lb.EverTagged = everTagged
			return json.Marshal(lb)
		},
	}
}

// elbClassicNameFromARN returns the load balancer name a classic ARN names, or "" when the ARN is
// not a classic load balancer's.
//
// This is the arity rule read in the other direction from [elbClassicLoadBalancerARN]: the type
// segment is `loadbalancer` and exactly one non-empty segment follows it. It is the whole of what
// [elbAnyGenerationKindFromARN] adds to [elbResourceKindFromARN], and it sits beside the builder it
// inverts because the two must agree about what one segment means.
func elbClassicNameFromARN(arn string) string {
	_, resource, ok := elbARNResource(arn)
	if !ok {
		return ""
	}
	resourceType, rest, ok := strings.Cut(resource, "/")
	if !ok || resourceType != elbKindLoadBalancer || strings.Contains(rest, "/") || rest == "" {
		return ""
	}
	return rest
}
