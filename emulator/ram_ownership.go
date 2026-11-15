package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"
)

// RAM's read operations consult the request, a disassociation reports what it did, and a delete is
// soft (#1171).
//
// # resourceOwner is required and read
//
// GetResourceShares, ListPrincipals and ListResources each mark `resourceOwner` Required: Yes with Valid
// Values `SELF | OTHER-ACCOUNTS`. GetResourceShares decoded it and never read it; the two list
// operations took no request at all. Now:
//
//   - An absent `resourceOwner` is [ramMissingParameter]'s ValidationError/400, the vocabulary #1169
//     settled for every RAM member marked Required: Yes.
//   - A value outside the two is InvalidParameterException/400, which all three pages publish as "The
//     operation failed because a parameter you specified isn't valid." The message is substrate's.
//   - `SELF` answers the caller's own shares, as every request did before.
//   - `OTHER-ACCOUNTS` answers an **empty list**. It asks for shares other accounts have shared with the
//     caller, and nothing in substrate shares across accounts: a share's principals are recorded but no
//     other account's view is derived from them. So the empty list is the true answer here, not a
//     placeholder, and it replaced the caller's own shares, which was the opposite of what was asked.
//
// # The published filters are applied
//
// GetResourceShares applies `name`, `resourceShareArns` and now `resourceShareStatus` (validated
// against `PENDING | ACTIVE | FAILED | DELETING | DELETED`). ListPrincipals applies `principals`,
// `resourceArn` (principals of the shares that include that resource), `resourceShareArns` and
// `resourceType`. ListResources applies `principal`, `resourceArns`, `resourceShareArns`, `resourceType`
// and `resourceRegionScope` (validated against `ALL | REGIONAL | GLOBAL`, default ALL).
//
// A resource's type is RAM's published type name, looked up from its ARN in [ramResourceTypes] —
// `arn:aws:ec2:…:transit-gateway/tgw-1` is `ec2:TransitGateway`, not a name built from the ARN's
// kebab-case segment — and compared case-insensitively, since API_Resource glosses the type as "of the
// form service-code:resource-code, and is case-insensitive". ListResources reports it as the Resource's
// `type`. An ARN whose service and resource segment the table does not name has no type: it matches no
// `resourceType` filter and is reported without one. A `resourceType` that RAM's ListResourceTypes would
// reject is not refused but simply matches nothing. A resource's scope is GLOBAL when its ARN has
// an empty Region field and REGIONAL otherwise.
//
// Since #1542 all three read `maxResults` and `nextToken` (ram_pagination.go) and GetResourceShares
// applies `tagFilters`. Accepted and not applied, recorded so they are not mistaken for applied:
// `permissionArn` and `permissionVersion` on GetResourceShares, since substrate attaches no managed
// permission to a share and so has none to filter on.
//
// # Statuses
//
// DisassociateResourceShare reports each association it removed as `DISASSOCIATED`; it reported
// `ASSOCIATED`, contradicting the operation that produced it. That is the settled state, as
// AssociateResourceShare's `ASSOCIATED` is; the transitional ASSOCIATING and DISASSOCIATING the pages
// also publish are not modeled. ListPrincipals no longer answers a `status` on a principal, because
// API_Principal publishes none. ListResources keeps `AVAILABLE`, which API_Resource publishes and which
// is the state of a resource substrate holds in a share.
//
// # A delete is soft
//
// DeleteResourceShare moved the record out of state, so a GetResourceShares after it reported the share
// absent where RAM reports it `DELETED`. The share now stays, with status DELETED and lastUpdatedTime the
// moment of deletion, for [ramDeletedShareRetention] of simulated time — the RAM User Guide's "The
// deleted resource share disappears after two hours. Until then, it remains visible in the console with
// a deleted status." — and is then neither listed nor addressable, answering UnknownResourceException as
// a share that never existed does. The window is read against the simulated clock at request time, so a
// replay sees exactly what the recording saw.
//
// Within the window a DELETED share is listed by GetResourceShares with no status filter or with
// `resourceShareStatus: DELETED`, and by no other filter value. It shares nothing: its principals and
// resources are not reported by ListPrincipals or ListResources, since "all principals that were
// associated with the resource share lose access to the shared resources". A second delete is
// InvalidStateTransitionException/400, as is an associate or a disassociate — each of those three pages
// publishes the code as "the requested operation isn't valid for the resource share in its current
// state". UpdateResourceShare does not publish that code; it publishes OperationNotPermittedException,
// "the requested operation isn't permitted", and answers that. The messages are substrate's.
//
// # Read errors
//
// The three read operations skipped a share whose record failed to load or decode, so a store fault
// shortened the answer and reported success. They now fail. An index entry whose record is absent is
// still passed over, since that is an index outliving its record rather than a fault.

// ramDeletedShareRetention is how long a deleted share stays visible, per the RAM User Guide.
const ramDeletedShareRetention = 2 * time.Hour

// ramShareStatusDeleted is the terminal ResourceShare status a delete moves a share to.
const ramShareStatusDeleted = "DELETED"

// ramShareStatuses is the Valid Values list API_GetResourceShares publishes for resourceShareStatus.
var ramShareStatuses = []string{"PENDING", "ACTIVE", "FAILED", "DELETING", ramShareStatusDeleted}

// ramRegionScopes is the Valid Values list API_ListResources publishes for resourceRegionScope.
var ramRegionScopes = []string{"ALL", "REGIONAL", "GLOBAL"}

// ramInvalidParameter is RAM's published refusal for a parameter value it does not accept.
func ramInvalidParameter(message string) *AWSError {
	return &AWSError{Code: "InvalidParameterException", Message: message, HTTPStatus: http.StatusBadRequest}
}

// ramInvalidStateTransition refuses an operation on a share whose status does not permit it.
func ramInvalidStateTransition(share *RAMResourceShare) *AWSError {
	return &AWSError{
		Code:       "InvalidStateTransitionException",
		Message:    "ResourceShare " + share.ResourceShareArn + " is " + share.Status,
		HTTPStatus: http.StatusBadRequest,
	}
}

// ramOperationNotPermitted refuses an UpdateResourceShare on a deleted share.
func ramOperationNotPermitted(share *RAMResourceShare) *AWSError {
	return &AWSError{
		Code:       "OperationNotPermittedException",
		Message:    "ResourceShare " + share.ResourceShareArn + " is " + share.Status + " and cannot be updated",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ramCheckResourceOwner reads a required resourceOwner, reporting whether it asks for the caller's own
// shares (SELF) rather than other accounts' (OTHER-ACCOUNTS).
func ramCheckResourceOwner(owner string) (self bool, err error) {
	switch owner {
	case "":
		return false, ramMissingParameter("resourceOwner is required")
	case "SELF":
		return true, nil
	case "OTHER-ACCOUNTS":
		return false, nil
	default:
		return false, ramInvalidParameter("resourceOwner must be SELF or OTHER-ACCOUNTS, was " + owner)
	}
}

// ramShareVisible reports whether a share is still observable at now: every share but a DELETED one
// whose retention window has passed.
func ramShareVisible(share *RAMResourceShare, now time.Time) bool {
	return share.Status != ramShareStatusDeleted || now.Before(share.LastUpdatedTime.Add(ramDeletedShareRetention))
}

// loadShares returns every visible share in the caller's account and Region, in index order, failing
// on a record that cannot be read.
func (p *RAMPlugin) loadShares(goCtx context.Context, acct, region string) ([]RAMResourceShare, error) {
	arns, err := loadStringIndex(goCtx, p.state, ramNamespace, ramShareArnsKey(acct, region))
	if err != nil {
		return nil, fmt.Errorf("ram loadShares load index: %w", err)
	}
	now := p.tc.Now()
	shares := make([]RAMResourceShare, 0, len(arns))
	for _, arn := range arns {
		data, err := p.state.Get(goCtx, ramNamespace, ramShareKey(acct, region, arn))
		if err != nil {
			return nil, fmt.Errorf("ram loadShares get %s: %w", arn, err)
		}
		if data == nil {
			continue // an index entry outliving its record, which is not a fault
		}
		var share RAMResourceShare
		if err := json.Unmarshal(data, &share); err != nil {
			return nil, fmt.Errorf("ram loadShares unmarshal %s: %w", arn, err)
		}
		if !ramShareVisible(&share, now) {
			if err := p.pruneShare(goCtx, acct, region, arn); err != nil {
				return nil, err
			}
			continue
		}
		shares = append(shares, share)
	}
	return shares, nil
}

// pruneShare removes a DELETED share whose retention window has passed: its record and its index
// entry.
//
// Nothing else ever removes a deleted share, so without this the record and the index would grow with
// every delete for the life of the state. It runs on the observation that finds the share expired — a
// list or a lookup — rather than on a timer, and expiry is read from the simulated clock, so a replay
// that reaches the same request at the same simulated time prunes the same share. A share pruned or not
// yet pruned answers identically: either way it is not listed and its lookup is
// UnknownResourceException, so when the prune happens is not observable through the API.
func (p *RAMPlugin) pruneShare(goCtx context.Context, acct, region, arn string) error {
	if err := p.state.Delete(goCtx, ramNamespace, ramShareKey(acct, region, arn)); err != nil {
		return fmt.Errorf("ram pruneShare delete %s: %w", arn, err)
	}
	p.indexMu.Lock()
	defer p.indexMu.Unlock()
	if err := removeFromStringIndex(goCtx, p.state, ramNamespace, ramShareArnsKey(acct, region), arn); err != nil {
		return fmt.Errorf("ram pruneShare index %s: %w", arn, err)
	}
	return nil
}

// ramResourceTypes maps an ARN's `service:resource-segment` to the resource type RAM publishes for it.
//
// The values are the type codes the RAM User Guide's "Shareable AWS resources" page lists, verbatim
// (`ec2:TransitGateway`, `license-manager:LicenseConfiguration`, `route53resolver:ResolverRule`, …).
// The keys are the resource segment each service's ARN carries — kebab-case for most
// (`transit-gateway/tgw-…`, `prefix-list/pl-…`), but not all (`network-firewall`'s
// `stateful-rulegroup`, `vpc-lattice`'s `servicenetwork`, ELB's `truststore`) — which is why the type is
// looked up rather than built: no one spelling rule turns the segment into the published name. The
// table covers the shareable types whose ARN form is a `service:segment/…` or `service:segment:…`;
// types not listed here (API Gateway domain names, Marketplace entities and the rest) have no type.
var ramResourceTypes = map[string]string{
	"ec2:capacity-reservation":                  "ec2:CapacityReservation",
	"ec2:coip-pool":                             "ec2:CoipPool",
	"ec2:dedicated-host":                        "ec2:DedicatedHost",
	"ec2:ipam-pool":                             "ec2:IpamPool",
	"ec2:ipam-resource-discovery":               "ec2:IpamResourceDiscovery",
	"ec2:local-gateway-route-table":             "ec2:LocalGatewayRouteTable",
	"ec2:placement-group":                       "ec2:PlacementGroup",
	"ec2:prefix-list":                           "ec2:PrefixList",
	"ec2:security-group":                        "ec2:SecurityGroup",
	"ec2:subnet":                                "ec2:Subnet",
	"ec2:traffic-mirror-target":                 "ec2:TrafficMirrorTarget",
	"ec2:transit-gateway":                       "ec2:TransitGateway",
	"ec2:transit-gateway-multicast-domain":      "ec2:TransitGatewayMulticastDomain",
	"ec2:verified-access-group":                 "ec2:VerifiedAccessGroup",
	"ec2:volume":                                "ec2:Volume",
	"acm-pca:certificate-authority":             "acm-pca:CertificateAuthority",
	"appmesh:mesh":                              "appmesh:Mesh",
	"backup:backup-vault":                       "backup:BackupVault",
	"codebuild:project":                         "codebuild:Project",
	"codebuild:report-group":                    "codebuild:ReportGroup",
	"codeconnections:connection":                "codeconnections:Connection",
	"elasticloadbalancing:truststore":           "elasticloadbalancing:TrustStore",
	"fsx:volume":                                "fsx:Volume",
	"glue:catalog":                              "glue:Catalog",
	"glue:database":                             "glue:Database",
	"glue:table":                                "glue:Table",
	"imagebuilder:component":                    "imagebuilder:Component",
	"imagebuilder:container-recipe":             "imagebuilder:ContainerRecipe",
	"imagebuilder:image":                        "imagebuilder:Image",
	"imagebuilder:image-recipe":                 "imagebuilder:ImageRecipe",
	"license-manager:license-configuration":     "license-manager:LicenseConfiguration",
	"network-firewall:firewall":                 "network-firewall:Firewall",
	"network-firewall:firewall-policy":          "network-firewall:FirewallPolicy",
	"network-firewall:stateful-rulegroup":       "network-firewall:StatefulRuleGroup",
	"network-firewall:stateless-rulegroup":      "network-firewall:StatelessRuleGroup",
	"networkmanager:core-network":               "networkmanager:CoreNetwork",
	"outposts:outpost":                          "outposts:Outpost",
	"outposts:site":                             "outposts:Site",
	"rds:cluster":                               "rds:Cluster",
	"resource-groups:group":                     "resource-groups:Group",
	"route53profiles:profile":                   "route53profiles:Profile",
	"route53resolver:firewall-rule-group":       "route53resolver:FirewallRuleGroup",
	"route53resolver:resolver-query-log-config": "route53resolver:ResolverQueryLogConfig",
	"route53resolver:resolver-rule":             "route53resolver:ResolverRule",
	"sagemaker:feature-group":                   "sagemaker:FeatureGroup",
	"sagemaker:hub":                             "sagemaker:Hub",
	"sagemaker:model-card":                      "sagemaker:ModelCard",
	"sagemaker:pipeline":                        "sagemaker:Pipeline",
	"servicediscovery:namespace":                "servicediscovery:Namespace",
	"ssm:document":                              "ssm:Document",
	"ssm:parameter":                             "ssm:Parameter",
	"vpc-lattice:resourceconfiguration":         "vpc-lattice:ResourceConfiguration",
	"vpc-lattice:service":                       "vpc-lattice:Service",
	"vpc-lattice:servicenetwork":                "vpc-lattice:ServiceNetwork",
}

// ramResourceType returns the published RAM resource type of a resource ARN, or "" when
// [ramResourceTypes] does not name its service and resource segment.
func ramResourceType(arn string) string {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 {
		return ""
	}
	resource := parts[5]
	if i := strings.IndexAny(resource, "/:"); i >= 0 {
		resource = resource[:i]
	}
	return ramResourceTypes[parts[2]+":"+resource]
}

// ramResourceRegionScope is GLOBAL for an ARN with no Region and REGIONAL otherwise.
func ramResourceRegionScope(arn string) string {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) == 6 && parts[3] == "" {
		return "GLOBAL"
	}
	return "REGIONAL"
}

// ramMatchesAny reports whether filter is empty or contains v.
func ramMatchesAny(filter []string, v string) bool {
	return len(filter) == 0 || slices.Contains(filter, v)
}

// ramMatchesType reports whether want is empty or names the type of arn, case-insensitively.
func ramMatchesType(want, arn string) bool {
	return want == "" || strings.EqualFold(want, ramResourceType(arn))
}
