package emulator

import (
	"context"
	"encoding/json"
	"strings"
)

// cfn_getatt_elb.go answers the Fn::GetAtt attributes of an AWS::ElasticLoadBalancingV2::LoadBalancer
// that only the ELB plugin's record holds (#1591), and the list-valued attributes Fn::GetAtt can
// now return into a list context.

// cfnELBHostedZoneIDs maps a Region to the Route 53 hosted zone IDs of its load balancers: the
// first for Application (and Classic) Load Balancers, the second for Network Load Balancers.
//
// The values are AWS's, from the "Route 53 Hosted Zone ID" columns of the Service endpoints table
// on the AWS General Reference's "Elastic Load Balancing endpoints and quotas" page. A load
// balancer's CanonicalHostedZoneID is "The ID of the Amazon Route 53 hosted zone associated with
// the load balancer", which AWS assigns per Region and load balancer type rather than per load
// balancer, so a static table is the whole of the model — the ELB plugin itself does not model a
// hosted zone (see elbLBItem). A Region absent from the table answers no value, and the read is
// refused rather than answered empty.
var cfnELBHostedZoneIDs = map[string][2]string{
	"us-east-2":      {"Z3AADJGX6KTTL2", "ZLMOA37VPKANP"},
	"us-east-1":      {"Z35SXDOTRQ7X7K", "Z26RNL4JYFTOTI"},
	"us-west-1":      {"Z368ELLRRE2KJ0", "Z24FKFUX50B4VW"},
	"us-west-2":      {"Z1H1FL5HABSF5", "Z18D5FSROUN65G"},
	"af-south-1":     {"Z268VQBMOI5EKX", "Z203XCE67M25HM"},
	"ap-east-1":      {"Z3DQVH9N71FHZ0", "Z12Y7K3UBGUAD1"},
	"ap-east-2":      {"Z02789141MW7T1WBU19PO", "Z09176273OC2HWIAUNYW"},
	"ap-south-2":     {"Z0173938T07WNTVAEPZN", "Z0711778386UTO08407HT"},
	"ap-southeast-3": {"Z08888821HLRG5A9ZRTER", "Z01971771FYVNCOVWJU1G"},
	"ap-southeast-5": {"Z06010284QMVVW7WO5J", "Z026317210H9ACVTRO6FB"},
	"ap-southeast-4": {"Z09517862IB2WZLPXG76F", "Z01156963G8MIIL7X90IV"},
	"ap-south-1":     {"ZP97RAFLXTNZK", "ZVDDRBQ08TROA"},
	"ap-southeast-6": {"Z023301818UFJ50CIO0MV", "Z01392953RKV2Q3RBP0KU"},
	"ap-northeast-3": {"Z5LXEXXYW11ES", "Z1GWIQ4HH19I5X"},
	"ap-northeast-2": {"ZWKZPGTI48KDX", "ZIBE1TIR4HY56"},
	"ap-southeast-1": {"Z1LMS91P8CMLE5", "ZKVM4W9LS7TM"},
	"ap-southeast-2": {"Z1GM3OXH4ZPM65", "ZCT6FZBF4DROD"},
	"ap-southeast-7": {"Z0390008CMBRTHFGWBCB", "Z054363131YWATEMWRG5L"},
	"ap-northeast-1": {"Z14GRHDCWA56QT", "Z31USIVHYNEOWT"},
	"ca-central-1":   {"ZQSVJUPU6J1EY", "Z2EPGBW3API2WT"},
	"ca-west-1":      {"Z06473681N0SF6OS049SD", "Z02754302KBB00W2LKWZ9"},
	"cn-north-1":     {"Z1GDH35T77C1KE", "Z3QFB96KMJ7ED6"},
	"cn-northwest-1": {"ZM7IZAIOVVDZF", "ZQEIKTCZ8352D"},
	"eu-central-1":   {"Z215JYRZR1TBD5", "Z3F0SRJ5LGBH90"},
	"eu-west-1":      {"Z32O12XQLNTSW2", "Z2IFOLAFXWLO4F"},
	"eu-west-2":      {"ZHURV8PSTC4K8", "ZD4D7Y8KGAS4G"},
	"eu-south-1":     {"Z3ULH7SSC9OV64", "Z23146JA1KNAFP"},
	"eu-west-3":      {"Z3Q77PNBQS71R4", "Z1CMS0P5QUZ6D5"},
	"eu-south-2":     {"Z0956581394HF5D5LXGAP", "Z1011216NVTVYADP1SSV"},
	"eu-north-1":     {"Z23TAZ6LKFMNIO", "Z1UDT6IFJ4EJM"},
	"eu-central-2":   {"Z06391101F2ZOEP8P5EB3", "Z02239872DOALSIDCX66S"},
	"il-central-1":   {"Z09170902867EHPV2DABU", "Z0313266YDI6ZRHTGQY4"},
	"mx-central-1":   {"Z023552324OKD1BB28BH5", "Z02031231H3ID6HYJ9A7U"},
	"me-south-1":     {"ZS929ML54UICD", "Z3QSRYVP46NYYV"},
	"me-central-1":   {"Z08230872XQRWHG2XF6I", "Z00282643NTTLPANJJG2P"},
	"sa-east-1":      {"Z2P70J7HTTTPLU", "ZTK26PT1VY4CU"},
	"us-gov-east-1":  {"Z166TLBEWOO7G0", "Z1ZSMQQ6Q24QQ8"},
	"us-gov-west-1":  {"Z33AYJ8TM3BH4J", "ZMG1MZ2THAWF1"},
}

// cfnELBHostedZoneID returns the hosted zone ID of a load balancer of lbType in region, or "" when
// the table has none: a Region it does not list, or a Gateway Load Balancer, which the table's two
// columns do not cover.
func cfnELBHostedZoneID(region, lbType string) string {
	zones, ok := cfnELBHostedZoneIDs[region]
	if !ok {
		return ""
	}
	switch lbType {
	case "", "application":
		return zones[0]
	case "network":
		return zones[1]
	}
	return ""
}

// cfnELBTypeFromARN reads a load balancer's type from its ARN's resource segment —
// "loadbalancer/app/…" is an Application Load Balancer and "loadbalancer/net/…" a Network Load
// Balancer — or answers "gateway", which the hosted zone table does not cover, for anything else.
func cfnELBTypeFromARN(arn string) string {
	switch {
	case strings.Contains(arn, ":loadbalancer/app/"):
		return "application"
	case strings.Contains(arn, ":loadbalancer/net/"):
		return "network"
	}
	return "gateway"
}

// recordELBAttributes records on dr the published attributes of an ELBv2 load balancer that only
// the ELB plugin's record holds: CanonicalHostedZoneID, which depends on the load balancer's type,
// and SecurityGroups, a list. Any other type, or a record it cannot read, records nothing.
//
// SecurityGroups is recorded as the record holds it, which for a load balancer created with none
// is an empty list — the answer, not a gap.
func (d *StackDeployer) recordELBAttributes(ctx context.Context, dr *DeployedResource) {
	if d.state == nil || dr.PhysicalID == "" || dr.Type != "AWS::ElasticLoadBalancingV2::LoadBalancer" {
		return
	}
	data, err := d.state.Get(ctx, elbNamespace, "lb:"+d.identity.accountID+"/"+d.identity.region+"/"+dr.PhysicalID)
	if err != nil || data == nil {
		return
	}
	var lb ELBLoadBalancer
	if jsonErr := json.Unmarshal(data, &lb); jsonErr != nil {
		return
	}
	dr.Metadata = cfnCopyMetadata(dr.Metadata)
	if zone := cfnELBHostedZoneID(d.identity.region, lb.Type); zone != "" {
		dr.Metadata["CanonicalHostedZoneID"] = zone
	}
	groups := append([]string{}, lb.SecurityGroups...)
	dr.Metadata["SecurityGroups"] = groups
}

// cfnMetadataList reads a list-valued attribute out of dr.Metadata. A deploy records one as a
// []string, and a record read back from state holds it as a []interface{}, so both are accepted.
func cfnMetadataList(dr DeployedResource, key string) ([]string, bool) {
	if dr.Metadata == nil {
		return nil, false
	}
	switch v := dr.Metadata[key].(type) {
	case []string:
		return v, true
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out, true
	}
	return nil, false
}

// resolveFnGetAttList resolves an Fn::GetAtt in a list context: a list-valued attribute's
// members, so `!Select [0, !GetAtt LB.SecurityGroups]` and a list property receive the list rather
// than one joined string. Any other attribute is the one-element list of its scalar value, as it
// was before list-valued attributes existed.
func resolveFnGetAttList(args interface{}, cctx *cfnContext) []string {
	if arr, ok := args.([]interface{}); ok && len(arr) == 2 {
		logicalID, _ := arr[0].(string)
		attr, _ := arr[1].(string)
		if dr, declared := cctx.resources[logicalID]; declared && attr != "" {
			if list, ok := cfnMetadataList(dr, attr); ok {
				return list
			}
		}
	}
	return []string{resolveFnGetAtt(args, cctx)}
}

// cfnGetAttUnmodelledPrefixes maps a resource type to attribute-name prefixes substrate holds no
// value for, each with the reason, for the published attributes that are a family — every member
// of an object attribute — rather than one name. See [cfnUnmodelledAttribute].
var cfnGetAttUnmodelledPrefixes = map[string]map[string]string{
	"AWS::EC2::VPC": {
		"VpcEncryptionControl": "VPC encryption control is not modeled",
	},
	"AWS::EC2::Subnet": {
		"BlockPublicAccessStates": "VPC Block Public Access is not modeled",
	},
	"AWS::Lambda::Function": {
		"SnapStartResponse": "the Lambda plugin does not model SnapStart",
	},
}

// cfnUnmodelledAttribute returns why substrate holds no value for a published attribute, and
// whether it is one: an exact entry of cfnGetAttUnmodelled, or a member of a family in
// cfnGetAttUnmodelledPrefixes — the family's object attribute itself, or any dotted member of it.
func cfnUnmodelledAttribute(typ, attr string) (string, bool) {
	if reason, ok := cfnGetAttUnmodelled[typ][attr]; ok {
		return reason, true
	}
	for prefix, reason := range cfnGetAttUnmodelledPrefixes[typ] {
		if attr == prefix || strings.HasPrefix(attr, prefix+".") {
			return reason, true
		}
	}
	return "", false
}
