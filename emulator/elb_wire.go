package emulator

import "time"

// ELB, like RDS and ElastiCache before it, was already answering from a projection when #756 reached
// it. The types below are the published ELBv2 shapes; they were lifted out of elb_plugin.go unchanged
// except where noted, on the pattern emulator/ecr_wire.go established (#1090) and
// emulator/efs_wire.go (#1304), emulator/ecs_wire.go (#1309), emulator/glue_wire.go (#1310),
// emulator/rds_wire.go (#1311) and emulator/elasticache_wire.go (#1312) followed.
//
// # Why this file is a pin rather than a fix
//
// All five ELB records — ELBListener, ELBLoadBalancer, ELBRule and ELBTargetGroup (elb_types.go) plus
// ELBClassicLoadBalancer (elb_classic.go) — carry AccountID, Region and EverTagged, and none carries
// a CreatedAt: fifteen baseline lines, three apiece, the most uniform block AC3 has met. But
// `grep 'ELB[A-Za-z]* .*`xml:'` matches nothing in the tree. Not one record is XML-encoded directly;
// every one of the twelve sites that answers a resource hands a *…Item struct to elbOKResponse, so no
// bookkeeping member could reach a body even before this change.
//
// What was missing was therefore not a projection but the assertion that one exists: the fifteen
// lines were reachable-by-declaration with nothing pinning them shut, the position ECS's four
// already-projecting records were in at #1308, RDS's five at #1311 and ElastiCache's four at #1312.
// emulator/elb_wire_test.go is that pin and scripts/wire-bookkeeping-projected.txt cites it. The
// lines stay in scripts/wire-bookkeeping-baseline.txt because ecr_wire.go's rule forbids removing
// them: `json:"-"` and retyping a field in place both change the format of every recorded run, since
// MemoryStateManager snapshots those bytes and a replay reads them back.
//
// # The leaked account and Region have no published home in either generation
//
// Unlike EFS, where the leaked AccountID became the published OwnerId (#1304), and Glue, where it
// became CatalogId (#1310), no ELB shape publishes an account or a Region member. Checked
// member-by-member against API_LoadBalancer, API_TargetGroup, API_Listener, API_Rule and the
// 2012-06-01 API_LoadBalancerDescription: there is no candidate to move either value to. Each of the
// four ELBv2 shapes publishes the resource's ARN instead, from which both are recoverable, which is
// what the real API requires of a caller. The classic shape publishes no ARN either — the record's
// own ARN field says so, and exists for IAM and the Resource Groups Tagging API rather than for a
// response.
//
// Suffix is in the same position and is worth naming so it is not re-discovered as a leak: all four
// ELBv2 records persist it, no shape publishes it, and no item struct has ever carried it. It is the
// minted ARN component (generateELBSuffix), so a caller reads it off the ARN.
//
// # Two persisted load-balancer members were reported nowhere
//
// API_LoadBalancer publishes both of these and elbLBItem carried neither, though createLoadBalancer
// stores both:
//
//	record member                        published                        authority
//	ELBLoadBalancer.AvailabilityZones    AvailabilityZones.member.N       API_LoadBalancer
//	ELBLoadBalancer.SecurityGroups       SecurityGroups.member.N          API_LoadBalancer
//
// So this is a gap rather than a #1013 "substrate has not measured it" absence: substrate holds the
// values and a consumer asking the question got nothing. Both are Required: No and both are
// `omitempty` here, so a load balancer created without security groups reports no wrapper rather than
// an empty one.
//
// AvailabilityZones is an array of API_AvailabilityZone objects rather than of strings — that is the
// one place the two generations' shapes differ on this member, the classic
// LoadBalancerDescription.AvailabilityZones being an array of strings — so it is projected through
// elbAvailabilityZoneItem. See that type for why ZoneName is its only member.
//
// The timestamp format needs no fix, as with RDS and ElastiCache and unlike Glue's: both generations
// render CreatedTime pre-formatted as RFC3339, which is ISO8601, the AWS Query protocol's default. So
// ELB is not an instance of #1305. Nor is a *time.Time needed for the published-but-optional member
// the way it was on those two services, because the zero instant is unreachable at every site: every
// create sets CreatedTime from the simulated clock, and both describes skip a record whose stored
// JSON will not decode rather than falling back to a partial one.
//
// # Two sites were answering an empty result
//
// ModifyTargetGroup found its target group, applied the request's health-check changes, wrote the
// record back — and then answered `TargetGroups{}`, discarding the record it had just modified.
// SetRulePriorities did the same with `Rules{}` for every rule it repriced. Both members are
// published — API_ModifyTargetGroup's TargetGroups.member.N is "Information about the modified target
// group" and API_SetRulePriorities' Rules.member.N is "Information about the rules", and both
// reference pages' sample responses carry a full member — so a consumer reading the result of its own
// modification got an empty list. The same shape as DeleteDBCluster reporting 3 of 11 members
// (#1311), and fixed the same way: each found path projects the record it wrote.
//
// Neither refuses an ARN that names nothing. ModifyTargetGroup and ModifyListener still answer the
// empty list where AWS publishes TargetGroupNotFound and ListenerNotFound, and SetRulePriorities
// skips a rule where AWS publishes RuleNotFound. Inventing those refusals is a behavior change
// separate from reporting the record that was found; filed as #1313.
//
// # What stays on the records
//
// AccountID, Region, Tags and EverTagged stay in elb_types.go and elb_classic.go, untouched. The
// state keys already scope every record by account and Region so nothing reads those two back, but a
// persisted member is not free to remove, per ecr_wire.go's rule above — and Tags and EverTagged are
// read off the stored record by elbDecodeTaggedResource to answer both ELB's own DescribeTags and the
// Resource Groups Tagging API's GetResources.
//
// # What substrate does not model
//
// Each type below lists the published members it omits. They are absent rather than present and
// zero, per #1013: a count, a threshold or an interval reported as its zero value reads as a
// measurement, and substrate has measured nothing. Two are worth naming because a consumer is likely
// to look for them. API_AvailabilityZone.SubnetId is absent because createLoadBalancer reads
// Subnets.member only to derive a zone name and persists no subnet ID — so there is nothing to
// report, and persisting them is a record change rather than a projection one. And
// API_TargetGroup.LoadBalancerArns is absent because a target group's record holds no association to
// a load balancer; the listener holds it in the other direction.
//
// # The classic shapes are not here
//
// ELBClassicLoadBalancer's projection — elbClassicLBItem and the three satellites it nests — stays in
// elb_classic.go, where classicLBToItem's doc comment already carries this argument for it and states
// why every 2012-06-01 shape lives in one file. The test in elb_wire_test.go pins both generations;
// only the rendering is split.

// elbLBItem is the LoadBalancer of CreateLoadBalancer and DescribeLoadBalancers.
//
// Member names follow API_LoadBalancer, on which every member is Required: No. The members substrate
// does not model are absent rather than present and zero: CanonicalHostedZoneId,
// CustomerOwnedIpv4Pool, EnablePrefixForIpv6SourceNat,
// EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic, IpAddressType and IpamPools.
type elbLBItem struct {
	LoadBalancerArn   string                    `xml:"LoadBalancerArn"`
	LoadBalancerName  string                    `xml:"LoadBalancerName"`
	DNSName           string                    `xml:"DNSName"`
	Type              string                    `xml:"Type"`
	Scheme            string                    `xml:"Scheme"`
	VpcID             string                    `xml:"VpcId"`
	State             elbStateItem              `xml:"State"`
	CreatedTime       string                    `xml:"CreatedTime"`
	AvailabilityZones []elbAvailabilityZoneItem `xml:"AvailabilityZones>member,omitempty"`
	SecurityGroups    []string                  `xml:"SecurityGroups>member,omitempty"`
}

// elbStateItem is the LoadBalancerState of API_LoadBalancer.State.
//
// API_LoadBalancerState also publishes Reason, a human-readable explanation of a non-active state.
// Substrate reports every load balancer `active` on create, so there is no reason to report.
type elbStateItem struct {
	Code string `xml:"Code"`
}

// elbAvailabilityZoneItem is one AvailabilityZone of API_LoadBalancer.AvailabilityZones.
//
// ZoneName is its only member here. API_AvailabilityZone publishes four others —
// LoadBalancerAddresses, OutpostId, SourceNatIpv6Prefixes and SubnetId — and substrate holds a value
// for none of them: the record stores a []string of derived zone names and no subnet ID, which is why
// this is a struct with one member rather than the published shape's whole surface.
type elbAvailabilityZoneItem struct {
	ZoneName string `xml:"ZoneName"`
}

// lbToItem projects a persisted load balancer onto the published shape.
func lbToItem(lb ELBLoadBalancer) elbLBItem {
	item := elbLBItem{
		LoadBalancerArn:  lb.ARN,
		LoadBalancerName: lb.Name,
		DNSName:          lb.DNSName,
		Type:             lb.Type,
		Scheme:           lb.Scheme,
		VpcID:            lb.VpcID,
		State:            elbStateItem{Code: lb.State.Code},
		CreatedTime:      lb.CreatedTime.UTC().Format(time.RFC3339),
		SecurityGroups:   lb.SecurityGroups,
	}
	for _, zone := range lb.AvailabilityZones {
		item.AvailabilityZones = append(item.AvailabilityZones, elbAvailabilityZoneItem{ZoneName: zone})
	}
	return item
}

// elbTGItem is the TargetGroup of CreateTargetGroup, DescribeTargetGroups and ModifyTargetGroup.
//
// Member names follow API_TargetGroup, on which every member is Required: No. The members substrate
// does not model are absent rather than present and zero: HealthCheckEnabled,
// HealthCheckIntervalSeconds, HealthCheckTimeoutSeconds, HealthyThresholdCount, IpAddressType,
// LoadBalancerArns, Matcher, ProtocolVersion, TargetControlPort and UnhealthyThresholdCount.
type elbTGItem struct {
	TargetGroupArn      string `xml:"TargetGroupArn"`
	TargetGroupName     string `xml:"TargetGroupName"`
	Protocol            string `xml:"Protocol"`
	Port                int    `xml:"Port"`
	VpcID               string `xml:"VpcId"`
	TargetType          string `xml:"TargetType"`
	HealthCheckPath     string `xml:"HealthCheckPath"`
	HealthCheckProtocol string `xml:"HealthCheckProtocol"`
	HealthCheckPort     string `xml:"HealthCheckPort"`
}

// tgToItem projects a persisted target group onto the published shape.
//
// The record's Targets are not here, and that is the published shape rather than an omission:
// API_TargetGroup declares no targets member, and DescribeTargetHealth is the operation that reports
// them.
func tgToItem(tg ELBTargetGroup) elbTGItem {
	return elbTGItem{
		TargetGroupArn:      tg.ARN,
		TargetGroupName:     tg.Name,
		Protocol:            tg.Protocol,
		Port:                tg.Port,
		VpcID:               tg.VpcID,
		TargetType:          tg.TargetType,
		HealthCheckPath:     tg.HealthCheckPath,
		HealthCheckProtocol: tg.HealthCheckProtocol,
		HealthCheckPort:     tg.HealthCheckPort,
	}
}

// elbListenerItem is the Listener of CreateListener, DescribeListeners and ModifyListener.
//
// Member names follow API_Listener, on which every member is Required: No. The members substrate does
// not model are absent rather than present and zero: AlpnPolicy, Certificates, MutualAuthentication
// and SslPolicy.
type elbListenerItem struct {
	ListenerArn     string          `xml:"ListenerArn"`
	LoadBalancerArn string          `xml:"LoadBalancerArn"`
	Port            int             `xml:"Port"`
	Protocol        string          `xml:"Protocol"`
	DefaultActions  []elbActionItem `xml:"DefaultActions>member"`
}

// elbActionItem is one Action of a listener's DefaultActions or a rule's Actions.
//
// Member names follow API_Action. The members substrate does not model are absent rather than present
// and zero: AuthenticateCognitoConfig, AuthenticateOidcConfig, FixedResponseConfig, ForwardConfig and
// RedirectConfig — the per-type configuration blocks, none of which a create records.
type elbActionItem struct {
	Type           string `xml:"Type"`
	TargetGroupArn string `xml:"TargetGroupArn,omitempty"`
	Order          int    `xml:"Order,omitempty"`
}

// listenerToItem projects a persisted listener onto the published shape.
func listenerToItem(l ELBListener) elbListenerItem {
	item := elbListenerItem{
		ListenerArn:     l.ARN,
		LoadBalancerArn: l.LoadBalancerARN,
		Port:            l.Port,
		Protocol:        l.Protocol,
	}
	for _, a := range l.DefaultActions {
		item.DefaultActions = append(item.DefaultActions, elbActionItem{ //nolint:staticcheck
			Type:           a.Type,
			TargetGroupArn: a.TargetGroupArn,
			Order:          a.Order,
		})
	}
	return item
}

// elbConditionItem is one RuleCondition of a rule's Conditions.
//
// Field and Values are API_RuleCondition's two generic members. The typed condition blocks it also
// publishes — HostHeaderConfig, HttpHeaderConfig, HttpRequestMethodConfig, PathPatternConfig,
// QueryStringConfig and SourceIpConfig — are absent because a create records only the generic pair.
type elbConditionItem struct {
	Field  string   `xml:"Field"`
	Values []string `xml:"Values>member"`
}

// elbRuleItem is the Rule of CreateRule, DescribeRules and SetRulePriorities.
//
// Member names follow API_Rule, on which every member is Required: No. Transforms, its fifth member,
// is absent because substrate records no rule transform.
//
// The record's ListenerARN is not here, and that is the published shape rather than an omission:
// API_Rule declares no listener member. It is how DescribeRules selects, not something it reports.
type elbRuleItem struct {
	RuleArn    string             `xml:"RuleArn"`
	Priority   string             `xml:"Priority"`
	IsDefault  bool               `xml:"IsDefault"`
	Conditions []elbConditionItem `xml:"Conditions>member"`
	Actions    []elbActionItem    `xml:"Actions>member"`
}

// ruleToItem projects a persisted rule onto the published shape.
func ruleToItem(r ELBRule) elbRuleItem {
	item := elbRuleItem{
		RuleArn:   r.ARN,
		Priority:  r.Priority,
		IsDefault: r.IsDefault,
	}
	for _, c := range r.Conditions {
		item.Conditions = append(item.Conditions, elbConditionItem{Field: c.Field, Values: c.Values}) //nolint:staticcheck
	}
	for _, a := range r.Actions {
		item.Actions = append(item.Actions, elbActionItem{ //nolint:staticcheck
			Type:           a.Type,
			TargetGroupArn: a.TargetGroupArn,
			Order:          a.Order,
		})
	}
	return item
}
