package emulator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
)

// Security group rule IDs (#1490).
//
// EC2 gives every rule its own sgr- ID: "Each rule has a protocol, from and to ports, and source
// (CIDR range, security group, or prefix list)" (API_RevokeSecurityGroupIngress), and an
// IpPermission naming several sources is several rules. Substrate stores the IpPermission an
// authorize carried, which may hold several sources, so a rule is not a stored record. It is one
// source of one stored permission, and [ec2SecurityGroupRules] lists them.
//
// A rule's ID is derived from its content at read time — the group, the direction, the
// canonical protocol and port range, and the one source — by [ec2DerivedSGRuleID]. Nothing is
// minted and nothing is migrated: a group an older build stored has IDs the moment it is read,
// and replaying a log yields the same IDs because it yields the same content.
//
// ModifySecurityGroupRules is the one operation that changes a rule's content while the rule
// keeps its ID, so it is the one that records an ID: the rule's [EC2SecurityGroupRuleMeta]
// carries the ID it had before. That record is written by a request and read back by every
// later one, so it replays like any other state; a derivation that ignored it would hand a
// modified rule a new ID, which is the observable AWS does not produce.

// The kinds of source a security group rule names, as the first part of its content key.
const (
	ec2SGRuleSourceNone       = ""
	ec2SGRuleSourceCIDR       = "cidr"
	ec2SGRuleSourceIPv6       = "ipv6"
	ec2SGRuleSourcePrefixList = "pl"
	ec2SGRuleSourceGroup      = "sg"
)

// ec2SGRule is one security group rule: one source under one protocol and port range, in one
// direction of one group.
type ec2SGRule struct {
	// ID is the rule's sgr- ID.
	ID string
	// GroupID and OwnerID are the group the rule belongs to and the group's owner.
	GroupID, OwnerID string
	// Egress reports an outbound rule.
	Egress bool
	// Perm is the rule as a single-source permission, holding the stored spelling of its
	// protocol, ports and source, so a revoke built from it matches the stored rule exactly.
	Perm EC2IPPermission
	// Kind and Value are the rule's source in canonical form: Value is the canonical CIDR, the
	// prefix-list ID, or the referenced group's ID.
	Kind, Value string
	// Description is the rule's description.
	Description string
	// Tags are the rule's tags.
	Tags []EC2Tag
	// Key is the rule's content key, which keys [EC2SecurityGroup.RuleMeta].
	Key string
}

// ec2SGRuleSourceValue returns a CIDR in the canonical form [ec2CanonicalRuleCIDR] stores, or
// as stored when it does not parse, which only an earlier substrate could have written.
func ec2SGRuleSourceValue(cidr string) string {
	if prefix, err := netip.ParsePrefix(cidr); err == nil {
		return prefix.Masked().String()
	}
	return cidr
}

// ec2SGRuleKey is a rule's content key: its direction, its canonical protocol and port range
// ([ec2CanonicalPortRange], so tcp and 6 are one rule, and an all-protocols rule's ports are
// not part of it), and its source.
func ec2SGRuleKey(egress bool, perm EC2IPPermission, kind, value string) string {
	direction := "ingress"
	if egress {
		direction = "egress"
	}
	pr := ec2CanonicalPortRange(perm)
	return direction + "|" + pr.protocol + "|" + strconv.Itoa(pr.from) + "|" + strconv.Itoa(pr.to) +
		"|" + kind + "|" + value
}

// ec2DerivedSGRuleID is the sgr- ID derived from a rule's group and content key: "sgr-" and
// the first 17 hex digits of their SHA-256, the length of the IDs AWS mints
// (InvalidSecurityGroupRuleId.Malformed: "in the form sgr-xxxxxxxxxxxxxxxxx").
//
// salt is zero for the derivation every rule starts with. A rule whose derived ID another
// rule of the group already carries — possible only once ModifySecurityGroupRules has moved a
// rule off the content that ID was derived from and a new rule has been authorized with that
// content — takes the next salt instead; see [ec2SecurityGroupRules].
func ec2DerivedSGRuleID(groupID, key string, salt int) string {
	text := groupID + "|" + key
	if salt > 0 {
		text += "#" + strconv.Itoa(salt)
	}
	sum := sha256.Sum256([]byte(text))
	return "sgr-" + hex.EncodeToString(sum[:])[:17]
}

// ec2SGRuleARN is a rule's ARN, in the format the Service Authorization Reference publishes:
// arn:${Partition}:ec2:${Region}:${Account}:security-group-rule/${SecurityGroupRuleId}.
func ec2SGRuleARN(region, accountID, ruleID string) string {
	return "arn:aws:ec2:" + region + ":" + accountID + ":security-group-rule/" + ruleID
}

// ec2SplitPermission returns one rule per source of perm, in stored order — IPv4 ranges, IPv6
// ranges, prefix lists, referenced groups — or one sourceless rule for a permission naming no
// source. Only the content fields are set.
func ec2SplitPermission(egress bool, perm EC2IPPermission) []ec2SGRule {
	base := EC2IPPermission{IPProtocol: perm.IPProtocol, FromPort: perm.FromPort, ToPort: perm.ToPort}
	var rules []ec2SGRule
	add := func(one EC2IPPermission, kind, value string) {
		rules = append(rules, ec2SGRule{
			Egress: egress, Perm: one, Kind: kind, Value: value,
			Key: ec2SGRuleKey(egress, one, kind, value),
		})
	}
	for _, cidr := range perm.IPRanges {
		one := base
		one.IPRanges = []string{cidr}
		add(one, ec2SGRuleSourceCIDR, ec2SGRuleSourceValue(cidr))
	}
	for _, cidr := range perm.IPv6Ranges {
		one := base
		one.IPv6Ranges = []string{cidr}
		add(one, ec2SGRuleSourceIPv6, ec2SGRuleSourceValue(cidr))
	}
	for _, id := range perm.PrefixListIDs {
		one := base
		one.PrefixListIDs = []string{id}
		add(one, ec2SGRuleSourcePrefixList, id)
	}
	for _, pair := range perm.UserIDGroupPairs {
		one := base
		one.UserIDGroupPairs = []EC2UserIDGroupPair{pair}
		value := pair.GroupID
		if value == "" {
			value = "name:" + pair.GroupName
		}
		add(one, ec2SGRuleSourceGroup, value)
	}
	if !ec2PermHasSource(perm) {
		add(base, ec2SGRuleSourceNone, "")
	}
	return rules
}

// ec2SecurityGroupRules lists a group's rules, ingress first, each with its ID, description and
// tags.
//
// A content key held twice — which only state an earlier substrate wrote can contain, since an
// authorize refuses a duplicate (#1491) — is one rule, listed once.
//
// IDs are assigned in two passes so they are stable whatever order the rules are stored in: a
// rule whose [EC2SecurityGroupRuleMeta] records an ID keeps it, and every other rule takes the
// first salt of [ec2DerivedSGRuleID] that no rule of the group already carries. The salt is
// above zero only in the case that function describes.
func ec2SecurityGroupRules(sg EC2SecurityGroup) []ec2SGRule {
	var rules []ec2SGRule
	seen := map[string]bool{}
	collect := func(perms []EC2IPPermission, egress bool) {
		for _, perm := range perms {
			for _, r := range ec2SplitPermission(egress, perm) {
				if seen[r.Key] {
					continue
				}
				seen[r.Key] = true
				r.GroupID, r.OwnerID = sg.GroupID, sg.AccountID
				meta := sg.RuleMeta[r.Key]
				r.ID, r.Tags = meta.ID, slices.Clone(meta.Tags)
				r.Description = meta.Description
				if r.Kind == ec2SGRuleSourceGroup {
					r.Description = r.Perm.UserIDGroupPairs[0].Description
				}
				rules = append(rules, r)
			}
		}
	}
	collect(sg.IngressRules, false)
	collect(sg.EgressRules, true)

	taken := map[string]bool{}
	for _, r := range rules {
		if r.ID != "" {
			taken[r.ID] = true
		}
	}
	for i := range rules {
		if rules[i].ID != "" {
			continue
		}
		for salt := 0; ; salt++ {
			id := ec2DerivedSGRuleID(sg.GroupID, rules[i].Key, salt)
			if !taken[id] {
				rules[i].ID = id
				taken[id] = true
				break
			}
		}
	}
	return rules
}

// ec2PruneRuleMeta drops every [EC2SecurityGroup.RuleMeta] entry whose rule the group no longer
// holds, so a revoked rule's description, tags and ID do not return with a later rule of the
// same content. An empty map is dropped too, so a group with no metadata stores none.
func ec2PruneRuleMeta(sg *EC2SecurityGroup) {
	if len(sg.RuleMeta) == 0 {
		sg.RuleMeta = nil
		return
	}
	held := map[string]bool{}
	for _, r := range ec2SecurityGroupRules(*sg) {
		held[r.Key] = true
	}
	for key, meta := range sg.RuleMeta {
		if !held[key] || (meta.ID == "" && meta.Description == "" && len(meta.Tags) == 0) {
			delete(sg.RuleMeta, key)
		}
	}
	if len(sg.RuleMeta) == 0 {
		sg.RuleMeta = nil
	}
}

// ec2SetRuleMeta updates the metadata entry of the rule key names, creating the map if needed.
func ec2SetRuleMeta(sg *EC2SecurityGroup, key string, update func(*EC2SecurityGroupRuleMeta)) {
	if sg.RuleMeta == nil {
		sg.RuleMeta = map[string]EC2SecurityGroupRuleMeta{}
	}
	meta := sg.RuleMeta[key]
	update(&meta)
	sg.RuleMeta[key] = meta
}

// ec2SecurityGroupRuleIDKind is the sgr- ID family, with the two codes the EC2 client-error
// table publishes: InvalidSecurityGroupRuleId.Malformed, "The specified security group rule ID
// is not valid. Ensure that you specify the security group rule ID in the form
// sgr-xxxxxxxxxxxxxxxxx.", and InvalidSecurityGroupRuleId.NotFound, "The specified security
// group rule does not exist". The messages are the kind's, as for every ID family.
var ec2SecurityGroupRuleIDKind = ec2IDKind{ //nolint:gochecknoglobals // immutable, as the kinds in ec2_resourceid.go
	Prefix: "sgr-", NotFound: "InvalidSecurityGroupRuleId.NotFound",
	Malformed: "InvalidSecurityGroupRuleId.Malformed", Noun: "security group rule",
}

// ec2SGRuleItem is AWS's SecurityGroupRule as DescribeSecurityGroupRules and an authorize's
// securityGroupRuleSet render it.
type ec2SGRuleItem struct {
	SecurityGroupRuleID  string             `xml:"securityGroupRuleId"`
	GroupID              string             `xml:"groupId"`
	GroupOwnerID         string             `xml:"groupOwnerId"`
	IsEgress             bool               `xml:"isEgress"`
	IPProtocol           string             `xml:"ipProtocol"` //nolint:revive
	FromPort             int                `xml:"fromPort"`
	ToPort               int                `xml:"toPort"`
	CidrIPv4             string             `xml:"cidrIpv4,omitempty"` //nolint:revive
	CidrIPv6             string             `xml:"cidrIpv6,omitempty"` //nolint:revive
	PrefixListID         string             `xml:"prefixListId,omitempty"`
	ReferencedGroupInfo  *ec2SGRuleRefGroup `xml:"referencedGroupInfo,omitempty"`
	Description          string             `xml:"description,omitempty"`
	Tags                 *ec2TagSetXML      `xml:"tagSet,omitempty"`
	SecurityGroupRuleARN string             `xml:"securityGroupRuleArn"`
}

// ec2SGRuleRefGroup is AWS's ReferencedSecurityGroup, the group a rule names as its source.
type ec2SGRuleRefGroup struct {
	GroupID string `xml:"groupId,omitempty"`
	UserID  string `xml:"userId,omitempty"`
}

// ec2RevokedSGRuleItem is AWS's RevokedSecurityGroupRule, one item of a revoke's
// revokedSecurityGroupRuleSet. Its referenced group is a bare ID, where SecurityGroupRule's is a
// ReferencedSecurityGroup.
type ec2RevokedSGRuleItem struct {
	SecurityGroupRuleID string `xml:"securityGroupRuleId"`
	GroupID             string `xml:"groupId"`
	IsEgress            bool   `xml:"isEgress"`
	IPProtocol          string `xml:"ipProtocol"` //nolint:revive
	FromPort            int    `xml:"fromPort"`
	ToPort              int    `xml:"toPort"`
	CidrIPv4            string `xml:"cidrIpv4,omitempty"` //nolint:revive
	CidrIPv6            string `xml:"cidrIpv6,omitempty"` //nolint:revive
	PrefixListID        string `xml:"prefixListId,omitempty"`
	ReferencedGroupID   string `xml:"referencedGroupId,omitempty"`
	Description         string `xml:"description,omitempty"`
}

// ec2SGRuleXML renders one rule as AWS's SecurityGroupRule.
func ec2SGRuleXML(r ec2SGRule, region string) ec2SGRuleItem {
	item := ec2SGRuleItem{
		SecurityGroupRuleID: r.ID, GroupID: r.GroupID, GroupOwnerID: r.OwnerID, IsEgress: r.Egress,
		IPProtocol: r.Perm.IPProtocol, FromPort: r.Perm.FromPort, ToPort: r.Perm.ToPort,
		Description: r.Description, Tags: ec2TagSet(r.Tags),
		SecurityGroupRuleARN: ec2SGRuleARN(region, r.OwnerID, r.ID),
	}
	switch r.Kind {
	case ec2SGRuleSourceCIDR:
		item.CidrIPv4 = r.Perm.IPRanges[0]
	case ec2SGRuleSourceIPv6:
		item.CidrIPv6 = r.Perm.IPv6Ranges[0]
	case ec2SGRuleSourcePrefixList:
		item.PrefixListID = r.Perm.PrefixListIDs[0]
	case ec2SGRuleSourceGroup:
		pair := r.Perm.UserIDGroupPairs[0]
		item.ReferencedGroupInfo = &ec2SGRuleRefGroup{GroupID: pair.GroupID, UserID: pair.UserID}
	}
	return item
}

// ec2RevokedSGRuleXML renders one rule as AWS's RevokedSecurityGroupRule.
func ec2RevokedSGRuleXML(r ec2SGRule) ec2RevokedSGRuleItem {
	full := ec2SGRuleXML(r, "")
	item := ec2RevokedSGRuleItem{
		SecurityGroupRuleID: full.SecurityGroupRuleID, GroupID: full.GroupID, IsEgress: full.IsEgress,
		IPProtocol: full.IPProtocol, FromPort: full.FromPort, ToPort: full.ToPort,
		CidrIPv4: full.CidrIPv4, CidrIPv6: full.CidrIPv6, PrefixListID: full.PrefixListID,
		Description: full.Description,
	}
	if full.ReferencedGroupInfo != nil {
		item.ReferencedGroupID = full.ReferencedGroupInfo.GroupID
	}
	return item
}

// ec2RuleDescriptions reads the description of every IPv4, IPv6 and prefix-list source in an
// IpPermissions.N request, keyed by the rule's content key. perms is the request as parsed and
// canonicalised, whose N-th permission and M-th source are the ones IpPermissions.N's M-th
// entry names, because the parser stops at the first gap. A referenced group's description is
// parsed onto its pair, so it is not read here.
func ec2RuleDescriptions(params map[string]string, perms []EC2IPPermission, egress bool) map[string]string {
	out := map[string]string{}
	for i, perm := range perms {
		prefix := "IpPermissions." + strconv.Itoa(i+1) + "."
		base := EC2IPPermission{IPProtocol: perm.IPProtocol, FromPort: perm.FromPort, ToPort: perm.ToPort}
		read := func(list, kind string, values []string, canonical func(string) string) {
			for m, v := range values {
				if d := params[prefix+list+"."+strconv.Itoa(m+1)+".Description"]; d != "" {
					out[ec2SGRuleKey(egress, base, kind, canonical(v))] = d
				}
			}
		}
		read("IpRanges", ec2SGRuleSourceCIDR, perm.IPRanges, ec2SGRuleSourceValue)
		read("Ipv6Ranges", ec2SGRuleSourceIPv6, perm.IPv6Ranges, ec2SGRuleSourceValue)
		read("PrefixListIds", ec2SGRuleSourcePrefixList, perm.PrefixListIDs, func(v string) string { return v })
	}
	return out
}

// ec2SGRuleMetaKeys returns the set of content keys rules hold.
func ec2SGRuleMetaKeys(rules []ec2SGRule) map[string]bool {
	keys := make(map[string]bool, len(rules))
	for _, r := range rules {
		keys[r.Key] = true
	}
	return keys
}

// ec2SecurityGroupRuleFilterSpec is DescribeSecurityGroupRules' filter set, from
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeSecurityGroupRules.html:
// group-id, security-group-rule-id and tag:<key>, all three evaluated. The page lists no
// tag-key filter, so one is refused.
func ec2SecurityGroupRuleFilterSpec() ec2FilterSpec {
	return ec2FilterSpec{
		tagValueFilter: true,
		evaluated:      []string{"group-id", "security-group-rule-id"},
	}
}

// ec2SGRuleMatchesFilters reports whether a rule satisfies every DescribeSecurityGroupRules
// filter.
func ec2SGRuleMatchesFilters(r ec2SGRule, filters map[string][]string) bool {
	for name, values := range filters {
		if matched, isTag := ec2TagFilterMatch(r.Tags, name, values); isTag {
			if !matched {
				return false
			}
			continue
		}
		switch name {
		case "group-id":
			if !ec2FilterAccepts(values, r.GroupID) {
				return false
			}
		case "security-group-rule-id":
			if !ec2FilterAccepts(values, r.ID) {
				return false
			}
		}
	}
	return true
}

// ec2InvalidNextTokenError is the refusal for a NextToken substrate did not issue for the
// request, in the words [ec2NextTokenOffset] uses for the same refusal.
func ec2InvalidNextTokenError(token string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    "The token '" + token + "' is invalid",
		HTTPStatus: http.StatusBadRequest,
	}
}

// describeSecurityGroupRules lists the security group rules of the account's groups in the
// region, one rule per source (#1490).
//
// SecurityGroupRuleId.N asserts that every ID exists, as every EC2 ID list does: an ID naming no
// rule is InvalidSecurityGroupRuleId.NotFound and a malformed one
// InvalidSecurityGroupRuleId.Malformed ([ec2SecurityGroupRuleIDKind]). Filter.N takes the three
// names [ec2SecurityGroupRuleFilterSpec] lists.
//
// It pages with the shared cursor ([page]) rather than an offset, because the rules of a group
// are derived from its permissions, so a rule authorized or revoked between two pages would
// shift every offset after it. The listing is ordered by group ID, then rule ID, and the token
// names the last rule returned. The page publishes MaxResults with "This value can be between 5
// and 1000. If this parameter is not specified, then all items are returned." Naming both
// SecurityGroupRuleId.N and MaxResults is refused, as EC2's pagination rule states for every
// describe ([ec2RefuseIDsWithMaxResults]).
func (p *EC2Plugin) describeSecurityGroupRules(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ruleIDs := extractIndexedParams(req.Params, "SecurityGroupRuleId")
	if awsErr := ec2RefuseIDsWithMaxResults(req.Params, "SecurityGroupRuleId", ruleIDs); awsErr != nil {
		return nil, awsErr
	}
	maxResults, awsErr := ec2MaxResults(req.Params, ec2MinPublishedMaxResults, ec2MaxPublishedMaxResults1000)
	if awsErr != nil {
		return nil, awsErr
	}
	if err := ec2SecurityGroupRuleFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)
	pr := pageRequest{
		Op: "DescribeSecurityGroupRules", Filter: filters,
		Token: req.Params["NextToken"], Size: maxResults,
	}
	if err := pr.validate(); err != nil {
		return nil, ec2InvalidNextTokenError(req.Params["NextToken"])
	}
	ids := newEC2IDFilter(ruleIDs, ec2SecurityGroupRuleIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	keys, err := p.state.List(context.Background(), ec2Namespace, "sg:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeSecurityGroupRules list: %w", err)
	}
	var matched []ec2SGRule
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var sg EC2SecurityGroup
		if json.Unmarshal(data, &sg) != nil {
			continue
		}
		for _, r := range ec2SecurityGroupRules(sg) {
			if !ids.match(r.ID) || !ec2SGRuleMatchesFilters(r, filters) {
				continue
			}
			matched = append(matched, r)
		}
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	res, err := page(matched, func(r ec2SGRule) cursorKey { return cursorKey{Sort: r.GroupID, Tie: r.ID} }, pr)
	if err != nil {
		return nil, ec2InvalidNextTokenError(req.Params["NextToken"])
	}
	type response struct {
		XMLName   xml.Name        `xml:"DescribeSecurityGroupRulesResponse"`
		XMLNS     string          `xml:"xmlns,attr"`
		Rules     []ec2SGRuleItem `xml:"securityGroupRuleSet>item"`
		NextToken string          `xml:"nextToken,omitempty"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", NextToken: res.Next}
	for _, r := range res.Items {
		resp.Rules = append(resp.Rules, ec2SGRuleXML(r, reqCtx.Region))
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// ec2SGRuleUpdate is one SecurityGroupRule.N of a ModifySecurityGroupRules request.
type ec2SGRuleUpdate struct {
	// ID is SecurityGroupRuleId, the rule to change.
	ID string
	// Perm is the rule's new content as a single-source permission, its CIDR canonical.
	Perm EC2IPPermission
	// Description is the rule's new description.
	Description string
}

// ec2ParseSGRuleUpdates reads ModifySecurityGroupRules' SecurityGroupRule.N, each a
// SecurityGroupRuleUpdate: a SecurityGroupRuleId and a SecurityGroupRuleRequest holding the
// rule's whole new content (API_SecurityGroupRuleRequest). The request must name exactly one of
// CidrIpv4, CidrIpv6, PrefixListId and ReferencedGroupId, as a rule has one source: none is
// MissingParameter and two are InvalidParameterCombination, codes of EC2's common-error table
// with messages of substrate's own. A CIDR is canonicalised as an authorize canonicalises it, and
// a port that is not an integer is InvalidParameterValue.
func ec2ParseSGRuleUpdates(params map[string]string, accountID string) ([]ec2SGRuleUpdate, *AWSError) {
	var updates []ec2SGRuleUpdate
	for n := 1; ; n++ {
		prefix := "SecurityGroupRule." + strconv.Itoa(n) + "."
		body := prefix + "SecurityGroupRule."
		id, hasID := params[prefix+"SecurityGroupRuleId"]
		protocol, hasProtocol := params[body+"IpProtocol"]
		if !hasID && !hasProtocol {
			return updates, nil
		}
		if id == "" {
			return nil, ec2MissingParameter(prefix + "SecurityGroupRuleId")
		}
		if protocol == "" {
			return nil, ec2MissingParameter(body + "IpProtocol")
		}
		perm := EC2IPPermission{IPProtocol: protocol}
		for _, port := range []struct {
			name string
			into *int
		}{{"FromPort", &perm.FromPort}, {"ToPort", &perm.ToPort}} {
			raw, ok := params[body+port.name]
			if !ok || raw == "" {
				continue
			}
			n, err := strconv.Atoi(raw)
			if err != nil {
				// The reason sentence is substrate's own wording; no AWS source publishes one.
				return nil, ec2InvalidParameterValue(body+port.name, raw, "The port must be an integer.")
			}
			*port.into = n
		}
		description := params[body+"Description"]
		sources := 0
		if cidr := params[body+"CidrIpv4"]; cidr != "" {
			canonical, err := ec2CanonicalRuleCIDR(cidr, false)
			if err != nil {
				return nil, ec2MalformedCIDRError(cidr)
			}
			perm.IPRanges = []string{canonical}
			sources++
		}
		if cidr := params[body+"CidrIpv6"]; cidr != "" {
			canonical, err := ec2CanonicalRuleCIDR(cidr, true)
			if err != nil {
				return nil, ec2MalformedCIDRError(cidr)
			}
			perm.IPv6Ranges = []string{canonical}
			sources++
		}
		if pl := params[body+"PrefixListId"]; pl != "" {
			perm.PrefixListIDs = []string{pl}
			sources++
		}
		if group := params[body+"ReferencedGroupId"]; group != "" {
			perm.UserIDGroupPairs = []EC2UserIDGroupPair{{GroupID: group, UserID: accountID, Description: description}}
			sources++
		}
		switch {
		case sources == 0:
			return nil, &AWSError{
				Code: "MissingParameter",
				Message: "The security group rule must specify one of CidrIpv4, CidrIpv6, " +
					"PrefixListId or ReferencedGroupId",
				HTTPStatus: http.StatusBadRequest,
			}
		case sources > 1:
			return nil, &AWSError{
				Code: "InvalidParameterCombination",
				Message: "The security group rule may specify only one of CidrIpv4, CidrIpv6, " +
					"PrefixListId and ReferencedGroupId",
				HTTPStatus: http.StatusBadRequest,
			}
		}
		updates = append(updates, ec2SGRuleUpdate{ID: id, Perm: perm, Description: description})
	}
}

// modifySecurityGroupRules replaces the content of the security group rules it names, each
// keeping its sgr- ID (#1490).
//
// The rule is found among the group's rules in either direction, and stays in its direction. Its
// old source is removed from the stored permissions and the new single-source permission is
// added, so a rule another rule already holds is InvalidPermission.Duplicate, as an authorize
// of it would be. The rule's tags carry over. Its ID is recorded in the group's
// [EC2SecurityGroupRuleMeta] when it is not the one its new content derives, which is what keeps
// it stable: a later read derives every unrecorded ID from content, and this one reads back as
// recorded. Every update is applied to one copy of the group, written once, so a refusal of any
// update writes nothing.
//
// API_ModifySecurityGroupRules' response is return alone, and its Errors section is the common
// error types, so the codes above are the ones the same condition answers on the operations
// that publish them.
func (p *EC2Plugin) modifySecurityGroupRules(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	p.sgMu.Lock()
	defer p.sgMu.Unlock()
	sgID := req.Params["GroupId"]
	key := "sg:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + sgID
	var data []byte
	if sgID != "" {
		var err error
		if data, err = p.state.Get(context.Background(), ec2Namespace, key); err != nil {
			return nil, fmt.Errorf("ec2 modifySecurityGroupRules get: %w", err)
		}
	}
	if err := ec2RequireNamedResource(ec2SecurityGroupIDKind, "GroupId", sgID, data != nil); err != nil {
		return nil, err
	}
	var sg EC2SecurityGroup
	if err := json.Unmarshal(data, &sg); err != nil {
		return nil, fmt.Errorf("ec2 modifySecurityGroupRules unmarshal: %w", err)
	}
	updates, awsErr := ec2ParseSGRuleUpdates(req.Params, reqCtx.AccountID)
	if awsErr != nil {
		return nil, awsErr
	}
	if len(updates) == 0 {
		return nil, ec2MissingParameter("SecurityGroupRule")
	}
	requireGroup := func(perm EC2IPPermission) error {
		return p.requireReferencedGroups(reqCtx, sg.VPCID, []EC2IPPermission{perm})
	}
	for _, u := range updates {
		if err := ec2ApplySGRuleUpdate(&sg, u, requireGroup); err != nil {
			return nil, err
		}
	}
	ec2PruneRuleMeta(&sg)
	out, err := json.Marshal(sg)
	if err != nil {
		return nil, fmt.Errorf("ec2 modifySecurityGroupRules marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ec2Namespace, key, out); err != nil {
		return nil, fmt.Errorf("ec2 modifySecurityGroupRules state.Put: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"ModifySecurityGroupRulesResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// requireReferencedGroups refuses a rule whose referenced group, named by ID and owned by the
// caller's account, is no security group of the account in the region, with InvalidGroup.NotFound
// — EC2's observed answer to AuthorizeSecurityGroupIngress naming such a group, "The security
// group 'sg-…' does not exist" (hashicorp/terraform-provider-aws#39473). The message is the group
// kind's. A pair naming another account's group is not checked, since substrate holds no other
// account's groups to check it against.
//
// A same-account pair named by a GroupName that resolved to no group of vpcID — the default-VPC
// form, and the legacy SourceSecurityGroupName — is InvalidGroup.NotFound too, in the form EC2 is
// observed to answer it, "The security group 'name' does not exist in VPC 'vpc-…'"
// (aws/aws-sdk#402).
func (p *EC2Plugin) requireReferencedGroups(reqCtx *RequestContext, vpcID string, perms []EC2IPPermission) error {
	for _, perm := range perms {
		for _, pair := range perm.UserIDGroupPairs {
			if pair.UserID != "" && pair.UserID != reqCtx.AccountID {
				continue
			}
			if pair.GroupID == "" {
				if pair.GroupName == "" {
					continue
				}
				return &AWSError{
					Code:       "InvalidGroup.NotFound",
					Message:    "The security group '" + pair.GroupName + "' does not exist in VPC '" + vpcID + "'",
					HTTPStatus: http.StatusBadRequest,
				}
			}
			data, err := p.state.Get(context.Background(), ec2Namespace,
				"sg:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+pair.GroupID)
			if err != nil {
				return fmt.Errorf("ec2 referenced security group get: %w", err)
			}
			if err := ec2RequireResource(ec2SecurityGroupIDKind, pair.GroupID, data != nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// ec2ApplySGRuleUpdate applies one update to sg; see [EC2Plugin.modifySecurityGroupRules]. The
// rule's ID is checked first, then requireGroup checks the new content's referenced group.
func ec2ApplySGRuleUpdate(sg *EC2SecurityGroup, u ec2SGRuleUpdate, requireGroup func(EC2IPPermission) error) error {
	if !ec2SecurityGroupRuleIDKind.wellFormed(u.ID) {
		return ec2SecurityGroupRuleIDKind.malformedError(u.ID)
	}
	var rule *ec2SGRule
	rules := ec2SecurityGroupRules(*sg)
	for i := range rules {
		if rules[i].ID == u.ID {
			rule = &rules[i]
			break
		}
	}
	if rule == nil {
		return ec2SecurityGroupRuleIDKind.notFoundError(u.ID)
	}
	if err := requireGroup(u.Perm); err != nil {
		return err
	}
	stored := &sg.IngressRules
	if rule.Egress {
		stored = &sg.EgressRules
	}
	var remaining []EC2IPPermission
	if rule.Kind == ec2SGRuleSourceNone {
		remaining, _ = ec2DropSourcelessPermission(*stored, rule.Perm)
	} else {
		remaining, _ = ec2RevokePermissions(*stored, []EC2IPPermission{rule.Perm})
	}
	if ec2AuthorizeDuplicates(remaining, []EC2IPPermission{u.Perm}) {
		return ec2PermissionDuplicateError()
	}
	*stored = append(remaining, u.Perm)
	newKey := ec2SplitPermission(rule.Egress, u.Perm)[0].Key
	tags := rule.Tags
	// A stored permission an earlier build duplicated can still hold the old key; its metadata
	// stays, less the ID, which the modified rule takes with it.
	if ec2SGRuleMetaKeys(ec2SecurityGroupRules(*sg))[rule.Key] {
		ec2SetRuleMeta(sg, rule.Key, func(m *EC2SecurityGroupRuleMeta) { m.ID = "" })
	} else {
		delete(sg.RuleMeta, rule.Key)
	}
	ec2SetRuleMeta(sg, newKey, func(m *EC2SecurityGroupRuleMeta) {
		m.Tags = tags
		m.Description = ""
		if len(u.Perm.UserIDGroupPairs) == 0 {
			m.Description = u.Description
		}
		m.ID = ""
		if rule.ID != ec2DerivedSGRuleID(sg.GroupID, newKey, 0) {
			m.ID = rule.ID
		}
	})
	return nil
}

// ec2SGRuleTaggable resolves an sgr- ID for CreateTags and DeleteTags to the record of the group
// holding the rule, scanning the account's groups in the region, since a rule has no record of
// its own. An ID naming no rule resolves to no record, which both callers treat as they treat an
// absent resource of any other type; a failure to list, read or decode the groups is carried in
// lookupErr, so the caller answers it rather than reporting success, and authorization refuses
// rather than deciding against tags it could not read. The ARN is the security-group-rule one,
// [ec2SGRuleARN].
func ec2SGRuleTaggable(state StateManager, accountID, region, id string) ec2Taggable {
	target := ec2Taggable{arnType: "security-group-rule", arnID: id, sgRuleID: id}
	keys, err := state.List(context.Background(), ec2Namespace, "sg:"+accountID+"/"+region+"/")
	if err != nil {
		target.lookupErr = fmt.Errorf("ec2 security group rule %s list groups: %w", id, err)
		return target
	}
	for _, k := range keys {
		data, getErr := state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil {
			target.lookupErr = fmt.Errorf("ec2 security group rule %s get %s: %w", id, k, getErr)
			return target
		}
		if data == nil {
			continue
		}
		var sg EC2SecurityGroup
		if err := json.Unmarshal(data, &sg); err != nil {
			target.lookupErr = fmt.Errorf("ec2 security group rule %s unmarshal %s: %w", id, k, err)
			return target
		}
		for _, r := range ec2SecurityGroupRules(sg) {
			if r.ID == id {
				target.stateKey = k
				return target
			}
		}
	}
	return target
}

// ec2LoadSGRule reads the group record at stateKey and the rule of it id names. An absent group
// is an absent rule; a read that fails is an error.
func ec2LoadSGRule(state StateManager, stateKey, id string) (EC2SecurityGroup, *ec2SGRule, error) {
	var sg EC2SecurityGroup
	data, err := state.Get(context.Background(), ec2Namespace, stateKey)
	if err != nil {
		return sg, nil, fmt.Errorf("ec2 security group rule %s get: %w", id, err)
	}
	if data == nil {
		return sg, nil, nil
	}
	if err := json.Unmarshal(data, &sg); err != nil {
		return sg, nil, fmt.Errorf("ec2 security group rule %s unmarshal: %w", id, err)
	}
	for _, r := range ec2SecurityGroupRules(sg) {
		if r.ID == id {
			return sg, &r, nil
		}
	}
	return sg, nil, nil
}

// ec2SGRuleTags returns the tags of the rule id names, held in its group's record at stateKey,
// and whether the rule was found.
func ec2SGRuleTags(state StateManager, stateKey, id string) ([]EC2Tag, bool, error) {
	_, rule, err := ec2LoadSGRule(state, stateKey, id)
	if err != nil || rule == nil {
		return nil, false, err
	}
	return rule.Tags, true, nil
}

// ec2ApplySGRuleTags merges tags into, or with remove deletes their keys from, the tags of the
// rule id names, and writes its group's record back. The rule's ID is recorded beside its tags
// when it is not the unsalted derivation, for the reason [ec2RecordAddedRules] gives.
func ec2ApplySGRuleTags(state StateManager, stateKey, id string, tags []EC2Tag, remove bool) error {
	sg, rule, err := ec2LoadSGRule(state, stateKey, id)
	if err != nil || rule == nil {
		return err
	}
	merged := ec2MergeTags(rule.Tags, tags)
	if remove {
		drop := make(map[string]bool, len(tags))
		for _, t := range tags {
			drop[t.Key] = true
		}
		merged = slices.DeleteFunc(slices.Clone(rule.Tags), func(t EC2Tag) bool { return drop[t.Key] })
	}
	ec2SetRuleMeta(&sg, rule.Key, func(m *EC2SecurityGroupRuleMeta) {
		m.Tags = merged
		if rule.ID != ec2DerivedSGRuleID(sg.GroupID, rule.Key, 0) {
			m.ID = rule.ID
		}
	})
	ec2PruneRuleMeta(&sg)
	out, err := json.Marshal(sg)
	if err != nil {
		return fmt.Errorf("ec2 security group rule %s marshal: %w", id, err)
	}
	if err := state.Put(context.Background(), ec2Namespace, stateKey, out); err != nil {
		return fmt.Errorf("ec2 security group rule %s state.Put: %w", id, err)
	}
	return nil
}
