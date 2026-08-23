package emulator

import (
	"context"
	"fmt"
)

// elbServiceName is the service an ELBv2 request carries, which is [ELBPlugin.Name] and
// the prefix of every elasticloadbalancing: action.
//
// It is deliberately not [elbNamespace]: that is the shorter "elb" the state keys are
// written under, and using it to switch on req.Service would match nothing.
const elbServiceName = "elasticloadbalancing"

// elbCreateActionCondKey is the condition key that tells an
// elasticloadbalancing:AddTags statement which creating operation the tags are being
// applied by.
//
// AWS: "In the IAM policy definition for the elasticloadbalancing:AddTags action, you
// can use the Condition element with the elasticloadbalancing:CreateAction condition key
// to give tagging permissions to the action that creates the resource." It is a
// request-level key, so it lives in the condition context beside aws:RequestTag/* rather
// than on a resource — and it is **absent** from a direct AddTags, which is what makes
// the bundled ELBTaggingPolicy mean what it says.
//
// Provenance: the key is documented in the ELB user guide's "Tag resources during
// creation" page and is **not** in the same guide's own list of ELB-specific condition
// keys (which names ListenerProtocol, SecurityPolicy, Scheme, SecurityGroup, Subnet and
// ResourceTag). Both Service Authorization Reference pages for ELB render their key table
// in JavaScript and were unreachable. The value is the bare operation name —
// AWS's examples write "elasticloadbalancing:CreateAction": "CreateTargetGroup" — not a
// service-prefixed action.
const elbCreateActionCondKey = "elasticloadbalancing:CreateAction"

// elbAddTagsAction is the action a tagged create is authorized against a second time.
const elbAddTagsAction = "elasticloadbalancing:AddTags"

// elbResourceTagPrefix is the service-specific duplicate of aws:ResourceTag/ that ELB
// publishes.
//
// AWS: "The elasticloadbalancing:ResourceTag/{{key}} condition key is specific to Elastic
// Load Balancing. All mutating actions support this condition key." A policy written the
// way AWS writes it sees nothing unless a resource's tags are reported under this prefix
// too; see [authzResourceTagPrefixes].
const elbResourceTagPrefix = "elasticloadbalancing:ResourceTag/"

// elbAuthzARNParams are the request members that name an ELB resource by ARN, in the
// order they are resolved.
//
// ResourceArns.member.N is first because it is the plural one — the three tagging
// operations name up to 20 resources through it, and each must be allowed. The four
// singular members follow; no ELB operation carries two of them, so their relative order
// is inert today and fixed anyway, so a denial names the same resource on every run.
//
// Deliberately absent: `Names.member.N` on DescribeLoadBalancers and
// DescribeTargetGroups, which names resources by name rather than ARN. Resolving those
// would put the two describes on a resource-scoped path, and whether ELB's describes
// support resource-level permissions could not be verified — the Service Authorization
// Reference pages that would say are JavaScript-rendered and unreachable. Leaving them
// at "*" is the direction that cannot invent a grant.
var elbAuthzARNParams = []string{
	"LoadBalancerArn",
	"TargetGroupArn",
	"ListenerArn",
	"RuleArn",
}

// elbAuthzResources returns every ELB resource the request names by ARN, each paired
// with its own tags, or nil when it names none.
//
// Before this, every ELB request was authorized against `*` — the default arm of
// [AuthController.buildResourceARN] — so a policy scoped to one load balancer's ARN
// matched nothing at all ([resourceMatches] compares a statement's Resource against the
// request's resource string, and `*` is a literal there). A caller holding
// `elasticloadbalancing:DeleteLoadBalancer` on one ARN was refused on every ARN, and a
// tag-scoped condition had no tags to read. Both are fixed by naming the resource the
// request actually names, which is the same correction #744 made for EC2 and #660 for
// Organizations.
//
// A named ARN that resolves to nothing in state is still returned, with no tags: the
// resource the request names is the resource the decision is about, and substituting `*`
// for an unknown ARN would let a bogus ARN reach a statement scoped to `*` that the real
// one would not have matched. The handler refuses it afterwards on its own terms.
func elbAuthzResources(state StateManager, reqCtx *RequestContext, req *AWSRequest) []authzResource {
	arns := extractIndexedParams(req.Params, "ResourceArns.member")
	for _, param := range elbAuthzARNParams {
		if arn := req.Params[param]; arn != "" {
			arns = append(arns, arn)
		}
	}
	if len(arns) == 0 {
		return nil
	}

	scope := reqCtx.AccountID + "/" + reqCtx.Region
	out := make([]authzResource, 0, len(arns))
	for _, arn := range arns {
		out = append(out, authzResource{ARN: arn, Tags: elbAuthzTagsFor(state, scope, arn)})
	}
	return out
}

// elbAuthzTagsFor reads the tags on one ELB resource, or nil when the ARN names nothing
// or the read fails.
//
// A failed read is nil rather than an error for the same reason the tag readers in
// [AuthController.resourceTagsFor] are: a condition on a tag that cannot be read is
// unsatisfied, which denies, and that is the safe direction.
func elbAuthzTagsFor(state StateManager, scope, arn string) map[string]string {
	res, awsErr, err := elbResolveTaggedResource(state, scope, arn)
	if err != nil || awsErr != nil || res == nil {
		return nil
	}
	return elbTagsToMap(res.tags)
}

// elbTagsToMap converts []ELBTag to a map[string]string.
func elbTagsToMap(tags []ELBTag) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}
	return m
}

// elbCreateResourceKinds maps each creating operation to the ARN resource type the tags
// it applies land on.
//
// These are the four operations the bundled ELBTaggingPolicy names in its
// elasticloadbalancing:CreateAction condition, which is also every ELBv2 operation
// accepting Tags.member.N. The listener and rule types are AWS's own spellings rather
// than the ones substrate's ARNs carry; see [elbKindListener] and #774.
var elbCreateResourceKinds = map[string]string{
	"CreateLoadBalancer": elbKindLoadBalancer,
	"CreateTargetGroup":  elbKindTargetGroup,
	"CreateListener":     elbKindListener,
	"CreateRule":         elbKindRule,
}

// elbAuthzCreateTagsPass returns the resource ARN a tagged create must additionally be
// authorized against on elasticloadbalancing:AddTags, or "" when the request needs no
// such pass.
//
// AWS: "If tags are specified in the resource-creating action, additional authorization
// is required on the elasticloadbalancing:AddTags action to verify if users have
// permissions to apply tags to the resources being created." And the converse, which is
// why this can answer "": "The elasticloadbalancing:AddTags action is only evaluated if
// tags are applied during the resource-creating action. Therefore, a user that has
// permissions to create a resource … does not require permissions to use the
// elasticloadbalancing:AddTags action if no tags are specified in the request."
//
// The ARN is the wildcard for the created resource's type —
// arn:aws:elasticloadbalancing:<region>:<account>:<type>/*. **That is substrate's
// reading**, on the same reasoning as [ec2CreateTagsPass.resourceARNs]: the resource does
// not exist yet, and AWS's own example policies for this key write the Resource of the
// AddTags statement as `*` or as a type wildcard, so resolving it to anything narrower
// would make those policies mean something else. Unlike EC2 a create here makes one
// resource of one type, so there is one ARN rather than a list.
func elbAuthzCreateTagsPass(reqCtx *RequestContext, req *AWSRequest) string {
	kind, ok := elbCreateResourceKinds[req.Operation]
	if !ok {
		return ""
	}
	if len(extractELBTags(req.Params, "Tags.member")) == 0 {
		return ""
	}
	return "arn:aws:elasticloadbalancing:" + reqCtx.Region + ":" + reqCtx.AccountID + ":" + kind + "/*"
}

// elbTagsForCreate validates the Tags.member.N a create carries and returns the tag set
// to store on the new resource.
//
// It is called by each of the four creates before the record is written, so a tag the
// request cannot legally apply refuses the create rather than leaving a resource behind
// carrying it — the rollback rule [ec2CheckReservedTagKeys] documents. The tags are
// returned rather than written, because the create is what writes the record.
//
// refuseDuplicateKeys is a parameter rather than a rule because AWS makes it one:
// DuplicateTagKeys is listed on CreateLoadBalancer and on none of CreateTargetGroup,
// CreateListener or CreateRule. Passing false is what keeps substrate from inventing a
// code three of the four do not publish; the duplicate then resolves last-wins through
// [elbMergeTags], which is the only other thing it can do.
func elbTagsForCreate(req *AWSRequest, refuseDuplicateKeys bool) ([]ELBTag, *AWSError) {
	tags := extractELBTags(req.Params, "Tags.member")
	if len(tags) == 0 {
		return nil, nil
	}
	if refuseDuplicateKeys {
		if awsErr := elbCheckDuplicateTagKeys(tags); awsErr != nil {
			return nil, awsErr
		}
	}
	if awsErr := elbCheckCreateTags(tags); awsErr != nil {
		return nil, awsErr
	}
	return elbMergeTags(nil, tags), nil
}

// elbLoadTagsByARN reads a resource's tags straight from state, for the tests that
// assert what a create or a tagging call persisted.
func elbLoadTagsByARN(state StateManager, scope, arn string) ([]ELBTag, error) {
	kind := elbResourceKindFromARN(arn)
	if kind == "" {
		return nil, fmt.Errorf("elb load tags: %q names no ELB resource type", arn)
	}
	keys, err := state.List(context.Background(), elbNamespace, elbStateKeyPrefix(kind, scope))
	if err != nil {
		return nil, fmt.Errorf("elb load tags list: %w", err)
	}
	for _, k := range keys {
		data, getErr := state.Get(context.Background(), elbNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		res := elbDecodeTaggedResource(kind, k, data)
		if res == nil || res.arn != arn {
			continue
		}
		return res.tags, nil
	}
	return nil, fmt.Errorf("elb load tags: %q names nothing in state", arn)
}

// elbAuthzRequestTags reads the tags a request asks to apply or remove, for
// [addRequestTags].
//
// AddTags and the four creates all spell them Tags.member.N.Key / .Value, so one reader
// serves all five. RemoveTags names keys under TagKeys.member.N and supplies no values,
// and those are recorded with an empty value — the same reading substrate already applies
// to EC2's DeleteTags, and for the same reason: [conditionMatches] reads an absent key as
// the empty string too, so the two are indistinguishable to every operator, while the key
// still reaches aws:TagKeys, which is what a "may only remove approved tags" policy is
// written against.
func elbAuthzRequestTags(req *AWSRequest) map[string]string {
	tags := extractELBTags(req.Params, "Tags.member")
	keys := extractIndexedParams(req.Params, "TagKeys.member")
	if len(tags) == 0 && len(keys) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags)+len(keys))
	for _, t := range tags {
		out[t.Key] = t.Value
	}
	for _, k := range keys {
		out[k] = ""
	}
	return out
}
