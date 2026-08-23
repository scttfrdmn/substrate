package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
)

// ec2AuthzRunInstancesResources returns every resource a RunInstances request
// names, each paired with its own tags. It returns nil for every other EC2
// operation, which is what sends them down buildResourceARNs' single-resource
// path.
//
// RunInstances is the EC2 action the Service Authorization Reference marks with
// the most required resource types — image*, instance*, network-interface*,
// security-group* and subnet* — and AWS evaluates the entire statement against
// every one of them. Substrate authorized it against a single ARN derived from
// InstanceId.1, a parameter a launch never carries, so every launch was decided
// against the literal string "*" (#662).
//
// That broke in both directions. resourceMatches passes the statement's own
// Resource entry to globMatch as the *pattern*, so "*" as the request resource
// matches only a statement whose Resource begins with "*": a least-privilege
// policy naming the five ARNs could not grant a launch at all, and a broad Allow
// on "*" beside an ARN-scoped Deny ignored the Deny entirely — a guardrail
// written to fence off a subnet, AMI or security group was silently inert.
//
// Order is fixed: image, subnet, security groups, network interfaces, instance.
// The first resource a policy does not allow is the one the denial names, so a
// fixed order makes that message deterministic and the decision replay-stable.
// Request-named resources come first because those are the ARNs a caller can act
// on; the two synthesized wildcards come last.
//
// A member the request omits is skipped rather than resolved to "*": "*" for an
// absent ID would widen the policy the caller wrote instead of narrowing it.
//
// The exception is a launch that omits SubnetId, whose subnet and security group are
// substrate's default-VPC defaults. Those are resources the launch *will land in*,
// not resources it fails to name, so they are resolved before the decision rather
// than skipped — otherwise a caller defeats a subnet-scoped guardrail by leaving the
// parameter out, which is the one thing such a guardrail exists to prevent (#673).
// When there is no default VPC yet the launch is about to create both, so they are
// the wildcards subnet/* and security-group/*, on the same reasoning instance/* and
// network-interface/* already are.
//
// Three of the resources also carry condition keys of their own — ec2:Subnet on the
// network interface, ec2:Vpc on the interface, the subnet and each security group —
// which is the guardrail AWS itself documents for fencing off a subnet, and which
// #673's docs pointed at while substrate had neither key (#692).
func ec2AuthzRunInstancesResources(state StateManager, reqCtx *RequestContext, req *AWSRequest) []authzResource {
	if req.Operation != "RunInstances" {
		return nil
	}
	// No emptiness guard: every launch names at least instance/*, and one that named
	// nothing else still resolves its subnet from the default VPC, so RunInstances is
	// never decided against the literal "*" any more. The guard that used to send a
	// parameterless launch down buildResourceARNs' single-resource path was removed
	// with #673 rather than left as a branch nothing can reach.
	launch := ec2AuthzResolveLaunch(state, reqCtx, req)

	acct := reqCtx.AccountID
	region := reqCtx.Region
	var out []authzResource

	if launch.imageID != "" {
		out = append(out, authzResource{
			// No account field. The Service Authorization Reference gives the image
			// ARN as arn:${Partition}:ec2:${Region}::image/${ImageId}, and an AMI is
			// shareable, so the field is empty rather than omitted by accident. It has
			// to stay empty: arnAccountID reads an empty account as "no account", not
			// as "this account", and a policy naming the ARN AWS documents would not
			// match one carrying an account ID.
			ARN:  "arn:aws:ec2:" + region + "::image/" + launch.imageID,
			Tags: ec2AuthzTagsFor(state, ec2ImageStateKey(acct, region, launch.imageID)),
		})
	}
	// vpcID is the VPC this launch lands in, and it is what ec2:Vpc reports. The
	// default-VPC path resolved it already; a named or template subnet carries it on
	// the record read for that subnet's tags, so it costs no extra state read.
	vpcID := launch.vpcID
	switch {
	case launch.subnetID != "":
		tags, subnetVPC := ec2AuthzRecordFor(state, "subnet:"+acct+"/"+region+"/"+launch.subnetID)
		if subnetVPC != "" {
			vpcID = subnetVPC
		}
		out = append(out, authzResource{
			ARN:  ec2SubnetARN(acct, region, launch.subnetID),
			Tags: tags,
			// The reference lists ec2:Vpc on RunInstances' subnet resource as well as
			// on its network interface — a subnet belongs to exactly one VPC, so the
			// key means the same thing here.
			Context: ec2AuthzVpcContext(acct, region, vpcID),
		})
	case launch.subnetWildcard:
		// The default subnet this launch is about to create. No tags: it does not
		// exist, so an aws:ResourceTag condition about it cannot be satisfied — which
		// is the honest answer, since the subnet substrate mints carries no tags
		// either. It does carry ec2:Vpc when the default VPC exists, because the
		// subnet it mints will be in that VPC.
		out = append(out, authzResource{
			ARN:     "arn:aws:ec2:" + region + ":" + acct + ":subnet/*",
			Context: ec2AuthzVpcContext(acct, region, vpcID),
		})
	}
	for _, sgID := range launch.securityGroupIDs {
		tags, sgVPC := ec2AuthzRecordFor(state, "sg:"+acct+"/"+region+"/"+sgID)
		out = append(out, authzResource{
			ARN:  "arn:aws:ec2:" + region + ":" + acct + ":security-group/" + sgID,
			Tags: tags,
			// The group's *own* VPC, not the launch's: the reference lists ec2:Vpc on
			// the security-group resource, where it means the VPC the group belongs
			// to, and a group in another VPC is exactly the mismatch a policy scoped
			// this way is written to catch.
			Context: ec2AuthzVpcContext(acct, region, sgVPC),
		})
	}
	if launch.securityGroupWildcard {
		// No record to read, and this wildcard is set only when there is no default
		// VPC at all — so there is no VPC to report either.
		out = append(out, authzResource{ARN: "arn:aws:ec2:" + region + ":" + acct + ":security-group/*"})
	}
	// Every launch attaches an interface, so network-interface* is always evaluated.
	// An interface the launch creates has no ID yet, so it is the wildcard; one the
	// request brings by ID is named, because a policy can meaningfully scope to it.
	// Neither carries tags: substrate stores no standalone taggable ENI record, and
	// ec2TaggableResource has no eni- arm to read one through.
	//
	// This is the resource AWS's own subnet guardrail is written against, so it is
	// where ec2:Subnet and ec2:Vpc go — both of them, on every interface the launch
	// touches, named or wildcard, since they describe where the interface lands
	// rather than which interface it is.
	eniContext := ec2AuthzInterfaceContext(acct, region, launch.subnetID, vpcID)
	if len(launch.networkInterfaceIDs) == 0 {
		out = append(out, authzResource{
			ARN:     "arn:aws:ec2:" + region + ":" + acct + ":network-interface/*",
			Context: eniContext,
		})
	}
	for _, eniID := range launch.networkInterfaceIDs {
		out = append(out, authzResource{
			ARN:     "arn:aws:ec2:" + region + ":" + acct + ":network-interface/" + eniID,
			Context: eniContext,
		})
	}
	// The instance does not exist yet, so its ARN is the wildcard AWS's own
	// least-privilege RunInstances examples write. Including it is what makes a
	// policy that omits instance* a denial, which is the answer real AWS gives.
	out = append(out, authzResource{ARN: "arn:aws:ec2:" + region + ":" + acct + ":instance/*"})

	return out
}

// ec2AuthzTagResources returns every resource a CreateTags or DeleteTags request
// names, each paired with its own tags. It returns nil for every other EC2
// operation, which is what leaves them on buildResourceARNs' single-resource path.
//
// CreateTags accepts up to 1000 resource IDs of mixed types, and substrate decided
// all of them against one ARN built from InstanceId.1 — a parameter neither
// operation carries — so every tagging call was decided against the literal string
// "*" (#674). A Deny scoped to one instance, subnet or security group was
// therefore inert, and a least-privilege Allow naming those ARNs could not grant a
// tagging call at all: resourceMatches passes the statement's Resource to
// globMatch as the pattern, so "*" as the request resource matches only a
// statement whose Resource itself starts with "*".
//
// Neither action has a *required* resource type — the Service Authorization
// Reference marks zero of the 105 it lists for either one. Authorizing against
// every named resource instead rests on AWS's general rule for an action naming
// several resources, which its Organizations counterpart (#660) and RunInstances
// (#662) already follow here, and on AWS's own scoped tagging examples, which
// write instance/* as the Resource of an ec2:CreateTags statement and would be
// pointless if only one resource of a batch were evaluated.
//
// Order is the request's own ResourceId.N order, which makes the denial message —
// which names the first resource the policy does not allow — deterministic and
// replay-stable.
//
// An ID whose prefix names no resource type substrate can tag is skipped, not
// denied and not widened to "*". Skipping follows
// [ec2AuthzRunInstancesResources]' rule for a resource the request does not name,
// and it is the ID's own handler that owes the answer: AWS documents an
// unparseable tagging ID as InvalidID — "The specified ID for the resource you are
// trying to tag is not valid" — not AccessDenied, and substrate's handler answers
// exactly that (#708). A request naming only such IDs returns nil and is refused by
// the handler rather than by the policy.
//
// A well-formed pg- or key- ID that resolves to no record is skipped for a different
// reason: AWS's ARN for a placement group and a key pair is by *name*, so with no
// record there is no name to build one from. Inventing an ID-form ARN would decide the
// call against a string AWS never emits; the handler treats an absent resource as a
// no-op regardless, so nothing is authorized away.
func ec2AuthzTagResources(state StateManager, reqCtx *RequestContext, req *AWSRequest) []authzResource {
	if req.Operation != "CreateTags" && req.Operation != "DeleteTags" {
		return nil
	}
	acct := reqCtx.AccountID
	region := reqCtx.Region
	var out []authzResource
	for _, id := range extractIndexedParams(req.Params, "ResourceId") {
		target, ok := ec2TaggableResource(state, reqCtx, id)
		if !ok || !target.resolved() {
			continue
		}
		out = append(out, authzResource{
			ARN: target.arn(region, acct),
			// The same state key the handler will read and write, so the tags a
			// condition is evaluated against are the ones the call is about to change.
			Tags: ec2AuthzTagsFor(state, target.stateKey),
		})
	}
	return out
}

// ec2AuthzIDParams are the request parameters that name the resource an EC2 operation
// acts on, in the order they are tried.
//
// Only four, and each is here because a bundled AWS managed policy conditions an action on
// the tags of the resource that parameter names (#730). Substrate resolved a single EC2
// resource from `InstanceId` alone, so every other operation was decided against the
// literal "*" — and a tag condition about it could not be evaluated at all, because there
// was no resource whose tags to read. That made both `ec2:ResourceTag/…` statements in
// AWS's own `AmazonECSServiceRolePolicy`-shaped cleanup grants inert:
// `DeleteInternetGateway`, `DeleteRoute`, `DeleteRouteTable` and `DeleteSecurityGroup` are
// exactly the four actions they govern.
//
// Order is fixed so the decision is replay-stable, and it does not matter in practice:
// no EC2 operation carries two of these four. `DeleteRoute` and `DeleteRouteTable` share
// `RouteTableId`, which is the resource both act on.
//
// Each is plural: [ec2AuthzNamedResources] resolves every ID a request names under one of
// them, not the first alone (#744).
//
// Deliberately absent, each for its own reason rather than by oversight:
//
//   - `NetworkInterfaceId`, which `AmazonEKSClusterPolicy` conditions
//     `ec2:DeleteNetworkInterface` on. Substrate stores no standalone taggable ENI record —
//     [ec2TaggableResource] has no `eni-` arm to read one through, as
//     [ec2AuthzRunInstancesResources] already records — so there are no tags to compare and
//     that statement stays inert. Fabricating an empty tag set would be worse than leaving
//     it: it would turn "substrate cannot tell" into "the tag is absent", which decides the
//     condition rather than admitting it cannot.
//   - `VpcId` and `SubnetId`, which several *creates* carry (`CreateSubnet`, `CreateRoute`,
//     `AttachInternetGateway`) as the container the new resource goes into rather than as
//     the resource acted on. Resolving one there would authorize a create against its
//     parent's ARN, which is not the ARN AWS evaluates it against.
//
// One more is overloaded rather than absent: `GroupId` names a *placement* group on
// `DescribePlacementGroups`, which reads `pg-` IDs through it. Nothing mis-resolves —
// [ec2TaggableResource] keys on the ID's prefix, so a `pg-` ID becomes a placement-group
// ARN and an `sg-` one a security-group ARN — but it does mean that operation is decided
// against the groups it names rather than against "*".
var ec2AuthzIDParams = []string{"InstanceId", "GroupId", "RouteTableId", "InternetGatewayId"}

// ec2AuthzNamedResources returns every resource an EC2 request names by ID under one of
// [ec2AuthzIDParams], each paired with its own tags. It returns nil for a request that
// names none, which is what leaves those on buildResourceARNs' single-resource path.
//
// It reads each parameter through [extractIndexedParams], so `GroupId` and `GroupId.1` are
// the same request — which is the form the handlers themselves accept, and it is what
// keeps the resources a policy is evaluated against the resources the handler will act on.
//
// **Every** named ID is resolved, not the first alone (#744). Deciding a
// `TerminateInstances` naming three instances against `InstanceId.1` left the other two
// unauthorized, so a policy allowing `Env=dev` instances and denying the rest permitted a
// call naming one dev instance and two production ones — provided the dev one came first.
// That is the #674-shaped gap [ec2AuthzTagResources] closed for tagging calls, on the same
// reading of AWS's rule for an action naming several resources that #660 and #662 follow
// here: every resource required for the action must be allowed.
//
// Order is fixed parameter order, then the request's own index order within a parameter,
// which makes the denial message — which names the first resource the policy does not
// allow — deterministic and replay-stable. The parameter order is inert today, since no EC2
// operation carries two of the four.
//
// An unresolvable ID mid-batch is **skipped**, not denied and not widened to "*", which is
// [ec2AuthzTagResources]' rule and for the same reason: there is no ARN to build, and the
// refusal the caller is owed is the handler's `NotFound`, not an `AccessDenied` naming a
// resource that does not exist. Skipping cannot launder a state change past a policy for
// the operation where it would matter most, because AWS documents `TerminateInstances` as
// all-or-nothing — "If you specify multiple instances and the request fails (for example,
// because of a single incorrect instance ID), none of the instances are terminated" — and
// substrate implements it with a resolve-everything pre-pass, so the bogus ID fails the
// whole call. `StopInstances` and `StartInstances` are *not* atomic, so there the batch is
// simply authorized as the batch of resources that exist.
//
// A request naming only unresolvable IDs returns nil and falls back to the wildcard it used
// before. That is the only safe direction: "*" as the request resource matches a statement
// whose own Resource starts with "*", so it can only ever narrow a grant, never widen one.
func ec2AuthzNamedResources(state StateManager, reqCtx *RequestContext, req *AWSRequest) []authzResource {
	acct := reqCtx.AccountID
	region := reqCtx.Region
	var out []authzResource
	for _, param := range ec2AuthzIDParams {
		for _, id := range extractIndexedParams(req.Params, param) {
			if id == "" {
				continue
			}
			target, ok := ec2TaggableResource(state, reqCtx, id)
			if !ok || !target.resolved() {
				// A prefix naming no taggable type, or a pg-/key- ID naming no record.
				// See [ec2Taggable.resolved] and the skip rule above.
				continue
			}
			out = append(out, authzResource{
				ARN: target.arn(region, acct),
				// The same state key the handler will read, so a tag condition is
				// evaluated against the tags of the resource the ARN names.
				Tags: ec2AuthzTagsFor(state, target.stateKey),
			})
		}
	}
	return out
}

// ec2CreateTagsAction is the action AWS authorizes a tag-carrying create against a
// second time, in addition to the create itself.
const ec2CreateTagsAction = "ec2:CreateTags"

// ec2CreateActionCondKey is the condition key that tells an ec2:CreateTags statement
// which creating operation the tags are being applied by.
//
// AWS: "In the IAM policy definition for the ec2:CreateTags action, use the Condition
// element with the ec2:CreateAction condition key to give tagging permissions to the
// action that creates the resource." It is a request-level key, so it lives in the
// condition context beside aws:RequestTag/* rather than on a resource — and it is
// **absent** from a direct CreateTags or DeleteTags, which is what makes AWS's own
// examples ("users cannot tag existing resources") work.
const ec2CreateActionCondKey = "ec2:CreateAction"

// ec2CreateTagsPass is the extra ec2:CreateTags authorization a create that applies
// tags requires, resolved from the request and any launch template it names.
//
// AWS: "If tags are specified in the resource-creating action, Amazon performs
// additional authorization on the ec2:CreateTags action to verify if users have
// permissions to create tags. Therefore, users must also have explicit permissions to
// use the ec2:CreateTags action." And the converse, which is why this type is a
// pointer that can be nil: "The ec2:CreateTags action is only evaluated if tags are
// applied during the resource-creating action".
type ec2CreateTagsPass struct {
	// resourceTypes are the TagSpecification resource types the create actually
	// applies tags to, in the order they were discovered — the request's own
	// TagSpecification.N order, then any scope a launch template supplies. A fixed
	// order makes the denial, which names the first type the policy does not allow,
	// deterministic and replay-stable.
	resourceTypes []string

	// templateTags are the tags a launch template supplies to this create. They are
	// held apart from the request's own because [addRequestTags] has already read
	// those from the params: the second pass's context is that map plus these, so the
	// two readings of the request cannot disagree.
	templateTags map[string]string
}

// ec2AuthzCreateTagsPass returns the tagging pass a create requires, or nil when the
// request applies no tags and therefore needs no ec2:CreateTags permission.
//
// It is deliberately not gated on a list of creating operation names. An operation
// that does not accept TagSpecification.N never carries one, so the general rule costs
// nothing — and a create added to substrate later is covered without anyone
// remembering to extend a list, which is the only direction that cannot produce a
// false allow. A direct CreateTags or DeleteTags carries its tags as Tag.N rather than
// TagSpecification.N, so it resolves to nil here and is authorized once, as itself.
//
// A specification that declares a resource type and carries no tags applies no tags,
// so it contributes nothing: that is AWS's "only evaluated if tags are applied" rule
// read at the level of the individual scope.
func ec2AuthzCreateTagsPass(state StateManager, reqCtx *RequestContext, req *AWSRequest) *ec2CreateTagsPass {
	pass := &ec2CreateTagsPass{}
	for n := 1; ; n++ {
		// The absent-or-empty ResourceType ends the walk, which is how the query
		// protocol terminates an indexed list and what ec2TagSpecificationTags does.
		rt, ok := req.Params[fmt.Sprintf("TagSpecification.%d.ResourceType", n)]
		if !ok || rt == "" {
			break
		}
		// The tags themselves are read through the single tag-on-create parser rather
		// than a second walk of the same params, so a scope the handler will apply and
		// a scope the decision authorizes cannot drift apart. It re-walks per type,
		// which is fine: a request carries a handful of specifications, not a page of
		// them.
		pass.addScope(rt, ec2LaunchTagsForResource(req.Params, rt))
	}

	// AWS: "The ec2:CreateTags action is also evaluated if tags are provided in a
	// launch template." Only RunInstances reads a template, and only the two scopes
	// substrate stores — the same two the launch itself applies.
	if req.Operation == "RunInstances" {
		if data := ec2AuthzTemplateData(state, reqCtx, req); data != nil {
			// Each scope is gated on whether the request named that scope itself,
			// mirroring the launch's replace-rather-than-merge precedence: a template's
			// tags are the ones the create applies only when the caller named none.
			// ec2HasUserTags is that gate for the instance scope for the same reason the
			// handler uses it — substrate's own fleet stamp is not the caller naming tags.
			if !ec2HasUserTags(ec2LaunchTagsForResource(req.Params, "instance")) {
				pass.addTemplateScope("instance", data.TagSpecifications)
			}
			if len(ec2LaunchTagsForResource(req.Params, "volume")) == 0 {
				pass.addTemplateScope("volume", data.VolumeTagSpecifications)
			}
		}
	}

	if len(pass.resourceTypes) == 0 {
		return nil
	}
	return pass
}

// addScope records that the create applies tags to resourceType, ignoring the tags
// themselves.
//
// The tags are deliberately dropped here: a tag the request declared is already in the
// aws:RequestTag/* map [addRequestTags] built from the same params, and reading it a
// second time is how the two readings come to disagree. A scope carrying no tags
// applies none, so it records nothing at all.
func (p *ec2CreateTagsPass) addScope(resourceType string, tags []EC2Tag) {
	if len(tags) == 0 {
		return
	}
	if !slices.Contains(p.resourceTypes, resourceType) {
		p.resourceTypes = append(p.resourceTypes, resourceType)
	}
}

// addTemplateScope records a scope a launch template supplies, both its resource type
// and its tags — the tags being new information, since the request's params do not
// carry them.
func (p *ec2CreateTagsPass) addTemplateScope(resourceType string, tags []EC2Tag) {
	if len(tags) == 0 {
		return
	}
	p.addScope(resourceType, tags)
	if p.templateTags == nil {
		p.templateTags = make(map[string]string, len(tags))
	}
	for _, t := range tags {
		p.templateTags[t.Key] = t.Value
	}
}

// resourceARNs returns the ARN the tagging pass is authorized against for each scope
// the create applies tags to.
//
// The resources do not exist yet — that is the whole situation — so each is the
// wildcard for its declared type: arn:aws:ec2:<region>:<account>:<type>/*. **That is
// substrate's reading**, not a documented rule. It is the shape AWS's own examples
// require: they write the Resource of an ec2:CreateTags statement as either */* or
// instance/*, and the prose "users cannot tag volumes using the RunInstances request"
// turns on instance/* not matching the volume scope of the very same launch. Resolving
// the pass against one ARN, or against the resources the create *reads* (an AMI, a
// subnet, a security group), would make both of those policies mean something else.
//
// The type is passed through as the request spells it. TagSpecification's ResourceType
// values are the ARN resource types — "instance", "volume", "network-interface",
// "natgateway" — so no mapping table is needed, and a type substrate does not model
// still yields an ARN a policy naming that type matches. Filtering to a known set
// would be the one direction that can produce a false allow.
func (p *ec2CreateTagsPass) resourceARNs(reqCtx *RequestContext) []string {
	out := make([]string, 0, len(p.resourceTypes))
	for _, rt := range p.resourceTypes {
		out = append(out, "arn:aws:ec2:"+reqCtx.Region+":"+reqCtx.AccountID+":"+rt+"/*")
	}
	return out
}

// ec2AuthzLaunchMembers is the set of resources one RunInstances request names.
//
// Network interfaces hold only the IDs of interfaces the request brings, not the
// ones the launch creates: an interface with no ID is not a resource a policy can
// name.
type ec2AuthzLaunchMembers struct {
	imageID             string
	subnetID            string
	securityGroupIDs    []string
	networkInterfaceIDs []string
	// subnetWildcard and securityGroupWildcard mark the two default-VPC resources
	// this launch will create rather than find: no ID exists to name, so the decision
	// is made against subnet/* and security-group/*. They are a separate field rather
	// than a "*" written into subnetID because a sentinel that reads as an ID is the
	// kind of value that eventually reaches a state key (#656).
	subnetWildcard        bool
	securityGroupWildcard bool

	// vpcID is the VPC the launch lands in, when the default-VPC lookup resolved one.
	// A launch whose subnet came from the request or a template leaves it empty and
	// the VPC is read off that subnet's own record instead, which is the one place
	// that knows — including when the caller named a subnet outside the default VPC.
	vpcID string
}

// ec2AuthzResolveLaunch resolves the resources a RunInstances request names,
// reading them exactly as [EC2Plugin.runInstancesWithTags] does so the decision
// and the handler cannot disagree about which resources a launch touches: the
// same parameter spellings, the same primary-interface fallbacks, and the same
// all-or-nothing launch-template precedence.
//
// The template has to be resolved here rather than inherited from the handler,
// because CheckAccess runs before the handler does. AWS's CreateFleet reference
// is the authority for consulting it at all: "Resource-level permissions for this
// action do not include the resources specified in a launch template. To specify
// resource-level permissions for resources specified in a launch template, you
// must include the resources in the RunInstances action statement." So a
// template-supplied subnet is part of a RunInstances decision, and a policy
// scoped to one subnet must fence off a launch that reaches it through a
// template.
//
// A template that cannot be read contributes nothing rather than failing the
// request. The handler decides whether an unresolvable template is an error; this
// is the resource list, and refusing here would answer a missing template with
// AccessDenied instead of the InvalidLaunchTemplateId.NotFound it earns.
func ec2AuthzResolveLaunch(state StateManager, reqCtx *RequestContext, req *AWSRequest) ec2AuthzLaunchMembers {
	m := ec2AuthzLaunchMembers{
		imageID:  req.Params["ImageId"],
		subnetID: req.Params["SubnetId"],
	}

	interfaces := ec2ParseNetworkInterfaces(req.Params, "")
	primary := ec2PrimaryInterface(interfaces)
	if m.subnetID == "" && primary != nil {
		m.subnetID = primary.SubnetID
	}
	m.securityGroupIDs = indexedParams(req.Params, "SecurityGroupId.%d", "SecurityGroupIds.%d")
	if len(m.securityGroupIDs) == 0 && primary != nil {
		m.securityGroupIDs = primary.SecurityGroupIDs
	}
	m.networkInterfaceIDs = ec2AuthzInterfaceIDs(interfaces)

	m.mergeTemplate(state, reqCtx, req, len(interfaces) == 0)
	// Last, so it applies only to a launch nothing else gave a subnet — including a
	// template. A return from the template merge above must not skip this: a plain
	// request naming no template is exactly the launch whose subnet comes from the
	// default VPC (#673).
	m.resolveDefaultVPC(state, reqCtx)
	return m
}

// mergeTemplate folds in the resources a named launch template supplies, under the
// same field-by-field precedence the handler applies: a value the request already
// resolved wins.
//
// takeInterfaces carries the handler's all-or-nothing interface rule from the caller,
// which knows whether the *request* declared any — a request naming one interface is
// not authorized against a second it will never attach.
//
// A template that cannot be read or resolved contributes nothing, which is why both
// failures return rather than refuse.
func (m *ec2AuthzLaunchMembers) mergeTemplate(state StateManager, reqCtx *RequestContext, req *AWSRequest, takeInterfaces bool) {
	resolved := ec2AuthzTemplateData(state, reqCtx, req)
	if resolved == nil {
		return
	}
	data := *resolved
	if m.imageID == "" {
		m.imageID = data.ImageID
	}
	if m.subnetID == "" {
		m.subnetID = data.SubnetID
	}
	if len(m.securityGroupIDs) == 0 {
		m.securityGroupIDs = data.NetworkSecurityGroupIDs()
	}
	if takeInterfaces {
		m.networkInterfaceIDs = ec2AuthzInterfaceIDs(data.NetworkInterfaces)
	}
}

// ec2AuthzTemplateData resolves the launch-template data a RunInstances request
// names, or nil when it names none or names one that cannot be read.
//
// It is shared by the two halves of a launch's decision — the resources it names and
// the tags it applies — so neither can resolve a different version of the template
// than the other, and neither can resolve a different one than the handler.
//
// A template that cannot be read or whose version does not resolve contributes
// nothing rather than failing the request: the handler owes that answer
// (InvalidLaunchTemplateId.NotFound), and refusing here would turn it into
// AccessDenied.
func ec2AuthzTemplateData(state StateManager, reqCtx *RequestContext, req *AWSRequest) *EC2LaunchTemplateData {
	lt := ec2LookupLaunchTemplate(context.Background(), state, reqCtx,
		req.Params["LaunchTemplate.LaunchTemplateId"],
		req.Params["LaunchTemplate.LaunchTemplateName"])
	if lt == nil {
		return nil
	}
	version, awsErr := ec2ResolveTemplateVersion(lt, req.Params["LaunchTemplate.Version"])
	if awsErr != nil || version == nil {
		return nil
	}
	data := version.Data
	return &data
}

// resolveDefaultVPC fills in the subnet and security group a launch that named
// neither takes from the default VPC, reading state without creating anything.
//
// It runs last, after the template merge, because the default VPC applies only when
// nothing else supplied a subnet — the same precedence the handler uses, which is
// what keeps the decision and the launch from disagreeing about where an instance
// lands.
//
// The read is [ec2LookupDefaultVPC] rather than ensureDefaultVPC: the latter creates
// nine records, and calling it here would let a request the policy is about to refuse
// create a VPC.
//
// A launch whose default VPC does not exist yet is authorized against subnet/* and
// security-group/*, because it is about to create both. A Deny on subnet/* still
// matches, and a least-privilege Allow naming one specific subnet correctly refuses a
// launch that will mint a different one. Substrate's fixtures all start from empty
// state, so that is the common case rather than a corner.
func (m *ec2AuthzLaunchMembers) resolveDefaultVPC(state StateManager, reqCtx *RequestContext) {
	if m.subnetID != "" {
		return
	}
	namedGroups := len(m.securityGroupIDs) > 0
	vpc, subnet, err := ec2LookupDefaultVPC(context.Background(), state, reqCtx)
	if err != nil || vpc == nil {
		// No default VPC, or state cannot say: this launch creates both. A failed read
		// resolves to the wildcards rather than to nothing, so a storage fault cannot
		// turn a guarded launch into an unguarded one.
		m.subnetWildcard = true
		m.securityGroupWildcard = !namedGroups
		return
	}
	// The VPC is known even when its default subnet is not, so ec2:Vpc is answerable
	// for a launch that is about to mint the subnet.
	m.vpcID = vpc.VPCID
	if subnet != nil {
		m.subnetID = subnet.SubnetID
	} else {
		// The default VPC exists but has no default subnet, so the launch mints one.
		m.subnetWildcard = true
	}
	if namedGroups {
		return
	}
	// The default group already exists alongside the VPC, so it is named rather than
	// wildcarded. When it does not resolve, the launch attaches no group at all and
	// the decision names none either — the same rule the handler follows.
	if sgID := ec2LookupDefaultSecurityGroup(context.Background(), state, reqCtx, vpc.VPCID); sgID != "" {
		m.securityGroupIDs = []string{sgID}
	}
}

// ec2AuthzInterfaceIDs returns the IDs of the interfaces the launch attaches by
// ID, dropping the ones it creates.
func ec2AuthzInterfaceIDs(interfaces []EC2NetworkInterface) []string {
	var out []string
	for _, ifc := range interfaces {
		if ifc.NetworkInterfaceID != "" {
			out = append(out, ifc.NetworkInterfaceID)
		}
	}
	return out
}

// ec2AuthzTagsFor returns the tags stored on the EC2 record at key, or nil when
// it cannot be read.
//
// nil rather than a partial map on a failed read: an absent aws:ResourceTag key
// denies a condition that requires it, whereas a fabricated one would let a
// storage fault grant access. Every EC2 record this reads — image, subnet,
// security group — carries its tags under the same "Tags" member, so one reader
// serves all three and the ARN a resource is authorized under cannot drift from
// the tags it is matched against.
func ec2AuthzTagsFor(state StateManager, key string) map[string]string {
	tags, _ := ec2AuthzRecordFor(state, key)
	return tags
}

// ec2AuthzRecordFor returns the tags and the VPC ID stored on the EC2 record at key,
// each zero when the record cannot be read or does not carry it.
//
// Both come out of one read because both describe the same resource and are needed
// together: a subnet contributes its tags to aws:ResourceTag/* and its VPC to
// ec2:Vpc, and reading the record twice is how the two would come to describe
// different versions of it. An image record has no vpc_id, so it yields "" — which
// [ec2AuthzVpcContext] turns into an omitted key rather than an empty one.
func ec2AuthzRecordFor(state StateManager, key string) (map[string]string, string) {
	raw, err := state.Get(context.Background(), ec2Namespace, key)
	if err != nil || raw == nil {
		return nil, ""
	}
	var record struct {
		Tags []EC2Tag `json:"tags"`
		// Every EC2 record that belongs to a VPC spells the member this way — EC2Subnet
		// and EC2SecurityGroup both do — so one decoder serves both, for the same
		// reason one tag decoder serves all three record types.
		VPCID string `json:"vpc_id"`
	}
	if json.Unmarshal(raw, &record) != nil {
		return nil, ""
	}
	return ec2TagsToMap(record.Tags), record.VPCID
}

// ec2SubnetCondKey and ec2VpcCondKey are the two resource-level condition keys the
// Service Authorization Reference lists on RunInstances' network-interface resource,
// and the mechanism AWS's own documentation recommends for fencing a launch into one
// subnet — over the resource-ARN scoping substrate already had (#673, #692).
//
// Both take a **full ARN**, not an ID. AWS says so for the VPC outright ("To specify
// a VPC for the ec2:Vpc condition key, you must specify the full ARN of the VPC") and
// shows it for the subnet in the policy its subnet guardrail is written as, which
// compares ec2:Subnet against arn:aws:ec2:<region>:<account>:subnet/subnet-… . AWS's
// machine-readable service reference declares the Type of both as ARN.
//
// The two example policies use *different* operator families against those ARNs —
// ArnNotEquals for ec2:Subnet, StringEquals for ec2:Vpc — which both work here
// because the value is the full ARN either way and substrate's Arn* and String*
// operators compare the same string.
const (
	ec2SubnetCondKey = "ec2:Subnet"
	ec2VpcCondKey    = "ec2:Vpc"
)

// ec2VpcARN returns the ARN of a VPC, the form ec2:Vpc is compared against.
func ec2VpcARN(accountID, region, vpcID string) string {
	return "arn:aws:ec2:" + region + ":" + accountID + ":vpc/" + vpcID
}

// ec2AuthzVpcContext returns the ec2:Vpc entry for vpcID, or nil when no VPC
// resolved.
//
// **Omitted, not wildcarded.** A "*" value would be a string no caller's ArnEquals
// could ever match and an ARN-shaped lie about a VPC that does not exist; an absent
// key is what AWS's own evaluation rules already describe. The consequence is worth
// knowing: a condition on an absent single-valued key does not match, so an Allow
// gated on ec2:Vpc refuses such a launch and a Deny gated on it does not catch one —
// and a set qualifier over an absent key is vacuously true, which is AWS's documented
// behavior and the reason it says not to use one on a single-valued key.
func ec2AuthzVpcContext(accountID, region, vpcID string) map[string]string {
	if vpcID == "" {
		return nil
	}
	return map[string]string{ec2VpcCondKey: ec2VpcARN(accountID, region, vpcID)}
}

// ec2AuthzInterfaceContext returns the condition keys AWS documents on a launch's
// network-interface resource: ec2:Subnet for the subnet the interface lands in, and
// ec2:Vpc for that subnet's VPC. Each is omitted when it did not resolve, per
// [ec2AuthzVpcContext].
func ec2AuthzInterfaceContext(accountID, region, subnetID, vpcID string) map[string]string {
	out := ec2AuthzVpcContext(accountID, region, vpcID)
	if subnetID == "" {
		return out
	}
	if out == nil {
		out = make(map[string]string, 1)
	}
	out[ec2SubnetCondKey] = ec2SubnetARN(accountID, region, subnetID)
	return out
}
