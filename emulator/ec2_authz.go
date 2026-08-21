package emulator

import (
	"context"
	"encoding/json"
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
	switch {
	case launch.subnetID != "":
		out = append(out, authzResource{
			ARN:  ec2SubnetARN(acct, region, launch.subnetID),
			Tags: ec2AuthzTagsFor(state, "subnet:"+acct+"/"+region+"/"+launch.subnetID),
		})
	case launch.subnetWildcard:
		// The default subnet this launch is about to create. No tags: it does not
		// exist, so an aws:ResourceTag condition about it cannot be satisfied — which
		// is the honest answer, since the subnet substrate mints carries no tags
		// either.
		out = append(out, authzResource{ARN: "arn:aws:ec2:" + region + ":" + acct + ":subnet/*"})
	}
	for _, sgID := range launch.securityGroupIDs {
		out = append(out, authzResource{
			ARN:  "arn:aws:ec2:" + region + ":" + acct + ":security-group/" + sgID,
			Tags: ec2AuthzTagsFor(state, "sg:"+acct+"/"+region+"/"+sgID),
		})
	}
	if launch.securityGroupWildcard {
		out = append(out, authzResource{ARN: "arn:aws:ec2:" + region + ":" + acct + ":security-group/*"})
	}
	// Every launch attaches an interface, so network-interface* is always evaluated.
	// An interface the launch creates has no ID yet, so it is the wildcard; one the
	// request brings by ID is named, because a policy can meaningfully scope to it.
	// Neither carries tags: substrate stores no standalone taggable ENI record, and
	// ec2TaggableStateKey has no eni- arm to read one through.
	if len(launch.networkInterfaceIDs) == 0 {
		out = append(out, authzResource{ARN: "arn:aws:ec2:" + region + ":" + acct + ":network-interface/*"})
	}
	for _, eniID := range launch.networkInterfaceIDs {
		out = append(out, authzResource{ARN: "arn:aws:ec2:" + region + ":" + acct + ":network-interface/" + eniID})
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
// trying to tag is not valid" — not AccessDenied, and substrate's handler treats
// it as a no-op. A request naming only such IDs returns nil and is decided exactly
// as it was before.
func ec2AuthzTagResources(state StateManager, reqCtx *RequestContext, req *AWSRequest) []authzResource {
	if req.Operation != "CreateTags" && req.Operation != "DeleteTags" {
		return nil
	}
	acct := reqCtx.AccountID
	region := reqCtx.Region
	var out []authzResource
	for _, id := range extractIndexedParams(req.Params, "ResourceId") {
		stateKey, arnType, ok := ec2TaggableResource(reqCtx, id)
		if !ok {
			continue
		}
		out = append(out, authzResource{
			ARN: "arn:aws:ec2:" + region + ":" + acct + ":" + arnType + "/" + id,
			// The same state key the handler will read and write, so the tags a
			// condition is evaluated against are the ones the call is about to change.
			Tags: ec2AuthzTagsFor(state, stateKey),
		})
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
	lt := ec2LookupLaunchTemplate(context.Background(), state, reqCtx,
		req.Params["LaunchTemplate.LaunchTemplateId"],
		req.Params["LaunchTemplate.LaunchTemplateName"])
	if lt == nil {
		return
	}
	version, awsErr := ec2ResolveTemplateVersion(lt, req.Params["LaunchTemplate.Version"])
	if awsErr != nil || version == nil {
		return
	}
	data := version.Data
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
	raw, err := state.Get(context.Background(), ec2Namespace, key)
	if err != nil || raw == nil {
		return nil
	}
	var record struct {
		Tags []EC2Tag `json:"tags"`
	}
	if json.Unmarshal(raw, &record) != nil {
		return nil
	}
	return ec2TagsToMap(record.Tags)
}
