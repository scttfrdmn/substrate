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
// absent ID would widen the policy the caller wrote instead of narrowing it. A
// request naming none of them returns nil, so it is decided exactly as it was
// before.
func ec2AuthzRunInstancesResources(state StateManager, reqCtx *RequestContext, req *AWSRequest) []authzResource {
	if req.Operation != "RunInstances" {
		return nil
	}
	launch := ec2AuthzResolveLaunch(state, reqCtx, req)
	if launch.empty() {
		return nil
	}

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
	if launch.subnetID != "" {
		out = append(out, authzResource{
			ARN:  "arn:aws:ec2:" + region + ":" + acct + ":subnet/" + launch.subnetID,
			Tags: ec2AuthzTagsFor(state, "subnet:"+acct+"/"+region+"/"+launch.subnetID),
		})
	}
	for _, sgID := range launch.securityGroupIDs {
		out = append(out, authzResource{
			ARN:  "arn:aws:ec2:" + region + ":" + acct + ":security-group/" + sgID,
			Tags: ec2AuthzTagsFor(state, "sg:"+acct+"/"+region+"/"+sgID),
		})
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
}

// empty reports whether the request named no resource at all, in which case
// authorization falls back to the single-resource path unchanged.
func (m ec2AuthzLaunchMembers) empty() bool {
	return m.imageID == "" && m.subnetID == "" &&
		len(m.securityGroupIDs) == 0 && len(m.networkInterfaceIDs) == 0
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

	lt := ec2LookupLaunchTemplate(context.Background(), state, reqCtx,
		req.Params["LaunchTemplate.LaunchTemplateId"],
		req.Params["LaunchTemplate.LaunchTemplateName"])
	if lt == nil {
		return m
	}
	version, awsErr := ec2ResolveTemplateVersion(lt, req.Params["LaunchTemplate.Version"])
	if awsErr != nil || version == nil {
		return m
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
	// All-or-nothing, matching the handler: the template's interfaces apply only
	// when the request declared none, so a request naming one interface is not
	// authorized against a second it will never attach.
	if len(interfaces) == 0 {
		m.networkInterfaceIDs = ec2AuthzInterfaceIDs(data.NetworkInterfaces)
	}
	return m
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
