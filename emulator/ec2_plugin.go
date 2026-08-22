package emulator

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// EC2Plugin emulates the Amazon EC2 and VPC APIs using query protocol.
// It handles instance lifecycle operations (RunInstances, DescribeInstances,
// TerminateInstances, StopInstances, StartInstances) and VPC networking
// (CreateVpc, CreateSubnet, CreateSecurityGroup, InternetGateway, RouteTable,
// and related operations).
type EC2Plugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
	// auth authorizes the launches CreateFleet dispatches through the plugin's own
	// RunInstances path, which never passes through the server's check. nil for a
	// registry built without [WithPluginAuth], in which case a fleet launches exactly
	// as it did before (#673).
	auth *AuthController
}

// Name returns the service name "ec2".
func (p *EC2Plugin) Name() string { return "ec2" }

// Initialize sets up the EC2Plugin with the provided configuration.
func (p *EC2Plugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	if auth, ok := cfg.Options["auth_controller"].(*AuthController); ok {
		p.auth = auth
	}
	return nil
}

// Shutdown is a no-op for EC2Plugin.
func (p *EC2Plugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an EC2 query-protocol request to the appropriate handler.
func (p *EC2Plugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	action := req.Operation
	if action == "" {
		action = req.Params["Action"]
	}
	switch action {
	// Instance operations
	case "RunInstances":
		return p.runInstances(ctx, req)
	case "DescribeInstances":
		return p.describeInstances(ctx, req)
	case "TerminateInstances":
		return p.terminateInstances(ctx, req)
	case "StopInstances":
		return p.stopInstances(ctx, req)
	case "StartInstances":
		return p.startInstances(ctx, req)
	case "DescribeInstanceStatus":
		return p.describeInstanceStatus(ctx, req)
	// VPC operations
	case "CreateVpc":
		return p.createVPC(ctx, req)
	case "DescribeVpcs":
		return p.describeVPCs(ctx, req)
	case "DeleteVpc":
		return p.deleteVPC(ctx, req)
	case "CreateSubnet":
		return p.createSubnet(ctx, req)
	case "DescribeSubnets":
		return p.describeSubnets(ctx, req)
	case "DeleteSubnet":
		return p.deleteSubnet(ctx, req)
	case "CreateSecurityGroup":
		return p.createSecurityGroup(ctx, req)
	case "DescribeSecurityGroups":
		return p.describeSecurityGroups(ctx, req)
	case "DeleteSecurityGroup":
		return p.deleteSecurityGroup(ctx, req)
	case "AuthorizeSecurityGroupIngress":
		return p.authorizeSecurityGroupIngress(ctx, req)
	case "RevokeSecurityGroupIngress":
		return p.revokeSecurityGroupIngress(ctx, req)
	case "AuthorizeSecurityGroupEgress":
		return p.authorizeSecurityGroupEgress(ctx, req)
	case "RevokeSecurityGroupEgress":
		return p.revokeSecurityGroupEgress(ctx, req)
	case "CreateInternetGateway":
		return p.createInternetGateway(ctx, req)
	case "DescribeInternetGateways":
		return p.describeInternetGateways(ctx, req)
	case "AttachInternetGateway":
		return p.attachInternetGateway(ctx, req)
	case "DetachInternetGateway":
		return p.detachInternetGateway(ctx, req)
	case "DeleteInternetGateway":
		return p.deleteInternetGateway(ctx, req)
	case "CreateRouteTable":
		return p.createRouteTable(ctx, req)
	case "DescribeRouteTables":
		return p.describeRouteTables(ctx, req)
	case "AssociateRouteTable":
		return p.associateRouteTable(ctx, req)
	case "DisassociateRouteTable":
		return p.disassociateRouteTable(ctx, req)
	case "ReplaceRouteTableAssociation":
		return p.replaceRouteTableAssociation(ctx, req)
	case "CreateRoute":
		return p.createRoute(ctx, req)
	case "ReplaceRoute":
		return p.replaceRoute(ctx, req)
	case "DeleteRoute":
		return p.deleteRoute(ctx, req)
	case "DeleteRouteTable":
		return p.deleteRouteTable(ctx, req)
	// Instance management operations
	case "RebootInstances":
		return p.rebootInstances(ctx, req)
	case "CreateTags":
		return p.createTags(ctx, req)
	case "DeleteTags":
		return p.deleteTags(ctx, req)
	case "DescribeTags":
		return p.describeTags(ctx, req)
	case "ModifyInstanceAttribute":
		return p.modifyInstanceAttribute(ctx, req)
	case "DescribeInstanceAttribute":
		return p.describeInstanceAttribute(ctx, req)
	// Key pair operations
	case "CreateKeyPair":
		return p.createKeyPair(ctx, req)
	case "DescribeKeyPairs":
		return p.describeKeyPairs(ctx, req)
	case "DeleteKeyPair":
		return p.deleteKeyPair(ctx, req)
	case "ImportKeyPair":
		return p.importKeyPair(ctx, req)
	// AMI operations
	case "CreateImage":
		return p.createImage(ctx, req)
	case "RegisterImage":
		return p.registerImage(ctx, req)
	case "DescribeImages":
		return p.describeImages(ctx, req)
	case "DeregisterImage":
		return p.deregisterImage(ctx, req)
	// Placement group operations
	case "CreatePlacementGroup":
		return p.createPlacementGroup(ctx, req)
	case "DescribePlacementGroups":
		return p.describePlacementGroups(ctx, req)
	case "DeletePlacementGroup":
		return p.deletePlacementGroup(ctx, req)
	// Availability Zone operations
	case "DescribeAvailabilityZones":
		return p.describeAvailabilityZones(ctx, req)
	// Subnet/VPC attribute operations
	case "ModifySubnetAttribute":
		return p.modifySubnetAttribute(ctx, req)
	case "ModifyVpcAttribute":
		return p.modifyVpcAttribute(ctx, req)
	// Elastic IP operations
	case "AllocateAddress":
		return p.allocateAddress(ctx, req)
	case "AssociateAddress":
		return p.associateAddress(ctx, req)
	case "DisassociateAddress":
		return p.disassociateAddress(ctx, req)
	case "ReleaseAddress":
		return p.releaseAddress(ctx, req)
	case "DescribeAddresses":
		return p.describeAddresses(ctx, req)
	// NAT Gateway operations
	case "CreateNatGateway":
		return p.createNatGateway(ctx, req)
	case "DescribeNatGateways":
		return p.describeNatGateways(ctx, req)
	case "DeleteNatGateway":
		return p.deleteNatGateway(ctx, req)
	// Region operations
	case "DescribeRegions":
		return p.describeRegions(ctx, req)
	// Instance type and spot price operations
	case "DescribeInstanceTypes":
		return p.describeInstanceTypes(ctx, req)
	case "DescribeInstanceTypeOfferings":
		return p.describeInstanceTypeOfferings(ctx, req)
	case "DescribeSpotPriceHistory":
		return p.describeSpotPriceHistory(ctx, req)
	// Launch template operations
	case "CreateLaunchTemplate":
		return p.createLaunchTemplate(ctx, req)
	case "DescribeLaunchTemplates":
		return p.describeLaunchTemplates(ctx, req)
	case "DeleteLaunchTemplate":
		return p.deleteLaunchTemplate(ctx, req)
	case "CreateLaunchTemplateVersion":
		return p.createLaunchTemplateVersion(ctx, req)
	case "ModifyLaunchTemplate":
		return p.modifyLaunchTemplate(ctx, req)
	case "DescribeLaunchTemplateVersions":
		return p.describeLaunchTemplateVersions(ctx, req)
	case "DeleteLaunchTemplateVersions":
		return p.deleteLaunchTemplateVersions(ctx, req)
	// EC2 Fleet operations
	case "CreateFleet":
		return p.createFleet(ctx, req)
	case "DescribeFleets":
		return p.describeFleets(ctx, req)
	case "DeleteFleets":
		return p.deleteFleets(ctx, req)
	// EBS volume operations
	case "CreateVolume":
		return p.createVolume(ctx, req)
	case "DescribeVolumes":
		return p.describeVolumes(ctx, req)
	case "DeleteVolume":
		return p.deleteVolume(ctx, req)
	case "AttachVolume":
		return p.attachVolume(ctx, req)
	case "DetachVolume":
		return p.detachVolume(ctx, req)
	case "CreateSnapshot":
		return p.createSnapshot(ctx, req)
	case "DeleteSnapshot":
		return p.deleteSnapshot(ctx, req)
	case "DescribeSnapshots":
		return p.describeSnapshots(ctx, req)
	default:
		return nil, unknownActionError(p.Name(), action)
	}
}

// --- Instance operations ---

// runInstances handles a caller's RunInstances request: it parses the request's
// instance-scoped TagSpecification.N tags, rejects any reserved key among them, and
// delegates to [EC2Plugin.runInstancesWithTags].
//
// The parse-and-check is split from the launch so that substrate's own internal
// launches — the fleet path, which stamps [ec2FleetIDTagKey] — can supply a reserved
// tag without going through this check, while a caller's request cannot. The
// distinction is structural rather than a bypass flag: a request reaches only this
// function, and extraTags is not addressable from a param map. #468 rules out a flag
// explicitly, and rightly — an internal "skip validation" switch would make behavior
// depend on state a consumer cannot observe, the opposite of the deterministic-replay
// property this emulator exists for.
func (p *EC2Plugin) runInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	launchTags := ec2LaunchTagsForResource(req.Params, "instance")
	if awsErr := ec2CheckTagRules(launchTags); awsErr != nil {
		return nil, awsErr
	}
	// A new instance starts with no tags, so the request's own tags are the whole count
	// (#469). The fleet-ID tag substrate may add afterwards is reserved, and reserved
	// keys are excluded from the count — so a fleet instance launched from a template
	// carrying the full 50 user tags is still legal, exactly as on real EC2.
	if awsErr := ec2CheckTagLimit(nil, launchTags); awsErr != nil {
		return nil, awsErr
	}
	return p.runInstancesWithTags(reqCtx, req, launchTags)
}

// runInstancesWithTags launches instances carrying tags, which the caller has already
// parsed and, if they came from a request, checked.
//
// tags is the complete tag set for the new instances: [EC2Plugin.runInstances] passes
// the request's checked TagSpecification.N tags, and the fleet path passes those plus
// its own fleet-ID tag. See [EC2Plugin.runInstances] for why the check lives with the
// parse rather than here.
func (p *EC2Plugin) runInstancesWithTags(
	reqCtx *RequestContext,
	req *AWSRequest,
	tags []EC2Tag,
) (*AWSResponse, error) {
	imageID := req.Params["ImageId"]
	// The call-level instance type is kept verbatim, with no default applied yet: a
	// launch template may supply one, and the default can only be resolved once the
	// template has had its chance. Defaulting here and then testing for the default
	// value later cannot tell "absent" from "explicitly t3.micro" — which is how a
	// caller naming t3.micro alongside a template naming something else used to get
	// the template's type, exactly inverting AWS's precedence (#453).
	instanceType := req.Params["InstanceType"]
	// Absent counts still default; a count that is present and invalid is rejected
	// rather than clamped, because clamping MinCount=0 up to 1 launched an instance
	// where AWS fails the request (#431). See resolveInstanceCounts for why the
	// codes are the common-error ones and why presence is not required.
	//
	// Only MaxCount reaches the launch loop. AWS "launches the largest possible
	// number of instances above the specified minimum count", and substrate models
	// no capacity ceiling, so the largest possible number is always MaxCount and
	// MinCount can only ever be satisfied. It is still validated, and still sets
	// MaxCount's default, which is all it can affect here.
	_, maxCount, countErr := resolveInstanceCounts(req.Params)
	if countErr != nil {
		return nil, countErr
	}

	keyName := req.Params["KeyName"]
	iamInstanceProfile := req.Params["IamInstanceProfile.Name"]
	if iamInstanceProfile == "" {
		iamInstanceProfile = req.Params["IamInstanceProfile.Arn"]
	}
	userData := req.Params["UserData"]
	// Recorded so DescribeInstanceAttribute can report it (#473) and so
	// TerminateInstances can refuse (#489). AWS documents the default as false, so
	// an absent parameter and an explicit "false" agree.
	disableAPITermination := strings.EqualFold(req.Params["DisableApiTermination"], "true")
	// Placement.AvailabilityZone is optional; the reference says EC2 selects a zone
	// when neither it nor AvailabilityZoneId is given. Substrate resolves an absent
	// value from the subnet below, which is where a real launch's zone comes from
	// too, and only falls back to the region's first zone when there is no subnet.
	// AvailabilityZoneId is not modeled: substrate has no per-account zone-ID
	// mapping to resolve it through, and inventing one would be an observation
	// nothing backs.
	placementAZ := req.Params["Placement.AvailabilityZone"]

	subnetID := req.Params["SubnetId"]
	sgID := req.Params["SecurityGroupId.1"]
	if sgID == "" {
		sgID = req.Params["SecurityGroupIds.1"]
	}

	// If a placement group is named, it must exist — mirrors AWS so a
	// create → poll(available) → launch ordering is testable (#344).
	if pgName := req.Params["Placement.GroupName"]; pgName != "" {
		pgKey := ec2PlacementGroupStateKey(reqCtx.AccountID, reqCtx.Region, pgName)
		if data, _ := p.state.Get(context.Background(), ec2Namespace, pgKey); data == nil {
			return nil, &AWSError{Code: "InvalidPlacementGroup.Unknown", Message: "The placement group '" + pgName + "' is unknown.", HTTPStatus: http.StatusBadRequest}
		}
	}

	// Networking can be specified either at the top level (above) or nested in a
	// NetworkInterface.N spec. The AWS SDK puts SubnetId, security groups, and
	// AssociatePublicIpAddress inside the network interface whenever a public-IP
	// preference is set — which spawn always does. Read the nested form so that
	// subnet/SG/public-IP aren't silently dropped.
	//
	// Every declared interface is parsed, not just index 1 (#455). The top-level
	// fallbacks below read the *primary* interface, which is the one whose values
	// real EC2 reports at the top level of an instance — see [ec2PrimaryInterface]
	// for why that is the lowest device index rather than the first parameter index.
	networkInterfaces := ec2ParseNetworkInterfaces(req.Params, "")
	ec2SortInterfacesByDeviceIndex(networkInterfaces)

	// Storage is specified the same way, and was likewise accepted and discarded
	// until #666: a request naming BlockDeviceMapping.1.Ebs.VolumeSize succeeded and
	// DescribeVolumes reported nothing for the instance. The mappings become real
	// volumes in the launch loop below; see [ec2LaunchVolumesFor].
	blockDeviceMappings := ec2ParseBlockDeviceMappings(req.Params, "")
	// A launch tags its volumes through the same TagSpecification.N structure that
	// tags the instance, scoped to "volume" — and those tags were accepted and
	// discarded until #670. They are parsed here rather than in
	// [EC2Plugin.runInstances] because a template can also supply them, and because
	// substrate's fleet stamp is an instance tag with no volume-scoped counterpart, so
	// there is nothing for the caller-named-tags rule to distinguish.
	volumeTags := ec2LaunchTagsForResource(req.Params, "volume")
	primaryIfc := ec2PrimaryInterface(networkInterfaces)
	if subnetID == "" && primaryIfc != nil {
		subnetID = primaryIfc.SubnetID
	}
	// AssociatePublicIpAddress=true forces a public IP even on a non-default
	// subnet; "" means "use the subnet default". A launch template can also carry
	// this preference, so the call-level value is kept as the raw string and only
	// resolved to a bool once the template has had its chance to supply one.
	//
	// Read from the primary alone: AWS documents the public IP as assignable "to a
	// network interface for eth0" only, so a secondary interface's preference is not
	// a statement about the instance's public IP.
	var publicIPPref string
	if primaryIfc != nil {
		publicIPPref = primaryIfc.AssociatePublicIPAddress
	}

	// Security groups named at call level, from either the top-level params or the
	// primary interface's nested form (SecurityGroupId.M or Groups.M).
	securityGroupIDs := indexedParams(req.Params, "SecurityGroupId.%d", "SecurityGroupIds.%d")
	if len(securityGroupIDs) == 0 && primaryIfc != nil {
		securityGroupIDs = primaryIfc.SecurityGroupIDs
	}

	// Merge a named launch template into the request, per field.
	//
	// The template is consulted whenever one is named, not only when the request
	// omitted ImageId (#453). AWS's RunInstances reference states the rule plainly:
	// "Any additional parameters that you specify for the new instance overwrite the
	// corresponding parameters included in the launch template." A request carrying
	// both an ImageId and a template therefore gets its own AMI *and* the template's
	// instance type, key name, subnet, security groups and public-IP preference —
	// where the old imageID == "" gate meant such a request never read the template
	// at all and silently dropped every one of those values.
	//
	// Every fallback below is guarded on the call-level value being absent, which is
	// what implements that precedence. The networking fallbacks in particular must
	// run *after* the call-level NetworkInterface.1.* reads above rather than before
	// them (#444) — the subnet fallback used to sit ahead of this block, where there
	// was nothing yet to fall back to.
	//
	// Which *version* supplies those values is resolved from LaunchTemplate.Version.
	// An absent version means the template's default version, not its latest — see
	// [ec2ResolveTemplateVersion] (#456).
	ltID := req.Params["LaunchTemplate.LaunchTemplateId"]
	ltName := req.Params["LaunchTemplate.LaunchTemplateName"]
	if lt := p.resolveLaunchTemplate(context.Background(), reqCtx, ltID, ltName); lt != nil {
		version, awsErr := ec2ResolveTemplateVersion(lt, req.Params["LaunchTemplate.Version"])
		if awsErr != nil {
			return nil, awsErr
		}
		ltData := version.Data
		if imageID == "" {
			imageID = ltData.ImageID
		}
		if instanceType == "" {
			instanceType = ltData.InstanceType
		}
		if keyName == "" {
			keyName = ltData.KeyName
		}
		if userData == "" {
			userData = ltData.UserData
		}
		if subnetID == "" {
			subnetID = ltData.SubnetID
		}
		if publicIPPref == "" {
			publicIPPref = ltData.AssociatePublicIPAddress
		}
		if len(securityGroupIDs) == 0 {
			securityGroupIDs = ltData.NetworkSecurityGroupIDs()
		}
		// The template's interfaces apply only when the request declared none, the
		// same all-or-nothing rule the reference's "any additional parameters that you
		// specify for the new instance overwrite the corresponding parameters included
		// in the launch template" gives every other field. Merging per device index
		// would let a request that named one interface silently inherit a second.
		if len(networkInterfaces) == 0 {
			networkInterfaces = ltData.NetworkInterfaces
		}
		// Block device mappings follow the same all-or-nothing rule, for the same
		// reason: merging per device name would let a request that named one volume
		// silently inherit a second from the template. A template is also the only
		// route by which mappings reach a fleet launch, since CreateFleet forwards
		// the template reference rather than the caller's own mappings.
		if len(blockDeviceMappings) == 0 {
			blockDeviceMappings = ltData.BlockDeviceMappings
		}
		if sgID == "" && len(securityGroupIDs) > 0 {
			sgID = securityGroupIDs[0]
		}
		if iamInstanceProfile == "" {
			iamInstanceProfile = ltData.IamInstanceProfile
		}
		// A template's tags fill the gap only when the caller named none, so they
		// *replace* rather than merge: a request naming Env=req and a template naming
		// Env=tmpl,Team=x yields Env=req alone. The reference gives no
		// TagSpecifications-specific merge semantics, only the general rule quoted
		// above, and this is that rule applied — not a per-field citation.
		//
		// Substrate's own fleet stamp does not count as the caller naming tags. It is
		// a reserved key, and a fleet launched from a tagging template must still get
		// the template's tags alongside the stamp, exactly as on real EC2 — the same
		// exclusion that keeps the stamp out of the tag count (#469).
		if !ec2HasUserTags(tags) {
			// Tags arriving from a template are subject to both tag rules here as well
			// as at CreateLaunchTemplate, so a template is not a second unrestricted
			// path (#471). The check is not redundant: a template stored before those
			// checks existed, or one written straight into state by a replayed event
			// log, can still carry a reserved key or more than the limit, and this is
			// the launch that would otherwise apply them.
			if awsErr := ec2CheckTagRules(ltData.TagSpecifications); awsErr != nil {
				return nil, awsErr
			}
			if awsErr := ec2CheckTagLimit(nil, ltData.TagSpecifications); awsErr != nil {
				return nil, awsErr
			}
			tags = append(append([]EC2Tag{}, ltData.TagSpecifications...), tags...)
		}
		// The volume scope follows the same replace-rather-than-merge rule, on its own
		// gate: a request naming volume tags and a template naming others yields the
		// request's alone, and the two scopes resolve independently, so a template can
		// supply volume tags for a request that named only instance tags. There is no
		// reserved-key exclusion to make here — the fleet stamp is instance-scoped —
		// so the plain emptiness test is the whole rule (#670).
		if len(volumeTags) == 0 {
			volumeTags = ltData.VolumeTagSpecifications
		}
	}

	// The instance-type default is resolved last, so it applies only when neither the
	// request nor the template named one.
	if instanceType == "" {
		instanceType = "t3.micro"
	}

	associatePublicIP := strings.EqualFold(publicIPPref, "true")

	// An AMI must have resolved from *some* source by this point. AWS documents
	// ImageId as "Required: No" only because a launch template may supply it, so
	// the check belongs here rather than at the top: a launch that names a
	// template carrying an AMI is valid, and one that resolves to nothing is not.
	// Without this, an empty ImageId — how aws.String("") on an optional *string
	// serializes, i.e. absent from the wire — launched successfully here and
	// failed on real AWS (#412).
	if imageID == "" {
		return nil, ec2MissingParameter("ImageId")
	}

	// Block device mappings are validated here for two reasons that fix the position
	// exactly (#671). It is after the template merge above, so a mapping that reaches
	// this launch through a template is refused at RunInstances time rather than
	// materialized — and one validator here covers requests, templates, template
	// versions and fleet launches alike, since a fleet reaches mappings only through a
	// template. And it is before the ensureDefaultVPC branch below, which commits a
	// VPC, subnet, security group, internet gateway, route table and four index
	// mutations: a refusal past that point leaves state behind, which is worse than no
	// validation at all because the next request in the same test sees it.
	resolveSnapshot := p.ec2SnapshotResolver(reqCtx)
	if awsErr := ec2CheckBlockDeviceMappings(blockDeviceMappings, resolveSnapshot); awsErr != nil {
		return nil, awsErr
	}

	// A mapping that names a snapshot and no size takes the snapshot's, which the
	// validation above has just established exists. It happens here rather than inside
	// ec2VolumeFromMapping so that function stays pure; see [ec2ResolveSnapshotSizes].
	blockDeviceMappings = ec2ResolveSnapshotSizes(blockDeviceMappings, resolveSnapshot)

	// The volume tags are checked here for the same ordering reason, and it is not the
	// same as the instance tags' position: a volume is written inside the launch loop
	// *after* its instance's own Put, so a refusal there would leave instance 1 behind
	// on a MaxCount=2 launch. Both tag rules apply, whether the tags came from the
	// request or from a template — the template check is not redundant, since a
	// template stored before these checks existed, or one written straight into state
	// by a replayed event log, can still carry a reserved key or more than the limit.
	// A new volume starts with no tags, so the resolved set is the whole count (#670).
	if awsErr := ec2CheckTagRules(volumeTags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, volumeTags); awsErr != nil {
		return nil, awsErr
	}

	// The groups the request or its template named are checked for existence here,
	// before the ensureDefaultVPC branch below writes anything (#673). The membership
	// check has to wait for the subnet's VPC, but existence does not, and a launch
	// naming a group that does not exist was previously refused *after* nine records
	// had been committed — a VPC, subnet, security group, internet gateway, route
	// table and four index mutations, two of them swallowing their own failures. A
	// refusal that leaves a default VPC behind is worse than no check at all, because
	// the next request in the same test sees state the refused one created. Reachable
	// with no IAM configured, so this stands on its own regardless of the
	// authorization change beside it.
	if sgCheckErr := p.ec2CheckSecurityGroups(reqCtx, securityGroupIDs, ""); sgCheckErr != nil {
		return nil, sgCheckErr
	}

	// Auto-create default VPC/subnet if none specified.
	if subnetID == "" {
		vpc, subnet, err := p.ensureDefaultVPC(context.Background(), reqCtx)
		if err != nil {
			return nil, err
		}
		subnetID = subnet.SubnetID
		if sgID == "" {
			// Use the default security group — the same lookup the authorization
			// decision makes, so the group this launch attaches is the group its
			// policy was evaluated against (#673).
			sgID = ec2LookupDefaultSecurityGroup(context.Background(), p.state, reqCtx, vpc.VPCID)
		}
	}

	// sgID may have been filled from the default VPC above, after the call-level and
	// template lists were resolved.
	if len(securityGroupIDs) == 0 && sgID != "" {
		securityGroupIDs = []string{sgID}
	}

	// Resolve VPCID from subnet for SG validation.
	var targetVPCID string
	if subnetID != "" {
		subData, _ := p.state.Get(context.Background(), ec2Namespace, "subnet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+subnetID)
		if subData != nil {
			var sub EC2Subnet
			if json.Unmarshal(subData, &sub) == nil {
				targetVPCID = sub.VPCID
			}
		}
	}

	// Validate security groups exist and belong to the target VPC. The existence half
	// ran above, before the default VPC could be created; this pass adds the
	// membership half and covers the default group the branch above may have supplied.
	if sgCheckErr := p.ec2CheckSecurityGroups(reqCtx, securityGroupIDs, targetVPCID); sgCheckErr != nil {
		return nil, sgCheckErr
	}

	reservationID := generateReservationID()
	now := p.tc.Now().UTC().Format(time.RFC3339)
	var instances []EC2Instance

	for i := 0; i < maxCount; i++ {
		inst := EC2Instance{
			InstanceID:         generateEC2InstanceID(),
			ReservationID:      reservationID,
			ImageID:            imageID,
			InstanceType:       instanceType,
			State:              EC2InstanceState{Code: 16, Name: "running"},
			SubnetID:           subnetID,
			PrivateIPAddress:   fmt.Sprintf("172.31.%d.%d", i+1, i+10),
			SecurityGroupIDs:   filterEmpty(securityGroupIDs),
			LaunchTime:         now,
			AccountID:          reqCtx.AccountID,
			Region:             reqCtx.Region,
			KeyName:            keyName,
			IamInstanceProfile: iamInstanceProfile,
			UserData:           userData,
			Tags:               tags,

			DisableAPITermination: disableAPITermination,
			AvailabilityZone:      placementAZ,
		}

		// Look up VPCID from subnet and decide whether to assign a public IP.
		subnetData, _ := p.state.Get(context.Background(), ec2Namespace, "subnet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+subnetID)
		if subnetData != nil {
			var subnet EC2Subnet
			if json.Unmarshal(subnetData, &subnet) == nil {
				inst.VPCID = subnet.VPCID
				// A launch that named no zone lands in the subnet's, which is how a
				// real launch resolves it. An explicit Placement.AvailabilityZone
				// wins, because that is the caller's stated intent even where it
				// disagrees with the subnet.
				if inst.AvailabilityZone == "" {
					inst.AvailabilityZone = subnet.AvailabilityZone
				}
				// Always set private DNS name.
				inst.PrivateDNSName = ec2PrivateDNSName(inst.PrivateIPAddress, reqCtx.Region)
				// Assign public IP for default subnets, subnets with
				// MapPublicIPOnLaunch, or when the launch explicitly requested
				// AssociatePublicIpAddress=true (the spawn #308 behavior).
				if subnet.IsDefault || subnet.MapPublicIPOnLaunch || associatePublicIP {
					inst.PublicIPAddress = generatePublicIP(inst.InstanceID)
					inst.PublicDNSName = ec2PublicDNSName(inst.PublicIPAddress, reqCtx.Region)
				}
			}
		}
		// A launch naming neither a zone nor a resolvable subnet still runs
		// somewhere. Default to the region's first zone, matching what CreateSubnet
		// does with an absent AvailabilityZone, so every instance carries a zone and
		// #489's grouping has something to group by.
		if inst.AvailabilityZone == "" {
			inst.AvailabilityZone = reqCtx.Region + "a"
		}

		// Record the interfaces last, since the primary's address and DNS name are
		// the instance's own and those were only just resolved.
		p.ec2AttachInterfaces(&inst, networkInterfaces, i, reqCtx.Region)

		data, err := json.Marshal(inst)
		if err != nil {
			return nil, fmt.Errorf("ec2 runInstances marshal: %w", err)
		}
		stateKey := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + inst.InstanceID
		if err := p.state.Put(context.Background(), ec2Namespace, stateKey, data); err != nil {
			return nil, fmt.Errorf("ec2 runInstances state.Put: %w", err)
		}
		// Update instance_ids list.
		if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "instance_ids", inst.InstanceID); err != nil {
			return nil, err
		}
		// Materialize this instance's volumes, after the instance so they can carry
		// its ID and its zone. Each instance of a multi-count launch gets its own
		// volumes with their own IDs — two instances cannot share one EBS volume.
		if err := p.ec2CreateLaunchVolumes(&inst, blockDeviceMappings, volumeTags, now); err != nil {
			return nil, err
		}
		instances = append(instances, inst)
	}

	return p.runInstancesResponse(instances, reservationID, reqCtx)
}

// ec2GroupItem is one groupSet>item entry, mirroring AWS's GroupIdentifier.
//
// It is declared once at package level rather than inside each response builder
// because RunInstances and DescribeInstances each declared their own instance item
// type, and the family of fields they emit had already drifted — neither carried a
// groupSet at all, so security groups accepted on a launch were invisible to every
// read (#444). One shared type means the two responses cannot disagree about the
// shape.
type ec2GroupItem struct {
	// GroupID is the security group's ID.
	GroupID string `xml:"groupId"`

	// GroupName is the group's name, omitted when it cannot be resolved.
	GroupName string `xml:"groupName,omitempty"`
}

// ec2InstanceStateItem is an instance's instanceState element.
type ec2InstanceStateItem struct {
	// Code is AWS's numeric state code (0 pending, 16 running, 48 terminated).
	Code int `xml:"code"`

	// Name is the state name the code corresponds to.
	Name string `xml:"name"`
}

// ec2PlacementItem is an instance's placement element.
//
// Only availabilityZone is emitted: it is the one member substrate records, and it
// must be readable for a caller to reason about TerminateInstances' zone-scoped
// protection rule (#489).
type ec2PlacementItem struct {
	// AvailabilityZone is the zone the instance was placed in.
	AvailabilityZone string `xml:"availabilityZone"`
}

// ec2IAMInstanceProfileItem is an instance's iamInstanceProfile element.
type ec2IAMInstanceProfileItem struct {
	// ARN is the profile's ARN, derived from the stored name when the launch gave a
	// bare name.
	ARN string `xml:"arn"`

	// ID is the profile's AWS-side identifier.
	ID string `xml:"id"`
}

// ec2InstanceItem is one instancesSet>item, the shape both RunInstances and
// DescribeInstances report an instance in.
//
// Declared once at package level because the two responses each carried their own
// copy and those copies had already drifted twice. #444 was the first round —
// neither emitted a groupSet, so security groups accepted on a launch were invisible
// to every read. By #669 they had drifted again: DescribeInstances' copy grew an
// iamInstanceProfile that RunInstances' did not, they used different Go types for the
// state element, and their placement and instanceState members were in opposite
// orders. So a launch that attached an instance profile answered without it while the
// describe that followed reported it. One shared type and one builder
// ([EC2Plugin.ec2InstanceItemFor]) mean the two responses cannot disagree about an
// instance again.
type ec2InstanceItem struct {
	InstanceID         string                     `xml:"instanceId"`
	ImageID            string                     `xml:"imageId"`
	InstanceType       string                     `xml:"instanceType"`
	LaunchTime         string                     `xml:"launchTime"`
	PrivateIPAddress   string                     `xml:"privateIpAddress"`
	PublicIPAddress    string                     `xml:"publicIpAddress,omitempty"`
	PublicDNSName      string                     `xml:"dnsName,omitempty"`
	PrivateDNSName     string                     `xml:"privateDnsName,omitempty"`
	SubnetID           string                     `xml:"subnetId"`
	VpcID              string                     `xml:"vpcId"`
	KeyName            string                     `xml:"keyName,omitempty"`
	IamInstanceProfile *ec2IAMInstanceProfileItem `xml:"iamInstanceProfile,omitempty"`
	State              ec2InstanceStateItem       `xml:"instanceState"`
	Placement          ec2PlacementItem           `xml:"placement"`
	Groups             []ec2GroupItem             `xml:"groupSet>item"`
	Tags               []ec2TagItem               `xml:"tagSet>item"`

	// BlockDeviceMappings is the instance's view of its EBS volumes, derived from the
	// volume records rather than from the mappings the launch recorded (#669). It is
	// omitted when the instance has none, which is the shape AWS's own DescribeInstances
	// sample shows for an instance-store-backed instance.
	BlockDeviceMappings []ec2BlockDeviceMappingItem `xml:"blockDeviceMapping>item,omitempty"`

	NetworkInterfaces []ec2NetworkInterfaceItem `xml:"networkInterfaceSet>item,omitempty"`
}

// ec2InstanceItemFor builds one instance's response item, shared by RunInstances and
// DescribeInstances so neither can report an instance differently from the other.
//
// mappings is the instance's block device set, resolved by the caller: both callers
// read every volume in the account and region once and bucket by instance, because a
// per-instance read would turn one List into an instances × volumes scan.
func (p *EC2Plugin) ec2InstanceItemFor(
	reqCtx *RequestContext,
	inst EC2Instance,
	mappings []ec2BlockDeviceMappingItem,
) ec2InstanceItem {
	item := ec2InstanceItem{
		InstanceID:          inst.InstanceID,
		ImageID:             inst.ImageID,
		InstanceType:        inst.InstanceType,
		LaunchTime:          inst.LaunchTime,
		PrivateIPAddress:    inst.PrivateIPAddress,
		PublicIPAddress:     inst.PublicIPAddress,
		PublicDNSName:       inst.PublicDNSName,
		PrivateDNSName:      inst.PrivateDNSName,
		SubnetID:            inst.SubnetID,
		VpcID:               inst.VPCID,
		KeyName:             inst.KeyName,
		State:               ec2InstanceStateItem{Code: inst.State.Code, Name: inst.State.Name},
		Placement:           ec2PlacementItem{AvailabilityZone: inst.AvailabilityZone},
		Groups:              p.ec2GroupItems(reqCtx, inst.SecurityGroupIDs),
		BlockDeviceMappings: mappings,
		NetworkInterfaces:   p.ec2NetworkInterfaceItems(reqCtx, inst),
	}
	// Echo the IAM instance profile set at launch (#331). The stored value is the name
	// or ARN supplied; surface it as the ARN and derive an id so a caller can read back
	// the profile it attached.
	if inst.IamInstanceProfile != "" {
		arn := inst.IamInstanceProfile
		if !strings.HasPrefix(arn, "arn:") {
			arn = "arn:aws:iam::" + reqCtx.AccountID + ":instance-profile/" + inst.IamInstanceProfile
		}
		item.IamInstanceProfile = &ec2IAMInstanceProfileItem{ARN: arn, ID: "AIPA" + randomHex(8)}
	}
	// Echo launch-time tags (from TagSpecifications) — real EC2 populates tagSet in
	// the RunInstances response, not just DescribeInstances (#351).
	for _, t := range inst.Tags {
		item.Tags = append(item.Tags, ec2TagItem{Key: t.Key, Value: t.Value}) //nolint:staticcheck // XML tags differ from EC2Tag's JSON tags.
	}
	return item
}

// ec2NetworkInterfaceItem is one networkInterfaceSet>item, the shape both
// RunInstances and DescribeInstances report an instance's interfaces in.
//
// Declared once at package level for the reason [ec2GroupItem] is: the two responses
// carrying their own copies is what let their instance items drift apart before.
// Only the members substrate records are emitted — see [EC2NetworkInterface].
type ec2NetworkInterfaceItem struct {
	NetworkInterfaceID string                    `xml:"networkInterfaceId"`
	SubnetID           string                    `xml:"subnetId,omitempty"`
	VpcID              string                    `xml:"vpcId,omitempty"`
	Description        string                    `xml:"description,omitempty"`
	OwnerID            string                    `xml:"ownerId"`
	Status             string                    `xml:"status"`
	PrivateIPAddress   string                    `xml:"privateIpAddress,omitempty"`
	PrivateDNSName     string                    `xml:"privateDnsName,omitempty"`
	Groups             []ec2GroupItem            `xml:"groupSet>item,omitempty"`
	Attachment         ec2NetworkAttachmentItem  `xml:"attachment"`
	InterfaceType      string                    `xml:"interfaceType,omitempty"`
	PrivateIPAddresses []ec2NetworkPrivateIPItem `xml:"privateIpAddressesSet>item,omitempty"`
}

// ec2NetworkAttachmentItem is an interface's attachment element, which is where AWS
// reports the device index rather than on the interface itself.
type ec2NetworkAttachmentItem struct {
	DeviceIndex         int    `xml:"deviceIndex"`
	Status              string `xml:"status"`
	DeleteOnTermination bool   `xml:"deleteOnTermination"`
	NetworkCardIndex    int    `xml:"networkCardIndex"`
}

// ec2NetworkPrivateIPItem is one entry in an interface's privateIpAddressesSet.
//
// Substrate records a single address per interface, so exactly one entry is emitted
// and it is always primary. The set is still reported rather than omitted, because a
// caller reading privateIpAddressesSet to find the primary address should find it
// there rather than have to know substrate models no secondary addresses.
type ec2NetworkPrivateIPItem struct {
	PrivateIPAddress string `xml:"privateIpAddress"`
	PrivateDNSName   string `xml:"privateDnsName,omitempty"`
	Primary          bool   `xml:"primary"`
}

// ec2NetworkInterfaceItems builds the networkInterfaceSet entries for an instance.
//
// status is "in-use" and the attachment's is "attached" rather than the "attaching"
// a real launch reports mid-attachment: substrate's instances are running by the time
// RunInstances answers, so an interface still attaching would contradict the instance
// state alongside it.
func (p *EC2Plugin) ec2NetworkInterfaceItems(reqCtx *RequestContext, inst EC2Instance) []ec2NetworkInterfaceItem {
	if len(inst.NetworkInterfaces) == 0 {
		return nil
	}
	items := make([]ec2NetworkInterfaceItem, 0, len(inst.NetworkInterfaces))
	for _, ifc := range inst.NetworkInterfaces {
		item := ec2NetworkInterfaceItem{
			NetworkInterfaceID: ifc.NetworkInterfaceID,
			SubnetID:           ifc.SubnetID,
			VpcID:              inst.VPCID,
			Description:        ifc.Description,
			OwnerID:            reqCtx.AccountID,
			Status:             "in-use",
			PrivateIPAddress:   ifc.PrivateIPAddress,
			PrivateDNSName:     ifc.PrivateDNSName,
			Groups:             p.ec2GroupItems(reqCtx, ifc.SecurityGroupIDs),
			Attachment: ec2NetworkAttachmentItem{
				DeviceIndex:         ifc.DeviceIndex,
				Status:              "attached",
				DeleteOnTermination: ifc.DeleteOnTermination,
				NetworkCardIndex:    ifc.NetworkCardIndex,
			},
			InterfaceType: ifc.InterfaceType,
		}
		if ifc.PrivateIPAddress != "" {
			item.PrivateIPAddresses = []ec2NetworkPrivateIPItem{{
				PrivateIPAddress: ifc.PrivateIPAddress,
				PrivateDNSName:   ifc.PrivateDNSName,
				Primary:          true,
			}}
		}
		items = append(items, item)
	}
	return items
}

// ec2GroupItems builds the groupSet entries for an instance's security groups.
//
// A group whose stored record cannot be read contributes its ID with no name
// rather than a fabricated one: a caller comparing groupName against what it
// created would rather see the field absent than see a plausible wrong value.
func (p *EC2Plugin) ec2GroupItems(reqCtx *RequestContext, groupIDs []string) []ec2GroupItem {
	if len(groupIDs) == 0 {
		return nil
	}
	items := make([]ec2GroupItem, 0, len(groupIDs))
	for _, id := range groupIDs {
		item := ec2GroupItem{GroupID: id}
		key := "sg:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + id
		if data, err := p.state.Get(context.Background(), ec2Namespace, key); err == nil && data != nil {
			var sg EC2SecurityGroup
			if json.Unmarshal(data, &sg) == nil {
				item.GroupName = sg.GroupName
			}
		}
		items = append(items, item)
	}
	return items
}

// runInstancesResponse renders a launch's instances.
//
// It reports the same members DescribeInstances does, through the one shared
// [ec2InstanceItem]: networkInterfaceSet, whose presence AWS's own sample response
// carries (#455); tagSet, which real EC2 populates on a launch (#351);
// iamInstanceProfile, which DescribeInstances reported and this response silently
// omitted until #669; and blockDeviceMapping.
//
// blockDeviceMapping is a deliberate divergence. AWS's only RunInstances sample
// response emits <blockDeviceMapping /> empty, on a pending instance whose request
// declared no mappings at all — so the reference neither shows nor forbids a populated
// set here. Substrate renders one on the grounds networkInterfaceSet already uses: its
// instances are running by the time RunInstances answers and their volumes already
// exist, so reporting the instance as having no block devices would contradict the
// state beside it.
func (p *EC2Plugin) runInstancesResponse(instances []EC2Instance, reservationID string, reqCtx *RequestContext) (*AWSResponse, error) {
	type response struct {
		XMLName       xml.Name          `xml:"RunInstancesResponse"`
		XMLNS         string            `xml:"xmlns,attr"`
		ReservationID string            `xml:"reservationId"`
		OwnerID       string            `xml:"ownerId"`
		Instances     []ec2InstanceItem `xml:"instancesSet>item"`
	}

	mappings, err := p.ec2BlockDeviceMappingsByInstance(reqCtx.AccountID, reqCtx.Region)
	if err != nil {
		return nil, err
	}

	resp := response{
		XMLNS:         "http://ec2.amazonaws.com/doc/2016-11-15/",
		ReservationID: reservationID,
		OwnerID:       reqCtx.AccountID,
	}
	for _, inst := range instances {
		resp.Instances = append(resp.Instances,
			p.ec2InstanceItemFor(reqCtx, inst, mappings[inst.InstanceID]))
	}

	return ec2XMLResponse(http.StatusOK, resp)
}

// ec2InstanceMatchesFilters reports whether an instance satisfies every supplied
// DescribeInstances filter (filters are AND-combined; each filter's values are
// OR-combined).
//
// An unrecognized filter name never reaches here: [ec2InstanceFilterSpec] refuses it
// before the scan (#687). So the names this function does not handle are exactly the
// ones DescribeInstances documents and substrate keeps no state to answer, and those
// are inert — see [ec2InstanceMatchesFilter].
func ec2InstanceMatchesFilters(inst EC2Instance, filters map[string][]string) bool {
	for name, values := range filters {
		if !ec2InstanceMatchesFilter(inst, name, values) {
			return false
		}
	}
	return true
}

// ec2InstanceMatchesFilter evaluates a single filter against an instance.
//
// A name it does not handle is one of the 125 DescribeInstances documents over instance
// members substrate does not model, and it is **inert** — the instance matches. Until
// #687 it matched nothing, which was defensible while an unknown name landed here too
// ("never return resources the caller meant to exclude"), but a refused typo is the
// better answer to that concern and it left this operation returning empty where every
// other describe returned everything. One answer for the inert case is the point of
// #687, so this now agrees with [ec2VolumeMatchesFilter] rather than opposing it.
func ec2InstanceMatchesFilter(inst EC2Instance, name string, values []string) bool {
	// tag:<key> — instance has a tag with that key whose value is in values.
	if tagKey, ok := strings.CutPrefix(name, "tag:"); ok {
		for _, t := range inst.Tags {
			if t.Key == tagKey && ec2FilterAccepts(values, t.Value) {
				return true
			}
		}
		return false
	}

	switch name {
	case "instance-state-name":
		return ec2FilterAccepts(values, inst.State.Name)
	case "instance-state-code":
		return ec2FilterAccepts(values, strconv.Itoa(inst.State.Code))
	case "instance-id":
		return ec2FilterAccepts(values, inst.InstanceID)
	case "instance-type":
		return ec2FilterAccepts(values, inst.InstanceType)
	case "image-id":
		return ec2FilterAccepts(values, inst.ImageID)
	case "vpc-id":
		return ec2FilterAccepts(values, inst.VPCID)
	case "subnet-id":
		return ec2FilterAccepts(values, inst.SubnetID)
	case "key-name":
		return ec2FilterAccepts(values, inst.KeyName)
	case "availability-zone":
		// The reference names this filter "availability-zone", not
		// "placement.availability-zone" — the placement family's filters are
		// spelled out individually in DescribeInstances' filter list.
		return ec2FilterAccepts(values, inst.AvailabilityZone)
	case "tag-key":
		// Instance has a tag with any of the requested keys (any value).
		for _, t := range inst.Tags {
			if ec2FilterAccepts(values, t.Key) {
				return true
			}
		}
		return false
	default:
		// A documented filter substrate cannot evaluate is inert; see the doc comment.
		return true
	}
}

func (p *EC2Plugin) describeInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := newEC2IDFilter(extractIndexedParams(req.Params, "InstanceId"), ec2InstanceIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	// The spec check runs before the scan, so a refusal cannot depend on whether any
	// instance matched (#687). [ec2IDFilter.unresolved] already documents the same
	// ordering rule from the other side: a resource a filter excluded still counts as
	// resolved.
	if err := ec2InstanceFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)

	allKeys, err := p.state.List(context.Background(), ec2Namespace, "instance:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeInstances list: %w", err)
	}

	type reservationItem struct {
		ReservationID string            `xml:"reservationId"`
		OwnerID       string            `xml:"ownerId"`
		Instances     []ec2InstanceItem `xml:"instancesSet>item"`
	}
	type response struct {
		XMLName      xml.Name          `xml:"DescribeInstancesResponse"`
		XMLNS        string            `xml:"xmlns,attr"`
		Reservations []reservationItem `xml:"reservationSet>item"`
	}

	// Every volume in the account and region, read once and bucketed by instance:
	// this loop already does a Get per instance and a Get per security group, so a
	// per-instance volume List would compound into an instances × volumes scan.
	mappings, err := p.ec2BlockDeviceMappingsByInstance(reqCtx.AccountID, reqCtx.Region)
	if err != nil {
		return nil, err
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	resMap := make(map[string]*reservationItem)

	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var inst EC2Instance
		if json.Unmarshal(data, &inst) != nil {
			continue
		}
		// Filter by IDs.
		if !ids.match(inst.InstanceID) {
			continue
		}
		// Apply all DescribeInstances filters, AND-combined.
		if !ec2InstanceMatchesFilters(inst, filters) {
			continue
		}

		item := p.ec2InstanceItemFor(reqCtx, inst, mappings[inst.InstanceID])

		if _, ok := resMap[inst.ReservationID]; !ok {
			resMap[inst.ReservationID] = &reservationItem{
				ReservationID: inst.ReservationID,
				OwnerID:       reqCtx.AccountID,
			}
		}
		resMap[inst.ReservationID].Instances = append(resMap[inst.ReservationID].Instances, item)
	}

	if err := ids.unresolved(); err != nil {
		return nil, err
	}

	for _, res := range resMap {
		resp.Reservations = append(resp.Reservations, *res)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) terminateInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "InstanceId")
	type stateChange struct {
		InstanceID   string `xml:"instanceId"`
		CurrentState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"currentState"`
		PreviousState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"previousState"`
	}
	type response struct {
		XMLName xml.Name      `xml:"TerminateInstancesResponse"`
		XMLNS   string        `xml:"xmlns,attr"`
		Items   []stateChange `xml:"instancesSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}

	// Pass 1: resolve every named instance before terminating any. A bad ID fails
	// the whole request without writing state, which is what the reference means by
	// "If you specify multiple instances and the request fails (for example,
	// because of a single incorrect instance ID), none of the instances are
	// terminated." Termination protection is the other pre-flight, and it needs the
	// full set in hand because its scope is the Availability Zone (#489).
	instances := make([]EC2Instance, 0, len(ids))
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		key := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + id
		data, err := p.state.Get(context.Background(), ec2Namespace, key)
		if err != nil {
			return nil, fmt.Errorf("ec2 terminateInstances get: %w", err)
		}
		if lookupErr := ec2RequireResource(ec2InstanceIDKind, id, data != nil); lookupErr != nil {
			return nil, lookupErr
		}
		var inst EC2Instance
		if err := json.Unmarshal(data, &inst); err != nil {
			return nil, fmt.Errorf("ec2 terminateInstances unmarshal: %w", err)
		}
		instances = append(instances, inst)
		keys = append(keys, key)
	}

	// Pass 2: terminate every instance in a zone that holds no protected instance,
	// then report the failure if any zone was blocked. The terminations persist —
	// the request "reports failure" while the unaffected zones still went through —
	// so the ordering here is the behavior, not an implementation detail.
	blocked := ec2TerminationProtectionBlocked(instances)
	for i, inst := range instances {
		if _, azBlocked := blocked[inst.AvailabilityZone]; azBlocked {
			continue
		}
		prev := inst.State
		inst.State = EC2InstanceState{Code: 48, Name: "terminated"}
		inst.TerminatedTime = p.tc.Now().UTC().Format(time.RFC3339)
		newData, err := json.Marshal(inst)
		if err != nil {
			return nil, fmt.Errorf("ec2 terminateInstances marshal: %w", err)
		}
		if err := p.state.Put(context.Background(), ec2Namespace, keys[i], newData); err != nil {
			return nil, fmt.Errorf("ec2 terminateInstances state.Put: %w", err)
		}
		// Settle the instance's volumes: delete those whose attachment says to, and
		// release the rest. A volume left in-use on a terminated instance would be a
		// state real EC2 never reaches (#666).
		if err := p.ec2DeleteInstanceVolumes(reqCtx.AccountID, reqCtx.Region, inst.InstanceID); err != nil {
			return nil, err
		}

		sc := stateChange{InstanceID: inst.InstanceID}
		sc.CurrentState.Code = inst.State.Code
		sc.CurrentState.Name = inst.State.Name
		sc.PreviousState.Code = prev.Code
		sc.PreviousState.Name = prev.Name
		resp.Items = append(resp.Items, sc)
	}
	if len(blocked) > 0 {
		// Report the protected instance that appeared first in request order, so the
		// message is stable across replays rather than depending on map iteration.
		for _, inst := range instances {
			if blockingID, azBlocked := blocked[inst.AvailabilityZone]; azBlocked {
				return nil, ec2TerminationProtectedError(blockingID)
			}
		}
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) stopInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "InstanceId")
	type stateChange struct {
		InstanceID   string `xml:"instanceId"`
		CurrentState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"currentState"`
		PreviousState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"previousState"`
	}
	type response struct {
		XMLName xml.Name      `xml:"StopInstancesResponse"`
		XMLNS   string        `xml:"xmlns,attr"`
		Items   []stateChange `xml:"instancesSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, id := range ids {
		key := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + id
		data, err := p.state.Get(context.Background(), ec2Namespace, key)
		if err != nil {
			return nil, fmt.Errorf("ec2 stopInstances get: %w", err)
		}
		if lookupErr := ec2RequireResource(ec2InstanceIDKind, id, data != nil); lookupErr != nil {
			return nil, lookupErr
		}
		var inst EC2Instance
		if err := json.Unmarshal(data, &inst); err != nil {
			return nil, fmt.Errorf("ec2 stopInstances unmarshal: %w", err)
		}
		prev := inst.State
		inst.State = EC2InstanceState{Code: 80, Name: "stopped"}
		newData, _ := json.Marshal(inst)
		_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
		sc := stateChange{InstanceID: id}
		sc.CurrentState.Code = inst.State.Code
		sc.CurrentState.Name = inst.State.Name
		sc.PreviousState.Code = prev.Code
		sc.PreviousState.Name = prev.Name
		resp.Items = append(resp.Items, sc)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) startInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := extractIndexedParams(req.Params, "InstanceId")
	type stateChange struct {
		InstanceID   string `xml:"instanceId"`
		CurrentState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"currentState"`
		PreviousState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"previousState"`
	}
	type response struct {
		XMLName xml.Name      `xml:"StartInstancesResponse"`
		XMLNS   string        `xml:"xmlns,attr"`
		Items   []stateChange `xml:"instancesSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, id := range ids {
		key := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + id
		data, err := p.state.Get(context.Background(), ec2Namespace, key)
		if err != nil {
			return nil, fmt.Errorf("ec2 startInstances get: %w", err)
		}
		if lookupErr := ec2RequireResource(ec2InstanceIDKind, id, data != nil); lookupErr != nil {
			return nil, lookupErr
		}
		var inst EC2Instance
		if err := json.Unmarshal(data, &inst); err != nil {
			return nil, fmt.Errorf("ec2 startInstances unmarshal: %w", err)
		}
		prev := inst.State
		inst.State = EC2InstanceState{Code: 16, Name: "running"}
		newData, _ := json.Marshal(inst)
		_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
		sc := stateChange{InstanceID: id}
		sc.CurrentState.Code = inst.State.Code
		sc.CurrentState.Name = inst.State.Name
		sc.PreviousState.Code = prev.Code
		sc.PreviousState.Name = prev.Name
		resp.Items = append(resp.Items, sc)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) describeInstanceStatus(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := newEC2IDFilter(extractIndexedParams(req.Params, "InstanceId"), ec2InstanceIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	type statusItem struct {
		InstanceID    string `xml:"instanceId"`
		InstanceState struct {
			Code int    `xml:"code"`
			Name string `xml:"name"`
		} `xml:"instanceState"`
	}
	type response struct {
		XMLName xml.Name     `xml:"DescribeInstanceStatusResponse"`
		XMLNS   string       `xml:"xmlns,attr"`
		Items   []statusItem `xml:"instanceStatusSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}

	allKeys, err := p.state.List(context.Background(), ec2Namespace, "instance:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeInstanceStatus: %w", err)
	}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var inst EC2Instance
		if json.Unmarshal(data, &inst) != nil {
			continue
		}
		if !ids.match(inst.InstanceID) {
			continue
		}
		si := statusItem{InstanceID: inst.InstanceID}
		si.InstanceState.Code = inst.State.Code
		si.InstanceState.Name = inst.State.Name
		resp.Items = append(resp.Items, si)
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- VPC operations ---

func (p *EC2Plugin) createVPC(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	cidr := req.Params["CidrBlock"]
	if cidr == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "CidrBlock is required", HTTPStatus: http.StatusBadRequest}
	}
	vpcID := generateVPCID()
	vpc := EC2VPC{
		VPCID:            vpcID,
		CIDRBlock:        cidr,
		IsDefault:        false,
		State:            "available",
		EnableDNSSupport: true,
		AccountID:        reqCtx.AccountID,
		Region:           reqCtx.Region,
	}
	data, err := json.Marshal(vpc)
	if err != nil {
		return nil, fmt.Errorf("ec2 createVpc marshal: %w", err)
	}
	key := "vpc:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + vpcID
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createVpc state.Put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "vpc_ids", vpcID); err != nil {
		return nil, err
	}
	// Create default route table for VPC.
	if _, err := p.createRouteTableForVPC(reqCtx, vpcID, cidr, true, ""); err != nil {
		p.logger.Warn("ec2: failed to create default route table", "err", err)
	}

	type vpcItem struct {
		VpcID     string `xml:"vpcId"`
		CIDRBlock string `xml:"cidrBlock"`
		IsDefault bool   `xml:"isDefault"`
		State     string `xml:"vpcState"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateVpcResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Vpc     vpcItem  `xml:"vpc"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/",
		Vpc:   vpcItem{VpcID: vpcID, CIDRBlock: cidr, State: "available"},
	})
}

func (p *EC2Plugin) describeVPCs(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := newEC2IDFilter(extractIndexedParams(req.Params, "VpcId"), ec2VPCIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "vpc:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeVpcs: %w", err)
	}
	type vpcItem struct {
		VpcID     string `xml:"vpcId"`
		CIDRBlock string `xml:"cidrBlock"`
		IsDefault bool   `xml:"isDefault"`
		State     string `xml:"vpcState"`
	}
	type response struct {
		XMLName xml.Name  `xml:"DescribeVpcsResponse"`
		XMLNS   string    `xml:"xmlns,attr"`
		Vpcs    []vpcItem `xml:"vpcSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var vpc EC2VPC
		if json.Unmarshal(data, &vpc) != nil {
			continue
		}
		if !ids.match(vpc.VPCID) {
			continue
		}
		resp.Vpcs = append(resp.Vpcs, vpcItem{VpcID: vpc.VPCID, CIDRBlock: vpc.CIDRBlock, IsDefault: vpc.IsDefault, State: vpc.State})
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteVPC(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	vpcID := req.Params["VpcId"]
	if vpcID == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "VpcId is required", HTTPStatus: http.StatusBadRequest}
	}
	key := "vpc:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + vpcID
	existing, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 deleteVpc get: %w", err)
	}
	if lookupErr := ec2RequireResource(ec2VPCIDKind, vpcID, existing != nil); lookupErr != nil {
		return nil, lookupErr
	}
	if err := p.state.Delete(context.Background(), ec2Namespace, key); err != nil {
		return nil, fmt.Errorf("ec2 deleteVpc: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteVpcResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// createSubnet creates a subnet in a VPC, optionally tagging it on creation.
//
// TagSpecification.N arrived with #685 for the same reason it did for CreateVolume in #670:
// CDK and Terraform tag at create time, so without it the tags a caller can then filter on
// could never be set. The tag rules are checked before the record is written, so a rejected
// request creates no subnet.
func (p *EC2Plugin) createSubnet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	vpcID := req.Params["VpcId"]
	cidr := req.Params["CidrBlock"]
	az := req.Params["AvailabilityZone"]
	if az == "" {
		az = reqCtx.Region + "a"
	}
	tags := ec2TagSpecificationTags(req.Params, "", "subnet")
	if awsErr := ec2CheckTagRules(tags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, tags); awsErr != nil {
		return nil, awsErr
	}
	subnetID := generateSubnetID()
	subnet := EC2Subnet{
		SubnetID:         subnetID,
		VPCID:            vpcID,
		CIDRBlock:        cidr,
		AvailabilityZone: az,
		State:            "available",
		Tags:             tags,
		AccountID:        reqCtx.AccountID,
		Region:           reqCtx.Region,
	}
	data, err := json.Marshal(subnet)
	if err != nil {
		return nil, fmt.Errorf("ec2 createSubnet marshal: %w", err)
	}
	key := "subnet:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + subnetID
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createSubnet state.Put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "subnet_ids", subnetID); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name      `xml:"CreateSubnetResponse"`
		XMLNS   string        `xml:"xmlns,attr"`
		Subnet  ec2SubnetItem `xml:"subnet"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:  "http://ec2.amazonaws.com/doc/2016-11-15/",
		Subnet: ec2SubnetXML(subnet),
	})
}

// describeSubnets lists the account's subnets, honoring an optional list of SubnetId.N
// parameters and the eleven Filter.N names [ec2SubnetMatchesFilter] evaluates.
//
// Filter.N was parsed nowhere here before #685. The spec check runs before the scan so a
// refusal cannot depend on whether any subnet matched, and ids.match runs before the filters
// so a subnet a filter excluded still counts as resolved for [ec2IDFilter.unresolved].
func (p *EC2Plugin) describeSubnets(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := newEC2IDFilter(extractIndexedParams(req.Params, "SubnetId"), ec2SubnetIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	if err := ec2SubnetFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "subnet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeSubnets: %w", err)
	}
	type response struct {
		XMLName xml.Name        `xml:"DescribeSubnetsResponse"`
		XMLNS   string          `xml:"xmlns,attr"`
		Subnets []ec2SubnetItem `xml:"subnetSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var subnet EC2Subnet
		if json.Unmarshal(data, &subnet) != nil {
			continue
		}
		if !ids.match(subnet.SubnetID) {
			continue
		}
		if !ec2SubnetMatchesFilters(subnet, filters) {
			continue
		}
		resp.Subnets = append(resp.Subnets, ec2SubnetXML(subnet))
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteSubnet(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	subnetID := req.Params["SubnetId"]
	if subnetID == "" {
		return nil, ec2MissingParameter("SubnetId")
	}
	key := "subnet:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + subnetID
	existing, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 deleteSubnet get: %w", err)
	}
	if lookupErr := ec2RequireResource(ec2SubnetIDKind, subnetID, existing != nil); lookupErr != nil {
		return nil, lookupErr
	}
	if err := p.state.Delete(context.Background(), ec2Namespace, key); err != nil {
		return nil, fmt.Errorf("ec2 deleteSubnet: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteSubnetResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) createSecurityGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	groupName := req.Params["GroupName"]
	description := req.Params["GroupDescription"]
	vpcID := req.Params["VpcId"]
	sgID := generateSGID()
	sg := EC2SecurityGroup{
		GroupID:     sgID,
		GroupName:   groupName,
		Description: description,
		VPCID:       vpcID,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
		EgressRules: []EC2IPPermission{{IPProtocol: "-1", IPRanges: []string{"0.0.0.0/0"}}},
	}
	data, err := json.Marshal(sg)
	if err != nil {
		return nil, fmt.Errorf("ec2 createSecurityGroup marshal: %w", err)
	}
	key := "sg:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + sgID
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createSecurityGroup state.Put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "sg_ids", sgID); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"CreateSecurityGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		GroupID string   `xml:"groupId"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", GroupID: sgID, Return: true})
}

func (p *EC2Plugin) describeSecurityGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := newEC2IDFilter(extractIndexedParams(req.Params, "GroupId"), ec2SecurityGroupIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	if err := ec2SecurityGroupFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "sg:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeSecurityGroups: %w", err)
	}
	type ipRangeItem struct {
		CidrIP string `xml:"cidrIp"` //nolint:revive
	}
	type groupPairItem struct {
		UserID      string `xml:"userId,omitempty"`
		GroupID     string `xml:"groupId,omitempty"`
		GroupName   string `xml:"groupName,omitempty"`
		Description string `xml:"description,omitempty"`
	}
	type permItem struct {
		IPProtocol string          `xml:"ipProtocol"` //nolint:revive
		FromPort   int             `xml:"fromPort"`
		ToPort     int             `xml:"toPort"`
		IPRanges   []ipRangeItem   `xml:"ipRanges>item"` //nolint:revive
		Groups     []groupPairItem `xml:"groups>item,omitempty"`
	}
	type sgItem struct {
		GroupID        string     `xml:"groupId"`
		GroupName      string     `xml:"groupName"`
		Description    string     `xml:"groupDescription"`
		VpcID          string     `xml:"vpcId"`
		IPPermissions  []permItem `xml:"ipPermissions>item"`       //nolint:revive
		IPPermissionsE []permItem `xml:"ipPermissionsEgress>item"` //nolint:revive
	}
	type response struct {
		XMLName xml.Name `xml:"DescribeSecurityGroupsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Groups  []sgItem `xml:"securityGroupInfo>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var sg EC2SecurityGroup
		if json.Unmarshal(data, &sg) != nil {
			continue
		}
		if !ids.match(sg.GroupID) {
			continue
		}
		// A filter naming no values matches nothing, as it does everywhere else: these
		// three arms used to carry an `ok && len(vals) > 0 &&` guard, which returned every
		// security group for a request that had asked for a subset (#696).
		if vals, ok := filters["group-name"]; ok && !ec2FilterAccepts(vals, sg.GroupName) {
			continue
		}
		if vals, ok := filters["vpc-id"]; ok && !ec2FilterAccepts(vals, sg.VPCID) {
			continue
		}
		if vals, ok := filters["group-id"]; ok && !ec2FilterAccepts(vals, sg.GroupID) {
			continue
		}
		renderPerm := func(rule EC2IPPermission) permItem {
			pi := permItem{IPProtocol: rule.IPProtocol, FromPort: rule.FromPort, ToPort: rule.ToPort}
			for _, cidr := range rule.IPRanges {
				pi.IPRanges = append(pi.IPRanges, ipRangeItem{CidrIP: cidr})
			}
			for _, pair := range rule.UserIDGroupPairs {
				pi.Groups = append(pi.Groups, groupPairItem{
					UserID:      pair.UserID,
					GroupID:     pair.GroupID,
					GroupName:   pair.GroupName,
					Description: pair.Description,
				})
			}
			return pi
		}
		item := sgItem{GroupID: sg.GroupID, GroupName: sg.GroupName, Description: sg.Description, VpcID: sg.VPCID}
		for _, rule := range sg.IngressRules {
			item.IPPermissions = append(item.IPPermissions, renderPerm(rule))
		}
		for _, rule := range sg.EgressRules {
			item.IPPermissionsE = append(item.IPPermissionsE, renderPerm(rule))
		}
		resp.Groups = append(resp.Groups, item)
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteSecurityGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	sgID := req.Params["GroupId"]
	if sgID == "" {
		return nil, ec2MissingParameter("GroupId")
	}
	key := "sg:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + sgID
	existing, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 deleteSecurityGroup get: %w", err)
	}
	if lookupErr := ec2RequireResource(ec2SecurityGroupIDKind, sgID, existing != nil); lookupErr != nil {
		return nil, lookupErr
	}
	if err := p.state.Delete(context.Background(), ec2Namespace, key); err != nil {
		return nil, fmt.Errorf("ec2 deleteSecurityGroup: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteSecurityGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) authorizeSecurityGroupIngress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.modifySGRules(reqCtx, req, "ingress", true)
}

func (p *EC2Plugin) revokeSecurityGroupIngress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.modifySGRules(reqCtx, req, "ingress", false)
}

func (p *EC2Plugin) authorizeSecurityGroupEgress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.modifySGRules(reqCtx, req, "egress", true)
}

func (p *EC2Plugin) revokeSecurityGroupEgress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.modifySGRules(reqCtx, req, "egress", false)
}

func (p *EC2Plugin) modifySGRules(reqCtx *RequestContext, req *AWSRequest, direction string, add bool) (*AWSResponse, error) {
	sgID := req.Params["GroupId"]
	key := "sg:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + sgID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 modifySGRules get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2SecurityGroupIDKind, "GroupId", sgID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var sg EC2SecurityGroup
	if unmarshalErr := json.Unmarshal(data, &sg); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 modifySGRules unmarshal: %w", unmarshalErr)
	}

	perms := parseSGPermissions(req.Params, reqCtx.AccountID)
	if len(perms) == 0 {
		return nil, &AWSError{
			Code:       "MissingParameter",
			Message:    "No permissions specified",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	for _, perm := range perms {
		if direction == "ingress" {
			if add {
				sg.IngressRules = append(sg.IngressRules, perm)
			} else {
				sg.IngressRules = removePerm(sg.IngressRules, perm)
			}
		} else {
			if add {
				sg.EgressRules = append(sg.EgressRules, perm)
			} else {
				sg.EgressRules = removePerm(sg.EgressRules, perm)
			}
		}
	}

	newData, _ := json.Marshal(sg)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)

	opName := "AuthorizeSecurityGroupIngressResponse"
	if direction == "egress" && add {
		opName = "AuthorizeSecurityGroupEgressResponse"
	} else if direction == "ingress" && !add {
		opName = "RevokeSecurityGroupIngressResponse"
	} else if direction == "egress" && !add {
		opName = "RevokeSecurityGroupEgressResponse"
	}

	body, _ := xml.Marshal(struct {
		XMLName xml.Name `xml:"response"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
	// Use dynamic XML element name.
	xmlStr := strings.Replace(string(body), "<response ", "<"+opName+" ", 1)
	xmlStr = strings.Replace(xmlStr, "</response>", "</"+opName+">", 1)
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{"Content-Type": "text/xml; charset=UTF-8"}, Body: []byte(xmlStr)}, nil
}

func (p *EC2Plugin) createInternetGateway(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	igwID := generateIGWID()
	igw := EC2InternetGateway{
		InternetGatewayID: igwID,
		AccountID:         reqCtx.AccountID,
		Region:            reqCtx.Region,
	}
	data, _ := json.Marshal(igw)
	key := "igw:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + igwID
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createInternetGateway: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "igw_ids", igwID); err != nil {
		return nil, err
	}
	type igwItem struct {
		InternetGatewayID string `xml:"internetGatewayId"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateInternetGatewayResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		IGW     igwItem  `xml:"internetGateway"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", IGW: igwItem{igwID}})
}

func (p *EC2Plugin) describeInternetGateways(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := newEC2IDFilter(extractIndexedParams(req.Params, "InternetGatewayId"), ec2InternetGatewayIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "igw:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeInternetGateways: %w", err)
	}
	type igwItem struct {
		InternetGatewayID string `xml:"internetGatewayId"`
	}
	type response struct {
		XMLName xml.Name  `xml:"DescribeInternetGatewaysResponse"`
		XMLNS   string    `xml:"xmlns,attr"`
		IGWs    []igwItem `xml:"internetGatewaySet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var igw EC2InternetGateway
		if json.Unmarshal(data, &igw) != nil {
			continue
		}
		if !ids.match(igw.InternetGatewayID) {
			continue
		}
		resp.IGWs = append(resp.IGWs, igwItem{igw.InternetGatewayID})
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) attachInternetGateway(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	igwID := req.Params["InternetGatewayId"]
	vpcID := req.Params["VpcId"]
	key := "igw:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + igwID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 attachInternetGateway get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2InternetGatewayIDKind, "InternetGatewayId", igwID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var igw EC2InternetGateway
	if unmarshalErr := json.Unmarshal(data, &igw); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 attachInternetGateway unmarshal: %w", unmarshalErr)
	}
	igw.Attachments = append(igw.Attachments, EC2IGWAttachment{VPCID: vpcID, State: "available"})
	newData, _ := json.Marshal(igw)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
	type response struct {
		XMLName xml.Name `xml:"AttachInternetGatewayResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) detachInternetGateway(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	igwID := req.Params["InternetGatewayId"]
	vpcID := req.Params["VpcId"]
	key := "igw:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + igwID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 detachInternetGateway get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2InternetGatewayIDKind, "InternetGatewayId", igwID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var igw EC2InternetGateway
	if unmarshalErr := json.Unmarshal(data, &igw); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 detachInternetGateway unmarshal: %w", unmarshalErr)
	}
	newAttachments := igw.Attachments[:0]
	for _, a := range igw.Attachments {
		if a.VPCID != vpcID {
			newAttachments = append(newAttachments, a)
		}
	}
	igw.Attachments = newAttachments
	newData, _ := json.Marshal(igw)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
	type response struct {
		XMLName xml.Name `xml:"DetachInternetGatewayResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) deleteInternetGateway(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	igwID := req.Params["InternetGatewayId"]
	if igwID == "" {
		return nil, ec2MissingParameter("InternetGatewayId")
	}
	key := "igw:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + igwID
	existing, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 deleteInternetGateway get: %w", err)
	}
	if lookupErr := ec2RequireResource(ec2InternetGatewayIDKind, igwID, existing != nil); lookupErr != nil {
		return nil, lookupErr
	}
	if err := p.state.Delete(context.Background(), ec2Namespace, key); err != nil {
		return nil, fmt.Errorf("ec2 deleteInternetGateway: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteInternetGatewayResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) createRouteTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	vpcID := req.Params["VpcId"]
	rtbID, err := p.createRouteTableForVPC(reqCtx, vpcID, "", false, "")
	if err != nil {
		return nil, err
	}
	type rtbItem struct {
		RouteTableID string `xml:"routeTableId"`
		VpcID        string `xml:"vpcId"`
	}
	type response struct {
		XMLName    xml.Name `xml:"CreateRouteTableResponse"`
		XMLNS      string   `xml:"xmlns,attr"`
		RouteTable rtbItem  `xml:"routeTable"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", RouteTable: rtbItem{rtbID, vpcID}})
}

func (p *EC2Plugin) createRouteTableForVPC(reqCtx *RequestContext, vpcID, localCIDR string, main bool, igwID string) (string, error) {
	rtbID := generateRTBID()
	rtb := EC2RouteTable{
		RouteTableID: rtbID,
		VPCID:        vpcID,
		AccountID:    reqCtx.AccountID,
		Region:       reqCtx.Region,
	}
	if localCIDR != "" {
		rtb.Routes = []EC2Route{{DestinationCIDR: localCIDR, GatewayID: "local", State: "active"}}
	}
	// A default VPC's main route table carries a default route to its attached
	// internet gateway, matching real AWS.
	if igwID != "" {
		rtb.Routes = append(rtb.Routes, EC2Route{DestinationCIDR: "0.0.0.0/0", GatewayID: igwID, State: "active"})
	}
	if main {
		rtb.Associations = []EC2RTAssociation{{AssociationID: generateAssociationID(), Main: true}}
	}
	data, _ := json.Marshal(rtb)
	key := "rtb:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + rtbID
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return "", fmt.Errorf("ec2 createRouteTable state.Put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "rtb_ids", rtbID); err != nil {
		return "", err
	}
	return rtbID, nil
}

// routeTableHasSubnet reports whether the route table has an association with any of the
// given subnet IDs, which are the values of DescribeRouteTables' association.subnet-id
// filter.
//
// They are filter values rather than an identifier list, so they match through
// [ec2FilterAccepts] and honor EC2's wildcards like every other filter value (#697). The
// distinction is the one [containsStr] documents: an association.subnet-id of `subnet-*`
// narrows the answer to the route tables that are associated with any subnet at all, where
// the same string in `SubnetId.N` would be a malformed ID.
func routeTableHasSubnet(rtb EC2RouteTable, subnetIDs []string) bool {
	for _, a := range rtb.Associations {
		if a.SubnetID != "" && ec2FilterAccepts(subnetIDs, a.SubnetID) {
			return true
		}
	}
	return false
}

func (p *EC2Plugin) describeRouteTables(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := newEC2IDFilter(extractIndexedParams(req.Params, "RouteTableId"), ec2RouteTableIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	if err := ec2RouteTableFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "rtb:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeRouteTables: %w", err)
	}
	type routeItem struct {
		DestCIDR  string `xml:"destinationCidrBlock"`
		GatewayID string `xml:"gatewayId"`
		State     string `xml:"state"`
	}
	type assocItem struct {
		AssociationID string `xml:"routeTableAssociationId"`
		SubnetID      string `xml:"subnetId,omitempty"`
		Main          bool   `xml:"main"`
	}
	type rtbItem struct {
		RouteTableID string      `xml:"routeTableId"`
		VpcID        string      `xml:"vpcId"`
		Routes       []routeItem `xml:"routeSet>item"`
		Associations []assocItem `xml:"associationSet>item"`
	}
	type response struct {
		XMLName     xml.Name  `xml:"DescribeRouteTablesResponse"`
		XMLNS       string    `xml:"xmlns,attr"`
		RouteTables []rtbItem `xml:"routeTableSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var rtb EC2RouteTable
		if json.Unmarshal(data, &rtb) != nil {
			continue
		}
		if !ids.match(rtb.RouteTableID) {
			continue
		}
		if vals, ok := filters["vpc-id"]; ok && !ec2FilterAccepts(vals, rtb.VPCID) {
			continue
		}
		if vals, ok := filters["association.route-table-id"]; ok && !ec2FilterAccepts(vals, rtb.RouteTableID) {
			continue
		}
		if vals, ok := filters["association.subnet-id"]; ok && !routeTableHasSubnet(rtb, vals) {
			continue
		}
		item := rtbItem{RouteTableID: rtb.RouteTableID, VpcID: rtb.VPCID}
		for _, r := range rtb.Routes {
			item.Routes = append(item.Routes, routeItem{DestCIDR: r.DestinationCIDR, GatewayID: r.GatewayID, State: r.State})
		}
		for _, a := range rtb.Associations {
			item.Associations = append(item.Associations, assocItem{AssociationID: a.AssociationID, SubnetID: a.SubnetID, Main: a.Main}) //nolint:staticcheck // XML tags differ from JSON tags.
		}
		resp.RouteTables = append(resp.RouteTables, item)
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) associateRouteTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	rtbID := req.Params["RouteTableId"]
	subnetID := req.Params["SubnetId"]
	key := "rtb:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + rtbID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 associateRouteTable get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2RouteTableIDKind, "RouteTableId", rtbID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var rtb EC2RouteTable
	if unmarshalErr := json.Unmarshal(data, &rtb); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 associateRouteTable unmarshal: %w", unmarshalErr)
	}
	assocID := generateAssociationID()
	rtb.Associations = append(rtb.Associations, EC2RTAssociation{AssociationID: assocID, SubnetID: subnetID})
	newData, _ := json.Marshal(rtb)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
	type response struct {
		XMLName       xml.Name `xml:"AssociateRouteTableResponse"`
		XMLNS         string   `xml:"xmlns,attr"`
		AssociationID string   `xml:"associationId"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", AssociationID: assocID})
}

func (p *EC2Plugin) disassociateRouteTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	assocID := req.Params["AssociationId"]
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "rtb:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 disassociateRouteTable list: %w", err)
	}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var rtb EC2RouteTable
		if json.Unmarshal(data, &rtb) != nil {
			continue
		}
		newAssoc := rtb.Associations[:0]
		found := false
		for _, a := range rtb.Associations {
			if a.AssociationID == assocID {
				found = true
			} else {
				newAssoc = append(newAssoc, a)
			}
		}
		if found {
			rtb.Associations = newAssoc
			newData, _ := json.Marshal(rtb)
			_ = p.state.Put(context.Background(), ec2Namespace, k, newData)
			break
		}
	}
	type response struct {
		XMLName xml.Name `xml:"DisassociateRouteTableResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) createRoute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	rtbID := req.Params["RouteTableId"]
	destCIDR := req.Params["DestinationCidrBlock"]
	gwID := req.Params["GatewayId"]
	key := "rtb:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + rtbID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 createRoute get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2RouteTableIDKind, "RouteTableId", rtbID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var rtb EC2RouteTable
	if unmarshalErr := json.Unmarshal(data, &rtb); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 createRoute unmarshal: %w", unmarshalErr)
	}
	rtb.Routes = append(rtb.Routes, EC2Route{DestinationCIDR: destCIDR, GatewayID: gwID, State: "active"})
	newData, _ := json.Marshal(rtb)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
	type response struct {
		XMLName xml.Name `xml:"CreateRouteResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// routeTargetGateway returns the route target gateway ID from whichever EC2
// route-target parameter the caller supplied (internet/NAT gateway, instance,
// or network interface), normalised to the single GatewayID field Substrate
// stores per route.
func routeTargetGateway(params map[string]string) string {
	for _, k := range []string{"GatewayId", "NatGatewayId", "InstanceId", "NetworkInterfaceId", "TransitGatewayId", "VpcPeeringConnectionId"} {
		if v := params[k]; v != "" {
			return v
		}
	}
	return ""
}

func (p *EC2Plugin) replaceRoute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	rtbID := req.Params["RouteTableId"]
	destCIDR := req.Params["DestinationCidrBlock"]
	gwID := routeTargetGateway(req.Params)
	key := "rtb:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + rtbID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 replaceRoute get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2RouteTableIDKind, "RouteTableId", rtbID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var rtb EC2RouteTable
	if unmarshalErr := json.Unmarshal(data, &rtb); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 replaceRoute unmarshal: %w", unmarshalErr)
	}
	found := false
	for i := range rtb.Routes {
		if rtb.Routes[i].DestinationCIDR == destCIDR {
			rtb.Routes[i].GatewayID = gwID
			rtb.Routes[i].State = "active"
			found = true
			break
		}
	}
	if !found {
		return nil, &AWSError{Code: "InvalidRoute.NotFound", Message: "no route with destination " + destCIDR, HTTPStatus: http.StatusBadRequest}
	}
	newData, _ := json.Marshal(rtb)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
	type response struct {
		XMLName xml.Name `xml:"ReplaceRouteResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// replaceRouteTableAssociation repoints an existing subnet association to a
// different route table, returning a new association ID (matching AWS, which
// allocates a fresh rtbassoc-* on replacement).
func (p *EC2Plugin) replaceRouteTableAssociation(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	assocID := req.Params["AssociationId"]
	newRtbID := req.Params["RouteTableId"]

	// Resolve the target route table first. Both refusals below used to sit after the
	// source association had already been removed and committed, so a request naming a
	// bogus RouteTableId destroyed the association it was asked to move and then reported
	// a failure — the same "a refusal must leave no state behind" rule #673 established
	// for RunInstances. Every write now happens after both IDs have resolved.
	newKey := "rtb:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + newRtbID
	data, getErr := p.state.Get(context.Background(), ec2Namespace, newKey)
	if getErr != nil {
		return nil, fmt.Errorf("ec2 replaceRouteTableAssociation get: %w", getErr)
	}
	if reqErr := ec2RequireNamedResource(ec2RouteTableIDKind, "RouteTableId", newRtbID, data != nil); reqErr != nil {
		return nil, reqErr
	}

	allKeys, err := p.state.List(context.Background(), ec2Namespace, "rtb:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 replaceRouteTableAssociation list: %w", err)
	}

	// Locate the existing association, capturing its subnet/main flag and the key of the
	// route table holding it. Nothing is written until the association resolves too.
	var moved EC2RTAssociation
	var sourceKey string
	var sourceAssocs []EC2RTAssociation
	found := false
	for _, k := range allKeys {
		srcData, srcErr := p.state.Get(context.Background(), ec2Namespace, k)
		if srcErr != nil {
			return nil, fmt.Errorf("ec2 replaceRouteTableAssociation get %s: %w", k, srcErr)
		}
		if srcData == nil {
			continue
		}
		var rtb EC2RouteTable
		if json.Unmarshal(srcData, &rtb) != nil {
			continue
		}
		remaining := make([]EC2RTAssociation, 0, len(rtb.Associations))
		for _, a := range rtb.Associations {
			if a.AssociationID == assocID {
				moved = a
				found = true
			} else {
				remaining = append(remaining, a)
			}
		}
		if found {
			sourceKey, sourceAssocs = k, remaining
			break
		}
	}
	if !found {
		return nil, &AWSError{Code: "InvalidAssociationID.NotFound", Message: "association " + assocID + " not found", HTTPStatus: http.StatusBadRequest}
	}

	// Detach from the source. Re-read rather than reuse the loop's copy only if the source
	// and target are the same table, which the append below would otherwise clobber.
	if sourceKey != newKey {
		var source EC2RouteTable
		sourceData, srcErr := p.state.Get(context.Background(), ec2Namespace, sourceKey)
		if srcErr != nil {
			return nil, fmt.Errorf("ec2 replaceRouteTableAssociation reread source: %w", srcErr)
		}
		if unmarshalErr := json.Unmarshal(sourceData, &source); unmarshalErr != nil {
			return nil, fmt.Errorf("ec2 replaceRouteTableAssociation unmarshal source: %w", unmarshalErr)
		}
		source.Associations = sourceAssocs
		updated, marshalErr := json.Marshal(source)
		if marshalErr != nil {
			return nil, fmt.Errorf("ec2 replaceRouteTableAssociation marshal source: %w", marshalErr)
		}
		if putErr := p.state.Put(context.Background(), ec2Namespace, sourceKey, updated); putErr != nil {
			return nil, fmt.Errorf("ec2 replaceRouteTableAssociation put source: %w", putErr)
		}
	}

	// Attach a fresh association to the target route table. data was read before the
	// detach above, so when the caller replaced an association with one on the same route
	// table it still carries the old entry; sourceAssocs is that read minus the moved
	// association, which is what the append must build on.
	var target EC2RouteTable
	if unmarshalErr := json.Unmarshal(data, &target); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 replaceRouteTableAssociation unmarshal: %w", unmarshalErr)
	}
	if sourceKey == newKey {
		target.Associations = sourceAssocs
	}
	newAssocID := generateAssociationID()
	target.Associations = append(target.Associations, EC2RTAssociation{AssociationID: newAssocID, SubnetID: moved.SubnetID, Main: moved.Main})
	newData, marshalErr := json.Marshal(target)
	if marshalErr != nil {
		return nil, fmt.Errorf("ec2 replaceRouteTableAssociation marshal: %w", marshalErr)
	}
	if putErr := p.state.Put(context.Background(), ec2Namespace, newKey, newData); putErr != nil {
		return nil, fmt.Errorf("ec2 replaceRouteTableAssociation put: %w", putErr)
	}

	type assocState struct {
		State string `xml:"state"`
	}
	type response struct {
		XMLName          xml.Name   `xml:"ReplaceRouteTableAssociationResponse"`
		XMLNS            string     `xml:"xmlns,attr"`
		NewAssociationID string     `xml:"newAssociationId"`
		AssociationState assocState `xml:"associationState"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:            "http://ec2.amazonaws.com/doc/2016-11-15/",
		NewAssociationID: newAssocID,
		AssociationState: assocState{State: "associated"},
	})
}

func (p *EC2Plugin) deleteRoute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	rtbID := req.Params["RouteTableId"]
	destCIDR := req.Params["DestinationCidrBlock"]
	key := "rtb:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + rtbID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 deleteRoute get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2RouteTableIDKind, "RouteTableId", rtbID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var rtb EC2RouteTable
	if unmarshalErr := json.Unmarshal(data, &rtb); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 deleteRoute unmarshal: %w", unmarshalErr)
	}
	newRoutes := rtb.Routes[:0]
	for _, r := range rtb.Routes {
		if r.DestinationCIDR != destCIDR {
			newRoutes = append(newRoutes, r)
		}
	}
	rtb.Routes = newRoutes
	newData, _ := json.Marshal(rtb)
	_ = p.state.Put(context.Background(), ec2Namespace, key, newData)
	type response struct {
		XMLName xml.Name `xml:"DeleteRouteResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) deleteRouteTable(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	rtbID := req.Params["RouteTableId"]
	if rtbID == "" {
		return nil, ec2MissingParameter("RouteTableId")
	}
	key := "rtb:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + rtbID
	existing, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 deleteRouteTable get: %w", err)
	}
	if lookupErr := ec2RequireResource(ec2RouteTableIDKind, rtbID, existing != nil); lookupErr != nil {
		return nil, lookupErr
	}
	if err := p.state.Delete(context.Background(), ec2Namespace, key); err != nil {
		return nil, fmt.Errorf("ec2 deleteRouteTable: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteRouteTableResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// --- Instance management operations ---

// rebootInstances handles RebootInstances — a no-op in the emulator.
func (p *EC2Plugin) rebootInstances(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	type response struct {
		XMLName xml.Name `xml:"RebootInstancesResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// createTags handles CreateTags — applies key-value tags to one or more EC2 resources.
//
// Keys using the reserved "aws:" prefix are rejected before anything is applied, so a
// mixed request leaves every named resource untouched (#452); a key or value over its
// length limit likewise (#490); and every resource named is checked against the
// per-resource tag limit before any of them is modified (#469). All three checks run
// over the whole request for the same reason: CreateTags accepts up to 1000 resource
// IDs, and a rejection partway through the apply loop would leave a partially-tagged
// state real EC2 never produces.
func (p *EC2Plugin) createTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceIDs := extractIndexedParams(req.Params, "ResourceId")
	tags := extractEC2Tags(req.Params)
	if err := ec2CheckTagRules(tags); err != nil {
		return nil, err
	}
	for _, id := range resourceIDs {
		existing, found, err := p.resourceTags(reqCtx, id)
		if err != nil {
			return nil, err
		}
		// An unknown or absent resource is ignored by the apply loop below, so there is
		// nothing to count against — checking it would reject a request real EC2 accepts
		// as a no-op.
		if !found {
			continue
		}
		if awsErr := ec2CheckTagLimit(existing, tags); awsErr != nil {
			return nil, awsErr
		}
	}
	for _, id := range resourceIDs {
		if err := p.applyTagsToResource(reqCtx, id, tags, false); err != nil {
			return nil, err
		}
	}
	type response struct {
		XMLName xml.Name `xml:"CreateTagsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// deleteTags handles DeleteTags — removes tags from one or more EC2 resources.
//
// Reserved "aws:" keys are rejected here too (#452). The evidence for this is weaker
// than for CreateTags: substrate found no captured real-AWS DeleteTags rejection, so
// the code and message are inherited from the CreateTags capture rather than separately
// observed. The tagging documentation is unambiguous about the outcome — such a tag
// "can't be edited or deleted" by a caller — and a DeleteTags that returned success
// while leaving the tag in place would be its own infidelity, so rejecting is the
// closer of the two available behaviors.
//
// The length limits apply here as well, but asymmetrically: DeleteTags names keys and
// treats the value as optional, so an absent Tag.N.Value is the empty string and passes
// the value check unremarked. Only what the request actually supplied is checked
// (#490), which is what [ec2CheckTagLengths]' upper-bound-only shape gives for free.
func (p *EC2Plugin) deleteTags(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceIDs := extractIndexedParams(req.Params, "ResourceId")
	tags := extractEC2Tags(req.Params)
	if err := ec2CheckTagRules(tags); err != nil {
		return nil, err
	}
	for _, id := range resourceIDs {
		if err := p.applyTagsToResource(reqCtx, id, tags, true); err != nil {
			return nil, err
		}
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteTagsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// modifyInstanceAttribute handles ModifyInstanceAttribute — supports InstanceType,
// UserData and DisableApiTermination changes.
//
// Attribute is Required: No on this operation, unlike on DescribeInstanceAttribute:
// the .Value suffix on the parameter carries the selection, so a request setting
// UserData.Value needs no Attribute at all.
func (p *EC2Plugin) modifyInstanceAttribute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	instID := req.Params["InstanceId"]
	if instID == "" {
		return nil, ec2MissingParameter("InstanceId")
	}

	stateKey := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + instID
	inst, awsErr, err := p.loadInstance(reqCtx, instID, "modifyInstanceAttribute")
	if err != nil || awsErr != nil {
		return nil, errOrAWS(err, awsErr)
	}

	// Some attributes can only be modified while the instance is stopped, and the
	// check runs before anything is written so a refused call leaves the value
	// untouched (#473). This is a new rejection on a path that used to succeed:
	// substrate previously changed the type of a running instance, which real EC2
	// refuses — see [ec2ModifiableWhenStopped] for which attributes are gated and
	// why disableApiTermination is not among them.
	if attr := ec2AttributeModificationState(req.Params); attr != "" && !ec2InstanceStopped(inst) {
		return nil, ec2IncorrectInstanceState(attr, inst.State.Name)
	}

	// Apply supported attribute modifications.
	if v := req.Params["InstanceType.Value"]; v != "" {
		inst.InstanceType = v
	}
	// UserData.Value is read with a presence check rather than a non-empty one, so
	// clearing an instance's user data is expressible: AWS's SecureBlobAttributeValue
	// carries whatever the caller sends, including nothing, and treating "" as
	// "unchanged" would make the clear silently a no-op.
	if v, ok := req.Params["UserData.Value"]; ok {
		inst.UserData = v
	}
	if v, ok := req.Params["DisableApiTermination.Value"]; ok {
		inst.DisableAPITermination = strings.EqualFold(v, "true")
	}

	updated, err := json.Marshal(inst)
	if err != nil {
		return nil, fmt.Errorf("ec2 modifyInstanceAttribute marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ec2Namespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("ec2 modifyInstanceAttribute put: %w", err)
	}

	type response struct {
		XMLName xml.Name `xml:"ModifyInstanceAttributeResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// ec2TaggableResource resolves a taggable EC2 resource ID to the state key its record
// lives under and the resource type its ARN names, reporting whether the ID's prefix
// names a resource type substrate can tag at all.
//
// One switch answers both because for five of the nine prefixes the two spellings
// differ: substrate's state keys abbreviate where the Service Authorization Reference's
// ARN formats do not — sg/security-group, igw/internet-gateway, rtb/route-table,
// eip/elastic-ip, nat/natgateway. Deriving one from the other by string-munging would
// produce arn:aws:ec2:…:sg/sg-…, which matches no ARN a caller can write, so the
// authorization decision (#674) needs the ARN type stated rather than inferred. Keeping
// the pair in one place is what stops the tagging handler's list of taggable resources
// and the authorizer's from drifting apart.
//
// The ARN type is a bare resource type, not a whole ARN: the caller supplies the
// partition, region and account, and all nine of these ARN formats are
// region-and-account qualified — none has the image ARN's deliberately empty account
// field.
func ec2TaggableResource(reqCtx *RequestContext, id string) (stateKey, arnType string, ok bool) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	switch {
	case strings.HasPrefix(id, "i-"):
		return "instance:" + scope + "/" + id, "instance", true
	case strings.HasPrefix(id, "vpc-"):
		return "vpc:" + scope + "/" + id, "vpc", true
	case strings.HasPrefix(id, "subnet-"):
		return "subnet:" + scope + "/" + id, "subnet", true
	case strings.HasPrefix(id, "sg-"):
		return "sg:" + scope + "/" + id, "security-group", true
	case strings.HasPrefix(id, "igw-"):
		return "igw:" + scope + "/" + id, "internet-gateway", true
	case strings.HasPrefix(id, "rtb-"):
		return "rtb:" + scope + "/" + id, "route-table", true
	case strings.HasPrefix(id, "eipalloc-"):
		// elastic-ip, and the ARN carries the allocation ID — which is the ID CreateTags
		// takes for an address, so no translation is needed here either.
		return "eip:" + scope + "/" + id, "elastic-ip", true
	case strings.HasPrefix(id, "nat-"):
		// natgateway, unhyphenated, alone among the nine.
		return "nat:" + scope + "/" + id, "natgateway", true
	case strings.HasPrefix(id, "vol-"):
		// Volumes were the one taggable resource with no arm here, so CreateTags on a
		// vol- ID fell through to the default and answered <return>true</return> having
		// written nothing (#670). Nothing else needed changing:
		// [EC2Plugin.applyTagsToResource] is type-agnostic — it reads and writes the
		// record's "tags" JSON member — and [EC2Volume.Tags] already serializes there.
		return ec2VolumeStateKey(reqCtx.AccountID, reqCtx.Region, id), "volume", true
	case strings.HasPrefix(id, "snap-"):
		// Snapshots had the same silent no-op vol- did, and #689 made it reachable: a
		// caller can now create a snapshot of their own, so tagging one after the fact is
		// something they will do. DescribeTags already reported a snapshot's tags before
		// this arm existed (#688) — its scan reads state directly — so until now
		// DescribeTags could see tags CreateTags had no way to write.
		return ec2SnapshotStateKey(reqCtx.AccountID, reqCtx.Region, id), "snapshot", true
	default:
		return "", "", false
	}
}

// ec2TaggableStateKey returns the state key for a taggable resource ID, and whether
// the ID's prefix names a resource type substrate can tag at all.
//
// Shared by [EC2Plugin.applyTagsToResource] and [EC2Plugin.resourceTags] so the tag
// limit is counted against exactly the resources the apply step will modify: if the two
// disagreed about which IDs resolve, CreateTags would either check a resource it does
// not tag or tag one it did not check.
func ec2TaggableStateKey(reqCtx *RequestContext, id string) (string, bool) {
	stateKey, _, ok := ec2TaggableResource(reqCtx, id)
	return stateKey, ok
}

// resourceTags returns the tags currently on the resource named by id, and whether the
// resource was found at all.
//
// A missing resource reports found=false rather than an error, matching
// [EC2Plugin.applyTagsToResource]: CreateTags on an absent resource is a no-op in
// substrate rather than a rejection, so the tag-limit check has nothing to count.
func (p *EC2Plugin) resourceTags(reqCtx *RequestContext, id string) ([]EC2Tag, bool, error) {
	stateKey, ok := ec2TaggableStateKey(reqCtx, id)
	if !ok {
		return nil, false, nil
	}
	data, err := p.state.Get(context.Background(), ec2Namespace, stateKey)
	if err != nil || data == nil {
		return nil, false, nil //nolint:nilerr // Absent resource — ignored, as by applyTagsToResource.
	}
	var resource map[string]json.RawMessage
	if err := json.Unmarshal(data, &resource); err != nil {
		return nil, false, fmt.Errorf("ec2 resourceTags unmarshal %s: %w", id, err)
	}
	var existing []EC2Tag
	if raw, ok := resource["tags"]; ok {
		_ = json.Unmarshal(raw, &existing)
	}
	return existing, true, nil
}

// applyTagsToResource loads the EC2 resource identified by id, merges or
// removes the provided tags, and saves the updated resource back to state.
// When remove is true, matching tag keys are deleted; otherwise tags are upserted.
func (p *EC2Plugin) applyTagsToResource(reqCtx *RequestContext, id string, tags []EC2Tag, remove bool) error {
	stateKey, ok := ec2TaggableStateKey(reqCtx, id)
	if !ok {
		// Unknown resource type — silently ignore (matches AWS behavior).
		return nil
	}

	data, err := p.state.Get(context.Background(), ec2Namespace, stateKey)
	if err != nil || data == nil {
		return nil //nolint:nilerr // Resource not found — ignore (matches AWS behavior).
	}

	// Use a generic map to avoid switching on concrete struct types.
	var resource map[string]json.RawMessage
	if err := json.Unmarshal(data, &resource); err != nil {
		return fmt.Errorf("ec2 applyTagsToResource unmarshal %s: %w", id, err)
	}

	// Load existing tags.
	var existing []EC2Tag
	if raw, ok := resource["tags"]; ok {
		_ = json.Unmarshal(raw, &existing)
	}

	if remove {
		// Build set of keys to remove.
		removeKeys := make(map[string]bool, len(tags))
		for _, t := range tags {
			removeKeys[t.Key] = true
		}
		filtered := existing[:0]
		for _, t := range existing {
			if !removeKeys[t.Key] {
				filtered = append(filtered, t)
			}
		}
		existing = filtered
	} else {
		// Upsert: update matching keys, append new ones.
		idx := make(map[string]int, len(existing))
		for i, t := range existing {
			idx[t.Key] = i
		}
		for _, t := range tags {
			if i, ok := idx[t.Key]; ok {
				existing[i].Value = t.Value
			} else {
				existing = append(existing, t)
			}
		}
	}

	tagsRaw, _ := json.Marshal(existing)
	resource["tags"] = json.RawMessage(tagsRaw)

	updated, err := json.Marshal(resource)
	if err != nil {
		return fmt.Errorf("ec2 applyTagsToResource marshal %s: %w", id, err)
	}
	return p.state.Put(context.Background(), ec2Namespace, stateKey, updated)
}

// extractEC2Tags extracts Tag.N.Key / Tag.N.Value pairs from query params.
func extractEC2Tags(params map[string]string) []EC2Tag {
	var tags []EC2Tag
	for i := 1; ; i++ {
		key := params[fmt.Sprintf("Tag.%d.Key", i)]
		if key == "" {
			break
		}
		tags = append(tags, EC2Tag{Key: key, Value: params[fmt.Sprintf("Tag.%d.Value", i)]})
	}
	return tags
}

// --- Key pair operations ---

func (p *EC2Plugin) createKeyPair(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["KeyName"]
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "KeyName is required", HTTPStatus: http.StatusBadRequest}
	}

	// Check for duplicate.
	existing, _ := p.state.Get(context.Background(), ec2Namespace, "keypair:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name)
	if existing != nil {
		return nil, &AWSError{Code: "InvalidKeyPair.Duplicate", Message: "The keypair '" + name + "' already exists.", HTTPStatus: http.StatusBadRequest}
	}

	// Generate an EC P-256 key pair — fast and produces a compact PEM.
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("ec2 createKeyPair generate: %w", err)
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("ec2 createKeyPair marshal private key: %w", err)
	}
	keyMaterial := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}))

	pubDER, err := x509.MarshalPKIXPublicKey(&privKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("ec2 createKeyPair marshal public key: %w", err)
	}
	fp := ec2KeyFingerprint(pubDER)

	keyType := req.Params["KeyType"]
	if keyType == "" {
		keyType = "rsa"
	}

	kp := EC2KeyPair{
		KeyPairID:   generateKeyPairID(),
		KeyName:     name,
		Fingerprint: fp,
		KeyType:     keyType,
		CreatedAt:   p.tc.Now().UTC().Format(time.RFC3339),
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
	}
	data, _ := json.Marshal(kp)
	if err := p.state.Put(context.Background(), ec2Namespace, "keypair:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name, data); err != nil {
		return nil, fmt.Errorf("ec2 createKeyPair put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "keypair_names", name); err != nil {
		return nil, err
	}

	type response struct {
		XMLName        xml.Name `xml:"CreateKeyPairResponse"`
		XMLNS          string   `xml:"xmlns,attr"`
		KeyPairID      string   `xml:"keyPairId"`
		KeyName        string   `xml:"keyName"`
		KeyFingerprint string   `xml:"keyFingerprint"`
		KeyType        string   `xml:"keyType"`
		KeyMaterial    string   `xml:"keyMaterial"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:          "http://ec2.amazonaws.com/doc/2016-11-15/",
		KeyPairID:      kp.KeyPairID,
		KeyName:        kp.KeyName,
		KeyFingerprint: kp.Fingerprint,
		KeyType:        kp.KeyType,
		KeyMaterial:    keyMaterial,
	})
}

func (p *EC2Plugin) describeKeyPairs(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	filterNames := extractIndexedParams(req.Params, "KeyName")

	allKeys, err := p.state.List(context.Background(), ec2Namespace, "keypair:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeKeyPairs list: %w", err)
	}

	type kpItem struct {
		KeyPairID      string `xml:"keyPairId"`
		KeyName        string `xml:"keyName"`
		KeyFingerprint string `xml:"keyFingerprint"`
		KeyType        string `xml:"keyType"`
		CreateTime     string `xml:"createTime,omitempty"`
	}
	type response struct {
		XMLName  xml.Name `xml:"DescribeKeyPairsResponse"`
		XMLNS    string   `xml:"xmlns,attr"`
		KeyPairs []kpItem `xml:"keySet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}

	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var kp EC2KeyPair
		if json.Unmarshal(data, &kp) != nil {
			continue
		}
		if len(filterNames) > 0 && !containsStr(filterNames, kp.KeyName) {
			continue
		}
		resp.KeyPairs = append(resp.KeyPairs, kpItem{
			KeyPairID:      kp.KeyPairID,
			KeyName:        kp.KeyName,
			KeyFingerprint: kp.Fingerprint,
			KeyType:        kp.KeyType,
			CreateTime:     kp.CreatedAt,
		})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteKeyPair(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["KeyName"]
	if name == "" {
		name = req.Params["KeyPairId"]
	}
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "KeyName or KeyPairId is required", HTTPStatus: http.StatusBadRequest}
	}

	// Support lookup by KeyPairId: scan for matching pair.
	stateKey := "keypair:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + name
	if strings.HasPrefix(name, "key-") {
		allKeys, _ := p.state.List(context.Background(), ec2Namespace, "keypair:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
		for _, k := range allKeys {
			data, _ := p.state.Get(context.Background(), ec2Namespace, k)
			if data == nil {
				continue
			}
			var kp EC2KeyPair
			if json.Unmarshal(data, &kp) == nil && kp.KeyPairID == name {
				stateKey = k
				break
			}
		}
	}

	if err := p.state.Delete(context.Background(), ec2Namespace, stateKey); err != nil {
		return nil, fmt.Errorf("ec2 deleteKeyPair: %w", err)
	}

	type response struct {
		XMLName xml.Name `xml:"DeleteKeyPairResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) importKeyPair(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["KeyName"]
	pubKeyMaterial := req.Params["PublicKeyMaterial"]
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "KeyName is required", HTTPStatus: http.StatusBadRequest}
	}
	if pubKeyMaterial == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "PublicKeyMaterial is required", HTTPStatus: http.StatusBadRequest}
	}

	// Check for duplicate.
	existing, _ := p.state.Get(context.Background(), ec2Namespace, "keypair:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name)
	if existing != nil {
		return nil, &AWSError{Code: "InvalidKeyPair.Duplicate", Message: "The keypair '" + name + "' already exists.", HTTPStatus: http.StatusBadRequest}
	}

	// Decode the public key material and compute a fingerprint.
	pubBytes, err := base64.StdEncoding.DecodeString(pubKeyMaterial)
	if err != nil {
		// Treat as raw bytes if not base64.
		pubBytes = []byte(pubKeyMaterial)
	}
	fp := ec2KeyFingerprint(pubBytes)

	// Infer key type from SSH public key prefix.
	keyType := "rsa"
	pubStr := string(pubBytes)
	switch {
	case strings.HasPrefix(pubStr, "ssh-ed25519"):
		keyType = "ed25519"
	case strings.HasPrefix(pubStr, "ecdsa-"):
		keyType = "rsa" // EC keys are treated as rsa in EC2 API
	}

	kp := EC2KeyPair{
		KeyPairID:   generateKeyPairID(),
		KeyName:     name,
		Fingerprint: fp,
		KeyType:     keyType,
		CreatedAt:   p.tc.Now().UTC().Format(time.RFC3339),
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
	}
	data, _ := json.Marshal(kp)
	if err := p.state.Put(context.Background(), ec2Namespace, "keypair:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+name, data); err != nil {
		return nil, fmt.Errorf("ec2 importKeyPair put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "keypair_names", name); err != nil {
		return nil, err
	}

	type response struct {
		XMLName        xml.Name `xml:"ImportKeyPairResponse"`
		XMLNS          string   `xml:"xmlns,attr"`
		KeyPairID      string   `xml:"keyPairId"`
		KeyName        string   `xml:"keyName"`
		KeyFingerprint string   `xml:"keyFingerprint"`
		KeyType        string   `xml:"keyType"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:          "http://ec2.amazonaws.com/doc/2016-11-15/",
		KeyPairID:      kp.KeyPairID,
		KeyName:        kp.KeyName,
		KeyFingerprint: kp.Fingerprint,
		KeyType:        kp.KeyType,
	})
}

// ec2KeyFingerprint returns a colon-separated SHA-256 hex fingerprint of
// the provided DER-encoded key bytes, matching the format AWS uses for
// EC2 key pairs.
func ec2KeyFingerprint(derBytes []byte) string {
	sum := sha256.Sum256(derBytes)
	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = fmt.Sprintf("%02x", b)
	}
	return strings.Join(parts, ":")
}

// --- Helper functions ---

// ec2LookupDefaultVPC returns the account and region's default VPC and that VPC's
// default subnet, reading state without writing any of it.
//
// It is a free function taking a [StateManager] for the same reason
// [ec2LookupLaunchTemplate] is one: the authorization decision needs it, and
// CheckAccess runs before the handler and has no plugin to call a method on. A
// launch that omits SubnetId is authorized against the subnet it will actually land
// in, which is only knowable by reading the default VPC first (#673).
//
// The read is split out from [EC2Plugin.ensureDefaultVPC] rather than shared with
// it, because that one *creates* what it does not find — nine records, two of them
// swallowing their own failures. Calling it from the authorizer would let an
// unauthorized request create a VPC, which is the opposite of what authorizing
// early is for.
//
// A nil subnet with a non-nil VPC means the default VPC exists but has no default
// subnet: the caller decides whether to create one (the handler) or treat the
// subnet as one this launch will mint (the authorizer). Both nil means there is no
// default VPC yet. A state error is returned rather than reported as "no default
// VPC", so the handler still fails instead of creating a second one.
func ec2LookupDefaultVPC(ctx context.Context, state StateManager, reqCtx *RequestContext) (*EC2VPC, *EC2Subnet, error) {
	vpcKeys, err := state.List(ctx, ec2Namespace, "vpc:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, nil, fmt.Errorf("ec2 lookupDefaultVPC list vpcs: %w", err)
	}
	for _, k := range vpcKeys {
		data, getErr := state.Get(ctx, ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var vpc EC2VPC
		if json.Unmarshal(data, &vpc) != nil || !vpc.IsDefault {
			continue
		}
		subnetKeys, listErr := state.List(ctx, ec2Namespace, "subnet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
		if listErr != nil {
			return nil, nil, fmt.Errorf("ec2 lookupDefaultVPC list subnets: %w", listErr)
		}
		for _, sk := range subnetKeys {
			sdata, sErr := state.Get(ctx, ec2Namespace, sk)
			if sErr != nil || sdata == nil {
				continue
			}
			var subnet EC2Subnet
			if json.Unmarshal(sdata, &subnet) == nil && subnet.VPCID == vpc.VPCID && subnet.IsDefault {
				return &vpc, &subnet, nil
			}
		}
		return &vpc, nil, nil
	}
	return nil, nil, nil
}

// ec2LookupDefaultSecurityGroup returns the ID of the security group a launch that
// named none falls back to, or "" when none resolves.
//
// Shared between the launch handler and the authorization decision so the group the
// launch attaches and the group its policy is evaluated against cannot differ. The
// rule is deliberately the handler's own, unchanged: the first security-group key in
// the account and region, and only if it belongs to vpcID. That is narrower than
// "the group named default" — a launch in an account whose lexicographically first
// group lives in another VPC attaches no group at all — but widening it would change
// what a launch attaches, which is a separate question from what it is authorized
// against.
func ec2LookupDefaultSecurityGroup(ctx context.Context, state StateManager, reqCtx *RequestContext, vpcID string) string {
	sgKeys, err := state.List(ctx, ec2Namespace, "sg:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil || len(sgKeys) == 0 {
		return ""
	}
	data, getErr := state.Get(ctx, ec2Namespace, sgKeys[0])
	if getErr != nil || data == nil {
		return ""
	}
	var sg EC2SecurityGroup
	if json.Unmarshal(data, &sg) != nil || sg.VPCID != vpcID {
		return ""
	}
	return sg.GroupID
}

// ec2CheckSecurityGroups refuses a launch naming a security group that does not
// exist, or one that exists in a VPC other than vpcID.
//
// vpcID is "" for an existence-only pass, which is what lets a launch be refused
// before the default VPC is created: membership needs the subnet's VPC and the
// subnet may not exist yet, but existence needs nothing (#673). A group substrate's
// own default-VPC branch supplied is checked by the second pass, where vpcID is
// known.
func (p *EC2Plugin) ec2CheckSecurityGroups(reqCtx *RequestContext, ids []string, vpcID string) error {
	for _, id := range ids {
		key := "sg:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + id
		sgData, sgErr := p.state.Get(context.Background(), ec2Namespace, key)
		if sgErr != nil {
			return fmt.Errorf("ec2 ec2CheckSecurityGroups get %s: %w", id, sgErr)
		}
		if reqErr := ec2RequireResource(ec2SecurityGroupIDKind, id, sgData != nil); reqErr != nil {
			return reqErr
		}
		if vpcID == "" {
			continue
		}
		var sg EC2SecurityGroup
		if json.Unmarshal(sgData, &sg) == nil && sg.VPCID != vpcID {
			// Membership, not absence: the group resolved, so this cannot come from
			// [ec2IDKind.notFoundError]. It reuses InvalidGroup.NotFound because that is
			// what EC2 answers for a group in another VPC — the reference's own gloss is
			// "the specified security group does not exist", and from the target VPC's
			// point of view it does not.
			return &AWSError{
				Code:       "InvalidGroup.NotFound",
				Message:    "The security group '" + id + "' does not belong to VPC '" + vpcID + "'",
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
	return nil
}

// ensureDefaultVPC creates the default VPC and subnet if they don't already
// exist for the given account/region.
func (p *EC2Plugin) ensureDefaultVPC(ctx context.Context, reqCtx *RequestContext) (*EC2VPC, *EC2Subnet, error) {
	vpc, subnet, err := ec2LookupDefaultVPC(ctx, p.state, reqCtx)
	if err != nil {
		return nil, nil, err
	}
	if vpc != nil {
		if subnet != nil {
			return vpc, subnet, nil
		}
		// The default VPC exists but has no default subnet; create one.
		created, createErr := p.createDefaultSubnet(ctx, reqCtx, vpc)
		if createErr != nil {
			return nil, nil, createErr
		}
		return vpc, created, nil
	}

	// Create default VPC.
	vpcID := generateVPCID()
	created := EC2VPC{
		VPCID:              vpcID,
		CIDRBlock:          "172.31.0.0/16",
		IsDefault:          true,
		State:              "available",
		EnableDNSSupport:   true,
		EnableDNSHostnames: true,
		AccountID:          reqCtx.AccountID,
		Region:             reqCtx.Region,
	}
	vpcData, _ := json.Marshal(created)
	vpcKey := "vpc:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + vpcID
	if err := p.state.Put(ctx, ec2Namespace, vpcKey, vpcData); err != nil {
		return nil, nil, fmt.Errorf("ec2 ensureDefaultVPC create vpc: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "vpc_ids", vpcID); err != nil {
		return nil, nil, err
	}

	// Create default security group.
	sgID := generateSGID()
	sg := EC2SecurityGroup{
		GroupID:     sgID,
		GroupName:   "default",
		Description: "default VPC security group",
		VPCID:       vpcID,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
		EgressRules: []EC2IPPermission{{IPProtocol: "-1", IPRanges: []string{"0.0.0.0/0"}}},
	}
	sgData, _ := json.Marshal(sg)
	sgKey := "sg:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + sgID
	if err := p.state.Put(ctx, ec2Namespace, sgKey, sgData); err != nil {
		p.logger.Warn("ec2: failed to create default sg", "err", err)
	}
	_ = p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "sg_ids", sgID)

	// Create and attach a default internet gateway so the main route table can
	// carry a 0.0.0.0/0 → igw route, matching a real default VPC.
	igwID := generateIGWID()
	igw := EC2InternetGateway{
		InternetGatewayID: igwID,
		Attachments:       []EC2IGWAttachment{{VPCID: vpcID, State: "available"}},
		AccountID:         reqCtx.AccountID,
		Region:            reqCtx.Region,
	}
	igwData, _ := json.Marshal(igw)
	igwKey := "igw:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + igwID
	if err := p.state.Put(ctx, ec2Namespace, igwKey, igwData); err != nil {
		p.logger.Warn("ec2: failed to create default internet gateway", "err", err)
		igwID = ""
	} else {
		_ = p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "igw_ids", igwID)
	}

	// Create main route table with local route + default IGW route.
	if _, rtErr := p.createRouteTableForVPC(reqCtx, vpcID, "172.31.0.0/16", true, igwID); rtErr != nil {
		p.logger.Warn("ec2: failed to create default route table", "err", rtErr)
	}

	subnet, err = p.createDefaultSubnet(ctx, reqCtx, &created)
	if err != nil {
		return nil, nil, err
	}
	return &created, subnet, nil
}

func (p *EC2Plugin) createDefaultSubnet(ctx context.Context, reqCtx *RequestContext, vpc *EC2VPC) (*EC2Subnet, error) {
	subnetID := generateSubnetID()
	subnet := EC2Subnet{
		SubnetID:            subnetID,
		VPCID:               vpc.VPCID,
		CIDRBlock:           "172.31.0.0/20",
		AvailabilityZone:    reqCtx.Region + "a",
		IsDefault:           true,
		MapPublicIPOnLaunch: true,
		State:               "available",
		AccountID:           reqCtx.AccountID,
		Region:              reqCtx.Region,
	}
	data, _ := json.Marshal(subnet)
	key := "subnet:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + subnetID
	if err := p.state.Put(ctx, ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createDefaultSubnet: %w", err)
	}
	_ = p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "subnet_ids", subnetID)
	return &subnet, nil
}

// appendToList loads the JSON list at listKey, appends id, and saves it back.
func (p *EC2Plugin) appendToList(scope, listName, id string) error {
	key := listName + ":" + scope
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return fmt.Errorf("ec2 appendToList get %s: %w", key, err)
	}
	var ids []string
	if data != nil {
		_ = json.Unmarshal(data, &ids)
	}
	ids = append(ids, id)
	newData, _ := json.Marshal(ids)
	return p.state.Put(context.Background(), ec2Namespace, key, newData)
}

// generatePublicIP returns a deterministic synthetic public IPv4 address in
// Amazon's 54.0.0.0/8 range, derived from the instance ID via FNV-32a hash.
func generatePublicIP(instanceID string) string {
	h := fnv.New32a()
	h.Write([]byte(instanceID))
	n := h.Sum32()
	return fmt.Sprintf("54.%d.%d.%d", (n>>16)&0xFF, (n>>8)&0xFF, n&0xFF)
}

// ec2PublicDNSName builds the AWS-format public DNS hostname for a public IP.
func ec2PublicDNSName(ip, region string) string {
	dashed := strings.ReplaceAll(ip, ".", "-")
	if region == "us-east-1" {
		return fmt.Sprintf("ec2-%s.compute-1.amazonaws.com", dashed)
	}
	return fmt.Sprintf("ec2-%s.%s.compute.amazonaws.com", dashed, region)
}

// ec2PrivateDNSName builds the AWS-format private DNS hostname for a private IP.
func ec2PrivateDNSName(ip, region string) string {
	dashed := strings.ReplaceAll(ip, ".", "-")
	return fmt.Sprintf("ip-%s.%s.compute.internal", dashed, region)
}

// ec2XMLResponse serializes v to XML and returns an AWSResponse.
func ec2XMLResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("ec2 xml marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}

// extractIndexedParams returns values from params where keys match the pattern
// "prefix.1", "prefix.2", ... stopping at the first missing index.
func extractIndexedParams(params map[string]string, prefix string) []string {
	var vals []string
	for i := 1; ; i++ {
		v, ok := params[prefix+"."+strconv.Itoa(i)]
		if !ok {
			// Also check without index for single-value params.
			if i == 1 {
				if v2, ok2 := params[prefix]; ok2 {
					vals = append(vals, v2)
				}
			}
			break
		}
		vals = append(vals, v)
	}
	return vals
}

// extractEC2Filters parses EC2 query-protocol Filter.N.Name / Filter.N.Value.M
// parameters into a map of filter-name → allowed values.
//
// Presence rather than emptiness terminates both loops, so an explicitly empty filter
// value is recorded as a value: Filter.1.Value.1= asks for the empty string, which is a
// different request from naming no values at all, and every caller of this function
// distinguishes the two.
//
// A repeated filter name accumulates into one value list rather than overwriting the
// previous one (#686). AWS documents that filters are ANDed and the values within a
// filter ORed, and says nothing about the same name appearing twice; merging the values
// answers it as an OR, which is substrate's reading. Overwriting was not a reading of
// anything — it silently discarded the earlier filter, so a caller who sent two got the
// last one applied and no indication the first was dropped.
func extractEC2Filters(params map[string]string) map[string][]string {
	filters := make(map[string][]string)
	for i := 1; ; i++ {
		name, ok := params[fmt.Sprintf("Filter.%d.Name", i)]
		if !ok {
			break
		}
		var vals []string
		for j := 1; ; j++ {
			v, ok := params[fmt.Sprintf("Filter.%d.Value.%d", i, j)]
			if !ok {
				break
			}
			vals = append(vals, v)
		}
		if name != "" {
			filters[name] = append(filters[name], vals...)
		}
	}
	return filters
}

// containsStr reports whether s is in the slice, comparing exactly.
//
// Since #697 this is **not** how a filter value is compared — [ec2FilterAccepts] is, and it
// honors EC2's documented wildcards. What is left here is every membership test that is not
// a filter, and each one must stay exact:
//
//   - The `KeyName.N`, `GroupName.N`, `FleetId.N`, `RegionName.N` and `InstanceType.N`
//     parameter lists on DescribeKeyPairs, DescribePlacementGroups, DescribeFleets,
//     DescribeRegions and DescribeInstanceTypes. These are *identifiers*, not filter values:
//     AWS documents wildcards for `Filter.N.Value.N` only, and each of these parameters
//     asserts the resource exists — an unmatched entry raises Invalid*.NotFound rather than
//     narrowing a result set. Globbing them would let `i-*` stand in for an ID and quietly
//     turn a NotFound contract into a match.
//   - [ec2FilterSpec.documents]' name lookup. AWS's wildcards are for values; a filter *name*
//     is one of a fixed documented set and "Filter names are case-sensitive" per the Filter
//     type, so a name is either in the set or refused.
//   - [sgSourcesMatch], which compares two stored permissions to each other rather than a
//     request value to a record. It backs RevokeSecurityGroupIngress/Egress, where the
//     source a caller names is the rule being removed, not a pattern selecting rules.
func containsStr(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// indexedParams collects a 1-based indexed query-parameter list, stopping at the
// first missing index. Each format is a fmt pattern taking the index; they are
// tried in order per index, so a caller can accept several spellings of the same
// list (e.g. AWS's SecurityGroupId.N alongside the SecurityGroupIds.N form some
// hand-built requests use).
func indexedParams(params map[string]string, formats ...string) []string {
	var out []string
	for n := 1; ; n++ {
		var value string
		for _, format := range formats {
			if value = params[fmt.Sprintf(format, n)]; value != "" {
				break
			}
		}
		if value == "" {
			return out
		}
		out = append(out, value)
	}
}

// filterEmpty removes empty strings from slice.
func filterEmpty(slice []string) []string {
	var result []string
	for _, s := range slice {
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}

// removePerm removes the first matching permission from the slice.
// removePerm removes the first permission matching target's protocol, port
// range, and source. The source must match too: a CIDR rule and a
// source-security-group rule on the same port are distinct rules, so revoking
// one must not remove the other.
func removePerm(perms []EC2IPPermission, target EC2IPPermission) []EC2IPPermission {
	for i, p := range perms {
		if p.IPProtocol != target.IPProtocol || p.FromPort != target.FromPort || p.ToPort != target.ToPort {
			continue
		}
		if !sgSourcesMatch(p, target) {
			continue
		}
		return append(perms[:i], perms[i+1:]...)
	}
	return perms
}

// sgSourcesMatch reports whether two permissions have the same source, comparing
// CIDR ranges and referenced security groups. A target that names no source at
// all matches any source, preserving the pre-existing revoke-by-ports behavior.
func sgSourcesMatch(a, target EC2IPPermission) bool {
	if len(target.IPRanges) == 0 && len(target.UserIDGroupPairs) == 0 {
		return true
	}
	for _, cidr := range target.IPRanges {
		if containsStr(a.IPRanges, cidr) {
			return true
		}
	}
	for _, pair := range target.UserIDGroupPairs {
		for _, existing := range a.UserIDGroupPairs {
			if pair.GroupID != "" && existing.GroupID == pair.GroupID {
				return true
			}
			if pair.GroupID == "" && pair.GroupName != "" && existing.GroupName == pair.GroupName {
				return true
			}
		}
	}
	return false
}

// parseSGPermissions parses security-group rules from the EC2 query protocol,
// handling both the flattened IpPermissions.N form and the legacy top-level
// IpProtocol/FromPort/ToPort/CidrIp/SourceSecurityGroupName form.
//
// Each permission may carry several IpRanges.M and Groups.M entries. The Groups
// entries are what make a source-security-group rule — including a
// self-referencing rule, where a group allows traffic from itself — expressible;
// such a rule has no CIDR at all (#388).
func parseSGPermissions(params map[string]string, accountID string) []EC2IPPermission {
	var perms []EC2IPPermission
	for n := 1; ; n++ {
		prefix := fmt.Sprintf("IpPermissions.%d.", n)
		perm, ok := parseSGPermission(params, prefix, accountID)
		if !ok {
			break
		}
		perms = append(perms, perm)
	}
	if len(perms) > 0 {
		return perms
	}

	// Legacy top-level form.
	perm := EC2IPPermission{IPProtocol: params["IpProtocol"]}
	perm.FromPort, _ = strconv.Atoi(params["FromPort"])
	perm.ToPort, _ = strconv.Atoi(params["ToPort"])
	if cidr := params["CidrIp"]; cidr != "" {
		perm.IPRanges = []string{cidr}
	}
	if name := params["SourceSecurityGroupName"]; name != "" {
		perm.UserIDGroupPairs = append(perm.UserIDGroupPairs, EC2UserIDGroupPair{
			GroupName: name,
			UserID:    sgOwnerOrDefault(params["SourceSecurityGroupOwnerId"], accountID),
		})
	}
	if perm.IPProtocol == "" && len(perm.IPRanges) == 0 && len(perm.UserIDGroupPairs) == 0 {
		return nil
	}
	return []EC2IPPermission{perm}
}

// parseSGPermission parses a single IpPermissions.N entry. The bool reports
// whether the entry was present.
func parseSGPermission(params map[string]string, prefix, accountID string) (EC2IPPermission, bool) {
	proto, hasProto := params[prefix+"IpProtocol"]
	perm := EC2IPPermission{IPProtocol: proto}
	perm.FromPort, _ = strconv.Atoi(params[prefix+"FromPort"])
	perm.ToPort, _ = strconv.Atoi(params[prefix+"ToPort"])

	for m := 1; ; m++ {
		cidr, ok := params[fmt.Sprintf("%sIpRanges.%d.CidrIp", prefix, m)]
		if !ok || cidr == "" {
			break
		}
		perm.IPRanges = append(perm.IPRanges, cidr)
	}
	for m := 1; ; m++ {
		groupPrefix := fmt.Sprintf("%sGroups.%d.", prefix, m)
		groupID := params[groupPrefix+"GroupId"]
		groupName := params[groupPrefix+"GroupName"]
		if groupID == "" && groupName == "" {
			break
		}
		perm.UserIDGroupPairs = append(perm.UserIDGroupPairs, EC2UserIDGroupPair{
			GroupID:     groupID,
			GroupName:   groupName,
			UserID:      sgOwnerOrDefault(params[groupPrefix+"UserId"], accountID),
			Description: params[groupPrefix+"Description"],
		})
	}

	present := hasProto || len(perm.IPRanges) > 0 || len(perm.UserIDGroupPairs) > 0
	return perm, present
}

// sgOwnerOrDefault returns owner, or accountID when owner is empty — AWS fills
// in the requesting account for a same-account group reference.
func sgOwnerOrDefault(owner, accountID string) string {
	if owner != "" {
		return owner
	}
	return accountID
}

// --- AMI operations ----------------------------------------------------------

// ec2ImageStateKey returns the state key for an AMI.
func ec2ImageStateKey(accountID, region, imageID string) string {
	return "image:" + accountID + "/" + region + "/" + imageID
}

// createImage creates an AMI from a running or stopped instance.
func (p *EC2Plugin) createImage(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	instanceID := req.Params["InstanceId"]
	name := req.Params["Name"]
	description := req.Params["Description"]
	if instanceID == "" || name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "InstanceId and Name are required", HTTPStatus: http.StatusBadRequest}
	}

	// CreateImage can tag the AMI, the snapshots it creates, or both: "To tag the AMI,
	// the value for ResourceType must be image. To tag the snapshots […] the value for
	// ResourceType must be snapshot. The same tag is applied to all of the snapshots
	// that are created." Both are parsed through the shared parser, so ResourceType is
	// honored rather than ignored — this path previously read TagSpecification.1's tags
	// regardless of what resource they were scoped to, which put snapshot-scoped tags
	// on the AMI (#468).
	tags := ec2LaunchTagsForResource(req.Params, "image")
	snapshotTags := ec2LaunchTagsForResource(req.Params, "snapshot")
	// Checked before the snapshot and image are written, so a rejected request creates
	// neither. See [ec2CheckReservedTagKeys] for the rollback rule this follows.
	if awsErr := ec2CheckTagRules(tags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagRules(snapshotTags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, tags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, snapshotTags); awsErr != nil {
		return nil, awsErr
	}

	// Materialize a backing EBS snapshot for the AMI's root device, so that
	// DescribeSnapshots and the block-device-mapping.snapshot-id filter on
	// DescribeImages can model snapshot-retention logic (#322).
	//
	// The size and the source volume are read from the instance's own root volume rather
	// than being the literal 8 this recorded through v0.105.0; see
	// [EC2Plugin.ec2InstanceRootVolume] for what that constant cost a caller (#689).
	rootVolumeID, rootVolumeSize, err := p.ec2InstanceRootVolume(reqCtx, instanceID)
	if err != nil {
		return nil, err
	}
	snapshotID := generateEBSSnapshotID()
	snap := EC2Snapshot{
		SnapshotID:  snapshotID,
		VolumeID:    rootVolumeID,
		VolumeSize:  rootVolumeSize,
		State:       "completed",
		StartTime:   p.tc.Now().UTC().Format(time.RFC3339),
		Description: "Created by CreateImage for " + name,
		Tags:        snapshotTags,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
	}
	snapData, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("ec2 createImage snapshot marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ec2Namespace, ec2SnapshotStateKey(reqCtx.AccountID, reqCtx.Region, snapshotID), snapData); err != nil {
		return nil, fmt.Errorf("ec2 createImage snapshot put: %w", err)
	}

	imageID := generateImageID()
	img := EC2Image{
		ImageID:      imageID,
		Name:         name,
		Description:  description,
		InstanceID:   instanceID,
		State:        "available",
		CreationDate: p.tc.Now().UTC().Format(time.RFC3339),
		SnapshotID:   snapshotID,
		Tags:         tags,
		AccountID:    reqCtx.AccountID,
		Region:       reqCtx.Region,
	}
	data, err := json.Marshal(img)
	if err != nil {
		return nil, fmt.Errorf("ec2 createImage marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ec2Namespace, ec2ImageStateKey(reqCtx.AccountID, reqCtx.Region, imageID), data); err != nil {
		return nil, fmt.Errorf("ec2 createImage put: %w", err)
	}

	type response struct {
		XMLName xml.Name `xml:"CreateImageResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		ImageID string   `xml:"imageId"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:   "http://ec2.amazonaws.com/doc/2016-11-15/",
		ImageID: imageID,
	})
}

// registerImage registers an AMI, optionally pointing its root device at an
// existing EBS snapshot supplied via BlockDeviceMapping.N.Ebs.SnapshotId. Unlike
// CreateImage (which always materializes a fresh snapshot), RegisterImage lets a
// caller register multiple AMIs that *share* one snapshot — the AWS-faithful way
// to model snapshot sharing, so retain-shared-snapshot logic is testable (#328).
func (p *EC2Plugin) registerImage(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["Name"]
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}

	// Find the first block device mapping that names an EBS snapshot. EC2 sends
	// these as BlockDeviceMapping.N.Ebs.SnapshotId (1-indexed); scan a bounded
	// range so a sparse index doesn't cause an early stop.
	snapshotID := ""
	for i := 1; i <= 32; i++ {
		if v := req.Params[fmt.Sprintf("BlockDeviceMapping.%d.Ebs.SnapshotId", i)]; v != "" {
			snapshotID = v
			break
		}
	}

	imageID := generateImageID()
	img := EC2Image{
		ImageID:      imageID,
		Name:         name,
		Description:  req.Params["Description"],
		State:        "available",
		CreationDate: p.tc.Now().UTC().Format(time.RFC3339),
		SnapshotID:   snapshotID,
		AccountID:    reqCtx.AccountID,
		Region:       reqCtx.Region,
	}
	data, err := json.Marshal(img)
	if err != nil {
		return nil, fmt.Errorf("ec2 registerImage marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ec2Namespace, ec2ImageStateKey(reqCtx.AccountID, reqCtx.Region, imageID), data); err != nil {
		return nil, fmt.Errorf("ec2 registerImage put: %w", err)
	}

	type response struct {
		XMLName xml.Name `xml:"RegisterImageResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		ImageID string   `xml:"imageId"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:   "http://ec2.amazonaws.com/doc/2016-11-15/",
		ImageID: imageID,
	})
}

// describeImages lists AMIs owned by the account, with optional tag filters.
// Supports Owners=["self"] and tag:<key>=<value> Filter entries.
func (p *EC2Plugin) describeImages(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "image:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeImages list: %w", err)
	}

	if err := ec2ImageFilterSpec().check(req.Params); err != nil {
		return nil, err
	}

	// Filters come from the shared extractor rather than a walk of this function's own
	// (#686). The hand-rolled version broke out of its value loop on the first empty
	// string, which turned an explicitly empty filter value into the valueless form and
	// gave this operation a tag: rule no other describe had.
	filters := extractEC2Filters(req.Params)

	type ebsBlockDevice struct {
		SnapshotID string `xml:"snapshotId"`
		VolumeSize int64  `xml:"volumeSize"`
	}
	type blockDeviceItem struct {
		DeviceName string         `xml:"deviceName"`
		EBS        ebsBlockDevice `xml:"ebs"`
	}
	type imageItem struct {
		ImageID             string            `xml:"imageId"`
		Name                string            `xml:"name"`
		Description         string            `xml:"description,omitempty"`
		State               string            `xml:"imageState"`
		OwnerID             string            `xml:"imageOwnerId"`
		CreationDate        string            `xml:"creationDate,omitempty"`
		BlockDeviceMappings []blockDeviceItem `xml:"blockDeviceMapping>item,omitempty"`
		Tags                []ec2TagItem      `xml:"tagSet>item"`
	}
	type response struct {
		XMLName xml.Name    `xml:"DescribeImagesResponse"`
		XMLNS   string      `xml:"xmlns,attr"`
		Images  []imageItem `xml:"imagesSet>item"`
	}

	// The mapping's volumeSize is read from the backing snapshot rather than rendered as
	// the literal 8 this used through v0.105.0. That constant was a second, independent
	// copy of createImage's — so the two agreed only for as long as neither knew a real
	// size, and #689 gave createImage one. Reading the snapshot means there is one source
	// of truth for an AMI's root volume size, and DescribeImages and DescribeSnapshots
	// cannot disagree about the same snapshot.
	resolveSnapshot := p.ec2SnapshotResolver(reqCtx)

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var img EC2Image
		if json.Unmarshal(data, &img) != nil {
			continue
		}

		// Apply filters. Every named filter must match — AWS ANDs filters and ORs the
		// values within one — so the map's iteration order does not affect the outcome.
		if !ec2ImageMatchesFilters(img, filters) {
			continue
		}

		item := imageItem{
			ImageID:      img.ImageID,
			Name:         img.Name,
			Description:  img.Description,
			State:        img.State,
			OwnerID:      img.AccountID,
			CreationDate: img.CreationDate,
		}
		if img.SnapshotID != "" {
			// An AMI whose snapshot record is gone — DeleteSnapshot removes it and leaves
			// the AMI standing — falls back to the default rather than reporting 0, which
			// is what a caller sizing a volume off this member can act on.
			size := int64(ec2DefaultVolumeSizeGiB)
			if snap, found := resolveSnapshot(img.SnapshotID); found && snap.VolumeSize > 0 {
				size = snap.VolumeSize
			}
			item.BlockDeviceMappings = []blockDeviceItem{{
				DeviceName: "/dev/sda1",
				EBS:        ebsBlockDevice{SnapshotID: img.SnapshotID, VolumeSize: size},
			}}
		}
		item.Tags = ec2TagItems(img.Tags)
		resp.Images = append(resp.Images, item)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// ec2ImageMatchesFilters reports whether img satisfies every filter the caller named.
//
// Filters are ANDed and the values within one ORed, which is what Using_Filtering
// documents, so the map's iteration order cannot affect the answer. A name substrate does
// not evaluate is ignored rather than refused; one rule for an unrecognized filter name
// is #687's subject, not this function's.
//
// A filter naming no values matches nothing (#686). AWS documents no rule for that shape
// — Using_Filtering says only "You can't specify a filter value of null" — so this is
// substrate's reading, taken from ec2InstanceMatchesFilter and describeVolumes, which
// both already answer that way. describeImages previously read it as the any-value
// question instead, which is what tag-key spells and what this function now answers under
// that name.
func ec2ImageMatchesFilters(img EC2Image, filters map[string][]string) bool {
	for name, vals := range filters {
		matched := false
		switch {
		case strings.HasPrefix(name, "tag:"):
			key := strings.TrimPrefix(name, "tag:")
			for _, t := range img.Tags {
				if t.Key == key && ec2FilterAccepts(vals, t.Value) {
					matched = true
					break
				}
			}
		case name == "tag-key":
			for _, t := range img.Tags {
				if ec2FilterAccepts(vals, t.Key) {
					matched = true
					break
				}
			}
		case name == "block-device-mapping.snapshot-id":
			matched = img.SnapshotID != "" && ec2FilterAccepts(vals, img.SnapshotID)
		case name == "image-id":
			matched = ec2FilterAccepts(vals, img.ImageID)
		default:
			continue
		}
		if !matched {
			return false
		}
	}
	return true
}

// deregisterImage removes an AMI from state.
func (p *EC2Plugin) deregisterImage(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	imageID := req.Params["ImageId"]
	if imageID == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "ImageId is required", HTTPStatus: http.StatusBadRequest}
	}
	key := ec2ImageStateKey(reqCtx.AccountID, reqCtx.Region, imageID)
	if err := p.state.Delete(context.Background(), ec2Namespace, key); err != nil {
		return nil, fmt.Errorf("ec2 deregisterImage delete: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"DeregisterImageResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:  "http://ec2.amazonaws.com/doc/2016-11-15/",
		Return: true,
	})
}

// --- Availability Zone operations ---

func (p *EC2Plugin) describeAvailabilityZones(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	region := reqCtx.Region
	// Derive abbreviated region for zoneId (e.g. "use1" from "us-east-1").
	abbrev := azRegionAbbrev(region)
	type azItem struct {
		ZoneName   string `xml:"zoneName"`
		State      string `xml:"zoneState"`
		RegionName string `xml:"regionName"`
		ZoneID     string `xml:"zoneId"`
	}
	type response struct {
		XMLName           xml.Name `xml:"DescribeAvailabilityZonesResponse"`
		XMLNS             string   `xml:"xmlns,attr"`
		AvailabilityZones []azItem `xml:"availabilityZoneInfo>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for i, suffix := range ec2SeededAZSuffixes {
		resp.AvailabilityZones = append(resp.AvailabilityZones, azItem{
			ZoneName:   region + suffix,
			State:      "available",
			RegionName: region,
			ZoneID:     abbrev + "-az" + strconv.Itoa(i+1),
		})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// createPlacementGroup creates an EC2 placement group (#344). A repeat create of
// an existing name returns InvalidPlacementGroup.Duplicate.
func (p *EC2Plugin) createPlacementGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["GroupName"]
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "GroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	strategy := req.Params["Strategy"]
	if strategy == "" {
		strategy = "cluster"
	}

	key := ec2PlacementGroupStateKey(reqCtx.AccountID, reqCtx.Region, name)
	if existing, _ := p.state.Get(context.Background(), ec2Namespace, key); existing != nil {
		return nil, &AWSError{Code: "InvalidPlacementGroup.Duplicate", Message: "The placement group '" + name + "' already exists.", HTTPStatus: http.StatusBadRequest}
	}

	pg := EC2PlacementGroup{
		GroupName: name,
		GroupID:   generatePlacementGroupID(),
		Strategy:  strategy,
		State:     "available",
		AccountID: reqCtx.AccountID,
		Region:    reqCtx.Region,
	}
	data, err := json.Marshal(pg)
	if err != nil {
		return nil, fmt.Errorf("ec2 createPlacementGroup marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createPlacementGroup put: %w", err)
	}

	type pgItem struct {
		GroupName string `xml:"groupName"`
		GroupID   string `xml:"groupId"`
		Strategy  string `xml:"strategy"`
		State     string `xml:"state"`
	}
	type response struct {
		XMLName        xml.Name `xml:"CreatePlacementGroupResponse"`
		XMLNS          string   `xml:"xmlns,attr"`
		PlacementGroup pgItem   `xml:"placementGroup"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:          "http://ec2.amazonaws.com/doc/2016-11-15/",
		PlacementGroup: pgItem{GroupName: pg.GroupName, GroupID: pg.GroupID, Strategy: pg.Strategy, State: pg.State},
	})
}

// describePlacementGroups lists placement groups, optionally filtered by
// GroupName.N parameters (#344).
func (p *EC2Plugin) describePlacementGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	names := extractIndexedParams(req.Params, "GroupName")
	allKeys, err := p.state.List(context.Background(), ec2Namespace,
		"placement_group:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describePlacementGroups list: %w", err)
	}

	type pgItem struct {
		GroupName string `xml:"groupName"`
		GroupID   string `xml:"groupId"`
		Strategy  string `xml:"strategy"`
		State     string `xml:"state"`
	}
	type response struct {
		XMLName xml.Name `xml:"DescribePlacementGroupsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Groups  []pgItem `xml:"placementGroupSet>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var pg EC2PlacementGroup
		if json.Unmarshal(data, &pg) != nil {
			continue
		}
		if len(names) > 0 && !containsStr(names, pg.GroupName) {
			continue
		}
		resp.Groups = append(resp.Groups, pgItem{GroupName: pg.GroupName, GroupID: pg.GroupID, Strategy: pg.Strategy, State: pg.State})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// deletePlacementGroup removes a placement group by name (#344). Deleting an
// unknown group returns InvalidPlacementGroup.Unknown.
func (p *EC2Plugin) deletePlacementGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["GroupName"]
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "GroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	key := ec2PlacementGroupStateKey(reqCtx.AccountID, reqCtx.Region, name)
	data, _ := p.state.Get(context.Background(), ec2Namespace, key)
	if data == nil {
		return nil, &AWSError{Code: "InvalidPlacementGroup.Unknown", Message: "The placement group '" + name + "' is unknown.", HTTPStatus: http.StatusBadRequest}
	}
	if err := p.state.Delete(context.Background(), ec2Namespace, key); err != nil {
		return nil, fmt.Errorf("ec2 deletePlacementGroup delete: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"DeletePlacementGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// azRegionAbbrev returns a short abbreviation for a region name used in zone IDs
// (e.g. "us-east-1" → "use1", "eu-west-2" → "euw2").
func azRegionAbbrev(region string) string {
	// Remove hyphens and digits, keep first letters of each segment plus trailing digit.
	parts := strings.Split(region, "-")
	if len(parts) < 2 {
		return region
	}
	var sb strings.Builder
	for _, p := range parts {
		if len(p) > 0 {
			sb.WriteByte(p[0])
			// Append trailing digit if present.
			if last := p[len(p)-1]; last >= '0' && last <= '9' {
				sb.WriteByte(last)
			}
		}
	}
	return sb.String()
}

// --- Subnet/VPC attribute operations ---

func (p *EC2Plugin) modifySubnetAttribute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	subnetID := req.Params["SubnetId"]
	key := "subnet:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + subnetID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 modifySubnetAttribute get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2SubnetIDKind, "SubnetId", subnetID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var subnet EC2Subnet
	if unmarshalErr := json.Unmarshal(data, &subnet); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 modifySubnetAttribute unmarshal: %w", unmarshalErr)
	}
	if v, ok := req.Params["MapPublicIPOnLaunch.Value"]; ok {
		subnet.MapPublicIPOnLaunch = v == "true"
	}
	newData, _ := json.Marshal(subnet)
	if err := p.state.Put(context.Background(), ec2Namespace, key, newData); err != nil {
		return nil, fmt.Errorf("ec2 modifySubnetAttribute put: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"ModifySubnetAttributeResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) modifyVpcAttribute(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	vpcID := req.Params["VpcId"]
	key := "vpc:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + vpcID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 modifyVpcAttribute get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2VPCIDKind, "VpcId", vpcID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var vpc EC2VPC
	if unmarshalErr := json.Unmarshal(data, &vpc); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 modifyVpcAttribute unmarshal: %w", unmarshalErr)
	}
	if v, ok := req.Params["EnableDNSSupport.Value"]; ok {
		vpc.EnableDNSSupport = v == "true"
	}
	if v, ok := req.Params["EnableDNSHostnames.Value"]; ok {
		vpc.EnableDNSHostnames = v == "true"
	}
	newData, _ := json.Marshal(vpc)
	if err := p.state.Put(context.Background(), ec2Namespace, key, newData); err != nil {
		return nil, fmt.Errorf("ec2 modifyVpcAttribute put: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"ModifyVpcAttributeResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// --- Elastic IP operations ---

func (p *EC2Plugin) allocateAddress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	domain := req.Params["Domain"]
	if domain == "" {
		domain = "vpc"
	}
	allocationID := generateAllocationID()
	publicIP := generatePublicIP(allocationID)
	eip := EC2ElasticIP{
		AllocationID: allocationID,
		PublicIP:     publicIP,
		Domain:       domain,
		AccountID:    reqCtx.AccountID,
		Region:       reqCtx.Region,
	}
	data, err := json.Marshal(eip)
	if err != nil {
		return nil, fmt.Errorf("ec2 allocateAddress marshal: %w", err)
	}
	key := "eip:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + allocationID
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 allocateAddress put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "eip_ids", allocationID); err != nil {
		return nil, err
	}
	type response struct {
		XMLName            xml.Name `xml:"AllocateAddressResponse"`
		XMLNS              string   `xml:"xmlns,attr"`
		PublicIP           string   `xml:"publicIp"`
		AllocationID       string   `xml:"allocationId"`
		Domain             string   `xml:"domain"`
		NetworkBorderGroup string   `xml:"networkBorderGroup"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:              "http://ec2.amazonaws.com/doc/2016-11-15/",
		PublicIP:           publicIP,
		AllocationID:       allocationID,
		Domain:             domain,
		NetworkBorderGroup: reqCtx.Region,
	})
}

func (p *EC2Plugin) associateAddress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	allocationID := req.Params["AllocationId"]
	instanceID := req.Params["InstanceId"]
	networkInterfaceID := req.Params["NetworkInterfaceId"]

	key := "eip:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + allocationID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 associateAddress get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2AllocationIDKind, "AllocationId", allocationID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var eip EC2ElasticIP
	if unmarshalErr := json.Unmarshal(data, &eip); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 associateAddress unmarshal: %w", unmarshalErr)
	}

	assocID := generateEIPAssociationID()
	eip.AssociationID = assocID
	eip.InstanceID = instanceID
	eip.NetworkInterfaceID = networkInterfaceID

	// If associating with an instance, update the instance's public IP.
	if instanceID != "" {
		instKey := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + instanceID
		instData, instErr := p.state.Get(context.Background(), ec2Namespace, instKey)
		if instErr == nil && instData != nil {
			var inst EC2Instance
			if json.Unmarshal(instData, &inst) == nil {
				eip.PrivateIPAddress = inst.PrivateIPAddress
				inst.PublicIPAddress = eip.PublicIP
				inst.PublicDNSName = ec2PublicDNSName(eip.PublicIP, reqCtx.Region)
				newInstData, _ := json.Marshal(inst)
				_ = p.state.Put(context.Background(), ec2Namespace, instKey, newInstData)
			}
		}
	}

	newData, _ := json.Marshal(eip)
	if err := p.state.Put(context.Background(), ec2Namespace, key, newData); err != nil {
		return nil, fmt.Errorf("ec2 associateAddress put: %w", err)
	}

	type response struct {
		XMLName       xml.Name `xml:"AssociateAddressResponse"`
		XMLNS         string   `xml:"xmlns,attr"`
		AssociationID string   `xml:"associationId"`
		Return        bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:         "http://ec2.amazonaws.com/doc/2016-11-15/",
		AssociationID: assocID,
		Return:        true,
	})
}

func (p *EC2Plugin) disassociateAddress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	assocID := req.Params["AssociationId"]
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "eip:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 disassociateAddress list: %w", err)
	}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var eip EC2ElasticIP
		if json.Unmarshal(data, &eip) != nil || eip.AssociationID != assocID {
			continue
		}
		// Clear instance public IP if associated.
		if eip.InstanceID != "" {
			instKey := "instance:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + eip.InstanceID
			instData, instErr := p.state.Get(context.Background(), ec2Namespace, instKey)
			if instErr == nil && instData != nil {
				var inst EC2Instance
				if json.Unmarshal(instData, &inst) == nil {
					inst.PublicIPAddress = ""
					inst.PublicDNSName = ""
					newInstData, _ := json.Marshal(inst)
					_ = p.state.Put(context.Background(), ec2Namespace, instKey, newInstData)
				}
			}
		}
		eip.AssociationID = ""
		eip.InstanceID = ""
		eip.NetworkInterfaceID = ""
		eip.PrivateIPAddress = ""
		newData, _ := json.Marshal(eip)
		_ = p.state.Put(context.Background(), ec2Namespace, k, newData)
		break
	}
	type response struct {
		XMLName xml.Name `xml:"DisassociateAddressResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) releaseAddress(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	allocationID := req.Params["AllocationId"]
	key := "eip:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + allocationID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 releaseAddress get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2AllocationIDKind, "AllocationId", allocationID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var eip EC2ElasticIP
	if unmarshalErr := json.Unmarshal(data, &eip); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 releaseAddress unmarshal: %w", unmarshalErr)
	}
	if eip.AssociationID != "" {
		return nil, &AWSError{Code: "InvalidIPAddress.InUse", Message: "The address is currently in use and cannot be released", HTTPStatus: http.StatusBadRequest}
	}
	if err := p.state.Delete(context.Background(), ec2Namespace, key); err != nil {
		return nil, fmt.Errorf("ec2 releaseAddress delete: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"ReleaseAddressResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) describeAddresses(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	filterIDs := newEC2IDFilter(extractIndexedParams(req.Params, "AllocationId"), ec2AllocationIDKind)
	if err := filterIDs.validate(); err != nil {
		return nil, err
	}
	allKeys, err := p.state.List(context.Background(), ec2Namespace, "eip:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeAddresses list: %w", err)
	}
	type addressItem struct {
		AllocationID       string `xml:"allocationId"`
		PublicIP           string `xml:"publicIp"`
		AssociationID      string `xml:"associationId,omitempty"`
		InstanceID         string `xml:"instanceId,omitempty"`
		NetworkInterfaceID string `xml:"networkInterfaceId,omitempty"`
		PrivateIPAddress   string `xml:"privateIpAddress,omitempty"`
		Domain             string `xml:"domain"`
	}
	type response struct {
		XMLName   xml.Name      `xml:"DescribeAddressesResponse"`
		XMLNS     string        `xml:"xmlns,attr"`
		Addresses []addressItem `xml:"addressesSet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var eip EC2ElasticIP
		if json.Unmarshal(data, &eip) != nil {
			continue
		}
		if !filterIDs.match(eip.AllocationID) {
			continue
		}
		resp.Addresses = append(resp.Addresses, addressItem{
			AllocationID:       eip.AllocationID,
			PublicIP:           eip.PublicIP,
			AssociationID:      eip.AssociationID,
			InstanceID:         eip.InstanceID,
			NetworkInterfaceID: eip.NetworkInterfaceID,
			PrivateIPAddress:   eip.PrivateIPAddress,
			Domain:             eip.Domain,
		})
	}
	if err := filterIDs.unresolved(); err != nil {
		return nil, err
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- NAT Gateway operations ---

func (p *EC2Plugin) createNatGateway(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	subnetID := req.Params["SubnetId"]
	allocationID := req.Params["AllocationId"]
	connectivityType := req.Params["ConnectivityType"]
	if connectivityType == "" {
		connectivityType = "public"
	}

	// Look up subnet to get VPCID.
	subnetKey := "subnet:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + subnetID
	subnetData, err := p.state.Get(context.Background(), ec2Namespace, subnetKey)
	if err != nil {
		return nil, fmt.Errorf("ec2 createNatGateway get subnet: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2SubnetIDKind, "SubnetId", subnetID, subnetData != nil); reqErr != nil {
		return nil, reqErr
	}
	var subnet EC2Subnet
	if unmarshalErr := json.Unmarshal(subnetData, &subnet); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 createNatGateway unmarshal subnet: %w", unmarshalErr)
	}

	natID := generateNATGatewayID()

	// Compute a stable private IP using FNV hash on the NAT gateway ID.
	h := fnv.New32a()
	h.Write([]byte(natID))
	n := h.Sum32()
	privateIP := fmt.Sprintf("10.0.%d.%d", (n>>8)&0xFF, n&0xFF)

	gw := EC2NATGateway{
		NatGatewayID:     natID,
		SubnetID:         subnetID,
		VPCID:            subnet.VPCID,
		PrivateIP:        privateIP,
		State:            "available",
		ConnectivityType: connectivityType,
		CreateTime:       p.tc.Now().UTC().Format(time.RFC3339),
		AccountID:        reqCtx.AccountID,
		Region:           reqCtx.Region,
	}

	// For public NAT gateways, look up the EIP.
	if connectivityType == "public" && allocationID != "" {
		eipKey := "eip:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + allocationID
		eipData, eipErr := p.state.Get(context.Background(), ec2Namespace, eipKey)
		if eipErr == nil && eipData != nil {
			var eip EC2ElasticIP
			if json.Unmarshal(eipData, &eip) == nil {
				gw.AllocationID = allocationID
				gw.PublicIP = eip.PublicIP
			}
		}
	}

	gw.Tags = ec2LaunchTagsForResource(req.Params, "natgateway")
	if awsErr := ec2CheckTagRules(gw.Tags); awsErr != nil {
		return nil, awsErr
	}
	if awsErr := ec2CheckTagLimit(nil, gw.Tags); awsErr != nil {
		return nil, awsErr
	}

	data, marshalErr := json.Marshal(gw)
	if marshalErr != nil {
		return nil, fmt.Errorf("ec2 createNatGateway marshal: %w", marshalErr)
	}
	stateKey := "nat:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + natID
	if err := p.state.Put(context.Background(), ec2Namespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("ec2 createNatGateway put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "nat_ids", natID); err != nil {
		return nil, err
	}

	type natAddrItem struct {
		AllocationID string `xml:"allocationId,omitempty"`
		PublicIP     string `xml:"publicIp,omitempty"`
		PrivateIP    string `xml:"privateIp"`
	}
	type natItem struct {
		NatGatewayID     string        `xml:"natGatewayId"`
		SubnetID         string        `xml:"subnetId"`
		VpcID            string        `xml:"vpcId"`
		State            string        `xml:"state"`
		ConnectivityType string        `xml:"connectivityType"`
		CreateTime       string        `xml:"createTime"`
		Addresses        []natAddrItem `xml:"natGatewayAddressSet>item"`
	}
	type response struct {
		XMLName    xml.Name `xml:"CreateNatGatewayResponse"`
		XMLNS      string   `xml:"xmlns,attr"`
		NatGateway natItem  `xml:"natGateway"`
	}
	item := natItem{
		NatGatewayID:     natID,
		SubnetID:         subnetID,
		VpcID:            subnet.VPCID,
		State:            "available",
		ConnectivityType: connectivityType,
		CreateTime:       gw.CreateTime,
		Addresses: []natAddrItem{{
			AllocationID: gw.AllocationID,
			PublicIP:     gw.PublicIP,
			PrivateIP:    privateIP,
		}},
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:      "http://ec2.amazonaws.com/doc/2016-11-15/",
		NatGateway: item,
	})
}

func (p *EC2Plugin) describeNatGateways(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	filterIDs := newEC2IDFilter(extractIndexedParams(req.Params, "NatGatewayId"), ec2NatGatewayIDKind)
	if err := filterIDs.validate(); err != nil {
		return nil, err
	}
	if err := ec2NatGatewayFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)

	allKeys, err := p.state.List(context.Background(), ec2Namespace, "nat:"+reqCtx.AccountID+"/"+reqCtx.Region+"/")
	if err != nil {
		return nil, fmt.Errorf("ec2 describeNatGateways list: %w", err)
	}

	type natAddrItem struct {
		AllocationID string `xml:"allocationId,omitempty"`
		PublicIP     string `xml:"publicIp,omitempty"`
		PrivateIP    string `xml:"privateIp"`
	}
	type natItem struct {
		NatGatewayID     string        `xml:"natGatewayId"`
		SubnetID         string        `xml:"subnetId"`
		VpcID            string        `xml:"vpcId"`
		State            string        `xml:"state"`
		ConnectivityType string        `xml:"connectivityType"`
		CreateTime       string        `xml:"createTime"`
		Addresses        []natAddrItem `xml:"natGatewayAddressSet>item"`
	}
	type response struct {
		XMLName     xml.Name  `xml:"DescribeNatGatewaysResponse"`
		XMLNS       string    `xml:"xmlns,attr"`
		NatGateways []natItem `xml:"natGatewaySet>item"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var gw EC2NATGateway
		if json.Unmarshal(data, &gw) != nil {
			continue
		}
		if !filterIDs.match(gw.NatGatewayID) {
			continue
		}
		// Apply filters.
		if stateVals, ok := filters["state"]; ok && !ec2FilterAccepts(stateVals, gw.State) {
			continue
		}
		if vpcVals, ok := filters["vpc-id"]; ok && !ec2FilterAccepts(vpcVals, gw.VPCID) {
			continue
		}
		resp.NatGateways = append(resp.NatGateways, natItem{
			NatGatewayID:     gw.NatGatewayID,
			SubnetID:         gw.SubnetID,
			VpcID:            gw.VPCID,
			State:            gw.State,
			ConnectivityType: gw.ConnectivityType,
			CreateTime:       gw.CreateTime,
			Addresses: []natAddrItem{{
				AllocationID: gw.AllocationID,
				PublicIP:     gw.PublicIP,
				PrivateIP:    gw.PrivateIP,
			}},
		})
	}
	if err := filterIDs.unresolved(); err != nil {
		return nil, err
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteNatGateway(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	natID := req.Params["NatGatewayId"]
	key := "nat:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + natID
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 deleteNatGateway get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2NatGatewayIDKind, "NatGatewayId", natID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var gw EC2NATGateway
	if unmarshalErr := json.Unmarshal(data, &gw); unmarshalErr != nil {
		return nil, fmt.Errorf("ec2 deleteNatGateway unmarshal: %w", unmarshalErr)
	}
	gw.State = "deleted"
	newData, _ := json.Marshal(gw)
	if err := p.state.Put(context.Background(), ec2Namespace, key, newData); err != nil {
		return nil, fmt.Errorf("ec2 deleteNatGateway put: %w", err)
	}
	type response struct {
		XMLName      xml.Name `xml:"DeleteNatGatewayResponse"`
		XMLNS        string   `xml:"xmlns,attr"`
		NatGatewayID string   `xml:"natGatewayId"`
		State        string   `xml:"state"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:        "http://ec2.amazonaws.com/doc/2016-11-15/",
		NatGatewayID: natID,
		State:        "deleted",
	})
}

// --- DescribeRegions ---------------------------------------------------------

// ec2SeededRegions is the list of regions the emulator reports as enabled.
var ec2SeededRegions = []struct {
	Name     string
	Endpoint string
}{
	{"us-east-1", "ec2.us-east-1.amazonaws.com"},
	{"us-west-2", "ec2.us-west-2.amazonaws.com"},
	{"eu-west-1", "ec2.eu-west-1.amazonaws.com"},
}

func (p *EC2Plugin) describeRegions(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Build optional RegionName.N filter.
	wanted := map[string]bool{}
	for i := 1; ; i++ {
		v := req.Params[fmt.Sprintf("RegionName.%d", i)]
		if v == "" {
			break
		}
		wanted[v] = true
	}

	type regionItem struct {
		RegionName     string `xml:"regionName"`
		RegionEndpoint string `xml:"regionEndpoint"`
		OptInStatus    string `xml:"optInStatus"`
	}
	type response struct {
		XMLName    xml.Name     `xml:"DescribeRegionsResponse"`
		XMLNS      string       `xml:"xmlns,attr"`
		RegionInfo []regionItem `xml:"regionInfo>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, r := range ec2SeededRegions {
		if len(wanted) > 0 && !wanted[r.Name] {
			continue
		}
		resp.RegionInfo = append(resp.RegionInfo, regionItem{
			RegionName:     r.Name,
			RegionEndpoint: r.Endpoint,
			OptInStatus:    "opt-in-not-required",
		})
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- DescribeInstanceTypes ---------------------------------------------------

// describeInstanceTypes answers from the seeded [ec2InstanceTypeCatalog].
//
// Filter.N is not applied. The operation documents some 60 filter names, nearly all over
// response fields the seeded catalog does not carry, so applying the handful that are
// modellable and ignoring the rest would be the same silent-narrowing defect #485
// reported on the offerings operation (TODO(#495): apply the filters the catalog can
// answer and refuse the rest). InstanceType.N is honored, and unlike a filter it is an
// assertion that the types exist.
func (p *EC2Plugin) describeInstanceTypes(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	requested := indexedParams(req.Params, "InstanceType.%d")
	if err := ec2CheckInstanceTypesExist(requested); err != nil {
		return nil, err
	}
	wanted := map[string]bool{}
	for _, t := range requested {
		wanted[t] = true
	}

	type gpuInfoItem struct {
		Count int `xml:"gpus>item>count"`
	}
	type processorInfo struct {
		SupportedArchitectures []string `xml:"supportedArchitectures>item"`
	}
	type memoryInfo struct {
		SizeInMiB int `xml:"sizeInMiB"`
	}
	type vcpuInfo struct {
		DefaultVCpus int `xml:"defaultVCpus"`
	}
	type usageClassItem struct {
		Value string `xml:",chardata"`
	}
	type instanceTypeItem struct {
		InstanceType          string           `xml:"instanceType"`
		CurrentGeneration     bool             `xml:"currentGeneration"`
		VCpuInfo              vcpuInfo         `xml:"vCpuInfo"`
		MemoryInfo            memoryInfo       `xml:"memoryInfo"`
		ProcessorInfo         processorInfo    `xml:"processorInfo"`
		SupportedUsageClasses []usageClassItem `xml:"supportedUsageClasses>item"`
		GpuInfo               *gpuInfoItem     `xml:"gpuInfo,omitempty"`
	}
	type response struct {
		XMLName       xml.Name           `xml:"DescribeInstanceTypesResponse"`
		XMLNS         string             `xml:"xmlns,attr"`
		InstanceTypes []instanceTypeItem `xml:"instanceTypeSet>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, info := range ec2InstanceTypeCatalog {
		if len(wanted) > 0 && !wanted[info.InstanceType] {
			continue
		}
		item := instanceTypeItem{
			InstanceType:      info.InstanceType,
			CurrentGeneration: true,
			VCpuInfo:          vcpuInfo{DefaultVCpus: info.VCpus},
			MemoryInfo:        memoryInfo{SizeInMiB: info.MemoryMiB},
			ProcessorInfo:     processorInfo{SupportedArchitectures: info.SupportedArchs},
		}
		for _, uc := range info.SupportedUsageClasses {
			item.SupportedUsageClasses = append(item.SupportedUsageClasses, usageClassItem{Value: uc})
		}
		if info.GPU > 0 {
			item.GpuInfo = &gpuInfoItem{Count: info.GPU}
		}
		resp.InstanceTypes = append(resp.InstanceTypes, item)
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- DescribeInstanceTypeOfferings ------------------------------------------

// describeInstanceTypeOfferings lists the seeded catalog's types per location.
//
// The operation's filters are exactly two — its reference lists `instance-type` and
// `location` and no others — so both are applied and any other name is refused. Before
// #485 this read `wantedTypes` from an `InstanceType.N` parameter the operation does not
// have (the reference's parameters are `DryRun`, `Filter.N`, `LocationType`, `MaxResults`
// and `NextToken`; botocore rejects `InstanceTypes` outright), so the type filter was
// unreachable dead code and every query returned the whole catalog. The pattern had been
// copied from describeInstanceTypes, where the parameter does exist.
//
// An `instance-type` value matching nothing yields **zero offerings and HTTP 200**, not
// InvalidInstanceType. That is deliberately the opposite of DescribeInstanceTypes'
// InstanceType.N; see [ec2CheckInstanceTypesExist] for why, and #485 for the real-AWS
// diff of both.
func (p *EC2Plugin) describeInstanceTypeOfferings(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// The bespoke check this replaced iterated the filter *map*, so with two undocumented
	// names it reported whichever one Go's map order surfaced first (#687). The shared
	// spec walks Filter.N in request order instead, which is deterministic.
	if err := ec2InstanceTypeOfferingFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)

	// LocationType is a top-level parameter, not a filter name, and it selects what the
	// locations in the response *are*. Only availability-zone (the default per the
	// reference — "If no location is specified, the default is to list the instance types
	// that are offered in the current Region") and region are modeled;
	// availability-zone-id and outpost are documented as unmodelled in docs/services.md,
	// and are refused rather than silently treated as availability-zone.
	locationType := req.Params["LocationType"]
	if locationType == "" {
		locationType = "availability-zone"
	}
	var locations []string
	switch locationType {
	case "availability-zone":
		for _, suffix := range ec2SeededAZSuffixes {
			locations = append(locations, reqCtx.Region+suffix)
		}
	case "region":
		locations = []string{reqCtx.Region}
	case "availability-zone-id", "outpost":
		return nil, ec2UnmodelledLocationTypeError(locationType)
	default:
		return nil, ec2InvalidLocationTypeError(locationType)
	}

	type offeringItem struct {
		InstanceType string `xml:"instanceType"`
		LocationType string `xml:"locationType"`
		Location     string `xml:"location"`
	}
	type response struct {
		XMLName               xml.Name       `xml:"DescribeInstanceTypeOfferingsResponse"`
		XMLNS                 string         `xml:"xmlns,attr"`
		InstanceTypeOfferings []offeringItem `xml:"instanceTypeOfferingSet>item"`
	}

	// Both filters are looked up with the two-value form because an absent filter and a
	// filter naming no values are different requests and now get different answers: absent
	// constrains nothing, while an empty value list matches nothing (#696). Indexing the map
	// directly would conflate them and drop the whole catalog for an unfiltered query.
	wantTypes, filterByType := filters["instance-type"]
	wantLocations, filterByLocation := filters["location"]

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, info := range ec2InstanceTypeCatalog {
		if filterByType && !ec2FilterAccepts(wantTypes, info.InstanceType) {
			continue
		}
		for _, loc := range locations {
			if filterByLocation && !ec2FilterAccepts(wantLocations, loc) {
				continue
			}
			resp.InstanceTypeOfferings = append(resp.InstanceTypeOfferings, offeringItem{
				InstanceType: info.InstanceType,
				LocationType: locationType,
				Location:     loc,
			})
		}
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- DescribeSpotPriceHistory ------------------------------------------------

// describeSpotPriceHistory reports one stub price per catalog type per Availability Zone.
//
// InstanceType.N here is a filter, not an assertion: the reference describes it as
// "Filters the results by the specified instance types", so an unknown type yields an
// empty history rather than InvalidInstanceType. That is the opposite of
// DescribeInstanceTypes, whose InstanceType.N asserts existence; see
// [ec2CheckInstanceTypesExist].
func (p *EC2Plugin) describeSpotPriceHistory(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	wantedTypes := map[string]bool{}
	for _, t := range indexedParams(req.Params, "InstanceType.%d") {
		wantedTypes[t] = true
	}
	// AZ filter.
	azFilter := req.Params["AvailabilityZone"]
	// ProductDescription filter (e.g. "Linux/UNIX").
	pdFilter := req.Params["ProductDescription.1"]

	region := reqCtx.Region
	// Stub timestamp: use time controller's current time.
	ts := p.tc.Now().UTC().Format(time.RFC3339)

	type spotPriceItem struct {
		InstanceType       string `xml:"instanceType"`
		ProductDescription string `xml:"productDescription"`
		SpotPrice          string `xml:"spotPrice"`
		Timestamp          string `xml:"timestamp"`
		AvailabilityZone   string `xml:"availabilityZone"`
	}
	type response struct {
		XMLName          xml.Name        `xml:"DescribeSpotPriceHistoryResponse"`
		XMLNS            string          `xml:"xmlns,attr"`
		SpotPriceHistory []spotPriceItem `xml:"spotPriceHistorySet>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, info := range ec2InstanceTypeCatalog {
		if len(wantedTypes) > 0 && !wantedTypes[info.InstanceType] {
			continue
		}
		desc := "Linux/UNIX"
		if pdFilter != "" && pdFilter != desc {
			continue
		}
		for _, suffix := range ec2SeededAZSuffixes {
			az := region + suffix
			if azFilter != "" && azFilter != az {
				continue
			}
			resp.SpotPriceHistory = append(resp.SpotPriceHistory, spotPriceItem{
				InstanceType:       info.InstanceType,
				ProductDescription: desc,
				SpotPrice:          info.SpotPrice,
				Timestamp:          ts,
				AvailabilityZone:   az,
			})
		}
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- Launch template operations ---

// resolveLaunchTemplate looks up a launch template by ID or name and returns it,
// or nil if not found.
func (p *EC2Plugin) resolveLaunchTemplate(goCtx context.Context, ctx *RequestContext, ltID, ltName string) *EC2LaunchTemplate {
	return ec2LookupLaunchTemplate(goCtx, p.state, ctx, ltID, ltName)
}

// ec2LookupLaunchTemplate reads a launch template by ID or name, or returns nil
// when neither is given or the record cannot be read.
//
// A free function over the StateManager rather than a method, because the
// authorization decision needs it too: CheckAccess runs before the handler, so
// [ec2AuthzRunInstancesResources] has to resolve the template itself to know which
// subnet and security groups a launch reaches through one (#662). Two readers of
// the same two keys could drift about which template a request names, and then a
// policy would be evaluated against resources the launch does not use.
func ec2LookupLaunchTemplate(goCtx context.Context, state StateManager, ctx *RequestContext, ltID, ltName string) *EC2LaunchTemplate {
	if ltID == "" && ltName == "" {
		return nil
	}
	if ltID == "" {
		// Look up ID by name.
		nameKey := "lt_by_name:" + ctx.AccountID + "/" + ctx.Region + "/" + ltName
		data, err := state.Get(goCtx, ec2Namespace, nameKey)
		if err != nil || data == nil {
			return nil
		}
		ltID = string(data)
	}
	key := "lt:" + ctx.AccountID + "/" + ctx.Region + "/" + ltID
	data, err := state.Get(goCtx, ec2Namespace, key)
	if err != nil || data == nil {
		return nil
	}
	var lt EC2LaunchTemplate
	if json.Unmarshal(data, &lt) != nil {
		return nil
	}
	return &lt
}

// parseLaunchTemplateData parses the LaunchTemplateData.* params of a
// CreateLaunchTemplate request.
//
// Networking comes from the first network interface, because AWS's
// RequestLaunchTemplateData has no top-level SubnetId member — a network interface
// is the only place a template can name a subnet, and the only place
// AssociatePublicIpAddress exists at all. Substrate previously accepted those
// params and stored none of them, so an instance launched from a template
// configured the way AWS requires landed in a substrate-chosen subnet (#444).
//
// Every declared interface is parsed (#455). The flat SubnetID,
// AssociatePublicIPAddress and NetworkInterfaceGroups fields hold the primary
// interface's values, so a launch from this template lands where it did before and a
// template stored before the slice existed still reads correctly; the slice carries
// the rest, which used to be silently dropped.
//
// The interface's security groups are read from SecurityGroupId.N first, which is
// what real SDKs send — the AWS model gives that member the locationName
// "SecurityGroupId" — with Groups.N accepted as a secondary spelling for
// hand-built requests, mirroring runInstances.
//
// TagSpecification.N and IamInstanceProfile were likewise accepted and stored
// nowhere, so a template that tagged its instances produced untagged ones and a
// template naming a role produced an instance with none — with nothing failing to
// say so, and a tag: filter simply returning nothing (#471).
func parseLaunchTemplateData(params map[string]string) EC2LaunchTemplateData {
	const ltPrefix = "LaunchTemplateData."

	interfaces := ec2ParseNetworkInterfaces(params, ltPrefix)
	ec2SortInterfacesByDeviceIndex(interfaces)

	data := EC2LaunchTemplateData{
		ImageID:      params[ltPrefix+"ImageId"],
		InstanceType: params[ltPrefix+"InstanceType"],
		KeyName:      params[ltPrefix+"KeyName"],
		UserData:     params[ltPrefix+"UserData"],
		// One parse per scope, into a field per scope. The instance scope keeps the
		// original field because widening it would break replay; see
		// [EC2LaunchTemplateData.TagSpecifications] (#670).
		TagSpecifications:       ec2TagSpecificationTags(params, ltPrefix, "instance"),
		VolumeTagSpecifications: ec2TagSpecificationTags(params, ltPrefix, "volume"),
		NetworkInterfaces:       interfaces,
		BlockDeviceMappings:     ec2ParseBlockDeviceMappings(params, ltPrefix),
	}
	// The flat fields hold the primary interface's values; see
	// [EC2LaunchTemplateData.NetworkInterfaces].
	if primary := ec2PrimaryInterface(interfaces); primary != nil {
		data.SubnetID = primary.SubnetID
		data.AssociatePublicIPAddress = primary.AssociatePublicIPAddress
		data.NetworkInterfaceGroups = primary.SecurityGroupIDs
	}
	// Name before Arn, mirroring runInstances' own precedence for the call-level
	// pair; see [EC2LaunchTemplateData.IamInstanceProfile].
	data.IamInstanceProfile = params[ltPrefix+"IamInstanceProfile.Name"]
	if data.IamInstanceProfile == "" {
		data.IamInstanceProfile = params[ltPrefix+"IamInstanceProfile.Arn"]
	}
	if sg1 := params[ltPrefix+"SecurityGroupId.1"]; sg1 != "" {
		data.SecurityGroupIDs = []string{sg1}
	}
	return data
}

func (p *EC2Plugin) createLaunchTemplate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["LaunchTemplateName"]
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "LaunchTemplateName is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()

	// Check for name collision.
	nameKey := "lt_by_name:" + ctx.AccountID + "/" + ctx.Region + "/" + name
	existing, _ := p.state.Get(goCtx, ec2Namespace, nameKey)
	if existing != nil {
		return nil, &AWSError{
			Code:       "InvalidLaunchTemplateName.AlreadyExistsException",
			Message:    "Launch template with name '" + name + "' already exists",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	ltID := generateLaunchTemplateID()
	now := p.tc.Now().UTC().Format(time.RFC3339)

	ltData := parseLaunchTemplateData(req.Params)
	if awsErr := ec2CheckTemplateTags(ltData); awsErr != nil {
		return nil, awsErr
	}

	lt := EC2LaunchTemplate{
		LaunchTemplateID:   ltID,
		LaunchTemplateName: name,
		DefaultVersionNum:  1,
		LatestVersionNum:   1,
		CreatedBy:          ctx.AccountID,
		CreateTime:         now,
		LatestData:         ltData,
		// Creating a template creates its version 1, which is both the default and
		// the latest (#456).
		Versions: []EC2LaunchTemplateVersion{{
			VersionNumber:      1,
			VersionDescription: req.Params["VersionDescription"],
			CreateTime:         now,
			CreatedBy:          ctx.AccountID,
			Data:               ltData,
		}},
		AccountID: ctx.AccountID,
		Region:    ctx.Region,
	}

	ltJSON, err := json.Marshal(lt)
	if err != nil {
		return nil, fmt.Errorf("createLaunchTemplate: marshal: %w", err)
	}
	ltKey := "lt:" + ctx.AccountID + "/" + ctx.Region + "/" + ltID
	if err := p.state.Put(goCtx, ec2Namespace, ltKey, ltJSON); err != nil {
		return nil, fmt.Errorf("createLaunchTemplate: put lt: %w", err)
	}
	if err := p.state.Put(goCtx, ec2Namespace, nameKey, []byte(ltID)); err != nil {
		return nil, fmt.Errorf("createLaunchTemplate: put lt_by_name: %w", err)
	}
	idsKey := "lt_ids:" + ctx.AccountID + "/" + ctx.Region
	updateStringIndex(goCtx, p.state, ec2Namespace, idsKey, ltID)

	// The warning goes on this outer struct rather than on ec2LaunchTemplateXML, which
	// ModifyLaunchTemplate and DescribeLaunchTemplates share: neither documents the
	// member, and AWS's own order is launchTemplate then warning.
	type response struct {
		XMLName        xml.Name                 `xml:"CreateLaunchTemplateResponse"`
		XMLNS          string                   `xml:"xmlns,attr"`
		LaunchTemplate ec2LaunchTemplateXML     `xml:"launchTemplate"`
		Warning        *ec2ValidationWarningXML `xml:"warning,omitempty"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:          "http://ec2.amazonaws.com/doc/2016-11-15/",
		LaunchTemplate: ec2LaunchTemplateSummary(&lt),
		// Reported rather than refused: the template is created either way, because
		// AWS's Errors section lists nothing for an invalid one and its `warning`
		// member exists for exactly this. See [ec2CollectBlockDeviceMappings].
		Warning: ec2ValidationWarningFor(
			ec2CollectBlockDeviceMappings(ltData.BlockDeviceMappings, p.ec2SnapshotResolver(ctx))),
	})
}

// ec2LaunchTemplateXML is the launchTemplate summary element AWS returns from
// CreateLaunchTemplate, DescribeLaunchTemplates and ModifyLaunchTemplate.
//
// Notably it carries no launchTemplateData: DescribeLaunchTemplates cannot read a
// template's parameters back, which is why DescribeLaunchTemplateVersions exists and
// why anything asserting on a template's contents has to call it.
type ec2LaunchTemplateXML struct {
	LaunchTemplateID   string       `xml:"launchTemplateId"`
	LaunchTemplateName string       `xml:"launchTemplateName"`
	CreateTime         string       `xml:"createTime"`
	CreatedBy          string       `xml:"createdBy,omitempty"`
	DefaultVersionNum  int64        `xml:"defaultVersionNumber"`
	LatestVersionNum   int64        `xml:"latestVersionNumber"`
	Tags               []ec2TagItem `xml:"tagSet>item,omitempty"`
}

// ec2TagItem is a tagSet entry on the wire.
type ec2TagItem struct {
	// Key is the tag key.
	Key string `xml:"key"`

	// Value is the tag value.
	Value string `xml:"value"`
}

// ec2TagItems renders a stored tag list as tagSet entries, in slice order.
//
// [EC2Tag] and [ec2TagItem] have identical field sets, so the conversion is a cast per
// element rather than a copy — the same cast the five hand-rolled loops that predate
// this helper each perform. It returns nil for an empty list, which a caller renders as
// a present-but-empty element or omits by its own struct tag; the choice belongs to the
// response shape, not here.
func ec2TagItems(tags []EC2Tag) []ec2TagItem {
	if len(tags) == 0 {
		return nil
	}
	items := make([]ec2TagItem, 0, len(tags))
	for _, t := range tags {
		items = append(items, ec2TagItem(t))
	}
	return items
}

// ec2LaunchTemplateSummary renders a launch template as its summary element.
func ec2LaunchTemplateSummary(lt *EC2LaunchTemplate) ec2LaunchTemplateXML {
	out := ec2LaunchTemplateXML{
		LaunchTemplateID:   lt.LaunchTemplateID,
		LaunchTemplateName: lt.LaunchTemplateName,
		CreateTime:         lt.CreateTime,
		CreatedBy:          lt.CreatedBy,
		DefaultVersionNum:  lt.DefaultVersionNum,
		LatestVersionNum:   lt.LatestVersionNum,
	}
	for _, t := range lt.Tags {
		out.Tags = append(out.Tags, ec2TagItem(t))
	}
	return out
}

func (p *EC2Plugin) describeLaunchTemplates(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	var lts []EC2LaunchTemplate

	filterID := req.Params["LaunchTemplateId.1"]
	filterName := req.Params["LaunchTemplateName.1"]

	switch {
	case filterID != "":
		if lt := p.resolveLaunchTemplate(goCtx, ctx, filterID, ""); lt != nil {
			lts = append(lts, *lt)
		}
	case filterName != "":
		if lt := p.resolveLaunchTemplate(goCtx, ctx, "", filterName); lt != nil {
			lts = append(lts, *lt)
		}
	default:
		idsKey := "lt_ids:" + ctx.AccountID + "/" + ctx.Region
		ids, _ := loadStringIndex(goCtx, p.state, ec2Namespace, idsKey)
		for _, id := range ids {
			key := "lt:" + ctx.AccountID + "/" + ctx.Region + "/" + id
			data, err := p.state.Get(goCtx, ec2Namespace, key)
			if err != nil || data == nil {
				continue
			}
			var lt EC2LaunchTemplate
			if json.Unmarshal(data, &lt) == nil {
				lts = append(lts, lt)
			}
		}
	}

	type response struct {
		XMLName         xml.Name               `xml:"DescribeLaunchTemplatesResponse"`
		XMLNS           string                 `xml:"xmlns,attr"`
		LaunchTemplates []ec2LaunchTemplateXML `xml:"launchTemplates>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for i := range lts {
		resp.LaunchTemplates = append(resp.LaunchTemplates, ec2LaunchTemplateSummary(&lts[i]))
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteLaunchTemplate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	ltID := req.Params["LaunchTemplateId"]
	ltName := req.Params["LaunchTemplateName"]

	lt := p.resolveLaunchTemplate(goCtx, ctx, ltID, ltName)
	if lt == nil {
		return nil, &AWSError{
			Code:       "InvalidLaunchTemplateId.NotFound",
			Message:    "The launch template was not found",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	ltKey := "lt:" + ctx.AccountID + "/" + ctx.Region + "/" + lt.LaunchTemplateID
	nameKey := "lt_by_name:" + ctx.AccountID + "/" + ctx.Region + "/" + lt.LaunchTemplateName
	idsKey := "lt_ids:" + ctx.AccountID + "/" + ctx.Region

	if err := p.state.Delete(goCtx, ec2Namespace, ltKey); err != nil {
		return nil, fmt.Errorf("deleteLaunchTemplate: delete lt: %w", err)
	}
	if err := p.state.Delete(goCtx, ec2Namespace, nameKey); err != nil {
		return nil, fmt.Errorf("deleteLaunchTemplate: delete lt_by_name: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, ec2Namespace, idsKey, lt.LaunchTemplateID)

	type ltItem struct {
		LaunchTemplateID   string `xml:"launchTemplateId"`
		LaunchTemplateName string `xml:"launchTemplateName"`
	}
	type response struct {
		XMLName        xml.Name `xml:"DeleteLaunchTemplateResponse"`
		XMLNS          string   `xml:"xmlns,attr"`
		LaunchTemplate ltItem   `xml:"launchTemplate"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/",
		LaunchTemplate: ltItem{
			LaunchTemplateID:   lt.LaunchTemplateID,
			LaunchTemplateName: lt.LaunchTemplateName,
		},
	})
}

// --- EBS volume operations ---

// createVolume creates an EBS volume, tagging it from TagSpecification.N.
//
// The tags come from [ec2LaunchTagsForResource] scoped to "volume", which is the same
// walk RunInstances, CreateFleet, CreateImage and CreateNatGateway already use: AWS's
// wire shape here is byte-identical to theirs, so a second parser would only be a
// second thing to drift (#670). They are checked before the volume is written, per the
// rollback rule in [ec2CheckReservedTagKeys] — a refusal that had already stored the
// volume would leave an untagged one behind for the next request to see.
//
// Iops and Throughput are read here for the same reason the tags are: [EC2Volume]
// carried both and DescribeVolumes rendered both, but this parser ignored them, so
// CreateVolume(Iops=…) and RunInstances(…Ebs.Iops=…) disagreed about the same field. No
// per-type validation is applied — see [ec2CheckBlockDeviceMappings] for why substrate
// does not encode the documented numeric ranges.
func (p *EC2Plugin) createVolume(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	tags := ec2LaunchTagsForResource(req.Params, "volume")
	if awsErr := ec2CheckTagRules(tags); awsErr != nil {
		return nil, awsErr
	}
	// A new volume starts with no tags, so the request's own tags are the whole count.
	if awsErr := ec2CheckTagLimit(nil, tags); awsErr != nil {
		return nil, awsErr
	}

	az := req.Params["AvailabilityZone"]
	if az == "" {
		az = reqCtx.Region + "a"
	}
	sizeStr := req.Params["Size"]
	size := ec2DefaultVolumeSizeGiB
	if sizeStr != "" {
		if n, err := strconv.Atoi(sizeStr); err == nil && n > 0 {
			size = n
		}
	}
	volType := req.Params["VolumeType"]
	if volType == "" {
		volType = "gp2"
	}

	// A named snapshot is checked and its size honored, the same two rules the launch
	// path applies through [ec2CheckMappingSnapshot] — CreateVolume had an independent
	// copy of the gap, so a restore from a 30 GiB snapshot produced an 8 GiB volume here
	// too, and one naming a snapshot no account holds succeeded outright (#689). Both
	// AWS sentences are on CreateVolume's own Size member: "If you specify a snapshot,
	// the default is the snapshot size" and "You can specify a volume size that is equal
	// to or larger than the snapshot size."
	//
	// A request naming neither Size nor SnapshotId still gets the 8 GiB default rather
	// than the refusal AWS documents ("Size is required unless SnapshotId is specified").
	// That is a wider change than this one and a follow-up.
	snapshotID := req.Params["SnapshotId"]
	if snapshotID != "" {
		snap, found := p.ec2SnapshotResolver(reqCtx)(snapshotID)
		if awsErr := ec2RequireResource(ec2SnapshotIDKind, snapshotID, found); awsErr != nil {
			return nil, awsErr
		}
		switch {
		case sizeStr == "":
			size = int(snap.VolumeSize)
		case int64(size) < snap.VolumeSize:
			return nil, &AWSError{
				Code: "InvalidParameterValue",
				Message: fmt.Sprintf(
					"Size %d is smaller than the size of snapshot %s (%d GiB)",
					size, snapshotID, snap.VolumeSize),
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
	encrypted := strings.ToLower(req.Params["Encrypted"]) == "true"
	// Recorded as given, with the same tolerance Size above has: a value that does not
	// parse leaves the field at zero and the field is then omitted from the response,
	// which is what a volume with no provisioned performance looks like.
	iops, _ := strconv.Atoi(req.Params["Iops"])
	throughput, _ := strconv.Atoi(req.Params["Throughput"])

	vol := EC2Volume{
		VolumeID:         generateVolumeID(),
		Size:             size,
		VolumeType:       volType,
		AvailabilityZone: az,
		State:            "available",
		SnapshotID:       snapshotID,
		Encrypted:        encrypted,
		IOPS:             iops,
		Throughput:       throughput,
		Tags:             tags,
		CreateTime:       p.tc.Now().UTC().Format(time.RFC3339),
		AccountID:        reqCtx.AccountID,
		Region:           reqCtx.Region,
	}

	data, err := json.Marshal(vol)
	if err != nil {
		return nil, fmt.Errorf("ec2 createVolume marshal: %w", err)
	}
	key := ec2VolumeStateKey(reqCtx.AccountID, reqCtx.Region, vol.VolumeID)
	if err := p.state.Put(context.Background(), ec2Namespace, key, data); err != nil {
		return nil, fmt.Errorf("ec2 createVolume state.Put: %w", err)
	}
	if err := p.appendToList(reqCtx.AccountID+"/"+reqCtx.Region, "volume_ids", vol.VolumeID); err != nil {
		return nil, err
	}

	type attachmentItem struct {
		VolumeID   string `xml:"volumeId"`
		InstanceID string `xml:"instanceId"`
		Device     string `xml:"device"`
		Status     string `xml:"status"`
	}
	type response struct {
		XMLName          xml.Name `xml:"CreateVolumeResponse"`
		XMLNS            string   `xml:"xmlns,attr"`
		VolumeID         string   `xml:"volumeId"`
		Size             int      `xml:"size"`
		VolumeType       string   `xml:"volumeType"`
		AvailabilityZone string   `xml:"availabilityZone"`
		Status           string   `xml:"status"`
		Encrypted        bool     `xml:"encrypted"`
		CreateTime       string   `xml:"createTime"`
		SnapshotID       string   `xml:"snapshotId"`
		IOPS             int      `xml:"iops,omitempty"`
		Throughput       int      `xml:"throughput,omitempty"`
		// tagSet carries no omitempty, deliberately: AWS's Example 2 emits <tagSet/>
		// for a volume created with no tags, and an SDK maps a present-but-empty
		// element to an empty slice where it maps an omitted one to nil. Omitting it
		// would report "unknown" where AWS reports "none".
		Tags          []ec2TagItem     `xml:"tagSet>item"`
		AttachmentSet []attachmentItem `xml:"attachmentSet>item"`
	}
	resp := response{
		XMLNS:            "http://ec2.amazonaws.com/doc/2016-11-15/",
		VolumeID:         vol.VolumeID,
		Size:             vol.Size,
		VolumeType:       vol.VolumeType,
		AvailabilityZone: vol.AvailabilityZone,
		Status:           vol.State,
		Encrypted:        vol.Encrypted,
		CreateTime:       vol.CreateTime,
		SnapshotID:       vol.SnapshotID,
		IOPS:             vol.IOPS,
		Throughput:       vol.Throughput,
		Tags:             ec2TagItems(vol.Tags),
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) describeVolumes(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if err := ec2VolumeFilterSpec().check(req.Params); err != nil {
		return nil, err
	}

	// Collect requested volume IDs from repeated VolumeId.N params.
	requestedIDs := map[string]bool{}
	for i := 1; ; i++ {
		id := req.Params[fmt.Sprintf("VolumeId.%d", i)]
		if id == "" {
			break
		}
		requestedIDs[id] = true
	}

	// Filters come from the shared [extractEC2Filters] walk rather than a hand-rolled
	// one. The hand-rolled loop this replaces switched on the filter name *inside* the
	// value loop, into nine map[string]bool sets — a shape that cannot express
	// tag:<key>, since the key is part of the name (#670).
	//
	// Two behavior changes come with the shared walk, both fidelity improvements worth
	// naming rather than leaving to be discovered. It records an empty filter value as a
	// value, where the old loop's `if val == "" { break }` truncated the list at it. And
	// it keeps a filter whose value list is empty, so `tag:Env` with no value is a filter
	// that matches nothing rather than one that was never seen.
	filters := extractEC2Filters(req.Params)

	allKeys, err := p.state.List(context.Background(), ec2Namespace, ec2VolumeStatePrefix(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("ec2 describeVolumes list: %w", err)
	}

	type attachItem struct {
		VolumeID   string `xml:"volumeId"`
		InstanceID string `xml:"instanceId"`
		Device     string `xml:"device"`
		Status     string `xml:"status"`
		AttachTime string `xml:"attachTime"`
		// deleteOnTermination lives here rather than on the volume item because that
		// is where AWS's own DescribeVolumes response carries it.
		DeleteOnTermination bool `xml:"deleteOnTermination"`
	}
	type volItem struct {
		VolumeID         string `xml:"volumeId"`
		Size             int    `xml:"size"`
		VolumeType       string `xml:"volumeType"`
		AvailabilityZone string `xml:"availabilityZone"`
		Status           string `xml:"status"`
		Encrypted        bool   `xml:"encrypted"`
		CreateTime       string `xml:"createTime"`
		SnapshotID       string `xml:"snapshotId"`
		// iops and throughput were stored and never rendered, so a volume created
		// with provisioned performance read back as one without it.
		IOPS       int `xml:"iops,omitempty"`
		Throughput int `xml:"throughput,omitempty"`
		// tagSet carries no omitempty, for the reason CreateVolume's does not: AWS
		// renders the element for an untagged volume, and an SDK tells a
		// present-but-empty element from an omitted one.
		Tags          []ec2TagItem `xml:"tagSet>item"`
		AttachmentSet []attachItem `xml:"attachmentSet>item"`
	}
	var volumes []volItem

	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var vol EC2Volume
		if err := json.Unmarshal(data, &vol); err != nil {
			continue
		}
		// Filter by requested IDs.
		if len(requestedIDs) > 0 && !requestedIDs[vol.VolumeID] {
			continue
		}
		if !ec2VolumeMatchesFilters(vol, filters) {
			continue
		}
		item := volItem{
			VolumeID:         vol.VolumeID,
			Size:             vol.Size,
			VolumeType:       vol.VolumeType,
			AvailabilityZone: vol.AvailabilityZone,
			Status:           vol.State,
			Encrypted:        vol.Encrypted,
			CreateTime:       vol.CreateTime,
			SnapshotID:       vol.SnapshotID,
			IOPS:             vol.IOPS,
			Throughput:       vol.Throughput,
			Tags:             ec2TagItems(vol.Tags),
		}
		for _, att := range vol.Attachments {
			item.AttachmentSet = append(item.AttachmentSet, attachItem{
				VolumeID:            vol.VolumeID,
				InstanceID:          att.InstanceID,
				Device:              att.Device,
				Status:              att.State,
				AttachTime:          att.AttachTime,
				DeleteOnTermination: att.DeleteOnTermination,
			})
		}
		volumes = append(volumes, item)
	}

	type volumeSet struct {
		Items []volItem `xml:"item"`
	}
	type response struct {
		XMLName   xml.Name  `xml:"DescribeVolumesResponse"`
		XMLNS     string    `xml:"xmlns,attr"`
		VolumeSet volumeSet `xml:"volumeSet"`
	}
	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	resp.VolumeSet.Items = volumes
	if resp.VolumeSet.Items == nil {
		resp.VolumeSet.Items = []volItem{}
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

func (p *EC2Plugin) deleteVolume(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	volID := req.Params["VolumeId"]
	key := ec2VolumeStateKey(reqCtx.AccountID, reqCtx.Region, volID)
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 deleteVolume get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2VolumeIDKind, "VolumeId", volID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var vol EC2Volume
	if err := json.Unmarshal(data, &vol); err != nil {
		return nil, fmt.Errorf("ec2 deleteVolume unmarshal: %w", err)
	}
	if vol.State == "in-use" {
		return nil, &AWSError{Code: "VolumeInUse", Message: "Volume " + volID + " is currently attached to " + vol.Attachments[0].InstanceID, HTTPStatus: http.StatusBadRequest}
	}
	if err := p.state.Delete(context.Background(), ec2Namespace, key); err != nil {
		return nil, fmt.Errorf("ec2 deleteVolume delete: %w", err)
	}
	removeFromStringIndex(context.Background(), p.state, ec2Namespace, "volume_ids:"+reqCtx.AccountID+"/"+reqCtx.Region, volID)

	type response struct {
		XMLName xml.Name `xml:"DeleteVolumeResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

func (p *EC2Plugin) attachVolume(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	volID := req.Params["VolumeId"]
	instanceID := req.Params["InstanceId"]
	device := req.Params["Device"]
	// AttachVolume names two required IDs, so each is reported by name rather than as a
	// single refusal naming both: a caller who sent one of the two learns which. The
	// volume is checked first, being the resource the operation is named for — an absent
	// VolumeId falls to ec2RequireNamedResource below.
	if volID != "" && instanceID == "" {
		return nil, ec2MissingParameter("InstanceId")
	}
	if device == "" {
		device = "/dev/xvdf"
	}
	key := ec2VolumeStateKey(reqCtx.AccountID, reqCtx.Region, volID)
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 attachVolume get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2VolumeIDKind, "VolumeId", volID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var vol EC2Volume
	if err := json.Unmarshal(data, &vol); err != nil {
		return nil, fmt.Errorf("ec2 attachVolume unmarshal: %w", err)
	}
	if vol.State != "available" {
		return nil, &AWSError{Code: "IncorrectState", Message: "Volume " + volID + " is not in available state.", HTTPStatus: http.StatusBadRequest}
	}

	attachTime := p.tc.Now().UTC().Format(time.RFC3339)
	vol.State = "in-use"
	vol.Attachments = []EC2VolumeAttachment{{
		InstanceID: instanceID,
		Device:     device,
		State:      "attached",
		AttachTime: attachTime,
		// AWS preserves a volume attached after launch: deleting one the caller
		// brought would destroy something the launch did not make. Only a volume a
		// launch creates defaults the other way; see [ec2LaunchVolumesFor].
		DeleteOnTermination: false,
	}}

	newData, err := json.Marshal(vol)
	if err != nil {
		return nil, fmt.Errorf("ec2 attachVolume marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ec2Namespace, key, newData); err != nil {
		return nil, fmt.Errorf("ec2 attachVolume state.Put: %w", err)
	}

	type response struct {
		XMLName             xml.Name `xml:"AttachVolumeResponse"`
		XMLNS               string   `xml:"xmlns,attr"`
		VolumeID            string   `xml:"volumeId"`
		InstanceID          string   `xml:"instanceId"`
		Device              string   `xml:"device"`
		Status              string   `xml:"status"`
		AttachTime          string   `xml:"attachTime"`
		DeleteOnTermination bool     `xml:"deleteOnTermination"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:               "http://ec2.amazonaws.com/doc/2016-11-15/",
		VolumeID:            volID,
		InstanceID:          instanceID,
		Device:              device,
		Status:              "attached",
		AttachTime:          attachTime,
		DeleteOnTermination: false,
	})
}

func (p *EC2Plugin) detachVolume(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	volID := req.Params["VolumeId"]
	key := ec2VolumeStateKey(reqCtx.AccountID, reqCtx.Region, volID)
	data, err := p.state.Get(context.Background(), ec2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("ec2 detachVolume get: %w", err)
	}
	if reqErr := ec2RequireNamedResource(ec2VolumeIDKind, "VolumeId", volID, data != nil); reqErr != nil {
		return nil, reqErr
	}
	var vol EC2Volume
	if err := json.Unmarshal(data, &vol); err != nil {
		return nil, fmt.Errorf("ec2 detachVolume unmarshal: %w", err)
	}

	var prevInstanceID, prevDevice string
	if len(vol.Attachments) > 0 {
		prevInstanceID = vol.Attachments[0].InstanceID
		prevDevice = vol.Attachments[0].Device
	}
	vol.State = "available"
	vol.Attachments = nil

	newData, err := json.Marshal(vol)
	if err != nil {
		return nil, fmt.Errorf("ec2 detachVolume marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), ec2Namespace, key, newData); err != nil {
		return nil, fmt.Errorf("ec2 detachVolume state.Put: %w", err)
	}

	type response struct {
		XMLName    xml.Name `xml:"DetachVolumeResponse"`
		XMLNS      string   `xml:"xmlns,attr"`
		VolumeID   string   `xml:"volumeId"`
		InstanceID string   `xml:"instanceId"`
		Device     string   `xml:"device"`
		Status     string   `xml:"status"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:      "http://ec2.amazonaws.com/doc/2016-11-15/",
		VolumeID:   volID,
		InstanceID: prevInstanceID,
		Device:     prevDevice,
		Status:     "detached",
	})
}

// deleteSnapshot is a stub that accepts DeleteSnapshot requests without error.
// Substratefs does not persist snapshots; the operation succeeds to allow
// AMI deregistration cleanup workflows to proceed.
func (p *EC2Plugin) deleteSnapshot(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	snapshotID := req.Params["SnapshotId"]
	if snapshotID == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "SnapshotId is required", HTTPStatus: http.StatusBadRequest}
	}
	// Remove the snapshot from state so a subsequent DescribeSnapshots reflects
	// the deletion (enables shared-snapshot retain-vs-delete testing). Delete is
	// idempotent — a missing snapshot still returns success.
	if err := p.state.Delete(context.Background(), ec2Namespace, ec2SnapshotStateKey(reqCtx.AccountID, reqCtx.Region, snapshotID)); err != nil {
		return nil, fmt.Errorf("ec2 deleteSnapshot delete: %w", err)
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteSnapshotResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/", Return: true})
}

// describeSnapshots lists EBS snapshots owned by the account, honoring an optional list of
// SnapshotId.N parameters, Owner.N and RestorableBy.N, and the ten Filter.N names
// [ec2SnapshotMatchesFilter] evaluates.
//
// The three selectors compose as AWS documents: a snapshot must satisfy the ID list, both
// account scopes and every filter. ids.match runs first so that a snapshot excluded by a
// filter still counts as resolved for [ec2IDFilter.unresolved] — a named ID that exists is
// not "not found" merely because a filter rejected it.
func (p *EC2Plugin) describeSnapshots(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	ids := newEC2IDFilter(extractIndexedParams(req.Params, "SnapshotId"), ec2SnapshotIDKind)
	if err := ids.validate(); err != nil {
		return nil, err
	}
	if err := ec2SnapshotFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)
	owners := newEC2SnapshotOwners(req.Params, reqCtx.AccountID)
	restorableBy := newEC2SnapshotRestorableBy(req.Params, reqCtx.AccountID)

	allKeys, err := p.state.List(context.Background(), ec2Namespace,
		ec2SnapshotStatePrefix(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("ec2 describeSnapshots list: %w", err)
	}

	type snapshotItem struct {
		SnapshotID  string       `xml:"snapshotId"`
		VolumeID    string       `xml:"volumeId,omitempty"`
		VolumeSize  int64        `xml:"volumeSize"`
		State       string       `xml:"status"`
		StartTime   string       `xml:"startTime"`
		Encrypted   bool         `xml:"encrypted"`
		Description string       `xml:"description,omitempty"`
		OwnerID     string       `xml:"ownerId"`
		Tags        []ec2TagItem `xml:"tagSet>item"`
	}
	type response struct {
		XMLName   xml.Name       `xml:"DescribeSnapshotsResponse"`
		XMLNS     string         `xml:"xmlns,attr"`
		Snapshots []snapshotItem `xml:"snapshotSet>item"`
	}

	resp := response{XMLNS: "http://ec2.amazonaws.com/doc/2016-11-15/"}
	for _, k := range allKeys {
		data, getErr := p.state.Get(context.Background(), ec2Namespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var snap EC2Snapshot
		if json.Unmarshal(data, &snap) != nil {
			continue
		}
		if !ids.match(snap.SnapshotID) {
			continue
		}
		if !owners.match(snap) || !restorableBy.match(snap) {
			continue
		}
		if !ec2SnapshotMatchesFilters(snap, filters) {
			continue
		}
		resp.Snapshots = append(resp.Snapshots, snapshotItem{
			SnapshotID:  snap.SnapshotID,
			VolumeID:    snap.VolumeID,
			VolumeSize:  snap.VolumeSize,
			State:       snap.State,
			StartTime:   snap.StartTime,
			Encrypted:   snap.Encrypted,
			Description: snap.Description,
			OwnerID:     snap.AccountID,
			Tags:        ec2TagItems(snap.Tags),
		})
	}
	if err := ids.unresolved(); err != nil {
		return nil, err
	}
	return ec2XMLResponse(http.StatusOK, resp)
}
