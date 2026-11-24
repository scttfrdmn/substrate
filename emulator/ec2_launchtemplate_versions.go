package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Launch template versioning (#456).
//
// A launch template is versioned: CreateLaunchTemplate produces version 1, each
// CreateLaunchTemplateVersion appends one, and a launch names either a number or
// one of the two aliases. Every version is stored, so a consumer that ships a second
// version launches its parameters rather than the first one's.
//
// DescribeLaunchTemplateVersions is also the only operation that returns a
// template's data at all: DescribeLaunchTemplates' response carries just createTime,
// createdBy, defaultVersionNumber, latestVersionNumber, launchTemplateId and
// launchTemplateName. Anything asserting on what a template *contains* has to come
// through here.

// ec2LaunchTemplateVersionAliases are the two symbolic version specifiers AWS
// accepts in place of a number.
const (
	ec2LTVersionLatest  = "$latest"
	ec2LTVersionDefault = "$default"
)

// ec2ResolveTemplateVersion returns the version of lt that spec names.
//
// An empty spec resolves to the template's *default* version, not its latest. That
// is aws-sdk-go-v2's documented behavior for
// LaunchTemplateSpecification.Version ("Default: The default version of the launch
// template"), and it is the detail #456 asked to have confirmed: inverting it stays
// invisible until a consumer pins a default and keeps shipping newer versions, at
// which point every launch quietly uses the wrong parameters.
//
// The aliases are matched case-insensitively. AWS documents them capitalized
// ("$Latest", "$Default") and the CLI emits them that way, but a hand-written
// request using "$latest" is accepted rather than read as a malformed number.
func ec2ResolveTemplateVersion(lt *EC2LaunchTemplate, spec string) (*EC2LaunchTemplateVersion, *AWSError) {
	versions := lt.TemplateVersions()

	wanted := lt.DefaultVersionNum
	switch strings.ToLower(strings.TrimSpace(spec)) {
	case "", ec2LTVersionDefault:
		// wanted is already the default.
	case ec2LTVersionLatest:
		wanted = lt.LatestVersionNum
	default:
		n, err := strconv.ParseInt(strings.TrimSpace(spec), 10, 64)
		if err != nil {
			return nil, ec2LaunchTemplateVersionNotFound(lt.LaunchTemplateID, spec)
		}
		wanted = n
	}

	for i := range versions {
		if versions[i].VersionNumber == wanted {
			return &versions[i], nil
		}
	}
	return nil, ec2LaunchTemplateVersionNotFound(lt.LaunchTemplateID, spec)
}

// ec2LaunchTemplateVersionNotFound reports a version that does not exist.
//
// Code and message follow moto's InvalidLaunchTemplateVersionNotFound. AWS's
// published Errors section for these operations is empty, so this is a
// reimplementation's wording rather than a capture — the code is the load-bearing
// half, since SDKs dispatch on it and never on the message.
func ec2LaunchTemplateVersionNotFound(ltID, version string) *AWSError {
	return &AWSError{
		Code:       "InvalidLaunchTemplateId.VersionNotFound",
		Message:    fmt.Sprintf("Could not find the specified version %s for the launch template with ID %s.", version, ltID),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2LaunchTemplateNotFound reports a launch template that does not exist, by the identifier
// the request named it with: an ID gets InvalidLaunchTemplateId.NotFound and a name gets
// InvalidLaunchTemplateName.NotFoundException.
//
// Both codes are EC2's published ones — the error table describes the first as "The specified
// launch template ID does not exist" and the second as "The specified launch template name does
// not exist". A request that names its template by name, which is how most IaC names one, is
// refused with the name code rather than a code about an ID it never sent (#1469).
// When a request names both, the ID is what [ec2LookupLaunchTemplate] resolves by, so the ID
// is what is reported.
//
// The messages are observed, not published: "The specified launch template, with template name
// packer-fleet-launch-template, does not exist." is quoted verbatim from real EC2 in
// hashicorp/packer#7968 and aws/karpenter-provider-aws#2101, and the ID form is the same
// sentence, as Auto Scaling's own refusal here ([AutoScalingPlugin.asgResolveSpec]) already
// renders it.
func ec2LaunchTemplateNotFound(ltID, ltName string) *AWSError {
	if ltID != "" {
		return &AWSError{
			Code:       ec2LaunchTemplateIDKind.NotFound,
			Message:    fmt.Sprintf("The specified launch template, with template ID %s, does not exist.", ltID),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return &AWSError{
		Code:       "InvalidLaunchTemplateName.NotFoundException",
		Message:    fmt.Sprintf("The specified launch template, with template name %s, does not exist.", ltName),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2DescribeLaunchTemplatesNotFound is DescribeLaunchTemplates' refusal for a named template
// that does not exist: the same two codes as [ec2LaunchTemplateNotFound], by which list named
// it, with the list operation's own message.
//
// "At least one of the launch templates specified in the request does not exist." is real
// EC2's, observed: #1469 quotes it for an unknown name, and terraform-provider-aws#5967 records
// it under InvalidLaunchTemplateId.NotFound for an unknown ID (spelled "Atleast" in that 2018
// capture; the spelling here is #1469's). It names no template, so the code is what tells a
// caller which list held the miss.
func ec2DescribeLaunchTemplatesNotFound(byID bool) *AWSError {
	code := "InvalidLaunchTemplateName.NotFoundException"
	if byID {
		code = ec2LaunchTemplateIDKind.NotFound
	}
	return &AWSError{
		Code:       code,
		Message:    "At least one of the launch templates specified in the request does not exist.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2LaunchTemplateNameMalformedCode is the error table's code for a launch template name
// outside the published rule; see [ec2LaunchTemplateNameWellFormed].
const ec2LaunchTemplateNameMalformedCode = "InvalidLaunchTemplateName.MalformedException"

// ec2LaunchTemplateNameWellFormed reports whether a launch template name keeps the rule the
// API reference publishes on every LaunchTemplateName member — "Length Constraints: Minimum
// length of 3. Maximum length of 128. Pattern: [a-zA-Z0-9\(\)\.\-/_]+" — and the error table
// restates for InvalidLaunchTemplateName.MalformedException (#1507).
func ec2LaunchTemplateNameWellFormed(name string) bool {
	if len(name) < 3 || len(name) > 128 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '(' || c == ')' || c == '.' || c == '-' || c == '/' || c == '_':
		default:
			return false
		}
	}
	return true
}

// ec2LaunchTemplateIDMalformed is the refusal for a launch template ID of the wrong shape.
//
// The code is the error table's; the message is observed — "The specified launch template ID
// '{{ getLaunchTemplateData.LaunchtemplateID }}' is malformed." is quoted from a
// CreateLaunchTemplateVersion refusal on AWS re:Post (QUEsP8p2mvSv2TAnNzqbynCg), which names an
// unsubstituted template variable where an ID belonged.
func ec2LaunchTemplateIDMalformed(ltID string) *AWSError {
	return &AWSError{
		Code:       ec2LaunchTemplateIDKind.Malformed,
		Message:    fmt.Sprintf("The specified launch template ID '%s' is malformed.", ltID),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2LaunchTemplateNameMalformed is the refusal for a launch template name outside the
// published rule. The message is the error table's own description of the code, verbatim; no
// observed wording was found.
func ec2LaunchTemplateNameMalformed() *AWSError {
	return &AWSError{
		Code: ec2LaunchTemplateNameMalformedCode,
		Message: "The specified launch template name is invalid. A launch template name must be between 3 and " +
			"128 characters, and may contain letters, numbers, and the following characters: '-', '_', '.', " +
			"'/', '(', and ')'.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2LaunchTemplateIdentifierMalformed returns the Malformed refusal for the identifier a
// request resolves its template by, or nil when that identifier is well formed or absent.
//
// It checks the identifier [ec2LookupLaunchTemplate] resolves by — the ID when there is one,
// the name otherwise — so the syntax answer and the existence answer
// ([ec2LaunchTemplateNotFound]) are always about the same identifier (#1507).
//
// It is asked only about an identifier that resolved to nothing. A template already in state is
// acted on whatever its identifier looks like: one stored under a name the rule refuses —
// created by an older build, or written by a recorded event log that replays — must stay
// reachable, or the rule would lock it out and make the older recording
// fail on replay. A new name is held to the rule where it is introduced, at CreateLaunchTemplate.
// For every identifier AWS itself could have issued, checking shape only after the lookup
// misses gives the same answer as checking it first.
func ec2LaunchTemplateIdentifierMalformed(ltID, ltName string) *AWSError {
	switch {
	case ltID != "":
		if !ec2LaunchTemplateIDKind.wellFormed(ltID) {
			return ec2LaunchTemplateIDMalformed(ltID)
		}
	case ltName != "":
		if !ec2LaunchTemplateNameWellFormed(ltName) {
			return ec2LaunchTemplateNameMalformed()
		}
	}
	return nil
}

// requireLaunchTemplate resolves the template a request names by ID or name. An identifier
// that names nothing is refused as Malformed when it has the wrong shape and as NotFound
// otherwise — the two answers every operation naming one template gives (#1469, #1507). One
// that names a stored template is acted on, whatever its shape; see
// [ec2LaunchTemplateIdentifierMalformed].
func (p *EC2Plugin) requireLaunchTemplate(goCtx context.Context, ctx *RequestContext, ltID, ltName string) (*EC2LaunchTemplate, *AWSError) {
	if lt := p.resolveLaunchTemplate(goCtx, ctx, ltID, ltName); lt != nil {
		return lt, nil
	}
	if awsErr := ec2LaunchTemplateIdentifierMalformed(ltID, ltName); awsErr != nil {
		return nil, awsErr
	}
	return nil, ec2LaunchTemplateNotFound(ltID, ltName)
}

// ec2LTVersionItem is the launchTemplateVersion element shared by
// CreateLaunchTemplateVersion and DescribeLaunchTemplateVersions, whose response
// items are the same shape.
type ec2LTVersionItem struct {
	CreateTime         string              `xml:"createTime"`
	CreatedBy          string              `xml:"createdBy"`
	DefaultVersion     bool                `xml:"defaultVersion"`
	LaunchTemplateData ec2LTVersionDataXML `xml:"launchTemplateData"`
	LaunchTemplateID   string              `xml:"launchTemplateId"`
	LaunchTemplateName string              `xml:"launchTemplateName"`
	VersionDescription string              `xml:"versionDescription,omitempty"`
	VersionNumber      int64               `xml:"versionNumber"`
}

// ec2ValidationWarningXML is AWS's ValidationWarning: the `warning` member
// CreateLaunchTemplate and CreateLaunchTemplateVersion return for "parameters or
// parameter combinations that are not valid".
//
// Both operations document the member and neither documents an error for an invalid
// launch template, so a template carrying a mapping RunInstances refuses is *created*
// and warned about rather than refused — see [ec2CollectBlockDeviceMappings] (#693).
//
// It is always rendered through a pointer field, never a value: encoding/xml ignores
// omitempty on a struct, so a value field would emit an empty `<warning></warning>` on
// every valid template and a caller checking for the element's presence would read every
// template as warned about.
type ec2ValidationWarningXML struct {
	// Errors is ValidationWarning's sole member, ErrorSet.N. It carries no omitempty:
	// a warning with no errors is a shape substrate never builds, since the pointer is
	// nil when there is nothing to report.
	Errors []ec2ValidationErrorXML `xml:"errorSet>item"`
}

// ec2ValidationErrorXML is one ValidationError within a warning — AWS's two members are
// code and message, both documented as pointing at the same error-code table the
// equivalent refusal draws its code from.
type ec2ValidationErrorXML struct {
	Code    string `xml:"code"`
	Message string `xml:"message"`
}

// ec2ValidationWarningFor renders a collected problem list as the `warning` member, or
// nil when there is nothing to warn about.
//
// Returning nil for an empty list is what keeps the element absent from a valid
// template's response; see [ec2ValidationWarningXML] for why that cannot be a struct
// tag's job.
func ec2ValidationWarningFor(problems []*AWSError) *ec2ValidationWarningXML {
	if len(problems) == 0 {
		return nil
	}
	errs := make([]ec2ValidationErrorXML, 0, len(problems))
	for _, p := range problems {
		errs = append(errs, ec2ValidationErrorXML{Code: p.Code, Message: p.Message})
	}
	return &ec2ValidationWarningXML{Errors: errs}
}

// ec2LTVersionDataXML renders a version's stored parameters as AWS's
// ResponseLaunchTemplateData.
//
// The network-interface fields are nested rather than top-level because
// ResponseLaunchTemplateData has no top-level SubnetId member — a template can only
// name a subnet, or a public-IP preference, inside a network interface. That is the
// same asymmetry [EC2LaunchTemplateData.SubnetID] documents on the storage side.
//
// Note that the tag member is named tagSpecificationSet on the way out and
// TagSpecification.N on the way in: ResponseLaunchTemplateData and
// RequestLaunchTemplateData spell it differently, so a round-trip test has to use
// both names.
//
// Every list member is an [ec2ItemSet] pointer rather than a slice behind an
// `xml:"…Set>item,omitempty"` path (#1471). encoding/xml writes the parent of an `a>b`
// path before it consults omitempty on the child, so an empty slice still rendered
// `<networkInterfaceSet></networkInterfaceSet>` — and an SDK decodes that as a present,
// empty NetworkInterfaces list. Every template naming no interface read back with a
// NetworkInterfaces key the request never carried, and the same was true of each list a
// template left empty. A nil pointer omits the element whole.
type ec2LTVersionDataXML struct {
	BlockDeviceMappings   *ec2ItemSet[ec2LTVersionBDMXML]     `xml:"blockDeviceMappingSet,omitempty"`
	DisableAPITermination *bool                               `xml:"disableApiTermination,omitempty"`
	IamInstanceProfile    *ec2LTVersionProfileXML             `xml:"iamInstanceProfile,omitempty"`
	ImageID               string                              `xml:"imageId,omitempty"`
	InstanceType          string                              `xml:"instanceType,omitempty"`
	KeyName               string                              `xml:"keyName,omitempty"`
	MetadataOptions       *ec2LTMetadataOptionsXML            `xml:"metadataOptions,omitempty"`
	UserData              string                              `xml:"userData,omitempty"`
	SecurityGroupIDs      *ec2ItemSet[string]                 `xml:"securityGroupIdSet,omitempty"`
	NetworkInterfaces     *ec2ItemSet[ec2LTVersionNetIfcXML]  `xml:"networkInterfaceSet,omitempty"`
	TagSpecifications     *ec2ItemSet[ec2LTVersionTagSpecXML] `xml:"tagSpecificationSet,omitempty"`
}

// ec2ItemSet is an EC2 list element — `<xSet><item>…</item></xSet>` — that a nil pointer
// omits entirely. See [ec2LTVersionDataXML] for why a slice behind a `>` path cannot.
type ec2ItemSet[T any] struct {
	// Items are the list's members, in order.
	Items []T `xml:"item"`
}

// ec2Items wraps items as an [ec2ItemSet], or returns nil for an empty list so that the
// element is left out.
func ec2Items[T any](items []T) *ec2ItemSet[T] {
	if len(items) == 0 {
		return nil
	}
	return &ec2ItemSet[T]{Items: items}
}

// ec2LTVersionBDMXML is one LaunchTemplateBlockDeviceMapping within a version's data.
//
// It is what lets a caller read back the mapping a `warning` is about, and see what a
// SourceVersion-derived version inherited (#693).
//
// noDevice is a string on the wire, not a bool: AWS documents it as "To omit the device
// from the block device mapping, specify an empty string", so its presence is the signal
// and its value carries nothing. Substrate stores a bool and renders the empty string
// through a pointer, which is the only way encoding/xml can emit `<noDevice></noDevice>`
// for a suppressed device and nothing at all for every other mapping.
type ec2LTVersionBDMXML struct {
	DeviceName  string                 `xml:"deviceName,omitempty"`
	VirtualName string                 `xml:"virtualName,omitempty"`
	NoDevice    *string                `xml:"noDevice"`
	Ebs         *ec2LTVersionEbsBDMXML `xml:"ebs,omitempty"`
}

// ec2LTVersionEbsBDMXML is a mapping's LaunchTemplateEbsBlockDevice.
//
// Every member is a pointer or omitempty for the same reason the raw strings exist on
// [EC2BlockDeviceMapping]: a mapping that named no size is a different request from one
// that named zero, and rendering a 0 for the first would report a value the caller never
// sent. deleteOnTermination is a *bool because the stored field keeps three states —
// absent, true, false — and AWS's own member is a Boolean that a template can leave
// unset.
type ec2LTVersionEbsBDMXML struct {
	DeleteOnTermination *bool  `xml:"deleteOnTermination,omitempty"`
	Encrypted           *bool  `xml:"encrypted,omitempty"`
	Iops                int    `xml:"iops,omitempty"`
	SnapshotID          string `xml:"snapshotId,omitempty"`
	Throughput          int    `xml:"throughput,omitempty"`
	VolumeSize          int    `xml:"volumeSize,omitempty"`
	VolumeType          string `xml:"volumeType,omitempty"`
}

// ec2LTVersionNetIfcXML is one network interface within a version's data.
//
// groupSet is an [ec2ItemSet] and deleteOnTermination a pointer for #1471's reason: an
// interface naming no groups read back with an empty groupSet, and one that did not state
// deleteOnTermination read back with the launch default as though it had.
type ec2LTVersionNetIfcXML struct {
	DeviceIndex              int                 `xml:"deviceIndex"`
	SubnetID                 string              `xml:"subnetId,omitempty"`
	AssociatePublicIPAddress string              `xml:"associatePublicIpAddress,omitempty"`
	Groups                   *ec2ItemSet[string] `xml:"groupSet,omitempty"`
	NetworkInterfaceID       string              `xml:"networkInterfaceId,omitempty"`
	PrivateIPAddress         string              `xml:"privateIpAddress,omitempty"`
	Description              string              `xml:"description,omitempty"`
	InterfaceType            string              `xml:"interfaceType,omitempty"`
	NetworkCardIndex         int                 `xml:"networkCardIndex,omitempty"`
	DeleteOnTermination      *bool               `xml:"deleteOnTermination,omitempty"`
}

// ec2LTVersionProfileXML is a version's LaunchTemplateIamInstanceProfileSpecification,
// whose two members are arn and name.
type ec2LTVersionProfileXML struct {
	ARN  string `xml:"arn,omitempty"`
	Name string `xml:"name,omitempty"`
}

// ec2LTVersionTagSpecXML is one LaunchTemplateTagSpecification: a resource type and
// the tags scoped to it.
type ec2LTVersionTagSpecXML struct {
	ResourceType string       `xml:"resourceType"`
	Tags         []ec2TagItem `xml:"tagSet>item"`
}

// ec2LTVersionData renders stored template data for the wire.
func ec2LTVersionData(d EC2LaunchTemplateData) ec2LTVersionDataXML {
	out := ec2LTVersionDataXML{
		ImageID:          d.ImageID,
		InstanceType:     d.InstanceType,
		KeyName:          d.KeyName,
		UserData:         d.UserData,
		SecurityGroupIDs: ec2Items(d.SecurityGroupIDs),
		MetadataOptions:  ec2LTMetadataOptions(d.MetadataOptions),
	}
	if d.DisableAPITermination != "" {
		v := d.DisableAPITermination == "true"
		out.DisableAPITermination = &v
	}
	// The profile is stored as one string, so it is echoed back in whichever member
	// it arrived in: an "arn:"-prefixed value is an arn and anything else is a name.
	// Synthesizing the other member — as DescribeInstances does, where AWS's response
	// shape requires an ARN — would report a template as naming something the caller
	// never wrote.
	if d.IamInstanceProfile != "" {
		if strings.HasPrefix(d.IamInstanceProfile, "arn:") {
			out.IamInstanceProfile = &ec2LTVersionProfileXML{ARN: d.IamInstanceProfile}
		} else {
			out.IamInstanceProfile = &ec2LTVersionProfileXML{Name: d.IamInstanceProfile}
		}
	}
	out.BlockDeviceMappings = ec2Items(ec2LTVersionBDMs(d.BlockDeviceMappings))
	// The two stored scopes read back, instance then volume; see
	// [EC2LaunchTemplateData.TagSpecifications] (#670, #1471). The other scopes the request
	// shape allows are not stored, so they cannot read back.
	var specs []ec2LTVersionTagSpecXML
	for _, scope := range []struct {
		resourceType string
		tags         []EC2Tag
	}{{"instance", d.TagSpecifications}, {"volume", d.VolumeTagSpecifications}} {
		if len(scope.tags) == 0 {
			continue
		}
		spec := ec2LTVersionTagSpecXML{ResourceType: scope.resourceType}
		for _, t := range scope.tags {
			spec.Tags = append(spec.Tags, ec2TagItem(t))
		}
		specs = append(specs, spec)
	}
	out.TagSpecifications = ec2Items(specs)
	// Every interface the template declared reads back, not just the primary (#455).
	//
	// The flat-field fallback below is what a template stored before the slice existed
	// carries, so a replayed event log still reports its networking rather than
	// nothing. Either way an interface is emitted only when the template actually
	// named one, so a template carrying no networking does not read back with a
	// phantom eth0.
	var interfaces []ec2LTVersionNetIfcXML
	for _, ifc := range d.NetworkInterfaces {
		item := ec2LTVersionNetIfcXML{
			DeviceIndex:              ifc.DeviceIndex,
			SubnetID:                 ifc.SubnetID,
			AssociatePublicIPAddress: ifc.AssociatePublicIPAddress,
			Groups:                   ec2Items(ifc.SecurityGroupIDs),
			NetworkInterfaceID:       ifc.NetworkInterfaceID,
			PrivateIPAddress:         ifc.PrivateIPAddress,
			Description:              ifc.Description,
			InterfaceType:            ifc.InterfaceType,
			NetworkCardIndex:         ifc.NetworkCardIndex,
		}
		if !ifc.DeleteOnTerminationDefaulted {
			dot := ifc.DeleteOnTermination
			item.DeleteOnTermination = &dot
		}
		interfaces = append(interfaces, item)
	}
	if len(interfaces) == 0 &&
		(d.SubnetID != "" || d.AssociatePublicIPAddress != "" || len(d.NetworkInterfaceGroups) > 0) {
		// A template stored before the slice existed, which recorded no statement of the
		// member: it reads back as it always has.
		dot := true
		interfaces = []ec2LTVersionNetIfcXML{{
			DeviceIndex:              0,
			SubnetID:                 d.SubnetID,
			AssociatePublicIPAddress: d.AssociatePublicIPAddress,
			Groups:                   ec2Items(d.NetworkInterfaceGroups),
			DeleteOnTermination:      &dot,
		}}
	}
	out.NetworkInterfaces = ec2Items(interfaces)
	return out
}

// ec2LTVersionBDMs renders a version's stored block device mappings for the wire.
//
// A mapping is emitted whether or not it would materialize a volume — a virtualName-only
// entry and a NoDevice suppression are both parameters the template carries, and the
// point of reading them back is to see what the template says rather than what a launch
// from it would do.
//
// The ebs member is omitted entirely for a mapping that names none, so an instance-store
// or suppressed device does not read back with an empty `<ebs/>` that a caller could
// mistake for an EBS volume with defaults.
func ec2LTVersionBDMs(mappings []EC2BlockDeviceMapping) []ec2LTVersionBDMXML {
	if len(mappings) == 0 {
		return nil
	}
	empty := ""
	out := make([]ec2LTVersionBDMXML, 0, len(mappings))
	for _, bdm := range mappings {
		item := ec2LTVersionBDMXML{DeviceName: bdm.DeviceName, VirtualName: bdm.VirtualName}
		if bdm.NoDevice {
			item.NoDevice = &empty
		}
		if ec2NamesEbsMember(bdm) {
			ebs := &ec2LTVersionEbsBDMXML{
				Iops:       bdm.IOPS,
				SnapshotID: bdm.SnapshotID,
				Throughput: bdm.Throughput,
				VolumeSize: bdm.VolumeSize,
				VolumeType: bdm.VolumeType,
			}
			if bdm.Encrypted {
				encrypted := true
				ebs.Encrypted = &encrypted
			}
			// The stored value is the raw parameter, so "" is absent rather than false;
			// see [EC2BlockDeviceMapping.DeleteOnTermination].
			if bdm.DeleteOnTermination != "" {
				dot := bdm.DeleteOnTermination == "true"
				ebs.DeleteOnTermination = &dot
			}
			item.Ebs = ebs
		}
		out = append(out, item)
	}
	return out
}

// ec2LTVersionItemFor builds a response item for one version of a template.
func ec2LTVersionItemFor(lt *EC2LaunchTemplate, v EC2LaunchTemplateVersion) ec2LTVersionItem {
	return ec2LTVersionItem{
		CreateTime:         v.CreateTime,
		CreatedBy:          v.CreatedBy,
		DefaultVersion:     v.VersionNumber == lt.DefaultVersionNum,
		LaunchTemplateData: ec2LTVersionData(v.Data),
		LaunchTemplateID:   lt.LaunchTemplateID,
		LaunchTemplateName: lt.LaunchTemplateName,
		VersionDescription: v.VersionDescription,
		VersionNumber:      v.VersionNumber,
	}
}

// putLaunchTemplate writes a launch template back to state.
func (p *EC2Plugin) putLaunchTemplate(goCtx context.Context, lt *EC2LaunchTemplate) error {
	ltJSON, err := json.Marshal(lt)
	if err != nil {
		return fmt.Errorf("putLaunchTemplate: marshal: %w", err)
	}
	key := ec2LaunchTemplateStateKey(lt.AccountID, lt.Region, lt.LaunchTemplateID)
	if err := p.state.Put(goCtx, ec2Namespace, key, ltJSON); err != nil {
		return fmt.Errorf("putLaunchTemplate: put: %w", err)
	}
	return nil
}

// createLaunchTemplateVersion handles the CreateLaunchTemplateVersion action.
func (p *EC2Plugin) createLaunchTemplateVersion(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()

	lt, awsErr := p.requireLaunchTemplate(goCtx, ctx, req.Params["LaunchTemplateId"], req.Params["LaunchTemplateName"])
	if awsErr != nil {
		return nil, awsErr
	}

	data, awsErr := parseLaunchTemplateData(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}

	// SourceVersion inheritance, per the API reference: "the new version inherits
	// the launch parameters from the source version, except for parameters that you
	// specify in LaunchTemplateData. […] any additional launch parameters that you
	// specify […] overwrite any corresponding launch parameters inherited from the
	// source version."
	//
	// So an omitted SourceVersion inherits *nothing* — the new version holds only
	// what the request names. That asymmetry is easy to get backwards, and getting
	// it backwards silently carries forward parameters the caller meant to drop.
	if src := req.Params["SourceVersion"]; src != "" {
		srcVersion, awsErr := ec2ResolveTemplateVersion(lt, src)
		if awsErr != nil {
			return nil, awsErr
		}
		data = ec2OverlayTemplateData(srcVersion.Data, data)
	}

	// Checked after the overlay so an inherited tag set is checked too — a source
	// version predating this check can carry a reserved key that the new version
	// would otherwise inherit silently (#471).
	if awsErr := ec2CheckTemplateTags(data); awsErr != nil {
		return nil, awsErr
	}

	versions := lt.TemplateVersions()
	newVersion := EC2LaunchTemplateVersion{
		VersionNumber:      lt.LatestVersionNum + 1,
		VersionDescription: req.Params["VersionDescription"],
		CreateTime:         p.tc.Now().UTC().Format(time.RFC3339),
		CreatedBy:          ctx.AccountID,
		Data:               data,
	}
	lt.Versions = append(versions, newVersion)
	lt.LatestVersionNum = newVersion.VersionNumber
	// LatestData tracks the newest version; see its field comment for why it is kept
	// rather than derived.
	lt.LatestData = data

	if err := p.putLaunchTemplate(goCtx, lt); err != nil {
		return nil, err
	}

	// The warning goes on this outer struct rather than on ec2LTVersionItem, which
	// DescribeLaunchTemplateVersions shares: only the two create operations document
	// the member, and a describe reporting one would invent it.
	type response struct {
		XMLName xml.Name                 `xml:"CreateLaunchTemplateVersionResponse"`
		XMLNS   string                   `xml:"xmlns,attr"`
		Version ec2LTVersionItem         `xml:"launchTemplateVersion"`
		Warning *ec2ValidationWarningXML `xml:"warning,omitempty"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:   "http://ec2.amazonaws.com/doc/2016-11-15/",
		Version: ec2LTVersionItemFor(lt, newVersion),
		// Collected after the overlay, for the reason the tag check runs there: a
		// version inheriting a mapping from its source version is warned about it too.
		// The nil observer skips the snapshot-state rule for the reason
		// CreateLaunchTemplate's does; see [ec2CheckMappingSnapshot].
		Warning: ec2ValidationWarningFor(
			ec2CollectBlockDeviceMappings(data.BlockDeviceMappings, p.ec2SnapshotResolver(ctx), nil)),
	})
}

// ec2OverlayTemplateData returns src with every field overlay names replaced.
//
// Field-by-field rather than reflective so that adding a field to
// EC2LaunchTemplateData is a visible omission here — a reflective merge would
// silently do the wrong thing for any field whose zero value is meaningful.
func ec2OverlayTemplateData(src, overlay EC2LaunchTemplateData) EC2LaunchTemplateData {
	out := src
	if overlay.ImageID != "" {
		out.ImageID = overlay.ImageID
	}
	if overlay.InstanceType != "" {
		out.InstanceType = overlay.InstanceType
	}
	if overlay.KeyName != "" {
		out.KeyName = overlay.KeyName
	}
	if overlay.UserData != "" {
		out.UserData = overlay.UserData
	}
	if len(overlay.SecurityGroupIDs) > 0 {
		out.SecurityGroupIDs = overlay.SecurityGroupIDs
	}
	// SubnetID, AssociatePublicIPAddress and NetworkInterfaceGroups are not overlaid on their
	// own: [parseLaunchTemplateData] sets them only from the primary of a non-empty interface
	// list, so they are replaced with that list below.
	if len(overlay.TagSpecifications) > 0 {
		out.TagSpecifications = overlay.TagSpecifications
	}
	// Both scopes are overlaid, not just the instance one: a source version's volume
	// tags survived into the new version and its block device mappings did not, so a
	// SourceVersion-derived version silently launched instances with no mappings while
	// reporting the same template data (#693). The omission was invisible because
	// nothing rendered either member.
	if len(overlay.VolumeTagSpecifications) > 0 {
		out.VolumeTagSpecifications = overlay.VolumeTagSpecifications
	}
	if len(overlay.BlockDeviceMappings) > 0 {
		out.BlockDeviceMappings = overlay.BlockDeviceMappings
	}
	if overlay.IamInstanceProfile != "" {
		out.IamInstanceProfile = overlay.IamInstanceProfile
	}
	// MetadataOptions is one RequestLaunchTemplateData parameter, so a version naming it
	// replaces the source's whole rather than per option: the reference's rule is about
	// "parameters that you specify in LaunchTemplateData", and the options are members of
	// one (#1471).
	if overlay.MetadataOptions != nil {
		out.MetadataOptions = overlay.MetadataOptions
	}
	if overlay.DisableAPITermination != "" {
		out.DisableAPITermination = overlay.DisableAPITermination
	}
	// A version naming its own interfaces replaces the source's list — the one every read and
	// launch prefers — as well as the flat primary fields (#1471). The list and the primary's
	// flat fields are one parameter and are replaced together.
	if len(overlay.NetworkInterfaces) > 0 {
		out.NetworkInterfaces = overlay.NetworkInterfaces
		out.SubnetID = overlay.SubnetID
		out.AssociatePublicIPAddress = overlay.AssociatePublicIPAddress
		out.NetworkInterfaceGroups = overlay.NetworkInterfaceGroups
	}
	return out
}

// modifyLaunchTemplate handles the ModifyLaunchTemplate action.
func (p *EC2Plugin) modifyLaunchTemplate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()

	lt, awsErr := p.requireLaunchTemplate(goCtx, ctx, req.Params["LaunchTemplateId"], req.Params["LaunchTemplateName"])
	if awsErr != nil {
		return nil, awsErr
	}

	// SetDefaultVersion is the only modifiable attribute, and it is Required: No —
	// a request naming nothing is a no-op that still returns the template.
	if spec := req.Params["SetDefaultVersion"]; spec != "" {
		v, awsErr := ec2ResolveTemplateVersion(lt, spec)
		if awsErr != nil {
			return nil, awsErr
		}
		lt.DefaultVersionNum = v.VersionNumber
		if err := p.putLaunchTemplate(goCtx, lt); err != nil {
			return nil, err
		}
	}

	type response struct {
		XMLName        xml.Name             `xml:"ModifyLaunchTemplateResponse"`
		XMLNS          string               `xml:"xmlns,attr"`
		LaunchTemplate ec2LaunchTemplateXML `xml:"launchTemplate"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:          "http://ec2.amazonaws.com/doc/2016-11-15/",
		LaunchTemplate: ec2LaunchTemplateSummary(lt),
	})
}

// describeLaunchTemplateVersions handles the DescribeLaunchTemplateVersions action.
//
// Four of AWS's fourteen Filter.N names are evaluated, per [ec2LaunchTemplateVersionFilterSpec]
// (#695). The filter names are checked before anything
// is resolved, so an undocumented name is refused rather than answered after a NotFound for
// the template — the ordering #687 established and [TestEC2_FilterNames_UndocumentedNameRefusedBeforeTheScan]
// pins.
//
// Filters apply before pagination, so a page holds MaxResults *matching* versions rather than
// MaxResults versions of which some match.
func (p *EC2Plugin) describeLaunchTemplateVersions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()

	if err := ec2LaunchTemplateVersionFilterSpec().check(req.Params); err != nil {
		return nil, err
	}
	filters := extractEC2Filters(req.Params)

	maxResults, awsErr := ec2MaxResults(req.Params, ec2MinLaunchTemplateResults, ec2MaxLaunchTemplateResults)
	if awsErr != nil {
		return nil, awsErr
	}
	// Naming no MaxResults pages at the published maximum rather than answering the whole
	// listing, which is DescribeTags' default too and is kept for the same reason (#917): this
	// page publishes no unpaginated default, and paging at 200 is what existing callers see.
	// Its sibling DescribeLaunchTemplates reads an absent MaxResults as the whole listing
	// instead (#1024) — the two pages share a published range and not a default, because
	// neither publishes one.
	if maxResults == 0 {
		maxResults = ec2MaxLaunchTemplateResults
	}
	offset, awsErr := ec2NextTokenOffset(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}

	specs := indexedParams(req.Params, "LaunchTemplateVersion.%d", "Versions.member.%d")
	ltID := req.Params["LaunchTemplateId"]
	ltName := req.Params["LaunchTemplateName"]

	var items []ec2LTVersionItem
	if ltID == "" && ltName == "" {
		// The account-wide form: naming no template lists every template's $Latest
		// and/or $Default. AWS restricts it to those two aliases — a version number
		// makes no sense across templates — and rejects a request that names neither.
		var awsErr *AWSError
		items, awsErr = p.describeVersionsAccountWide(goCtx, ctx, specs)
		if awsErr != nil {
			return nil, awsErr
		}
	} else {
		lt, awsErr := p.requireLaunchTemplate(goCtx, ctx, ltID, ltName)
		if awsErr != nil {
			return nil, awsErr
		}
		items, awsErr = p.describeVersionsOfTemplate(lt, specs, req.Params)
		if awsErr != nil {
			return nil, awsErr
		}
	}

	if len(filters) > 0 {
		kept := make([]ec2LTVersionItem, 0, len(items))
		for _, item := range items {
			if ec2LTVersionMatchesFilters(item, filters) {
				kept = append(kept, item)
			}
		}
		items = kept
	}

	// A version's ImageId reads back as stored — a resolve:ssm: reference stays one — unless
	// the request sets ResolveAlias, which the reference documents as "If true, and if a
	// Systems Manager parameter is specified for ImageId, the AMI ID is displayed in the
	// Pagination is offset-based over a stable ordering, matching the other EC2
	// describes: the token is the index to resume at. The offset is read before the
	// templates are resolved (#917), so a token that is not one is refused whether or not
	// the request also names a template that does not exist.
	page, nextToken := ec2Page(items, offset, maxResults)

	// A version's ImageId reads back as stored — a resolve:ssm: reference stays one — unless
	// the request sets ResolveAlias, which the reference documents as "If true, and if a
	// Systems Manager parameter is specified for ImageId, the AMI ID is displayed in the
	// response for imageId", with "Default: false" (#1506). Only the page being answered is
	// resolved, so a parameter read costs nothing for versions on other pages. The parameter is
	// resolved the way a launch resolves it ([ec2ResolveSSMImageID]). One that does not resolve
	// is left as the reference: the page documents no refusal for it, and the describe is not a
	// launch. A store that cannot be read is an error, not a reference left as stored.
	if strings.EqualFold(req.Params["ResolveAlias"], "true") {
		for i := range page {
			if !ec2IsSSMImageID(page[i].LaunchTemplateData.ImageID) {
				continue
			}
			resolved, resolveErr := ec2ResolveSSMImageID(goCtx, p.state, ctx, page[i].LaunchTemplateData.ImageID)
			var unresolved *AWSError
			switch {
			case errors.As(resolveErr, &unresolved):
			case resolveErr != nil:
				return nil, resolveErr
			default:
				page[i].LaunchTemplateData.ImageID = resolved
			}
		}
	}

	type response struct {
		XMLName   xml.Name           `xml:"DescribeLaunchTemplateVersionsResponse"`
		XMLNS     string             `xml:"xmlns,attr"`
		Versions  []ec2LTVersionItem `xml:"launchTemplateVersionSet>item"`
		NextToken string             `xml:"nextToken,omitempty"`
	}
	return ec2XMLResponse(http.StatusOK, response{
		XMLNS:     "http://ec2.amazonaws.com/doc/2016-11-15/",
		Versions:  page,
		NextToken: nextToken,
	})
}

// describeVersionsOfTemplate returns the requested versions of one template.
func (p *EC2Plugin) describeVersionsOfTemplate(lt *EC2LaunchTemplate, specs []string, params map[string]string) ([]ec2LTVersionItem, *AWSError) {
	if len(specs) > 0 {
		items := make([]ec2LTVersionItem, 0, len(specs))
		for _, spec := range specs {
			v, awsErr := ec2ResolveTemplateVersion(lt, spec)
			if awsErr != nil {
				return nil, awsErr
			}
			items = append(items, ec2LTVersionItemFor(lt, *v))
		}
		return items, nil
	}

	// MinVersion is "the version number after which to describe launch template
	// versions" and MaxVersion "the version number up to which to describe" — both
	// inclusive bounds on the returned range.
	minVersion, awsErr := ec2VersionBound(params["MinVersion"])
	if awsErr != nil {
		return nil, awsErr
	}
	maxVersion, awsErr := ec2VersionBound(params["MaxVersion"])
	if awsErr != nil {
		return nil, awsErr
	}

	versions := lt.TemplateVersions()
	sorted := make([]EC2LaunchTemplateVersion, len(versions))
	copy(sorted, versions)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].VersionNumber < sorted[j].VersionNumber })

	items := make([]ec2LTVersionItem, 0, len(sorted))
	for _, v := range sorted {
		if minVersion > 0 && v.VersionNumber < minVersion {
			continue
		}
		if maxVersion > 0 && v.VersionNumber > maxVersion {
			continue
		}
		items = append(items, ec2LTVersionItemFor(lt, v))
	}
	return items, nil
}

// ec2VersionBound parses a MinVersion/MaxVersion bound, returning 0 when absent.
func ec2VersionBound(raw string) (int64, *AWSError) {
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 1 {
		return 0, &AWSError{
			Code:       "InvalidParameterValue",
			Message:    "Invalid launch template version number: " + raw,
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return n, nil
}

// describeVersionsAccountWide returns each template's $Latest and/or $Default.
//
// Reuses the same lt_ids: index walk describeLaunchTemplates does, which is why
// this form is a small addition rather than a separate query path.
func (p *EC2Plugin) describeVersionsAccountWide(goCtx context.Context, ctx *RequestContext, specs []string) ([]ec2LTVersionItem, *AWSError) {
	wantLatest, wantDefault := false, false
	for _, spec := range specs {
		switch strings.ToLower(strings.TrimSpace(spec)) {
		case ec2LTVersionLatest:
			wantLatest = true
		case ec2LTVersionDefault:
			wantDefault = true
		default:
			// A version number is meaningless without a template to number within.
			return nil, &AWSError{
				Code:       "InvalidParameterValue",
				Message:    "To describe the launch template data for all your launch templates, for 'Versions' specify '$Latest', '$Default', or both, and omit 'LaunchTemplateId' and 'LaunchTemplateName'.",
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
	if !wantLatest && !wantDefault {
		return nil, &AWSError{
			Code:       "MissingParameter",
			Message:    "Either a launch template ID or name, or a Versions value of '$Latest' or '$Default', is required",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	idsKey := "lt_ids:" + ctx.AccountID + "/" + ctx.Region
	ids, err := loadStringIndex(goCtx, p.state, ec2Namespace, idsKey)
	if err != nil {
		return nil, &AWSError{
			Code:       "InternalError",
			Message:    "Failed to list launch templates",
			HTTPStatus: http.StatusInternalServerError,
		}
	}

	var items []ec2LTVersionItem
	for _, id := range ids {
		key := ec2LaunchTemplateStateKey(ctx.AccountID, ctx.Region, id)
		data, err := p.state.Get(goCtx, ec2Namespace, key)
		if err != nil || data == nil {
			continue
		}
		var lt EC2LaunchTemplate
		if json.Unmarshal(data, &lt) != nil {
			continue
		}
		if wantLatest {
			if v, awsErr := ec2ResolveTemplateVersion(&lt, "$Latest"); awsErr == nil {
				items = append(items, ec2LTVersionItemFor(&lt, *v))
			}
		}
		if wantDefault {
			if v, awsErr := ec2ResolveTemplateVersion(&lt, "$Default"); awsErr == nil {
				items = append(items, ec2LTVersionItemFor(&lt, *v))
			}
		}
	}
	return items, nil
}

// deleteLaunchTemplateVersions handles the DeleteLaunchTemplateVersions action.
//
// Outcomes are reported per version, not as a whole-request error: the response
// carries successfullyDeletedLaunchTemplateVersionSet and
// unsuccessfullyDeletedLaunchTemplateVersionSet, and a request naming both a
// deletable and an undeletable version succeeds for one and fails for the other at
// HTTP 200. Same trap as SendMessageBatch — a caller checking only the status code
// sees success.
func (p *EC2Plugin) deleteLaunchTemplateVersions(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()

	specs := indexedParams(req.Params, "LaunchTemplateVersion.%d", "Versions.member.%d")
	if len(specs) == 0 {
		return nil, &AWSError{
			Code:       "MissingParameter",
			Message:    "LaunchTemplateVersion is required",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	type responseErrorXML struct {
		Code    string `xml:"code"`
		Message string `xml:"message"`
	}
	type deletedItem struct {
		LaunchTemplateID   string `xml:"launchTemplateId"`
		LaunchTemplateName string `xml:"launchTemplateName"`
		VersionNumber      int64  `xml:"versionNumber"`
	}
	type failedItem struct {
		LaunchTemplateID   string           `xml:"launchTemplateId"`
		LaunchTemplateName string           `xml:"launchTemplateName"`
		ResponseError      responseErrorXML `xml:"responseError"`
		VersionNumber      int64            `xml:"versionNumber"`
	}
	type response struct {
		XMLName xml.Name      `xml:"DeleteLaunchTemplateVersionsResponse"`
		XMLNS   string        `xml:"xmlns,attr"`
		Deleted []deletedItem `xml:"successfullyDeletedLaunchTemplateVersionSet>item"`
		Failed  []failedItem  `xml:"unsuccessfullyDeletedLaunchTemplateVersionSet>item"`
	}
	var deleted []deletedItem
	var failed []failedItem
	respond := func() (*AWSResponse, error) {
		return ec2XMLResponse(http.StatusOK, response{
			XMLNS:   "http://ec2.amazonaws.com/doc/2016-11-15/",
			Deleted: deleted,
			Failed:  failed,
		})
	}

	// An identifier of the wrong shape that names nothing is reported per version, through
	// ResponseError.code's own launchTemplateIdMalformed and launchTemplateNameMalformed, rather
	// than as the whole-request InvalidLaunchTemplateId.Malformed the other operations answer
	// (#1507): this operation publishes a per-item code for exactly that condition. The message
	// is the whole-request refusal's, so the two forms say the same thing. A stored template is
	// acted on whatever its identifier looks like ([ec2LaunchTemplateIdentifierMalformed]). A
	// template that does not exist is still the whole-request NotFound (#1469); the enum's
	// launchTemplateIdDoesNotExist and launchTemplateNameDoesNotExist are not raised, because no
	// capture shows which of the two forms real EC2 uses for absence (#1533).
	ltID, ltName := req.Params["LaunchTemplateId"], req.Params["LaunchTemplateName"]
	lt := p.resolveLaunchTemplate(goCtx, ctx, ltID, ltName)
	if lt == nil {
		malformed := ec2LaunchTemplateIdentifierMalformed(ltID, ltName)
		if malformed == nil {
			return nil, ec2LaunchTemplateNotFound(ltID, ltName)
		}
		code := "launchTemplateNameMalformed"
		if ltID != "" {
			code = "launchTemplateIdMalformed"
		}
		for _, spec := range specs {
			item := failedItem{
				LaunchTemplateID:   ltID,
				LaunchTemplateName: ltName,
				ResponseError:      responseErrorXML{Code: code, Message: malformed.Message},
			}
			if n, err := strconv.ParseInt(spec, 10, 64); err == nil {
				item.VersionNumber = n
			}
			failed = append(failed, item)
		}
		return respond()
	}
	remaining := lt.TemplateVersions()

	for _, spec := range specs {
		v, awsErr := ec2ResolveTemplateVersion(lt, spec)
		if awsErr != nil {
			// A missing version is the one failure mode with a modeled code:
			// ResponseError.code's documented valid values are exactly
			// launchTemplateIdDoesNotExist | launchTemplateIdMalformed |
			// launchTemplateNameDoesNotExist | launchTemplateNameMalformed |
			// launchTemplateVersionDoesNotExist | unexpectedError.
			failed = append(failed, failedItem{
				LaunchTemplateID:   lt.LaunchTemplateID,
				LaunchTemplateName: lt.LaunchTemplateName,
				ResponseError: responseErrorXML{
					Code:    "launchTemplateVersionDoesNotExist",
					Message: awsErr.Message,
				},
			})
			continue
		}

		// "You can't delete the default version of a launch template; you must first
		// assign a different version as the default." — DeleteLaunchTemplateVersions.
		//
		// The code is unexpectedError because ResponseError.code is a closed
		// six-value enum (listed above) with no default-version member, and a
		// typed SDK deserializes anything outside it as an unknown variant. AWS's
		// actual code for this case is not published and no capture of the
		// rejection exists — searched the API reference, the CLI help, moto (which
		// does not implement this operation at all), LocalStack, and the SDK
		// models. The message is the reference's own sentence; the code is the
		// modeled catch-all. Both are inferred, not captured.
		if v.VersionNumber == lt.DefaultVersionNum {
			failed = append(failed, failedItem{
				LaunchTemplateID:   lt.LaunchTemplateID,
				LaunchTemplateName: lt.LaunchTemplateName,
				ResponseError: responseErrorXML{
					Code:    "unexpectedError",
					Message: "You can't delete the default version of a launch template; you must first assign a different version as the default.",
				},
				VersionNumber: v.VersionNumber,
			})
			continue
		}

		kept := make([]EC2LaunchTemplateVersion, 0, len(remaining))
		for _, existing := range remaining {
			if existing.VersionNumber != v.VersionNumber {
				kept = append(kept, existing)
			}
		}
		remaining = kept
		deleted = append(deleted, deletedItem{
			LaunchTemplateID:   lt.LaunchTemplateID,
			LaunchTemplateName: lt.LaunchTemplateName,
			VersionNumber:      v.VersionNumber,
		})
	}

	if len(deleted) > 0 {
		lt.Versions = remaining
		// LatestVersionNum tracks the highest surviving version. Deleting the newest
		// version lowers it, and the next CreateLaunchTemplateVersion then reuses
		// that number — which is wrong per the user guide ("You cannot replace the
		// version number after you delete it"), so the highest number ever issued is
		// what matters. Only lower it when the deleted version was not the latest,
		// i.e. never: LatestVersionNum is left alone deliberately.
		lt.LatestData = ec2LatestVersionData(remaining, lt.LatestData)
		if err := p.putLaunchTemplate(goCtx, lt); err != nil {
			return nil, err
		}
	}

	return respond()
}

// ec2LatestVersionData returns the highest-numbered version's data, falling back to
// fallback when no versions survive.
func ec2LatestVersionData(versions []EC2LaunchTemplateVersion, fallback EC2LaunchTemplateData) EC2LaunchTemplateData {
	out := fallback
	var highest int64
	for _, v := range versions {
		if v.VersionNumber > highest {
			highest = v.VersionNumber
			out = v.Data
		}
	}
	return out
}
